#pragma once
#include "Interface.hh"
#include "TWrapped.hh"

#include "OptimisticLockCoupling/Tree.h"
#include "Key.h"

#include "measure_latencies.hh"
#include <map>
#include <list>

/* 
 *    A transactional version of ART running on top of STO
 *    ------------------------------------------------------------------
 *    Author: Dimokritos Stamatakis
 *    April 9 2018
 */

#include "bloom.hh"
#include "bloom_packing.hh"

using namespace std;
#include <iostream>

#define DEBUG 0
#if DEBUG == 1
    #define PRINT_DEBUG(...) {printf(__VA_ARGS__);}
#else
    #define PRINT_DEBUG(...)  {}
#endif

#define DEBUG_VALIDATION 0
#if DEBUG_VALIDATION == 1
    #define PRINT_DEBUG_VALIDATION(...) {printf(__VA_ARGS__);}
#else
    #define PRINT_DEBUG_VALIDATION(...) {}
#endif

using namespace ART_OLC;


using ins_res = std::tuple<bool, bool>;
using rem_res = std::tuple<bool, bool>;
using lookup_res = std::tuple<TID, bool>;

enum bloom_t {packing, nopacking, nobloom};


template <typename T, typename W = TWrapped<T>>
class TART : public Tree, TObject {

	typedef typename W::version_type version_type;

	static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
	static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
	static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1;

	static constexpr uintptr_t nodeset_bit = 1LU << 63;
    static constexpr uintptr_t bloom_validation_bit = 1LU << 62;
    BloomPacking* bloomPacking;
    BloomNoPacking* bloomNoPacking;
    bloom_t bloom_type;
	
public:
    // virtual functions are costly in C++. Thus, keep a void* for bloom and cast it to the appropriate type internally
	TART(LoadKeyFunction loadKeyFun, bloom_t bloom_type) : Tree(loadKeyFun), bloom_type(bloom_type) { }

    void setBloom(void* bloom){
        switch(bloom_type){
            case packing:
                bloomPacking = (BloomPacking*) bloom;
                break;
            case nopacking:
                bloomNoPacking = (BloomNoPacking*)bloom;
                break;
            case nobloom:
                break;
        };
    }

	typedef struct record {
		// TODO: We might not need to store key here!
		// ART itself does not store actual keys, client is responsible for
		// TID to key mapping. We can find a key by calling loadKey(tid, key); (supplied key is empty and initialized by loadKey)
		Key key;
		TID val;
		version_type version;
		bool deleted;

		record(const Key& k, const TID v, bool valid):val(v),
		// Old STO Does not take a bool argument in version constructor
		//		version(valid? Sto::initialized_tid(): Sto::initialized_tid() | invalid_bit, !valid), deleted(false) {}
		version(valid? Sto::initialized_tid(): Sto::initialized_tid() | invalid_bit), deleted(false) {
			char * key_dat = new char[k.getKeyLen()];
			for(uint32_t i=0; i<k.getKeyLen(); i++){
				key_dat[i] = (char) k[i];
			}
			key.set(key_dat, k.getKeyLen());
		}

		bool valid() const {
			return !(version.value() & invalid_bit);
		}

	}record;

	static TID getTIDFromRec(TID tid){
		record* rec = reinterpret_cast<record*>(tid);
		return rec->val;
	}

	inline TID checkKeyFromRec(const TID tid, const Key& k) const{
        Key kt;
        this->loadKey(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

	lookup_res t_lookup(const Key& k, ThreadInfo& threadEpocheInfo){
		return t_lookup( k, threadEpocheInfo, true);
	}

	lookup_res t_lookup(const Key& k, ThreadInfo& threadEpocheInfo, bool validate){
		PRINT_DEBUG("Lookup key %s\n", keyToStr(k).c_str())
		trans_info* t_info = new trans_info();
		memset(t_info, 0, sizeof(trans_info));
		TID tid = lookup(k, threadEpocheInfo, t_info);
		if(t_info->check_key){ // call the TART check Key! (casting from rec*)
			tid = checkKeyFromRec(tid, k);
		}
		if (tid == 0){ // not found. Add parent in the node set!
            PRINT_DEBUG("Not found!\n")
			ns_add_node(std::get<0>(t_info->updated_node1), std::get<1>(t_info->updated_node1));
			return lookup_res(0, true);
		}
		record* rec = reinterpret_cast<record*>(tid);
        if(validate) {
            auto item = Sto::item(this, rec);
			if(has_delete(item)){ // current transaction already marked for deletion, reply as it is absent!
				return lookup_res(0, true);
			}
			// add to read set
			item.observe(rec->version);
            //item.add_read(rec->version);
        }
		return lookup_res(rec->val, true);
		//abort:
		//	return lookup_res(0, false);
	}

	ins_res t_insert(const Key & k, TID tid, ThreadInfo &epocheInfo){
		trans_info* t_info = new trans_info();
		memset(t_info, 0, sizeof(trans_info));
		PRINT_DEBUG("Transactionally Inserting (key:%s, tid:%lu)\n", keyToStr(k).c_str(), tid)
		insert(k, tid, epocheInfo, t_info); 
		N* n = t_info->cur_node;
		N* l_n = t_info->l_node;
		N* l_p_n = t_info->l_parent_node;
        N* updated_nodes [2] ;
        bzero(updated_nodes, 2* sizeof(N*));
        updated_nodes[0] = std::get<0>(t_info->updated_node1);
        updated_nodes[1] = std::get<0>(t_info->updated_node2);
        uint64_t updated_nodes_v [2];
        updated_nodes_v[0] = std::get<1>(t_info->updated_node1);
        updated_nodes_v[1] = std::get<1>(t_info->updated_node2);
		uint8_t keyslice = t_info->keyslice;
		if(t_info->updatedVal > 0){ // it is an update
            record* rec = reinterpret_cast<record*>(t_info->prevVal);
            auto item = Sto::item(this, rec);
            // update AVN in node set, if exists
            // Use the version number after the unlock! (+2)
            if(! ns_update_node_AVN(updated_nodes[0], updated_nodes_v[0], updated_nodes[0]->getVersion()+2)) {
                PRINT_DEBUG("UPDATE NODE FAIL!\n")
                if(t_info->w_unlock_obsolete)
                    l_n->writeUnlockObsolete();
                else
                    l_n->writeUnlock();
                return ins_res(false, false);
            }
            item.add_write(t_info->updatedVal);
            // TODO: In some runs l_n was null! Check it!
            if(t_info->w_unlock_obsolete)
                l_n->writeUnlockObsolete();
            else
                l_n->writeUnlock();
            return ins_res(true, true);
        }

        // create a poisoned record (invalid bit set)
		record* rec = new record(k, tid, false);
		PRINT_DEBUG("Creating new record %p with key %s\n", rec, keyToStr(k).c_str())
		auto item = Sto::item(this, rec);
		// add this record in the appropriate node
		switch(n->getType()){
			case NTypes::N4:
				(static_cast<N4*>(n))->insert(keyslice, N::setLeaf(reinterpret_cast<TID>(rec)));
				break;
			case NTypes::N16:
				(static_cast<N16*>(n))->insert(keyslice, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
			case NTypes::N48:
                (static_cast<N48*>(n))->insert(keyslice, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
			case NTypes::N256:
                (static_cast<N256*>(n))->insert(keyslice, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
		}
		// update AVN in node set, if exists
		// Include the +2 version number increment that happens at unlock!!
		// We cannot update the AVN after unlocking, because a concurrent transaction could alter the version number
		// and we will not detect it!
		if(! ns_update_node_AVN(updated_nodes[0], updated_nodes_v[0], updated_nodes[0]->getVersion()+2)) {
			l_n->writeUnlock();
            PRINT_DEBUG("UPDATE NODE 1 FAIL!\n")
            cout<<"UPDATE NODE 1 FAIL!\n";
			if(l_p_n){
		  		l_p_n->writeUnlock();
		  	}
			goto abort;
		}
        if(updated_nodes[1] != nullptr){
            if(! ns_update_node_AVN(updated_nodes[1], updated_nodes_v[1], updated_nodes[1]->getVersion()+2)) {
                l_n->writeUnlock();
                PRINT_DEBUG("UPDATE NODE 2 FAIL!\n")
                cout<<"UPDATE NODE 1 FAIL!\n";
                if(l_p_n){
                    l_p_n->writeUnlock();
                }
                goto abort;
            }
        }
		item.add_write();
		item.add_flags(insert_bit);
		PRINT_DEBUG("-- Unlocking node %p\n", l_n);
		if(t_info->w_unlock_obsolete)
            l_n->writeUnlockObsolete();
        else
        l_n->writeUnlock();
		if(l_p_n){
			PRINT_DEBUG("-- Unlocking parent node %p\n", l_p_n);
			l_p_n->writeUnlock();
		}
		if(l_n == n)
			PRINT_DEBUG("We are unlocking the node we just inserted to!! (%p)\n", n)
		PRINT_DEBUG(" ==== Now node's %p AVN:%lu\n", n, n->getVersion())
		//item_tmp = item.item();
		//rec_tmp = item_tmp.key<record*>();
		//PRINT_DEBUG("Inserted key %s\n", keyToStr(rec_tmp->key).c_str())
        return ins_res(true, true);
		abort:
			return ins_res(false, false);
	}

	rem_res t_remove(const Key & k, TID tid, ThreadInfo &threadEpocheInfo){
		bool tid_mismatch = false;
		trans_info* t_info = new trans_info();
		memset(t_info, 0, sizeof(trans_info));
        PRINT_DEBUG("Transactionally Removing (key:%s, tid:%lu)\n", keyToStr(k).c_str(), tid)
        TID lookup_tid = lookup(k, threadEpocheInfo, t_info);
        if(t_info->check_key){ // call the TART check Key! (casting from rec*)
            lookup_tid = checkKeyFromRec(lookup_tid, k); 
        }
		if(lookup_tid == 0){ // not found, add to node set!
			ns_add_node(std::get<0>(t_info->updated_node1), std::get<1>(t_info->updated_node1));
			return rem_res(false, true);
		}
		record* rec = reinterpret_cast<record*>(lookup_tid);
		if(rec->val != tid){ // that's the behavior of ART insert: If the encountered tuple id is different than the supplied one, return
			tid_mismatch = true;
		}
		if(rec->deleted) { // abort
			 return rem_res(false, false);
		}
		auto item = Sto::item(this, rec);
		item.observe(rec->version);
		if(tid_mismatch){
			PRINT_DEBUG("Oops, tid mismatch!\n")
			return rem_res(false, true);
		}
		item.add_write();
		fence();
		item.add_flags(delete_bit);
		return rem_res(true, true);
	}

    // add in a separate data structure! Performance is bad when we create a Sto::item per absent bloom filter element
    void bloom_v_add_key(uint64_t* hashVal){
        if(bloom_type == nobloom){
            fprintf(stderr, "Should not call bloom_v_add_key when nobloom is selected!\n");
            return;
        }
        if (( reinterpret_cast<uintptr_t>(hashVal) & bloom_validation_bit ) != 0){
            cout<<"Oops, 62nd bit is set!\n";
            cout<<"Oops, hashVal is "<<hashVal<<endl;
        }
        if (( reinterpret_cast<uintptr_t>(hashVal) & nodeset_bit) != 0)
            cout<<"Oops, 63rd bit is set!\n";
        auto item = Sto::item(this, get_bloomset_key(hashVal));
        if(!item.has_read()){
            item.add_read(0);
        }
    }

    private:
    
    uintptr_t get_bloomset_key(uint64_t* hashVal){
        return reinterpret_cast<uintptr_t>(hashVal) | bloom_validation_bit;
    }

    uint64_t* get_bloomset_hash_val(uintptr_t b){
        return reinterpret_cast<uint64_t*>(b & ~bloom_validation_bit);
    }

    bool is_in_bloomset(TransItem& item){
        return (item.key<uintptr_t>() & bloom_validation_bit) != 0;
    }

	// Adds a node and its AVN in the node set
	bool ns_add_node(N* node, uint64_t vers){
        auto item = Sto::item(this, get_nodeset_key(node));
        if(!item.has_read()){
            PRINT_DEBUG("Adding node %p to node set with version %lu\n", node, vers)
            item.add_read(vers);
        }
        return false;
    }

	// Updates the AVN of a node in the node set
	bool ns_update_node_AVN(N* n, uint64_t before_vers, uint64_t after_vers){
		auto item = Sto::item(this,	get_nodeset_key(n));
		if(!item.has_read()){ // node not in the node set, do not add it!
			return true;
		}
		PRINT_DEBUG("node %p: AVN before insert:%lu, AVN after insert:%lu, AVN current in node set:%lu\n",
					n, before_vers, after_vers, item.template read_value<uint64_t>())
		//TODO: check this!
        //if(before_vers == item.template read_value<uint64_t>()){
			item.update_read(before_vers, after_vers);
			return true;
		//}
		//return false;
	}

    
	static uintptr_t get_nodeset_key(N* node) {
		return reinterpret_cast<uintptr_t>(node) | nodeset_bit;
	}

	N* get_node(uintptr_t k){
		return reinterpret_cast<N*>(k & ~nodeset_bit) ;
    }

	bool is_in_nodeset(TransItem& item){
		return (item.key<uintptr_t>() & nodeset_bit) != 0;
	}

	static bool has_insert(const TransItem& item){
		return item.flags() & insert_bit;
	}

	static bool has_delete(const TransItem& item){
		return item.flags() & delete_bit;
	}

	/* STO callbacks
     * -------------
     */
	bool lock(TransItem& item, Transaction& txn){
		PRINT_DEBUG("Lock\n")
		assert(!is_in_nodeset(item));
        record* rec = item.key<record*>();
		auto res = txn.try_lock(item, rec->version);
        return res;
    }

	bool check(TransItem& item, Transaction& ){
        INIT_COUNTING
        PRINT_DEBUG("Check\n")
        START_COUNTING
		bool okay = false;
        //printf("Is in node set? %u\n", is_in_nodeset(item));
        if(is_in_nodeset(item)){
			N* node = get_node(item.key<uintptr_t>());
			auto live_vers = node->getVersion();
			auto titem_vers = item.read_value<decltype(node->getVersion())>();
            if(live_vers != titem_vers){
                PRINT_DEBUG("Node set check: node %p version: %lu, TItem version: %lu\n", node, live_vers, titem_vers)
                PRINT_DEBUG_VALIDATION("VALIDATION FAILED: NODESET\n");
            }
            return live_vers == titem_vers;
		}
        #if VALIDATE
        if(bloom_type != nobloom) {
            if(is_in_bloomset(item)){
                uint64_t* hash = get_bloomset_hash_val(item.key<uintptr_t>());
                bool contains = false;
                switch(bloom_type){
                    case packing:
                        contains = bloomPacking->bloom_contains_hash(hash);
                        break;
                    case nopacking:
                        contains = bloomNoPacking->bloom_contains_hash(hash);
                        break;
                    default:
                        fprintf(stderr, "Unkown bloom filter type!\n");
                        return false;
                };
                if(contains){
                    PRINT_DEBUG_VALIDATION("VALIDATION FAILED: BLOOMSET\n");
                    return false;
                }
                return true;
            }
        }
        #endif
        record* rec = item.key<record*>();
        okay = item.check_version(rec->version);
        if(!okay)
            PRINT_DEBUG_VALIDATION("VALIDATION FAILED: STO VERSION MISMATCH\n");
        return okay;
    }
	void install(TransItem& item, Transaction& txn){
		PRINT_DEBUG("Install\n")
		assert(!is_in_nodeset(item));
		record* rec = item.key<record*>();
		PRINT_DEBUG("Key: %s\n", keyToStr(rec->key).c_str())
		if(has_delete(item)){
			PRINT_DEBUG("Has delete bit set!\n");
			if(!has_insert(item)){
				assert(rec->valid() && ! rec->deleted);
				txn.set_version(rec->version);
				rec->deleted = true;
				fence();
			}
			return;
		}
		if(!has_insert(item)){ // update : not supported yet
            PRINT_DEBUG("Will update\n")
            auto val = item.write_value<uint64_t>();
            rec->val = val;
		}
		// clear user bits: Make record valid!
		txn.set_version_unlock(rec->version, item);
	}

	void unlock(TransItem& item){
    	PRINT_DEBUG("Unlock\n")
		assert(!is_in_nodeset(item));
		record* rec = item.key<record*>();
		rec->version.unlock();
	}

	// bool committed
	void cleanup(TransItem& item, bool committed){
		PRINT_DEBUG("Cleanup\n")
		assert(!is_in_nodeset(item));
		record* rec = item.key<record*>();
		Key k;
		TID tid = reinterpret_cast<TID>(rec);
		loadKey(tid, k);
		ThreadInfo epocheInfo = getThreadInfo();
		if(committed? has_delete(item) : has_insert(item)){
			// TODO: might need to check the result of remove (if not found)!Even though we check it earlier in t_remove, it might have been removed later
            remove(k, tid, epocheInfo);
			Transaction::rcu_delete(rec);
		}
		item.clear_needs_unlock();
        #if VALIDATE
        if(bloom_type != nobloom && is_in_bloomset(item)){
            uint64_t* hashVal = get_bloomset_hash_val(item.key<uintptr_t>());
            delete hashVal;
        }
        #endif
    }
};
