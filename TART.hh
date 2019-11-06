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


using namespace std;
#include <iostream>

#define DEBUG 0
#define DEBUG_VALIDATION 0
#define MEASURE_ABORTS 0
#define ABSENT_VALIDATION 1 // 1 for node set, 2 for node set with absent keys, 3 for absent keys and lookup starting from target node, 4 for key set

#if DEBUG == 1
    #define PRINT_DEBUG(...) {printf(__VA_ARGS__);}
#else
    #define PRINT_DEBUG(...)  {}
#endif

#if DEBUG_VALIDATION == 1
    #define PRINT_DEBUG_VALIDATION(...) {printf(__VA_ARGS__);}
#else
    #define PRINT_DEBUG_VALIDATION(...) {}
#endif

using namespace ART_OLC;


using ins_res = std::tuple<bool, bool>;
using rem_res = std::tuple<bool, bool>;
using lookup_res = std::tuple<TID, bool>;

static constexpr uintptr_t dont_cast_from_rec_bit = 1LU << 60;

#if MEASURE_ART_NODE_ACCESSES == 1
static unsigned accessed_nodes_sum=0;
static unsigned accessed_nodes_num=0;
#endif


#if MEASURE_ABORTS == 1
static const unsigned aborts_sz = 10;
uint64_t aborts[N_THREADS][aborts_sz];
static string aborts_descr[aborts_sz];
#define INCR(arg) arg+=1;
#else
    #define INCR(arg) {}
#endif

#if ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
// That's the list of absent keys for a particular node. It will be stored in the value of the TItem for a node
typedef struct absent_keys {
    Key * k;
    struct absent_keys* next;
}absent_keys_t;
#endif

// forward declared, definition in util/bloom.hh
class DoubleLookup;

template <typename T, typename BloomT, typename W = TWrapped<T>>
class TART : public Tree, TObject {

	typedef typename W::version_type version_type;

	static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
	static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
	static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1;

	static constexpr uintptr_t nodeset_bit = 1LU << 63;
    static constexpr uintptr_t bloom_validation_bit = 1LU << 62;
    static constexpr uintptr_t keyset_bit = 1LU <<61;

    bool compacted=false;

    BloomT & bloom;

    inline bool is_using_bloom(){
        return ! std::is_same<BloomT, DoubleLookup>::value;
    }

public:

    TART(LoadKeyFunction loadKeyFun, BloomT& b) : TART(loadKeyFun, b, false) {
    }

	TART(LoadKeyFunction loadKeyFun, BloomT& b, bool comp) : Tree(loadKeyFun), compacted(comp), bloom(b) {
        #if MEASURE_ABORTS == 1
            bzero(aborts, N_THREADS * aborts_sz * sizeof(uint64_t));
            aborts_descr[0] = "nodeset validation failure";
            aborts_descr[1] = "bloomset validation failure";
            aborts_descr[2] = "STO version mismatch";
            aborts_descr[3] = "failure in lock";
            aborts_descr[4] = "record is poisoned";
            aborts_descr[5] = "update AVN failure - update key";
            aborts_descr[6] = "update AVN failure - insert key, failure while updating node 1";
            aborts_descr[7] = "update AVN failure - insert key, failure while updating node 2";
            aborts_descr[8] = "abort exception handled (hard opacity check, etc.)";
            aborts_descr[9] = "key inserted concurrently";
        #endif
    }

	typedef struct record {
		// DONE: We might not need to store key here!
		// ART itself does not store actual keys, client is responsible for
		// TID to key mapping. We can find a key by calling loadKey(tid, key); (supplied key is empty and initialized by loadKey)
		//Key key;
		TID val;
		version_type version;
		bool deleted;

		record(const TID v, bool valid):val(v),
		// Old STO Does not take a bool argument in version constructor
		//		version(valid? Sto::initialized_tid(): Sto::initialized_tid() | invalid_bit, !valid), deleted(false) {}
		version(valid? Sto::initialized_tid(): Sto::initialized_tid() | invalid_bit), deleted(false) {
			/*char * key_dat = new char[k.getKeyLen()];
			for(uint32_t i=0; i<k.getKeyLen(); i++){
				key_dat[i] = (char) k[i];
			}
			key.set(key_dat, k.getKeyLen());
            delete key_dat;*/
		}

		bool valid() const {
			return !(version.value() & invalid_bit);
		}

	}record;

    // extract the actual TID from the record*
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
        #if MEASURE_ART_NODE_ACCESSES == 1
        if(validate){
            accessed_nodes_sum+=t_info->accessed_nodes;
            accessed_nodes_num++;
        }
        #endif
		if(t_info->check_key){ // call the TART check Key! (casting from rec*)
			tid = checkKeyFromRec(tid, k);
		}
		if (tid == 0){ // not found. Add parent in the nodeset, or key in keyset
            PRINT_DEBUG("Not found!\n")
            if(validate){ // only add parent in the nodeset if we want to validate (TART RW, not TART compacted)
                #if ABSENT_VALIDATION == 1
                ns_add_node(std::get<0>(t_info->updated_node1), std::get<1>(t_info->updated_node1));
                //stringstream ss;
                //ss<<TThread::id()<<": Key not found, adding node "<< std::get<0>(t_info->updated_node1) << ", vers "<< std::get<1>(t_info->updated_node1) <<" to node set\n";
                //cout<<ss.str()<<std::flush;
                #elif ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
                ns_add_node(t_info->cur_node, k);
                #elif ABSENT_VALIDATION == 4
                ks_add_key(k);
                #endif
            }
            delete t_info;
			return lookup_res(0, true);
		}
		record* rec = reinterpret_cast<record*>(tid);
        delete t_info;
        if(validate) {
            auto item = Sto::item(this, rec);
            if(!rec->valid() && !has_insert(item)){
                INCR(aborts[TThread::id()][4])
                goto abort;
            }
			if(has_delete(item)){ // current transaction already marked for deletion, reply as it is absent!
				return lookup_res(0, true);
			}
			// add to read set
			item.observe(rec->version);
            //item.add_read(rec->version);
        }
		return lookup_res(rec->val, true);
		abort:
			return lookup_res(0, false);
	}

    
    // ins_res is <inserted, ok-to-commit>, where inserted is true when the new key caused an insertion and false when it was an udpate. ok-to-commit is false when the transaction must abort at run-time.
	ins_res t_insert(const Key & k, TID tid, ThreadInfo &epocheInfo){
        trans_info* t_info = new trans_info();
		memset(t_info, 0, sizeof(trans_info));
		//stringstream ss;
        //ss<<"Size: "<< sizeof(trans_info)<<endl;
        //cout<<ss.str();
        PRINT_DEBUG("Transactionally Inserting (key:%s, tid:%lu)\n", keyToStr(k).c_str(), tid)
		insert(k, tid, epocheInfo, t_info); 
		N* n = t_info->cur_node;
		N* l_n = t_info->l_node;
		N* l_p_n = t_info->l_parent_node;
        N* updated_nodes [2] ;
        bzero(updated_nodes, 2* sizeof(N*));
        updated_nodes[0] = std::get<0>(t_info->updated_node1);
        updated_nodes[1] = std::get<0>(t_info->updated_node2);
		uint8_t keyslice = t_info->keyslice;

        #if ABSENT_VALIDATION == 1
        uint64_t updated_nodes_v [2];
        updated_nodes_v[0] = std::get<1>(t_info->updated_node1);
        updated_nodes_v[1] = std::get<1>(t_info->updated_node2);
        #endif	
	
        if(t_info->updatedVal > 0){ // it is an update
            //stringstream ss;
            //ss<<"Update\n";
            //cout<<ss.str();
            record* rec = reinterpret_cast<record*>(t_info->prevVal);
            auto item = Sto::item(this, rec);
            if(!rec->valid() && !has_insert(item)){
                INCR(aborts[TThread::id()][4])
                delete t_info;
                return ins_res(false, false);
            }
            // UPDATE: We do not need to update AVN in node set as it was an update and thus AVN didn't change!
            // update AVN in node set, if exists
            // Use the version number after the unlock! (+2)
            /*#if ABSENT_VALIDATION == 1
            if(! ns_update_node_AVN(updated_nodes[0], updated_nodes_v[0], updated_nodes[0]->getVersion()+2)) {
                PRINT_DEBUG("UPDATE NODE FAIL!\n")
                INCR(aborts[TThread::id()][5])
                if(t_info->w_unlock_obsolete)
                    l_n->writeUnlockObsolete();
                else
                    l_n->writeUnlock();
                delete t_info;
                return ins_res(false, false);
            }
            #endif
            */
            item.add_write(t_info->updatedVal);
            // TODO: In some runs l_n was null! Check it!
            delete t_info;
            return ins_res(false, true);
        }

        // create a poisoned record (invalid bit set) and store the client provided tid
        // the actual tid of the ART node will be the record* (casted to TID)
		record* rec = new record(tid, false);
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
        // add to write set and mark as inserted BEFORE the absent validation! It might be that the absent validation
        // fails due to the parent node that changed version by a concurrent transaction!!
		item.add_write();
        item.add_flags(insert_bit);
        #if ABSENT_VALIDATION == 1
        // update AVN in node set, if exists
		// Include the +2 version number increment that happens at unlock!!
		// We cannot update the AVN after unlocking, because a concurrent transaction could alter the version number
		// and we will not detect it!
		// also check whether node is migrated! Do not update its AVN if it is!
		if(!l_n->isMigrated() && (! ns_update_node_AVN(updated_nodes[0], updated_nodes_v[0], updated_nodes[0]->getVersion()+2))) {
			if(t_info->w_unlock_obsolete)
                l_n->writeUnlockObsolete();
            else
                l_n->writeUnlock();
            INCR(aborts[TThread::id()][6])
            PRINT_DEBUG("UPDATE NODE 1 FAIL!\n")
            //cout<<"UPDATE NODE 1 FAIL!\n";
			if(l_p_n){
		  		l_p_n->writeUnlock();
		  	}
			goto abort;
		}
        if(updated_nodes[1] != nullptr){
            if(! ns_update_node_AVN(updated_nodes[1], updated_nodes_v[1], updated_nodes[1]->getVersion()+2)) {
                if(t_info->w_unlock_obsolete)
                    l_n->writeUnlockObsolete();
                else
                    l_n->writeUnlock();
                INCR(aborts[TThread::id()][7])
                PRINT_DEBUG("UPDATE NODE 2 FAIL!\n")
                //cout<<"UPDATE NODE 2 FAIL!\n";
                if(l_p_n){
                    l_p_n->writeUnlock();
                }
                goto abort;
            }
        }
        #elif ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        // we must remove that newly inserted key from the current node in the node set, if exists
        auto nodeset_item = Sto::item(this, get_nodeset_key(n)); //n is t_info->cur_node
        if(nodeset_item.has_read()){ // remove the newly inserted key from the absent key list!
            //cout <<"Inserting previously absent key\n";
            absent_keys_t* keys_list_cur = nodeset_item.template read_value<absent_keys_t*>();
            absent_keys_t* keys_list_prev = keys_list_cur;
            while(keys_list_cur != nullptr) {
                if(keys_list_cur->k == nullptr)
                    break;
                if(*keys_list_cur->k == k){ // remove that key
                    if(keys_list_prev == keys_list_cur){ // found in head of the list
                        delete keys_list_cur->k;
                        nodeset_item.update_read(nodeset_item.template read_value<absent_keys_t*>(), keys_list_cur->next); // change head of the list as the next element
                        delete keys_list_cur;
                        break;
                    }
                    else {
                        keys_list_prev->next = keys_list_cur->next;
                        delete keys_list_cur->k;
                        delete keys_list_cur;
                        break;
                    }
                }
                keys_list_prev = keys_list_cur;
                keys_list_cur = keys_list_cur->next;
            }
        }
        #endif
		PRINT_DEBUG("-- Unlocking node %p\n", l_n);
		if(t_info->w_unlock_obsolete)
            l_n->writeUnlockObsolete();
        else
            l_n->writeUnlock();
		if(l_p_n){
			PRINT_DEBUG("-- Unlocking parent node %p\n", l_p_n);
			l_p_n->writeUnlock();
		}
		if(l_n == n){
			PRINT_DEBUG("We are unlocking the node we just inserted to!! (%p)\n", n)
        }
		PRINT_DEBUG(" ==== Now node's %p AVN:%lu\n", n, n->getVersion())
		
        //item_tmp = item.item();
		//rec_tmp = item_tmp.key<record*>();
		//PRINT_DEBUG("Inserted key %s\n", keyToStr(rec_tmp->key).c_str())
        #if MEASURE_TREE_SIZE == 1
        if(t_info->addedSize > 0)
            tree_sz[TThread::id()] += t_info->addedSize;
        //cout<<"Adding "<<t_info->addedSize<<endl;
        #endif
        delete t_info;
        return ins_res(true, true);
		abort:
            delete t_info;
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
			#if ABSENT_VALIDATION == 1
            ns_add_node(std::get<0>(t_info->updated_node1), std::get<1>(t_info->updated_node1));
            #elif ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
            ns_add_node(t_info->cur_node, k);
            #elif ABSENT_VALIDATION == 4
            ks_add_key(k);
            #endif
            delete t_info;
			return rem_res(false, true);
		}
        delete t_info;
		record* rec = reinterpret_cast<record*>(lookup_tid);
		if(rec->val != tid){ // that's the behavior of ART insert: If the encountered tuple id is different than the supplied one, return
			tid_mismatch = true;
		}
		if(rec->deleted) { // abort
			 return rem_res(false, false);
		}
		if(tid_mismatch){
            //stringstream ss;
            //ss<<"TID mismatch in remove... Found TID: "<<rec->val<<", requested TID: "<<tid<<endl;
            //cout<<ss.str()<<std::flush;
            PRINT_DEBUG("Oops, tid mismatch!\n")
            return rem_res(false, true);
        }
        auto item = Sto::item(this, rec);
		item.observe(rec->version);
		//item.add_read(rec->version);
		item.add_write();
		fence();
		item.add_flags(delete_bit);
		return rem_res(true, true);
	}

    #if BLOOM_VALIDATE == 1
    // for BLOOM_VALIDATE 1
    // add in a separate data structure! Performance is bad when we create a Sto::item per absent bloom filter element
    void bloom_v_add_hash_key(uint64_t* hashVal){
        /*if (( reinterpret_cast<uintptr_t>(hashVal) & bloom_validation_bit ) != 0){
            cout<<"Oops, 62nd bit is set!\n";
            cout<<"Oops, hashVal is "<<hashVal<<endl;
        }
        if (( reinterpret_cast<uintptr_t>(hashVal) & nodeset_bit) != 0)
            cout<<"Oops, 63rd bit is set!\n";*/
        auto item = Sto::item(this, get_bloomset_hash_key(hashVal));
        if(!item.has_read()){
            item.add_read(0);
        }
    }
    #elif BLOOM_VALIDATE == 2
    // for BLOOM_VALIDATE 2
    void bloom_v_add_key(TID tid, uint64_t* hashVal){
        INIT_COUNTING_BLOOM
        auto item = Sto::item(this, get_bloomset_key(tid));
        if(!item.has_read()){
            item.add_read(0);
            START_COUNTING_BLOOM
            memcpy(item.item().hashValue, hashVal, 2 * sizeof(uint64_t));
            STOP_COUNTING_BLOOM("memcpy in TItem")
        }
    }
    #endif
    private:
    #if BLOOM_VALIDATE == 1
    // for BLOOM_VALIDATE 1
    uintptr_t get_bloomset_hash_key(uint64_t* hashVal){
        return reinterpret_cast<uintptr_t>(hashVal) | bloom_validation_bit;
    }
    uint64_t* get_bloomset_hash_val(uintptr_t b){
        return reinterpret_cast<uint64_t*>(b & ~bloom_validation_bit);
    }
    #elif BLOOM_VALIDATE == 2
    // for BLOOM_VALIDATE 2
    uintptr_t get_bloomset_key(TID tid){
        return reinterpret_cast<uintptr_t>(tid) | bloom_validation_bit;
    }
    TID get_bloomset_tid(uintptr_t t){
        return reinterpret_cast<TID>(t & ~bloom_validation_bit);
    }
    bool is_in_bloomset(TransItem& item){
        return (item.key<uintptr_t>() & bloom_validation_bit) != 0;
    }
    #endif

    // For ABSENT_VALIDATION 1
	#if ABSENT_VALIDATION == 1
    // Adds a node and its AVN in the node set
	void ns_add_node(N* node, uint64_t vers){
        auto item = Sto::item(this, get_nodeset_key(node));
        if(!item.has_read()){
            PRINT_DEBUG("Adding node %p to node set with version %lu\n", node, vers)
            item.add_read(vers);
        }
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
        if(before_vers == item.template read_value<uint64_t>()){
			item.update_read(before_vers, after_vers);
			return true;
		}
        //stringstream ss;
        //ss<<TThread::id()<<": Node "<< n <<" changed version. When read: "<< before_vers << ", in node set: "<<item.template read_value<uint64_t>() << ", current version: "<< after_vers <<endl;
        //cout<<ss.str()<<std::flush;
        // check what happens if we don't abort now, but leave it for commit time
		return false;
	}
    #endif
   
    // For ABSENT_VALIDATION 2, 3
    #if ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
    bool ns_add_node(N* node, const Key & key){
        auto item = Sto::item(this, get_nodeset_key(node));
        //stringstream ss;
        //ss<<TThread::id()<< ": Adding absent key "<< keyToStr(key) <<endl;
        //cout<<ss.str();
        if(!item.has_read()){ // create new absent keys list
            absent_keys_t* keys_list = new absent_keys_t;
            bzero(keys_list, sizeof(absent_keys_t));
            keys_list->k = copy_key(key);
            //cout<<"Add read!\n";
            item.add_read(keys_list);
        }
        else { // there is already an entry in the node set
            //cout<<"Has read\n";
            absent_keys_t* keys_list_before = item.item().template read_value<absent_keys_t*>();
            absent_keys_t* keys_list = keys_list_before;
            if(keys_list == nullptr){ // this can be true if we deleted a key due to insert!
                absent_keys_t* keys_list = new absent_keys_t;
                bzero(keys_list, sizeof(absent_keys_t));
                keys_list->k = copy_key(key);
                //cout<<"Update read!\n";
                item.update_read(keys_list_before, keys_list);
            }
            else {
                while(keys_list->next != nullptr)
                    keys_list = keys_list->next;
                absent_keys_t* new_key = new absent_keys_t;
                bzero(new_key, sizeof(absent_keys_t));
                new_key->k = copy_key(key);
                keys_list->next = new_key;
                /*unsigned i=1;
                while(keys_list_before->next != nullptr){
                    keys_list_before = keys_list_before->next;
                    i++;
                }*/
            }
        }
        return true;
    }
    #endif

    static uintptr_t get_nodeset_key(N* node) {
        return reinterpret_cast<uintptr_t>(node) | nodeset_bit;
    }

    bool is_in_nodeset(TransItem& item){
        return (item.key<uintptr_t>() & nodeset_bit) != 0;
    }

    N* get_node(uintptr_t k){
        return reinterpret_cast<N*>(k & ~nodeset_bit) ;
    }


    Key* copy_key(const Key& k){
        Key* key = new Key;
        bzero(key, sizeof(Key));
        key->set((const char*)&k[0], k.getKeyLen());
        return key;
    }

    // For ABSENT_VALIDATION 4
    // Adds a key to the key set
    #if ABSENT_VALIDATION == 4
    void ks_add_key(const Key& k){
        Key* key = copy_key(k);
        auto item = Sto::item(this, get_keyset_key(key));
        if(!item.has_read()){
            item.add_read(0);
        }
    }
    static uintptr_t get_keyset_key(Key* key){
        return reinterpret_cast<uintptr_t>(key) | keyset_bit;
    }

    bool is_in_keyset(TransItem& item){
        return (item.key<uintptr_t>() & keyset_bit) != 0;
    }
    
    Key* get_key(uintptr_t k){
        return reinterpret_cast<Key*>(k & ~keyset_bit);
    }
    #endif

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
		#if ABSENT_VALIDATION == 1 || ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        assert(!is_in_nodeset(item));
        #elif ABSENT_VALIDATION == 4
        assert(!is_in_keyset(item));
        #endif
        record* rec = item.key<record*>();
		auto res = txn.try_lock(item, rec->version);
        if(!res){
            INCR(aborts[TThread::id()][3])
        }
        return res;
    }

    #if ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
    void clear_absent_keys_list(absent_keys_t* keys_list){
        absent_keys_t* keys_list_cur;
        while(keys_list != nullptr){
            keys_list_cur = keys_list;
            keys_list = keys_list->next;
            delete keys_list_cur->k;
            delete keys_list_cur;
        }
    }
    #endif

	bool check(TransItem& item, Transaction& ){
        INIT_COUNTING
        PRINT_DEBUG("Check\n")
        START_COUNTING
		bool okay = false;
        //printf("Is in node set? %u\n", is_in_nodeset(item));
        #if ABSENT_VALIDATION == 1
        if(is_in_nodeset(item)){
			N* node = get_node(item.key<uintptr_t>());
			if(node->isMigrated() || node->isObsolete(node->getVersion())){ // node migrated or became obsolete in the meantime! Abort!
                // new: We need to mark node for deletion, if needed! That's when we grow a node to a bigger one and we need to delete the previous one
                // TODO: might be unsafe to delete at that time! A concurrent transaction could have added this node in the node set and will crash
                // when trying to access it!
                //if(node->isMigrated()){
                //    auto epocheInfo = this->getThreadInfo();
                //    epocheInfo.getEpoche().markNodeForDeletion(node, epocheInfo);
                //}
                return false;
            }
            auto live_vers = node->getVersion();
			auto titem_vers = item.read_value<decltype(node->getVersion())>();
            if(live_vers != titem_vers){
                PRINT_DEBUG("Node set check: node %p version: %lu, TItem version: %lu\n", node, live_vers, titem_vers)
                PRINT_DEBUG_VALIDATION("VALIDATION FAILED: NODESET\n");
                INCR(aborts[TThread::id()][0])
            }
            return live_vers == titem_vers;
        }
        #elif ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        if(is_in_nodeset(item)){
            absent_keys_t* keys_list = item.read_value<absent_keys_t*>();
            unsigned i=0;
            while(keys_list!= nullptr){
                if(keys_list->k != nullptr){
                    trans_info* t_info = new trans_info();
                    memset(t_info, 0, sizeof(trans_info));
                    auto t = this->getThreadInfo();
                    #if ABSENT_VALIDATION == 2
                    TID tid = lookup(*keys_list->k, t, t_info);
                    #elif ABSENT_VALIDATION == 3
                    N* node = get_node(item.key<uintptr_t>());
                    if(node->isMigrated() || node->isObsolete(node->getVersion())) { // node migrated or became obsolete in the meantime! Abort!
                        return false;
                    }
                    TID tid = lookup(*keys_list->k, t, t_info, node);
                    //  when looking up for a key from a startNode in ABSENT_VALIDATION 2 or 3 there is a case that the node
                    //   is obsolete and will always stay obsolete. Abort the transaction and the new attempt will end up in the new node.
                    if(t_info->shouldAbort){
                        delete t_info;
                        return false;
                    }
                    #endif
                    if(t_info->check_key){ // call the TART check Key! (casting from rec*)
                        tid = checkKeyFromRec(tid, *keys_list->k);
                    }
                    if(tid!=0){ // oops, someone else inserted that key! Abort!
                        record* rec = reinterpret_cast<record*>(tid);
                        auto found_item = Sto::item(this, rec);
                        if(!rec->valid() && !has_insert(found_item)){
                            INCR(aborts[TThread::id()][4])
                            delete t_info;
                            return false;
                        }
                        // add to read set
                        //found_item.observe(rec->version);
                        delete t_info;
                        /*stringstream ss;
                        ss<<TThread::id()<<": Abort! key " << keyToStr(*keys_list->k) <<endl;
                        cout<<ss.str();*/
                        clear_absent_keys_list(keys_list);
                        INCR(aborts[TThread::id()][9])
                        return false;
                    }
                }
                i++;
                keys_list = keys_list->next;
            }
            clear_absent_keys_list(keys_list);
            delete t_info;
            return true;
        }
        #elif ABSENT_VALIDATION == 4
        if(is_in_keyset(item)){
            Key *k = get_key(item.key<uintptr_t>());
            trans_info* t_info = new trans_info();
            memset(t_info, 0, sizeof(trans_info));
            auto t = this->getThreadInfo();
            TID tid = lookup(*k, t, t_info);
            if(t_info->check_key){ // call the TART check Key! (casting from rec*)
                tid = checkKeyFromRec(tid, *k);
            }
            delete t_info;
            if(tid !=0){ // oops, previously absent key exists now! Did we add it?
                record* rec = reinterpret_cast<record*>(tid);
                if(rec->valid() ) { // a concurrent transaction added that key! If it was the current transaction, valid 
                                    // bit would be false since check phase is before install.
                    delete k;
                    INCR(aborts[TThread::id()][9])
                    //cout<<"Absent node found!\n";
                    return false;
                }
            }
            delete k;
            return true;
        }
        #endif
        #if BLOOM_VALIDATE > 0
        if(is_using_bloom()){  // it's a compile-time check
            if(is_in_bloomset(item)){
                INIT_COUNTING_BLOOM
                uint64_t* hash;
                #if BLOOM_VALIDATE == 1
                hash = get_bloomset_hash_val(item.key<uintptr_t>());
                #elif BLOOM_VALIDATE == 2
                //TID key = get_bloomset_key(item.key<uintptr_t>());
                START_COUNTING_BLOOM
                hash = item.hashValue;
                STOP_COUNTING_BLOOM("get hashValue from TItem")
                #endif
                //TID tid = get_tid(item.key<uintptr_t>());
                //Key k;
                //uintptr_t tid_flagged = reinterpret_cast<uintptr_t>(tid | dont_cast_from_rec_bit);
                //loadKey(reinterpret_cast<TID>(tid_flagged), k);
                if(bloom.contains_hash(hash)){
                //if(bloom.contains(k.getKey(), k.getKeyLen(), nullptr)){
                    PRINT_DEBUG_VALIDATION("VALIDATION FAILED: BLOOMSET\n");
                    INCR(aborts[TThread::id()][1])
                    return false;
                }
                return true;
            }
        }
        #endif
        record* rec = item.key<record*>();
        //assert(txn.threadid() == TThread::id());
        okay = item.check_version(rec->version);
        STOP_COUNTING_PRINT("validate")
        START_COUNTING
        STOP_COUNTING_PRINT("empty")
        PRINT_DEBUG("Check returns: %u\n", okay)
        if(!okay){
            PRINT_DEBUG_VALIDATION("VALIDATION FAILED: STO VERSION MISMATCH\n");
            INCR(aborts[TThread::id()][2])
        }
        return okay;
    }

	void install(TransItem& item, Transaction& txn){
		PRINT_DEBUG("Install\n")
		#if ABSENT_VALIDATION == 1 || ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        assert(!is_in_nodeset(item));
        #elif ABSENT_VALIDATION == 4
        assert(!is_in_keyset(item));
        #endif
		record* rec = item.key<record*>();
		//PRINT_DEBUG("Key: %s\n", keyToStr(rec->key).c_str())
		if(has_delete(item)){
			PRINT_DEBUG("Has delete bit set!\n");
			if(!has_insert(item)){
                
				if(rec->deleted)
                    return;
                //assert(rec->valid() && ! rec->deleted);
				assert(rec->valid());
                // Dimos: For the case that we call delete in the same element!
                // (Might happen in the test_meme that accesses keys with zipf distribution)
                if(!rec->deleted){
                    txn.set_version(rec->version);
				    rec->deleted = true;
				    fence();
                }
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
		#if ABSENT_VALIDATION == 1 || ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        assert(!is_in_nodeset(item));
        #elif ABSENT_VALIDATION == 4
        assert(!is_in_keyset(item));
        #endif
		record* rec = item.key<record*>();
		rec->version.unlock();
	}

	// bool committed
	void cleanup(TransItem& item, bool committed){
		PRINT_DEBUG("Cleanup\n")
		#if ABSENT_VALIDATION == 1 || ABSENT_VALIDATION == 2 || ABSENT_VALIDATION == 3
        assert(!is_in_nodeset(item));
        #elif ABSENT_VALIDATION == 4
        assert(!is_in_keyset(item));
        #endif
		record* rec = item.key<record*>();
		Key k;
		TID tid = reinterpret_cast<TID>(rec);
		loadKey(tid, k);
		ThreadInfo epocheInfo = getThreadInfo();
		if(committed? has_delete(item) : has_insert(item)){
			// We check the result of remove (if not found)! Even though we check it earlier in t_remove, it might have been removed later. That's by using the 'shouldAbort' flag
            trans_info* t_info = new trans_info();
            bzero(t_info, sizeof(trans_info));
            remove(k, tid, epocheInfo, t_info);
			// Do not call RCU delete when element was actually not deleted (not found). We're ussing the shouldAbort field so that to not include an extra field for 'deleted'
			if(!t_info->shouldAbort)
                Transaction::rcu_delete(rec);
            delete t_info;
        }
		item.clear_needs_unlock();
        #if BLOOM_VALIDATE == 1
        if(is_using_bloom()) { // it's a compile-time check
            if(is_in_bloomset(item)){
                uint64_t* hashVal = get_bloomset_hash_val(item.key<uintptr_t>());
                delete hashVal;
            }
        }
        #endif
    }
};


