#pragma once
#include "Interface.hh"
#include "TWrapped.hh"

#include "OptimisticLockCoupling/Tree.h"
#include "Key.h"

/* 
 *    A transactional version of ART running on top of STO
 *    ------------------------------------------------------------------
 *    Author: Dimokritos Stamatakis
 *    April 9 2018
 */


using namespace std;
#include <iostream>

#define DEBUG 0
#if DEBUG == 1
    #define PRINT_DEBUG(...) printf(__VA_ARGS__);
#else
    #define PRINT_DEBUG(...)  
#endif

using namespace ART_OLC;

template <typename T, typename W = TWrapped<T>>
class TART : public Tree, TObject {

	typedef typename W::version_type version_type;

	static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
	static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
	static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1;

	static constexpr uintptr_t nodeset_bit = 1;
	
	using ins_res = std::tuple<bool, bool>;
	using rem_res = std::tuple<bool, bool>;
	using lookup_res = std::tuple<TID, bool>;

public:
	TART(LoadKeyFunction loadKeyFun) : Tree(loadKeyFun) { }

	typedef struct record {
		// TODO: We might not need to store key here!
		// ART itself does not store actual keys, client is responsible for
		// TID to key mapping. We can find a key by calling loadKey(tid, key); (supplied key is empty and initialized by loadKey)
		Key key;
		const TID val;
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

	lookup_res t_lookup(const Key& k, ThreadInfo& threadEpocheInfo){
		PRINT_DEBUG("Lookup key %s\n", keyToStr(k).c_str())
		trans_info* t_info = new trans_info();
		memset(t_info, 0, sizeof(trans_info));
		TID tid = lookup(k, threadEpocheInfo, t_info);
		if (tid == 0){ // not found. Add it in the node set!
			PRINT_DEBUG("Not found!\n")
			ns_add_node(t_info->cur_node, t_info->cur_node_vers);
			return lookup_res(0, true);
		}
		record* rec = reinterpret_cast<record*>(tid);
		// add to read set
		auto item = Sto::item(this, rec);
		if(has_delete(item)){ // current transaction already marked for deletion, reply as it is absent!
			return lookup_res(0, true);
		}
		item.observe(rec->version);
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
		uint8_t key_ind = t_info->key_ind;
		// create a poisoned record (invalid bit set)
		record* rec = new record(k, tid, false);
		PRINT_DEBUG("Creating new record %p with key %s\n", rec, keyToStr(k).c_str())
		auto item = Sto::item(this, rec);
		// add this record in the appropriate node
		switch(n->getType()){
			case NTypes::N4:
				(static_cast<N4*>(n))->insert(key_ind, N::setLeaf(reinterpret_cast<TID>(rec)));
				break;
			case NTypes::N16:
				(static_cast<N16*>(n))->insert(key_ind, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
			case NTypes::N48:
                (static_cast<N48*>(n))->insert(key_ind, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
			case NTypes::N256:
                (static_cast<N256*>(n))->insert(key_ind, N::setLeaf(reinterpret_cast<TID>(rec)));
                 break;
		}
		// update AVN in node set, if exists
		if(! ns_update_node_AVN(n, t_info->cur_node_vers, n->getVersion())) {
			goto abort;
		}
		item.add_write();
		item.add_flags(insert_bit);
		PRINT_DEBUG("-- Unlocking node %p\n", l_n);
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
		if(lookup_tid == 0){ // not found, add to node set!
			ns_add_node(t_info->cur_node, t_info->cur_node_vers);
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
		if(before_vers == item.template read_value<uint64_t>()){
			item.update_read(before_vers, after_vers);
			return true;
		}
		return false;
	}

	static uintptr_t get_nodeset_key(N* node) {
		return reinterpret_cast<uintptr_t>(node) | nodeset_bit;
	}

	N* get_node(uintptr_t k){
		return reinterpret_cast<N*>(k & ~nodeset_bit);
	}

	bool is_in_nodeset(TransItem& item){
		return item.key<uintptr_t>() & nodeset_bit;
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
		return txn.try_lock(item, rec->version);
    }

	bool check(TransItem& item, Transaction&){
        PRINT_DEBUG("Check\n")
		if(is_in_nodeset(item)){
			N* node = get_node(item.key<uintptr_t>());
			auto live_vers = node->getVersion();
			auto titem_vers = item.read_value<decltype(node->getVersion())>();
			PRINT_DEBUG("Node set check: node %p version: %ld, TItem version: %ld\n", node, live_vers, titem_vers)
			return live_vers == titem_vers;
		}
		record* rec = item.key<record*>();
		return item.check_version(rec->version);
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
    }
};
