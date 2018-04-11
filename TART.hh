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


using namespace ART_OLC;

template <typename T, typename W = TWrapped<T>>
class TART : public Tree, TObject {

	typedef typename W::version_type version_type;

	static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
	static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
	static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1;

	static constexpr uintptr_t nodeset_bit = 1;

	typedef struct record {
		Key key;
		TID val;
		version_type version;
		bool deleted;

		record(const Key& k, const TID v, bool valid): key(k), val(v),
				version(valid? Sto::initialized_tid(): Sto::initialized_tid() | invalid_bit, !valid), deleted(false) {}

		bool valid() const {
			return !(version.value() & invalid_bit);
		}

	}record;

	void t_insert(const Key & k, TID tid, ThreadInfo &epocheInfo){
		trans_info* t_info = new trans_info();
		insert(k, tid, epocheInfo, t_info); 
		N* n = t_info->ins_node;
		N* l_n = t_info->l_node;
		N* l_p_n = t_info->l_parent_node;
		uint8_t key_ind = t_info->key_ind;
		// create a poisoned record (invalid bit set)
		record* rec = new record(k, tid, false);
		auto item = Sto::item(this, rec);
		// add this record in the appropriate node
		switch(n->getType()){
			case NTypes::N4:
				(static_cast<N4*>(n))->insert(0, N::setLeaf(static_cast<TID>(rec)));
				break;
			case NTypes::N16:
				(static_cast<N16*>(n))->insert(0, N::setLeaf(static_cast<TID>(rec)));
                 break;
			case NTypes::N48:
                (static_cast<N48*>(n))->insert(0, N::setLeaf(static_cast<TID>(rec)));
                 break;
			case NTypes::N256:
                (static_cast<N256*>(n))->insert(0, N::setLeaf(static_cast<TID>(rec)));
                 break;
		}
		// add to node set
		if(! update_AVN(n, t_info->ins_node_vers, n->getVersion())) {
			//abort
		}
		item.add_write();
		item.add_flags(insert_bit);
		l_n->writeUnlock();
		if(l_p_n)
			l_p_n->writeUnlock();
	}

	// Updates the AVN of a node in the node set
	bool update_AVN(N* n, uint64_t before_vers, uint64_t after_vers){
		auto item = Sto::item(this,	get_nodeset_key(n));
		if(!item.has_read()){
			item.add_read(after_vers);
			return true;
		}
		if(before_vers == item.template read_value<uint64_t>){
			item.update_read(before_vers, after_vers);
			return true;
		}
		return false;
	}

	static uintptr_t get_nodeset_key(N* node) {
		return reinterpret_cast<uintptr_t>(node) | nodeset_bit;
	}


	/* STO callbacks
     * -------------
     */
	bool lock(TransItem&, Transaction&){
        return true;
    }

	bool check(TransItem&, Transaction&){
        return true;
    }

	void install(TransItem& item, Transaction& txn){
	}

	void unlock(TransItem&){
    }

	void cleanup(TransItem& item, bool committed){

    }

};
