#pragma once
#include "Interface.hh"
#include "TWrapped.hh"
#include "layoutLock/LayoutTree.hh"

/* 
 *    A transactional version of the cohen DLtree running on top of STO
 *    Initialization will be non-transactional on tree construction time
 *    ------------------------------------------------------------------
 *    Author: Dimokritos Stamatakis
 *    Nov 1 2017
 */


using namespace std;

// tracking set will be <GlobalLockTree*, treelet_log*>

// This version uses a pessimistick lock for each treelet.
// The optimistic version will either:
// a: 	record all treelet modifications in the tracking
// 		set, just like the STO protocol, or
// b:	apply the modifications right away, since there is a
// 		lock held for each treelet
template<typename T, typename W = TWrapped<T> >
class TLayoutBT: public LayoutTree, public TObject {
	std::map<T, bool> treelet_log;
	
	// When we have transactions, a treelet lock will not be released
	// until the transaction commits. Thus, we must maintain pointers to
	// all treelets used for this transaction. This means that
	// getTreelet will first look in the data structure holding the treelet pointers
	// before searching in the backbone.
    GlobalLockTree* getTreelet(T key, dptrtype *dirtyP){
		GlobalLockTree * t;
        SYNC(start: llock_.startRead();)
        node *cur = head;
        while(cur->keys.type==NORMAL_NODE){
            unsigned idx = asmsearch(key, (unsigned *)cur);
            cur = cur->next[idx-16];
            //assert(cur!=NULL);
        }
        t = (GlobalLockTree*)cur;
		auto item = Sto::item(this, t);
		// only acquire the lock if tracking set is empty!
		if (! item.has_write() && ! item.has_read()){
			t->acquire();
		}
#ifndef NOSYNC
        if(llock_.finishRead(dirtyP)==false){
            t->release();
            goto start;
        }
#endif
        return t;
    }


	public:

	TLayoutBT(){}

	bool insert(T key, dptrtype *dirtyP){
      CHCK(int n = __atomic_fetch_add(&next, 1, __ATOMIC_SEQ_CST);\
      buffer[n] = key;)
        bool insres;
		GlobalLockTree * t;
		t = getTreelet(key, dirtyP);
		std::map<T, bool> * treelet_log;
		auto item = Sto::item(this, t);
		if (item.has_write()){
			treelet_log = item.template write_value<std::map<T,bool>* >();
		}
		else {
        	treelet_log = new std::map<T, bool>();
			item.add_write(*treelet_log);
		}
		(*treelet_log)[key] = true;
        // will do the actual insert in install phase!
		//insres = t->insert(key);
        insres = true;
		if(unlikely((heuristic[stateOff_]+=insres)>=200))
        {
            int res = __sync_add_and_fetch(&fuzzySize, heuristic[stateOff_]);
            //printf("fuzzySize=%d, th=%d\n", tid_, res);
			int lenlargeWhen;
            SYNC(lb: llock_.startRead();) lenlargeWhen=enlargeWhen; SYNC(if(llock_.finishRead()==false) goto lb;)
            if(res >= lenlargeWhen) enlarge_tree(res);
            heuristic[stateOff_]=0;
        }
		return insres;
	}

	bool remove(T key, dptrtype *dirtyP){
        bool res;//, shrink=false;
		GlobalLockTree * t;
        t = getTreelet(key, dirtyP);
		std::map<T, bool> * treelet_log;
        auto item = Sto::item(this, t);
        if (item.has_write()){
			treelet_log = item.template write_value<std::map<T,bool> *>();
        }
		else {
			treelet_log = new std::map<T, bool>();
            item.add_write(*treelet_log);
		}
        (*treelet_log)[key] = false;
		//item.add_flags(TransItem::user0_bit); // specify it is a remove operation : No need now! We specify it at the boolean value of the treelet_log map
		res = true;
		//res = t->remove(key);
        if(unlikely((heuristic[stateOff_]-=res)<-200))
      	{
         	int res = __sync_add_and_fetch(&fuzzySize, heuristic[stateOff_]);
            int lshrinkWhen;
         	SYNC(lb: llock_.startRead();) lshrinkWhen=shrinkWhen; SYNC(if(llock_.finishRead()==false) goto lb;)
         	if(res <= lshrinkWhen) shrink_tree(res);
		 	//printf("REMOVE: fuzzySize=%d, th=%d\n", res, tid_);
         	heuristic[stateOff_]=0;
      	}
        return res;
    }


	/* STO callbacks
 	 * -------------
 	 */
	// pessimistic approach locks every treelet before accessing it,
	// thus we don't need to lock at commit time
    bool lock(TransItem&, Transaction&){
        return true;
    }
	// there is no tracking set check required
    bool check(TransItem&, Transaction&){
        return true;
    }
	// modifications will be applied now
    void install(TransItem& item, Transaction& txn){
		GlobalLockTree* t = item.key<GlobalLockTree*>();
		bool res;
		std::map<T, bool>* treelet_log = item.template write_value<std::map<T, bool>*>();
		for (const auto& log_entry: *treelet_log){
			if(log_entry.second){
				t->insert(log_entry.first);
			}
			else{
				t->remove(log_entry.first);
			}
		}
		// if (!res)
		// txn->abort();
	}

    void unlock(TransItem&){

    }

};
