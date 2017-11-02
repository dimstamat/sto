#pragma once
#include "Interface.hh"
#include "TWrapped.hh"
// layout tree includes
#include "tree.hpp"
#include "GlobalLockTree.hh"


/* 
 *  A transactional version of the cohen DLtree running on top of STO
 *  Initialization will be non-transactional on tree construction time
 *  ------------------------------------------------------------------
 *  Author: Dimokritos Stamatakis
 *  Nov 1 2017
 */

template<typename T, typename W = TWrapped<T> >
class TLayoutBT: public TObject {
public:
	/* ---------------------
 	 * layout tree members
 	 * ----------------------
    */
	extern globalLockTree NULL_TREE;
	enum NODE_TYPES {NORMAL_NODE=0XDE, DATA_NODE_T=1};
	struct cacheKeys{
        union{
            unsigned type;
            unsigned dummyKeys[];//keys starts at 1.
        };
        unsigned keys[15];
    } __attribute__ ((aligned (64)));
    struct node{
        cacheKeys keys;
        struct node *next[16];
        node(){memset(this, 0, sizeof(struct node)); keys.type=NORMAL_NODE;}
    }__attribute__ ((aligned(64)));
    struct datanode{
        unsigned type;
        unsigned key;
        void *data;
        datanode(unsigned key, void *data){
            this->key=key; this->data=data; this->type=DATA_NODE_T;
        }
    };
    node *head;


	// initialize the layout tree by constructing the backbone
	TLayoutBT(){
	
	}

	/* ---------------------
 	 * layout tree functions
 	 * ----------------------
	 */
	// build the backbone from start
    node *build(int level, long first){
        if(level==0){
            std::vector<unsigned> v(1);
            v[0]=first;
            return (node*)new GlobalLockTree(v.begin(),v.end());
        }
        int delta = 1<<(level-0);
        node *n = new node();
        for(int l=0; l<4; ++l, delta/=2){
            for(int i=0; i<(1<<l); ++i){
                n->keys.keys[(1<<l)-1+i] = first+(i*2+1)*delta;
            }
        }
        assert((level%4) == 0 && level>0);
        if(level>4)
        {
            for(int i=0; i<16; ++i)
                n->next[i]=build(level-4, first+(2*i)*delta);
        }
        else{
            for(int i=0; i<16; ++i){
                n->next[i]=build(0, first+2*(i+1)*delta);
            }
        }
        return n;
    }



	bool lock(TransItem& item, Transaction& txn){
		return false;
	}

	bool check(TransItem& item, Transaction& txn){
		return false;
	}

    void install(TransItem& item, Transaction& txn){
		
	}

	void unlock(TransItem& item){
	
	}
}
