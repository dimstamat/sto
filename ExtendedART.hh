#pragma once

#include "TART.hh"
#include "../util/bloom.hh"
#include "OptimisticLockCoupling/Tree.h"


#define MEASURE_BF_FALSE_POSITIVES 1

// needed for the range query when we merge. We store the
// result TIDs in an array and then insert all these keys
// into RO
#define MAX_KEYS 20000000



template <typename T, typename BloomT> class ExtendedART{
protected:
    TART<T, BloomT> tart;
    
    BloomT bloom;


inline bool is_using_bloom(){
    return !std::is_same<BloomT, DoubleLookup>::value;
    //return typeid(bloom) != typeid(DoubleLookup);
}

public:
#if MEASURE_BF_FALSE_POSITIVES
    int BF_false_positives[N_THREADS][2] __attribute__((aligned(128)));
#endif

    ExtendedART(Tree::LoadKeyFunction TARTloadKeyFun): tart(TARTloadKeyFun, bloom)
    {
        #if MEASURE_BF_FALSE_POSITIVES == 1
            bzero(BF_false_positives, N_THREADS * 2 * sizeof(int));
        #endif
    }

    ~ExtendedART(){
    }

    TART<T, BloomT>& getTART(){
        return tart;
    }

    #if MEASURE_TREE_SIZE == 1
    uint64_t getTARTSize(){
        return tart.getTreeSize();
    }
    #endif

    // Lookup a key with given key index. Lookup will be performed in both RW and RO, if necessary. The key index is 
    // required to guarantee key uniqueness for the bloom filter validation. We do this instead of performing a hash of the key.
    lookup_res lookup(const Key& k, uint64_t key_ind, ThreadInfo& t, unsigned thread_id){
        (void)thread_id;
        if(is_using_bloom()){
            bool contains = false;
            uint64_t hashVal[2];
            contains = bloom.contains(k.getKey(), k.getKeyLen(), hashVal);
            TID val;
            if(contains){ // bloom contains, lookup in RW
                //lookup_res l_res = tart.t_lookup(k, t, false);
                lookup_res l_res = tart.t_lookup(k, t);
                if(!std::get<1>(l_res)) // abort the transaction
                    return l_res;
                #if MEASURE_BF_FALSE_POSITIVES == 1
                    BF_false_positives[thread_id][0]++;
                #endif
                val = std::get<0>(l_res);
                if(val == 0){ // not found tree! False positive
                    #if MEASURE_BF_FALSE_POSITIVES == 1
                        BF_false_positives[thread_id][1]++;
                    #endif
                    //stringstream ss;
                    //ss<<"False positive!\n";
                    //cout<<ss.str();
                }
                return l_res;
            }
            else { // bloom doesn't contain
                #if MEASURE_BF_FALSE_POSITIVES == 1
                    BF_false_positives[thread_id][0]++;
                #endif
                // for now we only use BLOOM_VALIDATE 2, snce BLOOM_VALIDATE 1 is much costlier
                tart.bloom_v_add_key(key_ind, hashVal);
                return std::make_tuple(0, true);
            }
        }
        else {
            //lookup_res l_res = tart.t_lookup(k, t, false);
            lookup_res l_res = tart.t_lookup(k, t);
            return l_res;
        }
    }

    ins_res insert(const Key& k, TID tid, ThreadInfo& t, unsigned thread_id){
        return insert(k, tid, t, false, thread_id);
    }

    ins_res insert(const Key & k, TID tid, ThreadInfo& t, bool bloom_insert, unsigned thread_id){
        (void)thread_id;
        ins_res res = tart.t_insert(k, tid, t);
        if(!std::get<1>(res)) // abort the transaction
            return res;
        if(!std::get<0>(res)) // it is an update, do not insert to bloom!
            bloom_insert = false;
        if(is_using_bloom()){
            if(bloom_insert)
                bloom.insert(k.getKey(), k.getKeyLen());
        }
        return res;
    }

    rem_res remove(const Key & k, TID tid, ThreadInfo& t){
        auto res = tart.t_remove(k, tid, t);
        return res;
    }

};

