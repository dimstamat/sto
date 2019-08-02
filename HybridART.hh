#pragma once

#include "TART.hh"
#include "../util/bloom.hh"
#include "OptimisticLockCoupling/Tree.h"



#if MEASURE_LATENCIES > 0
extern double latencies_rw_lookup_found [nthreads][2] __attribute__((aligned(128)));
extern double latencies_rw_lookup_not_found [nthreads][2] __attribute__((aligned(128)));
extern double latencies_compacted_lookup [nthreads][2] __attribute__((aligned(128)));
#endif




template <typename T, typename BloomT> class HybridART{
protected:
    TART<T, BloomT> tart_rw;
    ART_OLC::Tree tree_ro;
    
    BloomT bloom;


inline bool is_using_bloom(){
    return !std::is_same<BloomT, DoubleLookup>::value;
    //return typeid(bloom) != typeid(DoubleLookup);
}


public:
    HybridART(Tree::LoadKeyFunction ARTloadKeyFun, Tree::LoadKeyFunction TARTloadKeyFun): tart_rw(TARTloadKeyFun, bloom), tree_ro(ARTloadKeyFun)
    {}

    ~HybridART(){
    }

    TART<T, BloomT>& getTART(){
        return tart_rw;
    }

    ART_OLC::Tree& getRO(){
        return tree_ro;
    }

    #if MEASURE_TREE_SIZE == 1
    uint64_t getTARTSize(){
        return tart_rw.getTreeSize();
    }
    #endif

    // Lookup a key with given key index. Lookup will be performed in both RW and RO, if necessary. The key index is 
    // required to guarantee key uniqueness for the bloom filter validation. We do this instead of performing a hash of the key.
    lookup_res lookup(const Key& k, uint64_t key_ind, ThreadInfo& t_rw, ThreadInfo& t_ro, unsigned thread_id){
        INIT_COUNTING
        if(is_using_bloom()){
            bool contains = false;
            uint64_t hashVal[2];
            contains = bloom.contains(k.getKey(), k.getKeyLen(), hashVal);
            TID val;
            if(contains){ // bloom contains, lookup in RW
                START_COUNTING
                lookup_res l_res = tart_rw.t_lookup(k, t_rw);
                if(!std::get<1>(l_res)) // abort the transaction
                    return l_res;
                val = std::get<0>(l_res);
                if(val == 0){ // not found in RW! False positive
                    STOP_COUNTING(latencies_rw_lookup_not_found, thread_id)
                    START_COUNTING
                    val = tree_ro.lookup(k, t_ro);
                    STOP_COUNTING(latencies_rw_lookup_found, thread_id)
                    return std::make_tuple(val, true);
                }
                return l_res;
            }
            else { // bloom doesn't contain
                // for now we only use BLOOM_VALIDATE 2, snce BLOOM_VALIDATE 1 is much costlier
                tart_rw.bloom_v_add_key(key_ind, hashVal);
                START_COUNTING
                val = tree_ro.lookup(k, t_ro);
                STOP_COUNTING(latencies_compacted_lookup, thread_id)
                return std::make_tuple(val, true);
            }
        }
        else { // double lookup
            TID val;
            START_COUNTING
            lookup_res l_res = tart_rw.t_lookup(k, t_rw);
            if(!std::get<1>(l_res)) // abort the transaction
                return l_res;
            val = std::get<0>(l_res);
            if(val == 0) { // not found in RW, look in compacted
                STOP_COUNTING(latencies_rw_lookup_not_found, thread_id)
                START_COUNTING
                val = tree_ro.lookup(k, t_ro);
                STOP_COUNTING(latencies_compacted_lookup, thread_id)
                return std::make_tuple(val, true);
            }
            else{
                STOP_COUNTING(latencies_rw_lookup_found, thread_id)
            }
            return l_res;
        }
    }

    ins_res insert(const Key& k, TID tid, ThreadInfo& t){
        return insert(k, tid, t, false);
    }

    ins_res insert(const Key & k, TID tid, ThreadInfo& t, bool bloom_insert){
        ins_res res = tart_rw.t_insert(k, tid, t);
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

    void ro_insert(const Key& k, TID tid, ThreadInfo& t){
        tree_ro.insert(k, tid, t);
    }

    rem_res remove(const Key & k, TID tid, ThreadInfo& t){
        auto res = tart_rw.t_remove(k, tid, t);
        return res;
    }
    
    void ro_remove(const Key & k, TID tid, ThreadInfo& t){
        tree_ro.remove(k, tid, t);
    }

    void merge(){
    }

};




