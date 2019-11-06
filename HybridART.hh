#pragma once

#include "TART.hh"
#include "../util/bloom.hh"
#include "OptimisticLockCoupling/Tree.h"


#define MEASURE_BF_FALSE_POSITIVES 1

#if MEASURE_LATENCIES > 0
extern double latencies_rw_lookup_found [N_THREADS][2] __attribute__((aligned(128)));
extern double latencies_rw_lookup_not_found [N_THREADS][2] __attribute__((aligned(128)));
extern double latencies_compacted_lookup [N_THREADS][2] __attribute__((aligned(128)));
extern double latencies_rw_insert [N_THREADS][2] __attribute__((aligned(128)));
#endif


// needed for the range query when we merge. We store the
// result TIDs in an array and then insert all these keys
// into RO
#define MAX_KEYS 20000000


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

    #if MEASURE_BF_FALSE_POSITIVES
        int BF_false_positives[N_THREADS][2] __attribute__((aligned(128)));
    #endif
    
    HybridART(Tree::LoadKeyFunction ARTloadKeyFun, Tree::LoadKeyFunction TARTloadKeyFun): tart_rw(TARTloadKeyFun, bloom), tree_ro(ARTloadKeyFun)
    {
        #if MEASURE_BF_FALSE_POSITIVES
            bzero(BF_false_positives, N_THREADS * 2 * sizeof(int));
        #endif
    }

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
        (void)thread_id;
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
                #if MEASURE_BF_FALSE_POSITIVES == 1
                    BF_false_positives[thread_id][0]++;
                #endif
                val = std::get<0>(l_res);
                if(val == 0){ // not found in RW! False positive
                    #if MEASURE_BF_FALSE_POSITIVES == 1
                        BF_false_positives[thread_id][1]++;
                    #endif
                    STOP_COUNTING(latencies_rw_lookup_not_found, thread_id)
                    START_COUNTING
                    val = tree_ro.lookup(k, t_ro);
                    STOP_COUNTING(latencies_rw_lookup_found, thread_id)
                    return std::make_tuple(val, true);
                }
                return l_res;
            }
            else { // bloom doesn't contain
                #if MEASURE_BF_FALSE_POSITIVES == 1
                    BF_false_positives[thread_id][0]++;
                #endif
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

    ins_res insert(const Key& k, TID tid, ThreadInfo& t, unsigned thread_id){
        return insert(k, tid, t, false, thread_id);
    }

    ins_res insert(const Key & k, TID tid, ThreadInfo& t, bool bloom_insert, unsigned thread_id){
        INIT_COUNTING
        START_COUNTING
        ins_res res = tart_rw.t_insert(k, tid, t);
        STOP_COUNTING(latencies_rw_insert, thread_id);
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

    //
    void merge(){
        sequentialMerge();
    }
   
    // this will be called by the main thread when 
    // making sure that all other threads block and wait
    // for the merge to finish
    void sequentialMerge(){
        Key key_start, key_end, key_cont;
        TID *results;
        ThreadInfo t_rw = tart_rw.getThreadInfo();
        ThreadInfo t_ro = tree_ro.getThreadInfo();
        results = new TID[MAX_KEYS];
        std::size_t resultsFound;
        char key_dat [][2] = {{(char)0}, {(char)255}};
        key_start.set(key_dat[0], (unsigned)1);
        key_end.set(key_dat[1], (unsigned)1);
        tart_rw.lookupRange(key_start, key_end, key_cont, results, MAX_KEYS, resultsFound, t_rw);
        cout<<"Found "<<resultsFound<<" keys in RW\n";
        for(std::size_t i=0; i<resultsFound; i++){
            Key k;
            tart_rw.loadKey(results[i], k);
            typename TART<T, BloomT>::record* rec = reinterpret_cast<typename TART<T, BloomT>::record*>(results[i]);
            tree_ro.insert(k, rec->val, t_ro);
        }
        delete [] results;
    }

};




