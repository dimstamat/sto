#include <iostream>
#include <chrono>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <getopt.h>

#include <unordered_map>
#include <mutex>

#include "tbb/tbb.h"

#include <thread>

#include <iomanip>

#include "tbb/enumerable_thread_specific.h"

using namespace std;

#define MULTITHREADED 1


#if MULTITHREADED == 1
const unsigned ops_per_thread=4000000;
// number of execution threads, including the main thread
//const int N_THREADS = 20;
// now defined in Tree.h as N_THREADS
#else
const unsigned ops_per_thread=20000000;
//const int N_THREADS = 1; 
#endif

#include "ExtendedART.hh"

#include "ARTSynchronized/OptimisticLockCoupling/Tree.h"

#define GUARDED if (TransactionGuard tguard{})

#define NUM_KEYS_MAX 40000000 // 40M keys max


#define HIT_RATIO_MOD 2

#define REMOVE 1
#define BLOOM 0

#include "Zipfian_generator.hh"

#define MEASURE_ELAPSED_TIME 0
#define MEASURE_WITH_STEADY_STATE 0
//#define BLOOM_ACCESS_TEST 1
#define MEASURE_KEY_ACCESSES 0


ZipfianGenerator zipf_inserts, zipf_lookups;

bool runZipf = false;

char * key_dat [NUM_KEYS_MAX];

// CPUs from NUMA node 0
unsigned CPUS [] = {0,4,8,12,16,20,24,28,32,36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76};
// CPUs from NUMA node 2
unsigned CPUS_N2[] = {2,6,10,14,18,22,26,30,34,38,42,46,50,54,58,62,66,70,74,78};
const unsigned thread_pool_sz = N_THREADS-1;
std::thread thread_pool[thread_pool_sz];

uint64_t txns_info_arr [N_THREADS][2] __attribute__((aligned(128)));

#if MEASURE_LATENCIES > 0
double latencies_lookup_found [N_THREADS][2] __attribute__((aligned(128)));
double latencies_lookup_not_found [N_THREADS][2] __attribute__((aligned(128)));
double latencies_insert [N_THREADS][2] __attribute__((aligned(128)));
double latencies_remove [N_THREADS][2] __attribute__((aligned(128)));
double latencies_commit [N_THREADS][2] __attribute__((aligned(128)));
double latencies_bloom_contains [N_THREADS][2] __attribute__((aligned(128)));
double latencies_bloom_insert [N_THREADS][2] __attribute__((aligned(128)));
double latencies_txn_prep [N_THREADS][2] __attribute__((aligned(128)));
#endif

#if MEASURE_LATENCIES == 2
int latencies_raw_lookup_found [N_THREADS][ops_per_thread];
int latencies_raw_lookup_not_found [N_THREADS][ops_per_thread];
#endif


void error(int param){
	fprintf(stderr, "Argument for option %c missing\n", param);
	exit(-1);
}


void loadKeyInit(TID tid, Key& key){
	key.set(key_dat[tid-1], strlen(key_dat[tid-1]));
}

uint64_t key_bytes_init=0, key_bytes_exec=0;

void addKeyStr(TID tid, const char* key_str, bool init){
    if ((key_dat[tid-1] = (char*) malloc((strlen(key_str)+1)*sizeof(char))) == nullptr) {
		fprintf(stderr, "Malloc returned null!\n");
		exit(-1);
	}
    if(init)
        key_bytes_init+=strlen(key_str)+1;
    else
        key_bytes_exec+=strlen(key_str)+1;
    memcpy(key_dat[tid-1], key_str, strlen(key_str)+1);
	//strcpy(key_dat[tid-1], key_str);
}

void cleanup_keys(uint64_t keys_num){
	for (TID tid=0; tid < keys_num; tid++)
		free(key_dat[tid]);
}

void loadKey(TID tid, Key &key){
	key.set(key_dat[tid-1], strlen(key_dat[tid-1]));
}

void loadKeyTART(TID tid, Key &key){
    TID actual_tid;
    uintptr_t tid_p = reinterpret_cast<uintptr_t>(tid);
    if(tid_p & dont_cast_from_rec_bit)
        actual_tid =  reinterpret_cast<TID>(tid_p & ~dont_cast_from_rec_bit);
	else
        // It doesn't matter what template arguments we pass.
        actual_tid = TART<uint64_t, DoubleLookup>::getTIDFromRec(tid);
	key.set(key_dat[actual_tid-1], strlen(key_dat[actual_tid-1]));
}

inline void checkVal(TID val, uint64_t tid){
	if (val != tid){
		stringstream ss; 
		ss << "Wrong key read: " << val << " expected: " << tid << std::endl;
        cout << ss.str();
        throw;
  	}
}
#if BLOOM == 0
ExtendedART<uint64_t, DoubleLookup> eART(loadKeyTART);
#elif BLOOM == 1
ExtendedART<uint64_t, BloomNoPacking> eART(loadKeyTART);
#elif BLOOM == 2
ExtendedART<uint64_t, BloomPacking> eART(loadKeyTART);
#endif



inline void set_affinity(std::thread& t, unsigned i){
    //cout <<"Setting thread " << t.get_id() << " to cpu "<< i<<endl;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}

bool initial_build_done = false;


inline bool do_insert(unsigned thread_id, uint64_t i, ThreadInfo& t){
	(void)thread_id; // to avoid compiler warnings for unused variable
    Key key;
    bool b_insert=true; // always insert to BF for now
    loadKeyInit(i, key);
    ins_res res = eART.insert(key, i, t, b_insert, thread_id); 
    if(!std::get<1>(res)) // abort the transaction
        return false;
    return true;
}

inline bool do_remove(unsigned thread_id, uint64_t i, ThreadInfo& t){
    (void)thread_id; // to avoid compiler warnings for unused variable
    Key key;
    loadKeyInit(i, key);
    rem_res res = eART.remove(key, i, t);
    if(!std::get<1>(res)) // abort the transaction
        return false;
    return true;
}


#if MEASURE_ELAPSED_TIME == 1
double time_b_contains=0, time_b_validation_add_key=0, time_lookup=0;
#endif


inline bool do_lookup(unsigned thread_id, uint64_t i, ThreadInfo& t, bool check_val){
	(void)thread_id; //to avoid compiler warnings for unused variable
    Key key;
    loadKeyInit(i, key);
    lookup_res res = eART.lookup(key, i, t, thread_id);
    if(!std::get<1>(res)) // abort the transaction
        return false;
    auto val = std::get<0>(res);
    if(check_val) checkVal(val, i);
    return true;
}

inline void insert_partition(unsigned ops_per_txn, unsigned thread_id, unsigned ind_start, unsigned ind_end){
    uint64_t cur_txns=0;
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    unsigned key_ind = ind_start;
    auto t = eART.getTART().getThreadInfo();
    //stringstream ss;
    //ss<<"Thread "<<thread_id <<" starting... ind_start: "<< ind_start <<", ind_end: " << ind_end <<endl;
    //cout<<ss.str();
    while(key_ind < ind_end){
        bool first=true;
        TRANSACTION {
            for (unsigned cur_op=0; cur_op<ops_per_txn && key_ind < ind_end; cur_op++, key_ind++){
                //ss.clear();
                //ss<<thread_id<<": inserting, key_ind: "<<key_ind<< ", cur_op: "<<cur_op<<endl;
                //cout<<ss.str();
                do_insert(thread_id, key_ind, t);
            }
            first=false;
        }RETRY(true);
        cur_txns++;
    }
    txns_info_arr[thread_id][0] = cur_txns;
    //ss.clear();
    //ss<<"Thread "<<thread_id<<" finished!\n";
    //cout<<ss.str();
}

inline void remove_partition(unsigned ops_per_txn, unsigned thread_id, unsigned ind_start, unsigned ind_end){
   uint64_t cur_txns=0;
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    unsigned key_ind = ind_start;
    auto t = eART.getTART().getThreadInfo();
    while(key_ind < ind_end){
        bool first=true;
        TRANSACTION {
            for (uint64_t cur_op=0; cur_op<ops_per_txn && key_ind < ind_end; cur_op++, key_ind++){
                do_remove(thread_id, key_ind, t);
            }
            first=false;
        }RETRY(true);
        cur_txns++;
    }
    txns_info_arr[thread_id][0] = cur_txns;
}

inline void lookup_partition(unsigned ops_per_txn, unsigned thread_id, unsigned ind_start, unsigned ind_end){
    uint64_t cur_txns=0;
    TThread::set_id(thread_id);
    Sto::update_threadid();
    unsigned key_ind = ind_start;
    auto t = eART.getTART().getThreadInfo();
    while(key_ind < ind_end){
        TRANSACTION {
            for (uint64_t cur_op=0; cur_op<ops_per_txn && key_ind < ind_end; cur_op++, key_ind++){
                do_lookup(thread_id, key_ind, t, true);
            }
        }RETRY(false);
        cur_txns++;
    }
    txns_info_arr[thread_id][0] = cur_txns;
}



uint64_t key_insert_indexes [N_THREADS][ops_per_thread];
uint64_t key_lookup_indexes [N_THREADS][ops_per_thread];

#define TXN_EXCEPTION_HANDLING 0


void remove_zipf(unsigned ops_per_txn, unsigned thread_id){
    unsigned i=0;
    uint64_t cur_txns = 0;
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    auto t = eART.getTART().getThreadInfo();
    while(i<ops_per_thread){
        uint64_t cur_op=0;
        TRANSACTION {
            for (cur_op=0; cur_op<ops_per_txn && i<ops_per_thread; cur_op++){
                TXN_DO(do_remove(thread_id, key_insert_indexes [thread_id][i+cur_op], t))
            }
        }RETRY(true)
        i+=cur_op;
        cur_txns++;
    }
}

// we need to know whether the accesed key is within the new keys or not, so that to add it in the bloom filter or not.
void insert_lookup_zipf(unsigned ops_per_txn, unsigned thread_id, unsigned insert_ratio_mod, uint64_t new_keys_ind){
    uint64_t cur_txns = 0;
    INIT_COUNTING
    #if MEASURE_ELAPSED_TIME == 1
    struct timespec starttimeinsert, endtimeinsert;
    double duration_ns;
    double elapsed_time_lookups=0;
    double elapsed_time_inserts=0;
    #endif    
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    auto t = eART.getTART().getThreadInfo();
    unsigned i=0;
    while(i<ops_per_thread){
        // periodically check whether we need to merge or not!
        
        uint64_t cur_op=0;
        #if TXN_EXCEPTION_HANDLING == 0
        TRANSACTION_DBG {
        #elif TXN_EXCEPTION_HANDLING == 1
        TRANSACTION_E_DBG {
        #endif
            for (cur_op=0; cur_op<ops_per_txn && i<ops_per_thread; cur_op++){
                if(insert_ratio_mod > 0 && (cur_op % insert_ratio_mod == 0)){ // insert
                    #if TXN_EXCEPTION_HANDLING == 0
                    TXN_DO(do_insert(thread_id, key_insert_indexes [thread_id][i+cur_op], t))
                    #elif TXN_EXCEPTION_HANDLING == 1
                    if(!do_insert(thread_id, key_insert_indexes [thread_id][i+cur_op], t)){
                        //cout<<"Should abort in insert\n";
                        throw Transaction::Abort();
                    }
                    #endif
                    //do_insert(thread_id, key_inds_txn[cur_op], tree_rw, tart_rw, t1);
                }
                else{   // lookup
                    #if MEASURE_ELAPSED_TIME == 1
                    clock_gettime(CLOCK_MONOTONIC, &starttimeinsert);
                    #endif
                    #if TXN_EXCEPTION_HANDLING == 0
                    TXN_DO(do_lookup(thread_id, key_lookup_indexes[thread_id][i+cur_op], t, (key_lookup_indexes[thread_id][i+cur_op] < new_keys_ind)))
                    #elif TXN_EXCEPTION_HANDLING == 1
                    if(!do_lookup(thread_id, key_lookup_indexes[thread_id][i+cur_op], t, (key_lookup_indexes[thread_id][i+cur_op] < new_keys_ind))){
                        //cout<<"Should abort in lookup\n";
                        throw Transaction::Abort();
                    }
                    #endif
                }
            }
        #if TXN_EXCEPTION_HANDLING == 0
        } RETRY_DBG(true, latencies_commit, thread_id);
        #elif TXN_EXCEPTION_HANDLING == 1
        } RETRY_E_DBG(true, latencies_commit, thread_id);
        #endif
        i+=cur_op;
        cur_txns++;
    }
    txns_info_arr[thread_id][0] = cur_txns;
    #if MEASURE_ELAPSED_TIME == 1
    //cout<<"elapsed time for lookups: "<<elapsed_time_lookups<<endl;
    cout<<"-- elapsed time for inserts: "<<elapsed_time_inserts<<endl;
    cout<<"-- LOOKUPS --"<<endl;
    cout<<"-- elapsed time for bloom contains: "<<time_b_contains<<endl;
    cout<<"-- elapsed time for bloom validation add key: "<<time_b_validation_add_key<<endl;
    cout<<"-- elapsed time for lookup: "<<time_lookup<<endl;
    #endif
}


enum Operation {
    lookup_op,
    insert_op,
    remove_op,
};

// starts the specified number of threads and distributes the work evenly
// operation: either lookup or insert
// ops_per_txn: the number of operations per transaction: 0 means non-transactional
void start_threads(uint64_t range_start, uint64_t range_end, Operation op, unsigned ops_per_txn){
    unsigned ind_start, ind_end;
    uint64_t partition_size = (range_end+1 - range_start) / N_THREADS;
    for(unsigned i=0; i<thread_pool_sz; i++){
        ind_start=i*partition_size+range_start;
        ind_end = ind_start + partition_size ;
        //stringstream ss;
        //ss<<"Thread "<<(i+1)<<": ["<<ind_start<<", "<<ind_end<<")"<<endl;
        //cout<<ss.str();
        if(op == lookup_op)
            thread_pool[i] = std::thread(lookup_partition, ops_per_txn, i+1, ind_start, ind_end);
        else if (op == insert_op)
            thread_pool[i] = std::thread(insert_partition, ops_per_txn, i+1, ind_start, ind_end);
        else 
            thread_pool[i] = std::thread(remove_partition, ops_per_txn, i+1, ind_start, ind_end);
        // start from 1 since we reserved CPU 0 for the main thread!
        set_affinity(thread_pool[i], CPUS[i+1]);
    }
}

void start_threads_mixed(unsigned ops_per_txn, unsigned insert_ratio_mod, uint64_t new_keys_ind, Operation op){
    for(unsigned i=0; i<thread_pool_sz; i++){
        if(op == Operation::insert_op)
            thread_pool[i] = std::thread(insert_lookup_zipf, ops_per_txn, i+1, insert_ratio_mod, new_keys_ind);
        else if(op == Operation::remove_op)
            thread_pool[i] = std::thread(remove_zipf, ops_per_txn, i+1);
        // start from 1 since we reserved CPU 0 for the main thread!
        set_affinity(thread_pool[i], CPUS[i+1]);
    }
}

void run_bench(uint64_t num_keys, unsigned insert_ratio, unsigned ops_per_txn, uint64_t new_keys_ind, bool multithreaded){
	bool transactional = ops_per_txn > 0;
    uint64_t total_txns=0;
    // Make sure that main thread has CPU 0.
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(CPUS[0], &cpu_set);
    int ret = sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
    if(ret!=0)
        cout<<"Error setting affinity for main thread!\n";
    // Build tree
	{
        uint64_t partition_size = num_keys / N_THREADS;
		auto starttime = std::chrono::system_clock::now();
		if(multithreaded && ! transactional){
            /*start_threads(1, num_keys, Operation::insert_op, 0);
            uint64_t ind_start = (thread_pool_sz)* partition_size +1;
            uint64_t ind_end = rw_size+1;
            insert_partition(0, 0, ind_start, ind_end);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
            */
		}
		else if (multithreaded && transactional){
            start_threads(1, num_keys, Operation::insert_op, ops_per_txn);
            uint64_t ind_start = (thread_pool_sz)* partition_size +1;
            uint64_t ind_end = num_keys+1;
            insert_partition(ops_per_txn, 0, ind_start, ind_end);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
		}
        /*
		else if (!multithreaded && !transactional){
			auto t1 = tree_rw.getThreadInfo();
			for(uint64_t i=1; i<=rw_size; i++){
				do_insert(0, i);
			}
		}
		else if (!multithreaded && transactional){
			unsigned ind=0;
			for (uint64_t i=1; i<= rw_size / ops_per_txn; i++){
				GUARDED {
					for(uint64_t j=1; j<=ops_per_txn; j++){
						ind = (i-1)*ops_per_txn + j;
						do_insert(0, ind);
					}
				}
                total_txns++;
			}
            GUARDED {
				uint64_t limit = rw_size % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // insert the rest of the keys! (mod)
                    ind++;
                    do_insert(0, ind);
                }
                if(limit>=1)
                    total_txns++;
            }

		}
        */
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
        //printf("insert R/W,%ld,%f\n", rw_size, (rw_size * 1.0) / duration.count());
        if(transactional){
            for(unsigned i=0; i<N_THREADS; i++){
                total_txns += txns_info_arr[i][0];
            }
        }
        printf("insert txn,%lu,%f\n", total_txns, (total_txns * 1.0) / duration.count());
    }
    initial_build_done=true;
    Transaction::clear_stats();
	bool lookups_only = (insert_ratio == 0);
    // Lookup
    {
		unsigned insert_ratio_mod = lookups_only? 0 :  (100 / insert_ratio);
        uint64_t num_ops = num_keys;
        //unsigned range_size = 0;
        total_txns=0;
        auto starttime = std::chrono::system_clock::now();
        if(multithreaded && !transactional){
        }
        else if (multithreaded && transactional){
            if(lookups_only && ! runZipf){
                start_threads(1, num_keys, Operation::lookup_op, ops_per_txn);
                uint64_t partition_size = num_keys / N_THREADS;
                uint64_t ind_start = thread_pool_sz* partition_size +1;
                uint64_t ind_end = num_keys+1;
                //cout<<"Thread 0: ["<<ind_start<<", "<<ind_end<<")"<<endl;
                lookup_partition(ops_per_txn, 0, ind_start, ind_end);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
            }
            else { //mixed workload
                start_threads_mixed(ops_per_txn, insert_ratio_mod, new_keys_ind, Operation::insert_op);
                insert_lookup_zipf(ops_per_txn, 0, insert_ratio_mod, new_keys_ind);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
            }
        }
        else if (!multithreaded && !transactional){
            /*auto t1 = tree_rw.getThreadInfo();
			auto t2 = tree_compacted.getThreadInfo();
            if(lookups_only){
                lookup_partition(0, 0, 1, num_keys+1);
            }
            else{
                insert_lookup_zipf(0, 0, insert_ratio_mod, new_keys_ind, rw_size);
            }*/
            /*for(uint64_t i=1; i<=num_keys; i++){
				if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
                	do_insert(0, i, tree_rw, tart_rw, t1);
				else
					do_lookup(0, i, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, false, true);
            }*/
        }
        else if (!multithreaded && transactional){
            if(lookups_only && ! runZipf){
                lookup_partition(ops_per_txn, 0, 1, num_keys+1);
            }   
            else{
                insert_lookup_zipf(ops_per_txn, 0, insert_ratio_mod, new_keys_ind);
            }   
            /*
            unsigned ind=0;
            for (uint64_t i=1; i<= num_keys / ops_per_txn; i++){
                GUARDED {
                    for(uint64_t j=1; j<=ops_per_txn; j++){
                        ind = (i-1)*ops_per_txn + j;
                        if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
					        //do_insert(0, num_keys+ind, tree_rw, tart_rw, t1);
					        // try to insert existing key
					        do_insert(0, ind, tree_rw, tart_rw, t1);
						else
						    do_lookup(0, ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, true, true);
                    }
                }
                total_txns++;
            }
            GUARDED {
                uint64_t limit = num_keys % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // lookup the rest of the keys! (mod)
                    ind++;
                    if (! lookups_only && ((j-1) % insert_ratio_mod == 0) ) // insert
                        do_insert(0, ind, tree_rw, tart_rw, t1);
                    else
                        do_lookup(0, ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, true, true);
                }
                if(limit>=1)
                    total_txns++;
            }*/
        }
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		//printf("%s,%ld,%f\n", (lookups_only? "lookup" : "lookup/insert" ),  num_keys, (num_keys * 1.0) / duration.count());
        if(transactional){
            for(unsigned i=0; i<N_THREADS; i++){
                total_txns += txns_info_arr[i][0];
            }
        }
        #if BLOOM == 1 && MEASURE_BF_FALSE_POSITIVES == 1
            auto FPs = eART.BF_false_positives;
            int BF_FPs=0, BF_accesses=0;
            for(unsigned i=0; i<N_THREADS; i++){
                BF_accesses += FPs[i][0];
                BF_FPs += FPs[i][1];
            }
            cout<<"Bloom Filter Stats:\n";
            cout<<"False positives: " << BF_FPs<<endl;
            cout<<"Total accesses: "<< BF_accesses<<endl;
            cout<<"False positive ratio: "<<std::setprecision(4)<< (double) BF_FPs / BF_accesses<<endl; 
        #endif
        printf("%s,%ld,%ld,%f (time:%ldsec)\n", (lookups_only? "lookup txn" : "lookup/insert txn" ),  num_ops, total_txns, (total_txns * 1.0) / duration.count(), duration.count()/1000000);
        #if STO_PROFILE_COUNTERS && MEASURE_ABORTS == 1
        Transaction::print_stats();
        unsigned long long aborts_total;
        {   
            txp_counters tc = Transaction::txp_counters_combined();
            aborts_total = tc.p(txp_total_aborts);
            printf("total_n: %llu, total_r: %llu, total_w: %llu, total_searched: %llu, total_aborts: %llu (%llu aborts at commit time)\n", tc.p(txp_total_n), tc.p(txp_total_r), tc.p(txp_total_w), tc.p(txp_total_searched), tc.p(txp_total_aborts), tc.p(txp_commit_time_aborts));
        }
        #endif
        #if MEASURE_LATENCIES > 0
            double lookup_not_found = 0, lookup_not_found_num=0, lookup_found = 0, lookup_found_num=0;
            double insert = 0, insert_num=0, commit=0, commit_num=0;
            double bloom_contains=0, bloom_contains_num=0, bloom_insert=0, bloom_insert_num=0, txn_prep=0, txn_prep_num=0;
            for(unsigned i=0; i<N_THREADS; i++){
                lookup_not_found+=latencies_lookup_not_found[i][0];
                lookup_not_found_num+=latencies_lookup_not_found[i][1];
                lookup_found+=latencies_lookup_found[i][0];
                lookup_found_num+=latencies_lookup_found[i][1];
                insert+=latencies_insert[i][0];
                insert_num+=latencies_insert[i][1];
                commit+=latencies_commit[i][0];
                commit_num+=latencies_commit[i][1];
                bloom_contains+=latencies_bloom_contains[i][0];
                bloom_contains_num+=latencies_bloom_contains[i][1];
                bloom_insert+=latencies_bloom_insert[i][0];
                bloom_insert_num+=latencies_bloom_insert[i][1];
                txn_prep+=latencies_txn_prep[i][0];
                txn_prep_num+=latencies_txn_prep[i][1];
            }
            printf("lookup found\t%lf\t%lf\n", (lookup_found / lookup_found_num) * 1000.0, lookup_found_num);
            printf("lookup not found\t%lf\t%lf\n", (lookup_not_found / lookup_not_found_num) * 1000.0, lookup_not_found_num);
            printf("insert\t%lf\t%lf\n", (insert / insert_num) * 1000.0, insert_num);
            printf("bloom contains\t%lf\t%lf\n", (bloom_contains / bloom_contains_num) * 1000.0, bloom_contains_num);
            printf("bloom insert\t%lf\t%lf\n", (bloom_insert / bloom_insert_num) * 1000.0, bloom_insert_num);
            printf("commit\t%lf\t%lf\n", (commit / commit_num) * 1000.0, commit_num);
            printf("txn prep\t%lf\t%lf\n", (txn_prep / txn_prep_num) * 1000.0, txn_prep_num);
        #endif
    #if MEASURE_LATENCIES == 2
        
        const unsigned nbuckets = 20;
        unsigned found_buckets[nbuckets];
        unsigned not_found_buckets[nbuckets];
        bzero(found_buckets, nbuckets * sizeof(unsigned));
        bzero(not_found_buckets, nbuckets * sizeof(unsigned));
        int min_found=INT_MAX, min_not_found=INT_MAX, max_found=0, max_not_found=0;
        #if MEASURE_WITH_STEADY_STATE == 1
        int found_measurements_num=0, not_found_measurements_num=0;
        std::list<int>found_list;
        std::list<int>not_found_list;
        #endif
        for(unsigned t=0; t<N_THREADS; t++){
            for(unsigned i=0; i<ops_per_thread; i++){
                //found
                if(latencies_raw_lookup_found[t][i] > 0 && latencies_raw_lookup_found[t][i] < min_found)
                    min_found = latencies_raw_lookup_found[t][i];
                if(latencies_raw_lookup_found[t][i] > 0 && latencies_raw_lookup_found[t][i] > max_found)
                    max_found = latencies_raw_lookup_found[t][i];
                #if MEASURE_WITH_STEADY_STATE == 1
                if(latencies_raw_lookup_found[t][i] > 0){
                    found_measurements_num++;
                    found_list.push_back(latencies_raw_lookup_found[t][i]);
                }
                #endif
                //not found
                if(latencies_raw_lookup_not_found[t][i] > 0 && latencies_raw_lookup_not_found[t][i] < min_not_found)
                      min_not_found = latencies_raw_lookup_not_found[t][i];
                if(latencies_raw_lookup_not_found[t][i] > 0 && latencies_raw_lookup_not_found[t][i] > max_not_found)
                      max_not_found = latencies_raw_lookup_not_found[t][i];
                #if MEASURE_WITH_STEADY_STATE == 1
                if(latencies_raw_lookup_not_found[t][i] > 0){
                    not_found_measurements_num++;
                    not_found_list.push_back(latencies_raw_lookup_not_found[t][i]);
                }
                #endif
            }
        }
        #if MEASURE_WITH_STEADY_STATE == 1
            double lookup_found[2], lookup_not_found[2];
            bzero(lookup_found, 2* sizeof(double));
            bzero(lookup_not_found, 2* sizeof(double));
            // calculate for thread 0 for now
            //found
            int i=0;
            for(auto val : found_list){
                if(i< found_measurements_num/2){
                    lookup_found[0]+=(val/1000.0);
                }
                else{
                    lookup_found[1]+=(val/1000.0);
                }
                i++;
            }
            lookup_found[0] = 1000.0 * (lookup_found[0] / (found_measurements_num/2));
            lookup_found[1] = 1000.0 * (lookup_found[1] / (found_measurements_num/2));
            //not found
            i=0;
            for(auto val : not_found_list){
                if(i< not_found_measurements_num/2)
                    lookup_not_found[0]+=(val/1000.0);
                else
                    lookup_not_found[1]+=(val/1000.0);
                i++;
            }
            lookup_not_found[0] = 1000.0 * (lookup_not_found[0] / (not_found_measurements_num/2));
            lookup_not_found[1] = 1000.0 * (lookup_not_found[1] / (not_found_measurements_num/2));
            cout<<"==== STEADY STATE ===="<<endl;
            printf("lookup found total\t%d\n", found_measurements_num);
            printf("lookup found first half\t%.2lf\n",lookup_found[0]);
            printf("lookup found second half\t%.2lf\n",lookup_found[1]);
            printf("lookup not found total\t%d\n",not_found_measurements_num);
            printf("lookup not found first half\t%.2lf\n",lookup_not_found[0]);
            printf("lookup not found second half\t%.2lf\n",lookup_not_found[1]);
        #endif
        bool calculate_found=false, calculate_not_found=false;
        if(min_found < max_found)
            calculate_found = true;
        if(min_not_found <max_not_found)
            calculate_not_found = true;
        unsigned found_bucket_size = (max_found - min_found) / nbuckets;
        unsigned not_found_bucket_size = (max_not_found - min_not_found) / nbuckets;
        // scale to exclude the outliers
        max_found = found_bucket_size/20;
        max_not_found = rw_not_found_bucket_size/20;
        found_bucket_size = (max_found - min_found) / nbuckets;
        not_found_bucket_size = (max_not_found - min_not_found) / nbuckets;
        // iterate again to create histogram
        for(unsigned t=0; t<N_THREADS; t++){
            for(unsigned i=0; i<ops_per_thread; i++){
                if(calculate_found && latencies_raw_lookup_found[t][i] > 0 && latencies_raw_lookup_found[t][i] <= max_found)
                    found_buckets[latencies_raw_lookup_found[t][i]< max_found?  ((latencies_raw_lookup_found[t][i] - min_found) / found_bucket_size) : ((latencies_raw_lookup_found[t][i] - min_found) / found_bucket_size) -1]+=1;
                if(calculate_not_found && latencies_raw_lookup_not_found[t][i] > 0 && latencies_raw_lookup_not_found[t][i] <= max_not_found)
                     not_found_buckets[latencies_raw_lookup_not_found[t][i]< max_not_found?  ((latencies_raw_lookup_not_found[t][i] - min_not_found) / not_found_bucket_size) : ((latencies_raw_lookup_not_found[t][i] - min_not_found) / not_found_bucket_size) -1]+=1;
            }
        }
        cout<<"Min found: "<<min_found<<", min not found: "<<min_not_found<<endl;
        cout<<"Max found: "<<max_found<<", max not found: "<<max_not_found<<endl;
        unsigned found_all=0, not_found_all=0;
        if(calculate_found || calculate_not_found){
            // calculate the total latencies (it is different than latencies_lookup_found because we scaled and excluded the outliers)
            for(unsigned i=0; i<nbuckets; i++){
                found_all+=found_buckets[i];
                not_found_all+=not_found_buckets[i];
            }
        }
        if(calculate_found){
            cout<<"--- lookup found --- bucket size: " << found_bucket_size <<"\n";
            for(unsigned i=0; i<nbuckets; i++){
                cout<< "["<<(i*found_bucket_size) << ", "<< ((i*found_bucket_size) + found_bucket_size)<<")" <<"\t" << found_buckets[i]<< "\t" <<  ((float)found_buckets[i]/found_all) <<endl;
            }
        }
        if(calculate_not_found){
            cout<<"--- lookup not found --- bucket size: " << not_found_bucket_size <<"\n";
            for(unsigned i=0; i<nbuckets; i++){
                cout<< "["<<(i*not_found_bucket_size) << ", "<< ((i*not_found_bucket_size) + not_found_bucket_size)<<")" <<"\t" << not_found_buckets[i] <<"\t" << ((float)not_found_buckets[i]/not_found_all) <<endl;
            }
        }
    #endif

    #if MEASURE_ART_NODE_ACCESSES == 1
    cout<<"Total accessed nodes: "<< accessed_nodes_sum <<endl;
    cout<<"Total lookup requests: "<<accessed_nodes_num <<endl;
    cout<<"Average accessed nodes: "<< ((float)accessed_nodes_sum / accessed_nodes_num) <<endl;
    #endif

    #if MEASURE_ABORTS == 1
        uint64_t aborts_sum[aborts_sz];
        bzero(aborts_sum, aborts_sz * sizeof(uint64_t));
        for(unsigned i=0; i<aborts_sz; i++){
            for(int t=0; t<N_THREADS; t++)
                aborts_sum[i]+=aborts[t][i];
        }
        for(unsigned i=0; i<aborts_sz; i++){
            cout<< "Aborts#"<<(i+1)<<":\t"<< aborts_descr[i]<<":\t" << aborts_sum[i]<< "\t" << std::fixed<< std::setprecision(2) << (double)aborts_sum[i]/aborts_total<<endl;
        }
        cout<<"Aborts total:\t"<< aborts_total<<endl;
    #endif

    }

    #if MEASURE_TREE_SIZE == 1
    cout<<"ART size: "<< (double)eART.getTARTSize() / 1024 / 1024 << "MB" <<endl;
    #endif

    #if REMOVE
	// Remove
	{ 
        bool transactional = ops_per_txn > 0;
        //uint64_t total_txns=0;
        /* Make sure that main thread has CPU 0. */
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        CPU_SET(CPUS[0], &cpu_set);
        int ret = sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
        if(ret!=0)
            cout<<"Error setting affinity for main thread!\n";
        {
            uint64_t partition_size = num_keys / N_THREADS;
            auto starttime = std::chrono::system_clock::now();
            if(multithreaded && ! transactional){
                /*start_threads(1, num_keys, Operation::remove_op, 0);
                uint64_t ind_start = (thread_pool_sz)* partition_size +1;
                uint64_t ind_end = rw_size+1;
                remove_partition(0, 0, ind_start, ind_end);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
                */
            }
            else if (multithreaded && transactional){
                start_threads(1, num_keys, Operation::remove_op, ops_per_txn);
                uint64_t ind_start = (thread_pool_sz)* partition_size +1;
                uint64_t ind_end = num_keys+1;
                remove_partition(ops_per_txn, 0, ind_start, ind_end);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
            }
            /*else if (!multithreaded && !transactional){
                auto t1 = tree_rw.getThreadInfo();
                for(uint64_t i=1; i<=rw_size; i++){
                    do_remove(0, i, tree_rw, tart_rw, t1);
                }
            }
            else if (!multithreaded && transactional){
                unsigned ind=0;
                for (uint64_t i=1; i<= rw_size / ops_per_txn; i++){
                    GUARDED {
                        for(uint64_t j=1; j<=ops_per_txn; j++){
                            ind = (i-1)*ops_per_txn + j;
                            do_remove(0, ind);
                        }
                    }
                    total_txns++;
                }
                GUARDED {
                    uint64_t limit = rw_size % ops_per_txn;
                    for(uint64_t j=1; j<=limit; j++) { // remove the rest of the keys! (mod)
                        ind++;
                        do_remove(0, ind);
                }
                if(limit>=1)
                    total_txns++;
                }
            }*/
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
            printf("remove,%ld,%f\n", num_keys, (num_keys * 1.0) / duration.count());
	    }
    }
  
    
    cout<<"Removing newly inserted keys now!\n"<<std::flush; 
    if(!lookups_only){ 
        // Remove newly inserted keys
        start_threads_mixed(ops_per_txn, 0, new_keys_ind, Operation::remove_op);
        remove_zipf(ops_per_txn, 0);
        for(unsigned i=0; i<thread_pool_sz; i++)
            thread_pool[i].join();
    }
    cout<<"Done removing newly inserted keys!\n"<<std::flush;
    #endif
}

#if MEASURE_KEY_ACCESSES == 1
std::unordered_map<uint64_t, uint64_t> lookups_m, inserts_m;
#endif

std::mutex m_lock;

void init_key_accesses(unsigned thread_id, uint64_t keys_read, bool lookupsOnly, unsigned bucket_size){
    (void)keys_read;
    (void)bucket_size;
    srand(time(nullptr));
    uint64_t key_ind_insert = 0, key_ind_lookup = 0;
    for(unsigned i=0; i<ops_per_thread; i++){
        if(!lookupsOnly)
            key_ind_insert = (uint64_t) zipf_inserts.nextLong((((double)rand()-1))/RAND_MAX);
        key_ind_lookup = (uint64_t) zipf_lookups.nextLong((((double)rand()-1))/RAND_MAX);
        if(!lookupsOnly)
            key_insert_indexes[thread_id][i] = key_ind_insert;
        key_lookup_indexes[thread_id][i] = key_ind_lookup;
        #if MEASURE_KEY_ACCESSES == 1
        m_lock.lock();
        //the first insert will initialize the counter with zero
        lookups_m[key_ind_lookup]++;
        m_lock.unlock();
        if(!lookupsOnly){
            m_lock.lock();
            //the first insert will initialize the counter with zero
            //rw_inserts_m[key_ind_insert]++;
            // measure for 1000-sized buckets
            inserts_m[(key_ind_insert-1)/bucket_size]++;
            m_lock.unlock();
        }
        #endif
    }

}

uint64_t read_keys_from_file(string file_name, uint64_t key_offset, bool init){
    std::ifstream file(file_name);
    std::string line;
    uint64_t keys_read=0;
    
    while(std::getline(file, line)){
        if(line.rfind("P", 0) == 0){
            line = line.replace(0, 2, "");
            keys_read++;
            //if (key_offset == keys_read){
                //keys_read--;
                //break;
            //}
            addKeyStr(key_offset + keys_read, line.c_str(), init);
        }
    }
    return keys_read;
}

int main(int argc, char **argv) {
	char init_files [256];
	char exec_files [256];
    extern char *optarg;
	extern int optopt;
	char c;
	bool init_f_set = false, exec_f_set = false, multithreaded=false;
    //uint64_t tree_size=0;
	unsigned insert_ratio=0, ops_per_txn=0;
    float skew_inserts = 0, skew_lookups=0;

	struct option long_opt [] = 
	{
		{"init-files", required_argument, NULL, 'f'},
		{"exec-files", required_argument, NULL, 'e'},
		{"insert-ratio", required_argument, NULL, 'i'},
		{"ops-per-txn", required_argument, NULL, 'x'},
        {"ops-per-thread", required_argument, NULL, 't'},
        {"skew-inserts", required_argument, NULL, 's'},
        {"skew-lookups", required_argument, NULL, 'l'},
		{"multithreaded", no_argument, NULL, 'm'},
        {NULL, 0, NULL, 0}
	};

    #if MEASURE_LATENCIES > 0
    bzero(latencies_lookup_found, (2*N_THREADS)*sizeof(double));
    bzero(latencies_lookup_not_found, (2*N_THREADS)*sizeof(double));
    bzero(latencies_insert, (2*N_THREADS)*sizeof(double));
    bzero(latencies_remove, (2*N_THREADS)*sizeof(double));
    bzero(latencies_commit, (2*N_THREADS)* sizeof(double));
    bzero(latencies_bloom_contains, (2*N_THREADS)* sizeof(double));
    bzero(latencies_bloom_insert, (2*N_THREADS)* sizeof(double));
    bzero(latencies_txn_prep, (2*N_THREADS)* sizeof(double));
    #endif

	while((c = getopt_long(argc, argv, ":f:e:r:i:x:t:sm", long_opt, NULL)) != -1){
		switch (c){
			case 'f':
				sprintf(init_files, optarg);
				init_f_set = true;
				break;
			case 'e':
				sprintf(exec_files, optarg);
				exec_f_set = true;
				break;
			case 'i':
				insert_ratio = std::stoul(optarg);
				break;
			case 'x':
				ops_per_txn = std::stoul(optarg);
				break;
            /*case 't':
                ops_per_thread = std::stoul(optarg);
                break;*/
            case 's':
                skew_inserts = std::stof(optarg);
                runZipf = true;
                break;
            case 'l':
                skew_lookups = std::stof(optarg);
                runZipf = true;
                break;
			case 'm':
				multithreaded = true;
				break;
			case ':':
				error(optopt);
				break;
			case '?':
				fprintf(stderr, "Unrecognized option %c\n", optopt);
				exit(-1);
            default:
                fprintf(stderr, "Unrecognized option %c\n", optopt);
                exit(-1);
		}
	}

	if(!init_f_set){
		fprintf(stderr, "Missing parameters!\n");
		exit(-1);
	}
	if(insert_ratio > 100){
		fprintf(stderr, "insert ratio cannot be greater than 100\n");
		exit(-1);
	}

    char* cur_file;
    uint64_t init_keys_read=0, exec_keys_read=0;
    cur_file = strtok(init_files, ",");
    while(cur_file != nullptr){
        cout<<"Reading from "<< cur_file<<endl;
        init_keys_read += read_keys_from_file(cur_file, 0, true);
        cur_file = strtok(nullptr, ",");
    }

    if(exec_f_set){
        cur_file = strtok(exec_files, ",");
        while(cur_file != nullptr){
            cout<<"Reading from "<<cur_file<<endl;
            exec_keys_read += read_keys_from_file(cur_file, init_keys_read+exec_keys_read, false);
            cur_file = strtok(nullptr, ",");
        }
	}

    cout<<"total keys read:" <<(init_keys_read + exec_keys_read) <<", init keys: "<< init_keys_read << ", exec keys: "<< exec_keys_read <<endl;
    zipf_inserts = ZipfianGenerator(1, init_keys_read+exec_keys_read, skew_inserts);
	zipf_lookups = ZipfianGenerator(1, init_keys_read+exec_keys_read, skew_lookups);
    cout<<"Generated zipf distribution of "<<zipf_inserts.getItems()<<" numbers for inserts\n";
    cout<<"Storing key accesses\n";
    unsigned bucket_size = 100;
    for (unsigned i=0; i<thread_pool_sz; i++){
        thread_pool[i] = std::thread(init_key_accesses, i+1, init_keys_read, insert_ratio == 0, bucket_size);
    }
    init_key_accesses(0, init_keys_read, insert_ratio == 0, bucket_size);
    for (unsigned i=0; i<thread_pool_sz; i++){
        thread_pool[i].join();
    }
    cout<<"Running bench with insert ratio "<< insert_ratio <<endl;
    run_bench(init_keys_read, insert_ratio, ops_per_txn, init_keys_read+1, multithreaded);
    #if MEASURE_KEY_ACCESSES == 1
    uint64_t rw_lookups=0, ro_lookups=0, off_lookups=0, rw_inserts=0, ro_inserts=0, off_inserts=0;
    double lookup_freq=0, insert_freq=0; // count the average frequency of key accesses
    for(const auto &lookup : lookups_m){
        lookups += lookup.second;
    }    
    lookup_freq = lookups_m.size() > 0 ? ((double)lookups / lookups_m.size()) : 0;

    cout<<"Number of lookups: "<<lookups<<endl;
    cout<<"avg key lookup frequency: "<<lookup_freq<<endl;
    if(insert_ratio>0){
        for(const auto &insert : inserts_m){
            inserts += insert.second;
        }
        //for(unsigned bucket_num=0; bucket_num < ( / bucket_size); bucket_num++){
        //    cout<<"["<< ((bucket_num * bucket_size) +1) << ", "<< ((bucket_num*bucket_size) + bucket_size) <<"]: "<< fixed << setprecision(2)<< (( rw_inserts_m[bucket_num] / (float) (ops_per_thread * N_THREADS) )* 100) <<endl;
        //}
        insert_freq = inserts_m.size() > 0 ? ((double)inserts / inserts_m.size()) : 0;

        cout<<"inserts: "<<inserts<<endl;
        cout<<"avg key insert frequency: "<<insert_freq<<endl;
    }
    #endif
    cout<<"Keys init (MB): "<< ((double)key_bytes_init) / 1024 / 1024 <<endl;
    cout<<"Keys exec (MB): "<< ((double)key_bytes_exec) / 1024 / 1024 <<endl;
	cleanup_keys(init_keys_read + exec_keys_read);
    return 0;
}
