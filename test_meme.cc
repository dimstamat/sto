#include <iostream>
#include <chrono>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <getopt.h>

#include "tbb/tbb.h"

#include <thread>

#include "tbb/enumerable_thread_specific.h"

using namespace std;


#include "TART_old.hh"

#include "ARTSynchronized/OptimisticLockCoupling/Tree.h"

#define GUARDED if (TransactionGuard tguard{})

#define NUM_KEYS_MAX 20000000 // 20M keys max


#define HIT_RATIO_MOD 2
#define FULL_RANGE_ZIPF 3 // 1 for full range, 2 for 50% of accesses in full range and 50% in RO range only, 3 for RO range only
// note that new inserts during the benchmark are inserted in the RW, even though the key range is beyond the initial RW.

#include "Zipfian_generator.hh"

#define PRINT_FALSE_POSITIVES 0

ZipfianGenerator zipf, zipfRO;

TID keys_read=1, keys2_read=1;


char * key_dat [NUM_KEYS_MAX];

// CPUs from the first NUMA node
unsigned CPUS [] = {0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76};
// number of execution threads, including the main thread
const unsigned nthreads = 20;
const unsigned thread_pool_sz = nthreads-1;
std::thread thread_pool[thread_pool_sz];

uint64_t txns_info_arr [nthreads][2] __attribute__((aligned(128)));

#if MEASURE_LATENCIES
double latencies_rw_lookup_found [nthreads][2] __attribute__((aligned(128)));
double latencies_rw_lookup_not_found [nthreads][2] __attribute__((aligned(128)));
double latencies_compacted_lookup [nthreads][2] __attribute__((aligned(128)));
double latencies_rw_insert [nthreads][2] __attribute__((aligned(128)));
double latencies_commit [nthreads][2] __attribute__((aligned(128)));
double latencies_bloom_contains [nthreads][2] __attribute__((aligned(128)));
double latencies_bloom_insert [nthreads][2] __attribute__((aligned(128)));
double latencies_txn_prep [nthreads][2] __attribute__((aligned(128)));
#endif


void error(int param){
	fprintf(stderr, "Argument for option %c missing\n", param);
	exit(-1);
}


void loadKeyInit(TID tid, Key& key){
	key.set(key_dat[tid-1], strlen(key_dat[tid-1]));
}

uint64_t key_bytes_total=0;

void addKeyStr(TID tid, const char* key_str){
    if ((key_dat[tid-1] = (char*) malloc((strlen(key_str)+1)*sizeof(char))) == nullptr) {
		fprintf(stderr, "Malloc returned null!\n");
		exit(-1);
	}
    key_bytes_total+=strlen(key_str)+1;
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
	TID actual_tid = TART<uint64_t>::getTIDFromRec(tid);
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

ART_OLC::Tree tree_rw(loadKey);
ART_OLC::Tree tree_compacted(loadKey);
TART<uint64_t> tart_rw(loadKeyTART);
TART<uint64_t> tart_compacted(loadKeyTART);

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

inline void do_insert(unsigned thread_id, uint64_t i, Tree& tree, TART<uint64_t>& tart, ThreadInfo& tinfo, bool txn, bool b_insert ){
	Key key;
    loadKeyInit(i, key);
	INIT_COUNTING
	START_COUNTING
	if(txn)
		tart.t_insert(key, i, tinfo);
	else
    	tree.insert(key, i, tinfo);
	if(!b_insert && initial_build_done){
        STOP_COUNTING_PRINT("R/W insert")
        STOP_COUNTING(latencies_rw_insert, thread_id)
    }
    #if USE_BLOOM > 0
		if(b_insert){
			START_COUNTING
			bloom_insert(key.getKey(), key.getKeyLen());
            if(initial_build_done)
			    STOP_COUNTING(latencies_bloom_insert, thread_id)
		}
    #endif
}

#define MEASURE_ELAPSED_TIME 0

#if MEASURE_ELAPSED_TIME == 1
double time_b_contains=0, time_b_validation_add_key=0, time_rw_lookup=0, time_compacted_lookup=0;
#endif

inline void do_lookup(unsigned thread_id, uint64_t i, Tree& tree_rw, Tree& tree_compacted, TART<uint64_t>& tart_rw, TART<uint64_t>& tart_compacted, ThreadInfo& t1, ThreadInfo& t2, uint64_t &num_keys, uint64_t &rw_size, bool txn, bool check_val){
	Key key;
    uint64_t key_ind = 0;
    bool inRW = false;
    if(num_keys != 0 && rw_size != 0) { // simulate a lookup from RW or RO, depending on HIT_RATIO_MOD
        if(i % HIT_RATIO_MOD == 0) { // read from R/W
		    //key_ind = (i-1) % rw_size + 1;
            key_ind = ((i / HIT_RATIO_MOD)-1 )% rw_size + 1;
		    inRW = true;
        }
        else { // read from compacted
		    key_ind = (i-1) % (num_keys - rw_size) + rw_size + 1;
        }
    }
    else { // just lookup the given index
        key_ind = i;
    }
    loadKeyInit(key_ind, key);
    #if USE_BLOOM > 0
		bool contains = false;
		INIT_COUNTING
        
        //struct timespec start_time, end_time;
		START_COUNTING
        #if MEASURE_ELAPSED_TIME == 1
        auto starttime = std::chrono::system_clock::now();
        #endif
        #if VALIDATE
            uint64_t* hashVal;
		    contains = bloom_contains(key.getKey(), key.getKeyLen(), &hashVal);
        #else
            contains = bloom_contains(key.getKey(), key.getKeyLen(), nullptr);
        #endif
        #if MEASURE_ELAPSED_TIME == 1
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
        time_b_contains+=duration.count() / 1000000.0;
        #endif

		STOP_COUNTING(latencies_bloom_contains, thread_id);
        STOP_COUNTING_PRINT("bloom contains")
        if(contains){
            //STOP_COUNTING(latencies_bloom_contains, thread_id);
        	START_COUNTING
			//cout<<"RW lookup!\n";
			#if MEASURE_ELAPSED_TIME == 1
			starttime = std::chrono::system_clock::now();
            #endif
            TID val = (txn? std::get<0>(tart_rw.t_lookup(key, t1)) : tree_rw.lookup(key, t1));
            #if MEASURE_ELAPSED_TIME == 1
            duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
            time_rw_lookup+=duration.count() / 1000000.0;
            #endif
            if(val == 0){ // not found in R/W! False positive
				STOP_COUNTING_PRINT("R/W lookup not found")
				STOP_COUNTING(latencies_rw_lookup_not_found, thread_id)
                #if PRINT_FALSE_POSITIVES
                cout <<"False positive!\n";
				#endif
                START_COUNTING
                //cout<<"compacted lookup!\n";
                #if MEASURE_ELAPSED_TIME == 1
                starttime = std::chrono::system_clock::now();
                #endif
                TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)) : tree_compacted.lookup(key, t2));
                #if MEASURE_ELAPSED_TIME == 1
                duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
                time_compacted_lookup+=duration.count() / 1000000.0;
                #endif
                STOP_COUNTING(latencies_compacted_lookup, thread_id)
				//printf("Checking after reading from compacted!\n");
				// only check if we do lookup for an existing key, otherwise we might lookup for a key that is not inserted yet!
                if(check_val) checkVal(val, key_ind);
			}
            else {
                STOP_COUNTING_PRINT("R/W lookup found")
                STOP_COUNTING(latencies_rw_lookup_found, thread_id)
				//printf("Checking after reading from R/W!\n");
            	if(check_val) checkVal(val, key_ind);
            }
		}
        else { // add key in bloom filter validation! Just the hash of the key is enough.
            STOP_COUNTING_PRINT("bloom doesn't contain")
            #if MEASURE_ELAPSED_TIME == 1
            starttime = std::chrono::system_clock::now();
            #endif
            #if VALIDATE
                tart_rw.bloom_v_add_key(hashVal);
            #endif
            #if MEASURE_ELAPSED_TIME == 1
            duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
            time_b_validation_add_key+=duration.count() / 1000000.0;
            #endif
            assert(!inRW);
            START_COUNTING
            //cout<<"compacted lookup!\n";
            #if MEASURE_ELAPSED_TIME == 1
            starttime = std::chrono::system_clock::now();
            #endif
            TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)): tree_compacted.lookup(key, t2));
            #if MEASURE_ELAPSED_TIME == 1
            duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
            time_compacted_lookup+= duration.count() / 1000000.0;
            #endif
            STOP_COUNTING(latencies_compacted_lookup, thread_id)
            if(check_val) checkVal(val, key_ind);
        }
	#else
		INIT_COUNTING
		START_COUNTING
        //cout<<"RW lookup!\n";
        #if MEASURE_ELAPSED_TIME == 1
        auto starttime = std::chrono::system_clock::now();
        #endif
        TID val = (txn ? std::get<0>(tart_rw.t_lookup(key, t1)): tree_rw.lookup(key, t1));
        #if MEASURE_ELAPSED_TIME == 1
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
        time_rw_lookup+= duration.count() / 1000000.0;
        #endif
        if(val == 0){
            STOP_COUNTING_PRINT("R/W lookup not found")
            STOP_COUNTING(latencies_rw_lookup_not_found, thread_id)
        	assert(!inRW);
            START_COUNTING
            //cout<<"compacted lookup!\n";
            #if MEASURE_ELAPSED_TIME == 1
            starttime = std::chrono::system_clock::now();
            #endif
            TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)) : tree_compacted.lookup(key, t2));
            #if MEASURE_ELAPSED_TIME == 1
            duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttime);
            time_compacted_lookup+= duration.count() / 1000000.0;
            #endif
            STOP_COUNTING(latencies_compacted_lookup, thread_id)
            //printf("Checking after reading from compacted!\n");
			if(check_val) checkVal(val, key_ind);
        }
        else {
            STOP_COUNTING_PRINT("R/W lookup found")
            STOP_COUNTING(latencies_rw_lookup_found, thread_id)
			//printf("Checking after reading from R/W!\n");
        	if(check_val) checkVal(val, key_ind);
        }
	#endif
}

void insert_partition(unsigned ops_per_txn, unsigned thread_id, unsigned ind_start, unsigned ind_end, bool rw_insert){
    uint64_t cur_txns=0;
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    if(ops_per_txn > 0){
        auto t = rw_insert ? tart_rw.getThreadInfo() : tart_compacted.getThreadInfo();
        unsigned key_ind = ind_start;
        while(key_ind < ind_end){
            bool first=true;
            TRANSACTION {
                for (uint64_t cur_op=0; cur_op<ops_per_txn && key_ind < ind_end; cur_op++, key_ind++){
                    if(rw_insert)
                        do_insert(thread_id, key_ind, tree_rw, tart_rw, t, true, true);
                    else
                        do_insert(thread_id, key_ind, tree_compacted, tart_compacted, t, true, false);
                }
                first=false;
            }RETRY(true);
            cur_txns++;
        }
        txns_info_arr[thread_id][0] = cur_txns;
    }
    else {
        auto t = rw_insert? tree_rw.getThreadInfo() : tree_compacted.getThreadInfo();
        for(unsigned key_ind = ind_start; key_ind < ind_end; key_ind++){
            if(rw_insert)
                do_insert(thread_id, key_ind, tree_rw, tart_rw, t, false, true);
            else
                do_insert(thread_id, key_ind, tree_compacted, tart_compacted, t, false, false);
        }
    }
}


void lookup_partition(unsigned ops_per_txn, unsigned thread_id, uint64_t num_keys, uint64_t rw_size, unsigned ind_start, unsigned ind_end){
    uint64_t cur_txns=0;
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
        auto t1 = tart_rw.getThreadInfo();
        auto t2 = tart_compacted.getThreadInfo();
        unsigned key_ind = ind_start;
        while(key_ind < ind_end){
            TRANSACTION {
                for (uint64_t cur_op=0; cur_op<ops_per_txn && key_ind < ind_end; cur_op++, key_ind++){
                    do_lookup(thread_id, key_ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, rw_size, true, true);
                }
            }RETRY(false);
            cur_txns++;
        }
        txns_info_arr[thread_id][0] = cur_txns;
    }
    else {
        auto t1 = tree_rw.getThreadInfo();
        auto t2 = tree_compacted.getThreadInfo();
        for(unsigned key_ind = ind_start; key_ind < ind_end; key_ind++){
            do_lookup(thread_id, key_ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, rw_size, false, true);
        }
    }
}


uint64_t key_indexes_per_txn [nthreads][ops_per_thread];

// we need to know whether the accesed key is within the new keys or not, so that to add it in the bloom filter or not.
void insert_lookup_zipf(unsigned ops_per_txn, unsigned ops_per_thread, unsigned thread_id, unsigned insert_ratio_mod, uint64_t new_keys_ind, uint64_t rw_size){
    uint64_t cur_txns = 0;
    INIT_COUNTING
    if(ops_per_txn > 0){
        TThread::set_id(thread_id);
        Sto::update_threadid();
    }
    double elapsed_time_lookups=0;
    double elapsed_time_inserts=0;
    if(ops_per_txn > 0){
        srand(time(nullptr));
        unsigned i=0;
        while(i<ops_per_thread){
            uint64_t key_inds_txn[ops_per_txn]; // we must remember the zipf distribution for each transaction so that to retry with the same one!
            START_COUNTING
            for(unsigned num=0; num<ops_per_txn; num++){
                #if FULL_RANGE_ZIPF == 1 
                key_inds_txn[num] = (uint64_t) zipf.nextLong((((double)rand()-1))/RAND_MAX);
                #elif FULL_RANGE_ZIPF == 2
                key_inds_txn[num] = (num % 2 == 0 ? (uint64_t) zipf.nextLong((((double)rand()-1))/RAND_MAX) : (uint64_t) zipfRO.nextLong((((double)rand()-1))/RAND_MAX) );
                #elif FULL_RANGE_ZIPF == 3
                //key_inds_txn[num] = (uint64_t) zipfRO.nextLong((((double)rand()-1))/RAND_MAX);
                key_inds_txn[num] = (700001 + (i+num)* (thread_id+1)) % (keys_read + keys2_read) + 1;
                #endif
                /*if(key_inds_txn[num] > (uint64_t)zipf.getMax()){
                    cout<<"Noooo, Zipf returned a larger number than specified: "<< key_inds_txn[num] <<", max should be "<< (uint64_t)zipf.getMax() <<"!\n";
                    exit(-1);
                }*/
            }
            uint64_t cur_op=0;
            auto t1 = tree_rw.getThreadInfo();
            auto t2 = tree_compacted.getThreadInfo();
            STOP_COUNTING(latencies_txn_prep, thread_id)
            TRANSACTION_DBG {
                for (cur_op=0; cur_op<ops_per_txn && i<ops_per_thread; cur_op++){
                    if(cur_op % insert_ratio_mod == 0){ // insert
                        #if MEASURE_ELAPSED_TIME == 1
                        auto starttimeinsert = std::chrono::system_clock::now();
                        #endif
                        // only add in the bloom filter if key index is beyond the rw_size
                        do_insert(thread_id, key_inds_txn[cur_op], tree_rw, tart_rw, t1, true, key_inds_txn[cur_op] > rw_size);
                        #if MEASURE_ELAPSED_TIME == 1
                        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttimeinsert);
                        elapsed_time_inserts+=duration.count() / 1000000.0;
                        #endif
                        //do_insert(thread_id, key_inds_txn[cur_op], tree_rw, tart_rw, t1, true, false);
                    }
                    else{   // lookup
                        uint64_t n1=0, n2=0;
                        #if MEASURE_ELAPSED_TIME == 1
                        auto starttimelookup = std::chrono::system_clock::now();
                        #endif
                        do_lookup(thread_id, key_inds_txn[cur_op], tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, n1, n2, true, (key_inds_txn[cur_op] < new_keys_ind));
                        #if MEASURE_ELAPSED_TIME == 1
                        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - starttimelookup);
                        elapsed_time_lookups+=duration.count() / 1000000.0;
                        #endif
                    }
                }
            } RETRY_DBG(true, latencies_commit, thread_id);
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
        cout<<"-- elapsed time for rw lookup: "<<time_rw_lookup<<endl;
        cout<<"-- elapsed time for compacted lookup: "<<time_compacted_lookup<<endl;
        #endif

    }
    else {
    }
}


enum Operation {
    lookup,
    insert_rw,
    insert_compacted,
};

// starts the specified number of threads and distributes the work evenly
// operation: either lookup or insert
// ops_per_txn: the number of operations per transaction: 0 means non-transactional
void start_threads(uint64_t range_start, uint64_t range_end, uint64_t num_keys, uint64_t rw_size, Operation op, unsigned ops_per_txn){
    unsigned ind_start, ind_end;
    uint64_t partition_size = (range_end+1 - range_start) / nthreads;
    for(unsigned i=0; i<thread_pool_sz; i++){
        ind_start=i*partition_size+range_start;
        ind_end = ind_start + partition_size ;
        //cout<<"Thread "<<(i+1)<<": ["<<ind_start<<", "<<ind_end<<")"<<endl;
        if(op == lookup)
            thread_pool[i] = std::thread(lookup_partition, ops_per_txn, i+1, num_keys, rw_size, ind_start, ind_end);
        else
            thread_pool[i] = std::thread(insert_partition, ops_per_txn, i+1, ind_start, ind_end, op == insert_rw);
        // start from 1 since we reserved CPU 0 for the main thread!
        set_affinity(thread_pool[i], CPUS[i+1]);
    }
}

void start_threads_mixed(unsigned ops_per_txn, unsigned ops_per_thread, unsigned insert_ratio_mod, uint64_t new_keys_ind, uint64_t rw_size){
    for(unsigned i=0; i<thread_pool_sz; i++){
        thread_pool[i] = std::thread(insert_lookup_zipf, ops_per_txn, ops_per_thread, i+1, insert_ratio_mod, new_keys_ind, rw_size);
        // start from 1 since we reserved CPU 0 for the main thread!
        set_affinity(thread_pool[i], CPUS[i+1]);
    }
}

void run_bench(uint64_t num_keys, uint64_t rw_size, unsigned insert_ratio, unsigned ops_per_txn, unsigned ops_per_thread, uint64_t new_keys_ind, bool multithreaded){
	rw_size = rw_size > num_keys ? num_keys : rw_size;
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
        uint64_t partition_size = rw_size / nthreads;
		auto starttime = std::chrono::system_clock::now();
		if(multithreaded && ! transactional){
            start_threads(1, rw_size, num_keys, rw_size, Operation::insert_rw, 0);
            uint64_t ind_start = (thread_pool_sz)* partition_size +1;
            uint64_t ind_end = rw_size+1;
            insert_partition(0, 0, ind_start, ind_end, true);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
		}
		else if (multithreaded && transactional){
            start_threads(1, rw_size, num_keys, rw_size, Operation::insert_rw, ops_per_txn);
            uint64_t ind_start = (thread_pool_sz)* partition_size +1;
            uint64_t ind_end = rw_size+1;
            insert_partition(ops_per_txn, 0, ind_start, ind_end, true);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
		}
		else if (!multithreaded && !transactional){
			auto t1 = tree_rw.getThreadInfo();
			for(uint64_t i=1; i<=rw_size; i++){
				do_insert(0, i, tree_rw, tart_rw, t1, false, true);
			}
		}
		else if (!multithreaded && transactional){
			unsigned ind=0;
			auto t1 = tart_rw.getThreadInfo();
			for (uint64_t i=1; i<= rw_size / ops_per_txn; i++){
				GUARDED {
					for(uint64_t j=1; j<=ops_per_txn; j++){
						ind = (i-1)*ops_per_txn + j;
						do_insert(0, ind, tree_rw, tart_rw, t1, true, true);
					}
				}
                total_txns++;
			}
            GUARDED {
				uint64_t limit = rw_size % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // insert the rest of the keys! (mod)
                    ind++;
                    do_insert(0, ind, tree_rw, tart_rw, t1, true, true);
                }
                if(limit>=1)
                    total_txns++;
            }

		}
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
        //printf("insert R/W,%ld,%f\n", rw_size, (rw_size * 1.0) / duration.count());
        if(multithreaded){
            for(unsigned i=0; i<nthreads; i++){
                total_txns += txns_info_arr[i][0];
            }
        }
        printf("insert R/W txn,%ld,%lu,%f\n", rw_size, total_txns, (total_txns * 1.0) / duration.count());
        // Insert compacted
        starttime = std::chrono::system_clock::now();
        if(multithreaded && ! transactional){
            start_threads(rw_size+1, num_keys, num_keys, rw_size, Operation::insert_compacted, 0);
            uint64_t partition_size = (num_keys - rw_size) / nthreads;
            uint64_t ind_start = thread_pool_sz* partition_size +1;
            uint64_t ind_end = num_keys+1;
            insert_partition(0, 0, ind_start, ind_end, false);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
        }
        else if (multithreaded && transactional){
            start_threads(rw_size+1, num_keys, num_keys, rw_size, Operation::insert_compacted, ops_per_txn);
            uint64_t partition_size = (num_keys - rw_size) / nthreads;
            uint64_t ind_start = thread_pool_sz* partition_size + + rw_size + 1;
            uint64_t ind_end = num_keys+1;
            //cout<<"Thread 0: ["<<ind_start<<", "<<ind_end<<")"<<endl;
            insert_partition(ops_per_txn, 0, ind_start, ind_end, false);
            for(unsigned i=0; i<thread_pool_sz; i++)
                thread_pool[i].join();
        }
        else if (!multithreaded && !transactional){
			auto t2 = tree_compacted.getThreadInfo();
			for(uint64_t i=rw_size+1; i<=num_keys; i++){
				do_insert(0, i, tree_compacted, tart_compacted, t2, false, false);
            }
        }
        else if (!multithreaded && transactional){
			auto t2 = tart_compacted.getThreadInfo();
			unsigned ind=0;
            for (uint64_t i=1; i<= (num_keys - rw_size) / ops_per_txn; i++){
                GUARDED {
                    for(uint64_t j=1; j<=ops_per_txn; j++){
                        ind = rw_size + (i-1)*ops_per_txn + j;
                        do_insert(0, ind, tree_compacted, tart_compacted, t2, true, false);
                    }
                }
			}
			uint64_t limit = (num_keys - rw_size) % ops_per_txn;
			GUARDED {
				for(uint64_t j=1; j<=limit; j++) { // insert the rest of the keys! (mod)
					ind++;
					do_insert(0, ind, tree_compacted, tart_compacted, t2, true, false);
				}
			}
        }
		duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		printf("insert compacted,%ld,%f\n", num_keys - rw_size+2, ((num_keys - rw_size +2)* 1.0) / duration.count());
    }
    initial_build_done=true;
    Transaction::clear_stats();
	// Lookup
    {
		bool lookups_only = insert_ratio == 0;
		unsigned insert_ratio_mod = lookups_only? 0 :  (100 / insert_ratio);
        uint64_t num_ops = num_keys;
        //unsigned range_size = 0;
        total_txns=0;
        auto starttime = std::chrono::system_clock::now();
        if(multithreaded && !transactional){
        }
        else if (multithreaded && transactional){
            if(lookups_only){
                start_threads(1, num_keys, num_keys, rw_size, Operation::lookup, ops_per_txn);
                uint64_t partition_size = num_keys / nthreads;
                uint64_t ind_start = thread_pool_sz* partition_size +1;
                uint64_t ind_end = num_keys+1;
                //cout<<"Thread 0: ["<<ind_start<<", "<<ind_end<<")"<<endl;
                lookup_partition(ops_per_txn, 0, num_keys, rw_size, ind_start, ind_end);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
            }
            else { //mixed workload
                start_threads_mixed(ops_per_txn, ops_per_thread, insert_ratio_mod, new_keys_ind, rw_size);
                insert_lookup_zipf(ops_per_txn, ops_per_thread, 0, insert_ratio_mod, new_keys_ind, rw_size);
                for(unsigned i=0; i<thread_pool_sz; i++)
                    thread_pool[i].join();
            }
        }
        else if (!multithreaded && !transactional){
            auto t1 = tree_rw.getThreadInfo();
			auto t2 = tree_compacted.getThreadInfo();
            if(lookups_only){
                lookup_partition(0, 0, num_keys, rw_size, 1, num_keys+1);
            }
            else{
                insert_lookup_zipf(0, ops_per_thread, 0, insert_ratio_mod, new_keys_ind, rw_size);
            }
            /*for(uint64_t i=1; i<=num_keys; i++){
				if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
                	do_insert(0, i, tree_rw, tart_rw, t1, false, false);
				else
					do_lookup(0, i, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, rw_size, false, true);
            }*/
        }
        else if (!multithreaded && transactional){
			unsigned ind=0;
            auto t1 = tart_rw.getThreadInfo();
            auto t2 = tart_compacted.getThreadInfo();
            if(lookups_only){
                lookup_partition(ops_per_txn, 0, num_keys, rw_size, 1, num_keys+1);
            }   
            else{
                insert_lookup_zipf(ops_per_txn, ops_per_thread, 0, insert_ratio_mod, new_keys_ind, rw_size);
            }   
            /*
            for (uint64_t i=1; i<= num_keys / ops_per_txn; i++){
                GUARDED {
                    for(uint64_t j=1; j<=ops_per_txn; j++){
                        ind = (i-1)*ops_per_txn + j;
                        if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
					        //do_insert(0, num_keys+ind, tree_rw, tart_rw, t1, true, true);
					        // try to insert existing key
					        do_insert(0, ind, tree_rw, tart_rw, t1, true, false);
						else
						    do_lookup(0, ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, rw_size, true, true);
                    }
                }
                total_txns++;
            }
            GUARDED {
                uint64_t limit = num_keys % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // lookup the rest of the keys! (mod)
                    ind++;
                    if (! lookups_only && ((j-1) % insert_ratio_mod == 0) ) // insert
                        do_insert(0, ind, tree_rw, tart_rw, t1, true, false);
                    else
                        do_lookup(0, ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, rw_size, true, true);
                }
                if(limit>=1)
                    total_txns++;
            }*/
        }
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		//printf("%s,%ld,%f\n", (lookups_only? "lookup" : "lookup/insert" ),  num_keys, (num_keys * 1.0) / duration.count());
        if(transactional){
            for(unsigned i=0; i<nthreads; i++){
                total_txns += txns_info_arr[i][0];
            }
        }        
        printf("%s,%ld,%ld,%f (time:%ldsec)\n", (lookups_only? "lookup txn" : "lookup/insert txn" ),  num_ops, total_txns, (total_txns * 1.0) / duration.count(), duration.count()/1000000);
        #if STO_PROFILE_COUNTERS
        Transaction::print_stats();
        {   
            txp_counters tc = Transaction::txp_counters_combined();
            printf("total_n: %llu, total_r: %llu, total_w: %llu, total_searched: %llu, total_aborts: %llu (%llu aborts at commit time)\n", tc.p(txp_total_n), tc.p(txp_total_r), tc.p(txp_total_w), tc.p(txp_total_searched), tc.p(txp_total_aborts), tc.p(txp_commit_time_aborts));
        }
        #endif
        #if MEASURE_LATENCIES
            double rw_lookup_not_found = 0, rw_lookup_not_found_num=0, rw_lookup_found = 0, rw_lookup_found_num=0;
            double compacted_lookup=0, compacted_lookup_num=0, rw_insert = 0, rw_insert_num=0, commit=0, commit_num=0;
            double bloom_contains=0, bloom_contains_num=0, bloom_insert=0, bloom_insert_num=0, txn_prep=0, txn_prep_num=0;
            for(unsigned i=0; i<nthreads; i++){
                rw_lookup_not_found+=latencies_rw_lookup_not_found[i][0];
                rw_lookup_not_found_num+=latencies_rw_lookup_not_found[i][1];
                rw_lookup_found+=latencies_rw_lookup_found[i][0];
                rw_lookup_found_num+=latencies_rw_lookup_found[i][1];
                compacted_lookup+=latencies_compacted_lookup[i][0];
                compacted_lookup_num+=latencies_compacted_lookup[i][1];
                rw_insert+=latencies_rw_insert[i][0];
                rw_insert_num+=latencies_rw_insert[i][1];
                commit+=latencies_commit[i][0];
                commit_num+=latencies_commit[i][1];
                bloom_contains+=latencies_bloom_contains[i][0];
                bloom_contains_num+=latencies_bloom_contains[i][1];
                bloom_insert+=latencies_bloom_insert[i][0];
                bloom_insert_num+=latencies_bloom_insert[i][1];
                txn_prep+=latencies_txn_prep[i][0];
                txn_prep_num+=latencies_txn_prep[i][1];
            }
            printf("RW lookup not found: %lf (#%lf)\n", (rw_lookup_not_found / rw_lookup_not_found_num) * 1000.0, rw_lookup_not_found_num);
            printf("RW lookup found: %lf (#%lf)\n", (rw_lookup_found / rw_lookup_found_num) * 1000.0, rw_lookup_found_num);
            printf("compacted lookup: %lf (#%lf)\n", (compacted_lookup / compacted_lookup_num) * 1000.0, compacted_lookup_num);
            printf("RW insert: %lf (#%lf)\n", (rw_insert / rw_insert_num) * 1000.0, rw_insert_num);
            printf("bloom contains: %lf (#%lf)\n", (bloom_contains / bloom_contains_num) * 1000.0, bloom_contains_num);
            printf("bloom insert: %lf (#%lf)\n", (bloom_insert / bloom_insert_num) * 1000.0, bloom_insert_num);
            printf("commit: %lf (#%lf)\n", (commit / commit_num) * 1000.0, commit_num);
            printf("txn prep: %lf (#%lf)\n", (txn_prep / txn_prep_num) * 1000.0, txn_prep_num);
        #endif
	}
	// Remove
	{ /*
		auto starttime = std::chrono::system_clock::now();
        #if MULTITHREADED
        tbb::parallel_for(tbb::blocked_range<uint64_t>(1, rw_size+1), [&](const tbb::blocked_range<uint64_t> &range) {
        auto t1 = tree_rw.getThreadInfo();
        for(uint64_t i=range.begin(); i!= range.end(); i++) {
		#else
		for(uint64_t i=1; i<=rw_size; i++){
		#endif
			Key key;
			loadKeyInit(i, key);
			tree_rw.remove(key, i, t1);
		}
		#if MULTITHREADED
		});
		#endif
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		printf("remove R/W,%ld,%f\n", rw_size, (rw_size * 1.0) / duration.count());
        
		starttime = std::chrono::system_clock::now();
        #if MULTITHREADED
        tbb::parallel_for(tbb::blocked_range<uint64_t>(rw_size+1, num_keys+1), [&](const tbb::blocked_range<uint64_t> &range) {
        auto t2 = tree_compacted.getThreadInfo();
        for(uint64_t i=range.begin(); i!= range.end(); i++) {
        #else
        for(uint64_t i=rw_size+1; i<=num_keys; i++){
        #endif
            Key key;
            loadKeyInit(i, key);
            tree_compacted.remove(key, i, t2);
        }
        #if MULTITHREADED
        }); 
        #endif
        duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("remove compacted,%ld,%f\n", num_keys-rw_size+2, ((num_keys-rw_size+2) * 1.0) / duration.count());
		*/
	}
}

int main(int argc, char **argv) {
	char filename1 [256];
	char filename2 [256];
    extern char *optarg;
	extern int optopt;
	char c;
	bool f1_set = false, f2_set = false, r_w_set = false, multithreaded=false;
	uint64_t rw_size=0;
	unsigned insert_ratio=0, ops_per_txn=0, ops_per_thread=1000000;
    float skew = 0;

	struct option long_opt [] = 
	{
		{"file1", required_argument, NULL, 'f'},
		{"file2", required_argument, NULL, 'g'},
		{"rw-size", required_argument, NULL, 'r'},
		{"insert-ratio", required_argument, NULL, 'i'},
		{"ops-per-txn", required_argument, NULL, 'x'},
        {"ops-per-thread", required_argument, NULL, 't'},
        {"skew", required_argument, NULL, 's'},
		{"multithreaded", no_argument, NULL, 'm'},
        {NULL, 0, NULL, 0}
	};

	#if USE_BLOOM > 0
	memset(bloom, 0, BLOOM_SIZE * sizeof(uint64_t));
    #endif

    #if MEASURE_LATENCIES
    bzero(latencies_rw_lookup_found, (2*nthreads)*sizeof(double));
    bzero(latencies_rw_lookup_not_found, (2*nthreads)*sizeof(double));
    bzero(latencies_rw_insert, (2*nthreads)*sizeof(double));
    bzero(latencies_commit, (2*nthreads)* sizeof(double));
    bzero(latencies_bloom_contains, (2*nthreads)* sizeof(double));
    bzero(latencies_bloom_insert, (2*nthreads)* sizeof(double));
    bzero(latencies_txn_prep, (2*nthreads)* sizeof(double));
    #endif

	while((c = getopt_long(argc, argv, ":f:g:r:i:x:t:sm", long_opt, NULL)) != -1){
		switch (c){
			case 'f':
				sprintf(filename1, optarg);
				f1_set = true;
				break;
			case 'g':
				sprintf(filename2, optarg);
				f2_set = true;
				break;
			case 'r':
				rw_size = std::stoul(optarg);
				r_w_set = true;
				break;
			case 'i':
				insert_ratio = std::stoul(optarg);
				break;
			case 'x':
				ops_per_txn = std::stoul(optarg);
				break;
            case 't':
                ops_per_thread = std::stoul(optarg);
                break;
            case 's':
                skew = std::stof(optarg);
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

	if(!f1_set || !r_w_set){
		fprintf(stderr, "Missing parameters!\n");
		exit(-1);
	}
	if(insert_ratio > 100){
		fprintf(stderr, "insert ratio cannot be greater than 100\n");
		exit(-1);
	}

	std::ifstream file(filename1);
	std::string line;
	//TID keys_read = 1;
 
	while(std::getline(file, line)){
		if(line.rfind("P", 0) == 0){
			line = line.replace(0, 2, "");
			//cout <<line<<endl;
			addKeyStr(keys_read, line.c_str());
			keys_read++;
		}
	}
	keys_read--;
	// because we started from 1 (since TIDs must be > 0)
	//TID keys2_read = 1;
	if(f2_set && insert_ratio > 0){ // mixed workload
		std::ifstream file2(filename2);
		std::string line;
		while(std::getline(file2, line)){
			if(keys2_read-1 == keys_read) // done, no need to read more keys from file2
				break;
			if(line.rfind("P", 0) == 0){
				line = line.replace(0, 2, "");
				addKeyStr(keys_read+keys2_read, line.c_str());
				keys2_read++;
			}
		}
		if(keys2_read-1 < keys_read) {
			fprintf(stderr, "provided keys from filename2 are less than these of filename1 (%lu vs %lu)\n", keys2_read-1, keys_read);
			cleanup_keys(keys_read +keys2_read-1);
			exit(-1);
		}
	}

	keys2_read--;
    cout<<"keys read:" <<(keys_read + keys2_read)<<endl;
	zipf = ZipfianGenerator(1, keys_read+keys2_read, skew);
    // ask from RO only!
    zipfRO = ZipfianGenerator(700001, keys_read+keys2_read, skew);
    cout<<"Generated zipf distribution of "<<zipf.getItems()<<" numbers\n";
    cout<<"Running bench with insert ratio "<< insert_ratio <<endl;
    run_bench(keys_read, rw_size, insert_ratio, ops_per_txn, ops_per_thread, keys_read+1, multithreaded);
    /*auto t = tree_rw.getThreadInfo();
    TRANSACTION {
     do_insert(1, tree_rw, tart_rw, t, true, true);
     do_insert(2, tree_rw, tart_rw, t, true, true);
     do_insert(3, tree_rw, tart_rw, t, true, true);
     do_insert(4, tree_rw, tart_rw, t, true, true);
     do_insert(5, tree_rw, tart_rw, t, true, true);
    } RETRY(false);
    Transaction::print_stats();*/
    cout<<"Keys total (GB): "<< ((double)key_bytes_total) / 1024 / 1024 / 1024 <<endl;
	cleanup_keys(keys_read + keys2_read);
	#if USE_BLOOM > 0
    inspect_bloom();
    #endif
    return 0;
}
