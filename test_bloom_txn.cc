#include <iostream>
#include <chrono>
#include "tbb/tbb.h"

using namespace std;


#include "measure_latencies.hh"


#define GUARDED if (TransactionGuard tguard{})

#define SINGLE_TREE 0
#define HIT_RATIO_MOD 2
#define SEQ_INSERT1 0
#define SEQ_INSERT2 0

#define MAX_CPUS 80

#define PRINT_LATENCIES 0
#define PRINT_BLOOM_LATENCIES 0
#define PRINT_FALSE_POSITIVES 0


#include "TART_old.hh"

#include <thread>
#include <sched.h>
#include <map>
#include <mutex>
#include <sstream>

// if we use a #define, the performance is different: the i%_NKEYS will be calculated on compile time and it is an expensive operation.
// for now we use a variable number of keys (passed through command line)
//uint64_t n_keys = 10000000;

// that's the internal loadKey used in ART
void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    // Extract the tid from the record! This is a record *!
    TID actual_tid = TART<long>::getTIDFromRec(tid);
	key.setKeyLen(sizeof(actual_tid));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(actual_tid);
}

// that's the loadKey used to generate the keys. They don't know about rec* to TID conversion,
// so that's like the default loadKey
void loadKeyInit(TID tid, Key &key){
	key.setKeyLen(sizeof(tid));
	reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

void singlethreaded(uint64_t n, uint64_t operations_n, unsigned ops_per_txn, unsigned key_type, unsigned insert_ratio) {
    std::cout << "single threaded:" << std::endl;
    insert_ratio=insert_ratio;
    uint64_t *keys = new uint64_t[n];
	uint64_t r_w_keys_n = 1000000; // We set the number of the r/w part as 1/10th of the compacted part (1M).

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (key_type == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (key_type == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

    printf("operation,n,ops/us\n");
    TART<long> tree_rw(loadKey);
	#if !SINGLE_TREE
		TART<long> tree_compacted(loadKey);
	#endif
    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        auto t = tree_rw.getThreadInfo();
		uint64_t txn_cnt=0;
        for (txn_cnt=0; txn_cnt!= r_w_keys_n / ops_per_txn; txn_cnt++){
			TRANSACTION {
			for(uint64_t j=0; j<ops_per_txn; j++){
				Key key;
				unsigned ind = txn_cnt*ops_per_txn + j;
				loadKeyInit(keys[ind]+n, key);
				tree_rw.t_insert(key, keys[ind]+n, t);
				#if USE_BLOOM
					uint64_t target_key = keys[ind]+n;
                    //cout << "Bloom inserting " << keys[i+j]+n << endl;
					bloom_insert(&target_key, sizeof(uint64_t));
				#endif
			}
            }RETRY(false);
		}
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        //printf("insert R/W,%ld,%f\n", r_w_keys_n, (r_w_keys_n * 1.0) / duration.count());
        printf("insert R/W txn,%ld,%ld,%f\n", r_w_keys_n, txn_cnt, (txn_cnt * 1.0) / duration.count());
        starttime = std::chrono::system_clock::now();
		#if !SINGLE_TREE
			for (uint64_t i = 0; i != n / ops_per_txn; i++) {
				TRANSACTION{
				for (uint64_t j=0; j< ops_per_txn; j++){
            		Key key;
					unsigned ind = i*ops_per_txn + j;
					loadKeyInit(keys[ind], key);
					tree_compacted.t_insert(key, keys[ind], t);
				}
                }RETRY(false);
        	}
		#endif
		duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("insert compacted,%ld,%f\n", n, (n * 1.0) / duration.count());
    }
    {
        // Lookup
        auto t1 = tree_rw.getThreadInfo();
		#if !SINGLE_TREE
		auto t2 = tree_compacted.getThreadInfo();
        #endif
		auto starttime = std::chrono::system_clock::now();
        uint64_t txn_cnt = 0;
        for (txn_cnt = 0; txn_cnt != operations_n/ops_per_txn; txn_cnt++) {
			TRANSACTION{
			for (uint64_t j=0; j<ops_per_txn; j++){
				Key key;
            	uint64_t key_ind=0;
				unsigned ind = txn_cnt*ops_per_txn + j;
        		#if SINGLE_TREE
					key_ind=ind%n;
					loadKeyInit(keys[key_ind], key);
					auto val1 = std::get<0>(tree_rw.t_lookup(key, t1));
					if(val1 != keys[key_ind]){
						std::cout << "wrong key read: " << val1 << " expected: " << keys[key_ind] << std::endl;
						throw;
					}
				#else
					bool inRW = false;
					uint64_t key_dat;
					if(ind%HIT_RATIO_MOD == 0){ // get from RW
                        //key_ind = ind%r_w_keys_n;
                        key_ind = (ind/HIT_RATIO_MOD)%r_w_keys_n;
                        key_dat = keys[key_ind]+n;
                        inRW = true;
					}
					else{
					    key_ind = ind%n;
                        key_dat = keys[key_ind];
                    }
					loadKeyInit(key_dat, key);
					#if USE_BLOOM
    					TID val;
						INIT_COUNTING
						START_COUNTING
                        uint64_t* hashVal;
						bool contains = bloom_contains(&key_dat, sizeof(uint64_t), &hashVal);
						STOP_COUNTING_PRINT("Bloom lookup")
						if(contains){
							START_COUNTING
        					val = std::get<0>(tree_rw.t_lookup(key, t1));
							STOP_COUNTING_PRINT("R/W lookup")
        					if (val == 0){ // not found in R/W! False positive!
								#if PRINT_FALSE_POSITIVES
								stringstream ss;
        						ss <<"Key " << key_dat << " not found in R/W! False positive!" <<endl;
            					cout << ss.str();
								#endif
								assert(!inRW);
								START_COUNTING
            					val = std::get<0>(tree_compacted.t_lookup(key, t2, false));
								STOP_COUNTING_PRINT("compacted lookup")
            					if (val != key_dat){
                					std::cout << "wrong key read: " << val << " expected: " << key_dat << std::endl;
                					throw;
            					}
        					}
    					}
    					else {
        					assert(!inRW);
							START_COUNTING
        					val = std::get<0>(tree_compacted.t_lookup(key, t2, false));
							STOP_COUNTING_PRINT("compacted lookup")
        					if (val != key_dat){
            					std::cout << "wrong key read: " << val << " expected: " << key_dat << std::endl;
            					throw;
        					}
    					}
					#else
                        INIT_COUNTING
						START_COUNTING
    					auto val = std::get<0>(tree_rw.t_lookup(key, t1));
						STOP_COUNTING_PRINT("R/W lookup")
						if (val == 0){ // not found in R/W! Look in Compacted.
							START_COUNTING
        					val = std::get<0>(tree_compacted.t_lookup(key, t2, false));
							STOP_COUNTING_PRINT("compacted lookup")
        					if (val != key_dat){
            					std::cout << "wrong key read: " << val << " expected: " << key_dat << std::endl;
            					throw;
        					}
    					}
					#endif
				#endif
			}
            }RETRY(false);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        //printf("single lookup,%ld,%f\n", operations_n, (operations_n * 1.0) / duration.count());
        printf("single lookup txn,%ld,%ld,%f\n", operations_n, txn_cnt, (txn_cnt * 1.0) / duration.count());
    }

    {
		auto t1 = tree_rw.getThreadInfo();
		#if !SINGLE_TREE
			auto t2 = tree_compacted.getThreadInfo();
		#endif

		for (uint64_t i = 0; i != r_w_keys_n / ops_per_txn; i++) {
            TRANSACTION{
            for (uint64_t j = 0; j<ops_per_txn; j++){
                unsigned ind = i*ops_per_txn + j;
                Key key;
                loadKeyInit(keys[ind]+n, key);
                tree_rw.t_remove(key, keys[ind]+n, t1);
            }
            }RETRY(false);
		}

		#if !SINGLE_TREE
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i = 0; i != n / ops_per_txn; i++) {
			TRANSACTION{
			for (uint64_t j = 0; j<ops_per_txn; j++){
				unsigned ind = i*ops_per_txn + j;
            	Key key;
            	loadKeyInit(keys[ind], key);
				tree_compacted.t_remove(key, keys[ind], t2);
			}
            }RETRY(false);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
    	#endif
	}
    delete[] keys;
    std::cout << std::endl;
}


void set_affinity(std::thread& t, unsigned i){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
	int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
	if (rc != 0) {
		std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
	}
}

void multithreaded(uint64_t n, uint64_t operations_n, unsigned ops_per_txn, unsigned key_type, unsigned insert_ratio) {
    std::cout << "multi threaded:" << std::endl;
	tbb::task_scheduler_init init(20);

	uint64_t *keys = new uint64_t[n];
	uint64_t r_w_keys_n = 1000000; // We set the number of the r/w part as 1/10th of the compacted part (1M).

    // Generate keys
    for (uint64_t i = 0; i < n; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (key_type == 1)
        // dense, random
        std::random_shuffle(keys, keys + n);
    if (key_type == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

    printf("operation,n,ops/ms\n");
    TART<long> tree_rw(loadKey);
	#if !SINGLE_TREE
		TART<long> tree_compacted(loadKey);
	#endif

    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        auto t1 = tree_rw.getThreadInfo();
		#if !SINGLE_TREE
		auto t2 = tree_compacted.getThreadInfo();
		#endif
		#if SEQ_INSERT1
		for (uint64_t i = 0; i<r_w_keys_n / ops_per_txn; i++){
			TRANSACTION{
			for (uint64_t j = 0; j<ops_per_txn; j++) {
				uint64_t ind = i*ops_per_txn + j;
				Key key;
				loadKeyInit(keys[ind]+n, key);
				tree_rw.t_insert(key, keys[ind]+n, t1);
			}
            }RETRY(false);
		}
		#else
        auto total_txns = tbb::parallel_reduce(tbb::blocked_range<uint64_t>(0, r_w_keys_n), 0, [&](const tbb::blocked_range<uint64_t> &range, uint64_t txn_cnt=0) {
            auto t1 = tree_rw.getThreadInfo();
            uint64_t i = range.begin();
            while (i < range.end()){
                uint64_t cur_op=0;
                TRANSACTION{
                for (cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
                    Key key;
                    loadKeyInit(keys[i]+n, key);
                    tree_rw.t_insert(key, keys[i]+n, t1);
                    #if USE_BLOOM
                    uint64_t target_key = keys[i]+n;
                    bloom_insert(&target_key, sizeof(uint64_t));
                    #endif
                }
                }RETRY(false);
                txn_cnt++;
            }
            return txn_cnt;
        }, std::plus<uint64_t>(), tbb::static_partitioner());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
        printf("insert R/W txn,%ld,%d,%f\n", r_w_keys_n, total_txns, (total_txns * 1.0) / duration.count());
		#endif
		// if I insert in the second tree as well, it becomes faster on smaller trees: NUMA effect! Use numactl to use CPUs from the same socket!
		// even with a single socket we see a difference when inserting the keys of tree_compacted sequentially vs in parallel.
		// When we insert sequentially, a single L1 cache is 'pollutted' with 64KB (1,000 keys) tree_compacted entries since lookups access only tree_rw.
		// When we insert in parallel, all 10 L1 caches will be 'pollutted', but less (6.4KB on average). This leaves more tree_rw entries in their 
		// caches and makes it faster.
		#if !SINGLE_TREE
			#if SEQ_INSERT2
			auto t2 = tree_compacted.getThreadInfo();
				for (uint64_t i = 0; i<n/ops_per_txn; i++){
					TRANSACTION{
						for(uint64_t j = 0; j<ops_per_txn; j++){
							uint64_t ind = i*ops_per_txn + j;
							Key key;
							loadKeyInit(keys[ind], key);
							tree_compacted.t_insert(key, keys[ind], t2);
						}
					}RETRY(false);
				}
			#else
            starttime = std::chrono::system_clock::now();
			tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
				//int cpu = sched_getcpu();
				//printf("Inserting. Current CPU: %d\n", cpu);
				auto t2 = tree_compacted.getThreadInfo();
				uint64_t i = range.begin();
            	while (i < range.end()){
                	TRANSACTION {
                	for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
                    	Key key;
                    	loadKeyInit(keys[i], key);
                    	tree_compacted.t_insert(key, keys[i], t2);
                	}   
                	}RETRY(false);
            	}   
			});
            duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
            printf("insert compacted,%ld,%f\n", n, (n * 1.0) / duration.count());

			#endif
		#endif
    }
    {
		bool lookups_only = insert_ratio == 0;
		unsigned insert_ratio_mod = lookups_only? 0 :  (100 / insert_ratio);
		// Lookup
        std::mutex mtx;
		//tbb::affinity_partitioner ap;
		INIT_COUNTING
		auto starttime = std::chrono::system_clock::now();
		auto total_txns = tbb::parallel_reduce(tbb::blocked_range<uint64_t>(0, operations_n), 0, [&](const tbb::blocked_range<uint64_t> &range, uint64_t txn_cnt=0) {
			//int cpu = sched_getcpu();
			//printf("Current CPU: %d\n", cpu);
			//unsigned num_cpus = std::thread::hardware_concurrency();
  			//std::cout << "Launching " << num_cpus << " threads\n";
			auto t1 = tree_rw.getThreadInfo();
			#if !SINGLE_TREE
				auto t2 = tree_compacted.getThreadInfo();
			#endif
			uint64_t i = range.begin();
            while (i < range.end()){
            	TRANSACTION {
                for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
	            	if (!lookups_only && i % insert_ratio_mod == 0) { // insert
						Key key;
						// insert a key that does not exist
                    	loadKeyInit(keys[i]+2*n, key);
                    	tree_rw.t_insert(key, keys[i]+2*n, t1);
                    	#if USE_BLOOM
                        uint64_t target_key = keys[i]+2*n;
                    	bloom_insert(&target_key, sizeof(uint64_t));
                    	#endif
					}
					else { // lookup
						Key key;
						uint64_t ind = 0;
						#if SINGLE_TREE
							ind=i%n;
							loadKeyInit(keys[ind], key);
							auto val1 = std::get<0>(tree_rw.t_lookup(key, t1));
                			if(val1 != keys[ind]){
                    			std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
                    			throw;
                			}
	            		#else
							bool inRW = false;
							uint64_t key_dat;
							START_COUNTING
							if(i%HIT_RATIO_MOD == 0){ // get from RW
                                //ind = i%r_w_keys_n;
                                ind = (i/HIT_RATIO_MOD)%r_w_keys_n;
                                key_dat = keys[ind]+n;
                                inRW = true;
	                		}
    	            		else{ // get from compacted
							    ind = i%n;
                                key_dat = keys[ind];
                            }
							STOP_COUNTING_PRINT("key index mod")
							loadKeyInit(key_dat, key);
							#if USE_BLOOM
                                bool contains = false;
								TID val;
                                #if VALIDATE
                                    uint64_t * hashVal;
								    contains = bloom_contains(&key_dat, sizeof(uint64_t), &hashVal);
                                #else
                                    contains = bloom_contains(&key_dat, sizeof(uint64_t), nullptr);
                                #endif
								if(contains){
									START_COUNTING
									val = std::get<0>(tree_rw.t_lookup(key, t1));
									STOP_COUNTING_PRINT("R/W lookup")
									if (val == 0){ // not found in R/W! False positive!
                                        #if PRINT_FALSE_POSITIVES
                                        stringstream ss; 
                                        ss <<"Key " << key_dat << " not found in R/W! False positive!" <<endl;
                                        cout << ss.str();
                                        #endif
										assert(!inRW);
										START_COUNTING
										val = std::get<0>(tree_compacted.t_lookup(key, t1, false));
										STOP_COUNTING_PRINT("compacted lookup")
										if (val != key_dat){
											stringstream ss;
											ss << "wrong key read from R/W: " << val << " expected: " << key_dat << std::endl;
											cout << ss.str();
											throw;
										}
									}
								}
								else {
                                    #if VALIDATE
                                        tree_rw.bloom_v_add_key(hashVal);
                                    #endif
									assert(!inRW);
									START_COUNTING
									val = std::get<0>(tree_compacted.t_lookup(key, t1, false));
									STOP_COUNTING_PRINT("compacted lookup")
									if (val != key_dat){
										stringstream ss;
										ss << "wrong key read from compacted: " << val << " expected: " << key_dat << std::endl;
										cout << ss.str();
										throw;
									}
								}
							#else
								START_COUNTING
								auto val = std::get<0>(tree_rw.t_lookup(key, t1));
								STOP_COUNTING_PRINT("R/W lookup")
								if (val == 0){ // not found in R/W! Look in Compacted.
									START_COUNTING
									val = std::get<0>(tree_compacted.t_lookup(key, t1, false));
									STOP_COUNTING_PRINT("compacted lookup")
									if (val != key_dat){
										stringstream ss;
										ss << "wrong key read from compacted: " << val << " expected: " << key_dat << std::endl;
										cout << ss.str();
										throw;
									}
								}
								if (val != key_dat){
									stringstream ss;
									ss << "wrong key read from R/W: " << val << " expected: " << key_dat << std::endl;
									cout << ss.str();
									throw;
								}
							#endif
						#endif
					}
				}
			}RETRY(true);
            txn_cnt++;
		}
        return txn_cnt;
		}, std::plus<uint64_t>(), tbb::static_partitioner());
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
		//printf("multi lookup,%ld,%f\n", operations_n, (operations_n * 1.0) / duration.count());
		printf("multi lookup txn,%ld,%d,%f\n", operations_n, total_txns, (total_txns * 1.0) / duration.count());
    }
    {
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, r_w_keys_n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t1 = tree_rw.getThreadInfo();
            uint64_t i = range.begin();
            while (i < range.end()){
            	TRANSACTION {
            	for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
                	Key key;
					loadKeyInit(keys[i]+n, key);
                	// TODO: Fix bug with RCU! TRcu.cc:49-> current_->next_ is nullptr.
					tree_rw.t_remove(key, keys[i]+n, t1);
				}
				}RETRY(false);
			}
        });
		#if !SINGLE_TREE
		auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t2 = tree_compacted.getThreadInfo();
            uint64_t i = range.begin();
            while (i < range.end()){
                TRANSACTION {
                for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
                    Key key;
                    loadKeyInit(keys[i], key);
                    //tree_compacted.t_remove(key, keys[i], t2);
                }
                }RETRY(false);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
		#endif
    }
    delete[] keys;
}

int main(int argc, char **argv) {
	#if USE_BLOOM > 0
    memset(bloom, 0, BLOOM_SIZE * sizeof(uint64_t));
    #endif
	int c;
	extern char *optarg;
    extern int optopt;
	// extern int optind;
	uint64_t n_keys=0, n_operations=0;
	unsigned ops_per_txn=0, key_type=0;
	unsigned insert_ratio = 0;
    bool mthreaded = false;

	while ((c = getopt(argc, argv, ":n:o:x:k:i:t:m")) != -1) {
		switch(c){
			case 'n':
				n_keys = std::atoll(optarg);
				break;
			case 'o':
				n_operations = std::atoll(optarg);
				break;
			case 'x':
				ops_per_txn = std::stoul(optarg);
				break;
			case 'k':
				key_type = std::stoul(optarg);
				break;
			case 'i':
				insert_ratio = std::stoul(optarg);
				break;
            case 'm':
                mthreaded = true;
                break;
			case ':':
				fprintf(stderr, "option %c requires an argument!\n", optopt);
				exit(-1);
			case '?':
				fprintf(stderr, "unrecognized option: %c\n", optopt);
				exit(-1);
		};
	}


	if (n_keys == 0 || n_operations == 0 || ops_per_txn == 0 || insert_ratio > 100) {
        printf("usage: %s -n <n_keys> [-o <n_operations>] -x <ops per txn> -k 0|1|2 [-i <0-100>] \nn: number of keys\nn_operations: number of operations\nops per txn: number of operations per transaction\n0: sorted keys\n1: dense keys\n2: sparse keys\n-i X: mixed workload with X percent inserts (X: [0,100]) -m: multithreaded\n", argv[0]);
        return 1;
    }
    if(!mthreaded)
        singlethreaded(n_keys, n_operations, ops_per_txn, key_type, insert_ratio);
    else 
        multithreaded(n_keys, n_operations, ops_per_txn, key_type, insert_ratio);

    return 0;
}
