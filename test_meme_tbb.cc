#include <iostream>
#include <chrono>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <getopt.h>

#include "tbb/tbb.h"

#include "tbb/enumerable_thread_specific.h"

using namespace std;

#include "ARTSynchronized/OptimisticLockCoupling/Tree.h"

#define GUARDED if (TransactionGuard tguard{})

#define NUM_KEYS_MAX 20000000 // 20M keys max


#define HIT_RATIO_MOD 2

#include "TART_old.hh"

#define PRINT_FALSE_POSITIVES 0

char * key_dat [NUM_KEYS_MAX];


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

inline void do_insert(uint64_t i, Tree& tree, TART<uint64_t>& tart, ThreadInfo& tinfo, bool txn, bool 
#if USE_BLOOM > 0
    b_insert 
#endif 
){
	Key key;
    loadKeyInit(i, key);
	INIT_COUNTING
	START_COUNTING
	if(txn)
		tart.t_insert(key, i, tinfo);
	else
    	tree.insert(key, i, tinfo);
	STOP_COUNTING_PRINT("R/W insert")
    #if USE_BLOOM > 0
		if(b_insert){
			START_COUNTING
			bloom_insert(key.getKey(), key.getKeyLen());
			STOP_COUNTING_PRINT("bloom insert")
		}
    #endif
}

inline void do_lookup(uint64_t i, Tree& tree_rw, Tree& tree_compacted, TART<uint64_t>& tart_rw, TART<uint64_t>& tart_compacted, ThreadInfo& t1, ThreadInfo& t2, uint64_t &num_keys, uint64_t &r_w_size, bool txn){
	Key key;
    uint64_t key_ind = 0;
    bool inRW = false;
    if(i % HIT_RATIO_MOD == 0) { // read from R/W
		//key_ind = (i-1) % r_w_size + 1;
		//stringstream ss;
        key_ind = ((i / HIT_RATIO_MOD)-1 )% r_w_size + 1;
		inRW = true;
        //ss<<"RW: "<<key_ind<<endl;
        //cout<<ss.str();
    }
    else { // read from compacted
		key_ind = (i-1) % (num_keys - r_w_size) + r_w_size + 1;
        //stringstream ss;
        //ss<<"RO: "<<key_ind<<endl;
        //cout<<ss.str();
    }
    loadKeyInit(key_ind, key);
	#if USE_BLOOM > 0
		bool contains = false;
		INIT_COUNTING
		START_COUNTING
        #if VALIDATE
            uint64_t* hashVal;
		    contains = bloom_contains(key.getKey(), key.getKeyLen(), &hashVal);
        #else
            contains = bloom_contains(key.getKey(), key.getKeyLen(), nullptr);
        #endif
		//STOP_COUNTING_PRINT("bloom contains")
		if(contains){
			STOP_COUNTING_PRINT("bloom contains")
			//cout<<"bloom contains!\n";
        	START_COUNTING
			TID val = (txn? std::get<0>(tart_rw.t_lookup(key, t1)) : tree_rw.lookup(key, t1));
            if(val == 0){ // not found in R/W! False positive
				STOP_COUNTING_PRINT("R/W lookup not found")
                #if PRINT_FALSE_POSITIVES
                cout <<"False positive!\n";
				#endif
                START_COUNTING
                TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)) : tree_compacted.lookup(key, t2));
                STOP_COUNTING_PRINT("compacted lookup")
				//printf("Checking after reading from compacted!\n");
                checkVal(val, key_ind);
			}
            else {
                STOP_COUNTING_PRINT("R/W lookup found")
				//printf("Checking after reading from R/W!\n");
            	checkVal(val, key_ind);
            }
		}
        else { // add key in bloom filter validation! Just the hash of the key is enough.
            STOP_COUNTING_PRINT("bloom doesn't contain")
            #if VALIDATE
                tart_rw.bloom_v_add_key(hashVal);
            #endif
            assert(!inRW);
            START_COUNTING
            TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)): tree_compacted.lookup(key, t2));
            STOP_COUNTING_PRINT("compacted lookup")
            checkVal(val, key_ind);
        }
	#else
		INIT_COUNTING
		START_COUNTING
        TID val = (txn ? std::get<0>(tart_rw.t_lookup(key, t1)): tree_rw.lookup(key, t1));
        if(val == 0){
            STOP_COUNTING_PRINT("R/W lookup not found")
        	assert(!inRW);
            START_COUNTING
            TID val = (txn? std::get<0>(tart_compacted.t_lookup(key, t2, false)) : tree_compacted.lookup(key, t2));
            STOP_COUNTING_PRINT("compacted lookup")
            //printf("Checking after reading from compacted!\n");
			checkVal(val, key_ind);
        }
        else {
            STOP_COUNTING_PRINT("R/W lookup found")
			//printf("Checking after reading from R/W!\n");
        	checkVal(val, key_ind);
        }
	#endif
}


void run_bench(uint64_t num_keys, uint64_t r_w_size, unsigned insert_ratio, unsigned ops_per_txn, bool multithreaded){
    ART_OLC::Tree tree_rw(loadKey);
    ART_OLC::Tree tree_compacted(loadKey);
	TART<uint64_t> tart_rw(loadKeyTART);
	TART<uint64_t> tart_compacted(loadKeyTART);
	r_w_size = r_w_size > num_keys ? num_keys : r_w_size;
	bool transactional = ops_per_txn > 0;
    // start 19 worker threads (20 total cores in a NUMA node)
    unsigned nthreads = 20;
	tbb::task_scheduler_init init(nthreads);
    uint64_t total_txns=0;
    // Build tree
	{
		auto starttime = std::chrono::system_clock::now();
		if(multithreaded && ! transactional){
			tbb::parallel_for(tbb::blocked_range<uint64_t>(1, r_w_size+1), [&](const tbb::blocked_range<uint64_t> &range) {
                auto t1 = tree_rw.getThreadInfo();
				for(uint64_t i=range.begin(); i!= range.end(); i++) {
					do_insert(i, tree_rw, tart_rw, t1, false, true);
				}
			});
		}
		else if (multithreaded && transactional){
			total_txns = tbb::parallel_reduce(tbb::blocked_range<uint64_t>(1, r_w_size+1), 0, [&](const tbb::blocked_range<uint64_t> &range, uint64_t txn_cnt=0) {
				auto t1 = tart_rw.getThreadInfo();
				uint64_t i = range.begin();
			    unsigned id = sched_getcpu()/4;
                TThread::set_id(id);
                Sto::update_threadid();
                while (i < range.end()){
					TRANSACTION {
						for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
							do_insert(i, tree_rw, tart_rw, t1, true, true);
						}
					} RETRY(false);
                    txn_cnt++;
				}
                return txn_cnt;
			}, std::plus<uint64_t>(), tbb::static_partitioner());
		}
		else if (!multithreaded && !transactional){
			auto t1 = tree_rw.getThreadInfo();
			for(uint64_t i=1; i<=r_w_size; i++){
				do_insert(i, tree_rw, tart_rw, t1, false, true);
			}
		}
		else if (!multithreaded && transactional){
			unsigned ind=0;
			auto t1 = tart_rw.getThreadInfo();
			for (uint64_t i=1; i<= r_w_size / ops_per_txn; i++){
				GUARDED {
					for(uint64_t j=1; j<=ops_per_txn; j++){
						ind = (i-1)*ops_per_txn + j;
						do_insert(ind, tree_rw, tart_rw, t1, true, true);
					}
				}
                total_txns++;
			}
            GUARDED {
				uint64_t limit = r_w_size % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // insert the rest of the keys! (mod)
                    ind++;
                    do_insert(ind, tree_rw, tart_rw, t1, true, true);
                }
                if(limit>=1)
                    total_txns++;
            }

		}
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
        //printf("insert R/W,%ld,%f\n", r_w_size, (r_w_size * 1.0) / duration.count());
        printf("insert R/W txn,%ld,%lu,%f\n", r_w_size, total_txns, (total_txns * 1.0) / duration.count());
        starttime = std::chrono::system_clock::now();
        if(multithreaded && ! transactional){
            tbb::parallel_for(tbb::blocked_range<uint64_t>(r_w_size+1, num_keys+1), [&](const tbb::blocked_range<uint64_t> &range) {
                auto t2 = tree_compacted.getThreadInfo();
                for(uint64_t i=range.begin(); i!= range.end(); i++) {
                    do_insert(i, tree_compacted, tart_compacted, t2, false, false);
                }
            });
        }
        else if (multithreaded && transactional){
            tbb::parallel_for(tbb::blocked_range<uint64_t>(r_w_size+1, num_keys+1), [&](const tbb::blocked_range<uint64_t> &range) {
                auto t2 = tart_compacted.getThreadInfo();
                uint64_t i = range.begin();
                while (i < range.end()){
                    TRANSACTION {
                        for (uint64_t cur_op=0; cur_op<ops_per_txn && i < range.end(); cur_op++, i++){
                            do_insert(i, tree_compacted, tart_compacted, t2, true, false);
                        }
                    }RETRY(true);
				}
            });
        }
        else if (!multithreaded && !transactional){
			auto t2 = tree_compacted.getThreadInfo();
			for(uint64_t i=r_w_size+1; i<=num_keys; i++){
				do_insert(i, tree_compacted, tart_compacted, t2, false, false);
            }
        }
        else if (!multithreaded && transactional){
			auto t2 = tart_compacted.getThreadInfo();
			unsigned ind=0;
            for (uint64_t i=1; i<= (num_keys - r_w_size) / ops_per_txn; i++){
                GUARDED {
                    for(uint64_t j=1; j<=ops_per_txn; j++){
                        ind = r_w_size + (i-1)*ops_per_txn + j;
                        do_insert(ind, tree_compacted, tart_compacted, t2, true, false);
                    }
                }
			}
			uint64_t limit = (num_keys - r_w_size) % ops_per_txn;
			GUARDED {
				for(uint64_t j=1; j<=limit; j++) { // insert the rest of the keys! (mod)
					ind++;
					do_insert(ind, tree_compacted, tart_compacted, t2, true, false);
				}
			}
        }
		duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		printf("insert compacted,%ld,%f\n", num_keys - r_w_size+2, ((num_keys - r_w_size +2)* 1.0) / duration.count());
    }
	// Lookup
	{
		bool lookups_only = insert_ratio == 0;
		unsigned insert_ratio_mod = lookups_only? 0 :  (100 / insert_ratio);
        uint64_t num_ops = num_keys;
        //unsigned range_size = 0;
		std::pair<uint64_t, unsigned> tinfo;
        total_txns=0;
        auto starttime = std::chrono::system_clock::now();
        if(multithreaded && !transactional){
            tbb::parallel_for(tbb::blocked_range<uint64_t>(1, num_keys+1), [&](const tbb::blocked_range<uint64_t> &range) {
                auto t1 = tree_rw.getThreadInfo();
				auto t2 = tree_compacted.getThreadInfo();
                for(uint64_t i=range.begin(); i!= range.end(); i++) {
                	if(! lookups_only && ((i-1) % insert_ratio_mod == 0)){ // insert
                		// keys indexes for the mixed workload are after the key indexes of the initial inserts
						do_insert(i, tree_rw, tart_rw, t1, false, false);
					} else 
						do_lookup(i, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, r_w_size, false);
                }
            });
        }
        else if (multithreaded && transactional){
            if(!lookups_only){
                num_ops = 4 * num_keys;
                //range_size = num_ops / nthreads;
            }
            tinfo = tbb::parallel_reduce(tbb::blocked_range<uint64_t>(1, num_ops+1), tinfo, [&](const tbb::blocked_range<uint64_t> &range, std::pair<uint64_t,unsigned> tinfo) {
                auto tt1 = tart_rw.getThreadInfo();
				auto tt2 = tart_compacted.getThreadInfo();
                uint64_t i = range.begin();
                // We are using CPUs 0,4,8,... which are located in the same NUMA node
                unsigned id = sched_getcpu()/4;
                TThread::set_id(id);
                Sto::update_threadid();
                uint64_t txn_cnt=0;
                unsigned retries=0;
                while (i < range.end()){
                    uint64_t cur_op = 0, cur_i = i;
                    TRANSACTION {
                        if(!lookups_only)
                            cur_i = (i-1)%num_keys + 1;
                        retries++;
                        unsigned actual_ops_per_txn=0;
                        for (cur_op=0; cur_op<ops_per_txn && cur_i < range.end(); cur_op++, cur_i++){
                            /*if(! lookups_only && ((cur_i-1) % insert_ratio_mod == 0)) // insert
					            //do_insert(num_ops+cur_i, tree_rw, tart_rw, tt1, true, true); // insert non-existing key
                                do_insert( cur_i, tree_rw, tart_rw, tt1, true, false);         // update existing key
							else
							    //do_lookup(cur_i, tree_rw, tree_compacted, tart_rw, tart_compacted, tt1, tt2, num_ops, r_w_size, true);
							    do_lookup(cur_i, tree_rw, tree_compacted, tart_rw, tart_compacted, tt1, tt2, num_keys, r_w_size, true);
                            */
                            if(!lookups_only && cur_i%2==0){
                                do_insert(cur_i, tree_rw, tart_rw, tt1, true, false);
                            }
                            else{
                                do_lookup(cur_i, tree_rw, tree_compacted, tart_rw, tart_compacted, tt1, tt2, num_keys, r_w_size, true);
                            }
                            actual_ops_per_txn++;
                        }
                    }RETRY(true);
                    txn_cnt++;
                    retries--; //  since we always count the first one
                    i+=cur_op;
                }
                tinfo.first = txn_cnt;
                tinfo.second = retries;
                return tinfo;
            }, [](std::pair<uint64_t,unsigned> tinfo1, std::pair<uint64_t,unsigned> tinfo2)->std::pair<uint64_t,unsigned> {return std::make_pair(tinfo1.first+tinfo2.first, tinfo1.second+tinfo2.second);}, tbb::static_partitioner());
        }
        else if (!multithreaded && !transactional){
            auto t1 = tree_rw.getThreadInfo();
			auto t2 = tree_compacted.getThreadInfo();
            for(uint64_t i=1; i<=num_keys; i++){
				if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
                	do_insert(i, tree_rw, tart_rw, t1, false, false);
				else
					do_lookup(i, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, r_w_size, false);
            }
        }
        else if (!multithreaded && transactional){
			unsigned ind=0;
            auto t1 = tart_rw.getThreadInfo();
            auto t2 = tart_compacted.getThreadInfo();
            for (uint64_t i=1; i<= num_keys / ops_per_txn; i++){
                GUARDED {
                    for(uint64_t j=1; j<=ops_per_txn; j++){
                        ind = (i-1)*ops_per_txn + j;
                        if(! lookups_only && ((i-1) % insert_ratio_mod == 0)) // insert
					        //do_insert(num_keys+ind, tree_rw, tart_rw, t1, true, true);
					        // try to insert existing key
					        do_insert(ind, tree_rw, tart_rw, t1, true, false);
						else
						    do_lookup(ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, r_w_size, true);
                    }
                }
                total_txns++;
            }
            GUARDED {
                uint64_t limit = num_keys % ops_per_txn;
                for(uint64_t j=1; j<=limit; j++) { // lookup the rest of the keys! (mod)
                    ind++;
                    if (! lookups_only && ((j-1) % insert_ratio_mod == 0) ) // insert
                        do_insert(ind, tree_rw, tart_rw, t1, true, false);
                    else
                        do_lookup(ind, tree_rw, tree_compacted, tart_rw, tart_compacted, t1, t2, num_keys, r_w_size, true);
                }
                if(limit>=1)
                    total_txns++;
            }

        }
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::system_clock::now() - starttime);
		//printf("%s,%ld,%f\n", (lookups_only? "lookup" : "lookup/insert" ),  num_keys, (num_keys * 1.0) / duration.count());
        printf("%s,%ld,%ld,%f\n", (lookups_only? "lookup txn" : "lookup/insert txn" ),  num_ops, tinfo.first, (tinfo.first * 1.0) / duration.count());
        printf("aborts:%u\n", tinfo.second);
	}
	// Remove
	{ /*
		auto starttime = std::chrono::system_clock::now();
        #if MULTITHREADED
        tbb::parallel_for(tbb::blocked_range<uint64_t>(1, r_w_size+1), [&](const tbb::blocked_range<uint64_t> &range) {
        auto t1 = tree_rw.getThreadInfo();
        for(uint64_t i=range.begin(); i!= range.end(); i++) {
		#else
		for(uint64_t i=1; i<=r_w_size; i++){
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
		printf("remove R/W,%ld,%f\n", r_w_size, (r_w_size * 1.0) / duration.count());
        
		starttime = std::chrono::system_clock::now();
        #if MULTITHREADED
        tbb::parallel_for(tbb::blocked_range<uint64_t>(r_w_size+1, num_keys+1), [&](const tbb::blocked_range<uint64_t> &range) {
        auto t2 = tree_compacted.getThreadInfo();
        for(uint64_t i=range.begin(); i!= range.end(); i++) {
        #else
        for(uint64_t i=r_w_size+1; i<=num_keys; i++){
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
        printf("remove compacted,%ld,%f\n", num_keys-r_w_size+2, ((num_keys-r_w_size+2) * 1.0) / duration.count());
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
	uint64_t r_w_size=0;
	unsigned insert_ratio=0, ops_per_txn=0;

	struct option long_opt [] = 
	{
		{"file1", required_argument, NULL, 'f'},
		{"file2", optional_argument, NULL, 'g'},
		{"rw-size", required_argument, NULL, 'r'},
		{"insert-ratio", optional_argument, NULL, 'i'},
		{"ops-per-txn", optional_argument, NULL, 'x'},
		{"multithreaded", no_argument, NULL, 'm'}
	};

	#if USE_BLOOM > 0
	memset(bloom, 0, BLOOM_SIZE * sizeof(uint64_t));
    #endif

	while((c = getopt_long(argc, argv, ":f:gr:i", long_opt, NULL)) != -1){
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
				r_w_size = std::stoul(optarg);
				r_w_set = true;
				break;
			case 'i':
				insert_ratio = std::stoul(optarg);
				break;
			case 'x':
				ops_per_txn = std::stoul(optarg);
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
	TID keys_read = 1;
 
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
	TID keys2_read = 1;
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
    cout<<"keys read:" <<keys_read<<endl;
    cout<<"multithreaded: "<<multithreaded<<endl;
	run_bench(keys_read, r_w_size, insert_ratio, ops_per_txn, multithreaded);
    cout<<"Keys total (GB): "<< ((double)key_bytes_total) / 1024 / 1024 / 1024 <<endl;
	cleanup_keys(keys_read + keys2_read);
	#if USE_BLOOM > 0
    inspect_bloom();
    #endif
    return 0;
}
