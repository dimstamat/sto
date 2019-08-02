#undef NDEBUG
#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <thread>
#include <unistd.h>

//#include "Transaction.hh"
#include "TART_old.hh"
//#include "StringWrapper.hh"


#define GUARDED if (TransactionGuard tguard{})

using namespace std;

const char key_dat [] [11] = { {3, 'A', 'R', 'T'},\
                             {2, 'A', 'B'},\
                             {2, 'A', 'C'},\
                             {4, 'A', 'B', 'B', 'A'},\
                             {3, 'S', 'T', 'O'},\
                             {9, 'S', 'T', 'O', 'H', 'A', 'S', 'T', 'I', 'C'},\
                             {5, 'S', 'T', 'O', 'C', 'K'},\
                             {5, 'S', 'T', 'E', 'A', 'K'},\
							 {8, 'S', 'T', 'O', 'L', 'I', 'D', 'I', 'A'},\
							 {5, 'S', 'T', 'O', 'M', 'A'},\
							 {5, 'A', 'R', 'T', 'O', 'S'}};

const char key2_dat [] [10]= {{8, 'A', 'R', 'T', 'I', 'S', 'T', 'I', 'C'},\
							  {4, 'S', 'T', 'O', 'A'},\
							  {4, 'C', 'O', 'C', 'O'},\
							  {4, 'A', 'M', 'A', 'N'},\
							  {4, 'A', 'U', 'G', 'O'},\
							  {7, 'A', 'L', 'A', 'B', 'A', 'M', 'A'},\
                              {1, 0},\
                              {1, 'Z'}};

void loadKeyTART(TID tid, Key &key) {
	// Store the key of the tuple into the key vector
    // Implementation is database specific
	// Extract the tid from the record! This is a record *!
	TID actual_tid = TART<long>::getTIDFromRec(tid);
	// that's for original ART
	//TID actual_tid = tid;
	key.set(key_dat[actual_tid-1]+1, (unsigned)key_dat[actual_tid-1][0]);
}


void loadKey(TID tid, Key &key){
    key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
}

void loadKey2(TID tid, Key &key){
    key.set(key2_dat[tid-1]+1, (unsigned)key2_dat[tid-1][0]);
}

inline string keyToStr(TID tid){
    std::string res = key_dat[tid-1]+1;
    return res;
}

using ins_res = std::tuple<bool,bool>;
using lookup_res = std::tuple<TID, bool>;

int main() {
	TART<long> tart(loadKeyTART);
	ART_OLC::Tree tree(loadKey);
	auto t = tart.getThreadInfo();
    auto tree_t = tree.getThreadInfo();
	Key key;
	{
		TestTransaction t1(1);
		// TIDs must start from 1, because checkKey in Tree.cpp returns TID zero when there is no match!
		for (TID tid=1; tid<=sizeof(key_dat) / sizeof(key_dat[0]); tid++){
        	key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
			tart.t_insert(key, tid, t);
            cout<<"Inserting "<<keyToStr(tid)<<"\t"<<tid<<endl;
            tree.insert(key, tid, tree_t);
        }
		assert(t1.try_commit());
	}
	{
        // remove
		/*TestTransaction t2(2);
		for (TID tid=1; tid<=sizeof(key_dat) / sizeof(key_dat[0]); tid++){
			key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
			tart.t_remove(key, tid, t);
		}*/
		/*TID tid = 2;
		key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
		printf("Removing key %s\n", key_dat[tid-1]+1);
		tart.remove(key, tid, t);
		key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
		printf("Looking up key %s\n", key_dat[tid-1]+1);
		TID l_tid = tart.lookup(key, t);
		if(l_tid == 0)
			printf("Key %s not found!\n", key_dat[tid-1]+1);
		*/
		//assert(t2.try_commit());
	}

	{
        /*
		TestTransaction t3(3);
		TID tid = 3;
		key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
		printf("Looking up key %s\n", key_dat[tid-1]+1);
        TID l_tid = std::get<0>(tart.t_lookup(key, t));
        if(l_tid == 0)
            printf("Key %s not found!\n", key_dat[tid-1]+1);
		assert(t3.try_commit());
	    */
    }

    TID tid = 3;
    key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);

    TID l_tid = tree.lookup(key, tree_t);
    printf("Lookup of key %s returned %lu\n", key_dat[tid-1]+1, l_tid);

    // lookup range
    Key key_start, key_end, key_cont;
    TID tid_start=7, tid_end=8;
    loadKey2(tid_start, key_start);
    loadKey2(tid_end, key_end);

    TID result[20];

    std::size_t resultsFound;

    TID res = tree.lookup(key_start, tree_t);
    cout<<"Lookup of " << key_dat[1]+1 << " returned "<<res<<endl;

    cout<<"Looking up keys ["<< keyToStr(tid_start) <<", "<< keyToStr(tid_end)<<")"<<endl;

    bool l_res = tree.lookupRange(key_start, key_end, key_cont, result, 20, resultsFound, tree_t);
    for(std::size_t i=0; i<resultsFound; i++){
        cout<<keyToStr(result[i])<<endl;
    }



	/*
 	// Proof of the bug of ART lookup():
	// if (level < k.getKeyLen() - 1 || optimisticPrefixMatch)
	// should be if (level < k.getKeyLen() || optimisticPrefixMatch)
	ART_OLC::Tree tree2(loadKey);
	auto t2 = tree2.getThreadInfo();
	TID tid = 4;
	key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
	tree2.insert(key, tid, t2);
	tid = 3;
	key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
	tree2.insert(key, tid, t2);
	tid = 2;
	key.set(key_dat[tid-1]+1, (unsigned)key_dat[tid-1][0]);
	printf("Looking up key %s\n", key_dat[tid-1]+1);
	TID l_tid = tree2.lookup(key, t2);
	if(l_tid == 0)
		printf("Key %s not found!\n", key_dat[tid-1]+1);
	*/
	{
		/*
		TestTransaction t2(2);
		key.set(key2_dat[4]+1, (unsigned)key2_dat[4][0]);
		auto res = tart.t_lookup(key, t);
		printf("Key found? %lu, early abort? %u\n", std::get<0>(res), !std::get<1>(res));
		key.set(key_dat[5]+1, (unsigned)key_dat[5][0]);
		res = tart.t_lookup(key, t);
		printf("Key found? %lu, early abort? %u\n", std::get<0>(res), !std::get<1>(res));
		TestTransaction t3(3);
		key.set(key2_dat[5]+1, (unsigned)key2_dat[5][0]);
		tart.t_insert(key, 5, t);
		assert(t2.try_commit());
		assert(t3.try_commit());
		*/
	}
	return 0;
}
