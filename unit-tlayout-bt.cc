#undef NDEBUG
#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <thread>
#include <unistd.h>

//#include "Transaction.hh"
#include "TLayoutBT.hh"
//#include "StringWrapper.hh"

#include "rwlock.hh"


#define GUARDED if (TransactionGuard tguard{})

using namespace std;

rwlock lock;

void thread_f (int i){
	if (i==9){
		lock.write_lock();
		usleep(200000);
		lock.write_unlock();
	}
	else {
		lock.read_lock();
		usleep(200000);
		lock.read_unlock();
	}
	
}

int main() {
	TLayoutBT<unsigned> tree;
	dptrtype* dirtyP = tree.llock_.getDirtyP();

	tree.insert(123, dirtyP);
	cout<< tree.size(tree.head) << endl;

	std::vector<std::thread> threads;

	for (int i=0; i<10; i++){
		threads.push_back(std::thread(thread_f, i));
	}

	for (int i=0; i<10; i++){
		threads[i].join();
	}

	return 0;
}
