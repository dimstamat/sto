#undef NDEBUG
#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include "Transaction.hh"
#include "TLayoutBT.hh"
#include "StringWrapper.hh"

#include "rwlock.hh"


#define GUARDED if (TransactionGuard tguard{})

int main() {
	TLayoutBT<int> tree;
    cout<< tree.size(tree.head) << endl;
	rwlock lock;

	lock.read_lock();	
	return 0;
}
