/*
 * tree1.hpp
 *
 *  Created on: Jan 18, 2015
 *      Author: nachshonc
 */

#pragma once
#include "Tree.hh"
#include "NoLockTree.hh"
#include "common/locks.hh"
#include <string>
#include <assert.h>
#include <iostream>
#include <atomic>
#include <algorithm>
#include <set>
#include <vector>
using namespace std;
using std::set;
//#define unlikely(x) __builtin_expect(!!(x), 0)
#define INVALID_KEY_SMALL 0
#define INVALID_KEY_LARGE (~0u)

#ifndef SYNC
#define SYNC(S) S
#endif

class GlobalLockTree{
public:
	tatas_lock_t lock;
	unsigned key_;
	void *data;
	NoLockHelper left, right;
	void acquire(){
		SYNC(tatas_acquire(&lock);)
	}
	void release(){
		SYNC(tatas_release(&lock);)
	}
	bool search(unsigned key){
		bool res=false;
		if(key==key_)
			res=true;
		else if(key<key_)
			res=left.search(key);
		else
			res=right.search(key);
		release();
		return res;
	}
	bool insert(unsigned key){
		bool res = true;
		if(key_==INVALID_KEY_SMALL)
			key_=key;
		else if(key_==key)
			res=false;	
		else if(key<key_){
			res=left.insert(key);
		}
		else{
			res=right.insert(key);
		}
		release();
		return res;
	}
	bool remove(unsigned key){
		bool res = true;
		if(key<key_)
			res=left.remove(key);
		else if(key>key_)
			res=right.remove(key);
		else{
			NoLockHelper::node *t=right.removeMin(right.head, &right.head);
			if(t!=NULL){
				key_=t->key;
				data=t->obj;
				delete t;
			}
			else{
				if(left.head==NULL) {key_=INVALID_KEY_SMALL;}//empty tree.}
				else{
					key_=left.head->key;
					data=left.head->obj;
					right.head=left.head->right;
					left.head=left.head->left;
				}
			}
		}
		release();
		return res;
	}
	int addItems(std::vector<unsigned> &v){
		if(key_==INVALID_KEY_SMALL) return 0;
		int l = left.addItems(left.head, v);
		v.push_back(key_);
		int r = right.addItems(right.head, v);
		return l+r+1;
	}
	void destroy(){
		left.destroy(left.head);
		right.destroy(right.head);
		left.head=NULL;
		right.head=NULL;
		//we do NOT free ourselves (delete this) because that would create a race on the lock.
	}
	bool isEmpty(){
		return key_==INVALID_KEY_SMALL;
	}
	int size(){
		if(key_==INVALID_KEY_SMALL) return 0;
		return left.size(left.head)+1+right.size(right.head);
	}
	GlobalLockTree():lock(UNLOCKED),key_(INVALID_KEY_SMALL), data(NULL), left(0), right(0){}
	//assume that [begin,end) is already sorted.
	GlobalLockTree(std::vector<unsigned>::iterator begin, std::vector<unsigned>::iterator end):lock(UNLOCKED)
		,key_( ((end-begin)==0)?INVALID_KEY_SMALL:*((begin+(end-begin)/2))),
		data(NULL), left(begin, begin+(end-begin)/2), right(begin+(end-begin)/2+1, end){}
	void print(){
		printf("[");
		left.print(left.head);
		if(key_!=INVALID_KEY_SMALL)
			printf("%d,", key_);
		else printf("EMPTY");
		right.print(right.head);
		printf("\b ]\n");
	}
	//do not put virtual function because that put lock at a different location than type
	//virtual std::string name(){return "GlobalLock"; }
	//virtual ~GlobalLockTree(){}
};

