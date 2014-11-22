#pragma once

#include <stdint.h>
#include "TransItem.hh"

class Transaction;

class Shared {
public:
  virtual ~Shared() {}

  virtual bool check(TransItem& item, Transaction& t) = 0;

  virtual void lock(TransItem& item) = 0;
  virtual void unlock(TransItem& item) = 0;
  virtual void install(TransItem& item, uint64_t tid) = 0;

  // optional
  virtual void undo(TransItem& item) { (void)item; }
  virtual void afterC(TransItem& item) { (void)item; }
  // probably just needs to call destructor
  virtual void cleanup(TransItem& item) { (void)item; }
  
  virtual uint64_t getTid(TransItem& item) { return 0; }
};
