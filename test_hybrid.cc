 const int nthreads = 20;

#include "HybridART.hh"

#define NUM_KEYS_MAX 20000000 // 20M keys max

#define BLOOM_TYPE 2

char * key_dat [NUM_KEYS_MAX];

void loadKey(TID tid, Key &key){
    key.set(key_dat[tid-1], strlen(key_dat[tid-1]));
}

void loadKeyTART(TID tid, Key &key){
    // It doesn't matter what template arguments we pass.
    TID actual_tid = TART<uint64_t, DoubleLookup>::getTIDFromRec(tid);
    key.set(key_dat[actual_tid-1], strlen(key_dat[actual_tid-1]));
}


int main(){

    #if BLOOM_TYPE == 0
    HybridART<uint64_t, DoubleLookup> hART(loadKey, loadKeyTART);
    #elif BLOOM_TYPE == 1
    HybridART<uint64_t, BloomNoPacking> hART(loadKey, loadKeyTART);
    #elif BLOOM_TYPE == 2
    HybridART<uint64_t, BloomPacking> hART(loadKey, loadKeyTART);
    #endif

}
