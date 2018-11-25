#include "HybridART.hh"

#define NUM_KEYS_MAX 20000000 // 20M keys max

char * key_dat [NUM_KEYS_MAX];

void loadKey(TID tid, Key &key){
    key.set(key_dat[tid-1], strlen(key_dat[tid-1]));
}

void loadKeyTART(TID tid, Key &key){
    TID actual_tid = TART<uint64_t>::getTIDFromRec(tid);
    key.set(key_dat[actual_tid-1], strlen(key_dat[actual_tid-1]));
}


int main(){

    bloom_t type = packing;

    HybridART hART(loadKey, loadKeyTART, type);

}
