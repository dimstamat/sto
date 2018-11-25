#pragma once


#include "TART.hh"
#include "OptimisticLockCoupling/Tree.h"

class HybridART{
protected:
    TART<uint64_t> tart_rw;
    ART_OLC::Tree tree_compacted;
    void* bloom;
    BloomPacking * bloomPacking;
    BloomNoPacking * bloomNoPacking;
    bloom_t bloom_type;
public:
    HybridART(Tree::LoadKeyFunction ARTloadKeyFun, Tree::LoadKeyFunction TARTloadKeyFun, bloom_t bloom_type): tart_rw(TARTloadKeyFun, bloom_type), tree_compacted(ARTloadKeyFun), bloom_type(bloom_type)
    {
        switch(bloom_type){
            case packing:
                bloomPacking = new BloomPacking();
                bloom = bloomPacking;
                break;
            case nopacking:
                bloomNoPacking = new BloomNoPacking();
                bloom = bloomNoPacking;
                break;
            case nobloom:
                bloom = nullptr;
                break;
        };
        tart_rw.setBloom(bloom);
    }

    ~HybridART(){
        switch(bloom_type){
            case packing:
                delete bloomPacking;
                break;
            case nopacking:
                delete bloomNoPacking;
                break;
            case nobloom:
                break;
        };
    }

    lookup_res lookup(const Key& k, ThreadInfo& threadEpocheInfo);

    ins_res insert(const Key & k, TID tid, ThreadInfo &epocheInfo);

    rem_res remove(const Key & k, TID tid, ThreadInfo &threadEpocheInfo);
};




