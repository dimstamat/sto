if [ "$1" == "" ] || [ "$2" == "" ]
then
    echo "Usage: " $0 "<bloom type>" "<hit ratio mod>"
    exit
fi


sed -i -r -e "s/#define USE_BLOOM [0-2]+/#define USE_BLOOM $1/" HybridART.hh
sed -i -r -e "s/#define USE_BLOOM [0-2]+/#define USE_BLOOM $1/" HybridART.hh
sed -i -r -e "s/#define HIT_RATIO_MOD [0-9]+[0-9]*/#define HIT_RATIO_MOD $2/" test_bloom_txn.cc
sed -i -r -e "s/#define HIT_RATIO_MOD [0-9]+[0-9]*/#define HIT_RATIO_MOD $2/" test_meme.cc

