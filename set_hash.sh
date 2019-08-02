if [ "$1" == "" ]
then
    echo "Usage: " $0 "<value to set>"
    exit
fi


sed -i -r -e "s/#define HASH_METHOD [1-3]+/#define HASH_METHOD $1/"  /home/dimos/util/bloom_common.hh
