#include "../util/Zipfian_generator.hh"
#include <cstdlib>
#include <time.h>
#include <iostream>
#include <string.h>

using namespace std;


#define EVEN_BINS 1

#if EVEN_BINS == 1
#define TOTAL_ITEMS 10000000
#define TOTAL_INSTANCES 100000000
#define NUM_BINS 20
#else
#define TOTAL_ITEMS 14681620 // that's the total keys of the test_meme workload
#define TOTAL_INSTANCES 20000000
#define NUM_BINS 3
#define RW_SIZE 700000
#define RO_SIZE 6640810
#endif

long zipf_numbers[TOTAL_INSTANCES];
long frequencies[NUM_BINS];

int main(){
    long bin_size = TOTAL_ITEMS / NUM_BINS;
    memset(frequencies, 0, NUM_BINS*sizeof(long));
    ZipfianGenerator zipf(1, TOTAL_ITEMS, 0.99);
    srand(time(nullptr));
    for(uint64_t i=0; i<TOTAL_INSTANCES; i++){
        zipf_numbers[i] = zipf.nextLong(((double)rand()-1)/RAND_MAX);
        long bin_num;
        #if EVEN_BINS == 1
            bin_num = zipf_numbers[i] / bin_size;
        #else
            if(zipf_numbers[i] < RW_SIZE) bin_num=0;
            else if(zipf_numbers[i] < RO_SIZE) bin_num=1;
            else    bin_num=2;
        #endif
        frequencies[bin_num]++;
        //cout<<zipf.nextLong((((double)rand()-1))/RAND_MAX)<<endl;
    }


    for(unsigned i=0; i<NUM_BINS; i++){
        cout<<i<<": "<< frequencies[i]<<endl;
    }

    
}
