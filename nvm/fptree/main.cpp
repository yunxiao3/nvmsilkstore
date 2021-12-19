#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "fptree.h"
#include "fptree_map.h"
#include <vector>
using namespace std;

#define OP_NUM 10000

typedef std::string Key_Type; 
typedef std::string Value_Type; 



struct timeval start_time, end_time;
uint64_t time_interval;

char * start_addr;
char * curr_addr;

void print_statics_insert(uint64_t t = 0)
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    others_latency = time_interval * 1000 - sort_latency - unsort_latency - balance_latency - insert_latency;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_latency * 1.0 / OP_NUM, unsort_latency * 1.0 / OP_NUM, balance_latency * 1.0 / OP_NUM, 
            insert_latency * 1.0 / OP_NUM, others_latency * 1.0 / OP_NUM);
    others_flush = flush_sum - sort_flush - unsort_flush - balance_flush - insert_flush;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_flush * 1.0 / OP_NUM, unsort_flush * 1.0 / OP_NUM, balance_flush * 1.0 / OP_NUM, 
            insert_flush * 1.0 / OP_NUM, others_flush * 1.0 / OP_NUM);
    others_wear= wear_byte_sum - sort_wear- unsort_wear - balance_wear - insert_wear;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_wear * 1.0 / OP_NUM, unsort_wear * 1.0 / OP_NUM, balance_wear * 1.0 / OP_NUM, 
            insert_wear * 1.0 / OP_NUM, others_wear * 1.0 / OP_NUM);
    flush_sum = wear_byte_sum = 0; 
#endif  
#ifdef STATICS_MALLOC
    printf("scm_size=%lu dram_size=%lu\n", scm_size, dram_size);
#endif
#ifdef TREE_LIST_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", t - ln_latency * 1.0 / OP_NUM, ln_latency * 1.0 / OP_NUM);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", flush_sum * 1.0 / OP_NUM - ln_flush * 1.0 / OP_NUM, ln_flush * 1.0 / OP_NUM);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", wear_byte_sum * 1.0 / OP_NUM - ln_wear * 1.0 / OP_NUM, ln_wear * 1.0 / OP_NUM);
    flush_sum = wear_byte_sum = 0; 
#endif  
}

void print_statics_update()
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    flush_sum = wear_byte_sum = 0;
#endif
#ifdef STATICS_MALLOC
    printf("scm_size=%lu dram_size=%lu\n", scm_size, dram_size);
#endif    
}

void print_statics_delete(int i)
{
#ifdef STATICS_MALLOC
        if (i % ((int)(OP_NUM * 0.2)) == 0) {
            int num = i * 1.0 / OP_NUM * 100;
            printf("Delete %d: scm_size=%lu dram_size=%lu pmalloc_sum=%lu pfree_sum=%lu\n", num, scm_size, dram_size, pmalloc_sum, pfree_sum);
        }
#endif    
}

void print_statics_delete_final()
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
#endif    
}



void Int64_test(){
    register int i;
    int fd = open("/dev/pmem0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)10ULL * 1024ULL * 1024ULL * 1024ULL, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    stx::btree_map<uint64_t, uint64_t> bt;
    pair<uint64_t, uint64_t> p;

    
    int64_t keys[OP_NUM];

    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = i;
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        uint64_t temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }

    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        p = make_pair(keys[i], i);
        bt.insert(p);
    }
    
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();

    printf("\n*********************************** The read operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
        bt.exists(keys[i]);
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);

}




void String_test(){

    Key_Type keys[OP_NUM];

    register int i;
    int fd = open("/dev/pmem0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)10ULL * 1024ULL * 1024ULL * 1024ULL, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    stx::btree_map<Key_Type, Value_Type> bt;
    pair<Key_Type, Value_Type> p;

    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = std::to_string(i);
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        Key_Type temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }

    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        p = make_pair(keys[i], std::to_string(i));
        bt.insert(p);
    }
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();

     printf("\n*********************************** The read operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
       if( !bt.exists(keys[i]) ){
        std::cout << "ERR\n";  
        return ;
       }
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);
}

void Test(){

   Key_Type keys[OP_NUM];

    register int i;
    int fd = open("/dev/pmem0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)10ULL * 1024ULL * 1024ULL * 1024ULL, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    stx::btree_map<Key_Type, Value_Type> bt;
    pair<Key_Type, Value_Type> p;

    std::cout<< "bt's count size:" << bt.size()<< "\n";


    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = std::to_string(i);
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        Key_Type temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }


    auto it = bt.begin();
    int count = 0;
    while(it != bt.end()){
        //it.key();
        it++;
        count++;
    }

    std::cout << "count : " << count << "\n";


    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        p = make_pair(keys[i], std::to_string(i));
        bt.insert(p);
    }
    gettimeofday(&end_time, NULL);



    std::cout<< "bt's count size:" << bt.size()<< "\n";
    it = bt.begin();
    count = 0;
    while(it != bt.end()){
        //it.key();
        it++;
        count++;
    }

    std::cout << "count : " << count << "\n";






    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();

     printf("\n*********************************** The read operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
       if( !bt.exists(keys[i]) ){
        std::cout << "ERR\n";  
        return ;
       }
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);

    printf("\n*********************************** The update operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        emulate_latency_ns(EXTRA_SCM_LATENCY);

        p = make_pair(keys[i], i);
        bt.update(p);
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Read time_interval = %lu ns\n", time_interval * 1000);
    printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_update();

   /*  printf("\n*********************************** The delete operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) { /// 10 * 9
        print_statics_delete(i);
        size_t del_num = bt.erase(keys[i]);
        if (del_num != 1) {
            //printf("i=%d\n", i); 
            //fflush(stdout);
            assert(del_num == 1);
        }
    }
    print_statics_delete(OP_NUM);
    
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Delete time_interval = %lu ns\n", time_interval * 1000);
    printf("average delete op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_delete_final(); */

    //return 0;

}

class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint32_t Uniform(int n) { return Next() % n; }

  bool OneIn(int n) { return (Next() % n) == 0; }
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

std::string RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return std::string(*dst);
}

std::string RandomNumberKey(Random* rnd) {
    char key[100];
    snprintf(key, sizeof(key), "%016d\n", rand() % 3000000);
    return std::string(key, 16);
}

std::string RandomString(Random* rnd, int len) {
    std::string r;
    RandomString(rnd, len, &r);
    return r;
}





void Bench_Test(){


    //Key_Type keys[OP_NUM];

    register int i;
    int fd = open("/dev/pmem0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)10ULL * 1024ULL * 1024ULL * 1024ULL, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    stx::btree_map<Key_Type, Value_Type> bt;
    pair<Key_Type, Value_Type> p;


  	std::cout << " ######### Bench Test ######## \n";
	static const int kNumOps = 1000;
	static const long int kNumKVs = 3000;
	static const int kValueSize = 2048*128;

    std::cout << "kNumOps: " << kNumOps << " kValueSize: " << kValueSize << "\n";

	Random rnd(0);
	//std::vector<std::string> keys(kNumKVs);
    Key_Type keys[kNumKVs];
	for (int i = 0; i < kNumKVs; ++i) {
	        keys[i] = RandomNumberKey(&rnd);
	}
//	std::map<std::string, std::string> m;
	std::cout << " ######### Begin Bench Insert Test ######## \n";
	clock_t startTime,endTime;
	startTime = clock();

	for (int i = 0; i < kNumOps; i++) {
	        std::string key = keys[i % kNumKVs];
	        std::string value = RandomString(&rnd, kValueSize);
	        p = make_pair(key, value);
            //std::cout<< "insert :" << key << " " << value.size() << "\n";
            bt.insert(p);
	}
	endTime = clock();

	//std::cout << "The Insert time is: " <<((float)(endTime - startTime))/CLOCKS_PER_SEC << "\n";
	std::cout << "The Insert time is: " <<endTime - startTime << "\n";

	std::cout << " @@@@@@@@@ PASS #########\n";
	std::cout << " ######### Begin Bench Get Test ######## \n";

	startTime = clock();
	for (int i = 0; i < kNumOps; i++) {
	        std::string key = keys[i % kNumKVs];
            auto it = bt.find(key);
    	    std::string res = it->second;
	}
	endTime = clock();
	//std::cout << "The Get time is: " <<((float)(endTime - startTime))/CLOCKS_PER_SEC << "\n";
	
    std::cout << "The Get time is: " << endTime - startTime << "\n";
    std::cout << " @@@@@@@@@ PASS #########\n";

	std::cout << " ######### Begin Bench Iterator Seek Test ######## \n";
	startTime = clock();
	for (int i = 0; i < kNumOps; i++) {
	     auto const it = bt.lower_bound(keys[rand() % kNumKVs]);
        // std::cout<<
	}        

	endTime = clock();
//	std::cout << "The Iterator & Seek time is: " <<((float)(endTime - startTime))/CLOCKS_PER_SEC << "\n";
    std::cout << "The Iterator & Seek time is:  " << endTime - startTime << "\n";

	std::cout << " @@@@@@@@@ PASS #########\n";
}

int main()
{
    //Int64_test();
    //String_test();
    Bench_Test();
    return 0;
    
  /*   register int i;
    int fd = open("/dev/pmem0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)10ULL * 1024ULL * 1024ULL * 1024ULL, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    stx::btree_map<uint64_t, uint64_t> bt;
    pair<uint64_t, uint64_t> p;

    

    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = i;
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        uint64_t temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }

    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        p = make_pair(keys[i], i);
        bt.insert(p);
    }
    
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();

    printf("\n*********************************** The read operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
        bt.exists(keys[i]);
        bt.
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);

    printf("\n*********************************** The update operations ********************************\n");
    gettimeofday(&start_time, NULL);

    for (int i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        emulate_latency_ns(EXTRA_SCM_LATENCY);

        p = make_pair(keys[i], i);
        bt.update(p);
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Read time_interval = %lu ns\n", time_interval * 1000);
    printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_update();

    printf("\n*********************************** The delete operations ********************************\n");
    gettimeofday(&start_time, NULL);
    
    for (i = 0; i < OP_NUM; i++) { /// 10 * 9
        print_statics_delete(i);
        size_t del_num = bt.erase(keys[i]);
        if (del_num != 1) {
            //printf("i=%d\n", i); 
            //fflush(stdout);
            assert(del_num == 1);
        }
    }
    print_statics_delete(OP_NUM);
    
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Delete time_interval = %lu ns\n", time_interval * 1000);
    printf("average delete op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_delete_final();

    return 0; */
}
