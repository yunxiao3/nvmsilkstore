#include <libpmemkv.hpp>

#include<iostream>
#include<map>
#include<vector>
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

void Bench(){





  pmem::kv::config cfg;
	std::string engine_path = "/mnt/NVMSilkstore/leafindex";
	pmem::kv::status s = cfg.put_path("/mnt/NVMSilkstore/pmemkv");
	s = cfg.put_size(1024ul*1024*1024*10);
	assert(s == pmem::kv::status::OK);
	s = cfg.put_create_if_missing(true);
	assert(s == pmem::kv::status::OK);
	pmem::kv::db *db_ = new pmem::kv::db();
	s = db_->open("stree", std::move(cfg));

  std::cout << " ######### Bench Test ######## \n";
  static const int kNumOps = 1000;
  static const long int kNumKVs = 3000;
  static const int kValueSize = 2048*128;


  Random rnd(0);
  std::vector<std::string> keys(kNumKVs);
  for (int i = 0; i < kNumKVs; ++i) {
          keys[i] = RandomNumberKey(&rnd);
  }
  //sort(keys.begin(), keys.end());
  std::map<std::string, std::string> m;
  std::cout << " ######### Begin Bench Insert Test ######## \n";
  clock_t startTime,endTime;
  startTime = clock();

  for (int i = 0; i < kNumOps; i++) {
          std::string key = keys[i % kNumKVs];
          std::string value = RandomString(&rnd, kValueSize);
          db_->put(key, value);
          // m[key] = value;
  }
  endTime = clock();
  std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

  std::cout << " @@@@@@@@@ PASS #########\n";
  std::cout << " ######### Begin Bench Get Test ######## \n";

  startTime = clock();
  for (int i = 0; i < kNumOps; i++) {
          std::string key = keys[i % kNumKVs];
          std::string res;
          db_->get(key, &res);
        /*   if (res != m[key]){
                  std::cout<< "ERR\n";
                  return ;
          } */
  }
  endTime = clock();
  std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
  std::cout << " @@@@@@@@@ PASS #########\n";

  /* std::cout << " ######### Begin Bench Iterator Test ######## \n";
  startTime = clock();        
  auto it = db_->NewSortedIterator("test");
  it->SeekToFirst();
  while (it->Valid()) {
      auto res_key = it->Key();
      auto res_value = it->Value();
      
      it->Next();
  }
  endTime = clock();
  std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";

  std::cout << " @@@@@@@@@ PASS #########\n"; */
  delete db_;
  std::cout << " Delete Open Db \n";
}




int main(int argc, char const *argv[]){
    //EmptyIter();
    Bench();
    //SequentialWrite();
    return 0;
}


  


