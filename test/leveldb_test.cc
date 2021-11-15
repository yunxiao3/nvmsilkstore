
/*
 make and install leveldb
 then compile leveldb_test.cc
 g++ -std=c++17 leveldb_test.cc -lleveldb -lsnappy -lpthread

######### Bench Test ######## 
 ######### Begin Bench Insert ######## 
The Insert time is: 13333287
 @@@@@@@@@ PASS #########
 ######### Begin Sequential Get Test ######## 
The Get time is: 1953592
 @@@@@@@@@ PASS #########
 ######### Begin Sequential Iterator Test ######## 
The Iterator time is: 66427
 @@@@@@@@@ PASS #########
*/

#include "leveldb/db.h"
#include<iostream>
#include<map>
#include<vector>
#include <stdlib.h>
#include<leveldb/write_batch.h>

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

leveldb::Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return leveldb::Slice(*dst);
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
        leveldb::DB* db_ = nullptr;
        leveldb::Options options;
        options.create_if_missing = true;

       // leveldb::silkstore::NVMLeafIndex* db = new NVMLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::DB::Open(options, "./leveldb_test_dir", &db_);
        assert(s.ok()==true);
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
        std::cout << " ######### Begin Bench Insert ######## \n";

        clock_t startTime,endTime;
        startTime = clock();
        leveldb::WriteBatch batch;
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Clear();
                batch.Put(key, value);
                db_->Write(leveldb::WriteOptions(),&batch);
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Sequential Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
        }
        endTime = clock();
        std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";

        std::cout << " ######### Begin Sequential Iterator Test ######## \n";
        startTime = clock();        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        while ( it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            it->Next();
        }
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}


int main(int argc, char const *argv[]){
    //EmptyIter();
    Bench();
    return 0;
}