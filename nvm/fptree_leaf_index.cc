#include <stdexcept>
#include "util/coding.h"
#include "nvm/fptree_leaf_index.h"

namespace leveldb {
namespace silkstore {
class FPTreeLeafIndexIterator: public Iterator {
 public:
  explicit FPTreeLeafIndexIterator(  stx::btree_map<std::string, std::string> *bt){
    bt_ = bt;
	iter_ = bt_->begin();
  }
  virtual bool Valid() const { 
      return iter_ != bt_->end(); 
  }
  virtual void Seek(const Slice& k) {
	  iter_ = bt_->lower_bound(k.ToString());
     // iter_->Seek(k.ToString());
  }
  virtual void SeekToFirst() {
      iter_ = bt_->begin();
  }
  virtual void SeekToLast() {
	  std::__throw_runtime_error("SeekToLast\n");
//      iter_->SeekToLast();
  }
  virtual void Next() {  
      iter_++;
  }
  virtual void Prev() {  
	  std::__throw_runtime_error("Prev\n");
    
	// iter_->Prev();
  }
  
  virtual Slice key() const {
    //std::cout<< "key: " <<  iter_->Key() << " ";
    store.push_back(iter_->first);
    return store.back();
    // If the value is returned directly, the content in slice will be released
    return Slice(iter_->first);
  }
  virtual Slice value() const {
    //std::cout<< "Value: " <<  iter_->Value().size() << " ";
    store.push_back(iter_->second);
    return store.back();

    return iter_->second;
  }

  virtual Status status() const { return Status::OK(); }

 private:
  mutable std::vector<std::string> store;
  stx::btree_map<std::string, std::string>::iterator iter_;
  stx::btree_map<std::string, std::string> *bt_;
  // No copying allowed
  FPTreeLeafIndexIterator(const FPTreeLeafIndexIterator&);
  void operator=(const FPTreeLeafIndexIterator&);
};

 Iterator* FPTreeLeafIndex::NewIterator(const ReadOptions& options){
  if (bt->size() == 0){
      //std::cout<< "return NewEmptyIterator \n";
      return NewEmptyIterator();
  }
  //std::cout<< "return FPTreeLeafIndexIterator \n";
  return new FPTreeLeafIndexIterator(bt);
} 





class EmptyKVIterator:public kvdk::Iterator {
public:
  virtual void Seek(const std::string &key) {};

  virtual void SeekToFirst(){};

  virtual void SeekToLast(){};

  virtual bool Valid(){return false;};

  virtual void Next() {};

  virtual void Prev(){};

  virtual std::string Key(){return nullptr;};

  virtual std::string Value(){return nullptr;};
};



FPTreeLeafIndex::FPTreeLeafIndex(const Options& options, const std::string& dbname){
    bt = new stx::btree_map<std::string, std::string>();
    printf("############# Successfully Opened FPTreeLeafIndex. ##############\n");
}

Status FPTreeLeafIndex::OpenLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr){
    *dbptr = new FPTreeLeafIndex(options, name);
    return Status::OK();
}


const Snapshot* FPTreeLeafIndex::GetSnapshot() {
    throw std::runtime_error("FPTreeLeafIndex::GetSnapshot not supported");
}

void FPTreeLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
    throw std::runtime_error("FPTreeLeafIndex::ReleaseSnapshot not supported");
}

FPTreeLeafIndex::~FPTreeLeafIndex() {
    if (bt != nullptr){
        delete bt;
    }
}


Status FPTreeLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
    throw std::runtime_error("FPTreeLeafIndex::Write not supported");
    mutex_.Lock();                  
    //Status status = WriteBatchInternal::InsertInto(my_batch, kv);
    mutex_.Unlock();
    //return status;
}

/* Status FPTreeLeafIndex::Put(const WriteOptions& options,
                         const Slice& key,
                         const Slice& value) {
    kvdk::Status s;
    mutex_.Lock();                                                      
    s = kv->SSet("leafindex",key.ToString(), value.ToString());
    mutex_.Unlock();                    
    if (s == kvdk::Status::Ok){
        return Status::OK();
    }
    return Status::Corruption("");               
}
 */


Status FPTreeLeafIndex::Put(const WriteOptions& options,  
                         const Slice& key,
                         const Slice& value) {
    mutex_.Lock();
	p = std::make_pair(key.ToString(), value.ToString());
	bt->insert(p);                                                      
    mutex_.Unlock();  
    
	return Status::OK();
	                  
}

Status FPTreeLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
	std::__throw_runtime_error("");
    return Status::Corruption("");                   
}


Status FPTreeLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {
    kvdk::Status s;
    mutex_.Lock(); 
	auto it = bt->find(key.ToString());  
	*value = it->second;                                                          
   // s = kv->SGet("leafindex",key.ToString(), value);
    //std::cout<< key.size() <<" " << value->size() <<"\n";
    mutex_.Unlock();                    
	return Status::OK();    
}



/* Iterator* FPTreeLeafIndex::NewIterator(const ReadOptions& options) {
    throw std::runtime_error("FPTreeLeafIndex::NewIterator not supported");
} */

bool FPTreeLeafIndex::GetProperty(const Slice& property, std::string* value){
    //throw std::runtime_error("FPTreeLeafIndex::GetProperty not supported");
    return true;
}
void FPTreeLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
    throw std::runtime_error("FPTreeLeafIndex::GetApproximateSizes not supported");
}
void FPTreeLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
    throw std::runtime_error("FPTreeLeafIndex::CompactRange not supported");
}

}
}