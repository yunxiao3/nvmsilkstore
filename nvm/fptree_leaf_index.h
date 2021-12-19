#ifndef STORAGE_LEVELDB_INCLUDE_FPTree_LEAF_INDEX_H_
#define STORAGE_LEVELDB_INCLUDE_FPTree_LEAF_INDEX_H_

#include <string>
#include <map>
#include <cstdint>
#include <cstdio>
#include "db/write_batch_internal.h"
#include"leveldb/write_batch.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "fptree_map.h"

#include "port/port.h"
#include "port/thread_annotations.h"


namespace leveldb {

namespace silkstore {





// A FPTreeLeafIndex is a persistent ordered map from keys to values.
// A FPTreeLeafIndex is safe for concurrent access from multiple threads without
// any external synchronization.
class FPTreeLeafIndex: public DB {
public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static Status OpenLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr);
  FPTreeLeafIndex(const Options& options, const std::string& dbname);
  FPTreeLeafIndex() = default;

  FPTreeLeafIndex(const FPTreeLeafIndex&) = delete;
  FPTreeLeafIndex& operator=(const FPTreeLeafIndex&) = delete;

  virtual ~FPTreeLeafIndex() ;

  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  // Note: consider setting options.sync = true.
  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) ;

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  virtual Status Delete(const WriteOptions& options, const Slice& key) ;

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) ;
  
  Status NvmWrite(const WriteOptions& options, NvmWriteBatch* updates){
    std::__throw_runtime_error("");
  };

  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  // May return some other Status on an error.
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::string* value) ;

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  //virtual Iterator* NewIterator(const ReadOptions& options) ;
  std::shared_ptr<kvdk::Iterator> NewIterator();
  //Iterator* NewIterator(const ReadOptions);
  Iterator* NewIterator(const ReadOptions& options);
  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.
  virtual const Snapshot* GetSnapshot() ;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot* snapshot) ;

  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "leveldb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "leveldb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  //  "leveldb.approximate-memory-usage" - returns the approximate number of
  //     bytes of memory in use by the DB.
  virtual bool GetProperty(const Slice& property, std::string* value) ;

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  // The results may not include the sizes of recently written data.
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) ;

  // Compact the underlying storage for the key range [*begin,*end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==nullptr is treated as a key before all keys in the database.
  // end==nullptr is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(nullptr, nullptr);
  virtual void CompactRange(const Slice* begin, const Slice* end) ;
private:

  stx::btree_map<std::string, std::string> *bt;
  std::pair<std::string, std::string> p;

  port::Mutex mutex_;
};




class FPTreeWriteBatch {
 public:
  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);


  static void SetContents(WriteBatch* batch, const Slice& contents);

  static Status InsertInto(const WriteBatch* batch, stx::btree_map<std::string, std::string> * kv);

  

  static void Append(WriteBatch* dst, const WriteBatch* src);
 //  static void Append(NvmWriteBatch* dst, const NvmWriteBatch* src);

};



class FPTreeInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  stx::btree_map<std::string, std::string>* kv_;
  std::pair<std::string, std::string> p;

  virtual void Put(const Slice& key, const Slice& value) {
   // printf("PmemkvInserter: Put set a default prefix leafindex \n");
   // std::cout<<"PmemkvInserter: "<<  key.ToString()<< " value.ToString size: "<<  value.ToString().size() <<  " \n";
    //std::cout<< "######## insert : " << key.ToString() << "########\n";

    p = std::make_pair(key.ToString(), value.ToString());
    kv_->insert(p);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    kv_->erase(key.ToString());
    sequence_++;
  }
};


Status FPTreeWriteBatch::InsertInto(const WriteBatch* b,
                                      stx::btree_map<std::string, std::string>* kv) {
  FPTreeInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.kv_ = kv;
  return b->Iterate(&inserter);
}



class FPTreeLeafIndexIterator: public Iterator {
 public:
  explicit FPTreeLeafIndexIterator(  stx::btree_map<std::string, std::string> *bt){
  bt_ = bt;
	iter_ = bt_->begin();
 // end =  bt_->end();

  count = bt_->size();
  cur_index = 0; 

  //std::cout<< "count: " << count << " cur_index: " << cur_index << "\n"; 
}
  virtual bool Valid() const {
    //  std::cout<< " Valid count: " << count << " cur_index: " << cur_index << "\n"; 
      return iter_ != bt_->end();
      return cur_index < count;
  }
  virtual void Seek(const Slice& k) {
	  iter_ = bt_->lower_bound(k.ToString());
     // iter_->Seek(k.ToString());
  }
  virtual void SeekToFirst() {
      cur_index = 0;
      iter_ = bt_->begin();
  }
  virtual void SeekToLast() {
	  std::__throw_runtime_error("SeekToLast\n");
//      iter_->SeekToLast();
  }
  virtual void Next() { 
      ++iter_;
      cur_index++;
     // std::cout<< "Next " << "count: " << count << " cur_index: " << cur_index << "\n"; 

  }
  virtual void Prev() {  
	  std::__throw_runtime_error("Prev\n");
    
	// iter_->Prev();
  }
  
  virtual Slice key() const {
    //std::cout<< "iter  key: " <<  iter_->first << " \n";
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
  //const stx::btree_map<std::string, std::string>::const_iterator end;
  int count;
  int cur_index;
  stx::btree_map<std::string, std::string> *bt_;
  
  // No copying allowed
  FPTreeLeafIndexIterator(const FPTreeLeafIndexIterator&);
  void operator=(const FPTreeLeafIndexIterator&);
};

 Iterator* FPTreeLeafIndex::NewIterator(const ReadOptions& options){
  if (bt->size() == 0){
      std::cout<< "return NewEmptyIterator \n";
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
    //throw std::runtime_error("FPTreeLeafIndex::Write not supported");
    mutex_.Lock();                  
    Status status = FPTreeWriteBatch::InsertInto(my_batch, bt);
    mutex_.Unlock();
    return status;
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







                               
}  // namespace silkstore
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_NVM_LEAF_INDEX_H_