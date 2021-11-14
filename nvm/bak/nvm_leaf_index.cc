#include <stdexcept>
#include "nvm_leaf_index.h"
#include "util/coding.h"

namespace leveldb {
namespace silkstore {
class NVMLeafIndexIterator: public Iterator {
 public:
  explicit NVMLeafIndexIterator(std::shared_ptr<kvdk::Iterator> iter){
	iter_ = iter;
  }
  virtual bool Valid() const { 
	  return iter_->Valid(); 
  }
  virtual void Seek(const Slice& k) {
	  iter_->Seek(k.ToString());
  }
  virtual void SeekToFirst() {
  	  iter_->SeekToFirst();
  }
  virtual void SeekToLast() {
	  iter_->SeekToLast();
  }
  virtual void Next() {  
	  iter_->Next();
  }
  virtual void Prev() {	 
	 iter_->Prev();
  }
  
  virtual Slice key() const { 
	return iter_->Key();
  }
  virtual Slice value() const {
    return iter_->Value();

  }

  virtual Status status() const { return Status::OK(); }

 private:
  std::shared_ptr<kvdk::Iterator>  iter_;
  // No copying allowed
  NVMLeafIndexIterator(const NVMLeafIndexIterator&);
  void operator=(const NVMLeafIndexIterator&);
};

Iterator* NVMLeafIndex::NewIterator(const ReadOptions& options){
  auto it =  kv->NewSortedIterator("leafindex");
  if (it == nullptr){
	  return NewEmptyIterator();
  }
  return new NVMLeafIndexIterator(it);
}


NVMLeafIndex::NVMLeafIndex(const Options& options, const std::string& dbname){
  	kvdk::Configs cfg;
    cfg.pmem_segment_blocks = (1ull << 8);
    cfg.hash_bucket_num = (1ull << 15);
	printf("###### Need to set pmem_segment_blocks  hash_bucket_num #####\n");	
 	std::string engine_path;
	if (options.leaf_index_path == nullptr){
		printf("############# Using Default /mnt/NVMSilkstore/leafindex ##############\n");
		engine_path = "/mnt/NVMSilkstore/leafindex";
	}else{
		engine_path = options.leaf_index_path;
	}
	if (options.leaf_index_size <= 0){
		printf("############# Using Default leaf_index_size  ##############\n");
		cfg.pmem_file_size = 1280UL * 1024UL * 1024UL * 6;
	}else{
		std::cout << " set default size need to fix\n";
		cfg.pmem_file_size = 1280UL * 1024UL * 1024UL * 6;
		//s = cfg.put_size(options.leaf_index_size);
	}
	kvdk::Engine::Open(engine_path, &kv, cfg, stdout);
	printf("############# Successfully opened a KVDK instance. ##############\n"); 
}

Status NVMLeafIndex::OpenLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr){
	*dbptr = new NVMLeafIndex(options, name);
	return Status::OK();
}


const Snapshot* NVMLeafIndex::GetSnapshot() {
	throw std::runtime_error("NVMLeafIndex::GetSnapshot not supported");
}

void NVMLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
	throw std::runtime_error("NVMLeafIndex::ReleaseSnapshot not supported");
}

NVMLeafIndex::~NVMLeafIndex() {
	if (kv != nullptr){
		delete kv;
	}
}


Status NVMLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
	//throw std::runtime_error("NVMLeafIndex::Write not supported");
	mutex_.Lock();				 	
	Status status = WriteBatchInternal::InsertInto(my_batch, kv);
	mutex_.Unlock();
	return status;
}

Status NVMLeafIndex::Put(const WriteOptions& options,
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


Status NVMLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
	kvdk::Status s;
	std::cout<<"Delete: " << key.ToString() << "\n";														
	s = kv->Delete(key.ToString());
	if (s == kvdk::Status::Ok){
		return Status::OK();
	}
	return Status::Corruption("");					 
}


Status NVMLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {
	kvdk::Status s;
	mutex_.Lock();				 										
	s = kv->SGet("leafindex",key.ToString(), value);
	mutex_.Unlock();				 	

	if (s == kvdk::Status::Ok){
		return Status::OK();
	}
	return Status::Corruption("");				 
}



/* Iterator* NVMLeafIndex::NewIterator(const ReadOptions& options) {
	throw std::runtime_error("NVMLeafIndex::NewIterator not supported");
} */

bool NVMLeafIndex::GetProperty(const Slice& property, std::string* value){
	throw std::runtime_error("NVMLeafIndex::GetProperty not supported");
}
void NVMLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
	throw std::runtime_error("NVMLeafIndex::GetApproximateSizes not supported");
}
void NVMLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
	throw std::runtime_error("NVMLeafIndex::CompactRange not supported");
}

}
}