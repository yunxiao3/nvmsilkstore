#include <stdexcept>
#include "nvmem_leaf_index.h"
#include "util/coding.h"

namespace leveldb {
namespace silkstore {
class NvmemLeafIndexIterator: public Iterator {
 public:
  explicit NvmemLeafIndexIterator(std::shared_ptr<kvdk::Iterator> iter){
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
	//std::cout<< "key: " <<  iter_->Key() << " ";
	store.push_back(iter_->Key());
	return store.back();
	// If the value is returned directly, the content in slice will be released
	return Slice(iter_->Key() );
  }
  virtual Slice value() const {
	//std::cout<< "Value: " <<  iter_->Value().size() << " ";
	store.push_back(iter_->Value());
	return store.back();

    return iter_->Value();
  }

  virtual Status status() const { return Status::OK(); }

 private:
  mutable std::vector<std::string> store;

  std::shared_ptr<kvdk::Iterator>  iter_;
  // No copying allowed
  NvmemLeafIndexIterator(const NvmemLeafIndexIterator&);
  void operator=(const NvmemLeafIndexIterator&);
};

 Iterator* NvmemLeafIndex::NewIterator(const ReadOptions& options){
  //std::__throw_runtime_error(" NvmemLeafIndex::NewIterator(const ReadOptions& options) not support\n");
  mutex_.Lock();				 
  auto it =  nvm_table->NewIterator();
  mutex_.Unlock();				 	

  if (it == nullptr){
	  //std::cout<< "return NewEmptyIterator \n";
	  return NewEmptyIterator();
  }
  //std::cout<< "return NvmemLeafIndexIterator \n";
  return it;
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




NvmemLeafIndex::NvmemLeafIndex(const Options& options, const std::string& dbname){
	cap_ = 10ul*1204ul*1024ul*1024ul;
	NvmManager *nvm_manager_ = new NvmManager("/mnt/NVMSilkstore/leaf_index", cap_);
    const InternalKeyComparator internal_comparator_(leveldb::BytewiseComparator());
	nvm_table =  new LeafIndex(internal_comparator_, nullptr, nvm_manager_->allocate(9ul*1204ul*1024ul*1024ul) );

	printf("#### Successfully opened NvmemLeafIndex #####\n");
}

Status NvmemLeafIndex::OpenLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr){
	*dbptr = new NvmemLeafIndex(options, name);
	return Status::OK();
}


const Snapshot* NvmemLeafIndex::GetSnapshot() {
	throw std::runtime_error("NvmemLeafIndex::GetSnapshot not supported");
}

void NvmemLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
	throw std::runtime_error("NvmemLeafIndex::ReleaseSnapshot not supported");
}

NvmemLeafIndex::~NvmemLeafIndex() {
	if (nvm_table != nullptr){
		delete nvm_table;
	}
}


Status NvmemLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
	//throw std::runtime_error("NvmemLeafIndex::Write not supported");
	if (nvm_table->ApproximateMemoryUsage() > cap_){
		throw std::runtime_error("NvmemLeafIndex out of memory\n");
	}
	mutex_.Lock();
	Status status = WriteBatchInternal::InsertInto(my_batch, nvm_table);
/* 	std::cout<<"########## sleep: #########\n";
	sleep(1000); */				 		
	mutex_.Unlock();				 	
	
	return status;
}

/* Status NvmemLeafIndex::Put(const WriteOptions& options,
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


Status NvmemLeafIndex::Put(const WriteOptions& options,  
						 const Slice& key,
		   				 const Slice& value) {


	std::__throw_runtime_error("");

	/* kvdk::Status s;
	mutex_.Lock();				 										
	s = kv->Add(key.ToString(), value.ToString());
	mutex_.Unlock();				 	
	if (s == kvdk::Status::Ok){
		return Status::OK();
	}else{
		std::cout<< "NvmemLeafIndex::Put ERR Code: "<< (int)s <<" \n";
		std::__throw_runtime_error(" NvmemLeafIndex::Put ERR \n");
		return Status::Corruption("");	
	} */
	return Status::Corruption("");	
}

Status NvmemLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
	std::__throw_runtime_error("");
	/* kvdk::Status s;
	std::cout<<"Delete: " << key.ToString() << "\n";														
	s = kv->Delete(key.ToString());
	if (s == kvdk::Status::Ok){
		return Status::OK();
	} */
	return Status::Corruption("");					 
}


Status NvmemLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {

	std::__throw_runtime_error("");
						  
	/* kvdk::Status s;
	mutex_.Lock();				 										
	s = kv->Get(ReadOptions(),key.ToString(), value);
	//std::cout<< key.size() <<" " << value->size() <<"\n";
	mutex_.Unlock();				 	

	if (s == kvdk::Status::Ok){
		return Status::OK();
	} */
	return Status::Corruption("");				 
}



/* Iterator* NvmemLeafIndex::NewIterator(const ReadOptions& options) {
	throw std::runtime_error("NvmemLeafIndex::NewIterator not supported");
} */

bool NvmemLeafIndex::GetProperty(const Slice& property, std::string* value){
	//throw std::runtime_error("NvmemLeafIndex::GetProperty not supported");
	return true;
}
void NvmemLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
	throw std::runtime_error("NvmemLeafIndex::GetApproximateSizes not supported");
}
void NvmemLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
	throw std::runtime_error("NvmemLeafIndex::CompactRange not supported");
}

}
}