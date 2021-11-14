#include <stdexcept>
#include "pmemkv_leaf_index.h"
#include "util/coding.h"
#include <string_view>
namespace leveldb {
namespace silkstore {


static std::string read_key(pmem::kv::db::read_iterator &it)
{
	/* key_result's type is a pmem::kv::result<string_view>, for more information
	 * check pmem::kv::result documentation */
	pmem::kv::result<std::string_view> key_result = it.key();
	/* check if the result is ok, you can also do:
	 * key_result == pmem::kv::status::OK
	 * or
	 * key_result.get_status() == pmem::kv::status::OK */
	assert(key_result.is_ok());

	return key_result.get_value().data();
}

static std::string read_value(pmem::kv::db::read_iterator &it){
	/* val_result's type is a pmem::kv::result<string_view>, for more information
	 * check pmem::kv::result documentation */
	pmem::kv::result<std::string_view> val_result = it.read_range();
	/* check if the result is ok, you can also do:
	 * val_result == pmem::kv::status::OK
	 * or
	 * val_result.get_status() == pmem::kv::status::OK */
	assert(val_result.is_ok());

	return val_result.get_value().data();
}


class PmemKVLeafIndexIterator: public Iterator {
 public:
  explicit PmemKVLeafIndexIterator(pmem::kv::db *kv)
  		:it(kv->new_read_iterator()),iter_(it.get_value()){
	  valid = true;
	  iter_.seek_to_first();
	  pmem::kv::status s = iter_.is_next();
	  islast = (s != pmem::kv::status::OK);
  }
  virtual bool Valid() const { 
	  return valid; 
  }
  virtual void Seek(const Slice& k) {
	  iter_.seek_lower_eq(k.ToString());
	  pmem::kv::status s = iter_.is_next();
	  islast = (s != pmem::kv::status::OK);
  }
  virtual void SeekToFirst() {
  	  iter_.seek_to_first();
	  pmem::kv::status s = iter_.is_next();
	  islast = (s != pmem::kv::status::OK);		
  }
  virtual void SeekToLast() {
	  iter_.seek_to_last();
	  islast = true;
  }
  virtual void Next() {  
	  iter_.next();
	   if (islast){
		  valid = false;
	  }else{
		  pmem::kv::status s = iter_.is_next();
	      islast = (s != pmem::kv::status::OK);	
	  }	 
  }
  virtual void Prev() {	 
	std::__throw_runtime_error("NOT Sport Prev\n");
	iter_.prev();
  }
  
  virtual Slice key() const { 
	//std::cout<<"get key \n";
	return read_key(iter_);
  }
  virtual Slice value() const {
	//std::cout<<"get value \n";	  
    return read_value(iter_);
  }

  virtual Status status() const { return Status::OK(); }

 private:
 // pmem::kv::db::read_iterator &w_it = it.get_value();
  bool valid;
  bool islast;
  pmem::kv::result<pmem::kv::db::read_iterator>  it;
  pmem::kv::db::read_iterator &iter_;
  // No copying allowed
  PmemKVLeafIndexIterator(const PmemKVLeafIndexIterator&);
  void operator=(const PmemKVLeafIndexIterator&);
};

Iterator* PmemKVLeafIndex::NewIterator(const ReadOptions& options){
  size_t cnt = 0; 
  kv->count_all(cnt);
  //auto &res = it.get_value();
  if (cnt == 0){
	  return NewEmptyIterator();
  }

  std::cout << "kv's cnt is " << cnt << "\n";
  //pmem::kv::result<pmem::kv::db::read_iterator>  it =  kv->new_read_iterator();

  //auto &rit = it.get_value();
 // return NewEmptyIterator();
  return new PmemKVLeafIndexIterator( kv ); 
}
 

PmemKVLeafIndex::PmemKVLeafIndex(const Options& options, const std::string& dbname){
  	pmem::kv::config cfg;

	pmem::kv::status s = cfg.put_path("/mnt/NVMSilkstore/pmemkv");
	s = cfg.put_size(1024ul*1024*1024*10);
	assert(s == pmem::kv::status::OK);
	s = cfg.put_create_if_missing(true);
	assert(s == pmem::kv::status::OK);
	kv = new pmem::kv::db();
	s = kv->open("stree", std::move(cfg));
	/* auto it = kv->new_read_iterator();
	 */
	assert(s == pmem::kv::status::OK);	
	printf("############# Successfully opened a pmem::kv instance. ##############\n"); 
}

Status PmemKVLeafIndex::OpenLeafIndex(const Options& options,
                     const std::string& name,
                     PmemKVLeafIndex** dbptr){
	*dbptr = new PmemKVLeafIndex(options, name);
	return Status::OK();
}


const Snapshot* PmemKVLeafIndex::GetSnapshot() {
	throw std::runtime_error("PmemKVLeafIndex::GetSnapshot not supported");
}

void PmemKVLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
	throw std::runtime_error("PmemKVLeafIndex::ReleaseSnapshot not supported");
}

PmemKVLeafIndex::~PmemKVLeafIndex() {

}


Status PmemKVLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
	throw std::runtime_error("PmemKVLeafIndex::Write not supported");
	/* mutex_.Lock();				 	
	Status status = WriteBatchInternal::InsertInto(my_batch, kv);
	mutex_.Unlock();
	return status; */
}

Status PmemKVLeafIndex::Put(const WriteOptions& options,
		   				 const Slice& key,
		   				 const Slice& value) {
	pmem::kv::status s;
	mutex_.Lock();				 										
	s = kv->put(key.ToString(), value.ToString());
	mutex_.Unlock();				 	
	if (s == pmem::kv::status::OK){
		return Status::OK();
	}
	std::__throw_runtime_error("PmemKVLeafIndex::Put Err\n");
	return Status::Corruption("");				 
}


Status PmemKVLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
	pmem::kv::status s;
	std::cout<<"Delete: " << key.ToString() << "\n";														
	s = kv->remove(key.ToString());
	if (s == pmem::kv::status::OK){
		return Status::OK();
	}
	std::__throw_runtime_error("PmemKVLeafIndex::Delete Err\n");	
	return Status::Corruption("");					 
}


Status PmemKVLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {
	pmem::kv::status s;
	mutex_.Lock();				 										
	s = kv->get(key.ToString(), value);
	mutex_.Unlock();				 	

	if (s == pmem::kv::status::OK){
		return Status::OK();
	}

	std::__throw_runtime_error("PmemKVLeafIndex::Get Err\n");	
	return Status::Corruption("");				 
}



/* Iterator* PmemKVLeafIndex::NewIterator(const ReadOptions& options) {
	throw std::runtime_error("PmemKVLeafIndex::NewIterator not supported");
} */

bool PmemKVLeafIndex::GetProperty(const Slice& property, std::string* value){
	throw std::runtime_error("PmemKVLeafIndex::GetProperty not supported");
}
void PmemKVLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
	throw std::runtime_error("PmemKVLeafIndex::GetApproximateSizes not supported");
}
void PmemKVLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
	throw std::runtime_error("PmemKVLeafIndex::CompactRange not supported");
}

}
}