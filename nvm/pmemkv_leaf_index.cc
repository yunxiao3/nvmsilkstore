#include <stdexcept>
#include "pmemkv_leaf_index.h"
#include "util/coding.h"
#include <string>
namespace leveldb {
namespace silkstore {


static std::string read_key(pmem::kv::db::read_iterator &it)
{
	/* key_result's type is a pmem::kv::result<string_view>, for more information
	 * check pmem::kv::result documentation */
	auto key_result = it.key();
	/* check if the result is ok, you can also do:
	 * key_result == pmem::kv::status::OK
	 * or
	 * key_result.get_status() == pmem::kv::status::OK */
	assert(key_result.is_ok());

	return key_result.get_value().data();
}

static std::string read_value(pmem::kv::db::read_iterator &it,std::string &rvalue){
	/* val_result's type is a pmem::kv::result<string_view>, for more information
	 * check pmem::kv::result documentation */
	auto val_result = it.read_range();
	/* check if the result is ok, you can also do:
	 * val_result == pmem::kv::status::OK
	 * or
	 * val_result.get_status() == pmem::kv::status::OK */
	assert(val_result.is_ok());
	

	return  val_result.get_value().data();

	std::string value = val_result.get_value().data();
	size_t size = val_result.get_value().size();
	rvalue.assign(val_result.get_value().data(),0,size*10);
	//rvalue = value ;
	std::cout << "val_result.get_value().data :" << val_result.get_value().data();
	std::cout << "val_result.get_value().data size: " << size << "\n";

	std::cout << "in read_value rvalue size: " << rvalue.size() << "\n";
	return value;
}


class PmemKVLeafIndexIterator: public Iterator {
 public:
  explicit PmemKVLeafIndexIterator(pmem::kv::db *kv)
  		:kv_(kv), it(kv->new_read_iterator()),iter_(it.get_value()){
	  valid = true;
	  iter_.seek_to_first();
	  pmem::kv::status s = iter_.is_next();
	  islast = (s != pmem::kv::status::OK);
  }
  ~PmemKVLeafIndexIterator(){
	//  std::cout<< "release PmemKVLeafIndexIterator\n";
  }
  virtual bool Valid() const { 
	  return valid; 
  }
  virtual void Seek(const Slice& k) {
	  pmem::kv::status s = iter_.seek_higher_eq(k.ToString());
	  if (s != pmem::kv::status::OK){
		  valid = false;
	  }else{
		s = iter_.is_next();
	  	islast = (s != pmem::kv::status::OK);
	  }
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
	//std::string key = read_key(iter_);
	store.push_back(read_key(iter_));
	return store.back();
  }
  virtual Slice value() const {
	//std::cout<<"get value \n";
	std::string value; 
	//std::string rvalue; 

	std::string key =  read_key(iter_);//"0000000002021968";
/* 	std::cout << " in value key: " << key << " \n";
	std::string tmp = "0000000002021968"; */
	kv_->get(key,&value);
//   	value = read_value(iter_, rvalue);
	store.push_back(value);
	//std::cout<< "value's size " << store.back().size()<< " rvalue " <<rvalue.size() << " \n";
    return store.back();
  }

  virtual Status status() const { return Status::OK(); }

 private:
 // pmem::kv::db::read_iterator &w_it = it.get_value();
  bool valid;
  bool islast;
  pmem::kv::result<pmem::kv::db::read_iterator>  it;
  pmem::kv::db::read_iterator &iter_;
  pmem::kv::db *kv_;
  //vector<st>
  mutable std::vector<std::string> store;
  // No copying allowed
  PmemKVLeafIndexIterator(const PmemKVLeafIndexIterator&);
  void operator=(const PmemKVLeafIndexIterator&);
};

Iterator* PmemKVLeafIndex::NewIterator(const ReadOptions& options){
  size_t cnt = 0; 
  kv->count_all(cnt);
  //auto &res = it.get_value();
  if (cnt == 0){
	 // std::cout << "PmemKVLeafIndex return NewEmptyIterator\n";
	  return NewEmptyIterator();
  }

  //std::cout << "kv's cnt is " << cnt << "\n";
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
	printf("#### Successfully opened a pmem::kv instance./mnt/NVMSilkstore/pmemkv #####\n"); 
}

Status PmemKVLeafIndex::OpenLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr){
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
	delete kv;
}





Status PmemKVLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
	//throw std::runtime_error("PmemKVLeafIndex::Write not supported");
	mutex_.lock();				 	
	Status status = WriteBatchInternal::InsertInto(my_batch, kv);
/* 

	std::string res;
	pmem::kv::status s = kv->get("0000000002999959",&res);

    std::cout<<"Get 0000000002021968 value  size:"<<  res.size()<< " \n";
	//kv.
	auto it = NewIterator(leveldb::ReadOptions());


	while (it->Valid()){
		std::cout << "key: " << it->key().ToString() 
			<< "value's size: " <<  it->value().size() <<" " << s << "\n";
		it->Next();
	}
	
	 s = kv->get("0000000002698972",&res);

    std::cout<<"Get 0000000002698972 value  size:"<<  res.size() << s << " \n"; */
	//kv.

	mutex_.unlock();
	return status;
}

Status PmemKVLeafIndex::Put(const WriteOptions& options,
		   				 const Slice& key,
		   				 const Slice& value) {
	pmem::kv::status s;
	mutex_.lock();				 										
	s = kv->put(key.ToString(), value.ToString());
	mutex_.unlock();				 	
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
	mutex_.lock();				 										
	s = kv->get(key.ToString(), value);
	mutex_.unlock();				 	

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
	//throw std::runtime_error("PmemKVLeafIndex::GetProperty not supported");
}
void PmemKVLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
	throw std::runtime_error("PmemKVLeafIndex::GetApproximateSizes not supported");
}
void PmemKVLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
	throw std::runtime_error("PmemKVLeafIndex::CompactRange not supported");
}

}
}