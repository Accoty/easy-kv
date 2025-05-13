#pragma once
#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/mman.h>

#include "easykv/utils/bloom_filter.hpp"
#include "easykv/lsm/memtable.hpp"

namespace easykv {
namespace lsm {
struct EntryIndex {
    std::string_view key;
    std::string_view value;
    size_t offset;
    size_t binary_size() {
        return key.size() + value.size() + 2 * sizeof(size_t);
    }
    size_t Load(char* s, size_t index) {
        offset = index;
        key = std::string_view(s + 2 * sizeof(size_t), *reinterpret_cast<size_t*>(s));
        value = std::string_view(s + 2 * sizeof(size_t) + key.size(), *reinterpret_cast<size_t*>(s + sizeof(size_t)));
        return binary_size();
    }
};
/*
IndexBlock in file [size(8byte) | cnt(8byte) | (offset(8byte) + key_size(8byte) + key(key_size byte)), ...]
DataBlock in file [(size(8byte) | bloom_filter | cnt(8byte) | (key_size(8byte) + value_size(8byte) + key(key_size byte) + value(value_size byte)), ...]
SST in file [IndexBlock | (DataBlock...)]

DataBlcok 和 IndexBlock 都以文件形式访问，Memtable 边序列化边构建 DataBlock
*/

class DataBlockIndex {
public:
    void SetOffset(size_t offset) {
        offset_ = offset;
    }
    size_t Load(char* s, int32_t offset = -1) {
        if (offset_ != -1) {
            offset_ = offset;
        }
        size_t index = offset_;
        cached_binary_size_ = *reinterpret_cast<size_t*>(s + index);
        // std::cout << " data block load size " << cached_binary_size_ << std::endl;
        index += sizeof(size_t);
        // std::cout << " index " << index << std::endl;
        index += bloom_filter_.Load(s + index);
        // std::cout << "bloom filter loaded" << " " << index << std::endl;
        // std::cout << "bloom filter loaded size " << bloom_filter_.binary_size() << std::endl;
        size_ = *reinterpret_cast<size_t*>(s + index);
        // std::cout << "data block load cnt " << size_ << std::endl;
        index += sizeof(size_t);
        data_index_.reserve(size_);
        for (size_t i = 0 ; i < size_; i++) {
            EntryIndex entry_index;
            index += entry_index.Load(s + index, index);
            data_index_.emplace_back(std::move(entry_index));
        }
        return index;
    }
    size_t binary_size() {
        if (cached_binary_size_ == -1) {
            size_t size = 2 * sizeof(size_t);
            for (auto& entry : data_index_) {
                size += entry.binary_size();
            }
            cached_binary_size_ = size;
        }
        return cached_binary_size_;
    }
    bool Get(std::string_view key, std::string& value) {
        if (!bloom_filter_.Check(key.data(), key.size())) {
            return false;
        }
        {
            size_t l = 0, r = data_index_.size();
            while (l < r) {
                size_t mid = (l + r) >> 1;
                if (data_index_[mid].key < key) {
                    l = mid + 1;
                } else {
                    r = mid;
                }
            }
            if (r == data_index_.size() || data_index_[r].key != key) {
                return false;
            } else {
                value = data_index_[r].value; // copy
                return true;
            }
        }
        return false;
    };

    std::vector<EntryIndex>& data_index() {
        return data_index_;
    }
private:
    size_t offset_;
    std::vector<EntryIndex> data_index_;
    easykv::common::BloomFilter bloom_filter_;
    size_t cached_binary_size_ = -1;
    size_t size_;
};

class DataBlockIndexIndex {
public:
    DataBlockIndex& Get() {
        return data_block_index_;
    }

    size_t Load(char* s, char* data) {
        offset_ = *reinterpret_cast<size_t*>(s);
        key_ = std::string_view(s + 2 * sizeof(size_t), *reinterpret_cast<size_t*>(s + sizeof(size_t)));
        // std::cout << " key " << key_ << " offset " << offset_ << std::endl;
        data_block_index_.Load(data, offset_);
        return 2 * sizeof(size_t) + key_.size();
    }

    const std::string_view key() const {
        return key_;
    }
private:
    size_t offset_;
    std::string_view key_;
    DataBlockIndex data_block_index_;
    bool data_block_loaded_ = false;
};

struct EntryView {
    EntryView(std::string& k, std::string& v): key(k), value(v) {
        
    }
    EntryView(std::string_view k, std::string_view v): key(k), value(v) {
        
    }
    std::string_view key;
    std::string_view value;
};

class IndexBlockIndex {
public:
    size_t Load(char* s) {
        size_t index = 0;
        binary_size_ = *reinterpret_cast<size_t*>(s);
        index += sizeof(size_t);
        size_ = *reinterpret_cast<size_t*>(s + index);
        index += sizeof(size_t);
        data_block_indexs_.reserve(size_);
        // std::cout << " binary size " << binary_size_ << " size " << size_ << std::endl;
        for (size_t i = 0; i < size_; i++) {
            DataBlockIndexIndex data_block_index_index;
            index += data_block_index_index.Load(s + index, s);
            data_block_indexs_.emplace_back(std::move(data_block_index_index));
        }
        return index;
    }
    
    size_t data_block_size() {
        return data_block_indexs_.size();
    }

    bool Get(std::string_view key, std::string& value) {
        // 1.find data_block
        // 2.search data_block
        {
            size_t l = 0, r = data_block_indexs_.size();
            while (l < r) {
                size_t mid = (l + r) >> 1;
                if (data_block_indexs_[mid].key() > key) {
                    r = mid;
                } else {
                    l = mid + 1;
                }
            }
            if (r != 0) {
                return data_block_indexs_[r - 1].Get().Get(key, value);
            }
        }
        return false;
    }

    const std::string_view key() const {
        return data_block_indexs_.begin()->key();
    }

    std::vector<DataBlockIndexIndex>& data_block_index() {
        return data_block_indexs_;
    }
private:
    size_t binary_size_;
    size_t size_{0};
    std::vector<DataBlockIndexIndex> data_block_indexs_;
};

// no empty sst
class SST {
public:
    class Iterator {
    public:
        Iterator(SST* sst, bool rbegin = false): sst_(sst) {
            if (rbegin) {
                data_block_index_it_ = sst_->data_block_index().end();
                --data_block_index_it_;
                data_block_entry_it_ = data_block_index_it_->Get().data_index().end();
                --data_block_entry_it_;
            } else {
                data_block_index_it_ = sst_->data_block_index().begin();
                data_block_entry_it_ = data_block_index_it_->Get().data_index().begin();
            }
        }

        EntryIndex& operator * () {
            return *data_block_entry_it_;
        }

        bool operator ! () {
            return data_block_index_it_ == sst_->data_block_index().end();
        }

        Iterator& operator ++ () {
            ++data_block_entry_it_;
            if (data_block_entry_it_ == data_block_index_it_->Get().data_index().end()) {
                ++data_block_index_it_;
                data_block_entry_it_ = data_block_index_it_->Get().data_index().begin();
            }
            return *this;
        }
    private:
        std::vector<DataBlockIndexIndex>::iterator data_block_index_it_;
        std::vector<EntryIndex>::iterator data_block_entry_it_;
        SST* sst_;
    };

    SST() {}

    SST(std::vector<EntryView> entries, int id) {

        common::BloomFilter bloom_filter;
        bloom_filter.Init(entries.size(), 0.01);
        size_t data_block_size = sizeof(size_t); // data_block_size
        size_t index_block_size = 2 * sizeof(size_t) * (entries.size() + 1);
        index_block_size += (*entries.begin()).key.size();
        for (auto it = entries.begin(); it != entries.end(); ++it) {
            bloom_filter.Insert((*it).key.data(), (*it).key.size());
            data_block_size += (*it).key.size() + (*it).value.size();
        }
        data_block_size += entries.size() * 2 * sizeof(size_t);
        data_block_size += bloom_filter.binary_size();
        data_block_size += sizeof(size_t); // cnt

        file_size_ = index_block_size + data_block_size;
        
        id_ = id;
        name_ = std::to_string(id_) + ".sst";
        // std::cout << "open file " << std::endl;
        fd_ = open(name_.c_str(), O_RDWR);
        if (fd_ == -1) {
            fd_ = open(name_.c_str(), O_RDWR | O_CREAT, 0700);
        }

        lseek(fd_, file_size_ - 1, SEEK_SET);
        write(fd_, "1", 1);

        // std::cout << " fd is " << fd_ << " size is " << file_size_ << std::endl;
        data_ = (char*)mmap(NULL, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        // std::cout << " create data " << std::endl;
        char* index_block_ptr = data_;
        char* data_block_ptr = data_ + index_block_size;
        size_t index_block_index = 0;
        size_t data_block_index = 0;
        // std::cout << "assign " << std::endl;
        // memcpy(data_, &index_block_size, 4);
        *reinterpret_cast<size_t*>(index_block_ptr) = index_block_size;
        std::cout << " index block size " << index_block_size << std::endl;
        std::cout << " data block size " << data_block_size << std::endl;
        std::cout << " bloom filter size " << bloom_filter.binary_size() << std::endl;
        index_block_index += sizeof(size_t);
        // std::cout << "assign 2" << std::endl;
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = 1; // cnt
        index_block_ptr += sizeof(size_t);
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = index_block_size; // offset
        index_block_index += sizeof(size_t);
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = (*entries.begin()).key.size(); // key size
        index_block_index += sizeof(size_t);
        memcpy(index_block_ptr + index_block_index, (*entries.begin()).key.data(), (*entries.begin()).key.size());

        // std::cout << "memcpy index_block" << std::endl;
        // std::cout << "offset " << data_block_ptr - data_ << std::endl;
        *reinterpret_cast<size_t*>(data_block_ptr) = data_block_size;
        data_block_index += sizeof(size_t);
        data_block_index += bloom_filter.Save(data_block_ptr + data_block_index);
        *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = entries.size();
        data_block_index += sizeof(size_t);
        for (auto it = entries.begin(); it != entries.end(); ++it) {
            *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = (*it).key.size();
            data_block_index += sizeof(size_t);
            *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = (*it).value.size();
            data_block_index += sizeof(size_t);
            memcpy(data_block_ptr + data_block_index, (*it).key.data(), (*it).key.size());
            data_block_index += (*it).key.size();
            memcpy(data_block_ptr + data_block_index, (*it).value.data(), (*it).value.size());
            data_block_index += (*it).value.size();
        }

        for (int i = 0; i < file_size_; i++) {
            std::cout << int(data_[i]) << " ";
        } std::cout << std::endl;

        // std::cout << "load index" << std::endl;

        index_block.Load(data_);

        // std::cout << " finish loaded " << std::endl;
        loaded_ = true;
        ready_ = true;
    }

    SST(MemeTable& memtable, size_t id) {
        // std::cout << "make sst " << id << std::endl;
        // 每个 memtable 保证了小于 pagesize，切分留到 compaction 做
        common::BloomFilter bloom_filter;
        bloom_filter.Init(memtable.size(), 0.01);
        size_t data_block_size = sizeof(size_t); // data_block_size
        size_t index_block_size = 2 * sizeof(size_t) * (memtable.size() + 1);
        index_block_size += (*memtable.begin()).key.size();
        std::cout << (*memtable.begin()).key.size() << " " << (*memtable.begin()).key << std::endl;
        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            bloom_filter.Insert((*it).key.c_str(), (*it).key.size());
            data_block_size += (*it).key.size() + (*it).value.size();
        }
        data_block_size += memtable.size() * 2 * sizeof(size_t);
        data_block_size += bloom_filter.binary_size();
        data_block_size += sizeof(size_t); // cnt

        file_size_ = index_block_size + data_block_size;
        
        id_ = id;
        name_ = std::to_string(id_) + ".sst";
        // std::cout << "open file " << std::endl;
        fd_ = open(name_.c_str(), O_RDWR);
        if (fd_ == -1) {
            fd_ = open(name_.c_str(), O_RDWR | O_CREAT, 0700);
        }

        lseek(fd_, file_size_ - 1, SEEK_SET);
        write(fd_, "1", 1);

        // std::cout << " fd is " << fd_ << " size is " << file_size_ << std::endl;
        data_ = (char*)mmap(NULL, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        // std::cout << " create data " << std::endl;
        char* index_block_ptr = data_;
        char* data_block_ptr = data_ + index_block_size;
        size_t index_block_index = 0;
        size_t data_block_index = 0;
        // std::cout << "assign " << std::endl;
        // memcpy(data_, &index_block_size, 4);
        *reinterpret_cast<size_t*>(index_block_ptr) = index_block_size;
        std::cout << " index block size " << index_block_size << std::endl;
        std::cout << " data block size " << data_block_size << std::endl;
        std::cout << " bloom filter size " << bloom_filter.binary_size() << std::endl;
        index_block_index += sizeof(size_t);
        // std::cout << "assign 2" << std::endl;
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = 1; // cnt
        index_block_ptr += sizeof(size_t);
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = index_block_size; // offset
        index_block_index += sizeof(size_t);
        *reinterpret_cast<size_t*>(index_block_ptr + index_block_index) = (*memtable.begin()).key.size(); // key size
        index_block_index += sizeof(size_t);
        memcpy(index_block_ptr + index_block_index, (*memtable.begin()).key.c_str(), (*memtable.begin()).key.size());

        // std::cout << "memcpy index_block" << std::endl;
        // std::cout << "offset " << data_block_ptr - data_ << std::endl;
        *reinterpret_cast<size_t*>(data_block_ptr) = data_block_size;
        data_block_index += sizeof(size_t);
        data_block_index += bloom_filter.Save(data_block_ptr + data_block_index);
        *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = memtable.size();
        data_block_index += sizeof(size_t);
        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = (*it).key.size();
            data_block_index += sizeof(size_t);
            *reinterpret_cast<size_t*>(data_block_ptr + data_block_index) = (*it).value.size();
            data_block_index += sizeof(size_t);
            memcpy(data_block_ptr + data_block_index, (*it).key.c_str(), (*it).key.size());
            data_block_index += (*it).key.size();
            memcpy(data_block_ptr + data_block_index, (*it).value.c_str(), (*it).value.size());
            data_block_index += (*it).value.size();
        }

        for (int i = 0; i < file_size_; i++) {
            std::cout << int(data_[i]) << " ";
        } std::cout << std::endl;

        // std::cout << "load index" << std::endl;

        index_block.Load(data_);

        // std::cout << " finish loaded " << std::endl;
        loaded_ = true;
        ready_ = true;
    }

    Iterator begin() {
        return Iterator(this);
    }

    Iterator rbegin() { // can NOT move
        return Iterator(this, true);
    }

    size_t id() const {
        return id_;
    }

    ~SST() {
        // std::cout << " ~SST " << std::endl;
        if (ready_) {
            Close();
        }
    }

    bool ready() {
        return ready_;
    }

    bool IsLoaded() {
        return loaded_;
    }

    size_t binary_size() {
        return file_size_;
    }

    void SetId(int id) {
        id_ = id;
        name_ = std::to_string(id_) + ".sst";
    }
    bool Load() {
        fd_ = open(name_.c_str(), O_RDWR);
        struct stat stat_buf;
        stat(name_.c_str(), &stat_buf);
        file_size_ = stat_buf.st_size;
        // std::cout << "file size " << file_size_ << std::endl;
        data_ = (char*)mmap(NULL, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (!data_) {
            return false;
        }
        index_block.Load(data_);
        loaded_ = true;
        ready_ = true;
        return true;
    }

    void Close() {
        if (!loaded_) {
            return;
        }
        // std::cout << " in close " << std::endl;
        munmap(data_, file_size_);
        // std::cout << "finish munmap" << std::endl;
        if (fd_ != -1) {
            close(fd_);
        }
        ready_ = false;
        loaded_ = false;
    }

    const std::string_view key() const {
        return index_block.key();
    }

    bool Get(std::string_view key, std::string& value) {
        return index_block.Get(key, value);
    }

    std::vector<DataBlockIndexIndex>& data_block_index() {
        return index_block.data_block_index();
    }
private:
    bool ready_ = false;
    int64_t id_ = 0;
    std::string name_;
    char* data_;
    int fd_ = -1;
    IndexBlockIndex index_block;
    bool loaded_ = false;
    size_t file_size_ = 0;
};


}
}