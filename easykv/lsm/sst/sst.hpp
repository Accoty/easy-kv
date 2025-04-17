#include <string>
#include <string_view>
#include <sys/mman.h>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/mman.h>

#include "easykv/utils/bloom_filter.hpp"

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
IndexBlock in file [size(4byte) | cnt(4byte) | (offset(4byte) + key_size(4byte) + key(key_size byte)), ...]
DataBlock in file [(size(4byte) | bloom_filter | cnt(4byte) | (key(key_size byte) + value(value_size byte) + key_size(4byte) + value_size(4byte)), ...]
SST in file [IndexBlock | (DataBlock...)]

DataBlcok 和 IndexBlock 都以文件形式访问，Memtable 边序列化边构建 DataBlock
*/

class DataBlockIndex {
public:
    void SetOffset(size_t offset) {
        offset_ = offset;
    }
    size_t Load(char* s, size_t offset = -1) {
        if (offset_ != -1) {
            offset_ = offset;
        }
        size_t index = offset_;
        cached_binary_size_ = *reinterpret_cast<size_t*>(s);
        index += sizeof(size_t);
        index += bloom_filter_.Load(s + index);
        size_ = *reinterpret_cast<size_t*>(s + index);
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
    std::string Get(std::string_view key) {
        return std::string();
    };
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
    size_t Load(char* s) {
        offset_ = *reinterpret_cast<size_t*>(s);
        key_ = std::string_view(s + 2 * sizeof(size_t), *reinterpret_cast<size_t*>(s + sizeof(size_t)));
        return 2 * sizeof(size_t) + key_.size();
    }
private:
    size_t offset_;
    std::string_view key_;
    DataBlockIndex data_block_index_;
    bool data_block_loaded_ = false;
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
        for (size_t i = 0; i < size_; i++) {
            DataBlockIndexIndex data_block_index_index;
            index += data_block_index_index.Load(s + index);
        }
        return index;
    }
private:
    size_t binary_size_;
    size_t size_{0};
    std::vector<DataBlockIndexIndex> data_block_indexs_;
};

class SST {
public:
    size_t binary_size() {

    }
    void SetId(int id) {
        id_ = id;
        name_ = std::to_string(id_) + ".sst";
    }
    bool Load() {
        int fd = open(name_.c_str(), O_RDWR);
        struct stat stat_buf;
        stat(name_.c_str(), &stat_buf);
        size_t file_size = stat_buf.st_size;
        data_ = (char*)mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
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
    }
private:
    bool ready_ = false;
    int64_t id_ = 0;
    std::string name_;
    char* data_;
    IndexBlockIndex index_block;
    bool loaded_ = false;
};


}
}