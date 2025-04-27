#pragma once
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "easykv/utils/global_random.h"
namespace easykv {
namespace common {
/*
BloomFilter in file:
[seed_num(4byte) | length(4byte) | (seed (4byte))... | (int64 对齐) data (ceil(length / 4) byte)]
*/

class BloomFilter {
public:
    size_t binary_size() {
        size_t index = seed_.size() * sizeof(size_t) + 2 * sizeof(size_t);
        size_t align = sizeof(uint64_t) - (index & (sizeof(uint64_t) - 1));
        return seed_.size() * sizeof(size_t) + 2 * sizeof(size_t) + size_ * sizeof(uint64_t)
            + align;
    }
    size_t length() {
        return length_;
    }
    size_t Load(char* s) {
        size_t index = 0;
        hash_num_ = *reinterpret_cast<size_t*>(s);
        index += sizeof(size_t);
        length_ = *reinterpret_cast<size_t*>(s + index);
        index += sizeof(size_t);
        seed_.clear();
        seed_.reserve(hash_num_);
        for (size_t i = 0; i < hash_num_; i++) {
            seed_.emplace_back(*reinterpret_cast<size_t*>(s + index));
            index += sizeof(size_t);
        }
        size_ = length_ / 64 + 1;
        index += sizeof(uint64_t) - (index & (sizeof(uint64_t) - 1)); // 对齐
        data_ = reinterpret_cast<uint64_t*>(s + index);
        index += size_ * sizeof(int64_t);
        loaded_ = true;
        return index;
    }
    size_t Save(char* s) {
        size_t index = 0;
        *reinterpret_cast<size_t*>(s) = hash_num_;
        index += sizeof(size_t);
        *reinterpret_cast<size_t*>(s + index) = length_;
        index += sizeof(size_t);
        for (size_t i = 0; i < hash_num_; i++) {
            *reinterpret_cast<size_t*>(s + index) = seed_[i];
            index += sizeof(size_t);
        }
        index += sizeof(uint64_t) - (index & (sizeof(uint64_t) - 1)); // 对齐
        memcpy(s + index, reinterpret_cast<char*>(data_), size_ * sizeof(uint64_t));
        return index + size_ * sizeof(uint64_t);
    }
    void Init(size_t n, double p) {
        length_ = CalcLength(n, p);
        hash_num_ = std::max(1, int(0.69 * length_ / n));
        seed_.reserve(hash_num_);
        for (size_t i = 0; i < hash_num_; i++) {
            seed_.emplace_back(cpputil::common::GlobalRand());
        }
        size_ = length_ / 64 + 1;
        data_ = new uint64_t[length_];
    }
    void Insert(const char* s, size_t len) {
        for (auto seed : seed_) {
            size_t key = CalcHash(s, len, seed) % length_;
            data_[key / 64] |= 1 << (key & 63);
        }
    }
    bool Check(const char* s, size_t len) {
        for (auto seed : seed_) {
            size_t key = CalcHash(s, len, seed) % length_;
            if (!(data_[key / 64] & (1 << (key & 63)))) {
                return false;
            }
        }
        return true;
    }
    ~BloomFilter() {
        if (!loaded_) {
            delete[] data_;
        }
    }
private:
    size_t CalcHash(const char* s, size_t len, size_t seed) {
        size_t res = 0;
        for (size_t i = 0; i < len; i++) {
            res *= seed;
            res += static_cast<size_t>(s[i]);
        }
        return res;
    }
    size_t CalcLength(size_t n, double p) {
        return int(-std::log(p) * double(n) / std::log(2) / std::log(2) * 2.35) + 1;
    }
    
private:
    size_t length_;
    size_t hash_num_;
    std::vector<size_t> seed_;
    uint64_t* data_ = nullptr;
    size_t size_;
    bool loaded_ = false;
};

}
}