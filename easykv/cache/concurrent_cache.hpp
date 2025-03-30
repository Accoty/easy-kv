#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/unordered/concurrent_flat_map.hpp>
#include "easykv/cache/list.hpp"
namespace cpputil {

namespace cache {

class Cache {
public:
    template <typename T>
    Cache(T&& name) {
        name_ = std::forward<T>(name);
    }
    const std::string& name() {
        return name_;
    }
protected:
    std::string name_;
};

template <typename TKey, typename TValue = TKey, typename TMap = boost::concurrent_flat_map<TKey, TValue> >
class ConcurrentLRUCache {
    using TNode = cpputil::list::Node<std::shared_ptr<TValue> >;

    template <typename T>
    struct PassBy { // use & for > 16 bit size object
        using type = typename std::conditional<(sizeof(T) > (sizeof(void*) << 1)) || !std::is_trivially_copyable_v<T>, T&, T>::type;
    };
    // struct HashCompare {
    //     static size_t Hash(const typename PassBy<TKey>::type key) {
    //         return THash()(key);
    //     }
    //     static bool Equal(const typename PassBy<TKey>::type lhs, const typename PassBy<TKey>::type rhs) {
    //         return TKeyEqual()(lhs, rhs);
    //     }
    // };
private:
    inline bool ShouldPromote(TNode* node) { // atomic
        return node->promotions.fetch_add(1, std::memory_order_acq_rel) >= should_promote_num_;
    }

    inline void ResetPromote(TNode* node) {
        node->promotions.store(0, std::memory_order_acq_rel);
    }

    inline void PromoteNoLock(TNode* node) {
        list_.Extract(node);
        list_.InsertFront(node);
        ResetPromote(node);
    }

    inline void Promote(TNode* node) noexcept {
        // std::unique_lock<std::mutex> lock(list_mutex_);
        if (ShouldPromote(node)) { // atomic
            // std::unique_lock<std::mutex> lock(list_mutex_);
            PromoteNoLock(node);
        }
    }
public:

    ConcurrentLRUCache(size_t capacity = 24, size_t should_promote_num = 8): capacity_(capacity), should_promote_num_(should_promote_num) {

    }

    void Reserve(size_t capacity) {
        this->capacity_ = capacity;
        map_.reserve(capacity);
    }

    void Put(const typename PassBy<TValue>::type value) {
        bool exist = false;
        {
            std::unique_lock<std::mutex> lock(list_mutex_);
            map_.visit(static_cast<TKey>(value), [&](auto& x) {
                Promote(x.second);
                exist = true;
            });
        }

        if (exist) {
            return;
        }

        TNode* node_ptr;
        {
            std::unique_lock<std::mutex> lock(list_mutex_);
            // std::unique_lock<std::mutex> lock(list_mutex_); // setting the same value concurrently produces garbage data 
            if (list_.size() == capacity_) {
                auto node_ptr = list_.PopBack();
                map_.erase(static_cast<TKey>(*(node_ptr->value)));
            }
            auto value_ptr = std::make_shared<TValue>(value);
            node_ptr = list_.PushFront(std::move(value_ptr));
            map_.emplace(static_cast<TKey>(value), node_ptr);
        }
    }

    
    template <typename U = TValue, typename std::enable_if<!std::is_same_v<U, typename PassBy<TValue>::type>, int>::type = 0>
    void Put(TValue&& value) {
        bool exist = false;
        {
            std::unique_lock<std::mutex> lock(list_mutex_);
            map_.visit(static_cast<TKey>(value), [&](auto& x) {
                // TODO optimize protomote
                Promote(x.second);
                exist = true;
            });
        }

        if (exist) {
            return;
        }

        TNode* node_ptr;
        {
            std::unique_lock<std::mutex> lock(list_mutex_); // setting the same value concurrently produces garbage data 
            if (list_.size() == capacity_) {
                auto node_ptr = list_.PopBack();
                map_.erase(static_cast<TKey>(*(node_ptr->value)));
            }
            auto value_ptr = std::make_shared<TValue>(std::move(value));
            node_ptr = list_.PushFront(std::move(value_ptr));
        }
        map_.emplace(static_cast<TKey>(*(node_ptr->value)), node_ptr);
    }

    std::shared_ptr<TValue> Get(const typename PassBy<TKey>::type key) {
        std::shared_ptr<TValue> res;
        {
            std::unique_lock<std::mutex> lock(list_mutex_);
            map_.visit(key, [&](const auto& x) {
                res = x.second->value;
                Promote(x.second);
            });
        }
        return res;
    }

    std::shared_ptr<TValue> Peek(const typename PassBy<TKey>::type key) {
        std::shared_ptr<TValue> res;
        map_.visit(key, [&](const auto& x) {
            res = x.second->value;
        });
        return res;
    }

    size_t TrueSize() const {
        return list_.size();
    }

    size_t size() const {
        return map_.size();
    }
private:
    size_t capacity_;
    size_t should_promote_num_;
    boost::concurrent_flat_map<TKey, TNode*> map_;
    cpputil::list::List<std::shared_ptr<TValue> > list_;
    std::mutex list_mutex_;
};

// need Value to Key conversion constructor
template <typename TKey, typename TValue = TKey, typename TMap = boost::concurrent_flat_map<TKey, TValue>,
    size_t ShardBits = 6, typename THash = std::hash<TKey> >
class ConcurrentBucketLRUCache : public Cache {
    constexpr const static size_t shard_num_ = 1 << ShardBits;
    constexpr const static size_t shard_mask_ = shard_num_ - 1;
    using TShard = ConcurrentLRUCache<TKey, TValue, TMap>;

    template <typename T>
    struct PassBy { // use & for > 16 bit size object
        using type = typename std::conditional<(sizeof(T) > (sizeof(void*) << 1)) || !std::is_trivially_copyable_v<T>, T&, T>::type;
    };
private:
    TShard& GetShard(const typename PassBy<TKey>::type key) {
        auto index = THash()(key) & shard_mask_;
        return shard_[index];
    }
    
public:
    ConcurrentBucketLRUCache(const std::string& name, size_t capacity = 1024): Cache(name) {
        capacity_ = capacity;
        for (size_t i = 0; i < shard_num_; i++) {
            shard_[i].Reserve((capacity >> ShardBits) + 1);
        }
    }
    
    void Put(const typename PassBy<TValue>::type value) {
        auto& shard= GetShard(TKey(value));
        shard.Put(value);
    }


    template <typename U = TValue, typename std::enable_if<!std::is_same_v<U, typename PassBy<TValue>::type>, int>::type = 0>
    void Put(TValue&& value) {
        auto& shard = GetShard(TKey(value));
        shard.Put(std::move(value));
    }

    std::shared_ptr<TValue> Get(const typename PassBy<TKey>::type key) {
        auto& shard = GetShard(key);
        return shard.Get(key);
    }

    std::shared_ptr<TValue> Peek(const typename PassBy<TKey>::type key) {
        auto& shard = GetShard(key);
        return shard.Peek(key);
    }

private:
    size_t capacity_;
    std::array<TShard, shard_num_> shard_;
};

} // cache

} // cpputil