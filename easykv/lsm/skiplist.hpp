#pragma once
#include <atomic>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <stack>

#include <iostream>

#include "easykv/utils/lock.hpp"
#include "easykv/utils/global_random.h"

namespace easykv {
namespace lsm {

struct Node {
    Node() {}
    Node(std::string_view key, std::string_view value, size_t size) {
        this->key = key;
        this->value = value;
        nexts.resize(size, nullptr);
    }
    std::vector<Node*> nexts;
    std::string key;
    std::string value;
    easykv::common::RWLock rw_lock;
};

class ConcurrentSkipList {
public:
    class Iterator {
    public:
        Iterator(Node* rhs) {
            it_ = rhs;
        }

        Iterator& operator ++() {
            it_ = it_->nexts[0];
            return *this;
        }

        Node& operator *() {
            return *it_;
        }

        bool operator ==(const Iterator& rhs) {
            return it_ == rhs.it_;
        }

        bool operator !=(const Iterator& rhs) {
            return it_ != rhs.it_;
        }

    private:
        Node* it_;
    };

    ConcurrentSkipList() {
        head_ = new Node();
        head_->nexts.resize(1);
    }

    Iterator begin() {
        return Iterator(head_->nexts[0]);
    }

    Iterator end() {
        return Iterator(nullptr);
    }

    size_t size() {
        return size_;
    }

    size_t binary_size() {
        return binary_size_;
    }

    bool Get(std::string_view key, std::string& value) {
        easykv::common::RWLock::ReadLock lock(delete_rw_lock_);

        auto p = head_;
        Node* last = nullptr;
        std::vector<easykv::common::RWLock::ReadLock> level_locks;
        level_locks.reserve(head_->nexts.size());
        for (int level = head_->nexts.size() - 1; level >= 0; level--) {
            while (p->nexts[level] && p->nexts[level]->key < key) {
                p = p->nexts[level];
            }
            if (p != last) {
                level_locks.emplace_back(easykv::common::RWLock::ReadLock(p->rw_lock));
                last = p;
            }
            if (p->nexts[level] && p->nexts[level]->key == key) {
                value = p->nexts[level]->value;
                return true;
            }
        }
        return false;
    }
    
    // 一写多读
    void Put(std::string_view key, std::string_view value) {
        // // std::cout << "put " << key << " " << value << std::endl;
        easykv::common::RWLock::ReadLock lock(delete_rw_lock_);

        {
            auto p = head_;
            Node* last = nullptr;
            std::vector<easykv::common::RWLock::WriteLock> level_locks;
            level_locks.reserve(head_->nexts.size());
            for (int level = head_->nexts.size() - 1; level >= 0; level--) {
                while (p->nexts[level] && p->nexts[level]->key < key) {
                    p = p->nexts[level];
                }
                if (p != last) {
                    level_locks.emplace_back(easykv::common::RWLock::WriteLock(p->rw_lock));
                    last = p;
                }
                if (p->nexts[level] && p->nexts[level]->key == key) {
                    binary_size_ += value.size();
                    binary_size_ -= p->nexts[level]->value.size();
                    p->nexts[level]->value = value; // string_view ->(copy) string
                    return;
                }
            }
        }
        auto new_level = RandLevel();
        auto node = new Node(key, value, new_level);
        easykv::common::RWLock::WriteLock(node->rw_lock);
        ++size_;
        binary_size_ += key.size() + value.size();
        if (new_level > head_->nexts.size()) {
            easykv::common::RWLock::WriteLock _lock(head_->rw_lock);
            head_->nexts.resize(new_level, nullptr);
        }
        {
            auto p = head_;
            Node* last = nullptr;
            std::vector<easykv::common::RWLock::WriteLock> level_locks;
            level_locks.reserve(head_->nexts.size());
            for (int level = head_->nexts.size() - 1; level >= 0; level--) {
                while (p->nexts[level] && p->nexts[level]->key < key) {
                    p = p->nexts[level];
                }
                if (p != last) {
                    level_locks.emplace_back(easykv::common::RWLock::WriteLock(p->rw_lock));
                    last = p;
                }
                if (level < new_level) {
                    node->nexts[level] = p->nexts[level];
                    p->nexts[level] = node;
                }
            }
        }
    }

    void Delete(std::string_view key) {
        easykv::common::RWLock::WriteLock lock(delete_rw_lock_);
        Node* delete_node = nullptr;
        auto p = head_;
        for (int level = p->nexts.size() - 1; level >= 0; level--) {
            while (p->nexts[level] && p->nexts[level]->key < key) {
                p = p->nexts[level];
            }
            if (!p->nexts[level] || p->nexts[level]->key != key) {
                continue;
            }
            delete_node = p->nexts[level];
            if (p->nexts[level]->nexts.size() > level) {
                p->nexts[level] = p->nexts[level]->nexts[level];
            } else {
                p->nexts[level] = nullptr;
            }
        }
        if (!delete_node) {
            return;
        }
        --size_;
        binary_size_ -= delete_node->key.size() + delete_node->value.size();
        delete delete_node;
        size_t remove_level_cnt = 0;
        for (size_t level = p->nexts.size() - 1; level >= 0; level--) {
            if (head_->nexts[level] == nullptr) {
                ++remove_level_cnt;
            } else {
                break;
            }
        }
        head_->nexts.resize(head_->nexts.size() - remove_level_cnt);
    }

private:
    size_t RandLevel() {
        size_t level = 1;
        while ((cpputil::common::GlobalRand() & 3) == 0) {
            ++level;
        }
        return level;
    }
private:
    std::atomic_size_t size_{0};
    std::atomic_size_t binary_size_{0};
    std::mutex lock_;
    easykv::common::RWLock delete_rw_lock_;
    Node* head_;
};


}
}