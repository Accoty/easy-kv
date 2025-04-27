#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "easykv/lsm/sst.hpp"
#include "easykv/lsm/memtable.hpp"

namespace easykv {
namespace lsm {

/*
ManiFest in file
|version(size_t)|levels(size_t)|(id_0,id_1,-1)|(id_2,id_3,-1)|...
*/

class Manifest {
    class Level {
    public:
        Level(size_t level): level_(level) {

        }

        size_t Load(char* s, size_t& max_sst_id_) {
            size_t index = 0;
            size_t sst_id;
            while (true) {
                sst_id = *reinterpret_cast<size_t*>(s + index);
                index += sizeof(size_t);
                if (sst_id != -1) {
                    auto sst_ptr = std::make_shared<SST>();
                    sst_ptr->SetId(sst_id);
                    sst_ptr->Load();
                    ssts_.emplace_back(sst_ptr);
                    if (sst_id > max_sst_id_) {
                        max_sst_id_ = sst_id;
                    }
                    // std::cout << "level " << level_ << " add sst " << sst_id << std::endl;
                } else {
                    break;
                }
            }
            return index;
        }

        size_t Save(char* s) {
            size_t index = 0;
            for (auto sst_ptr : ssts_) {
                *reinterpret_cast<size_t*>(s + index) = sst_ptr->id();
                index += sizeof(size_t);
            }
            *reinterpret_cast<size_t*>(s + index) = -1;
            index += sizeof(size_t);
            return index;
        }

        bool Get(std::string_view key, std::string& value) {
            if (level_ == 0) {
                for (auto it = ssts_.rbegin(); it != ssts_.rend(); ++it) {
                    if ((*it)->Get(key, value)) {
                        return true;
                    }
                }
            } else {
                size_t l = 0, r = ssts_.size();
                while (l < r) {
                    size_t mid = (l + r) >> 1;
                    if (ssts_[mid]->key() > key) {
                        r = mid;
                    } else {
                        l = mid + 1;
                    }
                }
                if (r != 0) {
                    return ssts_[r - 1]->Get(key, value);
                }
            }
            return false;
        }

        void Insert(std::shared_ptr<SST> sst) {
            ssts_.emplace_back(std::move(sst));
        }

        size_t level() {
            return level_;
        }

        size_t size() const {
            return ssts_.size();
        }

    private:
        size_t level_;
        std::vector<std::shared_ptr<SST> > ssts_;
    };
public:
    Manifest() {
        auto fd = open(name_, O_RDWR);
        if (fd != -1) {
            struct stat stat_buf;
            stat(name_, &stat_buf);
            auto file_size = stat_buf.st_size;
            auto data = (char*)mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
            size_t index = 0;
            version_ = *reinterpret_cast<size_t*>(data);
            index += sizeof(size_t);
            auto size = *reinterpret_cast<size_t*>(data + index);
            index += sizeof(size_t);
            levels_.reserve(size);
            for (size_t i = 0; i < size; i++) {
                Level level(i);
                index += level.Load(data + index, max_sst_id_);
                levels_.emplace_back(std::move(level));
            }
            close(fd);
        } else {
            version_ = 1;
            levels_.emplace_back(0);
        }
    }

    size_t Save() {
        auto fd = open(name_, O_RDWR);
        if (fd == -1) {
            fd = open(name_, O_RDWR | O_CREAT, 0700);
        }
        // std::cout << "manifest save " << " fd " << fd << std::endl;
        auto file_size = binary_size();
        lseek(fd, file_size - 1, SEEK_SET);
        write(fd, "1", 1);

        auto data = (char*)mmap(NULL, file_size, PROT_WRITE, MAP_SHARED, fd, 0);
        size_t index = 0;
        *reinterpret_cast<size_t*>(data) = version_;
        index += sizeof(size_t);
        *reinterpret_cast<size_t*>(data + index) = levels_.size();
        index += sizeof(size_t);
        for (auto& level : levels_) {
            index += level.Save(data + index);
        }
        return index;
    }

    size_t binary_size() {
        size_t res = 2 * sizeof(size_t);
        for (auto& level : levels_) {
            res += (level.size() + 1) * sizeof(size_t);
        }
        return res;
    }

    Manifest(const Manifest& manifest) {
        version_ = manifest.version_ + 1;
        levels_ = manifest.levels_; // copy
    }

    bool Get(std::string_view key, std::string& value) {
        easykv::common::RWLock::ReadLock r_lock(memtable_rw_lock_);
        ++count_;
        for (size_t i = 0; i < levels_.size(); i++) {
            if (levels_[i].Get(key, value)) {
                return true;
            }
        }
        --count_;
        return false;
    }

    void Insert(std::shared_ptr<SST> sst) {
        levels_.begin()->Insert(sst);
    }

    std::shared_ptr<Manifest> InsertAndUpdate(std::shared_ptr<SST> sst) {
        if (sst->id() > max_sst_id_) {
            max_sst_id_ = sst->id();
        }
        auto res = std::make_shared<Manifest>(*this);
        res->Insert(sst);
        return res;
    }
    
    size_t max_sst_id() {
        return max_sst_id_;
    }
private:
    constexpr static const char* name_ = "manifest";
    std::atomic_size_t count_{0};
    size_t version_;
    std::vector<Level> levels_;
    easykv::common::RWLock memtable_rw_lock_;
    size_t max_sst_id_ = 0;
};

}
}