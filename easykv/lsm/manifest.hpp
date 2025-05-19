#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "easykv/lsm/skiplist.hpp"
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
                    std::cout << "level " << level_ << "add sst " << sst_id << std::endl;
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

        size_t binary_size() {
            size_t res = 0;
            for (auto& sst_ptr : ssts_) {
                res += sst_ptr->binary_size();
            }
            return res;
        }

        std::vector<std::shared_ptr<SST> >& ssts() {
            return ssts_;
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
            // std::cout << "Find in level " << i << std::endl;
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

    struct SizeTieredCompactionStruct {
        SizeTieredCompactionStruct(SST& sst, size_t value): it(sst.begin()), second_value(value) {}
        bool operator < (const SizeTieredCompactionStruct& rhs) const {
            if ((*const_cast<SizeTieredCompactionStruct*>(this)->it).key == (*const_cast<SizeTieredCompactionStruct&>(rhs).it).key) {
                return second_value > rhs.second_value;
            }
            return (*const_cast<SizeTieredCompactionStruct*>(this)->it).key > (*const_cast<SizeTieredCompactionStruct&>(rhs).it).key;
        }
        SST::Iterator it;
        size_t second_value;
    };

    void SizeTieredCompaction(size_t level, int id) {
        std::priority_queue<SizeTieredCompactionStruct> queue;
        // std::priority_queue<SizeTieredCompactionStruct, 
        //     std::vector<SizeTieredCompactionStruct>, 
        //         std::greater<SizeTieredCompactionStruct> > queue; // 小根堆
        size_t value = 0;
        std::string_view min_key;
        std::string_view max_key;
        for (auto it = levels_[level].ssts().rbegin(); it != levels_[level].ssts().rend(); ++it) {
            SizeTieredCompactionStruct data(**it, value++);
            queue.push(std::move(data));
            if (min_key.empty()) {
                min_key = (*(*it)->begin()).key;
            } else if ((*(*it)->begin()).key < min_key) {
                min_key = (*(*it)->begin()).key;
            }
            // std::cout << (*(*it)->rbegin()).key << " " << (*(*it)->begin()).key << std::endl;
            if (max_key.empty()) {
                max_key = (*(*it)->rbegin()).key;
            } else if ((*(*it)->rbegin()).key > max_key) {
                max_key = (*(*it)->rbegin()).key;
            }
        }
        if (level + 1 == levels_.size()) {
            levels_.emplace_back(Level(level + 1));
        }
        std::cout << min_key << " min max " << max_key << " " << levels_[level + 1].ssts().size() << std::endl;
        int insert_l = -1, insert_r = levels_[level + 1].ssts().size();
        for (auto i = 0; i < levels_[level + 1].ssts().size(); i++) {
            auto& sst_ptr = levels_[level + 1].ssts()[i];
        // for (auto it = levels_[level + 1].ssts().rbegin(); it != levels_[level + 1].ssts().rend(); ++it) {
            if ((*(sst_ptr->rbegin())).key < min_key) {
                insert_l = std::max(insert_l, i);
            } else if ((*(sst_ptr->begin())).key > max_key) {
                insert_r = std::min(insert_r, i);
            } else {
                SizeTieredCompactionStruct data(*sst_ptr, value++);
                queue.push(std::move(data));
            }
        }
        std::cout << insert_l << " insert lr " << insert_r << " " << std::endl;
        

        std::vector<EntryView> entrys;
        while (!queue.empty()) {
            auto data = queue.top();
            queue.pop();
            if (entrys.empty() || entrys.back().key != (*data.it).key) {
                // std::cout << "emplace " << (*data.it).key << " " << (*data.it).value << std::endl;
                entrys.emplace_back((*data.it).key, (*data.it).value);
            }
            if (!(++data.it)) {
                continue;
            } else {
                queue.push(data);
            }
        }

        auto new_sst_ptr = std::make_shared<SST>(entrys, id);
        if (id > max_sst_id_) {
            max_sst_id_ = id;
        }
        
        std::vector<std::shared_ptr<SST>> new_ssts;
        for (int i = 0; i <= insert_l; i++) {
            new_ssts.emplace_back(levels_[level + 1].ssts()[i]);
        }
        new_ssts.emplace_back(new_sst_ptr);
        for (int i = insert_r; i < levels_[level + 1].ssts().size(); i++) {
            new_ssts.emplace_back(levels_[level + 1].ssts()[i]);
        }
        levels_[level].ssts().clear();
        levels_[level + 1].ssts() = std::move(new_ssts);
    }

    void SizeTieredCompaction(int id) {
        for (int i = 0; i < levels_.size() && i < max_level_size_; i++) {
            if (levels_[i].binary_size() > level_max_binary_size_[i]) {
                SizeTieredCompaction(i, id);
            } else {
                break;
            }
        }
    }

    bool CanDoCompaction() {
        return levels_[0].binary_size() > level_max_binary_size_[0];
    }
private:
    constexpr static const size_t max_level_size_ = 5;
    constexpr static const size_t level_max_binary_size_[] = {1024, 10 * 1024 * 1024, 100 * 1024 * 1024, 1000 * 1024 * 1024, 10000ll * 1024 * 1024};
    constexpr static const char* name_ = "manifest";
    std::atomic_size_t count_{0};
    size_t version_;
    std::vector<Level> levels_;
    easykv::common::RWLock memtable_rw_lock_;
    size_t max_sst_id_ = 0;
};

}
}