#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "easykv/lsm/manifest.hpp"
#include "easykv/lsm/memtable.hpp"
#include "easykv/lsm/sst.hpp"
#include "easykv/pool/thread_pool.hpp"
#include "easykv/utils/lock.hpp"

namespace easykv {

class DB {
public:
    DB() {
        memtable_ = std::make_shared<lsm::MemeTable>();
        to_sst_thread_ = std::thread(&DB::ToSSTLoop, this);
        manifest_queue_.emplace_back(std::make_shared<lsm::Manifest>());
        sst_id_ = manifest_queue_.back()->max_sst_id();
    }

    ~DB() {
        {
            easykv::common::RWLock::WriteLock w_lock(memtable_lock_);
            if (memtable_->size() > 0) {
                inmemtables_.emplace_back(memtable_);
            }
        }
        {
            std::unique_lock<std::mutex> lock(to_sst_mutex_);
            to_sst_stop_flag_ = true;
            to_sst_cv_.notify_all();
        }
        if (to_sst_thread_.joinable()) {
            to_sst_thread_.join();
        }
        {
            easykv::common::RWLock::WriteLock w_lock(manifest_lock_);
            manifest_queue_.back()->Save();
        }
    }

    bool Get(std::string_view key, std::string& value) {
        {
            easykv::common::RWLock::ReadLock r_lock(memtable_lock_);
            if (memtable_->Get(key, value)) {
                return true;
            }
            for (auto it = inmemtables_.rbegin(); it != inmemtables_.rend(); ++it) {
                if ((*it)->Get(key, value)) {
                    return true;
                }
            }
        }
        {
            easykv::common::RWLock::ReadLock r_lock(manifest_lock_);
            return manifest_queue_.back()->Get(key, value);
        }
        return false;
    }

    void Put(std::string_view key, std::string_view value) {
        memtable_->Put(key, value);
        if (memtable_->binary_size() > memetable_max_size_) {
            easykv::common::RWLock::WriteLock w_lock(memtable_lock_);
            inmemtables_.emplace_back(memtable_);
            memtable_ = std::make_shared<easykv::lsm::MemeTable>();
        }
    }
private:
    void ToSSTLoop() {
        while (true) {
            std::unique_lock<std::mutex> lock(to_sst_mutex_);
            to_sst_cv_.wait(lock);
            while (true) {
                bool to_sst = false;
                {
                    easykv::common::RWLock::ReadLock r_lock(memtable_lock_);
                    if (inmemtables_.size() > 0) {
                        to_sst = true;
                    }
                }
                if (to_sst) {
                    ToSST();
                } else {
                    break;
                }

            }
            if (to_sst_stop_flag_) {
                break;
            }
        }
    }
    void ToSST() {
        // inmemtables_ must > 0
        std::shared_ptr<lsm::MemeTable> inmemtable;
        {
            easykv::common::RWLock::ReadLock r_lock(memtable_lock_);
            inmemtable = *inmemtables_.begin();
        }
        
        auto sst = std::make_shared<lsm::SST>(*inmemtable, ++sst_id_);
        
        {
            easykv::common::RWLock::WriteLock w_lock(manifest_lock_);
            auto new_manifest = manifest_queue_.back()->InsertAndUpdate(sst);
            if (new_manifest->CanDoCompaction()) {
                new_manifest->SizeTieredCompaction(++sst_id_);
            }
            manifest_queue_.emplace_back(new_manifest);
        }

        {
            easykv::common::RWLock::WriteLock w_lock(memtable_lock_);
            std::vector<std::shared_ptr<easykv::lsm::MemeTable> > new_inmemtables;
            new_inmemtables.reserve(inmemtables_.size() - 1);
            for (auto it = inmemtables_.begin() + 1; it != inmemtables_.end(); ++it) {
                new_inmemtables.emplace_back(*it);
            }
            inmemtables_ = std::move(new_inmemtables);
        }
    }

private:
private:
    constexpr static size_t memetable_max_size_ = 4096 * 1024 - 1024 * 1024; // <= 4kb(page size)
    std::shared_ptr<easykv::lsm::MemeTable> memtable_;
    std::vector<std::shared_ptr<easykv::lsm::MemeTable> > inmemtables_;
    std::vector<std::shared_ptr<easykv::lsm::Manifest> > manifest_queue_;
    easykv::common::RWLock manifest_lock_;
    easykv::common::RWLock memtable_lock_;

    std::thread to_sst_thread_;
    std::mutex to_sst_mutex_;
    std::condition_variable to_sst_cv_;
    bool to_sst_stop_flag_ = false;

    std::atomic_size_t sst_id_{0};
};

}