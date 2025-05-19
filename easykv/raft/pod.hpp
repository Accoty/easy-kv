#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <future>
#include <grpcpp/support/status.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "easykv/db.hpp"
#include "easykv/pool/thread_pool.hpp"
#include "easykv/raft/raft_log.hpp"
#include "easykv/raft/client.hpp"

#include "easykv/raft/protos/raft.grpc.pb.h"
#include "easykv/utils/global_random.h"

namespace easykv {
namespace raft {

enum class PodStatus {
    Candidate,
    Leader,
    Follower,
};

class Pod;

class Follower {
public:
    Follower(const Address& addr, RaftLog* log, int32_t ld_id): leader_id_(ld_id) {
        addr_ = addr;
        rpc_client_.SetIp(addr.ip());
        rpc_client_.SetPort(addr.port());
        rpc_client_.Connect();
        main_log_ = log;
        stop_ = false;
    }
    
    void SetOtherFollower(std::vector<std::shared_ptr<Follower> >& all_follower) {
        for (auto& follower : all_follower) {
            if (follower->id() != addr_.id()) {
                other_followers_.emplace_back(follower.get());
            }
        }
    }

    void Run() {
        Stop();
        stop_ = false;
        sync_thread_ = std::thread([this]() {
            while (true) {
                // sleep(3);
                usleep(10000);
                std::cout << "id " << id() << " nextindex " << nextindex_ << " leader_commit " << main_log_->commited() << " leader index " << main_log_->index() << std::endl;
                if (stop_) {
                    break;
                }
                if (nextindex_ < main_log_->index()) {
                    while (true) {

                    // usleep(10000);
                        sleep(3);
                        if (stop_) {
                            break;
                        }
                        AppendReq req;
                        AppendRsp rsp;
                        grpc::ClientContext ctx;
                        gpr_timespec timespec;
                        timespec.tv_sec = 2;
                        timespec.tv_nsec = 0;
                        timespec.clock_type = GPR_TIMESPAN;
                        ctx.set_deadline(timespec);
                        auto entry = req.add_entrys();
                        auto& main_entry = main_log_->At(nextindex_ + 1);
                        entry->set_allocated_key(new std::string(main_entry.key()));
                        entry->set_allocated_value(new std::string(main_entry.value()));
                        entry->set_index(nextindex_ + 1);
                        entry->set_commited(main_log_->commited());
                        entry->set_term(main_entry.term());
                        req.set_id(leader_id_);
                        std::cout << "send append " << nextindex_ + 1 << main_log_->commited() << " term " << main_entry.term() << std::endl;
                        auto code = rpc_client_.stub().Append(&ctx, req, &rsp);
                        if (code.ok()) {
                            if (rsp.base().code() != 0) {
                                --nextindex_;
                                std::cout << "move nextindex to " << nextindex_ << std::endl;
                            } else {
                                ++nextindex_;
                                std::cout << "match, nextindex is " << nextindex_ << std::endl;
                                break;
                            }
                        } else {
                            continue;
                        }
                    }
                    int sum = 2;
                    for (auto follower_ptr : other_followers_) {
                        if (follower_ptr->nextindex() >= nextindex_) {
                            ++sum;
                        }
                    }
                    if (sum >= (sum + other_followers_.size() + 1) / 2) {
                        main_log_->UpdateCommit(nextindex_);
                    }
                }
            }
        });
    }
    
    void Stop() {
        stop_ = true;
        if (sync_thread_.joinable()) {
            sync_thread_.join();
        }
    }

    Client& rpc_client() {
        return rpc_client_;
    }
    Address& addr() {
        return addr_;
    }
    int32_t id() {
        return addr_.id();
    }
    int64_t nextindex() {
        return nextindex_;
    }
    void SetNextindex(int64_t nextindex) {
        nextindex_ = nextindex;
    }
    void SendHeartBeat(int32_t term) {
        AppendReq req;
        AppendRsp rsp;
        grpc::ClientContext ctx;
        gpr_timespec timespec;
        timespec.tv_sec = 2;
        timespec.tv_nsec = 0;
        timespec.clock_type = GPR_TIMESPAN;
        ctx.set_deadline(timespec);
        req.set_commited_index(main_log_->commited());
        req.set_term(term);
        req.set_id(leader_id_);
        auto code = rpc_client_.stub().Append(&ctx, req, &rsp);
        if (!code.ok()) {
            rpc_client_.Reset();
        }
        // std::cout << rpc_client_.ip() << " code = " << code.ok() << " " << rsp.base().code() << std::endl;
    }
private:
    Client rpc_client_;
    Address addr_;
    std::atomic_int64_t nextindex_;
    RaftLog* main_log_;
    std::atomic_bool stop_;
    std::thread sync_thread_;
    int32_t leader_id_;
    std::vector<Follower*> other_followers_;
    Pod* pod_;
};

class Pod {
public:
    Pod(int32_t id, const Config& config, std::shared_ptr<easykv::DB> db): id_(id), raft_log_(db.get()) {
        db_ = db;
        followers_.reserve(config.addresses_size());
        for (auto& addr : config.addresses()) {
            followers_.emplace_back(std::make_shared<Follower>(addr, &raft_log_, id));
        }
        for (auto& follower : followers_) {
            follower->SetOtherFollower(followers_);
        }
        status_ = PodStatus::Follower;
        StartHeartBeatAndTimeOutRout();
    }

    ~Pod() {
        std::cout << "in ~Pod" << std::endl;
        election_thread_stop_flag_ = true;
        election_cv_.notify_all();
        if (election_thread_.joinable()) {
            election_thread_.join();
        }
        for (auto& follower : followers_) {
            std::cout << "Stop follower " << follower->id() << std::endl;
            follower->Stop();
        }
        std::cout << "next " << std::endl;
        
    }

    bool Vote(const RequestVoteReq& req) { // status = Follower
        std::cout << "in vote, local term " << term_ << " req term " << req.term() << std::endl;
        std::unique_lock<std::mutex> lock(election_mutex_);
        last_time_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
        if (req.term() < term_ || (req.term() == term_ && req.index() < raft_log_.index())) {
            return false;
        }
        if (req.term() == term_ && voted_) {
            return false;
        }
        if (req.term() > term_) {
            term_ = req.term();
            if (status_ == PodStatus::Leader) {
                for (auto& follower : followers_) {
                    follower->Stop();
                }
            }
            status_ = PodStatus::Follower;
        }
        voted_ = true;
        return true;
    }
    
    void Put(const PutReq& req, PutRsp& rsp) {
        if (GetPodStatusWithLock() != PodStatus::Leader) {
            raft::Base* base = new raft::Base;
            raft::Address* addr = nullptr;
            for (auto& follower : followers_) {
                if (follower->id() == leader_id_) {
                    addr = new raft::Address(follower->addr());
                }
            }
            if (!addr) {
                base->set_code(-1);
            } else {
                base->set_code(-2); // redirect code
                rsp.set_allocated_leader_addr(addr);
            }
            rsp.set_allocated_base(base);
            return;
        }
        if (!InnerPut(req.key(), req.value())) {
            auto base = new Base;
            base->set_code(-1);
            rsp.set_allocated_base(base);
        } else {

        }
    }

    void Get(const GetReq* req, GetRsp* rsp) {
        if (req->read_from_leader() && status_ != PodStatus::Leader) {
            raft::Base* base = new raft::Base;
            raft::Address* addr;
            for (auto& follower : followers_) {
                if (follower->id() == leader_id_) {
                    addr = new raft::Address(follower->addr());
                }
            }
            base->set_code(-2); // redirect code
            rsp->set_allocated_base(base);
            rsp->set_allocated_leader_addr(addr);
            return;
        }
        std::string value;
        bool res = db_->Get(req->key(), value);
        if (!res) {
            auto base = new raft::Base();
            base->set_code(1); // empty
        } else {
            rsp->set_value(value);
        }
    }

    int32_t SolveAppend(const AppendReq& req) {
        std::cout << "in solve append" << std::endl;
        UpdateLastTime();
        std::unique_lock<std::mutex> lock(solve_append_lock_);
        {
            std::unique_lock<std::mutex> lock(election_mutex_);
            if (req.term() > term_ || (req.term() == term_ && req.entrys().size() && req.entrys().Get(0).index() > raft_log_.index())) {
                term_ = req.term();
                voted_ = false;
                if (status_ == PodStatus::Leader) {
                    for (auto& follower : followers_) {
                        follower->Stop();
                    }
                }
                status_ = PodStatus::Follower;
                leader_id_ = req.id();
            }
            raft_log_.UpdateCommit(req.commited_index());
        }
        if (req.entrys_size()) {
            std::cout << "update raft log" << std::endl;
            auto code = UpdateRaftLog(req.entrys(), req.commited_index());
            return code;
        }
        return 0;
    }
    
private:
    bool InnerPut(const std::string& key, const std::string& value) {
        //TODO 用异步回调优化
        size_t now_idx;
        if (!raft_log_.Put(key, value, term_, now_idx)) {
            return false;
        }
        while (true) {
            usleep(10000);
            size_t sum = 1;
            for (auto& follower : followers_) {
                if (follower->nextindex() > now_idx) {
                    ++sum;
                }
            }
            if (sum > (1 + followers_.size() + 1) / 2) {
                break;
            }
        }
        return true;
    }

    void UpdateLastTime() {
        last_time_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
    }
    PodStatus GetPodStatusWithLock() {
        std::unique_lock<std::mutex> lock(election_mutex_);
        return status_;
    }
    bool RequestVote() { // status = Candidate
        ++term_;
        std::cout << "start request vote, term " << term_ << std::endl;
        size_t ticket_num = 1;
        for (auto& follower : followers_) {
            follower->rpc_client().PutTest();
            grpc::ClientContext ctx;
            gpr_timespec timespec;
            timespec.tv_sec = 2;
            timespec.tv_nsec = 0;
            timespec.clock_type = GPR_TIMESPAN;
            ctx.set_deadline(timespec);
            RequestVoteReq req;
            req.set_id(id_);
            req.set_term(term_);
            req.set_index(raft_log_.index());
            RequestVoteRsp rsp;
            {
                std::unique_lock<std::mutex> lock(election_mutex_);
                if (status_ != PodStatus::Candidate) {
                    return false;
                }
                auto status = follower->rpc_client().stub().RequestVote(&ctx, req, &rsp);
                if (!status.ok() || rsp.base().code() != 0) {
                    continue;
                } else {
                    ++ticket_num;
                }
            }
        }
        std::cout << "tikcet " << ticket_num << std::endl;
        {
            std::unique_lock<std::mutex> lock(election_mutex_);
            if (status_ != PodStatus::Candidate) {
                return false;
            }
            if (ticket_num > (1 + followers_.size()) / 2) {
                status_ = PodStatus::Leader;
                for (auto& follower : followers_) {
                    follower->SetNextindex(raft_log_.commited());
                    follower->Run();
                }
                return true;
            }
        }
        return false;
    }
private:

    int32_t UpdateRaftLog(const google::protobuf::RepeatedPtrField<::easykv::raft::Entry>& entries, size_t leader_commit) {
        std::promise<int32_t> promise;
        if (entries.size() == 1) {
            std::cout << " UpdateRaftLog in size 1 " << std::endl;
            auto entry = entries.Get(0);
            // if (entry)
            if (entry.index() != raft_log_.index() + 1) {
                std::cout << "index not match " << entry.index() << " " << raft_log_.index() << std::endl;
                if (leader_commit < raft_log_.index() && leader_commit > raft_log_.commited()) {
                    raft_log_.Reset(raft_log_.commited());
                }
                if (entry.index() != raft_log_.index() + 1) {
                    return -2;
                } else {
                    raft_log_.Put(entry);
                }
            } else {
                raft_log_.Put(entry);
            }
        } else {
            // -3 means not support
            return -3;
        }
        return 0;
    }

    void SendHeartBeat() {
        // std::cout << "send heart beat, status" << int(status_) << std::endl;
        for (auto follower : followers_) {
            follower->SendHeartBeat(term_);
        }
    }

    
    void StartHeartBeatAndTimeOutRout() {
        election_thread_ = std::thread([this]() {
            while (true) {
                if (cpputil::common::GlobalRand() % 10 == 0) {
                    std::cout << id_ << " is " << int(status_) << std::endl;
                }
                std::unique_lock<std::mutex> lock(election_thread_mutex_);
                if (GetPodStatusWithLock() != PodStatus::Leader) {
                    auto future_time = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_time_ms_);
                    election_cv_.wait_until(lock, future_time, [this]() {
                        return election_thread_stop_flag_;
                    });
                    std::cout << " select time out flag " << election_thread_stop_flag_ << std::endl;
                    if (election_thread_stop_flag_) {
                        break;
                    }
                    std::cout << "next " << static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count()) - last_time_ << std::endl;
                    if (static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count()) - last_time_ < timeout_time_ms_) {
                        std::cout << "not time out " << std::endl;
                        continue;
                    }
                    status_ = PodStatus::Candidate;
                    RequestVote();
                } else {
                    auto future_time = std::chrono::system_clock::now() + std::chrono::milliseconds(heart_beat_time_ms_);
                    election_cv_.wait_until(lock, future_time, [this]() {
                        return election_thread_stop_flag_;
                    });
                    if (election_thread_stop_flag_) {
                        break;
                    }
                    SendHeartBeat();
                }
            }
        });
    }

private:
    constexpr static const int heart_beat_time_ms_ = 1000;
    constexpr static const int timeout_time_ms_ = 5000;
    std::thread election_thread_; // leader's election_thread_ send heart_beat or time out try to election
    std::mutex election_thread_mutex_; // common lock
    std::condition_variable election_cv_;
    
    std::mutex election_mutex_;
    bool election_thread_stop_flag_ = false;
    std::atomic_uint64_t last_time_{static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count())};
    int32_t id_;
    int32_t leader_id_;
    int32_t voted_ = false;
    PodStatus status_;
    std::atomic_int64_t term_{0};
    std::vector<std::shared_ptr<Follower> > followers_;
    RaftLog raft_log_;
    std::shared_ptr<easykv::DB> db_;
    std::unique_ptr<cpputil::pool::ThreadPool> pool_;
    std::mutex solve_append_lock_;
};

}
}