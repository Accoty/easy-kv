#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <future>
#include <grpcpp/support/status.h>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "easykv/pool/thread_pool.hpp"
#include "easykv/raft/raft_log.hpp"
#include "easykv/raft/client.hpp"

#include "easykv/raft/protos/raft.grpc.pb.h"

namespace easykv {
namespace raft {

enum class PodStatus {
    Candidate,
    Leader,
    Follower,
};

class Follower {
public:
    Follower(const Address& addr) {
        addr_ = addr;
        rpc_client_.SetIp(addr.ip());
        rpc_client_.SetPort(addr.port());
        rpc_client_.Connect();
    }

    Client& rpc_client() {
        return rpc_client_;
    }

    void TrackIndex(RaftLog& leader_raft_log) {

    }
private:
    Client rpc_client_;
    Address addr_;
    int64_t nextindex_;
};

class Pod {
public:
    Pod(int32_t id, const Config& config): id_(id) {
        followers_.reserve(config.addresses_size());
        for (auto& addr : config.addresses()) {
            followers_.emplace_back(Follower(addr));
        }
        status_ = PodStatus::Follower;
        StartHeartBeatAndTimeOutRout();
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
            status_ = PodStatus::Follower;
        }
        voted_ = true;
        return true;
    }
private:
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
            follower.rpc_client().PutTest();
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
                auto status = follower.rpc_client().stub().RequestVote(&ctx, req, &rsp);
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
                return true;
            }
        }
        return false;
    }

    bool SolveAppend(const AppendReq& req) {
        UpdateLastTime();
        {
            std::unique_lock<std::mutex> lock(election_mutex_);
            if (req.term() > term_) {
                term_ = req.term();
                voted_ = false;
                status_ = PodStatus::Follower;
            }
        }
        if (req.entrys_size()) {
            auto code = UpdateRaftLog(req.entrys());
            if (code != 0) {
                return false;
            }
        }
        return true;
    }
private:

    int32_t UpdateRaftLog(const google::protobuf::RepeatedPtrField<::easykv::raft::Entry>& entries) {
        std::promise<int32_t> promise;
        if (entries.size() == 1) {
            auto entry = entries.Get(0);
            if (entry.index() != raft_log_.index() + 1) {
                return -1;
            }
            if ()

        }
        return 0;
    }

    void SendHeartBeat() {
        std::cout << "send heart beat, status" << int(status_) << std::endl;
    }

    
    void StartHeartBeatAndTimeOutRout() {
        election_thread_ = std::thread([this]() {
            while (true) {
                std::cout << id_ << " is " << int(status_) << std::endl;
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
    constexpr static const int timeout_time_ms_ = 3000;
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
    int64_t term_ = 0;
    std::vector<Follower> followers_;
    RaftLog raft_log_;
    std::unique_ptr<cpputil::pool::ThreadPool> pool_;
};

}
}