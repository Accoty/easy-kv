#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <grpcpp/support/status.h>
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
    Cadidate,
    Leader,
    Follower,
};

class Follower {
public:
    Client& rpc_client() {
        return rpc_client_;
    }
private:
    Client rpc_client_;
    Address addr_;
    int64_t nextindex_;
};

class Pod {
public:
    Pod(int32_t id): id_(id) {

    }
private:
    bool RequestVote() { // status = Candidate
        ++term_;
        size_t ticket_num = 1;
        for (auto& follower : followers_) {
            grpc::ClientContext ctx;
            RequestVoteReq req;
            req.set_id(id_);
            req.set_term(term_);
            req.set_index(raft_log_.index());
            RequestVoteRsp rsp;
            {
                std::unique_lock<std::mutex> lock(election_mutex_);
                if (status_ != PodStatus::Cadidate) {
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
        {
            std::unique_lock<std::mutex> lock(election_mutex_);
            if (status_ != PodStatus::Cadidate) {
                return false;
            }
            if (ticket_num > (1 + followers_.size()) / 2 + 1) {
                std::unique_lock<std::mutex> lock(election_mutex_);
                status_ = PodStatus::Leader;
                return true;
            }
        }
        return false;
    }

    bool Vote(const RequestVoteReq& req) { // status = Follower
        std::unique_lock<std::mutex> lock(election_mutex_);
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
        last_time_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
        return true;
    }

    bool SolveAppend(const AppendReq& req) {
        {
            std::unique_lock<std::mutex> lock(election_mutex_);
            if (req.term() > term_) {
                status_ = PodStatus::Follower;
                term_ = req.term();
                voted_ = false;
                SelectTimeoutRout();
            }
        }
        return true;
    }

private:

    void SendHeartBeat() {

    }

    void SelectHeartBeatRout() {
        {
            std::unique_lock<std::mutex> lock(election_thread_mutex_);
            election_thread_stop_flag_ = true;
            election_cv_.notify_all();
        }
        if (election_thread_.joinable()) {
            election_thread_.join();
        }
        election_thread_ = std::thread([this]() {
            while (true) {
                std::unique_lock<std::mutex> lock(election_thread_mutex_);
                auto future_time = std::chrono::system_clock::now() + std::chrono::milliseconds(heart_beat_time_ms_);
                election_cv_.wait_until(lock, future_time, [this]() {
                    return election_thread_stop_flag_;
                });
                if (election_thread_stop_flag_) {
                    break;
                }
                SendHeartBeat();
            }
        });
    }

    void SelectTimeoutRout() {
        {
            std::unique_lock<std::mutex> lock(election_thread_mutex_);
            election_thread_stop_flag_ = true;
            election_cv_.notify_all();
        }
        if (election_thread_.joinable()) {
            election_thread_.join();
        }
        last_time_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
        election_thread_ = std::thread([this]() {
            while (true) {
                std::unique_lock<std::mutex> lock(election_thread_mutex_);
                auto future_time = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_time_ms_);
                election_cv_.wait_until(lock, future_time, [this]() {
                    return election_thread_stop_flag_;
                });
                if (election_thread_stop_flag_) {
                    break;
                }
                if (static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count()) - last_time_ < timeout_time_ms_) {
                    continue;
                }
                if (RequestVote()) {
                    break;
                }
            }
        });
        if (status_ == PodStatus::Leader) {
            SelectHeartBeatRout();
        }
    }

private:
    constexpr static const int heart_beat_time_ms_ = 50;
    constexpr static const int timeout_time_ms_ = 1000;
    std::thread election_thread_; // leader's election_thread_ send heart_beat
    std::mutex election_mutex_;
    std::mutex election_thread_mutex_;
    std::condition_variable election_cv_;
    bool election_thread_stop_flag_ = false;
    std::atomic_uint64_t last_time_{static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
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