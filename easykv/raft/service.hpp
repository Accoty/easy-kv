#pragma once
#include "easykv/raft/protos/raft.grpc.pb.h"
#include <grpcpp/support/status.h>

namespace easykv {

class EasyKvServiceServiceImpl final : public raft::EasyKvService::Service {
    using Status = grpc::Status;
    Status Put(grpc::ServerContext* ctx,
                                  const raft::PutReq* req,
                                  raft::PutRsp* rsp) {
        
        return Status::OK;
    }
    Status Get(grpc::ServerContext* ctx,
        const raft::GetReq* req,
        raft::GetRsp* rsp) {
        return Status::OK;
    }
    Status UpdateConfig(grpc::ServerContext* ctx,
        const raft::Config* req,
        raft::UpdateConfigRsp* rsp) {
        return Status::OK;
    }

    Status RequestVote(grpc::ServerContext* ctx,
        const raft::RequestVoteReq* req,
        raft::RequestVoteRsp* rsp) {
        return Status::OK;
    }
    Status Append(grpc::ServerContext* ctx,
        const raft::AppendReq* req,
        raft::AppendRsp* rsp) {
        return Status::OK;
    }
    Status Commit(grpc::ServerContext* ctx,
        const raft::CommitReq* req,
        raft::CommitRsp* rsp) {
        return Status::OK;
    }
};

}

/*
rpc Put(PutReq) returns (PutRsp) {}
    rpc Get(GetReq) returns (GetRsp) {}
    rpc UpdateConfig(Config) returns (UpdateConfigRsp) {}
    rpc Election(ElectionReq) returns (ElectionRsp) {}
    rpc Append(AppendReq) returns (AppendRsp) {}
    rpc Commit(CommitReq) returns (CommitRsp) {}
*/