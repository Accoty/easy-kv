#include "easykv/raft/protos/raft.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <iostream>
#include <string>
#include <string_view>

namespace easykv {

class Client {
public:
    void Connect() {
        std::string addr = ip_ + ":" + std::to_string(port_);
        auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        stub_ = raft::EasyKvService::NewStub(channel_ptr);
    }

    void PutTest() {
        grpc::ClientContext ctx;
        raft::PutReq req;
        raft::PutRsp rsp;
        gpr_timespec timespec;
        timespec.tv_sec = 2;
        timespec.tv_nsec = 0;
        timespec.clock_type = GPR_TIMESPAN;
        ctx.set_deadline(timespec);
        auto status = stub_->Put(&ctx, req, &rsp);
        std::cout << "status is ok ? " << status.ok() << " " << status.error_code() << std::endl;
    }

    raft::EasyKvService::Stub& stub() {
        return *stub_;
    }

    Client& SetIp(std::string_view ip) {
        ip_ = ip;
        return *this;
    }

    Client& SetPort(int32_t port) {
        port_ = port;
        return *this;
    }

private:
    std::string ip_;
    int32_t port_;
    std::unique_ptr<raft::EasyKvService::Stub> stub_;
};

}