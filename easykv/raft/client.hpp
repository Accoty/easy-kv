#include "easykv/raft/protos/raft.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <string>

namespace easykv {

class Client {
public:
    void Connect() {
        std::string addr = ip_ + ":" + std::to_string(port_);
        auto channel_ptr = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        stub_ = raft::EasyKvService::NewStub(channel_ptr);
    }

    raft::EasyKvService::Stub& stub() {
        return *stub_;
    }

private:
    std::string ip_;
    int32_t port_;
    std::unique_ptr<raft::EasyKvService::Stub> stub_;
};

}