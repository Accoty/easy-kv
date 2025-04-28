#include "easykv/raft/service.hpp"
#include "easykv/raft/config.hpp"
#include "easykv/resource_manager.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <memory>
#include <string>
void RunServer() {
    easykv::ResourceManager::instance().InitDb();
    auto local_addr = easykv::ResourceManager::instance().config_manager().local_address();
    std::string server_addr = local_addr.ip() + ":" + std::to_string(local_addr.port());
    easykv::EasyKvServiceServiceImpl service;
    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "server listening on " << local_addr.port() << std::endl;
    server->Wait();
}

int main () {
    RunServer();
}