#include "easykv/raft/service.hpp"
#include "easykv/raft/config.hpp"
#include "easykv/resource_manager.hpp"

#include <atomic>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <memory>
#include <mutex>
#include <string>
#include <csignal>
#include <thread>

std::unique_ptr<grpc::Server> server;

std::atomic_bool g_shutdown_flag{false};
void ExitHandler(int signum) {
    std::cout << "close server" << std::endl;
    
    // server->Shutdown();
    g_shutdown_flag = true;

    // std::signal(signum, SIG_DFL);
}


void RunServer() {
    easykv::ResourceManager::instance().InitDb();
    easykv::ResourceManager::instance().InitPod();
    auto local_addr = easykv::ResourceManager::instance().config_manager().local_address();
    std::string server_addr = local_addr.ip() + ":" + std::to_string(local_addr.port());
    easykv::EasyKvServiceServiceImpl service;
    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    server = builder.BuildAndStart();
    std::cout << "server listening on " << local_addr.port() << std::endl;
    server->Wait();
}

int main () {
    // std::signal(SIGINT, ExitHandler);
    RunServer();
    while (!g_shutdown_flag) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // server->Shutdown();

    easykv::ResourceManager::instance().Close();
}