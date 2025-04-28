#include <memory>

#include "easykv/db.hpp"
#include "easykv/raft/config.hpp"
#include "easykv/raft/pod.hpp"

namespace easykv {

class ResourceManager {
public:
    static ResourceManager& instance() {
        static std::unique_ptr<ResourceManager> instance = std::make_unique<ResourceManager>();
        return *instance;
    }

    ResourceManager() {
        config_manager_ = std::make_unique<raft::ConfigManager>();
        config_manager_->Load();
    }

    DB& db() {
        // db 需要延迟加载，因为 Client 和 service 共用 ResourceManager
        return *db_;
    }

    void InitDb() {
        db_ = std::make_unique<DB>();
    }

    raft::Pod& pod() {
        return *pod_;
    }

    void InitPod() {
        pod_ = std::make_unique<raft::Pod>();
    }

    raft::ConfigManager& config_manager() {
        return *config_manager_;
    }

    void Load() {
        instance();
    }

    void CloseDb() {
        db_ = nullptr;
    }

private:
    std::unique_ptr<raft::ConfigManager> config_manager_;
    std::unique_ptr<DB> db_;
    std::unique_ptr<raft::Pod> pod_;
};

}