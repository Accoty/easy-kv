#pragma once
#include "easykv/lsm/skiplist.hpp"

namespace easykv {
namespace lsm {

class MemeTable {
public:
    using Iterator = ConcurrentSkipList::Iterator;

    bool Get(std::string_view key, std::string& value) {
        return skip_list_.Get(key, value);
    }

    void Put(std::string_view key, std::string_view value) {
        skip_list_.Put(key, value);
    }

    void Delete(std::string_view key) {
        skip_list_.Delete(key);
    }
    
    size_t binary_size() {
        return skip_list_.binary_size();
    }

    size_t size() {
        return skip_list_.size();
    }

    Iterator begin() {
        return skip_list_.begin();
    }

    Iterator end() {
        return skip_list_.end();
    }
private:
    ConcurrentSkipList skip_list_;
    bool lock_ = false;
};

}
}