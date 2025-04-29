#include <array>
#include <cstddef>
#include <string>

#include "easykv/utils/ring_buffer_queue.hpp"
namespace easykv {
namespace raft {
class SnapShot {

};

class RaftLog {
public:
    struct Entry {
        int32_t term;
        int64_t index;
        std::string key;
        std::string value;
        bool operator == (const Entry& rhs) const {
            return term == rhs.term && index == rhs.index && key == rhs.key && value == rhs.value;
        }
    };
    size_t index() {
        return index_;
    }
    bool Put(Entry& entry) {
        return queue_.PushBack(entry);
    }

    bool Check(Entry& entry, size_t index) {
        if (index > index_) {
            return false;
        }
        return queue_.RAt(index_ - index) == entry;
    }
private:   
    size_t index_;
    cpputil::pbds::RingBufferQueue<Entry> queue_;
};

}
}