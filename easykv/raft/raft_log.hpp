#include <cstddef>
namespace easykv {
namespace raft {
class SnapShot {

};


class RaftLog {
public:
    size_t index() {
        return index_;
    }
private:
    size_t index_;
    size_t commit_;
};

}
}