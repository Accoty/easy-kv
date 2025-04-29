#include <cstddef>
#include <array>

namespace cpputil {
namespace pbds {

template <typename T>
class RingBufferQueue {
public:
    bool PushBack(T&& rhs) {
        if (((head_ + 1) & ring_buffer_size_musk_) == tail_) {
            return false;
        }
        data_[++head_] = std::move(rhs);
    }

    bool PushBack(const T& rhs) {
        if (((head_ + 1) & ring_buffer_size_musk_) == tail_) {
            return false;
        }
        data_[++head_] = rhs;
    }

    bool Empty() {
        return head_ == tail_;
    }

    bool PopFront() {
        ++tail_;
    }

    bool PopBack() {
        --head_;
    }

    T& Back() {
        return data_[head_ + 1];
    }

    T& Front() {
        return data_[tail_ + 1];
    }

    T& At(size_t index) {
        return data_[tail_ + index + 1];
    }

    T& RAt(size_t index) {
        return data_[head_ - index];
    }
private:
    constexpr static const size_t ring_buffer_size_ = (1 << 10) << 8;
    constexpr static const size_t ring_buffer_size_musk_ = ring_buffer_size_ - 1;
    std::array<T, ring_buffer_size_> data_;
    size_t head_ = 0;
    size_t tail_ = 0;
}; 

}
}