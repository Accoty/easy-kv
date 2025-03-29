#include <random>
#include "main/utils/global_random.h"

namespace cpputil {

namespace common {

uint64_t GlobalRand() {
    static thread_local std::mt19937 gen(std::random_device{}());
    return gen();
}

} // common
} // cpputil