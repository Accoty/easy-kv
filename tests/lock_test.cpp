#include <gtest/gtest.h>
#include <iostream>

#include "easykv/utils/lock.hpp"

TEST(Lock, Function) {
    easykv::common::RWLock lock;
    {
        easykv::common::RWLock::ReadLock r_lock_1(lock);
        easykv::common::RWLock::ReadLock r_lock_2(lock);
    }

    {
        // easykv::common::RWLock::ReadLock r_lock_1(lock);
        easykv::common::RWLock::WriteLock w_lock(lock);
    }
}