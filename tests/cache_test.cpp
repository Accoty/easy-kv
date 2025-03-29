#include <iostream>
#include <string>

#include "main/cache/concurrent_cache.hpp"
#include "main/utils/global_random.h"

#include <gtest/gtest.h>
#include <string_view>
#include <thread>

using namespace cpputil::cache;

TEST(CacheTest, ConcurrentLRUCacheBasicFunction) {
    // ConcurrentLRUCache<std::string_view, std::string> string_cahce(99, 1);
    
    // for (int i = 0; i < 100; i++) {
    //     string_cahce.Put(std::to_string(i));
    // }
    // ASSERT_EQ(string_cahce.Get("0"), nullptr);

    // string_cahce.Put("101");
    // ASSERT_EQ(string_cahce.Get("1"), nullptr);
    // ASSERT_EQ(*string_cahce.Get("2"), "2");

    // ConcurrentLRUCache<int> int_cache(99, 1);
    // for (int i = 0; i < 100; i++) {
    //     int_cache.Put(i);
    // }
    
    //     ASSERT_EQ(string_cahce.Get("0"), nullptr);

    // int_cache.Put(101);
    // ASSERT_EQ(int_cache.Get(1), nullptr);
    // ASSERT_EQ(*int_cache.Get(2), 2);
}

TEST(CacheTest, ConcurrentLRUCache) {
    ConcurrentLRUCache<int> cache(8);
    std::vector<std::thread> threads;
    // const int n = 30, m = 1000000;
    // for (int i = 0; i < n; i++) {
    //     threads.emplace_back([&] {
    //         int x = cpputil::common::GlobalRand() % n;
    //         for (int i = 0; i < m; i++) {
    //             cache.Put(x);
    //         }
    //     });
    // }
    // for (int i = 0; i < n; i++) {
    //     threads.emplace_back([&] {
    //         int x = cpputil::common::GlobalRand() % n;
    //         for (int i = 0; i < m; i++) {
    //             auto it = cache.Get(x);
    //             if (it) {
    //                 std::cout << (*it) << std::endl;
    //             } else {
    //                 std::cout << "nullptr " << x << " " << cpputil::common::GlobalRand() << std::endl;
    //             }
    //         }
    //     });
    // }
    for (auto& thread : threads) {
        thread.join();
    }
}