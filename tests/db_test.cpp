#include <functional>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "easykv/db.hpp"
#include "easykv/pool/thread_pool.hpp"

// TEST(DB, Function) {
//     easykv::DB db;
//     const int n = 1000000;
//     for (int i = 0; i < n; i++) {
//         db.Put(std::to_string(i), std::to_string(i + 1));
//     }
//     for (int i = 0; i < n; i++) {
//         std::string value;
//         ASSERT_EQ(db.Get(std::to_string(i), value), true);
//         ASSERT_EQ(value, std::to_string(i + 1));
//     }
// }

// TEST(DB, MultiThread) {
//     easykv::DB db;
//     const int n = 10000;
//     const int m = 5;
//     cpputil::pool::ThreadPool pool(10);
//     std::vector<std::function<void()> > read_functions;
//     std::vector<std::future<void> > read_futures;
//     for (int i = 0; i < n; i++) {
//         db.Put(std::to_string(i), std::to_string(i));
//     }
//     for (int i = 0; i < m; i++) {
//         read_functions.emplace_back([n, &db]() {
//             for (int i = 0; i < n; i++) {
//                 std::string value;
//                 if (db.Get(std::to_string(i), value)) {
//                     if (std::to_string(i) != value) {
//                         // std::cout << "read " << i << " value " << value << std::endl;
//                     }
//                 }
//             }
//         });
//     }
    
//     std::vector<std::function<void()> > put_funtions;
//     std::vector<std::future<void> > put_futures;
//     put_funtions.emplace_back([n, &db]() {
//         for (int i = 0; i < n; i++) {
//             db.Put(std::to_string(i), std::to_string(i + 1));
//         }
//     });
//     pool.MultiEnqueue(put_funtions, put_futures);
//     pool.MultiEnqueue(read_functions, read_futures);
//     for (auto& future : put_futures) {
//         std::move(future).get();
//     }
//     for (auto& future : read_futures) {
//         std::move(future).get();
//     }
// }

TEST(DB, Read) {
    easykv::DB db;
    const int n = 1000000;
    for (int i = 0; i < n; i++) {
        std::string value;
        db.Get(std::to_string(i), value);
        // std::cout << value << std::endl;
    }
}