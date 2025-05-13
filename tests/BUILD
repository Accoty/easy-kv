cc_test(
    name = "tests",
    srcs = glob(["*.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "compaction",
    srcs = glob(["compaction_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "cache",
    srcs = glob(["cache_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "bloom_filter",
    srcs = glob(["bloom_filter_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "thread_pool",
    srcs = glob(["thread_pool_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)
cc_binary(
    name = "skip_list",
    srcs = glob(["skip_list_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "lock",
    srcs = glob(["lock_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)

cc_binary(
    name = "db",
    srcs = glob(["db_test.cpp"]),
    copts = [
      "-Iexternal/gtest/googletest/include",
      "-Iexternal/gtest/googletest",
      "-g",
    ],
    deps = [
        "@googletest//:gtest_main",
        "//easykv:easykv",
    ],
)


