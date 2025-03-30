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
