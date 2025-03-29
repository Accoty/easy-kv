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
