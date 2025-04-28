cc_library(
    name = "easykv",
    srcs = glob([
        "utils/*.cpp",
    ]),
    hdrs = glob([
        "cache/*.hpp",
        "cache/*.h",
        "pool/*.hpp",
        "utils/*.h",
        "utils/*.hpp",
        "lsm/*.hpp",
        "raft/*.hpp",
        "*.hpp",
    ]),
    deps = [
        "//easykv/raft/protos:cc_raft_grpc"
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "easykv_service",
    srcs = [
        "server.cpp",
    ],
    deps = [
        ":easykv"
    ]
)

