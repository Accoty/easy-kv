load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_proto_library")
load("@rules_proto_grpc_cpp//:defs.bzl", "cpp_grpc_library")

proto_library(
    name = "raft_proto",
    srcs = glob(["*.proto"]),
)

cpp_proto_library(
    name = "cc_raft_proto",
    protos = [":raft_proto"]
)

cpp_grpc_library(
    name = "cc_raft_grpc",
    protos = [":raft_proto"],
    grpc_only = True,
    deps = [":cc_raft_proto"],
    visibility = ["//visibility:public"],
)
