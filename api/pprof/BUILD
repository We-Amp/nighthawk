# FORK-ONLY (We-Amp): the vendored pprof profile proto (google/pprof,
# perftools.profiles.Profile), generating protobuf C++ only. Deliberately NOT
# envoy's api_cc_py_proto_library: the TUI quarantine keeps the flamegraph
# pipeline off Envoy API machinery, so this uses the protobuf BCR rules
# directly. The generated header is included as
# "api/pprof/profile.pb.h" (workspace-relative import path).
load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

licenses(["notice"])  # Apache 2

proto_library(
    name = "profile_proto",
    srcs = ["profile.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "profile_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":profile_proto"],
)
