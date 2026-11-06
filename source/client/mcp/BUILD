load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): the MCP transport + dispatch skeleton (MCP plan Task 1)
# plus its driver. Quarantined dependency surface: the generated client protos
# (here: the Struct well-known type), protobuf's JSON parser, absl, and TCLAP
# for the flag surface — NO FTXUI, no tui_lib, no @envoy//source C++ targets
# (the standing cquery guard in source/client/tui/README.md runs over the
# nighthawk_mcp binary built on this library too).
envoy_cc_library(
    name = "mcp_lib",
    srcs = [
        "json_util.cc",
        "mcp_main.cc",
        "mcp_server.cc",
        "mcp_tools.cc",
    ],
    hdrs = [
        "json_util.h",
        "mcp_main.h",
        "mcp_server.h",
        "mcp_tools.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@protobuf//:json",
        "@protobuf//:struct_cc_proto",
        "@tclap//:tclap",
    ],
)
