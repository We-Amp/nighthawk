load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): the MCP transport + dispatch skeleton (Task 1) plus the
# steering tools and their driver (Task 2). Quarantined dependency surface:
# the generated client protos, protobuf's JSON parser, absl, grpc++ (RunFeed's
# transport), TCLAP for the flag surface, and the FTXUI-free client pieces
# (client_headless; the run_feed / start_run_dialog / session_conversion /
# knob_rules sub-libraries split out of tui_lib for exactly this) — NO FTXUI,
# no tui_lib, no @envoy//source C++ targets (the standing cquery guard in
# source/client/tui/README.md runs over the nighthawk_mcp binary built on
# this library too).
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
        "//source/client/headless:client_headless",
        "//source/client/tui:knob_rules_lib",
        "//source/client/tui:run_feed_lib",
        "//source/client/tui:session_conversion_lib",
        "//source/client/tui:start_run_dialog_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@grpc//:grpc++",
        "@protobuf//:json",
        "@protobuf//:struct_cc_proto",
        "@tclap//:tclap",
    ],
)
