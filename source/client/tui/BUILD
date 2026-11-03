load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): TUI library skeleton. Quarantined dependency surface:
# only FTXUI, TCLAP, the generated nighthawk client protos, and their
# transitive proto/grpc deps — no @envoy//source C++ targets. The standing
# cquery guard is documented in README.md next to this file.
envoy_cc_library(
    name = "tui_lib",
    srcs = ["tui_app.cc"],
    hdrs = ["tui_app.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:grpc_service_lib",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@tclap//:tclap",
    ],
)
