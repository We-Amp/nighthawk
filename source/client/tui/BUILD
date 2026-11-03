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
    srcs = [
        "charts.cc",
        "knob_pane.cc",
        "layout.cc",
        "run_feed.cc",
        "run_view_model.cc",
        "session_conversion.cc",
        "session_reader.cc",
        "session_writer.cc",
        "start_run_dialog.cc",
        "status_bar.cc",
        "tui_app.cc",
    ],
    hdrs = [
        "charts.h",
        "input_mode.h",
        "knob_pane.h",
        "layout.h",
        "run_feed.h",
        "run_view_model.h",
        "session_conversion.h",
        "session_framing.h",
        "session_reader.h",
        "session_writer.h",
        "start_run_dialog.h",
        "status_bar.h",
        "tui_app.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        "//api/client:grpc_service_lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@ftxui//:component",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@tclap//:tclap",
    ],
)
