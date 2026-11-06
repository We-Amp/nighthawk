load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): the headless client pieces extracted from the TUI (MCP
# plan Task 0) — the session file owner, the profile-window lifecycle state
# machine, and the phase/epoch-stamped series view — plus the moved-as-is
# session format pieces (session_writer/session_reader/session_framing) and
# the run view model they read. Quarantined dependency surface: the generated
# nighthawk client protos and absl ONLY — no FTXUI, no tui_lib, no
# @envoy//source C++ targets. The standing cquery guard (source/client/tui/
# README.md) runs over this target too.
envoy_cc_library(
    name = "client_headless",
    srcs = [
        "fold_tracker.cc",
        "run_view_model.cc",
        "series_view.cc",
        "session_reader.cc",
        "session_recorder.cc",
        "session_writer.cc",
    ],
    hdrs = [
        "fold_tracker.h",
        "run_view_model.h",
        "series_view.h",
        "session_framing.h",
        "session_reader.h",
        "session_recorder.h",
        "session_writer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)
