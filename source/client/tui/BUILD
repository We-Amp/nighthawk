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
        "admin_client.cc",
        "charts.cc",
        "flamegraph.cc",
        "knob_pane.cc",
        "layout.cc",
        "pprof_fold.cc",
        "pprof_worker_pool.cc",
        "profile_fetcher.cc",
        "profile_session_conversion.cc",
        "profile_symbolizer.cc",
        "profile_windows.cc",
        "run_feed.cc",
        "session_conversion.cc",
        "start_run_dialog.cc",
        "status_bar.cc",
        "sut_stats.cc",
        "tui_app.cc",
    ],
    hdrs = [
        "admin_client.h",
        "charts.h",
        "flamegraph.h",
        "input_mode.h",
        "knob_pane.h",
        "layout.h",
        "pprof_fold.h",
        "pprof_worker_pool.h",
        "profile_fetcher.h",
        "profile_session_conversion.h",
        "profile_symbolizer.h",
        "profile_windows.h",
        "run_feed.h",
        "session_conversion.h",
        "start_run_dialog.h",
        "status_bar.h",
        "sut_stats.h",
        "tui_app.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        "//api/client:grpc_service_lib",
        "//api/pprof:profile_cc_proto",
        # The headless extraction (MCP plan Task 0): the session writer/
        # reader/framing pieces, the run view model, and the session
        # recorder / fold tracker / series view layered on them. FTXUI-free,
        # quarantine-clean (the cquery guard proves it).
        "//source/client/headless:client_headless",
        # The pure phase-vector validation the dialog reuses (proto+absl deps
        # only — quarantine-clean; the cquery guard proves it).
        "//source/client:options_validation_phases_lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@ftxui//:component",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@protobuf//src/google/protobuf/io:gzip_stream",
        "@tclap//:tclap",
    ],
)
