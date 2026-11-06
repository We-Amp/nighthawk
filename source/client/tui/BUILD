load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): the FTXUI-free TUI sub-libraries (MCP plan Task 2's
# quarantine need: the nighthawk_mcp graph must carry RunFeed, the
# StartRunDialogModel, the session conversions and the knob rules WITHOUT
# FTXUI, which tui_lib links). Each is plain proto/grpc/absl only; tui_lib
# consumes them unchanged and the standing cquery guard (README.md) proves
# the mcp graph clean.
envoy_cc_library(
    name = "run_feed_lib",
    srcs = ["run_feed.cc"],
    hdrs = ["run_feed.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:grpc_service_lib",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/strings",
    ],
)

envoy_cc_library(
    name = "session_conversion_lib",
    srcs = ["session_conversion.cc"],
    hdrs = ["session_conversion.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

envoy_cc_library(
    name = "knob_rules_lib",
    srcs = ["knob_rules.cc"],
    hdrs = ["knob_rules.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        "@com_google_absl//absl/strings",
    ],
)

envoy_cc_library(
    name = "start_run_dialog_lib",
    srcs = ["start_run_dialog.cc"],
    hdrs = ["start_run_dialog.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//api/client:base_cc_proto",
        # The pure phase-vector validation the dialog reuses (proto+absl deps
        # only — quarantine-clean; the cquery guard proves it).
        "//source/client:options_validation_phases_lib",
        "@com_google_absl//absl/strings",
    ],
)

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
        # The FTXUI-free sub-libraries above (moved out of this target so the
        # nighthawk_mcp graph can consume them without FTXUI).
        ":knob_rules_lib",
        ":run_feed_lib",
        ":session_conversion_lib",
        ":start_run_dialog_lib",
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
