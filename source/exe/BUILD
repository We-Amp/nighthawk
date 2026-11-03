load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_library(
    name = "adaptive_load_client_entry_lib",
    srcs = ["adaptive_load_client_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/adaptive_load:adaptive_load_client_main",
        "//source/common:nighthawk_service_client_impl",
        "//source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
        "@envoy//source/exe:platform_header_lib_with_external_headers",
        "@envoy//source/exe:platform_impl_lib",
    ],
    alwayslink = True,
)

envoy_cc_library(
    name = "nighthawk_client_entry_lib",
    srcs = ["client_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    alwayslink = True,
)

envoy_cc_library(
    name = "nighthawk_service_entry_lib",
    srcs = ["service_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client:nighthawk_service_lib",
        "//source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    alwayslink = True,
)

# FORK-ONLY (We-Amp): entry point for the live probe binary.
envoy_cc_library(
    name = "live_probe_entry_lib",
    srcs = ["live_probe_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client:live_probe_lib",
        "//source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    alwayslink = True,
)

envoy_cc_library(
    name = "output_transform_main_entry_lib",
    srcs = ["output_transform_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client:output_transform_main_lib",
        "//source/common:version_linkstamp",
    ],
    alwayslink = True,
)

# FORK-ONLY (We-Amp): entry point for the nighthawk_tui binary. Kept free of
# the engine's version_linkstamp / symbolize deps so the TUI builds without
# Envoy C++ sources (quarantine rule, see source/client/tui/README.md).
envoy_cc_library(
    name = "tui_main_entry_lib",
    srcs = ["tui_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client/tui:tui_lib",
    ],
    alwayslink = True,
)

# FORK-ONLY (We-Amp): entry point for the nighthawk_session_dump binary. Kept
# free of the engine's version_linkstamp / symbolize deps so it builds without
# Envoy C++ sources (quarantine rule, see source/client/tui/README.md).
envoy_cc_library(
    name = "session_dump_entry_lib",
    srcs = ["session_dump_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//source/client/tui:tui_lib",
        "@tclap//:tclap",
    ],
    alwayslink = True,
)
