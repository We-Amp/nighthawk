load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_binary",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

exports_files(["LICENSE"])

envoy_package()

filegroup(
    name = "nighthawk",
    srcs = [
        ":nighthawk_adaptive_load_client.stripped",
        ":nighthawk_client.stripped",
        ":nighthawk_output_transform.stripped",
        ":nighthawk_service.stripped",
        ":nighthawk_test_server.stripped",
    ],
)

envoy_cc_binary(
    name = "nighthawk_adaptive_load_client",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:adaptive_load_client_entry_lib",
    ],
)

envoy_cc_binary(
    name = "nighthawk_client",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:nighthawk_client_entry_lib",
    ],
)

# A testonly version of the nighthawk client, intended to be built with any required test plugins
# to enable integration test use cases.
envoy_cc_binary(
    name = "nighthawk_client_testonly",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:nighthawk_client_entry_lib",
        "//source/user_defined_output:log_response_headers_plugin",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output",
    ],
)

envoy_cc_binary(
    name = "nighthawk_test_server",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/server:http_dynamic_delay_filter_config",
        "//source/server:http_test_server_filter_config",
        "//source/server:http_time_tracking_filter_config",
        "@envoy//source/exe:envoy_main_entry_lib",
    ],
)

envoy_cc_binary(
    name = "nighthawk_service",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:nighthawk_service_entry_lib",
    ],
)

# FORK-ONLY (We-Amp): live snapshot streaming probe client.
envoy_cc_binary(
    name = "nighthawk_live_probe",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:live_probe_entry_lib",
    ],
)

envoy_cc_binary(
    name = "nighthawk_output_transform",
    linkopts = [
        "-lrt",
    ],
    deps = [
        "//source/exe:output_transform_main_entry_lib",
    ],
)

# FORK-ONLY (We-Amp): TUI binary skeleton. Deliberately not in the "nighthawk"
# filegroup above (that is the engine binary set) and deliberately free of the
# engine binaries' -lrt linkopt: the TUI must build on a clean mac checkout
# without any librt crutch. Quarantine rule: source/client/tui/README.md.
envoy_cc_binary(
    name = "nighthawk_tui",
    deps = [
        "//source/exe:tui_main_entry_lib",
        # tui_lib carries grpc++ code (run_feed, the interactive shell's
        # channel); the binary must link grpc explicitly (same pattern as the
        # plain cc_test targets).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): headless reader for .nhsession files; prints one JSON
# line per record. Same reasoning as nighthawk_tui: not in the engine
# filegroup and free of the -lrt linkopt (quarantine rule,
# source/client/tui/README.md).
envoy_cc_binary(
    name = "nighthawk_session_dump",
    deps = [
        "//source/exe:session_dump_entry_lib",
        # tui_lib now carries grpc++ code beyond the reader (the interactive
        # shell); the whole archive links, so grpc must be explicit here too
        # (same pattern as nighthawk_tui).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)
