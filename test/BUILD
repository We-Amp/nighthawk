load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_test",
    "envoy_package",
)
load("@rules_cc//cc:cc_test.bzl", "cc_test")

licenses(["notice"])  # Apache 2

envoy_package()

# FORK-ONLY (We-Amp): plain cc_test for the TUI skeleton. Deliberately NOT an
# envoy_cc_test: the TUI quarantine rule keeps this target off Envoy's C++
# sources, so it must not pull in the Envoy test helper stack.
cc_test(
    name = "tui_app_test",
    srcs = ["tui_app_test.cc"],
    deps = [
        "//api/client:grpc_service_lib",
        "//source/client/tui:tui_lib",
        "@googletest//:gtest_main",
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): the headless extraction contract pins (MCP plan Task 0).
# Plain cc_test for the same quarantine reason as the tui_* tests:
# client_headless carries no Envoy C++ sources and no test-helper stack.
cc_test(
    name = "headless_extraction_test",
    srcs = ["headless_extraction_test.cc"],
    deps = [
        "//source/client/headless:client_headless",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
    ],
)

# FORK-ONLY (We-Amp): the MCP transport + dispatch skeleton contract pins
# (MCP plan Task 1). Plain cc_test for the same quarantine reason as the
# headless/TUI tests: mcp_lib carries no Envoy C++ sources and no test-helper
# stack.
cc_test(
    name = "mcp_server_test",
    srcs = ["mcp_server_test.cc"],
    deps = [
        "//source/client/mcp:mcp_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        "@protobuf//:struct_cc_proto",
    ],
)

# FORK-ONLY (We-Amp): the MCP steering tools' contract pins (MCP plan Task
# 2), driven against a wire-faithful in-proc NighthawkService double (see the
# test's header: the double's ack/guard semantics are the engine's, mirrored
# by contract and pinned against the real engine in tui_run_feed_test and
# live_update_test; what this suite pins is the MCP layer). Plain cc_test,
# same quarantine reasoning as mcp_server_test.
cc_test(
    name = "mcp_tools_test",
    srcs = ["mcp_tools_test.cc"],
    deps = [
        "//api/client:grpc_service_lib",
        "//source/client/mcp:mcp_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@googletest//:gtest_main",
        # mcp_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): session file format, writer/reader, and queue drop
# contract tests. Plain cc_test for the same reason as tui_app_test: the TUI
# quarantine keeps this off Envoy's C++ sources and test helpers.
# FORK-ONLY (We-Amp): pprof parse/fold/symbolize/worker-pool contract tests.
# Plain cc_test for the same reason as tui_app_test: the TUI quarantine keeps
# this off Envoy's C++ sources and test helpers. The data fixture is the Task
# 1 machine-route output (real staged SUT CPU profile, symbolized by
# `go tool pprof -proto`); see the test header for its provenance.
cc_test(
    name = "tui_pprof_fold_test",
    srcs = ["tui_pprof_fold_test.cc"],
    data = ["fixtures/nh_sut_pprof.pb.gz"],
    deps = [
        "//api/pprof:profile_cc_proto",
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): phase-pure window alignment + differential math
# contract tests. Plain cc_test for the same reason as tui_pprof_fold_test:
# the TUI quarantine keeps this off Envoy's C++ sources and test helpers.
cc_test(
    name = "tui_profile_windows_test",
    srcs = ["tui_profile_windows_test.cc"],
    deps = [
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): flamegraph renderer + interaction state machine tests.
# Plain cc_test with headless ftxui::Screen renders, same quarantine
# reasoning as tui_profile_windows_test.
cc_test(
    name = "tui_flamegraph_test",
    srcs = ["tui_flamegraph_test.cc"],
    data = ["fixtures/nh_sut_pprof.pb.gz"],
    deps = [
        "//api/pprof:profile_cc_proto",
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/strings",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

cc_test(
    name = "tui_session_test",
    srcs = ["tui_session_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//api/client:grpc_service_lib",
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)


# FORK-ONLY (We-Amp): immutable snapshot store / view model tests. Plain
# cc_test for the same reason as tui_app_test: the TUI quarantine keeps this
# off Envoy's C++ sources and test helpers.
cc_test(
    name = "tui_view_model_test",
    srcs = ["tui_view_model_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): SUT prometheus scrape parse + delta layer tests. Plain
# cc_test for the same reason as tui_view_model_test: the TUI quarantine keeps
# this off Envoy's C++ sources and test helpers.
cc_test(
    name = "tui_sut_stats_test",
    srcs = ["tui_sut_stats_test.cc"],
    deps = [
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/strings",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_view_model_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): profile-window session recording tests (M1 task 6).
# Plain cc_test, same quarantine reasoning as tui_view_model_test.
cc_test(
    name = "tui_profile_session_test",
    srcs = ["tui_profile_session_test.cc"],
    deps = [
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): headless flamegraph-pipeline integration tests (M1 task
# 6) — the full fetch -> fold -> align -> store -> session pipeline the TUI
# drives, over an in-proc Nighthawk service and a stub SUT admin. An
# envoy_cc_test (like tui_session_pipeline_test) because it needs ServiceImpl
# and the grpc server stack; the pipeline code under test stays inside the
# quarantine.
envoy_cc_test(
    name = "tui_flamegraph_pipeline_test",
    srcs = ["tui_flamegraph_pipeline_test.cc"],
    deps = [
        "//source/client:nighthawk_service_lib",
        "//source/client/tui:tui_lib",
    ],
)

# FORK-ONLY (We-Amp): chart renderer tests. Plain cc_test, same quarantine
# reasoning as tui_view_model_test.
cc_test(
    name = "tui_charts_test",
    srcs = ["tui_charts_test.cc"],
    deps = [
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/strings",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): layout-contract tests (min-size gate, status bar rows,
# knob tab). Plain cc_test, same quarantine reasoning as tui_view_model_test.
cc_test(
    name = "tui_layout_test",
    srcs = ["tui_layout_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): start-run dialog model + input-mode tests. Plain cc_test,
# same quarantine reasoning as tui_view_model_test.
cc_test(
    name = "tui_start_run_dialog_test",
    srcs = ["tui_start_run_dialog_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): display-only knob pane tests. Plain cc_test, same
# quarantine reasoning as tui_view_model_test.
cc_test(
    name = "tui_knob_pane_test",
    srcs = ["tui_knob_pane_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): M3 Task 8 knob-steering tests — the §4 knob state
# machine, the timeline-truth rule (epoch marks on engine acks only), the
# pending-timeout honesty rule, and the KnobEpochRecord session path. Plain
# cc_test, same quarantine reasoning as tui_view_model_test.
cc_test(
    name = "tui_knob_steering_test",
    srcs = ["tui_knob_steering_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@ftxui//:dom",
        "@ftxui//:screen",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): M2 phase timeline + PhaseBoundaryRecord tests (Task 7
# phase 1: the pure render/conversion halves). Plain cc_test, same quarantine
# reasoning as tui_view_model_test.
cc_test(
    name = "tui_phase_timeline_test",
    srcs = ["tui_phase_timeline_test.cc"],
    deps = [
        "//api/client:base_cc_proto",
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): profile-fetch layer tests (admin window control +
# out-of-band retrieval + stats scrape). Plain cc_test, same quarantine
# reasoning as tui_view_model_test; the SUT admin is stood in for by a tiny
# in-test HTTP stub server, not Envoy's test harness.
cc_test(
    name = "tui_profile_fetcher_test",
    srcs = ["tui_profile_fetcher_test.cc"],
    deps = [
        "//source/client/tui:tui_lib",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@googletest//:gtest_main",
        # tui_lib carries grpc++ code (run_feed); plain cc_test consumers must
        # link grpc explicitly (same as tui_app_test).
        "@grpc//:grpc",
        "@grpc//:grpc++",
    ],
)

# FORK-ONLY (We-Amp): headless session-pipeline integration tests — the same
# feed -> store -> writer pipeline the interactive shell drives, minus the
# terminal, over an in-proc Nighthawk service and a real HTTP listener. An
# envoy_cc_test (like tui_run_feed_test) because it needs ServiceImpl and the
# grpc server stack; the headless code under test stays inside the quarantine.
envoy_cc_test(
    name = "tui_session_pipeline_test",
    srcs = ["tui_session_pipeline_test.cc"],
    deps = [
        "//source/client:nighthawk_service_lib",
        "//source/client/tui:tui_lib",
    ],
)

envoy_cc_test(
    name = "benchmark_http_client_test",
    srcs = ["benchmark_http_client_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/common:request_impl_lib",
        "//test/test_common:environment_lib",
        "//test/test_common:proto_matchers",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output_proto_cc_proto",
        "@envoy//source/common/http:header_map_lib_with_external_headers",
        "@envoy//source/common/network:utility_lib_with_external_headers",
        "@envoy//source/common/protobuf:utility_lib_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//source/exe:process_wide_lib_with_external_headers",
        "@envoy//test/mocks/buffer:buffer_mocks",
        "@envoy//test/mocks/runtime:runtime_mocks",
        "@envoy//test/mocks/stream_info:stream_info_mocks",
        "@envoy//test/mocks/thread_local:thread_local_mocks",
        "@envoy//test/mocks/upstream:upstream_mocks",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "client_test",
    srcs = ["client_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/client:nighthawk_service_lib",
        "//test/client:utility_lib",
        "//test/test_common:environment_lib",
    ],
)

envoy_cc_test(
    name = "client_worker_test",
    srcs = ["client_worker_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//test/mocks/client:mock_benchmark_client",
        "//test/mocks/client:mock_benchmark_client_factory",
        "//test/mocks/common:mock_request_source",
        "//test/mocks/common:mock_request_source_factory",
        "//test/mocks/common:mock_sequencer",
        "//test/mocks/common:mock_sequencer_factory",
        "//test/mocks/common:mock_termination_predicate",
        "//test/mocks/common:mock_termination_predicate_factory",
        "@envoy//source/common/api:api_lib",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/local_info:local_info_mocks",
        "@envoy//test/mocks/protobuf:protobuf_mocks",
        "@envoy//test/mocks/thread_local:thread_local_mocks",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "flush_worker_test",
    srcs = ["flush_worker_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "@envoy//source/common/api:api_lib",
        "@envoy//source/common/event:dispatcher_includes_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/api:api_mocks",
        "@envoy//test/mocks/local_info:local_info_mocks",
        "@envoy//test/mocks/protobuf:protobuf_mocks",
        "@envoy//test/mocks/thread_local:thread_local_mocks",
        "@envoy//test/mocks/upstream:cluster_manager_mocks",
    ],
)

envoy_cc_test(
    name = "factories_test",
    srcs = ["factories_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//test/mocks/client:mock_benchmark_client",
        "//test/mocks/client:mock_options",
        "//test/mocks/common:mock_termination_predicate",
        "//test/test_common:environment_lib",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/mocks/tracing:tracing_mocks",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "frequency_test",
    srcs = ["frequency_test.cc"],
    deps = ["//source/common:nighthawk_common_lib"],
)

envoy_cc_test(
    name = "options_test",
    srcs = ["options_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//test/client:utility_lib",
        "//test/test_common:environment_lib",
        "//test/test_common:proto_matchers",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output",
    ],
)

envoy_cc_test(
    name = "options_phases_test",
    srcs = ["options_phases_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/client:options_validation_phases_lib",
        "//test/client:utility_lib",
        "//test/test_common:proto_matchers",
        "@envoy//test/test_common:utility_lib",
    ],
)

# FORK-ONLY (We-Amp): snapshot-acquisition-vs-slab-swap seam tests (M2 Task 4).
envoy_cc_test(
    name = "live_partial_aggregator_test",
    srcs = ["live_partial_aggregator_test.cc"],
    deps = [
        "//source/client:live_partial_aggregator_lib",
    ],
)

envoy_cc_test(
    name = "output_formatter_test",
    srcs = ["output_formatter_test.cc"],
    data = [
        "test_data/output_formatter.csv.gold",
        "test_data/output_formatter.dotted.gold",
        "test_data/output_formatter.json.gold",
        "test_data/output_formatter.medium.fortio.gold",
        "test_data/output_formatter.medium.fortio-noquirks.gold",
        "test_data/output_formatter.medium.proto.gold",
        "test_data/output_formatter.prometheus.gold",
        "test_data/output_formatter.txt.gold",
        "test_data/output_formatter.yaml.gold",
        "test_data/percentile-column-overflow.json",
        "test_data/percentile-column-overflow.txt.gold",
    ],
    deps = [
        "//source/client:output_collector_impl_lib",
        "//source/client:output_formatter_impl_lib",
        "//source/common:nighthawk_common_lib",
        "//test/mocks/client:mock_options",
        "//test/test_common:environment_lib",
        "//test/test_common:proto_matchers",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "output_collector_test",
    srcs = ["output_collector_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//test/client:utility_lib",
        "//test/test_common:environment_lib",
        "//test/test_common:proto_matchers",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "platform_util_test",
    srcs = ["platform_util_test.cc"],
    deps = ["//source/common:nighthawk_common_lib"],
)

envoy_cc_test(
    # FORK-ONLY (We-Amp): multi-phase run tests (M2 A1 Task 3): boundary
    # drain, non-exiting stop, phase events, plus PhaseOrchestrator unit
    # tests. envoy_cc_test: needs ProcessImpl and the output collector.
    name = "phase_run_test",
    srcs = ["phase_run_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/client:phase_orchestrator_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/test_common:utility_lib",
    ],
)

envoy_cc_test(
    name = "process_test",
    srcs = ["process_test.cc"],
    deps = [
        "//api/user_defined_output:log_response_headers_proto_cc_proto",
        "//source/client:nighthawk_client_lib",
        "//source/user_defined_output:log_response_headers_plugin",
        "//test/client:utility_lib",
        "//test/sink:test_stats_sink_config_proto_cc_proto",
        "//test/test_common:environment_lib",
        "//test/test_common:proto_matchers",
        "//test/user_defined_output/fake_plugin:fake_user_defined_output",
        "@envoy//test/mocks/network:network_mocks",
        "@envoy//test/test_common:network_utility_lib",
        "@envoy//test/test_common:registry_lib",
        "@envoy//test/test_common:simulated_time_system_lib",
        "@envoy//test/test_common:test_runtime_lib",
    ],
)

envoy_cc_test(
    name = "rate_limiter_test",
    srcs = ["rate_limiter_test.cc"],
    deps = [
        "//source/common:nighthawk_common_lib",
        "//test/mocks/common:mock_rate_limiter",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "rate_limiter_plugin_test",
    srcs = ["rate_limiter_plugin_test.cc"],
    deps = [
        "//api/rate_limiter:rate_limiter_plugin_cc_proto",
        "//source/common:nighthawk_common_lib",
        "//test/mocks/client:mock_options",
        "//test/test_common:proto_matchers",
        "@envoy//test/mocks/stats:stats_mocks",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "sequencer_test",
    srcs = ["sequencer_test.cc"],
    data = [
        "test_data/hdr_proto_json.gold",
        "test_data/output_formatter.json.gold",
        "test_data/output_formatter.txt.gold",
        "test_data/output_formatter.yaml.gold",
    ],
    deps = [
        "//source/common:nighthawk_common_lib",
        "//test/mocks/common:mock_platform_util",
        "//test/mocks/common:mock_rate_limiter",
        "//test/mocks/common:mock_termination_predicate",
        "@envoy//source/common/event:dispatcher_includes_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "service_main_test",
    srcs = ["service_main_test.cc"],
    deps = [
        "//source/client:nighthawk_service_lib",
        "//test/test_common:environment_lib",
    ],
)

envoy_cc_test(
    name = "live_update_test",
    srcs = ["live_update_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/client:nighthawk_service_lib",
        "//test/mocks/client:mock_options",
        "//test/mocks/common:mock_platform_util",
        "//test/mocks/common:mock_termination_predicate",
        "//test/test_common:environment_lib",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/mocks/stats:stats_mocks",
        "@envoy//test/mocks/tracing:tracing_mocks",
        "@envoy//test/test_common:network_utility_lib",
        "@envoy//test/test_common:simulated_time_system_lib",
        "@envoy//test/test_common:utility_lib",
    ],
)

envoy_cc_test(
    name = "service_test",
    srcs = ["service_test.cc"],
    deps = [
        "//api/rate_limiter:rate_limiter_plugin_cc_proto",
        "//source/client:nighthawk_service_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "tui_run_feed_test",
    srcs = ["tui_run_feed_test.cc"],
    deps = [
        "//source/client:nighthawk_service_lib",
        "//source/client/tui:tui_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "live_probe_test",
    srcs = ["live_probe_test.cc"],
    deps = [
        "//source/client:live_probe_lib",
        "//source/client:nighthawk_service_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "statistic_test",
    srcs = ["statistic_test.cc"],
    data = [
        "test_data/circllhist_proto_json.gold",
        "test_data/hdr_proto_json.gold",
    ],
    deps = [
        "//source/common:nighthawk_common_lib",
        "//test/test_common:environment_lib",
        "@envoy//source/common/protobuf:utility_lib_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/stats:stats_mocks",
    ],
)

envoy_cc_test(
    name = "stream_decoder_test",
    srcs = ["stream_decoder_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "@envoy//source/common/event:dispatcher_includes_with_external_headers",
        "@envoy//source/common/http:header_map_lib_with_external_headers",
        "@envoy//source/common/network:utility_lib_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/http:http_mocks",
        "@envoy//test/mocks/stream_info:stream_info_mocks",
    ],
)

envoy_cc_test(
    name = "utility_test",
    srcs = ["utility_test.cc"],
    deps = [
        "//source/common:nighthawk_common_lib",
        "//test/test_common:environment_lib",
        "@envoy//source/common/network:utility_lib_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
    ],
)

envoy_cc_test(
    name = "worker_test",
    srcs = ["worker_test.cc"],
    deps = [
        "//source/common:nighthawk_common_lib",
        "@envoy//source/common/common:random_generator_lib_with_external_headers",
        "@envoy//source/common/stats:isolated_store_lib_with_external_headers",
        "@envoy//test/mocks/local_info:local_info_mocks",
        "@envoy//test/mocks/protobuf:protobuf_mocks",
        "@envoy//test/mocks/thread_local:thread_local_mocks",
    ],
)

envoy_cc_test(
    name = "request_generator_test",
    srcs = ["request_generator_test.cc"],
    deps = [
        "//source/client:nighthawk_client_lib",
        "//test/client:utility_lib",
    ],
)

# We wrap the python integration tests here so we run the instrumented
# code in coverage tests. To make that work, we need to declare the top-level
# libraries as dependencies.
envoy_cc_test(
    name = "python_test",
    size = "enormous",
    srcs = ["python_test.cc"],
    data = ["//test/integration:integration_test"],
    exec_properties = select({
        "//test/config:github_ci_build": {
            "Pool": "linux_x64_large",
        },
        "//conditions:default": {},
    }),
    deps = [
        "//source/client:nighthawk_client_lib",
        "//source/client:nighthawk_service_lib",
        "//source/client:output_transform_main_lib",
        "//source/common:nighthawk_common_lib",
        "//source/server:http_test_server_filter_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "output_transform_main_test",
    srcs = ["output_transform_main_test.cc"],
    deps = [
        "//source/client:output_transform_main_lib",
        "//test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)

envoy_cc_test(
    name = "termination_predicate_test",
    srcs = ["termination_predicate_test.cc"],
    deps = [
        "//source/common:nighthawk_common_lib",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/test_common:simulated_time_system_lib",
    ],
)

envoy_cc_test(
    name = "request_stream_grpc_client_test",
    srcs = ["request_stream_grpc_client_test.cc"],
    deps = [
        "//api/request_source:grpc_request_source_service_lib",
        "//source/common:request_stream_grpc_client_lib",
        "@envoy//test/test_common:utility_lib",
        "@envoy_api//envoy/api/v2/core:pkg_cc_proto",
        "@envoy_api//envoy/config/core/v3:pkg_cc_proto",
    ],
)

envoy_cc_test(
    name = "sni_utility_test",
    srcs = ["sni_utility_test.cc"],
    deps = [
        "//source/client:sni_utility",
        "@envoy//test/test_common:utility_lib",
    ],
)

envoy_cc_test(
    name = "process_bootstrap_test",
    srcs = ["process_bootstrap_test.cc"],
    deps = [
        "//source/client:options_impl_lib",
        "//source/client:process_bootstrap",
        "//source/common:nighthawk_common_lib",
        "//test/client:utility_lib",
        "//test/mocks/client:mock_options",
        "//test/test_common:proto_matchers",
        "@envoy//test/mocks/api:api_mocks",
        "@envoy//test/mocks/event:event_mocks",
        "@envoy//test/mocks/network:network_mocks",
        "@envoy//test/test_common:status_utility_lib",
        "@envoy//test/test_common:utility_lib",
        "@envoy_api//envoy/config/bootstrap/v3:pkg_cc_proto",
        "@envoy_api//envoy/config/core/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg_cc_proto",
    ],
)

envoy_cc_test(
    name = "stopwatch_test",
    srcs = ["stopwatch_test.cc"],
    deps = [
        "//source/common:thread_safe_monotonic_time_stopwatch_lib",
        "//test/common:fake_time_source",
        "@envoy//test/test_common:simulated_time_system_lib",
        "@envoy//test/test_common:utility_lib",
    ],
)

# FORK-ONLY (We-Amp): spec 8.7 re-anchoring property tests (M3 Task 5, Review
# Focus 1). Plain cc_test: the math library is pure (chrono + absl, no Envoy),
# so the heavy property investment needs no engine link.
cc_test(
    name = "ramp_reanchor_test",
    srcs = ["ramp_reanchor_test.cc"],
    deps = [
        "//source/common:ramp_reanchor_lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@googletest//:gtest_main",
    ],
)

# FORK-ONLY (We-Amp): the TUI regression set as ONE label. Every tui_* test
# target must stay listed here, so a whole-suite regression run can never
# silently skip a target that a task forgot to run (the missed-target class:
# tui_session_pipeline_test stopped compiling after a record-type change and
# nobody noticed because the target was not in the run list).
test_suite(
    name = "tui_regression_tests",
    tests = [
        ":tui_app_test",
        ":tui_charts_test",
        ":tui_flamegraph_pipeline_test",
        ":tui_flamegraph_test",
        ":tui_knob_pane_test",
        ":tui_knob_steering_test",
        ":tui_layout_test",
        ":tui_phase_timeline_test",
        ":tui_pprof_fold_test",
        ":tui_profile_fetcher_test",
        ":tui_profile_session_test",
        ":tui_profile_windows_test",
        ":tui_run_feed_test",
        ":tui_session_pipeline_test",
        ":tui_session_test",
        ":tui_start_run_dialog_test",
        ":tui_sut_stats_test",
        ":tui_view_model_test",
    ],
)
