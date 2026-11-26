#include <string>

#include "gtest/gtest.h"

#include "api/request_source/request_source_plugin.pb.h"

#include "source/client/tui/input_mode.h"
#include "source/client/tui/start_run_dialog.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): start-run dialog model and input-mode contract tests.
// The dialog model is a pure state machine; its FTXUI binding is thin and
// lives in Task 6's app.

StartRunDialogModel validDialog() {
  StartRunDialogModel dialog;
  dialog.set_uri("http://127.0.0.1:10000/");
  dialog.set_rps(100);
  dialog.set_duration_seconds(60);
  return dialog;
}

TEST(StartRunDialogTest, DialogBuildsStartRequest) {
  StartRunDialogModel dialog = validDialog();
  ASSERT_TRUE(dialog.valid());

  nighthawk::client::StartRequest request = dialog.build_request();
  const nighthawk::client::CommandLineOptions& options = request.options();
  EXPECT_EQ(options.uri().value(), "http://127.0.0.1:10000/");
  EXPECT_EQ(options.requests_per_second().value(), 100);
  EXPECT_EQ(options.duration().seconds(), 60);
  // Live streaming must be opted into on the built request too: presence of
  // the message is the marker the service keys on (same contract RunFeed's
  // choke point re-asserts).
  EXPECT_TRUE(options.has_live_streaming());
  ASSERT_TRUE(options.has_stats_flush_interval());
  EXPECT_EQ(options.stats_flush_interval().value(), 1);
}

TEST(StartRunDialogTest, DialogRejectsInvalid) {
  StartRunDialogModel empty_uri;
  empty_uri.set_uri("");
  empty_uri.set_rps(100);
  empty_uri.set_duration_seconds(60);
  EXPECT_FALSE(empty_uri.valid());

  StartRunDialogModel zero_rps;
  zero_rps.set_uri("http://127.0.0.1:10000/");
  zero_rps.set_rps(0);
  zero_rps.set_duration_seconds(60);
  EXPECT_FALSE(zero_rps.valid());

  StartRunDialogModel zero_duration;
  zero_duration.set_uri("http://127.0.0.1:10000/");
  zero_duration.set_rps(100);
  zero_duration.set_duration_seconds(0);
  EXPECT_FALSE(zero_duration.valid());

  // A defaulted model starts invalid: nothing filled in yet.
  EXPECT_FALSE(StartRunDialogModel{}.valid());
}

// MCP-plan Task 2: the steering surface's connections field round-trips
// directly onto the built options (the dialog itself has no field for it);
// unset (0) stays unset — the engine default applies.
TEST(StartRunDialogTest, ConnectionsFieldRoundTrips) {
  StartRunDialogModel dialog = validDialog();
  EXPECT_FALSE(dialog.build_request().options().has_connections());

  dialog.set_connections(64);
  EXPECT_EQ(dialog.connections(), 64);
  ASSERT_TRUE(dialog.valid());
  const nighthawk::client::StartRequest request = dialog.build_request();
  ASSERT_TRUE(request.options().has_connections());
  EXPECT_EQ(request.options().connections().value(), 64);
}

// N3a: the steering surface's protocol and request body size round-trip onto
// the built options; unset stays unset (the engine defaults apply), and an
// explicit body size of 0 is written as 0.
TEST(StartRunDialogTest, ProtocolAndRequestBodySizeRoundTrip) {
  StartRunDialogModel dialog = validDialog();
  EXPECT_FALSE(dialog.build_request().options().has_protocol());
  EXPECT_FALSE(dialog.build_request().options().request_options().has_request_body_size());

  dialog.set_protocol(nighthawk::client::Protocol::HTTP3);
  dialog.set_request_body_size(0);
  ASSERT_TRUE(dialog.valid());
  nighthawk::client::StartRequest request = dialog.build_request();
  ASSERT_TRUE(request.options().has_protocol());
  EXPECT_EQ(request.options().protocol().value(), nighthawk::client::Protocol::HTTP3);
  ASSERT_TRUE(request.options().request_options().has_request_body_size());
  EXPECT_EQ(request.options().request_options().request_body_size().value(), 0);

  dialog.set_request_body_size(2048);
  request = dialog.build_request();
  EXPECT_EQ(request.options().request_options().request_body_size().value(), 2048);
}

// Routed from the Task 5 review: a whitespace-only uri must not pass — set_uri
// trims, so blank input lands on the invalid empty-uri path, and a
// fat-fingered submit with surrounding whitespace still works.
TEST(StartRunDialogTest, WhitespaceOnlyUriIsRejected) {
  StartRunDialogModel dialog;
  dialog.set_uri("   ");
  dialog.set_rps(100);
  dialog.set_duration_seconds(60);
  EXPECT_FALSE(dialog.valid());

  dialog.set_uri("  http://127.0.0.1:10000/  ");
  ASSERT_TRUE(dialog.valid());
  EXPECT_EQ(dialog.uri(), "http://127.0.0.1:10000/");
  EXPECT_EQ(dialog.build_request().options().uri().value(), "http://127.0.0.1:10000/");
}

// Review Focus 4: the explicit input mode is what suspends pane refresh while
// a dialog owns the keyboard. The exit-from-editing path must post exactly
// one forced redraw (wired in Task 6's dialog close, exercised in Task 7).
TEST(InputModeTest, RefreshSuspendedWhileEditing) {
  EXPECT_FALSE(RefreshSuspended(InputMode::kNavigate));
  EXPECT_TRUE(RefreshSuspended(InputMode::kEditingStartRun));
}

// M2: the phase-vector entry. A two-phase vector builds a phase run whose
// phases carry through to the StartRequest; the mutually-exclusive top-level
// duration is never written alongside it; the dialog's validation REUSES the
// pure phase-vector validation (a duplicate phase name fails it, with the
// reused message surfacing on the duration field's slot).
TEST(StartRunDialogTest, PhaseDialogBuildsVectorRequest) {
  StartRunDialogModel dialog = validDialog();
  std::vector<nighthawk::client::PhaseSpec> phases;
  for (const char* name : {"warm", "spike"}) {
    nighthawk::client::PhaseSpec phase;
    phase.set_name(name);
    phase.mutable_duration()->set_seconds(5);
    phase.set_requests_per_second(100);
    phase.mutable_concurrency()->set_value("2");
    phases.push_back(phase);
  }
  dialog.set_phases(phases);
  ASSERT_TRUE(dialog.valid());

  const nighthawk::client::StartRequest request = dialog.build_request();
  const nighthawk::client::CommandLineOptions& options = request.options();
  ASSERT_EQ(options.phases_size(), 2);
  EXPECT_EQ(options.phases(0).name(), "warm");
  EXPECT_EQ(options.phases(1).name(), "spike");
  // Mutual exclusion by construction: the vector replaces the top-level
  // duration; run-level rps stays legal alongside it.
  EXPECT_FALSE(options.has_duration());
  EXPECT_EQ(options.requests_per_second().value(), 100);
  EXPECT_TRUE(options.has_live_streaming());

  // The reused validation rejects a vector the dialog must not start: the
  // message surfaces on the phases field's own row (the duration field's
  // requirement is waived for a phase run).
  StartRunDialogModel invalid = validDialog();
  phases[1].set_name("warm"); // duplicate name
  invalid.set_phases(phases);
  EXPECT_FALSE(invalid.valid());
  EXPECT_EQ(StartRunDialogModel::FieldError(invalid, StartRunDialogModel::kDuration), "");
  EXPECT_NE(StartRunDialogModel::FieldError(invalid, StartRunDialogModel::kPhases).find("warm"),
            std::string::npos)
      << StartRunDialogModel::FieldError(invalid, StartRunDialogModel::kPhases);
}

// M2 fix round: the phases FIELD is the production affordance. A non-empty
// field parses into the request's phase vector (comma-separated
// name:rps:duration[:concurrency[:connections]] entries), waives the
// duration requirement, and builds a phase run end to end.
TEST(StartRunDialogTest, PhasesFieldParsesAndBuildsAPhaseRun) {
  StartRunDialogModel dialog = validDialog();
  dialog.set_phases_field("warm:100:5:2,spike:400:5:4:64");
  // Duration not required for a phase run (the vector replaces it).
  dialog.set_duration_seconds(0);
  ASSERT_TRUE(dialog.valid());
  EXPECT_TRUE(StartRunDialogModel::FieldError(dialog, StartRunDialogModel::kPhases).empty());
  EXPECT_TRUE(StartRunDialogModel::FieldError(dialog, StartRunDialogModel::kDuration).empty());

  const nighthawk::client::StartRequest request = dialog.build_request();
  ASSERT_EQ(request.options().phases_size(), 2);
  EXPECT_EQ(request.options().phases(0).name(), "warm");
  EXPECT_EQ(request.options().phases(0).concurrency().value(), "2");
  EXPECT_EQ(request.options().phases(1).requests_per_second(), 400);
  EXPECT_EQ(request.options().phases(1).connections().value(), 64u);
  EXPECT_FALSE(request.options().has_duration());
}

// A malformed entry surfaces the parser's message inline on the phases row.
TEST(StartRunDialogTest, PhasesFieldMalformedEntrySurfacesInline) {
  StartRunDialogModel dialog = validDialog();
  dialog.set_phases_field("warm:100:5,oops");
  EXPECT_FALSE(dialog.valid());
  const std::string error = StartRunDialogModel::FieldError(dialog, StartRunDialogModel::kPhases);
  EXPECT_NE(error.find("oops"), std::string::npos) << error;
  EXPECT_NE(error.find("name:rps:duration"), std::string::npos) << error;
}

// NH1: the phases field's sixth part carries per-worker weights, with the
// connections part optionally empty before it; the reused validation then
// checks the count and the whole-number split.
TEST(StartRunDialogTest, PhasesFieldParsesWorkerWeights) {
  auto parsed = StartRunDialogModel::ParsePhasesField("hot:800:20:4::5/1/1/1,cx:60:5:2:8:1/2");
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  ASSERT_EQ(parsed->size(), 2u);
  EXPECT_FALSE((*parsed)[0].has_connections());
  ASSERT_EQ((*parsed)[0].worker_weights_size(), 4);
  EXPECT_EQ((*parsed)[0].worker_weights(0), 5u);
  EXPECT_EQ((*parsed)[1].connections().value(), 8u);
  ASSERT_EQ((*parsed)[1].worker_weights_size(), 2);
  EXPECT_EQ((*parsed)[1].worker_weights(1), 2u);

  for (const char* bad : {"hot:800:20:4::5/0/1/1", "hot:800:20:4::", "hot:800:20:4::a",
                          "hot:800:20:4:x:1/1/1/1", "hot:800:20:4::1:1"}) {
    EXPECT_FALSE(StartRunDialogModel::ParsePhasesField(bad).ok()) << bad;
  }

  StartRunDialogModel dialog = validDialog();
  dialog.set_phases_field("hot:801:20:4::5/1/1/1");
  EXPECT_FALSE(dialog.valid());
  EXPECT_NE(StartRunDialogModel::FieldError(dialog, StartRunDialogModel::kPhases)
                .find("does not split into whole per-worker rates"),
            std::string::npos);
  dialog.set_phases_field("hot:800:20:4::5/1/1/1");
  EXPECT_TRUE(dialog.valid());
}

// N3b / NH1: the remaining request-shape setters land on their option
// fields; unset, nothing is written.
TEST(StartRunDialogTest, ShapeSettersBuildOptions) {
  StartRunDialogModel dialog = validDialog();
  dialog.set_burst_size(16);
  dialog.set_open_loop(true);
  dialog.set_sequencer_idle_strategy(nighthawk::client::SequencerIdleStrategy::POLL);
  dialog.set_max_concurrent_streams(8);
  dialog.set_request_method(envoy::config::core::v3::POST);
  dialog.set_request_body_size(64);
  envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext tls;
  tls.mutable_max_session_keys()->set_value(0);
  dialog.set_tls_context(tls);
  dialog.set_worker_series(true);
  dialog.set_max_pending_requests(32);
  dialog.set_prefetch_connections(true);
  const nighthawk::client::CommandLineOptions options = dialog.build_request().options();
  EXPECT_EQ(options.burst_size().value(), 16u);
  EXPECT_TRUE(options.open_loop().value());
  EXPECT_EQ(options.sequencer_idle_strategy().value(),
            nighthawk::client::SequencerIdleStrategy::POLL);
  EXPECT_EQ(options.max_concurrent_streams().value(), 8u);
  EXPECT_EQ(options.request_options().request_method(), envoy::config::core::v3::POST);
  EXPECT_EQ(options.request_options().request_body_size().value(), 64u);
  EXPECT_TRUE(options.tls_context().has_max_session_keys());
  EXPECT_TRUE(options.worker_series().value());
  EXPECT_EQ(options.max_pending_requests().value(), 32u);
  EXPECT_TRUE(options.prefetch_connections().value());

  const nighthawk::client::CommandLineOptions plain = validDialog().build_request().options();
  EXPECT_FALSE(plain.has_burst_size());
  EXPECT_FALSE(plain.has_open_loop());
  EXPECT_FALSE(plain.has_sequencer_idle_strategy());
  EXPECT_FALSE(plain.has_max_concurrent_streams());
  EXPECT_FALSE(plain.has_tls_context());
  EXPECT_FALSE(plain.has_worker_series());
  EXPECT_FALSE(plain.has_max_pending_requests());
  EXPECT_FALSE(plain.has_prefetch_connections());
  // An explicit 0 pending limit is written as 0, not dropped.
  StartRunDialogModel zero = validDialog();
  zero.set_max_pending_requests(0);
  ASSERT_TRUE(zero.build_request().options().has_max_pending_requests());
  EXPECT_EQ(zero.build_request().options().max_pending_requests().value(), 0u);
  EXPECT_FALSE(plain.has_request_options());
}

// N3b: a request mix replaces the static RequestOptions (a oneof): the built
// request carries the in-line options-list plugin and no static headers.
TEST(StartRunDialogTest, RequestMixReplacesStaticRequestOptions) {
  StartRunDialogModel dialog = validDialog();
  dialog.add_request_headers("x-static", "1");
  nighthawk::client::RequestOptions entry;
  entry.set_request_method(envoy::config::core::v3::PUT);
  dialog.set_request_mix({entry, entry});
  const nighthawk::client::CommandLineOptions options = dialog.build_request().options();
  EXPECT_FALSE(options.has_request_options());
  ASSERT_TRUE(options.has_request_source_plugin_config());
  EXPECT_EQ(options.request_source_plugin_config().name(),
            "nighthawk.in-line-options-list-request-source-plugin");
  nighthawk::request_source::InLineOptionsListRequestSourceConfig config;
  ASSERT_TRUE(options.request_source_plugin_config().typed_config().UnpackTo(&config));
  ASSERT_EQ(config.options_list().options_size(), 2);
  EXPECT_EQ(config.options_list().options(1).request_method(), envoy::config::core::v3::PUT);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
