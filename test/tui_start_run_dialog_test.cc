#include <string>

#include "gtest/gtest.h"

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

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
