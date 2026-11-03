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

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
