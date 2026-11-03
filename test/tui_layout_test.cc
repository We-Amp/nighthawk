#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"

#include "api/client/options.pb.h"

#include "source/client/tui/input_mode.h"
#include "source/client/tui/layout.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): layout-contract tests (spec §4: persistent 3-row status
// bar, full-screen tabs, declared 100x30 minimum below which the TUI renders
// a clear message instead of wrapping garbage). Assertions work on the
// rendered screen text so they pin what the user actually sees.

// Splits an ftxui::Screen into per-row strings. ToString() terminates rows
// with "\r\n"; the carriage returns are stripped so row indexing stays exact.
std::vector<std::string> screenLines(const ftxui::Element& element, int width, int height) {
  ftxui::Screen screen(width, height);
  ftxui::Render(screen, element);
  std::string text = screen.ToString();
  std::vector<std::string> lines;
  size_t start = 0;
  while (start <= text.size()) {
    size_t newline = text.find('\n', start);
    std::string line =
        (newline == std::string::npos) ? text.substr(start) : text.substr(start, newline - start);
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    lines.push_back(line);
    if (newline == std::string::npos) {
      break;
    }
    start = newline + 1;
  }
  return lines;
}

constexpr const char* kUri = "http://127.0.0.1:10000/";
constexpr const char* kSessionPath = "/tmp/nh-m0b-demo-1.nhsession";

RunViewModel runningModel() {
  RunViewModel model;
  model.run_state = "running";
  for (uint64_t i = 0; i < 10; ++i) {
    model.latency_band.push_back(PercentilePoint{/*monotonic_ns=*/i * 1000000000ull,
                                                 /*p50_ns=*/10000000, /*p90_ns=*/20000000,
                                                 /*p99_ns=*/30000000});
  }
  model.counters["benchmark.http_2xx"].push_back(CounterPoint{0, 1000});
  model.last_snapshot.set_phase_id("main");
  model.last_snapshot.set_epoch_id("bench-1");
  return model;
}

// A 100x30 frame's lines plus the screen, for attribute-level assertions
// (the reverse-video caret and mode cell).
struct Frame {
  std::vector<std::string> lines;
  std::string text;
  std::unique_ptr<ftxui::Screen> screen;

  bool contains(const std::string& needle) const { return text.find(needle) != std::string::npos; }
  bool lacks(const std::string& needle) const { return text.find(needle) == std::string::npos; }
};

Frame renderFrame(const ftxui::Element& element, int width, int height) {
  Frame frame;
  frame.screen = std::make_unique<ftxui::Screen>(width, height);
  ftxui::Render(*frame.screen, element);
  std::string text = frame.screen->ToString();
  size_t start = 0;
  while (start <= text.size()) {
    size_t newline = text.find('\n', start);
    std::string line =
        (newline == std::string::npos) ? text.substr(start) : text.substr(start, newline - start);
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    frame.lines.push_back(line);
    if (newline == std::string::npos) {
      break;
    }
    start = newline + 1;
  }
  frame.text = std::move(text);
  return frame;
}

// Renders the full 100x30 layout with the given state.
Frame frameAt(InputMode mode, const RunViewModel& model, const StartRunDialogModel& dialog,
              const OverlayState* overlay = nullptr) {
  return renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, mode, model, dialog, kSessionPath,
                   nighthawk::client::CommandLineOptions{}, overlay),
      100, 30);
}

StartRunDialogModel filledDialog() {
  StartRunDialogModel dialog;
  dialog.set_uri(kUri);
  dialog.set_rps(100);
  dialog.set_duration_seconds(60);
  return dialog;
}

// Review Focus 5: below the declared minimum the TUI must render the clear
// size message and nothing else — no chart or status content wrapping into
// garbage.
TEST(LayoutTest, BelowMinSizeRendersMessageOnly) {
  ftxui::Element element = RenderLayout(99, 29, /*active_tab=*/0, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath);
  std::string text;
  for (const std::string& line : screenLines(element, 99, 29)) {
    text += line + "\n";
  }

  EXPECT_NE(text.find("at least 100x30"), std::string::npos);
  // None of the normal layout's status/chart content may appear.
  EXPECT_EQ(text.find("running"), std::string::npos);
  EXPECT_EQ(text.find("phase:"), std::string::npos);
  EXPECT_EQ(text.find("p50"), std::string::npos);
  EXPECT_EQ(text.find(kUri), std::string::npos);
  EXPECT_EQ(text.find(kSessionPath), std::string::npos);
}

// The layout contract at the exact minimum: rows 0-2 always carry the status
// bar (state + uri | phase placeholder | session path), the charts pane owns
// everything below row 2.
TEST(LayoutTest, StatusBarOccupiesThreeRowsAtMinSize) {
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath);
  std::vector<std::string> lines = screenLines(element, 100, 30);

  ASSERT_GE(lines.size(), 4u);
  EXPECT_NE(lines[0].find("running"), std::string::npos);
  EXPECT_NE(lines[0].find(kUri), std::string::npos);
  EXPECT_NE(lines[1].find("phase: main"), std::string::npos);
  EXPECT_NE(lines[1].find("pre-phase-engine placeholder"), std::string::npos);
  EXPECT_NE(lines[2].find(kSessionPath), std::string::npos);
  // The charts pane starts at row 3: the percentile band's legend row.
  EXPECT_NE(lines[3].find("p50"), std::string::npos);
}

// Pre-A1 placeholder semantics: phase and epoch ids carry placeholder markers
// in the status bar; an empty epoch id renders "(none)", not an empty field.
TEST(StatusBarTest, ShowsPhaseEpochPlaceholders) {
  RunViewModel model = runningModel();

  std::string with_epoch;
  for (const std::string& line :
       screenLines(RenderStatusBar(model, InputMode::kNavigate, kSessionPath), 100, 3)) {
    with_epoch += line + "\n";
  }
  EXPECT_NE(with_epoch.find("phase: main"), std::string::npos);
  EXPECT_NE(with_epoch.find("pre-phase-engine placeholder"), std::string::npos);
  EXPECT_NE(with_epoch.find("epoch: bench-1"), std::string::npos);
  EXPECT_NE(with_epoch.find("[placeholder]"), std::string::npos);

  model.last_snapshot.set_epoch_id("");
  std::string empty_epoch;
  for (const std::string& line :
       screenLines(RenderStatusBar(model, InputMode::kNavigate, kSessionPath), 100, 3)) {
    empty_epoch += line + "\n";
  }
  EXPECT_NE(empty_epoch.find("epoch: (none)"), std::string::npos);
}

// Tab 1 renders the real knob pane from the run options (fix round: the k key
// used to expose Task 5's static placeholder while the pane was test-only).
TEST(LayoutTest, KnobsTabRendersTheKnobPane) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/1, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath, options);
  std::string text;
  for (const std::string& line : screenLines(element, 100, 30)) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("steering lands in M3"), std::string::npos);
  EXPECT_NE(text.find("LIVE (M3)"), std::string::npos);
  EXPECT_NE(text.find("PHASE-GATED"), std::string::npos);
  EXPECT_NE(text.find("100"), std::string::npos);
  EXPECT_EQ(text.find("knobs (M0b Task 6)"), std::string::npos);

  // Before any run (defaulted empty options): the unset vocabulary, still no
  // placeholder.
  ftxui::Element defaults = RenderLayout(100, 30, /*active_tab=*/1, InputMode::kNavigate,
                                         runningModel(), filledDialog(), kSessionPath);
  std::string defaults_text;
  for (const std::string& line : screenLines(defaults, 100, 30)) {
    defaults_text += line + "\n";
  }
  EXPECT_NE(defaults_text.find("unset"), std::string::npos);
  EXPECT_NE(defaults_text.find("not configured"), std::string::npos);
  EXPECT_EQ(defaults_text.find("knobs (M0b Task 6)"), std::string::npos);
}

// The persistent, mode-aware key bar pinned to the bottom row (backlog item
// 3): rendered strictly from the shell's real key map.
TEST(KeyBarTest, NavigateIdleBarNamesTheNavigationKeys) {
  Frame frame = frameAt(InputMode::kNavigate, RunViewModel{}, filledDialog());
  ASSERT_EQ(frame.lines.size(), 30u);
  EXPECT_NE(frame.lines[29].find("n new run"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("c charts"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("k knobs"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("q quit"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("? help"), std::string::npos);
  // No run is live, so neither stop nor the quit-stops warning appear.
  EXPECT_EQ(frame.lines[29].find("s stop"), std::string::npos);
  EXPECT_EQ(frame.lines[29].find("(stops run)"), std::string::npos);
  // frameAt renders with a session path present (a session open until q):
  // the n hint is annotated as unavailable there, since pressing it would
  // silently refuse (M0b final-review finding).
  EXPECT_NE(frame.lines[29].find("n new run (unavailable: session open)"), std::string::npos);
}

// With no session open the n hint is a plain affordance again: no stale
// "unavailable" annotation.
TEST(KeyBarTest, NavigateBarWithClosedSessionNamesNAvailable) {
  ftxui::Element element =
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, RunViewModel{},
                   filledDialog(), /*session_path=*/"");
  Frame frame = renderFrame(element, 100, 30);
  EXPECT_NE(frame.lines[29].find("n new run |"), std::string::npos);
  EXPECT_EQ(frame.lines[29].find("unavailable"), std::string::npos);
}

TEST(KeyBarTest, RunningBarNamesStopAndTheQuitStopsWarning) {
  Frame frame = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  ASSERT_EQ(frame.lines.size(), 30u);
  EXPECT_NE(frame.lines[29].find("s stop"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("q quit (stops run)"), std::string::npos);
}

TEST(KeyBarTest, EditingBarNamesTheDialogKeys) {
  OverlayState overlay;
  overlay.active_field = StartRunDialogModel::kRps;
  Frame frame = frameAt(InputMode::kEditingStartRun, RunViewModel{}, filledDialog(), &overlay);
  ASSERT_EQ(frame.lines.size(), 30u);
  EXPECT_NE(frame.lines[29].find("tab field"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("enter apply"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("esc cancel"), std::string::npos);
  // The status bar and key bar stay visible around the overlay.
  EXPECT_NE(frame.lines[0].find("idle"), std::string::npos);
}

// The `?` help overlay lists every key and every status-bar field with a
// one-line definition.
TEST(HelpOverlayTest, HelpListsKeysAndStatusBarFields) {
  OverlayState overlay;
  overlay.help_open = true;
  Frame frame = frameAt(InputMode::kNavigate, runningModel(), filledDialog(), &overlay);
  EXPECT_TRUE(frame.contains("help"));
  EXPECT_TRUE(frame.contains("n          open the start-run dialog"));
  EXPECT_TRUE(frame.contains("q          quit; a running run is stopped first"));
  EXPECT_TRUE(frame.contains("tab        move to the next field"));
  EXPECT_TRUE(frame.contains("enter      apply"));
  EXPECT_TRUE(frame.contains("esc        cancel"));
  EXPECT_TRUE(frame.contains("state      idle | running | completed | stopped"));
  EXPECT_TRUE(frame.contains("flush 1s   chart cadence: one column per flush"));
  EXPECT_TRUE(frame.contains("session    session recording path"));
  // The help overlay never breaks the layout contract: status bar and key
  // bar stay on their rows.
  EXPECT_NE(frame.lines[0].find("running"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("? help"), std::string::npos);
}

// The start-run dialog overlay (backlog item 1): bordered box over the pane,
// labeled fields, and — after a rejected enter — inline errors on the
// offending field's own row, the summary on the border, and the box open.
TEST(DialogOverlayTest, RejectedEnterRendersInlineErrorsAndKeepsBoxOpen) {
  OverlayState overlay;
  overlay.active_field = StartRunDialogModel::kRps;
  overlay.show_errors = true; // the shell's post-rejected-enter state
  Frame frame = frameAt(InputMode::kEditingStartRun, RunViewModel{}, StartRunDialogModel{},
                        &overlay);

  // The editing tell survives a monochrome paste: the "[editing] " prefix on
  // row 1 next to the inverse-video mode cell on row 3.
  EXPECT_NE(frame.lines[0].find("[editing] idle"), std::string::npos);
  EXPECT_TRUE(frame.contains("mode: "));

  // The bordered box sits at the spec'd geometry (~rows 8-20, cols 25-75).
  EXPECT_EQ(frame.lines[8][25], '+');
  EXPECT_EQ(frame.lines[8][74], '+');
  EXPECT_NE(frame.lines[9].find("start run - 3 errors"), std::string::npos);

  // One labeled field per row, each with its inline error.
  EXPECT_TRUE(frame.contains("uri: must not be empty"));
  EXPECT_TRUE(frame.contains("rps: must be > 0"));
  EXPECT_TRUE(frame.contains("duration (s): must be > 0"));
  EXPECT_TRUE(frame.contains("tab: next field  enter: start  esc: cancel"));

  // The reverse-video caret block on the active field (rps, row 12): the
  // value area of that row renders inverted (attribute, not color).
  bool inverted_caret = false;
  for (int x = 43; x < 55; ++x) {
    if (frame.screen->CellAt(x, 12).inverted) {
      inverted_caret = true;
    }
  }
  EXPECT_TRUE(inverted_caret) << frame.lines[12];
}

// Before a rejected enter the box renders clean: no error text anywhere, no
// error summary on the border.
TEST(DialogOverlayTest, CleanDialogRendersFieldsWithoutErrors) {
  OverlayState overlay;
  overlay.uri_field = "https://demo.we-amp.local";
  overlay.rps_field = "300";
  overlay.duration_field = "60";
  overlay.active_field = StartRunDialogModel::kUri;
  Frame frame = frameAt(InputMode::kEditingStartRun, RunViewModel{}, filledDialog(), &overlay);

  EXPECT_NE(frame.lines[9].find("start run"), std::string::npos);
  EXPECT_TRUE(frame.lacks("errors"));
  EXPECT_TRUE(frame.lacks("must be > 0"));
  EXPECT_TRUE(frame.lacks("must not be empty"));
  // The draft renders inside the box, values visible.
  EXPECT_TRUE(frame.contains("https://demo.we-amp.local"));
  EXPECT_TRUE(frame.contains("300"));
  EXPECT_TRUE(frame.contains("60"));
}

// M0b final-review finding: a rejected enter whose fields validated but whose
// start the controller refused (a session already open — the steady state
// after every run until q) must render a distinct line inside the dialog.
// Reusing the field-error path would put "start run - 0 errors" on the
// border: a false statement about a failure that is not a field error.
TEST(DialogOverlayTest, ControllerRejectionRendersInsideTheBoxWithoutAnErrorCount) {
  OverlayState overlay;
  overlay.uri_field = kUri;
  overlay.rps_field = "100";
  overlay.duration_field = "60";
  overlay.active_field = StartRunDialogModel::kUri;
  overlay.start_rejected = "cannot start: a session is already open (press q to close it)";
  Frame frame = frameAt(InputMode::kEditingStartRun, RunViewModel{}, filledDialog(), &overlay);

  // The rejection reads inside the box, naming the way out. The message
  // wraps at the fixed border width rather than being clipped (both wrapped
  // rows appear).
  EXPECT_TRUE(frame.contains("cannot start: a session is already open"));
  EXPECT_TRUE(frame.contains("(press q to close it)"));
  // No error-count border title (and in particular no "0 errors"), no
  // per-field inline errors: this failure is not a field error.
  EXPECT_TRUE(frame.lacks("errors"));
  EXPECT_TRUE(frame.lacks("must be > 0"));
  EXPECT_TRUE(frame.lacks("must not be empty"));
  EXPECT_NE(frame.lines[9].find("start run"), std::string::npos);
}

// The target-slot invariant (backlog item 6): "(no target)" only ever reads
// on idle; a left-idle state carries the accepted target, with "(target
// pending)" reserved for the pre-ack window.
TEST(StatusBarTest, TargetSlotNeverReadsNoTargetWhileRunning) {
  RunViewModel model = runningModel();

  // Pre-ack: running, no accepted uri anywhere yet.
  Frame pre_ack = frameAt(InputMode::kNavigate, model, StartRunDialogModel{});
  EXPECT_TRUE(pre_ack.contains("(target pending)"));
  EXPECT_TRUE(pre_ack.lacks("(no target)"));

  // Acked: the model carries the accepted uri.
  model.target_uri = "https://demo.we-amp.local/";
  Frame acked = frameAt(InputMode::kNavigate, model, StartRunDialogModel{});
  EXPECT_TRUE(acked.contains("https://demo.we-amp.local/"));
  EXPECT_TRUE(acked.lacks("(no target)"));
  EXPECT_TRUE(acked.lacks("(target pending)"));

  // Idle (and not editing): the reserved string reads again.
  Frame idle = frameAt(InputMode::kNavigate, RunViewModel{}, StartRunDialogModel{});
  EXPECT_TRUE(idle.contains("(no target)"));
}

// Elapsed with a duration reads as progress plus remaining; without one it
// says open-ended (backlog item 6).
TEST(StatusBarTest, ElapsedRendersRemainingOrOpenEnded) {
  RunViewModel model = runningModel(); // 10 one-second stamps -> elapsed 9s

  nighthawk::client::CommandLineOptions with_duration;
  with_duration.mutable_duration()->set_seconds(60);
  Frame against_duration = renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, model, filledDialog(),
                   kSessionPath, with_duration),
      100, 30);
  EXPECT_TRUE(against_duration.contains("elapsed 9s/60s"));
  EXPECT_TRUE(against_duration.contains("remaining 51s"));

  nighthawk::client::CommandLineOptions open_ended;
  Frame open = renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, model, filledDialog(),
                   kSessionPath, open_ended),
      100, 30);
  EXPECT_TRUE(open.contains("elapsed 9s (open-ended)"));
  EXPECT_TRUE(open.lacks("remaining"));

  // A finished run's clock is frozen: the progress pair renders, no
  // remaining readout steers anybody.
  RunViewModel done = runningModel();
  done.run_state = "completed";
  Frame finished = renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, done, filledDialog(),
                   kSessionPath, with_duration),
      100, 30);
  EXPECT_TRUE(finished.contains("elapsed 9s/60s"));
  EXPECT_TRUE(finished.lacks("remaining"));
}

// The state-aware empty state (backlog item 8): idle names the user-initiated
// next action; only a running run before its first flush may say waiting.
TEST(EmptyStateTest, IdleAndRunningRenderDifferentEmptyStates) {
  Frame idle = frameAt(InputMode::kNavigate, RunViewModel{}, StartRunDialogModel{});
  EXPECT_TRUE(idle.contains("no run yet - press n to start one"));
  EXPECT_TRUE(idle.lacks("waiting for first flush"));

  RunViewModel running_empty;
  running_empty.run_state = "running";
  Frame running = frameAt(InputMode::kNavigate, running_empty, StartRunDialogModel{});
  EXPECT_TRUE(running.contains("waiting for first flush"));
  EXPECT_TRUE(running.lacks("no run yet"));

  // A completed run keeps its charts — no empty state at all.
  Frame done = frameAt(InputMode::kNavigate, runningModel(), StartRunDialogModel{});
  EXPECT_TRUE(done.lacks("no run yet"));
  EXPECT_TRUE(done.lacks("waiting for first flush"));
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
