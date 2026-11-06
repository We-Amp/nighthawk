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
#include "source/client/tui/profile_windows.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/status_bar.h"
#include "source/client/tui/tui_app.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): layout-contract tests (spec §4: persistent 3-row status
// bar, full-screen tabs, declared 100x30 minimum below which the TUI renders
// a clear message instead of wrapping garbage). Assertions work on the
// rendered screen text so they pin what the user actually sees.

// Removes ANSI SGR escape sequences (this ftxui's Screen::ToString emits
// them for inverted/dim cells). The text assertions below pin what the user
// READS; the attribute-level assertions (the inverted caret, the strip's
// active cell) go through Screen::CellAt directly.
std::string stripAnsi(const std::string& text) {
  std::string out;
  out.reserve(text.size());
  for (size_t i = 0; i < text.size();) {
    if (text[i] == '\x1b' && i + 1 < text.size() && text[i + 1] == '[') {
      size_t j = i + 2;
      while (j < text.size() && text[j] != 'm' && text[j] != '\n') {
        ++j;
      }
      if (j < text.size() && text[j] == 'm') {
        i = j + 1;
        continue;
      }
    }
    out.push_back(text[i]);
    ++i;
  }
  return out;
}

// Splits an ftxui::Screen into per-row strings. ToString() terminates rows
// with "\r\n"; the carriage returns are stripped so row indexing stays exact.
std::vector<std::string> screenLines(const ftxui::Element& element, int width, int height) {
  ftxui::Screen screen(width, height);
  ftxui::Render(screen, element);
  std::string text = stripAnsi(screen.ToString());
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
  std::string text = stripAnsi(frame.screen->ToString());
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
// bar (state + uri | phase placeholder | session path), row 3 the tab strip,
// and the charts pane everything below it.
TEST(LayoutTest, StatusBarOccupiesThreeRowsAtMinSize) {
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath);
  std::vector<std::string> lines = screenLines(element, 100, 30);

  ASSERT_GE(lines.size(), 5u);
  EXPECT_NE(lines[0].find("running"), std::string::npos);
  EXPECT_NE(lines[0].find(kUri), std::string::npos);
  EXPECT_NE(lines[1].find("phase: main"), std::string::npos);
  EXPECT_NE(lines[1].find("pre-phase-engine placeholder"), std::string::npos);
  EXPECT_NE(lines[2].find(kSessionPath), std::string::npos);
  // Row 3 is the tab strip; the charts pane starts at row 4 with the
  // percentile band's legend row. M1: all three panes exist, so the strip
  // names them plainly.
  EXPECT_NE(lines[3].find("| charts | flame | knobs |"), std::string::npos);
  EXPECT_NE(lines[4].find("p50"), std::string::npos);
}

// The tab strip (backlog item 11): every frame names the panes and the
// active tab renders inverted. As of M1 all three panes exist, so nothing
// renders dimmed anymore.
TEST(TabStripTest, StripNamesPanesAndMarksTheActiveTab) {
  // Charts active (the default tab): "charts" inverted.
  Frame charts = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  EXPECT_NE(charts.lines[3].find("| charts | flame | knobs |"), std::string::npos)
      << "row3=[" << charts.lines[3] << "]";
  bool inverted_charts = false;
  bool inverted_knobs = false;
  for (int x = 0; x < 40; ++x) {
    const ftxui::Cell& cell = charts.screen->CellAt(x, 3);
    if (cell.character == "c") {
      inverted_charts = inverted_charts || cell.inverted;
    }
    if (cell.character == "k") {
      inverted_knobs = inverted_knobs || cell.inverted;
    }
  }
  EXPECT_TRUE(inverted_charts);
  EXPECT_FALSE(inverted_knobs);

  // Knobs active (the k key lands on tab 2): the inversion moves.
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/2, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath);
  Frame knobs = renderFrame(element, 100, 30);
  inverted_charts = false;
  inverted_knobs = false;
  for (int x = 0; x < 40; ++x) {
    const ftxui::Cell& cell = knobs.screen->CellAt(x, 3);
    if (cell.character == "c") {
      inverted_charts = inverted_charts || cell.inverted;
    }
    if (cell.character == "k") {
      inverted_knobs = inverted_knobs || cell.inverted;
    }
  }
  EXPECT_FALSE(inverted_charts);
  EXPECT_TRUE(inverted_knobs);

  // M1: the flame cell is a live pane — named plainly, never dimmed.
  EXPECT_NE(knobs.lines[3].find("flame"), std::string::npos);
  EXPECT_EQ(knobs.lines[3].find("flame (m1)"), std::string::npos);
}

// Tab 1 is the flamegraph pane (M1): without a pulled window it renders the
// honest empty state (never a blank screen); with a window the pane renders
// the provenance header and the flamegraph itself.
TEST(LayoutTest, FlameTabRendersEmptyStateThenThePane) {
  // Empty state: no flame state at all.
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/1, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath);
  std::string text;
  for (const std::string& line : screenLines(element, 100, 30)) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("flame: no profile window yet"), std::string::npos);
  EXPECT_NE(text.find("press p during a run"), std::string::npos);

  // A window: BuildFlamePane composes the same state the shell does, and the
  // pane renders the provenance header plus the folded tree.
  FoldedTree tree;
  tree.total_samples = 2;
  tree.total_value = 30000000;
  tree.value_unit = "cpu-nanoseconds";
  tree.symbolized = true;
  FoldedFrame root;
  root.function = "envoy.worker.main";
  root.total_samples = 2;
  root.total_value = 30000000;
  root.symbolized = true;
  tree.root.push_back(root);

  ProfileWindow window;
  window.tree = tree;
  window.start_unix_ns = 1;
  window.end_unix_ns = 2;
  window.purity_fraction = 1.0;
  window.phase_id = "main";
  window.worker_count = 1;
  window.work_units = 42;
  window.profile_type = "cpu";
  window.symbolized = true;
  std::vector<ProfileWindow> windows{window};
  ProfileWindowStore store;
  store.store(std::move(windows));

  FlamegraphViewState view;
  FlamePaneState flame = BuildFlamePane(store.load(), view, /*note=*/"", /*overlay=*/nullptr);
  ftxui::Element pane_element =
      RenderLayout(100, 30, /*active_tab=*/1, InputMode::kNavigate, runningModel(),
                   filledDialog(), kSessionPath,
                   nighthawk::client::CommandLineOptions{}, /*overlay=*/nullptr, &flame);
  std::string pane_text;
  for (const std::string& line : screenLines(pane_element, 100, 30)) {
    pane_text += line + "\n";
  }
  EXPECT_NE(pane_text.find("window #1: cpu"), std::string::npos) << pane_text;
  EXPECT_NE(pane_text.find("purity 1.00"), std::string::npos);
  EXPECT_NE(pane_text.find("work units 42"), std::string::npos);
  EXPECT_NE(pane_text.find("envoy.worker.main"), std::string::npos) << pane_text;
}

// Pre-A1 placeholder semantics: the phase id carries its placeholder marker
// in the status bar; an empty epoch id renders "(none)", not an empty field.
// M3: the epoch cell dropped its placeholder suffix — the ack path makes the
// epoch id real (a non-empty epoch renders bare).
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
  EXPECT_EQ(with_epoch.find("[placeholder]"), std::string::npos);

  model.last_snapshot.set_epoch_id("");
  std::string empty_epoch;
  for (const std::string& line :
       screenLines(RenderStatusBar(model, InputMode::kNavigate, kSessionPath), 100, 3)) {
    empty_epoch += line + "\n";
  }
  EXPECT_NE(empty_epoch.find("epoch: (none)"), std::string::npos);
}

// Tab 2 renders the real knob pane from the run options (fix round: the k key
// used to expose Task 5's static placeholder while the pane was test-only).
TEST(LayoutTest, KnobsTabRendersTheKnobPane) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  ftxui::Element element = RenderLayout(100, 30, /*active_tab=*/2, InputMode::kNavigate,
                                        runningModel(), filledDialog(), kSessionPath, options);
  std::string text;
  for (const std::string& line : screenLines(element, 100, 30)) {
    text += line + "\n";
  }
  // M3: the pane is live — the total-across-workers semantics render, the
  // display-only banner is gone.
  EXPECT_NE(text.find("total across the active workers"), std::string::npos);
  EXPECT_NE(text.find("LIVE"), std::string::npos);
  EXPECT_NE(text.find("PHASE-GATED"), std::string::npos);
  EXPECT_NE(text.find("100"), std::string::npos);
  EXPECT_EQ(text.find("knobs (M0b Task 6)"), std::string::npos);

  // Before any run (defaulted empty options): the unset vocabulary, still no
  // placeholder.
  ftxui::Element defaults = RenderLayout(100, 30, /*active_tab=*/2, InputMode::kNavigate,
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
  // the n hint is annotated there, since pressing it would silently refuse
  // (M0b final-review finding; compressed to the 100-column minimum in M1).
  EXPECT_NE(frame.lines[29].find("n new run (session open)"), std::string::npos);
}

// With no session open the n hint is a plain affordance again: no stale
// annotation.
TEST(KeyBarTest, NavigateBarWithClosedSessionNamesNAvailable) {
  ftxui::Element element =
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, RunViewModel{},
                   filledDialog(), /*session_path=*/"");
  Frame frame = renderFrame(element, 100, 30);
  EXPECT_NE(frame.lines[29].find("n new run |"), std::string::npos);
  EXPECT_EQ(frame.lines[29].find("(session open)"), std::string::npos);
}

TEST(KeyBarTest, RunningBarNamesStopAndTheQuitStopsWarning) {
  Frame frame = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  ASSERT_EQ(frame.lines.size(), 30u);
  EXPECT_NE(frame.lines[29].find("s stop"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("q quit (stops run)"), std::string::npos);
}

// M1: the navigate bars name the flame tab, and on the flame tab the key bar
// names the pane's own keys (the key map table's flame rows).
TEST(KeyBarTest, FlameKeysAreNamed) {
  Frame charts = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  EXPECT_NE(charts.lines[29].find("f flame"), std::string::npos);

  ftxui::Element element =
      RenderLayout(100, 30, /*active_tab=*/1, InputMode::kNavigate, runningModel(),
                   filledDialog(), kSessionPath);
  Frame frame = renderFrame(element, 100, 30);
  ASSERT_EQ(frame.lines.size(), 30u);
  EXPECT_NE(frame.lines[29].find("Arrows nav"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("Enter zoom"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("Esc root"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("/ find"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("p win"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("d diff"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("N noise"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("s stop"), std::string::npos);
  // The compressed flame bar stays under the width: nothing clips.
  EXPECT_LE(frame.lines[29].size(), 100u);
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

// M1 (M0b T5 seam): the flamegraph pane's frame-search editing mode extends
// the input-mode enum and suspends refresh under the same rule as the
// start-run dialog — the full truth table.
TEST(InputModeTest, SearchInputModeSuspendsRefresh) {
  EXPECT_FALSE(RefreshSuspended(InputMode::kNavigate));
  EXPECT_TRUE(RefreshSuspended(InputMode::kEditingStartRun));
  EXPECT_TRUE(RefreshSuspended(InputMode::kEditingSearch));
}

// The `?` help overlay lists every key (the M1 keys are grouped so the
// overlay still fits above the key bar) and every status-bar field with a
// definition. Coverage of the key map table is asserted per binding below.
TEST(HelpOverlayTest, HelpListsKeysAndStatusBarFields) {
  OverlayState overlay;
  overlay.help_open = true;
  Frame frame = frameAt(InputMode::kNavigate, runningModel(), filledDialog(), &overlay);
  EXPECT_TRUE(frame.contains("help"));
  EXPECT_TRUE(frame.contains("open the start-run dialog"));
  EXPECT_TRUE(frame.contains("noise-floor view"));
  EXPECT_TRUE(frame.contains("flame frame navigation"));
  EXPECT_TRUE(frame.contains("zoom to the selected frame"));
  EXPECT_TRUE(frame.contains("unzoom to the tree root"));
  EXPECT_TRUE(frame.contains("search"));
  EXPECT_TRUE(frame.contains("start/stop a profile window"));
  EXPECT_TRUE(frame.contains("differential"));
  EXPECT_TRUE(frame.contains("tab next field"));
  EXPECT_TRUE(frame.contains("idle|running|completed|stopped"));
  EXPECT_TRUE(frame.contains("chart cadence: one column per flush"));
  EXPECT_TRUE(frame.contains("per-flush client-side failure rates"));
  EXPECT_TRUE(frame.contains("session recording path"));
  EXPECT_TRUE(frame.contains("idle | window open | folding"));
  EXPECT_TRUE(frame.contains("reset-or-label-set"));

  // Coverage: every documented binding's key token appears somewhere in the
  // help text — the grouped overlay cannot silently drop a binding from the
  // NavigateKeyBindings table.
  std::string text;
  for (const std::string& line : frame.lines) {
    text += line + "\n";
  }
  for (const KeyBinding& binding : NavigateKeyBindings()) {
    EXPECT_NE(text.find(binding.key), std::string::npos)
        << "help overlay lost key: " << binding.key;
  }

  // The help overlay never breaks the layout contract: status bar and key
  // bar stay on their rows (the grouped overlay fits above row 29).
  EXPECT_NE(frame.lines[0].find("running"), std::string::npos);
  EXPECT_NE(frame.lines[29].find("? help"), std::string::npos);
}

// Batch-2 polish: with live charts behind it, the narrowed help overlay must
// not fragment the charts' right-margin readouts — the overlay's right edge
// clears the readout pairs entirely (they render near columns 76-99 at the
// 100-column minimum).
TEST(HelpOverlayTest, HelpOverlayLeavesTheChartReadoutsIntact) {
  OverlayState overlay;
  overlay.help_open = true;
  Frame frame = frameAt(InputMode::kNavigate, runningModel(), filledDialog(), &overlay);

  // The overlay's box: left border at column 10, top border on row 3 (the
  // strip row), right border at column 10 + 64 - 1 = 73.
  ASSERT_EQ(frame.lines[3][10], '+');
  ASSERT_EQ(frame.lines[3][73], '+');
  ASSERT_EQ(frame.lines[3][74], ' ');

  // The band legend row (row 4) carries the readout pair, intact right of the
  // overlay: "now" and "max" both land past column 73.
  const std::string& legend = frame.lines[4];
  const size_t now_at = legend.find("now ");
  const size_t max_at = legend.find("max ");
  ASSERT_NE(now_at, std::string::npos);
  ASSERT_NE(max_at, std::string::npos);
  EXPECT_GT(now_at, 73u);
  EXPECT_GT(max_at, 73u);
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
  overlay.start_rejected = "cannot start: a session is already open (q quits and saves)";
  Frame frame = frameAt(InputMode::kEditingStartRun, RunViewModel{}, filledDialog(), &overlay);

  // The rejection reads inside the box, naming the way out. The message
  // wraps at the fixed border width rather than being clipped (both wrapped
  // rows appear).
  EXPECT_TRUE(frame.contains("cannot start: a session is already open"));
  EXPECT_TRUE(frame.contains("(q quits and saves)"));
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

// A completed model carrying the response-class counters and a 60s band.
RunViewModel completedModel() {
  RunViewModel model = runningModel(); // 10 one-second stamps, ~30ms p99
  model.run_state = "completed";
  model.target_uri = kUri;
  auto& two_xx = model.counters["benchmark.http_2xx"];
  two_xx.push_back(CounterPoint{9000000000ull, 27444});
  auto& four_xx = model.counters["benchmark.http_4xx"];
  four_xx.push_back(CounterPoint{9000000000ull, 23});
  auto& five_xx = model.counters["benchmark.http_5xx"];
  five_xx.push_back(CounterPoint{9000000000ull, 198});
  auto& resets = model.counters["benchmark.stream_resets"];
  resets.push_back(CounterPoint{9000000000ull, 42});
  return model;
}

// The completion-summary block (backlog item 7): totals, error ratio, final
// percentiles, the saved session path, the §8.2 counts-shown disclosure —
// with the charts intact below.
TEST(CompletionSummaryTest, CompletedRunRendersTheSummaryAboveIntactCharts) {
  Frame frame = frameAt(InputMode::kNavigate, completedModel(), filledDialog());

  // The summary rides at the top of the pane (rows 4..6), built from the same
  // live view the charts draw.
  const std::string summary = frame.lines[4] + "\n" + frame.lines[5] + "\n" + frame.lines[6];
  EXPECT_NE(summary.find("p50/p90/p99 10.0/20.0/30.0 ms"), std::string::npos)
      << "rows4-6=[" << frame.lines[4] << "][" << frame.lines[5] << "][" << frame.lines[6]
      << "]";
  EXPECT_NE(summary.find("run complete: 9s"), std::string::npos);
  EXPECT_NE(summary.find("27,707 requests"), std::string::npos); // 27444+23+198+42
  EXPECT_NE(summary.find("errors 263 (0.9%)"), std::string::npos); // 23+198+42
  EXPECT_NE(summary.find("p50/p90/p99 10.0/20.0/30.0 ms"), std::string::npos);
  EXPECT_NE(summary.find("counts: 2xx 27,444, 4xx 23, 5xx 198, rst 42, t/o 0"), std::string::npos);
  EXPECT_NE(summary.find("every sample counted, none hidden"), std::string::npos);
  EXPECT_NE(summary.find("recording saved: /tmp/nh-m0b-demo-1.nhsession"), std::string::npos);

  // Charts intact below: the band legend and its data, then the counters.
  bool legend_below_summary = false;
  for (size_t row = 7; row < 12; ++row) {
    if (frame.lines[row].find("# p50") != std::string::npos) {
      legend_below_summary = true;
    }
  }
  EXPECT_TRUE(legend_below_summary);
  EXPECT_TRUE(frame.contains("benchmark.http_2xx"));

  // A running run renders no summary block.
  Frame running = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  EXPECT_TRUE(running.lacks("run complete"));
  EXPECT_TRUE(running.lacks("every sample counted, none hidden"));
}

// The compact health line (backlog item 10) renders in the pane from real
// counter series — and with zeros when the engine never emitted them.
TEST(HealthLineTest, PaneRendersTheFailureRateLineIncludingZeros) {
  // No failure counters at all: the line still renders, all zeros.
  Frame zeros = frameAt(InputMode::kNavigate, runningModel(), filledDialog());
  EXPECT_TRUE(zeros.contains("4xx 0/s | rst 0/s | t/o 0/s"));

  // A reset burst on the running model.
  RunViewModel model = runningModel();
  auto& resets = model.counters["benchmark.stream_resets"];
  resets.push_back(CounterPoint{8000000000ull, 30});
  resets.push_back(CounterPoint{9000000000ull, 72});
  Frame burst = frameAt(InputMode::kNavigate, model, filledDialog());
  EXPECT_TRUE(burst.contains("rst 42/s"));
}

// Pool-pending capacity context (backlog item 12): the pending series
// renders as a fraction of the run's configured gate, with the ceiling rule.
TEST(PoolCapacityTest, PendingSeriesRendersFractionOfTheConfiguredCap) {
  RunViewModel model = runningModel();
  auto& pending = model.counters["pool_rq_pending_release"];
  pending.push_back(CounterPoint{9000000000ull, 312});

  nighthawk::client::CommandLineOptions with_cap;
  with_cap.mutable_max_pending_requests()->set_value(2048);
  Frame capped = renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, model, filledDialog(),
                   kSessionPath, with_cap),
      100, 30);
  EXPECT_TRUE(capped.contains("pool_rq_pending_release count"));
  EXPECT_TRUE(capped.contains("cur 312/2048 (15%)"));
  bool ceiling_rule = false;
  for (const std::string& line : capped.lines) {
    if (line.find("||") != std::string::npos) {
      ceiling_rule = true;
    }
  }
  EXPECT_TRUE(ceiling_rule);

  // Without the configured gate the series falls back to the plain readout.
  Frame uncapped = frameAt(InputMode::kNavigate, model, filledDialog());
  EXPECT_TRUE(uncapped.contains("pool_rq_pending_release count"));
  EXPECT_FALSE(uncapped.contains("cur 312/2048"));
}

// The fixed-width scrolling viewport (backlog item 9) sizes to the terminal:
// at the 100-column minimum the window is the newest 100 seconds; at 110
// columns it widens to match. The stamps move with the window, not with a
// hardcoded width.
TEST(ViewportTest, WindowScalesWithTheTerminalWidth) {
  // 150 one-second flushes: more history than either window shows.
  RunViewModel model;
  model.run_state = "running";
  model.target_uri = kUri;
  for (uint64_t i = 0; i < 150; ++i) {
    model.latency_band.push_back(
        PercentilePoint{i * 1000000000ull, /*p50_ns=*/10000000, /*p90_ns=*/20000000,
                        /*p99_ns=*/30000000});
    model.counters["benchmark.http_2xx"].push_back(CounterPoint{i * 1000000000ull, 1000 + i});
  }

  Frame at_100 = renderFrame(
      RenderLayout(100, 30, /*active_tab=*/0, InputMode::kNavigate, model, filledDialog(),
                   kSessionPath),
      100, 30);
  EXPECT_NE(at_100.text.find("last 99s (1s/col)"), std::string::npos);
  EXPECT_NE(at_100.text.find("t+50s..t+149s"), std::string::npos);

  Frame at_110 = renderFrame(
      RenderLayout(110, 30, /*active_tab=*/0, InputMode::kNavigate, model, filledDialog(),
                   kSessionPath),
      110, 30);
  EXPECT_NE(at_110.text.find("last 109s (1s/col)"), std::string::npos);
  EXPECT_NE(at_110.text.find("t+40s..t+149s"), std::string::npos);

  // Nothing clips at the 100x30 boundary (the max-width gate frame): every
  // row fits exactly 100 columns.
  for (size_t i = 0; i < at_100.lines.size(); ++i) {
    ASSERT_LE(at_100.lines[i].size(), 100u) << "row " << i << " size "
                                            << at_100.lines[i].size() << ": ["
                                            << at_100.lines[i] << "]";
  }
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
