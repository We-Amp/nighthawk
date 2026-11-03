#include <cstdint>
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
                                                 /*p50_ns=*/100, /*p90_ns=*/200, /*p99_ns=*/300});
  }
  model.counters["benchmark.http_2xx"].push_back(CounterPoint{0, 1000});
  model.last_snapshot.set_phase_id("main");
  model.last_snapshot.set_epoch_id("bench-1");
  return model;
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

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
