#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"

#include "api/client/options.pb.h"
#include "source/client/tui/knob_pane.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using nighthawk::client::CommandLineOptions;

// FORK-ONLY (We-Amp): display-only knob pane tests (spec §3 ruling: the live
// steering set is rps + ramp; everything else is phase-gated and M0b renders
// that distinction without implementing any of it — steering lands in M3).

// Splits an ftxui::Screen into per-row strings (same helper as the layout
// tests): ToString() terminates rows with "\r\n"; the carriage returns are
// stripped so per-line assertions stay exact.
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

// The one line containing `needle` (and nothing else), or "" when absent.
std::string lineContaining(const std::vector<std::string>& lines, const std::string& needle) {
  for (const std::string& line : lines) {
    if (line.find(needle) != std::string::npos) {
      return line;
    }
  }
  return "";
}

CommandLineOptions fullySetOptions() {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  options.mutable_concurrency()->set_value("2");
  options.mutable_connections()->set_value(50);
  options.mutable_max_pending_requests()->set_value(1024);
  options.mutable_max_active_requests()->set_value(512);
  return options;
}

// The five-knob vocabulary and its steering split: rps + ramp render
// LIVE (M3); concurrency, connections and the queue bounds render
// PHASE-GATED. The pane also carries the display-only banner so no hidden
// timing semantics can masquerade as implemented steering (spec §3).
TEST(KnobPaneTest, KnobPaneMarksLiveVsPhaseGated) {
  std::vector<KnobDisplay> knobs = KnobsFromOptions(fullySetOptions());
  ASSERT_EQ(knobs.size(), 5u);
  EXPECT_EQ(knobs[0].name, "rps");
  EXPECT_EQ(knobs[1].name, "ramp");
  EXPECT_EQ(knobs[2].name, "concurrency");
  EXPECT_EQ(knobs[3].name, "connections");
  EXPECT_EQ(knobs[4].name, "queue bounds");

  EXPECT_EQ(knobs[0].steering, KnobSteering::kLiveM3);
  EXPECT_EQ(knobs[1].steering, KnobSteering::kLiveM3);
  EXPECT_EQ(knobs[2].steering, KnobSteering::kPhaseGated);
  EXPECT_EQ(knobs[3].steering, KnobSteering::kPhaseGated);
  EXPECT_EQ(knobs[4].steering, KnobSteering::kPhaseGated);

  // Values carry through: set knobs show their current value.
  EXPECT_EQ(knobs[0].value, "100");
  EXPECT_EQ(knobs[2].value, "2");
  EXPECT_EQ(knobs[3].value, "50");

  ftxui::Element pane = RenderKnobPane(knobs, /*width=*/100, /*height=*/20);
  std::vector<std::string> lines = screenLines(pane, 100, 20);

  EXPECT_NE(lineContaining(lines, "rps").find("LIVE (M3)"), std::string::npos);
  EXPECT_NE(lineContaining(lines, "ramp").find("LIVE (M3)"), std::string::npos);
  EXPECT_NE(lineContaining(lines, "concurrency").find("PHASE-GATED"), std::string::npos);
  EXPECT_NE(lineContaining(lines, "connections").find("PHASE-GATED"), std::string::npos);
  EXPECT_NE(lineContaining(lines, "queue bounds").find("PHASE-GATED"), std::string::npos);

  // Display-only banner: the pane states that steering lands in M3.
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("steering lands in M3"), std::string::npos);

  // The set queue bounds carry their two values.
  std::string queue_line = lineContaining(lines, "queue bounds");
  EXPECT_NE(queue_line.find("1024"), std::string::npos);
  EXPECT_NE(queue_line.find("512"), std::string::npos);
}

// Empty options never render an empty value cell: unset knobs say
// "unset", the unconfigured ramp says "not configured".
TEST(KnobPaneTest, KnobPaneShowsUnsetDefaults) {
  nighthawk::client::CommandLineOptions options; // nothing set
  std::vector<KnobDisplay> knobs = KnobsFromOptions(options);
  ASSERT_EQ(knobs.size(), 5u);
  for (const KnobDisplay& knob : knobs) {
    EXPECT_FALSE(knob.value.empty()) << "knob " << knob.name << " renders an empty value";
    EXPECT_TRUE(knob.value == "unset" || knob.value == "not configured")
        << "knob " << knob.name << " renders \"" << knob.value << "\"";
  }
  EXPECT_EQ(knobs[1].value, "not configured");

  ftxui::Element pane = RenderKnobPane(knobs, /*width=*/100, /*height=*/20);
  std::string text;
  for (const std::string& line : screenLines(pane, 100, 20)) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("unset"), std::string::npos);
  EXPECT_NE(text.find("not configured"), std::string::npos);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
