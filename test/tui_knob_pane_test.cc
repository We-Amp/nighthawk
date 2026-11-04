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

// ---------------------------------------------------------------------------
// M2 Task 7: the staged next-phase preview. While a run is active, edits to
// phase-gated knobs stage into a preview that is visually distinct, never
// applied to the current phase, and discardable.
// ---------------------------------------------------------------------------

using nighthawk::client::PhaseSpec;

PhaseSpec stagedSpec() {
  PhaseSpec spec;
  spec.set_name("spike");
  spec.mutable_duration()->set_seconds(5);
  spec.set_requests_per_second(400);
  spec.mutable_concurrency()->set_value("4");
  spec.mutable_connections()->set_value(64);
  spec.mutable_max_pending_requests()->set_value(2048);
  return spec;
}

// The staged state is visually distinct: every preview line carries the
// STAGED tag (never LIVE (M3) or PHASE-GATED), a header names the staged
// phase and states the preview is discardable, and the staged values —
// including the connections value (PhaseSpec field 5) — all render.
TEST(KnobPaneTest, StagedEditsVisuallyDistinctAndDiscardable) {
  CommandLineOptions current = fullySetOptions();
  PhaseSpec staged = stagedSpec();

  std::vector<KnobDisplay> preview = NextPhasePreviewKnobs(staged, current);
  ASSERT_EQ(preview.size(), 4u);
  EXPECT_EQ(preview[0].name, "rps");
  EXPECT_EQ(preview[1].name, "concurrency");
  EXPECT_EQ(preview[2].name, "connections");
  EXPECT_EQ(preview[3].name, "queue bounds");
  for (const KnobDisplay& knob : preview) {
    EXPECT_EQ(knob.steering, KnobSteering::kStaged) << "knob " << knob.name;
  }
  EXPECT_EQ(preview[0].value, "400");
  EXPECT_EQ(preview[1].value, "4");
  // Staged connections value (PhaseSpec field 5, major-1 consistency).
  EXPECT_EQ(preview[2].value, "64");
  // Staged pending bound; active not staged: inherits the run-level value
  // (each half states its own provenance).
  EXPECT_EQ(preview[3].value, "pending=2048 active=512 (inherit)");

  ftxui::Element pane = RenderNextPhasePreview(staged, current, /*current_workers=*/2,
                                               /*next_phase_index=*/1, /*width=*/100, /*height=*/20);
  std::vector<std::string> lines = screenLines(pane, 100, 20);
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("[staged: spike"), std::string::npos) << text;
  EXPECT_NE(text.find("discard"), std::string::npos) << text;
  EXPECT_NE(text.find("STAGED"), std::string::npos) << text;
  EXPECT_NE(text.find("400"), std::string::npos) << text;
  EXPECT_NE(text.find("64"), std::string::npos) << text;
  // Cold-start surfacing: 4 staged workers over 2 active -> the two new
  // workers start cold (Task 6 metadata wording).
  EXPECT_NE(text.find("starts 2 new worker(s) (cold)"), std::string::npos) << text;
}

// Discarding the staged edits (empty staged spec) renders nothing: the
// preview is empty and the current-phase display is untouched.
TEST(KnobPaneTest, DiscardedStagingRendersNoPreview) {
  CommandLineOptions current = fullySetOptions();
  PhaseSpec discarded; // no name: nothing staged

  EXPECT_TRUE(NextPhasePreviewKnobs(discarded, current).empty());
  ftxui::Element pane = RenderNextPhasePreview(discarded, current, 2, 1, 100, 20);
  std::string text;
  for (const std::string& line : screenLines(pane, 100, 20)) {
    text += line;
  }
  EXPECT_EQ(text.find("staged"), std::string::npos) << text;
  EXPECT_EQ(text.find("STAGED"), std::string::npos) << text;
}

// Unstaged preview fields inherit: rps/concurrency/queue bounds fall back to
// the run-level values, connections falls back to the run-level value before
// reading unset.
TEST(KnobPaneTest, UnstagedPreviewFieldsInheritRunValues) {
  CommandLineOptions current = fullySetOptions();
  PhaseSpec staged; // a spec with a name but no staged values
  staged.set_name("coast");

  std::vector<KnobDisplay> preview = NextPhasePreviewKnobs(staged, current);
  ASSERT_EQ(preview.size(), 4u);
  EXPECT_EQ(preview[0].value, "100 (inherit)");
  EXPECT_EQ(preview[1].value, "2 (inherit)");
  EXPECT_EQ(preview[2].value, "50 (inherit)");
  EXPECT_EQ(preview[3].value, "pending=1024 (inherit) active=512 (inherit)");
}

// The worker-set annotation between the current phase and the staged one:
// growth cold-starts the delta, shrink folds it, equality says nothing.
TEST(KnobPaneTest, WorkerSetAnnotationCoversColdStartFoldAndFlat) {
  EXPECT_EQ(WorkerSetAnnotation(/*current_workers=*/4, /*staged_workers=*/4,
                                /*next_phase_index=*/2),
            "");
  EXPECT_EQ(WorkerSetAnnotation(2, 5, 2), "starts 3 new worker(s) (cold)");
  EXPECT_EQ(WorkerSetAnnotation(4, 2, 2), "2 workers folded into phase 2");
}

// A non-numeric staged concurrency ("auto") renders the preview without a
// worker-set annotation: the worker delta is a count comparison, and a
// non-numeric value carries no count.
TEST(KnobPaneTest, NonNumericStagedConcurrencyDoesNotAnnotate) {
  CommandLineOptions current = fullySetOptions();
  PhaseSpec staged = stagedSpec();
  staged.mutable_concurrency()->set_value("auto");

  ftxui::Element pane = RenderNextPhasePreview(staged, current, /*current_workers=*/2,
                                               /*next_phase_index=*/1, /*width=*/100,
                                               /*height=*/20);
  std::string text;
  for (const std::string& line : screenLines(pane, 100, 20)) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("[staged: spike"), std::string::npos) << text;
  EXPECT_EQ(text.find("new worker(s)"), std::string::npos) << text;
  EXPECT_EQ(text.find("workers folded"), std::string::npos) << text;
}

// The staged edits never leak into the current phase's knob display: the
// current pane is built only from the run options and never carries the
// staged tag or the staged values.
TEST(KnobPaneTest, StagedEditsNeverLeakIntoCurrentPane) {
  CommandLineOptions current = fullySetOptions();
  std::vector<KnobDisplay> knobs = KnobsFromOptions(current);
  for (const KnobDisplay& knob : knobs) {
    EXPECT_NE(knob.steering, KnobSteering::kStaged) << "knob " << knob.name;
    EXPECT_NE(knob.value, "400") << "knob " << knob.name;
    EXPECT_NE(knob.value, "64") << "knob " << knob.name;
  }

  ftxui::Element pane = RenderKnobPane(knobs, 100, 20);
  std::string text;
  for (const std::string& line : screenLines(pane, 100, 20)) {
    text += line + "\n";
  }
  EXPECT_EQ(text.find("STAGED"), std::string::npos) << text;
  EXPECT_EQ(text.find("staged"), std::string::npos) << text;
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
