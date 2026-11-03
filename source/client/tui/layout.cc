#include "source/client/tui/layout.h"

#include <algorithm>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"

#include "source/client/tui/charts.h"
#include "source/client/tui/knob_pane.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

ftxui::Element RenderMinSizeMessage(int width, int height) {
  return ftxui::text(absl::StrCat("terminal too small: Nighthawk TUI needs at least ",
                                  kMinTerminalWidth, "x", kMinTerminalHeight, ", got ", width, "x",
                                  height));
}

ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height) {
  std::vector<ftxui::Element> rows;
  const int band_height = std::max(1, height / 2);
  rows.push_back(RenderPercentileBand(model.latency_band, width, band_height));

  // Sorted counter names keep frames deterministic (the model's hash map
  // iterates in unspecified order).
  std::vector<std::string> names;
  names.reserve(model.counters.size());
  for (const auto& [name, points] : model.counters) {
    names.push_back(name);
  }
  std::sort(names.begin(), names.end());

  const int remaining = std::max(0, height - band_height);
  const int per_counter =
      names.empty() ? 0 : std::max(1, remaining / static_cast<int>(names.size()));
  for (const std::string& name : names) {
    rows.push_back(RenderCounterLine(model.counters.at(name), name, width, per_counter));
  }
  return ftxui::vbox(std::move(rows));
}

ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options) {
  // The min-size gate wraps everything: below the declared minimum, render
  // the message and nothing else — no partial status bar, no wrapped charts.
  if (width < kMinTerminalWidth || height < kMinTerminalHeight) {
    return RenderMinSizeMessage(width, height);
  }

  ftxui::Element pane;
  switch (active_tab) {
  case 0:
    pane = RenderChartsPane(model, width, height - 3);
    break;
  case 1:
    // Tab 1 is the knob pane, rendered from the most recent run's options
    // (unset defaults before any run). Empty options never render an empty
    // pane: KnobsFromOptions yields the five knobs with "unset"/"not
    // configured" values.
    pane = RenderKnobPane(KnobsFromOptions(knob_options), width, height - 3);
    break;
  default:
    pane = ftxui::text(absl::StrCat("unknown tab ", active_tab));
    break;
  }

  return ftxui::vbox({
      RenderStatusBar(model, mode, session_path, dialog.uri()),
      pane,
  });
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
