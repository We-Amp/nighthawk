#pragma once

#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): pure Snapshot-series → ftxui::Element chart renderers
// (spec §7). Hand-rolled sparkline / percentile-band drawing on the built-in
// FTXUI elements — no external chart dependency (quarantine rule). Both
// functions are windowed: when the series holds more points than `width`
// columns, only the newest `width` points render, one column per point
// (1 col = 1 flush).

// Per-band chart annotations the shell's model carries (see RunViewModel):
// the one-frame "(rescaled)" marker and the spike excursion salience.
struct BandAnnotations {
  // Render "!" at the newest p99 cell and the "^Nx" tail in the header.
  bool spike = false;
  double spike_ratio = 0.0;
  // Append the one-frame "(rescaled)" marker to the header readout.
  bool rescaled = false;
};

// A three-line stacked band for the p50/p90/p99 latency series, y-scaled to
// the max p99 in the window. The header row IS the legend — glyph to series,
// in draw order (`# p50   + p90   . p99 ms   1 col = 1 flush`) — and carries
// the right-margin readout pair (`now X  max Y`, ms). One labeled gridline
// (a "nice" ms step under half the window max) marks the scale. The glyph
// draw order puts p99 last so the topmost percentile owns shared cells; a
// spike annotation overwrites the newest p99 cell with "!" and adds the
// numeric tail (`p99 41.7ms ^4.0x`). Empty input renders `empty_message`.
ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height, const BandAnnotations& annotations = {},
                                    absl::string_view empty_message = "waiting for first flush");

// A labelled sparkline for one counter series: the header row reads
// "<label> count" with the right-margin readout pair (`now X  max Y`, the
// window's newest and largest values). Empty input renders the label plus
// `empty_message`.
ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height,
                                 absl::string_view empty_message = "waiting for first flush");

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
