#pragma once

#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): pure Snapshot-series -> ftxui::Element chart renderers
// (spec §7). Hand-rolled sparkline / percentile-band drawing on the built-in
// FTXUI elements — no external chart dependency (quarantine rule).
//
// Every chart is a FIXED-WIDTH SCROLLING VIEWPORT (backlog item 9): it is
// `width` columns wide from the first flush, columns are TIME-ANCHORED (one
// column per flush interval, derived from the points' monotonic stamps), and
// the oldest samples fall off the left edge once the series outgrows the
// window. A flush the reducer skipped leaves a blank column instead of
// shifting later samples left — the viewport IS the time reference.

// Per-band chart annotations the shell's model carries (see RunViewModel):
// the one-frame "(rescaled)" marker and the spike excursion salience.
struct BandAnnotations {
  // Render "!" at the newest p99 cell and the "^Nx" tail in the header, and
  // join "!" to the legend while a spike window is open (batch-2 polish).
  bool spike = false;
  double spike_ratio = 0.0;
  // Append the one-frame "(rescaled)" marker to the header readout.
  bool rescaled = false;
};

// A three-line stacked band for the p50/p90/p99 latency series, y-scaled to
// the max p99 in the VISIBLE window. The header row IS the legend — glyph to
// series in draw order (`# p50   + p90   . p99 ms`), joined by "! excursion"
// while a spike window is open — and carries the window stamp on its tail
// (`last 96s (1s/col)   oldest t+3s   newest t+42s`: the viewport's time
// reference, once per chart group) plus the right-margin readout pair
// (`now X  max Y`, ms; the one-frame "(rescaled)" marker rides on it). One
// labeled gridline (a "nice" ms step under half the window max) marks the
// scale, stamped at the first fully-free span of its row. The glyph draw
// order puts p99 last so the topmost percentile owns shared cells; a spike
// annotation overwrites the excursion's own cell with "!" and adds the
// numeric tail (`p99 41.7ms ^4.0x`). Empty input renders `empty_message`.
ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height, const BandAnnotations& annotations = {},
                                    absl::string_view empty_message = "waiting for first flush");

// A labelled sparkline for one counter series: the header row reads
// "<label> count" with the right-margin readout pair (`now X  max Y`, the
// window's newest and largest values). Empty input renders the label plus
// `empty_message`.
//
// With `capacity_cap` > 0 (backlog item 12, pool-pending capacity context):
// the header reads the pending depth as a fraction of the run's configured
// gate (`pool pending cur=312/2048 (15%)`) and the grid draws a literal
// CEILING RULE — a row of "|" at the cap's height, on blank cells only so
// every data glyph keeps its cell — so saturation is a ceiling the band can
// visibly hit.
ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height, uint64_t capacity_cap = 0,
                                 absl::string_view empty_message = "waiting for first flush");

// The compact health line (backlog item 10): the per-flush RATES of the
// client-side failure counters — `4xx 2/s | rst 0/s | t/o 0/s` — computed as
// deltas of the cumulative counter series (the engine suppresses zero
// counters, so an absent series reads as a steady 0/s: zeros are
// information). The right-margin pair (`now X/s  max Y/s`) is the summed
// bad-event rate, and the grid below draws that summed rate as one
// sparkline. Always renders, even when every series is empty.
ftxui::Element RenderHealthLine(const std::vector<CounterPoint>& http_4xx,
                                const std::vector<CounterPoint>& stream_resets,
                                const std::vector<CounterPoint>& pool_connection_failure,
                                int width, int height);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
