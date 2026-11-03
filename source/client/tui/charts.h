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
// columns, only the newest `width` points render, one column per point.

// A three-line stacked band for the p50/p90/p99 latency series, y-scaled to
// the max p99 in the window. The first row carries the p50/p90/p99 legend;
// empty input renders the "waiting for first flush" placeholder instead.
ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height);

// A labelled sparkline for one counter series: the first row reads
// "<label> max=<max in window>", the rows below draw one glyph per point,
// y-scaled to that max. Empty input renders the label plus the same
// placeholder as the band.
ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
