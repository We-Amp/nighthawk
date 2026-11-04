#include "source/client/tui/charts.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ftxui/dom/elements.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr const char* kDefaultPlaceholder = "waiting for first flush";

// The flush interval the charts anchor their columns to: the most common
// positive gap between consecutive stamps of the series (a skipped flush then
// leaves a blank column instead of shrinking the interval), falling back to
// the default 1s stats-flush cadence when the series is too short to say.
uint64_t intervalNsOf(const std::vector<uint64_t>& stamps) {
  constexpr uint64_t kDefaultIntervalNs = 1000000000ull;
  if (stamps.size() < 2) {
    return kDefaultIntervalNs;
  }
  std::map<uint64_t, int> gap_counts;
  for (size_t i = 1; i < stamps.size(); ++i) {
    if (stamps[i] > stamps[i - 1]) {
      ++gap_counts[stamps[i] - stamps[i - 1]];
    }
  }
  if (gap_counts.empty()) {
    return kDefaultIntervalNs;
  }
  uint64_t best = kDefaultIntervalNs;
  int best_count = -1;
  for (const auto& [gap, count] : gap_counts) {
    if (count > best_count) {
      best_count = count;
      best = gap;
    }
  }
  return best;
}

// The data column a sample lands on in a fixed-width scrolling viewport:
// time-anchored, one column per interval, newest at the right edge. Returns
// -1 for samples older than the window (they have fallen off the left edge).
int columnFor(uint64_t stamp_ns, uint64_t newest_ns, uint64_t interval_ns, int width) {
  if (stamp_ns > newest_ns || interval_ns == 0) {
    return -1;
  }
  const uint64_t age = newest_ns - stamp_ns;
  const uint64_t offset = age / interval_ns;
  if (offset >= static_cast<uint64_t>(width)) {
    return -1;
  }
  return static_cast<int>(width - 1 - offset);
}

// Nanoseconds as whole seconds, rounded to nearest.
uint64_t secondsOf(uint64_t ns) { return (ns + 500000000ull) / 1000000000ull; }

// Maps a value in [0, max] onto a row index in [0, rows - 1]; row 0 is the
// chart's top line. Overflow-safe: the value is clamped to max first, then
// scaled via a division-first long-double form, because even a clamped
// value * span multiply overflows uint64 when both near UINT64_MAX.
// Regression (ASan heap-buffer-overflow, intermittent TUI crashes during
// long dialog drives): with rows == 1 the span below is max(1, rows - 1) == 1,
// so every value below the max scaled to 0 and the returned row was 1 — one
// PAST the single-row grid. sparkGrid then wrote `grid[1][column] = '*'`,
// up to `width` bytes past a 24-byte heap buffer: random heap corruption
// (crash victims elsewhere: proto string headers, malloc freelist, ftxui
// std::functions). A one-row grid can only ever paint row 0.
int rowFor(uint64_t value, uint64_t max, int rows) {
  if (rows <= 1) {
    return 0;
  }
  const int span = std::max(1, rows - 1);
  const uint64_t ceiling = std::max<uint64_t>(max, 1);
  const uint64_t clamped = std::min(value, ceiling);
  const uint64_t scaled = static_cast<uint64_t>(
      (static_cast<long double>(clamped) / ceiling) * span);
  return static_cast<int>(span - std::min<uint64_t>(scaled, static_cast<uint64_t>(span)));
}

// Nanoseconds as a one-decimal millisecond readout ("41.7ms") for the chart
// headers and the gridline label.
std::string formatMs(uint64_t ns) {
  return absl::StrFormat("%.1fms", static_cast<double>(ns) / 1e6);
}

// One header row: `left` at the left edge, `right` flush at the right margin,
// padded between. `middle` is dropped first when the terminal is too narrow
// for the full triple, and the left part truncates at `width` last — so the
// readout pair survives to the smallest width the pane is ever given.
std::string composeHeader(const std::string& left, const std::string& middle,
                          const std::string& right, int width) {
  const auto fits = [&width, &right](const std::string& candidate) {
    return static_cast<int>(candidate.size() + 1 + right.size()) <= width;
  };
  std::string joined = left;
  if (fits(joined + middle)) {
    joined += middle;
  }
  if (!fits(joined)) {
    return joined.substr(0, std::min(static_cast<size_t>(width), joined.size()));
  }
  joined.resize(static_cast<size_t>(width) - right.size(), ' ');
  return joined + right;
}

std::string composeHeader(const std::string& left, const std::string& right, int width) {
  return composeHeader(left, "", right, width);
}

// The largest "nice" latency step (1/2/5 x 10^k ms) at or under `limit_ns`,
// for the band's single labeled gridline; {0, ""} when none qualifies.
std::pair<uint64_t, std::string> niceGridline(uint64_t limit_ns) {
  uint64_t best_ns = 0;
  std::string best_label;
  // 1/2/5 mantissas over ms decades: 1ms, 2ms, 5ms, 10ms, ... up to 500s.
  static const uint64_t kMantissas[] = {1, 2, 5};
  for (int decade = 0; decade <= 7; ++decade) {
    uint64_t decade_ms = 1;
    for (int i = 0; i < decade; ++i) {
      decade_ms *= 10;
    }
    for (uint64_t mantissa : kMantissas) {
      const uint64_t ns = mantissa * decade_ms * 1000000ull;
      if (ns > limit_ns / 2) {
        continue;
      }
      if (ns >= best_ns) {
        best_ns = ns;
        best_label = absl::StrCat(mantissa * decade_ms, "ms");
      }
    }
  }
  return {best_ns, best_label};
}

// The first fully-free span of `length` blanks in `line`, or -1 (batch-2
// polish: the gridline label stamps at the first fully-free span of its row,
// not only at column 0).
int firstFreeSpan(const std::string& line, size_t length) {
  if (length == 0 || line.size() < length) {
    return -1;
  }
  size_t run = 0;
  for (size_t i = 0; i < line.size(); ++i) {
    if (line[i] == ' ') {
      ++run;
      if (run >= length) {
        return static_cast<int>(i) - static_cast<int>(length) + 1;
      }
    } else {
      run = 0;
    }
  }
  return -1;
}

ftxui::Element rowsAsElement(const std::vector<std::string>& rows) {
  std::vector<ftxui::Element> elements;
  elements.reserve(rows.size());
  for (const std::string& row : rows) {
    elements.push_back(ftxui::text(row));
  }
  return ftxui::vbox(std::move(elements));
}

} // namespace

ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height, const BandAnnotations& annotations,
                                    absl::string_view empty_message,
                                    const std::vector<uint64_t>& boundary_stamps_ns) {
  if (points.empty()) {
    return ftxui::text(std::string(empty_message.empty() ? kDefaultPlaceholder : empty_message));
  }
  // Early-out before any width-derived allocation: a negative width would
  // turn the static_cast<size_t> into a huge allocation below.
  if (width <= 0) {
    return ftxui::emptyElement();
  }

  // Time-anchored placement: every sample lands on the column its own
  // monotonic stamp dictates (one column per flush interval), so a flush the
  // reducer skipped leaves a blank column and samples never shift left.
  const uint64_t newest_ns = points.back().monotonic_ns;
  const uint64_t interval_ns = intervalNsOf(
      [&points]() {
        std::vector<uint64_t> stamps;
        stamps.reserve(points.size());
        for (const PercentilePoint& point : points) {
          stamps.push_back(point.monotonic_ns);
        }
        return stamps;
      }());

  struct Placed {
    int column;
    const PercentilePoint* point;
  };
  std::vector<Placed> placed;
  placed.reserve(points.size());
  uint64_t max_p99 = 1;
  int max_column = 0;
  uint64_t oldest_visible_ns = newest_ns;
  for (const PercentilePoint& point : points) {
    const int column = columnFor(point.monotonic_ns, newest_ns, interval_ns, width);
    if (column < 0) {
      continue;
    }
    placed.push_back(Placed{column, &point});
    oldest_visible_ns = std::min(oldest_visible_ns, point.monotonic_ns);
    if (point.p99_ns >= max_p99) {
      max_p99 = point.p99_ns;
      max_column = column;
    }
  }
  const PercentilePoint& newest = points.back();

  // The header row IS the legend: glyph -> series in draw order (p99's "."
  // is painted last and owns shared cells), units in the label, one column
  // per flush interval. While a spike window is open the "!" glyph joins the
  // legend (batch-2 polish), the excursion annotates numerically, and the
  // right-margin readout pair carries the current and visible-window max
  // p99 with the one-frame "(rescaled)" marker riding on it.
  std::string left = "# p50   + p90   . p99 ms";
  if (annotations.spike) {
    absl::StrAppend(&left, "   ! excursion   p99 ", formatMs(max_p99), " ^",
                    absl::StrFormat("%.1f", annotations.spike_ratio), "x");
  }
  // The window stamp rides on the legend tail: the viewport's time reference,
  // once per chart group. Edges are relative to the series' first stored
  // sample (the run's first flush until the 600-point store rolls over).
  // Compact by design: the stamp plus edges must fit beside the legend and
  // the readout pair at the 100-column minimum.
  const std::string middle = absl::StrCat(
      "   last ", secondsOf(newest_ns - oldest_visible_ns), "s (",
      secondsOf(interval_ns), "s/col)  t+", secondsOf(oldest_visible_ns - points.front().monotonic_ns),
      "s..t+", secondsOf(newest_ns - points.front().monotonic_ns), "s");
  std::string right = absl::StrCat("now ", formatMs(newest.p99_ns), "  max ", formatMs(max_p99));
  if (annotations.rescaled) {
    absl::StrAppend(&right, " (rescaled)");
  }

  // One labeled gridline at a nice ms step under half the window max, drawn
  // after the data so every data glyph keeps its cell: dashes land on blank
  // cells only, and the label is stamped at the first fully-free span of the
  // row (batch-2 polish) — no label when no free span exists.
  const auto [grid_ns, grid_label] = niceGridline(max_p99);

  const int band_rows = std::max(1, height - 1);
  std::vector<std::string> grid(band_rows, std::string(static_cast<size_t>(width), ' '));
  for (const Placed& cell : placed) {
    const PercentilePoint& point = *cell.point;
    // Three stacked marks per data column: the p50 body, the p90 shoulder,
    // and the p99 envelope dot — drawn in that order so the highest
    // percentile owns shared cells (the p50 floor no longer erases the p99
    // tail when bands compress).
    grid[rowFor(point.p50_ns, max_p99, band_rows)][cell.column] = '#';
    grid[rowFor(point.p90_ns, max_p99, band_rows)][cell.column] = '+';
    grid[rowFor(point.p99_ns, max_p99, band_rows)][cell.column] = '.';
  }
  if (annotations.spike && max_column >= 0) {
    // The excursion salience glyph at the excursion's own cell (the window's
    // p99 max), where the lone envelope dot would be missed.
    grid[rowFor(max_p99, max_p99, band_rows)][max_column] = '!';
  }
  // The M2 band reset markers: at each visible phase boundary the band
  // restarts (a fresh slab), so the boundary's time-anchored column renders
  // a full-height "|" — the pre-boundary glyphs of that column yield to the
  // reset line (counters remain continuous; this is a slab boundary, not a
  // data gap).
  for (const uint64_t boundary_ns : boundary_stamps_ns) {
    const int column = columnFor(boundary_ns, newest_ns, interval_ns, width);
    if (column < 0) {
      continue;
    }
    for (std::string& line : grid) {
      line[static_cast<size_t>(column)] = '|';
    }
  }
  if (!grid_label.empty()) {
    const int grid_row = rowFor(grid_ns, max_p99, band_rows);
    std::string& line = grid[static_cast<size_t>(grid_row)];
    const int stamp_at = firstFreeSpan(line, grid_label.size());
    for (size_t column = 0; column < line.size(); ++column) {
      if (line[column] == ' ') {
        line[column] = '-';
      }
    }
    if (stamp_at >= 0) {
      std::copy(grid_label.begin(), grid_label.end(), line.begin() + stamp_at);
    }
  }

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(composeHeader(left, middle, right, width)));
  elements.push_back(rowsAsElement(grid));
  return ftxui::vbox(std::move(elements));
}

ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height, uint64_t capacity_cap,
                                 absl::string_view empty_message) {
  if (points.empty()) {
    return ftxui::text(absl::StrCat(
        label, " ",
        std::string(empty_message.empty() ? kDefaultPlaceholder : empty_message)));
  }
  // Same negative-width guard as RenderPercentileBand.
  if (width <= 0) {
    return ftxui::emptyElement();
  }

  // Time-anchored placement, exactly as the band's (see above).
  const uint64_t newest_ns = points.back().monotonic_ns;
  const uint64_t interval_ns = intervalNsOf(
      [&points]() {
        std::vector<uint64_t> stamps;
        stamps.reserve(points.size());
        for (const CounterPoint& point : points) {
          stamps.push_back(point.monotonic_ns);
        }
        return stamps;
      }());

  struct Placed {
    int column;
    uint64_t value;
  };
  std::vector<Placed> placed;
  placed.reserve(points.size());
  uint64_t max_value = 1;
  for (const CounterPoint& point : points) {
    const int column = columnFor(point.monotonic_ns, newest_ns, interval_ns, width);
    if (column < 0) {
      continue;
    }
    placed.push_back(Placed{column, point.value});
    max_value = std::max(max_value, point.value);
  }

  // Header: the raw series label with its unit (the live counters are
  // process-lifetime cumulative counts until the feed deltas them — labeling
  // them "/flush" would lie). With a capacity cap (the pool-pending series,
  // backlog item 12) the right margin reads the newest depth as a fraction of
  // the run's configured gate; without one, the now/max readout pair.
  const std::string left = absl::StrCat(label, " count");
  std::string right;
  if (capacity_cap > 0) {
    const uint64_t newest_value = points.back().value;
    const double fraction = static_cast<double>(std::min(newest_value, capacity_cap)) /
                            static_cast<double>(capacity_cap);
    right = absl::StrCat("cur ", newest_value, "/", capacity_cap, " (",
                         absl::StrFormat("%.0f%%", fraction * 100.0), ")");
  } else {
    right = absl::StrCat("now ", points.back().value, "  max ", max_value);
  }

  const int grid_rows = std::max(1, height - 1);
  std::vector<std::string> grid(grid_rows, std::string(static_cast<size_t>(width), ' '));
  for (const Placed& cell : placed) {
    grid[rowFor(cell.value, max_value, grid_rows)][cell.column] = '*';
  }
  if (capacity_cap > 0) {
    // The ceiling rule: a distinct row of "|" at the cap's height, on blank
    // cells only so every data glyph keeps its cell — saturation is a
    // ceiling the band can visibly hit.
    std::string& line = grid[static_cast<size_t>(rowFor(capacity_cap,
                                                        std::max(max_value, capacity_cap),
                                                        grid_rows))];
    for (size_t column = 0; column < line.size(); ++column) {
      if (line[column] == ' ') {
        line[column] = '|';
      }
    }
  }

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(composeHeader(left, right, width)));
  elements.push_back(rowsAsElement(grid));
  return ftxui::vbox(std::move(elements));
}

ftxui::Element RenderHealthLine(const std::vector<CounterPoint>& http_4xx,
                                const std::vector<CounterPoint>& stream_resets,
                                const std::vector<CounterPoint>& pool_connection_failure,
                                int width, int height) {
  // Per-flush rates: deltas of the cumulative counter series, keyed by the
  // snapshot's monotonic stamp (all series ride the same snapshots, so
  // stamps align). An absent series (the engine suppresses zero counters)
  // contributes a steady 0/s — zeros are information.
  const auto deltaAt = [](const std::vector<CounterPoint>& series) {
    std::map<uint64_t, uint64_t> deltas;
    for (size_t i = 0; i < series.size(); ++i) {
      // A counter's first appearance is its delta: the engine suppresses
      // zero counters, so the first sighting already carries the full
      // pre-appearance count (first-appearance-at-0 stays a 0 delta).
      const uint64_t previous = i > 0 ? series[i - 1].value : 0;
      deltas[series[i].monotonic_ns] =
          series[i].value > previous ? series[i].value - previous : 0;
    }
    return deltas;
  };
  const std::map<uint64_t, uint64_t> d4xx = deltaAt(http_4xx);
  const std::map<uint64_t, uint64_t> drst = deltaAt(stream_resets);
  const std::map<uint64_t, uint64_t> dpf = deltaAt(pool_connection_failure);

  std::map<uint64_t, uint64_t> total;
  for (const auto* deltas : {&d4xx, &drst, &dpf}) {
    for (const auto& [stamp, delta] : *deltas) {
      total[stamp] += delta;
    }
  }
  uint64_t max_total = 0;
  for (const auto& [stamp, rate] : total) {
    max_total = std::max(max_total, rate);
  }
  const auto latest = [](const std::map<uint64_t, uint64_t>& deltas) {
    return deltas.empty() ? 0 : deltas.rbegin()->second;
  };

  const std::string left = absl::StrCat("4xx ", latest(d4xx), "/s | rst ", latest(drst),
                                        "/s | t/o ", latest(dpf), "/s");
  const std::string right =
      absl::StrCat("now ", total.empty() ? 0 : total.rbegin()->second, "/s  max ", max_total,
                   "/s");

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(composeHeader(left, right, width)));
  if (width > 0 && height > 1 && !total.empty()) {
    // The summed bad-event rate as one sparkline, newest at the right edge,
    // anchored to the same flush interval as every other chart.
    std::vector<uint64_t> stamps;
    stamps.reserve(total.size());
    for (const auto& [stamp, rate] : total) {
      stamps.push_back(stamp);
    }
    const uint64_t newest_ns = total.rbegin()->first;
    const uint64_t interval_ns = intervalNsOf(stamps);
    const int grid_rows = std::max(1, height - 1);
    std::vector<std::string> grid(grid_rows, std::string(static_cast<size_t>(width), ' '));
    for (const auto& [stamp, rate] : total) {
      const int column = columnFor(stamp, newest_ns, interval_ns, width);
      if (column < 0) {
        continue;
      }
      grid[rowFor(rate, std::max<uint64_t>(max_total, 1), grid_rows)][column] = '*';
    }
    elements.push_back(rowsAsElement(grid));
  }
  return ftxui::vbox(std::move(elements));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
