#include "source/client/tui/charts.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
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

// Keeps only the newest `width` points in the window (older points drop off
// the left edge once the chart is full).
template <typename PointT>
std::vector<PointT> windowOf(const std::vector<PointT>& points, int width) {
  size_t start = 0;
  if (width > 0 && points.size() > static_cast<size_t>(width)) {
    start = points.size() - static_cast<size_t>(width);
  }
  return std::vector<PointT>(points.begin() + static_cast<long>(start), points.end());
}

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

// Draws one glyph per data column at the row the value scales to, on a blank
// character grid of `rows` lines by `width` columns.
std::vector<std::string> sparkGrid(const std::vector<uint64_t>& column_values, uint64_t max,
                                   int width, int rows) {
  if (width <= 0) {
    return std::vector<std::string>(rows, std::string());
  }
  std::vector<std::string> grid(rows, std::string(static_cast<size_t>(width), ' '));
  for (size_t column = 0; column < column_values.size() && static_cast<int>(column) < width;
       ++column) {
    const int row = rowFor(column_values[column], max, rows);
    grid[row][column] = '*';
  }
  return grid;
}

ftxui::Element rowsAsElement(const std::vector<std::string>& rows) {
  std::vector<ftxui::Element> elements;
  elements.reserve(rows.size());
  for (const std::string& row : rows) {
    elements.push_back(ftxui::text(row));
  }
  return ftxui::vbox(std::move(elements));
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

} // namespace

ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height, const BandAnnotations& annotations,
                                    absl::string_view empty_message) {
  if (points.empty()) {
    return ftxui::text(std::string(empty_message.empty() ? kDefaultPlaceholder : empty_message));
  }
  // Early-out before any width-derived allocation: a negative width would
  // turn the static_cast<size_t> into a huge allocation below.
  if (width <= 0) {
    return ftxui::emptyElement();
  }

  const std::vector<PercentilePoint> window = windowOf(points, width);
  uint64_t max_p99 = 1;
  size_t max_column = 0;
  for (size_t column = 0; column < window.size(); ++column) {
    if (window[column].p99_ns >= max_p99) {
      max_p99 = window[column].p99_ns;
      max_column = column;
    }
  }
  const PercentilePoint& newest = window.back();

  // The header row IS the legend: glyph -> series in draw order (p99's "."
  // is painted last and owns shared cells), units in the label, one column
  // per flush. The spike tail annotates the excursion numerically; the
  // right-margin readout pair carries the current and window-max p99, and
  // the one-frame "(rescaled)" marker rides on it.
  std::string left = "# p50   + p90   . p99 ms";
  const std::string middle = "   1 col = 1 flush";
  if (annotations.spike) {
    absl::StrAppend(&left, "   p99 ", formatMs(max_p99), " ^",
                    absl::StrFormat("%.1f", annotations.spike_ratio), "x");
  }
  std::string right = absl::StrCat("now ", formatMs(newest.p99_ns), "  max ", formatMs(max_p99));
  if (annotations.rescaled) {
    absl::StrAppend(&right, " (rescaled)");
  }

  // One labeled gridline at a nice ms step under half the window max, drawn
  // after the data so every data glyph keeps its cell: dashes land on blank
  // cells only, and the label is stamped only when its region stayed free.
  const auto [grid_ns, grid_label] = niceGridline(max_p99);

  const int band_rows = std::max(1, height - 1);
  std::vector<std::string> grid(band_rows, std::string(static_cast<size_t>(width), ' '));
  for (size_t column = 0; column < window.size() && static_cast<int>(column) < width; ++column) {
    const PercentilePoint& point = window[column];
    // Three stacked marks per data column: the p50 body, the p90 shoulder,
    // and the p99 envelope dot — drawn in that order so the highest
    // percentile owns shared cells (the p50 floor no longer erases the p99
    // tail when bands compress). Columns are point-indexed, so a snapshot
    // the reducer skipped (warm-up count == 0) shifts later points one
    // column left instead of leaving a blank column — a v0 data-model
    // limitation, not a rendered gap.
    grid[rowFor(point.p50_ns, max_p99, band_rows)][column] = '#';
    grid[rowFor(point.p90_ns, max_p99, band_rows)][column] = '+';
    grid[rowFor(point.p99_ns, max_p99, band_rows)][column] = '.';
  }
  if (annotations.spike) {
    // The excursion salience glyph at the excursion's own cell (the window's
    // p99 max), where the lone envelope dot would be missed.
    grid[rowFor(max_p99, max_p99, band_rows)][max_column] = '!';
  }
  if (!grid_label.empty()) {
    const int grid_row = rowFor(grid_ns, max_p99, band_rows);
    std::string& line = grid[static_cast<size_t>(grid_row)];
    for (size_t column = 0; column < line.size(); ++column) {
      if (line[column] == ' ') {
        line[column] = '-';
      }
    }
    const bool label_free =
        line.compare(0, grid_label.size(), std::string(grid_label.size(), '-')) == 0;
    if (label_free) {
      std::copy(grid_label.begin(), grid_label.end(), line.begin());
    }
  }

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(composeHeader(left, middle, right, width)));
  elements.push_back(rowsAsElement(grid));
  return ftxui::vbox(std::move(elements));
}

ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height, absl::string_view empty_message) {
  if (points.empty()) {
    return ftxui::text(absl::StrCat(
        label, " ",
        std::string(empty_message.empty() ? kDefaultPlaceholder : empty_message)));
  }
  // Same negative-width guard as RenderPercentileBand.
  if (width <= 0) {
    return ftxui::emptyElement();
  }

  const std::vector<CounterPoint> window = windowOf(points, width);
  uint64_t max_value = 1;
  for (const CounterPoint& point : window) {
    max_value = std::max(max_value, point.value);
  }

  std::vector<uint64_t> column_values;
  column_values.reserve(window.size());
  for (const CounterPoint& point : window) {
    column_values.push_back(point.value);
  }

  // Header: the raw series label with its unit (the live counters are
  // process-lifetime cumulative counts until the feed deltas them — labeling
  // them "/flush" would lie), and the now/max readout pair at the right
  // margin.
  const std::string left = absl::StrCat(label, " count");
  const std::string right =
      absl::StrCat("now ", window.back().value, "  max ", max_value);

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(composeHeader(left, right, width)));
  elements.push_back(rowsAsElement(sparkGrid(column_values, max_value, width,
                                             std::max(1, height - 1))));
  return ftxui::vbox(std::move(elements));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
