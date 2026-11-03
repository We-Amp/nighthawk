#include "source/client/tui/charts.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr const char* kPlaceholder = "waiting for first flush";

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
int rowFor(uint64_t value, uint64_t max, int rows) {
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

} // namespace

ftxui::Element RenderPercentileBand(const std::vector<PercentilePoint>& points, int width,
                                    int height) {
  if (points.empty()) {
    return ftxui::text(kPlaceholder);
  }
  // Early-out before any width-derived allocation: a negative width would
  // turn the static_cast<size_t> into a huge allocation below.
  if (width <= 0) {
    return ftxui::emptyElement();
  }

  const std::vector<PercentilePoint> window = windowOf(points, width);
  uint64_t max_p99 = 1;
  for (const PercentilePoint& point : window) {
    max_p99 = std::max(max_p99, point.p99_ns);
  }

  // One legend row above the band.
  const int band_rows = std::max(1, height - 1);
  std::vector<std::string> grid(band_rows, std::string(static_cast<size_t>(width), ' '));
  for (size_t column = 0; column < window.size() && static_cast<int>(column) < width; ++column) {
    const PercentilePoint& point = window[column];
    // Three stacked marks per data column: the p99 envelope dot, the p90
    // shoulder, and the p50 body. Columns are point-indexed, so a snapshot
    // the reducer skipped (warm-up count == 0) shifts later points one column
    // left instead of leaving a blank column — a v0 data-model limitation,
    // not a rendered gap.
    grid[rowFor(point.p99_ns, max_p99, band_rows)][column] = '.';
    grid[rowFor(point.p90_ns, max_p99, band_rows)][column] = '+';
    grid[rowFor(point.p50_ns, max_p99, band_rows)][column] = '#';
  }

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text("p50   p90   p99"));
  elements.push_back(rowsAsElement(grid));
  return ftxui::vbox(std::move(elements));
}

ftxui::Element RenderCounterLine(const std::vector<CounterPoint>& points, absl::string_view label,
                                 int width, int height) {
  if (points.empty()) {
    return ftxui::text(absl::StrCat(label, " ", kPlaceholder));
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

  std::vector<ftxui::Element> elements;
  elements.push_back(ftxui::text(absl::StrCat(label, " max=", max_value)));
  elements.push_back(rowsAsElement(sparkGrid(column_values, max_value, width,
                                             std::max(1, height - 1))));
  return ftxui::vbox(std::move(elements));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
