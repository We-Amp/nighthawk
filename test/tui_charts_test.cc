#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"

#include "source/client/tui/charts.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): pure Snapshot → ftxui::Element chart renderers. The
// assertions work on the rendered screen text so the tests pin what the user
// actually sees, not the element tree.

std::vector<PercentilePoint> percentilePoints(size_t count) {
  std::vector<PercentilePoint> points;
  for (size_t i = 0; i < count; ++i) {
    points.push_back(PercentilePoint{static_cast<uint64_t>(i),
                                     /*p50_ns=*/100, /*p90_ns=*/200,
                                     /*p99_ns=*/300 + static_cast<uint64_t>(i) * 10});
  }
  return points;
}

std::vector<std::string> screenLines(const ftxui::Element& element, int width, int height) {
  ftxui::Screen screen(width, height);
  ftxui::Render(screen, element);
  // ftxui::Screen::ToString() terminates rows with "\r\n"; split on the
  // newline and strip the carriage returns so column indexing stays exact.
  std::string text = screen.ToString();
  std::vector<std::string> lines;
  size_t start = 0;
  while (start <= text.size()) {
    size_t newline = text.find('\n', start);
    std::string line = (newline == std::string::npos)
                           ? text.substr(start)
                           : text.substr(start, newline - start);
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

TEST(ChartsTest, PercentileBandRendersLegendAndPoints) {
  std::vector<PercentilePoint> points = percentilePoints(10);
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 10), 60, 10);

  ASSERT_GE(lines.size(), 2u);
  EXPECT_NE(lines[0].find("p50"), std::string::npos);
  EXPECT_NE(lines[0].find("p90"), std::string::npos);
  EXPECT_NE(lines[0].find("p99"), std::string::npos);

  // The last data column (column 9, one column per point with 10 points in a
  // 60-column chart) carries band glyphs in the rows under the legend.
  bool glyph_in_last_column = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].size() > 9 && lines[row][9] != ' ') {
      glyph_in_last_column = true;
    }
  }
  EXPECT_TRUE(glyph_in_last_column);
}

TEST(ChartsTest, PercentileBandEmptyInputRendersPlaceholder) {
  std::vector<std::string> lines = screenLines(RenderPercentileBand({}, 60, 10), 60, 10);
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("waiting for first flush"), std::string::npos);
}

TEST(ChartsTest, CounterLineRendersLabelAndLatest) {
  std::vector<CounterPoint> points;
  for (uint64_t i = 0; i < 10; ++i) {
    points.push_back(CounterPoint{i, (i + 1) * 100});
  }
  std::vector<std::string> lines =
      screenLines(RenderCounterLine(points, "benchmark.http_2xx", 60, 10), 60, 10);

  ASSERT_GE(lines.size(), 1u);
  std::string header = lines[0];
  EXPECT_NE(header.find("benchmark.http_2xx"), std::string::npos);

  // The rendered maximum must match the last (and largest) point's value.
  size_t max_pos = header.find("max=");
  ASSERT_NE(max_pos, std::string::npos);
  size_t digits_start = max_pos + 4;
  size_t digits_end = digits_start;
  while (digits_end < header.size() && header[digits_end] >= '0' && header[digits_end] <= '9') {
    ++digits_end;
  }
  ASSERT_GT(digits_end, digits_start);
  EXPECT_EQ(std::stoul(header.substr(digits_start, digits_end - digits_start)), 1000u);
}

TEST(ChartsTest, CounterLineEmptyInputRendersPlaceholder) {
  std::vector<std::string> lines =
      screenLines(RenderCounterLine({}, "benchmark.http_5xx", 60, 10), 60, 10);
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("benchmark.http_5xx"), std::string::npos);
  EXPECT_NE(text.find("waiting for first flush"), std::string::npos);
}

TEST(ChartsTest, ChartsRenderWindowedColumnsWithinWidth) {
  // More points than columns: only the newest `width` points render, one
  // column each, with the last data column at index width - 1.
  std::vector<PercentilePoint> points = percentilePoints(100);
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 20, 10), 20, 10);
  bool glyph_in_last_column = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].size() > 19 && lines[row][19] != ' ') {
      glyph_in_last_column = true;
    }
  }
  EXPECT_TRUE(glyph_in_last_column);
  // No column beyond the chart width gets drawn into.
  for (size_t row = 1; row < lines.size(); ++row) {
    ASSERT_LE(lines[row].size(), 20u) << "row " << row << ": [" << lines[row] << "]";
  }
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
