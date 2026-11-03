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
                                     /*p50_ns=*/10000000, /*p90_ns=*/20000000,
                                     /*p99_ns=*/30000000 + static_cast<uint64_t>(i) * 1000000});
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
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 70, 10), 70, 10);

  ASSERT_GE(lines.size(), 2u);
  // The header row IS the legend: glyph -> series, units in the label, one
  // column per flush.
  EXPECT_NE(lines[0].find("# p50"), std::string::npos);
  EXPECT_NE(lines[0].find("+ p90"), std::string::npos);
  EXPECT_NE(lines[0].find(". p99 ms"), std::string::npos);
  EXPECT_NE(lines[0].find("1 col = 1 flush"), std::string::npos);

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

// The legend must be truthful: with all three percentiles equal, every band
// glyph compresses onto one cell — and the draw order (p50 '#', then p90 '+',
// then p99 '.') must leave the highest percentile's mark, not the p50 floor.
TEST(ChartsTest, DrawOrderGivesP99TheSharedCells) {
  std::vector<PercentilePoint> points;
  for (int i = 0; i < 10; ++i) {
    points.push_back(PercentilePoint{static_cast<uint64_t>(i), /*p50_ns=*/30000000,
                                     /*p90_ns=*/30000000, /*p99_ns=*/30000000});
  }
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 10), 60, 10);
  bool p99_mark_on_shared_cell = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find("..........") != std::string::npos) {
      p99_mark_on_shared_cell = true;
    }
  }
  EXPECT_TRUE(p99_mark_on_shared_cell);
}

// The right-margin readout pair: the newest and window-max p99 in ms, with
// the "(rescaled)" one-frame marker riding on it when the scale changed.
TEST(ChartsTest, BandHeaderCarriesNowMaxReadoutAndRescaledMarker) {
  std::vector<PercentilePoint> points = percentilePoints(10);
  std::string plain;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 60, 10), 60, 10)) {
    plain += line + "\n";
  }
  EXPECT_NE(plain.find("now 39.0ms"), std::string::npos);
  EXPECT_NE(plain.find("max 39.0ms"), std::string::npos);
  EXPECT_EQ(plain.find("(rescaled)"), std::string::npos);

  // Graceful degradation on a narrow pane: the flush note yields first; the
  // legend and the readout pair survive together down to legend+readout
  // width (24 + 22 = 47 columns).
  std::string narrow;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 50, 10), 50, 10)) {
    narrow += line + "\n";
  }
  EXPECT_NE(narrow.find("now 39.0ms  max 39.0ms"), std::string::npos);
  EXPECT_EQ(narrow.find("1 col = 1 flush"), std::string::npos);

  BandAnnotations rescaled;
  rescaled.rescaled = true;
  std::string marked;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 60, 10, rescaled), 60,
                                             10)) {
    marked += line + "\n";
  }
  EXPECT_NE(marked.find("(rescaled)"), std::string::npos);
}

// Spike salience: the excursion cell carries "!" and the header carries the
// numeric tail with the ratio.
TEST(ChartsTest, SpikeAnnotationRendersGlyphAndNumericTail) {
  std::vector<PercentilePoint> points = percentilePoints(10);
  points.back().p99_ns = 120000000; // a 4x excursion over the ~30ms floor
  BandAnnotations spike;
  spike.spike = true;
  spike.spike_ratio = 4.0;
  std::vector<std::string> lines =
      screenLines(RenderPercentileBand(points, 60, 10, spike), 60, 10);

  EXPECT_NE(lines[0].find("p99 120.0ms ^4.0x"), std::string::npos);
  bool spike_glyph = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find('!') != std::string::npos) {
      spike_glyph = true;
    }
  }
  EXPECT_TRUE(spike_glyph);

  // Without the annotation the same data renders no "!" and no tail.
  std::string plain;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 60, 10), 60, 10)) {
    plain += line + "\n";
  }
  EXPECT_EQ(plain.find('!'), std::string::npos);
  EXPECT_EQ(plain.find("^4.0x"), std::string::npos);
}

// One labeled gridline marks the latency scale (a nice ms step under half the
// window max: max 39ms -> 10ms). Data glyphs keep their cells — the test data
// keeps its p50 off the gridline row so the label stays readable.
TEST(ChartsTest, BandRendersLabeledGridline) {
  std::vector<PercentilePoint> points;
  for (int i = 0; i < 10; ++i) {
    points.push_back(PercentilePoint{static_cast<uint64_t>(i), /*p50_ns=*/15000000,
                                     /*p90_ns=*/22000000,
                                     /*p99_ns=*/30000000 + static_cast<uint64_t>(i) * 1000000});
  }
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 10), 60, 10);
  bool gridline = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find("10ms---") != std::string::npos) {
      gridline = true;
    }
  }
  EXPECT_TRUE(gridline);
}

TEST(ChartsTest, PercentileBandEmptyInputRendersPlaceholder) {
  std::vector<std::string> lines = screenLines(RenderPercentileBand({}, 60, 10), 60, 10);
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("waiting for first flush"), std::string::npos);
}

// The empty state is state-aware: the caller picks the message (the charts
// pane renders the idle message before any run).
TEST(ChartsTest, EmptyInputRendersCallerEmptyMessage) {
  std::vector<std::string> lines =
      screenLines(RenderPercentileBand({}, 60, 10, BandAnnotations{}, "no run yet - press n to start one"),
                  60, 10);
  std::string text;
  for (const std::string& line : lines) {
    text += line + "\n";
  }
  EXPECT_NE(text.find("no run yet - press n to start one"), std::string::npos);
}

TEST(ChartsTest, CounterLineRendersLabelAndReadoutPair) {
  std::vector<CounterPoint> points;
  for (uint64_t i = 0; i < 10; ++i) {
    points.push_back(CounterPoint{i, (i + 1) * 100});
  }
  std::vector<std::string> lines =
      screenLines(RenderCounterLine(points, "benchmark.http_2xx", 60, 10), 60, 10);

  ASSERT_GE(lines.size(), 1u);
  std::string header = lines[0];
  // Unit in the label (the live counters are cumulative counts) plus the
  // right-margin now/max readout pair.
  EXPECT_NE(header.find("benchmark.http_2xx count"), std::string::npos);
  EXPECT_NE(header.find("now 1000  max 1000"), std::string::npos);
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

TEST(ChartsTest, ChartsTolerateDegenerateWidthsAndExtremeValues) {
  std::vector<PercentilePoint> extreme = {
      PercentilePoint{1, 1, 2, UINT64_MAX},
      PercentilePoint{2, 1, 2, UINT64_MAX - 1},
  };
  // A p99 at UINT64_MAX must not overflow the row scaling: it lands on the
  // top band row, and the renderer stays inside the screen.
  std::vector<std::string> lines = screenLines(RenderPercentileBand(extreme, 60, 10), 60, 10);
  ASSERT_GE(lines.size(), 2u);
  EXPECT_NE(lines[1].find('.'), std::string::npos);

  // Degenerate widths must early-out without a huge (negative → size_t)
  // allocation or a crash.
  RenderPercentileBand(percentilePoints(10), 0, 10);
  RenderPercentileBand(percentilePoints(10), -5, 10);
  RenderCounterLine({CounterPoint{1, 5}}, "benchmark.http_2xx", 0, 10);
  RenderCounterLine({CounterPoint{1, 5}}, "benchmark.http_2xx", -5, 10);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
