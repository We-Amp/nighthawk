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

// Percentile points stamped at the real flush cadence (one point per second),
// so the time-anchored viewport math is exercised exactly as a live run
// drives it.
std::vector<PercentilePoint> percentilePoints(size_t count) {
  std::vector<PercentilePoint> points;
  for (size_t i = 0; i < count; ++i) {
    points.push_back(PercentilePoint{/*monotonic_ns=*/i * 1000000000ull,
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
  // 100 columns: enough for legend + window stamp + readout pair together —
  // the layout the charts pane actually gets at the declared minimum.
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 100, 10), 100, 10);

  ASSERT_GE(lines.size(), 2u);
  // The header row IS the legend: glyph -> series, units in the label, and —
  // per item 9 — the window stamp on the legend tail (the viewport's time
  // reference): span, seconds-per-column, and the oldest/newest edges.
  EXPECT_NE(lines[0].find("# p50"), std::string::npos);
  EXPECT_NE(lines[0].find("+ p90"), std::string::npos);
  EXPECT_NE(lines[0].find(". p99 ms"), std::string::npos);
  EXPECT_NE(lines[0].find("last 9s (1s/col)"), std::string::npos);
  EXPECT_NE(lines[0].find("t+0s..t+9s"), std::string::npos);

  // The last data column (column 9, one column per one-second point in a
  // 100-column chart) carries band glyphs in the rows under the legend.
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
    points.push_back(PercentilePoint{static_cast<uint64_t>(i) * 1000000000ull, /*p50_ns=*/30000000,
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

// Fixed-width scrolling viewport (backlog item 9): more points than columns,
// and the oldest samples fall off the LEFT edge while the newest stays at the
// right edge. With 120 one-second points in a 100-column chart the visible
// window is the newest 100 — the stamp says so, and the edges move.
TEST(ChartsTest, ViewportScrollsOldestSamplesOffTheLeft) {
  std::vector<PercentilePoint> points = percentilePoints(120);
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 100, 10), 100, 10);
  ASSERT_GE(lines.size(), 2u);
  // Visible span: 99 one-second columns (newest minus oldest visible).
  EXPECT_NE(lines[0].find("last 99s (1s/col)"), std::string::npos);
  // Oldest visible point is index 20 of 120: t+20s. Newest is t+119s.
  EXPECT_NE(lines[0].find("t+20s..t+119s"), std::string::npos);
  // Data reaches the right edge (the newest point).
  bool glyph_in_last_column = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].size() > 99 && lines[row][99] != ' ') {
      glyph_in_last_column = true;
    }
  }
  EXPECT_TRUE(glyph_in_last_column);
  // Nothing draws past the chart width.
  for (size_t row = 1; row < lines.size(); ++row) {
    ASSERT_LE(lines[row].size(), 100u) << "row " << row << ": [" << lines[row] << "]";
  }

  // A wider terminal shows a wider window from the same series: 110 columns
  // reach back to t+10s.
  std::vector<std::string> wide_lines = screenLines(RenderPercentileBand(points, 110, 10), 110, 10);
  EXPECT_NE(wide_lines[0].find("last 109s (1s/col)"), std::string::npos);
  EXPECT_NE(wide_lines[0].find("t+10s..t+119s"), std::string::npos);
}

// Time-anchored columns (backlog item 9's minor): a flush the reducer skipped
// leaves a BLANK column instead of shifting later samples left — the viewport
// is the time reference.
TEST(ChartsTest, SkippedFlushLeavesABlankColumn) {
  // Five one-second points, then a gap (t+5s never flushed), then t+6s.
  std::vector<PercentilePoint> points;
  const uint64_t stamps[] = {0, 1, 2, 3, 4, 6};
  for (uint64_t s : stamps) {
    points.push_back(PercentilePoint{s * 1000000000ull, /*p50_ns=*/10000000, /*p90_ns=*/20000000,
                                     /*p99_ns=*/30000000});
  }
  const int width = 20;
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, width, 10), width, 10);
  // All bands equal: the p99 '.' owns every data cell, one shared row (five
  // consecutive dots — the gap breaks the run).
  int data_row = -1;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find(".....") != std::string::npos) {
      data_row = static_cast<int>(row);
    }
  }
  ASSERT_NE(data_row, -1);
  // Columns: t+0s .. t+6s land at width-7 .. width-1; t+5s (column width-2)
  // must be BLANK — the gap is visible, nothing shifted left.
  const std::string& row = lines[static_cast<size_t>(data_row)];
  EXPECT_EQ(row.substr(width - 7, 5), ".....");
  EXPECT_EQ(row[width - 2], ' ') << "[" << row << "]";
  EXPECT_EQ(row[width - 1], '.');
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

  // Graceful degradation on a narrow pane: the window stamp yields first; the
  // legend and the readout pair survive together down to legend+readout
  // width (24 + 22 = 47 columns).
  std::string narrow;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 50, 10), 50, 10)) {
    narrow += line + "\n";
  }
  EXPECT_NE(narrow.find("now 39.0ms  max 39.0ms"), std::string::npos);
  EXPECT_EQ(narrow.find("last 9s"), std::string::npos);

  BandAnnotations rescaled;
  rescaled.rescaled = true;
  std::string marked;
  for (const std::string& line : screenLines(RenderPercentileBand(points, 60, 10, rescaled), 60,
                                             10)) {
    marked += line + "\n";
  }
  EXPECT_NE(marked.find("(rescaled)"), std::string::npos);
}

// Spike salience: the excursion cell carries "!", the header carries the
// numeric tail with the ratio, and — batch-2 polish — the "!" glyph joins the
// legend while a spike window is open.
TEST(ChartsTest, SpikeAnnotationRendersGlyphLegendAndNumericTail) {
  std::vector<PercentilePoint> points = percentilePoints(10);
  points.back().p99_ns = 120000000; // a 4x excursion over the ~30ms floor
  BandAnnotations spike;
  spike.spike = true;
  spike.spike_ratio = 4.0;
  std::vector<std::string> lines =
      screenLines(RenderPercentileBand(points, 60, 10, spike), 60, 10);

  EXPECT_NE(lines[0].find("p99 120.0ms ^4.0x"), std::string::npos);
  // "!" joined the legend: glyph -> meaning decodes without off-screen
  // knowledge even before the excursion appears.
  EXPECT_NE(lines[0].find("! excursion"), std::string::npos);
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
  EXPECT_EQ(plain.find("excursion"), std::string::npos);
}

// One labeled gridline marks the latency scale (a nice ms step under half the
// window max: max 39ms -> 10ms). Data glyphs keep their cells.
TEST(ChartsTest, BandRendersLabeledGridline) {
  std::vector<PercentilePoint> points;
  for (int i = 0; i < 10; ++i) {
    points.push_back(PercentilePoint{static_cast<uint64_t>(i) * 1000000000ull, /*p50_ns=*/15000000,
                                     /*p90_ns=*/22000000,
                                     /*p99_ns=*/30000000 + static_cast<uint64_t>(i) * 1000000});
  }
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 10), 60, 10);
  bool gridline = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find("10ms") != std::string::npos &&
        lines[row].find("--") != std::string::npos) {
      gridline = true;
    }
  }
  EXPECT_TRUE(gridline);
}

// The gridline label stamps at the FIRST fully-free span of its row (batch-2
// polish), not only at column 0: with a full window (column 0 occupied by
// data) and a 5-flush outage in the middle, the free span is the outage —
// the label lands there instead of being dropped or forced to column 0.
TEST(ChartsTest, GridlineLabelStampsAtFirstFreeSpan) {
  // 60 one-second points spanning t+0..t+64 with t+26..t+30 never flushed:
  // exactly fills the 60-column window (oldest visible at column 0), with a
  // 5-column outage at columns 21..25.
  std::vector<PercentilePoint> points;
  const auto addPoint = [&points](uint64_t t) {
    // p50 exactly on the 10ms gridline value (so the gridline row is data
    // everywhere except the outage); p99 well above it.
    points.push_back(PercentilePoint{t * 1000000000ull, /*p50_ns=*/10000000,
                                     /*p90_ns=*/15000000,
                                     /*p99_ns=*/39000000});
  };
  for (uint64_t t = 0; t <= 25; ++t) {
    addPoint(t);
  }
  for (uint64_t t = 31; t <= 64; ++t) {
    addPoint(t);
  }
  ASSERT_EQ(points.size(), 60u);
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 10), 60, 10);
  int labeled_row = -1;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find("10ms") != std::string::npos) {
      labeled_row = static_cast<int>(row);
    }
  }
  ASSERT_NE(labeled_row, -1);
  const std::string& row = lines[static_cast<size_t>(labeled_row)];
  // Column 0 is data (the window is full), so the label cannot stamp there:
  // it lands at the outage's first free column (21).
  EXPECT_EQ(row[0], '#');
  const size_t label_at = row.find("10ms");
  EXPECT_EQ(label_at, 21u) << "[" << row << "]";
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
    points.push_back(CounterPoint{i * 1000000000ull, (i + 1) * 100});
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

// Pool-pending capacity context (backlog item 12): with the run's configured
// gate, the header reads the depth as a fraction of the cap and the grid
// draws a ceiling rule at cap height — a ceiling the band can visibly hit.
TEST(ChartsTest, CounterLineWithCapRendersFractionAndCeilingRule) {
  std::vector<CounterPoint> points;
  for (uint64_t i = 0; i < 10; ++i) {
    points.push_back(CounterPoint{i * 1000000000ull, 300 + i * 50}); // 300..750
  }
  std::vector<std::string> lines =
      screenLines(RenderCounterLine(points, "pool_rq_pending_release", 60, 10,
                                    /*capacity_cap=*/1000),
                  60, 10);

  ASSERT_GE(lines.size(), 2u);
  // The fraction readout replaces the now/max pair: newest 750 of 1000 (75%).
  EXPECT_NE(lines[0].find("cur 750/1000 (75%)"), std::string::npos);
  // The ceiling rule: a row of "|" at the cap's height (the top row here,
  // cap above every value), on blank cells only.
  bool ceiling_rule = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find("||") != std::string::npos) {
      ceiling_rule = true;
    }
  }
  EXPECT_TRUE(ceiling_rule);
  // The data glyphs are intact under the ceiling.
  bool data_glyph = false;
  for (size_t row = 1; row < lines.size(); ++row) {
    if (lines[row].find('*') != std::string::npos) {
      data_glyph = true;
    }
  }
  EXPECT_TRUE(data_glyph);

  // Without a cap the same series renders the plain readout pair.
  std::vector<std::string> uncapped =
      screenLines(RenderCounterLine(points, "pool_rq_pending_release", 60, 10), 60, 10);
  EXPECT_NE(uncapped[0].find("now 750  max 750"), std::string::npos);
  EXPECT_EQ(uncapped[0].find("cur "), std::string::npos);
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

// The compact health line (backlog item 10): per-flush rates of the failure
// counters, zeros included — an absent series (the engine suppresses zero
// counters) reads as a steady 0/s.
TEST(ChartsTest, HealthLineRendersRatesIncludingZeros) {
  // Cumulative 4xx: 100 -> 102 -> 102: latest rate 0/s, peak 2/s. Resets and
  // pool connect failures absent entirely.
  std::vector<CounterPoint> http_4xx = {CounterPoint{0, 100}, CounterPoint{1000000000ull, 102},
                                        CounterPoint{2000000000ull, 102}};
  std::vector<std::string> lines =
      screenLines(RenderHealthLine(http_4xx, {}, {}, 60, 10), 60, 10);
  ASSERT_GE(lines.size(), 1u);
  EXPECT_NE(lines[0].find("4xx 0/s | rst 0/s | t/o 0/s"), std::string::npos);
  EXPECT_NE(lines[0].find("now 0/s  max 2/s"), std::string::npos);

  // All series empty (no failures ever): the line still renders — zeros are
  // information.
  std::vector<std::string> zeros = screenLines(RenderHealthLine({}, {}, {}, 60, 10), 60, 10);
  EXPECT_NE(zeros[0].find("4xx 0/s | rst 0/s | t/o 0/s"), std::string::npos);
  EXPECT_NE(zeros[0].find("now 0/s  max 0/s"), std::string::npos);

  // A live reset burst reads in the same line.
  std::vector<CounterPoint> resets = {CounterPoint{0, 0}, CounterPoint{1000000000ull, 42}};
  std::vector<std::string> burst =
      screenLines(RenderHealthLine({}, resets, {}, 60, 10), 60, 10);
  EXPECT_NE(burst[0].find("rst 42/s"), std::string::npos);
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
  RenderHealthLine({}, {}, {}, 0, 10);
  RenderHealthLine({}, {}, {}, -5, 10);
}

TEST(ChartsTest, CounterLineSingleRowGridStaysInBounds) {
  // Regression (ASan heap-buffer-overflow): a 1-row grid (pane height 2) made
  // rowFor return row 1 for every point below the max — grid[1] past the end
  // of a 1-element vector<string>. This is the live-run crash signature: many
  // counter series give each counter line a 1-row sparkline, and the OOB
  // writes trash unrelated heap (later victims: proto string headers,
  // malloc freelist, ftxui std::functions).
  std::vector<CounterPoint> points;
  for (uint64_t i = 0; i < 40; ++i) {
    points.push_back(CounterPoint{i * 1000000000ull, 1000 - i}); // values below the max
  }
  // height = 2 → sparkGrid rows = max(1, height - 1) = 1. Under ASan the
  // unfixed renderer aborts here.
  std::vector<std::string> lines = screenLines(RenderCounterLine(points, "benchmark.http_2xx", 60, 2), 60, 2);
  ASSERT_GE(lines.size(), 1u);
  // Every glyph lands inside the single chart row: nothing is drawn off-grid.
  for (const std::string& line : lines) {
    ASSERT_LE(line.size(), 61u);
  }
}

TEST(ChartsTest, PercentileBandSingleRowGridStaysInBounds) {
  // Same regression for the band: height 2 → band_rows = 1; every p50/p90/p99
  // below the window max must stay inside the single row.
  std::vector<PercentilePoint> points;
  for (uint64_t i = 0; i < 40; ++i) {
    points.push_back(PercentilePoint{i * 1000000000ull, /*p50_ns=*/1000, /*p90_ns=*/2000, /*p99_ns=*/3000});
  }
  std::vector<std::string> lines = screenLines(RenderPercentileBand(points, 60, 2), 60, 2);
  ASSERT_GE(lines.size(), 1u);
  for (const std::string& line : lines) {
    ASSERT_LE(line.size(), 61u);
  }
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
