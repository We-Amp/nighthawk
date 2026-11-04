#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"

#include "source/client/tui/charts.h"
#include "source/client/tui/sut_stats.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): the SUT stats scrape parser + delta layer. The parse
// fixtures are shaped after the LIVE SUT exposition (nh-sut-profiled:
// 1.40.2-gperftools, scraped 2026-11-04): `# TYPE <name> counter|gauge|
// histogram` family blocks, counter samples carrying empty (`name{} 0`) or
// labeled braces, and per-label duplicate samples of one counter name.

// Screen text helper, same convention as tui_charts_test.
std::vector<std::string> screenLines(const ftxui::Element& element, int width, int height) {
  ftxui::Screen screen(width, height);
  ftxui::Render(screen, element);
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

// A fixture shaped after the real exposition: a counter family (with a
// labeled, empty-braced and duplicated sample), a gauge family, a histogram
// family (bucket/sum/count samples), an untyped sample ahead of any TYPE,
// a HELP line, and a blank line.
constexpr absl::string_view kMixedFixture =
    "untyped_before_any_type 9\n"
    "# HELP envoy_filesystem_flushed_by_timer smithy_prom\n"
    "# TYPE envoy_filesystem_flushed_by_timer counter\n"
    "envoy_filesystem_flushed_by_timer{} 1\n"
    "\n"
    "# TYPE envoy_http_downstream_rq_xx counter\n"
    "envoy_http_downstream_rq_xx{envoy_response_code_class=\"4\","
    "envoy_http_conn_manager_prefix=\"admin\"} 3\n"
    "envoy_http_downstream_rq_xx{envoy_response_code_class=\"5\","
    "envoy_http_conn_manager_prefix=\"admin\"} 4\n"
    "# TYPE envoy_server_memory_allocated gauge\n"
    "envoy_server_memory_allocated{} 123456\n"
    "# TYPE envoy_http_downstream_cx_length_ms histogram\n"
    "envoy_http_downstream_cx_length_ms_bucket{le=\"0.5\"} 0\n"
    "envoy_http_downstream_cx_length_ms_bucket{le=\"1\"} 0\n"
    "envoy_http_downstream_cx_length_ms_sum{} 0\n"
    "envoy_http_downstream_cx_length_ms_count{} 1\n"
    "# TYPE envoy_cluster_manager_cluster_added counter\n"
    "envoy_cluster_manager_cluster_added{} 2\n";

TEST(SutStatsTest, ParsePrometheusCountersParsesCountersOnly) {
  const std::map<std::string, uint64_t> counters = ParsePrometheusCounters(kMixedFixture);
  // The counters are present with labels stripped.
  EXPECT_EQ(counters.count("envoy_filesystem_flushed_by_timer"), 1u);
  EXPECT_EQ(counters.at("envoy_filesystem_flushed_by_timer"), 1u);
  EXPECT_EQ(counters.count("envoy_cluster_manager_cluster_added"), 1u);
  EXPECT_EQ(counters.at("envoy_cluster_manager_cluster_added"), 2u);
  // Per-label duplicate samples of one counter name sum into process totals
  // (the charts pane's deltas are per-name totals).
  EXPECT_EQ(counters.at("envoy_http_downstream_rq_xx"), 7u);
  // The gauge, the whole histogram family (bucket/sum/count), and the untyped
  // line are excluded — deltas of those are meaningless.
  EXPECT_EQ(counters.count("envoy_server_memory_allocated"), 0u);
  EXPECT_EQ(counters.count("envoy_http_downstream_cx_length_ms_bucket"), 0u);
  EXPECT_EQ(counters.count("envoy_http_downstream_cx_length_ms_sum"), 0u);
  EXPECT_EQ(counters.count("envoy_http_downstream_cx_length_ms_count"), 0u);
  EXPECT_EQ(counters.count("untyped_before_any_type"), 0u);
}

TEST(SutStatsTest, ParsePrometheusCountersCountsHistogramDrops) {
  int histogram_drops = 0;
  ParsePrometheusCounters(kMixedFixture, &histogram_drops);
  // Four histogram-family sample lines in the fixture (two buckets + sum +
  // count); the gauge line is not a histogram drop.
  EXPECT_EQ(histogram_drops, 4);
}

TEST(SutStatsTest, ParsePrometheusCountersToleratesCrlfAndMalformedValues) {
  // CRLF endings survive; a non-numeric or negative value line is dropped
  // rather than poisoning the map.
  const std::map<std::string, uint64_t> counters = ParsePrometheusCounters(
      "# TYPE a counter\r\na{} 5\r\nb{} not_a_number\nc{} -3\n# TYPE d counter\nd{} 7\n");
  EXPECT_EQ(counters.size(), 2u);
  EXPECT_EQ(counters.at("a"), 5u);
  EXPECT_EQ(counters.at("d"), 7u);
}

TEST(SutStatsTest, ParsePrometheusCountersEmptyTextYieldsEmptyMap) {
  EXPECT_TRUE(ParsePrometheusCounters("").empty());
}

SutScrape scrapeAt(int64_t unix_ns, std::string prom_text) {
  return SutScrape{/*wall_time_unix_ns=*/unix_ns, /*prom_text=*/std::move(prom_text)};
}

TEST(SutStatsTest, DeltaScrapesComputesDeltasAndDetectsReset) {
  const SutScrape prev = scrapeAt(
      1730000000000000000LL,
      "# TYPE rq counter\nrq{} 10\n# TYPE cx counter\ncx{} 5\n# TYPE gone counter\ngone{} 9\n");
  const SutScrape curr = scrapeAt(
      1730000001000000000LL,
      "# TYPE rq counter\nrq{} 13\n# TYPE cx counter\ncx{} 3\n# TYPE fresh counter\nfresh{} 4\n");

  const absl::StatusOr<SutStatsDelta> delta = DeltaScrapes(prev, curr);
  ASSERT_TRUE(delta.ok()) << delta.status();

  // Rising counter → the difference, sut.-prefixed, stamped at the CURRENT
  // scrape's engine-side wall time.
  ASSERT_EQ(delta->series.count("sut.rq"), 1u);
  ASSERT_EQ(delta->series.at("sut.rq").size(), 1u);
  EXPECT_EQ(delta->series.at("sut.rq")[0].value, 3u);
  EXPECT_EQ(delta->series.at("sut.rq")[0].monotonic_ns, 1730000001000000000ULL);

  // A decreased value is a counter reset (SUT restart): the marker sets and
  // that series reports the post-reset value — never a negative delta.
  EXPECT_TRUE(delta->counter_reset_detected);
  ASSERT_EQ(delta->series.count("sut.cx"), 1u);
  EXPECT_EQ(delta->series.at("sut.cx")[0].value, 3u);

  // A counter that disappeared since the previous scrape is dropped; a
  // first-appearance counter is its own delta (the counter started at zero
  // within this interval).
  EXPECT_EQ(delta->series.count("sut.gone"), 0u);
  ASSERT_EQ(delta->series.count("sut.fresh"), 1u);
  EXPECT_EQ(delta->series.at("sut.fresh")[0].value, 4u);
}

TEST(SutStatsTest, DeltaScrapesNoResetOnSteadyCounters) {
  const SutScrape prev = scrapeAt(1730000000000000000LL, "# TYPE rq counter\nrq{} 10\n");
  const SutScrape curr = scrapeAt(1730000001000000000LL, "# TYPE rq counter\nrq{} 10\n");
  const absl::StatusOr<SutStatsDelta> delta = DeltaScrapes(prev, curr);
  ASSERT_TRUE(delta.ok()) << delta.status();
  EXPECT_FALSE(delta->counter_reset_detected);
  EXPECT_EQ(delta->series.at("sut.rq")[0].value, 0u);
}

TEST(SutStatsTest, DeltaScrapesRejectsNonIncreasingTimestamps) {
  const SutScrape prev = scrapeAt(1730000001000000000LL, "# TYPE rq counter\nrq{} 10\n");
  const SutScrape same = scrapeAt(1730000001000000000LL, "# TYPE rq counter\nrq{} 11\n");
  const SutScrape earlier = scrapeAt(1730000000000000000LL, "# TYPE rq counter\nrq{} 12\n");
  EXPECT_TRUE(absl::IsInvalidArgument(DeltaScrapes(prev, same).status()));
  EXPECT_TRUE(absl::IsInvalidArgument(DeltaScrapes(prev, earlier).status()));
}

// The reuse guard: delta points render through M0b's RenderCounterLine with
// no new chart code — the sut. label shows and the rendered maximum is the
// last point.
TEST(SutStatsTest, DeltaSeriesRenderViaCounterLine) {
  const std::vector<CounterPoint> points = {
      CounterPoint{/*monotonic_ns=*/1730000000000000000ULL, /*value=*/3},
      CounterPoint{/*monotonic_ns=*/1730000001000000000ULL, /*value=*/7},
  };
  const ftxui::Element line = RenderCounterLine(points, "sut.rq", /*width=*/60, /*height=*/10);
  const std::vector<std::string> rows = screenLines(line, 60, 10);
  bool found_label = false;
  bool found_now_max = false;
  for (const std::string& row : rows) {
    if (row.find("sut.rq") != std::string::npos) {
      found_label = true;
    }
    if (row.find("now 7") != std::string::npos && row.find("max 7") != std::string::npos) {
      found_now_max = true;
    }
  }
  EXPECT_TRUE(found_label);
  EXPECT_TRUE(found_now_max);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
