#include "source/client/tui/sut_stats.h"

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// The prefix every SUT-derived chart series carries (spec §4: `sut.*`).
constexpr absl::string_view kSutPrefix = "sut.";

// One exposition line's classification. The real SUT body (live-scraped,
// 2026-11-04, nh-sut-profiled:1.40.2-gperftools) is family blocks:
//   # TYPE <family> counter|gauge|histogram
//   <family>{labels} <value>
// with histogram families emitting suffixed `_bucket`/`_sum`/`_count`
// samples, and counter names re-sampled per label combination.
enum class LineKind { kComment, kSample };

LineKind classify(absl::string_view line) {
  // Trim leading spaces; exposition allows indentation.
  while (!line.empty() && absl::ascii_isspace(line.front())) {
    line.remove_prefix(1);
  }
  if (line.empty() || line.front() == '#') {
    return LineKind::kComment;
  }
  return LineKind::kSample;
}

// A sample line split into its metric name and numeric value: the name is
// everything before the first '{' (or before the value separator when the
// line carries no labels); the value is everything after the closing '}'
// (which cannot appear inside a value). Escaped quote tricks inside label
// values are irrelevant — labels are dropped wholesale.
bool splitSample(absl::string_view line, absl::string_view* name, absl::string_view* value) {
  const size_t brace = line.find('{');
  if (brace == absl::string_view::npos) {
    // Label-less form: "<name> <value>".
    const std::vector<absl::string_view> parts =
        absl::StrSplit(line, absl::ByAnyChar(" \t"), absl::SkipWhitespace());
    if (parts.size() != 2) {
      return false;
    }
    *name = parts[0];
    *value = parts[1];
    return true;
  }
  *name = line.substr(0, brace);
  const size_t close = line.find('}', brace);
  if (close == absl::string_view::npos) {
    return false;
  }
  *value = absl::StripAsciiWhitespace(line.substr(close + 1));
  return true;
}

// Parses one sample value. Prometheus exposition values are floats; the
// counters we keep are non-negative process totals, so anything negative,
// non-finite or non-numeric is a malformed line and dropped.
bool parseCounterValue(absl::string_view value_text, uint64_t* out) {
  double value = 0.0;
  if (!absl::SimpleAtod(value_text, &value) || !std::isfinite(value) || value < 0.0) {
    return false;
  }
  *out = static_cast<uint64_t>(value);
  return true;
}

} // namespace

std::map<std::string, uint64_t> ParsePrometheusCounters(absl::string_view prom_text,
                                                        int* histogram_line_drops) {
  std::map<std::string, uint64_t> counters;
  if (histogram_line_drops != nullptr) {
    *histogram_line_drops = 0;
  }

  // The family TYPE in force, and its declared kind. A sample line counts
  // only when its exact name matches the family and the kind is "counter" —
  // that is also what keeps a legit `..._count`-named counter (observed in
  // the live exposition) apart from a histogram's `_count` suffixed sample:
  // the latter's name differs from its histogram family, the former's TYPE
  // declares it a counter.
  absl::string_view type_family;
  absl::string_view type_kind;

  for (absl::string_view line :
       absl::StrSplit(prom_text, absl::ByChar('\n'), absl::AllowEmpty())) {
    if (!line.empty() && line.back() == '\r') {
      line.remove_suffix(1);
    }
    if (classify(line) == LineKind::kComment) {
      absl::string_view body = line;
      absl::ConsumePrefix(&body, "#");
      body = absl::StripLeadingAsciiWhitespace(body);
      if (absl::ConsumePrefix(&body, "TYPE")) {
        const std::vector<absl::string_view> parts =
            absl::StrSplit(absl::StripLeadingAsciiWhitespace(body), absl::ByAnyChar(" \t"),
                           absl::SkipWhitespace());
        if (parts.size() >= 2) {
          type_family = parts[0];
          type_kind = parts[1];
        }
      }
      // HELP and other comment lines leave the TYPE state untouched.
      continue;
    }

    absl::string_view name;
    absl::string_view value_text;
    if (!splitSample(line, &name, &value_text)) {
      continue;
    }
    if (name != type_family) {
      // A sample outside (or suffixed within) the current family: untyped,
      // or a histogram/summary member. Count the histogram drops for the
      // one-time throttled note.
      if (type_kind == "histogram" && histogram_line_drops != nullptr) {
        ++*histogram_line_drops;
      }
      continue;
    }
    if (type_kind != "counter") {
      continue;
    }
    uint64_t value = 0;
    if (!parseCounterValue(value_text, &value)) {
      continue;
    }
    // Per-label re-samples of one name sum into the process total.
    counters[std::string(name)] += value;
  }
  return counters;
}

absl::StatusOr<SutStatsDelta> DeltaScrapes(const SutScrape& prev, const SutScrape& curr) {
  if (curr.wall_time_unix_ns <= prev.wall_time_unix_ns) {
    return absl::InvalidArgumentError(
        "scrapes out of order: the current scrape's wall time must be strictly after the "
        "previous scrape's");
  }

  const std::map<std::string, uint64_t> prev_counters = ParsePrometheusCounters(prev.prom_text);
  const std::map<std::string, uint64_t> curr_counters = ParsePrometheusCounters(curr.prom_text);

  SutStatsDelta delta;
  const uint64_t stamp_ns = static_cast<uint64_t>(curr.wall_time_unix_ns);
  for (const auto& [name, curr_value] : curr_counters) {
    uint64_t point_value = curr_value; // First appearance = full value.
    const auto it = prev_counters.find(name);
    if (it != prev_counters.end()) {
      if (curr_value >= it->second) {
        point_value = curr_value - it->second;
      } else {
        // The counter went backwards: the SUT restarted. Report the
        // post-reset value — never a negative delta — under the reset marker.
        point_value = curr_value;
        delta.counter_reset_detected = true;
      }
    }
    delta.series[absl::StrCat(kSutPrefix, name)] = {CounterPoint{stamp_ns, point_value}};
  }
  return delta;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
