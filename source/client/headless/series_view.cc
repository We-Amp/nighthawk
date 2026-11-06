#include "source/client/headless/series_view.h"

#include <algorithm>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

std::vector<CounterPoint>
MergeCompletedSeries(const absl::flat_hash_map<std::string, std::vector<CounterPoint>>& counters) {
  static const char* const kCompletedNames[] = {
      "benchmark.http_1xx", "benchmark.http_2xx",      "benchmark.http_3xx",
      "benchmark.http_4xx", "benchmark.http_5xx",      "benchmark.http_xxx",
      "benchmark.stream_resets", "benchmark.pool_connection_failure",
  };
  // The union of stamps, ascending and deduplicated.
  std::vector<uint64_t> stamps;
  for (const char* name : kCompletedNames) {
    auto it = counters.find(name);
    if (it == counters.end()) {
      continue;
    }
    for (const CounterPoint& point : it->second) {
      stamps.push_back(point.monotonic_ns);
    }
  }
  std::sort(stamps.begin(), stamps.end());
  stamps.erase(std::unique(stamps.begin(), stamps.end()), stamps.end());

  // C(t) per source series (last value at or before t), summed at every
  // union stamp — the same convention CompletedWorkInMonotonicRange applies
  // to the merged result.
  std::vector<CounterPoint> merged;
  merged.reserve(stamps.size());
  for (const uint64_t stamp : stamps) {
    uint64_t value = 0;
    for (const char* name : kCompletedNames) {
      auto it = counters.find(name);
      if (it == counters.end()) {
        continue;
      }
      uint64_t at_or_before = 0;
      for (const CounterPoint& point : it->second) {
        if (point.monotonic_ns <= stamp) {
          at_or_before = point.value;
        }
      }
      value += at_or_before;
    }
    merged.push_back(CounterPoint{stamp, value});
  }
  return merged;
}

SeriesView::SeriesView(std::vector<nighthawk::client::MetricSeriesRecord> records)
    : records_(std::move(records)) {
  if (!records_.empty()) {
    // The first-snapshot anchor (the M1 pattern): the (wall, monotonic) pair
    // of the run's first record joins every later monotonic stamp onto the
    // wall clock.
    anchor_.wall_time_unix_ns = records_.front().wall_time_unix_ns();
    anchor_.monotonic_ns = records_.front().monotonic_ns();
  }
}

std::vector<std::string> SeriesView::SeriesNames() const {
  std::vector<std::string> names;
  for (const nighthawk::client::MetricSeriesRecord& record : records_) {
    for (const auto& [name, value] : record.counters()) {
      (void)value;
      if (std::find(names.begin(), names.end(), name) == names.end()) {
        names.push_back(name);
      }
    }
  }
  std::sort(names.begin(), names.end());
  return names;
}

absl::StatusOr<std::vector<StampedPoint>> SeriesView::Series(absl::string_view name) const {
  std::vector<StampedPoint> points;
  for (const nighthawk::client::MetricSeriesRecord& record : records_) {
    auto it = record.counters().find(name);
    if (it == record.counters().end()) {
      continue;
    }
    StampedPoint point;
    point.monotonic_ns = record.monotonic_ns();
    point.value = it->second;
    // The wall join through the first-snapshot anchor — the record's own
    // wall field is NOT used here (only the anchor's is): one join key for
    // the whole view, so a clock jump inside the run cannot split it.
    point.wall_time_unix_ns = point.monotonic_ns + (anchor_.wall_time_unix_ns - anchor_.monotonic_ns);
    point.phase_id = record.phase_id();
    point.epoch_id = record.epoch_id();
    points.push_back(std::move(point));
  }
  if (points.empty()) {
    return absl::NotFoundError(
        absl::StrCat("no series named \"", name, "\" in this run's snapshots"));
  }
  return points;
}

const ClockAnchor& SeriesView::anchor() const { return anchor_; }

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
