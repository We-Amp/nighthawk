#include "source/client/tui/run_view_model.h"

#include <utility>

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Appends point, dropping the oldest entry once the series is at capacity.
template <typename PointT> void appendCapped(std::vector<PointT>& series, PointT point) {
  if (series.size() >= kRunViewModelMaxPoints) {
    series.erase(series.begin());
  }
  series.push_back(std::move(point));
}

} // namespace

RunViewModel ApplySnapshot(RunViewModel model, const nighthawk::client::LiveSnapshot& snapshot) {
  const uint64_t monotonic_ns = static_cast<uint64_t>(snapshot.monotonic_ns());
  // live.proto's consumer contract: count == 0 means no samples yet, so the
  // percentile fields are meaningless — such statistics are skipped entirely
  // and leave a band gap rather than painting zeros at the chart's bottom.
  // The band's source is the FIRST statistic with samples, not statistics[0]:
  // the live aggregator's statistics order is unspecified (the per-worker
  // fold iterates a flat_hash_map), so under real traffic statistics[0] is
  // often a zero-count response-class statistic (e.g. latency_1xx) while the
  // sampled latency statistic (e.g. latency_2xx) sits elsewhere in the list.
  const nighthawk::client::StatisticSnapshot* sampled = nullptr;
  for (const nighthawk::client::StatisticSnapshot& stat : snapshot.statistics()) {
    if (stat.count() > 0) {
      sampled = &stat;
      break;
    }
  }
  if (sampled != nullptr) {
    appendCapped(model.latency_band, PercentilePoint{monotonic_ns, sampled->p50_ns(),
                                                     sampled->p90_ns(), sampled->p99_ns()});
  }
  for (const auto& [name, value] : snapshot.counters()) {
    appendCapped(model.counters[name], CounterPoint{monotonic_ns, value});
  }
  model.last_snapshot = snapshot;
  return model;
}

void SnapshotStore::store(RunViewModel model) {
  absl::MutexLock lock(&mu_);
  current_ = std::make_shared<const RunViewModel>(std::move(model));
}

std::shared_ptr<const RunViewModel> SnapshotStore::load() const {
  absl::ReaderMutexLock lock(&mu_);
  return current_;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
