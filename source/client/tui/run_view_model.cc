#include "source/client/tui/run_view_model.h"

#include <utility>

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Appends point, dropping the oldest entry once the series is at capacity.
template <typename PointT>
void appendCapped(std::vector<PointT>& series, PointT point) {
  if (series.size() >= kRunViewModelMaxPoints) {
    series.erase(series.begin());
  }
  series.push_back(std::move(point));
}

} // namespace

RunViewModel ApplySnapshot(RunViewModel model, const nighthawk::client::LiveSnapshot& snapshot) {
  const uint64_t monotonic_ns = static_cast<uint64_t>(snapshot.monotonic_ns());
  // live.proto's consumer contract: count == 0 means no samples yet, so the
  // percentile fields are meaningless — skip the band append and leave a gap
  // for the warm-up snapshot rather than painting zeros at the chart's bottom.
  if (!snapshot.statistics().empty() && snapshot.statistics()[0].count() > 0) {
    const nighthawk::client::StatisticSnapshot& stat = snapshot.statistics()[0];
    appendCapped(model.latency_band, PercentilePoint{monotonic_ns, stat.p50_ns(), stat.p90_ns(),
                                                     stat.p99_ns()});
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
