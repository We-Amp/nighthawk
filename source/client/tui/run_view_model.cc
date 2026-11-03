#include "source/client/tui/run_view_model.h"

#include <algorithm>
#include <cstdint>
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
    // One-frame chart markers, computed against the pre-append band: a p99
    // above everything already stored rescales any window that contains it
    // (the "(rescaled)" marker's trigger), and a p99 far above its recent
    // floor opens the spike annotation window (kSpikeFrames flushes).
    model.latency_rescaled = false;
    if (!model.latency_band.empty()) {
      uint64_t prev_max = 0;
      uint64_t floor_min = UINT64_MAX;
      const size_t start = model.latency_band.size() > kSpikeFloorWindow
                               ? model.latency_band.size() - kSpikeFloorWindow
                               : 0;
      for (size_t i = start; i < model.latency_band.size(); ++i) {
        prev_max = std::max(prev_max, model.latency_band[i].p99_ns);
        floor_min = std::min(floor_min, model.latency_band[i].p99_ns);
      }
      if (sampled->p99_ns() > prev_max) {
        model.latency_rescaled = true;
      }
      if (model.latency_band.size() >= 3 && floor_min > 0) {
        const double ratio = static_cast<double>(sampled->p99_ns()) /
                             static_cast<double>(floor_min);
        if (ratio >= kSpikeRatio) {
          model.spike_frames_remaining = kSpikeFrames;
          model.spike_ratio = ratio;
        } else if (model.spike_frames_remaining > 0) {
          --model.spike_frames_remaining;
        }
      }
    }
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
