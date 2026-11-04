#include "source/client/live_partial_aggregator.h"

#include <algorithm>

namespace Nighthawk {
namespace Client {

void LivePartialAggregator::addPartial(nighthawk::client::WorkerLiveSnapshotPartial partial) {
  auto& worker_state = worker_states_[partial.worker_id()];
  if (worker_state.phase_id != partial.phase_id()) {
    // The worker advanced to a new slab: its old slab's statistics are done,
    // replace them wholesale so no pre-boundary statistic ever survives into
    // the post-boundary fold. Workers never go back to an earlier phase, so
    // this is forward progress; latest advances with it (a straggler partial
    // of an unchanged stamp cannot drag the snapshot back).
    worker_state.phase_id = partial.phase_id();
    worker_state.statistics.clear();
    latest_phase_id_ = partial.phase_id();
  }
  auto& counter_values = worker_counter_values_[partial.worker_id()];
  for (const auto& [name, value] : partial.counters()) {
    counter_values[name] = value;
  }
  for (const nighthawk::client::StatisticSnapshot& stat : partial.statistics()) {
    worker_state.statistics[stat.id()] = stat;
  }
}

nighthawk::client::LiveSnapshot LivePartialAggregator::snapshot(const std::string& epoch_id) {
  nighthawk::client::LiveSnapshot snapshot;
  // Counters are cumulative from run start: sum the per-worker latests over
  // all workers, regardless of their stamps.
  for (const auto& [worker_id, counter_values] : worker_counter_values_) {
    for (const auto& [name, value] : counter_values) {
      (*snapshot.mutable_counters())[name] += value;
    }
  }
  // Statistics fold only from workers whose latest partial is of the
  // snapshot's stamp: counts are summed over the workers (they are
  // cumulative within the slab), percentiles take the per-worker maximum, as
  // raw histograms cannot be merged without their samples. This makes
  // percentile values an approximation of the phase picture, which the
  // counts alongside them let consumers interpret.
  absl::flat_hash_map<std::string, nighthawk::client::StatisticSnapshot*> statistics;
  for (const auto& [worker_id, worker_state] : worker_states_) {
    if (worker_state.phase_id != latest_phase_id_) {
      continue;
    }
    for (const auto& [id, stat] : worker_state.statistics) {
      auto inserted = statistics.emplace(id, nullptr);
      if (inserted.second) {
        nighthawk::client::StatisticSnapshot* merged = snapshot.add_statistics();
        *merged = stat;
        inserted.first->second = merged;
      } else {
        nighthawk::client::StatisticSnapshot* merged = inserted.first->second;
        merged->set_count(merged->count() + stat.count());
        merged->set_p50_ns(std::max(merged->p50_ns(), stat.p50_ns()));
        merged->set_p90_ns(std::max(merged->p90_ns(), stat.p90_ns()));
        merged->set_p99_ns(std::max(merged->p99_ns(), stat.p99_ns()));
      }
    }
  }
  const auto now_ns = [](auto clock_now) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clock_now.time_since_epoch())
        .count();
  };
  snapshot.set_wall_time_unix_ns(now_ns(std::chrono::system_clock::now()));
  // Snapshots are produced by a single collector thread, but enforce strictly
  // increasing values anyway: the assertion is a contract consumers may rely on.
  int64_t monotonic_ns = now_ns(std::chrono::steady_clock::now());
  if (monotonic_ns <= last_live_snapshot_monotonic_ns_) {
    monotonic_ns = last_live_snapshot_monotonic_ns_ + 1;
  }
  last_live_snapshot_monotonic_ns_ = monotonic_ns;
  snapshot.set_monotonic_ns(monotonic_ns);
  snapshot.set_phase_id(latest_phase_id_);
  snapshot.set_epoch_id(epoch_id);
  return snapshot;
}

} // namespace Client
} // namespace Nighthawk
