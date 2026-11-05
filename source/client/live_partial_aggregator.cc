#include "source/client/live_partial_aggregator.h"

#include <algorithm>

#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/strip.h"

namespace Nighthawk {
namespace Client {

void LivePartialAggregator::setPhaseOrder(std::vector<std::string> phase_order) {
  phase_order_ = std::move(phase_order);
}

int LivePartialAggregator::stampRank(const std::string& phase_id) const {
  for (size_t i = 0; i < phase_order_.size(); ++i) {
    if (phase_order_[i] == phase_id) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool LivePartialAggregator::advancesLatest(const std::string& phase_id) const {
  if (phase_order_.empty()) {
    // Legacy behavior without an order (single-phase runs): any new stamp
    // advances.
    return true;
  }
  const int rank = stampRank(phase_id);
  const int latest_rank = stampRank(latest_phase_id_);
  // Only a strictly later phase advances the label; an unknown stamp never
  // does (it cannot be ordered, so it must not be trusted with the label).
  return rank >= 0 && rank > latest_rank;
}

void LivePartialAggregator::addPartial(nighthawk::client::WorkerLiveSnapshotPartial partial) {
  auto& worker_state = worker_states_[partial.worker_id()];
  if (worker_state.epoch_id != partial.epoch_id()) {
    worker_state.epoch_id = partial.epoch_id();
    advanceEpochLabel(partial.epoch_id());
  }
  if (worker_state.phase_id != partial.phase_id()) {
    // The worker advanced to a new slab: its old slab's statistics are done,
    // replace them wholesale so no pre-boundary statistic ever survives into
    // the post-boundary fold. The label advances only MONOTONICALLY along
    // the phase order: a lagging partial -- notably a removed worker's
    // first-ever fold partial (M2 Task 6), carrying its old stamp -- must
    // not drag latest backward (it would freeze the percentiles for the
    // rest of the run; the worker stays excluded from the statistics fold
    // via its own stamp either way, and counters are unaffected).
    worker_state.phase_id = partial.phase_id();
    worker_state.statistics.clear();
    if (advancesLatest(partial.phase_id())) {
      latest_phase_id_ = partial.phase_id();
    }
  }
  auto& counter_values = worker_counter_values_[partial.worker_id()];
  for (const auto& [name, value] : partial.counters()) {
    counter_values[name] = value;
  }
  for (const nighthawk::client::StatisticSnapshot& stat : partial.statistics()) {
    worker_state.statistics[stat.id()] = stat;
  }
}

// FORK-ONLY (We-Amp): the numeric suffix of an epoch id ("epoch-<n>");
// -1 for the empty epoch (the pre-first-apply state sorts before everything)
// and for anything that is not an "epoch-<n>" id.
static int64_t epochNumber(const std::string& epoch_id) {
  if (epoch_id.empty() || !absl::StartsWith(epoch_id, "epoch-")) {
    return -1;
  }
  int64_t n = -1;
  if (!absl::SimpleAtoi(absl::StripPrefix(epoch_id, "epoch-"), &n)) {
    return -1;
  }
  return n;
}

void LivePartialAggregator::advanceEpochLabel(const std::string& worker_epoch) {
  const int64_t stamped = epochNumber(worker_epoch);
  const int64_t label = epochNumber(latest_epoch_id_);
  if (stamped > label) {
    // First-applied wins: a later epoch moves the label immediately.
    latest_epoch_id_ = worker_epoch;
    epoch_change_pending_ = true;
    return;
  }
  if (stamped < label) {
    // A stamp earlier than the label is a ROLLED-BACK update: demote only
    // once no worker is still stamped at the demoted epoch (the last
    // epoch-<n>-stamped worker's post-rollback partial). Retired/idle
    // workers frozen on "" or an older epoch never block the demotion.
    for (const auto& [worker_id, worker_state] : worker_states_) {
      if (epochNumber(worker_state.epoch_id) >= label) {
        return;
      }
    }
    int64_t highest = -1;
    std::string highest_id;
    for (const auto& [worker_id, worker_state] : worker_states_) {
      const int64_t n = epochNumber(worker_state.epoch_id);
      if (n > highest) {
        highest = n;
        highest_id = worker_state.epoch_id;
      }
    }
    latest_epoch_id_ = highest_id;
    epoch_change_pending_ = true;
  }
}

nighthawk::client::LiveSnapshot LivePartialAggregator::snapshot() {
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
  snapshot.set_epoch_id(latest_epoch_id_);
  snapshot.set_epoch_changed(epoch_change_pending_);
  epoch_change_pending_ = false;
  return snapshot;
}

} // namespace Client
} // namespace Nighthawk
