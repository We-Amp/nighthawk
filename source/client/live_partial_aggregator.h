#pragma once

// FORK-ONLY (We-Amp): folds immutable per-worker live snapshot partials into
// running snapshot state. Extracted from ProcessImpl::toLiveSnapshot so the
// fold is unit-testable without a ProcessImpl, and extended for the M2 A1
// phase dimension.

#include <map>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"

#include "api/client/live.pb.h"

namespace Nighthawk {
namespace Client {

// The snapshot-acquisition-vs-slab-swap seam (M2 A1, spec 8.1): every partial
// is stamped with its phase id at computation time on its worker's dispatcher
// (the same dispatcher that swaps the statistic slabs, so within one worker
// the swap and the partial are serialized and a partial never mixes slabs).
// This fold honors the stamps:
// - a worker's stored statistics are replaced wholesale when a partial with a
//   new stamp arrives (its old slab is done), and
// - the aggregated snapshot exposes only the statistics of the newest stamp
//   any worker has advanced to: a partial from the previous phase is never
//   joined with a post-boundary slab, and a worker whose latest partial is
//   still from the previous phase contributes no statistics to the snapshot
//   until its first partial of the current phase arrives.
// Counters are cumulative from run start, so their per-worker latests fold
// safely across stamps and the counter sums stay monotonic.
class LivePartialAggregator {
public:
  // Folds one immutable partial, computed and stamped on its worker
  // dispatcher, into the running state.
  void addPartial(nighthawk::client::WorkerLiveSnapshotPartial partial);

  // Completes a snapshot from the folded state. The monotonic stamp is
  // strictly increasing across calls.
  nighthawk::client::LiveSnapshot snapshot(const std::string& epoch_id);

private:
  struct WorkerState {
    std::string phase_id;
    std::map<std::string, nighthawk::client::StatisticSnapshot> statistics;
  };
  std::map<uint32_t, absl::flat_hash_map<std::string, uint64_t>> worker_counter_values_;
  std::map<uint32_t, WorkerState> worker_states_;
  // The newest stamp any worker has advanced to; "main" before the first
  // partial arrives (legacy single-phase runs have no other stamp).
  std::string latest_phase_id_{"main"};
  // Enforces strictly increasing monotonic_ns across snapshots.
  int64_t last_live_snapshot_monotonic_ns_{0};
};

} // namespace Client
} // namespace Nighthawk
