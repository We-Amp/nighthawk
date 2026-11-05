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
// - latest_phase_id_ advances MONOTONICALLY along the run's phase order
//   (setPhaseOrder): a lagging partial -- notably a removed worker's
//   first-ever fold partial (M2 Task 6), which carries its old stamp -- can
//   never drag the label backward and freeze the percentiles (M2 review
//   fix). Without an order set, any new stamp advances (legacy behavior).
// Counters are cumulative from run start, so their per-worker latests fold
// safely across stamps and the counter sums stay monotonic.
class LivePartialAggregator {
public:
  // Sets the run's phase ids in order; latest_phase_id_ then advances only
  // to a LATER entry. Call before the first partial (ProcessImpl does, for
  // multi-phase runs; empty order keeps the legacy advance-on-new-stamp
  // behavior).
  void setPhaseOrder(std::vector<std::string> phase_order);

  // Folds one immutable partial, computed and stamped on its worker
  // dispatcher, into the running state.
  void addPartial(nighthawk::client::WorkerLiveSnapshotPartial partial);

  // Completes a snapshot from the folded state. The monotonic stamp is
  // strictly increasing across calls. The snapshot's epoch_id is the run's
  // current knob epoch (M3 Task 6, spec 8.5): tracked from the per-worker
  // epoch stamps -- empty until the first applied change, "epoch-<n>" from
  // the bend instant on -- and epoch_changed is true on exactly the one
  // snapshot per epoch change (advance or post-rollback demotion).
  nighthawk::client::LiveSnapshot snapshot();

private:
  // Position of phase_id in phase_order_; -1 when unknown (or no order set).
  int stampRank(const std::string& phase_id) const;
  // Whether a worker advancing to phase_id may advance latest_phase_id_.
  bool advancesLatest(const std::string& phase_id) const;
  // FORK-ONLY (We-Amp): M3 Task 6. Updates latest_epoch_id_ from the
  // per-worker epoch stamps. "epoch-<n>" ids compare by their numeric
  // suffix ("" sorts before every epoch: it is the pre-first-apply state).
  // - Advance (first-applied wins): any worker stamping a LATER epoch moves
  //   the label there immediately and marks one epoch change.
  // - Demotion (rollback honesty): a stamp EARLIER than the label is a
  //   rolled-back update; the label moves back down only once NO worker is
  //   still stamped at the demoted epoch (the last epoch-<n>-stamped
  //   worker's post-rollback partial), and marks one epoch change.
  //   Retired/idle workers frozen on "" (or an older epoch) never block a
  //   demotion.
  void advanceEpochLabel(const std::string& worker_epoch);

  struct WorkerState {
    std::string phase_id;
    std::string epoch_id;
    std::map<std::string, nighthawk::client::StatisticSnapshot> statistics;
  };
  std::vector<std::string> phase_order_;
  std::map<uint32_t, absl::flat_hash_map<std::string, uint64_t>> worker_counter_values_;
  std::map<uint32_t, WorkerState> worker_states_;
  // The newest stamp any worker has advanced to; "main" before the first
  // partial arrives (legacy single-phase runs have no other stamp).
  std::string latest_phase_id_{"main"};
  // FORK-ONLY (We-Amp): M3 Task 6. The run's current knob epoch (empty
  // before the first applied change) and the pending one-snapshot change
  // marker.
  std::string latest_epoch_id_;
  bool epoch_change_pending_{false};
  // Enforces strictly increasing monotonic_ns across snapshots.
  int64_t last_live_snapshot_monotonic_ns_{0};
};

} // namespace Client
} // namespace Nighthawk
