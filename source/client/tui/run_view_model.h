#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "api/client/live.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the immutable snapshot store and its ApplySnapshot
// reducer (spec §4 threading model). The feed's read thread folds each
// LiveSnapshot into a RunViewModel via the pure ApplySnapshot and hands the
// finished model to the SnapshotStore; the render loop loads a cheap
// shared_ptr copy and renders it for as long as it likes — no render thread
// can ever observe a half-updated model, because stored models are never
// mutated again (copy-on-write: every store swaps in a fresh model).

// The number of data points the per-series buffers keep. 600 points equals
// 10 minutes at the default 1s stats flush interval; older points drop off
// the front.
inline constexpr size_t kRunViewModelMaxPoints = 600;

struct PercentilePoint {
  uint64_t monotonic_ns;
  uint64_t p50_ns;
  uint64_t p90_ns;
  uint64_t p99_ns;
};

struct CounterPoint {
  uint64_t monotonic_ns;
  uint64_t value;
};

struct RunViewModel {
  // "idle" | "running" | "completed" | "stopped". Owned by the feed's
  // lifecycle events, not by ApplySnapshot (a snapshot never changes state).
  std::string run_state;
  // The accepted StartRequest's target uri, set by the controller the moment
  // a run is launched (before the first snapshot). Once run_state leaves
  // "idle" the status bar renders this — never "(no target)" — with
  // "(target pending)" reserved for the sub-second pre-ack window.
  std::string target_uri;
  // The primary latency series (the snapshot's first statistic WITH samples;
  // the aggregator's statistics order is unspecified, so the sampled
  // statistic is found by scanning, not assumed at index 0), capped at
  // kRunViewModelMaxPoints.
  std::vector<PercentilePoint> latency_band;
  // One capped series per counter name, capped at kRunViewModelMaxPoints.
  absl::flat_hash_map<std::string, std::vector<CounterPoint>> counters;
  // The most recent raw snapshot, for whatever the text panes want to show.
  nighthawk::client::LiveSnapshot last_snapshot;
  // One-frame markers recomputed by every ApplySnapshot (true only on the
  // frame whose snapshot changed the scale), so the chart renderer can print
  // a one-frame "(rescaled)" marker when the latency window max grows.
  bool latency_rescaled = false;
  // Spike salience: while > 0, the chart renders the "!" excursion glyph and
  // the "^Nx" numeric tail. Set to kSpikeFrames when the newest p99 exceeds
  // kSpikeRatio over its recent floor; decremented otherwise.
  int spike_frames_remaining = 0;
  double spike_ratio = 0.0;
};

// How many consecutive flushes a detected spike stays annotated.
inline constexpr int kSpikeFrames = 3;
// The spike threshold: latest p99 over its recent floor (the minimum p99 of
// the last kSpikeFloorWindow points).
inline constexpr double kSpikeRatio = 2.0;
inline constexpr size_t kSpikeFloorWindow = 20;

// Pure reducer: folds one LiveSnapshot into the model, returning the updated
// model (pass-by-value + move keeps the copy-on-write shape obvious at call
// sites). Appends a latency_band point from the snapshot's first statistic
// with samples — count == 0 means the percentile fields are meaningless
// (live.proto's consumer contract), so zero-count statistics are skipped
// entirely and leave a gap; the aggregator's statistics order is unspecified,
// so the sampled statistic is found by scanning, not assumed at index 0 —
// and every counter to its series, both capped at kRunViewModelMaxPoints by
// dropping the oldest points. Sets last_snapshot; never touches run_state or
// target_uri. Recomputes the one-frame chart markers (latency_rescaled,
// spike_frames_remaining/spike_ratio) from the pre-append band.
RunViewModel ApplySnapshot(RunViewModel model, const nighthawk::client::LiveSnapshot& snapshot);

// Mutex-guarded immutable swap. Feed side stores fully-formed models; render
// side loads a shared_ptr — a two-instruction copy under the mutex, after
// which the renderer works against an immutable snapshot for free.
class SnapshotStore {
public:
  // Swaps in a fresh immutable model. Any previously loaded pointers keep
  // serving the old version.
  void store(RunViewModel model);
  // The current model, or nullptr before the first store(). The pointer is
  // stable between stores (repeat loads return the same object).
  std::shared_ptr<const RunViewModel> load() const;

private:
  mutable absl::Mutex mu_;
  std::shared_ptr<const RunViewModel> current_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
