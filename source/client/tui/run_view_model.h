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
  // The primary latency series (the snapshot's first statistic), capped at
  // kRunViewModelMaxPoints.
  std::vector<PercentilePoint> latency_band;
  // One capped series per counter name, capped at kRunViewModelMaxPoints.
  absl::flat_hash_map<std::string, std::vector<CounterPoint>> counters;
  // The most recent raw snapshot, for whatever the text panes want to show.
  nighthawk::client::LiveSnapshot last_snapshot;
};

// Pure reducer: folds one LiveSnapshot into the model, returning the updated
// model (pass-by-value + move keeps the copy-on-write shape obvious at call
// sites). Appends the snapshot's first statistic to latency_band (the
// aggregator places the primary latency statistic first) and every counter to
// its series, both capped at kRunViewModelMaxPoints by dropping the oldest
// points. Sets last_snapshot; never touches run_state.
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
