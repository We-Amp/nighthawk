// FORK-ONLY (We-Amp): bounded FIFO worker pool for pprof work (design spec
// §4): parse/fold/symbolize run off both the render and the steering paths.
// Symbolization is slow (~40 min per `-proto` run over a full-DWARF binary —
// runbook budget), so jobs must never block the FTXUI loop or the feed
// thread; Submit is non-blocking and FIFO.
#ifndef NIGHTHAWK_TUI_PPROF_WORKER_POOL_H_
#define NIGHTHAWK_TUI_PPROF_WORKER_POOL_H_

#include <deque>
#include <functional>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

class PprofWorkerPool {
 public:
  // `workers` bounded worker threads; values < 1 clamp to 1.
  explicit PprofWorkerPool(int workers = 2);

  // Joins: finishes all queued jobs, then exits. No job may Submit after the
  // destructor starts (jobs must not enqueue work on their own pool).
  ~PprofWorkerPool();

  PprofWorkerPool(const PprofWorkerPool&) = delete;
  PprofWorkerPool& operator=(const PprofWorkerPool&) = delete;

  // Non-blocking; FIFO dispatch to the next idle worker.
  void Submit(std::function<void()> job);

 private:
  void WorkerLoop();

  // Worker wake condition, checked under mu_.
  bool HasWorkOrStopped() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return stopped_ || !jobs_.empty();
  }

  std::vector<std::thread> workers_;
  absl::Mutex mu_;
  std::deque<std::function<void()>> jobs_ ABSL_GUARDED_BY(mu_);
  bool stopped_ ABSL_GUARDED_BY(mu_) = false;
};

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk

#endif  // NIGHTHAWK_TUI_PPROF_WORKER_POOL_H_
