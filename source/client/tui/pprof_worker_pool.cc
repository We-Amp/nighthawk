// FORK-ONLY (We-Amp): implementation of the bounded pprof worker pool.
// See pprof_worker_pool.h for the contract.
#include "source/client/tui/pprof_worker_pool.h"

#include "absl/synchronization/mutex.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

PprofWorkerPool::PprofWorkerPool(int workers) {
  if (workers < 1) {
    workers = 1;
  }
  workers_.reserve(static_cast<size_t>(workers));
  for (int i = 0; i < workers; ++i) {
    workers_.emplace_back([this] { WorkerLoop(); });
  }
}

PprofWorkerPool::~PprofWorkerPool() {
  {
    absl::MutexLock lock(&mu_);
    stopped_ = true;
  }
  for (std::thread& worker : workers_) {
    worker.join();
  }
}

void PprofWorkerPool::Submit(std::function<void()> job) {
  absl::MutexLock lock(&mu_);
  jobs_.push_back(std::move(job));
}

void PprofWorkerPool::WorkerLoop() {
  for (;;) {
    std::function<void()> job;
    {
      absl::MutexLock lock(&mu_);
      // The condition is checked under the lock; workers wake on mu_'s
      // implicit condvar whenever Submit or the destructor mutates state.
      mu_.Await(absl::Condition(this, &PprofWorkerPool::HasWorkOrStopped));
      if (jobs_.empty()) {
        // stopped_ and fully drained.
        return;
      }
      job = std::move(jobs_.front());
      jobs_.pop_front();
    }
    job();
  }
}

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
