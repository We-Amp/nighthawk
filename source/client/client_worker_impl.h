#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <vector>

#include "api/client/live.pb.h"

#include "envoy/api/api.h"
#include "envoy/event/dispatcher.h"
#include "envoy/stats/store.h"
#include "envoy/thread_local/thread_local.h"
#include "envoy/upstream/cluster_manager.h"

#include "nighthawk/client/benchmark_client.h"
#include "nighthawk/client/client_worker.h"
#include "nighthawk/client/factories.h"
#include "nighthawk/common/factories.h"
#include "nighthawk/common/phase.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/sequencer.h"
#include "nighthawk/common/termination_predicate.h"
#include "nighthawk/user_defined_output/user_defined_output_plugin.h"

#include "source/common/worker_impl.h"

namespace Nighthawk {
namespace Client {

// FORK-ONLY (We-Amp): one instance per phase boundary, owned by the worker.
// Watches the benchmark client's in-flight count on the worker dispatcher and
// completes THIS boundary's drain promise when it reaches zero. The promise is
// captured by value at construction: a watch that still fires after the next
// boundary began (a hung request completing during the gap, before the next
// begin-phase task cancels it) can only satisfy its own boundary's ack, never
// the next one's. Everything runs on the worker dispatcher; no connection
// operations of any kind -- the orchestrator's deadline bounds the wait.
class BoundaryDrainWatch {
public:
  BoundaryDrainWatch(Envoy::Event::Dispatcher& dispatcher,
                     std::function<uint64_t()> in_flight,
                     std::shared_ptr<std::promise<PhaseDrainAck>> promise,
                     int64_t stop_monotonic_ns)
      : dispatcher_(dispatcher), in_flight_(std::move(in_flight)),
        promise_(std::move(promise)), stop_monotonic_ns_(stop_monotonic_ns) {}

  void start() {
    timer_ = dispatcher_.createTimer([this]() { check(); });
    timer_->enableHRTimer(std::chrono::milliseconds(1));
  }

  // Called by the next begin-phase on the worker dispatcher; destroying the
  // timer also cancels any pending firing of this watch.
  void cancel() {
    if (timer_ != nullptr) {
      timer_->disableTimer();
      timer_.reset();
    }
  }

private:
  void check() {
    if (timer_ == nullptr) {
      return;
    }
    if (in_flight_() == 0) {
      PhaseDrainAck ack;
      ack.sequencer_stop_monotonic_ns = stop_monotonic_ns_;
      ack.drain_complete_monotonic_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
      promise_->set_value(ack);
      timer_->disableTimer();
    } else {
      timer_->enableHRTimer(std::chrono::milliseconds(5));
    }
  }

  Envoy::Event::Dispatcher& dispatcher_;
  const std::function<uint64_t()> in_flight_;
  const std::shared_ptr<std::promise<PhaseDrainAck>> promise_;
  const int64_t stop_monotonic_ns_;
  Envoy::Event::TimerPtr timer_;
};

class ClientWorkerImpl : public WorkerImpl, virtual public ClientWorker {
public:
  enum class HardCodedWarmupStyle { OFF, ON };

  ClientWorkerImpl(Envoy::Api::Api& api, Envoy::ThreadLocal::Instance& tls,
                   Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                   const BenchmarkClientFactory& benchmark_client_factory,
                   const TerminationPredicateFactory& termination_predicate_factory,
                   const SequencerFactory& sequencer_factory,
                   const RequestSourceFactory& request_generator_factory,
                   Envoy::Stats::Store& store, const int worker_number,
                   const Envoy::MonotonicTime starting_time,
                   Envoy::Tracing::TracerSharedPtr& tracer,
                   const HardCodedWarmupStyle hardcoded_warmup_style,
                   std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins,
                   const std::function<void(nighthawk::client::WorkerLiveSnapshotPartial)>&
                       live_snapshot_partial_cb = {},
                   std::chrono::milliseconds live_snapshot_interval = {},
                   bool phase_driven_run = false);
  StatisticPtrMap statistics() const override;

  const std::map<std::string, uint64_t>& threadLocalCounterValues() override {
    return threadLocalCounterValues_;
  }

  const Phase& phase() const override;

  void shutdownThread() override;

  void requestExecutionCancellation() override;

  /**
   * Returns additional output from any specified User Defined Output plugins.
   */
  std::vector<nighthawk::client::UserDefinedOutput> getUserDefinedOutputResults() const override;

  // FORK-ONLY (We-Amp): multi-phase support, see the ClientWorker interface.
  std::shared_future<void> readyFuture() const override { return ready_future_; }
  std::future<PhaseDrainAck> beginPhaseAsync(uint32_t phase_index,
                                             const nighthawk::client::PhaseSpec& phase,
                                             bool final_phase) override;
  const std::vector<StatisticPtrMap>& phaseStatistics() const override {
    return phase_statistics_;
  }
  const std::vector<int64_t>& phaseExecutionDurations() const override {
    return phase_execution_durations_;
  }

protected:
  void work() override;

private:
  void simpleWarmup();
  // FORK-ONLY (We-Amp): live snapshot streaming support. The worker computes
  // percentile summaries and counter values of its own, thread-confined
  // statistics on its own dispatcher thread (statistics are not shared or
  // locked), wrapping them in an immutable partial that is handed to the
  // collector via live_snapshot_partial_cb_. No locks are taken on request
  // hot paths; the callback only fires at the live snapshot cadence.
  void setupLiveSnapshotTimer();
  void emitLiveSnapshotPartial();
  // FORK-ONLY (We-Amp): multi-phase support. All of these run on this
  // worker's own dispatcher: beginPhase builds the per-phase sequencer
  // (fresh termination predicate and rate limiter per phase) and starts it;
  // the sequencer's stop hook chains into the boundary drain watch, which
  // completes the drain promise once the benchmark client's in-flight count
  // reaches zero. The dispatcher never blocks on the orchestrator.
  void beginPhase(uint32_t phase_index, const nighthawk::client::PhaseSpec& phase_spec,
                  bool final_phase);
  void startDrainWatch(int64_t stop_monotonic_ns);

  Envoy::Api::Api& api_;
  std::unique_ptr<Envoy::TimeSource> time_source_;
  const TerminationPredicateFactory& termination_predicate_factory_;
  const SequencerFactory& sequencer_factory_;
  Envoy::Stats::ScopeSharedPtr worker_scope_;
  Envoy::Stats::ScopeSharedPtr worker_number_scope_;
  const int worker_number_;
  Envoy::Tracing::TracerSharedPtr& tracer_;
  RequestSourcePtr request_generator_;
  BenchmarkClientPtr benchmark_client_;
  SequencerTarget sequencer_target_;
  PhasePtr phase_;
  Envoy::LocalInfo::LocalInfoPtr local_info_;
  std::map<std::string, uint64_t> threadLocalCounterValues_;
  const HardCodedWarmupStyle hardcoded_warmup_style_;
  const std::function<void(nighthawk::client::WorkerLiveSnapshotPartial)> live_snapshot_partial_cb_;
  const std::chrono::milliseconds live_snapshot_interval_;
  Envoy::Event::TimerPtr live_snapshot_timer_;
  // FORK-ONLY (We-Amp): multi-phase run state. Everything here is touched on
  // the worker dispatcher thread only, except ready_future_ (fixed at
  // construction) and the vectors read by the process after the workers have
  // been joined.
  const bool phase_driven_run_;
  // Points at the phase begun most recently; phases_history_ keeps ownership.
  Phase* current_phase_{nullptr};
  std::vector<PhasePtr> phases_history_;
  std::vector<StatisticPtrMap> phase_statistics_;
  std::vector<int64_t> phase_execution_durations_;
  std::promise<void> ready_promise_;
  std::shared_future<void> ready_future_;
  // Created on the orchestrator thread in beginPhaseAsync (before the post,
  // so the worker's later read is synchronized through the post queue) and
  // handed to the BoundaryDrainWatch by value on the worker dispatcher.
  std::shared_ptr<std::promise<PhaseDrainAck>> drain_promise_;
  std::unique_ptr<BoundaryDrainWatch> drain_watch_;
  // Keeps the blocking dispatcher run alive while no phase work is scheduled
  // yet (a libevent loop with no registered events returns immediately).
  Envoy::Event::TimerPtr dispatcher_loop_timer_;
};

using ClientWorkerImplPtr = std::unique_ptr<ClientWorkerImpl>;

} // namespace Client
} // namespace Nighthawk
