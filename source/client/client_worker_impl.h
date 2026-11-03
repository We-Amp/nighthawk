#pragma once

#include <chrono>
#include <functional>
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
                   std::chrono::milliseconds live_snapshot_interval = {});
  StatisticPtrMap statistics() const override;

  const std::map<std::string, uint64_t>& threadLocalCounterValues() override {
    return threadLocalCounterValues_;
  }

  const Phase& phase() const override { return *phase_; }

  void shutdownThread() override;

  void requestExecutionCancellation() override;

  /**
   * Returns additional output from any specified User Defined Output plugins.
   */
  std::vector<nighthawk::client::UserDefinedOutput> getUserDefinedOutputResults() const override;

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

  std::unique_ptr<Envoy::TimeSource> time_source_;
  const TerminationPredicateFactory& termination_predicate_factory_;
  const SequencerFactory& sequencer_factory_;
  Envoy::Stats::ScopeSharedPtr worker_scope_;
  Envoy::Stats::ScopeSharedPtr worker_number_scope_;
  const int worker_number_;
  Envoy::Tracing::TracerSharedPtr& tracer_;
  RequestSourcePtr request_generator_;
  BenchmarkClientPtr benchmark_client_;
  PhasePtr phase_;
  Envoy::LocalInfo::LocalInfoPtr local_info_;
  std::map<std::string, uint64_t> threadLocalCounterValues_;
  const HardCodedWarmupStyle hardcoded_warmup_style_;
  const std::function<void(nighthawk::client::WorkerLiveSnapshotPartial)> live_snapshot_partial_cb_;
  const std::chrono::milliseconds live_snapshot_interval_;
  Envoy::Event::TimerPtr live_snapshot_timer_;
};

using ClientWorkerImplPtr = std::unique_ptr<ClientWorkerImpl>;

} // namespace Client
} // namespace Nighthawk
