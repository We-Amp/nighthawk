#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "absl/container/flat_hash_map.h"

#include "api/client/live.pb.h"

#include "envoy/api/api.h"
#include "envoy/network/address.h"
#include "envoy/server/instance.h"
#include "envoy/stats/store.h"
#include "envoy/tracing/tracer.h"

#include "nighthawk/client/client_worker.h"
#include "nighthawk/client/factories.h"
#include "nighthawk/client/options.h"
#include "nighthawk/client/output_collector.h"
#include "nighthawk/client/process.h"
#include "nighthawk/common/live_tunable_rate_limiter.h"
#include "nighthawk/common/statistic.h"

#include "source/client/live_partial_aggregator.h"
#include "source/client/live_update_applier.h"
#include "source/client/live_update_validator.h"

#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "source/common/access_log/access_log_manager_impl.h"
#include "source/common/common/logger.h"
#include "source/common/common/random_generator.h"
#include "source/common/common/statusor.h"
#include "source/common/event/real_time_system.h"
#include "source/common/grpc/context_impl.h"
#include "source/common/http/context_impl.h"
#include "source/common/network/dns_resolver/dns_factory_util.h"
#include "source/common/protobuf/message_validator_impl.h"
#include "source/common/quic/quic_stat_names.h"
#include "source/common/router/context_impl.h"
#include "source/common/secret/secret_manager_impl.h"
#include "source/common/stats/allocator_impl.h"
#include "source/common/stats/thread_local_store.h"
#include "source/common/thread_local/thread_local_impl.h"
#include "source/common/tls/context_manager_impl.h"
#include "source/common/upstream/cluster_manager_impl.h"
#include "source/exe/platform_impl.h"
#include "source/exe/process_wide.h"
#include "source/server/config_validation/admin.h"
#include "source/server/options_impl.h"

#include "source/client/benchmark_client_impl.h"
#include "source/client/factories_impl.h"
#include "source/client/phase_orchestrator.h"
#include "source/client/flush_worker_impl.h"
#include "source/client/process_bootstrap.h"

namespace Nighthawk {
namespace Client {

class ClusterManagerFactory;

// FORK-ONLY (We-Amp): M3 Task 3 -- one worker's CURRENT-PHASE live-update
// capability. Recorded by the worker's begin-phase task ON the worker
// dispatcher (fresh sequencer + fresh limiter per phase, M2 T3), so a stale
// pre-boundary handle never survives a begin-phase.
struct LiveCapabilityEntry {
  uint32_t phase_index{0};
  // The captured pre-decorator inner limiter: identity for staleness checks
  // and Task 6 staging probes. May be null (a limiter without the
  // capability); apply then fails honestly.
  LiveTunableRateLimiter* handle{nullptr};
  // Binds the worker's current-phase sequencer: posts the apply onto the
  // worker's own dispatcher (thread confinement).
  LiveApplyFn apply;
  // Binds the same sequencer's cancel surface (M3 Task 6): cancels the
  // worker's staged-but-unfired bend (the staging-failure path). Posts onto
  // the worker dispatcher; a no-op when nothing is pending.
  std::function<void()> cancel_live_update;
};

// FORK-ONLY (We-Amp): M3 Task 3 -- the per-PHASE capability registry:
// worker-id -> the worker's current-phase capability. The limiter is
// per-phase (M2 T3 builds a fresh sequencer per phase), so entries are
// (re)captured at each begin-phase; fan-out to a worker whose current phase
// carries no stamp for the targeted phase (never begun, retired by the M2
// worker subsets, or raced by a boundary) fails the update honestly at
// lookup -- never a silent skip, never a mutation through a dead handle.
// Task 6 consumes this registry for its ordered fan-out.
class LiveCapabilityRegistry {
public:
  // Called from the worker's begin-phase task on the WORKER dispatcher (and
  // once for a legacy run's main sequencer): (re)stamps the worker's entry.
  void Record(int worker_id, uint32_t phase_index, LiveTunableRateLimiter* handle,
              LiveApplyFn apply, std::function<void()> cancel_live_update);
  // Reader side (the Task 6 fan-out): the worker's capability stamped for
  // `phase_index`, or an error when the worker has no stamp for that phase
  // (no entry at all, or a stale pre-boundary one). Thread-safe.
  absl::StatusOr<LiveCapabilityEntry> CapabilityForPhase(int worker_id,
                                                         uint32_t phase_index) const;
  // A new run starts from an empty registry (workers are recreated per run;
  // this guards a re-run on a long-lived process). Also cleared when the
  // workers are torn down (M3 Task 6's registry-outlives-sequencer ruling:
  // a stale entry whose sequencer is gone must never be reachable).
  void Reset();

private:
  mutable std::mutex mutex_;
  absl::flat_hash_map<int, LiveCapabilityEntry> entries_;
};

// FORK-ONLY (We-Amp): M3 Task 6. Everything the fan-out reads off the
// in-flight run, as injectable functions so the fan-out mechanics (ordering,
// staging, rollback, honest aggregation) are unit-testable without a
// ProcessImpl. ProcessImpl::MakeLiveUpdateApplier() builds the real one.
struct LiveFanOutContext {
  // The engine clock the rollback instant is taken at (and the fan-out's
  // deadline math).
  std::function<Envoy::MonotonicTime()> now;
  // OkStatus when a live update may be applied right now; the error message
  // is the rejection reason otherwise (boundary drain in progress, no
  // active workers, run teardown started).
  std::function<absl::Status()> update_admissible;
  // Error when t_bend would land outside the current phase's (phase-vector
  // runs) or the run's (legacy runs) remaining duration: a bend landing
  // past the boundary is undefined and rejected BEFORE any fan-out.
  std::function<absl::Status(Envoy::MonotonicTime)> bend_instant_admissible;
  std::function<uint32_t()> current_phase_index;
  std::function<uint32_t()> active_worker_count;
  std::function<absl::StatusOr<LiveCapabilityEntry>(int worker_id, uint32_t phase_index)>
      capability_for_phase;
  // The current phase's declared schedule (the per-phase rps inheritance
  // applied): initializes the fan-out's rollback prior-state tracker when
  // the phase changes (each phase constructs fresh limiters, so the prior
  // schedule is the phase's declared one, not the previous phase's bent
  // one).
  std::function<LiveKnobDelta()> current_phase_schedule;
  // The shutdown fence. fan_out_begin returns false once the run's workers
  // are being torn down (a fan-out must not be started against them); every
  // started fan-out pairs it with fan_out_end, and the teardown waits for
  // the count to drain before destroying the workers.
  std::function<bool()> fan_out_begin;
  std::function<void()> fan_out_end;
};

// FORK-ONLY (We-Amp): M3 Task 6 -- the real update applier: the process-side
// fan-out. Implements the LiveUpdateApplier seam (called on a
// short-lived orchestrator thread, honors the deadline): worker-id-ordered
// staging posts onto the active workers' own dispatchers through the
// per-PHASE capability registry, the staging-confirm-before-t_bend primary
// failure path (cancel-everything, nothing applied), the residual post-bend
// rollback at the rollback instant t_r (a re-anchored re-apply of the PRIOR
// schedule -- the A2b design note's Q7 form), and the honest single
// UpdateResult aggregation (applied=true only when every active worker
// confirmed the apply; the epoch number is consumed by the orchestrator
// only then).
class LiveCapabilityFanOutApplier : public LiveUpdateApplier,
                                    public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  explicit LiveCapabilityFanOutApplier(LiveFanOutContext context);

  nighthawk::client::UpdateResult
  ApplyDeltas(const nighthawk::client::UpdateRequest& request, const std::string& epoch_id,
              const EngineInstant& bend_instant, absl::Time deadline) override;

private:
  struct WorkerOutcome {
    int worker_id;
    absl::Status status;
  };
  // Condition callbacks for the bounded waits (the mutex is held).
  bool StageOutcomesComplete() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool ApplyOutcomesComplete() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Registers the stage/apply callbacks for one worker's post.
  void RecordStageOutcome(int worker_id, absl::Status status);
  void RecordApplyOutcome(int worker_id, absl::Status status);
  // The normalized delta set (the wire oneofs, resolved; validation
  // guarantees at least one knob and no duplicate knob).
  static LiveKnobDelta NormalizeDelta(const nighthawk::client::UpdateRequest& request);

  LiveFanOutContext context_;
  absl::Mutex mutex_;
  std::vector<WorkerOutcome> stage_outcomes_ ABSL_GUARDED_BY(mutex_);
  std::vector<WorkerOutcome> apply_outcomes_ ABSL_GUARDED_BY(mutex_);
  size_t expected_workers_ ABSL_GUARDED_BY(mutex_) = 0;
  // Task 4 Q4's no-queued-bends ruling, enforced at the fan-out: a second
  // update arriving before the first resolves is rejected.
  bool apply_in_progress_ ABSL_GUARDED_BY(mutex_) = false;
  // The rollback prior-state tracker: the current phase's declared schedule,
  // updated after every applied change. Reset whenever the phase index moves
  // (fresh limiters per phase).
  uint32_t tracked_phase_index_ ABSL_GUARDED_BY(mutex_) = 0;
  bool schedule_initialized_ ABSL_GUARDED_BY(mutex_) = false;
  LiveKnobDelta tracked_schedule_ ABSL_GUARDED_BY(mutex_);
  // The epoch id the rollback restores ("" before the run's first applied
  // change). Advances to the applied epoch on every success.
  std::string prior_epoch_id_ ABSL_GUARDED_BY(mutex_);
};

/**
 * Only a single instance is allowed at a time machine-wide in this implementation.
 * Running multiple instances at the same might introduce noise into the measurements.
 * If there turns out to be a desire to run multiple instances at the same time, we could
 * introduce a --lock-name option. Note that multiple instances in the same process may
 * be problematic because of Envoy enforcing a single runtime instance.
 */
// FORK-ONLY (m3-drain-pair): the boundary-drain live-update gate, scoped by
// boundary index. The workers' stop hooks fire open() with the boundary the
// stop belongs to; runPhases fires close() when that boundary's drain is
// collected. A straggler whose non-final sequencer stops only AFTER its
// boundary was closed (the censored-drain shape: collectDrainAcks gave up
// on it) fires a stale index, which open() ignores -- the old unscoped flag
// re-opened the gate mid-successor-phase and nothing cleared it until the
// NEXT boundary, spuriously rejecting legitimate live updates for the rest
// of the successor phase. Cheap: a mutex-protected pair, touched once per
// stop hook / boundary close and by the update-admission check.
class BoundaryDrainGate {
public:
  void open(uint32_t boundary_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (static_cast<int64_t>(boundary_index) <= last_closed_boundary_) {
      return;
    }
    in_progress_ = true;
  }
  void close(uint32_t boundary_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (static_cast<int64_t>(boundary_index) > last_closed_boundary_) {
      last_closed_boundary_ = boundary_index;
    }
    in_progress_ = false;
  }
  bool in_progress() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return in_progress_;
  }

private:
  mutable std::mutex mutex_;
  int64_t last_closed_boundary_{-1};
  bool in_progress_{false};
};

class ProcessImpl : public Process, public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * Creates a ProcessImpl.
   * @param options provides the options configuration to be used.
   * @param dns_resolver_factory provides a pluggable factory to create a DNS resolver. The
   * resolver is used for resolving DNS names in the bootstrap and then by the cluster manager.
   * @param typed_dns_resolver_config the config that defined the dns_resolver_factory, also
   * needed as an input to createDnsResolver() on the factory.
   * @param time_system provides the Envoy::Event::TimeSystem implementation that will be used.
   * @param process_wide optional parameter which can be used to pass a pre-setup reference to
   * an active Envoy::ProcessWide instance. ProcessImpl will add a reference to this when passed,
   * and hold on that that throughout its lifetime.
   * If this parameter is not supplied, ProcessImpl will contruct its own Envoy::ProcessWide
   * instance.
   */
  static absl::StatusOr<ProcessPtr>
  CreateProcessImpl(const Options& options,
                    Envoy::Network::DnsResolverFactory& dns_resolver_factory,
                    envoy::config::core::v3::TypedExtensionConfig typed_dns_resolver_config,
                    Envoy::Event::TimeSystem& time_system,
                    const std::shared_ptr<Envoy::ProcessWide>& process_wide = nullptr);

  ~ProcessImpl() override;

  /**
   * Runs the process.
   *
   * @param collector output collector implementation which will collect and hold the native output
   * format.
   * @return true iff execution should be considered successful.
   */
  bool run(OutputCollector& collector) override;

  /**
   * FORK-ONLY (We-Amp): drains the immutable per-worker live snapshot partials
   * accumulated so far and aggregates them into a LiveSnapshot. Safe to call
   * concurrently with a running benchmark; the partials are handed over under
   * a lock, and no worker statistics are read from the calling thread.
   */
  nighthawk::client::LiveSnapshot toLiveSnapshot() override;

  /**
   * Should be called before destruction to cleanly shut down.
   */
  void shutdown() override;

  bool requestExecutionCancellation() override;

  /**
   * FORK-ONLY (We-Amp): the in-flight run's current phase id (empty for
   * legacy single-phase runs). Mutex-guarded; safe to call concurrently with
   * a running benchmark (the live-update path reads it for phase targeting
   * validation, M3).
   */
  std::string currentPhaseId() const;

  /**
   * FORK-ONLY (We-Amp): the in-flight run's declared shape for live-update
   * whitelist validation (M3 Task 2). Mutex-guarded; safe to call
   * concurrently with a running benchmark (the update orchestrator calls it
   * on the stream reader thread).
   */
  InFlightRunShape liveUpdateRunShape() const;

  // FORK-ONLY (We-Amp): the per-PHASE live-update capability registry (M3
  // Task 3). Read-only accessor for the update fan-out's consumer (Task 6);
  // workers record entries themselves at each begin-phase.
  const LiveCapabilityRegistry& liveCapabilityRegistry() const {
    return live_capability_registry_;
  }

  // FORK-ONLY (We-Amp): M3 Task 6 -- the real update applier for this
  // in-flight run: the worker-id-ordered capability fan-out with staging
  // confirm, the rollback-at-t_r residual path and the honest aggregation.
  // The service wires it into the run's LiveUpdateOrchestrator; a test may
  // substitute its own applier at the service's factory seam instead.
  std::unique_ptr<LiveUpdateApplier> MakeLiveUpdateApplier();

  // FORK-ONLY (We-Amp): multi-phase support (M2 A1). The callback fires on
  // the run-calling thread at each phase boundary the orchestrator marks;
  // the TUI's phase timeline and window alignment (M2 Task 7) consume these
  // engine-emitted (wall, monotonic) clock pairs.
  void setPhaseEventCallback(std::function<void(const PhaseEvent&)> callback) {
    phase_event_cb_ = std::move(callback);
  }

  /**
   * FORK-ONLY (We-Amp): per-boundary drain bookkeeping (drain duration, idle
   * gap, censoring) recorded by the phase orchestrator during the last run.
   * Feeds the per-phase output metadata (M2 Task 4).
   */
  const std::vector<PhaseBoundaryTiming>& phaseBoundaryTimings() const {
    return boundary_timings_;
  }

  /**
   * FORK-ONLY (We-Amp): per-phase counts of starts refused by the per-phase
   * in-flight gates (M2 Task 5 refuse-and-count ruling: shed, never queued),
   * folded over all workers after the phase loop. One entry per phase in
   * order; feeds the per-phase output metadata (M2 Task 4).
   */
  const std::vector<uint64_t>& phaseGateExceededCounts() const { return phase_gate_exceeded_; }

  /**
   * FORK-ONLY (We-Amp): per-phase worker-subset bookkeeping (M2 Task 6), one
   * entry per phase in order: the active worker count, whether it changed
   * against the prior phase, and whether workers beyond the high-water mark
   * cold-started at the boundary (rejoins within the mark come back on
   * preserved pools and do not set it; see computeWorkerSubsets). This is
   * the engine-side data that feeds PhaseMetadata's worker_count /
   * workers_changed_vs_prior_phase / new_workers_cold_started fields (M2
   * Task 4 wiring) and the TUI's cold-start line (M2 Task 7).
   */
  const std::vector<PhaseWorkerSubset>& phaseWorkerSubsets() const {
    return phase_worker_subsets_;
  }

private:
  // Use CreateProcessImpl to construct an instance of ProcessImpl.
  ProcessImpl(const Options& options, Envoy::Event::TimeSystem& time_system,
              Envoy::Network::DnsResolverFactory& dns_resolver_factory,
              envoy::config::core::v3::TypedExtensionConfig typed_dns_resolver_config,
              const std::shared_ptr<Envoy::ProcessWide>& process_wide = nullptr);

  void addTracingCluster(envoy::config::bootstrap::v3::Bootstrap& bootstrap, const Uri& uri) const;
  void setupTracingImplementation(envoy::config::bootstrap::v3::Bootstrap& bootstrap,
                                  const Uri& uri) const;
  void maybeCreateTracingDriver(const envoy::config::trace::v3::Tracing& configuration);
  void configureComponentLogLevels(spdlog::level::level_enum level);

  /**
   * Prepare the ProcessImpl instance by creating and configuring the workers it needs for execution
   * of the load test.
   *
   * @param concurrency the amount of workers that should be created.
   */
  absl::Status createWorkers(const uint32_t concurrency,
                             const std::optional<Envoy::SystemTime>& schedule);
  std::vector<StatisticPtr> vectorizeStatisticPtrMap(const StatisticPtrMap& statistics) const;
  std::vector<StatisticPtr>
  mergeWorkerStatistics(const std::vector<ClientWorkerPtr>& workers) const;
  // FORK-ONLY (We-Amp): run-total statistics for a multi-phase run, folded
  // from the retained per-phase slabs (the workers' live sets are empty after
  // the end-of-run finalize). See mergePhaseTotalsStatistics() in the .cc.
  std::vector<StatisticPtr> mergePhaseTotalsStatistics() const;
  void setupForHRTimers();
  /**
   * If there are sinks configured in bootstrap, populate stats_sinks with sinks
   * created through NighthawkStatsSinkFactory and add them to store_root_.
   *
   * @param bootstrap the bootstrap configuration which include the stats sink configuration.
   * @param stats_sinks a Sink list to be populated.
   */
  void setupStatsSinks(const envoy::config::bootstrap::v3::Bootstrap& bootstrap,
                       std::list<std::unique_ptr<Envoy::Stats::Sink>>& stats_sinks);
  // FORK-ONLY (We-Amp): sink for immutable per-worker live snapshot partials.
  // Workers push from their own dispatcher threads; the service's collector
  // thread drains via toLiveSnapshot().
  void recordLiveSnapshotPartial(nighthawk::client::WorkerLiveSnapshotPartial partial);
  // FORK-ONLY (We-Amp): multi-phase run loop. Runs on the run-calling thread
  // (the thread that today joins the workers at teardown); posts begin-phase
  // onto each active worker's own dispatcher and waits each boundary's
  // per-worker drain futures with a shared deadline of --timeout.
  void runPhases();
  void emitPhaseEvent(const PhaseEvent& event);
  // FORK-ONLY (We-Amp): also records the phase's duration and start instant,
  // for liveUpdateRunShape()'s remaining-duration bound (M3 Task 2).
  void setCurrentPhaseId(absl::string_view phase_id, std::chrono::nanoseconds phase_duration_ns);
  // FORK-ONLY (We-Amp): the raise-only runtime breaker override arm (M2
  // Task 5, per the M2 Task 1 gate verdict). Runs on the run-calling thread
  // (the main thread: loadNewSnapshot asserts it) and must complete before
  // the phase's begin-phase posts so every worker installs the new snapshot
  // before its first phase work (per-dispatcher FIFO). Single caller: the
  // orchestrator is the only mergeValues caller in the process.
  void applyPhaseBreakerOverrides(const PhaseResourceLimits& desired_limits);
  // FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out context halves, all
  // thread-safe reads off the run's shared state (the LiveFanOutContext
  // function set MakeLiveUpdateApplier binds):
  absl::Status liveUpdateAdmissionStatus() const;
  absl::Status bendInstantAdmissibleForUpdate(Envoy::MonotonicTime t_bend) const;
  uint32_t currentPhaseIndexForUpdate() const;
  uint32_t activeWorkerCountForUpdate() const;
  LiveKnobDelta currentPhaseScheduleDelta() const;
  bool fanOutBegin();
  void fanOutEnd();
  // The boundary-drain live-update gate, see the .cc. Set from the worker
  // dispatchers' sequencer-stop observers; cleared by runPhases.
  // FORK-ONLY (m3-drain-pair): boundary-scoped halves of the old
  // setBoundaryDrainInProgress. The worker's stop hook reports the boundary
  // index it belongs to; open ignores an index at or below the closed mark
  // (a straggler firing after runPhases closed its boundary and began the
  // successor), so the stale fire can no longer hold the live-update gate
  // open until the next boundary.
  void openBoundaryDrainGate(uint32_t boundary_index);
  void closeBoundaryDrainGate(uint32_t boundary_index);
  // Fence condition callback (the fence mutex is held).
  bool LiveUpdateFanOutsDrained() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(live_update_fence_mutex_) {
    return in_flight_fan_outs_ == 0;
  }
  // Called in shutdown() BEFORE the workers are destroyed: disables new
  // fan-outs, then waits (bounded -- appliers honor their deadlines) for the
  // in-flight ones to register out, then clears the capability registry (a
  // stale entry's sequencer is about to be gone).
  void drainLiveUpdateFanOuts();
  std::function<void(const PhaseEvent&)> phase_event_cb_;
  std::vector<PhaseBoundaryTiming> boundary_timings_;
  std::vector<uint64_t> phase_gate_exceeded_;
  // FORK-ONLY (We-Amp): M2 Task 6 worker subsets. Filled by runPhases.
  std::vector<PhaseWorkerSubset> phase_worker_subsets_;
  // Per worker (indexed like workers_), the phase indexes this worker began,
  // in begin order. The workers keep their per-phase vectors POSITIONAL over
  // the phases they began; this maps positions to phase indexes for the
  // per-phase folds (statistics, durations, gate counts) -- a
  // removed-and-rejoined worker begins a non-contiguous set.
  std::vector<std::vector<uint32_t>> worker_begun_phases_;
  std::vector<std::shared_future<void>> worker_ready_futures_;
  mutable std::mutex current_phase_id_mutex_;
  std::string current_phase_id_{"main"};
  // FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out's view of the in-flight
  // run, guarded by current_phase_id_mutex_ (the same domain the phase id
  // and the run-shape bound already live in): the current phase index (0 for
  // legacy runs), the active worker count (the M2 subset for phase-driven
  // runs, the full fleet for legacy runs), and the boundary-drain gate (set
  // around each boundary's bounded drain wait: an update arriving inside the
  // drain window is rejected -- the operating point is undefined mid-drain,
  // and a bend must never be applied across a boundary).
  uint32_t current_phase_index_{0};
  uint32_t active_worker_count_{0};
  // FORK-ONLY (m3-drain-pair): the boundary-drain live-update gate, scoped
  // by boundary index (see BoundaryDrainGate).
  BoundaryDrainGate boundary_drain_gate_;
  // FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out shutdown fence: every
  // in-flight fan-out registers itself here; the workers are destroyed only
  // after the fence drained (bounded by the applier deadline), so no
  // capability apply can be posted to a dispatcher that is going away (the
  // registry-outlives-sequencer window, closed with the registry Reset below
  // in shutdown()).
  mutable absl::Mutex live_update_fence_mutex_;
  int in_flight_fan_outs_ ABSL_GUARDED_BY(live_update_fence_mutex_) = 0;
  bool live_updates_disabled_ ABSL_GUARDED_BY(live_update_fence_mutex_) = false;
  // FORK-ONLY (We-Amp): current-phase duration and start instant, and the
  // run's start instant, for liveUpdateRunShape()'s remaining-duration bound
  // (M3 Task 2). Monotonic ns on time_system_'s clock.
  int64_t current_phase_duration_ns_{0};
  int64_t current_phase_start_monotonic_ns_{0};
  int64_t run_start_monotonic_ns_{0};
  std::mutex live_partials_mutex_;
  std::vector<nighthawk::client::WorkerLiveSnapshotPartial> live_partials_;
  // FORK-ONLY (We-Amp): running fold of the drained partials. Honors the
  // snapshot-acquisition-vs-slab-swap seam (M2 A1): statistics never fold
  // across phase stamps; counters stay cumulative. See
  // live_partial_aggregator.h.
  LivePartialAggregator live_aggregator_;
  bool runInternal(OutputCollector& collector, const UriPtr& tracing_uri,
                   const Envoy::Network::DnsResolverSharedPtr& dns_resolver,
                   const std::optional<Envoy::SystemTime>& schedule);

  /**
   * Compute the offset at which execution should start. We adhere to the scheduled start passed in
   * as an argument when specified, otherwise we need a delay that will be sufficient for all the
   * workers to get up and running.
   *
   * @param time_system Time system used to obtain the current time.
   * @param scheduled_start Optional scheduled start.
   * @param concurrency The number of workers that will be used during execution.
   * @return Envoy::MonotonicTime Time at which execution should start.
   */
  static Envoy::MonotonicTime
  computeFirstWorkerStart(Envoy::Event::TimeSystem& time_system,
                          const std::optional<Envoy::SystemTime>& scheduled_start,
                          const uint32_t concurrency);

  /**
   * We offset the start of each thread so that workers will execute tasks evenly spaced in
   * time. Let's assume we have two workers w0/w1, which should maintain a combined global pace of
   * 1000Hz. w0 and w1 both run at 500Hz, but ideally their execution is evenly spaced in time,
   * and not overlapping. Workers start offsets can be computed like
   * "worker_number*(1/global_frequency))", which would yield T0+[0ms, 1ms]. This helps reduce
   * batching/queueing effects, both initially, but also by calibrating the linear rate limiter we
   * currently have to a precise starting time, which helps later on.
   *
   * @param concurrency The number of workers that will be used during execution.
   * @param rps Anticipated requests per second during execution.
   * @return std::chrono::nanoseconds The delay that should be used as an offset between each
   * independent worker execution start.
   */
  static std::chrono::nanoseconds computeInterWorkerDelay(const uint32_t concurrency,
                                                          const uint32_t rps);

  const envoy::config::core::v3::Node node_;
  const Envoy::Protobuf::RepeatedPtrField<std::string> node_context_params_;
  const Options& options_;
  const int number_of_workers_;
  std::shared_ptr<Envoy::ProcessWide> process_wide_;
  Envoy::PlatformImpl platform_impl_;
  Envoy::Event::TimeSystem& time_system_;
  Envoy::Stats::SymbolTableImpl symbol_table_;
  Envoy::Stats::AllocatorImpl stats_allocator_;
  Envoy::ThreadLocal::InstanceImpl tls_;
  Envoy::Stats::ThreadLocalStoreImpl store_root_;
  Envoy::Stats::Scope& scope_root_{*store_root_.rootScope()};
  Envoy::Quic::QuicStatNames quic_stat_names_;
  envoy::config::bootstrap::v3::Bootstrap bootstrap_;
  Envoy::Api::ApiPtr api_;
  Envoy::Event::DispatcherPtr dispatcher_;
  std::vector<ClientWorkerPtr> workers_;
  // FORK-ONLY (We-Amp): M3 Task 3 -- per-PHASE live-update capability
  // registry, see LiveCapabilityRegistry above.
  LiveCapabilityRegistry live_capability_registry_;
  const BenchmarkClientFactoryImpl benchmark_client_factory_;
  const TerminationPredicateFactoryImpl termination_predicate_factory_;
  const SequencerFactoryImpl sequencer_factory_;
  const RequestSourceFactoryImpl request_generator_factory_;
  Envoy::Init::ManagerImpl init_manager_;
  Envoy::LocalInfo::LocalInfoPtr local_info_;
  Envoy::Random::RandomGeneratorImpl generator_;
  Envoy::Server::ConfigTrackerImpl config_tracker_;
  Envoy::Secret::SecretManagerImpl secret_manager_;
  Envoy::Http::ContextImpl http_context_;
  Envoy::Grpc::ContextImpl grpc_context_;
  Envoy::Thread::MutexBasicLockable access_log_lock_;
  Envoy::Singleton::ManagerPtr singleton_manager_;
  Envoy::AccessLog::AccessLogManagerImpl access_log_manager_;
  Envoy::Network::DnsResolverFactory& dns_resolver_factory_;
  // Config that was used to create dns_resolver_factory_. Also must be provided when calling the
  // factory to create a resolver.
  envoy::config::core::v3::TypedExtensionConfig typed_dns_resolver_config_;

  std::unique_ptr<Envoy::Extensions::TransportSockets::Tls::ContextManagerImpl>
      ssl_context_manager_;

  std::unique_ptr<ClusterManagerFactory> cluster_manager_factory_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_{};
  Envoy::Runtime::LoaderPtr runtime_loader_;
  Envoy::Init::WatcherImpl init_watcher_;
  Envoy::Tracing::TracerSharedPtr tracer_;
  Envoy::Server::ValidationAdmin admin_;
  Envoy::ProtobufMessage::ProdValidationContextImpl validation_context_;
  bool shutdown_{true};
  Envoy::Thread::MutexBasicLockable workers_lock_;
  bool cancelled_{false};
  std::unique_ptr<FlushWorkerImpl> flush_worker_;
  Envoy::Router::ContextImpl router_context_;
  Envoy::OptionsImpl envoy_options_;
  // Null server implementation used as a placeholder. Its methods should never get called
  // because Nighthawk is not a full Envoy server that performs xDS config validation.
  std::unique_ptr<Envoy::Server::Instance> server_;
  // Null server factory context implementation for the same reason as above.
  std::unique_ptr<Envoy::Server::Configuration::ServerFactoryContext> server_factory_context_;
  // The set of User Defined Output plugin factories and their corresponding configuration, used to
  // add plugin instances to each worker, and to aggregate outputs for the global result.
  std::vector<UserDefinedOutputConfigFactoryPair> user_defined_output_factories_{};
  // Tunnel Encapsulation envoy runner
  std::shared_ptr<EncapsulationSubProcessRunner> encap_runner_;
};

} // namespace Client
} // namespace Nighthawk
