#include "source/client/client_worker_impl.h"

#include "nighthawk/common/exception.h"

#include "source/common/stats/symbol_table.h"

#include "source/common/cached_time_source_impl.h"
#include "source/common/phase_impl.h"
#include "source/common/sequencer_impl.h"
#include "source/common/termination_predicate_impl.h"
#include "source/common/utility.h"

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

ClientWorkerImpl::ClientWorkerImpl(
    Envoy::Api::Api& api, Envoy::ThreadLocal::Instance& tls,
    Envoy::Upstream::ClusterManagerPtr& cluster_manager,
    const BenchmarkClientFactory& benchmark_client_factory,
    const TerminationPredicateFactory& termination_predicate_factory,
    const SequencerFactory& sequencer_factory,
    const RequestSourceFactory& request_generator_factory, Envoy::Stats::Store& store,
    const int worker_number, const Envoy::MonotonicTime starting_time,
    Envoy::Tracing::TracerSharedPtr& tracer, const HardCodedWarmupStyle hardcoded_warmup_style,
    std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins,
    const std::function<void(nighthawk::client::WorkerLiveSnapshotPartial)>&
        live_snapshot_partial_cb,
    std::chrono::milliseconds live_snapshot_interval, bool phase_driven_run,
    std::function<void(uint32_t phase_index, Nighthawk::LiveTunableRateLimiter*,
                       Nighthawk::LiveApplyFn)>
        live_capability_cb)
    : WorkerImpl(api, tls, store), api_(api),
      time_source_(std::make_unique<CachedTimeSourceImpl>(*dispatcher_)),
      termination_predicate_factory_(termination_predicate_factory),
      sequencer_factory_(sequencer_factory), worker_scope_(store_.createScope("cluster.")),
      worker_number_scope_(worker_scope_->createScope(fmt::format("{}.", worker_number))),
      worker_number_(worker_number), tracer_(tracer),
      request_generator_(
          request_generator_factory.create(cluster_manager, *dispatcher_, *worker_number_scope_,
                                           fmt::format("{}.requestsource", worker_number))),
      benchmark_client_(benchmark_client_factory.create(
          api, *dispatcher_, *worker_number_scope_, cluster_manager, tracer_,
          fmt::format("{}", worker_number), worker_number, *request_generator_,
          std::move(user_defined_output_plugins))),
      sequencer_target_([this](CompletionCallback f) -> bool {
        return benchmark_client_->tryStartRequest(std::move(f));
      }),
      hardcoded_warmup_style_(hardcoded_warmup_style),
      live_snapshot_partial_cb_(live_snapshot_partial_cb),
      live_snapshot_interval_(live_snapshot_interval),
      live_capability_cb_(live_capability_cb), phase_driven_run_(phase_driven_run),
      ready_future_(ready_promise_.get_future()) {
  if (!phase_driven_run) {
    // FORK-ONLY (We-Amp): a legacy run's main sequencer is built here
    // (previously in the member-init list above) so its live-update
    // capability is captured exactly like the per-phase sequencers capture
    // theirs (M3 Task 3): after construction, before decoration, and
    // published through the capability callback (phase index 0).
    LiveTunableRateLimiter* captured_live_tunable = nullptr;
    auto sequencer = sequencer_factory_.create(
        *time_source_, *dispatcher_, sequencer_target_,
        termination_predicate_factory_.create(*time_source_, *worker_number_scope_, starting_time),
        *worker_number_scope_, starting_time, api, std::nullopt, &captured_live_tunable);
    SequencerImpl& sequencer_impl = static_cast<SequencerImpl&>(*sequencer);
    phase_ = std::make_unique<PhaseImpl>("main", std::move(sequencer), /*measure=*/true);
    if (live_capability_cb_) {
      SequencerImpl* sequencer_ptr = &sequencer_impl;
      live_capability_cb_(
          0, captured_live_tunable,
          [sequencer_ptr](Envoy::MonotonicTime t_bend, uint32_t new_rps,
                          std::function<void(absl::Status)> callback) {
            sequencer_ptr->applyLiveUpdate(t_bend, new_rps, std::move(callback));
          });
    }
  }
}

void ClientWorkerImpl::simpleWarmup() {
  ENVOY_LOG(debug, "> worker {}: warmup start.", worker_number_);
  if (benchmark_client_->tryStartRequest([this](bool, bool) { dispatcher_->exit(); })) {
    dispatcher_->run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  } else {
    ENVOY_LOG(warn, "> worker {}: failed to initiate warmup request.", worker_number_);
  }
  ENVOY_LOG(debug, "> worker {}: warmup done.", worker_number_);
}

void ClientWorkerImpl::work() {
  benchmark_client_->setShouldMeasureLatencies(false);
  request_generator_->initOnThread();
  if (hardcoded_warmup_style_ == HardCodedWarmupStyle::ON) {
    simpleWarmup();
  }
  if (phase_driven_run_) {
    // Multi-phase run: every phase measures latencies (fresh per-phase
    // statistics are M2 Task 4's dimension; the per-phase sequencers already
    // measure per phase).
    benchmark_client_->setShouldMeasureLatencies(true);
  } else {
    benchmark_client_->setShouldMeasureLatencies(phase_->shouldMeasureLatencies());
  }
  setupLiveSnapshotTimer();
  if (phase_driven_run_) {
    // Thread-local initialization and warmup are done: the orchestrator (the
    // run-calling thread) may now post begin-phase onto this dispatcher.
    // This thread drives the event loop until the FINAL phase's sequencer
    // stops with the legacy exit-at-stop; the boundary stops of the earlier
    // phases do not exit the dispatcher.

    // The event loop only keeps running while it has something to wait on:
    // arm a long no-op timer BEFORE entering the blocking run, otherwise the
    // loop returns immediately when no event is registered yet (the first
    // begin-phase post arrives only after this thread signals readiness).
    dispatcher_loop_timer_ = dispatcher_->createTimer([]() {});
    dispatcher_loop_timer_->enableTimer(std::chrono::hours(24));
    ready_promise_.set_value();
    dispatcher_->run(Envoy::Event::Dispatcher::RunType::Block);

  } else {
    phase_->run();
  }
  // Stop the live snapshot timer before we leave the run: no partials may be
  // produced after the run's end, and the worker's dispatcher is not driven
  // again in a way that would fire it, but disabling explicitly keeps that
  // invariant local and obvious.
  if (live_snapshot_timer_ != nullptr) {
    live_snapshot_timer_->disableTimer();
    live_snapshot_timer_.reset();
  }
  if (phase_driven_run_) {
    // Fresh-per-phase statistics (M2 A1): retire the final phase's live set
    // into its slab, on this dispatcher, so the end-of-run fold can emit
    // per-phase Results for every phase including the last one. The legacy
    // run keeps its live set untouched (the global fold reads it below via
    // statistics()).
    benchmark_client_->finalizePhaseStatistics();
  }

  // Save a final snapshot of the worker-specific counter accumulations before
  // we exit the thread.
  for (const auto& stat : store_.counters()) {
    // First, we strip the cluster prefix
    std::string stat_name = std::string(absl::StripPrefix(stat->name(), "cluster."));
    stat_name = std::string(absl::StripPrefix(stat_name, "worker."));
    // Second, we strip our own prefix if it's there, else we skip.
    const std::string worker_prefix = fmt::format("{}.", worker_number_);
    if (stat->value() && absl::StartsWith(stat_name, worker_prefix)) {
      threadLocalCounterValues_[std::string(absl::StripPrefix(stat_name, worker_prefix))] =
          stat->value();
    }
  }
  // Note that benchmark_client_ is not terminated here, but in shutdownThread() below. This is to
  // to prevent the shutdown artifacts from influencing the test result counters. The main thread
  // still needs to be able to read the counters for reporting the global numbers, and those
  // should be consistent.
}

void ClientWorkerImpl::setupLiveSnapshotTimer() {
  if (live_snapshot_partial_cb_ == nullptr ||
      live_snapshot_interval_ <= std::chrono::milliseconds(0)) {
    return;
  }
  live_snapshot_timer_ = dispatcher_->createTimer([this]() -> void {
    emitLiveSnapshotPartial();
    live_snapshot_timer_->enableTimer(live_snapshot_interval_);
  });
  live_snapshot_timer_->enableTimer(live_snapshot_interval_);
}

void ClientWorkerImpl::emitLiveSnapshotPartial() {
  if (live_snapshot_partial_cb_ == nullptr) {
    return;
  }
  nighthawk::client::WorkerLiveSnapshotPartial partial;
  partial.set_worker_id(worker_number_);
  // Stamp the partial with the phase id of the slab its statistics came from,
  // computed here on this dispatcher: the same dispatcher the benchmark
  // client swaps slabs on, so within this worker the stamp is always in sync
  // with the slab the loop above just read. The aggregator never folds
  // statistics across different stamps (snapshot-acquisition-vs-slab-swap
  // seam, M2 A1).
  partial.set_phase_id(benchmark_client_->currentPhaseId());
  // These statistics are confined to this worker's thread; we are running on
  // that very dispatcher, so reading them here is race-free without locking.
  for (const auto& statistic : statistics()) {
    nighthawk::client::StatisticSnapshot* snapshot = partial.add_statistics();
    snapshot->set_id(statistic.first);
    // Pair the percentile queries with count(): a count of 0 means there were
    // no samples yet (the percentile fields are then meaningless), and a
    // statistic implementation that cannot answer percentile queries reports
    // zeros. Consumers decide based on count.
    snapshot->set_count(statistic.second->count());
    snapshot->set_p50_ns(statistic.second->rawPercentileValueAt(50));
    snapshot->set_p90_ns(statistic.second->rawPercentileValueAt(90));
    snapshot->set_p99_ns(statistic.second->rawPercentileValueAt(99));
  }
  // Snapshot this worker's counters with the same name normalization the
  // worker applies to its final counter snapshot (see work()).
  for (const auto& stat : store_.counters()) {
    std::string stat_name = std::string(absl::StripPrefix(stat->name(), "cluster."));
    stat_name = std::string(absl::StripPrefix(stat_name, "worker."));
    const std::string worker_prefix = fmt::format("{}.", worker_number_);
    if (stat->value() && absl::StartsWith(stat_name, worker_prefix)) {
      (*partial.mutable_counters())[std::string(absl::StripPrefix(stat_name, worker_prefix))] =
          stat->value();
    }
  }
  live_snapshot_partial_cb_(std::move(partial));
}

void ClientWorkerImpl::shutdownThread() {
  benchmark_client_->terminate();
  request_generator_->destroyOnThread();
}

void ClientWorkerImpl::requestExecutionCancellation() {
  // We just bump a counter, which is watched by a static termination predicate.
  // A useful side effect is that this counter will propagate to the output, which leaves
  // a note about that execution was subject to cancellation.
  dispatcher_->post(
      [this]() { worker_number_scope_->counterFromString("graceful_stop_requested").inc(); });
}

const Phase& ClientWorkerImpl::phase() const {
  // Legacy runs keep their eager "main" phase; a phase-driven run reports the
  // phase begun most recently (the run's final phase once the workers have
  // been joined).
  if (phase_ != nullptr) {
    return *phase_;
  }
  if (current_phase_ != nullptr) {
    return *current_phase_;
  }
  throw NighthawkException("phase() called before any phase began on this worker");
}

std::future<PhaseDrainAck> ClientWorkerImpl::beginPhaseAsync(
    uint32_t phase_index, const nighthawk::client::PhaseSpec& phase_spec, bool final_phase) {
  drain_promise_ = std::make_shared<std::promise<PhaseDrainAck>>();
  std::future<PhaseDrainAck> future = drain_promise_->get_future();
  dispatcher_->post([this, phase_index, phase_spec, final_phase]() {
    beginPhase(phase_index, phase_spec, final_phase);
  });
  return future;
}

void ClientWorkerImpl::goIdleAsync(bool exit_dispatcher) {
  dispatcher_->post([this, exit_dispatcher]() { goIdle(exit_dispatcher); });
}

void ClientWorkerImpl::goIdle(bool exit_dispatcher) {
  if (drain_watch_ != nullptr) {
    drain_watch_->cancel();
    drain_watch_.reset();
  }
  // The fold (M2 Task 6): one final live partial carrying this worker's
  // frozen cumulative values, then silence -- no post-fold partials, so the
  // live aggregation cannot double-count the worker after its removal. (A
  // later beginPhase restarts the timer: a rejoined worker emits again,
  // stamped with its new phase.)
  emitLiveSnapshotPartial();
  if (live_snapshot_timer_ != nullptr) {
    live_snapshot_timer_->disableTimer();
    live_snapshot_timer_.reset();
  }
  ENVOY_LOG(debug, "worker {}: idle after its last phase (exit: {})", worker_number_,
            exit_dispatcher);
  if (exit_dispatcher) {
    // The worker sits out the rest of the run: leave the blocking dispatcher
    // loop in work() so the run's join sees this worker complete.
    dispatcher_->exit();
  }
}

void ClientWorkerImpl::beginPhase(uint32_t phase_index,
                                  const nighthawk::client::PhaseSpec& phase_spec,
                                  bool final_phase) {
  // Phase-start ruling (M2 Task 6): phase begins are SAME-INSTANT across the
  // active subset, including workers joining at this phase. The legacy run
  // staggers the per-worker sequencer start anchors at BOOTSTRAP
  // (computeInterWorkerDelay, process_impl.cc); a phase boundary is not a
  // bootstrap -- every joining sequencer takes its start from its own
  // dispatcher's now, which lands within microseconds of the survivors'
  // (the begin posts are issued back-to-back from the orchestrator thread).
  // Documented as the explicit alternative to staggering subset starts.
  // Cancel the drain watch of the previous boundary (e.g. a censored drain
  // whose hung request never completed). The old watch holds its own
  // boundary's promise by value, so even a firing that slipped through
  // could only complete that boundary's ack, never this phase's.
  if (drain_watch_ != nullptr) {
    drain_watch_->cancel();
    drain_watch_.reset();
  }
  // A worker rejoining after goIdle had its live snapshot timer stopped (the
  // fold's "no post-fold partials" rule): resume it so this phase's partials
  // flow again. Workers that never idled own the timer work() created.
  if (live_snapshot_timer_ == nullptr) {
    setupLiveSnapshotTimer();
  }
  // FORK-ONLY (We-Amp): per-phase in-flight gates (M2 Task 5). The posted
  // spec carries the phase's effective values (declared per-phase values,
  // run-level values inherited when a field was absent); the clusters were
  // bootstrapped once at the max across phases and are never touched again,
  // so these client-side gates are what realize the per-phase ceiling. A
  // start they refuse is shed and counted in tryStartRequest, never queued.
  // The refusal-count snapshot lands before any of this phase's work runs
  // (every prior phase's work completed at its boundary drain).
  phase_gate_exceeded_starts_.push_back(benchmark_client_->gateExceededCount());
  if (phase_spec.has_connections()) {
    benchmark_client_->setConnectionLimit(phase_spec.connections().value());
  }
  if (phase_spec.has_max_pending_requests()) {
    benchmark_client_->setMaxPendingRequests(phase_spec.max_pending_requests().value());
  }
  if (phase_spec.has_max_active_requests()) {
    benchmark_client_->setMaxActiveRequests(phase_spec.max_active_requests().value());
  }
  const Envoy::MonotonicTime phase_start = time_source_->monotonicTime();
  auto termination_predicate = termination_predicate_factory_.create(
      *time_source_, *worker_number_scope_, phase_start,
      std::chrono::seconds(phase_spec.duration().seconds()));
  // FORK-ONLY (We-Amp): M3 Task 3 -- the fresh per-phase sequencer's inner
  // limiter is captured BEFORE decoration and published to the process-side
  // per-phase registry through the capability callback: at each begin-phase
  // the previous phase's stale handle is replaced by this phase's (the
  // registry ruling). Runs on this worker's dispatcher.
  LiveTunableRateLimiter* captured_live_tunable = nullptr;
  auto sequencer =
      sequencer_factory_.create(*time_source_, *dispatcher_, sequencer_target_,
                                std::move(termination_predicate), *worker_number_scope_,
                                phase_start, api_, phase_spec.requests_per_second(),
                                &captured_live_tunable);
  SequencerImpl& sequencer_impl = static_cast<SequencerImpl&>(*sequencer);
  if (live_capability_cb_) {
    SequencerImpl* sequencer_ptr = &sequencer_impl;
    live_capability_cb_(
        phase_index, captured_live_tunable,
        [sequencer_ptr](Envoy::MonotonicTime t_bend, uint32_t new_rps,
                        std::function<void(absl::Status)> callback) {
          sequencer_ptr->applyLiveUpdate(t_bend, new_rps, std::move(callback));
        });
  }
  // The sequencer factory only ever produces SequencerImpl instances; the
  // multi-phase stop-mode surface is SequencerImpl-only (the Sequencer
  // interface is untouched).
  auto phase =
      std::make_unique<PhaseImpl>(phase_spec.name(), std::move(sequencer), /*measure=*/true);
  current_phase_ = phase.get();
  phases_history_.push_back(std::move(phase));
  phase_statistics_.push_back(sequencer_impl.statistics());
  // The per-phase vectors are POSITIONAL over the phases this worker began
  // (a removed-and-rejoined worker begins a non-contiguous set, M2 Task 6);
  // the process maps positions to phase indexes via its own begin
  // bookkeeping. The stop hook writes through the slot, never the phase
  // index.
  const size_t duration_slot = phase_execution_durations_.size();
  phase_execution_durations_.push_back(0);
  sequencer_impl.setExitDispatcherOnStop(final_phase);
  sequencer_impl.setStopHook([this, final_phase, duration_slot](bool /*timed_out*/) {
    // On this dispatcher, inside the sequencer's stop.
    dispatcher_->updateApproximateMonotonicTime();
    phase_execution_durations_[duration_slot] =
        static_cast<SequencerImpl&>(current_phase_->sequencer()).executionDuration().count();
    if (final_phase) {
      // The final phase keeps today's exact behavior: the dispatcher exits
      // and work()'s event loop returns into the worker teardown.
      return;
    }
    startDrainWatch(time_source_->monotonicTime().time_since_epoch().count());
  });
  ENVOY_LOG(debug, "worker {}: begin phase {} ('{}'), final: {}", worker_number_, phase_index,
            phase_spec.name(), final_phase);
  // Fresh-per-phase statistics (M2 A1): retire the previous phase's
  // benchmark-client statistic set into its slab and install this phase's
  // fresh set before the sequencer starts the first request of the phase.
  // On this dispatcher, so the swap is serialized with request starts,
  // completions and live partials.
  benchmark_client_->beginPhaseStatistics(phase_spec.name());
  sequencer_impl.start();

}

void ClientWorkerImpl::startDrainWatch(const int64_t stop_monotonic_ns) {
  // The drain observable is the benchmark client's own in-flight tracking
  // (the same accounting the client-side gates consult). The promise is
  // copied into the watch here, ON the worker dispatcher, so the watch owns
  // this boundary's ack no matter when it fires (reading the member from a
  // re-armed timer would race with the orchestrator thread swapping it for
  // the next boundary's promise).
  drain_watch_ = std::make_unique<BoundaryDrainWatch>(
      *dispatcher_, [this]() { return benchmark_client_->inFlightRequests(); }, drain_promise_,
      stop_monotonic_ns);
  drain_watch_->start();
}

std::vector<uint64_t> ClientWorkerImpl::phaseGateExceededCounts() const {
  // Valid to read after the workers have been joined; the cumulative counter
  // only moves on this worker's dispatcher while a phase's sequencer runs.
  const uint64_t current = benchmark_client_->gateExceededCount();
  std::vector<uint64_t> counts;
  counts.reserve(phase_gate_exceeded_starts_.size());
  for (size_t i = 0; i < phase_gate_exceeded_starts_.size(); ++i) {
    const uint64_t phase_end =
        i + 1 < phase_gate_exceeded_starts_.size() ? phase_gate_exceeded_starts_[i + 1] : current;
    counts.push_back(phase_end >= phase_gate_exceeded_starts_[i]
                         ? phase_end - phase_gate_exceeded_starts_[i]
                         : 0);
  }
  return counts;
}

StatisticPtrMap ClientWorkerImpl::statistics() const {
  StatisticPtrMap statistics;
  StatisticPtrMap s1 = benchmark_client_->statistics();
  statistics.insert(s1.begin(), s1.end());
  const Phase* phase = phase_ != nullptr ? phase_.get() : current_phase_;
  if (phase != nullptr) {
    StatisticPtrMap s2 = phase->sequencer().statistics();
    statistics.insert(s2.begin(), s2.end());
  }
  return statistics;
}

std::vector<nighthawk::client::UserDefinedOutput>
ClientWorkerImpl::getUserDefinedOutputResults() const {
  return benchmark_client_->getUserDefinedOutputResults();
}

} // namespace Client
} // namespace Nighthawk
