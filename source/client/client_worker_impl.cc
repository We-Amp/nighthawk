#include "source/client/client_worker_impl.h"

#include "source/common/stats/symbol_table.h"

#include "source/common/cached_time_source_impl.h"
#include "source/common/phase_impl.h"
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
    std::chrono::milliseconds live_snapshot_interval)
    : WorkerImpl(api, tls, store),
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
      phase_(
          std::make_unique<PhaseImpl>("main",
                                      sequencer_factory_.create(
                                          *time_source_, *dispatcher_,
                                          [this](CompletionCallback f) -> bool {
                                            return benchmark_client_->tryStartRequest(std::move(f));
                                          },
                                          termination_predicate_factory_.create(
                                              *time_source_, *worker_number_scope_, starting_time),
                                          *worker_number_scope_, starting_time, api),
                                      true)),
      hardcoded_warmup_style_(hardcoded_warmup_style),
      live_snapshot_partial_cb_(live_snapshot_partial_cb),
      live_snapshot_interval_(live_snapshot_interval) {}

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
  benchmark_client_->setShouldMeasureLatencies(phase_->shouldMeasureLatencies());
  setupLiveSnapshotTimer();
  phase_->run();
  // Stop the live snapshot timer before we leave the run: no partials may be
  // produced after the run's end, and the worker's dispatcher is not driven
  // again in a way that would fire it, but disabling explicitly keeps that
  // invariant local and obvious.
  if (live_snapshot_timer_ != nullptr) {
    live_snapshot_timer_->disableTimer();
    live_snapshot_timer_.reset();
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

StatisticPtrMap ClientWorkerImpl::statistics() const {
  StatisticPtrMap statistics;
  StatisticPtrMap s1 = benchmark_client_->statistics();
  Sequencer& sequencer = phase_->sequencer();
  StatisticPtrMap s2 = sequencer.statistics();
  statistics.insert(s1.begin(), s1.end());
  statistics.insert(s2.begin(), s2.end());
  return statistics;
}

std::vector<nighthawk::client::UserDefinedOutput>
ClientWorkerImpl::getUserDefinedOutputResults() const {
  return benchmark_client_->getUserDefinedOutputResults();
}

} // namespace Client
} // namespace Nighthawk
