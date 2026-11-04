#pragma once

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
#include "nighthawk/common/statistic.h"

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
/**
 * Only a single instance is allowed at a time machine-wide in this implementation.
 * Running multiple instances at the same might introduce noise into the measurements.
 * If there turns out to be a desire to run multiple instances at the same time, we could
 * introduce a --lock-name option. Note that multiple instances in the same process may
 * be problematic because of Envoy enforcing a single runtime instance.
 */
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
  void setCurrentPhaseId(absl::string_view phase_id);
  std::string currentPhaseId() const;
  std::function<void(const PhaseEvent&)> phase_event_cb_;
  std::vector<PhaseBoundaryTiming> boundary_timings_;
  std::vector<std::shared_future<void>> worker_ready_futures_;
  mutable std::mutex current_phase_id_mutex_;
  std::string current_phase_id_{"main"};
  std::mutex live_partials_mutex_;
  std::vector<nighthawk::client::WorkerLiveSnapshotPartial> live_partials_;
  // Running latest per-worker counter values and statistic summaries. Each
  // worker's partials are cumulative since run start, so the latest value per
  // worker wins; the aggregated snapshot sums the per-worker latests over the
  // workers (and takes the percentile maximum, see toLiveSnapshot()). This
  // keeps snapshots cumulative from run start even when the collector drains
  // partials one interval at a time.
  absl::flat_hash_map<uint32_t, absl::flat_hash_map<std::string, uint64_t>>
      live_worker_counter_values_;
  absl::flat_hash_map<uint32_t,
                      absl::flat_hash_map<std::string, nighthawk::client::StatisticSnapshot>>
      live_worker_statistics_;
  // Enforces strictly increasing monotonic_ns across snapshots.
  int64_t last_live_snapshot_monotonic_ns_{0};
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
