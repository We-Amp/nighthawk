#include "source/client/process_impl.h"

#include "api/rate_limiter/linear_ramping_rate_limiter.pb.h"

#include <sys/file.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <random>

#include "envoy/common/optref.h"
#include "envoy/http/http_server_properties_cache.h"
#include "envoy/network/address.h"
#include "envoy/server/filter_config.h"
#include "envoy/stats/sink.h"
#include "envoy/stats/store.h"

#include "nighthawk/client/output_collector.h"
#include "nighthawk/common/factories.h"
#include "nighthawk/user_defined_output/user_defined_output_plugin.h"

#include "envoy/config/core/v3/resolver.pb.h"
#include "envoy/config/xds_manager.h"
#include "source/common/api/api_impl.h"
#include "source/common/common/cleanup.h"
#include "source/common/common/regex.h"
#include "source/common/common/statusor.h"
#include "source/common/common/utility.h"
#include "source/common/config/utility.h"
#include "source/common/config/xds_manager_impl.h"
#include "source/common/event/dispatcher_impl.h"
#include "source/common/event/real_time_system.h"
#include "source/common/http/http_server_properties_cache_manager_impl.h"
#include "source/common/init/manager_impl.h"
#include "source/common/local_info/local_info_impl.h"
#include "source/common/network/dns_resolver/dns_factory_util.h"
#include "source/common/network/utility.h"
#include "source/common/protobuf/protobuf.h"
#include "source/common/runtime/runtime_impl.h"
#include "source/common/singleton/manager_impl.h"
#include "source/common/stats/tag_producer_impl.h"
#include "source/common/thread_local/thread_local_impl.h"
#include "source/exe/main_common.h"
#include "source/server/null_overload_manager.h"
#include "source/server/server.h"

#include "source/client/process_bootstrap.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/numbers.h"

// TODO(oschaaf): See if we can leverage a static module registration like Envoy does to avoid the
// ifdefs in this file.
#ifdef ZIPKIN_ENABLED
#include "source/extensions/tracers/zipkin/zipkin_tracer_impl.h"
#endif
#include "source/server/options_impl.h"
#include "source/server/options_impl_platform.h"

#include "api/client/options.pb.h"
#include "api/client/output.pb.h"

#include "source/common/frequency.h"
#include "source/common/uri_impl.h"
#include "source/common/utility.h"

#include "source/client/benchmark_client_impl.h"
#include "source/client/client.h"
#include "source/client/client_worker_impl.h"
#include "source/client/factories_impl.h"
#include "source/client/options_impl.h"
#include "source/client/sni_utility.h"

#include "source/user_defined_output/user_defined_output_plugin_creator.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

using ::envoy::config::bootstrap::v3::Bootstrap;
using ::envoy::config::core::v3::TypedExtensionConfig;

// FORK-ONLY (We-Amp): folds one statistic into an id-keyed map of combined
// per-phase statistics (M2 Task 4's per-phase output fold). Clones on first
// sight so the map owns an instance of the right backend to combine into.
void combineStatisticIntoMap(std::map<std::string, StatisticPtr>& merged, absl::string_view id,
                             const Statistic& statistic) {
  auto it = merged.find(std::string(id));
  if (it == merged.end()) {
    auto clone = statistic.createNewInstanceOfSameType()->combine(statistic);
    clone->setId(id);
    merged.emplace(std::string(id), std::move(clone));
  } else {
    auto combined = it->second->combine(statistic);
    combined->setId(id);
    it->second = std::move(combined);
  }
}

// FORK-ONLY (We-Amp): per-worker pool health surfaced from the engine's own
// pool counters into PhaseMetadata (M2 Task 4, spec 8.6: the multi-phase run
// surfaces what its no-churn acceptance asserts). Created is the cluster's
// cumulative upstream_cx_total counter; active is the upstream_cx_active
// gauge, read after the workers have been joined.
nighthawk::client::WorkerPoolHealth readWorkerPoolHealth(Envoy::Stats::Store& store,
                                                         uint32_t worker_id) {
  nighthawk::client::WorkerPoolHealth health;
  health.set_worker_id(worker_id);
  const std::string prefix = fmt::format("cluster.{}.", worker_id);
  const std::string created_name = prefix + "upstream_cx_total";
  const std::string active_name = prefix + "upstream_cx_active";
  for (const auto& counter : store.counters()) {
    if (counter->name() == created_name) {
      health.set_connections_created(counter->value());
    }
  }
  for (const auto& gauge : store.gauges()) {
    if (gauge->name() == active_name) {
      health.set_connections_active(gauge->value());
    }
  }
  return health;
}

// Helps in generating a bootstrap for the process.
// This is a class only to allow the use of the ENVOY_LOG macros.
class BootstrapFactory : public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  // Determines the concurrency Nighthawk should use based on configuration
  // (options) and the available machine resources.
  static uint32_t determineConcurrency(const Options& options) {
    // FORK-ONLY (We-Amp): with a phase vector the top-level concurrency is
    // rejected by validation, so the worker count is the maximum concurrency
    // declared over the phases. (M2 Task 6: this bootstraps the full fleet --
    // phases activate the declared prefix subset per phase and retire the
    // rest; see runPhases / computeWorkerSubsets.)
    if (options.hasPhaseVector()) {
      uint32_t declared_max = 0;
      bool any_declared = false;
      for (const nighthawk::client::PhaseSpec& phase : options.phases()) {
        uint32_t value = 0;
        if (phase.has_concurrency() && absl::SimpleAtoi(phase.concurrency().value(), &value) &&
            value > 0) {
          declared_max = std::max(declared_max, value);
          any_declared = true;
        }
      }
      if (any_declared) {
        ENVOY_LOG(info, "Starting {} threads / event loops (max over phase concurrencies).",
                  declared_max);
        return declared_max;
      }
      // No phase declares a concurrency: fall through to the legacy
      // determination below.
    }
    bool autoscale = options.concurrency() == "auto";
    uint32_t concurrency;
    if (autoscale) {
      uint32_t cpu_cores_with_affinity = Envoy::OptionsImplPlatform::getCpuCount();
      ENVOY_LOG(info, "Detected {} (v)CPUs with affinity..", cpu_cores_with_affinity);
      concurrency = cpu_cores_with_affinity;
    } else {
      // TODO(oschaaf): Maybe, in the case where the concurrency flag is left out, but
      // affinity is set / we don't have affinity with all cores, we should default to autoscale.
      // (e.g. we are called via taskset).
      concurrency = std::stoi(options.concurrency());
    }
    std::string duration_as_string =
        options.noDuration() ? "No time limit"
                             : fmt::format("Time limit: {} seconds", options.duration().count());
    ENVOY_LOG(info, "Starting {} threads / event loops. {}.", concurrency, duration_as_string);
    ENVOY_LOG(info, "Global targets: {} connections and {} calls per second.",
              options.connections() * concurrency, options.requestsPerSecond() * concurrency);

    if (concurrency > 1) {
      ENVOY_LOG(info, "   (Per-worker targets: {} connections and {} calls per second)",
                options.connections(), options.requestsPerSecond());
    }

    return concurrency;
  }
};

// Implementation of dummy StatsConfig.
class StatsConfigImpl : public Envoy::Server::Configuration::StatsConfig {
public:
  StatsConfigImpl() : flush_interval_(std::chrono::seconds(5)) {};

  const std::list<Envoy::Stats::SinkPtr>& sinks() const override { return sinks_; }
  std::chrono::milliseconds flushInterval() const override { return flush_interval_; }
  bool flushOnAdmin() const override { return false; }

  void addSink(Envoy::Stats::SinkPtr sink) { sinks_.emplace_back(std::move(sink)); }

  bool enableDeferredCreationStats() const override { return false; }

  uint32_t evictOnFlush() const override { return 0; }

  const std::string& evictionIntervalRuntimeKey() const override {
    return eviction_interval_runtime_key_;
  }

private:
  std::list<Envoy::Stats::SinkPtr> sinks_;
  const std::chrono::milliseconds flush_interval_;
  const std::string eviction_interval_runtime_key_;
};

// A fake ServerLifecycleNotifier. Because it does nothing, it's safe to just create
// multiple instances of it, rather than manage the lifetime of a single one.
class NighthawkLifecycleNotifierImpl : public Envoy::Server::ServerLifecycleNotifier {
public:
  HandlePtr registerCallback(Stage, StageCallback) override {
    PANIC(
        "NighthawkLifecycleNotifierImpl::registerCallbacki(Stage, StageCallback) not implemented");
  }
  HandlePtr registerCallback(Stage, StageCallbackWithCompletion) override {
    PANIC("NighthawkLifecycleNotifierImpl::registerCallback(Stage, StageCallbackWithCompletion) "
          "not implemented");
  }
};

// Implementation of Envoy::Server::Configuration::ServerFactoryContext.
class NighthawkServerFactoryContext : public Envoy::Server::Configuration::ServerFactoryContext {
public:
  explicit NighthawkServerFactoryContext(Envoy::Server::Instance& server)
      : server_(server), server_scope_(server_.stats().createScope("")) {}

  const Envoy::Server::Options& options() override { return server_.options(); };

  Envoy::Event::Dispatcher& mainThreadDispatcher() override { return server_.dispatcher(); }

  Envoy::Api::Api& api() override { return server_.api(); }

  Envoy::LocalInfo::LocalInfo& localInfo() const override { return server_.localInfo(); }

  Envoy::OptRef<Envoy::Server::Admin> admin() override { return server_.admin(); }

  Envoy::Runtime::Loader& runtime() override { return server_.runtime(); }

  Envoy::Singleton::Manager& singletonManager() override { return server_.singletonManager(); }

  Envoy::ProtobufMessage::ValidationVisitor& messageValidationVisitor() override {
    return Envoy::ProtobufMessage::getStrictValidationVisitor();
  };

  Envoy::Stats::Scope& scope() override { return *server_scope_; };

  Envoy::Stats::Scope& serverScope() override { return *server_scope_; };

  Envoy::ThreadLocal::Instance& threadLocal() override { return server_.threadLocal(); }

  Envoy::Upstream::ClusterManager& clusterManager() override {
    if (cluster_manager_ != nullptr) {
      return *cluster_manager_;
    }
    PANIC("NighthawkServerFactoryContext::clusterManager not implemented");
  };

  Envoy::Config::XdsManager& xdsManager() override { return server_.xdsManager(); };

  Envoy::Http::HttpServerPropertiesCacheManager& httpServerPropertiesCacheManager() override {
    return server_.httpServerPropertiesCacheManager();
  }

  Envoy::ProtobufMessage::ValidationContext& messageValidationContext() override {
    return server_.messageValidationContext();
  };

  Envoy::TimeSource& timeSource() override { return api().timeSource(); };

  Envoy::AccessLog::AccessLogManager& accessLogManager() override {
    return server_.accessLogManager();
  }

  Envoy::Server::ServerLifecycleNotifier& lifecycleNotifier() override {
    return lifecycle_notifier_;
  }

  Envoy::Regex::Engine& regexEngine() override { return regex_engine_; }

  Envoy::Init::Manager& initManager() override { return server_.initManager(); };

  Envoy::Grpc::Context& grpcContext() override { return server_.grpcContext(); };

  Envoy::Router::Context& routerContext() override { return server_.routerContext(); };

  Envoy::ProcessContextOptRef processContext() override {
    PANIC("NighthawkServerFactoryContext::processContext not implemented");
  }

  Envoy::Server::DrainManager& drainManager() override {
    PANIC("NighthawkServerFactoryContext::drainManager not implemented");
  };

  Envoy::Ssl::ContextManager& sslContextManager() override {
    if (ssl_context_manager_ != nullptr) {
      return *ssl_context_manager_;
    }
    PANIC("NighthawkServerFactoryContext::sslContextManager not implemented");
  }

  Envoy::Secret::SecretManager& secretManager() override { return server_.secretManager(); }

  Envoy::Server::Configuration::StatsConfig& statsConfig() override { return stats_config_; }

  envoy::config::bootstrap::v3::Bootstrap& bootstrap() override {
    PANIC("NighthawkServerFactoryContext::bootstrap not implemented");
  }

  Envoy::Http::Context& httpContext() override { return server_.httpContext(); }

  Envoy::Server::OverloadManager& overloadManager() override { return server_.overloadManager(); }

  Envoy::Server::OverloadManager& nullOverloadManager() override {
    PANIC("NighthawkServerFactoryContext::nullOverloadManager not implemented");
  }

  bool healthCheckFailed() const override {
    PANIC("NighthawkServerFactoryContext::healthCheckFailed not implemented");
  }

  void setClusterManager(Envoy::Upstream::ClusterManager& cluster_manager) {
    cluster_manager_ = &cluster_manager;
  }

  void setSslContextManager(Envoy::Ssl::ContextManager& ssl_context_manager) {
    ssl_context_manager_ = &ssl_context_manager;
  }

private:
  Envoy::Ssl::ContextManager* ssl_context_manager_ = nullptr;
  Envoy::Upstream::ClusterManager* cluster_manager_ = nullptr;
  Envoy::Server::Instance& server_;
  Envoy::Stats::ScopeSharedPtr server_scope_;
  StatsConfigImpl stats_config_;                      // Using the object created here.
  NighthawkLifecycleNotifierImpl lifecycle_notifier_; // A no-op object that lives here.
  Envoy::Regex::GoogleReEngine regex_engine_;         // Using the object created here.
};

// Implementation of Envoy::Server::Instance. Only methods used by Envoy's code
// when Nighthawk is running are implemented.
class NighthawkServerInstance : public Envoy::Server::Instance {
public:
  NighthawkServerInstance(Envoy::OptRef<Envoy::Server::Admin> admin, Envoy::Api::Api& api,
                          Envoy::Init::Manager& init_manager, Envoy::Event::Dispatcher& dispatcher,
                          Envoy::AccessLog::AccessLogManager& log_manager,
                          Envoy::Server::Options& options, Envoy::Runtime::Loader& runtime,
                          Envoy::Singleton::Manager& singleton_manager,
                          Envoy::ThreadLocal::Instance& tls,
                          Envoy::LocalInfo::LocalInfo& local_info,
                          Envoy::ProtobufMessage::ProdValidationContextImpl& validation_context,
                          Envoy::Grpc::Context& grpc_context, Envoy::Http::Context& http_context,
                          Envoy::Router::Context& router_context, Envoy::Stats::StoreRoot& store,
                          Envoy::Secret::SecretManagerImpl& secret_manager)
      : admin_(admin), api_(api), init_manager_(init_manager), dispatcher_(dispatcher),
        log_manager_(log_manager), options_(options), runtime_(runtime),
        singleton_manager_(singleton_manager), stats_store_(store), tls_(tls),
        local_info_(local_info), validation_context_(validation_context),
        grpc_context_(grpc_context), http_context_(http_context), router_context_(router_context),
        server_factory_context_(*this),
        http_server_properties_cache_manager_(
            server_factory_context_, Envoy::ProtobufMessage::getStrictValidationVisitor(), tls),
        xds_manager_(dispatcher, api, store, local_info, validation_context_, *this),
        secret_manager_(secret_manager),
        null_overload_manager_(std::make_unique<Envoy::Server::NullOverloadManager>(tls, false)) {}

  void run() override { PANIC("NighthawkServerInstance::run not implemented"); }
  Envoy::OptRef<Envoy::Server::Admin> admin() override { return admin_; }
  Envoy::Api::Api& api() override { return api_; }
  Envoy::Upstream::ClusterManager& clusterManager() override {
    PANIC("NighthawkServerInstance::clusterManager not implemented");
  }
  Envoy::Http::HttpServerPropertiesCacheManager& httpServerPropertiesCacheManager() override {
    return http_server_properties_cache_manager_;
  }
  const Envoy::Upstream::ClusterManager& clusterManager() const override {
    PANIC("NighthawkServerInstance::clusterManager not implemented");
  }
  Envoy::Ssl::ContextManager& sslContextManager() override {
    PANIC("NighthawkServerInstance::sslContextManager not implemented");
  }
  Envoy::Event::Dispatcher& dispatcher() override { return dispatcher_; }
  Envoy::Network::DnsResolverSharedPtr dnsResolver() override {
    PANIC("NighthawkServerInstance::dnsResolver not implemented");
  }
  void drainListeners(Envoy::OptRef<const Envoy::Network::ExtraShutdownListenerOptions>) override {
    PANIC("NighthawkServerInstance::drainListeners not implemented");
  }
  Envoy::Server::DrainManager& drainManager() override {
    PANIC("NighthawkServerInstance::drainManager not implemented");
  }
  Envoy::AccessLog::AccessLogManager& accessLogManager() override { return log_manager_; }
  void failHealthcheck(bool) override {
    PANIC("NighthawkServerInstance::failHealthcheck not implemented");
  }
  bool healthCheckFailed() override {
    PANIC("NighthawkServerInstance::healthCheckFailed not implemented");
  }
  Envoy::Server::HotRestart& hotRestart() override {
    PANIC("NighthawkServerInstance::hotRestart not implemented");
  }
  Envoy::Init::Manager& initManager() override { return init_manager_; }
  Envoy::Server::ListenerManager& listenerManager() override {
    PANIC("NighthawkServerInstance::listenerManager not implemented");
  }
  Envoy::MutexTracer* mutexTracer() override {
    PANIC("NighthawkServerInstance::mutexTracer not implemented");
  }
  Envoy::Server::OverloadManager& overloadManager() override { return *null_overload_manager_; }
  Envoy::Server::OverloadManager& nullOverloadManager() override { return *null_overload_manager_; }
  Envoy::Secret::SecretManager& secretManager() override { return secret_manager_; }
  const Envoy::Server::Options& options() override { return options_; }
  Envoy::Runtime::Loader& runtime() override { return runtime_; }
  Envoy::Server::ServerLifecycleNotifier& lifecycleNotifier() override {
    return lifecycle_notifier_;
  }
  void shutdown() override { PANIC("NighthawkServerInstance::shutdown not implemented"); }
  bool isShutdown() override { PANIC("NighthawkServerInstance::isShutdown not implemented"); }
  void shutdownAdmin() override { PANIC("NighthawkServerInstance::shutdownAdmin not implemented"); }
  Envoy::Singleton::Manager& singletonManager() override { return singleton_manager_; }
  time_t startTimeCurrentEpoch() override {
    PANIC("NighthawkServerInstance::startTimeCurrentEpoch not implemented");
  }
  time_t startTimeFirstEpoch() override {
    PANIC("NighthawkServerInstance::startTimeFirstEpoch not implemented");
  }
  Envoy::Stats::Store& stats() override { return stats_store_; }
  Envoy::Grpc::Context& grpcContext() override { return grpc_context_; }
  Envoy::Http::Context& httpContext() override { return http_context_; }
  Envoy::Router::Context& routerContext() override { return router_context_; }
  Envoy::ProcessContextOptRef processContext() override {
    PANIC("NighthawkServerInstance::processContext not implemented");
  }
  Envoy::ThreadLocal::Instance& threadLocal() override { return tls_; }
  Envoy::LocalInfo::LocalInfo& localInfo() const override { return local_info_; }
  Envoy::TimeSource& timeSource() override { return api_.timeSource(); }
  void flushStats() override { PANIC("NighthawkServerInstance::flushStats not implemented"); }
  Envoy::ProtobufMessage::ValidationContext& messageValidationContext() override {
    return validation_context_;
  }
  Envoy::ProtobufMessage::ValidationVisitor& messageValidationVisitor() override {
    return validation_context_.staticValidationVisitor();
  }
  Envoy::Server::Configuration::StatsConfig& statsConfig() override {
    PANIC("NighthawkServerInstance::statsConfig not implemented");
  }
  envoy::config::bootstrap::v3::Bootstrap& bootstrap() override {
    PANIC("NighthawkServerInstance::bootstrap not implemented");
  }
  Envoy::Server::Configuration::ServerFactoryContext& serverFactoryContext() override {
    return server_factory_context_;
  }
  Envoy::Server::Configuration::TransportSocketFactoryContext&
  transportSocketFactoryContext() override {
    PANIC("NighthawkServerInstance::transportSocketFactoryContext not implemented");
  }
  void setDefaultTracingConfig(const envoy::config::trace::v3::Tracing&) override {
    PANIC("NighthawkServerInstance::setDefaultTracingConfig not implemented");
  }
  bool enableReusePortDefault() override {
    PANIC("NighthawkServerInstance::enableReusePortDefault not implemented");
  }
  void setSinkPredicates(std::unique_ptr<Envoy::Stats::SinkPredicates>&&) override {
    PANIC("NighthawkServerInstance::setSinkPredicates not implemented");
  }
  Envoy::Config::XdsManager& xdsManager() override { return xds_manager_; }
  Envoy::Regex::Engine& regexEngine() override {
    PANIC("NighthawkServerInstance::regexEngine not implemented");
  };

private:
  Envoy::OptRef<Envoy::Server::Admin> admin_;
  Envoy::Api::Api& api_;
  Envoy::Init::Manager& init_manager_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::AccessLog::AccessLogManager& log_manager_;
  Envoy::Server::Options& options_;
  Envoy::Runtime::Loader& runtime_;
  Envoy::Singleton::Manager& singleton_manager_;
  Envoy::Stats::StoreRoot& stats_store_;
  Envoy::ThreadLocal::Instance& tls_;
  Envoy::LocalInfo::LocalInfo& local_info_;
  Envoy::ProtobufMessage::ProdValidationContextImpl& validation_context_;
  Envoy::Grpc::Context& grpc_context_;
  Envoy::Http::Context& http_context_;
  Envoy::Router::Context& router_context_;
  NighthawkServerFactoryContext server_factory_context_;
  Envoy::Http::HttpServerPropertiesCacheManagerImpl http_server_properties_cache_manager_;
  Envoy::Config::XdsManagerImpl xds_manager_;
  NighthawkLifecycleNotifierImpl lifecycle_notifier_; // A no-op object that lives here.
  Envoy::Secret::SecretManagerImpl& secret_manager_;
  std::unique_ptr<Envoy::Server::OverloadManager>
      null_overload_manager_; // Created in the constructor.
};

/**
 * Compiles a list of factories and the configurations they will use to create plugins.
 *
 * @param options The options used for initializing the process
 * @return std::vector<std::pair<TypedExtensionConfig, UserDefinedOutputPluginFactory*>> vector of
 * pairs, each containing a factory and its corresponding configuration.
 */
std::vector<UserDefinedOutputConfigFactoryPair>
getUserDefinedFactoryConfigPairs(const Options& options) {
  std::vector<UserDefinedOutputConfigFactoryPair> factory_config_pairs;
  for (const TypedExtensionConfig& config : options.userDefinedOutputPluginConfigs()) {
    auto* factory = Envoy::Config::Utility::getAndCheckFactory<UserDefinedOutputPluginFactory>(
        config, /*is_optional=*/false);
    UserDefinedOutputConfigFactoryPair pair(config, factory);
    factory_config_pairs.push_back(pair);
  }
  return factory_config_pairs;
}

/**
 * Takes in a single worker's user defined outputs and collects them into the
 * user_defined_outputs_by_plugin map. When called for each worker, transforms the outputs from
 * being mapped by Worker to being mapped by Plugin.
 *
 * @param user_defined_outputs_by_plugin The map that this function will collect the worker data
 * into, maps plugin name to the set of results for that plugin.
 * @param worker_user_defined_outputs The per worker results to collect and organize.
 */
void collectUserDefinedOutputs(
    absl::flat_hash_map<std::string, std::vector<nighthawk::client::UserDefinedOutput>>&
        user_defined_outputs_by_plugin,
    const std::vector<nighthawk::client::UserDefinedOutput>& worker_user_defined_outputs) {
  for (const nighthawk::client::UserDefinedOutput& user_defined_output :
       worker_user_defined_outputs) {
    absl::string_view plugin_name = user_defined_output.plugin_name();
    if (!user_defined_outputs_by_plugin.contains(plugin_name)) {
      user_defined_outputs_by_plugin[plugin_name] = {user_defined_output};
    } else {
      user_defined_outputs_by_plugin[plugin_name].push_back(user_defined_output);
    }
  }
}

/**
 * For each provided user defined output plugin factory, aggregates all of its corresponding results
 * into a global user defined output.
 *
 * @param user_defined_outputs_by_plugin A map of plugin name to the set of collected results for
 * that plugin.
 * @param user_defined_output_factories A vector of the plugin factories used in this execution
 * process.
 * @return std::vector<nighthawk::client::UserDefinedOutput> The aggregated global results for each
 * plugin.
 */
std::vector<nighthawk::client::UserDefinedOutput> compileGlobalUserDefinedPluginOutputs(
    const absl::flat_hash_map<std::string, std::vector<nighthawk::client::UserDefinedOutput>>&
        user_defined_outputs_by_plugin,
    const std::vector<UserDefinedOutputConfigFactoryPair>& user_defined_output_factories) {
  std::vector<nighthawk::client::UserDefinedOutput> global_outputs;
  for (const UserDefinedOutputConfigFactoryPair& config_factory_pair :
       user_defined_output_factories) {
    UserDefinedOutputPluginFactory* factory = config_factory_pair.second;
    nighthawk::client::UserDefinedOutput global_output;
    global_output.set_plugin_name(factory->name());

    auto it = user_defined_outputs_by_plugin.find(factory->name());
    if (it != user_defined_outputs_by_plugin.end()) {
      absl::StatusOr<Envoy::Protobuf::Any> global_output_any =
          factory->AggregateGlobalOutput(it->second);
      if (global_output_any.ok()) {
        *global_output.mutable_typed_output() = *global_output_any;
      } else {
        *global_output.mutable_error_message() = global_output_any.status().ToString();
      }
    } else {
      *global_output.mutable_error_message() =
          "No per worker outputs found for a factory when performing aggregation";
    }
    global_outputs.push_back(global_output);
  }
  return global_outputs;
}

// Disables the hot restart Envoy functionality.
std::string HotRestartDisabled(bool) { return "hot restart is disabled"; }

} // namespace

// We customize ProdClusterManagerFactory for the sole purpose of returning our specialized
// http1 pool to the benchmark client, which allows us to offer connection prefetching.
class ClusterManagerFactory : public Envoy::Upstream::ProdClusterManagerFactory {
public:
  using Envoy::Upstream::ProdClusterManagerFactory::ProdClusterManagerFactory;

  Envoy::Http::ConnectionPool::InstancePtr allocateConnPool(
      Envoy::Event::Dispatcher& dispatcher, Envoy::Upstream::HostConstSharedPtr host,
      Envoy::Upstream::ResourcePriority priority, std::vector<Envoy::Http::Protocol>& protocols,
      const std::optional<envoy::config::core::v3::AlternateProtocolsCacheOptions>&
          alternate_protocol_options,
      const Envoy::Network::ConnectionSocket::OptionsSharedPtr& options,
      const Envoy::Network::TransportSocketOptionsConstSharedPtr& transport_socket_options,
      Envoy::TimeSource& time_source, Envoy::Upstream::ClusterConnectivityState& state,
      Envoy::Http::PersistentQuicInfoPtr& quic_info,
      Envoy::OptRef<Envoy::Quic::EnvoyQuicNetworkObserverRegistry> network_observer_registry)
      override {
    // This changed in
    // https://github.com/envoyproxy/envoy/commit/93ee668a690d297ab5e8bd2cbf03771d852ebbda ALPN may
    // be set up to negotiate a protocol, in which case we'd need a HttpConnPoolImplMixed. However,
    // our integration tests pass, and for now this might suffice. In case we do run into the need
    // for supporting multiple protocols in a single pool, ensure we hear about it soon, by
    // asserting.
    RELEASE_ASSERT(protocols.size() == 1, "Expected a single protocol in protocols vector.");
    const Envoy::Http::Protocol& protocol = protocols[0];
    if (protocol == Envoy::Http::Protocol::Http11 || protocol == Envoy::Http::Protocol::Http10) {
      auto* h1_pool = new Http1PoolImpl(
          host, priority, dispatcher, options, transport_socket_options,
          context_.api().randomGenerator(), state,
          [](Envoy::Http::HttpConnPoolImplBase* pool) {
            return std::make_unique<Envoy::Http::Http1::ActiveClient>(*pool, std::nullopt);
          },
          [](Envoy::Upstream::Host::CreateConnectionData& data,
             Envoy::Http::HttpConnPoolImplBase* pool) {
            Envoy::Http::CodecClientPtr codec{new Envoy::Http::CodecClientProd(
                Envoy::Http::CodecClient::Type::HTTP1, std::move(data.connection_),
                data.host_description_, pool->dispatcher(), pool->randomGenerator(),
                pool->transportSocketOptions())};
            return codec;
          },
          protocols, context_.overloadManager());
      h1_pool->setConnectionReuseStrategy(connection_reuse_strategy_);
      h1_pool->setPrefetchConnections(prefetch_connections_);
      return Envoy::Http::ConnectionPool::InstancePtr{h1_pool};
    }
    return Envoy::Upstream::ProdClusterManagerFactory::allocateConnPool(
        dispatcher, host, priority, protocols, alternate_protocol_options, options,
        transport_socket_options, time_source, state, quic_info, network_observer_registry);
  }

  void setConnectionReuseStrategy(
      const Http1PoolImpl::ConnectionReuseStrategy connection_reuse_strategy) {
    connection_reuse_strategy_ = connection_reuse_strategy;
  }
  void setPrefetchConnections(const bool prefetch_connections) {
    prefetch_connections_ = prefetch_connections;
  }

private:
  Http1PoolImpl::ConnectionReuseStrategy connection_reuse_strategy_{};
  bool prefetch_connections_{};
};

ProcessImpl::ProcessImpl(const Options& options, Envoy::Event::TimeSystem& time_system,
                         Envoy::Network::DnsResolverFactory& dns_resolver_factory,
                         TypedExtensionConfig typed_dns_resolver_config,
                         const std::shared_ptr<Envoy::ProcessWide>& process_wide)
    : options_(options), number_of_workers_(BootstrapFactory::determineConcurrency(options_)),
      process_wide_(process_wide == nullptr ? std::make_shared<Envoy::ProcessWide>()
                                            : process_wide),
      time_system_(time_system), stats_allocator_(symbol_table_), store_root_(stats_allocator_),
      quic_stat_names_(store_root_.symbolTable()),
      api_(std::make_unique<Envoy::Api::Impl>(platform_impl_.threadFactory(), store_root_,
                                              time_system_, platform_impl_.fileSystem(), generator_,
                                              bootstrap_)),
      dispatcher_(api_->allocateDispatcher("main_thread")), benchmark_client_factory_(options),
      termination_predicate_factory_(options), sequencer_factory_(options),
      request_generator_factory_(options, *api_), init_manager_("nh_init_manager"),
      local_info_(new Envoy::LocalInfo::LocalInfoImpl(
          store_root_.symbolTable(), node_, node_context_params_,
          Envoy::Network::Utility::getLocalAddress(Envoy::Network::Address::IpVersion::v4),
          "nighthawk_service_zone", "nighthawk_service_cluster", "nighthawk_service_node")),
      secret_manager_(config_tracker_), http_context_(store_root_.symbolTable()),

      grpc_context_(store_root_.symbolTable()),
      singleton_manager_(std::make_unique<Envoy::Singleton::ManagerImpl>()),
      access_log_manager_(std::chrono::milliseconds(1000), 0, *api_, *dispatcher_, access_log_lock_,
                          store_root_),
      dns_resolver_factory_(dns_resolver_factory),
      typed_dns_resolver_config_(std::move(typed_dns_resolver_config)),
      init_watcher_("Nighthawk", []() {}),

      admin_(Envoy::Network::Address::InstanceConstSharedPtr()),
      validation_context_(false, false, false, false), router_context_(store_root_.symbolTable()),
      envoy_options_(/* args = */ {"process_impl"}, HotRestartDisabled, spdlog::level::info) {
  // Any dispatchers created after the following call will use hr timers.
  setupForHRTimers();
  std::string lower = absl::AsciiStrToLower(
      nighthawk::client::Verbosity::VerbosityOptions_Name(options_.verbosity()));
  configureComponentLogLevels(spdlog::level::from_str(lower));
}

absl::StatusOr<ProcessPtr> ProcessImpl::CreateProcessImpl(
    const Options& options, Envoy::Network::DnsResolverFactory& dns_resolver_factory,
    TypedExtensionConfig typed_dns_resolver_config, Envoy::Event::TimeSystem& time_system,
    const std::shared_ptr<Envoy::ProcessWide>& process_wide) {
  std::unique_ptr<ProcessImpl> process(new ProcessImpl(options, time_system, dns_resolver_factory,
                                                       std::move(typed_dns_resolver_config),
                                                       process_wide));

  absl::StatusOr<Bootstrap> bootstrap = createBootstrapConfiguration(
      *process->dispatcher_, *process->api_, process->options_, process->dns_resolver_factory_,
      process->typed_dns_resolver_config_, process->number_of_workers_);
  if (!bootstrap.ok()) {
    ENVOY_LOG(error, "Failed to create bootstrap configuration: {}", bootstrap.status().message());
    process->shutdown();
    return bootstrap.status();
  }

  // Ideally we would create the bootstrap first and then pass it to the
  // constructor of Envoy::Api::Api. That cannot be done because of a circular
  // dependency:
  // 1) The constructor of Envoy::Api::Api requires an instance of Bootstrap.
  // 2) The bootstrap generator requires an Envoy::Event::Dispatcher to resolve
  //    URIs to IPs required in the Bootstrap.
  // 3) The constructor of Envoy::Event::Dispatcher requires Envoy::Api::Api.
  //
  // Replacing the bootstrap_ after the Envoy::Api::Api has been created is
  // assumed to be safe, because we still do it while constructing the
  // ProcessImpl, i.e. before we start running the process.
  process->bootstrap_ = *bootstrap;
  process->user_defined_output_factories_ = getUserDefinedFactoryConfigPairs(options);

  return process;
}

ProcessImpl::~ProcessImpl() {
  RELEASE_ASSERT(shutdown_, "shutdown not called before destruction.");
}

void ProcessImpl::shutdown() {
  // FORK-ONLY (We-Amp): M3 Task 6 -- no capability apply may be posted to a
  // dispatcher that is about to go away: disable new fan-outs, drain the
  // in-flight ones (bounded), and clear the registry before the workers are
  // destroyed (the registry-outlives-sequencer ruling).
  drainLiveUpdateFanOuts();
  if (encap_runner_ != nullptr) {
    auto status = encap_runner_->TerminateEncapSubProcess();
    if (status != absl::OkStatus()) {
      ENVOY_LOG(error, status);
    }
  }
  // Before we shut down the worker threads, stop threading.
  tls_.shutdownGlobalThreading();
  store_root_.shutdownThreading();

  {
    auto guard = std::make_unique<Envoy::Thread::LockGuard>(workers_lock_);
    // flush_worker_->shutdown() needs to happen before workers_.clear() so that
    // metrics defined in workers scope will be included in the final stats
    // flush which happens in FlushWorkerImpl::shutdownThread() after
    // flush_worker_->shutdown() is called. For the order between worker shutdown() and
    // shutdownThread(), see worker_impl.cc.
    if (flush_worker_) {
      flush_worker_->shutdown();
    }
    // Before shutting down the cluster manager, stop the workers. Signal all workers to exit
    // first, and only then join them, so that their shutdown sequences (which may block while
    // draining connection pools) run concurrently instead of serialized per worker.
    for (auto& worker : workers_) {
      worker->initiateShutdown();
    }
    for (auto& worker : workers_) {
      worker->shutdown();
    }
    workers_.clear();
  }
  if (cluster_manager_ != nullptr) {
    cluster_manager_->shutdown();
  }
  tls_.shutdownThread();
  dispatcher_->shutdown();
  shutdown_ = true;
}

bool ProcessImpl::requestExecutionCancellation() {
  auto guard = std::make_unique<Envoy::Thread::LockGuard>(workers_lock_);
  for (auto& worker : workers_) {
    worker->requestExecutionCancellation();
  }
  if (encap_runner_ != nullptr) {
    auto status = encap_runner_->TerminateEncapSubProcess();
    if (status != absl::OkStatus()) {
      ENVOY_LOG(error, status);
      return false;
    }
  }
  cancelled_ = true;
  return true;
}

Envoy::MonotonicTime
ProcessImpl::computeFirstWorkerStart(Envoy::Event::TimeSystem& time_system,
                                     const std::optional<Envoy::SystemTime>& scheduled_start,
                                     const uint32_t concurrency) {
  const std::chrono::nanoseconds first_worker_delay =
      scheduled_start.has_value() ? scheduled_start.value() - time_system.systemTime()
                                  : 500ms + (concurrency * 50ms);
  const Envoy::MonotonicTime monotonic_now = time_system.monotonicTime();
  const Envoy::MonotonicTime first_worker_start = monotonic_now + first_worker_delay;
  return first_worker_start;
}

std::chrono::nanoseconds ProcessImpl::computeInterWorkerDelay(const uint32_t concurrency,
                                                              const uint32_t rps) {
  const double inter_worker_delay_usec = (1. / rps) * 1000000 / concurrency;
  return std::chrono::duration_cast<std::chrono::nanoseconds>(inter_worker_delay_usec * 1us);
}

absl::Status ProcessImpl::createWorkers(const uint32_t concurrency,
                                        const std::optional<Envoy::SystemTime>& scheduled_start) {
  ASSERT(workers_.empty());
  // FORK-ONLY (We-Amp): M3 Task 3 -- each run starts from an empty
  // capability registry (before the workers are constructed: a legacy run's
  // main-sequencer capability is recorded by the worker constructor itself);
  // phase-driven runs re-stamp at every begin-phase.
  live_capability_registry_.Reset();
  // FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out's run-state seed: a legacy
  // run's single "phase" is index 0 with the full fleet active; a
  // phase-driven run overwrites both at every begin-phase (runPhases).
  {
    std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
    current_phase_index_ = 0;
    active_worker_count_ = concurrency;
  }
  const Envoy::MonotonicTime first_worker_start =
      computeFirstWorkerStart(time_system_, scheduled_start, concurrency);
  const std::chrono::nanoseconds inter_worker_delay =
      computeInterWorkerDelay(concurrency, options_.requestsPerSecond());
  int worker_number = 0;
  // FORK-ONLY (We-Amp): when live snapshot streaming is opted in, workers
  // compute immutable partials on their own dispatcher threads and hand them
  // to this process, which aggregates them into LiveSnapshots.
  std::function<void(nighthawk::client::WorkerLiveSnapshotPartial)> live_partial_cb;
  std::chrono::milliseconds live_snapshot_interval{0};
  if (options_.liveStreamingEnabled()) {
    live_partial_cb = [this](nighthawk::client::WorkerLiveSnapshotPartial partial) {
      recordLiveSnapshotPartial(std::move(partial));
    };
    const Envoy::Protobuf::Duration flush_duration = options_.statsFlushIntervalDuration();
    if (flush_duration.seconds() > 0 || flush_duration.nanos() > 0) {
      live_snapshot_interval = std::chrono::milliseconds(flush_duration.seconds() * 1000 +
                                                         flush_duration.nanos() / 1000000);
    } else {
      live_snapshot_interval = std::chrono::seconds(options_.statsFlushInterval());
    }
  }
  while (workers_.size() < concurrency) {
    absl::StatusOr<std::vector<UserDefinedOutputNamePluginPair>> plugins =
        createUserDefinedOutputPlugins(user_defined_output_factories_, worker_number);
    if (!plugins.ok()) {
      return plugins.status();
    }
    // FORK-ONLY (We-Amp): M3 Task 3 -- workers publish each phase's captured
    // pre-decorator live-update capability to this process's per-phase
    // registry; the callback fires on the worker's own dispatcher at each
    // begin-phase (and once for a legacy run's main sequencer).
    auto live_capability_cb = [this,
                               worker_number](uint32_t phase_index, LiveTunableRateLimiter* handle,
                                              LiveApplyFn apply,
                                              std::function<void()> cancel_live_update) {
      live_capability_registry_.Record(worker_number, phase_index, handle, std::move(apply),
                                       std::move(cancel_live_update));
    };
    workers_.push_back(std::make_unique<ClientWorkerImpl>(
        *api_, tls_, cluster_manager_, benchmark_client_factory_, termination_predicate_factory_,
        sequencer_factory_, request_generator_factory_, store_root_, worker_number,
        first_worker_start + (inter_worker_delay * worker_number), tracer_,
        options_.simpleWarmup() ? ClientWorkerImpl::HardCodedWarmupStyle::ON
                                : ClientWorkerImpl::HardCodedWarmupStyle::OFF,
        std::move(*plugins), live_partial_cb, live_snapshot_interval,
        /*phase_driven_run=*/options_.hasPhaseVector(), std::move(live_capability_cb)));
    // FORK-ONLY (We-Amp): M3 Task 6 -- the worker opens the boundary-drain
    // live-update gate when its non-final sequencer stops (the boundary
    // begins); runPhases closes the gate when the drain is collected. Set
    // before the worker thread starts.
    workers_.back()->setSequencerStopObserver(
        [this]() { setBoundaryDrainInProgress(true); });
    if (options_.hasPhaseVector()) {
      // FORK-ONLY (We-Amp): the phase orchestrator waits on these before
      // posting the first begin-phase: begin-phase must not land while a
      // worker is still in its warmup's nested dispatcher run.
      worker_ready_futures_.push_back(workers_.back()->readyFuture());
    }
    worker_number++;
  }

  return absl::OkStatus();
}

void ProcessImpl::recordLiveSnapshotPartial(nighthawk::client::WorkerLiveSnapshotPartial partial) {
  {
    std::lock_guard<std::mutex> lock(live_partials_mutex_);
    live_partials_.push_back(std::move(partial));
  }
}

// FORK-ONLY (We-Amp): M3 Task 3, see process_impl.h. Record runs on the
// worker's dispatcher thread (inside the worker's begin-phase task); the
// mutex is the only cross-thread surface and is never held while doing
// engine work.
void LiveCapabilityRegistry::Record(int worker_id, uint32_t phase_index,
                                    LiveTunableRateLimiter* handle, LiveApplyFn apply,
                                    std::function<void()> cancel_live_update) {
  std::lock_guard<std::mutex> lock(mutex_);
  entries_[worker_id] =
      LiveCapabilityEntry{phase_index, handle, std::move(apply), std::move(cancel_live_update)};
}

absl::StatusOr<LiveCapabilityEntry>
LiveCapabilityRegistry::CapabilityForPhase(int worker_id, uint32_t phase_index) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = entries_.find(worker_id);
  if (it == entries_.end()) {
    return absl::NotFoundError(fmt::format(
        "worker {} has no captured live-update capability (no phase begun)", worker_id));
  }
  if (it->second.phase_index != phase_index) {
    // Stale-handle case: the worker's stamp is for another phase (a boundary
    // raced the fan-out, or the worker was retired by the M2 subsets).
    return absl::FailedPreconditionError(fmt::format(
        "worker {}'s live-update capability is stale: stamped for phase index {}, "
        "not the targeted {}",
        worker_id, it->second.phase_index, phase_index));
  }
  return it->second;
}

void LiveCapabilityRegistry::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.clear();
}

namespace {

// FORK-ONLY (We-Amp): M3 Task 6, see process_impl.h. The deadline reserve
// held back for the residual rollback path: the forward waits never run past
// deadline - reserve, so the rollback (the honest completion of a
// partially-applied update) fits inside the applier deadline the orchestrator
// bounds its join with.
constexpr absl::Duration kLiveUpdateRollbackReserve = absl::Seconds(2);

// The shutdown fence drain bound: an in-flight fan-out is bounded by the
// orchestrator's ack-wait deadline (its applier contract); the fence waits
// that long plus slack before proceeding with worker teardown anyway (and
// says so).
constexpr absl::Duration kLiveUpdateFenceDrainBound = absl::Seconds(8);

// FORK-ONLY (M3 Task 8a): the live knob speaks GLOBAL rps -- "set rate to X"
// means the phase's total, exactly like a phase's declared requests_per_second
// since the M2 burstfix. The fan-out divides a delta across the ACTIVE
// workers with the same deterministic rule the phase-begin loop uses: base =
// declared / active, the remainder to the lowest worker indexes, and a
// declaration below the worker count paces 1 rps per worker rather than
// constructing a zero-rate limiter. The ramp knob's target rps is a global
// rate too and divides by the same rule; the ramp time (a shape, not a rate)
// passes through undivided. The baseline (the rollback prior-state tracker)
// stays declared-global: every apply post -- forward AND rollback -- divides
// at this one point, so a no-op bend at the declared rps is a true no-op on
// every worker and a rollback restores each worker's true per-worker
// operating point (never the full global value re-applied N times).
LiveKnobDelta PerWorkerDelta(const LiveKnobDelta& global_delta, uint32_t worker_id,
                             uint32_t active_workers) {
  ASSERT(active_workers > 0);
  ASSERT(worker_id < active_workers);
  LiveKnobDelta per_worker = global_delta;
  const auto divide = [worker_id, active_workers](uint32_t declared) -> uint32_t {
    const uint32_t base = declared / active_workers;
    const uint32_t remainder = declared % active_workers;
    return base > 0 ? base + (worker_id < remainder ? 1u : 0u) : 1u;
  };
  if (per_worker.new_rps.has_value()) {
    per_worker.new_rps = divide(*per_worker.new_rps);
  }
  if (per_worker.ramp.has_value() && per_worker.ramp->target_rps.has_value()) {
    per_worker.ramp->target_rps = divide(*per_worker.ramp->target_rps);
  }
  return per_worker;
}

} // namespace

LiveCapabilityFanOutApplier::LiveCapabilityFanOutApplier(LiveFanOutContext context)
    : context_(std::move(context)) {}

bool LiveCapabilityFanOutApplier::StageOutcomesComplete() {
  return stage_outcomes_.size() >= expected_workers_;
}

bool LiveCapabilityFanOutApplier::ApplyOutcomesComplete() {
  return apply_outcomes_.size() >= expected_workers_;
}

LiveKnobDelta LiveCapabilityFanOutApplier::NormalizeDelta(
    const nighthawk::client::UpdateRequest& request) {
  // The Task-2 whitelist validator ran in front of this and guarantees: at
  // least one knob, no duplicate knob, invariants satisfied.
  LiveKnobDelta delta;
  for (const nighthawk::client::KnobDelta& knob : request.deltas()) {
    if (knob.has_requests_per_second()) {
      delta.new_rps = knob.requests_per_second().value();
    } else if (knob.has_ramp()) {
      RampShapeChange change;
      if (knob.ramp().has_ramp_time()) {
        // The proto duration's seconds ARE seconds (the same conversion the
        // plugin config path uses): casting them as nanoseconds turned every
        // wire-sent ramp bend into a ~instantaneous ramp.
        change.ramp_time = std::chrono::seconds(knob.ramp().ramp_time().seconds()) +
                           std::chrono::nanoseconds(knob.ramp().ramp_time().nanos());
      }
      if (knob.ramp().has_target_rps()) {
        change.target_rps = knob.ramp().target_rps().value();
      }
      delta.ramp = change;
    }
  }
  return delta;
}

void LiveCapabilityFanOutApplier::RecordStageOutcome(int worker_id, absl::Status status) {
  absl::MutexLock lock(&mutex_);
  stage_outcomes_.push_back({worker_id, std::move(status)});
}

void LiveCapabilityFanOutApplier::RecordApplyOutcome(int worker_id, absl::Status status) {
  absl::MutexLock lock(&mutex_);
  apply_outcomes_.push_back({worker_id, std::move(status)});
}

nighthawk::client::UpdateResult LiveCapabilityFanOutApplier::ApplyDeltas(
    const nighthawk::client::UpdateRequest& request, const std::string& epoch_id,
    const EngineInstant& bend_instant, absl::Time deadline) {
  nighthawk::client::UpdateResult result;
  result.set_applied(false);
  const auto reject = [&result](absl::string_view reason) {
    result.set_rejection_reason(std::string(reason));
    ENVOY_LOG(debug, "live update rejected: {}", reason);
    return result;
  };

  // One bend at a time (Task 4 Q4's ruling, enforced at the fan-out): a
  // second update arriving before the first resolves is rejected, never
  // queued.
  {
    absl::MutexLock lock(&mutex_);
    if (apply_in_progress_) {
      return reject("another live update is still being applied; bends are not queued");
    }
    apply_in_progress_ = true;
  }
  struct InProgressGuard {
    ~InProgressGuard() {
      absl::MutexLock lock(&applier->mutex_);
      applier->apply_in_progress_ = false;
    }
    LiveCapabilityFanOutApplier* applier;
  } in_progress_guard{this};

  // The shutdown fence: past this point every exit path releases the
  // registration.
  if (!context_.fan_out_begin()) {
    return reject("the benchmark run is shutting down; live updates are no longer accepted");
  }
  struct FanOutScope {
    ~FanOutScope() { end(); }
    std::function<void()> end;
  } fan_out_scope{context_.fan_out_end};

  // Gate 1: admissibility (boundary drain in progress, no active workers,
  // run teardown).
  if (absl::Status admissible = context_.update_admissible(); !admissible.ok()) {
    return reject(admissible.message());
  }

  // Gate 2: the bend instant must land inside the current phase's remaining
  // duration (phase-vector runs) or the run's remaining duration (legacy
  // runs) -- a bend landing past the boundary is undefined and rejected
  // BEFORE any fan-out.
  const Envoy::MonotonicTime t_bend =
      Envoy::MonotonicTime(std::chrono::nanoseconds(bend_instant.monotonic_ns) +
                           kLiveUpdateBendLead);
  if (absl::Status bend_ok = context_.bend_instant_admissible(t_bend); !bend_ok.ok()) {
    return reject(bend_ok.message());
  }

  // The normalized delta set and the rollback prior-state tracker (reset
  // when the phase moved: fresh limiters per phase mean the prior schedule
  // is the new phase's declared one).
  const LiveKnobDelta delta = NormalizeDelta(request);
  const uint32_t phase_index = context_.current_phase_index();
  {
    absl::MutexLock lock(&mutex_);
    if (!schedule_initialized_ || phase_index != tracked_phase_index_) {
      tracked_phase_index_ = phase_index;
      tracked_schedule_ = context_.current_phase_schedule();
      schedule_initialized_ = true;
    }
  }

  // Staging, part 1 -- lookups in worker-id order over the ACTIVE subset
  // only. Any lookup failure (worker never began a phase, stale
  // pre-boundary stamp, retired worker) fails the update honestly before a
  // single post: never a silent skip, never a mutation through a dead
  // handle.
  const uint32_t active_workers = context_.active_worker_count();
  std::vector<LiveCapabilityEntry> entries;
  entries.reserve(active_workers);
  for (uint32_t worker_id = 0; worker_id < active_workers; ++worker_id) {
    absl::StatusOr<LiveCapabilityEntry> entry_or =
        context_.capability_for_phase(worker_id, phase_index);
    if (!entry_or.ok()) {
      return reject(
          absl::StrCat("worker ", worker_id, " cannot accept the live update: ",
                       entry_or.status().message()));
    }
    entries.push_back(*std::move(entry_or));
  }

  // Staging, part 2 -- the arming posts, in worker-id order (the defined
  // fan-out ordering). stage_callback fires on the worker dispatcher BEFORE
  // t_bend with the arming result; apply_callback fires at/after t_bend.
  size_t expected = 0;
  {
    absl::MutexLock lock(&mutex_);
    stage_outcomes_.clear();
    apply_outcomes_.clear();
    expected_workers_ = active_workers;
    expected = expected_workers_;
  }
  const auto stage_cb = [this](int worker_id) {
    return [this, worker_id](absl::Status status) { RecordStageOutcome(worker_id, status); };
  };
  const auto apply_cb = [this](int worker_id) {
    return [this, worker_id](absl::Status status) { RecordApplyOutcome(worker_id, status); };
  };
  for (uint32_t worker_id = 0; worker_id < active_workers; ++worker_id) {
    // Task 8a: the delta is GLOBAL; each worker is posted its own share (the
    // phase-begin division), so a no-op bend at the declared rps re-arms
    // every worker at exactly the rate it is already pacing.
    entries[worker_id].apply(t_bend, epoch_id, PerWorkerDelta(delta, worker_id, active_workers),
                             stage_cb(worker_id), apply_cb(worker_id));
  }

  // Deadline budget: the forward waits hold back a reserve for the residual
  // rollback path, so the honest completion of a partially-applied update
  // still fits inside the applier deadline the orchestrator bounds its join
  // with.
  const absl::Duration remaining = deadline - absl::Now();
  if (remaining <= absl::ZeroDuration()) {
    return reject("the live-update deadline has already passed before staging began");
  }
  const absl::Duration reserve = std::min(kLiveUpdateRollbackReserve, remaining / 3);
  const absl::Time apply_deadline = deadline - reserve;
  // The staging wait must complete strictly before the bend instant (minus
  // the dispatch grace): a staged bend can only be cancelled honestly while
  // it has not fired.
  const Envoy::MonotonicTime now_mono = context_.now();
  const auto until_bend_grace = (t_bend - kLiveUpdateBendGrace) - now_mono;
  const absl::Time stage_deadline =
      std::min(apply_deadline,
               absl::Now() + absl::FromChrono(until_bend_grace) - absl::Milliseconds(20));

  const auto cancel_everything_staged = [&entries]() {
    for (const LiveCapabilityEntry& entry : entries) {
      if (entry.cancel_live_update != nullptr) {
        entry.cancel_live_update();
      }
    }
  };

  // Staging wait (the PRIMARY failure path): bounded strictly before the
  // bend instant.
  {
    mutex_.Lock();
    mutex_.AwaitWithDeadline(absl::Condition(this, &LiveCapabilityFanOutApplier::StageOutcomesComplete),
                             stage_deadline);
    const bool staging_complete = stage_outcomes_.size() >= expected_workers_;
    const WorkerOutcome* stage_failure = nullptr;
    if (staging_complete) {
      for (const WorkerOutcome& outcome : stage_outcomes_) {
        if (!outcome.status.ok()) {
          stage_failure = &outcome;
          break;
        }
      }
    }
    const WorkerOutcome failure_copy =
        stage_failure != nullptr ? *stage_failure : WorkerOutcome{0, absl::OkStatus()};
    mutex_.Unlock();
    if (stage_failure != nullptr || !staging_complete) {
      // A worker failed staging (or did not confirm in time): cancel every
      // staged bend -- including the ones armed on workers that staged OK --
      // BEFORE the bend instant, so nothing is applied anywhere. The
      // limiter was never touched (arming is not applying).
      cancel_everything_staged();
      if (!staging_complete) {
        return reject(absl::StrCat("staging did not confirm on all ", expected,
                                   " active worker(s) before the bend instant; the staged bends "
                                   "were cancelled and nothing was applied"));
      }
      return reject(absl::StrCat("worker ", failure_copy.worker_id,
                                 " failed to stage the live update: ",
                                 failure_copy.status.message(),
                                 "; all staged bends were cancelled and nothing was applied"));
    }
  }

  // Apply wait: every worker's apply result fires at/after the bend instant.
  {
    mutex_.Lock();
    mutex_.AwaitWithDeadline(absl::Condition(this, &LiveCapabilityFanOutApplier::ApplyOutcomesComplete),
                             apply_deadline);
    const bool apply_complete = apply_outcomes_.size() >= expected_workers_;
    // Copy the outcomes out: the rollback below reuses the slots.
    const std::vector<WorkerOutcome> outcomes = apply_outcomes_;
    mutex_.Unlock();

    const WorkerOutcome* apply_failure = nullptr;
    for (const WorkerOutcome& outcome : outcomes) {
      if (!outcome.status.ok()) {
        apply_failure = &outcome;
        break;
      }
    }
    std::string failure_text;
    if (apply_failure != nullptr) {
      failure_text = absl::StrCat("worker ", apply_failure->worker_id,
                                  " failed to apply the live update at the bend instant: ",
                                  apply_failure->status.message());
    } else if (!apply_complete) {
      // The bend instant passed without every worker reporting (a worker
      // dispatcher is stuck). Resolve honestly: cancel whatever is still
      // staged (a no-op once a bend fired), roll the applied workers back.
      failure_text = absl::StrCat("only ", outcomes.size(), " of ", expected,
                                  " active worker(s) confirmed the apply in time");
      cancel_everything_staged();
    }

    if (failure_text.empty()) {
      // Committed: every active worker applied at the agreed bend instant.
      // Advance the rollback tracker and the epoch the next rollback would
      // restore.
      absl::MutexLock lock(&mutex_);
      if (delta.new_rps.has_value()) {
        tracked_schedule_.new_rps = delta.new_rps;
      }
      if (delta.ramp.has_value()) {
        RampShapeChange& tracked =
            tracked_schedule_.ramp ? *tracked_schedule_.ramp : tracked_schedule_.ramp.emplace();
        if (delta.ramp->ramp_time.has_value()) {
          tracked.ramp_time = delta.ramp->ramp_time;
        }
        if (delta.ramp->target_rps.has_value()) {
          tracked.target_rps = delta.ramp->target_rps;
        }
      }
      prior_epoch_id_ = epoch_id;
      result.set_applied(true);
      result.set_epoch_id(epoch_id);
      // The engine pair at the bend instant (8.5): the monotonic stamp is
      // the agreed instant itself; the wall stamp follows it by the lead.
      result.set_monotonic_ns(t_bend.time_since_epoch().count());
      result.set_wall_time_unix_ns(bend_instant.wall_time_unix_ns + kLiveUpdateBendLead.count());
      return result;
    }

    // --- The residual post-bend failure path: rollback at t_r. ---
    // A re-anchored re-apply of the PRIOR schedule AT THE ROLLBACK INSTANT
    // (the A2b design note's Q7 form: solve the prior schedule's origin so
    // its integrated releases to t_r equal the acquired count at t_r --
    // exactly what the limiter's apply does when invoked at t_r), never a
    // naive re-apply at t_bend. Workers that did NOT apply (staged bends
    // cancelled above) never mutated and are already on the prior schedule;
    // they are not posted.
    std::vector<int> rolled_back_workers;
    {
      LiveKnobDelta prior;
      std::string prior_epoch;
      Envoy::MonotonicTime t_r = context_.now();
      {
        absl::MutexLock lock(&mutex_);
        prior = tracked_schedule_;
        prior_epoch = prior_epoch_id_;
      }
      // The rollback results reuse the apply slots: clear the forward
      // outcomes and set the expectation BEFORE the posts (a fake worker --
      // or a real one -- may fire its rollback result inside the post).
      {
        absl::MutexLock lock(&mutex_);
        apply_outcomes_.clear();
      }
      const auto rollback_stage = [](absl::Status) {};
      const auto rollback_cb = [this](int worker_id) {
        return [this, worker_id](absl::Status status) { RecordApplyOutcome(worker_id, status); };
      };
      for (const WorkerOutcome& outcome : outcomes) {
        if (!outcome.status.ok()) {
          continue; // never mutated: nothing to roll back
        }
        entries[outcome.worker_id].apply(t_r, prior_epoch,
                                         PerWorkerDelta(prior, outcome.worker_id, active_workers),
                                         rollback_stage, rollback_cb(outcome.worker_id));
        rolled_back_workers.push_back(outcome.worker_id);
      }
      std::string rollback_failures;
      if (!rolled_back_workers.empty()) {
        {
          absl::MutexLock lock(&mutex_);
          expected_workers_ = rolled_back_workers.size();
        }
        absl::MutexLock lock(&mutex_);
        mutex_.AwaitWithDeadline(
            absl::Condition(this, &LiveCapabilityFanOutApplier::ApplyOutcomesComplete), deadline);
        for (const WorkerOutcome& outcome : apply_outcomes_) {
          if (!outcome.status.ok()) {
            absl::StrAppend(&rollback_failures, " worker ", outcome.worker_id,
                            " failed the rollback re-apply: ", outcome.status.message(), ";");
          }
        }
      }
      if (!rollback_failures.empty()) {
        // Honest, never silent: the ack names the double failure; the
        // affected workers may still carry the attempted epoch on their
        // snapshot stamps until the next successful apply.
        ENVOY_LOG(warn, "live update rollback incomplete:{}", rollback_failures);
        failure_text = absl::StrCat(failure_text, " ROLLBACK INCOMPLETE:", rollback_failures);
      } else {
        failure_text = absl::StrCat(
            failure_text, "; rolled back to the prior schedule at the rollback instant (",
            rolled_back_workers.size(), " applied worker(s) reverted)");
      }
    }
    return reject(failure_text);
  }
}

// FORK-ONLY (We-Amp): M3 Task 6 -- the boundary-drain live-update gate. Set
// TRUE by the workers' sequencer-stop observer (on the worker dispatcher,
// inside the stop hook: a phase boundary has begun, possibly EARLY via a
// termination predicate), cleared by runPhases when the boundary's drain is
// collected. An update arriving inside the window is rejected with a clear
// reason.
void ProcessImpl::setBoundaryDrainInProgress(bool in_progress) {
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  boundary_drain_in_progress_ = in_progress;
}

void ProcessImpl::setCurrentPhaseId(absl::string_view phase_id,
                                    std::chrono::nanoseconds phase_duration_ns) {
  const int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             time_system_.monotonicTime().time_since_epoch())
                             .count();
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  current_phase_id_ = std::string(phase_id);
  current_phase_start_monotonic_ns_ = now_ns;
  current_phase_duration_ns_ = phase_duration_ns.count();
}

InFlightRunShape ProcessImpl::liveUpdateRunShape() const {
  InFlightRunShape shape;
  shape.has_phase_vector = options_.hasPhaseVector();
  // The ramp plugin is the linear-ramping rate limiter: only its runs can
  // bend ramps, and per C1 a live rps change on one is ramp
  // re-parameterization (both enforced by the whitelist validator).
  static constexpr absl::string_view kRampingPluginName =
      "nighthawk.linear-ramping-rate-limiter-plugin";
  const auto& plugin = options_.rateLimiterPluginConfig();
  shape.ramp_plugin_configured = plugin.has_value() && plugin->name() == kRampingPluginName;
  const int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             time_system_.monotonicTime().time_since_epoch())
                             .count();
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  shape.current_phase_id = current_phase_id_;
  if (shape.has_phase_vector) {
    // The CURRENT phase's remaining duration; clamped at zero (a phase past
    // its nominal end but not yet past the boundary offers no room).
    const int64_t remaining =
        current_phase_duration_ns_ - (now_ns - current_phase_start_monotonic_ns_);
    shape.remaining_duration = std::chrono::nanoseconds(std::max<int64_t>(remaining, 0));
  } else if (!options_.noDuration() && run_start_monotonic_ns_ > 0) {
    // Legacy/single-phase run: the remaining RUN duration, the same bound
    // the ramp limiter factory enforces at construction.
    const int64_t remaining =
        std::chrono::duration_cast<std::chrono::nanoseconds>(options_.duration()).count() -
        (now_ns - run_start_monotonic_ns_);
    shape.remaining_duration = std::chrono::nanoseconds(std::max<int64_t>(remaining, 0));
  }
  return shape;
}

std::string ProcessImpl::currentPhaseId() const {
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  return current_phase_id_;
}

// --- FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out context halves. All of
// these are safe to call from the update orchestrator's ack thread: they
// only read run state under current_phase_id_mutex_ (or the fence mutex),
// never engine objects. ---

absl::Status ProcessImpl::liveUpdateAdmissionStatus() const {
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  if (boundary_drain_in_progress_) {
    return absl::FailedPreconditionError(
        "the run is inside a phase boundary drain; live updates are rejected "
        "there (the operating point is undefined mid-drain, and a bend is "
        "never applied across a boundary)");
  }
  if (active_worker_count_ == 0) {
    return absl::FailedPreconditionError(
        "no workers are active yet; live updates need a begun phase with an "
        "active worker subset");
  }
  return absl::OkStatus();
}

absl::Status ProcessImpl::bendInstantAdmissibleForUpdate(Envoy::MonotonicTime t_bend) const {
  const int64_t t_bend_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(t_bend.time_since_epoch()).count();
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  const bool has_phase_vector = options_.hasPhaseVector();
  if (has_phase_vector) {
    const int64_t phase_end_ns = current_phase_start_monotonic_ns_ + current_phase_duration_ns_;
    if (t_bend_ns > phase_end_ns) {
      return absl::FailedPreconditionError(absl::StrCat(
          "the bend instant would land ", (t_bend_ns - phase_end_ns) / 1000000,
          "ms past the current phase's end; a bend is never applied across a "
          "phase boundary"));
    }
    return absl::OkStatus();
  }
  if (options_.noDuration()) {
    return absl::OkStatus();
  }
  const int64_t run_end_ns =
      run_start_monotonic_ns_ +
      std::chrono::duration_cast<std::chrono::nanoseconds>(options_.duration()).count();
  if (run_start_monotonic_ns_ > 0 && t_bend_ns > run_end_ns) {
    return absl::FailedPreconditionError(absl::StrCat(
        "the bend instant would land past the run's end; a bend is never "
        "applied past the last instant the schedule can act on"));
  }
  return absl::OkStatus();
}

uint32_t ProcessImpl::currentPhaseIndexForUpdate() const {
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  return current_phase_index_;
}

uint32_t ProcessImpl::activeWorkerCountForUpdate() const {
  std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
  return active_worker_count_;
}

LiveKnobDelta ProcessImpl::currentPhaseScheduleDelta() const {
  LiveKnobDelta schedule;
  if (liveUpdateRunShape().ramp_plugin_configured) {
    // The plugin path's schedule is the ramp: the declared ramp time (the
    // plugin config is run-level) and the target frequency. The plugin
    // factory ignores per-phase rps overrides, so the run-level option is
    // the target on every phase.
    nighthawk::rate_limiter::LinearRampingRateLimiterConfig config;
    const auto& plugin = options_.rateLimiterPluginConfig();
    if (plugin.has_value() && plugin->typed_config().UnpackTo(&config)) {
      RampShapeChange change;
      change.ramp_time = std::chrono::seconds(config.ramp_time().seconds()) +
                         std::chrono::nanoseconds(config.ramp_time().nanos());
      change.target_rps = options_.requestsPerSecond();
      schedule.ramp = change;
    }
    return schedule;
  }
  // Non-plugin path: the CURRENT phase's declared rps (the per-phase
  // sequencer is built at that rate; unset inherits the run-level option).
  const auto& phases = options_.phases();
  uint32_t rps = options_.requestsPerSecond();
  if (options_.hasPhaseVector()) {
    std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
    if (current_phase_index_ < phases.size() && phases[current_phase_index_].requests_per_second() > 0) {
      rps = phases[current_phase_index_].requests_per_second();
    }
  }
  schedule.new_rps = rps;
  return schedule;
}

bool ProcessImpl::fanOutBegin() {
  absl::MutexLock lock(&live_update_fence_mutex_);
  if (live_updates_disabled_) {
    return false;
  }
  ++in_flight_fan_outs_;
  return true;
}

void ProcessImpl::fanOutEnd() {
  absl::MutexLock lock(&live_update_fence_mutex_);
  --in_flight_fan_outs_;
}

void ProcessImpl::drainLiveUpdateFanOuts() {
  // Called from shutdown() BEFORE the workers are destroyed: disable new
  // fan-outs, then wait (bounded -- appliers honor their deadlines) for the
  // in-flight ones to register out, then clear the capability registry (the
  // registry-outlives-sequencer ruling: a stale entry's sequencer is about
  // to be gone).
  {
    absl::MutexLock lock(&live_update_fence_mutex_);
    live_updates_disabled_ = true;
    const bool drained = live_update_fence_mutex_.AwaitWithDeadline(
        absl::Condition(this, &ProcessImpl::LiveUpdateFanOutsDrained),
        absl::Now() + kLiveUpdateFenceDrainBound);
    if (!drained) {
      ENVOY_LOG(warn, "live update fan-out(s) still in flight ", in_flight_fan_outs_,
                " after the drain bound; proceeding with worker teardown");
    }
  }
  live_capability_registry_.Reset();
}

std::unique_ptr<LiveUpdateApplier> ProcessImpl::MakeLiveUpdateApplier() {
  LiveFanOutContext context;
  context.now = [this]() { return time_system_.monotonicTime(); };
  context.update_admissible = [this]() { return liveUpdateAdmissionStatus(); };
  context.bend_instant_admissible = [this](Envoy::MonotonicTime t_bend) {
    return bendInstantAdmissibleForUpdate(t_bend);
  };
  context.current_phase_index = [this]() { return currentPhaseIndexForUpdate(); };
  context.active_worker_count = [this]() { return activeWorkerCountForUpdate(); };
  context.capability_for_phase = [this](int worker_id, uint32_t phase_index) {
    return live_capability_registry_.CapabilityForPhase(worker_id, phase_index);
  };
  context.current_phase_schedule = [this]() { return currentPhaseScheduleDelta(); };
  context.fan_out_begin = [this]() { return fanOutBegin(); };
  context.fan_out_end = [this]() { fanOutEnd(); };
  return std::make_unique<LiveCapabilityFanOutApplier>(std::move(context));
}

void ProcessImpl::emitPhaseEvent(const PhaseEvent& event) {
  ENVOY_LOG(debug, "phase event: index {} ('{}') wall {} monotonic {}", event.phase_index,
            event.phase_id, event.wall_time_unix_ns, event.monotonic_ns);
  if (phase_event_cb_ != nullptr) {
    // FORK-ONLY (T7 wiring): enrich a completed boundary with the worker
    // subset facts (the orchestrator does not know the fleet plan). Same
    // thread as runPhases (the sink is synchronous), so the subset vector is
    // stable here.
    PhaseEvent enriched = event;
    if (enriched.boundary_complete && !phase_worker_subsets_.empty()) {
      if (enriched.ending_phase_index < phase_worker_subsets_.size()) {
        enriched.worker_count = phase_worker_subsets_[enriched.ending_phase_index].worker_count;
      }
      if (enriched.phase_index < phase_worker_subsets_.size()) {
        enriched.next_phase_cold_started =
            phase_worker_subsets_[enriched.phase_index].new_workers_cold_started;
      }
    }
    phase_event_cb_(enriched);
  }
}

// FORK-ONLY (We-Amp): the raise-only runtime breaker override arm (M2 Task 5,
// per the M2 Task 1 gate verdict: FEASIBLE-WITH-CAVEATS). With the breakers
// bootstrapped at the max across phases, a phase vector can never request a
// ceiling above the bootstrap max, so from the option surface this merge is
// structurally empty in M2 -- the seam is the verified-warm raise path that a
// live knob drives from M3. The raise-only rule is enforced in
// raiseOnlyBreakerOverrides: a lowering attempt is refused by omission, and
// the bootstrapped breaker capacity is never undercut.
void ProcessImpl::applyPhaseBreakerOverrides(const PhaseResourceLimits& desired_limits) {
  if (runtime_loader_ == nullptr) {
    return;
  }
  const PhaseResourceLimits bootstrap_limits = maxPhaseResourceLimits(options_);
  absl::node_hash_map<std::string, std::string> overrides;
  for (uint32_t worker_number = 0; worker_number < workers_.size(); ++worker_number) {
    for (auto& [key, value] : raiseOnlyBreakerOverrides(fmt::format("{}", worker_number),
                                                        bootstrap_limits, desired_limits)) {
      overrides[key] = value;
    }
  }
  if (overrides.empty()) {
    return;
  }
  absl::Status status = runtime_loader_->mergeValues(overrides);
  if (!status.ok()) {
    ENVOY_LOG(warn, "phase breaker override merge failed: {}", status.message());
  }
}

// FORK-ONLY (We-Amp): the multi-phase run loop. Threading contract (M2 A1):
// this runs on the run-calling thread, adds no thread of its own. All
// cross-thread traffic is one-way posts into worker dispatchers plus drain
// promises completed from the worker side; a worker dispatcher NEVER blocks
// on another worker or on the orchestrator. The only blocking wait in the
// system is the orchestrator's bounded future collection (deadline =
// --timeout), and it can always time out. The boundary drain is
// non-destructive (spec 8.6): no connection operations of any kind, no
// drainConnections/DrainAndDelete -- that stays reserved for final
// terminate().
void ProcessImpl::runPhases() {
  const std::chrono::milliseconds drain_budget(options_.timeout());
  PhaseOrchestrator orchestrator(
      options_.phases(), [this](const PhaseEvent& event) { emitPhaseEvent(event); });
  // FORK-ONLY (We-Amp): worker subsets (M2 Task 6). The bootstrap already
  // constructed the max-over-phases worker count (determineConcurrency);
  // phases activate the declared PREFIX of that fleet and retire the rest.
  phase_worker_subsets_ = computeWorkerSubsets(options_.phases(), workers_.size());
  worker_begun_phases_.assign(workers_.size(), {});
  // The live aggregator advances its snapshot label only monotonically along
  // the phase order, so a removed worker's fold partial (old stamp) cannot
  // drag the label backward (M2 review fix).
  std::vector<std::string> phase_order;
  phase_order.reserve(options_.phases().size());
  for (const nighthawk::client::PhaseSpec& phase : options_.phases()) {
    phase_order.push_back(phase.name());
  }
  live_aggregator_.setPhaseOrder(std::move(phase_order));
  // Every worker must have finished thread-local initialization and entered
  // its dispatcher loop before the first begin-phase post (the warmup's
  // nested dispatcher run must not pick it up). These futures always
  // complete; the wait here is bounded in practice by worker init.
  for (const std::shared_future<void>& ready : worker_ready_futures_) {
    ready.wait();
  }

  for (uint32_t phase_index = 0; phase_index < orchestrator.phaseCount(); ++phase_index) {
    const nighthawk::client::PhaseSpec& phase = orchestrator.phase(phase_index);
    // FORK-ONLY (We-Amp): effective per-phase limits (M2 Task 5): declared
    // values when present, run-level values inherited when absent. The posted
    // spec carries the effective values so the worker's begin-phase applies
    // the per-phase gates without re-deriving inheritance; the raise-only
    // breaker override runs before any begin-phase post so every worker
    // installs the new snapshot before its first phase work.
    const PhaseResourceLimits limits = phaseResourceLimits(options_, &phase);
    nighthawk::client::PhaseSpec effective_phase = phase;
    effective_phase.mutable_connections()->set_value(limits.connections);
    effective_phase.mutable_max_pending_requests()->set_value(limits.max_pending_requests);
    effective_phase.mutable_max_active_requests()->set_value(limits.max_active_requests);
    applyPhaseBreakerOverrides(limits);
    setCurrentPhaseId(phase.name(), std::chrono::seconds(phase.duration().seconds()) +
                                        std::chrono::nanoseconds(phase.duration().nanos()));
    orchestrator.markNextPhaseStarted(phase_index);
    const bool final_phase = orchestrator.isFinalPhase(phase_index);
    // M2 Task 6: activate this phase's subset (the worker prefix) and retire
    // the workers it drops. The posts are one-way, per-dispatcher FIFO; the
    // retiring workers' boundary drain for their last phase was already
    // collected at the previous boundary.
    const PhaseWorkerSubset& subset = phase_worker_subsets_[phase_index];
    const uint32_t active_workers = subset.worker_count;
    // FORK-ONLY (We-Amp): M3 Task 6 -- the fan-out targets exactly this
    // phase's active subset at exactly this phase index; both are stamped
    // before the begin-phase posts (the registry re-stamps happen on the
    // worker dispatchers inside those posts, so a fan-out racing the
    // boundary fails honestly at lookup, never half-targets).
    {
      std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
      current_phase_index_ = phase_index;
      active_worker_count_ = active_workers;
    }
    const uint32_t prior_active =
        phase_index == 0 ? 0 : phase_worker_subsets_[phase_index - 1].worker_count;
    // Highest active count posted so far: workers beyond it have never begun
    // a phase.
    uint32_t activated_high_water = 0;
    for (uint32_t i = 0; i < phase_index; ++i) {
      activated_high_water = std::max(activated_high_water, phase_worker_subsets_[i].worker_count);
    }
    if (final_phase) {
      // Every worker outside the final subset sits out the rest of the run:
      // fold its final live partial and let it exit, so the run's join (which
      // waits on all workers) completes.
      for (uint32_t w = active_workers; w < workers_.size(); ++w) {
        workers_[w]->goIdleAsync(/*exit_dispatcher=*/true);
      }
    } else {
      // Retire every worker outside this phase's subset. [active,
      // prior_active) ran a previous phase (removed at this boundary);
      // anything beyond the activation high-water mark has never begun a
      // phase -- retire it too, so its live snapshot timer does not emit
      // zero partials for the whole run (M2 review fix). Both groups idle on
      // their live dispatchers with no sequencer and no requests, ready for
      // a later phase to re-activate them.
      const uint32_t never_begun_from = std::max(active_workers, activated_high_water);
      for (uint32_t w = active_workers; w < prior_active; ++w) {
        workers_[w]->goIdleAsync(/*exit_dispatcher=*/false);
      }
      for (uint32_t w = never_begun_from; w < workers_.size(); ++w) {
        workers_[w]->goIdleAsync(/*exit_dispatcher=*/false);
      }
    }
    if (subset.new_workers_cold_started) {
      // Surfaced, never silent (spec 8.4/Task 6): the joining workers open
      // their first connections at this boundary.
      ENVOY_LOG(info, "phase {} ('{}'): {} workers active, {} new worker(s) cold-starting",
                phase_index, phase.name(), active_workers, active_workers - prior_active);
    } else if (active_workers > prior_active) {
      // A growth within the high-water mark: rejoined workers on preserved
      // (warm) pools -- a worker-count change, but not a cold start.
      ENVOY_LOG(info,
                "phase {} ('{}'): {} workers active, {} worker(s) rejoining on preserved pools",
                phase_index, phase.name(), active_workers, active_workers - prior_active);
    } else if (active_workers < prior_active) {
      ENVOY_LOG(info, "phase {} ('{}'): {} workers active, {} worker(s) retired to idle",
                phase_index, phase.name(), active_workers, prior_active - active_workers);
    }
    std::vector<std::future<PhaseDrainAck>> drain_acks;
    // FORK-ONLY (M2 burstfix): the phase's declared rps is its GLOBAL rate:
    // divide it across the phase's active workers so the paced total matches
    // the declaration (the remainder goes to the lowest worker indexes; a
    // declaration below the worker count paces 1 rps per worker rather than
    // constructing a zero-rate limiter). Legacy runs are untouched: this is
    // the phase path only -- without the division each active worker paced
    // the full declared rps and a phase's offered load multiplied by its
    // worker count.
    const uint32_t declared_rps = phase.requests_per_second();
    const uint32_t rps_base = declared_rps / active_workers;
    const uint32_t rps_remainder = declared_rps % active_workers;
    for (uint32_t w = 0; w < active_workers; ++w) {
      worker_begun_phases_[w].push_back(phase_index);
      nighthawk::client::PhaseSpec worker_phase = effective_phase;
      worker_phase.set_requests_per_second(rps_base > 0 ? rps_base + (w < rps_remainder ? 1 : 0)
                                                        : 1);
      drain_acks.push_back(
          workers_[w]->beginPhaseAsync(phase_index, worker_phase, final_phase));
    }
    activated_high_water = std::max(activated_high_water, active_workers);
    if (final_phase) {
      // The final phase keeps today's exact behavior: its sequencer stops
      // with the legacy exit-at-stop, and the workers are joined like any
      // legacy run.
      for (auto& worker : workers_) {
        worker->waitForCompletion();
      }
      orchestrator.markFinalPhaseComplete();
    } else {
      // The boundary: sequencers stop without exiting the dispatcher
      // (worker side), then a bounded non-destructive wait for in-flight
      // completion per worker. A deadline miss is censored and recorded;
      // the run continues into the next phase either way.
      // FORK-ONLY (M3
      // Task 6): the drain window is a live-update gate. The flag is set
      // by the worker's own sequencer-stop observer (an early termination
      // predicate can end the phase before its nominal duration, so the
      // orchestrator cannot time the window itself) and cleared here, when
      // the boundary's drain is fully collected.
      // FORK-ONLY (M2 burstfix): the drain budget bounds the drain itself,
      // measured from the sequencer STOP. The stop lands at the phase's
      // declared duration (the duration termination predicate), so the wait
      // for the drain acks must first cover the rest of the phase;
      // bounding the whole wait by --timeout censored a healthy phase at
      // the cap and began the successor while the predecessor's sequencer
      // was still running -- its remaining load then landed in the
      // successor's slab, reading as an rps overshoot (the T8
      // close-blocker). +1 s slop for stop/teardown.
      const std::chrono::nanoseconds phase_duration_ns =
          std::chrono::seconds(phase.duration().seconds()) +
          std::chrono::nanoseconds(phase.duration().nanos());
      const std::chrono::milliseconds stop_grace(
          std::chrono::duration_cast<std::chrono::milliseconds>(phase_duration_ns) +
          std::chrono::milliseconds(1000));
      orchestrator.collectDrainAcks(drain_acks, drain_budget, phase_index, stop_grace);
      setBoundaryDrainInProgress(false);
    }
  }

  // FORK-ONLY (We-Amp): fold the per-phase gate-refusal counts over all
  // workers (the workers are joined once the final phase completed; M2 Task
  // 4's PhaseMetadata surfaces these as gate_exceeded). The workers keep
  // their per-phase counts positional over the phases they began; map the
  // positions through worker_begun_phases_ (a removed-and-rejoined worker
  // begins a non-contiguous set).
  phase_gate_exceeded_.assign(orchestrator.phaseCount(), 0);
  for (size_t w = 0; w < workers_.size(); ++w) {
    const std::vector<uint64_t> worker_counts = workers_[w]->phaseGateExceededCounts();
    const std::vector<uint32_t>& begun = worker_begun_phases_[w];
    for (size_t p = 0; p < worker_counts.size() && p < begun.size(); ++p) {
      if (begun[p] < phase_gate_exceeded_.size()) {
        phase_gate_exceeded_[begun[p]] += worker_counts[p];
      }
    }
  }

  boundary_timings_ = orchestrator.boundaryTimings();
}

nighthawk::client::LiveSnapshot ProcessImpl::toLiveSnapshot() {
  std::vector<nighthawk::client::WorkerLiveSnapshotPartial> partials;
  {
    std::lock_guard<std::mutex> lock(live_partials_mutex_);
    partials = std::move(live_partials_);
    live_partials_.clear();
  }
  // FORK-ONLY (We-Amp): the fold lives in LivePartialAggregator, which honors
  // the snapshot-acquisition-vs-slab-swap seam: every partial carries the
  // phase id stamped on its worker dispatcher, statistics never fold across
  // different stamps, and the snapshot is labeled with the newest stamp seen
  // ("main" until a worker advanced). The worker stamp, not the orchestrator
  // view, labels the snapshot: a partial is only ever read after it was
  // fully computed and stamped.
  for (nighthawk::client::WorkerLiveSnapshotPartial& partial : partials) {
    live_aggregator_.addPartial(std::move(partial));
  }
  // FORK-ONLY (We-Amp): M3 Task 6 -- the snapshot's epoch_id is the run's
  // current knob epoch, tracked from the per-worker partial stamps by the
  // aggregator (empty before the run's first applied change; repurposed
  // from the execution-id semantics per the A2 ruling in live.proto).
  return live_aggregator_.snapshot();
}

void ProcessImpl::configureComponentLogLevels(spdlog::level::level_enum level) {
  // TODO(oschaaf): Add options to tweak the log level of the various log tags
  // that are available.
  Envoy::Logger::Registry::setLogLevel(level);
  Envoy::Logger::Logger* logger_to_change = Envoy::Logger::Registry::logger("main");
  logger_to_change->setLevel(level);
}

std::vector<StatisticPtr>
ProcessImpl::vectorizeStatisticPtrMap(const StatisticPtrMap& statistics) const {
  std::vector<StatisticPtr> v;
  for (const auto& statistic : statistics) {
    // Clone the original statistic into a new one.
    auto new_statistic =
        statistic.second->createNewInstanceOfSameType()->combine(*(statistic.second));
    new_statistic->setId(statistic.first);
    v.push_back(std::move(new_statistic));
  }
  return v;
}

std::vector<StatisticPtr>
ProcessImpl::mergeWorkerStatistics(const std::vector<ClientWorkerPtr>& workers) const {
  // First we init merged_statistics with newly created statistics instances.
  // We do that by adding the same amount of Statistic instances that the first worker has.
  // (We always have at least one worker, and all workers have the same number of Statistic
  // instances associated to them, in the same order).
  std::vector<StatisticPtr> merged_statistics;
  StatisticPtrMap w0_statistics;
  if (!workers.empty()) {
    w0_statistics = workers[0]->statistics();
  }
  for (const auto& w0_statistic : w0_statistics) {
    auto new_statistic = w0_statistic.second->createNewInstanceOfSameType();
    new_statistic->setId(w0_statistic.first);
    merged_statistics.push_back(std::move(new_statistic));
  }

  // Merge the statistics of all workers into the statistics vector we initialized above.
  for (auto& w : workers) {
    uint32_t i = 0;
    for (const auto& wx_statistic : w->statistics()) {
      auto merged = merged_statistics[i]->combine(*(wx_statistic.second));
      merged->setId(merged_statistics[i]->id());
      merged_statistics[i] = std::move(merged);
      i++;
    }
  }
  return merged_statistics;
}

// FORK-ONLY (We-Amp): run-total statistics for a multi-phase run (M2 Task 4).
// The global Result of a phase run must carry every sample cumulatively, but
// the workers' live sets were retired into slabs by the end-of-run finalize
// (the last phase's set was reinstalled empty). Fold every benchmark-client
// slab plus every phase's sequencer statistics into one id-keyed map: the
// run total is exactly the combine of all phase slabs (count conservation,
// see the PhaseStatisticSlab fold in the per-phase result groups below).
std::vector<StatisticPtr> ProcessImpl::mergePhaseTotalsStatistics() const {
  std::map<std::string, StatisticPtr> totals;
  for (const auto& worker : workers_) {
    for (const PhaseStatisticSlab& slab : worker->benchmarkPhaseSlabs()) {
      for (const auto& [id, statistic] : slab.statistics) {
        combineStatisticIntoMap(totals, id, *statistic);
      }
    }
    const std::vector<StatisticPtrMap>& phase_statistics = worker->phaseStatistics();
    for (uint32_t phase_index = 0; phase_index < phase_statistics.size(); ++phase_index) {
      for (const auto& [id, statistic] : phase_statistics[phase_index]) {
        combineStatisticIntoMap(totals, id, *statistic);
      }
    }
  }
  std::vector<StatisticPtr> merged;
  for (auto& [id, statistic] : totals) {
    statistic->setId(id);
    merged.push_back(std::move(statistic));
  }
  return merged;
}

void ProcessImpl::addTracingCluster(envoy::config::bootstrap::v3::Bootstrap& bootstrap,
                                    const Uri& uri) const {
  auto* cluster = bootstrap.mutable_static_resources()->add_clusters();
  cluster->set_name("tracing");
  cluster->mutable_connect_timeout()->set_seconds(options_.timeout().count());
  cluster->set_type(
      envoy::config::cluster::v3::Cluster::DiscoveryType::Cluster_DiscoveryType_STATIC);
  auto* load_assignment = cluster->mutable_load_assignment();
  load_assignment->set_cluster_name(cluster->name());
  auto* socket = cluster->mutable_load_assignment()
                     ->add_endpoints()
                     ->add_lb_endpoints()
                     ->mutable_endpoint()
                     ->mutable_address()
                     ->mutable_socket_address();
  socket->set_address(uri.address()->ip()->addressAsString());
  socket->set_port_value(uri.port());
}

void ProcessImpl::setupTracingImplementation(envoy::config::bootstrap::v3::Bootstrap& bootstrap,
                                             const Uri& uri) const {
#ifdef ZIPKIN_ENABLED
  auto* http = bootstrap.mutable_tracing()->mutable_http();
  auto scheme = uri.scheme();
  const std::string kTracingClusterName = "tracing";
  http->set_name(fmt::format("envoy.{}", scheme));
  RELEASE_ASSERT(scheme == "zipkin", "Only zipkin is supported");
  envoy::config::trace::v3::ZipkinConfig config;
  config.mutable_collector_cluster()->assign(kTracingClusterName);
  config.mutable_collector_endpoint()->assign(std::string(uri.path()));
  config.set_collector_endpoint_version(envoy::config::trace::v3::ZipkinConfig::HTTP_JSON);
  config.mutable_shared_span_context()->set_value(true);
  std::ignore = http->mutable_typed_config()->PackFrom(config);
#else
  ENVOY_LOG(error, "Not build with any tracing support");
  UNREFERENCED_PARAMETER(bootstrap);
  UNREFERENCED_PARAMETER(uri);
#endif
}

void ProcessImpl::maybeCreateTracingDriver(const envoy::config::trace::v3::Tracing& configuration) {
  if (configuration.has_http()) {
#ifdef ZIPKIN_ENABLED
    std::string type = configuration.http().name();
    ENVOY_LOG(info, "loading tracing driver: {}", type);
    // Envoy::Server::Configuration::TracerFactory would be useful here to create the right
    // tracer implementation for us. However that ends up needing a Server::Instance to be passed
    // in which we do not have, and creating a fake for that means we risk code-churn because of
    // upstream code changes.
    auto& factory =
        Envoy::Config::Utility::getAndCheckFactory<Envoy::Server::Configuration::TracerFactory>(
            configuration.http());
    Envoy::ProtobufTypes::MessagePtr message = Envoy::Config::Utility::translateToFactoryConfig(
        configuration.http(), Envoy::ProtobufMessage::getStrictValidationVisitor(), factory);
    const auto* zipkin_config =
        Envoy::Protobuf::DynamicCastToGenerated<const envoy::config::trace::v3::ZipkinConfig>(
            message.get());
    Envoy::Tracing::DriverPtr zipkin_driver =
        std::make_unique<Envoy::Extensions::Tracers::Zipkin::Driver>(
            *zipkin_config, server_->serverFactoryContext());
    tracer_ = std::make_unique<Envoy::Tracing::TracerImpl>(std::move(zipkin_driver), *local_info_);
#else
    ENVOY_LOG(error, "Not build with any tracing support");
#endif
  }
}

void ProcessImpl::setupStatsSinks(const envoy::config::bootstrap::v3::Bootstrap& bootstrap,
                                  std::list<std::unique_ptr<Envoy::Stats::Sink>>& stats_sinks) {
  for (const envoy::config::metrics::v3::StatsSink& stats_sink : bootstrap.stats_sinks()) {
    ENVOY_LOG(info, "loading stats sink configuration in Nighthawk");
    // Envoy's own stats sink registry is consulted first: it holds the linked
    // `envoy.stat_sinks.*` factories, which is the registry users configure in
    // their sink configs (see
    // https://github.com/envoyproxy/nighthawk/issues/1607). The lookup is by
    // typed config type URL, which is how Envoy's own server resolves sinks.
    Envoy::Server::Configuration::StatsSinkFactory* envoy_factory =
        Envoy::Config::Utility::getAndCheckFactory<Envoy::Server::Configuration::StatsSinkFactory>(
            stats_sink, /*is_optional=*/true);
    if (envoy_factory != nullptr) {
      Envoy::ProtobufTypes::MessagePtr message = Envoy::Config::Utility::translateToFactoryConfig(
          stats_sink, Envoy::ProtobufMessage::getStrictValidationVisitor(), *envoy_factory);
      absl::StatusOr<Envoy::Stats::SinkPtr> sink_or_error =
          envoy_factory->createStatsSink(*message, server_->serverFactoryContext());
      if (!sink_or_error.ok()) {
        Envoy::ExceptionUtil::throwEnvoyException(
            absl::StrCat("Failed to create stats sink '", stats_sink.name(),
                         "': ", sink_or_error.status().message()));
      }
      stats_sinks.emplace_back(std::move(sink_or_error).value());
      continue;
    }
    // Fall back to Nighthawk's own stats sink registry. Configs that match
    // neither registry keep failing fast with an error naming the sink.
    auto& factory =
        Envoy::Config::Utility::getAndCheckFactory<NighthawkStatsSinkFactory>(stats_sink);
    stats_sinks.emplace_back(factory.createStatsSink(store_root_.symbolTable()));
  }
  for (std::unique_ptr<Envoy::Stats::Sink>& sink : stats_sinks) {
    store_root_.addSink(*sink);
  }
}

bool ProcessImpl::runInternal(OutputCollector& collector, const UriPtr& tracing_uri,
                              const Envoy::Network::DnsResolverSharedPtr& dns_resolver,
                              const std::optional<Envoy::SystemTime>& scheduled_start) {
  const Envoy::SystemTime now = time_system_.systemTime();
  std::shared_ptr<Envoy::MainCommonBase> encap_main_common = nullptr;

  if (scheduled_start.value_or(now) < now) {
    ENVOY_LOG(error, "Scheduled execution date already transpired.");
    return false;
  }

  Bootstrap encap_bootstrap;

  if (!options_.tunnelUri().empty()) {
    // Spin up an envoy for tunnel encapsulation.

    UriImpl tunnel_uri(options_.tunnelUri());

    auto status_or_bootstrap =
        createEncapBootstrap(options_, tunnel_uri, *dispatcher_.get(), dns_resolver);
    if (!status_or_bootstrap.ok()) {
      ENVOY_LOG(error, status_or_bootstrap.status().ToString());
      return false;
    }
    encap_bootstrap = *status_or_bootstrap;
  }

  std::function<void(sem_t&)> envoy_routine = [this, &encap_main_common,
                                               &encap_bootstrap](sem_t& nighthawk_control_sem) {
    const Envoy::OptionsImpl::HotRestartVersionCb hot_restart_version_cb = [](bool) {
      return "disabled";
    };

    std::string lower = absl::AsciiStrToLower(
        nighthawk::client::Verbosity::VerbosityOptions_Name(options_.verbosity()));

    Envoy::OptionsImpl envoy_options({"encap_envoy"}, hot_restart_version_cb,
                                     spdlog::level::from_str(lower));

    ENVOY_LOG(info, encap_bootstrap.DebugString());
    envoy_options.setConfigProto(encap_bootstrap);
    // for now, match the concurrency of nighthawk
    envoy_options.setConcurrency(number_of_workers_);

    Envoy::ProdComponentFactory prod_component_factory;
    auto listener_test_hooks = std::make_unique<Envoy::DefaultListenerHooks>();

    if (!options_.tunnelUri().empty()) {
      // Spin up an envoy for tunnel encapsulation.
      try {
        encap_main_common = std::make_shared<Envoy::MainCommonBase>(
            envoy_options, time_system_, *listener_test_hooks, prod_component_factory,
            std::make_unique<Envoy::PlatformImpl>(),
            std::make_unique<Envoy::Random::RandomGeneratorImpl>(), nullptr);

        // spin up envoy thread that first manages envoy.
        auto startup_envoy_thread_ptr =
            encap_main_common->server()->lifecycleNotifier().registerCallback(
                NighthawkLifecycleNotifierImpl::Stage::PostInit, [&nighthawk_control_sem]() {
                  // signal nighthawk to start.
                  sem_post(&nighthawk_control_sem);
                });
        encap_main_common->run();
      } catch (const Envoy::EnvoyException& ex) {
        ENVOY_LOG(error, "error caught by envoy: {}", ex.what());
        // let nighthawk start and close envoy process
        sem_post(&nighthawk_control_sem);
      }
    } else {
      sem_post(&nighthawk_control_sem);
    }
  };

  bool result = true;
  std::function<void()> nigthawk_fn = [this, &result, &dns_resolver, &scheduled_start,
                                       &tracing_uri]() {
    {
      auto guard = std::make_unique<Envoy::Thread::LockGuard>(workers_lock_);
      if (cancelled_) {
        return;
      }
      shutdown_ = false;

      // Needs to happen as early as possible (before createWorkers()) in the instantiation to
      // preempt the objects that require stats.
      if (!options_.statsSinks().empty()) {
        absl::StatusOr<Envoy::Stats::TagProducerPtr> producer_or_error =
            Envoy::Stats::TagProducerImpl::createTagProducer(bootstrap_.stats_config(),
                                                             envoy_options_.statsTags());
        if (!producer_or_error.ok()) {
          ENVOY_LOG(error, "createTagProducer failed. Received bad status: {}",
                    producer_or_error.status());
          result = false;
          return;
        }
        store_root_.setTagProducer(std::move(producer_or_error.value()));
      }

      absl::Status workers_status = createWorkers(number_of_workers_, scheduled_start);
      if (!workers_status.ok()) {
        ENVOY_LOG(error, "createWorkers failed. Received bad status: {}", workers_status.message());
        result = false;
        return;
      }
      tls_.registerThread(*dispatcher_, true);
      store_root_.initializeThreading(*dispatcher_, tls_);

      // FORK-ONLY (We-Amp): M2 Task 5, per the M2 Task 1 gate verdict: the
      // loader is created with an admin layer so raise-only breaker overrides
      // can be merged mid-run via mergeValues (with empty layers the merge is
      // refused with "No admin layer specified"). The layer starts empty, so
      // every runtime read behaves exactly as before: the breaker config
      // baked at bootstrap remains the getInteger fallback.
      envoy::config::bootstrap::v3::LayeredRuntime runtime_config;
      auto* admin_layer = runtime_config.add_layers();
      admin_layer->set_name("nighthawk_admin_overrides");
      admin_layer->mutable_admin_layer();
      absl::StatusOr<Envoy::Runtime::LoaderPtr> loader = Envoy::Runtime::LoaderImpl::create(
          *dispatcher_, tls_, runtime_config, *local_info_, store_root_, generator_,
          Envoy::ProtobufMessage::getStrictValidationVisitor(), *api_);

      if (!loader.ok()) {
        ENVOY_LOG(error, "create runtime loader failed. Received bad status: {}", loader.status());
        result = false;
        return;
      }

      runtime_loader_ = *std::move(loader);

      server_ = std::make_unique<NighthawkServerInstance>(
          admin_, *api_, init_manager_, *dispatcher_, access_log_manager_, envoy_options_,
          *runtime_loader_.get(), *singleton_manager_, tls_, *local_info_, validation_context_,
          grpc_context_, http_context_, router_context_, store_root_, secret_manager_);
      ssl_context_manager_ =
          std::make_unique<Envoy::Extensions::TransportSockets::Tls::ContextManagerImpl>(
              server_->serverFactoryContext());
      dynamic_cast<NighthawkServerFactoryContext*>(&server_->serverFactoryContext())
          ->setSslContextManager(*ssl_context_manager_);
      cluster_manager_factory_ = std::make_unique<ClusterManagerFactory>(
          server_->serverFactoryContext(),
          [dns_resolver]() -> Envoy::Network::DnsResolverSharedPtr { return dns_resolver; },
          quic_stat_names_);
      cluster_manager_factory_->setConnectionReuseStrategy(
          options_.h1ConnectionReuseStrategy() == nighthawk::client::H1ConnectionReuseStrategy::LRU
              ? Http1PoolImpl::ConnectionReuseStrategy::LRU
              : Http1PoolImpl::ConnectionReuseStrategy::MRU);
      cluster_manager_factory_->setPrefetchConnections(options_.prefetchConnections());
      if (tracing_uri != nullptr) {
        setupTracingImplementation(bootstrap_, *tracing_uri);
        addTracingCluster(bootstrap_, *tracing_uri);
      }
      ENVOY_LOG(debug, "Computed configuration: {}", absl::StrCat(bootstrap_));
      absl::StatusOr<Envoy::Upstream::ClusterManagerPtr> cluster_manager =
          cluster_manager_factory_->clusterManagerFromProto(bootstrap_);
      if (!cluster_manager.ok()) {
        ENVOY_LOG(error, "clusterManagerFromProto failed. Received bad status: {}",
                  cluster_manager.status().message());
        result = false;
        return;
      }
      cluster_manager_ = std::move(*cluster_manager);
      dynamic_cast<NighthawkServerFactoryContext*>(&server_->serverFactoryContext())
          ->setClusterManager(*cluster_manager_);
      absl::Status status = cluster_manager_->initialize(bootstrap_);
      if (!status.ok()) {
        ENVOY_LOG(error, "cluster_manager initialize failed. Received bad status: {}",
                  status.message());
        result = false;
        return;
      }
      maybeCreateTracingDriver(bootstrap_.tracing());
      cluster_manager_->setInitializedCb(
          [this]() -> void { init_manager_.initialize(init_watcher_); });

      absl::Status initialize_status = runtime_loader_->initialize(*cluster_manager_);
      if (!initialize_status.ok()) {
        ENVOY_LOG(error, "runtime_loader initialize failed. Received bad status: {}",
                  initialize_status.message());
        result = false;
        return;
      }

      std::list<std::unique_ptr<Envoy::Stats::Sink>> stats_sinks;
      setupStatsSinks(bootstrap_, stats_sinks);
      std::chrono::milliseconds stats_flush_interval = std::chrono::milliseconds(
          Envoy::DurationUtil::durationToMilliseconds(bootstrap_.stats_flush_interval()));

      ENVOY_LOG(error, bootstrap_.DebugString());

      if (!options_.statsSinks().empty()) {
        // There should be only a single live flush worker instance at any time.
        flush_worker_ = std::make_unique<FlushWorkerImpl>(
            stats_flush_interval, *api_, tls_, store_root_, stats_sinks, *cluster_manager_);
        flush_worker_->start();
      }

      // FORK-ONLY (We-Amp): the run's start instant, for live-update
      // validation's remaining-run bound on legacy runs (M3 Task 2).
      {
        std::lock_guard<std::mutex> lock(current_phase_id_mutex_);
        run_start_monotonic_ns_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                      time_system_.monotonicTime().time_since_epoch())
                                      .count();
      }
      for (auto& w : workers_) {
        w->start();
      }
    }
  };
  encap_runner_ = std::make_shared<EncapsulationSubProcessRunner>(nigthawk_fn, envoy_routine);
  auto status = encap_runner_->Run();

  if (!result) {
    return result;
  }

  if (!status.ok()) {
    ENVOY_LOG(error, status);
    return false;
  }

  if (options_.hasPhaseVector()) {
    // FORK-ONLY (We-Amp): multi-phase run. The orchestrator drives the phase
    // boundaries from this thread (the run-calling thread that today joins
    // the workers at teardown) and ends by joining the workers, exactly like
    // the legacy run below.

    runPhases();

  } else {
    for (auto& w : workers_) {
      w->waitForCompletion();
    }
  }

  if (!options_.statsSinks().empty() && flush_worker_ != nullptr) {
    // Stop the running dispatcher in flush_worker_. Needs to be called after all
    // client workers are complete so that all the metrics can be flushed.
    flush_worker_->exitDispatcher();
    flush_worker_->waitForCompletion();
  }

  int i = 0;
  std::chrono::nanoseconds total_execution_duration = 0ns;
  std::optional<Envoy::SystemTime> first_acquisition_time = std::nullopt;
  // Maps registered user defined output plugin name to the output results for every worker's plugin
  // of that name.
  absl::flat_hash_map<std::string, std::vector<nighthawk::client::UserDefinedOutput>>
      user_defined_outputs_by_plugin{};
  for (auto& worker : workers_) {
    auto sequencer_execution_duration = worker->phase().sequencer().executionDuration();
    std::optional<Envoy::SystemTime> worker_first_acquisition_time =
        worker->phase().sequencer().rate_limiter().firstAcquisitionTime();
    if (worker_first_acquisition_time.has_value()) {
      first_acquisition_time =
          first_acquisition_time.has_value()
              ? std::min(first_acquisition_time.value(), worker_first_acquisition_time.value())
              : worker_first_acquisition_time.value();
    }
    std::vector<nighthawk::client::UserDefinedOutput> worker_user_defined_outputs =
        worker->getUserDefinedOutputResults();
    collectUserDefinedOutputs(user_defined_outputs_by_plugin, worker_user_defined_outputs);
    // We don't write per-worker results if we only have a single worker, because the global
    // results will be precisely the same.
    if (workers_.size() > 1) {
      StatisticFactoryImpl statistic_factory(options_);
      collector.addResult(fmt::format("worker_{}", i),
                          vectorizeStatisticPtrMap(worker->statistics()),
                          worker->threadLocalCounterValues(), sequencer_execution_duration,
                          worker_first_acquisition_time, worker_user_defined_outputs);
    }
    total_execution_duration += sequencer_execution_duration;
    i++;
  }

  // Note that above we use use counter values snapshotted by the workers right after its
  // execution completes. Here we query the live counters to get to the global numbers. To make
  // sure the global aggregated numbers line up, we must take care not to shut down the benchmark
  // client before we do this, as that will increment certain counters like connections closed,
  // etc.
  const std::map<std::string, uint64_t>& counters = Utility().mapCountersFromStore(
      store_root_, [](absl::string_view, uint64_t value) { return value > 0; });
  StatisticFactoryImpl statistic_factory(options_);
  std::vector<nighthawk::client::UserDefinedOutput> global_user_defined_outputs =
      compileGlobalUserDefinedPluginOutputs(user_defined_outputs_by_plugin,
                                            user_defined_output_factories_);
  if (workers_.size() > 0) {
    // FORK-ONLY (We-Amp): in a phase run the global Result folds the
    // retained per-phase slabs (the workers' live sets were retired empty by
    // the end-of-run finalize; the legacy fold would carry zero
    // benchmark-client samples). The legacy run keeps today's fold.
    collector.addResult("global",
                        options_.hasPhaseVector() ? mergePhaseTotalsStatistics()
                                                  : mergeWorkerStatistics(workers_),
                        counters, total_execution_duration / workers_.size(),
                        first_acquisition_time, global_user_defined_outputs);
  }
  if (options_.hasPhaseVector()) {
    // FORK-ONLY (We-Amp): the per-phase output dimension (M2 Task 4, spec
    // 8.1/8.2/8.4/8.6). One Result group per phase, combining the sequencer
    // statistics and the benchmark client's fresh per-phase statistic slabs
    // across the workers, stamped with the phase id; plus PhaseMetadata with
    // the claim-discipline counts (censoring counted, not hidden; the
    // completion ratio computed here, not in the consumer), the boundary
    // timing, and per-worker pool health surfaced from the engine's pool
    // counters. Reads of worker state happen after the workers have been
    // joined (same contract as the final counter snapshot above).
    for (uint32_t phase_index = 0; phase_index < options_.phases().size(); ++phase_index) {
      const nighthawk::client::PhaseSpec& phase = options_.phases()[phase_index];
      std::map<std::string, StatisticPtr> merged_statistics;
      std::chrono::nanoseconds total_phase_duration = 0ns;
      uint32_t contributing_workers = 0;
      uint64_t initiated = 0;
      uint64_t completed = 0;
      uint64_t stream_resets = 0;
      for (size_t w = 0; w < workers_.size(); ++w) {
        auto& worker = workers_[w];
        const std::vector<StatisticPtrMap>& phase_statistics = worker->phaseStatistics();
        // The worker's per-phase vectors are positional over the phases it
        // began (M2 Task 6 subsets: a removed-and-rejoined worker begins a
        // non-contiguous set); map this phase's slot through the begin
        // bookkeeping. Workers that never began this phase (removed before
        // it) contribute nothing to it -- their slabs already live in the
        // phases they ran.
        uint32_t slot = 0;
        bool began_this_phase = false;
        for (const uint32_t begun : worker_begun_phases_[w]) {
          if (begun == phase_index) {
            began_this_phase = true;
            break;
          }
          slot++;
        }
        if (!began_this_phase || slot >= phase_statistics.size() ||
            slot >= worker->phaseExecutionDurations().size()) {
          continue;
        }
        // Sequencer statistics of this phase, at the worker's positional
        // slot for it (mapped above: a removed-and-rejoined worker begins a
        // non-contiguous set, M2 Task 6).
        for (const auto& [id, statistic] : phase_statistics[slot]) {
          combineStatisticIntoMap(merged_statistics, id, *statistic);
        }
        // Benchmark-client slabs of this phase (matched by phase id; the
        // slabs retain every phase's set, including the run's final one).
        for (const PhaseStatisticSlab& slab : worker->benchmarkPhaseSlabs()) {
          if (slab.phase_id != phase.name()) {
            continue;
          }
          for (const auto& [id, statistic] : slab.statistics) {
            combineStatisticIntoMap(merged_statistics, id, *statistic);
          }
          initiated += slab.requests_initiated;
          completed += slab.requests_completed;
          stream_resets += slab.stream_resets;
        }
        total_phase_duration +=
            std::chrono::nanoseconds(worker->phaseExecutionDurations()[slot]);
        contributing_workers++;
      }
      if (contributing_workers == 0) {
        continue;
      }
      std::vector<StatisticPtr> merged;
      for (auto& [id, statistic] : merged_statistics) {
        merged.push_back(std::move(statistic));
      }
      collector.addResult(fmt::format("phase_{}_{}", phase_index, phase.name()), merged, {},
                          total_phase_duration / contributing_workers, std::nullopt, {},
                          phase.name());
      // The boundary timing recorded when this phase ENDED (absent for the
      // run's final phase, which ends at the run teardown).
      const PhaseBoundaryTiming* boundary = nullptr;
      const PhaseBoundaryTiming* prior_boundary = nullptr;
      for (const PhaseBoundaryTiming& timing : boundary_timings_) {
        if (timing.phase_index == phase_index) {
          boundary = &timing;
        } else if (phase_index > 0 && timing.phase_index == phase_index - 1) {
          prior_boundary = &timing;
        }
      }
      // Stragglers inherited from the predecessor boundary: at a censored
      // boundary the requests still in flight when the deadline fired
      // complete after the successor's swap and land in this phase's raw
      // completed delta. The predecessor slab records the in-flight count at
      // its retire (zero at uncensored boundaries); when that boundary was
      // censored, that count is exactly the straggler set the predecessor
      // already counted in its reset_or_timed_out, so exclude it here.
      uint64_t inherited_stragglers = 0;
      if (prior_boundary != nullptr && prior_boundary->drain_censored) {
        const std::string& predecessor_phase = options_.phases()[phase_index - 1].name();
        for (const auto& worker : workers_) {
          for (const PhaseStatisticSlab& slab : worker->benchmarkPhaseSlabs()) {
            if (slab.phase_id == predecessor_phase) {
              inherited_stragglers += slab.requests_in_flight_at_retire;
            }
          }
        }
      }
      // M2 Task 6: worker_count is this phase's ACTIVE subset (not the
      // bootstrapped fleet), and the worker-set delta flags come from the
      // subset bookkeeping (surfaced, never silent).
      nighthawk::client::PhaseMetadata metadata =
          BuildPhaseMetadata(phase_index, phase, boundary, phase_worker_subsets_[phase_index],
                             initiated, completed, stream_resets,
                             phase_index < phase_gate_exceeded_.size()
                                 ? phase_gate_exceeded_[phase_index]
                                 : 0,
                             inherited_stragglers);
      // One pool-health entry per worker ACTIVE in the phase (the prefix
      // subset); retired workers carry no phase traffic.
      const uint32_t phase_active_workers = phase_worker_subsets_[phase_index].worker_count;
      for (uint32_t worker_number = 0; worker_number < phase_active_workers; ++worker_number) {
        *metadata.add_pool_health() = readWorkerPoolHealth(store_root_, worker_number);
      }
      collector.addPhaseMetadata(std::move(metadata));
    }
  }
  if (counters.find("sequencer.failed_terminations") == counters.end()) {
    return true;
  } else {
    ENVOY_LOG(error, "Terminated early because of a failure predicate.");
    ENVOY_LOG(
        info,
        "Check the output for problematic counter values. The default Nighthawk failure predicates "
        "report failure if (1) Nighthawk could not connect to the target (see "
        "'benchmark.pool_connection_failure' counter; check the address and port number, and try "
        "explicitly setting --address-family v4 or v6, especially when using DNS; instead of "
        "localhost try 127.0.0.1 or ::1 explicitly), (2) the protocol was not supported by the "
        "target (see 'benchmark.stream_resets' counter; check http/https in the URI, --h2), (3) "
        "the target returned a 4xx or 5xx HTTP response code (see 'benchmark.http_4xx' and "
        "'benchmark.http_5xx' counters; check the URI path and the server config), or (4) a custom "
        "gRPC RequestSource failed. --failure-predicate can be used to relax expectations.");
    return false;
  }
}

bool ProcessImpl::run(OutputCollector& collector) {
  UriPtr tracing_uri;

  absl::StatusOr<Envoy::Network::DnsResolverSharedPtr> dns_resolver =
      dns_resolver_factory_.createDnsResolver(*dispatcher_, *api_, typed_dns_resolver_config_);
  if (!dns_resolver.ok()) {
    ENVOY_LOG(error, "Failed to create DNS resolver: {}", dns_resolver.status());
    return false;
  }
  try {
    if (options_.trace() != "") {
      tracing_uri = std::make_unique<UriImpl>(options_.trace());
      tracing_uri->resolve(*dispatcher_, *dns_resolver.value(),
                           Utility::translateFamilyOptionString(options_.addressFamily()));
    }
  } catch (const UriException& ex) {
    ENVOY_LOG(error,
              "URI exception (for example, malformed URI syntax, bad "
              "MultiTarget path, unresolvable host DNS): {}",
              ex.what());
    return false;
  }

  try {
    return runInternal(collector, tracing_uri, *dns_resolver, options_.scheduled_start());
  } catch (Envoy::EnvoyException& ex) {
    ENVOY_LOG(error, "Fatal EnvoyException exception: {}", ex.what());
    throw;
  } catch (NighthawkException& ex) {
    ENVOY_LOG(error, "Fatal NighthawkException exception: {}", ex.what());
    throw;
  }
}

void ProcessImpl::setupForHRTimers() {
  // We override the local environment to indicate to libevent that we favor precision over
  // efficiency. Note that it is also possible to do this at setup time via libevent's api's.
  // The upside of the approach below is that we are very loosely coupled and have a one-liner.
  // Getting to libevent for the other approach is going to introduce more code as we would need to
  // derive our own customized versions of certain Envoy concepts.
  putenv(const_cast<char*>("EVENT_PRECISE_TIMER=1"));
}

} // namespace Client
} // namespace Nighthawk
