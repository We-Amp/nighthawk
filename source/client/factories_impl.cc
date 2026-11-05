#include "source/client/factories_impl.h"
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>

#include "nighthawk/common/rate_limiter.h"
#include "nighthawk/common/rate_limiter_plugin_config_factory.h"
#include "nighthawk/request_source/request_source_plugin_config_factory.h"
#include "nighthawk/user_defined_output/user_defined_output_plugin.h"

#include "source/common/config/utility.h"
#include "source/common/http/header_map_impl.h"

#include "api/client/options.pb.h"

#include "source/client/benchmark_client_impl.h"
#include "source/client/output_collector_impl.h"
#include "source/client/output_formatter_impl.h"
#include "source/common/platform_util_impl.h"
#include "source/common/rate_limiter_impl.h"
#include "source/common/request_source_impl.h"
#include "source/common/sequencer_impl.h"
#include "source/common/statistic_impl.h"
#include "source/common/termination_predicate_impl.h"
#include "source/common/uri_impl.h"
#include "source/common/utility.h"
#include "source/request_source/request_options_list_plugin_impl.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {

OptionBasedFactoryImpl::OptionBasedFactoryImpl(const Options& options) : options_(options) {}

BenchmarkClientFactoryImpl::BenchmarkClientFactoryImpl(const Options& options)
    : OptionBasedFactoryImpl(options) {}

BenchmarkClientPtr BenchmarkClientFactoryImpl::create(
    Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
    Envoy::Upstream::ClusterManagerPtr& cluster_manager, Envoy::Tracing::TracerSharedPtr& tracer,
    absl::string_view cluster_name, int worker_id, RequestSource& request_generator,
    std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins) const {
  StatisticFactoryImpl statistic_factory(options_);
  // While we lack options to configure which statistic backend goes where, we directly pass
  // StreamingStatistic for the stats that track response sizes. Ideally we would have options
  // for this to route the right stat to the right backend (HdrStatistic, SimpleStatistic,
  // NullStatistic).
  // TODO(#292): Create options and have the StatisticFactory consider those when instantiating
  // statistics.
  // FORK-ONLY (We-Amp): the same construction is captured as the fresh-set
  // provider the benchmark client uses at each phase boundary (spec 8.1:
  // fresh-per-phase statistics come from the same factory), so every phase's
  // set has the same backends as the initial one.
  const auto build_statistic_set = [statistic_factory, &scope,
                                    worker_id]() -> BenchmarkClientStatistic {
    return BenchmarkClientStatistic(
        statistic_factory.create(), statistic_factory.create(),
        std::make_unique<StreamingStatistic>(), std::make_unique<StreamingStatistic>(),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id),
        std::make_unique<SinkableHdrStatistic>(scope, worker_id));
  };
  BenchmarkClientStatistic statistic = build_statistic_set();
  auto benchmark_client = std::make_unique<BenchmarkClientHttpImpl>(
      api, dispatcher, scope, statistic, options_.protocol(), cluster_manager, tracer, cluster_name,
      request_generator.get(),
      // FORK-ONLY (We-Amp): with a phase vector the in-flight gates are
      // active regardless of open-loop mode (M2 Task 5: phase boundaries are
      // not live turns -- the gates enforce the phase's declared ceiling).
      !options_.openLoop() || options_.hasPhaseVector(),
      options_.responseHeaderWithLatencyInput(), std::move(user_defined_output_plugins),
      build_statistic_set);
  benchmark_client->setConnectionLimit(options_.connections());
  benchmark_client->setMaxPendingRequests(options_.maxPendingRequests());
  benchmark_client->setMaxActiveRequests(options_.maxActiveRequests());
  // FORK-ONLY (We-Amp): a start refused by the gates is shed and counted --
  // never queued (M2 Task 5 refuse-and-count ruling). Only phase-driven runs
  // count: in a legacy closed-loop run a refusal is backpressure (the request
  // is retried, not shed) and must not count as shed load.
  benchmark_client->setCountGateExceeded(options_.hasPhaseVector());
  benchmark_client->setMaxRequestsPerConnection(options_.maxRequestsPerConnection());
  benchmark_client->setTimeout(options_.timeout());

  return benchmark_client;
}

SequencerFactoryImpl::SequencerFactoryImpl(const Options& options)
    : OptionBasedFactoryImpl(options) {}

SequencerPtr SequencerFactoryImpl::create(
    Envoy::TimeSource& time_source, Envoy::Event::Dispatcher& dispatcher,
    const SequencerTarget& sequencer_target, TerminationPredicatePtr&& termination_predicate,
    Envoy::Stats::Scope& scope, const Envoy::MonotonicTime scheduled_starting_time,
    Envoy::Api::Api& api, std::optional<uint32_t> requests_per_second_override,
    LiveTunableRateLimiter** captured_live_tunable) const {
  StatisticFactoryImpl statistic_factory(options_);
  RateLimiterPtr rate_limiter;
  // FORK-ONLY (We-Amp): M3 Task 3 -- the captured pre-decorator capability
  // handle; exported through the caller's out-param below.
  LiveTunableRateLimiter* captured_handle = nullptr;

  // Check if there is a rate limiter plugin to load and use.
  if (options_.rateLimiterPluginConfig().has_value()) {
    absl::StatusOr<RateLimiterPtr> plugin_or =
        LoadRateLimiterPlugin(options_.rateLimiterPluginConfig().value(), api, time_source);
    if (!plugin_or.ok()) {
      throw NighthawkException(
          absl::StrCat("Rate Limiter plugin loading error: ", plugin_or.status().message()));
    }
    rate_limiter = std::move(plugin_or.value());
    // FORK-ONLY (We-Amp): M3 Task 3 -- capture the live-tunable capability
    // handle to the constructed INNER limiter BEFORE the decorator chain
    // wraps it. A plugin limiter that does not implement the interface (or a
    // load that produced something else) captures null: the registry then
    // fails a live update honestly at lookup, never silently.
    if (captured_live_tunable != nullptr) {
      *captured_live_tunable = dynamic_cast<LiveTunableRateLimiter*>(rate_limiter.get());
      captured_handle = *captured_live_tunable;
    }
    rate_limiter = std::make_unique<ScheduledStartingRateLimiter>(std::move(rate_limiter),
                                                                  scheduled_starting_time);

    // If no rate limiter plugin is set, use the default linear rate limiter.
  } else {
    // FORK-ONLY (We-Amp): a multi-phase run builds its per-phase sequencer at
    // the phase's declared rate; unset, the run-level option applies.
    Frequency frequency(requests_per_second_override.value_or(options_.requestsPerSecond()));
    // FORK-ONLY (We-Amp): M3 Task 3 -- capture the handle to the constructed
    // INNER LinearRateLimiter BEFORE ScheduledStartingRateLimiter (and the
    // optional Bursting/DistributionSampling decorators below) wrap it.
    auto linear_rate_limiter = std::make_unique<LinearRateLimiter>(time_source, frequency);
    if (captured_live_tunable != nullptr) {
      *captured_live_tunable = linear_rate_limiter.get();
    }
    captured_handle = linear_rate_limiter.get();
    rate_limiter = std::make_unique<ScheduledStartingRateLimiter>(std::move(linear_rate_limiter),
                                                                  scheduled_starting_time);
    const uint64_t burst_size = options_.burstSize();

    if (burst_size) {
      rate_limiter = std::make_unique<BurstingRateLimiter>(std::move(rate_limiter), burst_size);
    }

    const std::chrono::nanoseconds jitter_uniform = options_.jitterUniform();
    if (jitter_uniform.count() > 0) {
      rate_limiter = std::make_unique<DistributionSamplingRateLimiterImpl>(
          std::make_unique<UniformRandomDistributionSamplerImpl>(jitter_uniform.count()),
          std::move(rate_limiter));
    }
  }

  return std::make_unique<SequencerImpl>(
      platform_util_, dispatcher, time_source, std::move(rate_limiter), sequencer_target,
      statistic_factory.create(), statistic_factory.create(), options_.sequencerIdleStrategy(),
      std::move(termination_predicate), scope,
      // FORK-ONLY (We-Amp): M3 Task 3 -- the sequencer holds the captured
      // pre-decorator handle and routes live updates through it, posted on
      // its own dispatcher.
      captured_handle);
}

absl::StatusOr<RateLimiterPtr> SequencerFactoryImpl::LoadRateLimiterPlugin(
    const envoy::config::core::v3::TypedExtensionConfig& config, Envoy::Api::Api& api,
    Envoy::TimeSource& time_source) const {
  try {
    auto& config_factory =
        Envoy::Config::Utility::getAndCheckFactoryByName<RateLimiterPluginConfigFactory>(
            config.name());
    return config_factory.createRateLimiterPlugin(config.typed_config(), api, time_source,
                                                  options_);
  } catch (const Envoy::EnvoyException& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Could not load plugin: ", config.name(), ": ", e.what()));
  }
}

StatisticFactoryImpl::StatisticFactoryImpl(const Options& options)
    : OptionBasedFactoryImpl(options) {}

StatisticPtr StatisticFactoryImpl::create() const { return std::make_unique<HdrStatistic>(); }

OutputFormatterPtr OutputFormatterFactoryImpl::create(
    const nighthawk::client::OutputFormat_OutputFormatOptions output_format) const {
  switch (output_format) {
  case nighthawk::client::OutputFormat::HUMAN:
    return std::make_unique<Client::ConsoleOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::JSON:
    return std::make_unique<Client::JsonOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::YAML:
    return std::make_unique<Client::YamlOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::DOTTED:
    return std::make_unique<Client::DottedStringOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::FORTIO:
    return std::make_unique<Client::FortioOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::EXPERIMENTAL_FORTIO_PEDANTIC:
    return std::make_unique<Client::FortioPedanticOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::CSV:
    return std::make_unique<Client::CsvOutputFormatterImpl>();
  case nighthawk::client::OutputFormat::PROMETHEUS:
    return std::make_unique<Client::PrometheusOutputFormatterImpl>();
  default:
    PANIC("not reached");
  }
}

RequestSourceFactoryImpl::RequestSourceFactoryImpl(const Options& options, Envoy::Api::Api& api)
    : OptionBasedFactoryImpl(options), api_(api) {}

void RequestSourceFactoryImpl::setRequestHeader(Envoy::Http::RequestHeaderMap& header,
                                                absl::string_view key,
                                                absl::string_view value) const {
  auto lower_case_key = Envoy::Http::LowerCaseString(std::string(key));
  header.remove(lower_case_key);
  // TODO(oschaaf): we've performed zero validation on the header key/value.
  header.addCopy(lower_case_key, std::string(value));
}

RequestSourcePtr
RequestSourceFactoryImpl::create(const Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                                 Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
                                 absl::string_view service_cluster_name) const {
  Envoy::Http::RequestHeaderMapPtr header = Envoy::Http::RequestHeaderMapImpl::create();
  if (options_.uri().has_value()) {
    // We set headers based on the URI, but we don't have all the prerequisites to call the
    // resolver to validate the address at this stage. Resolving is performed during a later stage
    // and it will fail if the address is incorrect.
    UriImpl uri(options_.uri().value());
    header->setPath(uri.path());
    header->setHost(uri.hostAndPort());
    header->setScheme(uri.scheme() == "https" ? Envoy::Http::Headers::get().SchemeValues.Https
                                              : Envoy::Http::Headers::get().SchemeValues.Http);
  } else {
    header->setPath(options_.multiTargetPath());
    header->setHost(
        "host-not-supported-in-multitarget-mode"); // We set a default here because Nighthawk Test
                                                   // Server fails when Host is not set. If you send
                                                   // traffic to non-Nighthawk Test Server backends,
                                                   // you can override this with a custom Host on
                                                   // the command line, provided that your system
                                                   // works with the same Host value sent to all
                                                   // backends.
    header->setScheme(options_.multiTargetUseHttps()
                          ? Envoy::Http::Headers::get().SchemeValues.Https
                          : Envoy::Http::Headers::get().SchemeValues.Http);
  }

  header->setMethod(envoy::config::core::v3::RequestMethod_Name(options_.requestMethod()));
  std::string body = options_.requestBody();
  if (!body.empty()) {
    header->setContentLength(body.size());
  } else {
    const uint32_t content_length = options_.requestBodySize();
    if (content_length > 0) {
      header->setContentLength(content_length);
    }
  }

  auto request_options = options_.toCommandLineOptions()->request_options();
  for (const auto& option_header : request_options.request_headers()) {
    setRequestHeader(*header, option_header.header().key(), option_header.header().value());
  }
  if (!options_.requestSource().empty()) {
    RELEASE_ASSERT(!service_cluster_name.empty(), "expected cluster name to be set");
    // We pass in options_.requestsPerSecond() as the header buffer length so the grpc client
    // will shoot for maintaining an amount of headers of at least one second.
    return std::make_unique<RemoteRequestSourceImpl>(cluster_manager, dispatcher, scope,
                                                     service_cluster_name, std::move(header),
                                                     options_.requestsPerSecond());
  } else if (options_.requestSourcePluginConfig().has_value()) {
    absl::StatusOr<RequestSourcePtr> plugin_or = LoadRequestSourcePlugin(
        options_.requestSourcePluginConfig().value(), api_, std::move(header));
    if (!plugin_or.ok()) {
      throw NighthawkException(
          absl::StrCat("Request Source plugin loading error should have been caught "
                       "during input validation: ",
                       plugin_or.status().message()));
    }
    RequestSourcePtr request_source = std::move(plugin_or.value());
    return request_source;
  } else {
    return std::make_unique<StaticRequestSourceImpl>(std::move(header), UINT64_MAX,
                                                     std::move(body));
  }
}
absl::StatusOr<RequestSourcePtr> RequestSourceFactoryImpl::LoadRequestSourcePlugin(
    const envoy::config::core::v3::TypedExtensionConfig& config, Envoy::Api::Api& api,
    Envoy::Http::RequestHeaderMapPtr header) const {
  try {
    auto& config_factory =
        Envoy::Config::Utility::getAndCheckFactoryByName<RequestSourcePluginConfigFactory>(
            config.name());
    return config_factory.createRequestSourcePlugin(config.typed_config(), api, std::move(header));
  } catch (const Envoy::EnvoyException& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Could not load plugin: ", config.name(), ": ", e.what()));
  }
}

TerminationPredicateFactoryImpl::TerminationPredicateFactoryImpl(const Options& options)
    : OptionBasedFactoryImpl(options) {}

TerminationPredicatePtr TerminationPredicateFactoryImpl::create(
    Envoy::TimeSource& time_source, Envoy::Stats::Scope& scope,
    const Envoy::MonotonicTime scheduled_starting_time,
    std::optional<std::chrono::seconds> duration_override) const {
  // We'll always link a predicate which checks for requests to cancel.
  TerminationPredicatePtr root_predicate =
      std::make_unique<StatsCounterAbsoluteThresholdTerminationPredicateImpl>(
          scope.counterFromString("graceful_stop_requested"), 0,
          TerminationPredicate::Status::TERMINATE);

  TerminationPredicate* current_predicate = root_predicate.get();
  if (!options_.noDuration()) {
    // FORK-ONLY (We-Amp): a multi-phase run builds its per-phase termination
    // predicate from the phase's declared duration; unset, the run-level
    // option applies.
    const std::chrono::seconds duration = duration_override.value_or(options_.duration());
    current_predicate = &current_predicate->link(std::make_unique<DurationTerminationPredicateImpl>(
        time_source, duration, scheduled_starting_time));
  }

  current_predicate = linkConfiguredPredicates(*current_predicate, options_.failurePredicates(),
                                               TerminationPredicate::Status::FAIL, scope);
  linkConfiguredPredicates(*current_predicate, options_.terminationPredicates(),
                           TerminationPredicate::Status::TERMINATE, scope);

  return root_predicate;
}

TerminationPredicate* TerminationPredicateFactoryImpl::linkConfiguredPredicates(
    TerminationPredicate& last_predicate, const TerminationPredicateMap& predicates,
    const TerminationPredicate::Status termination_status, Envoy::Stats::Scope& scope) const {
  RELEASE_ASSERT(termination_status != TerminationPredicate::Status::PROCEED,
                 "PROCEED was unexpected");
  TerminationPredicate* current_predicate = &last_predicate;
  for (const auto& predicate : predicates) {
    ENVOY_LOG(trace, "Adding {} predicate for {} with threshold {}",
              termination_status == TerminationPredicate::Status::TERMINATE ? "termination"
                                                                            : "failure",
              predicate.first, predicate.second);
    current_predicate = &current_predicate->link(
        std::make_unique<StatsCounterAbsoluteThresholdTerminationPredicateImpl>(
            scope.counterFromString(predicate.first), predicate.second, termination_status));
  }
  return current_predicate;
}

} // namespace Client
} // namespace Nighthawk
