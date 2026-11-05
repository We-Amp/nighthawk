#pragma once

#include <chrono>
#include <memory>
#include <optional>

#include "envoy/api/api.h"
#include "envoy/common/pure.h"
#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"
#include "envoy/upstream/cluster_manager.h"

#include "nighthawk/common/live_tunable_rate_limiter.h"
#include "nighthawk/common/platform_util.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/sequencer.h"
#include "nighthawk/common/statistic.h"
#include "nighthawk/common/termination_predicate.h"

namespace Nighthawk {

class SequencerFactory {
public:
  virtual ~SequencerFactory() = default;
  // FORK-ONLY (We-Amp): requests_per_second_override lets a multi-phase run
  // build a per-phase sequencer at the phase's declared rate; unset, the
  // run-level option is used exactly as before. Only the default linear rate
  // limiter honors the override.
  // FORK-ONLY (We-Amp): captured_live_tunable (M3 Task 3) -- when non-null,
  // receives the live-tunable capability handle captured AFTER the inner
  // limiter is constructed and BEFORE any decorator
  // (ScheduledStartingRateLimiter/Bursting/DistributionSampling) wraps it.
  // Null when the constructed limiter exposes no live-tunable capability
  // (e.g. a third-party rate limiter plugin that does not implement the
  // interface): a null handle must fail a live update honestly, never
  // silently.
  virtual SequencerPtr create(Envoy::TimeSource& time_source, Envoy::Event::Dispatcher& dispatcher,
                              const SequencerTarget& sequencer_target,
                              TerminationPredicatePtr&& termination_predicate,
                              Envoy::Stats::Scope& scope,
                              const Envoy::MonotonicTime scheduled_starting_time,
                              Envoy::Api::Api& api,
                              std::optional<uint32_t> requests_per_second_override =
                                  std::nullopt,
                              LiveTunableRateLimiter** captured_live_tunable = nullptr) const PURE;
};

class StatisticFactory {
public:
  virtual ~StatisticFactory() = default;
  virtual StatisticPtr create() const PURE;
};

class RequestSourceFactory {
public:
  virtual ~RequestSourceFactory() = default;
  virtual RequestSourcePtr create(const Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                                  Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
                                  absl::string_view service_cluster_name) const PURE;
};

class TerminationPredicateFactory {
public:
  virtual ~TerminationPredicateFactory() = default;
  // FORK-ONLY (We-Amp): duration_override lets a multi-phase run build a
  // per-phase termination predicate from the phase's declared duration;
  // unset, the run-level option is used exactly as before.
  virtual TerminationPredicatePtr
  create(Envoy::TimeSource& time_source, Envoy::Stats::Scope& scope,
         const Envoy::MonotonicTime scheduled_starting_time,
         std::optional<std::chrono::seconds> duration_override = std::nullopt) const PURE;
};

/**
 * Factory Interface to create Envoy::Stats::Sink in Nighthawk.
 * Implemented for each Envoy::Stats::Sink and registered via
 * Registry::registerFactory() or the convenience class RegisterFactory.
 */
class NighthawkStatsSinkFactory : public Envoy::Config::TypedFactory {
public:
  ~NighthawkStatsSinkFactory() override = default;

  /**
   * Create a particular Envoy::Stats::Sink implementation. If the
   * implementation is unable to produce a Sink with the provided parameters, it
   * should throw an EnvoyException. The returned pointer should always be
   * valid.
   * @param symbol_table supplies the symbol_table instance. For the definition
   * of SymbolTable, see envoy/include/envoy/stats/symbol_table.h.
   */
  virtual std::unique_ptr<Envoy::Stats::Sink>
  createStatsSink(Envoy::Stats::SymbolTable& symbol_table) PURE;

  std::string category() const override { return "nighthawk.stats_sinks"; }
};

} // namespace Nighthawk
