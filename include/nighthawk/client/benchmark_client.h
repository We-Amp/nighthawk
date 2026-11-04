#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "envoy/http/header_map.h"
#include "envoy/runtime/runtime.h"
#include "envoy/stats/store.h"

#include "nighthawk/common/operation_callback.h"
#include "nighthawk/common/statistic.h"
#include "nighthawk/common/uri.h"

namespace Nighthawk {
namespace Client {

using CompletionCallback = OperationCallback;

// FORK-ONLY (We-Amp): fresh-per-phase statistics (M2 A1, spec 8.1/8.2). One
// phase's retired benchmark-client statistic set, retained until end-of-run
// so per-phase Results can be emitted from it, plus the per-phase claim
// counts (boundary deltas of the client's cumulative counters). Keys of
// `statistics` are the canonical statistic ids used in the run output (e.g.
// "benchmark_http_client.latency_2xx").
struct PhaseStatisticSlab {
  // The phase the set measured, equal to the phase's PhaseSpec::name. The set
  // installed at client construction (before any phase began) is retired
  // under the id "main".
  std::string phase_id;
  std::map<std::string, StatisticPtr> statistics;
  // Requests initiated in this phase.
  uint64_t requests_initiated{0};
  // Requests completed while this phase's set was live. Exact at every
  // uncensored boundary (the drain waits for in-flight to reach zero before
  // the next phase's set is installed); at a censored boundary, stragglers
  // complete after the swap and land in the successor phase's raw delta --
  // PhaseMetadata adjusts them out again via requests_in_flight_at_retire
  // (see the PhaseMetadata proto comments).
  uint64_t requests_completed{0};
  // Stream resets observed while this phase's set was live (spec 8.2).
  uint64_t stream_resets{0};
  // The client's cumulative in-flight count at the moment this slab was
  // retired (the begin-phase swap of the successor, or the end-of-run
  // finalize). Zero at every uncensored boundary (the drain completed
  // before the swap); at a censored boundary it is the straggler set that
  // completes during the successor phase.
  uint64_t requests_in_flight_at_retire{0};
};

class BenchmarkClient {
public:
  virtual ~BenchmarkClient() = default;

  /**
   * Terminate will be called on the worker thread before it ends.
   */
  virtual void terminate() PURE;

  /**
   * Turns latency measurement on or off.
   *
   * @param measure_latencies true iff latencies should be measured.
   */
  virtual void setShouldMeasureLatencies(bool measure_latencies) PURE;

  /**
   * Gets the statistics, keyed by id.
   * @return StatisticPtrMap A map of Statistics keyed by id.
   */
  virtual StatisticPtrMap statistics() const PURE;

  // FORK-ONLY (We-Amp): fresh-per-phase statistics (M2 A1, spec 8.1/8.2).
  // Retires the live statistic set into its phase's slab and installs a fresh
  // set that `next_phase_id` names. Must run on the client's own dispatcher
  // (the worker's): the swap is serialized with every request start (which
  // binds the request's completion path to the current set's statistics), so
  // an in-flight request keeps writing into the slab of the phase that
  // initiated it, and with every live partial computation. The swap and the
  // partial never race; the aggregator only reads completed, immutable
  // partials stamped by the worker dispatcher.
  virtual void beginPhaseStatistics(absl::string_view next_phase_id) PURE;

  /**
   * The phase id whose slab the live statistic set currently feeds. "main"
   * until the first phase begins. Call only on the client's own dispatcher
   * (the worker's): the string is swapped by begin-phase on that dispatcher,
   * so reading it elsewhere is a race.
   */
  virtual const std::string& currentPhaseId() const PURE;

  /**
   * Slabs retired so far, in boundary order. Read cross-thread only after the
   * owning worker has been joined (same contract as the worker's final
   * counter snapshot).
   */
  virtual const std::vector<PhaseStatisticSlab>& phaseSlabs() const PURE;

  /**
   * Retires the live statistic set (the run's final phase) into its slab.
   * End-of-run only, on the worker dispatcher before the worker exits its run
   * loop; no request may start afterwards.
   */
  virtual void finalizePhaseStatistics() PURE;

  /**
   * Tries to start a request. In open-loop mode this MUST always return true.
   *
   * @param caller_completion_callback The callback the client must call back upon completion of a
   * successfully started request.
   *
   * @return true if the request could be started, otherwise the request could not be started, for
   * example due to resource limits
   */
  virtual bool tryStartRequest(CompletionCallback caller_completion_callback) PURE;

  /**
   * FORK-ONLY (We-Amp): the number of requests initiated but not yet
   * completed: the same in-flight accounting the client-side gates consult in
   * tryStartRequest. The phase-boundary drain waits for this to reach zero
   * (bounded); it never closes connections to get there.
   *
   * @return uint64_t the in-flight request count.
   */
  virtual uint64_t inFlightRequests() const PURE;

  /**
   * FORK-ONLY (We-Amp): per-phase in-flight gates (M2 Task 5). The gates
   * realize a phase's declared ceilings on top of circuit breakers that were
   * bootstrapped once at the max across phases: a start the gate refuses is
   * shed and counted (gateExceededCount), never queued -- an unbounded queue
   * would silently convert open loop into closed loop and distort the
   * measurement. The orchestrator applies the phase's effective values on the
   * worker dispatcher before the phase's sequencer starts.
   */
  virtual void setConnectionLimit(uint32_t connection_limit) PURE;
  virtual void setMaxPendingRequests(uint32_t max_pending_requests) PURE;
  virtual void setMaxActiveRequests(uint32_t max_active_requests) PURE;
  /**
   * FORK-ONLY (We-Amp): enables the gate-refusal count. Only phase-driven
   * runs enable it: in a legacy closed-loop run a refusal is backpressure
   * (the request is retried, not shed) and must not count as shed load.
   */
  virtual void setCountGateExceeded(bool count_gate_exceeded) PURE;
  /**
   * FORK-ONLY (We-Amp): the cumulative count of starts refused by the
   * in-flight gates since run start (only incremented when
   * setCountGateExceeded(true)).
   */
  virtual uint64_t gateExceededCount() const PURE;

  /**
   * @return const Envoy::Stats::Scope& the statistics scope associated the benchmark client.
   */
  virtual Envoy::Stats::Scope& scope() const PURE;

  /**
   * Determines if latency measurement is on.
   *
   * @return bool indicating if latency measurement is enabled.
   */
  virtual bool shouldMeasureLatencies() const PURE;

  /**
   * Get additional output generated by UserDefinedOutput plugins associated with this benchmark
   * client.
   *
   * @return a vector of protos, each the output result to a User Defined Output Plugin.
   */
  virtual std::vector<nighthawk::client::UserDefinedOutput>
  getUserDefinedOutputResults() const PURE;
};

using BenchmarkClientPtr = std::unique_ptr<BenchmarkClient>;

} // namespace Client
} // namespace Nighthawk
