#pragma once

// FORK-ONLY (We-Amp): the live-tunable rate limiter capability (M3 Task 3,
// spec 4 A2 / 8.7). Implemented by the INNER limiters only; the decorators
// (ScheduledStartingRateLimiter, BurstingRateLimiter,
// DistributionSamplingRateLimiterImpl) neither implement nor forward it --
// that is the point of capturing the handle at the factory AFTER the inner
// limiter is constructed and BEFORE any decorator wraps it.

#include <chrono>
#include <functional>

#include "absl/status/status.h"
#include "envoy/common/pure.h"
#include "envoy/common/time.h"

namespace Nighthawk {

// Dispatch grace for a bend instant: an apply invoked within this window of
// t_bend (observed timer jitter is a few ms: ms-truncation in the bend
// timer's arm duration plus event-loop wake granularity) is honored;
// anything invoked meaningfully AHEAD of t_bend is rejected without
// mutation -- the caller must schedule the apply at the agreed bend instant
// (the sequencer's dispatcher-posted apply does).
constexpr std::chrono::nanoseconds kLiveUpdateBendGrace{10000000};

// The capability interface: implemented by the INNER limiters only. Decorators
// neither implement nor forward it -- that is the point of capturing BEFORE
// the wrap.
class LiveTunableRateLimiter {
public:
  virtual ~LiveTunableRateLimiter() = default;

  // Apply a live re-parameterization of the limiter's target rate to
  // `new_rps` (M3 Task 3: the NON-plugin plain-rps path; the plugin-path rps
  // knob is Task 5's -- C1: on the ramping limiter a target-frequency change
  // IS ramp re-parameterization). The re-solve preserves the operating point
  // (spec 8.7, linear degenerate case): the cumulative acquired count is
  // conserved at the bend, the instantaneous rate steps at the bend
  // (left-limit == old rate, right-limit == new rate), and there is no credit
  // windfall/drought -- a mid-second rps change must not hand out the
  // remainder of the OLD second's credit as a burst.
  //
  // t_bend is the ENGINE-AGREED monotonic instant (spec 8.7), not the
  // callee's now. Implementations anchor the re-solve at their own current
  // monotonic time when invoked (conservation is exact at the actual apply
  // instant), so callers MUST invoke at t_bend: an apply invoked ahead of
  // t_bend - kLiveUpdateBendGrace is rejected without touching the limiter.
  //
  // Down-bend clamp (interim contract, feeds Tasks 4/5): implementations
  // CLAMP internally at zero credit and never emit an acquisition sourced
  // from negative credit, until Task 5's formal negative-credit handling
  // lands.
  //
  // Returns an error when the limiter cannot honor the delta in its current
  // regime (honest per-worker failure; the orchestrator aggregates). A
  // rejected apply mutates nothing.
  virtual absl::Status ApplyLiveRps(Envoy::MonotonicTime t_bend, uint32_t new_rps) PURE;

  // Per-PHASE: the capability is (re)captured at each begin-phase (M2 T3's
  // fresh per-phase sequencer). A stale pre-boundary handle never survives a
  // begin-phase.
  //
  // A2b (Task 5): virtual absl::Status ApplyLiveRamp(...) and the real
  // plugin-path ApplyLiveRps land there (C1 -- same equations as the ramp
  // bend). Until then the ramping limiter's capture is CAPTURE-ONLY: its
  // ApplyLiveRps rejects honestly with the `pending ramp re-anchoring`
  // reason and mutates nothing.
};

// The worker-side apply surface the Task 6 fan-out consumes: posts onto the
// worker's own dispatcher (limiter state is dispatcher-confined, M0a Review
// Focus 5 discipline) and applies the re-parameterization at t_bend. The
// status callback fires on the worker dispatcher with the per-worker apply
// result.
using LiveApplyFn = std::function<void(Envoy::MonotonicTime t_bend, uint32_t new_rps,
                                       std::function<void(absl::Status)> callback)>;

} // namespace Nighthawk
