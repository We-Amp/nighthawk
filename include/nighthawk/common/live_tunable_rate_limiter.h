#pragma once

// FORK-ONLY (We-Amp): the live-tunable rate limiter capability (M3 Task 3,
// spec 4 A2 / 8.7). Implemented by the INNER limiters only; the decorators
// (ScheduledStartingRateLimiter, BurstingRateLimiter,
// DistributionSamplingRateLimiterImpl) neither implement nor forward it --
// that is the point of capturing the handle at the factory AFTER the inner
// limiter is constructed and BEFORE any decorator wraps it.

#include <chrono>
#include <functional>
#include <optional>

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

// A live ramp-shape change for the ramping (plugin) limiter (M3 Task 5, A2b).
// Absent fields preserve the current schedule's value: a target-only change
// re-anchors the same ramp shape under the new target (the C1 rps-on-plugin
// case); a ramp-time-only change steepens/shallows the ramp at the same
// target. The re-anchoring equations and the invariant semantics are the A2b
// design note's (spec 8.7): cumulative count conserved exactly at the bend,
// the artifact-free rate step, no credit windfall, silence bounded.
struct RampShapeChange {
  std::optional<std::chrono::nanoseconds> ramp_time;
  std::optional<uint32_t> target_rps;
};

// The capability interface: implemented by the INNER limiters only. Decorators
// neither implement nor forward it -- that is the point of capturing BEFORE
// the wrap.
class LiveTunableRateLimiter {
public:
  virtual ~LiveTunableRateLimiter() = default;

  // Apply a live re-parameterization of the limiter's target rate to
  // `new_rps`. The re-solve preserves the operating point (spec 8.7, the
  // linear degenerate case / the A2b note's Q6 corollary): the cumulative
  // acquired count is conserved exactly at the bend, the instantaneous rate
  // steps at the bend (left-limit == old rate, right-limit == the
  // re-anchored schedule's rate), and there is no credit windfall/drought --
  // a mid-second rps change must not hand out the remainder of the OLD
  // second's credit as a burst.
  //
  // On the ramping (plugin) limiter this IS ramp re-parameterization (C1):
  // the cumulative release schedule is a function of the target frequency,
  // so the rps knob re-anchors the ramp under the preserved ramp time -- the
  // same equations as ApplyLiveRamp (M3 Task 5, the A2b note's Q6).
  //
  // t_bend is the ENGINE-AGREED monotonic instant (spec 8.7), not the
  // callee's now. Implementations anchor the re-solve at their own current
  // monotonic time when invoked (conservation is exact at the actual apply
  // instant), so callers MUST invoke at t_bend: an apply invoked ahead of
  // t_bend - kLiveUpdateBendGrace is rejected without touching the limiter.
  //
  // Down-bend handling (the A2b note's R3 ruling): the re-anchoring origin
  // solve makes negative credit unreachable -- implementations hold at zero
  // credit while the new schedule accrues, never emit an acquisition sourced
  // from negative credit, and bound the resulting silence by two acquisition
  // intervals of the new schedule. The `acquireable_count_ > 0` gate is
  // mandatory regardless (defense in depth).
  //
  // Returns an error when the limiter cannot honor the delta in its current
  // regime (honest per-worker failure; the orchestrator aggregates). A
  // rejected apply mutates nothing.
  virtual absl::Status ApplyLiveRps(Envoy::MonotonicTime t_bend, uint32_t new_rps) PURE;

  // Apply a live RAMP-SHAPE bend (M3 Task 5, A2b; plugin path only -- the
  // plain linear limiter rejects honestly since a ramp delta requires the
  // ramp plugin, which the Task-2 whitelist validator already gates).
  // Implements the A2b note's piecewise re-anchoring: the new schedule's
  // origin is solved so its integrated releases from the origin to the bend
  // equal the acquired count exactly, with the bend's regime branch selected
  // by the acquired count vs the new ramp segment's integral (a mid-ramp bend
  // may re-anchor into the new schedule's post-ramp segment and vice versa).
  // Same t_bend / grace / error contract as ApplyLiveRps.
  virtual absl::Status ApplyLiveRamp(Envoy::MonotonicTime t_bend,
                                     const RampShapeChange& change) PURE;

  // Per-PHASE: the capability is (re)captured at each begin-phase (M2 T3's
  // fresh per-phase sequencer). A stale pre-boundary handle never survives a
  // begin-phase.
};

// One normalized knob delta on the apply path (the wire oneof, resolved;
// validation guarantees at least one field is set and never duplicates a
// knob). When both fields are set (a mixed request), the rps knob applies
// first, then the ramp change on top of the resulting schedule.
struct LiveKnobDelta {
  std::optional<uint32_t> new_rps;     // the requests_per_second knob (both paths)
  std::optional<RampShapeChange> ramp; // the ramp knob (plugin path only)
};

// The worker-side apply surface the Task 6 fan-out consumes: posts onto the
// worker's own dispatcher (limiter state is dispatcher-confined, M0a Review
// Focus 5 discipline) and applies the re-parameterization at t_bend. The
// status callback fires on the worker dispatcher with the per-worker apply
// result.
using LiveApplyFn = std::function<void(Envoy::MonotonicTime t_bend, const LiveKnobDelta& delta,
                                       std::function<void(absl::Status)> callback)>;

} // namespace Nighthawk
