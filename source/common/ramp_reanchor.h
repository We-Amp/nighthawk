#pragma once

// FORK-ONLY (We-Amp): the spec 8.7 piecewise ramp re-anchoring math (M3 Task
// 5). The equations are a literal transcription of the A2b design note (the
// operations repo's docs/superpowers/specs/2026-11-03-nighthawk-a2b-ramp-
// reanchoring-note.md). PURE: no Envoy types -- chrono and absl only, so the
// property tests drive it without an engine.
//
// The schedule family and its release-count forms mirror the stock
// LinearRampingRateLimiterImpl expressions verbatim, and the limiter
// evaluates CumulativeReleases() at runtime: one source of truth, so the
// solver and the limiter can never diverge in floating point.

#include <chrono>
#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace Nighthawk {
namespace RampReanchor {

// A ramp schedule: ramp time T (positive) to a target frequency f (rps >= 1).
struct RampSchedule {
  std::chrono::nanoseconds ramp_time;
  double target_rps;
};

// The cumulative release count of `schedule` at `elapsed` since its origin --
// the stock LinearRampingRateLimiterImpl forms, verbatim (same expressions,
// bit for bit):
//   ramping (e < T): round( e_s * (frac * f) / 2 ), frac = 1 - (T-e)/T
//   post-ramp (e >= T): round(T_ns * f_ns / 2) + round((e-T)_ns * f_ns)
int64_t CumulativeReleases(const RampSchedule& schedule, std::chrono::nanoseconds elapsed);

// The instantaneous release rate (rps) of `schedule` at `elapsed` since its
// origin: f*e/T in the ramping segment, f from T onward. 0 at/below the origin.
double InstantaneousRate(const RampSchedule& schedule, std::chrono::nanoseconds elapsed);

struct ReanchorResult {
  RampSchedule schedule;
  // The NEW schedule's elapsed at the bend instant: the count-conserving
  // solve of CumulativeReleases(schedule, new_origin_offset) ==
  // acquired_count, exactly on the stock rounded forms (spec 8.7(a) at the
  // discrete level, not merely continuously). The new origin sits at
  // t_bend - new_origin_offset; the first post-bend release lands on the new
  // schedule's own grid (no windfall), and the instantaneous rate right-limit
  // is InstantaneousRate(schedule, new_origin_offset) -- the A2b note's
  // artifact-free-step ruling (R2).
  std::chrono::nanoseconds new_origin_offset;
};

// Solve the effective origin for `next` such that its integrated releases
// from the origin to the bend equal `acquired_count` exactly (design note
// Q1): the smallest tau with CumulativeReleases(next, tau) == acquired_count.
// The regime branch (ramping vs post-ramp under `next`) falls out of the
// acquired count vs the ramp segment's integral; a bend landing mid-ramp
// under the outgoing schedule may re-anchor into the new schedule's post-ramp
// segment and vice versa -- the same two equations cover all cases.
//
// `current` and `t_bend_elapsed` carry the outgoing schedule's state for the
// bookkeeping sanity check: the acquired count may not run ahead of the
// outgoing schedule's release count at the bend by more than one count of
// rounding slack.
//
// Errors: InvalidArgument for an invalid `next` schedule (target_rps < 1 or
// non-finite, ramp_time <= 0) or a negative bend elapsed; InternalError when
// the caller's bookkeeping is inconsistent or the exact integer solve fails
// (defensive: unreachable for any admissible schedule).
absl::StatusOr<ReanchorResult> Reanchor(const RampSchedule& current,
                                        std::chrono::nanoseconds t_bend_elapsed,
                                        uint64_t acquired_count, const RampSchedule& next);

} // namespace RampReanchor
} // namespace Nighthawk
