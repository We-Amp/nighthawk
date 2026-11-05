#include "source/common/ramp_reanchor.h"

#include <cmath>
#include <cstdint>

#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace RampReanchor {
namespace {

constexpr double kNanosPerSecond = 1e9;

bool isValidSchedule(const RampSchedule& schedule) {
  return schedule.ramp_time > std::chrono::nanoseconds::zero() &&
         schedule.target_rps >= 1.0 && std::isfinite(schedule.target_rps);
}

// Seconds -> nanoseconds, saturated at the int64 range (a hard down-bend's
// solved origin can sit far past the new ramp; that offset is fictitious
// history, not a real duration, and must not overflow on the way there).
std::chrono::nanoseconds saturatedNanos(double seconds) {
  constexpr double kMaxSeconds =
      static_cast<double>((std::chrono::nanoseconds::max().count())) / kNanosPerSecond;
  if (!std::isfinite(seconds) || seconds >= kMaxSeconds) {
    return std::chrono::nanoseconds::max();
  }
  if (seconds <= 0.0) {
    return std::chrono::nanoseconds::zero();
  }
  return std::chrono::nanoseconds(static_cast<int64_t>(seconds * kNanosPerSecond));
}

} // namespace

int64_t CumulativeReleases(const RampSchedule& schedule, const std::chrono::nanoseconds elapsed) {
  const std::chrono::nanoseconds ramp_time = schedule.ramp_time;
  if (elapsed < ramp_time) {
    double elapsed_fraction = 1.0;
    elapsed_fraction -= static_cast<double>(ramp_time.count() - elapsed.count()) /
                        static_cast<double>(ramp_time.count());
    const double current_frequency = elapsed_fraction * schedule.target_rps;
    // Linearly ramping: half of what a constant pace at the current frequency
    // would deliver (the stock form, verbatim).
    return static_cast<int64_t>(
        std::round((elapsed.count() / kNanosPerSecond) * current_frequency / 2.0));
  }
  const double target_freq_ns = schedule.target_rps / kNanosPerSecond;
  return static_cast<int64_t>(std::round(ramp_time.count() * target_freq_ns / 2.0)) +
         static_cast<int64_t>(std::round((elapsed - ramp_time).count() * target_freq_ns));
}

double InstantaneousRate(const RampSchedule& schedule, const std::chrono::nanoseconds elapsed) {
  if (elapsed <= std::chrono::nanoseconds::zero()) {
    return 0.0;
  }
  if (elapsed < schedule.ramp_time) {
    return schedule.target_rps * (elapsed.count() / kNanosPerSecond) /
           (schedule.ramp_time.count() / kNanosPerSecond);
  }
  return schedule.target_rps;
}

absl::StatusOr<ReanchorResult> Reanchor(const RampSchedule& current,
                                        const std::chrono::nanoseconds t_bend_elapsed,
                                        const uint64_t acquired_count, const RampSchedule& next) {
  if (t_bend_elapsed < std::chrono::nanoseconds::zero()) {
    return absl::InvalidArgumentError("t_bend_elapsed must be >= 0");
  }
  if (!isValidSchedule(next)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "invalid bend schedule: ramp_time must be > 0 and target_rps must be >= 1 (got "
        "ramp_time=",
        next.ramp_time.count(), "ns, target_rps=", next.target_rps, ")"));
  }
  // Bookkeeping sanity (design note Q1): the acquired count may not run ahead
  // of the outgoing schedule's release count at the bend by more than one
  // count of rounding slack.
  const int64_t outgoing = CumulativeReleases(current, t_bend_elapsed);
  if (acquired_count > static_cast<uint64_t>(outgoing) + 1) {
    return absl::InternalError(absl::StrCat(
        "bookkeeping inconsistency: acquired_count=", acquired_count,
        " exceeds the outgoing schedule's release count at the bend (", outgoing, ")"));
  }
  if (acquired_count == 0) {
    // Nothing to conserve: the new schedule starts at the bend.
    return ReanchorResult{next, std::chrono::nanoseconds::zero()};
  }

  // Continuous seed (design note Q1 closed forms). The branch selection is by
  // the acquired count vs the new ramp segment's integral f*T/2, not by where
  // the bend landed under the outgoing schedule: case (i) re-anchors into the
  // new ramp's ramping segment, case (ii) into its post-ramp segment, and the
  // two forms meet at the boundary. A mid-ramp bend under a hard down-bend
  // lands in (ii); a steep up-bend from post-ramp lands in (i).
  const double ramp_seconds = next.ramp_time.count() / kNanosPerSecond;
  const double ramp_integral = next.target_rps * ramp_seconds / 2.0;
  const double tau_seed_seconds =
      (static_cast<double>(acquired_count) <= ramp_integral)
          ? std::sqrt(2.0 * static_cast<double>(acquired_count) * ramp_seconds / next.target_rps)
          : ramp_seconds + (static_cast<double>(acquired_count) - ramp_integral) / next.target_rps;

  // Exact integer solve: the smallest tau on the nanosecond grid with
  // CumulativeReleases(next, tau) == acquired_count. CumulativeReleases is
  // nondecreasing, attains every integer on the grid for any admissible
  // schedule (its per-ns envelope increment f*1e-9 never skips a count), and
  // the seed's round-window width is at most ~1.5 counts at the seed's rate,
  // so this bounded window always contains the solution.
  const std::chrono::nanoseconds window =
      2 * next.ramp_time +
      saturatedNanos((static_cast<double>(acquired_count) + 2.0) / next.target_rps);
  std::chrono::nanoseconds hi = saturatedNanos(tau_seed_seconds) + window;
  const int64_t target = static_cast<int64_t>(acquired_count);
  if (CumulativeReleases(next, hi) < target) {
    return absl::InternalError(
        "re-anchor search window exhausted before reaching the acquired count (defensive)");
  }
  std::chrono::nanoseconds lo{0};
  while (lo < hi) {
    const std::chrono::nanoseconds mid = lo + (hi - lo) / 2;
    if (CumulativeReleases(next, mid) >= target) {
      hi = mid;
    } else {
      lo = mid + std::chrono::nanoseconds(1);
    }
  }
  if (CumulativeReleases(next, lo) != target) {
    return absl::InternalError(absl::StrCat(
        "re-anchor solve did not attain the acquired count exactly (defensive): got ",
        CumulativeReleases(next, lo), ", want ", target));
  }
  return ReanchorResult{next, lo};
}

} // namespace RampReanchor
} // namespace Nighthawk
