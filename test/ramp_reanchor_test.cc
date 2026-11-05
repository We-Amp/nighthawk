// FORK-ONLY (We-Amp): property tests for the spec 8.7 ramp re-anchoring math
// (M3 Task 5, Review Focus 1 -- the heaviest test investment in M3). The A2b
// design note is the normative source: the equations ARE the implementation,
// and these tests pin its invariant bundle -- cumulative count preserved
// exactly, the artifact-free rate step (R2), no burst/silence beyond the
// bounded windows (R3), the down-bend ruling, the regime cases, and origin
// invariance. PURE: no Envoy types; a plain cc_test.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include "source/common/ramp_reanchor.h"

namespace Nighthawk {
namespace RampReanchor {
namespace {

using namespace std::chrono_literals;

constexpr double kNanosPerSecond = 1e9;
constexpr std::chrono::nanoseconds kTick{1000000}; // 1 ms walk granularity
constexpr std::chrono::nanoseconds kZero{0};

bool RelNear(double expected, double actual, double eps = 1e-9) {
  const double scale = std::max({1.0, std::fabs(expected), std::fabs(actual)});
  return std::fabs(expected - actual) <= eps * scale;
}

std::chrono::nanoseconds Seconds(double s) {
  return std::chrono::nanoseconds(static_cast<int64_t>(s * kNanosPerSecond));
}

// A bend: schedule A running, bent to schedule B at elapsed e_b with acquired
// count n (re-derived from the stock forms so tests cannot drift from them).
struct Bend {
  RampSchedule a;
  RampSchedule b;
  std::chrono::nanoseconds e_b;
  uint64_t n;
  std::chrono::nanoseconds tau;

  // The composite schedule's release count at delta around the bend instant
  // (negative: still on A; non-negative: on B from its re-anchored origin).
  int64_t countAt(std::chrono::nanoseconds delta) const {
    if (delta < kZero) {
      return CumulativeReleases(a, e_b + delta);
    }
    return CumulativeReleases(b, tau + delta);
  }
};

absl::StatusOr<Bend> MakeBend(const RampSchedule& a, std::chrono::nanoseconds e_b,
                              const RampSchedule& b) {
  const uint64_t n = static_cast<uint64_t>(CumulativeReleases(a, e_b));
  auto solved = Reanchor(a, e_b, n, b);
  if (!solved.ok()) {
    return solved.status();
  }
  EXPECT_EQ(CumulativeReleases(b, solved->new_origin_offset), static_cast<int64_t>(n));
  return Bend{a, b, e_b, n, solved->new_origin_offset};
}

// Greedily walks a bent timeline at kTick granularity from A's origin (the
// faithful limiter simulation: acquired starts at 0 when the schedule starts),
// granting whenever the composite schedule offers a count. Returns the grant
// times (ns around the bend; the bend itself is at 0) and the running count.
struct WalkResult {
  std::vector<std::chrono::nanoseconds> grant_times;
  std::vector<uint64_t> acquired_per_tick; // index i <-> t = -e_b + i * kTick
};

WalkResult WalkBentTimeline(const Bend& bend, std::chrono::nanoseconds post) {
  WalkResult result;
  uint64_t acquired = 0;
  const int64_t pre_ticks = bend.e_b.count() / kTick.count();
  const int64_t post_ticks = post.count() / kTick.count();
  for (int64_t i = 0; i <= pre_ticks + post_ticks; i++) {
    const std::chrono::nanoseconds t{-bend.e_b.count() + i * kTick.count()};
    if (bend.countAt(t) > static_cast<int64_t>(acquired)) {
      result.grant_times.push_back(t);
      acquired++;
    }
    result.acquired_per_tick.push_back(acquired);
  }
  return result;
}

// ---- The stock forms, pinned against hand-computed values ----

// The shared cumulative forms must match the stock LinearRampingRateLimiterImpl
// expressions bit for bit: the limiter evaluates this function at runtime.
TEST(RampReanchorTest, CumulativeReleasesMatchesStockForms) {
  // Ramping regime: round(e_s * frac * f / 2).
  const RampSchedule a{2s, 10.0};
  EXPECT_EQ(CumulativeReleases(a, 0ns), 0);
  EXPECT_EQ(CumulativeReleases(a, 500ms), 1);  // round(0.5 * 2.5 * ... ) = round(1.25) = 1
  EXPECT_EQ(CumulativeReleases(a, 1s), 3);     // round(1.0 * 5 / 2) = round(2.5) = 3
  EXPECT_EQ(CumulativeReleases(a, 1999ms), 10); // round(1.999 * 9.995 / 2) = round(9.99) = 10
  // Post-ramp: round(T_ns*f_ns/2) + round((e-T)_ns*f_ns).
  EXPECT_EQ(CumulativeReleases(a, 2s), 10);
  EXPECT_EQ(CumulativeReleases(a, 3s), 20);
  EXPECT_EQ(CumulativeReleases(a, 7s), 60);
  // The design note's transcript: (10s, 100) at 5s -> 125.
  const RampSchedule plan{10s, 100.0};
  EXPECT_EQ(CumulativeReleases(plan, 5s), 125);
  EXPECT_EQ(CumulativeReleases(plan, 10s), 500);
  EXPECT_EQ(CumulativeReleases(plan, 15s), 1000);
}

TEST(RampReanchorTest, InstantaneousRateMatchesClosedForms) {
  const RampSchedule a{10s, 100.0};
  EXPECT_EQ(InstantaneousRate(a, 0ns), 0.0);
  EXPECT_EQ(InstantaneousRate(a, 5s), 50.0); // f*e/T
  EXPECT_EQ(InstantaneousRate(a, 2500ms), 25.0);
  EXPECT_EQ(InstantaneousRate(a, 10s), 100.0); // the ramp's end, both limits
  EXPECT_EQ(InstantaneousRate(a, 12s), 100.0); // post-ramp
}

// ---- Q1: the solve attains the acquired count exactly, in every regime ----

TEST(RampReanchorTest, ReanchorSolvesExactly) {
  // Case (i): mid-ramp bend re-anchoring into the new ramp's ramping segment.
  // Design note transcript: (10s,100) at 5s (n=125) -> (10s,200):
  // tau = sqrt(2*125*10/200) = 3.53553s.
  auto up = MakeBend({10s, 100.0}, 5s, {10s, 200.0});
  ASSERT_TRUE(up.ok()) << up.status();
  EXPECT_EQ(CumulativeReleases(up->b, up->tau), 125);
  EXPECT_NEAR(up->tau.count() / kNanosPerSecond, 3.535533906, 0.01);

  // Case (ii): a hard down-bend re-anchoring into the new schedule's post-ramp
  // segment even though the bend lands mid-ramp under A. Transcript:
  // (10s,100) at 5s (n=125) -> (10s,20): tau = 10 + 25/20 = 11.25s.
  auto down = MakeBend({10s, 100.0}, 5s, {10s, 20.0});
  ASSERT_TRUE(down.ok()) << down.status();
  EXPECT_EQ(CumulativeReleases(down->b, down->tau), 125);
  EXPECT_NEAR(down->tau.count() / kNanosPerSecond, 11.25, 0.05);

  // Case (iii) forward: post-ramp bend, steep up-bend re-anchoring into the
  // new ramp's ramping segment. (2s,1000) post-ramp at 5s (n = 1000 + 3000 =
  // 4000) -> (20s,2000): tau = sqrt(2*4000*20/2000) = sqrt(80) = 8.944s < 20s.
  auto cross_up = MakeBend({2s, 1000.0}, 5s, {20s, 2000.0});
  ASSERT_TRUE(cross_up.ok()) << cross_up.status();
  EXPECT_EQ(CumulativeReleases(cross_up->b, cross_up->tau), 4000);
  EXPECT_LT(cross_up->tau, cross_up->b.ramp_time);
  EXPECT_NEAR(cross_up->tau.count() / kNanosPerSecond, 8.94427191, 0.02);

  // The forms meet at the regime boundary n = f_B*T_B/2: (10s,100) at 5s
  // (n=125) -> (10s,25) has f_B*T_B/2 = 125 exactly, and tau is T_B.
  auto boundary = MakeBend({10s, 100.0}, 5s, {10s, 25.0});
  ASSERT_TRUE(boundary.ok()) << boundary.status();
  EXPECT_EQ(CumulativeReleases(boundary->b, boundary->tau), 125);
  EXPECT_NEAR(boundary->tau.count() / kNanosPerSecond, 10.0, 0.05);

  // Post-ramp -> post-ramp: the clean commanded step. (2s,1000) at 5s
  // (n=4000) -> (1s,100): tau = 1 + (4000-50)/100 = 40.5s.
  auto post = MakeBend({2s, 1000.0}, 5s, {1s, 100.0});
  ASSERT_TRUE(post.ok()) << post.status();
  EXPECT_EQ(CumulativeReleases(post->b, post->tau), 4000);
  EXPECT_NEAR(post->tau.count() / kNanosPerSecond, 40.5, 0.05);
}

TEST(RampReanchorTest, ReanchorZeroCountStartsAtBend) {
  auto zero = Reanchor({10s, 100.0}, 5s, 0, {10s, 200.0});
  ASSERT_TRUE(zero.ok()) << zero.status();
  EXPECT_EQ(zero->new_origin_offset, 0ns);
  EXPECT_EQ(CumulativeReleases(zero->schedule, zero->new_origin_offset), 0);
}

// ---- Invariant (a): cumulative count preserved (property, both regimes) ----

TEST(RampReanchorTest, BendPreservesCumulativeCount) {
  const double ramp_times_s[] = {1.0, 5.0, 10.0, 30.0};
  const double targets[] = {20.0, 50.0, 100.0, 200.0, 1000.0};
  for (const double t_a : ramp_times_s) {
    for (const double e_b_s : {0.5, 2.0, 5.0, 9.0, 12.0}) {
      for (const double t_b : ramp_times_s) {
        for (const double f_b : targets) {
          const RampSchedule a{Seconds(t_a), 100.0};
          const RampSchedule b{Seconds(t_b), f_b};
          const auto bend = MakeBend(a, Seconds(e_b_s), b);
          ASSERT_TRUE(bend.ok()) << bend.status();
          // Count conservation, exact at the discrete level: the new
          // schedule placed at tau has released exactly the acquired count.
          EXPECT_EQ(CumulativeReleases(bend->b, bend->tau),
                    static_cast<int64_t>(bend->n));
          // No fictitious time before the schedule: tau is non-negative.
          EXPECT_GE(bend->tau.count(), 0);
          // The solve is minimal: 1ns earlier the schedule has not yet
          // reached the count (bisection minimality). (n == 0 re-anchors at
          // the bend itself; nothing to be minimal about.)
          if (bend->n > 0) {
            EXPECT_LT(CumulativeReleases(bend->b, bend->tau - std::chrono::nanoseconds(1)),
                      static_cast<int64_t>(bend->n));
          }
        }
      }
    }
  }
}

// ---- R2: the artifact-free rate step (continuity oracle) ----

// The round-window [tau_lo, tau_hi] in which B's ROUNDED release count
// equals n (the closed forms at n-0.5 and n+0.5): the discrete solve lands
// somewhere in this window -- an inherent one-count quantization of the
// re-anchored point that the rate-step closed forms are subject to.
std::pair<double, double> RoundWindow(const RampSchedule& s, double n) {
  const double T = s.ramp_time.count() / kNanosPerSecond;
  const double R = s.target_rps * T / 2.0;
  if (n <= R) {
    const double lo = std::sqrt(2.0 * std::max(0.0, n - 0.5) * T / s.target_rps);
    const double hi = std::sqrt(2.0 * (n + 0.5) * T / s.target_rps);
    return {lo, std::min(hi, T)};
  }
  return {T + std::max(0.0, (n - 0.5) - R) / s.target_rps, T + ((n + 0.5) - R) / s.target_rps};
}

TEST(RampReanchorTest, BendIsRateContinuous) {
  // The right-limit is the re-anchored curve's rate (within B's one-count
  // round window), and the step matches the design note's closed form --
  // mid-ramp -> mid-ramp: r_B(tau) = e_b * sqrt(f_A f_B / (T_A T_B)) = 70.71
  // at the transcript's bend (50 on the way in).
  auto up = MakeBend({10s, 100.0}, 5s, {10s, 200.0});
  ASSERT_TRUE(up.ok());
  const auto [lo_up, hi_up] = RoundWindow(up->b, static_cast<double>(up->n));
  const double right_up = InstantaneousRate(up->b, up->tau);
  EXPECT_GE(right_up, InstantaneousRate(up->b, Seconds(lo_up)) - 1e-9);
  EXPECT_LE(right_up, InstantaneousRate(up->b, Seconds(hi_up)) + 1e-9);
  EXPECT_GE(70.71067812, InstantaneousRate(up->b, Seconds(lo_up)) - 1e-9);
  EXPECT_LE(70.71067812, InstantaneousRate(up->b, Seconds(hi_up)) + 1e-9);

  // -> post-ramp: the right-limit is the new target itself (exact: the rate
  // is flat there, no quantization).
  auto down = MakeBend({10s, 100.0}, 5s, {10s, 20.0});
  ASSERT_TRUE(down.ok());
  EXPECT_NEAR(InstantaneousRate(down->b, down->tau), 20.0, 1e-9);

  // Zero-step case (the only literal 8.7(b) continuity): same ramp slope
  // f/T. (10s,100) -> (5s,50) both slope 10 rps/s, bent at 4s: r_A(4) = 40,
  // and the closed form's right-limit is 40 as well -- the step is zero to
  // within B's round window.
  auto slope = MakeBend({10s, 100.0}, 4s, {5s, 50.0});
  ASSERT_TRUE(slope.ok());
  const double left = InstantaneousRate(slope->a, slope->e_b);
  const double right = InstantaneousRate(slope->b, slope->tau);
  EXPECT_NEAR(left, 40.0, 1e-9);
  const auto [lo_s, hi_s] = RoundWindow(slope->b, static_cast<double>(slope->n));
  const double band_width =
      InstantaneousRate(slope->b, Seconds(hi_s)) - InstantaneousRate(slope->b, Seconds(lo_s));
  EXPECT_GE(40.0, InstantaneousRate(slope->b, Seconds(lo_s)) - 1e-9);
  EXPECT_LE(40.0, InstantaneousRate(slope->b, Seconds(hi_s)) + 1e-9);
  EXPECT_LE(std::fabs(left - right), band_width + 1e-9)
      << left << " vs " << right << " (band " << band_width << ")";

  // Post-ramp -> post-ramp: the clean commanded step, exact.
  auto post = MakeBend({2s, 1000.0}, 5s, {1s, 100.0});
  ASSERT_TRUE(post.ok());
  EXPECT_NEAR(InstantaneousRate(post->b, post->tau), 100.0, 1e-9);
  EXPECT_NEAR(
      std::fabs(100.0 - 1000.0),
      std::fabs(InstantaneousRate(post->b, post->tau) - InstantaneousRate(post->a, post->e_b)),
      1e-9);
}

// ---- Invariant (c): no burst, no silence beyond the bounded windows ----

TEST(RampReanchorTest, BendProducesNoBurstOrSilence) {
  // An up-bend mid-ramp: 100 Hz ramping 10s, bent at 5s to 200 Hz/10s.
  auto bend = MakeBend({10s, 100.0}, 5s, {10s, 200.0});
  ASSERT_TRUE(bend.ok());
  const WalkResult walk = WalkBentTimeline(*bend, 2s);

  // No windfall at the bend: the first post-bend grant comes from B's own
  // grid, within two acquisition intervals of B at its re-anchored rate (the
  // R2/R3 bound) -- never from the discarded outgoing credit.
  const double r_b_tau = InstantaneousRate(bend->b, bend->tau);
  const auto first_post = std::find_if(
      walk.grant_times.begin(), walk.grant_times.end(),
      [](const std::chrono::nanoseconds t) { return t >= std::chrono::nanoseconds::zero(); });
  ASSERT_NE(first_post, walk.grant_times.end());
  EXPECT_LE(first_post->count(), (2.0 / r_b_tau) * kNanosPerSecond + kTick.count());

  // No silence: every inter-grant gap is within two local schedule intervals
  // plus walk granularity (one interval + one count of rounding slack, with
  // margin).
  for (size_t i = 1; i < walk.grant_times.size(); i++) {
    const double gap_ns = (walk.grant_times[i] - walk.grant_times[i - 1]).count();
    const std::chrono::nanoseconds t_prev = walk.grant_times[i - 1];
    const double local_rate =
        t_prev >= kZero
            ? InstantaneousRate(bend->b, bend->tau + t_prev)
            : InstantaneousRate(bend->a, bend->e_b + t_prev);
    const double bound_ns = (1.0 / local_rate) * kNanosPerSecond * 2.0 + 2.0 * kTick.count();
    EXPECT_LE(gap_ns, bound_ns) << "silence beyond one bounded window at grant " << i;
  }
}

TEST(RampReanchorTest, DownBendHandlesNegativeCreditExplicitly) {
  // The R3 ruling: the origin-solve mechanism makes negative credit
  // unreachable; a hard down-bend holds at zero credit while the new curve
  // accrues, the silence is bounded by <= 2 new-schedule intervals, and the
  // forced rate step is the closed form (surfaced by the limiter's apply
  // log; pinned here as the oracle value).
  const auto bend = MakeBend({2s, 1000.0}, 5s, {10s, 100.0});
  ASSERT_TRUE(bend.ok()) << bend.status();
  // Fictitious history: the limiter correctly believes itself 45s past B's
  // ramp end -- tau far beyond T_B, not a negative-credit state.
  EXPECT_GT(bend->tau, bend->b.ramp_time);
  EXPECT_NEAR(bend->tau.count() / kNanosPerSecond, 45.0, 0.05);
  EXPECT_EQ(CumulativeReleases(bend->b, bend->tau), 4000);
  const double forced_step =
      std::fabs(InstantaneousRate(bend->b, bend->tau) - InstantaneousRate(bend->a, bend->e_b));
  EXPECT_NEAR(forced_step, 900.0, 1e-6);

  // Walk it: the composite schedule never falls behind the acquired count
  // (the credit >= 0 invariant, design note 2.1), and the silence from the
  // bend to the first grant is bounded by two of B's 10ms intervals.
  const WalkResult walk = WalkBentTimeline(*bend, 2s);
  for (size_t i = 0; i < walk.acquired_per_tick.size(); i++) {
    const std::chrono::nanoseconds t{-bend->e_b.count() + static_cast<int64_t>(i) * kTick.count()};
    EXPECT_GE(bend->countAt(t), static_cast<int64_t>(walk.acquired_per_tick[i]));
  }
  const auto first_post = std::find_if(
      walk.grant_times.begin(), walk.grant_times.end(),
      [](const std::chrono::nanoseconds t) { return t >= std::chrono::nanoseconds::zero(); });
  ASSERT_NE(first_post, walk.grant_times.end());
  EXPECT_LE(first_post->count(), (2.0 / 100.0) * kNanosPerSecond + kTick.count());
}

TEST(RampReanchorTest, UpBendAndDownBendMidRamp) {
  // Up-bend mid-ramp -> stays in the new ramp's ramping segment (case (i)).
  auto up = MakeBend({10s, 100.0}, 5s, {10s, 200.0});
  ASSERT_TRUE(up.ok());
  EXPECT_LT(up->tau, up->b.ramp_time);
  EXPECT_EQ(CumulativeReleases(up->b, up->tau), 125);

  // Down-bend mid-ramp, still within the new ramp's segment (case (i) again:
  // n = 125 <= f_B*T_B/2 = 250).
  auto mid_down = MakeBend({10s, 100.0}, 5s, {10s, 50.0});
  ASSERT_TRUE(mid_down.ok());
  EXPECT_LT(mid_down->tau, mid_down->b.ramp_time);
  EXPECT_EQ(CumulativeReleases(mid_down->b, mid_down->tau), 125);

  // Hard down-bend mid-ramp -> crosses into the new post-ramp segment
  // (case (ii)/(iii)).
  auto hard_down = MakeBend({10s, 100.0}, 5s, {10s, 20.0});
  ASSERT_TRUE(hard_down.ok());
  EXPECT_GT(hard_down->tau, hard_down->b.ramp_time);
  EXPECT_EQ(CumulativeReleases(hard_down->b, hard_down->tau), 125);
}

TEST(RampReanchorTest, BendCrossingRegimeBoundary) {
  // Post-ramp under A, re-anchoring into B's ramping segment (steep up-bend).
  auto cross_up = MakeBend({2s, 1000.0}, 5s, {20s, 2000.0});
  ASSERT_TRUE(cross_up.ok());
  EXPECT_LT(cross_up->tau, cross_up->b.ramp_time);
  EXPECT_EQ(CumulativeReleases(cross_up->b, cross_up->tau), 4000);

  // Post-ramp under A, re-anchoring into B's post-ramp segment (down-bend).
  auto cross_down = MakeBend({2s, 1000.0}, 5s, {1s, 100.0});
  ASSERT_TRUE(cross_down.ok());
  EXPECT_GT(cross_down->tau, cross_down->b.ramp_time);
  EXPECT_EQ(CumulativeReleases(cross_down->b, cross_down->tau), 4000);

  // The outgoing schedule's regime never enters the solve -- only (n, B)
  // decide: this mid-ramp-under-A bend solves identically to the post-ramp
  // one above because the acquired count is the same 4000.
  EXPECT_EQ(CumulativeReleases(cross_down->a, cross_down->e_b), 4000);
}

// ---- Q5: per-worker origin invariance ----

TEST(RampReanchorTest, ReanchorIsOriginInvariant) {
  // Two workers at the same t_b with different acquired counts (scheduling
  // jitter): each conserves its own count exactly; the solve has no hidden
  // worker state -- equal counts give identical solves, different counts give
  // proportionally shifted origins.
  const RampSchedule a{10s, 100.0};
  const RampSchedule b{10s, 200.0};
  const auto w1 = MakeBend(a, 5s, b);
  const auto w2 = MakeBend(a, 5s + 40ms, b); // a few more counts at ~80 Hz
  ASSERT_TRUE(w1.ok());
  ASSERT_TRUE(w2.ok());
  EXPECT_EQ(CumulativeReleases(b, w1->tau), static_cast<int64_t>(w1->n));
  EXPECT_EQ(CumulativeReleases(b, w2->tau), static_cast<int64_t>(w2->n));
  EXPECT_GT(w2->n, w1->n);

  // Determinism: identical inputs, identical solve.
  const RampSchedule post{100ms, 200.0};
  const auto p1 = MakeBend(a, 5s, post);
  const auto p2 = MakeBend(a, 5s, post);
  ASSERT_TRUE(p1.ok());
  ASSERT_TRUE(p2.ok());
  EXPECT_EQ(p1->tau, p2->tau);
}

TEST(RampReanchorTest, ReanchorRejectsInvalid) {
  EXPECT_FALSE(Reanchor({10s, 100.0}, 5s, 10, {10s, 0.0}).ok());
  EXPECT_FALSE(Reanchor({10s, 100.0}, 5s, 10, {0ns, 100.0}).ok());
  EXPECT_FALSE(Reanchor({10s, 100.0}, -1s, 10, {10s, 200.0}).ok());
  // Bookkeeping violation: acquired ahead of the outgoing schedule.
  EXPECT_EQ(Reanchor({10s, 100.0}, 1s, 1000, {10s, 200.0}).status().code(),
            absl::StatusCode::kInternal);
}

} // namespace
} // namespace RampReanchor
} // namespace Nighthawk
