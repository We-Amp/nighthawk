#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "source/client/tui/profile_windows.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): phase-purity, per-unit-of-work normalization, and
// same-state noise-floor contract tests (M1 plan task 4; spec 8.3/8.4).
// Plain cc_test for the same reason as tui_pprof_fold_test: the TUI
// quarantine keeps this off Envoy's C++ sources and test helpers.

// Builds a frame with subtree totals folded up from the children.
FoldedFrame MakeFrame(std::string name, uint64_t self_value,
                      std::vector<FoldedFrame> children = {}) {
  FoldedFrame frame;
  frame.function = std::move(name);
  frame.self_value = self_value;
  frame.self_samples = 1;
  frame.children = std::move(children);
  frame.total_value = frame.self_value;
  frame.total_samples = frame.self_samples;
  for (const FoldedFrame& child : frame.children) {
    frame.total_value += child.total_value;
    frame.total_samples += child.total_samples;
  }
  return frame;
}

FoldedTree MakeTree(std::vector<FoldedFrame> roots, int64_t time_nanos = 0,
                    int64_t duration_nanos = 0) {
  FoldedTree tree;
  tree.root = std::move(roots);
  tree.value_unit = "cpu-nanoseconds";
  tree.time_nanos = time_nanos;
  tree.duration_nanos = duration_nanos;
  for (const FoldedFrame& frame : tree.root) {
    tree.total_value += frame.total_value;
    tree.total_samples += frame.total_samples;
  }
  return tree;
}

// A pure, aligned-looking window for the differential-math tests. The diff
// functions accept any pure window; this bypasses Align to control work_units
// and worker_count directly.
ProfileWindow WindowWith(FoldedTree tree, uint64_t work_units, uint64_t worker_count = 4) {
  ProfileWindow window;
  window.tree = std::move(tree);
  window.start_unix_ns = 1000;
  window.end_unix_ns = 2000;
  window.purity_fraction = 1.0;
  window.phase_id = "main";
  window.worker_count = worker_count;
  window.work_units = work_units;
  window.profile_type = "cpu";
  window.symbolized = true;
  window.interval_source = WindowIntervalSource::kFetchOrchestration;
  return window;
}

// House assertion style: plain gtest, status code checks (no gmock).
template <typename T>
void ExpectStatus(const absl::StatusOr<T>& result, absl::StatusCode code) {
  EXPECT_EQ(result.status().code(), code) << result.status();
}

const DifferentialNodeDiff* FindDiff(const DifferentialResult& result,
                                     const std::string& function) {
  for (const DifferentialNodeDiff& diff : result.diffs) {
    if (diff.frame != nullptr && diff.frame->function == function) {
      return &diff;
    }
  }
  return nullptr;
}

// --- Review Focus 3: window <-> phase alignment owns the purity function.

TEST(WindowAlignmentTest, StraddlingWindowExcludedAndMarked) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  PhaseInterval phase;
  phase.phase_id = "main";
  phase.start_unix_ns = 1000;
  phase.end_unix_ns = 2000;
  phase.drain_complete = true;

  // Window end past the phase end: not fully inside -> excluded-and-marked
  // (the split-at-boundary variant is M2 scope, never silently comparable).
  auto past_end = WindowPhaseAlignment::Align(tree, phase, 1200, 2100, 4, 10, "cpu");
  ExpectStatus(past_end, absl::StatusCode::kFailedPrecondition);

  // Drain not complete: the phase's tail work is censored -> excluded.
  phase.drain_complete = false;
  auto no_drain = WindowPhaseAlignment::Align(tree, phase, 1200, 1800, 4, 10, "cpu");
  ExpectStatus(no_drain, absl::StatusCode::kFailedPrecondition);
  phase.drain_complete = true;

  // Contained window: purity exactly 1.0, phase id carried.
  auto contained = WindowPhaseAlignment::Align(tree, phase, 1200, 1800, 4, 10, "cpu");
  ASSERT_TRUE(contained.ok()) << contained.status();
  EXPECT_DOUBLE_EQ(contained->purity_fraction, 1.0);
  EXPECT_EQ(contained->phase_id, "main");
  EXPECT_EQ(contained->start_unix_ns, 1200);
  EXPECT_EQ(contained->end_unix_ns, 1800);
  EXPECT_EQ(contained->worker_count, 4u);
  EXPECT_EQ(contained->work_units, 10u);
  EXPECT_EQ(contained->profile_type, "cpu");
  EXPECT_EQ(contained->tree.total_value, 100u);

  // Partial overlap into a PRIOR phase (start before the phase start): kept
  // with a purity fraction < 1.0, computed as overlap/window (header formula).
  auto straddle = WindowPhaseAlignment::Align(tree, phase, 900, 1500, 4, 10, "cpu");
  ASSERT_TRUE(straddle.ok()) << straddle.status();
  EXPECT_DOUBLE_EQ(straddle->purity_fraction, 500.0 / 600.0);

  // Entirely outside the phase: nothing pure here -> excluded.
  auto outside = WindowPhaseAlignment::Align(tree, phase, 500, 800, 4, 10, "cpu");
  ExpectStatus(outside, absl::StatusCode::kFailedPrecondition);

  // Degenerate window (end <= start) is an argument error, not a purity claim.
  auto inverted = WindowPhaseAlignment::Align(tree, phase, 1500, 1500, 4, 10, "cpu");
  ExpectStatus(inverted, absl::StatusCode::kInvalidArgument);
}

TEST(WindowAlignmentTest, PreA1IntervalSpansRunAndMarksDrainComplete) {
  PhaseInterval interval = PreA1RunInterval(1000, 2000, 4);
  EXPECT_EQ(interval.phase_id, "main");
  EXPECT_EQ(interval.start_unix_ns, 1000);
  EXPECT_EQ(interval.end_unix_ns, 2000);
  EXPECT_TRUE(interval.drain_complete);
  EXPECT_EQ(interval.worker_count, 4u);
}

// --- Design finding (Task 3): real gperftools profiles carry NO time_nanos.
// The interval source must be labeled honestly either way.

TEST(WindowAlignmentTest, AlignLabelsFetchClockFallback) {
  PhaseInterval phase = PreA1RunInterval(1000, 2000, 4);

  // No timestamps in the profile (the real gperftools route): the interval
  // the caller passed came from the FETCH orchestration clock, and the
  // window must say so.
  FoldedTree stampless = MakeTree({MakeFrame("main", 100)});
  auto from_fetch = WindowPhaseAlignment::Align(stampless, phase, 1200, 1800, 4, 10, "cpu");
  ASSERT_TRUE(from_fetch.ok()) << from_fetch.status();
  EXPECT_EQ(from_fetch->interval_source, WindowIntervalSource::kFetchOrchestration);

  // Timestamped profile: the interval is the profile's own SUT clock.
  FoldedTree stamped = MakeTree({MakeFrame("main", 100)}, 1200'000'000, 600'000'000);
  auto from_profile = WindowPhaseAlignment::Align(stamped, phase, 1200, 1800, 4, 10, "cpu");
  ASSERT_TRUE(from_profile.ok()) << from_profile.status();
  EXPECT_EQ(from_profile->interval_source, WindowIntervalSource::kProfileTimestamps);
}

// --- Review Focus 4, denominator half: the work-units join.

TEST(WindowAlignmentTest, CompletedWorkInRangeAnchored) {
  // Cumulative completed-request counts on the monotonic clock (the M0b
  // RunViewModel counter series shape).
  std::vector<CounterPoint> series = {
      {100, 10}, {200, 20}, {300, 40}, {400, 50},
  };

  // Window (150, 350]: C(350) - C(150) = 40 - 10 = 30.
  EXPECT_EQ(CompletedWorkInMonotonicRange(series, 150, 350), 30u);
  // Empty/degenerate range: 0.
  EXPECT_EQ(CompletedWorkInMonotonicRange(series, 250, 250), 0u);
  // Inverted range: 0 (never an underflow).
  EXPECT_EQ(CompletedWorkInMonotonicRange(series, 350, 150), 0u);
  // Range ending before the series starts: 0.
  EXPECT_EQ(CompletedWorkInMonotonicRange(series, 0, 99), 0u);
  // Range covering everything including the head: 50 - 0 = 50.
  EXPECT_EQ(CompletedWorkInMonotonicRange(series, 50, 450), 50u);
  // Empty series: 0.
  EXPECT_EQ(CompletedWorkInMonotonicRange({}, 100, 400), 0u);
}

// --- Review Focus 4: differential normalization.

TEST(DifferentialTest, DiffNormalizesPerUnitOfWork) {
  // Base: total 100, work 100 -> 1.0 value per completed request.
  FoldedTree base_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 60),  //
                                                        MakeFrame("cold", 40)})});

  // (1) Pure workload scaling: tree doubled everywhere AND work doubled ->
  // every per-work value is unchanged -> every delta_ratio is 0.
  FoldedTree doubled = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 120),  //
                                                       MakeFrame("cold", 80)})});
  // NodeDiff.frame points into the input trees: the windows must outlive the
  // result (named locals here; in the pipeline they live in the store).
  ProfileWindow base_win = WindowWith(base_tree, 100);
  ProfileWindow doubled_win = WindowWith(doubled, 200);
  auto scaled = ComputeDifferential(base_win, doubled_win);
  ASSERT_TRUE(scaled.ok()) << scaled.status();
  EXPECT_TRUE(scaled->comparable);
  ASSERT_EQ(scaled->diffs.size(), 3u);
  for (const DifferentialNodeDiff& diff : scaled->diffs) {
    EXPECT_DOUBLE_EQ(diff.delta_ratio, 0.0) << diff.frame->function;
  }

  // (2) Workload change ALONE: identical trees, work doubled -> every
  // per-work value halves -> a uniform -0.5 delta on every node. This is the
  // normalization working: raw counts would show "no change" and hide a real
  // per-request regression. (The plan sketch stated this case as ratio 0;
  // under the per-unit-of-work definition that is only true when the tree
  // scales with the work, which is case (1) — see the header note.)
  ProfileWindow workload_base = WindowWith(base_tree, 100);
  ProfileWindow workload_cur = WindowWith(base_tree, 200);
  auto workload = ComputeDifferential(workload_base, workload_cur);
  ASSERT_TRUE(workload.ok()) << workload.status();
  ASSERT_EQ(workload->diffs.size(), 3u);
  for (const DifferentialNodeDiff& diff : workload->diffs) {
    EXPECT_DOUBLE_EQ(diff.delta_ratio, -0.5) << diff.frame->function;
  }

  // (3) Localization: one subtree doubles, work unchanged -> that subtree's
  // ratio is +1.0, untouched subtrees are 0, and the union root carries the
  // blended remainder. Drift vs. knob effect separability (8.4) rests on this.
  FoldedTree hot_doubled =
      MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 120), MakeFrame("cold", 40)})});
  ProfileWindow localized_base = WindowWith(base_tree, 100);
  ProfileWindow localized_cur = WindowWith(hot_doubled, 100);
  auto localized = ComputeDifferential(localized_base, localized_cur);
  ASSERT_TRUE(localized.ok()) << localized.status();
  EXPECT_DOUBLE_EQ(localized->base_total_per_work, 1.0);
  EXPECT_DOUBLE_EQ(localized->cur_total_per_work, 1.6);
  // Sorted by |delta_ratio| descending: hot (1.0), main (0.6), cold (0).
  ASSERT_EQ(localized->diffs.size(), 3u);
  EXPECT_EQ(localized->diffs[0].frame->function, "hot");
  EXPECT_DOUBLE_EQ(localized->diffs[0].delta_ratio, 1.0);
  EXPECT_DOUBLE_EQ(localized->diffs[0].base_per_work, 0.6);
  EXPECT_DOUBLE_EQ(localized->diffs[0].cur_per_work, 1.2);
  const DifferentialNodeDiff* cold = FindDiff(*localized, "cold");
  ASSERT_NE(cold, nullptr);
  EXPECT_DOUBLE_EQ(cold->delta_ratio, 0.0);

  // The positional address (task 5 interface addition): child indices from
  // the tree root — main {0}, hot {0,0}, cold {0,1}. This is what the
  // renderer's overlay addressing uses.
  EXPECT_EQ(localized->diffs[0].child_path, FramePath({0, 0}));
  EXPECT_EQ(FindDiff(*localized, "main")->child_path, FramePath({0}));
  EXPECT_EQ(cold->child_path, FramePath({0, 1}));
}

// The child-index path keeps addressing honest for same-named frames: a
// recursive stack a <- a gives two diffs both carrying function "a", uniquely
// addressed {0} and {0,0} (a name-only address would alias them).
TEST(DifferentialTest, ChildPathAddressesSameNamedFrames) {
  FoldedTree base_tree = MakeTree({MakeFrame("a", 10, {MakeFrame("a", 20)})});
  FoldedTree cur_tree = MakeTree({MakeFrame("a", 30, {MakeFrame("a", 0)})});
  ProfileWindow base = WindowWith(base_tree, 100);
  ProfileWindow current = WindowWith(cur_tree, 100);
  auto result = ComputeDifferential(base, current);
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_EQ(result->diffs.size(), 2u);
  bool saw_outer = false;
  bool saw_inner = false;
  for (const DifferentialNodeDiff& diff : result->diffs) {
    EXPECT_EQ(diff.frame->function, "a");
    if (diff.child_path == FramePath({0})) {
      saw_outer = true;
      // Per-work values are TOTALS (self + subtree): outer a totals 30 on
      // both sides; only the self share moved up.
      EXPECT_DOUBLE_EQ(diff.base_per_work, 0.3);
      EXPECT_DOUBLE_EQ(diff.cur_per_work, 0.3);
    }
    if (diff.child_path == FramePath({0, 0})) {
      saw_inner = true;
      EXPECT_DOUBLE_EQ(diff.base_per_work, 0.2);
      EXPECT_DOUBLE_EQ(diff.cur_per_work, 0.0);
    }
  }
  EXPECT_TRUE(saw_outer);
  EXPECT_TRUE(saw_inner);
}

// When a node sits at different child positions on the two sides (a sibling
// overtook it), the address follows the CURRENT side: the renderer overlays
// onto the after picture, and a base-side address would collide with a
// different node's position there.
TEST(DifferentialTest, ChildPathFollowsCurrentSideWhenNodeMoves) {
  FoldedTree base_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 60),  //
                                                         MakeFrame("cold", 40)})});
  // The cold subtree overtook hot: positions flipped between the sides.
  FoldedTree cur_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("cold", 50),  //
                                                        MakeFrame("hot", 10)})});
  ProfileWindow base = WindowWith(base_tree, 100);
  ProfileWindow current = WindowWith(cur_tree, 100);
  auto result = ComputeDifferential(base, current);
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_EQ(result->diffs.size(), 3u);
  const DifferentialNodeDiff* hot = FindDiff(*result, "hot");
  const DifferentialNodeDiff* cold = FindDiff(*result, "cold");
  ASSERT_NE(hot, nullptr);
  ASSERT_NE(cold, nullptr);
  EXPECT_EQ(hot->child_path, FramePath({0, 1}));   // hot's CURRENT position
  EXPECT_EQ(cold->child_path, FramePath({0, 0}));  // cold's CURRENT position
}

// --- T4 review follow-up: the two previously untested guard branches. -----

// cpu vs alloc is a semantic error, not a comparability case: rejected.
TEST(DifferentialTest, DiffRejectsProfileTypeMismatch) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100);
  ProfileWindow current = WindowWith(tree, 100);
  current.profile_type = "alloc";
  ExpectStatus(ComputeDifferential(base, current), absl::StatusCode::kFailedPrecondition);
}

// A zero work-units side has no per-request denominator: rejected.
TEST(DifferentialTest, DiffRejectsZeroWorkUnits) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100);
  ProfileWindow zero = WindowWith(tree, 0);
  ExpectStatus(ComputeDifferential(base, zero), absl::StatusCode::kFailedPrecondition);
  ExpectStatus(ComputeDifferential(zero, base), absl::StatusCode::kFailedPrecondition);
}

// --- Review Focus 3, call-site half: impure pairs are REJECTED (8.3-strict).

TEST(DifferentialTest, DiffRejectsImpurePair) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100);
  ProfileWindow current = WindowWith(tree, 100);

  auto pure = ComputeDifferential(base, current);
  ASSERT_TRUE(pure.ok()) << pure.status();

  current.purity_fraction = 0.9;
  auto impure_current = ComputeDifferential(base, current);
  ExpectStatus(impure_current, absl::StatusCode::kFailedPrecondition);

  base.purity_fraction = 0.99;
  current.purity_fraction = 1.0;
  auto impure_base = ComputeDifferential(base, current);
  ExpectStatus(impure_base, absl::StatusCode::kFailedPrecondition);
}

TEST(DifferentialTest, SameStateNoiseFloorComputes) {
  // Adjacent same-state pure pair: the noise floor is the same differential
  // math with a same-state claim label — M1's only differential (Global
  // Constraint claim discipline), so it must mirror ComputeDifferential
  // field-for-field on the pair.
  FoldedTree w1_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 60),  //
                                                       MakeFrame("cold", 40)})});
  FoldedTree w2_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 63),  //
                                                       MakeFrame("cold", 40)})});
  ProfileWindow w1 = WindowWith(w1_tree, 100);
  ProfileWindow w2 = WindowWith(w2_tree, 100);

  auto noise = ComputeNoiseFloor(w1, w2);
  ASSERT_TRUE(noise.ok()) << noise.status();
  auto direct = ComputeDifferential(w1, w2);
  ASSERT_TRUE(direct.ok()) << direct.status();
  EXPECT_EQ(noise->comparable, direct->comparable);
  ASSERT_EQ(noise->diffs.size(), direct->diffs.size());
  for (size_t i = 0; i < noise->diffs.size(); ++i) {
    EXPECT_EQ(noise->diffs[i].frame, direct->diffs[i].frame);
    EXPECT_DOUBLE_EQ(noise->diffs[i].base_per_work, direct->diffs[i].base_per_work);
    EXPECT_DOUBLE_EQ(noise->diffs[i].cur_per_work, direct->diffs[i].cur_per_work);
    EXPECT_DOUBLE_EQ(noise->diffs[i].delta_ratio, direct->diffs[i].delta_ratio);
  }
  EXPECT_DOUBLE_EQ(noise->base_total_per_work, direct->base_total_per_work);
  EXPECT_DOUBLE_EQ(noise->cur_total_per_work, direct->cur_total_per_work);

  // The hot subtree drifted ~5%: visible as a small nonzero ratio.
  const DifferentialNodeDiff* hot = FindDiff(*noise, "hot");
  ASSERT_NE(hot, nullptr);
  EXPECT_NEAR(hot->delta_ratio, 0.05, 1e-9);
}

TEST(DifferentialTest, WorkerCountChangeMarksNonComparable) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100, /*worker_count=*/4);
  ProfileWindow current = WindowWith(tree, 100, /*worker_count=*/8);

  auto mismatched = ComputeDifferential(base, current);
  ASSERT_TRUE(mismatched.ok()) << mismatched.status();
  EXPECT_FALSE(mismatched->comparable);
  EXPECT_FALSE(mismatched->incomparable_reason.empty());
  EXPECT_TRUE(mismatched->diffs.empty());

  ProfileWindow matched_cur = WindowWith(tree, 100, /*worker_count=*/4);
  auto matched = ComputeDifferential(base, matched_cur);
  ASSERT_TRUE(matched.ok()) << matched.status();
  EXPECT_TRUE(matched->comparable);
}

// --- The store: immutable swap (pattern regression guard, mirrors the M0b
// SnapshotStore concurrency contract at smaller scale).

TEST(ProfileWindowStoreTest, StoreSwapIsImmutable) {
  ProfileWindowStore store;
  EXPECT_EQ(store.load(), nullptr);

  std::vector<ProfileWindow> first(1);
  first[0].work_units = 100;
  store.store(first);
  auto held = store.load();
  ASSERT_NE(held, nullptr);
  EXPECT_EQ(held->size(), 1u);

  std::vector<ProfileWindow> second(2);
  second[1].work_units = 200;
  store.store(second);
  // The previously loaded pointer keeps serving the first snapshot.
  EXPECT_EQ(held->size(), 1u);
  EXPECT_EQ((*held)[0].work_units, 100u);
  // New loads see the second snapshot.
  auto fresh = store.load();
  ASSERT_NE(fresh, nullptr);
  EXPECT_EQ(fresh->size(), 2u);
  EXPECT_EQ((*fresh)[1].work_units, 200u);
}

}  // namespace
}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
