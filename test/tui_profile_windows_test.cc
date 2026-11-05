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

// --- M2: the split-at-boundary producer replacement. -----------------------

using nighthawk::client::PhaseBoundaryEvent;
using nighthawk::client::PhaseSpec;

// Two intervals from one boundary: phase "warm" ends at the boundary's wall
// stamp, "spike" runs from it to the run end. Worker counts resolve from the
// specs by phase id.
TEST(ProducerReplacementTest, IntervalsBuiltFromBoundaryEvents) {
  PhaseBoundaryEvent event;
  event.set_phase_index(1);
  event.set_phase_id("spike");
  event.set_wall_time_unix_ns(5'000);
  event.set_ending_phase_index(0);
  event.set_ending_phase_id("warm");
  event.set_worker_count(2);

  PhaseSpec warm;
  warm.set_name("warm");
  warm.mutable_concurrency()->set_value("2");
  PhaseSpec spike;
  spike.set_name("spike");
  spike.mutable_concurrency()->set_value("4");

  const std::vector<PhaseInterval> intervals =
      PhaseIntervalsFromBoundaries(/*run_start_unix_ns=*/1'000, /*run_end_unix_ns=*/9'000,
                                   {event}, {warm, spike});
  ASSERT_EQ(intervals.size(), 2u);
  EXPECT_EQ(intervals[0].phase_id, "warm");
  EXPECT_EQ(intervals[0].start_unix_ns, 1'000);
  EXPECT_EQ(intervals[0].end_unix_ns, 5'000);
  EXPECT_EQ(intervals[0].worker_count, 2u);
  EXPECT_TRUE(intervals[0].drain_complete);
  EXPECT_EQ(intervals[1].phase_id, "spike");
  EXPECT_EQ(intervals[1].start_unix_ns, 5'000);
  EXPECT_EQ(intervals[1].end_unix_ns, 9'000);
  EXPECT_EQ(intervals[1].worker_count, 4u);
}

// A window straddling the boundary SPLITS into two pure parts, each with its
// phase id and worker count; the tree rides into the first part.
TEST(ProducerReplacementTest, StraddlingWindowSplitsAtBoundary) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)}, /*time_nanos=*/2'000,
                             /*duration_nanos=*/6'000);

  PhaseInterval warm;
  warm.phase_id = "warm";
  warm.start_unix_ns = 1'000'000'000;
  warm.end_unix_ns = 5'000'000'000;
  warm.drain_complete = true;
  warm.worker_count = 2;
  PhaseInterval spike;
  spike.phase_id = "spike";
  spike.start_unix_ns = 5'000'000'000;
  spike.end_unix_ns = 9'000'000'000;
  spike.drain_complete = true;
  spike.worker_count = 4;

  // The completed-work series is in the monotonic domain; the anchor offset
  // maps wall -> monotonic (offset 0 here).
  std::vector<CounterPoint> series = {{2'000'000'000ull, 10}, {5'000'000'000ull, 50},
                                      {8'000'000'000ull, 100}};

  SplitAlignment split = WindowPhaseAlignment::AlignSplit(
      tree, {warm, spike}, /*start=*/2'000'000'000, /*end=*/8'000'000'000, "cpu",
      /*uncertainty=*/0, series,
      /*anchor_offset_ns=*/0);
  ASSERT_EQ(split.pure.size(), 2u);
  EXPECT_TRUE(split.excluded.empty());

  EXPECT_EQ(split.pure[0].phase_id, "warm");
  EXPECT_EQ(split.pure[0].start_unix_ns, 2'000'000'000);
  EXPECT_EQ(split.pure[0].end_unix_ns, 5'000'000'000);
  EXPECT_DOUBLE_EQ(split.pure[0].purity_fraction, 1.0);
  EXPECT_EQ(split.pure[0].worker_count, 2u);
  // Each part carries its OWN work-units denominator.
  EXPECT_EQ(split.pure[0].work_units, 40u);
  EXPECT_EQ(split.pure[1].phase_id, "spike");
  EXPECT_EQ(split.pure[1].start_unix_ns, 5'000'000'000);
  EXPECT_EQ(split.pure[1].end_unix_ns, 8'000'000'000);
  EXPECT_DOUBLE_EQ(split.pure[1].purity_fraction, 1.0);
  EXPECT_EQ(split.pure[1].worker_count, 4u);
  EXPECT_EQ(split.pure[1].work_units, 50u);
  // The tree rides into the first part; the differential pair across the
  // boundary is the 8.4 case (different worker counts -> marked
  // non-comparable), computed on two separately built windows.
  EXPECT_EQ(split.pure[0].tree.root.size(), 1u);

  // The 8.4 machinery's first real-data path: differential selection across
  // the split refuses the pair with the reason.
  ProfileWindow current = split.pure[1];
  current.tree = MakeTree({MakeFrame("main", 100)});
  auto diff = ComputeDifferential(split.pure[0], current);
  ASSERT_TRUE(diff.ok()) << diff.status();
  EXPECT_FALSE(diff->comparable);
  EXPECT_NE(diff->incomparable_reason.find("worker"), std::string::npos) << diff->incomparable_reason;
}

// A part below the meaningful-minimum rule (one stats flush) is
// EXCLUDED-AND-MARKED with the reason — never silently merged, never
// comparable (purity 0.0).
TEST(ProducerReplacementTest, TooShortPartExcludedAndMarked) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  PhaseInterval warm;
  warm.phase_id = "warm";
  warm.start_unix_ns = 1'000'000'000;
  warm.end_unix_ns = 5'000'000'000;
  warm.drain_complete = true;
  warm.worker_count = 2;
  PhaseInterval spike;
  spike.phase_id = "spike";
  spike.start_unix_ns = 5'000'000'000;
  spike.end_unix_ns = 9'000'000'000;
  spike.drain_complete = true;
  spike.worker_count = 4;

  SplitAlignment split = WindowPhaseAlignment::AlignSplit(
      tree, {warm, spike}, /*start=*/4'999'500'000, /*end=*/6'000'000'000, "cpu", 0, {}, 0);
  // The "warm" part is 0.5ms — below kMinSplitWindowNs; the "spike" part is
  // 1s exactly (the rule is inclusive: >= the minimum is pure).
  ASSERT_EQ(split.pure.size(), 1u);
  ASSERT_EQ(split.excluded.size(), 1u);
  EXPECT_EQ(split.pure[0].phase_id, "spike");
  EXPECT_DOUBLE_EQ(split.pure[0].purity_fraction, 1.0);
  EXPECT_DOUBLE_EQ(split.excluded[0].purity_fraction, 0.0);
  ASSERT_EQ(split.reasons.size(), 1u);
  EXPECT_NE(split.reasons[0].find("meaningful-minimum"), std::string::npos) << split.reasons[0];
}

// 8.3 via the censoring flag (re-review remainder): a CENSORED boundary
// makes the ENDING phase's interval not drain-complete, so the straddling
// window's adjacent part is EXCLUDED-AND-MARKED — never purity 1.0.
TEST(ProducerReplacementTest, CensoredBoundaryExcludesTheAdjacentPart) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  PhaseBoundaryEvent event;
  event.set_phase_index(1);
  event.set_phase_id("spike");
  event.set_wall_time_unix_ns(5'000'000'000);
  event.set_ending_phase_index(0);
  event.set_ending_phase_id("warm");
  event.mutable_drain_duration()->set_seconds(5); // the capped full budget
  event.mutable_idle_gap()->set_nanos(100'000'000);
  event.set_worker_count(2);
  event.set_drain_censored(true);

  PhaseSpec warm;
  warm.set_name("warm");
  warm.mutable_concurrency()->set_value("2");
  PhaseSpec spike;
  spike.set_name("spike");
  spike.mutable_concurrency()->set_value("4");

  const std::vector<PhaseInterval> intervals =
      PhaseIntervalsFromBoundaries(/*run_start_unix_ns=*/1'000'000'000,
                                   /*run_end_unix_ns=*/9'000'000'000, {event}, {warm, spike});
  ASSERT_EQ(intervals.size(), 2u);
  // The ENDING interval ("warm") carries the censoring: not drain-complete.
  EXPECT_FALSE(intervals[0].drain_complete);
  // The interval the boundary opened ("spike") is unaffected.
  EXPECT_TRUE(intervals[1].drain_complete);

  SplitAlignment split = WindowPhaseAlignment::AlignSplit(
      tree, intervals, /*start=*/4'000'000'000, /*end=*/8'000'000'000, "cpu", 0, {}, 0);
  // The "warm" part [4s, 5s] contains the censored drain: EXCLUDED with the
  // rule-naming reason; the "spike" part [5s, 8s] stays pure.
  ASSERT_EQ(split.pure.size(), 1u);
  ASSERT_EQ(split.excluded.size(), 1u);
  EXPECT_EQ(split.pure[0].phase_id, "spike");
  EXPECT_DOUBLE_EQ(split.pure[0].purity_fraction, 1.0);
  EXPECT_EQ(split.excluded[0].phase_id, "warm");
  EXPECT_DOUBLE_EQ(split.excluded[0].purity_fraction, 0.0);
  ASSERT_EQ(split.reasons.size(), 1u);
  EXPECT_NE(split.reasons[0].find("drain not complete"), std::string::npos) << split.reasons[0];
}

// ---------------------------------------------------------------------------
// M3 Task 8: epoch-pure windows (spec 8.5). A window straddling an epoch
// change splits AT it, each part stamped with the epoch in effect; an
// epoch-boundary differential pair is COMPARABLE and labeled
// `knob effect: <epoch_a> -> <epoch_b>`; only worker-count changes stay
// non-comparable (M2's rule, unchanged).
// ---------------------------------------------------------------------------

TEST(EpochWindowTest, StraddlingWindowSplitsAtEpoch) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  PhaseInterval phase;
  phase.phase_id = "warm";
  phase.start_unix_ns = 1'000'000'000;
  phase.end_unix_ns = 8'000'000'000;
  phase.drain_complete = true;
  phase.worker_count = 2;

  // One applied change at t=5s inside the window [4s, 8s).
  std::vector<EpochMark> marks;
  marks.push_back({"epoch-1", /*wall=*/5'000'000'000, /*monotonic=*/100});

  SplitAlignment split = WindowPhaseAlignment::AlignSplit(
      tree, {phase}, /*start=*/4'000'000'000, /*end=*/8'000'000'000, "cpu", 0, {},
      /*anchor_offset=*/0, marks);
  // Two epoch-pure parts, split exactly at the epoch change.
  ASSERT_EQ(split.pure.size(), 2u);
  EXPECT_EQ(split.pure[0].start_unix_ns, 4'000'000'000);
  EXPECT_EQ(split.pure[0].end_unix_ns, 5'000'000'000);
  EXPECT_EQ(split.pure[0].epoch_id, "") << "before the first applied change";
  EXPECT_EQ(split.pure[1].start_unix_ns, 5'000'000'000);
  EXPECT_EQ(split.pure[1].end_unix_ns, 8'000'000'000);
  EXPECT_EQ(split.pure[1].epoch_id, "epoch-1");
  for (const ProfileWindow& part : split.pure) {
    EXPECT_DOUBLE_EQ(part.purity_fraction, 1.0);
    EXPECT_EQ(part.phase_id, "warm") << "the epoch cut does not change the phase";
    EXPECT_EQ(part.worker_count, 2u);
  }
  EXPECT_TRUE(split.reasons.empty());

  // A mark OUTSIDE the window cuts nothing: one part, epoch from the mark
  // before the window.
  std::vector<EpochMark> outside;
  outside.push_back({"epoch-1", /*wall=*/1'500'000'000, /*monotonic=*/10});
  SplitAlignment unsplit = WindowPhaseAlignment::AlignSplit(
      tree, {phase}, 4'000'000'000, 5'000'000'000, "cpu", 0, {}, 0, outside);
  ASSERT_EQ(unsplit.pure.size(), 1u);
  EXPECT_EQ(unsplit.pure[0].epoch_id, "epoch-1") << "the epoch in effect from the window start";

  // A sub-part below the meaningful minimum is excluded-and-marked, naming
  // its epoch span (the part under the applied change names it; the part
  // before the first change renders no epoch).
  std::vector<EpochMark> near;
  near.push_back({"epoch-1", /*wall=*/4'500'000'000, /*monotonic=*/50});
  SplitAlignment thin = WindowPhaseAlignment::AlignSplit(
      tree, {phase}, 4'000'000'000, 5'000'000'000, "cpu", 0, {}, 0, near);
  ASSERT_EQ(thin.pure.size(), 0u);
  ASSERT_EQ(thin.excluded.size(), 2u); // both sub-parts are below the minimum
  ASSERT_EQ(thin.reasons.size(), 2u);
  EXPECT_EQ(thin.excluded[0].epoch_id, "") << "before the first applied change";
  EXPECT_EQ(thin.excluded[1].epoch_id, "epoch-1");
  EXPECT_NE(thin.reasons[1].find("epoch 'epoch-1'"), std::string::npos) << thin.reasons[1];
  EXPECT_NE(thin.reasons[1].find("meaningful-minimum"), std::string::npos) << thin.reasons[1];
}

TEST(EpochWindowTest, EpochBoundaryPairIsComparableAndLabeled) {
  FoldedTree tree = MakeTree({MakeFrame("work", 100)});
  ProfileWindow before = WindowWith(tree, /*work_units=*/100, /*worker_count=*/4);
  before.epoch_id = "epoch-1";
  ProfileWindow after = WindowWith(tree, /*work_units=*/100, /*worker_count=*/4);
  after.epoch_id = "epoch-2";

  // The epoch-boundary pair is COMPARABLE by default (that is the point):
  // only worker-count changes stay non-comparable.
  auto diff = ComputeDifferential(before, after);
  ASSERT_TRUE(diff.ok()) << diff.status();
  EXPECT_TRUE(diff->comparable);

  // The label names the knob effect.
  EXPECT_EQ(DifferentialPairLabel(before, after), "knob effect: epoch-1 -> epoch-2");
  EXPECT_EQ(DifferentialPairLabel(after, before), "knob effect: epoch-2 -> epoch-1");
  // An empty epoch id (before the first applied change) renders as (none).
  ProfileWindow first = WindowWith(tree, 100, 4);
  first.epoch_id = "";
  EXPECT_EQ(DifferentialPairLabel(first, before), "knob effect: (none) -> epoch-1");

  // Same-state pairs route to the noise floor: no knob-effect label.
  EXPECT_TRUE(SameStatePair(before, before));
  EXPECT_TRUE(SameStatePair(first, first));
  EXPECT_FALSE(SameStatePair(before, after));
  EXPECT_EQ(DifferentialPairLabel(before, before), "");

  // A worker-count change across an epoch boundary stays non-comparable
  // (M2's rule wins).
  ProfileWindow scaled = WindowWith(tree, 100, /*worker_count=*/8);
  scaled.epoch_id = "epoch-2";
  auto scaled_diff = ComputeDifferential(before, scaled);
  ASSERT_TRUE(scaled_diff.ok());
  EXPECT_FALSE(scaled_diff->comparable);
  EXPECT_NE(scaled_diff->incomparable_reason.find("worker count"), std::string::npos);

  // The noise-floor claim is honest only for a same-state pair: the caller
  // routes an epoch-boundary pair to the knob-effect differential (the
  // pure predicates the routing reads).
  ProfileWindow other_epoch = WindowWith(tree, 100, 4);
  other_epoch.epoch_id = "epoch-2";
  EXPECT_FALSE(SameStatePair(before, other_epoch));
  ProfileWindow same_epoch = WindowWith(tree, 100, 4);
  same_epoch.epoch_id = "epoch-1";
  EXPECT_TRUE(SameStatePair(before, same_epoch));
}

}  // namespace
}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
