// FORK-ONLY (We-Amp): phase-pure profile-window alignment and the
// differential math for the TUI flamegraph pane (M1 plan task 4; design
// spec 8.3 window phase-purity and 8.4 per-unit-of-work normalization +
// same-state noise floor).
//
// Everything here is pure except ProfileWindowStore (a mutex-guarded
// immutable swap in the M0b SnapshotStore pattern). The fetch orchestration
// (Task 6) produces the inputs; this layer owns the measurement semantics.
//
// Quarantine note: depends only on pprof_fold.h, run_view_model.h, and absl —
// no Envoy C++ sources.
#ifndef NIGHTHAWK_TUI_PROFILE_WINDOWS_H_
#define NIGHTHAWK_TUI_PROFILE_WINDOWS_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "api/client/live.pb.h"
#include "api/client/options.pb.h"
#include "source/client/tui/pprof_fold.h"
#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Where a window's [start, end] interval came from. Design finding (Task 3):
// real gperftools profiles carry NO time_nanos (legacy conversion), so most
// real windows fall in the fetch-orchestration case; the label keeps 8.3
// purity claims honest about whose clock bounded the window.
enum class WindowIntervalSource {
  // The parsed profile carried sample timestamps: the interval is the
  // profile's own SUT clock (8.5's sanctioned join key).
  kProfileTimestamps,
  // The profile carries no timestamps: the interval boundary is the FETCH
  // orchestration's own clock — the collector's start/stop around the
  // enable-window — as passed by the caller (Task 6 wiring). The interval is
  // never fabricated inside the profile layer.
  kFetchOrchestration,
};

// A phase interval on the unix clock. Pre-A1 (M1) there is exactly one phase,
// "main", spanning the whole run; M2 replaces the producer with phase events.
struct PhaseInterval {
  std::string phase_id;
  int64_t start_unix_ns = 0;
  int64_t end_unix_ns = 0;
  // Whether the phase's post-boundary drain has completed: 8.3 counts only
  // time where the drain is done; a still-draining phase is never pure.
  bool drain_complete = false;
  // Informational on the interval itself (the start request's concurrency).
  // Align() takes the window's worker count as its own parameter — the two
  // carry the same value pre-A1; M2's phase events make them distinct.
  uint64_t worker_count = 0;
};

// One applied knob-epoch change (M3, spec 8.5): the epoch whose schedule
// took over, and the ENGINE (wall, monotonic) pair at the bend instant —
// the same join-key discipline as the phase boundaries. Marks are consumed
// in ascending wall order; the epoch in effect at instant t is the last
// mark with wall <= t (empty before the first mark).
struct EpochMark {
  std::string epoch_id;
  int64_t wall_time_unix_ns = 0;
  int64_t monotonic_ns = 0;
};

// Pre-A1 producer (plan major-fix): the whole run is one phase — the interval
// spans run start->stop, the drain is trivially complete, and the worker
// count comes from the start request's concurrency. M2 REPLACES this producer
// with PhaseIntervalsFromBoundaries (kept for legacy single-phase runs).
inline PhaseInterval PreA1RunInterval(int64_t run_start_unix_ns, int64_t run_end_unix_ns,
                                      uint64_t worker_count) {
  PhaseInterval interval;
  interval.phase_id = "main";
  interval.start_unix_ns = run_start_unix_ns;
  interval.end_unix_ns = run_end_unix_ns;
  interval.drain_complete = true;
  interval.worker_count = worker_count;
  return interval;
}

// The M2 producer: the run's phase intervals from the STREAMED boundary
// events, joined on the engine-emitted (wall, monotonic) pairs (the DQ-3
// rule: the alignment joins on the discrete events, never synthetic
// flushes). Interval i spans [start_i, start_{i+1}) where the starts are the
// run start, then each boundary's wall stamp; the final interval ends at
// `run_end_unix_ns` (a live run's caller passes the fetch boundary — never a
// fabricated future end). An event-opened interval is drain-complete UNLESS
// its boundary was CENSORED (8.3: a still-draining phase is never pure — the
// censored drain means in-flight work was still completing at the interval's
// end, so the ending interval's parts are excluded-and-marked); the final
// interval counts as drain-complete like the M1 producer did (no observed
// censoring to attribute). Worker counts resolve from the specs
// by phase id (an unparseable/absent concurrency leaves 0 — such intervals
// still align but carry the unknown count, which marks differentials
// non-comparable per 8.4 rather than guessing). Pure.
//
// CLOCK DOMAIN (m6 caveat): every stamp here is the ENGINE's wall clock, and
// the intervals join only same-domain stamps (engine wall to engine wall), so
// the split math is internally consistent on any venue. The TUI host's
// mac-wall run stamps bound the OUTER interval; on the local venue the two
// clocks are one. On a remote rig the mac<->SUT skew rides the recorded
// clock_offset_uncertainty_ns of the resulting windows (the same 8.5 caveat
// the fetch-orchestration interval carries) — the local-first venue is
// exact; rig windows are honest about their uncertainty, never silently so.
std::vector<PhaseInterval> PhaseIntervalsFromBoundaries(
    int64_t run_start_unix_ns, int64_t run_end_unix_ns,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseSpec>& specs);

// One symbolized profile window, aligned to a phase.
struct ProfileWindow {
  FoldedTree tree;
  // Window interval on the SUT clock per 8.5 when the profile carries
  // timestamps; otherwise the fetch-orchestration collector boundary in the
  // caller's clock (see interval_source).
  int64_t start_unix_ns = 0;
  int64_t end_unix_ns = 0;
  // Fraction of the window's duration inside one phase whose drain is
  // complete (8.3). Contained window: 1.0. Anything below 1.0 is excluded
  // from differentials by ComputeDifferential (never rendered as comparable).
  double purity_fraction = 0.0;
  std::string phase_id;      // constant "main" pre-A1
  // The knob epoch whose schedule produced the samples (M3, spec 8.5):
  // empty before the run's first applied live change. Epoch-pure windows
  // never straddle an epoch change (the split produces per-epoch parts);
  // an epoch-boundary PAIR is comparable by default (that is the point) —
  // only worker-count changes stay non-comparable.
  std::string epoch_id;
  uint64_t worker_count = 0;  // non-comparability key (8.4)
  // Completed requests inside the window — the per-unit-of-work denominator
  // (8.4), anchor-mapped by the caller (CompletedWorkInMonotonicRange).
  uint64_t work_units = 0;
  std::string profile_type;  // "cpu" | "alloc"
  bool symbolized = false;
  // Which clock bounded [start, end] (see enum). Derived by Align from the
  // tree's timestamp fields, never fabricated.
  WindowIntervalSource interval_source = WindowIntervalSource::kFetchOrchestration;
  // mac<->SUT clock-offset uncertainty in ns (0 on the local venue; recorded
  // per 8.5 when admin lacks a clock surface). Metadata only.
  int64_t clock_offset_uncertainty_ns = 0;
};

// The split-at-boundary outcome (8.3, M2): a window straddling a phase
// boundary is SPLIT at the boundary into per-phase parts — each part is
// fully contained in one interval (purity exactly 1.0, its own phase id and
// worker count). A part below kMinSplitWindowNs (the documented
// meaningful-minimum rule: one stats flush is the smallest span a percentile
// band can even observe), or one inside a drain-incomplete interval, is
// EXCLUDED-AND-MARKED: it still lands (store + session) but stamped purity
// 0.0 so no consumer can ever treat it as comparable, with the reason
// recorded alongside.
struct SplitAlignment {
  // The pure split parts, ordered by start time. The tree rides into the
  // FIRST part (one window's samples; the newest part lands last in the
  // store so the pane renders the tree it expects).
  std::vector<ProfileWindow> pure;
  // The excluded-and-marked remainders, ordered by start time (purity 0.0;
  // the reasons align by index).
  std::vector<ProfileWindow> excluded;
  std::vector<std::string> reasons;
};

// A split part shorter than this is "too short to be meaningful" (the
// documented un-splittable rule): one stats flush at the default cadence is
// the smallest span a percentile band can even observe, so a thinner part
// carries no honest sample mass. EXCLUDED-AND-MARKED, never silently merged
// into its neighbor.
inline constexpr int64_t kMinSplitWindowNs = 1'000'000'000;

// The pure owning function of Review Focus 3: aligns a folded tree's window
// to a phase interval and computes its purity fraction.
//
// Purity formula: purity = overlap([start, end], [phase.start, phase.end]) /
// (end - start). A fully contained window is exactly 1.0; a window that
// starts inside a PRIOR phase but ends within this one keeps a fraction < 1.0
// (M2 machinery — impossible pre-A1, where the single phase spans the run).
//
// Rejections (caller EXCLUDES-AND-MARKS; M1's only handling — the
// split-at-boundary variant is AlignSplit's, kept next to Align so the two
// contracts read together):
//   - drain not complete                      -> kFailedPrecondition
//   - end past the phase end                  -> kFailedPrecondition
//   - no overlap with the phase               -> kFailedPrecondition
//   - end <= start / inverted phase interval  -> kInvalidArgument
class WindowPhaseAlignment {
 public:
  static absl::StatusOr<ProfileWindow> Align(
      FoldedTree tree, const PhaseInterval& phase, int64_t start_unix_ns, int64_t end_unix_ns,
      uint64_t worker_count, uint64_t work_units, std::string profile_type,
      int64_t clock_offset_uncertainty_ns = 0);

  // The split-at-boundary alignment (the SplitAlignment contract above).
  // Degenerate input (end <= start, empty intervals) yields an empty result
  // — the caller excludes-and-marks with its own reason. M3: `epoch_marks`
  // (ascending wall order) sub-split each phase part at the applied knob
  // changes — a window straddling an epoch change splits AT it (spec 8.5),
  // each part stamped with the epoch in effect over its span (empty before
  // the first mark). A below-minimum sub-part is excluded-and-marked like
  // any other part, with its epoch span named.
  static SplitAlignment AlignSplit(FoldedTree tree,
                                   const std::vector<PhaseInterval>& intervals,
                                   int64_t start_unix_ns, int64_t end_unix_ns,
                                   std::string profile_type,
                                   int64_t clock_offset_uncertainty_ns,
                                   const std::vector<CounterPoint>& completed_series,
                                   int64_t anchor_offset_ns,
                                   const std::vector<EpochMark>& epoch_marks = {});
};

// The epoch in effect at unix instant `t` (the last mark with wall <= t);
// empty before the first mark. Ascending-order marks expected; pure.
std::string EpochInEffect(const std::vector<EpochMark>& marks, int64_t t);

// Work-units join (plan major-fix): the completed-request counter series
// lives in the monotonic domain; the window lives in the unix/SUT domain.
// The caller maps the window interval through the first-snapshot
// (wall, monotonic) anchor — monotonic ~= wall - (first_snapshot.wall -
// first_snapshot.monotonic) — and passes the resulting monotonic [lo, hi]
// here. The series is the M0b RunViewModel completed counter series
// (ascending monotonic order, as the feed appends it).
//
// Returns C(hi) - C(lo) where C(t) is the counter's last value at or before
// t (0 before the series starts). Degenerate (lo >= hi) or empty input: 0.
// Non-monotone data is clamped at 0 rather than underflowing.
uint64_t CompletedWorkInMonotonicRange(const std::vector<CounterPoint>& completed_series,
                                       uint64_t lo_monotonic_ns, uint64_t hi_monotonic_ns);

// One node of the differential overlay, on the union tree (paths present in
// either side). LIFETIME (a test caught the trap): `frame` points into the
// INPUT trees (base preferred when the node exists on both sides) — the two
// ProfileWindow arguments must OUTLIVE the result. Never call
// ComputeDifferential/ComputeNoiseFloor with temporaries and use the result
// afterwards; in the pipeline the windows live in the ProfileWindowStore, so
// the pointers are stable for as long as the loaded store snapshot is held.
struct DifferentialNodeDiff {
  const FoldedFrame* frame = nullptr;
  // Positional address of this node (M1 task 5 interface addition): child
  // indices from the tree root. This is how the renderer addresses the
  // overlay onto a (possibly zoomed) FoldedTree — honest for same-named
  // frames, where a function-name path would alias recursion and repeated
  // stacks. When the node exists on the CURRENT side the path addresses the
  // current tree (the renderer paints the after picture); for a node that
  // exists only on the base side it addresses the base tree (it then never
  // matches a rendered node, which is the truthful outcome). Note this can
  // address a different tree than `frame` points into — `frame` stays
  // base-preferred (its total_value is the T4 anchor), `child_path` is the
  // render-addressing half. The string name path remains the BASE<->CURRENT
  // match key (folding merges same-name-same-parent nodes, so a name path is
  // unique inside one tree).
  FramePath child_path;
  // Per-unit-of-work values (total_value / work_units).
  double base_per_work = 0.0;
  double cur_per_work = 0.0;
  // (cur - base) / max(base, epsilon); nodes absent on the base side rise
  // out of the epsilon floor.
  double delta_ratio = 0.0;
};

struct DifferentialResult {
  // Union-tree nodes, |delta_ratio| descending (ties: frame path ascending,
  // for determinism).
  std::vector<DifferentialNodeDiff> diffs;
  double base_total_per_work = 0.0;
  double cur_total_per_work = 0.0;
  // False only for the worker-count change (8.4's sanction: marked
  // non-comparable by default, with the reason). IMPURE pairs never reach a
  // DifferentialResult at all — see ComputeDifferential.
  bool comparable = true;
  std::string incomparable_reason;
};

// Normalization floor for delta_ratio when a node has no base-side samples.
inline constexpr double kDeltaRatioEpsilon = 1e-12;

// 8.3-strict differential (Review Focus 3 + 4): either side
// purity_fraction < 1.0 -> kFailedPrecondition — the pair is NEVER computed,
// never rendered dimmed (stricter is cheaper). Worker-count mismatch is NOT
// an error: it returns OK with comparable == false and the reason (8.4's
// marked-non-comparable-by-default path). Mismatched profile types are a
// semantic error (cpu vs alloc is not a ratio), not a comparability case:
// kFailedPrecondition.
//
// Per-unit-of-work math: both sides normalize total_value by work_units;
// delta_ratio = (cur_per_work - base_per_work) / max(base_per_work, epsilon).
// NOTE on the plan's test sketch: "identical trees with different work_units
// -> ratio 0" does not hold under this definition (and must not: a uniform
// per-request shift from a workload change is exactly what normalization
// should surface); ratios are 0 precisely when per-work values are equal,
// e.g. the tree scaling with the work.
absl::StatusOr<DifferentialResult> ComputeDifferential(const ProfileWindow& base,
                                                       const ProfileWindow& current);

// Same-state adjacent-window pair (8.4's noise floor): identical math and
// checks as ComputeDifferential. M3: the noise floor claim is honest ONLY
// for a same-state pair (SameStatePair below) — a pair spanning a knob
// epoch boundary must route to the knob-effect differential instead.
absl::StatusOr<DifferentialResult> ComputeNoiseFloor(const ProfileWindow& w1,
                                                     const ProfileWindow& w2);

// The differential pair's claim label (8.4 + M3): an epoch-boundary pair is
// the knob-effect differential — "knob effect: <epoch_a> -> <epoch_b>".
// A same-state pair (equal epoch ids, both empty included) returns "" — the
// caller's existing same-state/noise-floor labeling applies. Worker-count
// changes are orthogonal (ComputeDifferential's non-comparable verdict
// wins); profile types/purity are checked before the label is consulted.
std::string DifferentialPairLabel(const ProfileWindow& base, const ProfileWindow& current);

// Whether a same-state (noise-floor) claim is honest for this pair: the
// noise floor is for windows under the SAME knob epoch (8.4); a pair that
// spans an epoch boundary is the knob-effect differential, not noise.
bool SameStatePair(const ProfileWindow& base, const ProfileWindow& current);

// Mutex-guarded immutable swap, same pattern as the M0b SnapshotStore: the
// pool-completion side stores fully-formed windows; the render side loads a
// cheap shared_ptr copy and works against an immutable snapshot.
class ProfileWindowStore {
 public:
  // Swaps in a fresh immutable snapshot. Previously loaded pointers keep
  // serving the old version.
  void store(std::vector<ProfileWindow> windows);
  // The current snapshot, or nullptr before the first store().
  std::shared_ptr<const std::vector<ProfileWindow>> load() const;

 private:
  mutable absl::Mutex mu_;
  std::shared_ptr<const std::vector<ProfileWindow>> current_ ABSL_GUARDED_BY(mu_);
};

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk

#endif  // NIGHTHAWK_TUI_PROFILE_WINDOWS_H_
