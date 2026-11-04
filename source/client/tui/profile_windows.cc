// FORK-ONLY (We-Amp): phase-pure window alignment and differential math for
// the TUI flamegraph pane (M1 plan task 4). Pure functions plus one
// mutex-guarded store; see the header for the measurement-semantics contracts
// (spec 8.3 / 8.4).
#include "source/client/tui/profile_windows.h"

#include <algorithm>
#include <cmath>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

absl::Status FailedPrecondition(absl::string_view message) {
  return absl::FailedPreconditionError(message);
}

// Frame-path index of one side's tree: full root-to-frame function-name path
// -> frame + its child-index address (FramePath). The string path is the
// base<->current match key (a separator control character that cannot appear
// in a sane symbol name; unique per node because folding merges same-name
// same-parent nodes); the child-index path is what the renderer addresses
// the overlay with (DifferentialNodeDiff::child_path).
constexpr char kPathSeparator = '\x1f';

struct IndexedFrame {
  const FoldedFrame* frame;
  FramePath child_path;
};

using FrameIndex = absl::flat_hash_map<std::string, IndexedFrame>;

void IndexFrames(const std::vector<FoldedFrame>& frames, std::string* prefix, FramePath* indices,
                 FrameIndex* out) {
  for (size_t i = 0; i < frames.size(); ++i) {
    const size_t base_len = prefix->size();
    const size_t base_depth = indices->size();
    if (!prefix->empty()) {
      prefix->push_back(kPathSeparator);
    }
    prefix->append(frames[i].function);
    indices->push_back(static_cast<int>(i));
    (*out)[*prefix] = {&frames[i], *indices};
    IndexFrames(frames[i].children, prefix, indices, out);
    prefix->resize(base_len);
    indices->resize(base_depth);
  }
}

// C(t): the counter's last value at or before t (0 before the series starts).
// The series is in the feed's append order (ascending monotonic); a later
// point at or before t wins.
uint64_t CounterValueAtOrBefore(const std::vector<CounterPoint>& series, uint64_t t) {
  uint64_t value = 0;
  for (const CounterPoint& point : series) {
    if (point.monotonic_ns <= t) {
      value = point.value;
    }
  }
  return value;
}

}  // namespace

std::vector<PhaseInterval> PhaseIntervalsFromBoundaries(
    int64_t run_start_unix_ns, int64_t run_end_unix_ns,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseSpec>& specs) {
  if (events.empty() || run_end_unix_ns <= run_start_unix_ns) {
    return {};
  }
  // Interval starts: run start, then each boundary's wall stamp (ascending;
  // the engine emits boundaries in phase order).
  std::vector<PhaseInterval> intervals;
  intervals.reserve(events.size() + 1);
  int64_t cursor = run_start_unix_ns;
  for (size_t i = 0; i < events.size(); ++i) {
    const nighthawk::client::PhaseBoundaryEvent& event = events[i];
    const int64_t boundary = event.wall_time_unix_ns();
    if (boundary <= cursor || boundary > run_end_unix_ns) {
      // A boundary outside the run span cannot open an interval; skip it
      // (recorded data, never fabricated).
      continue;
    }
    PhaseInterval interval;
    interval.phase_id = event.ending_phase_id();
    interval.start_unix_ns = cursor;
    interval.end_unix_ns = boundary;
    interval.drain_complete = true;
    interval.worker_count = event.worker_count();
    intervals.push_back(std::move(interval));
    cursor = boundary;
  }
  // The final interval: the phase the last boundary opened (the event's own
  // phase fields name it), ending at the run end.
  PhaseInterval last;
  last.phase_id = events.back().phase_id();
  last.start_unix_ns = cursor;
  last.end_unix_ns = run_end_unix_ns;
  last.drain_complete = true;
  last.worker_count = 0;
  // Worker count from the specs by phase id; unknown (0) when absent — that
  // marks differentials non-comparable instead of guessing.
  for (const nighthawk::client::PhaseSpec& spec : specs) {
    if (spec.name() == last.phase_id) {
      int parsed = 0;
      if (spec.has_concurrency() && absl::SimpleAtoi(spec.concurrency().value(), &parsed) &&
          parsed > 0) {
        last.worker_count = static_cast<uint64_t>(parsed);
      }
      break;
    }
  }
  intervals.push_back(std::move(last));
  return intervals;
}

SplitAlignment WindowPhaseAlignment::AlignSplit(
    FoldedTree tree, const std::vector<PhaseInterval>& intervals, int64_t start_unix_ns,
    int64_t end_unix_ns, std::string profile_type, int64_t clock_offset_uncertainty_ns,
    const std::vector<CounterPoint>& completed_series, int64_t anchor_offset_ns) {
  if (end_unix_ns <= start_unix_ns) {
    // Mirror Align's degenerate guard; the caller excludes-and-marks.
    return SplitAlignment{};
  }
  SplitAlignment result;
  int64_t cursor = start_unix_ns;
  for (const PhaseInterval& interval : intervals) {
    if (cursor >= end_unix_ns) {
      break;
    }
    const int64_t boundary = std::min<int64_t>(interval.end_unix_ns, end_unix_ns);
    if (boundary <= cursor) {
      continue;
    }
    // One part: [cursor, min(interval end, window end)). Fully inside the
    // interval by construction.
    const int64_t part_start = std::max(cursor, interval.start_unix_ns);
    const int64_t part_end = boundary;
    cursor = boundary;
    if (part_end <= part_start) {
      continue;
    }

    ProfileWindow part;
    part.start_unix_ns = part_start;
    part.end_unix_ns = part_end;
    part.phase_id = interval.phase_id;
    part.worker_count = interval.worker_count;
    part.profile_type = profile_type;
    part.symbolized = tree.symbolized;
    part.interval_source = (tree.time_nanos != 0 && tree.duration_nanos != 0)
                               ? WindowIntervalSource::kProfileTimestamps
                               : WindowIntervalSource::kFetchOrchestration;
    part.clock_offset_uncertainty_ns = clock_offset_uncertainty_ns;
    // Each part carries its own per-unit-of-work denominator through the
    // caller's anchor mapping.
    const uint64_t lo = static_cast<uint64_t>(std::max<int64_t>(0, part_start - anchor_offset_ns));
    const uint64_t hi = static_cast<uint64_t>(std::max<int64_t>(0, part_end - anchor_offset_ns));
    part.work_units = CompletedWorkInMonotonicRange(completed_series, lo, hi);

    if (!interval.drain_complete) {
      part.purity_fraction = 0.0;
      result.reasons.push_back(absl::StrCat("part [", part_start, ", ", part_end,
                                            "] in phase '", interval.phase_id,
                                            "': drain not complete"));
      result.excluded.push_back(std::move(part));
      continue;
    }
    if (part_end - part_start < kMinSplitWindowNs) {
      part.purity_fraction = 0.0;
      result.reasons.push_back(absl::StrCat("part [", part_start, ", ", part_end, "] in phase '",
                                            interval.phase_id, "': below the ",
                                            kMinSplitWindowNs / 1000000000,
                                            "s meaningful-minimum rule; excluded-and-marked"));
      result.excluded.push_back(std::move(part));
      continue;
    }
    part.purity_fraction = 1.0;
    result.pure.push_back(std::move(part));
  }
  // The tree rides into the FIRST part (the only tree the flame renderer
  // needs; the parts are one window's samples). Later parts carry an empty
  // tree by design: the store renders the newest window, and a split lands
  // newest-part-last so the visible tree is the part the pane shows.
  if (!result.pure.empty()) {
    result.pure.front().tree = std::move(tree);
  } else if (!result.excluded.empty()) {
    result.excluded.front().tree = std::move(tree);
  }
  return result;
}

absl::StatusOr<ProfileWindow> WindowPhaseAlignment::Align(
    FoldedTree tree, const PhaseInterval& phase, int64_t start_unix_ns, int64_t end_unix_ns,
    uint64_t worker_count, uint64_t work_units, std::string profile_type,
    int64_t clock_offset_uncertainty_ns) {
  if (end_unix_ns <= start_unix_ns) {
    return absl::InvalidArgumentError(
        absl::StrCat("degenerate window interval [", start_unix_ns, ", ", end_unix_ns, "]"));
  }
  if (phase.end_unix_ns <= phase.start_unix_ns) {
    return absl::InvalidArgumentError(absl::StrCat("degenerate phase interval [",
                                                   phase.start_unix_ns, ", ", phase.end_unix_ns,
                                                   "]"));
  }
  // 8.3: only time inside a phase whose drain is complete can be pure.
  if (!phase.drain_complete) {
    return FailedPrecondition(
        absl::StrCat("phase '", phase.phase_id, "' drain not complete: window excluded-and-marked"));
  }
  // Not fully inside: extending past the boundary is the split-at-boundary
  // case (M2 scope) — never silently comparable.
  if (end_unix_ns > phase.end_unix_ns) {
    return FailedPrecondition(absl::StrCat("window end ", end_unix_ns, " past phase '",
                                           phase.phase_id, "' end ", phase.end_unix_ns,
                                           ": window excluded-and-marked"));
  }
  const int64_t overlap_start = std::max(start_unix_ns, phase.start_unix_ns);
  const int64_t overlap_end = std::min(end_unix_ns, phase.end_unix_ns);
  if (overlap_end <= overlap_start) {
    return FailedPrecondition(absl::StrCat("window [", start_unix_ns, ", ", end_unix_ns,
                                           "] does not overlap phase '", phase.phase_id,
                                           "'"));
  }

  ProfileWindow window;
  window.tree = std::move(tree);
  window.start_unix_ns = start_unix_ns;
  window.end_unix_ns = end_unix_ns;
  // Purity formula (header): overlap / window duration. Contained -> 1.0;
  // starting inside a prior phase -> fraction < 1.0 (M2 machinery).
  window.purity_fraction =
      static_cast<double>(overlap_end - overlap_start) / static_cast<double>(end_unix_ns - start_unix_ns);
  window.phase_id = phase.phase_id;
  window.worker_count = worker_count;
  window.work_units = work_units;
  window.profile_type = std::move(profile_type);
  window.symbolized = window.tree.symbolized;
  window.interval_source =
      (window.tree.time_nanos != 0 && window.tree.duration_nanos != 0)
          ? WindowIntervalSource::kProfileTimestamps
          : WindowIntervalSource::kFetchOrchestration;
  window.clock_offset_uncertainty_ns = clock_offset_uncertainty_ns;
  return window;
}

uint64_t CompletedWorkInMonotonicRange(const std::vector<CounterPoint>& completed_series,
                                       uint64_t lo_monotonic_ns, uint64_t hi_monotonic_ns) {
  if (hi_monotonic_ns <= lo_monotonic_ns || completed_series.empty()) {
    return 0;
  }
  const uint64_t hi_value = CounterValueAtOrBefore(completed_series, hi_monotonic_ns);
  const uint64_t lo_value = CounterValueAtOrBefore(completed_series, lo_monotonic_ns);
  return hi_value >= lo_value ? hi_value - lo_value : 0;
}

absl::StatusOr<DifferentialResult> ComputeDifferential(const ProfileWindow& base,
                                                       const ProfileWindow& current) {
  // 8.3-strict: an impure side rejects the pair outright — the differential
  // is never computed, never rendered dimmed.
  if (base.purity_fraction < 1.0 || current.purity_fraction < 1.0) {
    return FailedPrecondition(
        "impure window pair rejected (purity < 1.0 on either side): no differential computed");
  }
  if (base.profile_type != current.profile_type) {
    return FailedPrecondition(absl::StrCat("profile_type mismatch ('", base.profile_type,
                                           "' vs '", current.profile_type,
                                           "'): not a ratio, no differential computed"));
  }
  if (base.work_units == 0 || current.work_units == 0) {
    return FailedPrecondition(
        "zero completed work in a window: the per-unit-of-work denominator is undefined");
  }
  // 8.4: a worker-count boundary is marked non-comparable by default — a
  // result, not an error (the caller renders the mark).
  if (base.worker_count != current.worker_count) {
    DifferentialResult result;
    result.comparable = false;
    result.incomparable_reason = absl::StrCat("worker count changed across the boundary (",
                                              base.worker_count, " -> ", current.worker_count, ")");
    return result;
  }

  FrameIndex base_index;
  FrameIndex cur_index;
  std::string path_a;
  std::string path_b;
  FramePath indices_a;
  FramePath indices_b;
  IndexFrames(base.tree.root, &path_a, &indices_a, &base_index);
  IndexFrames(current.tree.root, &path_b, &indices_b, &cur_index);

  // Union of paths present on either side, deterministically ordered by the
  // final sort.
  std::vector<const std::string*> union_paths;
  absl::flat_hash_map<std::string_view, const std::string*> union_index;
  union_index.reserve(base_index.size() + cur_index.size());
  auto add_path = [&union_paths, &union_index](const std::string& path) {
    auto [it, inserted] = union_index.emplace(path, &path);
    if (inserted) {
      union_paths.push_back(it->second);
    }
  };
  for (const auto& entry : base_index) {
    add_path(entry.first);
  }
  for (const auto& entry : cur_index) {
    add_path(entry.first);
  }

  const double base_total_per_work =
      static_cast<double>(base.tree.total_value) / static_cast<double>(base.work_units);
  const double cur_total_per_work =
      static_cast<double>(current.tree.total_value) / static_cast<double>(current.work_units);

  std::vector<std::pair<const std::string*, DifferentialNodeDiff>> scored;
  scored.reserve(union_paths.size());
  for (const std::string* path : union_paths) {
    DifferentialNodeDiff diff;
    auto base_it = base_index.find(*path);
    auto cur_it = cur_index.find(*path);
    if (base_it != base_index.end()) {
      diff.frame = base_it->second.frame;
      diff.child_path = base_it->second.child_path;
      diff.base_per_work = static_cast<double>(base_it->second.frame->total_value) /
                           static_cast<double>(base.work_units);
    }
    if (cur_it != cur_index.end()) {
      if (diff.frame == nullptr) {
        diff.frame = cur_it->second.frame;
      }
      // The address follows the CURRENT side when the node renders there
      // (the renderer paints the after picture); base-only nodes keep the
      // base address (see DifferentialNodeDiff::child_path).
      diff.child_path = cur_it->second.child_path;
      diff.cur_per_work = static_cast<double>(cur_it->second.frame->total_value) /
                          static_cast<double>(current.work_units);
    }
    diff.delta_ratio =
        (diff.cur_per_work - diff.base_per_work) / std::max(diff.base_per_work, kDeltaRatioEpsilon);
    scored.emplace_back(path, std::move(diff));
  }

  std::sort(scored.begin(), scored.end(),
            [](const std::pair<const std::string*, DifferentialNodeDiff>& a,
               const std::pair<const std::string*, DifferentialNodeDiff>& b) {
              const double mag_a = std::fabs(a.second.delta_ratio);
              const double mag_b = std::fabs(b.second.delta_ratio);
              if (mag_a != mag_b) {
                return mag_a > mag_b;
              }
              return *a.first < *b.first;
            });

  DifferentialResult result;
  result.comparable = true;
  result.base_total_per_work = base_total_per_work;
  result.cur_total_per_work = cur_total_per_work;
  result.diffs.reserve(scored.size());
  for (auto& [path, diff] : scored) {
    result.diffs.push_back(std::move(diff));
  }
  return result;
}

absl::StatusOr<DifferentialResult> ComputeNoiseFloor(const ProfileWindow& w1,
                                                     const ProfileWindow& w2) {
  // Identical math and checks; the distinct name is the claim label (see the
  // header): pre-A1 this IS the differential the demo shows.
  return ComputeDifferential(w1, w2);
}

void ProfileWindowStore::store(std::vector<ProfileWindow> windows) {
  absl::MutexLock lock(&mu_);
  current_ = std::make_shared<const std::vector<ProfileWindow>>(std::move(windows));
}

std::shared_ptr<const std::vector<ProfileWindow>> ProfileWindowStore::load() const {
  absl::MutexLock lock(&mu_);
  return current_;
}

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
