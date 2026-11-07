// FORK-ONLY (We-Amp): see profile_pipeline.h. The moved-as-is pipeline body
// (formerly tui_app.cc's anonymous namespace) — behavior pinned unchanged by
// the flamegraph pipeline suite.
#include "source/client/tui/profile_pipeline.h"

#include <cmath>
#include <fstream>
#include <sstream>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"

#include "source/client/tui/pprof_fold.h"
#include "source/client/tui/profile_symbolizer.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Reads a whole file; nullopt on any failure (missing, unreadable).
std::optional<std::string> ReadWholeFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return std::nullopt;
  }
  std::ostringstream ss;
  ss << file.rdbuf();
  return ss.str();
}

} // namespace

ProfileJobOutcome RunProfilePipeline(const ProfilePipelineConfig& config,
                                     const FetchedProfile& fetched,
                                     const ProfileJobInput& input,
                                     int64_t clock_offset_uncertainty_ns) {
  ProfileJobOutcome outcome;
  outcome.symbolizer_tool = absl::StrJoin(config.pprof_tool_argv, " ");

  // The windows carry the owning run's identity (drive backlog item 4):
  // stamped on every produced window before each return below.
  const auto stamp_run_key = [&input, &outcome]() {
    for (ProfileWindow& window : outcome.windows) {
      window.run_key = input.run_key;
    }
  };

  outcome.raw_bytes = ReadWholeFile(fetched.local_path).value_or("");
  if (outcome.raw_bytes.empty()) {
    outcome.note = absl::StrCat("profile fetch failed: staged file unreadable: ",
                                fetched.local_path);
    return outcome;
  }

  // Tree: symbolize-then-fallback. A symbolization failure that the fallback
  // recovers from is carried in `symbolizer_note` and APPENDED to the final
  // outcome note (review round 1 minor: the fallback must not silently
  // overwrite why the machine route did not produce the tree).
  absl::StatusOr<FoldedTree> tree_or = absl::InternalError("profile not loaded");
  std::string symbolizer_note;
  bool used_fallback = false;
  if (!config.sut_binary_path.empty()) {
    ProfileSymbolizer symbolizer(config.pprof_tool_argv, config.pprof_binary_path);
    const std::string symbolized_path = absl::StrCat(fetched.local_path, ".symbolized.pb");
    const absl::Status symbolized =
        symbolizer.Symbolize(fetched.local_path, config.sut_binary_path, symbolized_path);
    if (symbolized.ok()) {
      auto profile_or = LoadProfile(symbolized_path);
      if (profile_or.ok()) {
        tree_or = FoldProfile(*profile_or);
      } else {
        tree_or = profile_or.status();
      }
    } else if (!absl::IsNotFound(symbolized)) {
      // A tool that exists but failed is worth reporting verbatim; a missing
      // tool falls through to the fallback (the graceful degradation path,
      // with the absence noted).
      symbolizer_note = absl::IsNotFound(symbolized)
                            ? "symbolizer tool absent"
                            : absl::StrCat("symbolization failed: ", symbolized.message());
    }
  }
  if (!tree_or.ok()) {
    // Raw-proto fallback: proto-native staged profiles parse directly (the
    // unsymbolized-degradation shape — locations without functions fold to
    // "0x<addr>" frames). A legacy gperftools profile cannot; the outcome
    // note says so.
    used_fallback = true;
    auto profile_or = LoadProfile(fetched.local_path);
    if (profile_or.ok()) {
      tree_or = FoldProfile(*profile_or);
    } else if (symbolizer_note.empty()) {
      symbolizer_note = "no working symbolizer";
    } else {
      absl::StrAppend(&symbolizer_note, "; raw-proto fallback also failed");
    }
    if (!tree_or.ok() && !profile_or.ok()) {
      symbolizer_note = absl::StrCat(
          symbolizer_note, (symbolizer_note.empty() ? "" : "; "),
          "profile unusable: the staged profile is not a pprof proto (legacy gperftools "
          "format needs the machine route)");
    }
  }
  if (!tree_or.ok()) {
    outcome.note = symbolizer_note.empty()
                       ? absl::StrCat("profile fold failed: ", tree_or.status().message())
                       : absl::StrCat(symbolizer_note, " (", tree_or.status().message(), ")");
    return outcome;
  }
  if (used_fallback) {
    absl::StrAppend(&symbolizer_note,
                    symbolizer_note.empty() ? "" : "; ",
                    "window folded from the raw staged proto");
  }

  // Window interval selection + the §8.5 cross-check (plan task 4's TODO,
  // owned by this wiring): when the tree carries timestamps, the tree's own
  // interval is authoritative — the fetch-orchestration interval is verified
  // against it, with any disagreement beyond the admin Date granularity
  // recorded as mac<->SUT clock-offset uncertainty. Without tree timestamps
  // (the legacy gperftools conversion — the live M1 path), the fetch
  // orchestration bounds the interval.
  int64_t window_start = fetched.requested_start_unix_ns;
  int64_t window_end = fetched.requested_end_unix_ns;
  int64_t uncertainty = clock_offset_uncertainty_ns;
  if (tree_or->time_nanos != 0 && tree_or->duration_nanos != 0) {
    const int64_t observed_offset =
        std::abs(tree_or->time_nanos - fetched.requested_start_unix_ns);
    uncertainty = std::max(uncertainty, std::max<int64_t>(0, observed_offset - kDateGranularityNs));
    window_start = tree_or->time_nanos;
    window_end = tree_or->time_nanos + tree_or->duration_nanos;
  }

  // Work units through the anchor: monotonic ~= wall - (anchor.wall -
  // anchor.monotonic).
  const int64_t anchor_offset = input.anchor.wall_time_unix_ns - input.anchor.monotonic_ns;

  if (!input.boundary_events.empty() || !input.phase_specs.empty() || !input.epoch_marks.empty()) {
    // M2 split-at-boundary alignment: engine intervals joined on the
    // streamed (wall, monotonic) pairs (DQ-3). A window straddling a
    // boundary splits into per-phase parts. Before the run's FIRST boundary
    // lands, a phase run still gets its phase-0 interval from the specs
    // (bounded by observed time only — never a fabricated future end). M3:
    // a legacy single-phase run WITH applied knob epochs also goes through
    // the split producer — the epoch cuts join the phase cuts (spec 8.5).
    const int64_t interval_end =
        input.run_finished ? input.run_end_unix_ns
                           : std::max(input.run_start_unix_ns + 1, fetched.requested_end_unix_ns);
    std::vector<PhaseInterval> intervals = PhaseIntervalsFromBoundaries(
        input.run_start_unix_ns, interval_end, input.boundary_events, input.phase_specs);
    if (intervals.empty() && !input.phase_specs.empty()) {
      // Pre-first-boundary phase run: one interval for the live phase-0,
      // ending at the observed bound.
      PhaseInterval first;
      first.phase_id = input.phase_specs.front().name();
      first.start_unix_ns = input.run_start_unix_ns;
      first.end_unix_ns = interval_end;
      first.drain_complete = true;
      int parsed = 0;
      if (input.phase_specs.front().has_concurrency() &&
          absl::SimpleAtoi(input.phase_specs.front().concurrency().value(), &parsed) &&
          parsed > 0) {
        first.worker_count = static_cast<uint64_t>(parsed);
      }
      intervals.push_back(std::move(first));
    }
    if (intervals.empty() && !input.epoch_marks.empty()) {
      // Legacy single-phase run with epochs: one "main" interval over the
      // run span; the epoch cuts sub-split it.
      intervals.push_back(PreA1RunInterval(input.run_start_unix_ns, interval_end,
                                           input.worker_count));
    }
    if (intervals.empty()) {
      outcome.note = "window rejected: no phase intervals from the boundary events";
      return outcome;
    }
    SplitAlignment split = WindowPhaseAlignment::AlignSplit(
        *std::move(tree_or), intervals, window_start, window_end, fetched.profile_type,
        uncertainty, input.completed_series, anchor_offset, input.epoch_marks);
    // The excluded-and-marked parts ALWAYS land (store + session, purity 0.0,
    // the rule-naming reason) — the fix-round rule: a marked part must never
    // vanish just because a pure sibling exists; only the note changes shape.
    // Snapshot the part counts BEFORE the move: the notes below classify the
    // split result, and reading a moved-from vector there would always look
    // empty — clearing windows the splitter had just produced.
    const size_t pure_count = split.pure.size();
    outcome.windows = std::move(split.pure);
    for (ProfileWindow& excluded : split.excluded) {
      outcome.windows.push_back(std::move(excluded));
    }
    std::string note;
    if (pure_count > 0) {
      note = absl::StrCat("window split at the boundary: ", pure_count, " pure part(s)");
      for (const std::string& reason : split.reasons) {
        absl::StrAppend(&note, " | ", reason);
      }
    } else if (!split.reasons.empty()) {
      note = "window EXCLUDED-AND-MARKED: " + split.reasons.front();
      for (size_t i = 1; i < split.reasons.size(); ++i) {
        absl::StrAppend(&note, " | ", split.reasons[i]);
      }
    } else {
      // The winalign diagnosis's honesty affordance: the note names the run
      // interval it aligned against, so a wrong-context pull (a stale
      // finished-run instance) is self-identifying at read time.
      note = absl::StrCat("window rejected: no part overlaps the run's phase intervals [",
                          input.run_start_unix_ns, ", ", interval_end, ")");
      outcome.windows.clear();
    }
    if (!symbolizer_note.empty()) {
      absl::StrAppend(&note, " | ", symbolizer_note);
    }
    outcome.note = std::move(note);
    stamp_run_key();
    return outcome;
  }

  // Legacy single-interval path (M1 producer; no boundary events). A
  // finished run's span is [run_start, recorded run end]; a live run's
  // phase-so-far ends at the window's own fetch boundary.
  PhaseInterval phase = PreA1RunInterval(
      input.run_start_unix_ns,
      input.run_finished ? input.run_end_unix_ns
                         : std::max(input.run_start_unix_ns + 1, fetched.requested_end_unix_ns),
      input.worker_count);

  const uint64_t lo_monotonic_ns =
      static_cast<uint64_t>(std::max<int64_t>(0, window_start - anchor_offset));
  const uint64_t hi_monotonic_ns =
      static_cast<uint64_t>(std::max<int64_t>(0, window_end - anchor_offset));
  const uint64_t work_units =
      CompletedWorkInMonotonicRange(input.completed_series, lo_monotonic_ns, hi_monotonic_ns);

  // Align. The rejected (impure) case builds the EXCLUDED-AND-MARKED window
  // directly instead of round-tripping through WindowPhaseAlignment::Align:
  // Align takes the tree BY VALUE and destroys it on every error return, so
  // the marked window (which carries the tree for rendering) must be
  // assembled here. The rejection conditions mirror Align's documented
  // contract exactly, and the purity fraction is the same overlap formula.
  const int64_t overlap_start = std::max(window_start, phase.start_unix_ns);
  const int64_t overlap_end = std::min(window_end, phase.end_unix_ns);
  const bool contained =
      phase.drain_complete && window_end <= phase.end_unix_ns && overlap_end > overlap_start;

  ProfileWindow window;
  window.tree = *std::move(tree_or);
  window.profile_samples = window.tree.total_samples;
  window.start_unix_ns = window_start;
  window.end_unix_ns = window_end;
  window.phase_id = phase.phase_id;
  window.worker_count = input.worker_count;
  window.work_units = work_units;
  window.profile_type = fetched.profile_type;
  window.symbolized = window.tree.symbolized;
  window.interval_source = (window.tree.time_nanos != 0 && window.tree.duration_nanos != 0)
                               ? WindowIntervalSource::kProfileTimestamps
                               : WindowIntervalSource::kFetchOrchestration;
  window.clock_offset_uncertainty_ns = uncertainty;

  if (contained) {
    auto aligned = WindowPhaseAlignment::Align(std::move(window.tree), phase, window.start_unix_ns,
                                               window.end_unix_ns, window.worker_count,
                                               window.work_units, window.profile_type, uncertainty);
    if (aligned.ok()) {
      outcome.windows.push_back(*std::move(aligned));
      outcome.note = absl::StrCat("window pulled: pure (work units ", work_units, ")");
      if (!symbolizer_note.empty()) {
        absl::StrAppend(&outcome.note, " | ", symbolizer_note);
      }
      stamp_run_key();
      return outcome;
    }
    // Align can still reject a "contained" pair only on the degenerate
    // guards (kInvalidArgument) — no window, honest failure.
    outcome.note = absl::StrCat("window rejected: ", aligned.status().message());
    return outcome;
  }

  // Excluded-and-marked: never pure, never comparable (ComputeDifferential
  // refuses every pair with an impure side). The window still lands — in the
  // store and the session — with the impurity carried in purity_fraction.
  const int64_t duration = window_end - window_start;
  const double purity =
      duration > 0 && overlap_end > overlap_start
          ? static_cast<double>(overlap_end - overlap_start) / static_cast<double>(duration)
          : 0.0;
  window.purity_fraction = purity;
  outcome.windows.push_back(std::move(window));
  outcome.note = absl::StrCat(
      "window EXCLUDED-AND-MARKED (purity ", absl::StrFormat("%.3f", purity),
      " < 1.0): never comparable — ", "window straddles the run interval");
  if (!symbolizer_note.empty()) {
    absl::StrAppend(&outcome.note, " | ", symbolizer_note);
  }
  stamp_run_key();
  return outcome;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
