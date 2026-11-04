#include "source/client/tui/tui_app.h"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ftxui/component/component.hpp"
#include "ftxui/component/event.hpp"
#include "ftxui/component/screen_interactive.hpp"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"
#include "tclap/CmdLine.h"

#include "source/client/tui/pprof_fold.h"

#include "source/client/tui/layout.h"
#include "source/client/tui/profile_session_conversion.h"
#include "source/client/tui/profile_symbolizer.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using ftxui::CatchEvent;
using ftxui::Component;
using ftxui::Event;
using ftxui::Input;
using ftxui::Renderer;

// How long finishSession waits for the stopped run's on_finished before
// giving up (and letting the writer drain without a run end record).
constexpr std::chrono::seconds kFinishTimeout{30};

// The SUT stats scrape cadence: one scrape per stats flush interval (spec §4;
// plan task 7 wiring).
constexpr auto kScrapeCadence = std::chrono::seconds(kStatusBarFlushIntervalSeconds);

// The admin clock surface's granularity: the RFC 7231 Date header carries
// whole seconds only, so an observed mac<->SUT offset within this bound is
// truncation, not skew. Clock-offset uncertainty is recorded only beyond it.
constexpr int64_t kDateGranularityNs = 1'000'000'000;

// The live run's worker count, for the staged preview's worker-set
// annotation: the last declared phase's workers when the run carries a phase
// vector, else the run-level concurrency; 1 when nothing is declared.
uint32_t CurrentRunWorkerCount(const nighthawk::client::CommandLineOptions& options) {
  std::string concurrency;
  if (!options.phases().empty()) {
    if (options.phases().rbegin()->has_concurrency()) {
      concurrency = options.phases().rbegin()->concurrency().value();
    }
  } else if (options.has_concurrency()) {
    concurrency = options.concurrency().value();
  }
  int parsed = 0;
  return !concurrency.empty() && absl::SimpleAtoi(concurrency, &parsed) && parsed > 0
             ? static_cast<uint32_t>(parsed)
             : 1;
}

int64_t nowUnixNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

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

// One pool job's outcome: the produced windows (pure, and excluded-and-
// marked; a split window yields up to two parts) plus what the session
// records and the flame-pane note need.
struct ProfileJobOutcome {
  std::vector<ProfileWindow> windows;
  // The staged raw profile bytes (for the session record) and the
  // provenance label.
  std::string raw_bytes;
  std::string symbolizer_tool;
  // Human-facing outcome line for the flame pane note.
  std::string note;
};

// ONE pool-job pipeline (plan task 6): the fetched+retrieved staged profile
// becomes an aligned ProfileWindow (pure) or an excluded-and-marked one
// (impure — Review Focus 3 end-to-end). Never crashes on a bad profile and
// never fabricates an interval: the window interval is the parsed profile's
// own stamps when it carries timestamps (the §8.5 sanctioned join key), the
// fetch orchestration's collector boundary otherwise.
//
// Symbolization path: the machine route runs FIRST when a SUT binary is
// configured; the documented raw-proto fallback (LoadProfile on the staged
// file directly) covers proto-native sources — a staged legacy gperftools
// profile without a working symbolizer degrades to no window (the reason
// lands in the outcome note), never a crash.
ProfileJobOutcome RunProfilePipeline(const ProfilePipelineConfig& config,
                                     const FetchedProfile& fetched,
                                     const ProfileJobInput& input,
                                     int64_t clock_offset_uncertainty_ns) {
  ProfileJobOutcome outcome;
  outcome.symbolizer_tool = absl::StrJoin(config.pprof_tool_argv, " ");

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

  if (!input.boundary_events.empty()) {
    // M2 split-at-boundary alignment: engine intervals joined on the
    // streamed (wall, monotonic) pairs (DQ-3). A window straddling a
    // boundary splits into per-phase parts; too-short or drain-incomplete
    // parts are excluded-and-marked with the reason.
    const int64_t interval_end =
        input.run_finished ? input.run_end_unix_ns
                           : std::max(input.run_start_unix_ns + 1, fetched.requested_end_unix_ns);
    const std::vector<PhaseInterval> intervals = PhaseIntervalsFromBoundaries(
        input.run_start_unix_ns, interval_end, input.boundary_events, input.phase_specs);
    if (intervals.empty()) {
      outcome.note = "window rejected: no phase intervals from the boundary events";
      return outcome;
    }
    SplitAlignment split = WindowPhaseAlignment::AlignSplit(
        *std::move(tree_or), intervals, window_start, window_end, fetched.profile_type,
        uncertainty, input.completed_series, anchor_offset);
    outcome.windows = std::move(split.pure);
    const size_t split_count = outcome.windows.size();
    std::string note;
    if (split_count > 0) {
      note = absl::StrCat("window split at the boundary: ", split_count, " pure part(s)");
      for (const std::string& reason : split.reasons) {
        absl::StrAppend(&note, " | ", reason);
      }
    } else if (!split.excluded.empty()) {
      note = "window EXCLUDED-AND-MARKED: " + split.reasons.front();
      for (size_t i = 1; i < split.reasons.size(); ++i) {
        absl::StrAppend(&note, " | ", split.reasons[i]);
      }
      outcome.windows = std::move(split.excluded);
    } else {
      note = "window rejected: no part overlaps the run's phase intervals";
    }
    if (!symbolizer_note.empty()) {
      absl::StrAppend(&note, " | ", symbolizer_note);
    }
    outcome.note = std::move(note);
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
  return outcome;
}

} // namespace

// The q path: stop-if-running, close the session (completed reflects how the
// run actually ended), then exit the loop. The last snapshot shown before
// exit is the final data (gRPC cancel semantics). A close error mid-exit is
// not actionable: the writer then drains without a run end record at
// destruction (documented).
bool quitShell(RunController& controller, const std::function<void()>& exit_loop) {
  (void)controller.finishSession(controller.lastRunEndedCompleted());
  exit_loop();
  return true;
}

void FillOverlayState(const ShellState& state, OverlayState* overlay) {
  overlay->uri_field = state.uri_field;
  overlay->rps_field = state.rps_field;
  overlay->duration_field = state.duration_field;
  overlay->show_errors = state.show_errors;
  overlay->help_open = state.help_open;
  // The controller-rejection line rides along: dropping this copy would make
  // a refused submit silent again while every state-level assertion stays
  // green (the re-review finding).
  overlay->start_rejected = state.start_rejected;
}

std::vector<CounterPoint>
MergeCompletedSeries(const absl::flat_hash_map<std::string, std::vector<CounterPoint>>& counters) {
  static const char* const kCompletedNames[] = {
      "benchmark.http_1xx", "benchmark.http_2xx",      "benchmark.http_3xx",
      "benchmark.http_4xx", "benchmark.http_5xx",      "benchmark.http_xxx",
      "benchmark.stream_resets", "benchmark.pool_connection_failure",
  };
  // The union of stamps, ascending and deduplicated.
  std::vector<uint64_t> stamps;
  for (const char* name : kCompletedNames) {
    auto it = counters.find(name);
    if (it == counters.end()) {
      continue;
    }
    for (const CounterPoint& point : it->second) {
      stamps.push_back(point.monotonic_ns);
    }
  }
  std::sort(stamps.begin(), stamps.end());
  stamps.erase(std::unique(stamps.begin(), stamps.end()), stamps.end());

  // C(t) per source series (last value at or before t), summed at every
  // union stamp — the same convention CompletedWorkInMonotonicRange applies
  // to the merged result.
  std::vector<CounterPoint> merged;
  merged.reserve(stamps.size());
  for (const uint64_t stamp : stamps) {
    uint64_t value = 0;
    for (const char* name : kCompletedNames) {
      auto it = counters.find(name);
      if (it == counters.end()) {
        continue;
      }
      uint64_t at_or_before = 0;
      for (const CounterPoint& point : it->second) {
        if (point.monotonic_ns <= stamp) {
          at_or_before = point.value;
        }
      }
      value += at_or_before;
    }
    merged.push_back(CounterPoint{stamp, value});
  }
  return merged;
}

FlamePaneState BuildFlamePane(std::shared_ptr<const std::vector<ProfileWindow>> windows,
                              const FlamegraphViewState& view, absl::string_view note,
                              const DiffOverlay* overlay) {
  FlamePaneState pane;
  pane.view = view;
  pane.note = std::string(note);
  if (windows == nullptr || windows->empty()) {
    return pane;
  }
  const ProfileWindow& window = windows->back();
  pane.tree = &window.tree;
  pane.window_header = absl::StrCat(
      "window #", windows->size(), ": ", window.profile_type,
      " | purity ", absl::StrFormat("%.2f", window.purity_fraction),
      " | work units ", window.work_units,
      window.symbolized ? ""
                        : " | UNSYMBOLIZED (some/all frames unresolved; check --pprof-tool and "
                          "the PPROF_BINARY_PATH layout)",
      window.interval_source == WindowIntervalSource::kProfileTimestamps
          ? " | interval: profile timestamps"
          : " | interval: fetch orchestration");
  if (view.diff_mode && overlay != nullptr) {
    pane.diff = &overlay->result();
  }
  return pane;
}

bool HandleShellEvent(const ftxui::Event& event, ShellState& state, RunController& controller,
                      const std::function<void()>& force_redraw,
                      const std::function<void()>& exit_loop) {
  // Ctrl-C quits from EVERY mode, before any mode-specific map can swallow
  // it: with the dialog open the editing branch would otherwise return false,
  // FTXUI's default machinery would record SIGINT, and the re-raise would
  // kill the process with the session open (the finding-1 residual). The
  // dialog state is irrelevant to shutdown — the same quitShell path (flush
  // inside the loop, then exit) runs in both modes.
  if (event == Event::CtrlC) {
    return quitShell(controller, exit_loop);
  }
  if (state.mode == InputMode::kEditingStartRun) {
    if (event == Event::Escape) {
      state.uri_field.clear();
      state.rps_field.clear();
      state.duration_field.clear();
      state.show_errors = false;
      state.start_rejected.clear();
      state.mode = InputMode::kNavigate;
      state.mode_mirror.store(state.mode, std::memory_order_relaxed);
      // Exactly one forced redraw so the panes catch up on the state edited
      // under the suspension (input_mode.h's rule).
      force_redraw();
      return true;
    }
    if (event == Event::Return) {
      state.dialog.set_uri(state.uri_field);
      uint32_t rps = 0;
      uint32_t duration_seconds = 0;
      // Unparseable (or empty) fields stay 0, which the dialog rejects.
      const bool parsed = absl::SimpleAtoi(state.rps_field, &rps) &&
                          absl::SimpleAtoi(state.duration_field, &duration_seconds);
      state.dialog.set_rps(rps);
      state.dialog.set_duration_seconds(duration_seconds);
      // Field-validation failure first: inline errors on the offending field
      // rows plus the error-count border title.
      if (!parsed || !state.dialog.valid()) {
        // A failed enter is never silent and never closes the box: the
        // inline errors render on the offending field rows until the form
        // validates or esc cancels.
        state.show_errors = true;
        return true;
      }
      const absl::Status started = controller.startRun(state.dialog.build_request());
      if (started.ok()) {
        state.uri_field.clear();
        state.rps_field.clear();
        state.duration_field.clear();
        state.show_errors = false;
        state.start_rejected.clear();
        state.mode = InputMode::kNavigate;
        state.mode_mirror.store(state.mode, std::memory_order_relaxed);
        // Exactly one forced redraw on the dialog close (input_mode.h).
        force_redraw();
      } else {
        // A controller rejection (e.g. a session already open) is not a
        // field error: say why the start is impossible on a distinct line
        // inside the box. Reusing the field-error path would render a
        // "start run - 0 errors" border title — a false statement (M0b
        // review finding).
        state.show_errors = false;
        state.start_rejected = absl::StrCat("cannot start: ", started.message());
      }
      return true;
    }
    return false; // let the text inputs handle the character
  }

  // The search input mode (M1, the flame pane's frame search): typed
  // characters edit the draft; enter applies it to the view state, esc
  // cancels and discards. Both exits post the one forced redraw
  // RefreshSuspended's rule requires.
  if (state.mode == InputMode::kEditingSearch) {
    if (event == Event::Escape) {
      state.search_field.clear();
      state.mode = InputMode::kNavigate;
      state.mode_mirror.store(state.mode, std::memory_order_relaxed);
      force_redraw();
      return true;
    }
    if (event == Event::Return) {
      state.flame_view.search = state.search_field;
      state.mode = InputMode::kNavigate;
      state.mode_mirror.store(state.mode, std::memory_order_relaxed);
      force_redraw();
      return true;
    }
    if (event == Event::Backspace) {
      if (!state.search_field.empty()) {
        state.search_field.pop_back();
      }
      return true;
    }
    if (event.is_character()) {
      state.search_field += event.character();
      return true;
    }
    return false;
  }

  // The staged next-phase editor (M2, knobs tab + a running run): typed
  // digits edit the draft for the selected knob; Tab cycles the knob; Enter
  // stages the draft into the preview; Esc discards EVERYTHING staged and
  // returns to navigate. The staged values are preview-only — the timeline
  // marks nothing until the engine confirms a boundary, and in M2 no staged
  // value ever reaches the running engine.
  if (state.mode == InputMode::kEditingStagedPhase) {
    const char* field_names[] = {"concurrency", "connections", "pending", "active"};
    if (event == Event::Escape) {
      state.staged_next_phase = nighthawk::client::PhaseSpec{};
      state.staged_draft.clear();
      state.staged_field = 0;
      state.mode = InputMode::kNavigate;
      state.mode_mirror.store(state.mode, std::memory_order_relaxed);
      force_redraw();
      return true;
    }
    if (event == Event::Tab) {
      state.staged_field = (state.staged_field + 1) % 4;
      state.staged_draft.clear();
      controller.NoteFlameStatus(
          absl::StrCat("staged edit field: ", field_names[state.staged_field]));
      return true;
    }
    if (event == Event::Return) {
      uint32_t value = 0;
      if (!absl::SimpleAtoi(state.staged_draft, &value) || value == 0) {
        controller.NoteFlameStatus("staged edit: enter a positive integer");
        return true;
      }
      switch (state.staged_field) {
      case 0:
        state.staged_next_phase.mutable_concurrency()->set_value(std::to_string(value));
        break;
      case 1:
        state.staged_next_phase.mutable_connections()->set_value(value);
        break;
      case 2:
        state.staged_next_phase.mutable_max_pending_requests()->set_value(value);
        break;
      default:
        state.staged_next_phase.mutable_max_active_requests()->set_value(value);
        break;
      }
      // A staged spec needs a non-empty name to render its preview.
      state.staged_next_phase.set_name("next");
      state.staged_draft.clear();
      controller.NoteFlameStatus(absl::StrCat("staged for the next phase: ",
                                              field_names[state.staged_field], "=", value,
                                              " (preview only; esc discards)"));
      return true;
    }
    if (event == Event::Backspace) {
      if (!state.staged_draft.empty()) {
        state.staged_draft.pop_back();
      }
      return true;
    }
    if (event.is_character()) {
      state.staged_draft += event.character();
      return true;
    }
    return false;
  }

  // Navigate mode key map. Tab indices match the tab strip's order: c ->
  // charts (0), f -> flame (1, M1), k -> knobs (2).
  if (event == Event::Character('c')) {
    state.active_tab = 0;
    return true;
  }
  if (event == Event::Character('f')) {
    state.active_tab = 1;
    return true;
  }
  if (event == Event::Character('k')) {
    state.active_tab = 2;
    return true;
  }
  if (event == Event::Character('?')) {
    state.help_open = !state.help_open;
    return true;
  }
  if (state.help_open && event == Event::Escape) {
    // Only escape's navigate-mode job is closing the help overlay; it is
    // listed in the overlay's own footer line. (On the flame tab a second
    // esc unzooms — the overlay's footer names esc's flame-tab job too.)
    state.help_open = false;
    return true;
  }
  if (event == Event::Character('n')) {
    state.uri_field.clear();
    state.rps_field.clear();
    state.duration_field.clear();
    state.show_errors = false;
    state.start_rejected.clear();
    state.mode = InputMode::kEditingStartRun;
    state.mode_mirror.store(state.mode, std::memory_order_relaxed);
    return true;
  }
  if (event == Event::Character('s')) {
    // Stop key: feed stop() only; the service's watchdog ends the run.
    controller.stopRun();
    return true;
  }
  if (event == Event::Character('e')) {
    // Staged next-phase editing (M2): only while a run is active — the
    // preview describes the NEXT phase of a live run. Refusals surface as a
    // transient flame-pane note, never a silent no-op.
    if (state.active_tab != 2) {
      controller.NoteFlameStatus("staged edits live on the knobs tab (k, then e)");
      return true;
    }
    if (!controller.runInProgress()) {
      controller.NoteFlameStatus("no run to stage for: e stages edits while a run is live");
      return true;
    }
    state.staged_draft.clear();
    state.staged_field = 0;
    state.mode = InputMode::kEditingStagedPhase;
    state.mode_mirror.store(state.mode, std::memory_order_relaxed);
    return true;
  }
  if (event == Event::Character('q')) {
    return quitShell(controller, exit_loop);
  }

  // --- M1 flame-pane keys --------------------------------------------------
  //
  // p/d/N work from any tab (they drive the pipeline, whose pane lives on
  // tab 1); the frame-navigation keys act only when the flame tab is up.
  if (event == Event::Character('/')) {
    // Search entry: the draft starts empty; the applied query (any earlier
    // enter) stays in force until a new one lands, and esc discards the
    // draft without touching the applied query. Global (the highlights are
    // only visible on the flame tab, but entering the mode from anywhere is
    // harmless and keeps the key map simple).
    state.search_field.clear();
    state.mode = InputMode::kEditingSearch;
    state.mode_mirror.store(state.mode, std::memory_order_relaxed);
    return true;
  }

  if (event == Event::Character('p')) {
    const absl::Status status = controller.profileWindowOpen()
                                    ? controller.StopProfileWindowAndFold()
                                    : controller.StartProfileWindow();
    if (!status.ok()) {
      controller.NoteFlameStatus(std::string(status.message()));
    }
    return true;
  }
  if (event == Event::Character('d') || event == Event::Character('N')) {
    const bool noise_floor = event == Event::Character('N');
    if (state.flame_view.diff_mode && state.flame_diff_is_noise == noise_floor) {
      // The toggle off: a second press of the same key exits the view.
      state.flame_view.diff_mode = false;
      controller.NoteFlameStatus(noise_floor ? "noise-floor view closed" : "differential closed");
    } else {
      state.flame_view.diff_mode = true;
      state.flame_diff_is_noise = noise_floor;
      controller.ComputeWindowDiffForLastTwoPureWindows(noise_floor);
    }
    return true;
  }

  if (state.active_tab == 1) {
    const auto windows = controller.loadWindows();
    const FoldedTree* tree =
        (windows != nullptr && !windows->empty()) ? &windows->back().tree : nullptr;
    if (event == Event::ArrowDown || event == Event::ArrowUp || event == Event::ArrowLeft ||
        event == Event::ArrowRight) {
      if (tree != nullptr) {
        // Navigation addresses the ZOOMED tree (paths are post-re-root,
        // pre-collapse per flamegraph.h's addressing contract).
        const FoldedTree zoomed =
            ZoomToFrame(*tree, state.flame_view.zoom_path.value_or(FramePath{}));
        const FramePath current = state.flame_view.selected.value_or(FramePath{});
        FramePath next = current;
        if (event == Event::ArrowDown) {
          next = NavDown(zoomed, current);
        } else if (event == Event::ArrowUp) {
          next = NavUp(zoomed, current);
        } else {
          next = NavSibling(zoomed, current, event == Event::ArrowRight ? 1 : -1);
        }
        if (ResolveSelection(zoomed, next).has_value()) {
          state.flame_view.selected = next;
        }
      }
      return true;
    }
    if (event == Event::Return) {
      // Zoom to the selected frame: re-root (the addressing contract keeps
      // the selection in post-zoom coordinates; it restarts at the new root).
      if (tree != nullptr && state.flame_view.selected.has_value() &&
          !state.flame_view.selected->empty()) {
        const FoldedTree zoomed =
            ZoomToFrame(*tree, state.flame_view.zoom_path.value_or(FramePath{}));
        if (ResolveSelection(zoomed, *state.flame_view.selected).has_value()) {
          state.flame_view.zoom_path = *state.flame_view.selected;
          state.flame_view.selected = FramePath{};
        }
      }
      return true;
    }
    if (event == Event::Escape || event == Event::Backspace) {
      if (state.flame_view.zoom_path.has_value()) {
        state.flame_view.zoom_path.reset();
        state.flame_view.selected = FramePath{};
      }
      return true;
    }
  }

  return false;
}

RunController::RunController(const std::shared_ptr<grpc::ChannelInterface>& channel,
                             std::string session_prefix)
    : session_prefix_(std::move(session_prefix)),
      // feed_ last, matching declaration order: it is destroyed first, which
      // is what makes teardown-with-a-live-run safe (see the header comment).
      // pool_ after it joins any in-flight M1 job first of all members.
      scrape_admin_(AdminEndpoints{}),
      feed_(channel,
            RunFeedCallbacks{std::bind(&RunController::onSnapshot, this, std::placeholders::_1),
                             std::bind(&RunController::onPhaseEvent, this, std::placeholders::_1),
                             std::bind(&RunController::onCompleted, this, std::placeholders::_1),
                             std::bind(&RunController::onFinished, this, std::placeholders::_1)}) {}

RunController::~RunController() {
  // Stop the scrape cadence and join it before any member it touches goes
  // away (the pool's destructor joins the profile/differential jobs).
  scraping_.store(false, std::memory_order_relaxed);
  if (scraper_.joinable()) {
    scraper_.join();
  }
}

void RunController::set_snapshot_notification(std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  snapshot_notification_ = std::move(callback);
}

// TEST-ONLY retriever decorator (see ProfilePipelineConfig's
// test_retriever_delay_ms): delays the retrieval so tests can land events
// inside the pool job's retrieval window.
class DelayedRetriever : public ProfileRetriever {
public:
  DelayedRetriever(std::unique_ptr<ProfileRetriever> delegate, int delay_ms)
      : delegate_(std::move(delegate)), delay_ms_(delay_ms) {}

  absl::StatusOr<std::string> Retrieve(absl::string_view sut_path) override {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
    return delegate_->Retrieve(sut_path);
  }

private:
  std::unique_ptr<ProfileRetriever> delegate_;
  int delay_ms_;
};

void RunController::ConfigureProfiling(ProfilePipelineConfig config) {
  // Configure-once, before use (pool jobs capture config by value at submit;
  // nothing here is hot-swappable — a second call is a no-op).
  std::lock_guard<std::mutex> lock(mutex_);
  if (profiling_configured_) {
    return;
  }
  profiling_configured_ = true;
  profile_config_ = std::move(config);
  std::unique_ptr<ProfileRetriever> retriever =
      profile_config_.retriever_kind == ProfilePipelineConfig::RetrieverKind::kVolumeMount
          ? std::unique_ptr<ProfileRetriever>(
                std::make_unique<VolumeMountRetriever>(profile_config_.mount_root))
          : std::unique_ptr<ProfileRetriever>(
                std::make_unique<ScpRetriever>(profile_config_.scp_host));
  if (profile_config_.test_retriever_delay_ms > 0) {
    retriever = std::make_unique<DelayedRetriever>(std::move(retriever),
                                                   profile_config_.test_retriever_delay_ms);
  }
  fetcher_ = std::make_unique<ProfileFetcher>(AdminClient(profile_config_.admin),
                                              std::move(retriever),
                                              profile_config_.sut_profile_path,
                                              profile_config_.staging_dir);
  scrape_admin_ = AdminClient(profile_config_.admin);
}

bool RunController::profilingConfigured() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return profiling_configured_;
}

absl::Status RunController::StartProfileWindow() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!profiling_configured_) {
      return absl::FailedPreconditionError(
          "profiling is not configured (pass --profile-mount or --scp-host)");
    }
    if (!has_run_stamp_) {
      return absl::FailedPreconditionError(
          "no run to align against: open profile windows during a run - only "
          "windows pulled inside a run can be pure");
    }
  }
  // M1 final fixes: an in-flight close owns fetcher_ (its retrieval is
  // running on the pool); re-arming here would race it on the shared fetcher
  // and can tear the staged profile. Refuse until the close job lands.
  if (folding_.load(std::memory_order_relaxed)) {
    return absl::FailedPreconditionError(
        "a profile window is still folding: wait for it to land before opening the next");
  }
  if (window_open_.exchange(true)) {
    return absl::FailedPreconditionError("a profile window is already open (p closes it)");
  }
  int window_seconds = 5;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    window_seconds = profile_config_.window_seconds;
  }
  // Single small POST on the steering thread; the blocking fetch belongs to
  // the pool job (profile_fetcher.h's threading contract).
  const absl::Status started = fetcher_->StartWindow(window_seconds);
  if (!started.ok()) {
    window_open_.store(false, std::memory_order_relaxed);
    return started;
  }
  {
    absl::MutexLock lock(&diff_mu_);
    flame_note_ = absl::StrCat("profile window open (", window_seconds, "s) - p closes it");
  }
  return absl::OkStatus();
}

absl::Status RunController::StopProfileWindowAndFold() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!profiling_configured_) {
      return absl::FailedPreconditionError(
          "profiling is not configured (pass --profile-mount or --scp-host)");
    }
    if (!anchor_.valid()) {
      return absl::FailedPreconditionError(
          "no snapshot anchor yet: the work-units join needs the run's first flush");
    }
  }
  if (!window_open_.exchange(false)) {
    return absl::FailedPreconditionError("no profile window is open (p opens one)");
  }
  folding_.store(true, std::memory_order_relaxed);

  // Deferred-close fix (M1 final fixes): disable the SUT profiler HERE, on
  // the steering thread — one small POST, symmetric with StartWindow's. The
  // pool job keeps retrieve -> symbolize -> ... -> align only, so pool
  // latency can never extend the SUT profiler window (or the purity
  // interval) past this p-press. The requested_end stamp is taken inside
  // StopWindow, before the disable result is inspected (a wedged state never
  // persists; the honest interval end is the p-press).
  const absl::Status stopped = fetcher_->StopWindow();
  if (!stopped.ok()) {
    // Post-failed-disable recovery (profile_fetcher.h's contract): the SUT
    // profiler may still be RUNNING — best-effort disable with this job's
    // own client, then surface the reason. No window: a retrieval would race
    // a live profiler and read a partial file.
    AdminClient recovery(profile_config_.admin);
    (void)recovery.EnableCpuProfiler(false);
    folding_.store(false, std::memory_order_relaxed);
    {
      absl::MutexLock lock(&diff_mu_);
      flame_note_ = absl::StrCat("profile close failed: ", stopped.message(),
                                 "; sent a best-effort disable");
    }
    std::function<void()> notification;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      notification = snapshot_notification_;
    }
    if (notification != nullptr) {
      notification();
    }
    return stopped;
  }

  // Capture the job inputs on the steering thread, under the model mutex:
  // the completed-work series as of the stop moment, the anchor, and the run
  // stamps. A run that ENDS while the retrieval runs is handled inside the
  // job (it upgrades the stamps after the fetch — M1 final fixes).
  // shared_ptr (not unique_ptr): Submit's queue is a std::function, which
  // needs a copyable callable.
  auto input = std::make_shared<ProfileJobInput>();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    input->anchor = anchor_;
    input->run_start_unix_ns = run_start_unix_ns_;
    input->run_finished = run_finished_;
    input->run_end_unix_ns = run_end_unix_ns_;
    input->worker_count = worker_count_;
    input->completed_series = MergeCompletedSeries(model_.counters);
    // M2 split alignment inputs: the run's streamed boundaries and the phase
    // specs (empty events keep the M1 producer).
    input->boundary_events = boundary_events_;
    input->phase_specs = {knob_options_.phases().begin(), knob_options_.phases().end()};
    // RunFeed clears its running flag BEFORE on_finished fires (run_feed.cc's
    // teardown order), so "feed idle but stamp not yet recorded" means the
    // terminal event is in flight — wait briefly for the REAL stamp.
    if (!feed_.running() && !input->run_finished) {
      cv_.wait_for(lock, std::chrono::seconds(2), [this]() { return finished_; });
      if (run_finished_) {
        input->run_finished = true;
        input->run_end_unix_ns = run_end_unix_ns_;
      } else {
        // The stamp never landed: the run demonstrably ended BEFORE this
        // p-press (the feed is idle and the SUT profiler kept sampling until
        // the disable above). Take the conservative bound — phase end just
        // BEFORE the window end (the fetcher's requested-end stamp) — so the
        // unverifiable tail is EXCLUDED, never a fabricated pure window.
        input->run_finished = true;
        input->run_end_unix_ns = fetcher_->requested_end_unix_ns() - 1;
      }
    }
  }

  // ONE pool job: retrieve -> symbolize -> load -> fold -> align -> store +
  // session record (plan task 6 pipeline; the admin calls stayed on the
  // steering thread — see above). The job owns copies of everything mutable
  // it touches.
  PprofWorkerPool* pool = &pool_;
  RunController* self = this;
  pool->Submit([self, input]() { self->FinishProfileWindowJob(*input); });
  return absl::OkStatus();
}

void RunController::FinishProfileWindowJob(ProfileJobInput& input) {
  ProfilePipelineConfig config;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    config = profile_config_;
  }
  // The job's own admin client for the metadata-only clock read: AdminClient
  // is not thread-safe, and the scrape cadence may be running on its own
  // client concurrently. (The profiler enable/disable admin calls stay on
  // the steering thread — M1 final fixes.)
  AdminClient job_admin(config.admin);

  // Retrieval only: the profiler was disabled on the steering thread at the
  // p-press (profile_fetcher.h's split contract).
  absl::StatusOr<FetchedProfile> fetched = fetcher_->FetchProfile();
  if (!fetched.ok()) {
    {
      absl::MutexLock lock(&diff_mu_);
      flame_note_ = absl::StrCat("profile fetch failed: ", fetched.status().message());
    }
    folding_.store(false, std::memory_order_relaxed);
    std::function<void()> notification;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      notification = snapshot_notification_;
    }
    if (notification != nullptr) {
      notification();
    }
    return;
  }

  // Straddle belt (M1 final fixes): the run may have ENDED while this
  // retrieval ran; upgrade the stamps with the real terminal record when it
  // has landed. NO fabrication here: without a real stamp the capture-time
  // classification stands (it was taken conservatively on the steering
  // thread). With the deferred close the interval end is the p-press stamp,
  // so a run ending during retrieval does NOT taint purity — the samples
  // stopped at the disable.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (run_finished_) {
      input.run_finished = true;
      input.run_end_unix_ns = run_end_unix_ns_;
    }
  }

  // Clock-offset uncertainty (plan major-fix): the observed mac<->SUT offset
  // beyond the admin Date granularity. Unavailable (no clock surface) -> 0,
  // recorded-absent.
  int64_t uncertainty_ns = 0;
  const absl::StatusOr<int64_t> sut_now = job_admin.FetchSutUnixTimeNs();
  if (sut_now.ok()) {
    const int64_t observed = std::abs(*sut_now - nowUnixNs());
    uncertainty_ns = std::max<int64_t>(0, observed - kDateGranularityNs);
  }

  ProfileJobOutcome outcome = RunProfilePipeline(config, *fetched, input, uncertainty_ns);

  if (!outcome.windows.empty()) {
    // Append to the immutable store; the render side only ever reads it.
    auto windows = window_store_.load();
    std::vector<ProfileWindow> updated = windows != nullptr ? *windows : std::vector<ProfileWindow>();
    for (ProfileWindow& window : outcome.windows) {
      updated.push_back(window);
    }
    window_store_.store(std::move(updated));

    // Session records: the raw staged bytes ride along (self-describing
    // session). Enqueued under the writer mutex; enqueue after close counts
    // as a drop, so a race with finishSession is safe by contract. When the
    // session closed before this job landed, say so (review minor): the
    // window renders, but it is not in the session file.
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (writer_ != nullptr) {
        for (const ProfileWindow& window : outcome.windows) {
          nighthawk::client::SessionRecord record;
          *record.mutable_profile_window() =
              ToProfileWindowRecord(window, outcome.raw_bytes, outcome.symbolizer_tool);
          writer_->enqueue(std::move(record));
        }
      } else {
        absl::StrAppend(&outcome.note, " | record not persisted: session already closed");
      }
    }
  }

  {
    absl::MutexLock lock(&diff_mu_);
    flame_note_ = outcome.note;
  }
  folding_.store(false, std::memory_order_relaxed);
  std::function<void()> notification;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    notification = snapshot_notification_;
  }
  if (notification != nullptr) {
    notification();
  }
}

bool RunController::profileWindowOpen() const {
  return window_open_.load(std::memory_order_relaxed);
}

std::string RunController::profileWindowState() const {
  if (folding_.load(std::memory_order_relaxed)) {
    return "folding...";
  }
  if (window_open_.load(std::memory_order_relaxed)) {
    std::lock_guard<std::mutex> lock(mutex_);
    return absl::StrCat("window open ", profile_config_.window_seconds, "s");
  }
  return "idle";
}

std::shared_ptr<const std::vector<ProfileWindow>> RunController::loadWindows() const {
  return window_store_.load();
}

void RunController::ComputeWindowDiffForLastTwoPureWindows(bool noise_floor) {
  const auto snapshots = window_store_.load();
  const std::string label = noise_floor ? "noise floor" : "differential";
  if (snapshots == nullptr || snapshots->size() < 2) {
    absl::MutexLock lock(&diff_mu_);
    flame_note_ = absl::StrCat(label, " needs two pure windows (pulled ",
                               snapshots == nullptr ? 0 : snapshots->size(), ")");
    return;
  }
  // The default selection: the two most recent pure windows (append-only
  // store: indices stay valid across later appends).
  std::vector<size_t> pure;
  for (size_t i = 0; i < snapshots->size(); ++i) {
    if ((*snapshots)[i].purity_fraction >= 1.0) {
      pure.push_back(i);
    }
  }
  if (pure.size() < 2) {
    absl::MutexLock lock(&diff_mu_);
    flame_note_ = absl::StrCat(label, " needs two pure windows (", pure.size(),
                               " pure of ", snapshots->size(), " pulled)");
    return;
  }
  const size_t base = pure[pure.size() - 2];
  const size_t cur = pure[pure.size() - 1];
  RunController* self = this;
  pool_.Submit([self, snapshots, base, cur, noise_floor]() {
    // Pool-side computation; the overlay TYPE-pairs the result with the
    // snapshot that owns its frame pointers (flamegraph.h's lifetime rule).
    absl::StatusOr<DifferentialResult> result =
        noise_floor ? ComputeNoiseFloor((*snapshots)[base], (*snapshots)[cur])
                    : ComputeDifferential((*snapshots)[base], (*snapshots)[cur]);
    std::string note;
    std::optional<DiffOverlay> overlay;
    if (result.ok()) {
      auto paired = DiffOverlay::Pair(snapshots, *std::move(result));
      if (paired.ok()) {
        overlay = *std::move(paired);
        // Claim discipline (the plan's Global Constraint, minor b): until M3
        // adds knob epochs every differential IS a same-state pair, so the
        // d-view's label says so on-screen — the "effect size" reading the
        // name implies is not claimable at M1.
        note = absl::StrCat(noise_floor ? "noise floor" : "differential", ": windows #", base + 1,
                            " vs #", cur + 1,
                            noise_floor ? "" : " (same-state; no knob epochs until M3)",
                            overlay->result().comparable
                                ? ""
                                : absl::StrCat(" (non-comparable: ",
                                               overlay->result().incomparable_reason, ")"));
      } else {
        note = paired.status().message();
      }
    } else {
      note = result.status().message();
    }
    absl::MutexLock lock(&self->diff_mu_);
    self->diff_overlay_ = std::move(overlay);
    self->flame_note_ = note;
    std::function<void()> notification;
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      notification = self->snapshot_notification_;
    }
    if (notification != nullptr) {
      notification();
    }
  });
}

std::optional<DiffOverlay> RunController::loadDiffOverlay() const {
  absl::MutexLock lock(&diff_mu_);
  return diff_overlay_;
}

std::string RunController::flameNote() const {
  absl::MutexLock lock(&diff_mu_);
  return flame_note_;
}

void RunController::NoteFlameStatus(std::string note) {
  absl::MutexLock lock(&diff_mu_);
  flame_note_ = std::move(note);
}

void RunController::ScrapeLoop() {
  while (scraping_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(kScrapeCadence);
    if (!scraping_.load(std::memory_order_relaxed)) {
      return;
    }
    // Engine-side wall stamp when the clock surface answers (§8.5); the mac
    // wall otherwise (documented fallback — deltas only need forward time,
    // and per-series rendering never cross-compares the timelines).
    absl::StatusOr<int64_t> wall = scrape_admin_.FetchSutUnixTimeNs();
    absl::StatusOr<std::string> prom = scrape_admin_.FetchPrometheusStats();
    if (!prom.ok()) {
      continue; // no SUT admin / unreachable: the charts simply lack sut.*
    }
    const SutScrape scrape{wall.ok() ? *wall : nowUnixNs(), *prom};
    // Histogram-line drop count for the throttled note (the pure layer does
    // not log; this is the wiring's counting half).
    int histogram_drops = 0;
    ParsePrometheusCounters(scrape.prom_text, &histogram_drops);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!has_prev_scrape_) {
        // Baseline-only first scrape (task 7's folded T6 note): strict
        // interval deltas only — no synthetic first-interval points.
        prev_scrape_ = scrape;
        has_prev_scrape_ = true;
        continue;
      }
      absl::StatusOr<SutStatsDelta> delta = DeltaScrapes(prev_scrape_, scrape);
      prev_scrape_ = scrape;
      if (delta.ok()) {
        for (auto& [name, points] : delta->series) {
          auto& series = model_.counters[name];
          for (const CounterPoint& point : points) {
            series.push_back(point);
          }
          while (series.size() > kRunViewModelMaxPoints) {
            series.erase(series.begin());
          }
        }
        model_.sut_counter_reset_detected =
            model_.sut_counter_reset_detected || delta->counter_reset_detected;
        model_.sut_histogram_line_drops += histogram_drops;
        store_.store(model_);
      }
      // Out-of-order scrapes re-baseline silently (never a negative delta).
    }
  }
}

absl::Status RunController::startRun(const nighthawk::client::StartRequest& request) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_ != nullptr) {
      // User-facing through the start-run dialog's rejection line: name the
      // way out honestly — in M0b there is no close-without-exit, q quits
      // the TUI and saves the session.
      return absl::FailedPreconditionError("a session is already open (q quits and saves)");
    }
  }

  // Mark the run un-finished before the read thread can race us: start()
  // returns only once the run is in flight, and a very short run could
  // otherwise deliver on_finished before the flag flips. The reset model is
  // stored too, so the render side drops the previous run's data immediately
  // instead of serving it until this run's first snapshot.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = false;
    completed_ = false;
    has_output_ = false;
    final_output_ = nighthawk::client::Output{};
    knob_options_ = request.options();
    model_ = RunViewModel{};
    // The target-slot invariant's source: the accepted request's uri lands in
    // the model before the run launches, so row 0 never reads "(no target)"
    // once the state leaves idle.
    model_.target_uri = request.options().uri().value();
    // M1 run-state reset + worker count (the PreA1RunInterval producer's
    // inputs; spec §8.5 metadata). NOTE (review round 1): the run stamp and
    // the scrape cadence are NOT armed here — both arm only after feed_.start()
    // succeeds below, so a failed start (unwritable session, dead stream)
    // leaves no phantom run: no window could ever open against it, and no
    // orphan scrape thread leaks idle-time sut.* noise.
    run_start_unix_ns_ = nowUnixNs();
    run_end_unix_ns_ = 0;
    run_finished_ = false;
    anchor_ = ClockAnchor{};
    has_prev_scrape_ = false;
    boundary_events_.clear();
    const std::string& concurrency = request.options().concurrency().value();
    uint32_t parsed = 0;
    worker_count_ =
        (!concurrency.empty() && absl::SimpleAtoi(concurrency, &parsed) && parsed > 0)
            ? parsed
            : 1;
  }
  store_.store(model_);

  // Open the session file pair BEFORE the run starts: once feed_.start()
  // returns, snapshots can arrive on the read thread, and one delivered before
  // a later-opened writer would be lost to the session file. A failure here
  // surfaces without a run ever launching.
  const int64_t unix_ns = nowUnixNs();
  const std::string path = absl::StrCat(session_prefix_, "-", unix_ns, ".nhsession");
  auto writer_or =
      SessionWriter::open(path, ToSessionHeader(request, /*session_id=*/path, unix_ns));
  if (!writer_or.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = true;
    return writer_or.status();
  }
  // Publish the writer before the run launches: from the moment feed_.start()
  // returns, snapshots can arrive on the read thread, and every one of them
  // must find an open session to land in.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_path_ = path;
    writer_ = std::move(*writer_or);
  }

  const absl::Status started = feed_.start(request);
  if (!started.ok()) {
    // No run launched; take the writer back and close the header-only session
    // (completed=false, single close), then surface the start failure.
    std::unique_ptr<SessionWriter> writer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
      writer = std::move(writer_);
      session_path_.clear();
      run_finished_ = true;
      run_end_unix_ns_ = nowUnixNs();
    }
    nighthawk::client::RunEndRecord run_end;
    run_end.set_completed(false);
    run_end.set_ended_unix_ns(nowUnixNs());
    // A close error on this header-only session is not surfaced (the start
    // failure is the actionable status); the close still happened exactly
    // once.
    (void)writer->close(run_end);
    return started;
  }

  // The run is demonstrably in flight: NOW arm the M1 live-run state (review
  // round 1 — both were gated on this point so a failed start leaves no
  // phantom run and no orphan scrape thread).
  //  - the run stamp (window pulls align against it),
  //  - the SUT stats scrape cadence (plan task 7 wiring; runs while the run
  //    is live),
  //  - run_state "running" (the live-path marker the status bar renders;
  //    a superfast run whose on_finished already landed keeps its final
  //    state instead).
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!finished_) {
      model_.run_state = "running";
      store_.store(model_);
    }
    has_run_stamp_ = true;
    // The scrape cadence arms only while the run is demonstrably live (the
    // !finished_ guard is the T8-parked one-liner, legal in this wave: a run
    // that finished between start() and this block — possible in principle —
    // must not start a scraper).
    if (profiling_configured_ && !finished_ && !scraping_.exchange(true)) {
      RunController* self = this;
      scraper_ = std::thread([self]() { self->ScrapeLoop(); });
    }
  }
  return absl::OkStatus();
}

void RunController::stopRun() { feed_.stop(); }

bool RunController::runInProgress() const { return feed_.running(); }

bool RunController::lastRunEndedCompleted() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return completed_;
}

std::shared_ptr<const RunViewModel> RunController::model() const { return store_.load(); }

std::vector<nighthawk::client::PhaseBoundaryEvent> RunController::boundaryEvents() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return boundary_events_;
}

std::vector<nighthawk::client::PhaseMetadata> RunController::finalPhaseMetadata() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {final_output_.phase_metadata().begin(), final_output_.phase_metadata().end()};
}

std::string RunController::sessionPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return session_path_;
}

const nighthawk::client::CommandLineOptions& RunController::knobOptions() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return knob_options_;
}

absl::Status RunController::finishSession(bool completed) {
  // Stop-if-running, then wait for on_finished: close() is single-call and
  // strictly after the run feed's terminal event, per the session writer's
  // ordering contract.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (feed_.running()) {
      feed_.stop();
    }
  }
  bool finished = false;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    finished = cv_.wait_for(lock, kFinishTimeout, [this]() { return finished_; });
  }
  if (!finished) {
    return absl::InternalError(
        "run feed did not deliver on_finished within the shutdown budget; the open session "
        "drains without a run end record");
  }

  // Single-call close: moving the writer out makes a second close impossible.
  std::unique_ptr<SessionWriter> writer;
  nighthawk::client::Output output;
  bool has_output = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    writer = std::move(writer_);
    session_path_.clear();
    output = final_output_;
    has_output = has_output_;
  }
  if (writer == nullptr) {
    return absl::OkStatus();
  }
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(completed);
  if (completed && has_output) {
    *run_end.mutable_output() = output;
  }
  run_end.set_ended_unix_ns(nowUnixNs());
  return writer->close(run_end);
}

absl::Status RunController::handleLoopExit() {
  // Drop the feed-thread notification before anything can wait: no stale
  // postEvent may fire into a screen whose loop is already over while the
  // shutdown below blocks on the feed's terminal event.
  set_snapshot_notification(nullptr);
  // The same flush/close sequence the q path runs: stop-if-running, wait for
  // on_finished, close the writer with a run end record. After a q exit this
  // is a no-op (the session is already closed; finishSession returns OkStatus
  // on a null writer).
  return finishSession(lastRunEndedCompleted());
}

void RunController::onSnapshot(const nighthawk::client::LiveSnapshot& snapshot) {
  // The snapshot path (spec §4): fold into the model, swap the immutable
  // store copy, persist the record, then ping the shell for exactly one
  // gated redraw. The model mutation is under the mutex now that the M1
  // scrape thread also folds into the model; the notification fires outside
  // the mutex so a callback that re-enters the controller cannot deadlock.
  std::function<void()> notification;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // The clock anchor: the FIRST live snapshot's (wall, monotonic) pair —
    // the §8.5 join key the profile pipeline maps window intervals through.
    if (!anchor_.valid()) {
      anchor_.wall_time_unix_ns = snapshot.wall_time_unix_ns();
      anchor_.monotonic_ns = snapshot.monotonic_ns();
    }
    model_ = ApplySnapshot(std::move(model_), snapshot);
    store_.store(model_);
    if (writer_ != nullptr) {
      nighthawk::client::SessionRecord record;
      *record.mutable_metric_series() = ToMetricSeries(snapshot);
      writer_->enqueue(std::move(record));
    }
    notification = snapshot_notification_;
  }
  if (notification != nullptr) {
    notification();
  }
}

void RunController::onPhaseEvent(const nighthawk::client::PhaseBoundaryEvent& event) {
  // Stream-at-boundary intake (M2 Task 7 wiring, DQ-3: METADATA only): the
  // boundary event joins the timeline state and persists as the session's
  // PhaseBoundaryRecord exactly when the engine emitted it. The pool/claim
  // fields stay default until the final Output's PhaseMetadata completes
  // them (the segment builder's post-run completeness source).
  std::function<void()> notification;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    boundary_events_.push_back(event);
    if (writer_ != nullptr) {
      nighthawk::client::SessionRecord record;
      *record.mutable_phase_boundary() = ToPhaseBoundaryRecord(event);
      writer_->enqueue(std::move(record));
    }
    notification = snapshot_notification_;
  }
  if (notification != nullptr) {
    notification();
  }
}

void RunController::onCompleted(const nighthawk::client::Output& output) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    final_output_ = output;
    completed_ = true;
    has_output_ = true;
    model_.run_state = "completed";
    store_.store(model_);
  }
  // A natural run end only; a cancelled run never sees this callback.
}

void RunController::onFinished(const absl::Status& /*status*/) {
  // The run-end stamp (mac wall, §8.5 metadata) — the PreA1RunInterval end
  // for windows fetched after the run. The scrape cadence stops with the
  // run: deltas of a stopped run would be idle-time noise.
  scraping_.store(false, std::memory_order_relaxed);
  if (scraper_.joinable() && scraper_.get_id() != std::this_thread::get_id()) {
    scraper_.join();
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // completed_ (set by onCompleted, same thread) — lastRunEndedCompleted()
    // would deadlock here: it locks the mutex this block already holds.
    model_.run_state = completed_ ? "completed" : "stopped";
    store_.store(model_);
    run_finished_ = true;
    run_end_unix_ns_ = nowUnixNs();
    finished_ = true;
  }
  cv_.notify_all();
}

namespace {

// The interactive shell: RenderLayout composition, the navigate-mode key map
// (c charts, f flame, k knobs, n start-run dialog, s stop, q quit) and the
// dialog editor. The lifecycle logic lives in RunController; this layer only
// owns the terminal. FTXUI's ScreenInteractive loop is not headless-testable
// — this binding is exercised manually (Task 8).
int runInteractive(const std::string& address, const std::string& session_prefix,
                   const std::optional<ProfilePipelineConfig>& profiling) {
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  RunController controller(channel, session_prefix);
  if (profiling.has_value()) {
    controller.ConfigureProfiling(*profiling);
  }

  ShellState state;
  Component uri_input = Input(&state.uri_field, "target uri");
  Component rps_input = Input(&state.rps_field, "requests per second");
  Component duration_input = Input(&state.duration_field, "duration in seconds");

  // Fullscreen (the alternate-screen buffer) keeps the live TUI from messing
  // up the terminal's scrollback.
  ftxui::App screen = ftxui::App::Fullscreen();

  // Exactly one redraw per stored snapshot, gated by the input mode (spec §4):
  // while an input mode owns the keyboard the panes catch up only on the one
  // forced redraw the dialog close posts. Pool-job completions (window
  // folded, differential computed) post through the same gate.
  controller.set_snapshot_notification([&screen, &state]() {
    if (!RefreshSuspended(state.mode_mirror.load(std::memory_order_relaxed))) {
      screen.PostEvent(Event::Custom);
    }
  });

  Component editor = ftxui::Container::Vertical({uri_input, rps_input, duration_input});
  Component view = Renderer(editor, [&]() {
    static const RunViewModel kIdleModel;
    std::shared_ptr<const RunViewModel> model = controller.model();
    const RunViewModel& view_model = model == nullptr ? kIdleModel : *model;
    // The overlay state the pure layout renders from: draft fields, the
    // active field (the vertical container's focus), the rejected-submit
    // flag, and the help toggle. The shared mapping carries the shell state;
    // the focus-derived active field and the live Input components are the
    // shell-only extras.
    OverlayState overlay;
    FillOverlayState(state, &overlay);
    overlay.active_field = uri_input->Focused() ? StartRunDialogModel::kUri
                          : rps_input->Focused() ? StartRunDialogModel::kRps
                                                 : StartRunDialogModel::kDuration;
    overlay.uri_input = uri_input;
    overlay.rps_input = rps_input;
    overlay.duration_input = duration_input;
    // The flame pane (tab 1) composes from the controller's immutable stores
    // + the shell's view state — the same BuildFlamePane the tests use. The
    // window snapshot is held in a LOCAL (fix, review round 1): tree points
    // INTO the snapshot's vector, so the shared_ptr must outlive the render
    // — a temporary would free it mid-frame if a pool job swaps the store
    // between this expression and RenderLayout's read. The overlay (and its
    // owning snapshot, held inside the DiffOverlay) stays alive through the
    // render the same way.
    const std::shared_ptr<const std::vector<ProfileWindow>> windows = controller.loadWindows();
    const std::optional<DiffOverlay> overlay_optional =
        state.flame_view.diff_mode ? controller.loadDiffOverlay() : std::nullopt;
    FlamePaneState flame = BuildFlamePane(windows, state.flame_view,
                                          controller.flameNote(),
                                          overlay_optional.has_value() ? &*overlay_optional
                                                                       : nullptr);
    // The phase timeline (M2): post-run metadata when the Output landed,
    // else the streamed boundary events; empty for legacy runs (the bar
    // keeps its placeholder row). Built fresh per frame from the
    // controller's immutable snapshots.
    const nighthawk::client::CommandLineOptions knob_options = controller.knobOptions();
    const std::vector<PhaseTimelineSegment> timeline =
        BuildPhaseTimeline(knob_options, controller.boundaryEvents(),
                           controller.finalPhaseMetadata(),
                           view_model.last_snapshot.phase_id());
    // The staged next-phase preview (M2): nothing staged renders nothing.
    const std::vector<KnobDisplay> staged_preview =
        state.staged_next_phase.name().empty()
            ? std::vector<KnobDisplay>{}
            : NextPhasePreviewKnobs(state.staged_next_phase, knob_options);
    std::string staged_annotation;
    if (!staged_preview.empty()) {
      int parsed = 0;
      if (state.staged_next_phase.has_concurrency() &&
          absl::SimpleAtoi(state.staged_next_phase.concurrency().value(), &parsed) &&
          parsed > 0) {
        staged_annotation = WorkerSetAnnotation(CurrentRunWorkerCount(knob_options),
                                                static_cast<uint32_t>(parsed),
                                                static_cast<uint32_t>(knob_options.phases_size()));
      }
    }
    return RenderLayout(screen.dimx(), screen.dimy(), state.active_tab, state.mode, view_model,
                        state.dialog, controller.sessionPath(), knob_options, &overlay, &flame,
                        controller.profileWindowState(), &timeline,
                        staged_preview.empty() ? nullptr : &staged_preview,
                        staged_preview.empty() ? nullptr : &staged_annotation);
  });

  Component app = CatchEvent(view, [&](Event event) {
    return HandleShellEvent(event, state, controller,
                            [&screen]() { screen.PostEvent(Event::Custom); },
                            [&screen]() { screen.Exit(); });
  });

  // FTXUI's default signal machinery handles Ctrl-C itself (it records SIGINT
  // and re-raises it after PostMain, terminating the process before the loop
  // even returns — the T7 finding: a 0-byte session on Ctrl-C). Handing
  // Ctrl-C to this shell instead lets HandleShellEvent route it through the
  // same flush-then-exit sequence as q, inside the loop where the flush is
  // reachable.
  screen.ForceHandleCtrlC(false);
  screen.Loop(app);
  // The loop is over (q, Ctrl-C, or EOF). The q and Ctrl-C paths already
  // flushed inside the loop; an EOF/signal exit never reaches that handler,
  // so route the teardown through the same flush/close sequence the q path
  // uses — without this, an open session drains in the destructor with no
  // run end record, silently losing it (finding F2: a 0-byte session on
  // Ctrl-C).
  (void)controller.handleLoopExit();
  return 0;
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk

namespace Nighthawk {
namespace Client {

int runTui(int argc, const char* const* argv, std::ostream& err) {
  TCLAP::CmdLine cmd("nighthawk_tui: terminal UI for Nighthawk benchmark runs", ' ', "1.0");
  TCLAP::ValueArg<std::string> address(
      "", "address",
      "Address of the Nighthawk service gRPC endpoint, for example 127.0.0.1:8443. Default: "
      "127.0.0.1:8443.",
      false, "127.0.0.1:8443", "string", cmd);
  TCLAP::ValueArg<std::string> session_prefix(
      "", "session-prefix", "Prefix for Nighthawk session naming. Default: nighthawk-session.",
      false, "nighthawk-session", "string", cmd);
  TCLAP::SwitchArg selftest(
      "", "selftest",
      "Render one static frame into an off-screen screen, print \"tui ok\" and exit.", cmd);
  // M1 profiling flags (plan task 6). Profiling is configured when a profile
  // mount (volume retriever) or scp host (rig variant) is given; without one
  // the p/d/N keys refuse with an honest message.
  TCLAP::ValueArg<std::string> admin_base(
      "", "admin-base", "SUT admin endpoint base URL. Default: http://127.0.0.1:9901.", false,
      "http://127.0.0.1:9901", "string", cmd);
  TCLAP::ValueArg<std::string> profile_retriever(
      "", "profile-retriever", "Profile retriever kind: mount or scp. Default: mount.", false,
      "mount", "string", cmd);
  TCLAP::ValueArg<std::string> profile_mount(
      "", "profile-mount",
      "Local path of the SUT profile bind mount (the mount retriever reads there). Empty = "
      "profiling unconfigured.",
      false, "", "string", cmd);
  TCLAP::ValueArg<std::string> scp_host(
      "", "scp-host", "scp destination (user@host) for the rig retriever.", false, "", "string",
      cmd);
  TCLAP::ValueArg<std::string> sut_profile_path(
      "", "sut-profile-path",
      "SUT-side path the CPU profiler writes (admin.profile_path). Default: "
      "/var/local/profiling/envoy.prof.",
      false, "/var/local/profiling/envoy.prof", "string", cmd);
  TCLAP::ValueArg<std::string> profile_staging(
      "", "profile-staging", "Local staging directory for fetched profiles. Default: "
      "/tmp/nh-profiles.",
      false, "/tmp/nh-profiles", "string", cmd);
  TCLAP::ValueArg<std::string> pprof_tool(
      "", "pprof-tool",
      "Symbolizer argv prefix, space separated (SUBPROCESS ONLY - never a linked dependency). "
      "Default: \"go tool pprof\".",
      false, "go tool pprof", "string", cmd);
  TCLAP::ValueArg<std::string> sut_binary(
      "", "sut-binary",
      "SUT binary path inside the PPROF_BINARY_PATH layout (binary reachability for "
      "symbolization). Empty = no symbolization attempted.",
      false, "", "string", cmd);
  TCLAP::ValueArg<int> profile_window_seconds(
      "", "profile-window-seconds", "Profile window length for the p flow. Default: 5.", false, 5,
      "int", cmd);
  cmd.setExceptionHandling(false);
  try {
    cmd.parse(argc, argv);
  } catch (const TCLAP::ArgException& e) {
    // Same translation as Utility::parseCommand, minus the MalformedArgvException
    // dependency: write the message and fail.
    err << e.what() << "\n";
    return 1;
  } catch (const TCLAP::ExitException&) {
    // --help or --version: the output has already been written by TCLAP.
    return 0;
  }

  if (selftest.getValue()) {
    // One static frame of the real layout (status bar over the charts tab with
    // an idle model): proves the FTXUI dependency and the render stack without
    // needing a TTY, so CI and scripts can smoke the binary.
    const Tui::RunViewModel idle_model;
    const Tui::StartRunDialogModel dialog;
    ftxui::Element frame =
        Tui::RenderLayout(/*width=*/100, /*height=*/30, /*active_tab=*/0, Tui::InputMode::kNavigate,
                          idle_model, dialog, "selftest");
    ftxui::Screen screen(100, 30);
    ftxui::Render(screen, frame);
    err << "tui ok\n";
    return 0;
  }

  std::optional<Tui::ProfilePipelineConfig> profiling;
  if (!profile_mount.getValue().empty() || !scp_host.getValue().empty()) {
    Tui::ProfilePipelineConfig config;
    config.admin.admin_base = admin_base.getValue();
    config.retriever_kind =
        (profile_retriever.getValue() == "scp" || !scp_host.getValue().empty())
            ? Tui::ProfilePipelineConfig::RetrieverKind::kScp
            : Tui::ProfilePipelineConfig::RetrieverKind::kVolumeMount;
    config.mount_root = profile_mount.getValue();
    config.scp_host = scp_host.getValue();
    config.sut_profile_path = sut_profile_path.getValue();
    config.staging_dir = profile_staging.getValue();
    // The argv prefix splits on plain spaces ("go tool pprof").
    config.pprof_tool_argv = absl::StrSplit(pprof_tool.getValue(), ' ', absl::SkipEmpty());
    config.sut_binary_path = sut_binary.getValue();
    config.window_seconds = profile_window_seconds.getValue();
    const char* binary_layout = std::getenv("PPROF_BINARY_PATH");
    config.pprof_binary_path = binary_layout != nullptr ? binary_layout : "";
    profiling = std::move(config);
  }

  return Tui::runInteractive(address.getValue(), session_prefix.getValue(), profiling);
}

} // namespace Client
} // namespace Nighthawk
