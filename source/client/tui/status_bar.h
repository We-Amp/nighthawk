#pragma once

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "api/client/live.pb.h"
#include "api/client/output.pb.h"
#include "api/client/options.pb.h"
#include "source/client/tui/input_mode.h"
#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the persistent status bar — exactly three terminal rows
// that are always on screen under the layout contract (spec §4), regardless
// of which full-screen tab is active. Pure model → Element; the Screen-based
// tests stay deterministic.

// The stats flush cadence the RunFeed requests by default (1s, within the
// proto's [1s, 300s] bounds); the status bar advertises it as the rate at
// which the panes move.
inline constexpr int kStatusBarFlushIntervalSeconds = 1;

// Rows (top to bottom):
//   0: "[editing] " prefix while a dialog owns the keyboard, run state,
//      elapsed ("39s/60s | remaining Ns" against the run's duration,
//      "elapsed Ns (open-ended)" when unset), and the target uri. Target
//      invariant: once run_state leaves "idle" the row carries the accepted
//      target — model.target_uri, then the dialog draft, then "(target
//      pending)" for the sub-second pre-ack window; "(no target)" is
//      reserved for idle.
//   1: phase + "pre-phase-engine placeholder" marker, epoch + "[placeholder]"
//      marker (empty renders "(none)"), flush cadence
//   2: session path + input mode; the mode cell renders inverse-video while
//      an input mode owns the keyboard, so the routing state also survives a
//      monochrome paste next to row 0's "[editing] " prefix.
//
// `uri` is the dialog draft (the only pre-M3 source of a target uri);
// `duration_seconds` is the run's configured duration (0 = unset).
// `profiler_state` is the M1 flamegraph pipeline's state cell ("idle" |
// "window open <n>s" | "folding..."; empty renders "idle"). The M1 SUT-stats
// cells (reset-or-label-set-change marker, histogram-drop count) render from
// the model's scrape state. Pure function, no terminal I/O.
struct PhaseTimelineSegment;
ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri = "",
                               int duration_seconds = 0,
                               absl::string_view profiler_state = "",
                               const std::vector<PhaseTimelineSegment>* timeline = nullptr);

// ---------------------------------------------------------------------------
// M2: the phase timeline (spec §4 layout contract). One entry per phase
// span, in run order; the entry also carries the boundary that ENDED the
// phase — its drain/idle gap rendered as a visible gap segment, the pool
// transition into the successor, and the §8.4 non-comparable + cold-start
// marks. The run's final phase carries no boundary at all (it ends at run
// teardown, not at a boundary drain).
//
// NORMATIVE (the DQ-3 ruling, mirrored on PhaseBoundaryRecord in
// api/client/session.proto): the timeline joins on the engine's boundary
// records — the discrete (wall, monotonic) events — and on nothing else.
// Boundary data is METADATA, never a synthetic metric snapshot; the idle gap
// sits INSIDE the drain-complete flag for purity accounting and is rendered
// as a labeled gap, never subtracted from or added to any percentile series.
// ---------------------------------------------------------------------------

// One phase span of the timeline.
struct PhaseTimelineSegment {
  std::string phase_id;
  // The phase's duration in seconds (from its PhaseSpec).
  double duration_seconds = 0.0;
  bool is_current = false;
  // The boundary that ended this phase (absent for the final phase):
  double drain_seconds = 0.0;
  double idle_gap_seconds = 0.0;
  // §8.4: the worker set changed at this boundary — the differential pair
  // across it is marked non-comparable.
  bool worker_count_changed = false;
  // Task 6 metadata: workers idle in the prior phase cold-started here.
  bool new_workers_cold_started = false;
  // §8.6 pool-health surfacing at the boundary: the cumulative created
  // DELTA across the boundary (+N; the flat +0 form IS the warm-transfer
  // claim) and the active count. has_pool gates the whole line.
  bool has_pool = false;
  int64_t pool_created_delta = 0;
  uint64_t pool_active = 0;
  // Claim-discipline pair shown next to the current phase (spec 8.2).
  bool has_completion = false;
  double completion_ratio = 0.0;
  bool has_gate = false;
  uint64_t gate_exceeded = 0;
};

// Builds the timeline segments from the run's phase specs (durations) and
// the merged per-phase metadata (boundary timings, worker-set identity, pool
// health, claim counts). `current_phase_id` highlights the phase being run.
// Pool aggregates are the per-worker sums of each phase's pool_health; the
// boundary's created delta is the transition delta into the successor phase:
// successor-vs-ending cumulative sums, full stop (positive for growth, zero
// for the flat warm transfer, negative when a shrinking set takes its
// cumulative counts out of the successor's sum). Pure.
std::vector<PhaseTimelineSegment> PhaseTimelineSegments(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id);

// The LIVE builder: segments from the streamed boundary events (M2 Task 7
// wiring), one segment per spec. Boundary fields (drain/idle/cold) come from
// each event; the §8.4 cmp! mark compares the specs' declared worker counts
// around the boundary (unparseable/absent concurrency = unknown = no mark).
// The pool and claim cells stay unset — the engine computes them in the
// output pass, and the strip renders only what it knows (DQ-3: never a
// fabricated cell). Pure.
std::vector<PhaseTimelineSegment> PhaseTimelineSegmentsFromEvents(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    absl::string_view current_phase_id);

// The render-side source selection: post-run metadata (complete cells) when
// the Output has arrived, else the streamed boundary events. Empty input
// (no phases configured) yields no segments and the bar renders the legacy
// placeholder row. Pure.
std::vector<PhaseTimelineSegment> BuildPhaseTimeline(
    const nighthawk::client::CommandLineOptions& options,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id);

// The compact one-row strip, ASCII only:
//   [warm 2s | drain 0.5s gap 0.2s pool: created=+0 active=8 | *spike 2s*
//   ratio=0.98 gate=7]
// Phase segments render "<id> <D>s"; the current phase is wrapped in "*";
// the boundary between consecutive phases renders its drain and idle gap as
// a visible gap segment with the pool line in the plan's
// `pool: created=+N active=M` vocabulary (negative deltas render -N); a
// worker-count change adds the "cmp!" non-comparable mark, a cold start
// adds "cold". Empty input renders "" (nothing staged to show).
// Exact-render pinned by tests, like the charts.
std::string PhaseTimelineStrip(const std::vector<PhaseTimelineSegment>& segments);

// The timeline as an ftxui element: the strip on one row, clipped to
// `width` columns (the persistent bar's truncation contract). A
// non-positive width renders an empty pane, matching RenderKnobPane.
ftxui::Element RenderPhaseTimeline(const std::vector<PhaseTimelineSegment>& segments,
                                   int width);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
