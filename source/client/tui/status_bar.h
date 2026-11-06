#pragma once

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "api/client/live.pb.h"
#include "api/client/output.pb.h"
#include "api/client/options.pb.h"
#include "api/client/session.pb.h"
#include "source/client/tui/input_mode.h"
#include "source/client/headless/run_view_model.h"
// The phase timeline segment builders (MCP plan Task 0 extraction) — pure
// model -> model, moved out of this FTXUI-coupled header for the mcp_lib
// quarantine. Same namespace; every call site below (and in the TUI) is
// unchanged.
#include "source/client/headless/timeline_segments.h"

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
ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri = "",
                               int duration_seconds = 0,
                               absl::string_view profiler_state = "",
                               const std::vector<PhaseTimelineSegment>* timeline = nullptr,
                               int bar_width = 0);


// The compact one-row strip, ASCII only:
//   [warm 2s | drain 0.5s gap 0.2s pool: created=+0 active=8 | *spike 2s*
//   ratio=0.98 gate=7]
// Phase segments render "<id> <D>s"; the current phase is wrapped in "*";
// the boundary between consecutive phases renders its drain and idle gap as
// a visible gap segment with the pool line in the plan's
// `pool: created=+N active=M` vocabulary (negative deltas render -N); a
// worker-count change adds the "cmp!" non-comparable mark, a cold start
// adds "cold", a censored drain keeps its "cens" mark (live AND post-run:
// the metadata path carries drain_censored since the fork's output.proto
// field), and each applied knob epoch in the phase adds " +<epoch_id>".
// Empty input renders "" (nothing staged to show).
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
