#pragma once

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

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
// `duration_seconds` is the run's configured duration (0 = unset). Pure
// function, no terminal I/O.
ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri = "",
                               int duration_seconds = 0);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
