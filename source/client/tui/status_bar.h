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
//   0: run state | elapsed | target uri (the start-run dialog's field; empty
//      renders "(no target)")
//   1: phase + "pre-phase-engine placeholder" marker, epoch + "[placeholder]"
//      marker (empty renders "(none)"), flush cadence
//   2: session path + input mode
//
// `uri` is passed in by the layout (the dialog is the only pre-M3 source of a
// target uri; the RunViewModel carries none). Pure function, no terminal I/O.
ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri = "");

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
