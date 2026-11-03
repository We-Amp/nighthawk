#pragma once

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the explicit input mode that keeps text editing from
// fighting the live redraw loop (spec §4 threading model). While a dialog
// owns the keyboard, pane refresh is suspended; navigation mode refreshes on
// the flush cadence.
enum class InputMode {
  kNavigate,
  kEditingStartRun,
};

// True while an input mode owns the keyboard and the pane refresh cadence is
// suspended. Exiting an editing mode must post EXACTLY ONE forced redraw so
// the panes catch up on the state edited under the suspension — never a
// refresh storm, never a stale first frame. The posting side is wired in
// Task 6's dialog close; Task 7 exercises it by hand.
inline bool RefreshSuspended(InputMode mode) { return mode == InputMode::kEditingStartRun; }

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
