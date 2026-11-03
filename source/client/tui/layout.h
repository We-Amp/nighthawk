#pragma once

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "api/client/options.pb.h"

#include "source/client/tui/input_mode.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/start_run_dialog.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the TUI shell's layout contract (spec §4):
//   - a persistent 3-row status bar always on screen (rows 0..2),
//   - the active tab's pane owns the full screen below it,
//   - a declared minimum terminal size below which the TUI renders one clear
//     message instead of wrapping garbage.
// All renderers are pure Snapshot/Model → Element functions so the
// Screen-based tests stay deterministic.

// Spec §4 declared minimum. Below either bound, RenderLayout renders only the
// min-size message.
inline constexpr int kMinTerminalWidth = 100;
inline constexpr int kMinTerminalHeight = 30;

// The full frame for one redraw: min-size gate, then status bar over the
// active tab's pane. `width`/`height` are the terminal dimensions — Task 6
// feeds them from the terminal size each frame. Tabs: 0 = charts, 1 = knobs
// (rendered from `knob_options`, the most recent run's options; unset defaults
// before any run). The dialog's uri feeds the status bar's target field.
ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options =
                                nighthawk::client::CommandLineOptions{});

// The charts tab (tab 0): percentile band on top, one sparkline per counter
// series below (sorted by name for deterministic frames). `width`/`height`
// are the pane-local dimensions (terminal size minus the status bar).
ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height);

// The min-size screen: one message naming the declared minimum and the actual
// size, and nothing else from the normal layout.
ftxui::Element RenderMinSizeMessage(int width, int height);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
