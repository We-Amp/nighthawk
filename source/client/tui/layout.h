#pragma once

#include <string>

#include "absl/strings/string_view.h"
#include "ftxui/component/component.hpp"
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
//   - the active tab's pane owns the screen below it,
//   - a persistent mode-aware key bar pinned to the bottom row (row 29 at the
//     declared minimum),
//   - a declared minimum terminal size below which the TUI renders one clear
//     message instead of wrapping garbage,
//   - while the start-run dialog owns the keyboard, a bordered dialog overlay
//     (~rows 8-20, cols 25-75) with labeled fields, a reverse-video caret on
//     the active field, inline errors on the offending field's own row, and
//     the error summary on the border; `?` opens a help overlay over the same
//     mechanism.
// All renderers are pure Snapshot/Model → Element functions so the
// Screen-based tests stay deterministic.

// Spec §4 declared minimum. Below either bound, RenderLayout renders only the
// min-size message.
inline constexpr int kMinTerminalWidth = 100;
inline constexpr int kMinTerminalHeight = 30;

// The dialog overlay's placement: a bordered box at (row, col) sized
// kDialogWidth x kDialogHeight, sitting over the charts pane.
inline constexpr int kDialogOverlayRow = 8;
inline constexpr int kDialogOverlayCol = 25;
inline constexpr int kDialogOverlayWidth = 50;
// The help overlay's placement.
inline constexpr int kHelpOverlayRow = 3;
inline constexpr int kHelpOverlayCol = 10;

// The shell's interactive overlay state, passed to RenderLayout by the app
// loop; tests and the corpus capture pass plain strings. The draft field
// contents render inside the dialog overlay (the target slot of the status
// bar stays the accepted run's target).
struct OverlayState {
  // What the user has typed per dialog field.
  std::string uri_field;
  std::string rps_field;
  std::string duration_field;
  // 0=uri, 1=rps, 2=duration (StartRunDialogModel::DialogField numbering);
  // the active field's value renders with a reverse-video caret.
  int active_field = 0;
  // True after a rejected enter whose failure was field validation: the
  // inline errors, the border error summary, and the box staying open are
  // all keyed on this.
  bool show_errors = false;
  // A rejected enter whose fields validated but whose start the controller
  // refused (e.g. a session is already open): a distinct line inside the
  // dialog, never the error-count border title. Empty when nothing was
  // rejected.
  std::string start_rejected;
  // The `?` help overlay (navigate mode).
  bool help_open = false;
  // The live Input components (interactive shell only). When set, their
  // Render() replaces the plain string field so typing works in place.
  ftxui::Component uri_input;
  ftxui::Component rps_input;
  ftxui::Component duration_input;
};

// The full frame for one redraw: min-size gate, then status bar over the
// active tab's pane and the key bar, plus the dialog/help overlay while one
// is open. `width`/`height` are the terminal dimensions — Task 6 feeds them
// from the terminal size each frame. Tabs: 0 = charts, 1 = knobs (rendered
// from `knob_options`, the most recent run's options; unset defaults before
// any run). `overlay` may be null (the plain pane then renders).
ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options =
                                nighthawk::client::CommandLineOptions{},
                            const OverlayState* overlay = nullptr);

// The charts tab (tab 0): percentile band on top, one sparkline per counter
// series below (sorted by name for deterministic frames). `width`/`height`
// are the pane-local dimensions (terminal size minus the status bar and key
// bar). The empty state is state-aware: idle (no run yet) renders "no run
// yet - press n to start one"; a running run before its first flush renders
// "waiting for first flush".
ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height);

// The persistent bottom-row key bar, rendered from the shell's real key map:
// navigate names n/c/k/q/? (plus s while a run is live, and q's stop-then-
// quit), editing names the dialog's tab/enter/esc. `session_open` marks the
// state where `n` can never succeed (a session is open until it is closed
// with q): the n hint is then annotated as unavailable instead of
// advertising a key that would silently refuse.
ftxui::Element RenderKeyBar(InputMode mode, bool running, bool session_open);

// The `?` help overlay: every key of the real key map and every status-bar
// field with a one-line definition, so no on-screen token stays undefined.
ftxui::Element RenderHelpOverlay();

// The start-run dialog overlay: a bordered box (kDialogOverlay* geometry)
// with one labeled field per row (uri / rps / duration (s)), a reverse-video
// caret on the active field, and — after a rejected enter (`show_errors`) —
// inline errors on the offending field's own row plus the summary on the
// border ("start run - 2 errors"). A rejected enter never closes the box.
ftxui::Element RenderStartRunDialog(const StartRunDialogModel& dialog,
                                    const OverlayState& overlay);

// The min-size screen: one message naming the declared minimum and the actual
// size, and nothing else from the normal layout.
ftxui::Element RenderMinSizeMessage(int width, int height);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
