#pragma once

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/component/component.hpp"
#include "ftxui/dom/elements.hpp"

#include "api/client/options.pb.h"

#include "source/client/tui/flamegraph.h"
#include "source/client/tui/input_mode.h"
#include "source/client/tui/knob_pane.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the TUI shell's layout contract (spec §4):
//   - a persistent 3-row status bar always on screen (rows 0..2),
//   - a one-row tab strip under the status bar (row 3): `| charts | flame |
//     knobs |`, the active tab inverted (backlog item 11; as of M1 the flame
//     cell is a live pane, no longer the dimmed placeholder),
//   - the active tab's pane owns the screen below the strip,
//   - a persistent mode-aware key bar pinned to the bottom row (row 29 at the
//     declared minimum),
//   - a declared minimum terminal size below which the TUI renders one clear
//     message instead of wrapping garbage,
//   - while the start-run dialog owns the keyboard, a bordered dialog overlay
//     (~rows 8-20, cols 25-75) with labeled fields (uri / rps /
//     duration (s) / phases), a reverse-video caret on the active field,
//     inline errors on the offending field's own row, and the error summary
//     on the border; `?` opens a help overlay over the same mechanism.
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
  // The M2 phase-vector entry's draft (comma-separated
  // name:rps:duration[:concurrency[:connections]] entries).
  std::string phases_field;
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
  ftxui::Component phases_input;
};

// The M1 flame pane's render input (composed by TuiApp's BuildFlamePane): the
// current window's tree, the shell's live view state, the paired differential,
// and the status/note lines. RenderLayout renders it for tab 1; a null `flame`
// (or a null tree) renders the honest empty state instead.
struct FlamePaneState {
  // The most recent window's tree (uncollapsed, unzoomed; the renderer
  // applies zoom + collapse from the view state). Null when no window exists.
  const FoldedTree* tree = nullptr;
  // One-line provenance: window number, type, purity, work units,
  // symbolization, interval source.
  std::string window_header;
  // The controller's latest note (pipeline outcomes, refusals).
  std::string note;
  // The shell's live view state (zoom/selection/search/diff toggles).
  FlamegraphViewState view;
  // The paired differential overlay's result when diff_mode is on and an
  // overlay has landed; null otherwise.
  const DifferentialResult* diff = nullptr;
};

// The full frame for one redraw: min-size gate, then status bar over the
// tab strip, the active tab's pane, and the key bar, plus the dialog/help
// overlay while one is open. `width`/`height` are the terminal dimensions —
// Task 6 feeds them from the terminal size each frame. Tabs (the tab strip's
// order): 0 = charts, 1 = flame (M1's live pane, rendered from `flame`),
// 2 = knobs (rendered from `knob_options`, the most recent run's options;
// unset defaults before any run). `overlay` may be null (the plain pane
// renders). `flame` may be null (tab 1 then renders the empty state).
// `profiler_state` feeds the status bar's profiler cell (empty = "idle").
ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options =
                                nighthawk::client::CommandLineOptions{},
                            const OverlayState* overlay = nullptr,
                            const FlamePaneState* flame = nullptr,
                            absl::string_view profiler_state = "",
                            const std::vector<PhaseTimelineSegment>* timeline = nullptr,
                            const std::vector<KnobDisplay>* staged_preview = nullptr,
                            const std::string* staged_workers_annotation = nullptr,
                            const std::vector<uint64_t>* boundary_stamps = nullptr,
                            const LiveKnobTurn* rps_turn = nullptr);

// The charts tab (tab 0): on a completed run a completion-summary block
// first (backlog item 7: totals, error ratio, final percentiles, the saved
// session path, and the §8.2 counts-shown disclosure — charts intact below),
// then the percentile band, the compact health line (4xx / rst / t/o, item
// 10), and one sparkline per counter series below (sorted by name for
// deterministic frames; the pool-pending series renders its fraction of
// `pool_pending_cap` when the run's config sets it, item 12). `width`/
// `height` are the pane-local dimensions (terminal size minus the status
// bar, the tab strip, and the key bar). The empty state is state-aware:
// idle (no run yet) renders "no run yet - press n to start one"; a running
// run before its first flush renders "waiting for first flush".
ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height,
                                absl::string_view session_path = "",
                                uint64_t pool_pending_cap = 0);

// The one-row tab strip (backlog item 11): `| charts | flame | knobs |` under
// the status bar. The active tab renders inverted. All three panes exist as
// of M1: the flame cell is no longer the dimmed M1 placeholder.
ftxui::Element RenderTabStrip(int active_tab);

// The persistent bottom-row key bar, rendered from the shell's real key map:
// navigate names n/c/f/k/q/? (plus s while a run is live, and q's stop-then-
// quit); on the flame tab it names the flame-pane keys instead; editing modes
// name the dialog's or the search field's tab/enter/esc. `session_open`
// marks the state where `n` can never succeed (a session is open until it is
// closed with q): the n hint is then annotated as unavailable instead of
// advertising a key that would silently refuse.
ftxui::Element RenderKeyBar(InputMode mode, bool running, bool session_open,
                            int active_tab = 0);

// The navigate-mode key map as data — the SINGLE SOURCE OF TRUTH the help
// overlay renders from and the key-map test asserts (TuiApp's handler is
// written against the same table; a row dropped from either side is a
// visible test failure). The M2/M3 collision fix is pinned here in code:
// plain n opens the start-run dialog, N (Shift+n) opens the noise-floor
// view, / enters search.
struct KeyBinding {
  const char* key;
  const char* action;
};
std::vector<KeyBinding> NavigateKeyBindings();

// The `?` help overlay: every key of the real key map (rendered from
// NavigateKeyBindings) and every status-bar field with a one-line
// definition, so no on-screen token stays undefined.
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
