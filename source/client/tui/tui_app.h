#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>

#include "grpc++/grpc++.h"

#include "absl/status/status.h"
#include "ftxui/component/event.hpp"

#include "source/client/tui/input_mode.h"
#include "source/client/tui/run_feed.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/session_writer.h"
#include "source/client/tui/start_run_dialog.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the headless run/session lifecycle carrier behind the
// interactive shell — the exact pipeline the TUI drives (feed -> ApplySnapshot
// + store + SessionWriter via ToMetricSeries; stop key -> RunFeed::stop();
// close strictly after on_finished), minus the terminal. FTXUI's
// ScreenInteractive loop is not headless-testable, so all lifecycle logic
// lives here and the shell (in tui_app.cc) only composes RenderLayout and the
// key map on top.
//
// Threading (spec §4): this class is called from the shell's UI thread
// (startRun/stopRun/finishSession) and from the feed's read thread (the
// internal callbacks); the model crosses to the render side only as immutable
// snapshots via the SnapshotStore.
class RunController {
public:
  // @param channel a channel to a Nighthawk service gRPC endpoint.
  // @param session_prefix prefix for the session file naming
  // ("<prefix>-<unix_ns>.nhsession", plus the ".jsonl" sidecar).
  RunController(const std::shared_ptr<grpc::ChannelInterface>& channel, std::string session_prefix);
  // Cancels an in-flight run and joins the feed's read thread. Because feed_
  // is declared LAST, it is destroyed FIRST: the teardown callbacks (a final
  // onSnapshot/onFinished on the read thread while the destructor cancels and
  // joins) find every other member — the store, the writer, the mutex — still
  // alive, so destruction is safe even with the run live (the Ctrl-C/EOF path,
  // where finishSession never ran). An open session (close() never called)
  // drains its queued records in the writer's destructor, but writes no run
  // end record — prefer finishSession().
  ~RunController();

  RunController(const RunController&) = delete;
  RunController& operator=(const RunController&) = delete;

  // Callback invoked on the feed's read thread after a snapshot has been
  // folded into the model, stored, and enqueued to the session writer. The
  // shell posts one gated redraw per call (suppressed while an input mode owns
  // the keyboard, per RefreshSuspended); headless consumers may leave it null.
  void set_snapshot_notification(std::function<void()> callback);

  // Starts a run and opens its session file pair. kFailedPrecondition while a
  // run is in flight or a session is still open (the close happens on
  // finishSession — one session per run at M0b), or an error status when the
  // run could not be started / the session files could not be created.
  absl::Status startRun(const nighthawk::client::StartRequest& request);

  // The stop key: RunFeed::stop() only — the run stops because the service's
  // M0a watchdog observes the RPC cancellation; the TUI adds nothing else.
  void stopRun();

  // True while a started run has not yet delivered its on_finished.
  bool runInProgress() const;

  // Whether the most recent run delivered its final output (natural
  // completion); a stopped run reports false. The shell keys finishSession's
  // `completed` off this.
  bool lastRunEndedCompleted() const;

  // The current immutable model for rendering, or nullptr before the first
  // snapshot of a session.
  std::shared_ptr<const RunViewModel> model() const;

  // The open session's data file path (empty when none is open).
  std::string sessionPath() const;

  // The options of the most recent startRun (empty when none has happened) —
  // the layout's knob pane renders from these.
  const nighthawk::client::CommandLineOptions& knobOptions() const;

  // The quit / end-of-session path, single-call: stops an in-flight run, waits
  // (bounded) for its on_finished, then closes the session writer exactly once
  // with the given completed flag (a naturally completed run's final output is
  // carried in the run end record). OkStatus also when no session was ever
  // opened. An error return means on_finished did not land within the
  // shutdown budget; the writer then drains without a run end record in the
  // destructor.
  absl::Status finishSession(bool completed);

  // The loop-exit teardown, shared by every way the interactive shell's
  // ScreenInteractive::Loop can return: the q and Ctrl-C handlers run
  // finishSession inside the loop (stop + flush + exit, per the runbook; the
  // shell disables FTXUI's Ctrl-C interception so the event reaches the key
  // map), but an EOF exit never reaches that handler — the shell therefore
  // calls this unconditionally once the loop is over. It drops the snapshot
  // notification first (no stale postEvent into a finished screen), then
  // runs the same flush/close sequence the q path uses. After a q or Ctrl-C
  // exit this is a no-op: the session is already closed and finishSession
  // returns OkStatus on a null writer. So a Ctrl-C mid-run leaves the same
  // parseable, run-end-stamped session file a q would have. (A signal-driven
  // exit — SIGTERM, or SIGHUP on terminal detach — terminates the process
  // before this runs; FTXUI re-raises installed signals after PostMain, and
  // installs no SIGHUP handler at all.)
  absl::Status handleLoopExit();

private:
  // The feed callbacks; all fire on the feed's read thread.
  void onSnapshot(const nighthawk::client::LiveSnapshot& snapshot);
  void onCompleted(const nighthawk::client::Output& output);
  void onFinished(const absl::Status& status);

  const std::string session_prefix_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<SessionWriter> writer_; // null when no session is open
  std::string session_path_;
  bool finished_ = true;   // false between startRun and the run's on_finished
  bool completed_ = false; // most recent run delivered on_completed
  bool has_output_ = false;
  nighthawk::client::Output final_output_;
  std::function<void()> snapshot_notification_;
  nighthawk::client::CommandLineOptions knob_options_;

  // Members the feed's read-thread callbacks touch (model_, store_, and, via
  // onSnapshot, writer_/mutex_ above) MUST be declared before feed_: members
  // are destroyed in reverse declaration order, so feed_ — whose destructor
  // cancels the RPC and joins the read thread, letting the last callbacks
  // fire — is destroyed first and everything its callbacks reference is still
  // alive. Destroying this object with a run live (Ctrl-C / EOF, where
  // finishSession never ran) is therefore safe.
  RunViewModel model_;
  SnapshotStore store_;
  RunFeed feed_;
};

// FORK-ONLY (We-Amp): the interactive shell's mutable UI state, extracted
// from runInteractive so the key map (HandleShellEvent) is headless-testable —
// the FTXUI ScreenInteractive loop itself is not. A test drives real
// ftxui::Event values (including Event::CtrlC) against a real RunController
// and asserts on this state plus the controller's session lifecycle. The
// interactive shell keeps its render-side extras (the live Input components)
// next to this struct and reads its fields each frame.
struct ShellState {
  StartRunDialogModel dialog;
  int active_tab = 0;
  InputMode mode = InputMode::kNavigate;
  // The feed thread snapshots the mode to decide whether posting a redraw is
  // allowed (RefreshSuspended); HandleShellEvent updates the mirror on every
  // transition.
  std::atomic<InputMode> mode_mirror{InputMode::kNavigate};
  std::string uri_field;
  std::string rps_field;
  std::string duration_field;
  // The `?` help overlay (navigate mode).
  bool help_open = false;
  // True after a rejected enter whose failure was field validation: the
  // inline errors and the border error summary render keyed on this.
  bool show_errors = false;
  // Set after a rejected enter whose fields validated but whose start the
  // controller refused (e.g. a session is already open). A distinct,
  // honest line inside the dialog — never the field-error path: a
  // "start run - 0 errors" border title would be a false statement.
  std::string start_rejected;
};

// The shell's key map — the exact handler the interactive CatchEvent
// installs, over the shell state above. Returns true when the event was
// consumed (the CatchEvent must not let it reach the text inputs). The two
// callbacks stand in for the terminal the loop owns: `force_redraw` posts the
// one forced redraw the dialog close and escape need while input is suspended
// (input_mode.h's rule); `exit_loop` leaves the ScreenInteractive loop.
// A Ctrl-C that reaches this handler routes through the same flush-then-exit
// sequence as `q` (finishSession inside the loop, then exit): FTXUI's default
// signal machinery would otherwise record SIGINT and re-raise it after
// PostMain, killing the process before the post-loop teardown ever runs. The
// shell pairs this with App::ForceHandleCtrlC(false) so the event actually
// arrives here.
bool HandleShellEvent(const ftxui::Event& event, ShellState& state, RunController& controller,
                      const std::function<void()>& force_redraw,
                      const std::function<void()>& exit_loop);

} // namespace Tui

/**
 * FORK-ONLY (We-Amp): entry point for nighthawk_tui, the terminal UI that
 * visualizes live Nighthawk benchmark snapshots over a gRPC
 * NighthawkService/ExecutionStream connection.
 *
 * Flags: --address (Nighthawk service gRPC endpoint, default
 * 127.0.0.1:8443), --session-prefix (default nighthawk-session), and
 * --selftest (render one static FTXUI frame of the real layout into an
 * off-screen ftxui::Screen(100, 30), print "tui ok" on the error stream, and
 * return 0 — a decidable smoke test that works in a non-TTY environment).
 *
 * Dependency quarantine: this library and everything it pulls in must stay
 * clear of @envoy//source/... C++ targets (see source/client/tui/README.md
 * for the standing guard that enforces this).
 *
 * @param argc argument count, in main() form.
 * @param argv argument strings, in main() form.
 * @param err receives diagnostics and the selftest output.
 * @return int process exit code: 0 on success, 1 on failure.
 */
int runTui(int argc, const char* const* argv, std::ostream& err);

} // namespace Client
} // namespace Nighthawk
