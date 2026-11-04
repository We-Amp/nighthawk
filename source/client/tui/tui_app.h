#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "grpc++/grpc++.h"

#include <vector>

#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "ftxui/component/event.hpp"

#include "source/client/tui/admin_client.h"
#include "source/client/tui/flamegraph.h"
#include "source/client/tui/input_mode.h"
#include "source/client/tui/layout.h"
#include "source/client/tui/pprof_worker_pool.h"
#include "source/client/tui/profile_fetcher.h"
#include "source/client/tui/profile_windows.h"
#include "source/client/tui/run_feed.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/session_writer.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/sut_stats.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// The clock anchor pair (spec §8.5): (engine wall, engine monotonic) from the
// FIRST live snapshot of a run. monotonic ~= wall - (anchor.wall -
// anchor.monotonic) maps a wall-clock interval into the engine-monotonic
// domain the completed-work series lives in.
struct ClockAnchor {
  int64_t wall_time_unix_ns = 0;
  int64_t monotonic_ns = 0;

  bool valid() const { return wall_time_unix_ns != 0 || monotonic_ns != 0; }
};

// The M1 profiling configuration, gathered from the nighthawk_tui flags (plan
// task 6: --admin-base, --profile-retriever, --profile-mount, --scp-host,
// --sut-profile-path, --profile-staging, --pprof-tool, --sut-binary,
// --profile-window-seconds, plus the PPROF_BINARY_PATH environment for the
// symbolizer child).
struct ProfilePipelineConfig {
  enum class RetrieverKind { kVolumeMount, kScp };

  AdminEndpoints admin;
  RetrieverKind retriever_kind = RetrieverKind::kVolumeMount;
  // Retriever: the mount root (bind-mount translation by basename) or the
  // scp destination ("user@host").
  std::string mount_root;
  std::string scp_host;
  // The SUT-side path the profiler writes (admin.profile_path); the
  // retriever fetches this path.
  std::string sut_profile_path;
  // Local staging directory fetched profiles land in.
  std::string staging_dir;
  // The --pprof-tool argv prefix (symbolizer provenance + invocation).
  std::vector<std::string> pprof_tool_argv{"go", "tool", "pprof"};
  // PPROF_BINARY_PATH value for the symbolizer child env (binary
  // reachability); empty = not provided.
  std::string pprof_binary_path;
  // The SUT binary path inside the layout (--sut-binary); empty = no
  // symbolization attempted (the raw-proto fallback path only).
  std::string sut_binary_path;
  // Default profile window length for the p flow (5s; the dialog variant of
  // the plan sketch was collapsed into this fixed default + flag — see the
  // task report).
  int window_seconds = 5;
  // TEST-ONLY seam (M1 final fixes): delays FetchProfile's retrieval inside
  // the pool job so a test can land the run's end inside the retrieval
  // window (the straddle-belt case). Production keeps 0.
  int test_retriever_delay_ms = 0;
};

// The completed-work series for the §8.4 per-unit-of-work denominator: the
// client-side request counters (the benchmark.http_* classes plus
// stream_resets and pool_connection_failure — every sample the run produced
// counts, per the charts' counts-shown disclosure) merged into one ascending
// monotonic series. At each union stamp the merged value is the sum of every
// source series' last-value-at-or-before that stamp (the same convention
// CompletedWorkInMonotonicRange applies).
std::vector<CounterPoint>
MergeCompletedSeries(const absl::flat_hash_map<std::string, std::vector<CounterPoint>>& counters);

// One pool job's captured inputs: everything StopProfileWindowAndFold grabs
// on the steering thread, so the job itself touches only its own copies plus
// the stores. Captured BEFORE the fetch — a run that ends while the fetch
// runs is exactly the excluded-and-marked straddle case.
struct ProfileJobInput {
  ClockAnchor anchor;
  int64_t run_start_unix_ns = 0;
  bool run_finished = false;
  int64_t run_end_unix_ns = 0;
  uint64_t worker_count = 1;
  std::vector<CounterPoint> completed_series;
};

// FORK-ONLY (We-Amp): the headless run/session lifecycle carrier behind the
// interactive shell — the exact pipeline the TUI drives (feed -> ApplySnapshot
// + store + SessionWriter via ToMetricSeries; stop key -> RunFeed::stop();
// close strictly after on_finished), minus the terminal. FTXUI's
// ScreenInteractive loop is not headless-testable, so all lifecycle logic
// lives here and the shell (in tui_app.cc) only composes RenderLayout and the
// key map on top.
//
// Threading (spec §4): this class is called from the shell's UI thread
// (startRun/stopRun/finishSession and the M1 profile orchestration entries)
// and from the feed's read thread (the internal callbacks); the model crosses
// to the render side only as immutable snapshots via the SnapshotStore. The
// M1 additions (below) run their blocking work — profile fetch/symbolize/
// fold/differential, and the SUT stats scrape cadence — on a PprofWorkerPool
// and a dedicated scrape thread; the render thread only ever reads the
// immutable stores.
class RunController {
public:
  // @param channel a channel to a Nighthawk service gRPC endpoint.
  // @param session_prefix prefix for the session file naming
  // ("<prefix>-<unix_ns>.nhsession", plus the ".jsonl" sidecar).
  RunController(const std::shared_ptr<grpc::ChannelInterface>& channel, std::string session_prefix);
  // Cancels an in-flight run, stops the SUT scrape cadence, joins the pool
  // and the feed's read thread. Because feed_ is declared LAST, it is
  // destroyed FIRST: the teardown callbacks (a final onSnapshot/onFinished on
  // the read thread while the destructor cancels and joins) find every other
  // member — the store, the writer, the mutex — still alive, so destruction
  // is safe even with the run live (the Ctrl-C/EOF path, where finishSession
  // never ran). pool_ is declared after feed_, so it is destroyed FIRST of
  // all members: its destructor joins any in-flight profile/differential jobs
  // before the state they touch (window store, writer, mutexes) goes away.
  // An open session (close() never called) drains its queued records in the
  // writer's destructor, but writes no run end record — prefer
  // finishSession().
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

  // The boundary events of the current/most recent run, in boundary order
  // (M2 Task 7 wiring: stream-at-boundary intake; cleared at every startRun).
  // The timeline strip and the window-split intervals build from these; the
  // final Output's PhaseMetadata (carried in the run end record) stays the
  // post-run completeness source for the strip's claim/pool cells.
  std::vector<nighthawk::client::PhaseBoundaryEvent> boundaryEvents() const;

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

  // --- M1 profile pipeline (plan task 6) -----------------------------------
  //
  // Installs the profiling configuration: the SUT admin endpoint, the profile
  // retriever, the staging directory, the symbolizer argv prefix and the
  // binary-reachability layout, and the default window length. Call ONCE,
  // before the first run; pool jobs capture what they need by value at
  // submit time, so nothing here is hot-swappable. Without a configuration
  // the p/d/N keys refuse with an honest kFailedPrecondition.
  void ConfigureProfiling(ProfilePipelineConfig config);
  bool profilingConfigured() const;

  // 'p' open: enables the SUT CPU profiler for a window of the configured
  // length (the admin mutator itself takes no duration; the window ends at
  // StopWindow). Requires a run — live or already finished — whose
  // stamps the window will align against. kFailedPrecondition when profiling
  // is unconfigured, no run stamps exist, or a window is already open; the
  // underlying admin failure otherwise.
  absl::Status StartProfileWindow();

  // 'p' close: submits ONE pool job — {disable + fetch + retrieve ->
  // symbolize -> LoadProfile -> FoldProfile -> PreA1RunInterval alignment ->
  // CompletedWorkInMonotonicRange through the first-snapshot anchor ->
  // ProfileWindowStore append + SessionWriter enqueue}. Non-blocking. The
  // window state moves to "folding..." until the job lands.
  // kFailedPrecondition when profiling is unconfigured, no window is open,
  // or no snapshot anchor exists yet (the work-units join needs the first
  // flush). A pure window AND an excluded-and-marked (impure) one both end up
  // in the store and the session; only a pipeline failure (unreadable
  // profile) lands nowhere, with the reason in flameNote().
  absl::Status StopProfileWindowAndFold();

  bool profileWindowOpen() const;

  // The status-bar profiler state: "idle" | "window open <n>s" |
  // "folding...".
  std::string profileWindowState() const;

  // The immutable window store snapshot the render side reads, or nullptr
  // before the first window landed.
  std::shared_ptr<const std::vector<ProfileWindow>> loadWindows() const;

  // 'd'/'N': computes the differential (or same-state noise floor) over the
  // two most recent PURE windows, as a pool job. Non-blocking; the paired
  // overlay arrives via loadDiffOverlay(). With fewer than two pure windows
  // it refuses synchronously with the reason in flameNote() (never a silent
  // no-op). An incomparable pair (worker-count boundary) still produces an
  // overlay — it carries the marked reason for the renderer.
  void ComputeWindowDiffForLastTwoPureWindows(bool noise_floor);

  // The latest paired differential (DiffOverlay type-pairs the result with
  // the window snapshot that owns its frame pointers), or nullopt before the
  // first computation.
  std::optional<DiffOverlay> loadDiffOverlay() const;

  // The latest flame-pane status line: pipeline outcomes, differential
  // results, refusals. Empty until something worth saying happened.
  std::string flameNote() const;

  // Surfaces a transient flame-pane note from the shell (key refusals with
  // their reason). Display only; never mutates pipeline state.
  void NoteFlameStatus(std::string note);

private:
  // The feed callbacks; all fire on the feed's read thread.
  void onSnapshot(const nighthawk::client::LiveSnapshot& snapshot);
  void onPhaseEvent(const nighthawk::client::PhaseBoundaryEvent& event);
  void onCompleted(const nighthawk::client::Output& output);
  void onFinished(const absl::Status& status);

  // The SUT stats scrape cadence (plan task 7 wiring): one scrape per flush
  // interval, delta against the previous scrape, merged into the model's
  // counters map as sut.* series. Runs on scraper_ until scraping_ clears.
  void ScrapeLoop();
  // The pool-job body behind StopProfileWindowAndFold.
  void FinishProfileWindowJob(ProfileJobInput& input);

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
  // M2 Task 7 wiring: the run's boundary events, in boundary order (the
  // feed's read thread appends; the render side copies under the mutex).
  std::vector<nighthawk::client::PhaseBoundaryEvent> boundary_events_;

  // --- M1 state (guarded by mutex_ unless noted) ---------------------------
  // Configure-once pipeline configuration; read by pool jobs by value.
  // GUARDED: none (written once by ConfigureProfiling before any job runs).
  ProfilePipelineConfig profile_config_;
  // Run stamps (mac wall, spec §8.5 metadata) recorded at startRun and
  // onFinished; the PreA1RunInterval producer's inputs.
  bool has_run_stamp_ = false;
  int64_t run_start_unix_ns_ = 0;
  int64_t run_end_unix_ns_ = 0;
  bool run_finished_ = true;
  // The start request's concurrency (the 8.4 non-comparability key); 1 when
  // the request left it unset.
  uint64_t worker_count_ = 1;
  bool profiling_configured_ = false;
  // Clock anchor pair (wall, monotonic) from the FIRST live snapshot of the
  // run — the §8.5 join key between the mac-wall window interval and the
  // engine-monotonic completed-work series.
  ClockAnchor anchor_;
  // Previous scrape for the delta cadence.
  bool has_prev_scrape_ = false;
  SutScrape prev_scrape_{/*wall_time_unix_ns=*/0, /*prom_text=*/""};

  // Differential overlay + note; guarded by diff_mu_ (jobs write, render
  // side copies).
  mutable absl::Mutex diff_mu_;
  std::optional<DiffOverlay> diff_overlay_ ABSL_GUARDED_BY(diff_mu_);
  std::string flame_note_ ABSL_GUARDED_BY(diff_mu_);

  ProfileWindowStore window_store_;
  std::atomic<bool> window_open_{false};
  std::atomic<bool> folding_{false};
  std::atomic<bool> scraping_{false};

  // Members the pool jobs / scrape thread touch MUST be declared before
  // feed_ (see the destructor note). Ordering within the block: scraper_ and
  // pool_ last-ish so they join early in teardown; pool_ is the LAST member
  // of the class, so it is destroyed FIRST and joins every in-flight job
  // before any state a job touches goes away.
  std::unique_ptr<ProfileFetcher> fetcher_; // pool jobs only (serialized)
  AdminClient scrape_admin_;                // the scrape thread's own client
  std::thread scraper_;
  RunViewModel model_;
  SnapshotStore store_;
  RunFeed feed_;
  PprofWorkerPool pool_;
};

// Composes the flame pane's render input (layout.h's FlamePaneState) the same
// way the interactive shell does — shared with the tests so a dropped field
// is a visible test failure. The current window is the snapshot's most
// recent. LIFETIME (review round 1 fix): `windows` and the DiffOverlay's
// owning snapshot must be held alive by the CALLER across the whole render —
// tree and diff frame pointers point INTO them, so a temporary shared_ptr
// dies at end-of-expression while RenderLayout still reads the pane. The
// overlay is attached only when `view.diff_mode` is set (per flamegraph.h's
// lifetime rule).
FlamePaneState BuildFlamePane(std::shared_ptr<const std::vector<ProfileWindow>> windows,
                              const FlamegraphViewState& view, absl::string_view note,
                              const DiffOverlay* overlay);

// The shell's interactive UI state, extracted
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
  // --- M1 flame pane (tab 1) ----------------------------------------------
  // The pane's interaction state: zoom/selection/search/collapse/diff flags.
  // The DATA (windows, paired overlays, notes) lives in the controller; this
  // holds only the view state.
  FlamegraphViewState flame_view;
  // The search input mode's draft (kEditingSearch); applied to
  // flame_view.search on enter, discarded on esc.
  std::string search_field;
  // Whether the current diff overlay was computed as the noise floor (drives
  // the d/N toggle logic).
  bool flame_diff_is_noise = false;
};

// The shell's key map — the exact handler the interactive CatchEvent
// installs, over the shell state above. Returns true when the event was
// consumed (the CatchEvent must not let it reach the text inputs). The two
// callbacks stand in for the terminal the loop owns: `force_redraw` posts the
// one forced redraw the dialog close and escape need while input is suspended
// (input_mode.h's rule); `exit_loop` leaves the ScreenInteractive loop.
// A Ctrl-C that reaches this handler routes through the same flush-then-exit
// sequence as `q` (finishSession inside the loop, then exit), from ANY mode —
// checked before the mode maps so an open dialog cannot swallow it: FTXUI's
// default signal machinery would otherwise record SIGINT and re-raise it
// after PostMain, killing the process before the post-loop teardown ever
// runs. The shell pairs this with App::ForceHandleCtrlC(false) so the event
// actually arrives here.
bool HandleShellEvent(const ftxui::Event& event, ShellState& state, RunController& controller,
                      const std::function<void()>& force_redraw,
                      const std::function<void()>& exit_loop);

// The ShellState → OverlayState mapping the interactive shell applies every
// frame — shared with the tests so a field dropped from the mapping is a
// visible test failure, not an invisible screen (a state-only assertion
// cannot catch a missing copy). The shell-only extras the caller still fills:
// the focus-derived active field and the live Input components.
void FillOverlayState(const ShellState& state, OverlayState* overlay);

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
 * The M1 profiling flags (--admin-base, --profile-retriever, --profile-mount,
 * --scp-host, --sut-profile-path, --profile-staging, --pprof-tool,
 * --sut-binary, --profile-window-seconds) wire the flamegraph pipeline;
 * profiling is configured when a profile mount or scp host is given.
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
