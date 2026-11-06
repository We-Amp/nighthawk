// FORK-ONLY (We-Amp): the MCP tool and resource registries, plus the
// Global-Constraints envelope helpers. Every tools/call success is the full
// envelope (content:[{type:"text",text:<canonical JSON>}] +
// structuredContent:<handler object>); every handler error is the same
// envelope with isError:true and the reason verbatim in the text — never a
// JSON-RPC error (those are reserved for protocol faults).
//
// Tool descriptions are specified copy: the agent's only manual. Each names
// its arguments, the global-total rps semantics, the ceiling, and the error
// shape.
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_

#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

#include "source/client/headless/fold_tracker.h"
#include "source/client/headless/run_view_model.h"
#include "source/client/headless/series_view.h"
#include "source/client/headless/session_recorder.h"
#include "source/client/tui/pprof_worker_pool.h"
#include "source/client/tui/profile_pipeline.h"
#include "source/client/tui/profile_windows.h"
#include "source/client/tui/run_feed.h"

namespace Nighthawk {
namespace Client {

using ToolHandler =
    std::function<absl::StatusOr<google::protobuf::Struct>(const google::protobuf::Struct&)>;

struct ToolSpec {
  std::string name;
  std::string description;
  // JSON Schema (as a Struct) for the arguments object: type "object",
  // "properties", "required", and "additionalProperties": false are honored
  // by the dispatcher's validation (field-naming invalid-params). Every
  // shipped tool pins "additionalProperties": false explicitly — the
  // validator defaults closed, and the schema states it so a tool listing
  // reader sees the posture without reading the dispatcher.
  google::protobuf::Struct input_schema;
  ToolHandler handler;
  // True when the handler may block on a bounded wait (steer 4s, cancel_run
  // 35s): with a dispatcher installed the dispatcher runs it OFF the dispatch
  // loop so ping keeps answering during the wait (Global Constraints). The
  // dispatcher's invalid-params validation still runs inline.
  bool deferred = false;
};

class ToolRegistry {
public:
  // Registration order is the listing order (tools/list).
  void Register(ToolSpec spec);
  const std::vector<ToolSpec>& tools() const;
  const ToolSpec* Find(absl::string_view name) const;

private:
  std::vector<ToolSpec> tools_;
};

using ResourceReader = std::function<absl::StatusOr<google::protobuf::Struct>()>;

struct ResourceSpec {
  std::string uri;
  std::string name;
  std::string description;
  std::string mime_type; // "application/json" for every structured resource.
  ResourceReader reader;
};

class ResourceRegistry {
public:
  void Register(ResourceSpec spec);
  const std::vector<ResourceSpec>& resources() const;
  const ResourceSpec* Find(absl::string_view uri) const;

private:
  std::vector<ResourceSpec> resources_;
};

// The success envelope: {content:[{type:"text",text:<canonical JSON of
// object>}], structuredContent:<object>}.
google::protobuf::Struct CallToolResult(const google::protobuf::Struct& object);

// The handler-error envelope: same shape with isError:true and the reason
// verbatim as the text part.
google::protobuf::Struct CallToolError(absl::string_view reason);

// The seed tool (the plan's Task 1 surface; NOT "ping", which is a protocol
// method). Its description is the agent-facing copy for its single argument.
void RegisterEchoTool(ToolRegistry& registry);

// --- MCP plan Task 2: the steering tools -----------------------------------
//
// The steering surface (start_run, steer, cancel_run, get_status) over one
// RunFeed + SessionRecorder + FoldTracker triple — the exact lifecycle the
// TUI's RunController drives, minus the terminal. The Global Constraints
// bind here:
//   - Target scoping binds at CONNECT: start_run resolves the uri host with
//     one canonical parser, checks EVERY resolved address against loopback
//     ∪ --allow-target-cidr, refuses off-box targets before any engine
//     connection, and rewrites the engine target to the resolved IP.
//   - The load ceiling (--max-rps, the declared global total) is enforced at
//     start_run and steer.
//   - Model validation errors surface verbatim (the StartRunDialogModel's
//     field errors).
//   - Bounded waits are stated: steer waits 2x stats_flush_interval + 2s
//     (default 4s) for the engine's apply ack; cancel_run waits --timeout +
//     5s grace (default 35s) for the final output. Expiry is an isError
//     envelope reading "timed out after <X>s: <what>". The waits run on the
//     dispatcher's waiter pool (ToolSpec::deferred), never the dispatch
//     loop.
struct SteeringToolOptions {
  // The declared global-total load ceiling (start_run/steer; exceeding is a
  // tool error naming the cap).
  long long max_rps = 100000;
  // The bound for cancel_run's final-output wait (seconds) + 5s grace.
  long long timeout_s = 30;
  // The allowlist widening the default loopback-only target posture (CIDR
  // strings; validated at startup — ValidateCidrs below).
  std::vector<std::string> allow_target_cidrs;
  // Session file prefix (SessionRecorder names "<prefix>-<unix_ns>.
  // nhsession").
  std::string session_prefix = "nighthawk-session";
};

// Resolves one host name to ALL its addresses (numeric strings). The default
// is getaddrinfo (AF_UNSPEC, numeric output). Tests inject — the scoping
// pins resolve names deterministically.
using HostResolver =
    std::function<absl::StatusOr<std::vector<std::string>>(absl::string_view host)>;

// Validates the --allow-target-cidr surface (malformed = startup failure).
// Each entry must parse as addr/len (IPv4 or IPv6); the message names the
// offending entry.
absl::Status ValidateCidrs(const std::vector<std::string>& cidrs);

// --- the security floor (MCP plan Task 5) -----------------------------------
//
// ONE canonical parse, ONE target check, ONE ceiling — every entry that
// takes a target or a load figure goes through these, so the confusion
// cases (userinfo tricks, IPv6-mapped forms, mixed resolution results)
// cannot diverge between surfaces.

// The canonical target-uri parse: ONE parse path for every consumer. The
// host is what follows the LAST '@' in the authority (the userinfo trick
// pins there), bracketed IPv6 literals keep their port after ']', and a
// zone id (%en0) is stripped — link-local scoping this server has no
// business following.
struct ParsedTarget {
  std::string scheme;
  std::string userinfo; // "" when absent; never inspected, kept for the rewrite
  std::string host;     // lowercase, brackets and zone stripped
  std::string port;     // "" when absent
  std::string path_query;
};

absl::StatusOr<ParsedTarget> ParseTargetUri(absl::string_view uri);

// The single shared target check — resolve -> check EVERY resolved address
// against loopback ∪ `allow_cidrs` -> rewrite-to-IP — for EVERY entry that
// takes a target (start_run today; any future target-taking entry must call
// this, not re-implement it). Refusals happen before any engine connection
// and name the offending address.
absl::StatusOr<std::string> CheckTarget(absl::string_view uri,
                                        const std::vector<std::string>& allow_cidrs,
                                        HostResolver resolver);

// The shared load-ceiling check behind start_run (the declared total AND
// each phase's rps) and steer: a non-positive `declared` refuses with the
// field-naming positive-integer error (BEFORE every caller's uint32 cast —
// a negative would wrap past any ceiling), and `declared` above `max_rps`
// refuses with the cap-naming error. `subject` names the refused figure
// ("declared total rps", or "phase 'warm' total rps").
absl::Status CheckRpsCeiling(long long declared, long long max_rps,
                             absl::string_view subject = "declared total rps");

// The host-blocking cap for a profile window (pull_profile_window blocks
// for the whole window): the tool's duration_s and the --profile-window-
// seconds default are both bounded by this. Named in the tool description.
constexpr int64_t kMaxProfileWindowSeconds = 300;

// The run-driving carrier behind the steering tools. One in-flight run at a
// time (the feed's own discipline); every callback lands on the feed's read
// thread and hops into this controller's mutex. Handlers are called by the
// dispatcher: get_status/start_run inline, steer/cancel_run on a waiter
// thread (their bounded waits must not occupy the dispatch loop).
class SteeringController {
public:
  // @param channel a channel to a Nighthawk service gRPC endpoint.
  // @param options the flag-derived posture (ceiling, timeout, cidrs,
  //   session prefix).
  // @param resolver the host resolver (default: getaddrinfo).
  SteeringController(const std::shared_ptr<grpc::ChannelInterface>& channel,
                     SteeringToolOptions options, HostResolver resolver = nullptr);
  // Stops an in-flight run and joins the feed's read thread (declared last,
  // destroyed first — the teardown callbacks find every other member alive).
  ~SteeringController();

  SteeringController(const SteeringController&) = delete;
  SteeringController& operator=(const SteeringController&) = delete;

  // start_run handler. Scope-check -> ceiling-check -> model validation ->
  // session open -> feed start. Returns {run_started, session,
  // resolved_target}; every refusal is an error status whose message is the
  // tool's isError text verbatim.
  absl::StatusOr<google::protobuf::Struct> StartRun(const google::protobuf::Struct& arguments);

  // steer handler (deferred: bounded wait). Ceiling-check -> SendUpdate ->
  // bounded wait (2x stats_flush_interval + 2s) for the ack. The applied
  // ack is the result ({applied, epoch_id, wall_time_unix_ns, monotonic_ns});
  // a rejection (including the engine's one-in-flight guard and the feed's
  // late-rejection surfacing, verbatim) and the timeout are error statuses.
  absl::StatusOr<google::protobuf::Struct> Steer(const google::protobuf::Struct& arguments);

  // cancel_run handler (deferred: bounded wait). SendCancellation -> bounded
  // wait (--timeout + 5s grace) for the final output -> session seal.
  // Returns {cancelled, session}.
  absl::StatusOr<google::protobuf::Struct> CancelRun(const google::protobuf::Struct& arguments);

  // get_status handler. The canonical status strip (see BuildStatus).
  absl::StatusOr<google::protobuf::Struct> GetStatus(const google::protobuf::Struct& arguments);

  // The canonical status strip: run state (in_flight/completed/session/
  // resolved target), the current phase id + knob epoch, the pending
  // steering state, the phase boundaries (drain/idle/censored), and the
  // latest counters. ONE builder — the nighthawk://status resource (Task 3)
  // renders this same object.
  google::protobuf::Struct BuildStatus();

  // --- MCP plan Task 3: the telemetry resource payloads ---------------------
  //
  // Pure-store reads: nothing here blocks. series reads the live series the
  // callbacks captured (exactly the records the session persists; fresher
  // than nighthawk://session's sealed-prefix read, which sees only frames
  // that reached the disk); timeline joins the boundary events and knob
  // epochs with the TUI's segment builders; session reads the session file's
  // sealed prefix with the torn-tail honesty. Payloads are rendered by
  // mcp_resources' pure renderers (RenderSeriesPayload et al.).
  google::protobuf::Struct BuildSeries();
  google::protobuf::Struct BuildTimeline();
  google::protobuf::Struct BuildSession();

  // The process-shutdown path (SIGTERM, stdin EOF): stops an in-flight run
  // (bounded wait for its terminal event), then seals the open session.
  // Idempotent; safe to call with nothing in flight.
  void GracefulStop();

  // The profile tools' run-context capture (Task 4): the anchor, the run
  // stamps, the worker count, the completed-work series, the streamed
  // boundary events + phase specs, and the epoch-change marks of the current
  // or most recent run — the ProfileJobInput the fold pipeline aligns
  // against. Thread-safe (the profile controller calls it from a waiter
  // thread AND from a pool job for the straddle upgrade).
  Tui::ProfileJobInput CaptureProfileContext();

  // The session recorder and the fold tracker (both referenced by the
  // profile controller; this controller outlives it).
  Tui::SessionRecorder& recorder() { return recorder_; }
  Tui::FoldTracker& fold_tracker() { return fold_tracker_; }

private:
  // Builds the feed callback set (the handlers below); called once at
  // construction — feed_ is the LAST-initialized member, so every callback
  // target is alive before a callback can fire.
  Tui::RunFeedCallbacks Callbacks();
  // Removes one steer waiter (the timed-out / served one). mutex_ held by
  // the caller.
  void UnregisterSteerWaiter(int64_t seq);
  // The feed callbacks; all fire on the feed's read thread.
  void OnSnapshot(const nighthawk::client::LiveSnapshot& snapshot);
  void OnPhaseEvent(const nighthawk::client::PhaseBoundaryEvent& event);
  void OnCompleted(const nighthawk::client::Output& output);
  void OnUpdateResult(const nighthawk::client::UpdateResult& result);
  void OnFinished(const absl::Status& status);

  // Drops queued update results no remaining waiter can ever claim (their
  // seq predates every waiter's send): a timed-out steer's late ack must not
  // wedge the queue front (mutex_ held by the caller).
  void PurgeUnclaimableResults();

  // Seals the open session (run end record, completed flag); no-op when no
  // session is open. Caller holds no lock (the recorder is a leaf).
  void SealSession(bool completed);

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  SteeringToolOptions options_;
  HostResolver resolver_;
  // The state the feed callbacks write and the handlers read; guarded by
  // mutex_.
  bool finished_ = true;
  bool completed_ = false;
  int finished_count_ = 0;
  nighthawk::client::Output final_output_;
  nighthawk::client::LiveSnapshot last_snapshot_;
  bool has_snapshot_ = false;
  std::vector<nighthawk::client::PhaseBoundaryEvent> boundary_events_;
  // The run's metric-series records (exactly what OnSnapshot persists to the
  // session), for the nighthawk://series payload; kept in memory so the
  // series resource is fresh — the session FILE's sealed prefix lags by the
  // writer's buffered frames. Cleared at every start_run.
  std::vector<nighthawk::client::MetricSeriesRecord> series_records_;
  // The knob-epoch records built from the engine verdicts (applied AND
  // rejected — WithEpochMarks/renderers keep the timeline truth: only an
  // applied ack marks), for the nighthawk://timeline payload; the same
  // filter as the session's knob_epoch records. Cleared at every start_run.
  std::vector<nighthawk::client::KnobEpochRecord> knob_epochs_;
  // The profile-window alignment inputs (Task 4), the TUI RunController's
  // counterparts: the first-snapshot anchor (the 8.5 join key), the run
  // stamps, the start request's worker count, the per-counter series (the
  // completed-work denominator's input, capped like the RunViewModel), and
  // the snapshot-carried epoch-change marks (the window-split inputs).
  Tui::ClockAnchor anchor_;
  int64_t run_start_unix_ns_ = 0;
  int64_t run_end_unix_ns_ = 0;
  uint64_t worker_count_ = 1;
  absl::flat_hash_map<std::string, std::vector<Tui::CounterPoint>> counter_series_;
  std::vector<Tui::EpochMark> epoch_marks_;
  std::string last_seen_epoch_id_;
  // Update acks not yet consumed by a steer wait, with their arrival
  // sequence; correlation between results and waiters follows the engine's
  // delivery discipline (see Steer — the guard's rejections arrive in arm
  // order before the oldest arm's applier-bound ack).
  struct QueuedResult {
    int64_t seq;
    nighthawk::client::UpdateResult result;
  };
  // One active steer wait, in send order: its send sequence and whether
  // another steer was already in flight at its send (the engine's guard
  // rejects such an arm — its verdict is a rejection, in arm order).
  struct SteerWaiter {
    int64_t seq;
    bool overlap;
  };
  std::deque<QueuedResult> update_results_;
  std::deque<SteerWaiter> steer_waiters_;
  // ONE correlation counter: both waiter registrations and result arrivals
  // draw from it, so "arrived after my send" is seq comparison against the
  // same scale.
  int64_t next_seq_ = 0;
  // Send-time context for the session's knob-epoch record (the TUI pattern).
  uint32_t pending_rps_ = 0;
  std::string pending_phase_id_;
  bool pending_ramp_bent_ = false;
  // True while the most recent outstanding steer arm TIMED OUT mid-run and
  // no newer arm has been sent: its fate is genuinely unknown (the proto's
  // late-rejection drop window may still resolve it). Resolved — back to
  // false — by the verdict (OnUpdateResult), the run's terminal
  // (OnFinished), or the next send; get_status surfaces it as
  // pending.state "unresolved".
  bool pending_unknown_ = false;
  // The accepted run's request (its stats flush interval bounds steer's
  // wait) and its rewritten target.
  nighthawk::client::StartRequest start_request_;
  std::string resolved_target_;
  // Members the teardown callbacks touch MUST be declared before feed_ (it
  // is destroyed first). The recorder is a leaf (its own mutex); the
  // tracker is mutex-guarded.
  // GUARDED-BY-NONE: recorder_ and feed_ are internally synchronized;
  // fold_tracker_ carries its own mutex (profile tools land in Task 4).
  Tui::SessionRecorder recorder_;
  Tui::FoldTracker fold_tracker_;
  Tui::RunFeed feed_;
};

// Registers the four steering tools (start_run, steer, cancel_run,
// get_status) on `registry`, bound to `controller`. Descriptions are the
// agent-facing copy: each names its arguments, the global-total rps
// semantics, the ceiling, and the error shape. steer and cancel_run are
// deferred (their bounded waits run on the dispatcher's waiter pool).
void RegisterSteeringTools(ToolRegistry& registry, SteeringController& controller);

// --- MCP plan Task 4: the profile-window + differential tools ---------------
//
// The window surface (pull_profile_window, window_status, diff_windows) over
// the TUI's profile pipeline (the FTXUI-free profile_pipeline_lib) + T0's
// FoldTracker — the exact p-flow the TUI drives, minus the terminal. The
// Global Constraints bind here:
//   - The full profile flag surface (--admin-base, --profile-retriever,
//     --profile-mount, --scp-host, --sut-profile-path, --profile-staging,
//     --pprof-tool, --sut-binary, --profile-window-seconds) configures the
//     pipeline exactly like the TUI's (--profile-mount or --scp-host given =
//     configured). Absent profile flags leave the tools REGISTERED but
//     honest: every one refuses with "profiling is not configured ...".
//   - Window ids are SERVER-ASSIGNED monotonic ints (T0's FoldTracker); an
//     agent never supplies an index, and a non-positive or unknown id is
//     refused naming the discipline.
//   - pull_profile_window blocks for its duration_s (the stated bound: the
//     window's own length plus two small admin POSTs) and runs deferred (on
//     the dispatcher's waiter pool), then submits the fold job and answers
//     {window_id, status:"folding"}; window_status{id} polls the tracker
//     until ready / failed:<reason>.
struct ProfileToolOptions {
  // True when a profile mount or scp host was given (the TUI's configured
  // rule); false = every profile tool refuses with the honest
  // not-configured error.
  bool configured = false;
  // The pipeline configuration (mirrors the TUI flags).
  Tui::ProfilePipelineConfig pipeline;
};

// The window-driving carrier behind the profile tools. One window in flight
// at a time (the tracker's single-flight discipline). Handlers are called by
// the dispatcher: window_status/diff_windows inline, pull_profile_window on
// a waiter thread (its duration_s window must not occupy the dispatch loop).
// The fold job runs on this controller's own PprofWorkerPool.
class ProfileController {
public:
  // @param steering the steering controller whose run context windows align
  //   against (and whose session recorder receives the window records); MUST
  //   outlive this controller (declare it first).
  // @param options the flag-derived posture (configured + pipeline config).
  ProfileController(SteeringController& steering, ProfileToolOptions options);
  // Joins the worker pool: in-flight fold jobs land (and mark the tracker)
  // before any state they touch goes away.
  ~ProfileController();

  ProfileController(const ProfileController&) = delete;
  ProfileController& operator=(const ProfileController&) = delete;

  // pull_profile_window handler (deferred: blocks duration_s). Stage ->
  // arm the SUT collector (one small POST) -> hold the window for
  // duration_s -> disable the collector (one small POST) -> submit the fold
  // job -> {window_id, status:"folding"}. Every refusal is an error status
  // whose message is the tool's isError text verbatim; a failed arm/disable
  // marks the staged window failed:<reason> and surfaces the reason.
  absl::StatusOr<google::protobuf::Struct> PullProfileWindow(
      const google::protobuf::Struct& arguments);

  // window_status handler. {id, status, note?} — status is
  // staged|folding|ready|failed:<reason>; the id must be a server-assigned
  // int (positive; unknown ids are refused naming the discipline).
  absl::StatusOr<google::protobuf::Struct> WindowStatus(
      const google::protobuf::Struct& arguments);

  // diff_windows handler. The normalized per-unit-of-work differential of
  // two LANDED windows: union-tree per-frame deltas + totals + the pair
  // label. An epoch-separated pair (windows pulled on either side of an
  // applied steer) is labeled "knob effect: <a> -> <b>"; a same-state pair
  // is REFUSED (its differential is the noise floor, not an effect). A
  // worker-count change computes with comparable:false + the reason;
  // impure/mixed-type pairs are refused with the pipeline's reason verbatim.
  absl::StatusOr<google::protobuf::Struct> DiffWindows(
      const google::protobuf::Struct& arguments);

private:
  // The pool-job body behind PullProfileWindow: fetch -> (straddle upgrade)
  // -> clock-uncertainty read -> RunProfilePipeline -> store + session
  // records -> tracker ready/failed. `window_id` is the tracker id assigned
  // at Stage().
  void FinishProfileWindowJob(int64_t window_id, Tui::ProfileJobInput input);

  // One landed window's single PURE part for the differential (refusing
  // honestly when none or several landed).
  absl::StatusOr<Tui::ProfileWindow> LandedPureWindow(int64_t id);

  SteeringController& steering_;
  ProfileToolOptions options_;
  // Null when profiling is not configured (the fetcher is never touched).
  std::unique_ptr<Tui::ProfileFetcher> fetcher_;
  // The windows each tracker id produced (the differential's lookup).
  std::map<int64_t, std::vector<Tui::ProfileWindow>> windows_by_id_;
  mutable std::mutex mutex_;
  Tui::ProfileWindowStore window_store_;
  // Declared LAST: destroyed FIRST, joining in-flight fold jobs before any
  // state they touch (the RunController pattern).
  Tui::PprofWorkerPool pool_;
};

// Registers the three profile tools on `registry`, bound to `controller`.
// Descriptions are the agent-facing copy (args, server-assigned ids, the
// not-configured posture, and the error shape). pull_profile_window is
// deferred (its duration_s window runs on the dispatcher's waiter pool).
void RegisterProfileTools(ToolRegistry& registry, ProfileController& controller);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_
