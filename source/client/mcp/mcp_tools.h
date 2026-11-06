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
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

#include "source/client/headless/fold_tracker.h"
#include "source/client/headless/session_recorder.h"
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

  // The process-shutdown path (SIGTERM, stdin EOF): stops an in-flight run
  // (bounded wait for its terminal event), then seals the open session.
  // Idempotent; safe to call with nothing in flight.
  void GracefulStop();

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

  // The scope-check (resolve -> check-all -> rewrite-to-IP) for one target
  // uri. Returns the rewritten uri (resolved IP substituted for the host).
  absl::StatusOr<std::string> ScopeAndRewriteTarget(absl::string_view uri);

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

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_
