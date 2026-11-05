#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "absl/status/status.h"
#include "grpc++/grpc++.h"

#include "api/client/service.grpc.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the headless run feed is the gRPC client layer under
// nighthawk_tui (spec §4 headless requirement). Everything the TUI's charts
// display arrives here first: live snapshots while the run is in flight, the
// final output on natural completion, and a single terminal status. It is a
// library surface, not a process: the TUI and any future consumer sit on it.

// Callbacks fired by RunFeed. All fire ONLY on the feed's single dedicated
// read thread, in delivery order; consumers that hand work to other threads
// do the hopping themselves.
struct RunFeedCallbacks {
  // A live snapshot of the in-flight run (arrives only when the run opted in
  // to live streaming, which RunFeed::start() does on every request).
  std::function<void(const nighthawk::client::LiveSnapshot&)> on_snapshot;
  // One completed phase boundary of a multi-phase run (DQ-3: METADATA, never
  // a synthetic metric snapshot; api/client/live.proto's PhaseBoundaryEvent
  // is the wire form). Fired in boundary order, between the snapshots.
  std::function<void(const nighthawk::client::PhaseBoundaryEvent&)> on_phase_event;
  // The run's final output. Fired on natural run end ONLY (stop() — the
  // context cancel — yields no output); a SendCancellation() run DOES reach
  // it, because the explicit arm keeps the stream alive.
  std::function<void(const nighthawk::client::Output&)> on_completed;
  // The apply ack for a SendUpdate (or the targeting rejection of a
  // SendCancellation): an engine-signed UpdateResult — applied=true carries
  // the new knob epoch and bend instant, applied=false carries
  // rejection_reason. This callback ALSO fires, at most once per run and
  // always immediately before on_finished, for updates the service never
  // acked: the feed surfaces that pending state itself, and such a result is
  // NEVER an engine verdict — its rejection_reason is prefixed "run-feed: "
  // (see SendUpdate's late-rejection drop-window note). Send-ordering
  // disambiguation across arms is the service-side contract documented on
  // UpdateResult in api/client/service.proto.
  std::function<void(const nighthawk::client::UpdateResult&)> on_update_result;
  // The terminal event: fired exactly once per started run, always last. OK on
  // natural completion (including a run ended via SendCancellation); on
  // stop() (or any stream failure) it carries the gRPC failure status,
  // typically kCancelled.
  std::function<void(const absl::Status&)> on_finished;
};

// Drives one Nighthawk service ExecutionStream run at a time. Owns its gRPC
// read thread: start() spawns it, callbacks fire on it, stop() cancels the RPC
// from any thread (the TUI key path) and the read thread delivers the single
// on_finished afterwards. Note the client-side contract that mirrors
// nighthawk_live_probe: gRPC delivers no server response after the client
// cancels, so a stopped run ends via on_finished alone — the raw callbacks are
// surfaced as-is, with no interrupted-summary convention (that is the probe's).
//
// The stream is NOT half-closed while the run is in flight (DQ-2: the
// WritesDone-after-start is gone; the feed half-closes only after the final
// output has been received, which is what keeps mid-run Update/Cancel arms
// deliverable — see the Task 1 report's bidi diagnosis). Legacy consumers of
// the shared choke point keep their half-close-after-start behavior unchanged.
class RunFeed {
public:
  // @param channel a channel to a Nighthawk service gRPC endpoint.
  // @param callbacks invoked on the feed's read thread; they must not call
  // back into the same RunFeed (except stop(), which is safe from any thread).
  RunFeed(const std::shared_ptr<grpc::ChannelInterface>& channel, RunFeedCallbacks callbacks);
  // Cancels any in-flight run and joins the read thread before tearing down.
  ~RunFeed();

  RunFeed(const RunFeed&) = delete;
  RunFeed& operator=(const RunFeed&) = delete;

  // Writes the start request and returns immediately; the run progresses on
  // the read thread. Forces the fork-only live_streaming opt-in on the request
  // (field 911, presence is the marker) and defaults stats_flush_interval to
  // 1s when the caller left it unset. Note this deliberately bypasses the
  // shared client choke point, which strips live_streaming for legacy
  // consumers. The stream is deliberately NOT half-closed here (DQ-2): it
  // stays writable for SendUpdate/SendCancellation, and the feed half-closes
  // after the final output arrives. Returns kFailedPrecondition if a run is
  // already in flight; a stop() immediately followed by start() also returns
  // kFailedPrecondition, until the stopped run's on_finished has landed
  // (inherent to the single read-thread design, not a bug).
  // If the request cannot be written, returns an error and no callbacks fire;
  // once start() returns OK, on_finished fires exactly once eventually.
  absl::Status start(const nighthawk::client::StartRequest& start_request);

  // Cancels the in-flight run's RPC, which the service's cancellation watchdog
  // turns into a prompt run stop. Idempotent; safe from any thread; no-op when
  // no run is in flight. The end of the run arrives as on_finished on the read
  // thread. This is the PRIMARY stop path (M0a-proven, watchdog-based): a
  // context-cancelled run yields no further server responses. Consumers that
  // want the run to end cleanly WITH its final output should use
  // SendCancellation() instead (the explicit arm keeps the stream alive).
  void stop();

  // Writes an UpdateRequest arm on the in-flight run's stream (M3 live
  // steering, DQ-2 same-stream ruling). Safe from any thread once start() has
  // returned OK; returns kFailedPrecondition when no run is in flight and
  // kInternal when the write fails (the stream may be gone). The ack or
  // rejection arrives via on_update_result on the read thread; a second
  // update written before the first resolves is rejected by the service
  // (with a structured ack — it never queues).
  //
  // LATE-REJECTION DROP WINDOW (the T3/T6 carry-in contract): an update that
  // resolves after the run ended — or after a cancellation was accepted —
  // resolves WITHOUT any stream write; the service never acks it. Consumers
  // must therefore treat the run's terminal event as resolving every
  // outstanding update, and the feed does that surfacing for them: any
  // update still un-acked at stream end is reported exactly once via
  // on_update_result immediately before on_finished, as a feed-generated
  // result with rejection_reason prefixed "run-feed: " — never as an engine
  // ack, and never silently dropped.
  absl::Status SendUpdate(const nighthawk::client::UpdateRequest& request);

  // Writes a CancellationRequest arm on the in-flight run's stream. Unlike
  // stop() (the context cancel — no responses follow it), the explicit arm
  // keeps the stream alive: the run ends and its final output STILL arrives
  // (on_completed), then on_finished with OK. A targeting rejection (only
  // possible when the request carries a non-matching execution id) arrives
  // as an applied=false UpdateResult via on_update_result, and the run keeps
  // going. Safe from any thread; kFailedPrecondition when no run is in
  // flight.
  absl::Status SendCancellation();

  // True while a started run has not yet delivered its on_finished.
  bool running() const;

private:
  // The body of the read thread: drains the stream, dispatches snapshots,
  // phase events, update acks and the final output, half-closes after the
  // output, then Finish()es the stream, surfaces any never-acked pending
  // update, and fires on_finished once.
  void readLoop();

  // Shared write path for the mid-run arms: populates an ExecutionRequest,
  // then validates the running state and writes it under the feed mutex.
  absl::Status sendArm(
      const std::function<void(nighthawk::client::ExecutionRequest&)>& populate);

  RunFeedCallbacks callbacks_;
  std::unique_ptr<nighthawk::client::NighthawkService::Stub> stub_;

  // Guards the stream/context/thread lifecycle so stop() and a follow-up
  // start() can be called from any thread. Callbacks fire without the lock
  // held, and start() releases it while joining the previous run's thread (so
  // a re-entrant stop() from that run's on_finished cannot deadlock). The
  // context is held by pointer because grpc::ClientContext is neither copyable
  // nor assignable and each run needs a fresh one. The mid-run arm writes
  // (SendUpdate/SendCancellation) and the read thread's post-output
  // half-close share this mutex, so a Write can never race a WritesDone.
  std::mutex mutex_;
  std::unique_ptr<grpc::ClientContext> context_;
  std::unique_ptr<grpc::ClientReaderWriter<nighthawk::client::ExecutionRequest,
                                           nighthawk::client::ExecutionResponse>>
      stream_;
  std::thread read_thread_;
  // Set while start() is between its two critical sections (joining the
  // previous run's thread), so concurrent start() calls cannot double-start.
  bool start_in_progress_ = false;
  std::atomic<bool> running_{false};
  // Updates written but not yet acked on the current run (guarded by
  // mutex_; the service resolves updates FIFO, so the count is all the feed
  // needs to know that something is still pending). Surfaced at stream end
  // via the feed-generated pending result, never fabricated as an engine
  // ack (spec §4 B honesty rule).
  int update_pending_count_ = 0; // guarded by mutex_
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
