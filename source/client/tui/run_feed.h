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

// Callbacks fired by RunFeed. All three fire ONLY on the feed's single
// dedicated read thread, in delivery order; consumers that hand work to other
// threads do the hopping themselves.
struct RunFeedCallbacks {
  // A live snapshot of the in-flight run (arrives only when the run opted in
  // to live streaming, which RunFeed::start() does on every request).
  std::function<void(const nighthawk::client::LiveSnapshot&)> on_snapshot;
  // One completed phase boundary of a multi-phase run (DQ-3: METADATA, never
  // a synthetic metric snapshot; api/client/live.proto's PhaseBoundaryEvent
  // is the wire form). Fired in boundary order, between the snapshots.
  std::function<void(const nighthawk::client::PhaseBoundaryEvent&)> on_phase_event;
  // The run's final output. Fired on natural run end ONLY; a cancelled run
  // never sees this.
  std::function<void(const nighthawk::client::Output&)> on_completed;
  // The terminal event: fired exactly once per started run, always last. OK on
  // natural completion; on stop() (or any stream failure) it carries the gRPC
  // failure status, typically kCancelled.
  std::function<void(const absl::Status&)> on_finished;
};

// Drives one Nighthawk service ExecutionStream run at a time. Owns its gRPC
// read thread: start() spawns it, callbacks fire on it, stop() cancels the RPC
// from any thread (the TUI key path) and the read thread delivers the single
// on_finished afterwards. Note the client-side contract that mirrors
// nighthawk_live_probe: gRPC delivers no server response after the client
// cancels, so a stopped run ends via on_finished alone — the raw callbacks are
// surfaced as-is, with no interrupted-summary convention (that is the probe's).
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
  // consumers. Returns kFailedPrecondition if a run is already in flight; a
  // stop() immediately followed by start() also returns kFailedPrecondition,
  // until the stopped run's on_finished has landed (inherent to the single
  // read-thread design, not a bug).
  // If the request cannot be written, returns an error and no callbacks fire;
  // once start() returns OK, on_finished fires exactly once eventually.
  absl::Status start(const nighthawk::client::StartRequest& start_request);

  // Cancels the in-flight run's RPC, which the service's cancellation watchdog
  // turns into a prompt run stop. Idempotent; safe from any thread; no-op when
  // no run is in flight. The end of the run arrives as on_finished on the read
  // thread.
  void stop();

  // True while a started run has not yet delivered its on_finished.
  bool running() const;

private:
  // The body of the read thread: drains the stream, dispatches snapshots and
  // the final output, then Finish()es the stream and fires on_finished once.
  void readLoop();

  RunFeedCallbacks callbacks_;
  std::unique_ptr<nighthawk::client::NighthawkService::Stub> stub_;

  // Guards the stream/context/thread lifecycle so stop() and a follow-up
  // start() can be called from any thread. Callbacks fire without the lock
  // held, and start() releases it while joining the previous run's thread (so
  // a re-entrant stop() from that run's on_finished cannot deadlock). The
  // context is held by pointer because grpc::ClientContext is neither copyable
  // nor assignable and each run needs a fresh one.
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
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
