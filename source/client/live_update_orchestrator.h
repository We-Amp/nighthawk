#pragma once

// FORK-ONLY (We-Amp): process-side live-update state machine (M3, A2). One
// instance per in-flight run, owned by the run thread inside
// ServiceImpl::handleExecutionRequest. The stream reader thread hands update
// and cancellation arms to it; it owns the targeting validation, the knob
// epoch bookkeeping and the pending-ack lifecycle.
//
// Threading contract (M4, test-pinned):
// - HandleUpdate runs on the stream reader thread and is CHEAP: targeting
//   validation plus handing the deltas to the applier on a short-lived
//   thread. It never waits on engine work or on an ack.
// - The bounded ack wait runs OFF the reader thread: the applier is invoked
//   on the orchestrator's own thread and MUST honor its deadline, so an ack
//   wait can never stall a cancellation dispatched behind it.
// - No ack after the final output: once the run ends (OnRunEnded) or a
//   cancellation is accepted (AcceptCancellation), pending updates resolve
//   WITHOUT any further stream write — the final output stays the last
//   response on the stream, ever. The ack writer closure enforces the same
//   gate under the stream write mutex (acks_allowed_), and every resolution
//   path rechecks the flags before writing.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

#include "api/client/service.pb.h"

#include "source/client/live_update_validator.h"

namespace Nighthawk {
namespace Client {

// The engine clock pair at one instant (spec 8.5 discipline, the same pair
// PhaseEvent emits).
struct EngineInstant {
  int64_t wall_time_unix_ns;
  int64_t monotonic_ns;
};

// The default engine clock: (wall, monotonic) ns pair captured at now.
EngineInstant DefaultEngineInstant();

class LiveUpdateOrchestrator {
public:
  // The engine-apply step. The default applier (MakeDefaultUpdateApplier)
  // rejects every delta that reaches it; the Task-2 whitelist validator runs
  // in front of it, and Tasks 3/6 replace the applier with the real
  // capability fan-out + ack aggregation.
  class Applier {
  public:
    virtual ~Applier() = default;
    // Applies the request's deltas to the engine. Called on a short-lived
    // orchestrator thread, never on the stream reader thread. MUST honor
    // `deadline`: on expiry it resolves the update as a rejection instead of
    // blocking past it — JoinPendingAcks waits at most this long.
    virtual nighthawk::client::UpdateResult
    ApplyDeltas(const nighthawk::client::UpdateRequest& request, const std::string& epoch_id,
                const EngineInstant& bend_instant, absl::Time deadline) = 0;
  };

  // Writes one UpdateResult ack onto the execution stream. Returns false when
  // the ack was not delivered (stream gone, or the stream no longer carries
  // acks because the run ended or a cancellation was accepted).
  using AckWriter = std::function<bool(const nighthawk::client::UpdateResult&)>;

  // Cancels the in-flight run (ProcessImpl::requestExecutionCancellation).
  using CancelFn = std::function<void()>;
  // The in-flight run's current phase id.
  using PhaseIdFn = std::function<std::string()>;
  // The in-flight run's declared shape, as the whitelist validator sees it
  // (built by the engine, never from the request).
  using RunShapeFn = std::function<InFlightRunShape()>;

  LiveUpdateOrchestrator(Applier* applier, AckWriter ack_writer, CancelFn cancel_fn,
                         PhaseIdFn phase_id_fn, std::string run_execution_id,
                         absl::Duration ack_wait_bound = absl::Seconds(5),
                         RunShapeFn run_shape_fn = nullptr);
  ~LiveUpdateOrchestrator();

  LiveUpdateOrchestrator(const LiveUpdateOrchestrator&) = delete;
  LiveUpdateOrchestrator& operator=(const LiveUpdateOrchestrator&) = delete;

  // Stream reader thread, update arm. Validates targeting (execution id /
  // phase id match, non-empty deltas) and then the knob whitelist (M3 Task
  // 2, judged against the engine's declared run shape); a targeting or
  // whitelist rejection resolves synchronously with an ack written before
  // return, and never reaches the applier. Otherwise the update goes
  // pending: the deltas are handed to the applier on a short-lived thread
  // which writes the ack. Returns promptly either way.
  void HandleUpdate(const nighthawk::client::UpdateRequest& request);

  // Stream reader thread, cancellation arm. Returns nullopt when the
  // cancellation targets the in-flight run and may be accepted; the caller
  // flips its stream-side ack gate BEFORE calling AcceptCancellation, so the
  // accepted cancellation's mark and the gate flip are ordered without a
  // window. A non-nullopt return is the structured targeting rejection to
  // ack instead (the stream stays up).
  std::optional<nighthawk::client::UpdateResult> CheckCancellation(const std::string& execution_id);

  // Marks the cancellation accepted (pending updates now resolve without a
  // stream write) and cancels the run. Returns promptly. Call only after a
  // nullopt CheckCancellation, after flipping the stream-side ack gate.
  void AcceptCancellation();

  // Writes one ack synchronously (the shared gate applies). Used for
  // targeting rejections on both arms.
  void WriteAckSync(const nighthawk::client::UpdateResult& result);

  // Run thread, after the run finished and BEFORE the final response is
  // written: pending updates resolve without a stream write from here on.
  void OnRunEnded();

  // Run thread: join every pending-ack thread. Bounded: appliers honor their
  // deadline. Called after the final response is written and again (defensively)
  // in the destructor, always before the orchestrator and its applier go away.
  void JoinPendingAcks();

private:
  // The per-resolution ack gate: false once the run ended or a cancellation
  // was accepted.
  bool StreamTakesAcks() ABSL_LOCKS_EXCLUDED(mutex_);

  void AckThreadMain(nighthawk::client::UpdateRequest request, uint64_t epoch_number,
                     std::string epoch_id, EngineInstant bend_instant, absl::Time deadline);

  Applier* applier_;
  AckWriter ack_writer_;
  CancelFn cancel_fn_;
  PhaseIdFn phase_id_fn_;
  RunShapeFn run_shape_fn_;
  const std::string run_execution_id_;
  const absl::Duration ack_wait_bound_;

  absl::Mutex mutex_;
  // Number of APPLIED updates; the epoch id candidate handed to the next
  // apply is "epoch-<applied_epoch_count_ + 1>" (plain per-run monotonic
  // counter, its own namespace — never compared with phase ids). A rejected
  // or dropped attempt does not consume the number.
  uint64_t applied_epoch_count_ ABSL_GUARDED_BY(mutex_) = 0;
  bool run_finished_ ABSL_GUARDED_BY(mutex_) = false;
  bool cancelled_ ABSL_GUARDED_BY(mutex_) = false;
  std::vector<std::thread> ack_threads_ ABSL_GUARDED_BY(mutex_);
};

// The default applier: rejects every delta that reaches it with a clear
// reason, never touching the engine. Only deltas that passed the Task-2
// whitelist validation arrive here; Tasks 3+ replace this stub with the real
// capability apply path.
std::unique_ptr<LiveUpdateOrchestrator::Applier> MakeDefaultUpdateApplier();

} // namespace Client
} // namespace Nighthawk
