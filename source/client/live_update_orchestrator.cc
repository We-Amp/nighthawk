#include "source/client/live_update_orchestrator.h"

#include <algorithm>
#include <chrono>

#include "absl/strings/str_cat.h"

#include "source/common/common/logger.h"

namespace Nighthawk {
namespace Client {

EngineInstant DefaultEngineInstant() {
  const auto wall = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  const auto steady = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch());
  return {wall.count(), steady.count()};
}

namespace {

// The default applier: every delta that reaches it (i.e. that already passed
// the Task-2 whitelist validation) is rejected with a clear reason and the
// engine is never touched. Tasks 3+ replace this stub with the real
// capability apply path.
class RejectingUpdateApplier : public LiveUpdateOrchestrator::Applier,
                               public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  nighthawk::client::UpdateResult ApplyDeltas(const nighthawk::client::UpdateRequest& /*request*/,
                                              const std::string& /*epoch_id*/,
                                              const EngineInstant& /*bend_instant*/,
                                              absl::Time /*deadline*/) override {
    ENVOY_LOG(debug, "Live update rejected: no applier is wired yet");
    nighthawk::client::UpdateResult result;
    result.set_applied(false);
    result.set_rejection_reason("live knob updates are not yet supported by this build");
    return result;
  }
};

} // namespace

std::unique_ptr<LiveUpdateOrchestrator::Applier> MakeDefaultUpdateApplier() {
  return std::make_unique<RejectingUpdateApplier>();
}

LiveUpdateOrchestrator::LiveUpdateOrchestrator(Applier* applier, AckWriter ack_writer,
                                               CancelFn cancel_fn, PhaseIdFn phase_id_fn,
                                               std::string run_execution_id,
                                               absl::Duration ack_wait_bound,
                                               RunShapeFn run_shape_fn)
    : applier_(applier), ack_writer_(std::move(ack_writer)), cancel_fn_(std::move(cancel_fn)),
      phase_id_fn_(std::move(phase_id_fn)), run_shape_fn_(std::move(run_shape_fn)),
      run_execution_id_(std::move(run_execution_id)), ack_wait_bound_(ack_wait_bound) {}

LiveUpdateOrchestrator::~LiveUpdateOrchestrator() {
  OnRunEnded();
  JoinPendingAcks();
}

bool LiveUpdateOrchestrator::StreamTakesAcks() {
  absl::MutexLock lock(&mutex_);
  return !run_finished_ && !cancelled_;
}

void LiveUpdateOrchestrator::WriteAckSync(const nighthawk::client::UpdateResult& result) {
  if (StreamTakesAcks()) {
    ack_writer_(result);
  }
}

void LiveUpdateOrchestrator::HandleUpdate(const nighthawk::client::UpdateRequest& request) {
  // Targeting validation at the routing layer (Task 2 adds the knob
  // whitelist); every rejection below resolves synchronously with a
  // structured ack and never touches the engine.
  if (!request.execution_id().empty() && request.execution_id() != run_execution_id_) {
    nighthawk::client::UpdateResult result;
    result.set_applied(false);
    result.set_rejection_reason(absl::StrCat("execution_id \"", request.execution_id(),
                                             "\" does not match the in-flight run \"",
                                             run_execution_id_, "\""));
    WriteAckSync(result);
    return;
  }
  if (!request.phase_id().empty()) {
    const std::string current_phase_id = phase_id_fn_();
    if (request.phase_id() != current_phase_id) {
      nighthawk::client::UpdateResult result;
      result.set_applied(false);
      result.set_rejection_reason(absl::StrCat("phase_id \"", request.phase_id(),
                                               "\" does not match the current phase \"",
                                               current_phase_id, "\""));
      WriteAckSync(result);
      return;
    }
  }
  if (request.deltas().empty()) {
    nighthawk::client::UpdateResult result;
    result.set_applied(false);
    result.set_rejection_reason("update carries no deltas; at least one knob delta is required");
    WriteAckSync(result);
    return;
  }

  // Whitelist validation (M3 Task 2): every delta must be a live knob ({rps,
  // ramp shape}) and satisfy its invariants, judged against the ENGINE's
  // declared run shape. A rejection resolves synchronously with a structured
  // ack naming the offending field and never reaches the applier or the
  // engine. Cheap and pure -- it belongs on the reader thread.
  const absl::Status delta_status =
      ValidateUpdateDeltas(request, run_shape_fn_ ? run_shape_fn_() : InFlightRunShape{});
  if (!delta_status.ok()) {
    nighthawk::client::UpdateResult result;
    result.set_applied(false);
    result.set_rejection_reason(std::string(delta_status.message()));
    WriteAckSync(result);
    return;
  }

  // Pending: the bounded ack wait runs OFF the reader thread (M4 contract) —
  // stamp the bend instant, hand the deltas to the applier on a short-lived
  // thread, and return immediately so the reader loop keeps reading (a
  // cancellation queued behind this update is never stalled by its ack wait).
  const EngineInstant bend_instant = DefaultEngineInstant();
  uint64_t candidate_epoch;
  {
    // Registered-and-checked spawn: the run-end / cancel flags are tested in
    // the same critical section that stores the thread, so no ack thread can
    // be spawned after JoinPendingAcks starts draining. A thread spawned here
    // blocks on mutex_ until this critical section releases — no deadlock.
    absl::MutexLock lock(&mutex_);
    if (run_finished_ || cancelled_) {
      // The run is winding down; the update resolves without a stream write.
      return;
    }
    candidate_epoch = applied_epoch_count_ + 1;
    ack_threads_.push_back(std::thread(&LiveUpdateOrchestrator::AckThreadMain, this, request,
                                       candidate_epoch, absl::StrCat("epoch-", candidate_epoch),
                                       bend_instant, absl::Now() + ack_wait_bound_));
  }
}

void LiveUpdateOrchestrator::AckThreadMain(nighthawk::client::UpdateRequest request,
                                           uint64_t epoch_number, std::string epoch_id,
                                           EngineInstant bend_instant, absl::Time deadline) {
  nighthawk::client::UpdateResult result =
      applier_->ApplyDeltas(request, epoch_id, bend_instant, deadline);
  if (result.applied()) {
    // The ack's epoch identity is orchestrator-owned (Task 6's aggregation
    // keeps this): an applier that leaves the fields empty gets the candidate
    // epoch and the stamped bend instant.
    if (result.epoch_id().empty()) {
      result.set_epoch_id(epoch_id);
    }
    if (result.wall_time_unix_ns() == 0) {
      result.set_wall_time_unix_ns(bend_instant.wall_time_unix_ns);
    }
    if (result.monotonic_ns() == 0) {
      result.set_monotonic_ns(bend_instant.monotonic_ns);
    }
    // Commit the candidate epoch number: one epoch per APPLIED update — a
    // rejected or dropped attempt does not consume the number.
    absl::MutexLock lock(&mutex_);
    applied_epoch_count_ = std::max(applied_epoch_count_, epoch_number);
  }
  // The no-ack-after-final-output rule: resolve against the flags, not the
  // stream — once the run ended or a cancellation was accepted, this ack is
  // dropped without a write, no matter what the applier returned.
  if (StreamTakesAcks()) {
    ack_writer_(result);
  }
}

std::optional<nighthawk::client::UpdateResult>
LiveUpdateOrchestrator::CheckCancellation(const std::string& execution_id) {
  if (!execution_id.empty() && execution_id != run_execution_id_) {
    nighthawk::client::UpdateResult result;
    result.set_applied(false);
    result.set_rejection_reason(absl::StrCat("cancellation execution_id \"", execution_id,
                                             "\" does not match the in-flight run \"",
                                             run_execution_id_, "\""));
    return result;
  }
  return std::nullopt;
}

void LiveUpdateOrchestrator::AcceptCancellation() {
  {
    absl::MutexLock lock(&mutex_);
    cancelled_ = true;
  }
  // Invoked outside mutex_: it walks the workers under their own lock and
  // must never run while an ack thread or HandleUpdate holds mutex_. Safe on
  // the reader thread: requestExecutionCancellation only flags the workers
  // and returns — the watchdog thread already calls it concurrently with the
  // run today.
  cancel_fn_();
}

void LiveUpdateOrchestrator::OnRunEnded() {
  absl::MutexLock lock(&mutex_);
  run_finished_ = true;
}

void LiveUpdateOrchestrator::JoinPendingAcks() {
  // Swap under mutex_, join outside: an ack thread's resolution path takes
  // mutex_ (via StreamTakesAcks), so joining while holding it would deadlock.
  std::vector<std::thread> threads;
  {
    absl::MutexLock lock(&mutex_);
    threads.swap(ack_threads_);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace Client
} // namespace Nighthawk
