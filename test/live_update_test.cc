// FORK-ONLY (We-Amp): LiveUpdateOrchestrator unit tests (M3 Task 1). The
// service-level routing tests live in service_test.cc; these pin the
// orchestrator contract directly, including the M4 threading properties
// (bounded ack wait off the dispatching thread; no ack after the run ended or
// a cancellation was accepted) with precise fake-level observability.

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

#include "api/client/service.pb.h"

#include "source/client/live_update_orchestrator.h"
#include "source/client/live_update_validator.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

using nighthawk::client::UpdateRequest;
using nighthawk::client::UpdateResult;
using testing::HasSubstr;

// Records every ack the orchestrator writes.
class AckRecorder {
public:
  LiveUpdateOrchestrator::AckWriter writer() {
    return [this](const UpdateResult& result) {
      std::lock_guard<std::mutex> lock(mutex_);
      acks_.push_back(result);
      return true;
    };
  }
  std::vector<UpdateResult> acks() {
    std::lock_guard<std::mutex> lock(mutex_);
    return acks_;
  }
  size_t size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return acks_.size();
  }

private:
  std::mutex mutex_;
  std::vector<UpdateResult> acks_;
};

// Canned-result fake applier; counts invocations and records what it got.
class FakeApplier : public LiveUpdateOrchestrator::Applier {
public:
  explicit FakeApplier(UpdateResult result) : result_(std::move(result)) {}
  UpdateResult ApplyDeltas(const UpdateRequest& /*request*/, const std::string& epoch_id,
                           const EngineInstant& bend_instant, absl::Time deadline) override {
    absl::MutexLock lock(&mutex_);
    ++calls_;
    last_epoch_id_ = epoch_id;
    last_bend_instant_ = bend_instant;
    last_deadline_ = deadline;
    return result_;
  }
  int calls() {
    absl::MutexLock lock(&mutex_);
    return calls_;
  }
  std::string last_epoch_id() {
    absl::MutexLock lock(&mutex_);
    return last_epoch_id_;
  }

private:
  absl::Mutex mutex_;
  int calls_ ABSL_GUARDED_BY(mutex_) = 0;
  std::string last_epoch_id_ ABSL_GUARDED_BY(mutex_);
  EngineInstant last_bend_instant_{0, 0};
  absl::Time last_deadline_ = absl::InfinitePast();
  UpdateResult result_;
};

// Blocks until released or its deadline, then returns an applied=true result.
class BlockingApplier : public LiveUpdateOrchestrator::Applier {
public:
  UpdateResult ApplyDeltas(const UpdateRequest& /*request*/, const std::string& epoch_id,
                           const EngineInstant& bend_instant, absl::Time deadline) override {
    while (!released_.load() && absl::Now() < deadline) {
      std::this_thread::sleep_for(10ms);
    }
    blocked_to_deadline_ = !released_.load();
    UpdateResult result;
    result.set_applied(true);
    result.set_epoch_id(epoch_id);
    result.set_wall_time_unix_ns(bend_instant.wall_time_unix_ns);
    result.set_monotonic_ns(bend_instant.monotonic_ns);
    return result;
  }
  void Release() { released_.store(true); }
  bool blockedToDeadline() const { return blocked_to_deadline_.load(); }

private:
  std::atomic<bool> released_{false};
  std::atomic<bool> blocked_to_deadline_{false};
};

UpdateRequest MakeDeltasUpdate() {
  UpdateRequest request;
  request.add_deltas()->mutable_requests_per_second()->set_value(5);
  return request;
}

std::unique_ptr<LiveUpdateOrchestrator>
MakeOrchestrator(LiveUpdateOrchestrator::Applier* applier, AckRecorder* recorder,
                 const std::string& execution_id = "",
                 LiveUpdateOrchestrator::RunShapeFn run_shape_fn = nullptr) {
  return std::make_unique<LiveUpdateOrchestrator>(
      applier, recorder->writer(), []() {}, []() { return std::string("current-phase"); },
      execution_id, absl::Seconds(1), std::move(run_shape_fn));
}

// An applied update's ack carries the orchestrator-stamped epoch id and the
// bend instant's engine clock pair, and successive applied updates advance
// both monotonically.
TEST(LiveUpdateOrchestratorTest, AckCarriesEpochAndClockPair) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder);

  // Let each update resolve fully before the next: the second observes the
  // committed epoch counter, and the run does not end while an update is
  // still resolving (that would gate its ack off by design).
  orchestrator->HandleUpdate(MakeDeltasUpdate());
  const auto first_ack_deadline = std::chrono::steady_clock::now() + 2s;
  while (recorder.size() == 0 && std::chrono::steady_clock::now() < first_ack_deadline) {
    std::this_thread::sleep_for(2ms);
  }
  ASSERT_EQ(recorder.size(), 1);
  orchestrator->HandleUpdate(MakeDeltasUpdate());
  const auto second_ack_deadline = std::chrono::steady_clock::now() + 2s;
  while (recorder.size() < 2 && std::chrono::steady_clock::now() < second_ack_deadline) {
    std::this_thread::sleep_for(2ms);
  }
  ASSERT_EQ(recorder.size(), 2);
  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(recorder.size(), 2);
  const auto acks = recorder.acks();
  EXPECT_TRUE(acks[0].applied());
  EXPECT_TRUE(acks[1].applied());
  EXPECT_EQ(acks[0].epoch_id(), "epoch-1");
  EXPECT_EQ(acks[1].epoch_id(), "epoch-2");
  EXPECT_GT(acks[0].wall_time_unix_ns(), 0);
  EXPECT_GT(acks[0].monotonic_ns(), 0);
  EXPECT_GE(acks[1].wall_time_unix_ns(), acks[0].wall_time_unix_ns());
  EXPECT_GE(acks[1].monotonic_ns(), acks[0].monotonic_ns());
}

// Targeting validation resolves synchronously with structured rejections and
// never reaches the applier.
TEST(LiveUpdateOrchestratorTest, TargetingRejectionsResolveSynchronously) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder, "run-id");

  UpdateRequest wrong_run = MakeDeltasUpdate();
  wrong_run.set_execution_id("other-run");
  orchestrator->HandleUpdate(wrong_run);

  UpdateRequest wrong_phase = MakeDeltasUpdate();
  wrong_phase.set_phase_id("some-other-phase");
  orchestrator->HandleUpdate(wrong_phase);

  UpdateRequest no_deltas;
  orchestrator->HandleUpdate(no_deltas);

  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(applier.calls(), 0);
  EXPECT_EQ(recorder.size(), 3);
  const auto acks = recorder.acks();
  EXPECT_FALSE(acks[0].applied());
  EXPECT_NE(acks[0].rejection_reason().find("execution_id"), std::string::npos);
  EXPECT_FALSE(acks[1].applied());
  EXPECT_NE(acks[1].rejection_reason().find("phase_id"), std::string::npos);
  EXPECT_FALSE(acks[2].applied());
  EXPECT_NE(acks[2].rejection_reason().find("no deltas"), std::string::npos);
}

// Cancellation targeting: a mismatched execution_id yields a structured
// rejection; a matching (or empty) one is acceptable.
TEST(LiveUpdateOrchestratorTest, CancellationTargetingValidation) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder, "run-id");

  auto mismatch = orchestrator->CheckCancellation("other-run");
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_FALSE(mismatch->applied());
  EXPECT_NE(mismatch->rejection_reason().find("execution_id"), std::string::npos);
  EXPECT_FALSE(orchestrator->CheckCancellation("run-id").has_value());
  EXPECT_FALSE(orchestrator->CheckCancellation("").has_value());
}

// M4: accepting a cancellation returns promptly while an update's ack wait is
// in flight, and the resolved pending update is never written.
TEST(LiveUpdateOrchestratorTest, CancellationNotBlockedByPendingAckWait) {
  BlockingApplier applier;
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder);

  orchestrator->HandleUpdate(MakeDeltasUpdate());
  std::this_thread::sleep_for(50ms);

  const auto start = std::chrono::steady_clock::now();
  orchestrator->AcceptCancellation();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, 1s);
  // Release the pending update; even though it resolves as applied=true, the
  // accepted cancellation gates the write off.
  applier.Release();
  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();
  EXPECT_EQ(recorder.size(), 0);
}

// M4: a pending update whose run ends resolves without a stream write.
TEST(LiveUpdateOrchestratorTest, PendingUpdateResolvesWithoutWriteAfterRunEnd) {
  BlockingApplier applier;
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder);

  orchestrator->HandleUpdate(MakeDeltasUpdate());
  std::this_thread::sleep_for(50ms);
  orchestrator->OnRunEnded();
  applier.Release();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(recorder.size(), 0);
}

// An update arriving after the run ended resolves without a write and never
// reaches the applier.
TEST(LiveUpdateOrchestratorTest, UpdateAfterRunEndResolvesWithoutWrite) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder);

  orchestrator->OnRunEnded();
  orchestrator->HandleUpdate(MakeDeltasUpdate());
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(applier.calls(), 0);
  EXPECT_EQ(recorder.size(), 0);
}

// The Task-1 default applier rejects every delta with a clear reason and
// without engine effects (there are none to have).
TEST(LiveUpdateOrchestratorTest, DefaultApplierRejectsWithReason) {
  auto applier = MakeDefaultUpdateApplier();
  UpdateResult result = applier->ApplyDeltas(MakeDeltasUpdate(), "epoch-1", DefaultEngineInstant(),
                                             absl::InfiniteFuture());
  EXPECT_FALSE(result.applied());
  EXPECT_FALSE(result.rejection_reason().empty());
}

// --- Whitelist validator (pure, M3 Task 2) ---

// Run shapes used across the validator tests.
InFlightRunShape LegacyRunShape() {
  // No phase vector, no ramp plugin, unbounded remaining time (--no-duration
  // semantics: the ramp-time bound does not apply).
  return InFlightRunShape{};
}

InFlightRunShape LegacyBoundedRunShape(std::chrono::nanoseconds remaining) {
  InFlightRunShape shape = LegacyRunShape();
  shape.remaining_duration = remaining;
  return shape;
}

InFlightRunShape RampPluginRunShape(std::optional<std::chrono::nanoseconds> remaining) {
  InFlightRunShape shape = LegacyRunShape();
  shape.ramp_plugin_configured = true;
  shape.remaining_duration = remaining;
  return shape;
}

InFlightRunShape PhaseRunShape(std::chrono::nanoseconds remaining) {
  InFlightRunShape shape = LegacyRunShape();
  shape.has_phase_vector = true;
  shape.ramp_plugin_configured = true; // A phase-vector run carrying the ramp plugin.
  shape.current_phase_id = "phase-1";
  shape.remaining_duration = remaining;
  return shape;
}

// Every whitelisted shape of update passes: a plain rps delta on a
// non-plugin run; a ramp delta with both fields on an unbounded ramp-plugin
// run; a ramp delta with only one field set (absent fields keep their
// current value); a ramp time that fits the current phase's remaining
// duration. Note there is no valid rps+ramp mix while C1's interim rule
// holds: rps deltas only pass on NON-plugin runs and ramp deltas only on
// plugin runs (pinned by RpsOnRampPluginRunIsRampShapeValidation).
TEST(LiveUpdateValidatorTest, ValidRpsAndRampDeltasPass) {
  UpdateRequest rps_only;
  rps_only.add_deltas()->mutable_requests_per_second()->set_value(5);
  EXPECT_TRUE(ValidateUpdateDeltas(rps_only, LegacyRunShape()).ok());
  EXPECT_TRUE(ValidateUpdateDeltas(rps_only, LegacyBoundedRunShape(30s)).ok());

  UpdateRequest ramp_both;
  auto* ramp = ramp_both.add_deltas()->mutable_ramp();
  ramp->mutable_ramp_time()->set_seconds(5);
  ramp->mutable_target_rps()->set_value(10);
  EXPECT_TRUE(ValidateUpdateDeltas(ramp_both, RampPluginRunShape(std::nullopt)).ok());
  EXPECT_TRUE(ValidateUpdateDeltas(ramp_both, RampPluginRunShape(30s)).ok());
  EXPECT_TRUE(ValidateUpdateDeltas(ramp_both, PhaseRunShape(30s)).ok());

  UpdateRequest ramp_partial;
  ramp_partial.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(5);
  EXPECT_TRUE(ValidateUpdateDeltas(ramp_partial, RampPluginRunShape(30s)).ok());
  UpdateRequest target_only;
  target_only.add_deltas()->mutable_ramp()->mutable_target_rps()->set_value(10);
  EXPECT_TRUE(ValidateUpdateDeltas(target_only, RampPluginRunShape(30s)).ok());
}

// A delta with no oneof arm set is rejected: unknown/absent arms fail
// validation, never default. The reason names the whitelist -- knobs outside
// it (queue bounds, connections, protocol, TLS, uri) have NO live arm at all
// (DQ-1), so the unset-arm case is the only way a non-whitelisted "delta"
// can present itself on the wire.
TEST(LiveUpdateValidatorTest, OutOfWhitelistRejectedWithReason) {
  UpdateRequest request;
  request.add_deltas(); // KnobDelta with no arm set.
  const absl::Status status = ValidateUpdateDeltas(request, LegacyRunShape());
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("no knob field is set"));
  EXPECT_THAT(status.message(), HasSubstr("live-knob whitelist"));
}

// A ramp delta on a run without the ramp plugin is rejected with a reason
// pointing at the plugin requirement; rps-only updates still pass on that
// run.
TEST(LiveUpdateValidatorTest, RampWithoutPluginRejected) {
  UpdateRequest ramp_only;
  ramp_only.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(5);
  const absl::Status status = ValidateUpdateDeltas(ramp_only, LegacyBoundedRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("ramp"));
  EXPECT_THAT(status.message(), HasSubstr("plugin"));

  UpdateRequest rps_only;
  rps_only.add_deltas()->mutable_requests_per_second()->set_value(5);
  EXPECT_TRUE(ValidateUpdateDeltas(rps_only, LegacyBoundedRunShape(30s)).ok());
}

// The ramp invariants: a non-positive ramp time, a zero rps (both the plain
// and the ramp target form), and a ramp time exceeding the remaining bound
// are each rejected with the specific reason.
TEST(LiveUpdateValidatorTest, RampInvariantsValidated) {
  UpdateRequest zero_ramp;
  zero_ramp.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(0);
  absl::Status status = ValidateUpdateDeltas(zero_ramp, RampPluginRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("ramp_time"));
  EXPECT_THAT(status.message(), HasSubstr("positive"));

  UpdateRequest negative_ramp;
  auto* ramp = negative_ramp.add_deltas()->mutable_ramp();
  ramp->mutable_ramp_time()->set_seconds(0);
  ramp->mutable_ramp_time()->set_nanos(-1);
  status = ValidateUpdateDeltas(negative_ramp, RampPluginRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("positive"));

  UpdateRequest zero_target;
  zero_target.add_deltas()->mutable_ramp()->mutable_target_rps()->set_value(0);
  status = ValidateUpdateDeltas(zero_target, RampPluginRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("target_rps"));
  EXPECT_THAT(status.message(), HasSubstr(">= 1"));

  UpdateRequest long_ramp;
  long_ramp.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(11);
  status = ValidateUpdateDeltas(long_ramp, PhaseRunShape(10s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("ramp_time"));
  EXPECT_THAT(status.message(), HasSubstr("current phase"));

  // On a legacy bounded run the ramp delta is rejected for the PLUGIN reason
  // first (no ramp plugin configured); the ramp-time bound reason only ever
  // appears on ramp-plugin runs (RampWithoutPluginRejected pins the plugin
  // rejection too).
  status = ValidateUpdateDeltas(long_ramp, RampPluginRunShape(10s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("ramp_time"));
  EXPECT_THAT(status.message(), HasSubstr("run"));

  // A ramp time exactly equal to the remaining duration is allowed (<=, not
  // <).
  UpdateRequest equal_ramp;
  equal_ramp.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(10);
  EXPECT_TRUE(ValidateUpdateDeltas(equal_ramp, PhaseRunShape(10s)).ok());
}

// A plain rps value of zero is rejected with the >= 1 reason.
TEST(LiveUpdateValidatorTest, ZeroRpsRejected) {
  UpdateRequest request;
  request.add_deltas()->mutable_requests_per_second()->set_value(0);
  const absl::Status status = ValidateUpdateDeltas(request, LegacyRunShape());
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("requests_per_second"));
  EXPECT_THAT(status.message(), HasSubstr(">= 1"));
}

// C1: an rps delta on a ramp-plugin run is a ramp-shape change (it sets the
// ramp's target frequency), so it routes through the ramp-shape validation
// path: a zero value fails the ramp invariant, and a valid value is rejected
// with the pending ramp re-anchoring reason until re-anchoring lands.
TEST(LiveUpdateValidatorTest, RpsOnRampPluginRunIsRampShapeValidation) {
  UpdateRequest zero_rps;
  zero_rps.add_deltas()->mutable_requests_per_second()->set_value(0);
  const absl::Status invariant = ValidateUpdateDeltas(zero_rps, RampPluginRunShape(30s));
  EXPECT_EQ(invariant.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(invariant.message(), HasSubstr("requests_per_second"));
  EXPECT_THAT(invariant.message(), HasSubstr(">= 1"));

  UpdateRequest valid_rps;
  valid_rps.add_deltas()->mutable_requests_per_second()->set_value(5);
  const absl::Status status = ValidateUpdateDeltas(valid_rps, RampPluginRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("requests_per_second"));
  EXPECT_THAT(status.message(), HasSubstr("pending ramp re-anchoring"));
}

// Duplicates for the same knob in one request are rejected (ambiguous
// order), for both knobs; distinct knobs in one request are fine (pinned by
// ValidRpsAndRampDeltasPass).
TEST(LiveUpdateValidatorTest, DuplicateKnobDeltasRejected) {
  UpdateRequest double_rps;
  double_rps.add_deltas()->mutable_requests_per_second()->set_value(5);
  double_rps.add_deltas()->mutable_requests_per_second()->set_value(6);
  const absl::Status rps_status = ValidateUpdateDeltas(double_rps, LegacyRunShape());
  EXPECT_EQ(rps_status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(rps_status.message(), HasSubstr("requests_per_second"));

  UpdateRequest double_ramp;
  double_ramp.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(5);
  double_ramp.add_deltas()->mutable_ramp()->mutable_target_rps()->set_value(10);
  const absl::Status ramp_status = ValidateUpdateDeltas(double_ramp, RampPluginRunShape(30s));
  EXPECT_EQ(ramp_status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(ramp_status.message(), HasSubstr("ramp"));
}

// --- Orchestrator wiring of the validator ---

// A whitelist rejection resolves synchronously with a structured ack naming
// the offending field, and never reaches the applier: at this milestone the
// applier is the only engine-mutation entry point, so a rejected update
// mutates nothing.
TEST(LiveUpdateOrchestratorTest, WhitelistRejectionsResolveSynchronouslyWithoutApplier) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder, "", []() {
    // A ramp-plugin run: the run shape comes from the engine, never from
    // the request.
    return RampPluginRunShape(30s);
  });

  UpdateRequest unset_arm;
  unset_arm.add_deltas();
  orchestrator->HandleUpdate(unset_arm);

  UpdateRequest rps_delta;
  rps_delta.add_deltas()->mutable_requests_per_second()->set_value(5);
  orchestrator->HandleUpdate(rps_delta); // pending ramp re-anchoring on a ramp-plugin run.

  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(applier.calls(), 0);
  ASSERT_EQ(recorder.size(), 2);
  const auto acks = recorder.acks();
  EXPECT_FALSE(acks[0].applied());
  EXPECT_NE(acks[0].rejection_reason().find("no knob field is set"), std::string::npos);
  EXPECT_FALSE(acks[1].applied());
  EXPECT_NE(acks[1].rejection_reason().find("pending ramp re-anchoring"), std::string::npos);
}

// A valid delta is forwarded to the applier (the Task-3+ engine apply seam):
// with a non-plugin run shape, a plain rps delta passes validation and
// reaches it.
TEST(LiveUpdateOrchestratorTest, ValidRpsDeltaIsForwardedToApplier) {
  FakeApplier applier([] {
    UpdateResult result;
    result.set_applied(true);
    return result;
  }());
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder, "", []() { return LegacyRunShape(); });

  orchestrator->HandleUpdate(MakeDeltasUpdate());
  const auto ack_deadline = std::chrono::steady_clock::now() + 2s;
  while (recorder.size() == 0 && std::chrono::steady_clock::now() < ack_deadline) {
    std::this_thread::sleep_for(2ms);
  }
  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(applier.calls(), 1);
  ASSERT_EQ(recorder.size(), 1);
  EXPECT_TRUE(recorder.acks()[0].applied());
}

} // namespace
} // namespace Client
} // namespace Nighthawk
