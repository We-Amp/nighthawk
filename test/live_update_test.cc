// FORK-ONLY (We-Amp): LiveUpdateOrchestrator unit tests (M3 Task 1). The
// service-level routing tests live in service_test.cc; these pin the
// orchestrator contract directly, including the M4 threading properties
// (bounded ack wait off the dispatching thread; no ack after the run ended or
// a cancellation was accepted) with precise fake-level observability.

#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

#include "api/client/service.pb.h"

#include "envoy/api/api.h"

#include "nighthawk/common/live_tunable_rate_limiter.h"

#include "source/client/factories_impl.h"
#include "source/client/live_update_orchestrator.h"
#include "source/client/live_update_validator.h"
#include "source/client/process_impl.h"
#include "source/common/ramp_reanchor.h"
#include "source/common/rate_limiter_impl.h"
#include "source/common/sequencer_impl.h"
#include "source/common/statistic_impl.h"
#include "source/common/stats/isolated_store_impl.h"

#include "test/mocks/client/mock_options.h"
#include "test/mocks/common/mock_platform_util.h"
#include "test/mocks/common/mock_termination_predicate.h"
#include "test/mocks/event/mocks.h"
#include "test/mocks/stats/mocks.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/utility.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

using nighthawk::client::UpdateRequest;
using nighthawk::client::UpdateResult;
// M3 Task 3 tests below use the gmock actions directly (Return/ReturnRef/
// NiceMock), mirroring test/factories_test.cc's usage.
using namespace testing;
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
    calls_.fetch_add(1);
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
  int calls() const { return calls_.load(); }

private:
  std::atomic<bool> released_{false};
  std::atomic<bool> blocked_to_deadline_{false};
  std::atomic<int> calls_{0};
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

// Task 8a: the SERVICE-side one-in-flight guard. A second update arriving
// while one is pending is rejected here with a structured ack -- never
// spawned as a concurrent applier thread, so two overlapping pendings can
// never both claim the same epoch number. The rejected attempt consumes no
// epoch: the next applied update gets the next number.
TEST(LiveUpdateOrchestratorTest, SecondUpdateWhileInFlightRejectedExactlyOneEpochClaim) {
  BlockingApplier applier;
  AckRecorder recorder;
  auto orchestrator = MakeOrchestrator(&applier, &recorder);

  // The first update goes pending (its applier blocks until released).
  orchestrator->HandleUpdate(MakeDeltasUpdate());
  // The second, arriving while the first is in flight: an honest structured
  // rejection, resolved synchronously, and NO second applier invocation.
  orchestrator->HandleUpdate(MakeDeltasUpdate());

  applier.Release();
  orchestrator->JoinPendingAcks();

  EXPECT_EQ(applier.calls(), 1) << "the overlapping second update must never reach the applier";
  const std::vector<UpdateResult> acks = recorder.acks();
  ASSERT_EQ(acks.size(), 2u);
  size_t applied_acks = 0;
  size_t rejected_acks = 0;
  for (const UpdateResult& ack : acks) {
    if (ack.applied()) {
      applied_acks++;
      // Exactly one epoch claim from the overlapping pair: epoch-1, never a
      // second pending sharing it.
      EXPECT_EQ(ack.epoch_id(), "epoch-1");
    } else {
      rejected_acks++;
      EXPECT_THAT(ack.rejection_reason(), HasSubstr("in flight"));
      EXPECT_THAT(ack.rejection_reason(), HasSubstr("bends are not queued"));
    }
  }
  EXPECT_EQ(applied_acks, 1u);
  EXPECT_EQ(rejected_acks, 1u);

  // The rejected attempt consumed no epoch number: the next applied update
  // is epoch-2, never a second epoch-1.
  orchestrator->HandleUpdate(MakeDeltasUpdate());
  orchestrator->JoinPendingAcks();
  EXPECT_EQ(applier.calls(), 2);
  const std::vector<UpdateResult> acks_after = recorder.acks();
  ASSERT_EQ(acks_after.size(), 3u);
  EXPECT_TRUE(acks_after.back().applied());
  EXPECT_EQ(acks_after.back().epoch_id(), "epoch-2");
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
// ramp's target frequency). A zero value still fails the invariant; a valid
// value VALIDATES through since M3 Task 5 landed the re-anchoring (the
// interim `pending ramp re-anchoring` rejection is superseded -- the apply
// path re-anchors via LinearRampingRateLimiterImpl::ApplyLiveRps).
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
  EXPECT_TRUE(status.ok()) << status.message();
}

// Duplicates for the same knob in one request are rejected (ambiguous
// order), for both knobs. (Distinct knobs in one request: a mixed rps+ramp
// request on a ramp-plugin run validates since M3 Task 5 landed plugin-path
// rps -- the apply composes them rps-first, then ramp. The mix on a NON-plugin
// run still fails at the ramp arm; pinned by RampWithoutPluginRejected.)
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
  // M3 Task 5 superseded the interim `pending ramp re-anchoring` rejection
  // (C1): on a ramp-plugin run a valid rps delta IS a ramp-shape delta and
  // validates through -- it is forwarded to the applier, not rejected here.
  // The forward is async (the ack thread), so wait for it BEFORE ending the
  // run: past OnRunEnded the no-ack-after-final-output gate drops it.
  orchestrator->HandleUpdate(rps_delta);
  const auto forward_deadline = std::chrono::steady_clock::now() + 2s;
  while (applier.calls() < 1 && std::chrono::steady_clock::now() < forward_deadline) {
    std::this_thread::sleep_for(2ms);
  }

  orchestrator->OnRunEnded();
  orchestrator->JoinPendingAcks();

  ASSERT_EQ(recorder.size(), 2);
  const auto acks = recorder.acks();
  EXPECT_FALSE(acks[0].applied());
  EXPECT_NE(acks[0].rejection_reason().find("no knob field is set"), std::string::npos);
  // The rps-on-plugin delta reached the applier (applied=true from the fake;
  // the real fan-out is Task 6's).
  EXPECT_EQ(applier.calls(), 1);
  EXPECT_TRUE(acks[1].applied());
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

// --- M3 Task 3: capability capture, registry, dispatcher-posted apply ---

// A minimal capability stand-in for registry tests.
class FakeLiveTunable : public LiveTunableRateLimiter {
public:
  absl::Status ApplyLiveRps(Envoy::MonotonicTime /*t_bend*/, uint32_t /*new_rps*/) override {
    return absl::OkStatus();
  }
  absl::Status ApplyLiveRamp(Envoy::MonotonicTime /*t_bend*/,
                             const RampShapeChange& /*change*/) override {
    return absl::OkStatus();
  }
};

// T2 parked minor: a ramp time with an absurd seconds value saturates in the
// conversion (no int64 overflow UB) and is rejected by the bound checks.
TEST(LiveUpdateValidatorTest, HugeRampTimeSaturatesNotOverflows) {
  UpdateRequest huge;
  auto* ramp = huge.add_deltas()->mutable_ramp();
  ramp->mutable_ramp_time()->set_seconds(std::numeric_limits<int64_t>::max());
  const absl::Status status = ValidateUpdateDeltas(huge, RampPluginRunShape(30s));
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("ramp_time"));
}

// The per-PHASE registry: a begin-phase re-stamp replaces the stale
// pre-boundary entry (the stale handle is gone, never silently reusable),
// a worker with no stamp at all fails honestly, and a run reset clears
// everything.
TEST(LiveCapabilityRegistryTest, CaptureRecapturedAtEachPhaseBegin) {
  LiveCapabilityRegistry registry;
  FakeLiveTunable handle_phase0;
  FakeLiveTunable handle_phase1;
  int phase0_applies = 0;
  int phase1_applies = 0;

  registry.Record(0, 0, &handle_phase0,
                  [&phase0_applies](Envoy::MonotonicTime, const std::string&,
                                    const LiveKnobDelta&,
                                    std::function<void(absl::Status)> stage,
                                    std::function<void(absl::Status)> callback) {
                    phase0_applies++;
                    stage(absl::OkStatus());
                    callback(absl::OkStatus());
                  },
                  [] {});
  auto cap0 = registry.CapabilityForPhase(0, 0);
  ASSERT_TRUE(cap0.ok());
  EXPECT_EQ(cap0->handle, &handle_phase0);
  EXPECT_EQ(cap0->phase_index, 0u);

  // Phase 1 begins: the worker re-stamps, and the phase-0 entry is gone.
  registry.Record(0, 1, &handle_phase1,
                  [&phase1_applies](Envoy::MonotonicTime, const std::string&,
                                    const LiveKnobDelta&,
                                    std::function<void(absl::Status)> stage,
                                    std::function<void(absl::Status)> callback) {
                    phase1_applies++;
                    stage(absl::OkStatus());
                    callback(absl::OkStatus());
                  },
                  [] {});
  const auto stale = registry.CapabilityForPhase(0, 0);
  EXPECT_FALSE(stale.ok());
  EXPECT_EQ(stale.status().code(), absl::StatusCode::kFailedPrecondition);
  const auto cap1 = registry.CapabilityForPhase(0, 1);
  ASSERT_TRUE(cap1.ok());
  EXPECT_EQ(cap1->handle, &handle_phase1);

  // The stale entry's apply surface is unreachable through the registry:
  // lookups only ever yield the current phase's capability.
  int applies = 0;
  auto current = registry.CapabilityForPhase(0, 1);
  ASSERT_TRUE(current.ok());
  LiveKnobDelta delta;
  delta.new_rps = 5;
  current->apply(std::chrono::steady_clock::now(), "epoch-1", delta,
                 [](absl::Status) {}, [&applies](absl::Status) { applies++; });
  EXPECT_EQ(applies, 1);
  EXPECT_EQ(phase0_applies, 0);
  EXPECT_EQ(phase1_applies, 1);

  // A worker that never began a phase (or was retired by the M2 subsets)
  // fails the lookup honestly.
  const auto unknown_worker = registry.CapabilityForPhase(1, 1);
  EXPECT_FALSE(unknown_worker.ok());
  EXPECT_EQ(unknown_worker.status().code(), absl::StatusCode::kNotFound);

  registry.Reset();
  EXPECT_FALSE(registry.CapabilityForPhase(0, 1).ok());
}

// The factory captures the capability handle to the INNER LinearRateLimiter,
// BEFORE ScheduledStartingRateLimiter/Bursting/DistributionSampling wrap it:
// the handle's dynamic target is the inner limiter, not any decorator, and
// applying through it re-parameterizes the inner schedule.
class SequencerCapabilityTest : public testing::Test {
public:
  Envoy::Event::SimulatedTimeSystem time_system_;
  Envoy::Stats::MockIsolatedStatsStore stats_store_;
  Envoy::Stats::Scope& stats_scope_{*stats_store_.rootScope()};
  NiceMock<Envoy::Event::MockDispatcher> dispatcher_;
  NiceMock<MockOptions> options_;
  Envoy::Api::ApiPtr api_{Envoy::Api::createApiForTest(stats_store_)};

  SequencerPtr createSequencer(LiveTunableRateLimiter** captured) {
    SequencerFactoryImpl factory(options_);
    EXPECT_CALL(dispatcher_, createTimer_(_)).Times(2);
    const SequencerTarget dummy_target = [](const CompletionCallback&) -> bool { return true; };
    return factory.create(time_system_, dispatcher_, dummy_target,
                          std::make_unique<NiceMock<MockTerminationPredicate>>(), stats_scope_,
                          time_system_.monotonicTime(), *api_, std::nullopt, captured);
  }

  static uint64_t acquireAll(RateLimiter& limiter) {
    uint64_t n = 0;
    while (limiter.tryAcquireOne()) {
      n++;
    }
    return n;
  }
};

TEST_F(SequencerCapabilityTest, CapabilityCapturedBeforeNonPluginDecoratorChain) {
  std::optional<envoy::config::core::v3::TypedExtensionConfig> no_plugin;
  EXPECT_CALL(options_, rateLimiterPluginConfig()).WillRepeatedly(ReturnRef(no_plugin));
  EXPECT_CALL(options_, requestsPerSecond()).WillRepeatedly(Return(10));
  EXPECT_CALL(options_, burstSize()).WillRepeatedly(Return(2));
  EXPECT_CALL(options_, jitterUniform()).WillRepeatedly(Return(1ns));
  EXPECT_CALL(options_, sequencerIdleStrategy())
      .WillRepeatedly(Return(nighthawk::client::SequencerIdleStrategy::POLL));

  LiveTunableRateLimiter* captured = nullptr;
  auto sequencer = createSequencer(&captured);
  ASSERT_NE(captured, nullptr);
  // The captured handle IS the inner limiter, never a decorator.
  auto* inner = dynamic_cast<LinearRateLimiter*>(captured);
  ASSERT_NE(inner, nullptr);
  EXPECT_EQ(dynamic_cast<ScheduledStartingRateLimiter*>(captured), nullptr);
  EXPECT_EQ(dynamic_cast<BurstingRateLimiter*>(captured), nullptr);
  EXPECT_EQ(dynamic_cast<DistributionSamplingRateLimiterImpl*>(captured), nullptr);

  // Start the inner schedule (the decorators hold none of the pacing state),
  // then bend through the captured handle and observe the inner schedule
  // step: half a second at 10 Hz, then a second at the new 20 Hz grid.
  uint64_t total = 0;
  for (int i = 0; i < 500; i++) {
    total += acquireAll(*inner);
    time_system_.advanceTimeWait(1ms);
  }
  ASSERT_EQ(total, 5u);
  const absl::Status status = captured->ApplyLiveRps(time_system_.monotonicTime(), 20);
  ASSERT_TRUE(status.ok()) << status.message();
  std::vector<int64_t> post_bend_acquisition_ms;
  for (int i = 0; i < 1000; i++) {
    const uint64_t n = acquireAll(*inner);
    if (n > 0) {
      post_bend_acquisition_ms.push_back(i);
    }
    total += n;
    time_system_.advanceTimeWait(1ms);
  }
  EXPECT_NEAR(total, 25, 1); // 5 conserved + ~20 at the new rate
  ASSERT_FALSE(post_bend_acquisition_ms.empty());
  EXPECT_NEAR(post_bend_acquisition_ms.front(), 25, 1); // half a new interval
  for (size_t i = 1; i < post_bend_acquisition_ms.size(); i++) {
    EXPECT_NEAR(post_bend_acquisition_ms[i] - post_bend_acquisition_ms[i - 1], 50, 1);
  }
}

TEST_F(SequencerCapabilityTest, CapabilityCapturedBeforePluginDecoratorChain) {
  std::optional<envoy::config::core::v3::TypedExtensionConfig> rate_limiter_plugin_config;
  const std::string rate_limiter_plugin_config_json =
      "{"
      "name:\"nighthawk.linear-ramping-rate-limiter-plugin\","
      "typed_config:{"
      "\"@type\":\"type.googleapis.com/"
      "nighthawk.rate_limiter.LinearRampingRateLimiterConfig\","
      "ramp_time:{seconds:5}"
      "}"
      "}";
  rate_limiter_plugin_config.emplace(envoy::config::core::v3::TypedExtensionConfig());
  Envoy::MessageUtil::loadFromJson(rate_limiter_plugin_config_json,
                                   rate_limiter_plugin_config.value(),
                                   Envoy::ProtobufMessage::getStrictValidationVisitor());
  EXPECT_CALL(options_, rateLimiterPluginConfig()).WillRepeatedly(ReturnRef(rate_limiter_plugin_config));
  EXPECT_CALL(options_, requestsPerSecond()).WillRepeatedly(Return(100));
  EXPECT_CALL(options_, noDuration()).WillRepeatedly(Return(false));
  EXPECT_CALL(options_, duration()).WillRepeatedly(Return(std::chrono::seconds(10)));
  EXPECT_CALL(options_, sequencerIdleStrategy())
      .WillRepeatedly(Return(nighthawk::client::SequencerIdleStrategy::POLL));

  LiveTunableRateLimiter* captured = nullptr;
  auto sequencer = createSequencer(&captured);
  ASSERT_NE(captured, nullptr);
  // Captured BEFORE the ScheduledStartingRateLimiter wrap: the handle is the
  // inner ramping limiter, not the decorator.
  auto* inner = dynamic_cast<LinearRampingRateLimiterImpl*>(captured);
  ASSERT_NE(inner, nullptr);
  EXPECT_EQ(dynamic_cast<ScheduledStartingRateLimiter*>(captured), nullptr);
}

// C1 superseded (M3 Task 5): the plugin-path rps knob IS ramp
// re-parameterization. Through the captured handle, an rps delta re-anchors
// the ramping limiter -- count conserved at the bend, cadence follows the new
// target -- through the same apply surface the Task 6 fan-out drives.
TEST_F(SequencerCapabilityTest, RpsDeltaOnRampPluginRunReanchors) {
  std::optional<envoy::config::core::v3::TypedExtensionConfig> rate_limiter_plugin_config;
  const std::string rate_limiter_plugin_config_json =
      "{"
      "name:\"nighthawk.linear-ramping-rate-limiter-plugin\","
      "typed_config:{"
      "\"@type\":\"type.googleapis.com/"
      "nighthawk.rate_limiter.LinearRampingRateLimiterConfig\","
      "ramp_time:{nanos:200000000}"
      "}"
      "}";
  rate_limiter_plugin_config.emplace(envoy::config::core::v3::TypedExtensionConfig());
  Envoy::MessageUtil::loadFromJson(rate_limiter_plugin_config_json,
                                   rate_limiter_plugin_config.value(),
                                   Envoy::ProtobufMessage::getStrictValidationVisitor());
  EXPECT_CALL(options_, rateLimiterPluginConfig()).WillRepeatedly(ReturnRef(rate_limiter_plugin_config));
  EXPECT_CALL(options_, requestsPerSecond()).WillRepeatedly(Return(100));
  EXPECT_CALL(options_, noDuration()).WillRepeatedly(Return(false));
  EXPECT_CALL(options_, duration()).WillRepeatedly(Return(std::chrono::seconds(10)));
  EXPECT_CALL(options_, sequencerIdleStrategy())
      .WillRepeatedly(Return(nighthawk::client::SequencerIdleStrategy::POLL));

  LiveTunableRateLimiter* captured = nullptr;
  auto sequencer = createSequencer(&captured);
  ASSERT_NE(captured, nullptr);
  auto* inner = dynamic_cast<LinearRampingRateLimiterImpl*>(captured);
  ASSERT_NE(inner, nullptr);

  // Drive the ramp a little (in lockstep with an un-bent counterfactual
  // started at the same instant) so there is live state to protect.
  LinearRampingRateLimiterImpl counterfactual(time_system_, 200ms, 100_Hz);
  uint64_t total = 0;
  uint64_t cf_total = 0;
  for (int i = 0; i < 600; i++) {
    total += acquireAll(*inner);
    cf_total += acquireAll(counterfactual);
    time_system_.advanceTimeWait(1ms);
  }
  ASSERT_GT(total, 0u);
  ASSERT_EQ(total, cf_total);

  const absl::Status status = captured->ApplyLiveRps(time_system_.monotonicTime(), 5);
  ASSERT_TRUE(status.ok()) << status.message();
  const uint64_t at_bend = total;
  ASSERT_EQ(at_bend, cf_total); // conserved at the bend

  // Post-bend: ~3 grants at the new 5 Hz, ~60 at the counterfactual's 100 Hz.
  uint64_t post_bent = 0;
  for (int i = 0; i < 600; i++) {
    post_bent += acquireAll(*inner);
    cf_total += acquireAll(counterfactual);
    time_system_.advanceTimeWait(1ms);
  }
  EXPECT_NEAR(post_bent, 3, 1);
  EXPECT_NEAR(cf_total - at_bend, 60, 2);
}

// The ramp bend observed (A2b acceptance shape, sequencer leg): a ramp-plugin
// run mid-ramp bends by COLLAPSING the ramp time; count conserved at the
// bend, and the observed cadence follows the new shape immediately (the
// snapshot/flamegraph leg of the acceptance demo is Task 6's fan-out).
TEST_F(SequencerCapabilityTest, RampBendChangesObservedRps) {
  std::optional<envoy::config::core::v3::TypedExtensionConfig> rate_limiter_plugin_config;
  const std::string rate_limiter_plugin_config_json =
      "{"
      "name:\"nighthawk.linear-ramping-rate-limiter-plugin\","
      "typed_config:{"
      "\"@type\":\"type.googleapis.com/"
      "nighthawk.rate_limiter.LinearRampingRateLimiterConfig\","
      "ramp_time:{seconds:5}"
      "}"
      "}";
  rate_limiter_plugin_config.emplace(envoy::config::core::v3::TypedExtensionConfig());
  Envoy::MessageUtil::loadFromJson(rate_limiter_plugin_config_json,
                                   rate_limiter_plugin_config.value(),
                                   Envoy::ProtobufMessage::getStrictValidationVisitor());
  EXPECT_CALL(options_, rateLimiterPluginConfig()).WillRepeatedly(ReturnRef(rate_limiter_plugin_config));
  EXPECT_CALL(options_, requestsPerSecond()).WillRepeatedly(Return(100));
  EXPECT_CALL(options_, noDuration()).WillRepeatedly(Return(false));
  EXPECT_CALL(options_, duration()).WillRepeatedly(Return(std::chrono::seconds(30)));
  EXPECT_CALL(options_, sequencerIdleStrategy())
      .WillRepeatedly(Return(nighthawk::client::SequencerIdleStrategy::POLL));

  LiveTunableRateLimiter* captured = nullptr;
  auto sequencer = createSequencer(&captured);
  ASSERT_NE(captured, nullptr);
  auto* inner = dynamic_cast<LinearRampingRateLimiterImpl*>(captured);
  ASSERT_NE(inner, nullptr);

  // 500ms into the 5s ramp, in lockstep with an un-bent counterfactual.
  LinearRampingRateLimiterImpl counterfactual(time_system_, 5s, 100_Hz);
  uint64_t total = 0;
  uint64_t cf_total = 0;
  for (int i = 0; i < 500; i++) {
    total += acquireAll(*inner);
    cf_total += acquireAll(counterfactual);
    time_system_.advanceTimeWait(1ms);
  }
  ASSERT_GT(total, 0u);
  ASSERT_EQ(total, cf_total);
  const uint64_t at_bend = total;

  // Collapse the ramp to 1s, target preserved. The re-anchor lands mid-ramp
  // under A yet inside the new ramp's segment (n = 3 <= f_B*T_B/2 = 50); the
  // forced instantaneous rate at the bend is e_b*sqrt(f_A f_B / T_A T_B) =
  // 0.5 * sqrt(100*100/(5*1)) ~= 24.5 rps (vs 10 on the way in) -- the closed
  // form the A2b note pins.
  const absl::Status status = captured->ApplyLiveRamp(
      time_system_.monotonicTime(), RampShapeChange{std::chrono::seconds(1), std::nullopt});
  ASSERT_TRUE(status.ok()) << status.message();

  // One second post-bend: the observed grants must match the SHARED math --
  // the implementation is a transcription of the A2b note's equations, so
  // the oracle is the pure library itself (plus walk quantization slack for
  // the 1ms greedy drive).
  uint64_t post_bent = 0;
  for (int i = 0; i < 1000; i++) {
    post_bent += acquireAll(*inner);
    cf_total += acquireAll(counterfactual);
    time_system_.advanceTimeWait(1ms);
  }
  EXPECT_EQ(total, at_bend); // conservation held up to the bend
  auto reanchor = RampReanchor::Reanchor({5s, 100.0}, 500ms, at_bend, {1s, 100.0});
  ASSERT_TRUE(reanchor.ok()) << reanchor.status();
  const int64_t expected_bent =
      RampReanchor::CumulativeReleases({1s, 100.0}, reanchor->new_origin_offset + 1s) -
      static_cast<int64_t>(at_bend);
  const int64_t expected_cf = RampReanchor::CumulativeReleases({5s, 100.0}, 1500ms) -
                              static_cast<int64_t>(at_bend);
  EXPECT_NEAR(post_bent, expected_bent, 2);
  EXPECT_NEAR(cf_total - at_bend, expected_cf, 2);
  EXPECT_GT(post_bent, (cf_total - at_bend) + 20); // the bend actually steepened
}

// Through-decoration semantics survive a bend: the Bursting and
// DistributionSampling decorators keep their own behavior (bursts stay
// bounded by the burst size; the delivered cumulative tracks the inner
// schedule across the bend, within the one-tick jitter slack).
TEST_F(SequencerCapabilityTest, BurstAndJitterDecoratorsUnaffected) {
  std::optional<envoy::config::core::v3::TypedExtensionConfig> no_plugin;
  EXPECT_CALL(options_, rateLimiterPluginConfig()).WillRepeatedly(ReturnRef(no_plugin));
  EXPECT_CALL(options_, requestsPerSecond()).WillRepeatedly(Return(10));
  EXPECT_CALL(options_, burstSize()).WillRepeatedly(Return(2));
  EXPECT_CALL(options_, jitterUniform()).WillRepeatedly(Return(1ns));
  EXPECT_CALL(options_, sequencerIdleStrategy())
      .WillRepeatedly(Return(nighthawk::client::SequencerIdleStrategy::POLL));

  LiveTunableRateLimiter* captured = nullptr;
  auto sequencer = createSequencer(&captured);
  ASSERT_NE(captured, nullptr);
  auto* inner = dynamic_cast<LinearRateLimiter*>(captured);
  ASSERT_NE(inner, nullptr);
  const RateLimiter& decorated = sequencer->rate_limiter();

  // Un-bent counterfactual: the same manual chain WITHOUT the bend (jitter
  // omitted: with the 1 ns upper bound the delays land inside one tick; the
  // comparisons carry that slack).
  auto cf_inner = std::make_unique<LinearRateLimiter>(time_system_, 10_Hz);
  RateLimiterPtr cf_decorated =
      std::make_unique<BurstingRateLimiter>(std::move(cf_inner), 2);

  uint64_t decorated_total = 0;
  uint64_t cf_total = 0;
  for (int i = 0; i < 500; i++) {
    decorated_total += acquireAll(const_cast<RateLimiter&>(decorated));
    cf_total += acquireAll(*cf_decorated);
    time_system_.advanceTimeWait(1ms);
  }
  ASSERT_EQ(decorated_total, cf_total); // identical pre-bend

  const absl::Status status = captured->ApplyLiveRps(time_system_.monotonicTime(), 20);
  ASSERT_TRUE(status.ok()) << status.message();

  for (int i = 0; i < 1000; i++) {
    const uint64_t n = acquireAll(const_cast<RateLimiter&>(decorated));
    // Burst semantics intact across the bend: never more than the burst size
    // delivered per tick.
    EXPECT_LE(n, 2u);
    decorated_total += n;
    cf_total += acquireAll(*cf_decorated);
    time_system_.advanceTimeWait(1ms);
  }
  // The bend reached the INNER limiter through the decorators: the delivered
  // cumulative is the conserved 5 plus the new 20 Hz second (the slack covers
  // burst boundaries and the sub-tick jitter).
  EXPECT_NEAR(decorated_total, 25, 2);
  // The unbent chain stayed at 10 Hz throughout: 5 pre-bend + 10 post.
  EXPECT_NEAR(cf_total, 15, 1);
  EXPECT_GE(decorated_total - cf_total, 8);
}

// The sequencer routes live updates through its OWN dispatcher: an immediate
// bend applies on a posted task (not on the caller's thread), a future bend
// arms a bend timer, a second bend before the first fires is rejected (not
// queued), and a null captured handle fails honestly.
// The dispatcher/timer surface under test here is real (libevent timers on a
// real event loop), so this fixture runs on REAL time: simulated time does
// not drive a real dispatcher's HRTimers, and the bend-window assertions
// carry scheduler-jitter slack.
class SequencerLiveUpdateTest : public testing::Test {
public:
  SequencerLiveUpdateTest()
      : api_(Envoy::Api::createApiForTest()),
        dispatcher_(api_->allocateDispatcher("sequencer_live_update_test")) {}

  Envoy::TimeSource& timeSource() { return api_->timeSource(); }

  // Builds a sequencer owning its inner LinearRateLimiter, wired the way the
  // factory wires it (the sequencer holds the capability handle of the
  // limiter it owns); `with_handle=false` is the honest no-capability case.
  // The factory only ever produces SequencerImpl instances (see the
  // factory's own comment); the tests call the fork-only live-update surface
  // on it directly.
  std::unique_ptr<SequencerImpl> makeSequencer(uint32_t rps, bool with_handle) {
    auto linear = std::make_unique<LinearRateLimiter>(timeSource(), Frequency(rps));
    LiveTunableRateLimiter* handle = with_handle ? linear.get() : nullptr;
    return std::make_unique<SequencerImpl>(
        platform_util_, *dispatcher_, timeSource(), std::move(linear),
        SequencerTarget([](const CompletionCallback& f) -> bool {
          f(true, true);
          return true;
        }),
        std::make_unique<StreamingStatistic>(), std::make_unique<StreamingStatistic>(),
        nighthawk::client::SequencerIdleStrategy::POLL,
        std::make_unique<NiceMock<MockTerminationPredicate>>(), stats_scope_, handle);
  }

  static uint64_t acquireAll(RateLimiter& limiter) {
    uint64_t n = 0;
    while (limiter.tryAcquireOne()) {
      n++;
    }
    return n;
  }

  // The plugin-path twin of makeSequencer: a ramping inner limiter captured
  // BEFORE the ScheduledStartingRateLimiter wrap, exactly the way the factory
  // wires it.
  std::unique_ptr<SequencerImpl> makeRampSequencer(uint32_t rps, std::chrono::nanoseconds ramp) {
    auto inner = std::make_unique<LinearRampingRateLimiterImpl>(timeSource(), ramp, Frequency(rps));
    LiveTunableRateLimiter* handle = inner.get();
    RateLimiterPtr decorated =
        std::make_unique<ScheduledStartingRateLimiter>(std::move(inner), timeSource().monotonicTime());
    return std::make_unique<SequencerImpl>(
        platform_util_, *dispatcher_, timeSource(), std::move(decorated),
        SequencerTarget([](const CompletionCallback& f) -> bool {
          f(true, true);
          return true;
        }),
        std::make_unique<StreamingStatistic>(), std::make_unique<StreamingStatistic>(),
        nighthawk::client::SequencerIdleStrategy::POLL,
        std::make_unique<NiceMock<MockTerminationPredicate>>(), stats_scope_, handle);
  }

  // Drives real time forward in `tick` steps, greedily acquiring through the
  // sequencer's limiter; returns the acquisitions granted within the window.
  static uint64_t drive(SequencerImpl& sequencer, std::chrono::milliseconds window,
                        std::chrono::milliseconds tick) {
    uint64_t total = 0;
    const auto deadline = std::chrono::steady_clock::now() + window;
    while (std::chrono::steady_clock::now() < deadline) {
      total += acquireAll(const_cast<RateLimiter&>(sequencer.rate_limiter()));
      std::this_thread::sleep_for(tick);
    }
    return total;
  }

  // Runs ready posted tasks. Bounded.
  void pump(int iterations) {
    for (int i = 0; i < iterations; i++) {
      dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
    }
  }

  void pumpUntil(const std::function<bool()>& ready) {
    for (int i = 0; i < 500 && !ready(); i++) {
      dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
      std::this_thread::sleep_for(2ms);
    }
  }

  Envoy::Stats::IsolatedStoreImpl stats_store_;
  Envoy::Stats::Scope& stats_scope_{*stats_store_.rootScope()};
  MockPlatformUtil platform_util_;
  Envoy::Api::ApiPtr api_;
  Envoy::Event::DispatcherPtr dispatcher_;
};

// Review Focus 2, completed (C1): the SAME sequencer apply surface serves
// both paths -- a plain rps delta on a non-plugin run (the linear degenerate
// re-solve) and an rps delta on a ramp-plugin run (the ramp re-anchor), each
// followed by the observed cadence of its new rate.
TEST_F(SequencerLiveUpdateTest, RpsUpdateServesPluginAndNonPluginPaths) {
  auto linear_seq = makeSequencer(10, /*with_handle=*/true);
  auto ramp_seq = makeRampSequencer(100, 200ms);

  // Half a second on both: linear 10 Hz (~5), ramp 100 Hz ramping 200ms to
  // full (f*T/2 = 10 ramp counts + 30 at the target = 40).
  uint64_t linear_total = drive(*linear_seq, 500ms, 1ms);
  uint64_t ramp_total = drive(*ramp_seq, 500ms, 1ms);
  ASSERT_GE(linear_total, 4u);
  ASSERT_GE(ramp_total, 35u);

  // The same update surface, both knobs at 5 Hz.
  absl::Status linear_status = absl::InternalError("callback not run");
  absl::Status ramp_status = absl::InternalError("callback not run");
  std::atomic<bool> linear_done{false};
  std::atomic<bool> ramp_done{false};
  linear_seq->applyLiveUpdate(timeSource().monotonicTime(),
                              LiveKnobDelta{std::uint32_t{5}, std::nullopt},
                              [&](absl::Status s) {
                                linear_status = s;
                                linear_done = true;
                              });
  ramp_seq->applyLiveUpdate(timeSource().monotonicTime(),
                            LiveKnobDelta{std::uint32_t{5}, std::nullopt},
                            [&](absl::Status s) {
                              ramp_status = s;
                              ramp_done = true;
                            });
  pumpUntil([&linear_done, &ramp_done]() {
    return linear_done.load() && ramp_done.load();
  });
  ASSERT_TRUE(linear_done.load());
  ASSERT_TRUE(ramp_done.load());
  EXPECT_TRUE(linear_status.ok()) << linear_status.message();
  EXPECT_TRUE(ramp_status.ok()) << ramp_status.message();

  // Half a second at 5 Hz through both surfaces, driven INTERLEAVED so each
  // limiter's accrual window is the same 500ms (a sequential drive would let
  // the second limiter accrue through the first's window): ~3 grants each,
  // each far below its own pre-bend rate.
  uint64_t linear_post = 0;
  uint64_t ramp_post = 0;
  const auto post_deadline = std::chrono::steady_clock::now() + 500ms;
  while (std::chrono::steady_clock::now() < post_deadline) {
    linear_post += acquireAll(const_cast<RateLimiter&>(linear_seq->rate_limiter()));
    ramp_post += acquireAll(const_cast<RateLimiter&>(ramp_seq->rate_limiter()));
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_NEAR(linear_post, 3, 1);
  EXPECT_NEAR(ramp_post, 3, 1);
}

TEST_F(SequencerLiveUpdateTest, ApplyLiveUpdatePostsToOwnDispatcherAndApplies) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  absl::Status status = absl::InternalError("callback not run");
  std::atomic<bool> done{false};
  sequencer->applyLiveUpdate(timeSource().monotonicTime(), LiveKnobDelta{std::uint32_t{20}, std::nullopt},
                             [&](absl::Status s) {
                               status = s;
                               done = true;
                             });
  pumpUntil([&done]() { return done.load(); });
  ASSERT_TRUE(done.load());
  EXPECT_TRUE(status.ok()) << status.message();

  // The inner limiter now paces at the new rate (it never started, so there
  // was no operating point to conserve: adopt-and-go). One second at 20 Hz.
  const uint64_t total = drive(*sequencer, 1000ms, 10ms);
  EXPECT_NEAR(total, 20, 2);
}

TEST_F(SequencerLiveUpdateTest, FutureBendAppliedByBendTimer) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  // Give the limiter an operating point to conserve: half a second at 10 Hz.
  const uint64_t total = drive(*sequencer, 500ms, 10ms);
  ASSERT_NEAR(total, 5, 1);

  absl::Status status = absl::InternalError("callback not run");
  std::atomic<bool> done{false};
  const Envoy::MonotonicTime t_bend = timeSource().monotonicTime() + 100ms;
  sequencer->applyLiveUpdate(t_bend, LiveKnobDelta{std::uint32_t{20}, std::nullopt}, [&](absl::Status s) {
    status = s;
    done = true;
  });
  // The posted task arms the bend timer; the callback does not run yet.
  pump(10);
  EXPECT_FALSE(done.load());
  // Time reaches the bend: the timer fires and applies.
  pumpUntil([&done]() { return done.load(); });
  ASSERT_TRUE(done.load());
  EXPECT_TRUE(status.ok()) << status.message();
  // The bend conserved the operating point and stepped to the new grid:
  // 5 conserved + ~20 over the next second.
  const uint64_t post = drive(*sequencer, 1000ms, 10ms);
  EXPECT_NEAR(total + post, 25, 3);
}

TEST_F(SequencerLiveUpdateTest, SecondBendWhilePendingRejectedNotQueued) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  const uint64_t total = drive(*sequencer, 500ms, 10ms);
  ASSERT_NEAR(total, 5, 1);

  absl::Status first_status = absl::InternalError("callback not run");
  absl::Status second_status = absl::InternalError("callback not run");
  std::atomic<bool> first_done{false};
  std::atomic<bool> second_done{false};
  const Envoy::MonotonicTime t_bend = timeSource().monotonicTime() + 100ms;
  sequencer->applyLiveUpdate(t_bend, LiveKnobDelta{std::uint32_t{20}, std::nullopt}, [&](absl::Status s) {
    first_status = s;
    first_done = true;
  });
  pump(10); // arm bend 1's timer
  sequencer->applyLiveUpdate(t_bend, LiveKnobDelta{std::uint32_t{30}, std::nullopt}, [&](absl::Status s) {
    second_status = s;
    second_done = true;
  });
  pumpUntil([&second_done]() { return second_done.load(); });
  ASSERT_TRUE(second_done.load());
  EXPECT_EQ(second_status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(second_status.message(), HasSubstr("bends are not queued"));
  EXPECT_FALSE(first_done.load());
  pumpUntil([&first_done]() { return first_done.load(); });
  ASSERT_TRUE(first_done.load());
  EXPECT_TRUE(first_status.ok()) << first_status.message();
}

// Two SEQUENTIAL bends (not bend-while-pending) on the same sequencer: the
// timer is reused, and each firing must apply its OWN bend's rate and invoke
// its OWN callback exactly once -- never a stale capture from the first
// arming.
TEST_F(SequencerLiveUpdateTest, SequentialBendsEachApplyTheirOwnRate) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  const uint64_t base = drive(*sequencer, 500ms, 10ms); // ~5 at 10 Hz
  ASSERT_NEAR(base, 5, 1);

  std::atomic<int> first_calls{0};
  std::atomic<int> second_calls{0};
  absl::Status first_status = absl::InternalError("callback not run");
  absl::Status second_status = absl::InternalError("callback not run");
  std::atomic<bool> first_done{false};
  std::atomic<bool> second_done{false};

  sequencer->applyLiveUpdate(timeSource().monotonicTime() + 100ms, LiveKnobDelta{std::uint32_t{20}, std::nullopt}, [&](absl::Status s) {
    first_calls++;
    first_status = s;
    first_done = true;
  });
  pumpUntil([&first_done]() { return first_done.load(); });
  ASSERT_TRUE(first_done.load());
  EXPECT_TRUE(first_status.ok()) << first_status.message();

  const uint64_t mid = drive(*sequencer, 500ms, 10ms); // ~10 at 20 Hz
  EXPECT_NEAR(mid, 10, 2);

  // Second sequential bend on the SAME timer object.
  sequencer->applyLiveUpdate(timeSource().monotonicTime() + 100ms, LiveKnobDelta{std::uint32_t{40}, std::nullopt}, [&](absl::Status s) {
    second_calls++;
    second_status = s;
    second_done = true;
  });
  pumpUntil([&second_done]() { return second_done.load(); });
  ASSERT_TRUE(second_done.load());
  EXPECT_TRUE(second_status.ok()) << second_status.message();

  const uint64_t last = drive(*sequencer, 1000ms, 10ms); // ~40 at 40 Hz
  EXPECT_NEAR(last, 40, 3);

  // Each bend's callback fired EXACTLY once.
  EXPECT_EQ(first_calls.load(), 1);
  EXPECT_EQ(second_calls.load(), 1);
}

TEST_F(SequencerLiveUpdateTest, NoCapturedHandleFailsHonestly) {
  auto sequencer = makeSequencer(10, /*with_handle=*/false);
  absl::Status status = absl::OkStatus();
  std::atomic<bool> done{false};
  sequencer->applyLiveUpdate(timeSource().monotonicTime(), LiveKnobDelta{std::uint32_t{20}, std::nullopt}, [&](absl::Status s) {
    status = s;
    done = true;
  });
  pumpUntil([&done]() { return done.load(); });
  ASSERT_TRUE(done.load());
  EXPECT_EQ(status.code(), absl::StatusCode::kInternal);
  EXPECT_THAT(status.message(), HasSubstr("no captured live-update capability"));
}


// ===================================================================
// M3 Task 6: the fan-out (staging confirm, ordered posts, rollback at
// t_r, honest aggregation), the knob epochs and the flush-window
// change markers.
// ===================================================================

// (T5-review carry-in): the mixed rps+ramp LiveKnobDelta composition path.
// The normalized delta applies the rps knob FIRST, then the ramp change on
// top of the resulting schedule -- observable when both knobs set the same
// sub-field: the ramp's explicit target wins, never the rps knob's value.
TEST_F(SequencerLiveUpdateTest, MixedDeltaAppliesRpsFirstThenRampOnTop) {
  auto ramp_seq = makeRampSequencer(100, 100ms);
  // Half a second: the 100ms ramp completes (~5 counts) and the schedule
  // settles at ~100 Hz in the post-ramp regime.
  const uint64_t pre = drive(*ramp_seq, 500ms, 1ms);
  ASSERT_GE(pre, 40u);

  // Mixed request {new_rps: 40, ramp: {target_rps: 90}} on a live (immediate)
  // bend. rps-first order: ApplyLiveRps(40) re-anchors at target 40, then
  // ApplyLiveRamp(target 90, ramp time preserved) re-anchors ON TOP -- the
  // final schedule targets 90 Hz. An rps-LAST order would leave 40 Hz.
  LiveKnobDelta mixed;
  mixed.new_rps = 40;
  RampShapeChange ramp_change;
  ramp_change.target_rps = 90;
  mixed.ramp = ramp_change;

  absl::Status status = absl::InternalError("callback not run");
  std::atomic<bool> done{false};
  ramp_seq->applyLiveUpdate(timeSource().monotonicTime(), mixed, [&](absl::Status s) {
    status = s;
    done = true;
  });
  pumpUntil([&done]() { return done.load(); });
  ASSERT_TRUE(done.load());
  EXPECT_TRUE(status.ok()) << status.message();

  // ~90 Hz over 500ms: ~45 grants (NOT ~20, which the wrong order gives).
  const uint64_t post = drive(*ramp_seq, 500ms, 1ms);
  EXPECT_NEAR(post, 45, 5) << "the ramp knob's explicit target must win the mixed apply";
}

// The staging surface: a FUTURE bend arms and confirms (stage callback OK)
// strictly BEFORE t_bend, while the apply callback stays silent until the
// bend fires.
TEST_F(SequencerLiveUpdateTest, StageConfirmsBeforeFutureBendApplies) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  drive(*sequencer, 500ms, 10ms);
  const Envoy::MonotonicTime t_bend = timeSource().monotonicTime() + 200ms;

  absl::Status stage_status = absl::InternalError("stage callback not run");
  absl::Status apply_status = absl::InternalError("apply callback not run");
  std::atomic<bool> staged{false};
  std::atomic<bool> applied{false};
  LiveKnobDelta delta;
  delta.new_rps = 20;
  sequencer->applyLiveUpdate(
      t_bend, delta, [&](absl::Status s) { stage_status = s; staged = true; },
      [&](absl::Status s) { apply_status = s; applied = true; });
  pump(20);
  ASSERT_TRUE(staged.load()) << "the staging confirm must fire at arming time";
  EXPECT_TRUE(stage_status.ok()) << stage_status.message();
  EXPECT_FALSE(applied.load()) << "the apply must not fire before the bend instant";
  pumpUntil([&applied]() { return applied.load(); });
  ASSERT_TRUE(applied.load());
  EXPECT_TRUE(apply_status.ok()) << apply_status.message();
}

// The staging-failure path, sequencer-side: a staged future bend that is
// cancelled before its instant never applies -- the limiter's schedule stays
// on the prior rate and the apply callback resolves cancelled.
TEST_F(SequencerLiveUpdateTest, CancelledStagedBendNeverApplies) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  const uint64_t pre = drive(*sequencer, 500ms, 10ms);
  ASSERT_NEAR(pre, 5, 1);
  const Envoy::MonotonicTime t_bend = timeSource().monotonicTime() + 200ms;

  std::atomic<bool> staged{false};
  absl::Status apply_status = absl::OkStatus();
  std::atomic<bool> applied{false};
  LiveKnobDelta delta;
  delta.new_rps = 100;
  sequencer->applyLiveUpdate(
      t_bend, delta, [&](absl::Status s) { ASSERT_TRUE(s.ok()); staged = true; },
      [&](absl::Status s) { apply_status = s; applied = true; });
  pump(20);
  ASSERT_TRUE(staged.load());
  sequencer->cancelPendingLiveUpdate();
  pump(20);

  // Drive past the (now cancelled) bend instant: still ~10 Hz, never a step
  // to 100 Hz.
  const uint64_t post = drive(*sequencer, 500ms, 10ms);
  EXPECT_NEAR(post, 5, 1) << "the cancelled bend must not have applied";
  pumpUntil([&applied]() { return applied.load(); });
  ASSERT_TRUE(applied.load());
  EXPECT_EQ(apply_status.code(), absl::StatusCode::kCancelled);
  EXPECT_THAT(apply_status.message(), HasSubstr("cancelled"));
}

// --- The process-side fan-out (LiveCapabilityFanOutApplier) against an
// injected LiveFanOutContext: deterministic ordering, staging, rollback and
// aggregation mechanics without a ProcessImpl. ---


// One fake active worker: records every apply post (forward AND rollback),
// replays configurable stage/apply results, and can defer its callbacks so a
// test controls exactly when (and at what fan-out-clock reading) they fire.
class FakeFanOutWorker {
public:
  struct ApplyRecord {
    Envoy::MonotonicTime t_bend;
    std::string epoch_id;
    LiveKnobDelta delta;
  };

  void Post(Envoy::MonotonicTime t_bend, const std::string& epoch_id, const LiveKnobDelta& delta,
            std::function<void(absl::Status)> stage_cb,
            std::function<void(absl::Status)> apply_cb, Envoy::MonotonicTime* fan_out_clock) {
    applies.push_back({t_bend, epoch_id, delta});
    pending_stage_cb_ = std::move(stage_cb);
    pending_apply_cb_ = std::move(apply_cb);
    fan_out_clock_ = fan_out_clock;
    if (!defer_stage) {
      FireStage();
    }
    if (!defer_apply && stage_ok) {
      // The sequencer fires the apply at the bend instant: simulate the
      // timer by advancing the fan-out clock to the bend (+5ms of dispatch
      // slack) before the result lands.
      if (fan_out_clock_ != nullptr) {
        *fan_out_clock_ = t_bend + std::chrono::milliseconds(5);
      }
      FireApply();
    }
  }

  void FireStage() {
    ASSERT_TRUE(pending_stage_cb_ != nullptr);
    stage_results.push_back(stage_status);
    pending_stage_cb_(stage_status);
    pending_stage_cb_ = nullptr;
  }

  void FireApply() {
    ASSERT_TRUE(pending_apply_cb_ != nullptr);
    apply_results.push_back(apply_status);
    pending_apply_cb_(apply_status);
    pending_apply_cb_ = nullptr;
  }

  // Config
  bool defer_stage = false;
  bool defer_apply = false;
  bool stage_ok = true;
  bool apply_ok = true;
  absl::Status stage_status = absl::OkStatus();
  absl::Status apply_status = absl::OkStatus();
  // Recorded
  std::vector<ApplyRecord> applies;
  std::vector<absl::Status> stage_results;
  std::vector<absl::Status> apply_results;
  int cancel_calls = 0;

private:
  std::function<void(absl::Status)> pending_stage_cb_;
  std::function<void(absl::Status)> pending_apply_cb_;
  Envoy::MonotonicTime* fan_out_clock_ = nullptr;
};

class LiveCapabilityFanOutTest : public testing::Test {
public:
  LiveCapabilityFanOutTest() : fake_now_(std::chrono::steady_clock::now()) {}

  LiveFanOutContext MakeContext() {
    LiveFanOutContext context;
    context.now = [this]() { return fake_now_; };
    context.update_admissible = [this]() { return admission_status_; };
    context.bend_instant_admissible = [this](Envoy::MonotonicTime) {
      return bend_admissible_status_;
    };
    context.current_phase_index = [this]() { return phase_index_; };
    context.active_worker_count = [this]() { return static_cast<uint32_t>(workers_.size()); };
    context.capability_for_phase = [this](int worker_id, uint32_t) -> absl::StatusOr<LiveCapabilityEntry> {
      if (missing_worker_.has_value() && worker_id == *missing_worker_) {
        return absl::NotFoundError(
            absl::StrCat("worker ", worker_id, " has no captured live-update capability"));
      }
      LiveCapabilityEntry entry;
      entry.phase_index = 0;
      entry.handle = nullptr;
      FakeFanOutWorker* worker = workers_[worker_id].get();
      const int recorded_worker_id = worker_id;
      entry.apply = [worker, recorded_worker_id, this](
                        Envoy::MonotonicTime t_bend, const std::string& epoch_id,
                        const LiveKnobDelta& delta,
                        std::function<void(absl::Status)> stage_cb,
                        std::function<void(absl::Status)> apply_cb) {
        post_order_.push_back(recorded_worker_id);
        worker->Post(t_bend, epoch_id, delta, std::move(stage_cb), std::move(apply_cb),
                     &fake_now_);
      };
      entry.cancel_live_update = [worker]() { worker->cancel_calls++; };
      return entry;
    };
    context.current_phase_schedule = [this]() { return declared_schedule_; };
    context.fan_out_begin = [this]() {
      if (!updates_enabled_) {
        return false;
      }
      in_flight_fan_outs_++;
      return true;
    };
    context.fan_out_end = [this]() { in_flight_fan_outs_--; };
    return context;
  }

  UpdateRequest RpsRequest(uint32_t rps) {
    UpdateRequest request;
    request.add_deltas()->mutable_requests_per_second()->set_value(rps);
    return request;
  }

  absl::Time Deadline() const { return absl::Now() + absl::Seconds(1); }

  // Config
  std::vector<std::unique_ptr<FakeFanOutWorker>> workers_;
  std::optional<int> missing_worker_;
  absl::Status admission_status_ = absl::OkStatus();
  absl::Status bend_admissible_status_ = absl::OkStatus();
  uint32_t phase_index_ = 0;
  LiveKnobDelta declared_schedule_;
  bool updates_enabled_ = true;
  Envoy::MonotonicTime fake_now_;
  int in_flight_fan_outs_ = 0;

  // Recorded
  std::vector<int> post_order_;
};

// Review Focus 3: the posts go to the ACTIVE workers in worker-id order,
// every worker sees the SAME agreed bend instant, and the aggregation
// resolves ONE applied epoch.
TEST_F(LiveCapabilityFanOutTest, FanOutTargetsActiveWorkersInOrderAtOneInstant) {
  for (int i = 0; i < 3; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(result.applied()) << result.rejection_reason();
  EXPECT_EQ(result.epoch_id(), "epoch-1");
  // The ack carries the bend instant's engine pair (8.5): the monotonic
  // stamp IS the agreed instant; the wall stamp follows by the lead.
  const int64_t t_bend_ns = bend_instant.monotonic_ns + kLiveUpdateBendLead.count();
  EXPECT_EQ(result.monotonic_ns(), t_bend_ns);
  EXPECT_EQ(result.wall_time_unix_ns(), bend_instant.wall_time_unix_ns + kLiveUpdateBendLead.count());

  // Worker-id-ordered posts, one per worker, one shared t_bend.
  ASSERT_EQ(post_order_.size(), 3u);
  EXPECT_EQ(post_order_, (std::vector<int>{0, 1, 2}));
  for (size_t i = 0; i < workers_.size(); i++) {
    const FakeFanOutWorker& worker = *workers_[i];
    ASSERT_EQ(worker.applies.size(), 1u);
    EXPECT_EQ(worker.applies[0].epoch_id, "epoch-1");
    EXPECT_EQ(worker.applies[0].t_bend.time_since_epoch().count(), t_bend_ns);
    ASSERT_EQ(worker.applies[0].delta.new_rps.has_value(), true);
    // Task 8a: the delta speaks GLOBAL rps -- each worker gets its share of
    // 20 across 3 active workers (base 6, remainder to the lowest indexes),
    // pinned in detail by BendDeltaRemainderGoesToLowestWorkerIndexes below.
    EXPECT_EQ(*worker.applies[0].delta.new_rps, i < 2 ? 7u : 6u);
    EXPECT_EQ(worker.cancel_calls, 0);
  }
}

// Task 8a: the knob speaks GLOBAL rps. A no-op bend AT the phase's declared
// rps must leave every worker's limiter at its phase-begin share (the total
// observed rate unchanged -- not N x the declaration), and a bend to 2x the
// declaration must exactly double the total. The division happens at the
// fan-out, mirroring the phase-begin rule.
TEST_F(LiveCapabilityFanOutTest, BendDeltaDividesAcrossWorkersLikePhaseStart) {
  for (int i = 0; i < 4; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  // The phase declared a global 8 rps; phase begin paced 2 per worker.
  declared_schedule_.new_rps = 8;
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};

  // The no-op bend: every worker keeps its phase-begin share of 2, so the
  // total observed rate is unchanged (8, never 4 x 8).
  const UpdateResult noop = applier.ApplyDeltas(RpsRequest(8), "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(noop.applied()) << noop.rejection_reason();
  uint32_t total = 0;
  for (const std::unique_ptr<FakeFanOutWorker>& worker : workers_) {
    ASSERT_EQ(worker->applies.size(), 1u);
    ASSERT_TRUE(worker->applies[0].delta.new_rps.has_value());
    EXPECT_EQ(*worker->applies[0].delta.new_rps, 2u);
    total += *worker->applies[0].delta.new_rps;
  }
  EXPECT_EQ(total, 8u) << "a no-op bend at the declared rps must not multiply the load";

  // Bend to 2x the declaration (16 global): exactly 4 per worker, 16 total.
  const EngineInstant second_instant{2000, fake_now_.time_since_epoch().count() + 1000000000};
  const UpdateResult double_bend =
      applier.ApplyDeltas(RpsRequest(16), "epoch-2", second_instant, Deadline());
  ASSERT_TRUE(double_bend.applied()) << double_bend.rejection_reason();
  total = 0;
  for (const std::unique_ptr<FakeFanOutWorker>& worker : workers_) {
    ASSERT_EQ(worker->applies.size(), 2u);
    ASSERT_TRUE(worker->applies[1].delta.new_rps.has_value());
    EXPECT_EQ(*worker->applies[1].delta.new_rps, 4u);
    total += *worker->applies[1].delta.new_rps;
  }
  EXPECT_EQ(total, 16u) << "bending to 2x the declaration must exactly double the total";
}

// The remainder goes to the LOWEST worker indexes, exactly like phase begin:
// 20 global across 3 active workers paces 7/7/6. The ramp knob's target rps
// is divided by the same rule.
TEST_F(LiveCapabilityFanOutTest, BendDeltaRemainderGoesToLowestWorkerIndexes) {
  for (int i = 0; i < 3; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(result.applied()) << result.rejection_reason();
  EXPECT_EQ(*workers_[0]->applies[0].delta.new_rps, 7u);
  EXPECT_EQ(*workers_[1]->applies[0].delta.new_rps, 7u);
  EXPECT_EQ(*workers_[2]->applies[0].delta.new_rps, 6u);

  // The ramp knob's target is a GLOBAL rate too: 100 across 3 workers is
  // 34/33/33; the ramp time (a shape, not a rate) passes through undivided.
  // One KnobDelta carries both ramp fields (the oneof must not be duplicated).
  UpdateRequest ramp_bend;
  nighthawk::client::RampShapeDelta* ramp_delta = ramp_bend.add_deltas()->mutable_ramp();
  ramp_delta->mutable_ramp_time()->set_seconds(5);
  ramp_delta->mutable_target_rps()->set_value(100);
  const EngineInstant second_instant{2000, fake_now_.time_since_epoch().count() + 1000000000};
  const UpdateResult ramp_result =
      applier.ApplyDeltas(ramp_bend, "epoch-2", second_instant, Deadline());
  ASSERT_TRUE(ramp_result.applied()) << ramp_result.rejection_reason();
  const uint32_t expected_targets[3] = {34u, 33u, 33u};
  for (size_t i = 0; i < workers_.size(); i++) {
    ASSERT_EQ(workers_[i]->applies.size(), 2u);
    ASSERT_TRUE(workers_[i]->applies[1].delta.ramp.has_value());
    ASSERT_TRUE(workers_[i]->applies[1].delta.ramp->target_rps.has_value());
    EXPECT_EQ(*workers_[i]->applies[1].delta.ramp->target_rps, expected_targets[i]);
    // The ramp time passes through undivided (identical on every worker --
    // the shape is global; only the target rps divides). NOTE: the absolute
    // value is NormalizeDelta's nanoseconds-of-proto-seconds conversion, a
    // pre-existing quirk parked out of this task's scope.
    ASSERT_TRUE(workers_[i]->applies[1].delta.ramp->ramp_time.has_value());
    EXPECT_EQ(*workers_[i]->applies[1].delta.ramp->ramp_time,
              *workers_[0]->applies[1].delta.ramp->ramp_time);
  }
}

// A single-worker phase is the identity: the declared global rate IS the
// worker's rate.
TEST_F(LiveCapabilityFanOutTest, BendDeltaSingleWorkerPhaseIsIdentity) {
  workers_.push_back(std::make_unique<FakeFanOutWorker>());
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(result.applied()) << result.rejection_reason();
  ASSERT_EQ(workers_[0]->applies.size(), 1u);
  ASSERT_TRUE(workers_[0]->applies[0].delta.new_rps.has_value());
  EXPECT_EQ(*workers_[0]->applies[0].delta.new_rps, 20u);
}

// The staging-confirm ruling (the PRIMARY failure path): a worker that
// cannot stage fails the update with nothing applied anywhere -- the workers
// that staged OK get their armed bends cancelled, and no apply ever fires.
TEST_F(LiveCapabilityFanOutTest, StagingFailureLeavesNothingApplied) {
  for (int i = 0; i < 3; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
    // The applies are deferred on every worker: in the real engine the bend
    // timer fires at t_bend, and a staged bend that gets cancelled must
    // never reach that fire. The fakes therefore keep their applies unfired
    // unless the test releases them.
    workers_.back()->defer_apply = true;
  }
  workers_[1]->stage_ok = false;
  workers_[1]->stage_status = absl::InvalidArgumentError(
      "a live-update bend is already pending for this worker; bends are not queued");
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("worker 1"));
  EXPECT_THAT(result.rejection_reason(), HasSubstr("bends are not queued"));
  EXPECT_THAT(result.rejection_reason(), HasSubstr("nothing was applied"));
  // The staged workers' bends were cancelled; no apply ever fired anywhere.
  EXPECT_EQ(workers_[0]->cancel_calls, 1);
  EXPECT_EQ(workers_[2]->cancel_calls, 1);
  EXPECT_EQ(workers_[1]->cancel_calls, 1);
  for (const std::unique_ptr<FakeFanOutWorker>& worker : workers_) {
    EXPECT_TRUE(worker->apply_results.empty()) << "nothing may apply after a staging failure";
  }
}

// The residual post-bend failure path: one worker's apply fails AT the bend;
// every worker that DID apply is rolled back by a re-anchored re-apply of
// the PRIOR schedule AT THE ROLLBACK INSTANT t_r (Q7's ratified form: t_r is
// the failure handling instant, strictly after t_bend -- never a naive
// re-apply at t_bend), carrying the PRIOR epoch id; the ack reports
// applied=false with the reason, and the failed worker (never mutated) is
// not posted.
TEST_F(LiveCapabilityFanOutTest, PartialFanOutRollsBackHonestlyAtTR) {
  for (int i = 0; i < 2; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  declared_schedule_.new_rps = 10; // the phase's declared schedule
  workers_[1]->apply_ok = false;
  workers_[1]->apply_status = absl::InternalError("limiter cannot honor the delta");
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("worker 1"));
  EXPECT_THAT(result.rejection_reason(), HasSubstr("limiter cannot honor the delta"));
  EXPECT_THAT(result.rejection_reason(), HasSubstr("rolled back"));

  // Worker 0: forward apply at the shared t_bend, then the rollback re-apply
  // at a strictly later rollback instant, restoring the PRIOR schedule and
  // the PRIOR (empty) epoch id.
  ASSERT_EQ(workers_[0]->applies.size(), 2u);
  const int64_t t_bend_ns = bend_instant.monotonic_ns + kLiveUpdateBendLead.count();
  EXPECT_EQ(workers_[0]->applies[0].t_bend.time_since_epoch().count(), t_bend_ns);
  const FakeFanOutWorker::ApplyRecord& rollback = workers_[0]->applies[1];
  EXPECT_GT(rollback.t_bend.time_since_epoch().count(), t_bend_ns)
      << "the rollback re-apply must run at t_r > t_bend, never a naive re-apply at the bend";
  ASSERT_TRUE(rollback.delta.new_rps.has_value());
  // The tracked prior schedule is the declared GLOBAL rps; the rollback
  // re-apply carries each worker's share of it (10 across 2 workers), the
  // same division the forward apply used (Task 8a) -- never the full global
  // value re-applied per worker (the M2-burstfix multiplication class).
  EXPECT_EQ(*rollback.delta.new_rps, 5u) << "the PRIOR schedule is restored";
  EXPECT_EQ(rollback.epoch_id, "") << "the prior (empty) epoch id is restored";

  // Worker 1 never applied: no rollback post (it is already on the prior
  // schedule), and no staged bend to cancel.
  EXPECT_EQ(workers_[1]->applies.size(), 1u);
  EXPECT_EQ(workers_[1]->cancel_calls, 0);
}

// Sequential bends: a second applied bend updates the rollback prior-state,
// and a failure of the second bend rolls back to the FIRST bend's schedule
// and epoch -- the tracker follows the applied history, per phase.
TEST_F(LiveCapabilityFanOutTest, SequentialBendsRollBackToPreviousScheduleAndEpoch) {
  for (int i = 0; i < 2; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  declared_schedule_.new_rps = 10;
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult first = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(first.applied()) << first.rejection_reason();

  // The second bend fails on worker 1 at its bend instant.
  workers_[1]->apply_ok = false;
  workers_[1]->apply_status = absl::InternalError("boom");
  const EngineInstant second_instant{2000, fake_now_.time_since_epoch().count() + 1000000000};
  const UpdateResult second =
      applier.ApplyDeltas(RpsRequest(30), "epoch-2", second_instant, Deadline());
  ASSERT_FALSE(second.applied());
  // The rollback restores the FIRST bend's state: schedule 20 global
  // (epoch-1), each worker re-applied its own share of it (Task 8a).
  ASSERT_EQ(workers_[0]->applies.size(), 3u);
  const FakeFanOutWorker::ApplyRecord& rollback = workers_[0]->applies[2];
  EXPECT_EQ(rollback.epoch_id, "epoch-1");
  EXPECT_EQ(*rollback.delta.new_rps, 10u);
}

// The rollback prior-state for a MIXED applied bend carries the full prior
// schedule: the changed knob's prior value AND the untouched knob's
// (tracker-preserved) value.
TEST_F(LiveCapabilityFanOutTest, MixedBendRollbackCarriesFullPriorSchedule) {
  for (int i = 0; i < 2; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  // A plugin-style declared schedule: ramp time 1s, target 100.
  RampShapeChange declared_ramp;
  declared_ramp.ramp_time = std::chrono::seconds(1);
  declared_ramp.target_rps = 100;
  declared_schedule_.ramp = declared_ramp;

  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  // Apply a mixed bend {new_rps: 20, ramp: {target_rps: 90}} -- succeeds.
  UpdateRequest mixed;
  mixed.add_deltas()->mutable_requests_per_second()->set_value(20);
  mixed.add_deltas()->mutable_ramp()->mutable_target_rps()->set_value(90);
  const UpdateResult first = applier.ApplyDeltas(mixed, "epoch-1", bend_instant, Deadline());
  ASSERT_TRUE(first.applied()) << first.rejection_reason();

  // A follow-up rps-only bend fails post-bend ON WORKER 0 (worker 1 applies
  // it and is rolled back too); the rollback must restore the FULL prior
  // schedule: rps 20 AND the ramp {target 90, ramp time 1s preserved through
  // the applied bend}.
  workers_[0]->apply_ok = false;
  workers_[0]->apply_status = absl::InternalError("boom");
  const UpdateResult second = applier.ApplyDeltas(RpsRequest(30), "epoch-2", bend_instant, Deadline());
  ASSERT_FALSE(second.applied());
  // Worker 0: mixed forward apply, failed forward apply -- and NO rollback
  // post (it never mutated, so it is already on the prior schedule).
  ASSERT_EQ(workers_[0]->applies.size(), 2u);
  // Worker 1 applied both bends: mixed forward, rps forward, rollback
  // re-apply -- the rollback must carry the FULL prior schedule.
  ASSERT_EQ(workers_[1]->applies.size(), 3u);
  const FakeFanOutWorker::ApplyRecord& rollback = workers_[1]->applies[2];
  ASSERT_TRUE(rollback.delta.new_rps.has_value());
  // The FULL prior schedule is restored, in each worker's share of the
  // global values (Task 8a): 20 -> 10, 90 -> 45 across 2 workers; the ramp
  // time passes through undivided.
  EXPECT_EQ(*rollback.delta.new_rps, 10u);
  ASSERT_TRUE(rollback.delta.ramp.has_value());
  ASSERT_TRUE(rollback.delta.ramp->target_rps.has_value());
  EXPECT_EQ(*rollback.delta.ramp->target_rps, 45u);
  ASSERT_TRUE(rollback.delta.ramp->ramp_time.has_value());
  EXPECT_EQ(*rollback.delta.ramp->ramp_time, std::chrono::seconds(1));
}

// The honest no-handle case (the M2 registry ruling): a stale/missing
// capability fails the update at LOOKUP, before any post.
TEST_F(LiveCapabilityFanOutTest, FanOutToWorkerWithoutCurrentPhaseHandleFailsHonestly) {
  for (int i = 0; i < 2; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  missing_worker_ = 1;
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("worker 1"));
  EXPECT_THAT(result.rejection_reason(), HasSubstr("no captured live-update capability"));
  EXPECT_TRUE(post_order_.empty()) << "a lookup failure must fail the fan-out before any post";
}

// A bend instant that would land past the current phase's end is rejected
// pre-fan-out.
TEST_F(LiveCapabilityFanOutTest, BendInstantPastPhaseEndRejected) {
  workers_.push_back(std::make_unique<FakeFanOutWorker>());
  bend_admissible_status_ = absl::FailedPreconditionError(
      "the bend instant would land 300ms past the current phase's end; a bend "
      "is never applied across a phase boundary");
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("past the current phase's end"));
  EXPECT_TRUE(post_order_.empty());
}

// An update arriving inside the boundary-drain window is rejected with a
// clear reason, pre-fan-out.
TEST_F(LiveCapabilityFanOutTest, UpdateDuringBoundaryDrainRejected) {
  workers_.push_back(std::make_unique<FakeFanOutWorker>());
  admission_status_ = absl::FailedPreconditionError(
      "the run is inside a phase boundary drain; live updates are rejected "
      "there");
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("boundary drain"));
  EXPECT_TRUE(post_order_.empty());
}

// Task 4 Q4's ruling enforced at the fan-out: a second update arriving
// before the first resolves is rejected, never queued.
TEST_F(LiveCapabilityFanOutTest, SecondUpdateWhileInFlightRejectedNotQueued) {
  for (int i = 0; i < 2; i++) {
    workers_.push_back(std::make_unique<FakeFanOutWorker>());
  }
  workers_[0]->defer_apply = true;
  workers_[1]->defer_apply = true;
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};

  // The first update stalls in its apply wait (deferred callbacks).
  std::atomic<bool> first_done{false};
  UpdateResult first_result;
  std::thread first_thread([&]() {
    first_result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
    first_done = true;
  });
  // Wait for the first fan-out to be registered and staged.
  for (int i = 0; i < 500 && workers_[1]->applies.empty(); i++) {
    std::this_thread::sleep_for(2ms);
  }
  ASSERT_FALSE(first_done.load());

  // The second update while the first is in flight: honest rejection, and
  // no NEW post to any worker.
  const size_t posts_before = post_order_.size();
  const UpdateResult second =
      applier.ApplyDeltas(RpsRequest(30), "epoch-2", bend_instant, Deadline());
  ASSERT_FALSE(second.applied());
  EXPECT_THAT(second.rejection_reason(), HasSubstr("bends are not queued"));
  EXPECT_EQ(post_order_.size(), posts_before)
      << "the rejected second update must not have posted to any worker";

  // Release the first: its staged bends resolve through the deadline path --
  // the deferred applies fire here, completing the first update applied.
  workers_[0]->FireApply();
  workers_[1]->FireApply();
  first_thread.join();
  EXPECT_TRUE(first_result.applied()) << first_result.rejection_reason();
}

// The shutdown fence: once the fence refuses, the update resolves honestly
// with no post to any worker.
TEST_F(LiveCapabilityFanOutTest, FanOutRefusedWhenUpdatesDisabled) {
  workers_.push_back(std::make_unique<FakeFanOutWorker>());
  updates_enabled_ = false;
  LiveCapabilityFanOutApplier applier(MakeContext());
  const EngineInstant bend_instant{1000, fake_now_.time_since_epoch().count()};
  const UpdateResult result = applier.ApplyDeltas(RpsRequest(20), "epoch-1", bend_instant, Deadline());
  ASSERT_FALSE(result.applied());
  EXPECT_THAT(result.rejection_reason(), HasSubstr("shutting down"));
  EXPECT_TRUE(post_order_.empty());
  EXPECT_EQ(in_flight_fan_outs_, 0) << "the fence registration must always pair";
}

} // namespace
} // namespace Client
} // namespace Nighthawk
