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
// order), for both knobs. (Distinct knobs in one request: while C1's interim
// rule holds an rps+ramp mix can never PASS -- rps only passes on non-plugin
// runs and ramp only on plugin runs -- so no distinct-knob acceptance test is
// possible until Task 5 lands plugin-path rps; the mix's impossibility is
// pinned by RpsOnRampPluginRunIsRampShapeValidation and RampWithoutPluginRejected.)
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

// --- M3 Task 3: capability capture, registry, dispatcher-posted apply ---

// A minimal capability stand-in for registry tests.
class FakeLiveTunable : public LiveTunableRateLimiter {
public:
  absl::Status ApplyLiveRps(Envoy::MonotonicTime /*t_bend*/, uint32_t /*new_rps*/) override {
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
                  [&phase0_applies](Envoy::MonotonicTime, uint32_t,
                                    std::function<void(absl::Status)> callback) {
                    phase0_applies++;
                    callback(absl::OkStatus());
                  });
  auto cap0 = registry.CapabilityForPhase(0, 0);
  ASSERT_TRUE(cap0.ok());
  EXPECT_EQ(cap0->handle, &handle_phase0);
  EXPECT_EQ(cap0->phase_index, 0u);

  // Phase 1 begins: the worker re-stamps, and the phase-0 entry is gone.
  registry.Record(0, 1, &handle_phase1,
                  [&phase1_applies](Envoy::MonotonicTime, uint32_t,
                                    std::function<void(absl::Status)> callback) {
                    phase1_applies++;
                    callback(absl::OkStatus());
                  });
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
  current->apply(std::chrono::steady_clock::now(), 5,
                 [&applies](absl::Status) { applies++; });
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

// CAPTURE-ONLY interim (C1): applying an rps delta through the plugin-path
// captured handle rejects with the honest `pending ramp re-anchoring` reason
// and never touches the limiter.
TEST_F(SequencerCapabilityTest, RpsDeltaOnRampPluginRunPendingRejection) {
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
  EXPECT_EQ(status.code(), absl::StatusCode::kUnimplemented);
  EXPECT_THAT(status.message(), HasSubstr("pending ramp re-anchoring"));

  // Untouched: the limiter still matches the counterfactual exactly.
  for (int i = 0; i < 600; i++) {
    total += acquireAll(*inner);
    cf_total += acquireAll(counterfactual);
    time_system_.advanceTimeWait(1ms);
  }
  EXPECT_EQ(total, cf_total);
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

TEST_F(SequencerLiveUpdateTest, ApplyLiveUpdatePostsToOwnDispatcherAndApplies) {
  auto sequencer = makeSequencer(10, /*with_handle=*/true);
  absl::Status status = absl::InternalError("callback not run");
  std::atomic<bool> done{false};
  sequencer->applyLiveUpdate(timeSource().monotonicTime(), 20,
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
  sequencer->applyLiveUpdate(t_bend, 20, [&](absl::Status s) {
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
  sequencer->applyLiveUpdate(t_bend, 20, [&](absl::Status s) {
    first_status = s;
    first_done = true;
  });
  pump(10); // arm bend 1's timer
  sequencer->applyLiveUpdate(t_bend, 30, [&](absl::Status s) {
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

TEST_F(SequencerLiveUpdateTest, NoCapturedHandleFailsHonestly) {
  auto sequencer = makeSequencer(10, /*with_handle=*/false);
  absl::Status status = absl::OkStatus();
  std::atomic<bool> done{false};
  sequencer->applyLiveUpdate(timeSource().monotonicTime(), 20, [&](absl::Status s) {
    status = s;
    done = true;
  });
  pumpUntil([&done]() { return done.load(); });
  ASSERT_TRUE(done.load());
  EXPECT_EQ(status.code(), absl::StatusCode::kInternal);
  EXPECT_THAT(status.message(), HasSubstr("no captured live-update capability"));
}

} // namespace
} // namespace Client
} // namespace Nighthawk
