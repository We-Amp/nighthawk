// FORK-ONLY (We-Amp): LiveUpdateOrchestrator unit tests (M3 Task 1). The
// service-level routing tests live in service_test.cc; these pin the
// orchestrator contract directly, including the M4 threading properties
// (bounded ack wait off the dispatching thread; no ack after the run ended or
// a cancellation was accepted) with precise fake-level observability.

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

#include "api/client/service.pb.h"

#include "source/client/live_update_orchestrator.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

using nighthawk::client::UpdateRequest;
using nighthawk::client::UpdateResult;

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

std::unique_ptr<LiveUpdateOrchestrator> MakeOrchestrator(
    LiveUpdateOrchestrator::Applier* applier, AckRecorder* recorder,
    const std::string& execution_id = "") {
  return std::make_unique<LiveUpdateOrchestrator>(
      applier, recorder->writer(), []() {}, []() { return std::string("current-phase"); },
      execution_id, absl::Seconds(1));
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
  UpdateResult result = applier->ApplyDeltas(MakeDeltasUpdate(), "epoch-1",
                                             DefaultEngineInstant(), absl::InfiniteFuture());
  EXPECT_FALSE(result.applied());
  EXPECT_FALSE(result.rejection_reason().empty());
}

} // namespace
} // namespace Client
} // namespace Nighthawk
