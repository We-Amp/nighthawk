#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "envoy/common/exception.h"
#include "envoy/upstream/cluster_manager.h"

#include "nighthawk/user_defined_output/user_defined_output_plugin.h"

#include "source/common/common/random_generator.h"
#include "source/common/frequency.h"
#include "source/common/platform_util_impl.h"
#include "source/common/rate_limiter_impl.h"
#include "source/common/runtime/runtime_impl.h"
#include "source/common/sequencer_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "test/mocks/local_info/mocks.h"
#include "test/mocks/protobuf/mocks.h"
#include "test/mocks/thread_local/mocks.h"
#include "test/test_common/simulated_time_system.h"

#include "source/client/client_worker_impl.h"
#include "source/common/statistic_impl.h"

#include "test/mocks/client/mock_benchmark_client.h"
#include "test/mocks/client/mock_benchmark_client_factory.h"
#include "test/mocks/common/mock_request_source.h"
#include "test/mocks/common/mock_request_source_factory.h"
#include "test/mocks/common/mock_sequencer.h"
#include "test/mocks/common/mock_sequencer_factory.h"
#include "test/mocks/common/mock_termination_predicate.h"
#include "test/mocks/common/mock_termination_predicate_factory.h"

#include "gtest/gtest.h"

using namespace testing;
using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {

namespace {
using EnvoyException = Envoy::EnvoyException;
} // namespace

class ClientWorkerTest : public Test {
public:
  ClientWorkerTest()
      : api_(Envoy::Api::createApiForTest()), thread_id_(std::this_thread::get_id()) {
    absl::StatusOr<Envoy::Runtime::LoaderPtr> loader = Envoy::Runtime::LoaderImpl::create(
        dispatcher_, tls_, {}, local_info_, store_, rand_, validation_visitor_, *api_);

    THROW_IF_NOT_OK(loader.status());
    loader_ = *std::move(loader);

    benchmark_client_ = new MockBenchmarkClient();
    sequencer_ = new MockSequencer();
    request_generator_ = new MockRequestSource();

    EXPECT_CALL(benchmark_client_factory_, create(_, _, _, _, _, _, _, _, _))
        .Times(1)
        .WillOnce(Return(ByMove(std::unique_ptr<BenchmarkClient>(benchmark_client_))));

    EXPECT_CALL(sequencer_factory_, create(_, _, _, _, _, _, _, _, _))
        .Times(1)
        .WillOnce(Return(ByMove(std::unique_ptr<Sequencer>(sequencer_))));

    EXPECT_CALL(request_generator_factory_, create(_, _, _, _))
        .Times(1)
        .WillOnce(Return(ByMove(std::unique_ptr<RequestSource>(request_generator_))));
    EXPECT_CALL(*request_generator_, initOnThread());
    EXPECT_CALL(*request_generator_, destroyOnThread());

    EXPECT_CALL(termination_predicate_factory_, create(_, _, _, _))
        .WillOnce(Return(ByMove(createMockTerminationPredicate())));
  }

  StatisticPtrMap createStatisticPtrMap() const {
    StatisticPtrMap map;
    map["foo1"] = &statistic_;
    map["foo2"] = &statistic_;
    return map;
  }

  bool CheckThreadChanged(const CompletionCallback&) {
    EXPECT_NE(thread_id_, std::this_thread::get_id());
    return false;
  }

  TerminationPredicatePtr createMockTerminationPredicate() {
    auto predicate = std::make_unique<NiceMock<MockTerminationPredicate>>();
    ON_CALL(*predicate, appendToChain(_)).WillByDefault(ReturnRef(*predicate));
    return predicate;
  }

  StreamingStatistic statistic_;
  Envoy::Api::ApiPtr api_;
  std::thread::id thread_id_;
  MockBenchmarkClientFactory benchmark_client_factory_;
  MockTerminationPredicateFactory termination_predicate_factory_;
  MockSequencerFactory sequencer_factory_;
  MockRequestSourceFactory request_generator_factory_;
  Envoy::Stats::IsolatedStoreImpl store_;
  NiceMock<Envoy::ThreadLocal::MockInstance> tls_;
  Envoy::Event::SimulatedTimeSystem time_system_;
  MockBenchmarkClient* benchmark_client_;
  MockSequencer* sequencer_;
  MockRequestSource* request_generator_;
  Envoy::Random::RandomGeneratorImpl rand_;
  NiceMock<Envoy::Event::MockDispatcher> dispatcher_;
  Envoy::Runtime::LoaderPtr loader_;
  NiceMock<Envoy::LocalInfo::MockLocalInfo> local_info_;
  NiceMock<Envoy::ProtobufMessage::MockValidationVisitor> validation_visitor_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_ptr_;
  Envoy::Tracing::TracerSharedPtr tracer_;
};

TEST_F(ClientWorkerTest, BasicTest) {
  ASSERT_EQ(std::this_thread::get_id(), thread_id_);

  {
    InSequence dummy;
    EXPECT_CALL(*benchmark_client_, setShouldMeasureLatencies(false));
    EXPECT_CALL(*benchmark_client_, tryStartRequest(_))
        .WillOnce(Invoke(this, &ClientWorkerTest::CheckThreadChanged));
    EXPECT_CALL(*benchmark_client_, setShouldMeasureLatencies(true));
    EXPECT_CALL(*sequencer_, start);
    EXPECT_CALL(*sequencer_, waitForCompletion);
    EXPECT_CALL(*benchmark_client_, terminate());
  }
  int worker_number = 12345;

  std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins;

  auto worker = std::make_unique<ClientWorkerImpl>(
      *api_, tls_, cluster_manager_ptr_, benchmark_client_factory_, termination_predicate_factory_,
      sequencer_factory_, request_generator_factory_, store_, worker_number,
      time_system_.monotonicTime(), tracer_, ClientWorkerImpl::HardCodedWarmupStyle::ON,
      std::move(user_defined_output_plugins));

  worker->start();
  worker->waitForCompletion();

  EXPECT_CALL(*benchmark_client_, statistics()).WillOnce(Return(createStatisticPtrMap()));
  EXPECT_CALL(*sequencer_, statistics()).WillOnce(Return(createStatisticPtrMap()));

  auto statistics = worker->statistics();
  EXPECT_EQ(2, statistics.size());
  std::vector<nighthawk::client::UserDefinedOutput> mocked_vector{};
  EXPECT_CALL(*benchmark_client_, getUserDefinedOutputResults()).WillOnce(Return(mocked_vector));
  std::vector<nighthawk::client::UserDefinedOutput> outputs = worker->getUserDefinedOutputResults();
  EXPECT_TRUE(outputs.empty());

  worker->shutdown();
}

// ---------------------------------------------------------------------------
// FORK-ONLY (We-Amp): censored-boundary straggler race fix round.
// ---------------------------------------------------------------------------

namespace {

// A termination predicate whose verdict the test thread flips at will: the
// sequencer under test keeps running (PROCEED) until the test decides its
// boundary is over, which is what lets the test hold a straggler's sequencer
// stop until AFTER the successor phase's begin.
class ControllableTerminationPredicate : public TerminationPredicate {
public:
  explicit ControllableTerminationPredicate(std::shared_ptr<std::atomic<int>> status)
      : status_(std::move(status)) {}
  TerminationPredicate& link(TerminationPredicatePtr&&) override { return *this; }
  TerminationPredicate& appendToChain(TerminationPredicatePtr&&) override { return *this; }
  Status evaluateChain() override { return evaluate(); }
  Status evaluate() override { return static_cast<Status>(status_->load()); }

private:
  std::shared_ptr<std::atomic<int>> status_;
};

// Hands every begin-phase a controllable predicate and remembers the status
// cells by creation order (phase 0's predicate is cell 0, and so on).
class ControllableTerminationPredicateFactory : public TerminationPredicateFactory {
public:
  TerminationPredicatePtr
  create(Envoy::TimeSource&, Envoy::Stats::Scope&, const Envoy::MonotonicTime,
         std::optional<std::chrono::seconds> /*duration_override*/) const override {
    auto status = std::make_shared<std::atomic<int>>(
        static_cast<int>(TerminationPredicate::Status::PROCEED));
    std::lock_guard<std::mutex> lock(mutex_);
    statuses_.push_back(status);
    return std::make_unique<ControllableTerminationPredicate>(status);
  }

  size_t created() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return statuses_.size();
  }

  void terminate(size_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    *statuses_.at(index) = static_cast<int>(TerminationPredicate::Status::TERMINATE);
  }

private:
  mutable std::mutex mutex_;
  mutable std::vector<std::shared_ptr<std::atomic<int>>> statuses_;
};

// Produces real SequencerImpl instances (the worker's begin-phase requires
// it: the dynamic cast to SequencerImpl is a RELEASE_ASSERT) at a quiet 1 Hz
// with POLL idle strategy, so the worker's real dispatcher thread stays
// event-driven and quiet between the test's steering steps.
class QuietSequencerFactory : public SequencerFactory {
public:
  SequencerPtr create(Envoy::TimeSource& time_source, Envoy::Event::Dispatcher& dispatcher,
                      const SequencerTarget& sequencer_target,
                      TerminationPredicatePtr&& termination_predicate, Envoy::Stats::Scope& scope,
                      const Envoy::MonotonicTime, Envoy::Api::Api&,
                      std::optional<uint32_t> requests_per_second_override,
                      LiveTunableRateLimiter** captured_live_tunable) const override {
    if (captured_live_tunable != nullptr) {
      *captured_live_tunable = nullptr;
    }
    auto linear = std::make_unique<LinearRateLimiter>(
        time_source, Frequency(requests_per_second_override.value_or(1)));
    return std::make_unique<SequencerImpl>(
        platform_util_, dispatcher, time_source, std::move(linear), sequencer_target,
        std::make_unique<StreamingStatistic>(), std::make_unique<StreamingStatistic>(),
        nighthawk::client::SequencerIdleStrategy::POLL, std::move(termination_predicate), scope,
        /*live_tunable_rate_limiter=*/nullptr);
  }

private:
  PlatformUtilImpl platform_util_;
};

} // namespace

// A phase-driven worker on its real dispatcher thread, with real sequencers
// whose termination the test controls.
class ClientWorkerPhaseTest : public Test {
public:
  ClientWorkerPhaseTest() : api_(Envoy::Api::createApiForTest()) {
    // Nice: the phase path touches several benchmark-client surfaces this
    // fixture does not constrain (statistics slabs, gates, latency switches);
    // the runner escalates uninteresting-call warnings to failures.
    benchmark_client_ = new NiceMock<MockBenchmarkClient>();
    request_generator_ = new NiceMock<MockRequestSource>();

    EXPECT_CALL(benchmark_client_factory_, create(_, _, _, _, _, _, _, _, _))
        .WillOnce(Return(ByMove(std::unique_ptr<BenchmarkClient>(benchmark_client_))));
    EXPECT_CALL(request_generator_factory_, create(_, _, _, _))
        .WillOnce(Return(ByMove(std::unique_ptr<RequestSource>(request_generator_))));
    EXPECT_CALL(*request_generator_, initOnThread());
    EXPECT_CALL(*request_generator_, destroyOnThread());
  }

  NiceMock<MockBenchmarkClient>* benchmark_client_;
  NiceMock<MockRequestSource>* request_generator_;
  ControllableTerminationPredicateFactory predicate_factory_;
  QuietSequencerFactory sequencer_factory_;
  MockBenchmarkClientFactory benchmark_client_factory_;
  MockRequestSourceFactory request_generator_factory_;
  Envoy::Stats::IsolatedStoreImpl store_;
  NiceMock<Envoy::ThreadLocal::MockInstance> tls_;
  Envoy::Api::ApiPtr api_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_ptr_;
  Envoy::Tracing::TracerSharedPtr tracer_;
};

// The censored-boundary straggler race. beginPhaseAsync swaps the worker's
// drain promise on the ORCHESTRATOR thread, and a censored boundary's
// collectDrainAcks returns on a pure wait_for timeout -- no synchronization
// to a straggler whose sequencer stops only later. The straggler's late stop
// hook then armed its boundary drain watch FROM THE drain_promise_ MEMBER,
// reading whatever the successor phase had just written there: at best a
// torn/stale read (a genuine data race, TSan territory), at worst the
// straggler's hung drain resolving the SUCCESSOR phase's ack and corrupting
// the successor's drain accounting.
//
// Deterministic behavioral pin on the real worker dispatcher thread: the
// straggler's sequencer is held open past the successor's begin, then stops;
// its hung in-flight request completes. The straggler's own ack must
// resolve, and the successor's ack must not have been pre-satisfied by it.
TEST_F(ClientWorkerPhaseTest, StragglerStopAfterNextBeginCompletesOnlyItsOwnAck) {
  ClientWorkerImpl worker(*api_, tls_, cluster_manager_ptr_, benchmark_client_factory_,
                          predicate_factory_, sequencer_factory_, request_generator_factory_,
                          store_, 0, api_->timeSource().monotonicTime(), tracer_,
                          ClientWorkerImpl::HardCodedWarmupStyle::OFF, {}, {},
                          /*live_snapshot_interval=*/{}, /*phase_driven_run=*/true);
  std::atomic<uint64_t> in_flight{1}; // phase 0's hung request
  // Read the live count at call time (a captured Return(value) would freeze
  // it at setup, and the drain watches would never observe the release).
  ON_CALL(*benchmark_client_, inFlightRequests())
      .WillByDefault(Invoke([&in_flight]() { return in_flight.load(); }));
  // The sequencer targets the benchmark client; refusing every start keeps
  // the run quiet while the test steers termination.
  ON_CALL(*benchmark_client_, tryStartRequest(_)).WillByDefault(Return(false));

  worker.start();
  ASSERT_EQ(std::future_status::ready, worker.readyFuture().wait_for(std::chrono::seconds(5)));

  nighthawk::client::PhaseSpec phase;
  phase.set_name("phase");
  phase.set_requests_per_second(1);
  phase.mutable_duration()->set_seconds(1);

  // Phase 0 begins; the test holds its sequencer open by keeping its
  // termination predicate at PROCEED (the hung in-flight request stands in
  // for a drain that never completes).
  auto future0 = worker.beginPhaseAsync(0, phase, /*final_phase=*/false);
  // The censored boundary: the orchestrator gives up on future0 without an
  // ack and begins the successor.
  auto future1 = worker.beginPhaseAsync(1, phase, /*final_phase=*/false);
  // Wait until the successor's begin-phase actually ran on the worker
  // dispatcher (its termination predicate was created), so the straggler
  // stop below lands after the successor's begin, deterministically.
  const auto wait_until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (predicate_factory_.created() < 2 && std::chrono::steady_clock::now() < wait_until) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GE(predicate_factory_.created(), 2u);

  // The straggler stops -- AFTER the successor's beginPhaseAsync wrote the
  // successor's drain promise.
  predicate_factory_.terminate(0);
  // The hung request completes; the straggler's boundary drain watch fires.
  in_flight.store(0);

  // The straggler's late stop must have armed a watch holding its OWN
  // boundary's promise: the successor's ack must not be pre-satisfied.
  EXPECT_EQ(std::future_status::timeout, future1.wait_for(std::chrono::milliseconds(100)))
      << "the straggler's late stop corrupted the successor's drain ack";
  // The straggler's OWN boundary ack resolves with a real ack value (not a
  // broken-promise placeholder -- its promise must still be alive, held by
  // the boundary it belongs to).
  ASSERT_EQ(std::future_status::ready, future0.wait_for(std::chrono::seconds(5)));
  EXPECT_GE(future0.get().sequencer_stop_monotonic_ns, 0);

  // The successor's lifecycle is intact: ending phase 1 arms its own drain
  // watch and completes the successor's ack normally.
  predicate_factory_.terminate(1);
  ASSERT_EQ(std::future_status::ready, future1.wait_for(std::chrono::seconds(5)));
  EXPECT_GE(future1.get().sequencer_stop_monotonic_ns, 0);

  worker.goIdleAsync(/*exit_dispatcher=*/true);
  worker.waitForCompletion();
  worker.shutdown();
}

} // namespace Client
} // namespace Nighthawk
