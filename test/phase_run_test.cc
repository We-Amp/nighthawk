// FORK-ONLY (We-Amp): multi-phase run tests (M2 A1 Task 3). The boundary
// contract -- non-destructive drain, non-exiting stop, warm carry-over, phase
// events -- is exercised end-to-end by driving ProcessImpl directly against a
// local HTTP listener (same pattern as the M0a in-proc tests), and the
// PhaseOrchestrator transition table is unit-tested without a run.

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"

#include "api/client/options.pb.h"

#include "source/client/client_worker_impl.h"
#include "source/client/options_impl.h"
#include "source/client/output_collector_impl.h"
#include "source/client/phase_orchestrator.h"
#include "source/client/process_impl.h"

#include "test/mocks/event/mocks.h"
#include "test/test_common/environment.h"
#include "test/test_common/utility.h"

using ::envoy::config::core::v3::TypedExtensionConfig;
using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

int64_t wallNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response, giving the benchmark real upstream traffic, and records the
// wall-clock timestamps of every accepted TCP connection: the SUT-side
// observable for the no-churn-at-the-boundary assertion. Same listener
// pattern as test/tui_run_feed_test.cc, plus per-connection serving threads
// so concurrent pool connections don't stall each other.
class CountingTestHttpListener {
public:
  CountingTestHttpListener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(listen_fd_, -1);
    int one = 1;
    EXPECT_EQ(::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)), 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    EXPECT_EQ(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    EXPECT_EQ(::listen(listen_fd_, 16), 0);
    socklen_t addr_len = sizeof(addr);
    EXPECT_EQ(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this]() { acceptLoop(); });
  }

  ~CountingTestHttpListener() {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
    for (std::thread& worker : connection_workers_) {
      worker.join();
    }
  }

  CountingTestHttpListener(const CountingTestHttpListener&) = delete;
  CountingTestHttpListener& operator=(const CountingTestHttpListener&) = delete;

  std::string uri() const { return fmt::format("http://127.0.0.1:{}/", port_); }

  // Wall-clock ns timestamps of every accepted TCP connection.
  std::vector<int64_t> acceptTimestamps() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return accepts_;
  }

private:
  void acceptLoop() {
    while (true) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        return;
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        accepts_.push_back(wallNowNs());
      }
      connection_workers_.emplace_back([fd]() { serve(fd); });
    }
  }

  static void serve(int fd) {
    static constexpr absl::string_view kResponse = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    std::string buffer;
    char chunk[4096];
    while (true) {
      size_t request_end;
      while ((request_end = buffer.find("\r\n\r\n")) != std::string::npos) {
        buffer.erase(0, request_end + 4);
        if (!sendAll(fd, kResponse)) {
          ::close(fd);
          return;
        }
      }
      ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
      if (n <= 0) {
        break;
      }
      buffer.append(chunk, static_cast<size_t>(n));
    }
    ::close(fd);
  }

  static bool sendAll(int fd, absl::string_view data) {
    size_t sent = 0;
    while (sent < data.size()) {
      ssize_t n = ::send(fd, data.data() + sent, data.size(), 0);
      if (n <= 0) {
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

  int listen_fd_{-1};
  uint16_t port_{0};
  std::thread thread_;
  std::vector<std::thread> connection_workers_;
  mutable std::mutex mutex_;
  std::vector<int64_t> accepts_;
};

// Two 2s phases on one worker: a warm phase at rps 50, then a phase at rps
// 100. Validation requires every phase to declare a duration >= 1s and rps
// >= 1; per-phase concurrency declares the (single) worker.
nighthawk::client::CommandLineOptions makeTwoPhaseOptions(const std::string& uri) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value(uri);
  cmd.mutable_connections()->set_value(4);
  cmd.mutable_timeout()->set_seconds(5);
  const std::vector<std::pair<uint32_t, uint32_t>> phases{{2, 50}, {2, 100}};
  for (uint32_t i = 0; i < phases.size(); ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(fmt::format("phase{}", i));
    phase->mutable_duration()->set_seconds(phases[i].first);
    phase->set_requests_per_second(phases[i].second);
    phase->mutable_concurrency()->set_value("1");
  }
  return cmd;
}

class PhaseRunTest : public testing::Test {
public:
  struct RunOutcome {
    bool success{false};
    nighthawk::client::Output output;
    std::vector<PhaseEvent> events;
    std::vector<PhaseBoundaryTiming> boundary_timings;
    std::vector<nighthawk::client::LiveSnapshot> live_snapshots;
  };

  RunOutcome runPhases(const nighthawk::client::CommandLineOptions& cmd_proto,
                       bool sample_live_snapshots = false) {
    auto options = std::make_unique<OptionsImpl>(cmd_proto);
    TypedExtensionConfig typed_dns_resolver_config;
    Envoy::Network::DnsResolverFactory& dns_resolver_factory =
        Envoy::Network::createDefaultDnsResolverFactory(typed_dns_resolver_config);
    absl::StatusOr<ProcessPtr> process_or_status = ProcessImpl::CreateProcessImpl(
        *options, dns_resolver_factory, typed_dns_resolver_config, time_system_);
    if (!process_or_status.ok()) {
      ADD_FAILURE() << "ProcessImpl creation failed: " << process_or_status.status();
      return {};
    }
    ProcessPtr process = std::move(process_or_status.value());
    auto& process_impl = static_cast<ProcessImpl&>(*process);
    RunOutcome outcome;
    std::mutex events_mutex;
    process_impl.setPhaseEventCallback([&outcome, &events_mutex](const PhaseEvent& event) {
      std::lock_guard<std::mutex> lock(events_mutex);
      outcome.events.push_back(event);
    });

    std::atomic<bool> stop_sampling{false};
    std::thread sampler;
    if (sample_live_snapshots) {
      sampler = std::thread([&]() {
        while (!stop_sampling.load()) {
          nighthawk::client::LiveSnapshot snapshot = process_impl.toLiveSnapshot();
          if (!snapshot.statistics().empty() || !snapshot.counters().empty()) {
            std::lock_guard<std::mutex> lock(outcome_mutex);
            outcome.live_snapshots.push_back(std::move(snapshot));
          }
          std::this_thread::sleep_for(50ms);
        }
      });
    }

    OutputCollectorImpl collector(time_system_, *options);
    outcome.success = process->run(collector);
    if (sampler.joinable()) {
      stop_sampling.store(true);
      sampler.join();
    }
    outcome.output = collector.toProto();
    outcome.boundary_timings = process_impl.phaseBoundaryTimings();
    process->shutdown();
    return outcome;
  }

protected:
  CountingTestHttpListener& listener() {
    if (!listener_) {
      listener_ = std::make_unique<CountingTestHttpListener>();
    }
    return *listener_;
  }

  std::mutex outcome_mutex;
  Envoy::Event::RealTimeSystem time_system_; // NO_CHECK_FORMAT(real_time)

private:
  std::unique_ptr<CountingTestHttpListener> listener_;
};

// Both phases of a 2-phase vector complete inside one ProcessImpl, presenting
// as exactly one run with one Output that carries a result group per phase.
TEST_F(PhaseRunTest, TwoPhasesRunInOneProcess) {
  RunOutcome outcome = runPhases(makeTwoPhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);
  std::vector<std::string> phase_group_names;
  int global_groups = 0;
  for (const nighthawk::client::Result& result : outcome.output.results()) {
    if (absl::StartsWith(result.name(), "phase_")) {
      phase_group_names.push_back(result.name());
    } else if (result.name() == "global") {
      global_groups++;
    }
  }
  ASSERT_EQ(2, phase_group_names.size());
  EXPECT_NE(phase_group_names[0].find("phase0"), std::string::npos);
  EXPECT_NE(phase_group_names[1].find("phase1"), std::string::npos);
  // The multi-phase run presents as a single completion: one global result.
  EXPECT_EQ(1, global_groups);
}

// Review Focus 1: at the boundary no new connections are created and no idle
// connections are closed -- the SUT never sees a new accept across the
// boundary window, and the drain bookkeeping (drain duration, idle gap,
// uncensored) is recorded.
TEST_F(PhaseRunTest, BoundaryDrainIsNonDestructive) {
  RunOutcome outcome = runPhases(makeTwoPhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);

  // Events: phase0 start, phase0 drain-complete, phase1 start, phase1
  // drain-complete.
  ASSERT_GE(outcome.events.size(), 4u);
  const int64_t drain_complete_wall_ns = outcome.events[1].wall_time_unix_ns;
  const int64_t next_phase_started_wall_ns = outcome.events[2].wall_time_unix_ns;

  const int64_t window_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(250ms).count();
  int accepts_in_window = 0;
  for (const int64_t accept_ns : listener().acceptTimestamps()) {
    if (accept_ns >= drain_complete_wall_ns - window_ns &&
        accept_ns <= next_phase_started_wall_ns + window_ns) {
      accepts_in_window++;
    }
  }
  EXPECT_EQ(0, accepts_in_window)
      << "connection churn observed across the phase boundary (non-destructive drain violation)";

  // Drain bookkeeping recorded for the phase-0 boundary: uncensored, with a
  // drain duration and an idle gap, never zero-silent.
  ASSERT_EQ(1, outcome.boundary_timings.size());
  EXPECT_EQ("phase0", outcome.boundary_timings[0].phase_id);
  EXPECT_FALSE(outcome.boundary_timings[0].drain_censored);
  EXPECT_GE(outcome.boundary_timings[0].drain_duration_ns, 0);
  EXPECT_GE(outcome.boundary_timings[0].idle_gap_ns, 0);
}

// The worker dispatcher survives the boundary: live snapshots (produced by a
// worker timer task on its dispatcher) keep flowing after the boundary, and
// carry the real phase ids of the phase they were taken in.
TEST_F(PhaseRunTest, NonExitingStopKeepsDispatcherAlive) {
  nighthawk::client::CommandLineOptions cmd = makeTwoPhaseOptions(listener().uri());
  cmd.mutable_live_streaming();
  cmd.mutable_stats_flush_interval()->set_value(1);
  RunOutcome outcome = runPhases(cmd, /*sample_live_snapshots=*/true);
  ASSERT_TRUE(outcome.success);

  ASSERT_GE(outcome.events.size(), 4u);
  const int64_t boundary_wall_ns = outcome.events[1].wall_time_unix_ns;

  bool snapshot_in_phase0_before_boundary = false;
  bool snapshot_in_phase1_after_boundary = false;
  std::lock_guard<std::mutex> lock(outcome_mutex);
  for (const nighthawk::client::LiveSnapshot& snapshot : outcome.live_snapshots) {
    if (snapshot.wall_time_unix_ns() < boundary_wall_ns &&
        snapshot.phase_id() == "phase0") {
      snapshot_in_phase0_before_boundary = true;
    }
    if (snapshot.wall_time_unix_ns() > boundary_wall_ns && snapshot.phase_id() == "phase1") {
      snapshot_in_phase1_after_boundary = true;
    }
  }
  EXPECT_TRUE(snapshot_in_phase0_before_boundary)
      << "no pre-boundary snapshot in phase0 (live snapshots missing entirely)";
  EXPECT_TRUE(snapshot_in_phase1_after_boundary)
      << "no post-boundary snapshot in phase1: the worker dispatcher did not "
         "survive the boundary (non-exiting stop violation)";
}

// Phase events arrive in phase order, each carrying both clock stamps, with
// wall and monotonic time strictly non-decreasing across the run (spec 8.5).
TEST_F(PhaseRunTest, PhaseEventsCarryClockPairs) {
  RunOutcome outcome = runPhases(makeTwoPhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(4u, outcome.events.size());

  const std::vector<std::string> expected_ids = {"phase0", "phase0", "phase1", "phase1"};
  const std::vector<uint32_t> expected_indices = {0, 0, 1, 1};
  int64_t last_wall_ns = -1;
  int64_t last_monotonic_ns = -1;
  for (size_t i = 0; i < outcome.events.size(); ++i) {
    EXPECT_EQ(expected_ids[i], outcome.events[i].phase_id);
    EXPECT_EQ(expected_indices[i], outcome.events[i].phase_index);
    EXPECT_GT(outcome.events[i].wall_time_unix_ns, 0);
    EXPECT_GT(outcome.events[i].monotonic_ns, 0);
    EXPECT_GE(outcome.events[i].wall_time_unix_ns, last_wall_ns);
    EXPECT_GE(outcome.events[i].monotonic_ns, last_monotonic_ns);
    last_wall_ns = outcome.events[i].wall_time_unix_ns;
    last_monotonic_ns = outcome.events[i].monotonic_ns;
  }
}

// --- PhaseOrchestrator unit tests: the boundary state machine, without a run. ---

std::vector<nighthawk::client::PhaseSpec> twoPhaseSpecs() {
  std::vector<nighthawk::client::PhaseSpec> phases;
  for (uint32_t i = 0; i < 2; ++i) {
    nighthawk::client::PhaseSpec phase;
    phase.set_name(fmt::format("phase{}", i));
    phase.mutable_duration()->set_seconds(2);
    phase.set_requests_per_second(50);
    phases.push_back(phase);
  }
  return phases;
}

TEST(PhaseOrchestratorTest, TransitionTableOrdering) {
  std::vector<PhaseEvent> events;
  PhaseOrchestrator orchestrator(
      twoPhaseSpecs(), [&events](const PhaseEvent& event) { events.push_back(event); });
  EXPECT_EQ(2, orchestrator.phaseCount());
  EXPECT_FALSE(orchestrator.isFinalPhase(0));
  EXPECT_TRUE(orchestrator.isFinalPhase(1));

  orchestrator.markNextPhaseStarted(0);
  EXPECT_EQ(BoundaryState::kNextPhaseStarted, orchestrator.state());
  orchestrator.markSequencerStopped(0);
  EXPECT_EQ(BoundaryState::kSequencerStopped, orchestrator.state());

  std::promise<PhaseDrainAck> ack_promise;
  std::vector<std::future<PhaseDrainAck>> acks;
  acks.push_back(ack_promise.get_future());
  ack_promise.set_value(PhaseDrainAck{/*sequencer_stop_monotonic_ns=*/1000,
                                      /*drain_complete_monotonic_ns=*/2000});
  EXPECT_TRUE(orchestrator.collectDrainAcks(acks, std::chrono::milliseconds(1000), 0));
  EXPECT_EQ(BoundaryState::kDrainComplete, orchestrator.state());

  orchestrator.markNextPhaseStarted(1);
  EXPECT_EQ(BoundaryState::kNextPhaseStarted, orchestrator.state());

  ASSERT_EQ(3, events.size());
  EXPECT_EQ("phase0", events[0].phase_id);
  EXPECT_EQ("phase0", events[1].phase_id);
  EXPECT_EQ("phase1", events[2].phase_id);

  ASSERT_EQ(1, orchestrator.boundaryTimings().size());
  EXPECT_EQ(1000, orchestrator.boundaryTimings()[0].drain_duration_ns);
  EXPECT_FALSE(orchestrator.boundaryTimings()[0].drain_censored);
  // The idle gap is recorded when the next phase starts (not yet observed
  // through the public accessor after only one boundary: timings.back() is
  // still open until the NEXT markNextPhaseStarted).
}

TEST(PhaseOrchestratorTest, DrainWaitBoundedByTimeout) {
  std::vector<PhaseEvent> events;
  PhaseOrchestrator orchestrator(
      twoPhaseSpecs(), [&events](const PhaseEvent& event) { events.push_back(event); });
  // A hung in-flight request: the worker's drain future never completes.
  std::promise<PhaseDrainAck> never_satisfied;
  std::vector<std::future<PhaseDrainAck>> acks;
  acks.push_back(never_satisfied.get_future());

  const auto start = std::chrono::steady_clock::now();
  const bool all_acked = orchestrator.collectDrainAcks(acks, std::chrono::milliseconds(100), 0);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(all_acked);
  EXPECT_GE(elapsed, std::chrono::milliseconds(100));
  EXPECT_LT(elapsed, std::chrono::milliseconds(100) + std::chrono::seconds(5));
  // The boundary is censored and recorded, never hidden, and the orchestrator
  // can proceed: the state advanced past the drain.
  EXPECT_EQ(BoundaryState::kDrainComplete, orchestrator.state());
  ASSERT_EQ(1, orchestrator.boundaryTimings().size());
  EXPECT_TRUE(orchestrator.boundaryTimings()[0].drain_censored);
}

TEST(PhaseOrchestratorTest, IdleGapRecordedAgainstNextPhaseStart) {
  std::vector<PhaseEvent> events;
  PhaseOrchestrator orchestrator(
      twoPhaseSpecs(), [&events](const PhaseEvent& event) { events.push_back(event); });

  orchestrator.markNextPhaseStarted(0);
  std::promise<PhaseDrainAck> ack_promise;
  std::vector<std::future<PhaseDrainAck>> acks;
  acks.push_back(ack_promise.get_future());
  // The ack carries steady-clock (monotonic) nanoseconds; take a sample now
  // so the idle gap computed against the next markNextPhaseStarted() is small.
  const int64_t drained_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now().time_since_epoch())
                                 .count();
  ack_promise.set_value(PhaseDrainAck{drained_ns - 1000, drained_ns});
  EXPECT_TRUE(orchestrator.collectDrainAcks(acks, std::chrono::milliseconds(1000), 0));
  std::this_thread::sleep_for(20ms);
  orchestrator.markNextPhaseStarted(1);

  ASSERT_EQ(1, orchestrator.boundaryTimings().size());
  EXPECT_GE(orchestrator.boundaryTimings()[0].idle_gap_ns, 0);
  EXPECT_LT(orchestrator.boundaryTimings()[0].idle_gap_ns, std::chrono::nanoseconds(2s).count());
}

// The review's Important fix: a boundary's drain watch holds its promise by
// value. Simulates the censored-then-next-phase sequence: boundary 1's
// in-flight hangs, the orchestrator censors, boundary 2 begins, THEN the hung
// request completes and boundary 1's stale watch fires once more -- it must
// complete only boundary 1's ack and never pre-satisfy boundary 2's.
TEST(BoundaryDrainWatchTest, StaleWatchCannotSatisfyNextBoundary) {
  testing::NiceMock<Envoy::Event::MockDispatcher> dispatcher;
  auto* timer1 = new testing::NiceMock<Envoy::Event::MockTimer>(); // owned by the watches
  auto* timer2 = new testing::NiceMock<Envoy::Event::MockTimer>();
  uint64_t in_flight = 1; // the hung request

  auto promise1 = std::make_shared<std::promise<PhaseDrainAck>>();
  std::future<PhaseDrainAck> future1 = promise1->get_future();
  BoundaryDrainWatch watch1(dispatcher, [&in_flight]() { return in_flight; }, promise1,
                            /*stop_monotonic_ns=*/1111);
  Envoy::Event::TimerCb cb1;
  Envoy::Event::TimerCb cb2;
  EXPECT_CALL(dispatcher, createTimer_(_))
      .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
        cb1 = std::move(cb);
        return timer1;
      }))
      .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
        cb2 = std::move(cb);
        return timer2;
      }));
  watch1.start();
  cb1(); // in-flight still hung: re-arms itself

  // The orchestrator censors boundary 1 at its deadline and starts boundary 2.
  auto promise2 = std::make_shared<std::promise<PhaseDrainAck>>();
  std::future<PhaseDrainAck> future2 = promise2->get_future();
  BoundaryDrainWatch watch2(dispatcher, [&in_flight]() { return in_flight; }, promise2,
                            /*stop_monotonic_ns=*/2222);
  watch2.start();

  // The hung request completes during the gap; boundary 1's stale watch fires
  // once more before the next begin-phase cancels it.
  in_flight = 0;
  cb1();

  ASSERT_EQ(future1.wait_for(std::chrono::milliseconds(0)), std::future_status::ready);
  EXPECT_EQ(1111, future1.get().sequencer_stop_monotonic_ns);
  EXPECT_EQ(future2.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout)
      << "stale watch pre-satisfied the next boundary's ack";

  // The live watch still completes its own boundary with its own anchor.
  cb2();
  ASSERT_EQ(future2.wait_for(std::chrono::milliseconds(0)), std::future_status::ready);
  EXPECT_EQ(2222, future2.get().sequencer_stop_monotonic_ns);
}

TEST(PhaseOrchestratorTest, PartialAcksAreCensored) {
  std::vector<PhaseEvent> events;
  PhaseOrchestrator orchestrator(
      twoPhaseSpecs(), [&events](const PhaseEvent& event) { events.push_back(event); });
  std::promise<PhaseDrainAck> ack_promise;
  std::promise<PhaseDrainAck> hung_promise;
  std::vector<std::future<PhaseDrainAck>> acks;
  acks.push_back(ack_promise.get_future());
  acks.push_back(hung_promise.get_future());
  ack_promise.set_value(PhaseDrainAck{1000, 5000});

  const auto start = std::chrono::steady_clock::now();
  const bool all_acked = orchestrator.collectDrainAcks(acks, std::chrono::milliseconds(100), 0);
  EXPECT_FALSE(all_acked);
  EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(100));
  ASSERT_EQ(1, orchestrator.boundaryTimings().size());
  EXPECT_TRUE(orchestrator.boundaryTimings()[0].drain_censored);
}

} // namespace

} // namespace Client
} // namespace Nighthawk
