// FORK-ONLY (We-Amp): multi-phase run tests (M2 A1 Task 3). The boundary
// contract -- non-destructive drain, non-exiting stop, warm carry-over, phase
// events -- is exercised end-to-end by driving ProcessImpl directly against a
// local HTTP listener (same pattern as the M0a in-proc tests), and the
// PhaseOrchestrator transition table is unit-tested without a run.

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
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

#include "source/common/protobuf/protobuf.h"

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

// One served request, as observed on the SUT side: wall-clock (start, end).
struct RequestSpan {
  int64_t start_ns;
  int64_t end_ns;
};

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response, giving the benchmark real upstream traffic, and records the
// wall-clock timestamps of every accepted TCP connection: the SUT-side
// observable for the no-churn-at-the-boundary assertion. Same listener
// pattern as test/tui_run_feed_test.cc, plus per-connection serving threads
// so concurrent pool connections don't stall each other. An optional
// per-request delay turns it into a slow SUT for the gate tests: with a
// known service time, the offered load provably exceeds what a phase's
// in-flight gate admits, so shed load is observable as client-side gate
// refusals rather than as hidden queueing.
class CountingTestHttpListener {
public:
  explicit CountingTestHttpListener(std::chrono::microseconds request_delay =
                                        std::chrono::microseconds::zero())
      : request_delay_ns_(
            std::chrono::duration_cast<std::chrono::nanoseconds>(request_delay).count()) {
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

  // FORK-ONLY (We-Amp): per-response delay for the per-phase statistics tests
  // (a phase-dependent latency profile). Flipping it in a phase-start event
  // callback gives the phases distinct, deterministic latency distributions.
  void setResponseDelayNs(int64_t delay_ns) { request_delay_ns_.store(delay_ns); }

  // Wall-clock ns timestamps of every accepted TCP connection.
  std::vector<int64_t> acceptTimestamps() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return accepts_;
  }

  // SUT-side request spans (start, end wall ns), for per-phase concurrency
  // and completion-count observations.
  std::vector<RequestSpan> requestSpans() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return spans_;
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
      connection_workers_.emplace_back([this, fd]() { serve(fd); });
    }
  }

  void serve(int fd) {
    static constexpr absl::string_view kResponse = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    std::string buffer;
    char chunk[4096];
    while (true) {
      size_t request_end;
      while ((request_end = buffer.find("\r\n\r\n")) != std::string::npos) {
        buffer.erase(0, request_end + 4);
        const int64_t start_ns = wallNowNs();
        const int64_t delay_ns = request_delay_ns_.load();
        if (delay_ns > 0) {
          // NO_CHECK_FORMAT(real_time)
          std::this_thread::sleep_for(std::chrono::nanoseconds(delay_ns));
        }
        const bool sent = sendAll(fd, kResponse);
        {
          std::lock_guard<std::mutex> lock(mutex_);
          spans_.push_back(RequestSpan{start_ns, wallNowNs()});
        }
        if (!sent) {
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
  // Atomic so a test can flip the SUT's service time mid-run (the per-phase
  // statistics tests give phase 2 a slower profile from its start event).
  std::atomic<int64_t> request_delay_ns_{0};
  std::thread thread_;
  std::vector<std::thread> connection_workers_;
  mutable std::mutex mutex_;
  std::vector<int64_t> accepts_;
  std::vector<RequestSpan> spans_;
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
    // FORK-ONLY (M2 Task 5): per-phase counts of gate-refused starts.
    std::vector<uint64_t> phase_gate_exceeded;
    // FORK-ONLY (M2 Task 6): per-phase worker-subset bookkeeping.
    std::vector<PhaseWorkerSubset> phase_worker_subsets;
  };

  RunOutcome runPhases(
      const nighthawk::client::CommandLineOptions& cmd_proto, bool sample_live_snapshots = false,
      // FORK-ONLY (We-Amp): extra event hook for the per-phase statistics
      // tests (invoked synchronously on the run-calling thread at each phase
      // event, before the corresponding begin-phase posts).
      const std::function<void(const PhaseEvent&)>& event_hook = {}) {
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
    process_impl.setPhaseEventCallback([&outcome, &events_mutex, &event_hook](const PhaseEvent& event) {
      std::lock_guard<std::mutex> lock(events_mutex);
      outcome.events.push_back(event);
      if (event_hook != nullptr) {
        event_hook(event);
      }
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
    outcome.phase_gate_exceeded = process_impl.phaseGateExceededCounts();
    outcome.phase_worker_subsets = process_impl.phaseWorkerSubsets();
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

  // A slow SUT (each request holds the connection ~100ms): the gate tests
  // drive offered load far above the admitted concurrency so shedding is
  // observable.
  CountingTestHttpListener& slowListener() {
    if (!slow_listener_) {
      slow_listener_ = std::make_unique<CountingTestHttpListener>(std::chrono::milliseconds(100));
    }
    return *slow_listener_;
  }

  std::mutex outcome_mutex;
  Envoy::Event::RealTimeSystem time_system_; // NO_CHECK_FORMAT(real_time)

private:
  std::unique_ptr<CountingTestHttpListener> listener_;
  std::unique_ptr<CountingTestHttpListener> slow_listener_;
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

// FORK-ONLY (M2 close-blocker repro): the phase vector's declared rps is the
// phase's GLOBAL rate, and every phase must run its declared duration before
// the boundary drains. These two tests pin the acceptance shape at test
// scale: per-second SUT-side initiation counts track the declared rps within
// tolerance, the boundary drain is uncensored and short, the phase lasts its
// declared duration, and nothing is shed (gate-exceeded stays zero).
//
// The --timeout < phase-duration shape matters: the boundary's drain wait
// must be bounded by --timeout measured from the sequencer STOP, never from
// the phase begin (a budget applied at begin-phase censors a healthy phase
// at the cap and begins the successor while the predecessor's sequencer is
// still running -- the T8 close-blocker).

namespace {

// Builds a two-phase vector: [duration, rps, workers] per phase.
nighthawk::client::CommandLineOptions makePhaseOptions(const std::string& uri, int64_t timeout_s,
                                                       const std::vector<
                                                           std::array<uint32_t, 3>>& phases) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value(uri);
  cmd.mutable_connections()->set_value(4);
  cmd.mutable_timeout()->set_seconds(timeout_s);
  for (uint32_t i = 0; i < phases.size(); ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(fmt::format("phase{}", i));
    phase->mutable_duration()->set_seconds(phases[i][0]);
    phase->set_requests_per_second(phases[i][1]);
    phase->mutable_concurrency()->set_value(fmt::format("{}", phases[i][2]));
  }
  return cmd;
}

} // namespace

// Pacing at one worker: each phase's per-second initiation count tracks its
// declared rps, the boundary is uncensored, nothing is shed.
TEST_F(PhaseRunTest, PhaseScheduleIsPacedNotBurst) {
  RunOutcome outcome = runPhases(makePhaseOptions(listener().uri(), 5, {{4, 50, 1}, {4, 100, 1}}));
  ASSERT_TRUE(outcome.success);

  // The boundary drain is uncensored and fast (a direct-response listener
  // holds nothing in flight past the sequencer stop).
  ASSERT_EQ(1, outcome.boundary_timings.size());
  EXPECT_FALSE(outcome.boundary_timings[0].drain_censored);
  EXPECT_LT(outcome.boundary_timings[0].drain_duration_ns,
            std::chrono::duration_cast<std::chrono::nanoseconds>(1s).count());

  ASSERT_EQ(2, outcome.phase_gate_exceeded.size());
  EXPECT_EQ(0, outcome.phase_gate_exceeded[0]);
  EXPECT_EQ(0, outcome.phase_gate_exceeded[1]);

  // Phase-0 lasted its declared duration: its start event to the next
  // phase's start event is ~4 s, not the censoring-cap shape.
  ASSERT_GE(outcome.events.size(), 4u);
  const int64_t phase0_declared_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(4s).count();
  const int64_t phase0_wall_ns =
      outcome.events[2].wall_time_unix_ns - outcome.events[0].wall_time_unix_ns;
  EXPECT_NEAR(phase0_wall_ns, phase0_declared_ns, phase0_declared_ns / 8)
      << "phase0 did not last its declared duration (wall ns: " << phase0_wall_ns << ")";

  // Per-second SUT-side initiation buckets per phase, relative to each
  // phase's start event. Full buckets only: the final (partial) bucket of
  // each phase window is dropped.
  const std::vector<std::array<int64_t, 3>> phase_windows{
      {outcome.events[0].wall_time_unix_ns, outcome.events[2].wall_time_unix_ns, 50},
      {outcome.events[2].wall_time_unix_ns, std::numeric_limits<int64_t>::max(), 100}};
  const std::vector<RequestSpan> spans = listener().requestSpans();
  std::string histogram;
  for (const std::array<int64_t, 3>& window : phase_windows) {
    const int64_t window_start_ns = window[0];
    const int64_t window_end_ns = window[1];
    const int64_t rps = window[2];
    histogram.clear();
    std::vector<int64_t> buckets;
    for (const RequestSpan& span : spans) {
      if (span.start_ns < window_start_ns || span.start_ns >= window_end_ns) {
        continue;
      }
      const int64_t sec = (span.start_ns - window_start_ns) / 1000000000;
      if (static_cast<size_t>(sec) >= buckets.size()) {
        buckets.resize(sec + 1, 0);
      }
      buckets[sec]++;
    }
    if (!buckets.empty()) {
      buckets.pop_back();
    }
    EXPECT_GE(buckets.size(), 3u)
        << "phase produced fewer than 3 full seconds of load; declared rps " << rps;
    for (size_t sec = 0; sec < buckets.size(); ++sec) {
      absl::StrAppend(&histogram,
                      fmt::format("  second {} (declared {}): {}\n", sec, rps, buckets[sec]));
    }
    for (size_t sec = 0; sec < buckets.size(); ++sec) {
      EXPECT_NEAR(buckets[sec], rps, rps / 2) << "bucket burst/undershoot; histogram:\n"
                                              << histogram;
    }
  }
}

// The T8 close-blocker shape at test scale: a worker-count change across the
// boundary, a 4x rps delta, and --timeout BELOW the phase durations. The
// declared rps is the phase's GLOBAL rate (divided across the active
// workers), the phase runs its declared duration before the boundary, and
// the boundary drain is uncensored.
TEST_F(PhaseRunTest, PhaseBoundaryWaitsForSequencerStop) {
  RunOutcome outcome = runPhases(makePhaseOptions(listener().uri(), 2, {{3, 100, 2}, {3, 400, 4}}));
  ASSERT_TRUE(outcome.success);

  ASSERT_EQ(1, outcome.boundary_timings.size());
  EXPECT_FALSE(outcome.boundary_timings[0].drain_censored)
      << "boundary drain censored at the timeout cap: the drain budget was "
         "applied from the phase begin instead of the sequencer stop";
  EXPECT_LT(outcome.boundary_timings[0].drain_duration_ns,
            std::chrono::duration_cast<std::chrono::nanoseconds>(1s).count());

  ASSERT_EQ(2, outcome.phase_gate_exceeded.size());
  EXPECT_EQ(0, outcome.phase_gate_exceeded[0]);
  EXPECT_EQ(0, outcome.phase_gate_exceeded[1]);

  // Phase-0 ran its declared 3 s, not the timeout cap.
  ASSERT_GE(outcome.events.size(), 4u);
  const int64_t phase0_declared_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(3s).count();
  const int64_t phase0_wall_ns =
      outcome.events[2].wall_time_unix_ns - outcome.events[0].wall_time_unix_ns;
  EXPECT_NEAR(phase0_wall_ns, phase0_declared_ns, phase0_declared_ns / 8)
      << "phase0 did not last its declared duration (wall ns: " << phase0_wall_ns << ")";

  // Global pacing per phase: declared rps is the phase total across the
  // active workers (100, then 400). A predecessor sequencer that keeps
  // running into the successor's window shows up here as overshoot.
  const std::vector<std::array<int64_t, 3>> phase_windows{
      {outcome.events[0].wall_time_unix_ns, outcome.events[2].wall_time_unix_ns, 100},
      {outcome.events[2].wall_time_unix_ns, std::numeric_limits<int64_t>::max(), 400}};
  const std::vector<RequestSpan> spans = listener().requestSpans();
  std::string histogram;
  for (const std::array<int64_t, 3>& window : phase_windows) {
    const int64_t window_start_ns = window[0];
    const int64_t window_end_ns = window[1];
    const int64_t rps = window[2];
    histogram.clear();
    std::vector<int64_t> buckets;
    for (const RequestSpan& span : spans) {
      if (span.start_ns < window_start_ns || span.start_ns >= window_end_ns) {
        continue;
      }
      const int64_t sec = (span.start_ns - window_start_ns) / 1000000000;
      if (static_cast<size_t>(sec) >= buckets.size()) {
        buckets.resize(sec + 1, 0);
      }
      buckets[sec]++;
    }
    if (!buckets.empty()) {
      buckets.pop_back();
    }
    EXPECT_GE(buckets.size(), 2u)
        << "phase produced fewer than 2 full seconds of load; declared rps " << rps;
    for (size_t sec = 0; sec < buckets.size(); ++sec) {
      absl::StrAppend(&histogram,
                      fmt::format("  second {} (declared {}): {}\n", sec, rps, buckets[sec]));
    }
    for (size_t sec = 0; sec < buckets.size(); ++sec) {
      EXPECT_NEAR(buckets[sec], rps, rps / 2) << "bucket burst/overshoot; histogram:\n"
                                              << histogram;
    }
  }
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

// T7 wiring: exactly one emit per boundary carries the completed boundary —
// the next-phase-start emit, with drain and idle both final and the worker
// facts enriched from the subset plan. The other emits carry no boundary.
TEST_F(PhaseRunTest, PhaseEventsCarryCompletedBoundary) {
  RunOutcome outcome = runPhases(makeTwoPhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(4u, outcome.events.size());

  int completed_boundaries = 0;
  for (const PhaseEvent& event : outcome.events) {
    if (!event.boundary_complete) {
      EXPECT_EQ(event.drain_duration_ns, -1);
      EXPECT_EQ(event.idle_gap_ns, -1);
      continue;
    }
    ++completed_boundaries;
    // The boundary that ended phase0, observed at phase1's first scheduled
    // work (the event's own phase fields name the STARTING phase).
    EXPECT_EQ(event.ending_phase_index, 0u);
    EXPECT_EQ(event.ending_phase_id, "phase0");
    EXPECT_EQ(event.phase_index, 1u);
    EXPECT_EQ(event.phase_id, "phase1");
    // Both timings are final and non-negative; the drain is bounded by
    // --timeout (5s here).
    EXPECT_GE(event.drain_duration_ns, 0);
    EXPECT_GE(event.idle_gap_ns, 0);
    EXPECT_LT(event.drain_duration_ns, 5000000000);
    // Same concurrency ("1") in both phases: one active worker, no cold
    // start at the transition.
    EXPECT_EQ(event.worker_count, 1u);
    EXPECT_FALSE(event.next_phase_cold_started);
  }
  EXPECT_EQ(completed_boundaries, 1);
}

// --- M2 Task 5: per-phase resource limits without rebuilding clusters. ---

// Two 2s phases at rps 100 against a SUT with ~100ms service time: both
// phases offer far more load than their gates admit. Phase 0 inherits the
// run-level connections (4); phase 1 declares a lower per-phase connections
// gate (1). Bootstrap breakers sit at the max across phases (4), so only the
// client-side gates can realize phase 1's tighter ceiling.
nighthawk::client::CommandLineOptions makeGateTestOptions(const std::string& uri) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value(uri);
  cmd.mutable_connections()->set_value(4);
  cmd.mutable_timeout()->set_seconds(5);
  const char* names[] = {"gate_phase0", "gate_phase1"};
  for (uint32_t i = 0; i < 2; ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(names[i]);
    phase->mutable_duration()->set_seconds(2);
    phase->set_requests_per_second(100);
    phase->mutable_concurrency()->set_value("1");
  }
  cmd.mutable_phases(1)->mutable_connections()->set_value(1);
  return cmd;
}

nighthawk::client::CommandLineOptions makeThreePhaseOptions(const std::string& uri) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value(uri);
  cmd.mutable_connections()->set_value(4);
  cmd.mutable_timeout()->set_seconds(5);
  for (uint32_t i = 0; i < 3; ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(fmt::format("phase{}", i));
    phase->mutable_duration()->set_seconds(2);
    phase->set_requests_per_second(50);
    phase->mutable_concurrency()->set_value("1");
  }
  return cmd;
}

// Completions observed SUT-side within [from_ns, to_ns].
int completionsInWindow(const std::vector<RequestSpan>& spans, int64_t from_ns, int64_t to_ns) {
  int completions = 0;
  for (const RequestSpan& span : spans) {
    if (span.end_ns >= from_ns && span.end_ns <= to_ns) {
      completions++;
    }
  }
  return completions;
}

// Max requests the SUT served concurrently within [from_ns, to_ns].
int maxConcurrentInWindow(const std::vector<RequestSpan>& spans, int64_t from_ns, int64_t to_ns) {
  int max_concurrent = 0;
  for (const RequestSpan& span : spans) {
    if (span.end_ns < from_ns || span.start_ns > to_ns) {
      continue;
    }
    int concurrent = 0;
    for (const RequestSpan& other : spans) {
      if (other.end_ns < from_ns || other.start_ns > to_ns) {
        continue;
      }
      if (other.start_ns <= span.start_ns && other.end_ns >= span.start_ns) {
        concurrent++;
      }
    }
    max_concurrent = std::max(max_concurrent, concurrent);
  }
  return max_concurrent;
}

// The per-phase client-side gate is enforced even though the bootstrapped
// breakers allow more: with a ~100ms SUT, phase 1's gate of one admitted
// request yields roughly a tenth of phase 0's completions (gate 4), and the
// SUT never observes the phase-1 offered load being served concurrently.
TEST_F(PhaseRunTest, PhaseGateEnforcedClientSide) {
  RunOutcome outcome = runPhases(makeGateTestOptions(slowListener().uri()));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(4u, outcome.events.size());

  const int64_t phase0_from = outcome.events[0].wall_time_unix_ns;
  const int64_t phase0_to = outcome.events[1].wall_time_unix_ns;
  const int64_t phase1_from = outcome.events[2].wall_time_unix_ns;
  const int64_t phase1_to = outcome.events[3].wall_time_unix_ns;
  const std::vector<RequestSpan> spans = slowListener().requestSpans();

  const int phase0_completions = completionsInWindow(spans, phase0_from, phase0_to);
  const int phase1_completions = completionsInWindow(spans, phase1_from, phase1_to);
  // Both phases ran and served traffic through their gates.
  EXPECT_GE(phase0_completions, 20);
  EXPECT_GE(phase1_completions, 5);
  // Phase 1's tighter gate sheds the offered load: its throughput sits well
  // below phase 0's despite identical rps.
  EXPECT_LT(phase1_completions, phase0_completions / 2)
      << "phase 1 throughput not shed by its gate (gate not enforced client-side)";
  // The SUT never saw more than the gated concurrency in phase 1 (one
  // admitted in-flight request; 2 admits one transient completion overlap).
  EXPECT_LE(maxConcurrentInWindow(spans, phase1_from, phase1_to), 2);
}

// Gate-blocked behavior is refuse-and-count (never queued, never silent):
// offered load above a phase's gate shows up as gate_exceeded counts while
// completions stay bounded by the gated capacity.
TEST_F(PhaseRunTest, GateExceededCountedNotQueued) {
  RunOutcome outcome = runPhases(makeGateTestOptions(slowListener().uri()));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(4u, outcome.events.size());
  ASSERT_EQ(2u, outcome.phase_gate_exceeded.size());

  // Both phases offer ~100 rps against ~40/s (gate 4) and ~10/s (gate 1)
  // capacity: refusals are counted in both, visibly in the tight phase.
  EXPECT_GT(outcome.phase_gate_exceeded[0], 0u);
  EXPECT_GT(outcome.phase_gate_exceeded[1], 0u)
      << "gate-refused starts were neither shed-and-counted nor enforced";

  const int64_t phase1_from = outcome.events[2].wall_time_unix_ns;
  const int64_t phase1_to = outcome.events[3].wall_time_unix_ns;
  const int phase1_completions =
      completionsInWindow(slowListener().requestSpans(), phase1_from, phase1_to);
  // Bounded by the gated capacity (~10/s x 2s), not by the offered load: no
  // queue absorbed the excess (an unbounded queue would distort the
  // measurement; the refusals above prove the excess was shed).
  EXPECT_GT(phase1_completions, 0);
  EXPECT_LE(phase1_completions, 35);
}

// Review Focus 1 guard against the addOrUpdateCluster temptation: across a
// three-phase run, the cluster set must be untouched at every boundary --
// observable SUT-side as zero connection churn in either boundary window,
// with both boundaries recorded uncensored.
TEST_F(PhaseRunTest, NoClusterMutationAtBoundaries) {
  RunOutcome outcome = runPhases(makeThreePhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(6u, outcome.events.size());
  ASSERT_EQ(2u, outcome.boundary_timings.size());

  const int64_t window_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(250ms).count();
  for (uint32_t boundary = 0; boundary < 2; ++boundary) {
    EXPECT_FALSE(outcome.boundary_timings[boundary].drain_censored);
    const int64_t drain_complete_ns = outcome.events[2 * boundary + 1].wall_time_unix_ns;
    const int64_t next_start_ns = outcome.events[2 * boundary + 2].wall_time_unix_ns;
    int accepts_in_window = 0;
    for (const int64_t accept_ns : listener().acceptTimestamps()) {
      if (accept_ns >= drain_complete_ns - window_ns && accept_ns <= next_start_ns + window_ns) {
        accepts_in_window++;
      }
    }
    EXPECT_EQ(0, accepts_in_window)
        << "connection churn at boundary " << boundary
        << " (cluster mutation / destructive drain violation)";
  }
}

// --- M2 Task 6: worker subsets -- bootstrap at max, start/stop per phase. ---

// A phase vector with an explicit per-phase concurrency per entry.
nighthawk::client::CommandLineOptions makeSubsetPhaseOptions(
    const std::string& uri, const std::vector<uint32_t>& concurrencies) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value(uri);
  cmd.mutable_connections()->set_value(4);
  cmd.mutable_timeout()->set_seconds(5);
  for (uint32_t i = 0; i < concurrencies.size(); ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(fmt::format("phase{}", i));
    phase->mutable_duration()->set_seconds(2);
    phase->set_requests_per_second(50);
    phase->mutable_concurrency()->set_value(fmt::format("{}", concurrencies[i]));
  }
  return cmd;
}

const nighthawk::client::Result* findResult(const nighthawk::client::Output& output,
                                            absl::string_view name) {
  for (const nighthawk::client::Result& result : output.results()) {
    if (result.name() == name) {
      return &result;
    }
  }
  return nullptr;
}

uint64_t counterValue(const nighthawk::client::Result& result, absl::string_view name) {
  for (const nighthawk::client::Counter& counter : result.counters()) {
    if (counter.name() == name) {
      return counter.value();
    }
  }
  return 0;
}

// Sample count carried by the phase group's sequencer completion statistic.
uint64_t sequencerCallbackCount(const nighthawk::client::Output& output,
                                absl::string_view group_name) {
  const nighthawk::client::Result* result = findResult(output, group_name);
  if (result == nullptr) {
    return 0;
  }
  for (const nighthawk::client::Statistic& statistic : result->statistics()) {
    if (statistic.id() == "sequencer.callback") {
      return statistic.count();
    }
  }
  return 0;
}

// Three phases declaring concurrency [2, 4, 2]: four workers are bootstrapped
// (the max over phases), phase traffic comes from exactly the declared subset
// per phase, and the workers that sit out the last phase keep their
// cumulative counters.
TEST_F(PhaseRunTest, SubsetPhasesRunCorrectWorkers) {
  RunOutcome outcome = runPhases(makeSubsetPhaseOptions(listener().uri(), {2, 4, 2}));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(6u, outcome.events.size());

  // Bootstrap at max-over-phases: a result group per bootstrapped worker,
  // four of them, even though no phase declares more than 4 and the first
  // and last phases use only 2.
  int worker_groups = 0;
  for (const nighthawk::client::Result& result : outcome.output.results()) {
    if (absl::StartsWith(result.name(), "worker_")) {
      worker_groups++;
    }
  }
  EXPECT_EQ(4, worker_groups);

  // Per-phase request traffic: the declared rps (50) is the phase's GLOBAL
  // rate (divided across the phase's active workers), so every phase serves
  // ~rps x duration completions regardless of its worker count (M2
  // burstfix: without the division each worker paced the full declared rps
  // and phase traffic scaled with the worker count). The subset itself is
  // proven by the per-worker counters below.
  const int64_t p0_from = outcome.events[0].wall_time_unix_ns;
  const int64_t p0_to = outcome.events[1].wall_time_unix_ns;
  const int64_t p1_from = outcome.events[2].wall_time_unix_ns;
  const int64_t p1_to = outcome.events[3].wall_time_unix_ns;
  const int64_t p2_from = outcome.events[4].wall_time_unix_ns;
  const int64_t p2_to = outcome.events[5].wall_time_unix_ns;
  const std::vector<RequestSpan> spans = listener().requestSpans();
  const int p0_completions = completionsInWindow(spans, p0_from, p0_to);
  const int p1_completions = completionsInWindow(spans, p1_from, p1_to);
  const int p2_completions = completionsInWindow(spans, p2_from, p2_to);
  // 50 rps x 2 s = ~100 per phase, on every subset size.
  EXPECT_GT(p0_completions, 50);
  EXPECT_GT(p1_completions, 50);
  EXPECT_GT(p2_completions, 50);
  EXPECT_LT(p0_completions, 160);
  EXPECT_LT(p1_completions, 160);
  EXPECT_LT(p2_completions, 160);
  EXPECT_LT(p1_completions * 3, p0_completions * 5)
      << "phase 1 traffic scaled with its worker count (declared rps is the "
         "phase's global rate, not a per-worker rate)";
  EXPECT_LT(p2_completions * 3, p1_completions * 5)
      << "phase 2 traffic scaled with its worker count (declared rps is the "
         "phase's global rate, not a per-worker rate)";

  // The workers beyond phase 2's subset sat it out: worker_0 ran all three
  // phases, worker_2 only the first two, on identical per-phase load.
  const nighthawk::client::Result* worker_0 = findResult(outcome.output, "worker_0");
  const nighthawk::client::Result* worker_2 = findResult(outcome.output, "worker_2");
  ASSERT_NE(worker_0, nullptr);
  ASSERT_NE(worker_2, nullptr);
  const uint64_t w0_requests = counterValue(*worker_0, "benchmark.http_2xx");
  const uint64_t w2_requests = counterValue(*worker_2, "benchmark.http_2xx");
  EXPECT_GT(w0_requests, w2_requests);
  EXPECT_GT(w2_requests, 0u) << "worker_2 never ran any phase";
}

// A worker removed at a phase boundary folds its last phase's slab fully into
// that phase's result, and its cumulative counters survive into the final
// output. Phase 1 here runs a single worker; the removed worker's phase-0
// work must not leak into phase 1's group.
TEST_F(PhaseRunTest, RemovedWorkerFoldsIntoLastPhase) {
  RunOutcome outcome = runPhases(makeSubsetPhaseOptions(listener().uri(), {2, 1}));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(4u, outcome.events.size());

  // Both phases declare rps 50 for 2s -- the phase's GLOBAL rate (M2
  // burstfix: divided across the phase's active workers) -- so each phase's
  // slab holds ~100 completions: phase 0 from its two workers, phase 1 from
  // the single remaining worker. The removed worker's slab lands in phase
  // 0's group only.
  const uint64_t p0 = sequencerCallbackCount(outcome.output, "phase_0_phase0");
  const uint64_t p1 = sequencerCallbackCount(outcome.output, "phase_1_phase1");
  EXPECT_GT(p0, 50u) << "phase 0 group missing the removed worker's slab";
  EXPECT_LT(p0, 160u);
  EXPECT_GT(p1, 40u) << "phase 1 group missing entirely";
  EXPECT_LT(p1, 160u) << "phase 1 group holds more than the single active worker's slab";

  // The removed worker's cumulative counters are preserved in the final
  // output: worker_1 shows its phase-0 work, worker_0 shows more (it also ran
  // phase 1).
  const nighthawk::client::Result* worker_0 = findResult(outcome.output, "worker_0");
  const nighthawk::client::Result* worker_1 = findResult(outcome.output, "worker_1");
  ASSERT_NE(worker_0, nullptr);
  ASSERT_NE(worker_1, nullptr);
  const uint64_t w0_requests = counterValue(*worker_0, "benchmark.http_2xx");
  const uint64_t w1_requests = counterValue(*worker_1, "benchmark.http_2xx");
  EXPECT_GT(w1_requests, 0u) << "removed worker's cumulative counters lost";
  EXPECT_GT(w0_requests, w1_requests);
}

// New workers joining at a later phase are surfaced, never silent: the
// engine-side per-phase subset bookkeeping that feeds PhaseMetadata
// (M2 Task 4 wiring) marks the growth as a worker-count change with a cold
// start, and an unchanged subset marks neither.
TEST_F(PhaseRunTest, ColdStartSurfacedInPhaseMetadata) {
  RunOutcome outcome = runPhases(makeSubsetPhaseOptions(listener().uri(), {1, 2}));
  ASSERT_TRUE(outcome.success);
  ASSERT_EQ(2u, outcome.phase_worker_subsets.size());
  EXPECT_EQ(1u, outcome.phase_worker_subsets[0].worker_count);
  EXPECT_FALSE(outcome.phase_worker_subsets[0].workers_changed_vs_prior_phase);
  EXPECT_FALSE(outcome.phase_worker_subsets[0].new_workers_cold_started);
  EXPECT_EQ(2u, outcome.phase_worker_subsets[1].worker_count);
  EXPECT_TRUE(outcome.phase_worker_subsets[1].workers_changed_vs_prior_phase);
  EXPECT_TRUE(outcome.phase_worker_subsets[1].new_workers_cold_started)
      << "cold start of the joining worker not surfaced";
}

// After a removed worker's final partial is folded, the live aggregation
// excludes it without losing its contribution: counter sums stay monotonic
// across the boundary and match the cumulative expectations at the end (no
// double-count of the final partial).
TEST_F(PhaseRunTest, LiveAggregationIgnoresIdleWorkers) {
  nighthawk::client::CommandLineOptions cmd = makeSubsetPhaseOptions(listener().uri(), {2, 1});
  cmd.mutable_live_streaming();
  cmd.mutable_stats_flush_interval()->set_value(1);
  RunOutcome outcome = runPhases(cmd, /*sample_live_snapshots=*/true);
  ASSERT_TRUE(outcome.success);

  std::vector<uint64_t> totals;
  {
    std::lock_guard<std::mutex> lock(outcome_mutex);
    for (const nighthawk::client::LiveSnapshot& snapshot : outcome.live_snapshots) {
      auto it = snapshot.counters().find("benchmark.http_2xx");
      if (it != snapshot.counters().end()) {
        totals.push_back(it->second);
      }
    }
  }
  ASSERT_GT(totals.size(), 2u);
  for (size_t i = 1; i < totals.size(); ++i) {
    EXPECT_GE(totals[i], totals[i - 1])
        << "live counter sum decreased at sample " << i
        << " (removed worker double-counted or dropped)";
  }

  // The final live total matches the cumulative per-worker counters of the
  // final output (the removed worker counted exactly once).
  const nighthawk::client::Result* worker_0 = findResult(outcome.output, "worker_0");
  const nighthawk::client::Result* worker_1 = findResult(outcome.output, "worker_1");
  ASSERT_NE(worker_0, nullptr);
  ASSERT_NE(worker_1, nullptr);
  const uint64_t output_total =
      counterValue(*worker_0, "benchmark.http_2xx") + counterValue(*worker_1, "benchmark.http_2xx");
  EXPECT_GT(output_total, 0u);
  EXPECT_GE(totals.back(), output_total * 9 / 10);
  EXPECT_LE(totals.back(), output_total + output_total / 10)
      << "live aggregation diverged from the cumulative counters (fold double-counted)";
}

// Phase begins are same-instant across the active subset (the documented
// ruling: the legacy bootstrap-time inter-worker stagger does not apply at
// phase boundaries). Both workers of a [1, 2] vector run the final phase, so
// their per-worker result groups carry that phase's first rate-limiter
// acquisition time; the two must be near-simultaneous. The bound is tens of
// milliseconds -- at this run's rps the legacy stagger it guards against
// would be ~10ms, so a wider bound could not fail.
TEST_F(PhaseRunTest, SubsetStartIsSameInstant) {
  RunOutcome outcome = runPhases(makeSubsetPhaseOptions(listener().uri(), {1, 2}));
  ASSERT_TRUE(outcome.success);
  const nighthawk::client::Result* worker_0 = findResult(outcome.output, "worker_0");
  const nighthawk::client::Result* worker_1 = findResult(outcome.output, "worker_1");
  ASSERT_NE(worker_0, nullptr);
  ASSERT_NE(worker_1, nullptr);
  ASSERT_TRUE(worker_0->has_execution_start());
  ASSERT_TRUE(worker_1->has_execution_start());
  const int64_t t0 = Envoy::Protobuf::util::TimeUtil::TimestampToNanoseconds(
      worker_0->execution_start());
  const int64_t t1 = Envoy::Protobuf::util::TimeUtil::TimestampToNanoseconds(
      worker_1->execution_start());
  const int64_t spread_ns = t1 > t0 ? t1 - t0 : t0 - t1;
  EXPECT_LT(spread_ns, std::chrono::duration_cast<std::chrono::nanoseconds>(50ms).count())
      << "new worker's phase start not same-instant with the survivor's";
}

// A worker retired before it ever began a phase (never-begun, outside phase
// 0's prefix) has its live snapshot timer stopped; when a later phase
// re-activates it, the timer resumes and its partials flow again -- its
// contribution reaches the live aggregation, stamped with its first phase.
// Without the resume, the final live total would sit near the survivor-only
// level (~half the run's completions here) instead of the full amount.
TEST_F(PhaseRunTest, RejoinedWorkerResumesLivePartials) {
  nighthawk::client::CommandLineOptions cmd = makeSubsetPhaseOptions(listener().uri(), {1, 2});
  cmd.mutable_live_streaming();
  cmd.mutable_stats_flush_interval()->set_value(1);
  RunOutcome outcome = runPhases(cmd, /*sample_live_snapshots=*/true);
  ASSERT_TRUE(outcome.success);

  const nighthawk::client::Result* worker_0 = findResult(outcome.output, "worker_0");
  const nighthawk::client::Result* worker_1 = findResult(outcome.output, "worker_1");
  ASSERT_NE(worker_0, nullptr);
  ASSERT_NE(worker_1, nullptr);
  const uint64_t output_total =
      counterValue(*worker_0, "benchmark.http_2xx") + counterValue(*worker_1, "benchmark.http_2xx");
  // worker_0 ran both phases (~300 completions), worker_1 only phase 1
  // (~100): the cumulative total covers the whole run.
  EXPECT_GT(output_total, 0u);

  uint64_t last_live_total = 0;
  {
    std::lock_guard<std::mutex> lock(outcome_mutex);
    for (const nighthawk::client::LiveSnapshot& snapshot : outcome.live_snapshots) {
      auto it = snapshot.counters().find("benchmark.http_2xx");
      if (it != snapshot.counters().end()) {
        last_live_total = it->second;
      }
    }
  }
  // The rejoined worker's phase-1 traffic reaches the live aggregation:
  // the final total covers the survivor's two phases plus the joiner's one
  // (>= 3/4 of the cumulative total; a missing resume caps it near 2/3).
  EXPECT_GE(last_live_total, output_total * 3 / 4)
      << "rejoined worker's live partials did not resume after re-activation";
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

// 8.6 counted-not-hidden (fix round): a FULLY CENSORED boundary still
// completes and streams at the next phase start — the one case that must
// never vanish. The recorded drain is the full budget (capped, never
// zero-silent) and the idle gap is measured from the censoring deadline.
TEST(PhaseOrchestratorTest, CensoredBoundaryStillEmitsAtNextPhaseStart) {
  std::vector<PhaseEvent> events;
  PhaseOrchestrator orchestrator(
      twoPhaseSpecs(), [&events](const PhaseEvent& event) { events.push_back(event); });
  std::promise<PhaseDrainAck> never_satisfied;
  std::vector<std::future<PhaseDrainAck>> acks;
  acks.push_back(never_satisfied.get_future());

  ASSERT_FALSE(orchestrator.collectDrainAcks(acks, std::chrono::milliseconds(100), 0));
  // The drain-complete emit carries no boundary.
  ASSERT_EQ(1, events.size());
  EXPECT_FALSE(events[0].boundary_complete);

  orchestrator.markNextPhaseStarted(1);
  ASSERT_EQ(2, events.size());
  EXPECT_TRUE(events[1].boundary_complete);
  EXPECT_EQ(events[1].ending_phase_index, 0u);
  EXPECT_EQ(events[1].ending_phase_id, "phase0");
  EXPECT_EQ(events[1].phase_index, 1u);
  EXPECT_EQ(events[1].phase_id, "phase1");
  // The capped drain: the full budget, never zero.
  EXPECT_EQ(events[1].drain_duration_ns,
            std::chrono::duration_cast<std::chrono::nanoseconds>(100ms).count());
  // The gap is the deadline-anchored lower bound (small but recorded).
  EXPECT_GE(events[1].idle_gap_ns, 0);
  EXPECT_LT(events[1].idle_gap_ns, std::chrono::nanoseconds(2s).count());
  EXPECT_TRUE(events[1].boundary_censored);
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

// ---------------------------------------------------------------------------
// FORK-ONLY (We-Amp): per-phase output dimension (M2 Task 4).
// ---------------------------------------------------------------------------

const nighthawk::client::Statistic* findStatistic(const nighthawk::client::Result& result,
                                                  absl::string_view id) {
  for (const nighthawk::client::Statistic& statistic : result.statistics()) {
    if (statistic.id() == id) {
      return &statistic;
    }
  }
  return nullptr;
}

const nighthawk::client::Result* findPhaseResult(const nighthawk::client::Output& output,
                                                 absl::string_view phase_id) {
  for (const nighthawk::client::Result& result : output.results()) {
    if (result.phase_id() == phase_id) {
      return &result;
    }
  }
  return nullptr;
}

// Latency statistics are serialized in the duration domain; return the raw
// nanoseconds of the smallest emitted percentile that covers `fraction`
// (HdrHistogram emits percentile steps at 1 - 1/2^n, not the literal value).
uint64_t percentileNanos(const nighthawk::client::Statistic& statistic, double fraction) {
  std::optional<uint64_t> best;
  for (const nighthawk::client::Percentile& p : statistic.percentiles()) {
    if (p.percentile() < fraction) {
      continue;
    }
    const uint64_t value = p.has_duration()
                               ? static_cast<uint64_t>(p.duration().seconds()) * 1000000000ull +
                                     p.duration().nanos()
                               : static_cast<uint64_t>(p.raw_value());
    if (!best.has_value() || value < *best) {
      best = value;
    }
  }
  return best.value_or(0);
}

// The plan's freshness test, end to end: phase 2 runs against an SUT with a
// 50 ms response delay (flipped in the phase-start event, before its first
// request is scheduled), so the phase slabs carry distinct latency profiles.
// Per-phase percentiles must differ correctly, and each phase's Result group
// must reflect only its own slab.
TEST_F(PhaseRunTest, PhaseStatisticsAreFreshPerBoundary) {
  nighthawk::client::CommandLineOptions cmd = makeTwoPhaseOptions(listener().uri());
  listener().setResponseDelayNs(0);
  RunOutcome outcome = runPhases(cmd, /*sample_live_snapshots=*/false,
                                 [this](const PhaseEvent& event) {
                                   if (event.phase_id == "phase1") {
                                     listener().setResponseDelayNs(50 * 1000 * 1000);
                                   }
                                 });
  ASSERT_TRUE(outcome.success);
  listener().setResponseDelayNs(0);

  const nighthawk::client::Result* r0 = findPhaseResult(outcome.output, "phase0");
  const nighthawk::client::Result* r1 = findPhaseResult(outcome.output, "phase1");
  ASSERT_NE(r0, nullptr) << "no result group for phase0";
  ASSERT_NE(r1, nullptr) << "no result group for phase1";
  const nighthawk::client::Statistic* lat0 =
      findStatistic(*r0, "benchmark_http_client.latency_2xx");
  const nighthawk::client::Statistic* lat1 =
      findStatistic(*r1, "benchmark_http_client.latency_2xx");
  ASSERT_NE(lat0, nullptr) << "phase0 result carries no benchmark-client latency statistics";
  ASSERT_NE(lat1, nullptr) << "phase1 result carries no benchmark-client latency statistics";
  // Fresh slabs: each phase's samples are its own.
  EXPECT_GT(lat0->count(), 0);
  EXPECT_GT(lat1->count(), 0);
  // Distinct profiles: the delayed phase's p99 sits near 50 ms, the warm
  // phase's near the loopback floor.
  EXPECT_GT(percentileNanos(*lat1, 0.99), 40 * 1000 * 1000);
  EXPECT_LT(percentileNanos(*lat0, 0.99), 10 * 1000 * 1000);
}

// Per-phase Results carry their phase id, the metadata array aligns 1:1 with
// the phase vector, and pool health has one entry per active worker with
// created >= active (spec 8.6 surfacing).
TEST_F(PhaseRunTest, PerPhaseResultsCarryPhaseId) {
  RunOutcome outcome = runPhases(makeTwoPhaseOptions(listener().uri()));
  ASSERT_TRUE(outcome.success);

  ASSERT_NE(findPhaseResult(outcome.output, "phase0"), nullptr);
  ASSERT_NE(findPhaseResult(outcome.output, "phase1"), nullptr);
  // No result outside the phase groups claims a phase id.
  for (const nighthawk::client::Result& result : outcome.output.results()) {
    if (absl::StartsWith(result.name(), "phase_")) {
      EXPECT_FALSE(result.phase_id().empty());
    } else {
      EXPECT_TRUE(result.phase_id().empty());
    }
  }

  // The global Result conserves samples across phases: its benchmark-client
  // counts equal the sum over the phase groups (the run-total fold reads the
  // retained slabs, not the post-finalize live sets).
  const nighthawk::client::Result* global_result = nullptr;
  for (const nighthawk::client::Result& result : outcome.output.results()) {
    if (result.name() == "global") {
      global_result = &result;
    }
  }
  ASSERT_NE(global_result, nullptr);
  const nighthawk::client::Statistic* global_lat =
      findStatistic(*global_result, "benchmark_http_client.latency_2xx");
  ASSERT_NE(global_lat, nullptr) << "global result carries no benchmark-client samples";
  uint64_t phase_sum = 0;
  for (const char* phase_id : {"phase0", "phase1"}) {
    const nighthawk::client::Statistic* phase_lat =
        findStatistic(*findPhaseResult(outcome.output, phase_id),
                      "benchmark_http_client.latency_2xx");
    ASSERT_NE(phase_lat, nullptr);
    phase_sum += phase_lat->count();
  }
  EXPECT_GT(phase_sum, 0);
  EXPECT_EQ(phase_sum, global_lat->count())
      << "global sample count does not conserve across the phase slabs";

  ASSERT_EQ(2, outcome.output.phase_metadata_size());
  for (int i = 0; i < outcome.output.phase_metadata_size(); ++i) {
    const nighthawk::client::PhaseMetadata& metadata = outcome.output.phase_metadata(i);
    EXPECT_EQ(fmt::format("phase{}", i), metadata.phase_id());
    EXPECT_EQ(static_cast<uint32_t>(i), metadata.phase_index());
    EXPECT_EQ(1u, metadata.worker_count());
    EXPECT_FALSE(metadata.workers_changed_vs_prior_phase());
    EXPECT_FALSE(metadata.new_workers_cold_started());
    ASSERT_EQ(1, metadata.pool_health_size());
    EXPECT_EQ(0u, metadata.pool_health(0).worker_id());
    // Strictly > 0: the run connected at least once, so a renamed or missing
    // engine pool counter (stat-name drift) fails loudly instead of silently
    // reading as a warm transfer.
    EXPECT_GT(metadata.pool_health(0).connections_created(), 0u);
    EXPECT_GE(metadata.pool_health(0).connections_created(),
              metadata.pool_health(0).connections_active());
    // Claim discipline, uncensored run: everything initiated completed.
    EXPECT_GT(metadata.initiated(), 0u);
    EXPECT_EQ(metadata.initiated(), metadata.completed());
    EXPECT_EQ(0u, metadata.reset_or_timed_out());
    EXPECT_DOUBLE_EQ(1.0, metadata.completion_ratio());
    EXPECT_EQ(0u, metadata.gate_exceeded());
  }
  // Phase 0's boundary drain timing is recorded; the final phase ends at the
  // run teardown and carries no drain duration.
  EXPECT_GE(outcome.output.phase_metadata(0).drain_duration().seconds() * 1000000000 +
                outcome.output.phase_metadata(0).drain_duration().nanos(),
            0);
  EXPECT_GE(outcome.output.phase_metadata(0).idle_gap().seconds(), 0);
  EXPECT_EQ(0, outcome.output.phase_metadata(1).drain_duration().seconds());
  EXPECT_EQ(0, outcome.output.phase_metadata(1).drain_duration().nanos());
}

// A live snapshot after the boundary carries percentiles computed from the
// NEW slab: the band resets at the boundary instead of mixing. The delay flip
// makes the new slab's p99 an order of magnitude above the old one's.
TEST_F(PhaseRunTest, LiveStreamResetsBandAtBoundary) {
  nighthawk::client::CommandLineOptions cmd = makeTwoPhaseOptions(listener().uri());
  cmd.mutable_live_streaming();
  cmd.mutable_stats_flush_interval()->set_value(1);
  listener().setResponseDelayNs(0);
  RunOutcome outcome = runPhases(cmd, /*sample_live_snapshots=*/true,
                                 [this](const PhaseEvent& event) {
                                   if (event.phase_id == "phase1") {
                                     listener().setResponseDelayNs(50 * 1000 * 1000);
                                   }
                                 });
  ASSERT_TRUE(outcome.success);
  listener().setResponseDelayNs(0);

  uint64_t max_phase0_p99 = 0;
  uint64_t min_phase1_p99 = std::numeric_limits<uint64_t>::max();
  bool saw_phase1_statistic = false;
  std::lock_guard<std::mutex> lock(outcome_mutex);
  for (const nighthawk::client::LiveSnapshot& snapshot : outcome.live_snapshots) {
    for (const nighthawk::client::StatisticSnapshot& statistic : snapshot.statistics()) {
      if (statistic.id() != "benchmark_http_client.latency_2xx" || statistic.count() == 0) {
        continue;
      }
      if (snapshot.phase_id() == "phase0") {
        max_phase0_p99 = std::max(max_phase0_p99, statistic.p99_ns());
      } else if (snapshot.phase_id() == "phase1") {
        saw_phase1_statistic = true;
        min_phase1_p99 = std::min(min_phase1_p99, statistic.p99_ns());
      }
    }
  }
  ASSERT_TRUE(saw_phase1_statistic) << "no live snapshot with phase1 latency statistics";
  EXPECT_LT(max_phase0_p99, 10 * 1000 * 1000);
  EXPECT_GT(min_phase1_p99, 40 * 1000 * 1000)
      << "post-boundary snapshots carry the pre-boundary band (mixed slabs)";
}

// Claim discipline (spec 8.2): censoring is counted, not hidden. Unit seam:
// BuildPhaseMetadata with a censored boundary turns the in-flight delta into
// reset_or_timed_out and pushes completion_ratio below 1; an uncensored
// boundary leaves the clean counts untouched.
TEST(BuildPhaseMetadataTest, DrainCensoringCountedNotHidden) {
  nighthawk::client::PhaseSpec spec;
  spec.set_name("phase0");

  PhaseBoundaryTiming censored;
  censored.phase_index = 0;
  censored.phase_id = "phase0";
  censored.drain_duration_ns = int64_t{5000} * 1000000; // the full 5 s deadline
  censored.idle_gap_ns = 1000000;
  censored.drain_censored = true;
  const nighthawk::client::PhaseMetadata censored_metadata =
      BuildPhaseMetadata(0, spec, &censored, PhaseWorkerSubset{/*worker_count=*/1},
                         /*initiated=*/100,
                         /*completed=*/97, /*stream_resets=*/1, /*gate_exceeded=*/3,
                         /*inherited_stragglers=*/0);
  EXPECT_EQ(4u, censored_metadata.reset_or_timed_out()); // 1 reset + 3 hung in flight
  EXPECT_EQ(100u, censored_metadata.initiated());
  EXPECT_EQ(97u, censored_metadata.completed());
  EXPECT_NEAR(0.97, censored_metadata.completion_ratio(), 1e-9);
  EXPECT_EQ(3u, censored_metadata.gate_exceeded());
  EXPECT_EQ(censored.drain_duration_ns,
            censored_metadata.drain_duration().seconds() * 1000000000 +
                censored_metadata.drain_duration().nanos());
  EXPECT_EQ(censored.idle_gap_ns,
            censored_metadata.idle_gap().seconds() * 1000000000 +
                censored_metadata.idle_gap().nanos());
  // The censoring mark rides into the metadata (fork-only field): the
  // streamed boundary event carries it live, this stamp keeps it alive in
  // the post-run metadata (the TUI's post-run strip rebuild reads it).
  EXPECT_TRUE(censored_metadata.drain_censored());

  // Uncensored boundary: in-flight reached zero, nothing extra is counted.
  PhaseBoundaryTiming clean;
  clean.phase_index = 0;
  clean.phase_id = "phase0";
  clean.drain_duration_ns = 1000;
  clean.idle_gap_ns = 2000;
  clean.drain_censored = false;
  const nighthawk::client::PhaseMetadata clean_metadata =
      BuildPhaseMetadata(0, spec, &clean, PhaseWorkerSubset{/*worker_count=*/1}, 100, 100, 1,
                         /*gate_exceeded=*/2,
                         /*inherited_stragglers=*/0);
  EXPECT_EQ(1u, clean_metadata.reset_or_timed_out());
  EXPECT_EQ(2u, clean_metadata.gate_exceeded());
  EXPECT_DOUBLE_EQ(1.0, clean_metadata.completion_ratio());
  EXPECT_FALSE(clean_metadata.drain_censored());

  // The run's final phase has no boundary timing at all.
  const nighthawk::client::PhaseMetadata final_metadata =
      BuildPhaseMetadata(1, spec, /*boundary=*/nullptr, PhaseWorkerSubset{/*worker_count=*/1},
                         50, 50, 0, /*gate_exceeded=*/0,
                         /*inherited_stragglers=*/0);
  EXPECT_EQ(0, final_metadata.drain_duration().seconds());
  EXPECT_EQ(0, final_metadata.drain_duration().nanos());
  EXPECT_EQ(0, final_metadata.idle_gap().seconds());
  EXPECT_DOUBLE_EQ(1.0, final_metadata.completion_ratio());
}

// Straggler semantics at a censored boundary (spec 8.2, chosen reading): the
// predecessor counts the hung in-flight set in its reset_or_timed_out -- even
// when the stragglers complete later -- and the successor excludes that same
// set from its completed count, so nothing is counted twice and no ratio
// reads above 1.
TEST(BuildPhaseMetadataTest, StragglerCompletionsAreExcludedFromTheSuccessor) {
  nighthawk::client::PhaseSpec spec;
  spec.set_name("phase1");
  // The successor phase's own boundary is clean; its raw completed delta is
  // inflated by the 3 stragglers the censored phase0 boundary left in flight
  // (10 completions observed, only 8 requests initiated by this phase).
  PhaseBoundaryTiming clean;
  clean.phase_index = 1;
  clean.phase_id = "phase1";
  clean.drain_duration_ns = 1000;
  clean.idle_gap_ns = 2000;
  clean.drain_censored = false;
  const nighthawk::client::PhaseMetadata successor = BuildPhaseMetadata(
      1, spec, &clean, PhaseWorkerSubset{/*worker_count=*/1}, /*initiated=*/8,
      /*completed=*/10, /*stream_resets=*/0,
      /*gate_exceeded=*/0, /*inherited_stragglers=*/3);
  EXPECT_EQ(7u, successor.completed()); // 10 raw - 3 stragglers
  EXPECT_EQ(8u, successor.initiated());
  EXPECT_EQ(0u, successor.reset_or_timed_out());
  EXPECT_NEAR(0.875, successor.completion_ratio(), 1e-9);
  EXPECT_LE(successor.completion_ratio(), 1.0);

  // A raw delta wholly made of stragglers floors at zero instead of
  // underflowing; the ratio then honestly reads 0 (the phase's only own
  // request did not complete).
  const nighthawk::client::PhaseMetadata floored =
      BuildPhaseMetadata(1, spec, &clean, PhaseWorkerSubset{/*worker_count=*/1},
                         /*initiated=*/1, /*completed=*/3,
                         /*stream_resets=*/0, /*gate_exceeded=*/0,
                         /*inherited_stragglers=*/3);
  EXPECT_EQ(0u, floored.completed());
  EXPECT_DOUBLE_EQ(0.0, floored.completion_ratio());

  // And the paired predecessor side: the same 3 stragglers are what phase0's
  // censored boundary already counted (1 reset + 3 hung = 4 total there).
  nighthawk::client::PhaseSpec predecessor_spec;
  predecessor_spec.set_name("phase0");
  PhaseBoundaryTiming censored;
  censored.phase_index = 0;
  censored.phase_id = "phase0";
  censored.drain_censored = true;
  const nighthawk::client::PhaseMetadata predecessor = BuildPhaseMetadata(
      0, predecessor_spec, &censored, PhaseWorkerSubset{/*worker_count=*/1},
      /*initiated=*/10, /*completed=*/7, /*stream_resets=*/1,
      /*gate_exceeded=*/0, /*inherited_stragglers=*/0);
  EXPECT_EQ(4u, predecessor.reset_or_timed_out());
  EXPECT_EQ("phase0", predecessor.phase_id());
}

// ---------------------------------------------------------------------------
// FORK-ONLY (We-Amp): worker subsets (M2 Task 6).
// ---------------------------------------------------------------------------

// --- M2 Task 6: the pure worker-subset bookkeeping, without a run. ---

std::vector<nighthawk::client::PhaseSpec>
subsetSpecs(const std::vector<std::optional<uint32_t>>& concurrencies) {
  std::vector<nighthawk::client::PhaseSpec> phases;
  for (size_t i = 0; i < concurrencies.size(); ++i) {
    nighthawk::client::PhaseSpec phase;
    phase.set_name(fmt::format("phase{}", i));
    phase.mutable_duration()->set_seconds(2);
    phase.set_requests_per_second(50);
    if (concurrencies[i].has_value()) {
      phase.mutable_concurrency()->set_value(fmt::format("{}", concurrencies[i].value()));
    }
    phases.push_back(phase);
  }
  return phases;
}

TEST(WorkerSubsetTest, SubsetsTrackChangeAndColdStart) {
  // [2, 4, 2]: growth at phase 1 is a cold start; the shrink at phase 2 is a
  // change without a cold start.
  const std::vector<PhaseWorkerSubset> subsets =
      computeWorkerSubsets(subsetSpecs({2, 4, 2}), /*bootstrapped_workers=*/4);
  ASSERT_EQ(3, subsets.size());
  EXPECT_EQ(2, subsets[0].worker_count);
  EXPECT_FALSE(subsets[0].workers_changed_vs_prior_phase);
  EXPECT_FALSE(subsets[0].new_workers_cold_started);
  EXPECT_EQ(4, subsets[1].worker_count);
  EXPECT_TRUE(subsets[1].workers_changed_vs_prior_phase);
  EXPECT_TRUE(subsets[1].new_workers_cold_started);
  EXPECT_EQ(2, subsets[2].worker_count);
  EXPECT_TRUE(subsets[2].workers_changed_vs_prior_phase);
  EXPECT_FALSE(subsets[2].new_workers_cold_started);
}

TEST(WorkerSubsetTest, RejoiningWorkersAreNotColdStarts) {
  // [4, 2, 4]: the phase-2 growth stays within the high-water mark -- those
  // workers rejoined on preserved pools, so the flag stays false.
  const std::vector<PhaseWorkerSubset> subsets =
      computeWorkerSubsets(subsetSpecs({4, 2, 4}), /*bootstrapped_workers=*/4);
  ASSERT_EQ(3, subsets.size());
  EXPECT_FALSE(subsets[1].new_workers_cold_started);
  EXPECT_TRUE(subsets[1].workers_changed_vs_prior_phase);
  EXPECT_TRUE(subsets[2].workers_changed_vs_prior_phase);
  EXPECT_FALSE(subsets[2].new_workers_cold_started);
}

TEST(WorkerSubsetTest, UndeclaredConcurrencyInheritsBootstrapCount) {
  const std::vector<PhaseWorkerSubset> subsets =
      computeWorkerSubsets(subsetSpecs({std::nullopt, 1}), /*bootstrapped_workers=*/3);
  ASSERT_EQ(2, subsets.size());
  EXPECT_EQ(3, subsets[0].worker_count);
  EXPECT_FALSE(subsets[0].workers_changed_vs_prior_phase);
  EXPECT_FALSE(subsets[0].new_workers_cold_started);
  EXPECT_EQ(1, subsets[1].worker_count);
  EXPECT_TRUE(subsets[1].workers_changed_vs_prior_phase);
  EXPECT_FALSE(subsets[1].new_workers_cold_started);
}

} // namespace

} // namespace Client
} // namespace Nighthawk
