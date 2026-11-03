#include <grpc++/grpc++.h>

#include <netinet/in.h>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/strings/match.h"

#include "api/client/service.grpc.pb.h"
#include "api/client/service.pb.h"

#include "source/client/service_impl.h"
#include "source/client/tui/run_feed.h"

#include "test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response, giving the feed's benchmark real upstream traffic. Same pattern as
// test/live_probe_test.cc.
class TestHttpListener {
public:
  TestHttpListener() {
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

  ~TestHttpListener() {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  TestHttpListener(const TestHttpListener&) = delete;
  TestHttpListener& operator=(const TestHttpListener&) = delete;

  std::string uri() const { return fmt::format("http://127.0.0.1:{}/", port_); }

private:
  static constexpr int kNoSignalFlags =
#ifdef MSG_NOSIGNAL
      MSG_NOSIGNAL;
#else
      0;
#endif

  void acceptLoop() {
    while (true) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        return;
      }
#ifdef SO_NOSIGPIPE
      int one = 1;
      ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
      serve(fd);
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
    while (sent < size_t(data.size())) {
      ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, kNoSignalFlags);
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
};

// Records every callback invocation on the feed's read thread, so tests can
// assert the delivery order (snapshots ... completed, finished last) and the
// exact firing counts.
struct FeedRecorder {
  void arm() {
    RunFeedCallbacks callbacks;
    callbacks.on_snapshot = [this](const nighthawk::client::LiveSnapshot& snapshot) {
      std::lock_guard<std::mutex> lock(mutex);
      events.push_back("snapshot");
      snapshots.push_back(snapshot);
      cv.notify_all();
    };
    callbacks.on_completed = [this](const nighthawk::client::Output& output) {
      std::lock_guard<std::mutex> lock(mutex);
      events.push_back("completed");
      completed = true;
      final_output = output;
      cv.notify_all();
    };
    callbacks.on_finished = [this](const absl::Status& status) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        events.push_back("finished");
        finished_count++;
        finished_status = status;
        if (stop_from_finished) {
          finished_entered = true;
        }
        cv.notify_all();
      }
      // Re-entrancy stress for the deadlock regression: call stop() from
      // inside on_finished (the one sanctioned re-entrant call), delayed so a
      // concurrent start() is already holding/waiting on the feed lock first.
      if (stop_from_finished && feed != nullptr) {
        std::this_thread::sleep_for(100ms);
        feed->stop();
      }
    };
    feed_callbacks_ = std::move(callbacks);
  }

  const RunFeedCallbacks& callbacks() const { return feed_callbacks_; }

  // Waits until `predicate` (called with the recorder locked) holds, or the
  // budget expires. Returns false on timeout.
  bool waitFor(absl::Duration budget, const std::function<bool()>& predicate) {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, std::chrono::nanoseconds(absl::ToChronoNanoseconds(budget)),
                       predicate);
  }

  // When set, on_finished signals finished_entered and then calls stop() on
  // `feed` (the deadlock regression's re-entrant call).
  bool stop_from_finished = false;
  RunFeed* feed = nullptr;
  bool finished_entered = false;
  bool second_start_returned = false;
  absl::Status second_start_status = absl::UnknownError("not returned");

  std::mutex mutex;
  std::condition_variable cv;
  std::vector<std::string> events;
  std::vector<nighthawk::client::LiveSnapshot> snapshots;
  bool completed = false;
  nighthawk::client::Output final_output;
  int finished_count = 0;
  absl::Status finished_status = absl::UnknownError("not fired");

private:
  RunFeedCallbacks feed_callbacks_;
};

class RunFeedTest : public testing::Test {
public:
  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>();
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    channel_ = grpc::CreateChannel(serviceAddress(), grpc::InsecureChannelCredentials());
  }

  void TearDown() override { server_->Shutdown(); }

  std::string serviceAddress() const { return fmt::format("127.0.0.1:{}", grpc_server_port_); }

  const std::shared_ptr<grpc::ChannelInterface>& channel() const { return channel_; }

  static nighthawk::client::StartRequest makeStartRequest(const std::string& uri, uint32_t rps,
                                                          uint32_t duration_seconds,
                                                          uint32_t stats_flush_interval) {
    nighthawk::client::StartRequest request;
    auto* options = request.mutable_options();
    options->mutable_uri()->set_value(uri);
    options->mutable_requests_per_second()->set_value(rps);
    options->mutable_duration()->set_seconds(duration_seconds);
    options->mutable_stats_flush_interval()->set_value(stats_flush_interval);
    return request;
  }

  // Runs one 2s benchmark on a fresh feed against the in-proc service and
  // returns whether it completed naturally with output. Used to prove the
  // service released its single-run lock after a cancelled run (the M0a
  // CancelViaContext follow-up-run pattern); a declined run surfaces as a
  // finish without completion, so the caller retries.
  bool runFollowUpAndCheckCompleted() {
    for (int attempt = 0; attempt < 20; attempt++) {
      FeedRecorder followup;
      followup.arm();
      RunFeed followup_feed(channel(), followup.callbacks());
      EXPECT_EQ(followup_feed.start(makeStartRequest(listener_->uri(), 10, 2, 1)),
                absl::OkStatus());
      followup.waitFor(absl::Seconds(30), [&followup]() { return followup.finished_count > 0; });
      if (followup.completed) {
        return true;
      }
      // The service was still winding down the cancelled run and declined this
      // one; give it a moment and try again.
      std::this_thread::sleep_for(500ms);
    }
    return false;
  }

protected:
  TestHttpListener& listener() {
    if (!listener_) {
      listener_ = std::make_unique<TestHttpListener>();
    }
    return *listener_;
  }

private:
  std::unique_ptr<ServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  int grpc_server_port_{0};
  std::shared_ptr<grpc::ChannelInterface> channel_;
  std::unique_ptr<TestHttpListener> listener_;
};

// The happy path: a run with live streaming opted in delivers snapshots at the
// stats-flush cadence to on_snapshot, then the run's final output to
// on_completed, then exactly one OK on_finished — in that order, all on the
// feed's read thread.
TEST_F(RunFeedTest, SnapshotsDeliveredToCallbacks) {
  FeedRecorder recorder;
  recorder.arm();
  RunFeed feed(channel(), recorder.callbacks());

  EXPECT_EQ(feed.running(), false);
  EXPECT_EQ(feed.start(makeStartRequest(listener().uri(), /*rps=*/10, /*duration_seconds=*/4,
                                        /*stats_flush_interval=*/1)),
            absl::OkStatus());
  EXPECT_EQ(feed.running(), true);

  // A 4s run with 1s flushes must deliver at least two snapshots plus the
  // completion and finish events, well within a 30s budget.
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(30), [&recorder]() { return recorder.finished_count > 0; }));

  std::lock_guard<std::mutex> lock(recorder.mutex);
  EXPECT_EQ(feed.running(), false);
  EXPECT_EQ(recorder.finished_count, 1);
  EXPECT_TRUE(recorder.finished_status.ok()) << recorder.finished_status.ToString();

  // Order: snapshots, then completed, then finished exactly last.
  ASSERT_GE(recorder.events.size(), 4u);
  EXPECT_EQ(recorder.events.back(), "finished");
  EXPECT_EQ(recorder.events[recorder.events.size() - 2], "completed");
  for (size_t i = 0; i + 2 < recorder.events.size(); i++) {
    EXPECT_EQ(recorder.events[i], "snapshot");
  }

  // Snapshot content: strictly increasing monotonic_ns, and benchmark counters
  // that never go backwards (cumulative from run start).
  ASSERT_GE(recorder.snapshots.size(), 2);
  int64_t last_monotonic_ns = -1;
  std::map<std::string, uint64_t> last_counters;
  bool found_benchmark_counter = false;
  for (const nighthawk::client::LiveSnapshot& snapshot : recorder.snapshots) {
    EXPECT_GT(snapshot.monotonic_ns(), last_monotonic_ns);
    last_monotonic_ns = snapshot.monotonic_ns();
    for (const auto& [name, value] : snapshot.counters()) {
      if (absl::StartsWith(name, "benchmark.")) {
        found_benchmark_counter = true;
        auto it = last_counters.find(name);
        if (it != last_counters.end()) {
          EXPECT_GE(value, it->second) << "counter " << name << " went backwards";
        }
        last_counters[name] = value;
      }
    }
  }
  EXPECT_TRUE(found_benchmark_counter);

  // The completion callback carries the run's final output.
  EXPECT_GT(recorder.final_output.results_size(), 0);
}

// Stopping a run cancels the RPC: the feed ends promptly with a single
// on_finished (cancelled status), on_completed is never called, and the
// service releases its run lock so a follow-up run is served normally.
TEST_F(RunFeedTest, StopCancelsRun) {
  FeedRecorder recorder;
  recorder.arm();
  RunFeed feed(channel(), recorder.callbacks());

  EXPECT_EQ(feed.start(makeStartRequest(listener().uri(), /*rps=*/10, /*duration_seconds=*/60,
                                        /*stats_flush_interval=*/1)),
            absl::OkStatus());

  // Wait for the first snapshot so the run is demonstrably in flight.
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(10), [&recorder]() { return !recorder.snapshots.empty(); }));

  feed.stop();
  feed.stop(); // Idempotent.

  // grpc delivers nothing after a client cancel, so the run's end must arrive
  // as on_finished promptly (within 5s), never as on_completed.
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(5), [&recorder]() { return recorder.finished_count > 0; }));

  {
    std::lock_guard<std::mutex> lock(recorder.mutex);
    EXPECT_EQ(recorder.finished_count, 1);
    EXPECT_FALSE(recorder.finished_status.ok());
    EXPECT_EQ(recorder.finished_status.code(), absl::StatusCode::kCancelled);
    EXPECT_FALSE(recorder.completed);
    ASSERT_FALSE(recorder.events.empty());
    EXPECT_EQ(recorder.events.back(), "finished");
    for (const std::string& event : recorder.events) {
      EXPECT_NE(event, "completed");
    }
  }
  EXPECT_EQ(feed.running(), false);

  // The run ended server-side too: after the cancellation watchdog winds the
  // abandoned run down, a follow-up run is accepted and completes with output
  // (the CancelViaContext pattern from the M0a service tests).
  EXPECT_TRUE(runFollowUpAndCheckCompleted());

  // No callback may arrive after the cancel settled the feed.
  std::this_thread::sleep_for(200ms);
  std::lock_guard<std::mutex> lock(recorder.mutex);
  EXPECT_EQ(recorder.finished_count, 1);
  EXPECT_FALSE(recorder.completed);
}

// A second start() while a run is in flight is rejected with
// kFailedPrecondition and leaves the in-flight run untouched.
TEST_F(RunFeedTest, StartRejectedWhileRunning) {
  FeedRecorder recorder;
  recorder.arm();
  RunFeed feed(channel(), recorder.callbacks());

  EXPECT_EQ(feed.start(makeStartRequest(listener().uri(), /*rps=*/10, /*duration_seconds=*/4,
                                        /*stats_flush_interval=*/1)),
            absl::OkStatus());

  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(10), [&recorder]() { return !recorder.snapshots.empty(); }));

  absl::Status second = feed.start(makeStartRequest(listener().uri(), 10, 4, 1));
  EXPECT_EQ(second.code(), absl::StatusCode::kFailedPrecondition) << second.ToString();

  // The first run must still complete naturally.
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(30), [&recorder]() { return recorder.finished_count > 0; }));
  std::lock_guard<std::mutex> lock(recorder.mutex);
  EXPECT_EQ(recorder.finished_count, 1);
  EXPECT_TRUE(recorder.finished_status.ok());
  EXPECT_TRUE(recorder.completed);
  EXPECT_EQ(recorder.events.back(), "finished");
  EXPECT_EQ(recorder.events[recorder.events.size() - 2], "completed");
  EXPECT_GT(recorder.final_output.results_size(), 0);
}

// Deadlock regression: stop() is the one sanctioned re-entrant call from
// on_finished, and a start() may race with that callback (running() is already
// false when on_finished fires). start() must therefore not hold the feed lock
// while joining the old read thread, or start -> join -> on_finished -> stop
// -> lock would deadlock. This test delays a re-entrant stop() inside
// on_finished and calls start() concurrently; the pre-fix code hangs here.
TEST_F(RunFeedTest, StopFromFinishedDoesNotDeadlockConcurrentStart) {
  FeedRecorder recorder;
  recorder.stop_from_finished = true;
  recorder.arm();
  RunFeed feed(channel(), recorder.callbacks());
  recorder.feed = &feed;

  // A 1s run: it completes naturally, putting the read thread inside
  // on_finished while running() already reads false.
  EXPECT_EQ(feed.start(makeStartRequest(listener().uri(), /*rps=*/10, /*duration_seconds=*/1,
                                        /*stats_flush_interval=*/1)),
            absl::OkStatus());
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(15), [&recorder]() { return recorder.finished_entered; }));

  // Concurrent with the delayed re-entrant stop() inside on_finished: a
  // follow-up start(). Pre-fix it blocks forever on the lock the joining
  // start() holds.
  std::thread second_start([&]() {
    absl::Status status = feed.start(makeStartRequest(listener().uri(), 10, 2, 1));
    {
      std::lock_guard<std::mutex> lock(recorder.mutex);
      recorder.second_start_status = status;
      recorder.second_start_returned = true;
    }
    recorder.cv.notify_all();
  });
  if (!recorder.waitFor(absl::Seconds(10),
                        [&recorder]() { return recorder.second_start_returned; })) {
    ADD_FAILURE() << "start() deadlocked behind a re-entrant stop() from on_finished";
    second_start.detach();
    // A deadlocked start() cannot be cleaned up (its thread holds the feed
    // lock forever); fail the binary now rather than hang teardown.
    std::abort();
  }
  second_start.join();
  {
    std::lock_guard<std::mutex> lock(recorder.mutex);
    EXPECT_EQ(recorder.second_start_status.code(), absl::StatusCode::kOk)
        << recorder.second_start_status.ToString();
  }

  // The follow-up run launched by the second start() completes naturally.
  ASSERT_TRUE(
      recorder.waitFor(absl::Seconds(30), [&recorder]() { return recorder.finished_count >= 2; }));
  std::lock_guard<std::mutex> lock(recorder.mutex);
  EXPECT_GE(recorder.finished_count, 2);
  EXPECT_TRUE(recorder.completed);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
