#include <atomic>
#include <chrono>
#include <thread>

#include <grpc++/grpc++.h>

#include "test/test_common/environment.h"
#include "test/test_common/utility.h"

#include "source/client/client.h"
#include "source/client/output_collector_impl.h"
#include "source/client/remote_process_impl.h"
#include "source/client/service_impl.h"

#include "test/client/utility.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;
using namespace testing;

namespace Nighthawk {
namespace Client {

namespace {

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response (same pattern as test/service_test.cc / test/tui_run_feed_test.cc):
// it gives the remote-cancellation test's benchmark live upstream traffic so
// the run is genuinely in flight when the arm lands.
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
    // shutdown() reliably wakes a blocked accept(), close() alone may not.
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  TestHttpListener(const TestHttpListener&) = delete;
  TestHttpListener& operator=(const TestHttpListener&) = delete;

  std::string uri() const { return fmt::format("http://127.0.0.1:{}/", port_); }

  // Number of upstream connections the benchmark client has made: the
  // in-flight signal this test anchors its cancellation timing on (the
  // remote path strips live snapshot streaming, so there is no snapshot to
  // anchor on).
  int connections() const { return connections_.load(std::memory_order_relaxed); }

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
      connections_.fetch_add(1, std::memory_order_relaxed);
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
    while (sent < data.size()) {
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
  std::atomic<int> connections_{0};
};

} // namespace

class ClientTest : public testing::Test {};

// TODO(https://github.com/envoyproxy/nighthawk/issues/179): revisit this, and improve testability
// of the Main class, so we can mock its dependencies. We now have integration tests covering this
// much better.

// Note: these tests do not have a backend set up to talk to.
// That's why we expect exit codes indicating failure.
TEST_F(ClientTest, NormalRun) {
  Main program(Nighthawk::Client::TestUtility::createOptionsImpl(
      "foo --duration 1 --rps 10 http://localhost:63657/"));

  EXPECT_FALSE(program.run());
}

TEST_F(ClientTest, AutoConcurrencyRun) {
  // See the comment in TestUtility::createOptionsImpl()
  TCLAP::OptionalUnlabeledTracker::alreadyOptional() = false;

  std::vector<const char*> argv;
  argv.push_back("foo");
  argv.push_back("--concurrency");
  argv.push_back("auto");
  argv.push_back("--duration");
  argv.push_back("1");
  argv.push_back("--rps");
  argv.push_back("1");
  argv.push_back("--verbosity");
  argv.push_back("error");
  argv.push_back("--simple-warmup");
  argv.push_back("http://localhost:63657/");
  Main program(argv.size(), argv.data());
  EXPECT_FALSE(program.run());
}

// TODO(https://github.com/envoyproxy/nighthawk/issues/140):
// This is just for coverage, and we do not care where any traffic we send ends it or what that
// looks like. We do functional testing in python now, but unfortunately any code we hit there isn't
// counted as code-coverage. Ideally, the code hit during the python test runs would count for
// coverage, and we use unit-tests here to hit any edge cases we can't easily hit otherwise.
TEST_F(ClientTest, TracingRun) {
  // See the comment in TestUtility::createOptionsImpl()
  TCLAP::OptionalUnlabeledTracker::alreadyOptional() = false;

  std::vector<const char*> argv;
  argv.push_back("foo");
  argv.push_back("--duration");
  argv.push_back("5");
  argv.push_back("--rps");
  argv.push_back("10");
  argv.push_back("--verbosity");
  argv.push_back("error");
  argv.push_back("http://localhost:63657/");
  argv.push_back("--trace");
  argv.push_back("zipkin://localhost:9411/api/v2/spans");
  Main program(argv.size(), argv.data());
  EXPECT_FALSE(program.run());
}

TEST_F(ClientTest, BadRun) {
  Main program(Nighthawk::Client::TestUtility::createOptionsImpl(
      "foo --duration 1 --rps 1 https://unresolveable.host/"));
  EXPECT_FALSE(program.run());
}

TEST_F(ClientTest, BadRemoteRun) {
  Main program(Nighthawk::Client::TestUtility::createOptionsImpl(
      "foo --duration 1 --nighthawk-service grpc://unresolveable.host:8843 --rps 1 "
      "http://localhost:63657/"));
  EXPECT_FALSE(program.run());
}

TEST_F(ClientTest, RemoteRun) {
  Main program(Nighthawk::Client::TestUtility::createOptionsImpl(
      "foo --duration 1 --nighthawk-service grpc://localhost:8843 --rps 1 "
      "http://localhost:63657/"));
  EXPECT_FALSE(program.run());
}

// M3 Task 7 (#380): the remote process's requestExecutionCancellation() sends
// the CancellationRequest arm on the in-flight run's stream — the run ends
// promptly and its final output is still delivered to run()'s blocking read
// (the arm keeps the stream alive; today's logged no-op is gone).
TEST_F(ClientTest, RemoteCancellationSendsArm) {
  TestHttpListener listener;
  std::unique_ptr<ServiceImpl> service = std::make_unique<ServiceImpl>();
  grpc::ServerBuilder builder;
  int grpc_server_port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_server_port);
  builder.RegisterService(service.get());
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  auto channel = grpc::CreateChannel(fmt::format("127.0.0.1:{}", grpc_server_port),
                                     grpc::InsecureChannelCredentials());
  auto stub = nighthawk::client::NighthawkService::NewStub(channel);
  auto options = TestUtility::createOptionsImpl(fmt::format(
      "foo --duration 10 --rps 10 --concurrency 1 --verbosity error --nighthawk-service "
      "grpc://127.0.0.1:{} {}",
      grpc_server_port, listener.uri()));

  Envoy::Event::RealTimeSystem time_system; // NO_CHECK_FORMAT(real_time)
  OutputCollectorImpl collector(time_system, *options);
  RemoteProcessImpl process(*options, *stub);

  bool run_ok = false;
  std::thread run_thread([&process, &collector, &run_ok]() {
    run_ok = process.run(collector);
  });
  // Anchor on live upstream traffic (the feed tests' snapshot-anchored
  // shape, adapted: the remote path strips live snapshot streaming, so the
  // SUT listener's connection count is the in-flight signal) — a bounded
  // wait, so the arm lands demonstrably mid-run at any machine speed.
  const auto traffic_deadline = std::chrono::steady_clock::now() + 10s;
  while (listener.connections() == 0) {
    ASSERT_LT(std::chrono::steady_clock::now(), traffic_deadline)
        << "the benchmark never reached the upstream listener";
    std::this_thread::sleep_for(10ms);
  }

  const auto cancel_time = std::chrono::steady_clock::now();
  EXPECT_TRUE(process.requestExecutionCancellation())
      << "the remote cancellation must send the arm while the run is in flight";
  run_thread.join();
  const auto elapsed = std::chrono::steady_clock::now() - cancel_time;

  EXPECT_TRUE(run_ok);
  // The arm stops the run promptly and the final output still arrives (the
  // single-response contract survives cancellation).
  EXPECT_LT(elapsed, 2s);
  EXPECT_GT(collector.toProto().results_size(), 0);
}

} // namespace Client
} // namespace Nighthawk
