#include <grpc++/grpc++.h>

#include <netinet/in.h>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "absl/strings/str_split.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "api/client/service.pb.h"

#include "source/client/live_probe.h"
#include "source/client/service_impl.h"

#include "test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace {

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response, giving the probe's benchmark real upstream traffic.
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

class LiveProbeTest : public testing::Test {
public:
  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>();
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
  }

  void TearDown() override { server_->Shutdown(); }

  std::string serviceAddress() const { return fmt::format("127.0.0.1:{}", grpc_server_port_); }

private:
  std::unique_ptr<ServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  int grpc_server_port_{0};
};

// In-proc smoke test: the probe streams JSON-line snapshots of a live run to
// its output stream, followed by a final line carrying the run's output.
TEST_F(LiveProbeTest, StreamsJsonSnapshots) {
  TestHttpListener listener;
  std::vector<std::string> args = {
      "nighthawk_live_probe", "--address", serviceAddress(), "--uri", listener.uri(), "--rps", "10",
      "--duration",           "2",
  };

  std::ostringstream out;
  std::vector<const char*> argv;
  argv.reserve(args.size());
  for (const std::string& arg : args) {
    argv.push_back(arg.c_str());
  }
  int rc = runLiveProbe(static_cast<int>(argv.size()), argv.data(), out);
  EXPECT_EQ(rc, 0);

  std::vector<std::string> lines;
  for (absl::string_view line : absl::StrSplit(out.str(), '\n', absl::SkipEmpty())) {
    lines.push_back(std::string(line));
  }
  ASSERT_GE(lines.size(), 2);

  nighthawk::client::LiveSnapshot snapshot;
  size_t snapshot_lines = 0;
  for (size_t i = 0; i + 1 < lines.size(); i++) {
    EXPECT_TRUE(google::protobuf::util::JsonStringToMessage(lines[i], &snapshot).ok())
        << "line is not a parseable snapshot: " << lines[i];
    snapshot_lines++;
    EXPECT_GT(snapshot.monotonic_ns(), 0);
  }
  EXPECT_GE(snapshot_lines, 1);

  // The last line carries the final run output.
  nighthawk::client::Output final_output;
  EXPECT_TRUE(google::protobuf::util::JsonStringToMessage(lines.back(), &final_output).ok())
      << "final line is not a parseable output: " << lines.back();
  EXPECT_GT(final_output.results_size(), 0);
}

} // namespace
} // namespace Client
} // namespace Nighthawk
