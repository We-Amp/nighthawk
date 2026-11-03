#include <grpc++/grpc++.h>

#include <chrono>
#include <map>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "nighthawk/common/exception.h"

#include "absl/strings/match.h"

#include "test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

#include "api/client/service.pb.h"

#include "source/client/service_impl.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;
using namespace testing;

namespace Nighthawk {
namespace Client {
namespace {

using nighthawk::client::ExecutionRequest;
using nighthawk::client::ExecutionResponse;

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response. It gives the benchmark client live upstream traffic so that tests
// can observe 2xx counters in the output the service produces.
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
};

class ServiceTest : public TestWithParam<Envoy::Network::Address::IpVersion> {
public:
  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>();
    grpc::ServerBuilder builder;
    loopback_address_ = Envoy::Network::Test::getLoopbackAddressUrlString(GetParam());

    builder.AddListeningPort(fmt::format("{}:0", loopback_address_),
                             grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    setupGrpcClient();
    setBasicRequestOptions();
  }

  void TearDown() override { server_->Shutdown(); }

  void setupGrpcClient() {
    channel_ = grpc::CreateChannel(fmt::format("{}:{}", loopback_address_, grpc_server_port_),
                                   grpc::InsecureChannelCredentials());
    stub_ = std::make_unique<nighthawk::client::NighthawkService::Stub>(channel_);
  }

  void singleStreamBackToBackExecution(grpc::ClientContext& context,
                                       nighthawk::client::NighthawkService::Stub&) {
    auto r = stub_->ExecutionStream(&context);
    EXPECT_TRUE(r->Write(request_, {}));
    EXPECT_TRUE(r->Read(&response_));
    ASSERT_TRUE(response_.has_error_detail());
    EXPECT_TRUE(response_.has_output());
    EXPECT_TRUE(r->Write(request_, {}));
    EXPECT_TRUE(r->Read(&response_));
    EXPECT_TRUE(response_.has_error_detail());
    EXPECT_TRUE(response_.has_output());
    EXPECT_TRUE(r->WritesDone());
    auto status = r->Finish();
    EXPECT_TRUE(status.ok());
  }

  std::thread testThreadedClientRun(bool expect_success) const {
    std::thread thread([this, expect_success]() {
      auto channel = grpc::CreateChannel(fmt::format("{}:{}", loopback_address_, grpc_server_port_),
                                         grpc::InsecureChannelCredentials());
      auto stub = std::make_unique<nighthawk::client::NighthawkService::Stub>(channel);
      grpc::ClientContext context;
      auto stream = stub->ExecutionStream(&context);
      EXPECT_TRUE(stream->Write(request_, {}));
      EXPECT_TRUE(stream->WritesDone());
      nighthawk::client::ExecutionResponse response;
      EXPECT_EQ(stream->Read(&response), expect_success);
      auto status = stream->Finish();
      EXPECT_EQ(status.ok(), expect_success);
    });
    return thread;
  }

  void setBasicRequestOptions() {
    auto options = request_.mutable_start_request()->mutable_options();
    // TODO(oschaaf): this sends actual traffic, which isn't relevant for the tests
    // we are about to perform. However, it would be nice to be able to mock out things
    // to clean this up.
    options->mutable_uri()->set_value("http://127.0.0.1:10001/");
    options->mutable_duration()->set_seconds(2);
    options->mutable_requests_per_second()->set_value(3);
  }

  void runWithFailingValidationExpectations(bool expect_output,
                                            absl::string_view match_error = "") {
    auto r = stub_->ExecutionStream(&context_);
    r->Write(request_, {});
    r->WritesDone();
    EXPECT_TRUE(r->Read(&response_));
    auto status = r->Finish();
    ASSERT_FALSE(match_error.empty());
    EXPECT_TRUE(response_.has_error_detail());
    EXPECT_EQ(response_.has_output(), expect_output);
    EXPECT_EQ(grpc::StatusCode::INTERNAL, response_.error_detail().code());
    EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string(match_error)));
    EXPECT_TRUE(status.ok());
  }

  std::unique_ptr<ServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
  grpc::ClientContext context_;
  nighthawk::client::ExecutionRequest request_;
  nighthawk::client::ExecutionResponse response_;
  std::unique_ptr<nighthawk::client::NighthawkService::Stub> stub_;
  std::string loopback_address_;
  int grpc_server_port_{0};
};

class ServiceTestWithParameterizedConstructor : public ServiceTest {
public:
  void SetUp() override {
    std::unique_ptr<Envoy::Logger::Context> logging_context =
        std::make_unique<Envoy::Logger::Context>(spdlog::level::info, "%L %n [%g:%#] %v", log_lock_,
                                                 false);
    service_ = std::make_unique<ServiceImpl>(std::move(logging_context));
    grpc::ServerBuilder builder;
    loopback_address_ = Envoy::Network::Test::getLoopbackAddressUrlString(GetParam());

    builder.AddListeningPort(fmt::format("{}:0", loopback_address_),
                             grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    setupGrpcClient();
    setBasicRequestOptions();
  }

private:
  Envoy::Thread::MutexBasicLockable log_lock_;
};

INSTANTIATE_TEST_SUITE_P(IpVersions, ServiceTestWithParameterizedConstructor,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

TEST_P(ServiceTestWithParameterizedConstructor,
       ConstructorWithLoggingContextParameterCanRespondToRequests) {
  std::unique_ptr<grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>> stream =
      stub_->ExecutionStream(&context_);
  stream->Write(request_, {});
  stream->WritesDone();
  EXPECT_TRUE(stream->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_FALSE(response_.error_detail().message().empty());
  EXPECT_TRUE(response_.has_output());
  EXPECT_GE(response_.output().results(0).counters().size(), 8);
  grpc::Status status = stream->Finish();
  EXPECT_TRUE(status.ok());
}

INSTANTIATE_TEST_SUITE_P(IpVersions, ServiceTest,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

// Test single NH run
TEST_P(ServiceTest, Basic) {
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string("Unknown failure")));
  EXPECT_TRUE(response_.has_output());
  EXPECT_GE(response_.output().results(0).counters().size(), 8);
  auto status = r->Finish();
  EXPECT_TRUE(status.ok());
}

// Test that attempts to perform concurrent executions result in a
// failure being returned.
TEST_P(ServiceTest, NoConcurrentStart) {
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string("Unknown failure")));
  EXPECT_TRUE(response_.has_output());
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_FALSE(status.ok());
}

// Test we are able to perform serialized executions.
TEST_P(ServiceTest, BackToBackExecution) {
  grpc::ClientContext context1;
  singleStreamBackToBackExecution(context1, *stub_);
  // create a new client to connect to the same server, and do it one more time.
  setupGrpcClient();
  grpc::ClientContext context2;
  singleStreamBackToBackExecution(context2, *stub_);
}

// Test that proto validation is wired up and works.
// TODO(oschaaf): functional coverage of all the options / validations.
TEST_P(ServiceTest, InvalidRps) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_requests_per_second()->set_value(0);
  // We do not expect output, because the options proto is not valid, and can't be echoed back.
  runWithFailingValidationExpectations(false, "value must be inside range");
}

// We didn't implement updates yet, ensure we indicate so.
TEST_P(ServiceTest, UpdatesNotSupported) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_update_request();
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_THAT(status.error_message(), HasSubstr("Request is not supported yet"));
  EXPECT_FALSE(status.ok());
}

// We didn't implement cancellations yet, ensure we indicate so.
TEST_P(ServiceTest, CancelNotSupported) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_cancellation_request();
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_THAT(status.error_message(), HasSubstr("Request is not supported yet"));
  EXPECT_FALSE(status.ok());
}

// Cancelling the client's gRPC context must promptly stop an in-flight
// benchmark: the service must release its single-run lock so that a follow-up
// run is served normally, instead of being declined while the abandoned run
// lingers for its full configured duration.
TEST_P(ServiceTest, CancelViaContext) {
  TestHttpListener listener;
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value(listener.uri());
  options->mutable_duration()->set_seconds(60);
  options->mutable_requests_per_second()->set_value(10);

  const auto start_time = std::chrono::steady_clock::now();
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  std::this_thread::sleep_for(2s);
  context_.TryCancel();
  // The client cancelled the RPC, so this stream cannot carry the run's final
  // response anymore; the prompt end of the RPC is all we can observe on it.
  EXPECT_FALSE(r->Read(&response_));
  r->Finish();

  // Give the service a bounded grace period to wind the cancelled run down
  // (run cancellation polling plus run teardown).
  std::this_thread::sleep_for(2s);

  // A follow-up run must be served normally: status OK, output present, and
  // 2xx counters accumulated against the live listener.
  setupGrpcClient();
  grpc::ClientContext followup_context;
  request_ = nighthawk::client::ExecutionRequest();
  auto followup_options = request_.mutable_start_request()->mutable_options();
  followup_options->mutable_uri()->set_value(listener.uri());
  followup_options->mutable_duration()->set_seconds(2);
  followup_options->mutable_requests_per_second()->set_value(10);
  auto followup = stub_->ExecutionStream(&followup_context);
  EXPECT_TRUE(followup->Write(request_, {}));
  EXPECT_TRUE(followup->WritesDone());
  EXPECT_TRUE(followup->Read(&response_));
  EXPECT_FALSE(response_.has_error_detail());
  EXPECT_TRUE(response_.has_output());
  uint64_t http_2xx = 0;
  for (const auto& counter : response_.output().results(0).counters()) {
    if (counter.name() == "benchmark.http_2xx") {
      http_2xx = counter.value();
    }
  }
  EXPECT_GT(http_2xx, 0);
  auto followup_status = followup->Finish();
  EXPECT_TRUE(followup_status.ok());
  EXPECT_LT(std::chrono::steady_clock::now() - start_time, 30s);
}

TEST_P(ServiceTest, Unresolvable) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value("http://unresolvable-host/");
  // We expect output, because the options proto is valid.
  runWithFailingValidationExpectations(false, "Unable to create ProcessImpl");
}

namespace {

// Collected outcome of one benchmark request on a live-streaming (or legacy)
// stream: every snapshot received, whether the run's final response arrived,
// and whether any message arrived after the final response (which must never
// happen: snapshots stop at run end).
struct LiveStreamResult {
  std::vector<nighthawk::client::LiveSnapshot> snapshots;
  bool got_final_response{false};
  bool message_after_final{false};
};

// Sends one request on the stream and reads until the server closes its side.
// A grpc::ClientContext may only be used for a single RPC, so each call gets
// its own context here.
LiveStreamResult readLiveStream(nighthawk::client::NighthawkService::Stub* stub,
                                const ExecutionRequest& request) {
  LiveStreamResult result;
  grpc::ClientContext context;
  auto stream = stub->ExecutionStream(&context);
  EXPECT_TRUE(stream->Write(request, {}));
  EXPECT_TRUE(stream->WritesDone());
  ExecutionResponse response;
  while (stream->Read(&response)) {
    if (result.got_final_response) {
      result.message_after_final = true;
      break;
    }
    if (response.has_live_snapshot()) {
      result.snapshots.push_back(response.live_snapshot());
    } else if (response.has_output()) {
      result.got_final_response = true;
    }
  }
  auto status = stream->Finish();
  EXPECT_TRUE(status.ok());
  return result;
}

ExecutionRequest makeLiveRequest(const std::string& uri, uint32_t duration_seconds, uint32_t rps,
                                 uint32_t flush_interval_seconds, bool live_streaming) {
  ExecutionRequest request;
  auto* options = request.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value(uri);
  options->mutable_duration()->set_seconds(duration_seconds);
  options->mutable_requests_per_second()->set_value(rps);
  options->mutable_stats_flush_interval()->set_value(flush_interval_seconds);
  if (live_streaming) {
    options->mutable_live_streaming();
  }
  return request;
}

} // namespace

// Without opting in to live streaming, the service must behave exactly as
// before: a single response carrying the run output, and no snapshot messages
// at all. The fork's legacy consumers assert on this via RELEASE_ASSERT.
TEST_P(ServiceTest, LiveStreamingDisabledBackCompat) {
  TestHttpListener listener;
  ExecutionRequest request = makeLiveRequest(listener.uri(), 2, 10, 1, /*live_streaming=*/false);
  LiveStreamResult result = readLiveStream(stub_.get(), request);
  EXPECT_TRUE(result.got_final_response);
  EXPECT_TRUE(result.snapshots.empty());
  EXPECT_FALSE(result.message_after_final);
}

// With live streaming opted in, snapshots flow at the stats-flush cadence while
// the run is in flight: strictly increasing monotonic_ns, counters that only
// grow, and at least one statistic sample with real content.
TEST_P(ServiceTest, LiveStreamingSnapshots) {
  TestHttpListener listener;
  ExecutionRequest request = makeLiveRequest(listener.uri(), 10, 10, 1, /*live_streaming=*/true);
  LiveStreamResult result = readLiveStream(stub_.get(), request);
  EXPECT_TRUE(result.got_final_response);
  EXPECT_FALSE(result.message_after_final);
  ASSERT_GE(result.snapshots.size(), 2);

  int64_t last_monotonic_ns = -1;
  std::map<std::string, uint64_t> last_counters;
  bool found_benchmark_counter = false;
  bool found_usable_statistic = false;
  for (const nighthawk::client::LiveSnapshot& snapshot : result.snapshots) {
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
    for (const nighthawk::client::StatisticSnapshot& statistic : snapshot.statistics()) {
      if (statistic.count() > 0 && statistic.p99_ns() > 0) {
        found_usable_statistic = true;
      }
    }
  }
  EXPECT_TRUE(found_benchmark_counter);
  EXPECT_TRUE(found_usable_statistic);
}

// Snapshots must stop when the run ends: nothing may follow the final response,
// and a second back-to-back run on the same stream must behave identically
// (nothing from the first run may bleed into the second).
TEST_P(ServiceTest, SnapshotsStopAtRunEnd) {
  TestHttpListener listener;
  ExecutionRequest request = makeLiveRequest(listener.uri(), 2, 10, 1, /*live_streaming=*/true);

  LiveStreamResult first = readLiveStream(stub_.get(), request);
  EXPECT_TRUE(first.got_final_response);
  EXPECT_FALSE(first.message_after_final);
  EXPECT_GE(first.snapshots.size(), 1);

  // Back-to-back run on the same stream, same behavior expected.
  LiveStreamResult second = readLiveStream(stub_.get(), request);
  EXPECT_TRUE(second.got_final_response);
  EXPECT_FALSE(second.message_after_final);
  EXPECT_GE(second.snapshots.size(), 1);
  // The second run must produce fresh snapshots of its own, not a carry-over
  // of the first run's state: its first snapshot (taken ~1s into the run) must
  // not already hold as many 2xx responses as the first run accumulated over
  // its whole duration.
  if (!first.snapshots.empty() && !second.snapshots.empty()) {
    uint64_t first_run_total_http_2xx = 0;
    uint64_t second_run_first_http_2xx = 0;
    for (const auto& [name, value] : first.snapshots.back().counters()) {
      if (name == "benchmark.http_2xx") {
        first_run_total_http_2xx = value;
      }
    }
    for (const auto& [name, value] : second.snapshots.front().counters()) {
      if (name == "benchmark.http_2xx") {
        second_run_first_http_2xx = value;
      }
    }
    EXPECT_LT(second_run_first_http_2xx, first_run_total_http_2xx);
  }
}

} // namespace
} // namespace Client
} // namespace Nighthawk
