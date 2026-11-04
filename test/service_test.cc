#include <grpc++/grpc++.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "nighthawk/common/exception.h"

#include "absl/strings/match.h"
#include "absl/time/time.h"

#include "test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

#include "api/client/service.pb.h"
#include "api/rate_limiter/linear_ramping_rate_limiter.pb.h"

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

// Test double for the update applier (injected via
// ServiceImpl::SetUpdateApplierFactoryForTest): blocks until released or its
// deadline — honoring the deadline is the applier contract the orchestrator's
// bounded join relies on — then returns an applied=true result.
class BlockingTestApplier : public LiveUpdateOrchestrator::Applier {
public:
  nighthawk::client::UpdateResult
  ApplyDeltas(const nighthawk::client::UpdateRequest& /*request*/,
              const std::string& epoch_id, const EngineInstant& bend_instant,
              absl::Time deadline) override {
    while (!released_.load() && absl::Now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    nighthawk::client::UpdateResult result;
    result.set_applied(true);
    result.set_epoch_id(epoch_id);
    result.set_wall_time_unix_ns(bend_instant.wall_time_unix_ns);
    result.set_monotonic_ns(bend_instant.monotonic_ns);
    return result;
  }
  void Release() { released_.store(true); }
  bool Released() const { return released_.load(); }

private:
  std::atomic<bool> released_{false};
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

  // Starts a run against `listener` (live snapshot streaming opted in) and
  // returns the open stream; the run stays in flight until consumed.
  std::unique_ptr<grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>>
  startLiveRun(const TestHttpListener& listener, uint32_t seconds, uint32_t rps);

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

ExecutionRequest makeLiveRequest(const std::string& uri, uint32_t duration_seconds, uint32_t rps,
                                 uint32_t flush_interval_seconds, bool live_streaming,
                                 uint32_t concurrency = 1) {
  ExecutionRequest request;
  auto* options = request.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value(uri);
  options->mutable_duration()->set_seconds(duration_seconds);
  options->mutable_requests_per_second()->set_value(rps);
  options->mutable_stats_flush_interval()->set_value(flush_interval_seconds);
  options->mutable_concurrency()->set_value(fmt::format("{}", concurrency));
  if (live_streaming) {
    options->mutable_live_streaming();
  }
  return request;
}

std::unique_ptr<grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>>
ServiceTest::startLiveRun(const TestHttpListener& listener, uint32_t seconds, uint32_t rps) {
  request_ = makeLiveRequest(listener.uri(), seconds, rps, 1, /*live_streaming=*/true);
  auto stream = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(stream->Write(request_, {}));
  return stream;
}

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

// The reject-and-teardown behavior survives only for "no run in flight": an
// update arm with no active run tears the stream down with a clear message.
TEST_P(ServiceTest, UpdateWithoutRunInFlightTearsDown) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(5);
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.error_message(), HasSubstr("No benchmark run is in flight"));
}

// Same for the cancellation arm.
TEST_P(ServiceTest, CancellationWithoutRunInFlightTearsDown) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_cancellation_request();
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.error_message(), HasSubstr("No benchmark run is in flight"));
}

// Review Focus 5: an M2-shaped run with no update/cancel arm sent behaves
// byte-identically to pre-M3: snapshots flow, the final output is the LAST
// response on the stream, and the stream finishes OK.
TEST_P(ServiceTest, NoArmsIsLegacyRun) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 2, 10);
  bool saw_output = false;
  while (r->Read(&response_)) {
    EXPECT_FALSE(response_.has_update_result());
    if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_output);
  EXPECT_FALSE(response_.has_error_detail());
  // Half-close BEFORE the final read: the server's handler stays in Read()
  // until the client half-closes, so the reads must be ordered the other way.
  EXPECT_TRUE(r->WritesDone());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// Review Focus 5: an UpdateRequest mid-run no longer tears the stream down —
// it acks a structured result and the run keeps flowing snapshots to natural
// completion.
TEST_P(ServiceTest, UpdateArmDoesNotTearDownStream) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 4, 10);
  std::this_thread::sleep_for(1s);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(
      5);
  ASSERT_TRUE(r->Write(update_request, {}));
  bool saw_ack = false;
  bool saw_output = false;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      saw_ack = true;
      EXPECT_FALSE(response_.update_result().applied());
      EXPECT_FALSE(response_.update_result().rejection_reason().empty());
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    } else {
      EXPECT_TRUE(response_.has_live_snapshot());
    }
  }
  EXPECT_TRUE(saw_ack);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
}

// Targeting validation at the routing layer: a mismatched execution_id acks a
// structured rejection and the stream (and run) survive.
TEST_P(ServiceTest, ExecutionIdMismatchRejected) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 3, 10);
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  auto* update = update_request.mutable_update_request();
  update->set_execution_id("wrong-execution-id");
  update->add_deltas()->mutable_requests_per_second()->set_value(5);
  ASSERT_TRUE(r->Write(update_request, {}));
  bool saw_ack = false;
  bool saw_output = false;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      saw_ack = true;
      EXPECT_FALSE(response_.update_result().applied());
      EXPECT_THAT(response_.update_result().rejection_reason(),
                  HasSubstr("execution_id"));
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_ack);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
}

// Same for a mismatched phase_id.
TEST_P(ServiceTest, PhaseIdMismatchRejected) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 3, 10);
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  auto* update = update_request.mutable_update_request();
  update->set_phase_id("no-such-phase");
  update->add_deltas()->mutable_requests_per_second()->set_value(5);
  ASSERT_TRUE(r->Write(update_request, {}));
  bool saw_ack = false;
  bool saw_output = false;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      saw_ack = true;
      EXPECT_FALSE(response_.update_result().applied());
      EXPECT_THAT(response_.update_result().rejection_reason(), HasSubstr("phase_id"));
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_ack);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
}

// An update with no deltas is rejected with a structured reason.
TEST_P(ServiceTest, EmptyDeltasRejected) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 3, 10);
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request();
  ASSERT_TRUE(r->Write(update_request, {}));
  bool saw_ack = false;
  bool saw_output = false;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      saw_ack = true;
      EXPECT_FALSE(response_.update_result().applied());
      EXPECT_THAT(response_.update_result().rejection_reason(), HasSubstr("no deltas"));
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_ack);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
}

// Whitelist validation against the ENGINE-built run shape, end to end (M3
// Task 2). On a linear-ramping-plugin run: a ramp time exceeding the run's
// remaining duration is rejected with the bound reason, and a ramp delta
// that fits passes validation and is forwarded to the apply path (whose
// stub's "not yet supported" reason is distinguishable from any whitelist
// rejection). A validator never consulted would have produced the stub
// reason for BOTH deltas. The stream and run survive every rejection.
TEST_P(ServiceTest, RampDeltaValidatedAgainstRunShape) {
  TestHttpListener listener;
  // A 3s ramp-plugin run (the plugin's own 2s ramp time fits the duration,
  // the factory's construction-time backstop).
  ExecutionRequest start_request;
  auto* options = start_request.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value(listener.uri());
  options->mutable_duration()->set_seconds(3);
  options->mutable_requests_per_second()->set_value(10);
  options->mutable_stats_flush_interval()->set_value(1);
  options->mutable_concurrency()->set_value("1");
  options->mutable_live_streaming();
  auto* plugin = options->mutable_rate_limiter_plugin_config();
  plugin->set_name("nighthawk.linear-ramping-rate-limiter-plugin");
  nighthawk::rate_limiter::LinearRampingRateLimiterConfig ramp_config;
  ramp_config.mutable_ramp_time()->set_seconds(2);
  if (!plugin->mutable_typed_config()->PackFrom(ramp_config)) {
    ASSERT_TRUE(false) << "failed to pack the ramping plugin config";
  }
  auto r = stub_->ExecutionStream(&context_);
  ASSERT_TRUE(r->Write(start_request, {}));
  std::this_thread::sleep_for(300ms);

  nighthawk::client::ExecutionRequest too_long;
  too_long.mutable_update_request()->add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(
      100); // The run's remaining duration is far smaller.
  ASSERT_TRUE(r->Write(too_long, {}));

  nighthawk::client::ExecutionRequest fits;
  auto* ramp = fits.mutable_update_request()->add_deltas()->mutable_ramp();
  ramp->mutable_ramp_time()->set_seconds(1); // Fits the remaining duration.
  ramp->mutable_target_rps()->set_value(10);
  ASSERT_TRUE(r->Write(fits, {}));

  bool saw_bound_rejection = false;
  bool saw_forwarded_to_stub = false;
  bool saw_output = false;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      const std::string& reason = response_.update_result().rejection_reason();
      EXPECT_FALSE(response_.update_result().applied());
      if (reason.find("ramp_time") != std::string::npos &&
          reason.find("remaining") != std::string::npos) {
        saw_bound_rejection = true;
      }
      if (reason.find("not yet supported") != std::string::npos) {
        saw_forwarded_to_stub = true;
      }
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_bound_rejection);
  EXPECT_TRUE(saw_forwarded_to_stub);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
}

// A rejected update mutates nothing: an out-of-whitelist delta (an unset
// knob arm) is rejected with a structured whitelist reason before any engine
// contact, and the run completes normally afterwards.
TEST_P(ServiceTest, RejectedUpdateMutatesNothing) {
  TestHttpListener listener;
  auto r = startLiveRun(listener, 3, 10);
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas(); // No arm set.
  ASSERT_TRUE(r->Write(update_request, {}));
  bool saw_whitelist_rejection = false;
  bool saw_output = false;
  size_t snapshots_after_rejection = 0;
  while (r->Read(&response_)) {
    if (response_.has_update_result()) {
      EXPECT_FALSE(response_.update_result().applied());
      EXPECT_THAT(response_.update_result().rejection_reason(), HasSubstr("no knob field is set"));
      saw_whitelist_rejection = true;
    } else if (response_.has_output()) {
      saw_output = true;
      break;
    } else if (saw_whitelist_rejection) {
      ASSERT_TRUE(response_.has_live_snapshot());
      snapshots_after_rejection++;
    }
  }
  EXPECT_TRUE(saw_whitelist_rejection);
  EXPECT_GT(snapshots_after_rejection, 0);
  EXPECT_TRUE(saw_output);
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Finish().ok());
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

  // The cancelled run winds down asynchronously (run cancellation polling plus
  // run teardown), so poll for readiness instead of assuming a fixed grace
  // period is always enough: retry the follow-up run every 500ms, up to 10s,
  // and assert only once a follow-up is accepted or the budget expires. The
  // service signals it is still busy by declining the run, which the client
  // observes as Read() returning false.
  std::unique_ptr<grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>> followup;
  // grpc::ClientContext is neither copyable nor assignable, and each RPC needs
  // its own, so the retry loop creates a fresh one per attempt.
  std::unique_ptr<grpc::ClientContext> followup_context;
  const auto readiness_deadline = std::chrono::steady_clock::now() + 10s;
  bool followup_accepted = false;
  while (!followup_accepted) {
    ASSERT_LT(std::chrono::steady_clock::now(), readiness_deadline)
        << "service did not accept a follow-up run within the readiness budget";
    setupGrpcClient();
    followup_context = std::make_unique<grpc::ClientContext>();
    request_ = nighthawk::client::ExecutionRequest();
    auto followup_options = request_.mutable_start_request()->mutable_options();
    followup_options->mutable_uri()->set_value(listener.uri());
    followup_options->mutable_duration()->set_seconds(2);
    followup_options->mutable_requests_per_second()->set_value(10);
    followup = stub_->ExecutionStream(followup_context.get());
    followup_accepted =
        followup->Write(request_, {}) && followup->WritesDone() && followup->Read(&response_);
    if (!followup_accepted) {
      followup->Finish();
      std::this_thread::sleep_for(500ms);
    }
  }

  // A follow-up run must be served normally: status OK, output present, and
  // 2xx counters accumulated against the live listener.
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

// M4 (Review Focus 6): a cancellation queued behind an in-flight update ack
// wait must complete promptly — the bounded ack wait runs OFF the reader
// thread, so the cancellation is dispatched (and the 60s run winds down)
// while the update's applier is still blocked. No update ack may appear on
// the stream after the cancellation was accepted, and the final output stays
// the last response.
TEST_P(ServiceTest, CancellationRequestNotStalledBehindAckWait) {
  TestHttpListener listener;
  BlockingTestApplier* applier_ptr = nullptr;
  service_->SetUpdateApplierFactoryForTest(
      [&applier_ptr]() -> std::unique_ptr<LiveUpdateOrchestrator::Applier> {
        auto applier = std::make_unique<BlockingTestApplier>();
        applier_ptr = applier.get();
        return applier;
      });
  auto r = startLiveRun(listener, 60, 10);
  std::this_thread::sleep_for(500ms);
  // Send an update; from here its ack wait is in flight (the test applier
  // blocks until released or its deadline).
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(
      5);
  ASSERT_TRUE(r->Write(update_request, {}));
  std::this_thread::sleep_for(200ms);
  ASSERT_NE(applier_ptr, nullptr);

  const auto cancel_time = std::chrono::steady_clock::now();
  nighthawk::client::ExecutionRequest cancel_request;
  cancel_request.mutable_cancellation_request();
  ASSERT_TRUE(r->Write(cancel_request, {}));

  bool saw_output = false;
  while (r->Read(&response_)) {
    EXPECT_FALSE(response_.has_update_result())
        << "update ack written after the cancellation was accepted";
    if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_output);
  // The 60s run ended promptly (cancelled, not naturally), and the update's
  // applier was still blocked the whole time: the cancellation was dispatched
  // while the ack wait was in flight.
  const auto elapsed = std::chrono::steady_clock::now() - cancel_time;
  EXPECT_LT(elapsed, 2s);
  ASSERT_NE(applier_ptr, nullptr);
  EXPECT_FALSE(applier_ptr->Released());
  // The final output is the last response ever. Half-close before the final
  // read: the server's handler stays in Read() until the client half-closes.
  EXPECT_TRUE(r->WritesDone());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// A run that unwinds through an exception must leave the in-flight registry
// consistent: an update arm dispatched while the run thread unwinds (busy
// lock still held) is rejected honestly — no ack, no hang, no dangling
// orchestrator dereference — instead of tearing the stream down with a
// success-looking status.
TEST_P(ServiceTest, ArmDispatchAfterRunFailureIsHonestRejection) {
  TestHttpListener listener;
  service_->SetRunFailureInjectionForTest(
      []() { throw NighthawkException("injected run failure"); });
  auto r = startLiveRun(listener, 60, 10);
  // The injected failure fires right before run(); give the unwind (watchdog
  // join, registry guard, orchestrator teardown) time to complete, then
  // dispatch: the arm must be rejected honestly — no ack, stream torn down
  // with a clear failure status. (An arm dispatched DURING the unwind would
  // either land on the aborted-dispatch rejection or, while the run was still
  // alive, on the ordinary rejection ack — both honest; this test pins the
  // settled post-failure state.)
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(
      5);
  ASSERT_TRUE(r->Write(update_request, {}));
  // The arm never acks: the run is gone. The stream is torn down with an
  // honest failure status (either "run failed ..." while the unwind is still
  // in flight, or "no benchmark run is in flight" once the busy lock is
  // released) — never an ack, and never a hang.
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_FALSE(response_.has_update_result());
  auto status = r->Finish();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.error_message(),
              AnyOf(HasSubstr("benchmark run failed"),
                    HasSubstr("No benchmark run is in flight")));
}

// M4 (Review Focus 6): a pending update whose run ends resolves WITHOUT any
// stream write — the final output is the last response, and it is not delayed
// behind the pending ack wait (the bounded join happens after the final
// write).
TEST_P(ServiceTest, PendingUpdateResolvesWithoutWriteAfterRunEnd) {
  TestHttpListener listener;
  BlockingTestApplier* applier_ptr = nullptr;
  service_->SetUpdateApplierFactoryForTest(
      [&applier_ptr]() -> std::unique_ptr<LiveUpdateOrchestrator::Applier> {
        auto applier = std::make_unique<BlockingTestApplier>();
        applier_ptr = applier.get();
        return applier;
      });
  const auto start_time = std::chrono::steady_clock::now();
  auto r = startLiveRun(listener, 2, 10);
  std::this_thread::sleep_for(300ms);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(
      5);
  ASSERT_TRUE(r->Write(update_request, {}));
  ASSERT_NE(applier_ptr, nullptr);

  bool saw_output = false;
  while (r->Read(&response_)) {
    EXPECT_FALSE(response_.has_update_result()) << "update ack written after the run ended";
    if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_output);
  // The 2s run's final output arrived while the applier was still blocked:
  // the pending ack wait never delayed the final output (2s run + ~2s
  // winddown slack; the deadline-delayed case lands at ~5.3s and fails this).
  EXPECT_LT(std::chrono::steady_clock::now() - start_time, 4s);
  EXPECT_FALSE(applier_ptr->Released());
  // Resolve the pending update (it would report applied=true); it must not
  // produce any write after the final output.
  applier_ptr->Release();
  std::this_thread::sleep_for(200ms);
  // Half-close before the final read: the server's handler stays in Read()
  // until the client half-closes; a late ack written despite the gate would
  // surface as a response on this read.
  EXPECT_TRUE(r->WritesDone());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// M4 (Review Focus 6): after a cancellation is accepted, a pending update
// resolves — even as applied=true — WITHOUT any further stream write.
TEST_P(ServiceTest, PendingUpdateResolvesWithoutWriteAfterCancellationAccepted) {
  TestHttpListener listener;
  BlockingTestApplier* applier_ptr = nullptr;
  service_->SetUpdateApplierFactoryForTest(
      [&applier_ptr]() -> std::unique_ptr<LiveUpdateOrchestrator::Applier> {
        auto applier = std::make_unique<BlockingTestApplier>();
        applier_ptr = applier.get();
        return applier;
      });
  auto r = startLiveRun(listener, 60, 10);
  std::this_thread::sleep_for(500ms);
  nighthawk::client::ExecutionRequest update_request;
  update_request.mutable_update_request()->add_deltas()->mutable_requests_per_second()->set_value(
      5);
  ASSERT_TRUE(r->Write(update_request, {}));
  ASSERT_NE(applier_ptr, nullptr);
  nighthawk::client::ExecutionRequest cancel_request;
  cancel_request.mutable_cancellation_request();
  ASSERT_TRUE(r->Write(cancel_request, {}));

  bool saw_output = false;
  while (r->Read(&response_)) {
    EXPECT_FALSE(response_.has_update_result())
        << "update ack written after the cancellation was accepted";
    if (response_.has_output()) {
      saw_output = true;
      break;
    }
  }
  EXPECT_TRUE(saw_output);
  // The 60s run ended promptly: the cancellation took effect while the
  // update's applier was still blocked.
  EXPECT_FALSE(applier_ptr->Released());
  // Let the pending update resolve as applied=true; after the cancellation
  // was accepted it must not produce any stream write.
  applier_ptr->Release();
  std::this_thread::sleep_for(200ms);
  // Half-close before the final read: the server's handler stays in Read()
  // until the client half-closes; a late ack written despite the gate would
  // surface as a response on this read.
  EXPECT_TRUE(r->WritesDone());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
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
// the final response's output (kept so later assertions can bind to the run's
// true final totals rather than to its last snapshot), and whether any message
// arrived after the final response (which must never happen: snapshots stop at
// run end).
struct LiveStreamResult {
  std::vector<nighthawk::client::LiveSnapshot> snapshots;
  bool got_final_response{false};
  nighthawk::client::Output final_output;
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

// Writes one request on an already-open stream and reads responses until the
// run's final response arrives. The stream is deliberately left open afterwards
// (no WritesDone), so a follow-up request can be written on it for a genuine
// back-to-back same-stream run.
LiveStreamResult
readRunFromStream(grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>& stream,
                  const ExecutionRequest& request) {
  LiveStreamResult result;
  EXPECT_TRUE(stream.Write(request, {}));
  ExecutionResponse response;
  while (stream.Read(&response)) {
    if (response.has_live_snapshot()) {
      result.snapshots.push_back(response.live_snapshot());
    } else if (response.has_output()) {
      result.got_final_response = true;
      result.final_output = response.output();
      break;
    }
  }
  return result;
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
// and a second back-to-back run on the SAME stream must behave identically
// (nothing from the first run may bleed into the second). Both runs share one
// ExecutionStream RPC, exercising the service's stream/future reuse path: if
// any snapshot writer or run state survived run 1, it would surface here as a
// stray message or a carry-over of run 1's counters.
TEST_P(ServiceTest, SnapshotsStopAtRunEnd) {
  TestHttpListener listener;
  ExecutionRequest request = makeLiveRequest(listener.uri(), 2, 10, 1, /*live_streaming=*/true);

  auto stream = stub_->ExecutionStream(&context_);
  ASSERT_NE(stream, nullptr);

  // Run 1. Deliberately no WritesDone: the stream must stay open for run 2.
  LiveStreamResult first = readRunFromStream(*stream, request);
  EXPECT_TRUE(first.got_final_response);
  EXPECT_GE(first.snapshots.size(), 1);

  // Run 2, back-to-back on the same stream, same behavior expected.
  LiveStreamResult second = readRunFromStream(*stream, request);
  EXPECT_TRUE(second.got_final_response);
  EXPECT_GE(second.snapshots.size(), 1);

  // The second run must produce fresh snapshots of its own, not a carry-over
  // of the first run's state. Its first snapshot (taken ~1s into the run) must
  // hold strictly fewer 2xx responses than run 1's FINAL output total: leaked
  // counters would start run 2 at run 1's accumulated total plus new responses,
  // pushing every run-2 snapshot at or above that total. The binding is to the
  // final output rather than run 1's last snapshot, because run 1's final tick
  // can legitimately be lost to the collector's stop-check race (the last
  // snapshot may then undershoot the true run-1 total, and comparing run 2
  // against it could spuriously fail). A lost tick cannot push run 2 above the
  // final total, so a strict comparison below it stays sound. A stray snapshot
  // left over from run 1 would also surface as run 2's first snapshot here.
  if (!second.snapshots.empty()) {
    ASSERT_TRUE(first.got_final_response);
    ASSERT_GT(first.final_output.results_size(), 0);
    uint64_t first_run_total_http_2xx = 0;
    for (const auto& counter : first.final_output.results(0).counters()) {
      if (counter.name() == "benchmark.http_2xx") {
        first_run_total_http_2xx = counter.value();
      }
    }
    uint64_t second_run_first_http_2xx = 0;
    for (const auto& [name, value] : second.snapshots.front().counters()) {
      if (name == "benchmark.http_2xx") {
        second_run_first_http_2xx = value;
      }
    }
    EXPECT_LT(second_run_first_http_2xx, first_run_total_http_2xx);
  }

  // Nothing may follow run 2's final response: snapshots stop at run end.
  EXPECT_TRUE(stream->WritesDone());
  ExecutionResponse extra;
  EXPECT_FALSE(stream->Read(&extra));
  auto status = stream->Finish();
  EXPECT_TRUE(status.ok());
}

// Two clients may hold open ExecutionStream RPCs at the same time (only one run
// may be in flight, but a stream can idle while another run executes). Every
// response a run produces must go to the stream the run was started on: the
// run's snapshots and its final response may never be delivered to a different
// client's stream that happens to be open. This test opens a second stream
// mid-run-1 — precisely the window in which a shared stream pointer would get
// repointed — and then drives a second run over it, reading both streams to
// completion.
TEST_P(ServiceTest, TwoConcurrentStreamsStayIsolated) {
  TestHttpListener listener;

  // Run 1: long enough to comfortably overlap the second stream being opened.
  ExecutionRequest first_request = makeLiveRequest(listener.uri(), 10, 10, 1, true);
  grpc::ClientContext first_context;
  // A client-side deadline so a regression that strands run 1's responses on
  // another stream makes this test fail instead of hanging forever.
  first_context.set_deadline(std::chrono::system_clock::now() + 60s);
  auto first_stream = stub_->ExecutionStream(&first_context);
  ASSERT_NE(first_stream, nullptr);
  EXPECT_TRUE(first_stream->Write(first_request, {}));
  EXPECT_TRUE(first_stream->WritesDone());

  // Mid-run, a second client opens a stream (it sends nothing yet: merely
  // opening it must not affect where run 1's responses are delivered).
  std::this_thread::sleep_for(2s);
  grpc::ClientContext second_context;
  auto second_stream = stub_->ExecutionStream(&second_context);
  ASSERT_NE(second_stream, nullptr);

  // Read run 1 to completion on stream 1.
  LiveStreamResult first;
  ExecutionResponse response;
  while (first_stream->Read(&response)) {
    if (response.has_live_snapshot()) {
      first.snapshots.push_back(response.live_snapshot());
    } else if (response.has_output()) {
      first.got_final_response = true;
      break;
    }
  }
  EXPECT_TRUE(first.got_final_response);
  ASSERT_GE(first.snapshots.size(), 2);
  int64_t first_run_max_monotonic_ns = -1;
  for (const auto& snapshot : first.snapshots) {
    first_run_max_monotonic_ns = std::max(first_run_max_monotonic_ns, snapshot.monotonic_ns());
  }

  // Run 1 is over (its final response was read), so run 2 can start on stream 2.
  ExecutionRequest second_request = makeLiveRequest(listener.uri(), 2, 10, 1, true);
  EXPECT_TRUE(second_stream->Write(second_request, {}));
  EXPECT_TRUE(second_stream->WritesDone());
  LiveStreamResult second;
  bool run1_snapshot_on_stream2 = false;
  while (second_stream->Read(&response)) {
    if (second.got_final_response) {
      second.message_after_final = true;
      break;
    }
    if (response.has_live_snapshot()) {
      // Every snapshot on stream 2 must be from run 2: run 1's snapshots were
      // all emitted before run 1 ended, so any of them arriving here would
      // carry a monotonic_ns at or below run 1's last stream-1 snapshot.
      if (response.live_snapshot().monotonic_ns() <= first_run_max_monotonic_ns) {
        run1_snapshot_on_stream2 = true;
      }
      second.snapshots.push_back(response.live_snapshot());
    } else if (response.has_output()) {
      second.got_final_response = true;
    }
  }
  auto second_status = second_stream->Finish();
  EXPECT_TRUE(second_status.ok());
  EXPECT_FALSE(run1_snapshot_on_stream2);
  EXPECT_TRUE(second.got_final_response);
  EXPECT_FALSE(second.message_after_final);
  ASSERT_GE(second.snapshots.size(), 1);
  EXPECT_GT(second.snapshots.front().monotonic_ns(), first_run_max_monotonic_ns);

  // Stream 1 must carry nothing more after run 1's final response: read it to
  // completion and check it closes cleanly.
  EXPECT_FALSE(first_stream->Read(&response));
  auto first_status = first_stream->Finish();
  EXPECT_TRUE(first_status.ok());
}

// The live snapshot merge runs per worker: counters are summed over the
// workers, and per-statistic counts are summed with percentile values taking
// the per-worker maximum. Every other live-streaming test runs at concurrency
// 1, so this test drives a two-worker run and asserts the merged picture:
// the merged statistic counts must exceed what any single worker could have
// contributed at steady state, while counters keep growing and monotonic_ns
// keeps increasing.
TEST_P(ServiceTest, TwoWorkersMergeIntoLiveSnapshots) {
  TestHttpListener listener;
  const uint32_t kConcurrency = 2;
  const uint32_t kDurationSeconds = 10;
  const uint32_t kRps = 10;
  ExecutionRequest request =
      makeLiveRequest(listener.uri(), kDurationSeconds, kRps, 1, true, kConcurrency);
  LiveStreamResult result = readLiveStream(stub_.get(), request);
  EXPECT_TRUE(result.got_final_response);
  EXPECT_FALSE(result.message_after_final);
  ASSERT_GE(result.snapshots.size(), 2);

  // Merged counts: a single worker can contribute at most half of the run's
  // responses. At the final snapshot, the accumulated statistic count must
  // therefore exceed the rps-driven half-duration bound that bounds any
  // single worker's plausible contribution (each worker drives ~rps/2).
  uint64_t max_merged_statistic_count = 0;
  for (const nighthawk::client::StatisticSnapshot& statistic : result.snapshots.back().statistics()) {
    max_merged_statistic_count = std::max(max_merged_statistic_count, statistic.count());
  }
  EXPECT_GT(max_merged_statistic_count, static_cast<uint64_t>(kRps * kDurationSeconds / 2))
      << "merged statistic count does not exceed the single-worker bound; the multi-worker "
         "merge did not sum per-worker counts";

  int64_t last_monotonic_ns = -1;
  std::map<std::string, uint64_t> last_counters;
  for (const nighthawk::client::LiveSnapshot& snapshot : result.snapshots) {
    EXPECT_GT(snapshot.monotonic_ns(), last_monotonic_ns);
    last_monotonic_ns = snapshot.monotonic_ns();
    for (const auto& [name, value] : snapshot.counters()) {
      if (absl::StartsWith(name, "benchmark.")) {
        auto it = last_counters.find(name);
        if (it != last_counters.end()) {
          EXPECT_GE(value, it->second) << "counter " << name << " went backwards";
        }
        last_counters[name] = value;
      }
    }
  }
}

} // namespace
} // namespace Client
} // namespace Nighthawk
