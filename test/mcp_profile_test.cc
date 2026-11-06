// FORK-ONLY (We-Amp): the MCP profile-window + differential tools' contract
// pins (MCP plan Task 4). Absent profile flags leave the tools REGISTERED
// but honest (the not-configured error on every one); a configured pull
// reaches ready or failed-with-reason against fixture profile bytes (the
// canned proto-native profile through the volume-mount retriever + stub
// admin); window ids are server-assigned only; an epoch-separated pair
// diffs with the "knob effect" label; a same-state pair is refused as the
// noise floor.
//
// The in-proc service is the same wire-faithful NighthawkService double the
// steering suite pins (see mcp_tools_test.cc's header: its ack/guard
// semantics are the engine's, mirrored by contract). The stub admin answers
// the POST-only /cpuprofiler mutators the way Envoy does.
//
// Plain cc_test for the same quarantine reason as the other MCP suites:
// mcp_lib carries no Envoy C++ sources and no test-helper stack.
#include <grpc++/grpc++.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include <fcntl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

#include "api/client/service.grpc.pb.h"
#include "api/client/service.pb.h"
#include "api/pprof/profile.pb.h"

#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_main.h"
#include "source/client/mcp/mcp_server.h"
#include "source/client/mcp/mcp_tools.h"
#include "source/client/tui/profile_pipeline.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::Struct;
using google::protobuf::Value;

const Value& Field(const Struct& s, absl::string_view name) {
  static const Value* const empty = new Value();
  auto it = s.fields().find(std::string(name));
  return it == s.fields().end() ? *empty : it->second;
}

const Struct& StructOf(const Value& v) { return v.struct_value(); }

std::string Text(const Value& v) { return v.string_value(); }

bool IsIsError(const Struct& result) { return Field(result, "isError").bool_value(); }

const Struct& Structured(const Struct& result) { return StructOf(Field(result, "structuredContent")); }

std::string ErrorText(const Struct& result) {
  const Value& content = Field(result, "content");
  return Text(StructOf(content.list_value().values(0)).fields().at("text"));
}

// --- the wire-faithful service double (mcp_tools_test.cc's, by contract) ----

class FakeNighthawkService final : public nighthawk::client::NighthawkService::Service {
public:
  FakeNighthawkService() = default;

  ~FakeNighthawkService() override {
    StopTicker();
    JoinAcks();
  }

  grpc::Status ExecutionStream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse, nighthawk::client::ExecutionRequest>*
          stream) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stream_ = stream;
      run_epoch_ = 0;
      seen_epoch_ = 0;
      update_in_flight_ = false;
      done_ = false;
    }
    nighthawk::client::ExecutionRequest request;
    while (stream->Read(&request)) {
      if (request.has_start_request()) {
        WriteLocked(SnapshotResponse());
        StartTicker();
      } else if (request.has_update_request()) {
        bool reject = false;
        int candidate = 0;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (update_in_flight_) {
            reject = true;
          } else {
            update_in_flight_ = true;
            candidate = ++run_epoch_;
          }
        }
        if (reject) {
          nighthawk::client::ExecutionResponse response;
          auto* result = response.mutable_update_result();
          result->set_applied(false);
          result->set_rejection_reason(
              "another live update is still in flight; bends are not queued");
          WriteLocked(response);
        } else {
          // The applier analog: the ack lands after a short, fixed delay;
          // the epoch id rides the ack AND the subsequent snapshots. The
          // thread is JOINED on stream end (a late Write must never hit a
          // freed stream — mcp_tools_test.cc's rule).
          std::thread ack([this, candidate]() { AckAfterDelay(candidate); });
          std::lock_guard<std::mutex> lock(mutex_);
          ack_threads_.push_back(std::move(ack));
        }
      } else if (request.has_cancellation_request()) {
        StopTicker();
        {
          std::lock_guard<std::mutex> lock(mutex_);
          done_ = true;
        }
        nighthawk::client::ExecutionResponse response;
        response.mutable_output();
        WriteLocked(response);
        JoinAcks();
        return grpc::Status::OK;
      }
    }
    StopTicker();
    JoinAcks();
    return grpc::Status::OK;
  }

private:
  void AckAfterDelay(int candidate) {
    absl::SleepFor(absl::Milliseconds(300));
    nighthawk::client::ExecutionResponse response;
    auto* result = response.mutable_update_result();
    result->set_applied(true);
    result->set_epoch_id(absl::StrCat("epoch-", candidate));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const int64_t wall = absl::ToUnixNanos(absl::Now());
      result->set_monotonic_ns(wall - kMonoOffsetNs);
      result->set_wall_time_unix_ns(wall);
      update_in_flight_ = false;
    }
    WriteLocked(response);
  }

  void JoinAcks() {
    std::vector<std::thread> acks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      acks = std::move(ack_threads_);
      ack_threads_.clear();
    }
    for (std::thread& ack : acks) {
      ack.join();
    }
  }

  nighthawk::client::ExecutionResponse SnapshotResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* snapshot = response.mutable_live_snapshot();
    // The real engine's clock shape: the monotonic domain rides a fixed
    // offset below the wall clock (that offset IS the anchor pair — the
    // profile pipeline maps wall intervals through it), and the
    // completed-work counter grows with it (the per-unit-of-work
    // denominator's input).
    const int64_t wall = absl::ToUnixNanos(absl::Now());
    snapshot->set_wall_time_unix_ns(wall);
    snapshot->set_monotonic_ns(wall - kMonoOffsetNs);
    snapshot->set_phase_id("main");
    if (run_epoch_ > 0) {
      snapshot->set_epoch_id(absl::StrCat("epoch-", run_epoch_));
      snapshot->set_epoch_changed(run_epoch_ != seen_epoch_);
      seen_epoch_ = run_epoch_;
    }
    (*snapshot->mutable_counters())["benchmark.http_2xx"] =
        static_cast<uint64_t>((wall - kMonoOffsetNs) / 1000000);
    return response;
  }

  void WriteLocked(const nighthawk::client::ExecutionResponse& response) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stream_ != nullptr) {
      stream_->Write(response);
    }
  }

  void StartTicker() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (ticking_) {
        return;
      }
      ticking_ = true;
    }
    ticker_ = std::thread([this]() {
      while (true) {
        absl::SleepFor(absl::Milliseconds(100));
        nighthawk::client::ExecutionResponse response;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (!ticking_ || done_) {
            return;
          }
          response = SnapshotResponse();
        }
        WriteLocked(response);
      }
    });
  }

  void StopTicker() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!ticking_) {
        return;
      }
      ticking_ = false;
    }
    if (ticker_.joinable()) {
      ticker_.join();
    }
  }

  std::mutex mutex_;
  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                           nighthawk::client::ExecutionRequest>* stream_ = nullptr;
  std::vector<std::thread> ack_threads_;
  std::thread ticker_;
  bool ticking_ = false;
  bool done_ = false;
  bool update_in_flight_ = false;
  int run_epoch_ = 0;
  int seen_epoch_ = 0;
  // The fake engine's fixed wall->monotonic offset (the anchor pair).
  static constexpr int64_t kMonoOffsetNs = 1'000'000'000;
};

// --- the stub admin (POST-only /cpuprofiler mutators) -----------------------

// A single-connection-at-a-time HTTP stub standing in for the SUT admin
// endpoint. `fail_enable` makes the enable mutator answer 500 (the arm
// failure case).
class StubAdminServer {
public:
  explicit StubAdminServer(bool fail_enable = false) : fail_enable_(fail_enable) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(listen_fd_, 4);
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this]() { AcceptLoop(); });
  }

  ~StubAdminServer() {
    stop_ = true;
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  std::string admin_base() const { return absl::StrCat("http://127.0.0.1:", port_); }

  const std::vector<std::string>& requests() const { return requests_; }

private:
  void AcceptLoop() {
    while (!stop_) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        continue;
      }
      // Read to the header terminator (the mutators carry empty bodies).
      std::string data;
      char buf[4096];
      while (data.find("\r\n\r\n") == std::string::npos) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
          break;
        }
        data.append(buf, static_cast<size_t>(n));
      }
      const std::string request_line = data.substr(0, data.find("\r\n"));
      requests_.push_back(request_line);
      // No Date header on purpose: FetchSutUnixTimeNs maps the absence to
      // kUnimplemented and the pipeline records the uncertainty as absent.
      std::string response = "HTTP/1.1 ";
      response += fail_enable_ && request_line.find("enable=y") != std::string::npos
                      ? "500 Internal Server Error\r\ncontent-length: 8\r\n\r\ndouble-up"
                      : "200 OK\r\ncontent-length: 0\r\n\r\n";
      ssize_t unused = ::send(fd, response.data(), response.size(), 0);
      (void)unused;
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
    }
  }

  const bool fail_enable_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::vector<std::string> requests_;
};

// --- the canned fixture profile (proto-native; the raw-proto fallback path) -

constexpr int kStrSample = 1;
constexpr int kStrCount = 2;
constexpr int kStrCpu = 3;
constexpr int kStrNanoseconds = 4;
constexpr int kStrMain = 5;
constexpr int kStrWorker = 6;

// A small proto-native CPU profile (the flamegraph pipeline test's fixture
// shape): symbolized functions, no time_nanos (the fetch-orchestration
// interval is the live path).
std::string MakeCannedProfile(int sample_multiplier) {
  perftools::profiles::Profile p;
  p.add_string_table("");        // 0
  p.add_string_table("sample");  // 1
  p.add_string_table("count");   // 2
  p.add_string_table("cpu");     // 3
  p.add_string_table("nanoseconds"); // 4
  p.add_string_table("main");    // 5
  p.add_string_table("worker");  // 6
  auto* f_main = p.add_function();
  f_main->set_id(1);
  f_main->set_name(kStrMain);
  auto* f_worker = p.add_function();
  f_worker->set_id(2);
  f_worker->set_name(kStrWorker);
  auto* loc_main = p.add_location();
  loc_main->set_id(1);
  loc_main->set_address(0x1000);
  loc_main->add_line()->set_function_id(1);
  auto* loc_worker = p.add_location();
  loc_worker->set_id(2);
  loc_worker->set_address(0x2000);
  loc_worker->add_line()->set_function_id(2);
  auto* st_count = p.add_sample_type();
  st_count->set_type(kStrSample);
  st_count->set_unit(kStrCount);
  auto* st_cpu = p.add_sample_type();
  st_cpu->set_type(kStrCpu);
  st_cpu->set_unit(kStrNanoseconds);
  auto* pt = p.mutable_period_type();
  pt->set_type(kStrCpu);
  pt->set_unit(kStrNanoseconds);
  p.set_period(10000000);
  for (int i = 0; i < 3 * sample_multiplier; ++i) {
    auto* s = p.add_sample();
    s->add_location_id(2);
    s->add_location_id(1);
    s->add_value(1);
    s->add_value(20000000);
  }
  std::string out;
  EXPECT_TRUE(p.SerializeToString(&out));
  return out;
}

// --- the harness ------------------------------------------------------------

struct Harness {
  std::unique_ptr<FakeNighthawkService> service;
  std::unique_ptr<grpc::Server> grpc_server;
  int grpc_port = 0;
  std::shared_ptr<grpc::ChannelInterface> channel;
  std::unique_ptr<StubAdminServer> admin;
  // The volume-mount root (the canned profile's SUT path translates here by
  // basename) and the staging dir.
  std::string mount_root;
  std::string staging_root;
  std::string canned_path;
  ProfileToolOptions profile_options;
  SteeringToolOptions steering_options;
  std::unique_ptr<SteeringController> controller;
  std::unique_ptr<ProfileController> profile_controller;
  ToolRegistry tools;
  ResourceRegistry resources;
  std::unique_ptr<McpServer> server;
  // Declared AFTER the server so it is destroyed FIRST (mcp_tools_test.cc's
  // lifetime rule).
  std::unique_ptr<WaiterPool> pool;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<int, Struct> responses;
  int next_id = 0;
};

std::unique_ptr<Harness> MakeHarness(bool configure_profiling = false,
                                     bool fail_enable = false) {
  auto harness = std::make_unique<Harness>();
  harness->service = std::make_unique<FakeNighthawkService>();
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &harness->grpc_port);
  builder.RegisterService(harness->service.get());
  harness->grpc_server = builder.BuildAndStart();
  harness->channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", harness->grpc_port),
                                         grpc::InsecureChannelCredentials());
  harness->admin = std::make_unique<StubAdminServer>(fail_enable);

  char mount_dir[] = "/tmp/nh-mcp-profile-mount-XXXXXX";
  char staging_dir[] = "/tmp/nh-mcp-profile-staging-XXXXXX";
  ::mkdtemp(mount_dir);
  ::mkdtemp(staging_dir);
  harness->mount_root = mount_dir;
  harness->staging_root = staging_dir;

  harness->steering_options.session_prefix = "nh-mcp-profile-test";
  harness->controller =
      std::make_unique<SteeringController>(harness->channel, harness->steering_options);

  harness->profile_options.configured = configure_profiling;
  harness->profile_options.pipeline.admin.admin_base = harness->admin->admin_base();
  harness->profile_options.pipeline.retriever_kind =
      Tui::ProfilePipelineConfig::RetrieverKind::kVolumeMount;
  harness->profile_options.pipeline.mount_root = harness->mount_root;
  harness->profile_options.pipeline.sut_profile_path = "/var/local/profiling/envoy.prof";
  harness->profile_options.pipeline.staging_dir = harness->staging_root;
  // A deliberately absent symbolizer tool: the canned profile is
  // proto-native, so the documented raw-proto fallback parses it directly
  // (symbolization itself is the TUI suite's pinned territory).
  harness->profile_options.pipeline.pprof_tool_argv = {"/nonexistent/nh-test-pprof-tool"};
  harness->profile_options.pipeline.window_seconds = 1;
  // Declared AFTER the steering controller (destroyed before it).
  harness->profile_controller =
      std::make_unique<ProfileController>(*harness->controller, harness->profile_options);

  RegisterEchoTool(harness->tools);
  RegisterSteeringTools(harness->tools, *harness->controller);
  RegisterProfileTools(harness->tools, *harness->profile_controller);
  harness->server = std::make_unique<McpServer>(
      harness->tools, harness->resources,
      [raw = harness.get()](absl::string_view line) {
        auto parsed = ParseJsonObject(line);
        if (!parsed.ok()) {
          return;
        }
        const int id = static_cast<int>(Field(*parsed, "id").number_value());
        std::lock_guard<std::mutex> lock(raw->mutex);
        raw->responses[id] = StructOf(Field(*parsed, "result"));
        raw->cv.notify_all();
      },
      [](absl::string_view) {},
      [raw_pool = &harness->pool](std::function<void()> task) {
        raw_pool->get()->Enqueue(std::move(task));
      });
  harness->pool = std::make_unique<WaiterPool>(4);

  Struct init_request;
  (*init_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*init_request.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*init_request.mutable_fields())["method"].set_string_value("initialize");
  (*init_request.mutable_fields())["params"] = ToValue(*ParseJsonObject(
      R"json({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test"}})json"));
  (void)harness->server->HandleLine(EmitCanonicalStruct(init_request));
  return harness;
}

absl::StatusOr<Struct> CallTool(Harness& harness, const std::string& name,
                                const std::string& args_json,
                                absl::Duration budget = absl::Seconds(60)) {
  auto args = ParseJsonObject(args_json);
  if (!args.ok()) {
    return args.status();
  }
  const int id = ++harness.next_id;
  Struct request;
  (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*request.mutable_fields())["id"].set_number_value(id);
  (*request.mutable_fields())["method"].set_string_value("tools/call");
  Struct params;
  (*params.mutable_fields())["name"].set_string_value(name);
  (*params.mutable_fields())["arguments"] = ToValue(*args);
  (*request.mutable_fields())["params"] = ToValue(params);
  (void)harness.server->HandleLine(EmitCanonicalStruct(request));
  std::unique_lock<std::mutex> lock(harness.mutex);
  if (!harness.cv.wait_for(lock, std::chrono::nanoseconds(absl::ToChronoNanoseconds(budget)),
                           [&harness, id]() { return harness.responses.count(id) > 0; })) {
    return absl::DeadlineExceededError(absl::StrCat("no response for ", name, " id ", id));
  }
  return std::move(harness.responses[id]);
}

Struct CallToolOrDie(Harness& harness, const std::string& name, const std::string& args_json,
                     absl::Duration budget = absl::Seconds(60)) {
  auto result = CallTool(harness, name, args_json, budget);
  EXPECT_TRUE(result.ok()) << result.status();
  return result.value_or(Struct{});
}

// Starts a run and waits for the first snapshot (the anchor + a non-empty
// phase id in get_status).
void StartRunAndWaitForSnapshot(Harness& harness) {
  CallToolOrDie(harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness.grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  const absl::Time deadline = absl::Now() + absl::Seconds(5);
  while (absl::Now() < deadline) {
    Struct status = Structured(CallToolOrDie(harness, "get_status", "{}", absl::Seconds(5)));
    if (!Text(Field(StructOf(Field(status, "current")), "phase_id")).empty()) {
      return;
    }
    absl::SleepFor(absl::Milliseconds(50));
  }
  ADD_FAILURE() << "no snapshot within 5s";
}

// Polls window_status{id} until the status string satisfies `predicate`.
Struct WaitForWindow(Harness& harness, int64_t id, absl::Duration budget,
                     const std::function<bool(const std::string&)>& predicate) {
  const absl::Time deadline = absl::Now() + budget;
  Struct last;
  while (absl::Now() < deadline) {
    last = CallToolOrDie(harness, "window_status",
                         absl::StrCat(R"json({"id": )json", id, R"json(})json"),
                         absl::Seconds(5));
    if (predicate(Text(Field(Structured(last), "status")))) {
      return last;
    }
    absl::SleepFor(absl::Milliseconds(50));
  }
  ADD_FAILURE() << "window " << id << " never satisfied the predicate within " << budget;
  return last;
}

// Stages the canned profile where the volume-mount retriever translates the
// SUT path (basename under the mount root).
void StageCannedProfile(Harness& harness, int sample_multiplier) {
  const std::string bytes = MakeCannedProfile(sample_multiplier);
  harness.canned_path = absl::StrCat(harness.mount_root, "/envoy.prof");
  int fd = ::open(harness.canned_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::write(fd, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
  ::close(fd);
}

// --- the Task 4 pins ---------------------------------------------------------

// NotConfiguredIsHonestError: absent profile flags leave the tools registered
// but honest — every one refuses with the not-configured message.
TEST(McpProfileTest, NotConfiguredIsHonestError) {
  auto harness = MakeHarness(/*configure_profiling=*/false);
  for (const auto& [name, args] :
       std::vector<std::pair<std::string, std::string>>{{"pull_profile_window", "{}"},
                                                        {"window_status", R"json({"id": 1})json"},
                                                        {"diff_windows",
                                                         R"json({"a": 1, "b": 2})json"}}) {
    Struct result = CallToolOrDie(*harness, name, args);
    EXPECT_TRUE(IsIsError(result)) << name << " must refuse when unconfigured";
    EXPECT_EQ(ErrorText(result), "profiling is not configured (pass --profile-mount or "
                                 "--scp-host)")
        << name << ": " << ErrorText(result);
  }
}

// PullReachesReadyOrFailedWithReason: a configured pull against fixture
// profile bytes lands a READY window carrying the pipeline's outcome note
// (or a FAILED one carrying its reason — both honest terminal states).
TEST(McpProfileTest, PullReachesReadyOrFailedWithReason) {
  auto harness = MakeHarness(/*configure_profiling=*/true);
  StageCannedProfile(*harness, /*sample_multiplier=*/1);
  StartRunAndWaitForSnapshot(*harness);

  Struct pulled = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(pulled)) << ErrorText(pulled);
  const Struct& structured = Structured(pulled);
  const int64_t window_id =
      static_cast<int64_t>(Field(structured, "window_id").number_value());
  EXPECT_EQ(window_id, 1); // the first server-assigned id
  EXPECT_EQ(Text(Field(structured, "status")), "folding");

  // The fold job lands: ready with the pipeline's outcome note (the canned
  // profile is pure against the live run's interval) — or failed with its
  // reason. Either way the reason is CARRIED, never dropped.
  Struct landed = WaitForWindow(
      *harness, window_id, absl::Seconds(30),
      [](const std::string& status) { return status != "folding" && status != "staged"; });
  const std::string status = Text(Field(Structured(landed), "status"));
  if (status == "ready") {
    EXPECT_NE(Text(Field(Structured(landed), "note")).find("pure"), std::string::npos)
        << Text(Field(Structured(landed), "note"));
  } else {
    EXPECT_TRUE(absl::StartsWith(status, "failed:")) << status;
    EXPECT_FALSE(status == "failed:") << "a failed window carries its reason";
  }

  // The single-flight discipline freed: a second pull stages the next
  // server-assigned id.
  Struct second = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  EXPECT_EQ(static_cast<int64_t>(Field(Structured(second), "window_id").number_value()),
            window_id + 1);
}

// A collector-arm failure fails the window WITH its reason (the staged id
// never vanishes) and surfaces the admin's message verbatim.
TEST(McpProfileTest, PullArmFailureFailsWindowWithReason) {
  auto harness = MakeHarness(/*configure_profiling=*/true, /*fail_enable=*/true);
  StageCannedProfile(*harness, /*sample_multiplier=*/1);
  StartRunAndWaitForSnapshot(*harness);

  Struct pulled = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  EXPECT_TRUE(IsIsError(pulled));
  const std::string reason = ErrorText(pulled);
  EXPECT_FALSE(reason.empty());
  // The staged window failed with the same reason, queryably.
  Struct status = CallToolOrDie(*harness, "window_status", R"json({"id": 1})json");
  const std::string status_text = Text(Field(Structured(status), "status"));
  EXPECT_TRUE(absl::StartsWith(status_text, "failed:")) << status_text;
  EXPECT_NE(status_text.find(reason), std::string::npos)
      << "the window's failure reason is the admin's message: " << status_text;
}

// WindowIdValidatedServerAssigned: a non-positive or unknown id is refused
// naming the server-assigned discipline (the schema's type check handles the
// wrong types at the dispatcher; these pins cover the value space).
TEST(McpProfileTest, WindowIdValidatedServerAssigned) {
  auto harness = MakeHarness(/*configure_profiling=*/true);
  for (const std::string& args : {R"json({"id": 0})json", R"json({"id": -3})json",
                                  R"json({"id": 42})json"}) {
    Struct result = CallToolOrDie(*harness, "window_status", args);
    EXPECT_TRUE(IsIsError(result)) << args;
    EXPECT_NE(ErrorText(result).find("server-assigned"), std::string::npos)
        << args << ": " << ErrorText(result);
  }
  for (const std::string& args :
       {R"json({"a": 0, "b": 1})json", R"json({"a": 1, "b": -1})json",
        R"json({"a": 7, "b": 9})json"}) {
    Struct result = CallToolOrDie(*harness, "diff_windows", args);
    EXPECT_TRUE(IsIsError(result)) << args;
    EXPECT_NE(ErrorText(result).find("server-assigned"), std::string::npos)
        << args << ": " << ErrorText(result);
  }
  // The same-id case is refused too (its reason names the distinctness rule,
  // not the id discipline).
  Struct same = CallToolOrDie(*harness, "diff_windows", R"json({"a": 1, "b": 1})json");
  EXPECT_TRUE(IsIsError(same));
  EXPECT_NE(ErrorText(same).find("DISTINCT"), std::string::npos) << ErrorText(same);
}

// DiffAcrossEpochLabeled: two windows pulled on either side of an applied
// steer diff with the "knob effect: <a> -> <b>" label; the delta direction
// is honest (a doubled sample mass on the same work shape shows up as a
// positive delta on the sampled frame).
TEST(McpProfileTest, DiffAcrossEpochLabeled) {
  auto harness = MakeHarness(/*configure_profiling=*/true);
  StartRunAndWaitForSnapshot(*harness);

  // Window 1: under the run's starting epoch (before any steer).
  StageCannedProfile(*harness, /*sample_multiplier=*/1);
  Struct first = CallToolOrDie(*harness, "pull_profile_window",
                               R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(first)) << ErrorText(first);
  const int64_t base_id = static_cast<int64_t>(Field(Structured(first), "window_id").number_value());
  WaitForWindow(*harness, base_id, absl::Seconds(30),
                [](const std::string& status) { return status == "ready"; });

  // Steer: the applied ack carries epoch-1, and the snapshots carry the
  // epoch-change mark from here on.
  Struct steer = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  ASSERT_FALSE(IsIsError(steer)) << ErrorText(steer);
  EXPECT_EQ(Text(Field(Structured(steer), "epoch_id")), "epoch-1");

  // Window 2: under the applied epoch.
  StageCannedProfile(*harness, /*sample_multiplier=*/3);
  Struct second = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  const int64_t cur_id =
      static_cast<int64_t>(Field(Structured(second), "window_id").number_value());
  WaitForWindow(*harness, cur_id, absl::Seconds(30),
                [](const std::string& status) { return status == "ready"; });

  Struct diff = CallToolOrDie(*harness, "diff_windows",
                              absl::StrCat(R"json({"a": )json", base_id,
                                           R"json(, "b": )json", cur_id, R"json(})json"));
  ASSERT_FALSE(IsIsError(diff)) << ErrorText(diff);
  const Struct& structured = Structured(diff);
  EXPECT_EQ(Text(Field(structured, "label")), "knob effect: (none) -> epoch-1");
  EXPECT_TRUE(Field(structured, "comparable").bool_value());
  EXPECT_GT(Field(structured, "base_total_per_work").number_value(), 0);
  EXPECT_GT(Field(structured, "cur_total_per_work").number_value(), 0);
  // Per-unit-of-work normalization: the canned profile triples its sample
  // mass while the completed-work denominator grows too — the total ratio
  // stays honest and the diffs array carries per-frame entries.
  EXPECT_GT(Field(structured, "diffs").list_value().values_size(), 0);
  const Value& diffs = Field(structured, "diffs");
  double best_ratio = 0.0;
  for (const Value& entry : diffs.list_value().values()) {
    best_ratio = std::max(best_ratio, Field(StructOf(entry), "delta_ratio").number_value());
    EXPECT_FALSE(Text(Field(StructOf(entry), "function")).empty());
  }
  EXPECT_GT(best_ratio, 0.0) << "the heavier current side must show a positive delta";
}

// DiffSameStateRefusedNoiseFloor: two same-epoch windows are REFUSED with
// the noise-floor reason — noise is never dressed up as a knob effect.
TEST(McpProfileTest, DiffSameStateRefusedNoiseFloor) {
  auto harness = MakeHarness(/*configure_profiling=*/true);
  StartRunAndWaitForSnapshot(*harness);
  StageCannedProfile(*harness, /*sample_multiplier=*/1);

  Struct first = CallToolOrDie(*harness, "pull_profile_window",
                               R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(first)) << ErrorText(first);
  const int64_t base_id = static_cast<int64_t>(Field(Structured(first), "window_id").number_value());
  WaitForWindow(*harness, base_id, absl::Seconds(30),
                [](const std::string& status) { return status == "ready"; });

  Struct second = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  const int64_t cur_id =
      static_cast<int64_t>(Field(Structured(second), "window_id").number_value());
  WaitForWindow(*harness, cur_id, absl::Seconds(30),
                [](const std::string& status) { return status == "ready"; });

  Struct diff = CallToolOrDie(*harness, "diff_windows",
                              absl::StrCat(R"json({"a": )json", base_id,
                                           R"json(, "b": )json", cur_id, R"json(})json"));
  EXPECT_TRUE(IsIsError(diff));
  const std::string reason = ErrorText(diff);
  EXPECT_NE(reason.find("NOISE FLOOR"), std::string::npos) << reason;
  EXPECT_NE(reason.find("same knob epoch"), std::string::npos) << reason;
}

} // namespace
} // namespace Client
} // namespace Nighthawk
