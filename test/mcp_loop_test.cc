// FORK-ONLY (We-Amp): the MCP plan Task 6 acceptance — the agent-driven
// closed loop as ONE test, every step an MCP call (tools/call or
// resources/read through the McpServer dispatcher; no controller or library
// call is made directly):
//
//   start_run (phased) -> observe nighthawk://series until the derived p99
//   stabilizes -> pull a baseline profile window -> steer 2x -> observe the
//   series/timeline to the epoch mark -> pull the second window -> diff the
//   pair (the "knob effect" label + the per-unit-of-work delta direction)
//   -> cancel_run (natural seal) -> verify the session records carry the
//   whole story (header, phased metric series with knob epochs, the streamed
//   phase boundary, both knob-epoch records, both profile windows, run end).
//
// The baseline window is pulled BEFORE the first steer: a diff carries the
// knob-effect label only when its two windows span distinct knob epochs, so
// the pair (none) -> epoch-2 is the epoch-separated product of both steers.
//
// The oracle is the same wire-faithful in-proc NighthawkService double the
// other MCP suites pin (mcp_tools_test.cc's header: its ack/guard/clock
// semantics are the engine's, mirrored by contract and pinned against the
// real engine in tui_run_feed_test and live_update_test), extended here with
// the phase sequence + streamed boundaries + post-run metadata of
// mcp_resources_test and the clock shape + epoch-change markers of
// mcp_profile_test (the profile pipeline's alignment inputs), plus a stub
// SUT admin and fixture profile bytes through the volume-mount retriever.
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
#include <cmath>
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
#include "source/client/mcp/mcp_resources.h"
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

// The p99 of a sample list (nearest-rank over the sorted tail; the derived
// rate samples are few, so this is the honest small-sample p99).
double P99Of(std::vector<double> samples) {
  if (samples.empty()) {
    return 0.0;
  }
  std::sort(samples.begin(), samples.end());
  const size_t rank =
      std::min(samples.size() - 1, static_cast<size_t>(std::ceil(0.99 * samples.size())));
  return samples[rank];
}

// --- the wire-faithful service double (the union of the other suites':
// phases + streamed boundaries + post-run metadata over mcp_profile_test's
// clock shape and epoch-change markers) --------------------------------------

struct FakeServiceOptions {
  // The snapshot phase sequence; the phase advances every ticks_per_phase
  // snapshots, and each switch streams one PhaseBoundaryEvent first.
  std::vector<std::string> phases = {"main"};
  int ticks_per_phase = 2;
  // Extra counters every snapshot carries (the SUT-side series).
  std::map<std::string, uint64_t> extra_counters;
};

class FakeNighthawkService final : public nighthawk::client::NighthawkService::Service {
public:
  explicit FakeNighthawkService(FakeServiceOptions options) : options_(std::move(options)) {}

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
      last_phase_index_ = -1;
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
          // Joined on stream end (a late Write must never hit a freed
          // stream — the other suites' rule).
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
        WriteLocked(OutputResponse());
        JoinAcks();
        return grpc::Status::OK;
      }
    }
    StopTicker();
    JoinAcks();
    return grpc::Status::OK;
  }

private:
  int CurrentPhaseIndexLocked() const {
    return static_cast<int>(std::min<size_t>(snapshot_count_ / options_.ticks_per_phase,
                                             options_.phases.size() - 1));
  }

  nighthawk::client::ExecutionResponse SnapshotResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* snapshot = response.mutable_live_snapshot();
    // The real engine's clock shape: the monotonic domain rides a fixed
    // offset below the wall clock (that offset IS the anchor pair the
    // profile pipeline maps window intervals through), and the
    // completed-work counter grows with it.
    const int64_t wall = absl::ToUnixNanos(absl::Now());
    snapshot->set_wall_time_unix_ns(wall);
    snapshot->set_monotonic_ns(wall - kMonoOffsetNs);
    snapshot->set_phase_id(options_.phases[CurrentPhaseIndexLocked()]);
    ++snapshot_count_;
    if (run_epoch_ > 0) {
      snapshot->set_epoch_id(absl::StrCat("epoch-", run_epoch_));
      snapshot->set_epoch_changed(run_epoch_ != seen_epoch_);
      seen_epoch_ = run_epoch_;
    }
    (*snapshot->mutable_counters())["benchmark.http_2xx"] =
        static_cast<uint64_t>((wall - kMonoOffsetNs) / 1000000);
    (*snapshot->mutable_counters())["sut.cpu_ticks"] =
        static_cast<uint64_t>((wall - kMonoOffsetNs) / 1000000 * 7);
    for (const auto& [name, value] : options_.extra_counters) {
      (*snapshot->mutable_counters())[name] = value;
    }
    return response;
  }

  nighthawk::client::ExecutionResponse BoundaryResponse(int from, int to) {
    nighthawk::client::ExecutionResponse response;
    auto* event = response.mutable_phase_event();
    event->set_phase_index(to);
    event->set_phase_id(options_.phases[to]);
    event->set_ending_phase_index(from);
    event->set_ending_phase_id(options_.phases[from]);
    const int64_t wall = absl::ToUnixNanos(absl::Now());
    event->set_wall_time_unix_ns(wall);
    event->set_monotonic_ns(wall - kMonoOffsetNs);
    event->mutable_drain_duration()->set_nanos(500'000'000); // 0.5s
    event->mutable_idle_gap()->set_nanos(200'000'000);       // 0.2s
    event->set_worker_count(2);
    event->set_next_phase_cold_started(true);
    event->set_drain_censored(false);
    return response;
  }

  nighthawk::client::ExecutionResponse OutputResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* output = response.mutable_output();
    for (size_t i = 0; i < options_.phases.size(); ++i) {
      auto* metadata = output->add_phase_metadata();
      metadata->set_phase_id(options_.phases[i]);
      metadata->set_phase_index(static_cast<uint32_t>(i));
    }
    return response;
  }

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
        std::vector<nighthawk::client::ExecutionResponse> responses;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (!ticking_ || done_) {
            return;
          }
          const int next_index = CurrentPhaseIndexLocked();
          if (last_phase_index_ >= 0 && next_index != last_phase_index_) {
            responses.push_back(BoundaryResponse(last_phase_index_, next_index));
          }
          last_phase_index_ = next_index;
          responses.push_back(SnapshotResponse());
        }
        for (const nighthawk::client::ExecutionResponse& response : responses) {
          WriteLocked(response);
        }
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

  const FakeServiceOptions options_;
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
  int64_t snapshot_count_ = 0;
  int last_phase_index_ = -1;
  // The fake engine's fixed wall->monotonic offset (the anchor pair).
  static constexpr int64_t kMonoOffsetNs = 1'000'000'000;
};

// --- the stub admin (mcp_profile_test's, by contract) ------------------------

class StubAdminServer {
public:
  StubAdminServer() {
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

private:
  void AcceptLoop() {
    while (!stop_) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        continue;
      }
      std::string data;
      char buf[4096];
      while (data.find("\r\n\r\n") == std::string::npos) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
          break;
        }
        data.append(buf, static_cast<size_t>(n));
      }
      // No Date header on purpose: the fetch's clock-uncertainty read maps
      // the absence to kUnimplemented (the documented posture).
      const std::string response = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
      ssize_t unused = ::send(fd, response.data(), response.size(), 0);
      (void)unused;
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
    }
  }

  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
  std::atomic<bool> stop_{false};
};

// --- the canned fixture profile (mcp_profile_test's, by contract) ------------

constexpr int kStrSample = 1;
constexpr int kStrCount = 2;
constexpr int kStrCpu = 3;
constexpr int kStrNanoseconds = 4;
constexpr int kStrMain = 5;
constexpr int kStrWorker = 6;

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

// --- the harness (one wired server: steering + profile tools + resources) ----

struct Harness {
  std::unique_ptr<FakeNighthawkService> service;
  std::unique_ptr<grpc::Server> grpc_server;
  int grpc_port = 0;
  std::shared_ptr<grpc::ChannelInterface> channel;
  std::unique_ptr<StubAdminServer> admin;
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
  // Declared AFTER the server so it is destroyed FIRST (the other suites'
  // lifetime rule).
  std::unique_ptr<WaiterPool> pool;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<int, Struct> responses;
  int next_id = 0;
};

std::unique_ptr<Harness> MakeHarness() {
  auto harness = std::make_unique<Harness>();
  FakeServiceOptions service_options;
  // Two phases, 100 snapshots (10s) each: the observation and both 1s
  // profile windows land wholly inside the warm phase (a window straddling
  // the boundary would split into sub-minimum parts — the pipeline's
  // meaningful-minimum rule), and the loop still crosses into spike before
  // the cancel so the streamed boundary is recorded.
  service_options.phases = {"warm", "spike"};
  service_options.ticks_per_phase = 100;
  harness->service = std::make_unique<FakeNighthawkService>(service_options);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &harness->grpc_port);
  builder.RegisterService(harness->service.get());
  harness->grpc_server = builder.BuildAndStart();
  harness->channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", harness->grpc_port),
                                         grpc::InsecureChannelCredentials());
  harness->admin = std::make_unique<StubAdminServer>();

  char mount_dir[] = "/tmp/nh-mcp-loop-mount-XXXXXX";
  char staging_dir[] = "/tmp/nh-mcp-loop-staging-XXXXXX";
  ::mkdtemp(mount_dir);
  ::mkdtemp(staging_dir);
  harness->mount_root = mount_dir;
  harness->staging_root = staging_dir;

  harness->steering_options.session_prefix = "nh-mcp-loop-test";
  harness->controller =
      std::make_unique<SteeringController>(harness->channel, harness->steering_options);

  harness->profile_options.configured = true;
  harness->profile_options.pipeline.admin.admin_base = harness->admin->admin_base();
  harness->profile_options.pipeline.retriever_kind =
      Tui::ProfilePipelineConfig::RetrieverKind::kVolumeMount;
  harness->profile_options.pipeline.mount_root = harness->mount_root;
  harness->profile_options.pipeline.sut_profile_path = "/var/local/profiling/envoy.prof";
  harness->profile_options.pipeline.staging_dir = harness->staging_root;
  // A deliberately absent symbolizer tool: the canned profile is proto-native
  // (the documented raw-proto fallback parses it directly).
  harness->profile_options.pipeline.pprof_tool_argv = {"/nonexistent/nh-test-pprof-tool"};
  harness->profile_options.pipeline.window_seconds = 1;
  // Declared AFTER the steering controller (destroyed before it).
  harness->profile_controller =
      std::make_unique<ProfileController>(*harness->controller, harness->profile_options);

  RegisterEchoTool(harness->tools);
  RegisterSteeringTools(harness->tools, *harness->controller);
  RegisterProfileTools(harness->tools, *harness->profile_controller);
  RegisterTelemetryResources(harness->resources, *harness->controller);
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

// One resources/read round trip: contents[0].text parsed back
// (parse-then-compare, never raw strings).
absl::StatusOr<Struct> ReadPayload(Harness& harness, const std::string& uri) {
  const int id = ++harness.next_id;
  Struct request;
  (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*request.mutable_fields())["id"].set_number_value(id);
  (*request.mutable_fields())["method"].set_string_value("resources/read");
  Struct params;
  (*params.mutable_fields())["uri"].set_string_value(uri);
  (*request.mutable_fields())["params"] = ToValue(params);
  const std::string line =
      harness.server->HandleLine(EmitCanonicalStruct(request)).value_or("");
  auto parsed = ParseJsonObject(line);
  if (!parsed.ok()) {
    return absl::InternalError(absl::StrCat("unparseable response: ", line));
  }
  if (parsed->fields().contains("error")) {
    return absl::InvalidArgumentError(Text(Field(StructOf(Field(*parsed, "error")), "message")));
  }
  const Struct& result = StructOf(Field(*parsed, "result"));
  const Value& contents = Field(result, "contents");
  return ParseJsonObject(Text(StructOf(contents.list_value().values(0)).fields().at("text")));
}

// Stages the canned profile where the volume-mount retriever reads it
// (mcp_profile_test's basename translation).
void StageCannedProfile(Harness& harness, int sample_multiplier) {
  const std::string bytes = MakeCannedProfile(sample_multiplier);
  harness.canned_path = absl::StrCat(harness.mount_root, "/envoy.prof");
  int fd = ::open(harness.canned_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::write(fd, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
  ::close(fd);
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

// The observation half of the loop: polls nighthawk://series and derives the
// per-interval rate samples of the primary client series (cumulative counter
// deltas over wall deltas — the fake streams counters, not latency
// percentiles, so the run's p99 figure is the p99 of the derived rate
// samples). "Stabilized" = enough samples AND two consecutive polls whose p99
// agree within the fake's pacing tolerance. Returns the final p99.
absl::StatusOr<double> ObserveUntilP99Stabilizes(Harness& harness, absl::Duration budget) {
  const absl::Time deadline = absl::Now() + budget;
  double previous_p99 = 0.0;
  int stable_rounds = 0;
  while (absl::Now() < deadline) {
    auto payload = ReadPayload(harness, "nighthawk://series");
    if (!payload.ok()) {
      return payload.status();
    }
    const Value* primary = nullptr;
    for (const Value& entry : Field(*payload, "series").list_value().values()) {
      if (Text(Field(StructOf(entry), "name")) == "benchmark.http_2xx") {
        primary = &entry;
        break;
      }
    }
    if (primary == nullptr) {
      absl::SleepFor(absl::Milliseconds(100));
      continue;
    }
    const Value& points = Field(StructOf(*primary), "points");
    std::vector<double> rates;
    for (int i = 1; i < points.list_value().values_size(); ++i) {
      const Struct& a = StructOf(points.list_value().values(i - 1));
      const Struct& b = StructOf(points.list_value().values(i));
      const double dv = Field(b, "value").number_value() - Field(a, "value").number_value();
      const double dt = (Field(b, "wall_time_unix_ns").number_value() -
                         Field(a, "wall_time_unix_ns").number_value()) / 1e9;
      if (dt > 0) {
        rates.push_back(dv / dt);
      }
    }
    if (rates.size() >= 5) {
      const double p99 = P99Of(std::move(rates));
      if (previous_p99 > 0 && std::abs(p99 - previous_p99) <= 0.5 * previous_p99) {
        ++stable_rounds;
        if (stable_rounds >= 2) {
          return p99;
        }
      } else {
        stable_rounds = 0;
      }
      previous_p99 = p99;
    }
    absl::SleepFor(absl::Milliseconds(150));
  }
  return absl::DeadlineExceededError("the derived p99 never stabilized");
}

// --- the acceptance loop ------------------------------------------------------

// THE agent-driven closed loop, MCP calls only (see the file comment).
TEST(McpLoopTest, TheAgentDrivenClosedLoop) {
  auto harness = MakeHarness();

  // 1. start_run: a phased run (two phases; each phase's rps under the
  // ceiling). The agent's first MCP call after initialize.
  Struct started = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "phases": "warm:10:60:2,spike:40:60:4"})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);
  const Struct& started_structured = StructOf(Field(started, "structuredContent"));
  EXPECT_TRUE(Field(started_structured, "run_started").bool_value());
  EXPECT_EQ(Text(Field(started_structured, "resolved_target")),
            absl::StrCat("http://127.0.0.1:", harness->grpc_port, "/"));
  const std::string session_path = Text(Field(started_structured, "session"));
  EXPECT_FALSE(session_path.empty());

  // 2. Observe nighthawk://series until the derived p99 stabilizes.
  auto stabilized = ObserveUntilP99Stabilizes(*harness, absl::Seconds(15));
  ASSERT_TRUE(stabilized.ok()) << stabilized.status();
  EXPECT_GT(*stabilized, 0.0) << "the fake paces ~10 work/s; a positive p99 is the observation";

  // 3. Baseline profile window (the differential's epoch-(none) side).
  StageCannedProfile(*harness, /*sample_multiplier=*/1);
  Struct first = CallToolOrDie(*harness, "pull_profile_window",
                               R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(first)) << ErrorText(first);
  const int64_t base_id =
      static_cast<int64_t>(Field(Structured(first), "window_id").number_value());
  Struct base_landed = WaitForWindow(*harness, base_id, absl::Seconds(30),
                                     [](const std::string& status) {
                                       return status == "ready" ||
                                              absl::StartsWith(status, "failed:");
                                     });
  ASSERT_EQ(Text(Field(Structured(base_landed), "status")), "ready")
      << Text(Field(Structured(base_landed), "note"));

  // 4. Steer 2x (each acked before the next is sent — the one-in-flight
  // discipline, driven by the tool's own ack).
  Struct steer1 = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  ASSERT_FALSE(IsIsError(steer1)) << ErrorText(steer1);
  EXPECT_EQ(Text(Field(Structured(steer1), "epoch_id")), "epoch-1");
  Struct steer2 = CallToolOrDie(*harness, "steer", R"json({"rps": 30})json");
  ASSERT_FALSE(IsIsError(steer2)) << ErrorText(steer2);
  EXPECT_EQ(Text(Field(Structured(steer2), "epoch_id")), "epoch-2");

  // 5. Observe to the epoch mark: the series carries epoch-2 points and the
  // timeline marks the epoch on its segment.
  {
    const absl::Time deadline = absl::Now() + absl::Seconds(10);
    bool series_marked = false;
    bool timeline_marked = false;
    while (absl::Now() < deadline && !(series_marked && timeline_marked)) {
      auto series = ReadPayload(*harness, "nighthawk://series");
      ASSERT_TRUE(series.ok()) << series.status();
      for (const Value& entry : Field(*series, "series").list_value().values()) {
        for (const Value& point : Field(StructOf(entry), "points").list_value().values()) {
          if (Text(Field(StructOf(point), "epoch_id")) == "epoch-2") {
            series_marked = true;
          }
        }
      }
      auto timeline = ReadPayload(*harness, "nighthawk://timeline");
      ASSERT_TRUE(timeline.ok()) << timeline.status();
      for (const Value& segment : Field(*timeline, "segments").list_value().values()) {
        for (const Value& mark : Field(StructOf(segment), "epoch_ids").list_value().values()) {
          if (Text(mark) == "epoch-2") {
            timeline_marked = true;
          }
        }
      }
      if (!(series_marked && timeline_marked)) {
        absl::SleepFor(absl::Milliseconds(100));
      }
    }
    EXPECT_TRUE(series_marked) << "post-steer series points must carry epoch-2";
    EXPECT_TRUE(timeline_marked) << "the timeline must mark epoch-2";
  }

  // 6. The second profile window (the differential's epoch-2 side; triple
  // the sample mass — the knob-effect direction).
  StageCannedProfile(*harness, /*sample_multiplier=*/3);
  Struct second = CallToolOrDie(*harness, "pull_profile_window",
                                R"json({"duration_s": 1})json", absl::Seconds(30));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  const int64_t cur_id =
      static_cast<int64_t>(Field(Structured(second), "window_id").number_value());
  EXPECT_EQ(cur_id, base_id + 1); // server-assigned, monotonic
  Struct cur_landed = WaitForWindow(*harness, cur_id, absl::Seconds(30),
                                    [](const std::string& status) {
                                      return status == "ready" ||
                                             absl::StartsWith(status, "failed:");
                                    });
  ASSERT_EQ(Text(Field(Structured(cur_landed), "status")), "ready")
      << Text(Field(Structured(cur_landed), "note"));

  // 7. The differential: the knob-effect label spans both steers, the pair is
  // comparable, and the per-unit-of-work delta direction is honest (the
  // heavier current side shows a positive delta).
  Struct diff = CallToolOrDie(*harness, "diff_windows",
                              absl::StrCat(R"json({"a": )json", base_id,
                                           R"json(, "b": )json", cur_id, R"json(})json"));
  ASSERT_FALSE(IsIsError(diff)) << ErrorText(diff);
  const Struct& diff_structured = Structured(diff);
  EXPECT_EQ(Text(Field(diff_structured, "label")), "knob effect: (none) -> epoch-2");
  EXPECT_TRUE(Field(diff_structured, "comparable").bool_value());
  EXPECT_GT(Field(diff_structured, "base_total_per_work").number_value(), 0);
  EXPECT_GT(Field(diff_structured, "cur_total_per_work").number_value(), 0);
  const Value& diffs = Field(diff_structured, "diffs");
  ASSERT_GT(diffs.list_value().values_size(), 0);
  double best_ratio = 0.0;
  for (const Value& entry : diffs.list_value().values()) {
    best_ratio = std::max(best_ratio, Field(StructOf(entry), "delta_ratio").number_value());
    EXPECT_FALSE(Text(Field(StructOf(entry), "function")).empty());
  }
  EXPECT_GT(best_ratio, 0.0) << "the heavier current side must show a positive delta";

  // 8. Cancel (the run ends on the cancellation arm; the session seals).
  // First observe the phase boundary happening: the run crosses into spike
  // (get_status's current phase), so the streamed boundary record exists.
  {
    const absl::Time deadline = absl::Now() + absl::Seconds(20);
    bool in_spike = false;
    while (absl::Now() < deadline && !in_spike) {
      Struct current = Structured(CallToolOrDie(*harness, "get_status", "{}", absl::Seconds(5)));
      in_spike = Text(Field(StructOf(Field(current, "current")), "phase_id")) == "spike";
      if (!in_spike) {
        absl::SleepFor(absl::Milliseconds(200));
      }
    }
    ASSERT_TRUE(in_spike) << "the run never reached the spike phase";
  }
  Struct cancelled = CallToolOrDie(*harness, "cancel_run", "{}");
  ASSERT_FALSE(IsIsError(cancelled)) << ErrorText(cancelled);
  EXPECT_TRUE(Field(Structured(cancelled), "cancelled").bool_value());
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_FALSE(Field(StructOf(Field(status, "run")), "in_flight").bool_value());
  EXPECT_TRUE(Field(StructOf(Field(status, "run")), "completed").bool_value());

  // 9. The session records carry the whole story: header first, then phased
  // metric series with knob epochs, the streamed phase boundary, both
  // knob-epoch records, both profile windows, run end last.
  auto session = ReadPayload(*harness, "nighthawk://session");
  ASSERT_TRUE(session.ok()) << session.status();
  EXPECT_FALSE(Field(*session, "tail_skipped").bool_value());
  const Value& records = Field(*session, "records");
  ASSERT_GE(records.list_value().values_size(), 6);
  EXPECT_EQ(Text(Field(StructOf(records.list_value().values(0)), "type")), "header");

  int series_records = 0;
  int boundary_records = 0;
  int window_records = 0;
  std::vector<std::string> epoch_records;
  bool saw_warm_series = false;
  bool saw_epoch2_series = false;
  for (const Value& record_value : records.list_value().values()) {
    const Struct& record = StructOf(record_value);
    const std::string type = Text(Field(record, "type"));
    if (type == "metric_series") {
      ++series_records;
      const std::string phase = Text(Field(record, "phase_id"));
      if (phase == "warm") {
        saw_warm_series = true;
      }
      if (Text(Field(record, "epoch_id")) == "epoch-2") {
        saw_epoch2_series = true;
      }
    } else if (type == "phase_boundary") {
      ++boundary_records;
      EXPECT_EQ(Text(Field(record, "phase_id")), "warm");
      EXPECT_EQ(Text(Field(record, "next_phase_id")), "spike");
      EXPECT_FALSE(Field(record, "drain_censored").bool_value());
    } else if (type == "knob_epoch") {
      epoch_records.push_back(Text(Field(record, "epoch_id")));
    } else if (type == "profile_window") {
      ++window_records;
      EXPECT_EQ(Text(Field(record, "profile_type")), "cpu");
    }
  }
  EXPECT_GT(series_records, 0);
  EXPECT_TRUE(saw_warm_series) << "the phased run must carry warm-phase series";
  EXPECT_TRUE(saw_epoch2_series) << "post-steer series must carry the epoch-2 stamp";
  EXPECT_GE(boundary_records, 1) << "the streamed warm->spike boundary must be recorded";
  ASSERT_EQ(epoch_records.size(), 2) << "both applied steers must be recorded";
  EXPECT_EQ(epoch_records[0], "epoch-1");
  EXPECT_EQ(epoch_records[1], "epoch-2");
  EXPECT_EQ(window_records, 2) << "both pulled windows must be recorded";
  const Value& last =
      records.list_value().values(records.list_value().values_size() - 1);
  EXPECT_EQ(Text(Field(StructOf(last), "type")), "run_end");
  EXPECT_TRUE(Field(StructOf(last), "completed").bool_value());

  // The provenance field names the session file (never a prefix line).
  EXPECT_EQ(Text(Field(StructOf(Field(*session, "provenance")), "session")), session_path);
}

} // namespace
} // namespace Client
} // namespace Nighthawk
