// FORK-ONLY (We-Amp): the MCP telemetry resources' contract pins (MCP plan
// Task 3). nighthawk://status|series|timeline|session: series carry the wall
// join, phase id, and knob epoch on every point (SUT-side series included);
// the timeline matches the TUI strip's semantics (boundary/cens/epoch marks,
// post-run censor from the Output's phase metadata); the session resource
// renders sealed-prefix records with the torn-tail honesty; and SUT-derived
// strings (quotes, newlines) survive as escaped VALUES — parse-then-compare,
// never raw-string equality. The resource URIs are validated at the
// dispatcher like tool arguments (Review Focus 3), and nighthawk://status is
// the same builder as get_status.
//
// The in-proc service double is the Task 2 wire-faithful one, extended with
// a configurable phase sequence (with streamed PhaseBoundaryEvents at the
// switches, censor-flagged) and a post-run Output carrying phase metadata —
// the minimal wire shape the timeline resource consumes. Plain cc_test for
// the same quarantine reason as mcp_tools_test: mcp_lib carries no Envoy
// C++ sources and no test-helper stack.
#include <grpc++/grpc++.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

#include "api/client/live.pb.h"
#include "api/client/options.pb.h"
#include "api/client/output.pb.h"
#include "api/client/service.grpc.pb.h"
#include "api/client/service.pb.h"
#include "api/client/session.pb.h"
#include "google/protobuf/duration.pb.h"

#include "source/client/headless/session_recorder.h"
#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_main.h"
#include "source/client/mcp/mcp_resources.h"
#include "source/client/mcp/mcp_server.h"
#include "source/client/mcp/mcp_tools.h"

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

const Struct& Structured(const Struct& result) {
  return StructOf(Field(result, "structuredContent"));
}

std::string ErrorText(const Struct& result) {
  const Value& content = Field(result, "content");
  return Text(StructOf(content.list_value().values(0)).fields().at("text"));
}

// A distinct scratch prefix per test (the file naming stamps unix ns, so
// concurrent test runs cannot collide).
std::string TempPrefix() {
  static std::atomic<int64_t> seq{0};
  const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  return absl::StrCat("/tmp/nh-mcp-resources-test-", now, "-", seq.fetch_add(1));
}

// --- the wire-faithful service double (Task 2's, extended for the telemetry
// resources' phase/boundary/metadata surface) -------------------------------

struct FakeServiceOptions {
  absl::Duration ack_delay = absl::ZeroDuration();
  bool stall_after_cancel = false;
  // The snapshot phase sequence; the phase advances every ticks_per_phase
  // snapshots, and each switch streams one PhaseBoundaryEvent first.
  std::vector<std::string> phases = {"main"};
  int ticks_per_phase = 2;
  // The streamed boundaries' censor flag (8.6 counted-not-hidden) and the
  // post-run metadata's drain_censored for the ENDING phase (index 0).
  bool boundary_censored = false;
  // Extra counters every snapshot carries (e.g. SUT-side series and the
  // SUT-string pins). Names are rendered verbatim.
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
      context_ = context;
      stream_ = stream;
      run_epoch_ = 0;
      update_in_flight_ = false;
      done_ = false;
      last_phase_index_ = -1;
    }
    nighthawk::client::ExecutionRequest request;
    while (stream->Read(&request)) {
      if (request.has_start_request()) {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          start_requests_.push_back(request.start_request());
        }
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
          std::thread ack([this, candidate]() { AckAfterDelay(candidate); });
          std::lock_guard<std::mutex> lock(mutex_);
          ack_threads_.push_back(std::move(ack));
        }
      } else if (request.has_cancellation_request()) {
        if (options_.stall_after_cancel) {
          StopTicker();
          while (!context->IsCancelled()) {
            absl::SleepFor(absl::Milliseconds(50));
          }
          JoinAcks();
          return grpc::Status::OK;
        }
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

  std::vector<nighthawk::client::StartRequest> startRequests() {
    std::lock_guard<std::mutex> lock(mutex_);
    return start_requests_;
  }

private:
  int CurrentPhaseIndexLocked() const {
    return static_cast<int>(std::min<size_t>(snapshot_count_ / options_.ticks_per_phase,
                                             options_.phases.size() - 1));
  }

  nighthawk::client::ExecutionResponse SnapshotResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* snapshot = response.mutable_live_snapshot();
    snapshot->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    snapshot->set_monotonic_ns(++monotonic_ns_);
    snapshot->set_phase_id(options_.phases[CurrentPhaseIndexLocked()]);
    ++snapshot_count_;
    if (run_epoch_ > 0) {
      snapshot->set_epoch_id(absl::StrCat("epoch-", run_epoch_));
    }
    (*snapshot->mutable_counters())["benchmark.http_2xx"] =
        static_cast<uint64_t>(monotonic_ns_);
    (*snapshot->mutable_counters())["sut.cpu_ticks"] =
        static_cast<uint64_t>(monotonic_ns_ * 7);
    for (const auto& [name, value] : options_.extra_counters) {
      (*snapshot->mutable_counters())[name] = value;
    }
    return response;
  }

  // One boundary for the transition phases_[from] -> phases_[to]: the
  // engine's discrete (wall, monotonic) join pair, the drain/idle gap, and
  // the configured censor flag.
  nighthawk::client::ExecutionResponse BoundaryResponse(int from, int to) {
    nighthawk::client::ExecutionResponse response;
    auto* event = response.mutable_phase_event();
    event->set_phase_index(to);
    event->set_phase_id(options_.phases[to]);
    event->set_ending_phase_index(from);
    event->set_ending_phase_id(options_.phases[from]);
    event->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    event->set_monotonic_ns(++monotonic_ns_);
    event->mutable_drain_duration()->set_nanos(500'000'000); // 0.5s
    event->mutable_idle_gap()->set_nanos(200'000'000);       // 0.2s
    event->set_worker_count(2);
    event->set_next_phase_cold_started(true);
    event->set_drain_censored(options_.boundary_censored);
    return response;
  }

  nighthawk::client::ExecutionResponse OutputResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* output = response.mutable_output();
    // One PhaseMetadata entry per phase; the ENDING phase's (index 0)
    // drain_censored mirrors the streamed boundary (post-run completeness).
    for (size_t i = 0; i < options_.phases.size(); ++i) {
      auto* metadata = output->add_phase_metadata();
      metadata->set_phase_id(options_.phases[i]);
      metadata->set_phase_index(static_cast<uint32_t>(i));
      metadata->set_drain_censored(options_.boundary_censored && i == 0);
      metadata->set_completion_ratio(0.75);
      metadata->set_gate_exceeded(3);
    }
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

  void AckAfterDelay(int candidate) {
    absl::SleepFor(options_.ack_delay);
    nighthawk::client::ExecutionResponse response;
    auto* result = response.mutable_update_result();
    result->set_applied(true);
    result->set_epoch_id(absl::StrCat("epoch-", candidate));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      result->set_monotonic_ns(++monotonic_ns_);
      result->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    }
    WriteLocked(response);
    std::lock_guard<std::mutex> lock(mutex_);
    update_in_flight_ = false;
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
  grpc::ServerContext* context_ = nullptr;
  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                           nighthawk::client::ExecutionRequest>* stream_ = nullptr;
  std::vector<nighthawk::client::StartRequest> start_requests_;
  std::vector<std::thread> ack_threads_;
  std::thread ticker_;
  bool ticking_ = false;
  bool done_ = false;
  bool update_in_flight_ = false;
  int run_epoch_ = 0;
  int64_t monotonic_ns_ = 0;
  int64_t snapshot_count_ = 0;
  int last_phase_index_ = -1;
};

// One wired steering + telemetry server (the Task 2 harness, plus the
// resource registry).
struct Harness {
  std::unique_ptr<FakeNighthawkService> service;
  std::unique_ptr<grpc::Server> grpc_server;
  int grpc_port = 0;
  std::shared_ptr<grpc::ChannelInterface> channel;
  SteeringToolOptions options;
  std::unique_ptr<SteeringController> controller;
  ToolRegistry tools;
  ResourceRegistry resources;
  std::unique_ptr<McpServer> server;
  // Declared AFTER the server so it is destroyed FIRST (see mcp_tools_test).
  std::unique_ptr<WaiterPool> pool;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<int, Struct> responses;
  int next_id = 0;
};

std::unique_ptr<Harness> MakeHarness(FakeServiceOptions service_options = {},
                                     SteeringToolOptions tool_options = {}) {
  auto harness = std::make_unique<Harness>();
  harness->options = tool_options;
  harness->service = std::make_unique<FakeNighthawkService>(std::move(service_options));
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &harness->grpc_port);
  builder.RegisterService(harness->service.get());
  harness->grpc_server = builder.BuildAndStart();
  harness->channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", harness->grpc_port),
                                         grpc::InsecureChannelCredentials());
  harness->controller =
      std::make_unique<SteeringController>(harness->channel, harness->options);
  RegisterEchoTool(harness->tools);
  RegisterSteeringTools(harness->tools, *harness->controller);
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
  harness->pool = std::make_unique<WaiterPool>(2);

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

// One resources/read round trip: the result struct on success, the error
// status (the JSON-RPC error message) when the dispatcher refused the read.
absl::StatusOr<Struct> ReadResource(Harness& harness, const std::string& uri) {
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
  return StructOf(Field(*parsed, "result"));
}

// The telemetry payload of a resources/read result: contents[0].text parsed
// back (parse-then-compare, never raw strings).
absl::StatusOr<Struct> ReadPayload(Harness& harness, const std::string& uri) {
  auto read = ReadResource(harness, uri);
  if (!read.ok()) {
    return read.status();
  }
  const Value& contents = Field(*read, "contents");
  return ParseJsonObject(Text(StructOf(contents.list_value().values(0)).fields().at("text")));
}

Struct ReadPayloadOrDie(Harness& harness, const std::string& uri) {
  auto payload = ReadPayload(harness, uri);
  EXPECT_TRUE(payload.ok()) << payload.status();
  return payload.value_or(Struct{});
}

void WaitForStatus(Harness& harness, absl::Duration budget,
                   const std::function<bool(const Struct&)>& predicate) {
  const absl::Time deadline = absl::Now() + budget;
  while (absl::Now() < deadline) {
    Struct status = CallToolOrDie(harness, "get_status", "{}", absl::Seconds(5));
    if (predicate(Structured(status))) {
      return;
    }
    absl::SleepFor(absl::Milliseconds(50));
  }
  ADD_FAILURE() << "get_status never satisfied the predicate within " << budget;
}

// --- resources/list + dispatcher URI validation (Review Focus 3) ------------

TEST(McpResourcesTest, ResourcesListExactlyTheFourTelemetryUris) {
  auto harness = MakeHarness();
  Struct request;
  (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*request.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*request.mutable_fields())["method"].set_string_value("resources/list");
  (void)harness->server->HandleLine(EmitCanonicalStruct(request));
  Struct listing;
  {
    std::lock_guard<std::mutex> lock(harness->mutex);
    listing = harness->responses[harness->next_id];
  }
  const Value& resources = Field(listing, "resources");
  ASSERT_EQ(resources.list_value().values_size(), 4);
  std::vector<std::string> uris;
  for (const Value& entry : resources.list_value().values()) {
    const Struct& resource = StructOf(entry);
    EXPECT_EQ(Text(Field(resource, "mimeType")), "application/json");
    uris.push_back(Text(Field(resource, "uri")));
  }
  EXPECT_EQ(uris, (std::vector<std::string>{"nighthawk://status", "nighthawk://series",
                                            "nighthawk://timeline", "nighthawk://session"}));
}

// Resource URIs are validated at the dispatcher exactly like tool arguments:
// anything that is not an exact registered URI — a traversal shape, an
// agent-shaped suffix, an unknown name — is a field-naming invalid-params
// fault. There are no agent-shaped ids anywhere in the surface.
TEST(McpResourcesTest, ResourceUriValidatedAtDispatcher) {
  auto harness = MakeHarness();
  const std::vector<std::string> bad_uris = {
      "nighthawk://../../etc/passwd", "nighthawk://series/extra",
      "nighthawk://series?window=1", "file:///etc/passwd", "nighthawk://unknown",
  };
  for (const std::string& uri : bad_uris) {
    auto read = ReadResource(*harness, uri);
    ASSERT_FALSE(read.ok()) << uri << " must be refused";
    EXPECT_EQ(read.status().code(), absl::StatusCode::kInvalidArgument) << uri;
    EXPECT_NE(read.status().message().find("Invalid params"), std::string::npos) << uri;
    EXPECT_NE(read.status().message().find(uri), std::string::npos)
        << "the refusal names the offending uri: " << read.status().message();
  }
  // The registered URIs still read (no run yet: honest empty shapes, not
  // protocol faults).
  for (const std::string& uri :
       {"nighthawk://status", "nighthawk://series", "nighthawk://timeline",
        "nighthawk://session"}) {
    EXPECT_TRUE(ReadPayload(*harness, uri).ok()) << uri;
  }
}

// nighthawk://status is ONE builder with get_status: the resource payload and
// the tool's structuredContent are the same object.
TEST(McpResourcesTest, StatusResourceIsTheSameBuilderAsGetStatus) {
  auto harness = MakeHarness();
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });
  Struct from_tool = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  Struct from_resource = ReadPayloadOrDie(*harness, "nighthawk://status");
  // Both come from the same canonical emitter fed by the same builder —
  // byte equality IS the same-object pin here (and only here; telemetry
  // payloads elsewhere are parse-then-compared).
  EXPECT_EQ(EmitCanonicalStruct(from_tool), EmitCanonicalStruct(from_resource));
}

// --- series -----------------------------------------------------------------

// The series payload: a phased in-proc run — phase_id on every point, the
// knob epoch from the first applied bend onward, wall and monotonic both
// joined through the first-snapshot anchor, and the SUT-side series alongside
// the client series.
TEST(McpResourcesTest, SeriesCarriesWallJoinPhaseAndEpoch) {
  FakeServiceOptions service_options;
  service_options.phases = {"warm", "spike"};
  service_options.ticks_per_phase = 2;
  auto harness = MakeHarness(service_options);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return Text(Field(StructOf(Field(status, "current")), "phase_id")) == "spike";
  });
  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "steer", R"json({"rps": 20})json")));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return Text(Field(StructOf(Field(status, "current")), "epoch_id")) == "epoch-1";
  });

  Struct payload = ReadPayloadOrDie(*harness, "nighthawk://series");
  const Struct& anchor = StructOf(Field(payload, "anchor"));
  const int64_t anchor_wall = static_cast<int64_t>(Field(anchor, "wall_time_unix_ns").number_value());
  const int64_t anchor_monotonic = static_cast<int64_t>(Field(anchor, "monotonic_ns").number_value());
  ASSERT_NE(anchor_wall, 0);
  ASSERT_NE(anchor_monotonic, 0);

  const Value& series = Field(payload, "series");
  std::vector<std::string> names;
  bool saw_epoch_mark = false;
  for (const Value& entry : series.list_value().values()) {
    const Struct& one = StructOf(entry);
    names.push_back(Text(Field(one, "name")));
    const Value& points = Field(one, "points");
    ASSERT_GT(points.list_value().values_size(), 0) << names.back();
    for (const Value& point_value : points.list_value().values()) {
      const Struct& point = StructOf(point_value);
      const int64_t monotonic =
          static_cast<int64_t>(Field(point, "monotonic_ns").number_value());
      const int64_t wall =
          static_cast<int64_t>(Field(point, "wall_time_unix_ns").number_value());
      // The wall join: one anchor offset for the whole view. Struct numbers
      // are doubles — a ~1.8e18 ns wall stamp loses ~ULP(256) precision on
      // the JSON round trip, so the join is pinned within that precision
      // here; the exact int64 join is pinned by the T0 SeriesView tests.
      const int64_t drift =
          (wall - monotonic) - (anchor_wall - anchor_monotonic);
      EXPECT_LE(drift < 0 ? -drift : drift, 2048) << names.back();
      // Phase stamp on every point; the epoch stamp once applied.
      EXPECT_FALSE(Text(Field(point, "phase_id")).empty()) << names.back();
      const std::string epoch = Text(Field(point, "epoch_id"));
      if (!epoch.empty()) {
        EXPECT_EQ(epoch, "epoch-1") << names.back();
        saw_epoch_mark = true;
      }
    }
  }
  EXPECT_TRUE(saw_epoch_mark) << "post-bend points must carry the knob epoch";
  // Client and SUT-side series, side by side.
  EXPECT_NE(std::find(names.begin(), names.end(), "benchmark.http_2xx"), names.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "sut.cpu_ticks"), names.end());
}

// SUT-derived strings render only as VALUES: a scrape counter name carrying
// quotes, newlines, and backslashes survives as the exact string after a
// parse round trip — never as structure, never as a provenance prefix.
TEST(McpResourcesTest, SutStringsAreValuesNotStructure) {
  FakeServiceOptions service_options;
  const std::string hostile_name = "sut.title=\"a\\\"b\nc\\d\"";
  service_options.extra_counters[hostile_name] = 42;
  auto harness = MakeHarness(service_options);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  auto payload = ReadPayload(*harness, "nighthawk://series");
  ASSERT_TRUE(payload.ok()) << payload.status();
  bool found = false;
  for (const Value& entry : Field(*payload, "series").list_value().values()) {
    if (Text(Field(StructOf(entry), "name")) == hostile_name) {
      found = true;
      const Value& points = Field(StructOf(entry), "points");
      ASSERT_GT(points.list_value().values_size(), 0);
      EXPECT_EQ(Field(StructOf(points.list_value().values(0)), "value").number_value(), 42);
    }
  }
  EXPECT_TRUE(found) << "the hostile SUT name must survive verbatim as a value";

  // The same discipline through the session resource's persisted records.
  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "cancel_run", "{}")));
  auto session = ReadPayload(*harness, "nighthawk://session");
  ASSERT_TRUE(session.ok()) << session.status();
  found = false;
  for (const Value& record_value : Field(*session, "records").list_value().values()) {
    const Struct& record = StructOf(record_value);
    if (Text(Field(record, "type")) != "metric_series") {
      continue;
    }
    const Struct& counters = StructOf(Field(record, "counters"));
    if (counters.fields().contains(hostile_name)) {
      found = true;
      EXPECT_EQ(Field(counters, hostile_name).number_value(), 42);
    }
  }
  EXPECT_TRUE(found) << "the hostile SUT name must survive in the session records too";
}

// --- timeline ---------------------------------------------------------------

// PhaseSpec helpers for the renderer-direct pins: two phases whose declared
// worker counts differ (the cmp! mark), a censored boundary event, and an
// Output carrying post-run phase metadata.
nighthawk::client::PhaseSpec PhaseSpecOf(const std::string& name, int seconds,
                                         const std::string& concurrency) {
  nighthawk::client::PhaseSpec spec;
  spec.set_name(name);
  spec.mutable_duration()->set_seconds(seconds);
  spec.mutable_concurrency()->set_value(concurrency);
  spec.set_requests_per_second(100);
  return spec;
}

nighthawk::client::PhaseBoundaryEvent BoundaryOf(const std::string& ending, int ending_index,
                                                 bool censored) {
  nighthawk::client::PhaseBoundaryEvent event;
  event.set_phase_index(ending_index + 1);
  event.set_phase_id("spike");
  event.set_ending_phase_index(ending_index);
  event.set_ending_phase_id(ending);
  event.set_wall_time_unix_ns(1'000'000);
  event.set_monotonic_ns(2'000'000);
  event.mutable_drain_duration()->set_nanos(500'000'000);
  event.mutable_idle_gap()->set_nanos(200'000'000);
  event.set_worker_count(2);
  event.set_next_phase_cold_started(true);
  event.set_drain_censored(censored);
  return event;
}

nighthawk::client::KnobEpochRecord AppliedEpochOf(const std::string& epoch_id,
                                                  const std::string& phase_id) {
  nighthawk::client::KnobEpochRecord record;
  record.set_epoch_id(epoch_id);
  record.set_phase_id(phase_id);
  record.set_wall_time_unix_ns(3'000'000);
  record.set_monotonic_ns(4'000'000);
  record.set_new_rps(200);
  return record;
}

// The timeline matches the TUI strip's semantics (the extracted builders):
// the boundary fields land on their ending phase's segment, the censor and
// cold marks ride the same fields the strip renders, and the post-run source
// is the Output's phase metadata (the metadata censor survives the rebuild —
// the M2-close panel major).
TEST(McpResourcesTest, TimelineMatchesStripSemantics) {
  nighthawk::client::CommandLineOptions options;
  *options.add_phases() = PhaseSpecOf("warm", 2, "2");
  *options.add_phases() = PhaseSpecOf("spike", 2, "4");

  std::vector<nighthawk::client::PhaseBoundaryEvent> events;
  events.push_back(BoundaryOf("warm", 0, /*censored=*/true));

  // Pre-run: the streamed events are the source (the pool/claim cells stay
  // unset — DQ-3: never a fabricated cell).
  Struct payload = RenderTimelinePayload("session.nhsession", options, events, {},
                                         /*current_phase_id=*/"spike", {});
  const Value& segments = Field(payload, "segments");
  ASSERT_EQ(segments.list_value().values_size(), 2);
  const Struct& warm = StructOf(segments.list_value().values(0));
  const Struct& spike = StructOf(segments.list_value().values(1));
  EXPECT_EQ(Text(Field(warm, "phase_id")), "warm");
  EXPECT_EQ(Field(warm, "duration_seconds").number_value(), 2);
  EXPECT_EQ(Field(warm, "drain_seconds").number_value(), 0.5);
  EXPECT_EQ(Field(warm, "idle_gap_seconds").number_value(), 0.2);
  EXPECT_TRUE(Field(warm, "drain_censored").bool_value());
  EXPECT_TRUE(Field(warm, "worker_count_changed").bool_value());
  EXPECT_TRUE(Field(warm, "new_workers_cold_started").bool_value());
  EXPECT_FALSE(Field(warm, "has_completion").bool_value());
  EXPECT_TRUE(Field(spike, "is_current").bool_value());
  EXPECT_FALSE(Field(spike, "drain_censored").bool_value());
  // The boundary array carries the engine's discrete join keys verbatim.
  const Value& boundaries = Field(payload, "boundaries");
  ASSERT_EQ(boundaries.list_value().values_size(), 1);
  const Struct& boundary = StructOf(boundaries.list_value().values(0));
  EXPECT_EQ(Field(boundary, "wall_time_unix_ns").number_value(), 1'000'000);
  EXPECT_EQ(Field(boundary, "monotonic_ns").number_value(), 2'000'000);
  EXPECT_TRUE(Field(boundary, "drain_censored").bool_value());

  // Post-run: the Output's phase metadata is the source — the censor mark
  // must survive the rebuild, and the claim cells arrive from the metadata.
  std::vector<nighthawk::client::PhaseMetadata> metadata(2);
  metadata[0].set_phase_id("warm");
  metadata[0].set_phase_index(0);
  metadata[0].set_drain_censored(true);
  metadata[0].set_completion_ratio(0.75);
  metadata[0].set_gate_exceeded(3);
  metadata[1].set_phase_id("spike");
  metadata[1].set_phase_index(1);
  metadata[1].set_workers_changed_vs_prior_phase(true);
  payload = RenderTimelinePayload("session.nhsession", options, events, metadata, "spike", {});
  const Struct& post_warm = StructOf(Field(payload, "segments").list_value().values(0));
  const Struct& post_spike = StructOf(Field(payload, "segments").list_value().values(1));
  EXPECT_TRUE(Field(post_warm, "drain_censored").bool_value());
  EXPECT_TRUE(Field(post_warm, "has_completion").bool_value());
  EXPECT_EQ(Field(post_warm, "completion_ratio").number_value(), 0.75);
  EXPECT_EQ(Field(post_warm, "gate_exceeded").number_value(), 3);
  // The boundary's worker-set change is recorded on the SUCCESSOR's
  // metadata — it renders on the ending phase's segment (warm).
  EXPECT_TRUE(Field(post_warm, "worker_count_changed").bool_value());

  // The epoch marks: applied acks mark their phase in ack order; a rejected
  // attempt (empty epoch id) marks nothing; a bend whose phase no segment
  // carries lands on the LAST segment rather than vanishing.
  std::vector<nighthawk::client::KnobEpochRecord> epochs;
  epochs.push_back(AppliedEpochOf("epoch-1", "warm"));
  nighthawk::client::KnobEpochRecord rejected;
  rejected.set_rejection_note("another live update is still in flight; bends are not queued");
  epochs.push_back(rejected);
  epochs.push_back(AppliedEpochOf("epoch-2", "no-such-phase"));
  payload = RenderTimelinePayload("session.nhsession", options, events, metadata, "spike",
                                  epochs);
  const Struct& marked_warm = StructOf(Field(payload, "segments").list_value().values(0));
  const Struct& marked_spike = StructOf(Field(payload, "segments").list_value().values(1));
  const Value& warm_marks = Field(marked_warm, "epoch_ids");
  ASSERT_EQ(warm_marks.list_value().values_size(), 1);
  EXPECT_EQ(Text(warm_marks.list_value().values(0)), "epoch-1");
  const Value& spike_marks = Field(marked_spike, "epoch_ids");
  ASSERT_EQ(spike_marks.list_value().values_size(), 1);
  EXPECT_EQ(Text(spike_marks.list_value().values(0)), "epoch-2");

  // Empty phase specs (a legacy run without a phase vector): no segments,
  // the honest legacy shape.
  payload = RenderTimelinePayload("session.nhsession", {}, events, metadata, "spike", epochs);
  EXPECT_EQ(Field(payload, "segments").list_value().values_size(), 0);

  // Provenance is a FIELD, never a prefix line inside the JSON.
  EXPECT_EQ(Text(Field(StructOf(Field(payload, "provenance")), "session")), "session.nhsession");
  EXPECT_EQ(Text(Field(StructOf(Field(payload, "provenance")), "current_phase_id")), "spike");
}

// The wire path: after a phased run's cancellation, the timeline resource
// reflects the streamed boundary (censored, cold-started) and the applied
// bend's epoch mark, and the provenance field names the session.
TEST(McpResourcesTest, TimelineResourceOverTheWire) {
  FakeServiceOptions service_options;
  service_options.phases = {"warm", "spike"};
  service_options.ticks_per_phase = 2;
  service_options.boundary_censored = true;
  auto harness = MakeHarness(service_options);
  // A phased start: the timeline joins on the START REQUEST's phase specs
  // (the controller's truth), so the run must carry the same two phases the
  // double streams.
  Struct started = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "phases": "warm:100:60:2,spike:400:60:4"})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return Text(Field(StructOf(Field(status, "current")), "phase_id")) == "spike";
  });
  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "steer", R"json({"rps": 20})json")));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return Text(Field(StructOf(Field(status, "current")), "epoch_id")) == "epoch-1";
  });
  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "cancel_run", "{}")));

  Struct payload = ReadPayloadOrDie(*harness, "nighthawk://timeline");
  const Value& segments = Field(payload, "segments");
  ASSERT_EQ(segments.list_value().values_size(), 2);
  const Struct& warm = StructOf(segments.list_value().values(0));
  const Struct& spike = StructOf(segments.list_value().values(1));
  // The censor mark survives the post-run rebuild (the metadata path); the
  // applied bend marks the phase it landed in (the steer was sent while
  // spike was current — the controller captures the send-time phase).
  EXPECT_TRUE(Field(warm, "drain_censored").bool_value());
  EXPECT_EQ(Field(warm, "epoch_ids").list_value().values_size(), 0);
  const Value& spike_marks = Field(spike, "epoch_ids");
  ASSERT_EQ(spike_marks.list_value().values_size(), 1);
  EXPECT_EQ(Text(spike_marks.list_value().values(0)), "epoch-1");
  ASSERT_EQ(Field(payload, "boundaries").list_value().values_size(), 1);
  EXPECT_FALSE(Text(Field(StructOf(Field(payload, "provenance")), "session")).empty());
}

// --- session ----------------------------------------------------------------

// The session resource: a MID-RUN read renders the records honestly; a torn
// tail (a crashed process's length prefix claiming more bytes than the file
// holds) is DISCLOSED — tail_skipped + the reader's reason naming the byte
// offset — never silently skipped, never a parse error.
TEST(McpResourcesTest, SessionMidRunWithTailedSkip) {
  // The torn-tail half, against a real recorder (the file is sealed first,
  // then corrupted the way a crash mid-write corrupts it).
  Tui::SessionRecorder recorder;
  const std::string prefix = TempPrefix();
  const int64_t started_unix_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  nighthawk::client::SessionHeader header;
  header.set_format_version("1");
  header.set_started_unix_ns(started_unix_ns);
  auto path_or = recorder.StartRun(prefix, started_unix_ns, header);
  ASSERT_TRUE(path_or.ok()) << path_or.status();
  nighthawk::client::SessionRecord snapshot;
  auto& series = *snapshot.mutable_metric_series();
  series.set_wall_time_unix_ns(1'100);
  series.set_monotonic_ns(100);
  series.set_phase_id("warm");
  (*series.mutable_counters())["benchmark.http_2xx"] = 10;
  recorder.AppendSnapshot(snapshot);
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(true);
  ASSERT_TRUE(recorder.FlushAndSeal(run_end).ok());
  {
    std::ofstream torn(*path_or, std::ios::binary | std::ios::app);
    torn << static_cast<char>(0x85);
    torn << "torn!";
  }
  auto torn_prefix = recorder.ReadSealedPrefix();
  ASSERT_TRUE(torn_prefix.ok()) << torn_prefix.status();

  Struct payload = RenderSessionPayload(*path_or, *torn_prefix);
  EXPECT_TRUE(Field(payload, "tail_skipped").bool_value());
  EXPECT_FALSE(Text(Field(payload, "tail_reason")).empty());
  const Value& records = Field(payload, "records");
  ASSERT_EQ(records.list_value().values_size(), 3);
  const Struct& rendered_header = StructOf(records.list_value().values(0));
  EXPECT_EQ(Text(Field(rendered_header, "type")), "header");
  EXPECT_EQ(Text(Field(rendered_header, "format_version")), "1");
  const Struct& rendered_series = StructOf(records.list_value().values(1));
  EXPECT_EQ(Text(Field(rendered_series, "type")), "metric_series");
  EXPECT_EQ(Text(Field(rendered_series, "phase_id")), "warm");
  EXPECT_EQ(Field(rendered_series, "monotonic_ns").number_value(), 100);
  EXPECT_EQ(Field(StructOf(Field(rendered_series, "counters")), "benchmark.http_2xx")
                .number_value(),
            10);
  const Struct& rendered_run_end = StructOf(records.list_value().values(2));
  EXPECT_EQ(Text(Field(rendered_run_end, "type")), "run_end");
  EXPECT_TRUE(Field(rendered_run_end, "completed").bool_value());
  // Provenance is a FIELD naming the session file.
  EXPECT_EQ(Text(Field(StructOf(Field(payload, "provenance")), "session")), *path_or);

  // The mid-run half, over the wire: the resource reads while the run is in
  // flight (an honest prefix — only flushed frames), and after the seal the
  // full record set is there: header first, metric series, run end last.
  auto harness = MakeHarness();
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });
  auto mid_run = ReadPayload(*harness, "nighthawk://session");
  ASSERT_TRUE(mid_run.ok()) << mid_run.status();
  EXPECT_FALSE(Field(*mid_run, "tail_skipped").bool_value());
  // The honest prefix: only flushed frames. The writer's stream buffer may
  // hold everything mid-run, so the record count is not pinned here — but
  // whatever is visible must start at the header (file order).
  const Value& mid_records = Field(*mid_run, "records");
  if (mid_records.list_value().values_size() > 0) {
    EXPECT_EQ(Text(Field(StructOf(mid_records.list_value().values(0)), "type")), "header");
  }

  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "cancel_run", "{}")));
  auto sealed = ReadPayload(*harness, "nighthawk://session");
  ASSERT_TRUE(sealed.ok()) << sealed.status();
  const Value& sealed_records = Field(*sealed, "records");
  ASSERT_GE(sealed_records.list_value().values_size(), 3);
  EXPECT_EQ(Text(Field(StructOf(sealed_records.list_value().values(0)), "type")), "header");
  bool saw_series = false;
  for (const Value& record_value : sealed_records.list_value().values()) {
    if (Text(Field(StructOf(record_value), "type")) == "metric_series") {
      saw_series = true;
    }
  }
  EXPECT_TRUE(saw_series);
  const Value& last =
      sealed_records.list_value().values(sealed_records.list_value().values_size() - 1);
  EXPECT_EQ(Text(Field(StructOf(last), "type")), "run_end");
  // The double delivers the final output on the cancellation arm, so the
  // seal records the run as completed — the controller's honest semantics.
  EXPECT_TRUE(Field(StructOf(last), "completed").bool_value());
}

} // namespace
} // namespace Client
} // namespace Nighthawk
