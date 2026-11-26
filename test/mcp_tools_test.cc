// FORK-ONLY (We-Amp): the MCP steering tools' contract pins (MCP plan Task
// 2). Target scoping binds at CONNECT (resolve -> check-all -> rewrite-to-IP,
// refused before any engine connection), the load ceiling applies to the
// declared global total at start_run AND steer, model validation errors
// surface verbatim, the bounded waits return within their stated bounds
// (steer 2x flush + 2s = 4s default; cancel_run --timeout + 5s = 35s default)
// with "timed out after <X>s", and a concurrent steer surfaces the engine's
// one-in-flight rejection verbatim, never queued.
//
// The in-proc service is a wire-faithful NighthawkService double: it streams
// LiveSnapshots, acks updates with "epoch-<n>" after a configurable delay,
// enforces the same one-in-flight guard with the engine's exact rejection
// string, and completes cancelled runs with a final Output (or wedges, for
// the 35s bound pin). The engine-side ack/guard semantics it mirrors are
// pinned against the real engine in tui_run_feed_test and live_update_test;
// what is pinned HERE is the MCP layer's bounds, verbatim surfacing, and
// FIFO ack correlation.
//
// Plain cc_test for the same quarantine reason as the headless/TUI tests:
// mcp_lib carries no Envoy C++ sources and no test-helper stack.
#include <grpc++/grpc++.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
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
#include "api/request_source/request_source_plugin.pb.h"
#include "envoy/config/core/v3/base.pb.h"
#include "envoy/extensions/transport_sockets/tls/v3/tls.pb.h"

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

// The handler object of a tool-call envelope (the structuredContent part;
// CallTool returns the whole envelope).
const Struct& Structured(const Struct& result) { return StructOf(Field(result, "structuredContent")); }

// The verbatim reason of an isError envelope (content[0].text).
std::string ErrorText(const Struct& result) {
  const Value& content = Field(result, "content");
  return Text(StructOf(content.list_value().values(0)).fields().at("text"));
}

// A wire-faithful in-proc NighthawkService double (see the file comment).
// Writes are serialized (the sync stream's Write is not thread-safe across
// the ticker/ack threads); reads happen only on the handler thread, the
// same shape the real ServiceImpl's ack threads produce.
class FakeNighthawkService final : public nighthawk::client::NighthawkService::Service {
public:
  // `end_after_snapshots` > 0 makes the service end the run ON ITS OWN after
  // that many streamed snapshots (final output, then the stream closes) —
  // the duration-expired natural end the MCP server must seal on.
  FakeNighthawkService(absl::Duration ack_delay, bool stall_after_cancel,
                       int end_after_snapshots = 0)
      : ack_delay_(ack_delay),
        stall_after_cancel_(stall_after_cancel),
        end_after_snapshots_(end_after_snapshots) {}

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
      run_epoch_ = 0;
      update_in_flight_ = false;
      done_ = false;
      snapshot_count_ = 0;
    }
    nighthawk::client::ExecutionRequest request;
    while (stream->Read(&request)) {
      if (request.has_start_request()) {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          start_requests_.push_back(request.start_request());
        }
        WriteLocked(stream, SnapshotResponse());
        StartTicker(stream);
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
          // The engine's one-in-flight guard, verbatim (Task 8a).
          nighthawk::client::ExecutionResponse response;
          auto* result = response.mutable_update_result();
          result->set_applied(false);
          result->set_rejection_reason(
              "another live update is still in flight; bends are not queued");
          WriteLocked(stream, response);
        } else {
          // The ack thread captures ITS stream: a second run's handler must
          // never see a first run's late ack (a shared stream_ member would
          // hand the late write a freed stream once this handler returned).
          std::thread ack([this, stream, candidate]() { AckAfterDelay(stream, candidate); });
          std::lock_guard<std::mutex> lock(mutex_);
          ack_threads_.push_back(std::move(ack));
        }
      } else if (request.has_cancellation_request()) {
        if (stall_after_cancel_) {
          // The wedged service: the stream stays open, nothing more is
          // ever written — exactly the stalled-service case the cancel
          // bound exists for. Ends only when the client cancels the RPC.
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
        WriteLocked(stream, OutputResponse());
        JoinAcks();
        return grpc::Status::OK;
      }
    }
    // The client cancelled or closed the stream: no ack thread may outlive
    // the RPC's stream object (a late Write would hit freed memory).
    StopTicker();
    JoinAcks();
    return grpc::Status::OK;
  }

  // The StartRequests the engine "received" (the scope-check rewrite is
  // proven against what actually traveled the wire).
  std::vector<nighthawk::client::StartRequest> startRequests() {
    std::lock_guard<std::mutex> lock(mutex_);
    return start_requests_;
  }

private:
  nighthawk::client::ExecutionResponse SnapshotResponse() {
    nighthawk::client::ExecutionResponse response;
    auto* snapshot = response.mutable_live_snapshot();
    snapshot->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    snapshot->set_monotonic_ns(++monotonic_ns_);
    snapshot->set_phase_id("main");
    ++snapshot_count_;
    if (run_epoch_ > 0) {
      snapshot->set_epoch_id(absl::StrCat("epoch-", run_epoch_));
    }
    (*snapshot->mutable_counters())["benchmark.http_2xx"] =
        static_cast<uint64_t>(monotonic_ns_);
    return response;
  }

  nighthawk::client::ExecutionResponse OutputResponse() {
    nighthawk::client::ExecutionResponse response;
    response.mutable_output();
    return response;
  }

  void WriteLocked(
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse, nighthawk::client::ExecutionRequest>*
          stream,
      const nighthawk::client::ExecutionResponse& response) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stream != nullptr) {
      stream->Write(response);
    }
  }

  void StartTicker(
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse, nighthawk::client::ExecutionRequest>*
          stream) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (ticking_) {
        return;
      }
      ticking_ = true;
    }
    ticker_ = std::thread([this, stream]() {
      while (true) {
        absl::SleepFor(absl::Milliseconds(100));
        // Build the snapshot under the lock, write OUTSIDE it (WriteLocked
        // takes the lock; holding it across the write would self-deadlock).
        nighthawk::client::ExecutionResponse response;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (!ticking_ || done_) {
            return;
          }
          response = SnapshotResponse();
        }
        WriteLocked(stream, response);
        // The natural end: the service's own terminal output, then the RPC
        // closes (the server-side cancel ends the handler's Read loop, and
        // with it the stream — the duration-expired engine shape).
        if (end_after_snapshots_ > 0 && snapshot_count_ >= end_after_snapshots_) {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            done_ = true;
          }
          WriteLocked(stream, OutputResponse());
          if (context_ != nullptr) {
            context_->TryCancel();
          }
          return;
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

  void AckAfterDelay(
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse, nighthawk::client::ExecutionRequest>*
          stream,
      int candidate) {
    // The applier analog: it honors its deadline, so a delay past the
    // steer bound is exactly the "hogging applier" case — the ack resolves
    // only after the tool could ever wait for it.
    absl::SleepFor(ack_delay_);
    nighthawk::client::ExecutionResponse response;
    auto* result = response.mutable_update_result();
    result->set_applied(true);
    result->set_epoch_id(absl::StrCat("epoch-", candidate));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      result->set_monotonic_ns(++monotonic_ns_);
      result->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    }
    WriteLocked(stream, response);
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

  std::mutex mutex_;
  grpc::ServerContext* context_ = nullptr;
  const absl::Duration ack_delay_;
  const bool stall_after_cancel_;
  const int end_after_snapshots_;
  std::vector<nighthawk::client::StartRequest> start_requests_;
  std::vector<std::thread> ack_threads_;
  std::thread ticker_;
  bool ticking_ = false;
  bool done_ = false;
  bool update_in_flight_ = false;
  int run_epoch_ = 0;
  int64_t monotonic_ns_ = 0;
  int snapshot_count_ = 0;
};

// One wired steering server: fake service + gRPC server + channel +
// controller + registries + McpServer over the waiter pool, plus the
// response collection the CallTool helper waits on.
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
  // Declared AFTER the server so it is destroyed FIRST: its destructor
  // joins any in-flight deferred handler while the server and controller
  // are still alive (a steer completing into a torn-down server would be
  // use-after-free).
  std::unique_ptr<WaiterPool> pool;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<int, Struct> responses;
  int next_id = 0;
};

std::unique_ptr<Harness> MakeHarness(absl::Duration ack_delay = absl::ZeroDuration(),
                                     bool stall_after_cancel = false,
                                     SteeringToolOptions options = {},
                                     HostResolver resolver = nullptr,
                                     int end_after_snapshots = 0) {
  auto harness = std::make_unique<Harness>();
  harness->options = options;
  harness->service = std::make_unique<FakeNighthawkService>(ack_delay, stall_after_cancel,
                                                            end_after_snapshots);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &harness->grpc_port);
  builder.RegisterService(harness->service.get());
  harness->grpc_server = builder.BuildAndStart();
  harness->channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", harness->grpc_port),
                                         grpc::InsecureChannelCredentials());
  harness->controller =
      std::make_unique<SteeringController>(harness->channel, harness->options, resolver);
  RegisterEchoTool(harness->tools);
  RegisterSteeringTools(harness->tools, *harness->controller);
  // The telemetry resources (the session seal pins read nighthawk://session
  // through the server like an agent does).
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

  // Initialize once: every test speaks tools/* only.
  Struct init_request;
  (*init_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*init_request.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*init_request.mutable_fields())["method"].set_string_value("initialize");
  (*init_request.mutable_fields())["params"] = ToValue(*ParseJsonObject(
      R"json({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test"}})json"));
  (void)harness->server->HandleLine(EmitCanonicalStruct(init_request));
  return harness;
}

// One initialize round-trip, then one tools/call; returns the RESULT struct
// (the envelope) once the id's response lands — inline tools answer during
// HandleLine, deferred tools land through the sink when the handler
// completes.
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

// Waits for an ALREADY-SUBMITTED request's response by id (a deferred tool
// whose HandleLine returned nothing).
absl::StatusOr<Struct> CallToolWaitById(Harness& harness, int id, absl::Duration budget) {
  std::unique_lock<std::mutex> lock(harness.mutex);
  if (!harness.cv.wait_for(lock, std::chrono::nanoseconds(absl::ToChronoNanoseconds(budget)),
                           [&harness, id]() { return harness.responses.count(id) > 0; })) {
    return absl::DeadlineExceededError(absl::StrCat("no response for id ", id));
  }
  return std::move(harness.responses[id]);
}

// Blocks until `predicate` holds (polling get_status), or fails the test.
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

// Blocks until the fake service has processed the run's start request (the
// wire is asynchronous: feed.start() returns after the write, the service
// reads on its own thread).
void WaitForStartRequest(Harness& harness, absl::Duration budget = absl::Seconds(5)) {
  const absl::Time deadline = absl::Now() + budget;
  while (absl::Now() < deadline) {
    if (!harness.service->startRequests().empty()) {
      return;
    }
    absl::SleepFor(absl::Milliseconds(20));
  }
  ADD_FAILURE() << "the service never processed the start request within " << budget;
}

// One resources/read round trip: contents[0].text parsed back (the same
// parse-then-compare discipline as mcp_loop_test's ReadPayload).
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

// MCP-plan Task 2 pin: every shipped tool's schema states
// "additionalProperties": false explicitly (the validator defaults closed;
// the listing says so too).
TEST(McpToolsTest, SteeringToolSchemasPinAdditionalPropertiesClosed) {
  auto harness = MakeHarness();
  Struct listing_request;
  (*listing_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*listing_request.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*listing_request.mutable_fields())["method"].set_string_value("tools/list");
  (void)harness->server->HandleLine(EmitCanonicalStruct(listing_request));

  Struct listing;
  {
    std::lock_guard<std::mutex> lock(harness->mutex);
    listing = harness->responses[harness->next_id];
  }
  const Value& tools = Field(listing, "tools");
  ASSERT_EQ(tools.list_value().values_size(), 5); // echo + the four steering tools
  for (const Value& entry : tools.list_value().values()) {
    const std::string name = Text(Field(StructOf(entry), "name"));
    const Struct& schema = StructOf(Field(StructOf(entry), "inputSchema"));
    EXPECT_TRUE(Field(schema, "additionalProperties").bool_value() == false)
        << name << " must pin additionalProperties: false";
  }
}

// Review Focus 1: a public/off-box target (by literal, by DNS name, by
// IPv6-mapped form, by userinfo trick) refused BEFORE any engine connection,
// with the scoping message.
TEST(McpToolsTest, StartRunRefusesOffBoxTargets) {
  SteeringToolOptions options;
  // Every NAME resolves public; literal IPs are refused as-is. The fake
  // service is never touched — refusals happen pre-connect.
  auto harness = MakeHarness(
      absl::ZeroDuration(), false, options,
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"93.184.216.34"};
      });

  const std::vector<std::string> targets = {
      // A literal public IP.
      "http://93.184.216.34:80/",
      // A DNS name resolving public (injected).
      "http://example.test/",
      // The IPv6-mapped form of a public address (must not read as "some
      // IPv6 address" and pass).
      "http://[::ffff:93.184.216.34]/",
      // The userinfo trick: the host is what follows the '@'.
      "http://user@93.184.216.34:80/",
  };
  for (const std::string& target : targets) {
    Struct result = CallToolOrDie(*harness, "start_run",
                                  absl::StrCat(R"json({"uri": ")json", target, R"json("})json"));
    EXPECT_TRUE(IsIsError(result)) << target << " must be refused";
    const std::string reason = ErrorText(result);
    EXPECT_NE(reason.find("refusing before any engine connection"), std::string::npos)
        << target << ": " << reason;
    EXPECT_NE(reason.find("93.184.216.34"), std::string::npos)
        << target << ": the scoping message names the offending address: " << reason;
  }
  // Nothing started: no session, no run.
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_FALSE(Field(StructOf(Field(status, "run")), "in_flight").bool_value());
  EXPECT_TRUE(Text(Field(StructOf(Field(status, "run")), "session")).empty());
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// The scope-check rewrites the engine target to the resolved IP — proven
// against what the engine actually received on the wire.
TEST(McpToolsTest, StartRunRewritesTargetToResolvedIP) {
  auto harness = MakeHarness(
      absl::ZeroDuration(), false, {},
      [](absl::string_view host) -> absl::StatusOr<std::vector<std::string>> {
        if (host == "localhost") {
          return std::vector<std::string>{"127.0.0.1"};
        }
        return absl::InvalidArgumentError(absl::StrCat("unresolved: ", host));
      });
  const std::string uri =
      absl::StrCat("http://localhost:", harness->grpc_port, "/path?query=1");
  Struct result = CallToolOrDie(*harness, "start_run",
                                absl::StrCat(R"json({"uri": ")json", uri, R"json(",
                                              "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  const Struct& structured = StructOf(Field(result, "structuredContent"));
  EXPECT_TRUE(Field(structured, "run_started").bool_value());
  const std::string rewritten =
      absl::StrCat("http://127.0.0.1:", harness->grpc_port, "/path?query=1");
  EXPECT_EQ(Text(Field(structured, "resolved_target")), rewritten);
  // The rewritten target is what the engine received; the session path is
  // reported and the session file carries the rewritten request in its
  // header (the T0 recorder).
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  EXPECT_EQ(harness->service->startRequests()[0].options().uri().value(), rewritten);
  EXPECT_FALSE(Text(Field(structured, "session")).empty());
}

// The load ceiling: a declared total above --max-rps is refused naming the
// cap, before any engine connection.
TEST(McpToolsTest, StartRunEnforcesRpsCeiling) {
  SteeringToolOptions options;
  options.max_rps = 100;
  auto harness = MakeHarness(absl::ZeroDuration(), false, options);
  Struct result = CallToolOrDie(
      *harness, "start_run",
      R"json({"uri": "http://127.0.0.1:80/", "rps": 101, "duration_s": 5})json");
  EXPECT_TRUE(IsIsError(result));
  const std::string reason = ErrorText(result);
  EXPECT_NE(reason.find("exceeds the load ceiling of 100 rps (--max-rps)"), std::string::npos)
      << reason;
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// Model validation surfaces verbatim: a start_run without rps carries the
// dialog model's own field error.
TEST(McpToolsTest, StartRunSurfacesValidatorError) {
  auto harness = MakeHarness();
  Struct result = CallToolOrDie(*harness, "start_run", R"json({"uri": "http://127.0.0.1:80/"})json");
  EXPECT_TRUE(IsIsError(result));
  EXPECT_EQ(ErrorText(result), "rps: must be > 0");
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// The connections argument lands DIRECTLY on the built CommandLineOptions
// (the model carries it; see the dialog model pin) and travels the wire.
TEST(McpToolsTest, ConnectionsFieldRoundTrips) {
  auto harness = MakeHarness(
      absl::ZeroDuration(), false, {},
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"127.0.0.1"};
      });
  Struct result = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60, "connections": 64})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  ASSERT_TRUE(harness->service->startRequests()[0].options().has_connections());
  EXPECT_EQ(harness->service->startRequests()[0].options().connections().value(), 64);
}

// The churn control (drive backlog item 2): max_requests_per_connection
// travels the wire onto the StartRequest's options, so 1-req-per-connection
// storm arms are drivable through the tool layer.
TEST(McpToolsTest, MaxRequestsPerConnectionRoundTrips) {
  auto harness = MakeHarness(
      absl::ZeroDuration(), false, {},
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"127.0.0.1"};
      });
  Struct result = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60,
                    "max_requests_per_connection": 1})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  ASSERT_TRUE(
      harness->service->startRequests()[0].options().has_max_requests_per_connection());
  EXPECT_EQ(
      harness->service->startRequests()[0].options().max_requests_per_connection().value(), 1);
}

// The floor: a non-positive max_requests_per_connection is refused naming the
// field, before any engine connection (the proto pins gte 1; a wrap-past of
// the uint32 cast must not drive a huge connection-churn figure).
TEST(McpToolsTest, MaxRequestsPerConnectionRefusesNonPositive) {
  auto harness = MakeHarness();
  for (const std::string& value : {"0", "-4"}) {
    Struct result = CallToolOrDie(
        *harness, "start_run",
        absl::StrCat(R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5,
                            "max_requests_per_connection": )json",
                     value, R"json(})json"));
    EXPECT_TRUE(IsIsError(result)) << value << " must be refused";
    EXPECT_EQ(ErrorText(result),
              absl::StrCat("max_requests_per_connection must be a positive integer (got ",
                           value, ")"))
        << value << ": " << ErrorText(result);
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// Request headers (drive backlog item 2): the small headers array builds
// HeaderValueOption entries on the StartRequest — the connection:close
// fallback for the churn arms is composable again.
TEST(McpToolsTest, HeadersRoundTrip) {
  auto harness = MakeHarness(
      absl::ZeroDuration(), false, {},
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"127.0.0.1"};
      });
  Struct result = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60,
                    "headers": [{"name": "connection", "value": "close"},
                                {"name": "x-edge-probe", "value": "c2"}]})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  // Held BY VALUE: the reference-binding dance over the temporary that
  // startRequests() returns is not worth the lifetime subtleties.
  const nighthawk::client::StartRequest received = harness->service->startRequests()[0];
  const auto& built_headers = received.options().request_options().request_headers();
  ASSERT_EQ(built_headers.size(), 2);
  EXPECT_EQ(built_headers[0].header().key(), "connection");
  EXPECT_EQ(built_headers[0].header().value(), "close");
  EXPECT_EQ(built_headers[1].header().key(), "x-edge-probe");
  EXPECT_EQ(built_headers[1].header().value(), "c2");
}

// The header security floor: values, not instructions — entries that are not
// {name, value} string pairs, carry an empty/colon/space-bearing name, or
// smuggle CR/LF/NUL framing into a name or value are refused naming the rule;
// the array is capped before any engine connection.
TEST(McpToolsTest, HeadersRefuseUnsafeEntries) {
  auto harness = MakeHarness();
  const std::string base = R"json({"uri": "http://127.0.0.1:80/", "rps": 10,
      "duration_s": 5, "headers": [)json";
  const std::vector<std::pair<std::string, std::string>> cases = {
      {R"json("not-an-object")json", "headers entries must be objects"},
      {R"json({"name": "x-only"})json", "headers entries must be objects"},
      {R"json({"name": "", "value": "v"})json", "non-empty"},
      {R"json({"name": "bad name", "value": "v"})json", "name must be a non-empty token"},
      {R"json({"name": "bad:name", "value": "v"})json", "name must be a non-empty token"},
      {R"json({"name": "x-a\r\nb", "value": "v"})json", "header injection"},
      {R"json({"name": "x-ok", "value": "a\r\nb: c"})json", "header injection"},
  };
  for (const auto& [entry, needle] : cases) {
    Struct result = CallToolOrDie(*harness, "start_run",
                                  absl::StrCat(base, entry, R"json(]})json"));
    EXPECT_TRUE(IsIsError(result)) << entry << " must be refused";
    EXPECT_NE(ErrorText(result).find(needle), std::string::npos)
        << entry << ": " << ErrorText(result);
  }
  // The cap: one past kMaxStartRunHeaders entries is refused naming it.
  std::string many;
  for (int i = 0; i <= 64; ++i) {
    if (!many.empty()) {
      many += ",";
    }
    many += absl::StrCat(R"json({"name": "x-h)json", i, R"json(", "value": "v"})json");
  }
  Struct capped =
      CallToolOrDie(*harness, "start_run", absl::StrCat(base, many, R"json(]})json"));
  EXPECT_TRUE(IsIsError(capped));
  EXPECT_NE(ErrorText(capped).find("cap"), std::string::npos) << ErrorText(capped);
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// N3a: the protocol argument maps h1/h2/h3 onto the StartRequest's protocol
// option (one fresh harness per value: one run per harness). The h3 case uses
// the only shape that can run: an https uri plus a Host header.
TEST(McpToolsTest, ProtocolRoundTrips) {
  struct Case {
    std::string argument;
    nighthawk::client::Protocol::ProtocolOptions expected;
    std::string scheme;
    std::string extra;
  };
  const std::vector<Case> cases = {
      {"h1", nighthawk::client::Protocol::HTTP1, "http", ""},
      {"h2", nighthawk::client::Protocol::HTTP2, "http", ""},
      {"h3", nighthawk::client::Protocol::HTTP3, "https",
       R"json(, "headers": [{"name": "Host", "value": "www.lyft.com"}])json"}};
  for (const Case& test_case : cases) {
    auto harness = MakeHarness(absl::ZeroDuration(), false, {},
                               [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                                 return std::vector<std::string>{"127.0.0.1"};
                               });
    Struct result = CallToolOrDie(
        *harness, "start_run",
        absl::StrCat(R"json({"uri": ")json", test_case.scheme, "://127.0.0.1:", harness->grpc_port,
                     R"json(/", "rps": 10, "duration_s": 60, "protocol": ")json",
                     test_case.argument, R"json(")json", test_case.extra, "}"));
    ASSERT_FALSE(IsIsError(result)) << test_case.argument << ": " << ErrorText(result);
    WaitForStartRequest(*harness);
    ASSERT_EQ(harness->service->startRequests().size(), 1) << test_case.argument;
    const nighthawk::client::StartRequest received = harness->service->startRequests()[0];
    ASSERT_TRUE(received.options().has_protocol()) << test_case.argument;
    EXPECT_EQ(received.options().protocol().value(), test_case.expected) << test_case.argument;
  }
}

// M1 (review): an h3 start_run that cannot complete a QUIC handshake is
// refused at the tool boundary, before any engine connection: an http uri
// (no TLS, so no QUIC transport), and an https uri without a Host header
// (the target is rewritten to its IP, so the SNI would be the IP and QUIC
// cannot skip certificate verification). The Host name match is
// case-insensitive, and an empty Host value does not count (re-review N2).
TEST(McpToolsTest, ProtocolH3RefusesHttpUriAndMissingHost) {
  auto harness = MakeHarness();
  Struct http_uri = CallToolOrDie(
      *harness, "start_run",
      R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5, "protocol": "h3",
              "headers": [{"name": "Host", "value": "www.lyft.com"}]})json");
  EXPECT_TRUE(IsIsError(http_uri));
  EXPECT_EQ(ErrorText(http_uri), "protocol h3 needs an https uri (HTTP/3 runs over QUIC with TLS)");

  for (const std::string& headers :
       {std::string(""), std::string(R"json(, "headers": [{"name": "x-a", "value": "b"}])json"),
        std::string(R"json(, "headers": [{"name": "Host", "value": ""}])json")}) {
    Struct no_host = CallToolOrDie(
        *harness, "start_run",
        absl::StrCat(R"json({"uri": "https://127.0.0.1:443/", "rps": 10, "duration_s": 5,
                            "protocol": "h3")json",
                     headers, "}"));
    EXPECT_TRUE(IsIsError(no_host)) << headers;
    EXPECT_NE(ErrorText(no_host).find("protocol h3 needs a headers entry"), std::string::npos)
        << headers << ": " << ErrorText(no_host);
  }
  EXPECT_TRUE(harness->service->startRequests().empty());

  // A lower-case "host" entry satisfies the check.
  auto accepting = MakeHarness(absl::ZeroDuration(), false, {},
                               [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                                 return std::vector<std::string>{"127.0.0.1"};
                               });
  Struct lower =
      CallToolOrDie(*accepting, "start_run",
                    absl::StrCat(R"json({"uri": "https://127.0.0.1:)json", accepting->grpc_port,
                                 R"json(/", "rps": 10, "duration_s": 60, "protocol": "h3",
                   "headers": [{"name": "host", "value": "www.lyft.com"}]})json"));
  EXPECT_FALSE(IsIsError(lower)) << ErrorText(lower);
}

// N3a: without the new arguments the built request is unchanged: no protocol
// and no body size are written (the engine defaults apply).
TEST(McpToolsTest, ProtocolAndBodySizeAbsentStayUnset) {
  auto harness = MakeHarness(absl::ZeroDuration(), false, {},
                             [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                               return std::vector<std::string>{"127.0.0.1"};
                             });
  Struct result =
      CallToolOrDie(*harness, "start_run",
                    absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                                 R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  const nighthawk::client::StartRequest received = harness->service->startRequests()[0];
  EXPECT_FALSE(received.options().has_protocol());
  EXPECT_FALSE(received.options().request_options().has_request_body_size());
}

// N3a: a protocol outside h1/h2/h3 is refused naming the field, before any
// engine connection (the schema's enum is advisory; the boundary enforces it).
TEST(McpToolsTest, ProtocolRefusesUnknownValues) {
  auto harness = MakeHarness();
  for (const std::string& value : {"http2", "H2", "h4", ""}) {
    Struct result = CallToolOrDie(
        *harness, "start_run",
        absl::StrCat(R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5,
                            "protocol": ")json",
                     value, R"json("})json"));
    EXPECT_TRUE(IsIsError(result)) << "'" << value << "' must be refused";
    EXPECT_EQ(ErrorText(result),
              absl::StrCat("protocol must be one of h1, h2, h3 (got '", value, "')"))
        << value << ": " << ErrorText(result);
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// N3a: request_body_size lands on the static RequestOptions (the CLI's
// --request-body-size surface); an explicit 0 is written as 0. (N3b: a body
// needs a method that carries one, so the run says POST.)
TEST(McpToolsTest, RequestBodySizeRoundTrips) {
  for (const uint32_t size : {0u, 1024u, 4194304u}) {
    auto harness = MakeHarness(absl::ZeroDuration(), false, {},
                               [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                                 return std::vector<std::string>{"127.0.0.1"};
                               });
    Struct result =
        CallToolOrDie(*harness, "start_run",
                      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                                   R"json(/", "rps": 10, "duration_s": 60, "method": "POST",
                            "request_body_size": )json",
                                   size, R"json(})json"));
    ASSERT_FALSE(IsIsError(result)) << size << ": " << ErrorText(result);
    WaitForStartRequest(*harness);
    ASSERT_EQ(harness->service->startRequests().size(), 1) << size;
    const nighthawk::client::StartRequest received = harness->service->startRequests()[0];
    ASSERT_TRUE(received.options().request_options().has_request_body_size()) << size;
    EXPECT_EQ(received.options().request_options().request_body_size().value(), size);
  }
}

// N3a: the body-size floor and cap: a negative (which would wrap the uint32
// cast) or a size above the proto's 4194304 cap is refused naming the range.
TEST(McpToolsTest, RequestBodySizeRefusesOutOfRange) {
  auto harness = MakeHarness();
  for (const std::string& value : {"-1", "4194305"}) {
    Struct result = CallToolOrDie(
        *harness, "start_run",
        absl::StrCat(R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5,
                            "request_body_size": )json",
                     value, R"json(})json"));
    EXPECT_TRUE(IsIsError(result)) << value << " must be refused";
    EXPECT_EQ(
        ErrorText(result),
        absl::StrCat("request_body_size must be an integer from 0 to 4194304 (got ", value, ")"))
        << value << ": " << ErrorText(result);
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// S2 (review): the body size is range- and integrality-checked on the JSON
// number itself, before any integer conversion (1e30 would be undefined
// behaviour in the cast; 1.5 would truncate to 1). The server's schema check
// already refuses non-integers over the wire, so this drives the handler
// directly to pin the boundary's own floor.
TEST(McpToolsTest, RequestBodySizeRefusesNonIntegerNumbersAtTheHandler) {
  auto harness = MakeHarness();
  const std::vector<std::pair<std::string, std::string>> cases = {{"1.5", "1.5"},
                                                                  {"1e30", "1e+30"}};
  for (const auto& [json_value, printed] : cases) {
    auto arguments = ParseJsonObject(
        absl::StrCat(R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5,
                            "request_body_size": )json",
                     json_value, "}"));
    ASSERT_TRUE(arguments.ok()) << json_value;
    auto started = harness->controller->StartRun(*arguments);
    ASSERT_FALSE(started.ok()) << json_value;
    EXPECT_EQ(
        started.status().message(),
        absl::StrCat("request_body_size must be an integer from 0 to 4194304 (got ", printed, ")"));
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// N3a: both new arguments compose with a phase run and the existing request
// shape (one StartRequest carries protocol, body size and the phase vector).
TEST(McpToolsTest, ProtocolAndBodySizeComposeWithPhases) {
  auto harness = MakeHarness(absl::ZeroDuration(), false, {},
                             [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                               return std::vector<std::string>{"127.0.0.1"};
                             });
  Struct result =
      CallToolOrDie(*harness, "start_run",
                    absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                                 R"json(/", "rps": 10, "phases": "warm:100:5:2,measure:100:20:2",
                   "protocol": "h2", "method": "POST", "request_body_size": 512})json"));
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  WaitForStartRequest(*harness);
  ASSERT_EQ(harness->service->startRequests().size(), 1);
  const nighthawk::client::StartRequest received = harness->service->startRequests()[0];
  EXPECT_EQ(received.options().protocol().value(), nighthawk::client::Protocol::HTTP2);
  EXPECT_EQ(received.options().request_options().request_body_size().value(), 512);
  ASSERT_EQ(received.options().phases_size(), 2);
  EXPECT_EQ(received.options().phases(1).name(), "measure");
}

// --- N3b / NH1: the rest of the start_run request shape ---------------------

// Starts a run against the harness's fake service with `extra` appended to
// the arguments (a leading comma included) and returns the StartRequest the
// engine received.
nighthawk::client::StartRequest StartAndCapture(Harness& harness, const std::string& scheme,
                                                const std::string& extra) {
  Struct result =
      CallToolOrDie(harness, "start_run",
                    absl::StrCat(R"json({"uri": ")json", scheme, "://127.0.0.1:", harness.grpc_port,
                                 R"json(/", "rps": 10, "duration_s": 60)json", extra, "}"));
  EXPECT_FALSE(IsIsError(result)) << extra << ": " << ErrorText(result);
  WaitForStartRequest(harness);
  EXPECT_EQ(harness.service->startRequests().size(), 1) << extra;
  return harness.service->startRequests().empty() ? nighthawk::client::StartRequest()
                                                  : harness.service->startRequests()[0];
}

std::unique_ptr<Harness> MakeLoopbackHarness() {
  return MakeHarness(absl::ZeroDuration(), false, {},
                     [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
                       return std::vector<std::string>{"127.0.0.1"};
                     });
}

// Calls start_run's handler directly (past the server's schema check, so the
// boundary's own refusals are what is pinned) and returns the refusal text.
std::string StartRunRefusal(Harness& harness, const std::string& arguments_json) {
  auto arguments = ParseJsonObject(arguments_json);
  EXPECT_TRUE(arguments.ok()) << arguments_json;
  if (!arguments.ok()) {
    return "";
  }
  auto started = harness.controller->StartRun(*arguments);
  EXPECT_FALSE(started.ok()) << arguments_json << " must be refused";
  return started.ok() ? "" : std::string(started.status().message());
}

// N3b: a body is refused on GET (explicit or the default) and on HEAD, so no
// row can carry "GET + N-byte body"; size 0 stays fine on GET.
TEST(McpToolsTest, RequestBodyOnGetOrHeadIsRefused) {
  auto harness = MakeHarness();
  EXPECT_EQ(StartRunRefusal(*harness, R"json({"uri": "http://127.0.0.1:80/", "rps": 10,
                                             "duration_s": 5, "request_body_size": 1024})json"),
            "request_body_size 1024 needs a method that carries a body (for example POST); a "
            "body on GET (the default method) is refused");
  EXPECT_EQ(StartRunRefusal(*harness, R"json({"uri": "http://127.0.0.1:80/", "rps": 10,
                                             "duration_s": 5, "method": "HEAD",
                                             "request_body_size": 1})json"),
            "request_body_size 1 needs a method that carries a body (for example POST); a body "
            "on HEAD is refused");
  EXPECT_TRUE(harness->service->startRequests().empty());
  auto accepting = MakeLoopbackHarness();
  const nighthawk::client::StartRequest received =
      StartAndCapture(*accepting, "http", R"json(, "request_body_size": 0)json");
  EXPECT_EQ(received.options().request_options().request_body_size().value(), 0);
}

// N3b: the method lands on the static RequestOptions; unknown values are
// refused naming the field.
TEST(McpToolsTest, MethodRoundTripsAndRefusesUnknownValues) {
  for (const auto& [argument, expected] :
       std::vector<std::pair<std::string, envoy::config::core::v3::RequestMethod>>{
           {"POST", envoy::config::core::v3::POST},
           {"PUT", envoy::config::core::v3::PUT},
           {"GET", envoy::config::core::v3::GET}}) {
    auto harness = MakeLoopbackHarness();
    const nighthawk::client::StartRequest received = StartAndCapture(
        *harness, "http", absl::StrCat(R"json(, "method": ")json", argument, R"json(")json"));
    EXPECT_EQ(received.options().request_options().request_method(), expected) << argument;
  }
  auto harness = MakeHarness();
  for (const std::string& value : {"post", "FETCH", ""}) {
    EXPECT_EQ(
        StartRunRefusal(*harness, absl::StrCat(R"json({"uri": "http://127.0.0.1:80/", "rps": 10,
                                                  "duration_s": 5, "method": ")json",
                                               value, R"json("})json")),
        absl::StrCat("method must be one of GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, "
                     "TRACE, PATCH (got '",
                     value, "')"));
  }
}

// N3b / NH1: burst size, loop mode, idle strategy, stream limit and the
// per-worker series opt-in map onto their CommandLineOptions fields; absent,
// none is written.
TEST(McpToolsTest, ShapeFieldsRoundTrip) {
  auto harness = MakeLoopbackHarness();
  const nighthawk::client::StartRequest received = StartAndCapture(
      *harness, "http",
      R"json(, "protocol": "h2", "burst_size": 8, "open_loop": true, "idle_strategy": "sleep",
             "max_concurrent_streams": 4, "worker_series": true)json");
  const nighthawk::client::CommandLineOptions& options = received.options();
  EXPECT_EQ(options.burst_size().value(), 8);
  EXPECT_TRUE(options.open_loop().value());
  EXPECT_EQ(options.sequencer_idle_strategy().value(),
            nighthawk::client::SequencerIdleStrategy::SLEEP);
  EXPECT_EQ(options.max_concurrent_streams().value(), 4);
  EXPECT_TRUE(options.worker_series().value());

  auto plain = MakeLoopbackHarness();
  const nighthawk::client::CommandLineOptions absent =
      StartAndCapture(*plain, "http", "").options();
  EXPECT_FALSE(absent.has_burst_size());
  EXPECT_FALSE(absent.has_open_loop());
  EXPECT_FALSE(absent.has_sequencer_idle_strategy());
  EXPECT_FALSE(absent.has_max_concurrent_streams());
  EXPECT_FALSE(absent.has_worker_series());
  EXPECT_FALSE(absent.has_tls_context());
  EXPECT_FALSE(absent.has_request_source_plugin_config());
}

TEST(McpToolsTest, ShapeFieldsRefuseBadValues) {
  auto harness = MakeHarness();
  const std::string base =
      R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5, )json";
  const std::vector<std::pair<std::string, std::string>> cases = {
      {R"json("burst_size": 0})json", "burst_size must be an integer from 1 to 1000000 (got 0)"},
      {R"json("burst_size": 1000001})json",
       "burst_size must be an integer from 1 to 1000000 (got 1000001)"},
      {R"json("burst_size": 2.5})json",
       "burst_size must be an integer from 1 to 1000000 (got 2.5)"},
      {R"json("open_loop": "yes"})json", "open_loop must be a boolean"},
      {R"json("idle_strategy": "busy"})json",
       "idle_strategy must be one of spin, poll, sleep (got 'busy')"},
      {R"json("max_concurrent_streams": 4})json",
       "max_concurrent_streams applies to h2 and h3 only; set protocol to h2 or h3"},
      {R"json("protocol": "h1", "max_concurrent_streams": 4})json",
       "max_concurrent_streams applies to h2 and h3 only; set protocol to h2 or h3"},
      {R"json("protocol": "h2", "max_concurrent_streams": 0})json",
       "max_concurrent_streams must be an integer from 1 to 2147483647 (got 0)"},
      {R"json("worker_series": 1})json", "worker_series must be a boolean"},
  };
  for (const auto& [arguments, message] : cases) {
    EXPECT_EQ(StartRunRefusal(*harness, base + arguments), message) << arguments;
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// N3b: a request mix packs into the in-line options-list request source; the
// static RequestOptions stay unset (a oneof on the wire). Entry defaults:
// method GET; path becomes :path.
TEST(McpToolsTest, RequestMixPacksInlineOptionsList) {
  auto harness = MakeLoopbackHarness();
  const nighthawk::client::StartRequest received =
      StartAndCapture(*harness, "http",
                      R"json(, "request_mix": [{"path": "/a"},
                               {"method": "POST", "path": "/b", "body_size": 128,
                                "headers": [{"name": "x-mix", "value": "1"}]}])json");
  const nighthawk::client::CommandLineOptions& options = received.options();
  EXPECT_FALSE(options.has_request_options());
  ASSERT_TRUE(options.has_request_source_plugin_config());
  EXPECT_EQ(options.request_source_plugin_config().name(),
            "nighthawk.in-line-options-list-request-source-plugin");
  nighthawk::request_source::InLineOptionsListRequestSourceConfig config;
  ASSERT_TRUE(options.request_source_plugin_config().typed_config().UnpackTo(&config));
  ASSERT_EQ(config.options_list().options_size(), 2);
  const nighthawk::client::RequestOptions& first = config.options_list().options(0);
  EXPECT_EQ(first.request_method(), envoy::config::core::v3::GET);
  ASSERT_EQ(first.request_headers_size(), 1);
  EXPECT_EQ(first.request_headers(0).header().key(), ":path");
  EXPECT_EQ(first.request_headers(0).header().value(), "/a");
  const nighthawk::client::RequestOptions& second = config.options_list().options(1);
  EXPECT_EQ(second.request_method(), envoy::config::core::v3::POST);
  EXPECT_EQ(second.request_body_size().value(), 128);
  ASSERT_EQ(second.request_headers_size(), 2);
  EXPECT_EQ(second.request_headers(1).header().key(), "x-mix");
  EXPECT_EQ(config.num_requests(), 0u);
}

TEST(McpToolsTest, RequestMixRefusals) {
  auto harness = MakeHarness();
  const std::string base =
      R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "duration_s": 5, )json";
  std::string too_many = R"json("request_mix": [)json";
  for (int i = 0; i < 257; ++i) {
    absl::StrAppend(&too_many, i == 0 ? "" : ",", R"json({"path": "/"})json");
  }
  absl::StrAppend(&too_many, "]}");
  const std::vector<std::pair<std::string, std::string>> cases = {
      {R"json("request_mix": [{"path": "/"}], "headers": [{"name": "a", "value": "b"}]})json",
       "request_mix and headers are mutually exclusive; set it on each request_mix entry instead"},
      {R"json("request_mix": [{"path": "/"}], "method": "POST"})json",
       "request_mix and method are mutually exclusive; set it on each request_mix entry instead"},
      {R"json("request_mix": []})json", "request_mix must be an array of 1 to 256 entries"},
      {too_many, "request_mix must be an array of 1 to 256 entries"},
      {R"json("request_mix": [{"path": "/"}, {"url": "/x"}]})json",
       "request_mix[1]: unknown key 'url' (an entry carries method, path, headers or "
       "body_size)"},
      {R"json("request_mix": [{"body_size": 10}]})json",
       "request_mix[0]: body_size 10 needs a method that carries a body (for example POST); a "
       "body on GET is refused"},
      {R"json("request_mix": [{"path": "x y"}]})json",
       "request_mix[0]: path must be a string starting with '/', without spaces or CR/LF/NUL "
       "(got 'x y')"},
      {R"json("request_mix": [{"method": "FETCH"}]})json",
       "request_mix[0]: method must be one of GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, "
       "TRACE, PATCH (got 'FETCH')"},
      {R"json("request_mix": [{"headers": [{"name": "a:b", "value": "c"}]}]})json",
       "request_mix[0]: header name must be a non-empty token without ':', spaces, or CR/LF/NUL "
       "(header injection refused): 'a:b'"},
  };
  for (const auto& [arguments, message] : cases) {
    EXPECT_EQ(StartRunRefusal(*harness, base + arguments), message) << arguments.substr(0, 80);
  }
  // h3 needs a top-level Host header, which a mix excludes: either way the
  // run is refused before any engine connection.
  EXPECT_EQ(StartRunRefusal(*harness, R"json({"uri": "https://127.0.0.1:443/", "rps": 10,
                                             "duration_s": 5, "protocol": "h3",
                                             "headers": [{"name": "Host", "value": "x"}],
                                             "request_mix": [{"path": "/"}]})json"),
            "request_mix and headers are mutually exclusive; set it on each request_mix entry "
            "instead");
  EXPECT_EQ(StartRunRefusal(*harness, R"json({"uri": "https://127.0.0.1:443/", "rps": 10,
                                             "duration_s": 5, "protocol": "h3",
                                             "request_mix": [{"path": "/"}]})json"),
            "protocol h3 needs a headers entry {\"name\": \"Host\", \"value\": <certificate "
            "hostname>}: the target is rewritten to its IP and QUIC cannot skip certificate "
            "verification");
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// N3b: tls maps onto the UpstreamTlsContext the engine wraps for the
// protocol (versions, cipher suites, curves; session_resumption false sets
// max_session_keys to 0).
TEST(McpToolsTest, TlsMapsOntoTlsContext) {
  auto harness = MakeLoopbackHarness();
  const nighthawk::client::StartRequest received =
      StartAndCapture(*harness, "https",
                      R"json(, "tls": {"min_version": "1.2", "max_version": "1.3",
                       "cipher_suites": ["ECDHE-RSA-AES128-GCM-SHA256"],
                       "ecdh_curves": ["X25519"], "session_resumption": false})json");
  ASSERT_TRUE(received.options().has_tls_context());
  const auto& context = received.options().tls_context();
  const auto& params = context.common_tls_context().tls_params();
  using TlsParameters = envoy::extensions::transport_sockets::tls::v3::TlsParameters;
  EXPECT_EQ(params.tls_minimum_protocol_version(), TlsParameters::TLSv1_2);
  EXPECT_EQ(params.tls_maximum_protocol_version(), TlsParameters::TLSv1_3);
  EXPECT_THAT(params.cipher_suites(), testing::ElementsAre("ECDHE-RSA-AES128-GCM-SHA256"));
  EXPECT_THAT(params.ecdh_curves(), testing::ElementsAre("X25519"));
  ASSERT_TRUE(context.has_max_session_keys());
  EXPECT_EQ(context.max_session_keys().value(), 0);

  auto resumed = MakeLoopbackHarness();
  const nighthawk::client::StartRequest with_resumption =
      StartAndCapture(*resumed, "https", R"json(, "tls": {"session_resumption": true})json");
  EXPECT_FALSE(with_resumption.options().tls_context().has_max_session_keys());
}

TEST(McpToolsTest, TlsRefusals) {
  auto harness = MakeHarness();
  const std::string https =
      R"json({"uri": "https://127.0.0.1:443/", "rps": 10, "duration_s": 5, )json";
  EXPECT_EQ(StartRunRefusal(*harness, R"json({"uri": "http://127.0.0.1:80/", "rps": 10,
                                             "duration_s": 5, "tls": {}})json"),
            "tls applies to an https uri only");
  const std::vector<std::pair<std::string, std::string>> cases = {
      {R"json("tls": {"min_version": "1.4"}})json",
       "tls.min_version must be one of 1.0, 1.1, 1.2, 1.3 (got '1.4')"},
      {R"json("tls": {"min_version": "1.3", "max_version": "1.2"}})json",
       "tls.min_version must not be above tls.max_version"},
      {R"json("tls": {"cipher_suites": []}})json",
       "tls.cipher_suites must be an array of 1 to 64 strings"},
      {R"json("tls": {"ecdh_curves": [""]}})json",
       "tls.ecdh_curves entries must be non-empty strings without CR/LF/NUL"},
      {R"json("tls": {"session_resumption": "no"}})json",
       "tls.session_resumption must be a boolean"},
      {R"json("tls": {"sni": "x"}})json",
       "tls carries an unknown key 'sni' (known: min_version, max_version, cipher_suites, "
       "ecdh_curves, session_resumption)"},
  };
  for (const auto& [arguments, message] : cases) {
    EXPECT_EQ(StartRunRefusal(*harness, https + arguments), message) << arguments;
  }
  EXPECT_TRUE(harness->service->startRequests().empty());
}

// NH1: the phases string carries per-worker weights as a sixth part, with
// connections optionally empty before it; a split that is not whole, or a
// weight count that does not match the phase's workers, is refused with the
// shared validation's text.
TEST(McpToolsTest, PhasesStringCarriesWorkerWeights) {
  auto harness = MakeLoopbackHarness();
  const nighthawk::client::StartRequest received = StartAndCapture(
      *harness, "http", R"json(, "phases": "even:80:5:2,hot:80:5:2::3/1,cx:60:5:2:8:1/2")json");
  ASSERT_EQ(received.options().phases_size(), 3);
  EXPECT_TRUE(received.options().phases(0).worker_weights().empty());
  EXPECT_THAT(received.options().phases(1).worker_weights(), testing::ElementsAre(3, 1));
  EXPECT_FALSE(received.options().phases(1).has_connections());
  EXPECT_THAT(received.options().phases(2).worker_weights(), testing::ElementsAre(1, 2));
  EXPECT_EQ(received.options().phases(2).connections().value(), 8);

  auto refusing = MakeHarness();
  const std::string base = R"json({"uri": "http://127.0.0.1:80/", "rps": 10, "phases": ")json";
  EXPECT_THAT(StartRunRefusal(*refusing, base + R"json(hot:81:5:2::3/1"})json"),
              testing::HasSubstr("phase[0]: requests_per_second 81 does not split into whole "
                                 "per-worker rates"));
  EXPECT_THAT(StartRunRefusal(*refusing, base + R"json(hot:80:5:2::3/1/1"})json"),
              testing::HasSubstr("phase[0]: worker_weights has 3 entries"));
  EXPECT_THAT(StartRunRefusal(*refusing, base + R"json(hot:80:5:2::3/x"})json"),
              testing::HasSubstr("weights must be '/'-separated positive integers"));
  EXPECT_TRUE(refusing->service->startRequests().empty());
}

// Steering without a run surfaces the feed's refusal verbatim.
TEST(McpToolsTest, SteerWithoutRunSurfacesFeedRefusal) {
  auto harness = MakeHarness();
  Struct steer = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  EXPECT_TRUE(IsIsError(steer));
  EXPECT_EQ(ErrorText(steer), "no run is in flight on this RunFeed");
  Struct cancel = CallToolOrDie(*harness, "cancel_run", "{}");
  EXPECT_TRUE(IsIsError(cancel));
  EXPECT_EQ(ErrorText(cancel), "no run is in flight on this RunFeed");
}

// The happy steering path: the ack is the result, epoch id included.
TEST(McpToolsTest, SteerReturnsAckWithEpoch) {
  auto harness = MakeHarness();
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  Struct result = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
  const Struct& structured = StructOf(Field(result, "structuredContent"));
  EXPECT_TRUE(Field(structured, "applied").bool_value());
  EXPECT_EQ(Text(Field(structured, "epoch_id")), "epoch-1");
  EXPECT_GT(Field(structured, "monotonic_ns").number_value(), 0);
}

// Review Focus 2: a steer landing while another is pending reaches the
// engine and is rejected by its one-in-flight guard — surfaced verbatim,
// never queued; the first steer still gets its ack (FIFO).
TEST(McpToolsTest, SecondSteerWhilePendingIsEngineRejection) {
  auto harness = MakeHarness(absl::Seconds(2) /* the first update's applier works for 2s */);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  // Steer #1 in a thread (deferred: the bounded wait runs off the dispatch
  // loop); it stays pending while the applier works.
  std::thread first([&harness]() {
    Struct result = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
    ASSERT_FALSE(IsIsError(result)) << ErrorText(result);
    EXPECT_EQ(Text(Field(StructOf(Field(result, "structuredContent")), "epoch_id")), "epoch-1");
  });
  WaitForStatus(*harness, absl::Seconds(5),
                [](const Struct& status) {
                  return Field(StructOf(Field(status, "pending")), "update").bool_value();
                });

  // Steer #2 while #1 is in flight: the engine's structured rejection,
  // verbatim.
  Struct second = CallToolOrDie(*harness, "steer", R"json({"rps": 30})json");
  EXPECT_TRUE(IsIsError(second));
  EXPECT_EQ(ErrorText(second),
            "another live update is still in flight; bends are not queued");
  first.join();
}

// Review Focus 4 (steer): against a wedged applier (the ack resolves past
// the bound, never within it) the tool returns within its stated bound
// (2x flush + 2s = 4s) with the timed-out error text.
TEST(McpToolsTest, SteerTimesOutAt4sAgainstHoggingApplier) {
  // 6s ack delay: past the 4s bound, mirroring the hogging-applier pin.
  auto harness = MakeHarness(absl::Seconds(6));
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  const absl::Time start = absl::Now();
  Struct result = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  const absl::Duration elapsed = absl::Now() - start;
  EXPECT_TRUE(IsIsError(result));
  EXPECT_TRUE(absl::StartsWith(ErrorText(result), "timed out after 4s: "))
      << ErrorText(result);
  EXPECT_GE(elapsed, absl::Seconds(4) - absl::Milliseconds(200)) << elapsed;
  EXPECT_LE(elapsed, absl::Seconds(6)) << elapsed;
}

// The deferred dispatch: while steer's bounded wait runs, the dispatch loop
// still answers ping immediately.
TEST(McpToolsTest, PingAnswersDuringBoundedSteerWait) {
  auto harness = MakeHarness(absl::Seconds(6));
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  Struct steer_request;
  (*steer_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*steer_request.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*steer_request.mutable_fields())["method"].set_string_value("tools/call");
  {
    Struct params;
    (*params.mutable_fields())["name"].set_string_value("steer");
    (*params.mutable_fields())["arguments"] =
        ToValue(*ParseJsonObject(R"json({"rps": 20})json"));
    (*steer_request.mutable_fields())["params"] = ToValue(params);
  }
  (void)harness->server->HandleLine(EmitCanonicalStruct(steer_request));
  // The steer call is deferred (no immediate response line): ping NOW.
  Struct ping;
  (*ping.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*ping.mutable_fields())["id"].set_number_value(++harness->next_id);
  (*ping.mutable_fields())["method"].set_string_value("ping");
  const absl::Time ping_start = absl::Now();
  auto ping_line = harness->server->HandleLine(EmitCanonicalStruct(ping));
  const absl::Duration ping_elapsed = absl::Now() - ping_start;
  ASSERT_TRUE(ping_line.has_value());
  EXPECT_LT(ping_elapsed, absl::Seconds(1)) << "ping must answer during the steer wait";

  // The deferred envelope lands through the sink when the handler's bounded
  // wait expires — wait for it (and pin the timed-out text) before teardown.
  auto steer_response = CallToolWaitById(*harness, harness->next_id - 1, absl::Seconds(15));
  ASSERT_TRUE(steer_response.ok()) << steer_response.status();
  EXPECT_TRUE(IsIsError(*steer_response));
  EXPECT_TRUE(absl::StartsWith(ErrorText(*steer_response), "timed out after 4s: "))
      << ErrorText(*steer_response);
}

// Review Focus 4 (cancel): a wedged service that never delivers the final
// output after the cancellation arm — the tool returns within its stated
// bound (--timeout 30s + 5s grace = 35s) with the timed-out error text.
TEST(McpToolsTest, CancelBoundedAt35s) {
  auto harness = MakeHarness(absl::ZeroDuration(), /*stall_after_cancel=*/true);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  const absl::Time start = absl::Now();
  Struct result = CallToolOrDie(*harness, "cancel_run", "{}", absl::Seconds(60));
  const absl::Duration elapsed = absl::Now() - start;
  EXPECT_TRUE(IsIsError(result));
  EXPECT_TRUE(absl::StartsWith(ErrorText(result),
                               "timed out after 35s: the run's final output after the "
                               "cancellation"))
      << ErrorText(result);
  EXPECT_GE(elapsed, absl::Seconds(35) - absl::Milliseconds(200)) << elapsed;
  EXPECT_LE(elapsed, absl::Seconds(40)) << elapsed;
}

// Drive backlog item 7's fallback arm: a run that outlives its DECLARED
// schedule by the TTL grace with no terminal in sight is cancelled by the
// run-TTL watchdog — the ghost-engine contamination class (a dead MCP client
// whose disconnect signal never arrives leaves the engine running and the
// next drive inherits a polluted SUT) is bounded by declared end + grace.
TEST(McpToolsTest, RunTtlCancelsRunPastDeclaredEnd) {
  SteeringToolOptions options;
  options.run_ttl_grace_s = 1;
  auto harness = MakeHarness(absl::ZeroDuration(), false, options);
  // Declared 1s + 1s grace: the watchdog must end this ~2s after start —
  // the fake service never ends it on its own.
  Struct started = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 1})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);
  WaitForStatus(*harness, absl::Seconds(10), [](const Struct& status) {
    return !Field(StructOf(Field(status, "run")), "in_flight").bool_value();
  });
  // The cancellation resolved through the explicit arm, so the session is
  // sealed (the end record landed) and the next start_run is NOT latched
  // out by the still-open recorder.
  Struct second = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
}

// The watchdog spares a healthy run: a declared length the run has not yet
// reached stays in flight — the TTL is a contamination bound, never a run
// limiter.
TEST(McpToolsTest, RunTtlSparesHealthyLongRun) {
  SteeringToolOptions options;
  options.run_ttl_grace_s = 1;
  auto harness = MakeHarness(absl::ZeroDuration(), false, options);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 3600})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });
  absl::SleepFor(absl::Seconds(2)); // well past a 1s grace, far inside 3600s
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_TRUE(Field(StructOf(Field(status, "run")), "in_flight").bool_value());
}

// The status strip carries the phase id and, after an applied steer, the
// knob epoch — the CANONICAL shape (the nighthawk://status resource renders
// the same object).
TEST(McpToolsTest, GetStatusCarriesPhaseAndEpoch) {
  auto harness = MakeHarness();
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  const Struct& run = StructOf(Field(status, "run"));
  EXPECT_TRUE(Field(run, "in_flight").bool_value());
  EXPECT_FALSE(Text(Field(run, "session")).empty());
  EXPECT_FALSE(Text(Field(run, "resolved_target")).empty());
  const Struct& current = StructOf(Field(status, "current"));
  EXPECT_EQ(Text(Field(current, "phase_id")), "main");
  EXPECT_EQ(Text(Field(current, "epoch_id")), "");
  EXPECT_TRUE(Field(StructOf(Field(status, "pending")), "update").bool_value() == false);
  // The counters and the phases array are always present (the array empty on
  // a single-phase run: the only boundary is the run's own end).
  EXPECT_TRUE(status.fields().contains("counters"));
  EXPECT_TRUE(status.fields().contains("phases"));
  EXPECT_EQ(Field(status, "phases").list_value().values_size(), 0);

  ASSERT_FALSE(IsIsError(CallToolOrDie(*harness, "steer", R"json({"rps": 20})json")));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& s) {
    return Text(Field(StructOf(Field(s, "current")), "epoch_id")) == "epoch-1";
  });
  status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_EQ(Text(Field(StructOf(Field(status, "current")), "epoch_id")), "epoch-1");
}

// --- M4 close-fix pins (panel round) ----------------------------------------

// NaturalRunEndSealsSessionAndAllowsNextRun (panel major #1): a NATURAL run
// end (the engine's own terminal output; no cancel_run, no graceful stop)
// seals the session — the file carries its run_end(completed=true) record
// with a flushed tail, and a second start_run succeeds instead of being
// latched out by the still-open recorder ("a session is already open").
TEST(McpToolsTest, NaturalRunEndSealsSessionAndAllowsNextRun) {
  // The fake ends the run on its own after 8 snapshots (~0.8s of ticks).
  auto harness = MakeHarness(absl::ZeroDuration(), /*stall_after_cancel=*/false,
                             /*options=*/{}, /*resolver=*/nullptr,
                             /*end_after_snapshots=*/8);
  Struct started = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);
  const std::string session_path = Text(Field(Structured(started), "session"));

  // The run ends on its own (no cancel_run was ever sent).
  WaitForStatus(*harness, absl::Seconds(10), [](const Struct& status) {
    return !Field(StructOf(Field(status, "run")), "in_flight").bool_value();
  });
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_FALSE(Field(StructOf(Field(status, "run")), "in_flight").bool_value());
  EXPECT_TRUE(Field(StructOf(Field(status, "run")), "completed").bool_value());

  // The session file is SEALED: the tail is not torn (the writer flushed)
  // and run_end(completed=true) is the last record.
  auto session = ReadPayload(*harness, "nighthawk://session");
  ASSERT_TRUE(session.ok()) << session.status();
  EXPECT_FALSE(Field(*session, "tail_skipped").bool_value())
      << "the natural end must flush and seal the file";
  const Value& records = Field(*session, "records");
  ASSERT_GE(records.list_value().values_size(), 1);
  const Value& last = records.list_value().values(records.list_value().values_size() - 1);
  EXPECT_EQ(Text(Field(StructOf(last), "type")), "run_end");
  EXPECT_TRUE(Field(StructOf(last), "completed").bool_value());

  // The latch is gone: a second start_run succeeds with a fresh session.
  Struct second = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  EXPECT_TRUE(Field(Structured(second), "run_started").bool_value());
  EXPECT_NE(Text(Field(Structured(second), "session")), session_path);
}

// TimedOutSteerPendingIsUnresolvedThenResolved (panel major #3a): after a
// steer times out mid-run the pending state stays (the arm's fate is
// genuinely unknown mid-run — the proto's late-rejection drop window may
// still resolve it) but is TRUTHFUL: pending.state reads "unresolved"
// instead of an indistinguishable "awaiting_ack", until the late verdict
// resolves it back to "none".
TEST(McpToolsTest, TimedOutSteerPendingIsUnresolvedThenResolved) {
  auto harness = MakeHarness(absl::Seconds(6) /* the applier resolves past the bound */);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  Struct steer = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  ASSERT_TRUE(IsIsError(steer));
  EXPECT_TRUE(absl::StartsWith(ErrorText(steer), "timed out after 4s: ")) << ErrorText(steer);

  // Mid-run, post-timeout: pending truthfully reads unresolved.
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  const Struct& pending = StructOf(Field(status, "pending"));
  EXPECT_TRUE(Field(pending, "update").bool_value());
  EXPECT_EQ(Text(Field(pending, "state")), "unresolved");

  // The late ack lands at ~6s: the verdict resolves the arm, and the status
  // says so.
  WaitForStatus(*harness, absl::Seconds(10), [](const Struct& s) {
    return Text(Field(StructOf(Field(s, "pending")), "state")) == "none";
  });
}

// RunTerminalResolvesPendingAndNextRunStartsClean (panel major #3b): the
// run's terminal response resolves every outstanding arm (the proto's
// consumer rule) — after a steer whose verdict never arrives, the ended run
// reports no pending work, and the next run starts clean (no stale pending
// from the previous run).
TEST(McpToolsTest, RunTerminalResolvesPendingAndNextRunStartsClean) {
  // The applier resolves at 10s (never within the run); the run ends on its
  // own at ~6s (60 ticks) — the arm's verdict is still outstanding at the
  // terminal.
  auto harness = MakeHarness(absl::Seconds(10), /*stall_after_cancel=*/false,
                             /*options=*/{}, /*resolver=*/nullptr,
                             /*end_after_snapshots=*/60);
  CallToolOrDie(*harness, "start_run",
                absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                              R"json(/", "rps": 10, "duration_s": 60})json"));
  WaitForStatus(*harness, absl::Seconds(5), [](const Struct& status) {
    return !Text(Field(StructOf(Field(status, "current")), "phase_id")).empty();
  });

  Struct steer = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  ASSERT_TRUE(IsIsError(steer));
  EXPECT_TRUE(absl::StartsWith(ErrorText(steer), "timed out after 4s: ")) << ErrorText(steer);
  Struct status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_EQ(Text(Field(StructOf(Field(status, "pending")), "state")), "unresolved");

  // The run ends; the terminal response resolves the outstanding arm.
  WaitForStatus(*harness, absl::Seconds(15), [](const Struct& s) {
    return !Field(StructOf(Field(s, "run")), "in_flight").bool_value();
  });
  status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_FALSE(Field(StructOf(Field(status, "pending")), "update").bool_value())
      << "the terminal response must resolve the outstanding arm";
  EXPECT_EQ(Text(Field(StructOf(Field(status, "pending")), "state")), "none");

  // The next run starts clean: no phantom pending from the previous run.
  Struct second = CallToolOrDie(
      *harness, "start_run",
      absl::StrCat(R"json({"uri": "http://127.0.0.1:)json", harness->grpc_port,
                    R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  status = Structured(CallToolOrDie(*harness, "get_status", "{}"));
  EXPECT_FALSE(Field(StructOf(Field(status, "pending")), "update").bool_value());
  EXPECT_EQ(Text(Field(StructOf(Field(status, "pending")), "state")), "none");
}

} // namespace
} // namespace Client
} // namespace Nighthawk
