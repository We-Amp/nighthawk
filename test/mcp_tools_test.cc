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

#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_main.h"
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
  FakeNighthawkService(absl::Duration ack_delay, bool stall_after_cancel)
      : ack_delay_(ack_delay), stall_after_cancel_(stall_after_cancel) {}

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
          // The engine's one-in-flight guard, verbatim (Task 8a).
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
        WriteLocked(OutputResponse());
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

  void AckAfterDelay(int candidate) {
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

  std::mutex mutex_;
  grpc::ServerContext* context_ = nullptr;
  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                           nighthawk::client::ExecutionRequest>* stream_ = nullptr;
  const absl::Duration ack_delay_;
  const bool stall_after_cancel_;
  std::vector<nighthawk::client::StartRequest> start_requests_;
  std::vector<std::thread> ack_threads_;
  std::thread ticker_;
  bool ticking_ = false;
  bool done_ = false;
  bool update_in_flight_ = false;
  int run_epoch_ = 0;
  int64_t monotonic_ns_ = 0;
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
                                     HostResolver resolver = nullptr) {
  auto harness = std::make_unique<Harness>();
  harness->options = options;
  harness->service = std::make_unique<FakeNighthawkService>(ack_delay, stall_after_cancel);
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

} // namespace
} // namespace Client
} // namespace Nighthawk
