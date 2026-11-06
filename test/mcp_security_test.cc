// FORK-ONLY (We-Amp): the MCP security floor's contract pins (MCP plan Task
// 5), plus the carried review items pinned here as a whole:
//   - ONE canonical target-uri parse path (ParseTargetUri), with the
//     userinfo/host confusion cases pinned on it, and ONE shared
//     CheckTarget behind every target-taking entry (proven by verdict
//     parity between the start_run tool and CheckTarget itself);
//   - EVERY resolved address is checked (a name resolving to loopback +
//     public is refused, naming the public address);
//   - the load ceiling is the SHARED start/steer check (a steer's declared
//     total is refused naming the cap);
//   - the boundary validator covers resources/read too (closed params,
//     field-naming invalid-params, exact-match uri);
//   - the startup flag validation refuses malformed CIDRs / ceiling /
//     timeout / service endpoint / profile window (exit non-zero, stderr);
//   - a timed-out steer's late ack cannot wedge the result queue front once
//     no overlap waiter remains (the stale-result-front fix);
//   - pull_profile_window's host-blocking duration_s is capped (and the
//     tool description names the cap).
//
// Plain cc_test for the same quarantine reason as the other MCP suites:
// mcp_lib carries no Envoy C++ sources and no test-helper stack.
#include <grpc++/grpc++.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
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

// A minimal in-proc NighthawkService double: accepts starts (no snapshots —
// the pins here never read telemetry), accepts updates with PER-UPDATE ack
// delays (popped in acceptance order), and enforces the engine's
// one-in-flight guard with the exact rejection string. The accepted-update
// counter lets the pins prove what actually traveled the wire.
class MinimalFakeService final : public nighthawk::client::NighthawkService::Service {
public:
  explicit MinimalFakeService(std::deque<absl::Duration> ack_delays)
      : ack_delays_(std::move(ack_delays)) {}

  ~MinimalFakeService() override { JoinAcks(); }

  grpc::Status ExecutionStream(
      grpc::ServerContext* /*context*/,
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                               nighthawk::client::ExecutionRequest>* stream) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stream_ = stream;
    }
    nighthawk::client::ExecutionRequest request;
    while (stream->Read(&request)) {
      if (request.has_start_request()) {
        continue;
      }
      if (request.has_update_request()) {
        bool reject = false;
        int candidate = 0;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (update_in_flight_) {
            reject = true;
          } else {
            update_in_flight_ = true;
            candidate = ++accepted_;
          }
        }
        if (reject) {
          // The engine's one-in-flight guard, verbatim.
          nighthawk::client::ExecutionResponse response;
          auto* result = response.mutable_update_result();
          result->set_applied(false);
          result->set_rejection_reason(
              "another live update is still in flight; bends are not queued");
          std::lock_guard<std::mutex> lock(mutex_);
          if (stream_ != nullptr) {
            stream_->Write(response);
          }
        } else {
          std::thread ack([this, candidate]() { AckAfterDelay(candidate); });
          std::lock_guard<std::mutex> lock(mutex_);
          ack_threads_.push_back(std::move(ack));
        }
      }
    }
    JoinAcks();
    return grpc::Status::OK;
  }

  int accepted_updates() {
    std::lock_guard<std::mutex> lock(mutex_);
    return accepted_;
  }

private:
  void AckAfterDelay(int candidate) {
    absl::Duration delay;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      delay = ack_delays_.empty() ? absl::ZeroDuration() : ack_delays_.front();
      if (!ack_delays_.empty()) {
        ack_delays_.pop_front();
      }
    }
    absl::SleepFor(delay);
    nighthawk::client::ExecutionResponse response;
    auto* result = response.mutable_update_result();
    result->set_applied(true);
    result->set_epoch_id(absl::StrCat("epoch-", candidate));
    result->set_monotonic_ns(++monotonic_ns_);
    result->set_wall_time_unix_ns(absl::ToUnixNanos(absl::Now()));
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stream_ != nullptr) {
        stream_->Write(response);
      }
      update_in_flight_ = false;
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

  std::mutex mutex_;
  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                           nighthawk::client::ExecutionRequest>* stream_ = nullptr;
  std::deque<absl::Duration> ack_delays_;
  std::vector<std::thread> ack_threads_;
  bool update_in_flight_ = false;
  int accepted_ = 0;
  int64_t monotonic_ns_ = 0;
};

// One wired steering harness: fake service + gRPC server + channel +
// controller + profile controller + registries + McpServer over the waiter
// pool (the mcp_tools_test shape, trimmed to what these pins need).
struct Harness {
  std::unique_ptr<MinimalFakeService> service;
  std::unique_ptr<grpc::Server> grpc_server;
  int grpc_port = 0;
  std::shared_ptr<grpc::ChannelInterface> channel;
  SteeringToolOptions options;
  // Declared BEFORE profile_controller (the profile controller borrows it
  // and must outlive it — the RunMcpServer declaration order).
  std::unique_ptr<SteeringController> controller;
  std::unique_ptr<ProfileController> profile_controller;
  ToolRegistry tools;
  ResourceRegistry resources;
  std::unique_ptr<McpServer> server;
  // Declared AFTER the server so it is destroyed FIRST (see
  // mcp_tools_test's Harness: a deferred handler completing into a
  // torn-down server would be use-after-free).
  std::unique_ptr<WaiterPool> pool;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<int, Struct> responses;
  int next_id = 0;
};

std::unique_ptr<Harness> MakeHarness(SteeringToolOptions options = {},
                                     HostResolver resolver = nullptr,
                                     std::deque<absl::Duration> ack_delays = {}) {
  auto harness = std::make_unique<Harness>();
  harness->options = options;
  harness->service = std::make_unique<MinimalFakeService>(std::move(ack_delays));
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &harness->grpc_port);
  builder.RegisterService(harness->service.get());
  harness->grpc_server = builder.BuildAndStart();
  harness->channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", harness->grpc_port),
                                         grpc::InsecureChannelCredentials());
  harness->controller =
      std::make_unique<SteeringController>(harness->channel, harness->options, resolver);
  harness->profile_controller =
      std::make_unique<ProfileController>(*harness->controller, ProfileToolOptions{});
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

// One tools/call; returns the envelope once the id's response lands (inline
// tools answer during HandleLine, deferred tools through the sink).
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

// One resources/read against a bare server; returns the parsed response
// object (either the result or the error part). The response line is read
// from the sink capture (the wire truth).
Struct ResourcesRead(McpServer& server, std::string* last_line, int id,
                     const std::string& params_json) {
  Struct request;
  (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*request.mutable_fields())["id"].set_number_value(id);
  (*request.mutable_fields())["method"].set_string_value("resources/read");
  (*request.mutable_fields())["params"] = ToValue(*ParseJsonObject(params_json));
  (void)server.HandleLine(EmitCanonicalStruct(request));
  return *ParseJsonObject(*last_line);
}

bool HasJsonRpcError(const Struct& response, int expected_code,
                     const std::string& expected_substring) {
  const Value& error = Field(response, "error");
  if (error.kind_case() != Value::kStructValue) {
    return false;
  }
  if (static_cast<int>(Field(StructOf(error), "code").number_value()) != expected_code) {
    return false;
  }
  return Text(Field(StructOf(error), "message")).find(expected_substring) != std::string::npos;
}

// One initialize round on a bare server (the -32002 gate is not under pin
// here; every test speaks its protocol method only after this).
void Initialize(McpServer& server) {
  Struct init_request;
  (*init_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*init_request.mutable_fields())["id"].set_number_value(0);
  (*init_request.mutable_fields())["method"].set_string_value("initialize");
  (*init_request.mutable_fields())["params"] = ToValue(*ParseJsonObject(
      R"json({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test"}})json"));
  (void)server.HandleLine(EmitCanonicalStruct(init_request));
}

// --- Task 5 pin 1: ONE canonical parse path ---------------------------------

// CanonicalParserSingleSource: the userinfo/host confusion cases pin on the
// one parser, and the tool's refusal verdict is byte-identical to the shared
// CheckTarget's (no second parse path exists to diverge).
TEST(McpSecurityTest, CanonicalParserSingleSource) {
  // The host is what follows the LAST '@' — both trick directions.
  auto forward = ParseTargetUri("http://evil.test@127.0.0.1:80/");
  ASSERT_TRUE(forward.ok()) << forward.status();
  EXPECT_EQ(forward->host, "127.0.0.1");
  EXPECT_EQ(forward->userinfo, "evil.test");
  EXPECT_EQ(forward->port, "80");

  auto reversed = ParseTargetUri("http://127.0.0.1@evil.test/");
  ASSERT_TRUE(reversed.ok()) << reversed.status();
  EXPECT_EQ(reversed->host, "evil.test");

  auto port_trick = ParseTargetUri("http://127.0.0.1:80@evil.test/");
  ASSERT_TRUE(port_trick.ok()) << port_trick.status();
  EXPECT_EQ(port_trick->host, "evil.test");
  // The ':80' belongs to the userinfo, not to the host: nothing follows the
  // '@' but the bare host.
  EXPECT_EQ(port_trick->port, "");

  // Bracketed IPv6 keeps its port; a mapped form survives the parse (the
  // normalization happens in the check, not the parse); the host lowercases.
  auto v6 = ParseTargetUri("http://[::1]:8443/x?y=1");
  ASSERT_TRUE(v6.ok()) << v6.status();
  EXPECT_EQ(v6->host, "::1");
  EXPECT_EQ(v6->port, "8443");
  EXPECT_EQ(v6->path_query, "/x?y=1");

  auto mapped = ParseTargetUri("http://[::FFFF:127.0.0.1]/");
  ASSERT_TRUE(mapped.ok()) << mapped.status();
  EXPECT_EQ(mapped->host, "::ffff:127.0.0.1");

  auto upper = ParseTargetUri("http://LOCALHOST/");
  ASSERT_TRUE(upper.ok()) << upper.status();
  EXPECT_EQ(upper->host, "localhost");

  // Zone id stripped (link-local scoping the server has no business
  // following).
  auto zoned = ParseTargetUri("http://[fe80::1%en0]/");
  ASSERT_TRUE(zoned.ok()) << zoned.status();
  EXPECT_EQ(zoned->host, "fe80::1");

  // Single-source proof: the start_run tool's refusal and the shared
  // CheckTarget's refusal are the SAME bytes for the SAME uri (the tool has
  // no parse or check of its own).
  const std::string uri = "http://127.0.0.1@93.184.216.34/";
  const auto public_resolver =
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"93.184.216.34"};
      };
  auto harness = MakeHarness({}, public_resolver);
  Struct tool_result =
      CallToolOrDie(*harness, "start_run", absl::StrCat(R"json({"uri": ")json", uri, R"json("})json"));
  ASSERT_TRUE(IsIsError(tool_result));
  auto direct = CheckTarget(uri, harness->options.allow_target_cidrs, public_resolver);
  ASSERT_FALSE(direct.ok()) << "the shared check must refuse the public host too";
  EXPECT_EQ(ErrorText(tool_result), direct.status().message());
  EXPECT_NE(ErrorText(tool_result).find("refusing before any engine connection"),
            std::string::npos);
}

// --- Task 5 pin 2: EVERY resolved address is checked ------------------------

// AllResolvedAddressesChecked: a name resolving to loopback AND public is
// refused — the loopback result must not let the public one through — and
// the refusal names the offending (public) address. A name resolving to two
// loopback addresses passes and is rewritten to the FIRST.
TEST(McpSecurityTest, AllResolvedAddressesChecked) {
  const auto mixed_resolver =
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"127.0.0.1", "93.184.216.34"};
      };
  auto refused = CheckTarget("http://dual.test:80/", {}, mixed_resolver);
  ASSERT_FALSE(refused.ok()) << "the public address in the mix must refuse";
  EXPECT_NE(refused.status().message().find("93.184.216.34"), std::string::npos)
      << refused.status().message();
  EXPECT_NE(refused.status().message().find("resolves to"), std::string::npos);

  const auto dual_loopback =
      [](absl::string_view) -> absl::StatusOr<std::vector<std::string>> {
        return std::vector<std::string>{"127.0.0.1", "127.0.0.2"};
      };
  auto passed = CheckTarget("http://dual.test:8080/", {}, dual_loopback);
  ASSERT_TRUE(passed.ok()) << passed.status();
  EXPECT_EQ(*passed, "http://127.0.0.1:8080/");
}

// --- Task 5 pin 3: the ceiling is the shared start/steer check --------------

// CeilingAppliesToSteerTotal: a steer whose declared total exceeds
// --max-rps is refused naming the cap, BEFORE anything reaches the engine.
TEST(McpSecurityTest, CeilingAppliesToSteerTotal) {
  SteeringToolOptions options;
  options.max_rps = 100;
  auto harness = MakeHarness(options);
  Struct started = CallToolOrDie(*harness, "start_run",
                                 absl::StrCat(R"json({"uri": "http://127.0.0.1:)json",
                                               harness->grpc_port,
                                               R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);

  Struct steer = CallToolOrDie(*harness, "steer", R"json({"rps": 101})json");
  EXPECT_TRUE(IsIsError(steer));
  EXPECT_NE(ErrorText(steer).find("exceeds the load ceiling of 100 rps (--max-rps)"),
            std::string::npos)
      << ErrorText(steer);
  // Nothing reached the engine: the refusal happened before SendUpdate.
  EXPECT_EQ(harness->service->accepted_updates(), 0);
}

// --- Task 5 pin 4: the boundary validator covers resources too --------------

// BoundaryValidatorCoversResourcesToo: resources/read params go through the
// SAME closed-schema validator as tool arguments (unknown fields and wrong
// types are field-naming -32602), and the resource boundary itself stays
// exact-match (no traversal, no agent-shaped ids). The status reader is
// safe with no run in flight, so the happy path pins too.
TEST(McpSecurityTest, BoundaryValidatorCoversResourcesToo) {
  ToolRegistry tools;
  ResourceRegistry resources;
  auto channel = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
  auto controller = std::make_unique<SteeringController>(channel, SteeringToolOptions{});
  RegisterEchoTool(tools);
  RegisterSteeringTools(tools, *controller);
  RegisterTelemetryResources(resources, *controller);
  std::string last_line;
  McpServer server(
      tools, resources,
      [&last_line](absl::string_view line) { last_line = std::string(line); },
      [](absl::string_view) {}, nullptr);
  Initialize(server);

  // An unknown params field is a field-naming invalid-params — the same
  // validator tool arguments go through.
  Struct extra = ResourcesRead(server, &last_line, 1,
                               R"json({"uri": "nighthawk://status", "evil": 1})json");
  EXPECT_TRUE(HasJsonRpcError(extra, -32602, "unknown argument 'evil'")) << "extra";
  // A wrong type on the one known field is the same class of refusal.
  Struct wrong = ResourcesRead(server, &last_line, 2, R"json({"uri": 42})json");
  EXPECT_TRUE(HasJsonRpcError(wrong, -32602, "'uri' must be of type string")) << "wrong";
  // A missing uri is the schema's required check.
  Struct missing = ResourcesRead(server, &last_line, 3, R"json({})json");
  EXPECT_TRUE(HasJsonRpcError(missing, -32602, "missing required argument 'uri'")) << "missing";
  // The resource boundary is EXACT-match: traversal-shaped and agent-shaped
  // uris name no resource.
  Struct traversal =
      ResourcesRead(server, &last_line, 4, R"json({"uri": "nighthawk://status/../../etc"})json");
  EXPECT_TRUE(HasJsonRpcError(traversal, -32602, "unknown resource")) << "traversal";
  // The happy path still reads (the status reader is safe with no run).
  Struct good = ResourcesRead(server, &last_line, 5, R"json({"uri": "nighthawk://status"})json");
  EXPECT_TRUE(Field(good, "error").kind_case() == Value::KIND_NOT_SET) << "good";
  const Struct& good_result = StructOf(Field(good, "result"));
  EXPECT_TRUE(good_result.fields().contains("contents")) << "good";
}

// --- carried T2-M1: the stale-result-front wedge ----------------------------

// SteerDropsStaleResultFrontAfterOverlapWaitersGone: steer #1 times out at
// its 4s bound; its ack lands LATE, queuing a front no waiter sent before.
// The next steer must NOT wedge behind it — the unclaimable front is
// dropped (once no overlap waiter remains) and the steer consumes the
// oldest result with seq past its own send.
TEST(McpSecurityTest, SteerDropsStaleResultFrontAfterOverlapWaitersGone) {
  auto harness = MakeHarness({}, nullptr,
                             /*ack_delays=*/{absl::Seconds(5), absl::ZeroDuration()});
  Struct started = CallToolOrDie(*harness, "start_run",
                                 absl::StrCat(R"json({"uri": "http://127.0.0.1:)json",
                                               harness->grpc_port,
                                               R"json(/", "rps": 10, "duration_s": 60})json"));
  ASSERT_FALSE(IsIsError(started)) << ErrorText(started);

  // Steer #1: the ack resolves at 5s, past the 4s bound — the tool times
  // out and its waiter goes away BEFORE the ack arrives.
  Struct first = CallToolOrDie(*harness, "steer", R"json({"rps": 20})json");
  EXPECT_TRUE(IsIsError(first));
  EXPECT_TRUE(absl::StartsWith(ErrorText(first), "timed out after 4s: ")) << ErrorText(first);

  // The late ack lands 5s after its send; the tool returned at 4s. Wait
  // past the landing so the queue provably holds the stale front BEFORE the
  // next steer registers (else the race could hand it to steer #2 by the
  // send-sequence rule itself).
  absl::SleepFor(absl::Seconds(2));

  // Steer #2: the engine guard is free again (the ack landed); its own ack
  // is instant. Without the fix, steer #2 waits behind the stale front and
  // times out at 4s.
  Struct second = CallToolOrDie(*harness, "steer", R"json({"rps": 25})json");
  ASSERT_FALSE(IsIsError(second)) << ErrorText(second);
  EXPECT_EQ(Text(Field(Structured(second), "epoch_id")), "epoch-2");
}

// --- carried T4-Low: the host-blocking profile-window cap -------------------

// PullProfileWindowDurationCapped: duration_s above the 300 s cap is
// refused at the argument boundary (regardless of the profiling posture),
// naming the cap; the tool description names the cap so an agent knows the
// bound before calling.
TEST(McpSecurityTest, PullProfileWindowDurationCapped) {
  ToolRegistry tools;
  ResourceRegistry resources;
  auto channel = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
  auto controller = std::make_unique<SteeringController>(channel, SteeringToolOptions{});
  auto profile_controller = std::make_unique<ProfileController>(*controller, ProfileToolOptions{});
  RegisterProfileTools(tools, *profile_controller);
  std::string last_line;
  McpServer server(
      tools, resources,
      [&last_line](absl::string_view line) { last_line = std::string(line); },
      [](absl::string_view) {}, nullptr);
  Initialize(server);

  // Unconfigured posture, but the cap check still binds FIRST (the argument
  // boundary before the posture refusal — the pull would block the server
  // host for the whole window).
  int counter = 0;
  auto call = [&](const std::string& args_json) {
    const int id = ++counter;
    Struct request;
    (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
    (*request.mutable_fields())["id"].set_number_value(id);
    (*request.mutable_fields())["method"].set_string_value("tools/call");
    Struct params;
    (*params.mutable_fields())["name"].set_string_value("pull_profile_window");
    (*params.mutable_fields())["arguments"] = ToValue(*ParseJsonObject(args_json));
    (*request.mutable_fields())["params"] = ToValue(params);
    (void)server.HandleLine(EmitCanonicalStruct(request));
    return *ParseJsonObject(last_line);
  };

  Struct response = call(R"json({"duration_s": 301})json");
  ASSERT_TRUE(Field(response, "result").kind_case() == Value::kStructValue)
      << EmitCanonicalStruct(response);
  const Struct& envelope = StructOf(Field(response, "result"));
  EXPECT_TRUE(IsIsError(envelope)) << EmitCanonicalStruct(response);
  EXPECT_NE(ErrorText(envelope).find("exceeds the host-blocking cap of 300 s"),
            std::string::npos)
      << ErrorText(envelope);

  Struct zero_response = call(R"json({"duration_s": 0})json");
  ASSERT_TRUE(Field(zero_response, "result").kind_case() == Value::kStructValue)
      << EmitCanonicalStruct(zero_response);
  const Struct& zero_envelope = StructOf(Field(zero_response, "result"));
  EXPECT_TRUE(IsIsError(zero_envelope)) << EmitCanonicalStruct(zero_response);
  EXPECT_EQ(ErrorText(zero_envelope), "duration_s must be > 0");

  // The description names the cap (the agent's only manual).
  Struct listing_request;
  (*listing_request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*listing_request.mutable_fields())["id"].set_number_value(999);
  (*listing_request.mutable_fields())["method"].set_string_value("tools/list");
  (void)server.HandleLine(EmitCanonicalStruct(listing_request));
  Struct listing = *ParseJsonObject(last_line);
  bool found = false;
  for (const Value& entry :
       Field(StructOf(Field(listing, "result")), "tools").list_value().values()) {
    const Struct& tool = StructOf(entry);
    if (Text(Field(tool, "name")) != "pull_profile_window") {
      continue;
    }
    found = true;
    const std::string description = Text(Field(tool, "description"));
    EXPECT_NE(description.find("300 s"), std::string::npos)
        << "the description must name the host-blocking cap";
  }
  EXPECT_TRUE(found) << "pull_profile_window must be listed";
}

// --- Task 5 pin 5: the startup flag validation ------------------------------

// StartupFlagValidation: malformed flag values are a startup failure —
// exit non-zero, the reason on stderr — before any protocol line is spoken.
TEST(McpSecurityTest, StartupFlagValidation) {
  const auto run_main = [](const std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>("nighthawk_mcp"));
    for (const std::string& arg : args) {
      argv.push_back(const_cast<char*>(arg.c_str()));
    }
    std::stringstream in, out, err;
    const int code = RunMcpServerMain(static_cast<int>(argv.size()), argv.data(), in, out, err);
    return std::make_pair(code, err.str());
  };

  // A malformed CIDR.
  {
    auto [code, err] = run_main({"--allow-target-cidr", "10.0.0.0/33"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("10.0.0.0/33"), std::string::npos) << err;
  }
  // A malformed ceiling (non-positive).
  {
    auto [code, err] = run_main({"--max-rps", "0"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--max-rps"), std::string::npos) << err;
  }
  {
    auto [code, err] = run_main({"--max-rps", "-5"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--max-rps"), std::string::npos) << err;
  }
  // A malformed timeout.
  {
    auto [code, err] = run_main({"--timeout", "0"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--timeout"), std::string::npos) << err;
  }
  // A malformed service endpoint (no port).
  {
    auto [code, err] = run_main({"--service-address", "127.0.0.1"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--service-address"), std::string::npos) << err;
  }
  // A malformed service endpoint (port out of range).
  {
    auto [code, err] = run_main({"--service-address", "127.0.0.1:70000"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--service-address"), std::string::npos) << err;
  }
  // A profile window default past the host-blocking cap.
  {
    auto [code, err] = run_main({"--profile-window-seconds", "301"});
    EXPECT_NE(code, 0);
    EXPECT_NE(err.find("--profile-window-seconds"), std::string::npos) << err;
  }
  // A clean flag surface passes validation and reaches the (immediately
  // EOF-terminated) serve loop.
  {
    auto [code, err] = run_main({});
    EXPECT_EQ(code, 0) << err;
  }
}

} // namespace
} // namespace Client
} // namespace Nighthawk
