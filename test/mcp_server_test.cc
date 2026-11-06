// FORK-ONLY (We-Amp): the MCP transport + dispatch skeleton contract pins
// (MCP plan Task 1). Protocol conformance is the product: initialize answers
// the SERVER's latest supported version with both capabilities, ping answers
// an empty result, tool calls carry the full envelope (content +
// structuredContent), handler errors are isError envelopes (not JSON-RPC
// errors), pre-initialize requests are -32002, unknown methods -32601,
// malformed JSON -32700, and malformed arguments -32602 naming the field.
// Every comparison is parse-then-compare: raw strings are never compared
// (SUT-derived strings must survive as escaped VALUES, and the canonical
// emitter's key order is an implementation detail).
//
// Plain cc_test for the same quarantine reason as the headless/TUI tests:
// mcp_lib carries no Envoy C++ sources and no test-helper stack.
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_main.h"
#include "source/client/mcp/mcp_server.h"
#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::Struct;
using google::protobuf::Value;

// Builds a JSON-RPC request as canonical JSON text and sends it through the
// server's line transport. Returns the parsed response struct; fails the
// test when the line produced no response (e.g. it was a notification).
absl::StatusOr<Struct> SendAndParse(McpServer& server, const std::string& raw_line) {
  std::vector<std::string> responses;
  McpServer::LineSink sink = [&](absl::string_view line) { responses.push_back(std::string(line)); };
  // The sink is server-owned in production; for the test we re-drive
  // HandleLine directly and collect what it returns instead.
  auto response = server.HandleLine(raw_line);
  (void)sink;
  if (!response.has_value()) {
    return absl::InternalError("no response for line: " + raw_line);
  }
  return ParseJsonObject(*response);
}

// Sends a request built from a method, an id, and params given as raw JSON.
// Params are round-tripped through the parser, never string-spliced.
absl::StatusOr<Struct> RequestAndParse(McpServer& server, int id, const std::string& method,
                                       const std::string& params_json = "{}") {
  Struct params;
  auto params_or = ParseJsonObject(params_json);
  if (!params_or.ok()) return params_or.status();
  Struct request;
  (*request.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*request.mutable_fields())["id"].set_number_value(id);
  (*request.mutable_fields())["method"].set_string_value(method);
  (*request.mutable_fields())["params"] = ToValue(*params_or);
  return SendAndParse(server, EmitCanonicalStruct(request));
}

Value ToValue(const Struct& s) {
  Value v;
  *v.mutable_struct_value() = s;
  return v;
}

const Value& Field(const Struct& s, absl::string_view name) {
  static google::protobuf::Value empty;
  auto it = s.fields().find(std::string(name));
  return it == s.fields().end() ? empty : it->second;
}

const Struct& StructOf(const google::protobuf::Value& v) { return v.struct_value(); }

double Number(const google::protobuf::Value& v) { return v.number_value(); }

std::string Text(const Value& v) { return v.string_value(); }

bool HasKey(const Struct& s, absl::string_view name) {
  return s.fields().find(std::string(name)) != s.fields().end();
}

// content[0] of a tool-call result envelope (the single text part).
const Struct& content0(const Struct& result) {
  return StructOf(Field(result, "content").list_value().values(0));
}

// A minimal initialize round-trip, for tests that need the initialized state.
void DoInitialize(McpServer& server) {
  auto response = RequestAndParse(server, 1, "initialize", R"json(
      {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "test"}})json");
  ASSERT_TRUE(response.ok()) << response.status();
}

TEST(McpServerTest, InitializeRespondsWithServerProtocolVersionAndBothCapabilities) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  auto response = RequestAndParse(server, 1, "initialize", R"json(
      {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "test"}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& result = StructOf(Field(*response, "result"));
  // protocolVersion is the server's latest supported version, not an echo of
  // the client's offer.
  EXPECT_EQ(Text(Field(result, "protocolVersion")), kMcpProtocolVersion);
  const Struct& capabilities = StructOf(Field(result, "capabilities"));
  EXPECT_TRUE(HasKey(capabilities, "tools"));
  EXPECT_TRUE(HasKey(capabilities, "resources"));
  const Struct& server_info = StructOf(Field(result, "serverInfo"));
  EXPECT_FALSE(Text(Field(server_info, "name")).empty());
  EXPECT_TRUE(HasKey(*response, "id"));
  EXPECT_TRUE(server.initialized());
}

TEST(McpServerTest, PingAnswersEmptyResult) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 2, "ping");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& result = StructOf(Field(*response, "result"));
  EXPECT_TRUE(result.fields().empty()) << "ping must answer an empty result object";
}

TEST(McpServerTest, EchoToolReturnsFullEnvelope) {
  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 3, "tools/call",
                                  R"json({"name": "echo",
                                          "arguments": {"message": "hello \"world\"\nnext"}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& result = StructOf(Field(*response, "result"));
  EXPECT_FALSE(result.fields().contains("isError"));

  // structuredContent is the handler object, parse-compared.
  const Struct& structured = StructOf(Field(result, "structuredContent"));
  EXPECT_EQ(Text(Field(structured, "message")), "hello \"world\"\nnext");

  // content[0] is a text part whose text is the canonical JSON of the same
  // object — proven by parsing it back, never by string equality.
  const google::protobuf::Value& content = Field(result, "content");
  ASSERT_EQ(content.list_value().values_size(), 1);
  const Struct& part = StructOf(content.list_value().values(0));
  EXPECT_EQ(Text(Field(part, "type")), "text");
  auto text_as_struct = ParseJsonObject(Text(Field(part, "text")));
  ASSERT_TRUE(text_as_struct.ok()) << Text(Field(part, "text"));
  EXPECT_EQ(Text(Field(*text_as_struct, "message")), "hello \"world\"\nnext");
}

TEST(McpServerTest, HandlerErrorSetsIsErrorWithReason) {
  ToolRegistry tools;
  ResourceRegistry resources;
  ToolSpec failing;
  failing.name = "fail";
  failing.description = "always fails";
  failing.input_schema = ParseJsonObject(
      R"json({"type": "object", "properties": {}, "additionalProperties": false})json")
                            .value();
  failing.handler = [](const Struct&) -> absl::StatusOr<Struct> {
    return absl::InvalidArgumentError("engine rejected the update: validator said no");
  };
  tools.Register(failing);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 4, "tools/call",
                                  R"json({"name": "fail", "arguments": {}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& result = StructOf(Field(*response, "result"));
  EXPECT_TRUE(Field(result, "isError").bool_value());
  // The reason travels verbatim in the text part; there is no JSON-RPC error.
  EXPECT_EQ(Text(Field(content0(result), "text")),
            "engine rejected the update: validator said no");
}

TEST(McpServerTest, PreInitializeRequestRejected) {
  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);
  McpServer server(tools, resources, [](absl::string_view) {});
  auto response = RequestAndParse(server, 1, "tools/list");
  ASSERT_TRUE(response.ok()) << response.status();
  const google::protobuf::Value& error = Field(*response, "error");
  EXPECT_EQ(Number(Field(StructOf(error), "code")), -32002);
  EXPECT_FALSE(Text(Field(StructOf(error), "message")).empty());
}

TEST(McpServerTest, UnknownMethodIs32601) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 5, "sampling/createMessage");
  ASSERT_TRUE(response.ok()) << response.status();
  EXPECT_EQ(Number(Field(StructOf(Field(*response, "error")), "code")), -32601);
}

TEST(McpServerTest, MalformedJsonIs32700) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  auto response = SendAndParse(server, "{not json at all");
  ASSERT_TRUE(response.ok()) << response.status();
  EXPECT_EQ(Number(Field(StructOf(Field(*response, "error")), "code")), -32700);
  // The id of a parse-error response is null (the request id is unknowable).
  EXPECT_TRUE(Field(*response, "id").kind_case() == google::protobuf::Value::kNullValue);
}

TEST(McpServerTest, UnknownArgFieldIsInvalidParamsNamingTheField) {
  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 6, "tools/call",
                                  R"json({"name": "echo",
                                          "arguments": {"message": "x", "bogus_field": 1}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& error = StructOf(Field(*response, "error"));
  EXPECT_EQ(Number(Field(error, "code")), -32602);
  EXPECT_TRUE(Text(Field(error, "message")).find("bogus_field") != std::string::npos)
      << "the invalid-params message must name the offending field: "
      << Text(Field(error, "message"));
}

TEST(McpServerTest, WrongTypeArgIsInvalidParamsNamingTheField) {
  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 7, "tools/call",
                                  R"json({"name": "echo",
                                          "arguments": {"message": 42}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& error = StructOf(Field(*response, "error"));
  EXPECT_EQ(Number(Field(error, "code")), -32602);
  EXPECT_TRUE(Text(Field(error, "message")).find("message") != std::string::npos)
      << Text(Field(error, "message"));
}

TEST(McpServerTest, ResourcesListedAndRead) {
  ToolRegistry tools;
  ResourceRegistry resources;
  ResourceSpec resource;
  resource.uri = "nighthawk://test/object";
  resource.name = "test object";
  resource.description = "a test resource";
  resource.mime_type = "application/json";
  resource.reader = []() -> absl::StatusOr<Struct> {
    return ParseJsonObject(R"json({"kind": "status", "busy": false})json");
  };
  resources.Register(resource);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);

  auto listing = RequestAndParse(server, 8, "resources/list");
  ASSERT_TRUE(listing.ok()) << listing.status();
  const google::protobuf::Value& listed = Field(StructOf(Field(*listing, "result")), "resources");
  ASSERT_EQ(listed.list_value().values_size(), 1);
  const Struct& entry = StructOf(listed.list_value().values(0));
  EXPECT_EQ(Text(Field(entry, "uri")), "nighthawk://test/object");

  auto read = RequestAndParse(server, 9, "resources/read",
                              R"json({"uri": "nighthawk://test/object"})json");
  ASSERT_TRUE(read.ok()) << read.status();
  const Struct& read_result = StructOf(Field(*read, "result"));
  const google::protobuf::Value& contents = Field(read_result, "contents");
  ASSERT_EQ(contents.list_value().values_size(), 1);
  const Struct& content = StructOf(contents.list_value().values(0));
  EXPECT_EQ(Text(Field(content, "uri")), "nighthawk://test/object");
  auto payload = ParseJsonObject(Text(Field(content, "text")));
  ASSERT_TRUE(payload.ok()) << Text(Field(content, "text"));
  EXPECT_EQ(Text(Field(*payload, "kind")), "status");
  EXPECT_FALSE(Field(*payload, "busy").bool_value());

  // An unknown URI is invalid params, naming the URI.
  auto bad = RequestAndParse(server, 10, "resources/read",
                             R"json({"uri": "nighthawk://nope"})json");
  ASSERT_TRUE(bad.ok()) << bad.status();
  const Struct& error = StructOf(Field(*bad, "error"));
  EXPECT_EQ(Number(Field(error, "code")), -32602);
  EXPECT_TRUE(Text(Field(error, "message")).find("nighthawk://nope") != std::string::npos)
      << Text(Field(error, "message"));
}

TEST(McpServerTest, NotificationsProduceNoResponse) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  Struct notification;
  (*notification.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*notification.mutable_fields())["method"].set_string_value("notifications/initialized");
  auto response = server.HandleLine(EmitCanonicalStruct(notification));
  EXPECT_FALSE(response.has_value());
}

TEST(McpServerTest, EmptyRegistriesListEmptyArrays) {
  ToolRegistry tools;
  ResourceRegistry resources;
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  // tools/list and resources/list answer arrays even when empty — never
  // null (the canonical emitter would render an unset list as null).
  auto listing = RequestAndParse(server, 11, "tools/list");
  ASSERT_TRUE(listing.ok()) << listing.status();
  EXPECT_EQ(Field(StructOf(Field(*listing, "result")), "tools").list_value().values_size(), 0);
  auto resources_listing = RequestAndParse(server, 12, "resources/list");
  ASSERT_TRUE(resources_listing.ok()) << resources_listing.status();
  EXPECT_EQ(Field(StructOf(Field(*resources_listing, "result")), "resources")
                .list_value()
                .values_size(),
            0);
}

// Carried T1 review fix pin: a number whose magnitude exceeds int64_t's
// range must not be cast to int64_t before the range check (undefined
// behavior). The canonical emitter formats it losslessly through the %g
// fallback, and the parsed value round-trips exactly.
TEST(McpServerTest, HugeNumberEmitsAndParsesLosslessly) {
  Value number;
  number.set_number_value(1e30);
  auto emitted = ParseJsonObject(absl::StrCat("{\"n\": ", EmitCanonical(number), "}"));
  ASSERT_TRUE(emitted.ok()) << emitted.status();
  EXPECT_EQ(Number(Field(*emitted, "n")), 1e30);

  Value negative;
  negative.set_number_value(-1e30);
  auto emitted_negative = ParseJsonObject(absl::StrCat("{\"n\": ", EmitCanonical(negative), "}"));
  ASSERT_TRUE(emitted_negative.ok()) << emitted_negative.status();
  EXPECT_EQ(Number(Field(*emitted_negative, "n")), -1e30);
}

// Carried T1 review fix pin: an integer-typed argument carrying a huge
// number (1e30) is rejected as invalid-params naming the field — the
// type check must not cast it to int64_t (undefined behavior) on the way.
TEST(McpServerTest, HugeIntegerArgIsRejectedNotCast) {
  ToolRegistry tools;
  ResourceRegistry resources;
  ToolSpec counter;
  counter.name = "counter";
  counter.description = "takes an integer";
  counter.input_schema = ParseJsonObject(R"json(
      {"type": "object",
       "properties": {"n": {"type": "integer"}},
       "required": ["n"],
       "additionalProperties": false})json").value();
  counter.handler = [](const Struct& arguments) -> absl::StatusOr<Struct> {
    Struct result;
    (*result.mutable_fields())["n"] = arguments.fields().at("n");
    return result;
  };
  tools.Register(counter);
  McpServer server(tools, resources, [](absl::string_view) {});
  DoInitialize(server);
  auto response = RequestAndParse(server, 13, "tools/call",
                                  R"json({"name": "counter",
                                          "arguments": {"n": 1e30}})json");
  ASSERT_TRUE(response.ok()) << response.status();
  const Struct& error = StructOf(Field(*response, "error"));
  EXPECT_EQ(Number(Field(error, "code")), -32602);
  EXPECT_TRUE(Text(Field(error, "message")).find("n") != std::string::npos)
      << Text(Field(error, "message"));
}

TEST(McpServerTest, LogsGoToStderrNotStdout) {
  McpServerFlags flags;
  std::istringstream in(absl::StrCat(
      // A startup-time log-worthy malformed line, then the protocol traffic.
      "not json\n", R"({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})", "\n",
      R"({"jsonrpc":"2.0","id":2,"method":"ping"})", "\n"));
  std::ostringstream out;
  std::ostringstream err;
  const int rc = RunMcpServer(flags, in, out, err);
  EXPECT_EQ(rc, 0);

  // stdout carries ONLY protocol responses, one canonical line each: the
  // parse-error fault for the malformed line plus the two answers.
  std::vector<std::string> out_lines;
  for (const auto piece : absl::StrSplit(out.str(), '\n', absl::SkipEmpty())) {
    out_lines.push_back(std::string(piece));
  }
  ASSERT_EQ(out_lines.size(), 3) << out.str();
  for (const std::string& line : out_lines) {
    auto parsed = ParseJsonObject(line);
    ASSERT_TRUE(parsed.ok()) << "stdout line is not valid JSON: " << line;
  }
  EXPECT_EQ(
      Number(Field(StructOf(Field(*ParseJsonObject(out_lines[0]), "error")), "code")), -32700);
  EXPECT_TRUE(Field(*ParseJsonObject(out_lines[1]), "result").struct_value().fields().contains(
      "protocolVersion"));
  EXPECT_TRUE(Field(*ParseJsonObject(out_lines[2]), "result").struct_value().fields().empty());

  // stderr carried the diagnostics (the malformed-line log and the startup
  // banner); none of it leaked to stdout.
  EXPECT_FALSE(err.str().empty()) << "the malformed line must be logged to stderr";
  EXPECT_TRUE(out.str().find("not json") == std::string::npos);
}

} // namespace
} // namespace Client
} // namespace Nighthawk
