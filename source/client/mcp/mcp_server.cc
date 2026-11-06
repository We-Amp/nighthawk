// FORK-ONLY (We-Amp): see mcp_server.h.
#include "source/client/mcp/mcp_server.h"

#include <cmath>
#include <map>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"

#include "google/protobuf/json/json.h"
#include "google/protobuf/struct.pb.h"

#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::Struct;
using google::protobuf::Value;

// JSON-RPC fault codes (protocol faults only — handler errors are
// isError envelopes, see CallToolError).
constexpr int kCodeParseError = -32700;
constexpr int kCodeInvalidParams = -32602;
constexpr int kCodeMethodNotFound = -32601;
// MCP: requests before a successful initialize are rejected with -32002.
constexpr int kCodeServerNotInitialized = -32002;

const Value& Field(const Struct& s, absl::string_view name) {
  static const Value* const empty = new Value();
  auto it = s.fields().find(std::string(name));
  return it == s.fields().end() ? *empty : it->second;
}

bool IsObject(const Value& v) { return v.kind_case() == Value::kStructValue; }

// JSON-Schema type check for one argument value against a property schema
// of {"type": "<primitive>"}. Unrecognized schema types are unconstrained.
bool TypeMatches(const std::string& expected, const Value& value) {
  if (expected == "string") {
    return value.kind_case() == Value::kStringValue;
  }
  if (expected == "boolean") {
    return value.kind_case() == Value::kBoolValue;
  }
  if (expected == "number") {
    return value.kind_case() == Value::kNumberValue;
  }
  if (expected == "integer") {
    if (value.kind_case() != Value::kNumberValue) {
      return false;
    }
    // The range check MUST come before the cast: casting a double whose
    // magnitude exceeds int64_t's range to int64_t is undefined behavior,
    // and an agent-supplied argument (1e30 through an rps field) reaches
    // here first. Out-of-range numbers are simply not integers.
    const double v = value.number_value();
    return std::isfinite(v) && std::abs(v) < 9.0e15 &&
           v == static_cast<double>(static_cast<int64_t>(v));
  }
  if (expected == "object") {
    return IsObject(value);
  }
  if (expected == "array") {
    return value.kind_case() == Value::kListValue;
  }
  return true;
}

} // namespace

McpServer::McpServer(ToolRegistry& tools, ResourceRegistry& resources, LineSink sink, LogSink log)
    : tools_(tools), resources_(resources), sink_(std::move(sink)), log_(std::move(log)) {}

std::optional<std::string> McpServer::HandleLine(absl::string_view line) {
  // Trim the transport framing; an empty line is noise, not a fault.
  absl::string_view trimmed = absl::StripAsciiWhitespace(line);
  if (trimmed.empty()) {
    return std::nullopt;
  }

  Value request_value;
  google::protobuf::json::ParseOptions options;
  options.ignore_unknown_fields = true;
  if (!google::protobuf::json::JsonStringToMessage(trimmed, &request_value, options).ok() ||
      !IsObject(request_value)) {
    log_(absl::StrCat("malformed line dropped (not parseable JSON): ", trimmed));
    // The request id is unknowable: null.
    RespondError(Value(), kCodeParseError, "Parse error");
    return last_response_;
  }

  const Struct& request = request_value.struct_value();
  const std::string& method = Field(request, "method").string_value();

  // No method: not a request and not a notification this server cares about
  // (e.g. a stray response object). Nothing to answer.
  if (method.empty()) {
    return std::nullopt;
  }

  // Notifications (no id field) never get a response.
  const bool is_notification = !request.fields().contains("id");
  if (is_notification) {
    log_(absl::StrCat("notification received: ", method));
    return std::nullopt;
  }

  const Value& id = Field(request, "id");

  if (!initialized_ && method != "initialize") {
    RespondError(id, kCodeServerNotInitialized, "Server not initialized");
    return last_response_;
  }

  if (method == "initialize") {
    initialized_ = true;
    Respond(id, HandleInitialize());
    return last_response_;
  }
  if (method == "ping") {
    Respond(id, Struct());
    return last_response_;
  }
  if (method == "tools/list") {
    Respond(id, HandleToolsList());
    return last_response_;
  }
  if (method == "tools/call") {
    const auto& params = Field(request, "params");
    if (!IsObject(params)) {
      RespondError(id, kCodeInvalidParams, "Invalid params: 'params' must be an object");
      return last_response_;
    }
    auto call = HandleToolsCall(params.struct_value());
    if (!call.ok()) {
      RespondError(id, kCodeInvalidParams, call.status().message());
    } else {
      Respond(id, *call);
    }
    return last_response_;
  }
  if (method == "resources/list") {
    Respond(id, HandleResourcesList());
    return last_response_;
  }
  if (method == "resources/read") {
    const Value& params = Field(request, "params");
    if (!IsObject(params)) {
      RespondError(id, kCodeInvalidParams, "Invalid params: 'params' must be an object");
      return last_response_;
    }
    auto read = HandleResourcesRead(params.struct_value());
    if (!read.ok()) {
      RespondError(id, kCodeInvalidParams, read.status().message());
    } else {
      Respond(id, *read);
    }
    return last_response_;
  }

  RespondError(id, kCodeMethodNotFound, absl::StrCat("Method not found: ", method));
  return last_response_;
}

void McpServer::Respond(const Value& id, const Struct& result) {
  Struct response;
  (*response.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*response.mutable_fields())["id"] = id;
  (*response.mutable_fields())["result"] = ToValue(result);
  last_response_ = EmitCanonicalStruct(response);
  sink_(last_response_);
}

void McpServer::RespondError(const Value& id, int code, absl::string_view message) {
  Struct error;
  (*error.mutable_fields())["code"].set_number_value(code);
  (*error.mutable_fields())["message"].set_string_value(std::string(message));
  Struct response;
  (*response.mutable_fields())["jsonrpc"].set_string_value("2.0");
  (*response.mutable_fields())["id"] = id;
  (*response.mutable_fields())["error"] = ToValue(error);
  last_response_ = EmitCanonicalStruct(response);
  sink_(last_response_);
}

Struct McpServer::HandleInitialize() {
  Struct result;
  (*result.mutable_fields())["protocolVersion"].set_string_value(kMcpProtocolVersion);
  Struct capabilities;
  (*capabilities.mutable_fields())["tools"] = ToValue(Struct());
  (*capabilities.mutable_fields())["resources"] = ToValue(Struct());
  (*result.mutable_fields())["capabilities"] = ToValue(capabilities);
  Struct server_info;
  server_info.mutable_fields()->operator[]("name").set_string_value(kMcpServerName);
  server_info.mutable_fields()->operator[]("version").set_string_value(kMcpServerVersion);
  (*result.mutable_fields())["serverInfo"] = ToValue(server_info);
  return result;
}

Struct McpServer::HandleToolsList() {
  Struct result;
  // Force the list kind even for an empty registry: tools/list answers
  // "tools": [], never null.
  Value& tools_value = (*result.mutable_fields())["tools"];
  tools_value.mutable_list_value();
  for (const ToolSpec& tool : tools_.tools()) {
    Struct& entry = *tools_value.mutable_list_value()->add_values()->mutable_struct_value();
    (*entry.mutable_fields())["name"].set_string_value(tool.name);
    (*entry.mutable_fields())["description"].set_string_value(tool.description);
    (*entry.mutable_fields())["inputSchema"] = ToValue(tool.input_schema);
  }
  return result;
}

absl::StatusOr<Struct> McpServer::HandleToolsCall(const Struct& params) {
  const Value& name_value = Field(params, "name");
  if (name_value.kind_case() != Value::kStringValue) {
    return absl::InvalidArgumentError("Invalid params: 'name' (string) is required");
  }
  const ToolSpec* tool = tools_.Find(name_value.string_value());
  if (tool == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid params: unknown tool '", name_value.string_value(), "'"));
  }

  Struct arguments;
  if (params.fields().contains("arguments")) {
    const Value& arguments_value = params.fields().at("arguments");
    if (!IsObject(arguments_value)) {
      return absl::InvalidArgumentError("Invalid params: 'arguments' must be an object");
    }
    arguments = arguments_value.struct_value();
  }

  absl::Status valid = ValidateArguments(tool->input_schema, arguments);
  if (!valid.ok()) {
    return valid;
  }

  auto handler_result = tool->handler(arguments);
  if (!handler_result.ok()) {
    // Handler faults are isError envelopes, never JSON-RPC errors: the
    // reason travels verbatim in the text part.
    return CallToolError(handler_result.status().message());
  }
  return CallToolResult(*handler_result);
}

Struct McpServer::HandleResourcesList() {
  Struct result;
  // Same empty-list discipline as tools/list.
  Value& resources_value = (*result.mutable_fields())["resources"];
  resources_value.mutable_list_value();
  for (const ResourceSpec& resource : resources_.resources()) {
    Struct& entry = *resources_value.mutable_list_value()->add_values()->mutable_struct_value();
    (*entry.mutable_fields())["uri"].set_string_value(resource.uri);
    (*entry.mutable_fields())["name"].set_string_value(resource.name);
    (*entry.mutable_fields())["description"].set_string_value(resource.description);
    (*entry.mutable_fields())["mimeType"].set_string_value(resource.mime_type);
  }
  return result;
}

absl::StatusOr<Struct> McpServer::HandleResourcesRead(const Struct& params) {
  const Value& uri_value = Field(params, "uri");
  if (uri_value.kind_case() != Value::kStringValue) {
    return absl::InvalidArgumentError("Invalid params: 'uri' (string) is required");
  }
  const ResourceSpec* resource = resources_.Find(uri_value.string_value());
  if (resource == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid params: unknown resource '", uri_value.string_value(), "'"));
  }
  auto payload = resource->reader();
  if (!payload.ok()) {
    return payload.status();
  }
  Struct result;
  Struct& content = *(*result.mutable_fields())["contents"]
                         .mutable_list_value()
                         ->add_values()
                         ->mutable_struct_value();
  (*content.mutable_fields())["uri"].set_string_value(resource->uri);
  (*content.mutable_fields())["mimeType"].set_string_value(resource->mime_type);
  (*content.mutable_fields())["text"].set_string_value(EmitCanonicalStruct(*payload));
  return result;
}

absl::Status McpServer::ValidateArguments(const Struct& schema, const Struct& arguments) {
  // Only object schemas carry argument constraints; anything else is
  // treated as unconstrained.
  const Value& type_value = Field(schema, "type");
  if (type_value.kind_case() != Value::kStringValue || type_value.string_value() != "object") {
    return absl::OkStatus();
  }

  // Required arguments.
  const Value& required = Field(schema, "required");
  if (required.kind_case() == Value::kListValue) {
    for (const Value& name : required.list_value().values()) {
      if (name.kind_case() == Value::kStringValue &&
          !arguments.fields().contains(name.string_value())) {
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid params: missing required argument '", name.string_value(), "'"));
      }
    }
  }

  const Value& properties = Field(schema, "properties");
  const bool properties_object = IsObject(properties);
  const bool closed = Field(schema, "additionalProperties").bool_value() == false;

  // Per-argument checks: unknown fields (when the schema closes the object)
  // and per-property types.
  for (const auto& [key, value] : arguments.fields()) {
    const Struct* property_schema = nullptr;
    if (properties_object && properties.struct_value().fields().contains(key)) {
      property_schema = &properties.struct_value().fields().at(key).struct_value();
    }
    if (property_schema == nullptr) {
      if (closed) {
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid params: unknown argument '", key, "'"));
      }
      continue;
    }
    const Value& expected = Field(*property_schema, "type");
    if (expected.kind_case() == Value::kStringValue &&
        !TypeMatches(expected.string_value(), value)) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Invalid params: argument '", key, "' must be of type ", expected.string_value()));
    }
  }
  return absl::OkStatus();
}

} // namespace Client
} // namespace Nighthawk
