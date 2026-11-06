// FORK-ONLY (We-Amp): the MCP line transport and JSON-RPC dispatcher.
//
// Protocol conformance is the product (Global Constraints): every tools/call
// success carries the full envelope; handler errors are isError envelopes;
// JSON-RPC errors are reserved for protocol faults (parse -32700, method
// -32601, invalid-params -32602) and for the pre-initialize gate (-32002).
// ping answers an empty result; initialize answers the server's latest
// supported protocolVersion (never an echo) and advertises both the tools
// and resources capabilities. Logging NEVER touches the protocol sink: the
// protocol speaks on the LineSink (stdout), diagnostics on the LogSink
// (stderr).
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_SERVER_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_SERVER_H_

#include <functional>
#include <mutex>
#include <optional>
#include <string>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {

// The server's latest supported MCP protocol revision. initialize answers
// with THIS version, not with whatever the client offered.
inline constexpr char kMcpProtocolVersion[] = "2025-06-18";
inline constexpr char kMcpServerName[] = "nighthawk-mcp";
inline constexpr char kMcpServerVersion[] = "0.1.0";

class McpServer {
public:
  // Called with one canonical JSON line per response (stdout in production).
  using LineSink = std::function<void(absl::string_view line)>;
  // Called with one line per diagnostic (stderr in production).
  using LogSink = std::function<void(absl::string_view line)>;
  // Runs one deferred tool handler OFF the dispatch loop (the bounded waits
  // run on a waiter pool; the dispatcher stays free so ping answers during
  // a 4s/35s wait — Global Constraints). When unset, deferred handlers run
  // inline (indistinguishable from a fast handler for tests).
  using TaskDispatcher = std::function<void(std::function<void()>)>;

  // The registries must outlive the server.
  McpServer(ToolRegistry& tools, ResourceRegistry& resources, LineSink sink,
            LogSink log = [](absl::string_view) {}, TaskDispatcher dispatch = nullptr);

  // Handles one transport line; returns the emitted response line when (and
  // only when) the input was a REQUEST — notifications produce no response,
  // and empty lines are logged and skipped. The response is also delivered
  // through the LineSink.
  std::optional<std::string> HandleLine(absl::string_view line);

  bool initialized() const { return initialized_; }

private:
  void Respond(const google::protobuf::Value& id, const google::protobuf::Struct& result);
  void RespondError(const google::protobuf::Value& id, int code, absl::string_view message);
  // The most recently emitted response line, under the response mutex (a
  // deferred handler may have responded between two dispatch-loop lines).
  std::string LastResponse() const;

  // initialize: the server's protocolVersion plus both capabilities.
  google::protobuf::Struct HandleInitialize();
  google::protobuf::Struct HandleToolsList();
  // tools/call: full envelope for success AND handler error; invalid params
  // (unknown tool, malformed arguments) come back as a field-naming
  // InvalidArgumentError that the dispatcher turns into -32602. A DEFERRED
  // tool (ToolSpec::deferred) with a dispatcher installed is submitted to
  // the dispatcher instead — `deferred` is set true and no Struct is
  // returned (the envelope lands through the sink when the handler
  // completes, possibly out of band relative to later requests).
  absl::StatusOr<google::protobuf::Struct> HandleToolsCall(
      const google::protobuf::Struct& params, const google::protobuf::Value& id, bool* deferred);
  google::protobuf::Struct HandleResourcesList();
  // resources/read: unknown URI is a field-naming InvalidArgumentError
  // (-32602); a failing reader is the same fault class.
  absl::StatusOr<google::protobuf::Struct> HandleResourcesRead(
      const google::protobuf::Struct& params);

  // JSON-Schema subset validation (type "object", properties with primitive
  // types, required, additionalProperties:false); the message names the
  // offending field.
  static absl::Status ValidateArguments(const google::protobuf::Struct& schema,
                                        const google::protobuf::Struct& arguments);

  ToolRegistry& tools_;
  ResourceRegistry& resources_;
  LineSink sink_;
  LogSink log_;
  TaskDispatcher dispatch_;
  // Guards last_response_ and the sink: deferred handlers respond from
  // dispatcher threads while the dispatch loop keeps serving lines.
  mutable std::mutex response_mutex_;
  // The most recently emitted response line (what a line transport returns).
  std::string last_response_ ABSL_GUARDED_BY(response_mutex_);
  bool initialized_ = false;
};

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_SERVER_H_
