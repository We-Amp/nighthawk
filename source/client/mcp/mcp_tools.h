// FORK-ONLY (We-Amp): the MCP tool and resource registries, plus the
// Global-Constraints envelope helpers. Every tools/call success is the full
// envelope (content:[{type:"text",text:<canonical JSON>}] +
// structuredContent:<handler object>); every handler error is the same
// envelope with isError:true and the reason verbatim in the text — never a
// JSON-RPC error (those are reserved for protocol faults).
//
// Tool descriptions are specified copy: the agent's only manual. Each names
// its arguments, the global-total rps semantics, the ceiling, and the error
// shape.
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_

#include <functional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

namespace Nighthawk {
namespace Client {

using ToolHandler =
    std::function<absl::StatusOr<google::protobuf::Struct>(const google::protobuf::Struct&)>;

struct ToolSpec {
  std::string name;
  std::string description;
  // JSON Schema (as a Struct) for the arguments object: type "object",
  // "properties", "required", and "additionalProperties": false are honored
  // by the dispatcher's validation (field-naming invalid-params).
  google::protobuf::Struct input_schema;
  ToolHandler handler;
};

class ToolRegistry {
public:
  // Registration order is the listing order (tools/list).
  void Register(ToolSpec spec);
  const std::vector<ToolSpec>& tools() const;
  const ToolSpec* Find(absl::string_view name) const;

private:
  std::vector<ToolSpec> tools_;
};

using ResourceReader = std::function<absl::StatusOr<google::protobuf::Struct>()>;

struct ResourceSpec {
  std::string uri;
  std::string name;
  std::string description;
  std::string mime_type; // "application/json" for every structured resource.
  ResourceReader reader;
};

class ResourceRegistry {
public:
  void Register(ResourceSpec spec);
  const std::vector<ResourceSpec>& resources() const;
  const ResourceSpec* Find(absl::string_view uri) const;

private:
  std::vector<ResourceSpec> resources_;
};

// The success envelope: {content:[{type:"text",text:<canonical JSON of
// object>}], structuredContent:<object>}.
google::protobuf::Struct CallToolResult(const google::protobuf::Struct& object);

// The handler-error envelope: same shape with isError:true and the reason
// verbatim as the text part.
google::protobuf::Struct CallToolError(absl::string_view reason);

// The seed tool (the plan's Task 1 surface; NOT "ping", which is a protocol
// method). Its description is the agent-facing copy for its single argument.
void RegisterEchoTool(ToolRegistry& registry);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_TOOLS_H_
