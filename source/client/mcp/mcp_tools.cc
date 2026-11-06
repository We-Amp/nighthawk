// FORK-ONLY (We-Amp): see mcp_tools.h.
#include "source/client/mcp/mcp_tools.h"

#include "absl/strings/str_cat.h"

#include "source/client/mcp/json_util.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::Struct;
using google::protobuf::Value;

constexpr char kEchoDescription[] =
    "Echoes its argument back. Args: message (string, required). Returns "
    "{message: <the string>} in structuredContent and as canonical JSON in "
    "the text content part. Malformed arguments are rejected as "
    "invalid-params naming the offending field.";

constexpr char kEchoInputSchema[] = R"json(
    {"type": "object",
     "properties": {"message": {"type": "string",
                                "description": "The string to echo back."}},
     "required": ["message"],
     "additionalProperties": false})json";

} // namespace

void ToolRegistry::Register(ToolSpec spec) { tools_.push_back(std::move(spec)); }

const std::vector<ToolSpec>& ToolRegistry::tools() const { return tools_; }

const ToolSpec* ToolRegistry::Find(absl::string_view name) const {
  for (const ToolSpec& tool : tools_) {
    if (tool.name == name) {
      return &tool;
    }
  }
  return nullptr;
}

void ResourceRegistry::Register(ResourceSpec spec) { resources_.push_back(std::move(spec)); }

const std::vector<ResourceSpec>& ResourceRegistry::resources() const { return resources_; }

const ResourceSpec* ResourceRegistry::Find(absl::string_view uri) const {
  for (const ResourceSpec& resource : resources_) {
    if (resource.uri == uri) {
      return &resource;
    }
  }
  return nullptr;
}

google::protobuf::Struct CallToolResult(const Struct& object) {
  Struct envelope;
  Struct& text_part = *(*envelope.mutable_fields())["content"]
                           .mutable_list_value()
                           ->add_values()
                           ->mutable_struct_value();
  (*text_part.mutable_fields())["type"].set_string_value("text");
  (*text_part.mutable_fields())["text"].set_string_value(EmitCanonicalStruct(object));
  (*envelope.mutable_fields())["structuredContent"] = ToValue(object);
  return envelope;
}

google::protobuf::Struct CallToolError(absl::string_view reason) {
  Struct envelope;
  Struct& text_part = *(*envelope.mutable_fields())["content"]
                           .mutable_list_value()
                           ->add_values()
                           ->mutable_struct_value();
  (*text_part.mutable_fields())["type"].set_string_value("text");
  (*text_part.mutable_fields())["text"].set_string_value(std::string(reason));
  (*envelope.mutable_fields())["isError"].set_bool_value(true);
  return envelope;
}

void RegisterEchoTool(ToolRegistry& registry) {
  ToolSpec echo;
  echo.name = "echo";
  echo.description = kEchoDescription;
  echo.input_schema = ParseJsonObject(kEchoInputSchema).value();
  echo.handler = [](const Struct& arguments) -> absl::StatusOr<Struct> {
    Struct result;
    (*result.mutable_fields())["message"] = arguments.fields().at("message");
    return result;
  };
  registry.Register(std::move(echo));
}

} // namespace Client
} // namespace Nighthawk
