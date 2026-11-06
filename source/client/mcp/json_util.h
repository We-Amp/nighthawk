// FORK-ONLY (We-Amp): the MCP JSON seam. Parsing goes through protobuf's JSON
// parser into a google.protobuf.Value (Struct at the top level); emission is
// a canonical, deterministic traversal (sorted keys, fixed escapes) so that
// byte-exact string comparison is never needed and never used — consumers
// parse. Telemetry discipline lives here too: SUT-derived strings are only
// ever VALUES, escaped by construction, never structure.
#ifndef NIGHTHAWK_CLIENT_MCP_JSON_UTIL_H_
#define NIGHTHAWK_CLIENT_MCP_JSON_UTIL_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/struct.pb.h"

namespace Nighthawk {
namespace Client {

// Parses one JSON object text into a Struct. Fails (absl::InvalidArgumentError)
// on malformed JSON; unknown fields are ignored (the transport treats
// non-object top-level values as parse faults at the dispatch layer).
absl::StatusOr<google::protobuf::Struct> ParseJsonObject(absl::string_view json);

// Wraps a Struct in a Value (the emitter's input type).
google::protobuf::Value ToValue(const google::protobuf::Struct& s);

// Emits canonical JSON by explicit traversal: object keys sorted, control
// characters escaped, numbers formatted so that the emit->parse round trip is
// lossless. MessageToJsonString is NOT used (its key ordering is not stable).
std::string EmitCanonical(const google::protobuf::Value& value);

std::string EmitCanonicalStruct(const google::protobuf::Struct& s);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_JSON_UTIL_H_
