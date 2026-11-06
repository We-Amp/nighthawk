// FORK-ONLY (We-Amp): see json_util.h.
#include "source/client/mcp/json_util.h"

#include <cmath>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

#include "google/protobuf/json/json.h"
#include "google/protobuf/struct.pb.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::ListValue;
using google::protobuf::Struct;
using google::protobuf::Value;

// Escapes one string as a JSON string literal: quotes, backslashes, and the
// named control escapes, with every other control character as \u00XX.
// Valid UTF-8 passes through as-is (it is valid JSON).
void EmitJsonString(absl::string_view s, std::string* out) {
  out->push_back('"');
  for (char c : s) {
    switch (c) {
    case '"':
      *out += "\\\"";
      break;
    case '\\':
      *out += "\\\\";
      break;
    case '\b':
      *out += "\\b";
      break;
    case '\f':
      *out += "\\f";
      break;
    case '\n':
      *out += "\\n";
      break;
    case '\r':
      *out += "\\r";
      break;
    case '\t':
      *out += "\\t";
      break;
    default:
      if (static_cast<unsigned char>(c) < 0x20) {
        absl::StrAppendFormat(out, "\\u%04x", static_cast<unsigned char>(c));
      } else {
        out->push_back(c);
      }
    }
  }
  out->push_back('"');
}

// Formats a double losslessly for the emit->parse round trip: the shortest
// %g form that reads back bit-identical, falling back to %.17g.
std::string FormatNumber(double v) {
  // The range check MUST come before the cast: casting a double whose
  // magnitude exceeds int64_t's range to int64_t is undefined behavior, and
  // an agent-supplied number (1e30 through an rps argument) reaches here
  // first. Only after |v| is proven inside the range is the integral test
  // (whose cast is then well-defined) allowed to fire.
  if (std::isfinite(v) && std::abs(v) < 9.0e15 &&
      v == static_cast<double>(static_cast<int64_t>(v))) {
    // Integral values (ids, counts, rps figures) emit without a fraction.
    return absl::StrCat(static_cast<int64_t>(v));
  }
  char buf[64];
  for (int precision : {15, 16, 17}) {
    snprintf(buf, sizeof(buf), "%.*g", precision, v);
    if (strtod(buf, nullptr) == v) {
      return buf;
    }
  }
  return buf;
}

} // namespace

absl::StatusOr<Struct> ParseJsonObject(absl::string_view json) {
  Value value;
  google::protobuf::json::ParseOptions options;
  options.ignore_unknown_fields = true;
  auto status = google::protobuf::json::JsonStringToMessage(json, &value, options);
  if (!status.ok()) {
    return absl::InvalidArgumentError(std::string(status.message()));
  }
  if (value.kind_case() != Value::kStructValue) {
    return absl::InvalidArgumentError("line is not a JSON object");
  }
  return std::move(*value.mutable_struct_value());
}

google::protobuf::Value ToValue(const Struct& s) {
  Value v;
  *v.mutable_struct_value() = s;
  return v;
}

std::string EmitCanonical(const Value& value) {
  std::string out;
  switch (value.kind_case()) {
  case Value::kNullValue:
    out += "null";
    break;
  case Value::kBoolValue:
    out += value.bool_value() ? "true" : "false";
    break;
  case Value::kNumberValue:
    out += FormatNumber(value.number_value());
    break;
  case Value::kStringValue:
    EmitJsonString(value.string_value(), &out);
    break;
  case Value::kListValue: {
    out.push_back('[');
    bool first = true;
    for (const Value& element : value.list_value().values()) {
      if (!first) {
        out.push_back(',');
      }
      first = false;
      out += EmitCanonical(element);
    }
    out.push_back(']');
    break;
  }
  case Value::kStructValue: {
    // Deterministic key order: sorted. protobuf's map iteration order is
    // unspecified, so the fields are staged through a std::map.
    std::map<std::string, const Value*> sorted;
    for (const auto& [key, field_value] : value.struct_value().fields()) {
      sorted.emplace(key, &field_value);
    }
    out.push_back('{');
    bool first = true;
    for (const auto& [key, field_value] : sorted) {
      if (!first) {
        out.push_back(',');
      }
      first = false;
      EmitJsonString(key, &out);
      out.push_back(':');
      out += EmitCanonical(*field_value);
    }
    out.push_back('}');
    break;
  }
  case Value::KIND_NOT_SET:
    out += "null";
    break;
  }
  return out;
}

std::string EmitCanonicalStruct(const Struct& s) { return EmitCanonical(ToValue(s)); }

} // namespace Client
} // namespace Nighthawk
