// FORK-ONLY (We-Amp): see mcp_tools.h.
#include "source/client/mcp/mcp_tools.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstring>

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

#include "api/client/service.pb.h"

#include "source/client/headless/fold_tracker.h"
#include "source/client/headless/session_recorder.h"
#include "source/client/mcp/json_util.h"
#include "source/client/mcp/mcp_resources.h"
#include "source/client/tui/knob_rules.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/start_run_dialog.h"

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

// --- MCP plan Task 2: target scoping + the steering tools -------------------

namespace {

using google::protobuf::ListValue;

// Struct-building helpers (canonical JSON values only).
void SetString(Struct& s, absl::string_view key, absl::string_view value) {
  (*s.mutable_fields())[std::string(key)].set_string_value(std::string(value));
}
void SetBool(Struct& s, absl::string_view key, bool value) {
  (*s.mutable_fields())[std::string(key)].set_bool_value(value);
}
void SetInt(Struct& s, absl::string_view key, int64_t value) {
  (*s.mutable_fields())[std::string(key)].set_number_value(static_cast<double>(value));
}
void SetDouble(Struct& s, absl::string_view key, double value) {
  (*s.mutable_fields())[std::string(key)].set_number_value(value);
}

const Value* FindField(const Struct& s, absl::string_view name) {
  const auto it = s.fields().find(std::string(name));
  return it == s.fields().end() ? nullptr : &it->second;
}

// A schema-validated integer argument (the dispatcher's type check proved
// the magnitude inside int64's range before this cast — see TypeMatches).
int64_t IntegerOf(const Value& v) { return static_cast<int64_t>(v.number_value()); }

// A CIDR allowlist entry: family + network bytes + prefix length.
struct Cidr {
  int family; // AF_INET or AF_INET6
  std::array<uint8_t, 16> bytes{}; // network byte order, left-aligned
  int prefix_len;
};

absl::StatusOr<Cidr> ParseCidr(absl::string_view entry) {
  const std::string entry_str(entry);
  const size_t slash = entry_str.rfind('/');
  if (slash == std::string::npos) {
    return absl::InvalidArgumentError(
        absl::StrCat("malformed CIDR '", entry, "': expected <address>/<prefix length>"));
  }
  const std::string addr = entry_str.substr(0, slash);
  int prefix_len = 0;
  if (!absl::SimpleAtoi(entry_str.substr(slash + 1), &prefix_len)) {
    return absl::InvalidArgumentError(
        absl::StrCat("malformed CIDR '", entry, "': prefix length must be an integer"));
  }
  Cidr cidr;
  in_addr addr4;
  in6_addr addr6;
  if (inet_pton(AF_INET, addr.c_str(), &addr4) == 1) {
    if (prefix_len < 0 || prefix_len > 32) {
      return absl::InvalidArgumentError(absl::StrCat(
          "malformed CIDR '", entry, "': IPv4 prefix length must be 0..32"));
    }
    cidr.family = AF_INET;
    cidr.prefix_len = prefix_len;
    std::memcpy(cidr.bytes.data(), &addr4, 4);
    return cidr;
  }
  if (inet_pton(AF_INET6, addr.c_str(), &addr6) == 1) {
    if (prefix_len < 0 || prefix_len > 128) {
      return absl::InvalidArgumentError(absl::StrCat(
          "malformed CIDR '", entry, "': IPv6 prefix length must be 0..128"));
    }
    cidr.family = AF_INET6;
    cidr.prefix_len = prefix_len;
    std::memcpy(cidr.bytes.data(), &addr6, 16);
    return cidr;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("malformed CIDR '", entry, "': the address does not parse"));
}

// Left-aligned bit comparison over the first `prefix_len` bits.
bool BitsMatch(absl::string_view address_bytes, absl::string_view network_bytes, int prefix_len) {
  const size_t whole = prefix_len / 8;
  if (memcmp(address_bytes.data(), network_bytes.data(), whole) != 0) {
    return false;
  }
  const int remainder = prefix_len % 8;
  if (remainder == 0) {
    return true;
  }
  const uint8_t mask = static_cast<uint8_t>(0xffu << (8 - remainder));
  const size_t byte_index = whole;
  if (byte_index >= address_bytes.size()) {
    return false;
  }
  return (static_cast<uint8_t>(address_bytes[byte_index]) & mask) ==
         (static_cast<uint8_t>(network_bytes[byte_index]) & mask);
}

bool AddressMatchesCidr(const std::string& addr, int family, const Cidr& cidr) {
  if (family != cidr.family) {
    return false;
  }
  if (family == AF_INET) {
    in_addr parsed;
    if (inet_pton(AF_INET, addr.c_str(), &parsed) != 1) {
      return false;
    }
    return BitsMatch(absl::string_view(reinterpret_cast<const char*>(&parsed), 4),
                     absl::string_view(reinterpret_cast<const char*>(cidr.bytes.data()), 4),
                     cidr.prefix_len);
  }
  in6_addr parsed;
  if (inet_pton(AF_INET6, addr.c_str(), &parsed) != 1) {
    return false;
  }
  return BitsMatch(absl::string_view(reinterpret_cast<const char*>(&parsed), 16),
                   absl::string_view(reinterpret_cast<const char*>(cidr.bytes.data()), 16),
                   cidr.prefix_len);
}

bool IsLoopbackAddress(const std::string& addr, int family) {
  if (family == AF_INET) {
    in_addr parsed;
    if (inet_pton(AF_INET, addr.c_str(), &parsed) != 1) {
      return false;
    }
    return (ntohl(parsed.s_addr) >> 24) == 127;
  }
  in6_addr parsed;
  if (inet_pton(AF_INET6, addr.c_str(), &parsed) != 1) {
    return false;
  }
  return IN6_IS_ADDR_LOOPBACK(&parsed);
}

// Normalizes one numeric address: IPv4-mapped IPv6 (::ffff:a.b.c.d) collapses
// to its IPv4 form (a mapped public address must not pass as "some IPv6
// address", and a mapped loopback is loopback either way). Returns the
// normalized text and the family.
std::string NormalizeAddress(const std::string& addr, int* family) {
  in_addr addr4;
  in6_addr addr6;
  if (inet_pton(AF_INET, addr.c_str(), &addr4) == 1) {
    *family = AF_INET;
    return addr;
  }
  if (inet_pton(AF_INET6, addr.c_str(), &addr6) == 1) {
    static constexpr uint8_t kV4MappedPrefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (std::memcmp(addr6.s6_addr, kV4MappedPrefix, 12) == 0) {
      char buf[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, addr6.s6_addr + 12, buf, sizeof(buf));
      *family = AF_INET;
      return buf;
    }
    *family = AF_INET6;
    return addr;
  }
  *family = AF_UNSPEC;
  return addr;
}

// The one canonical target-uri parser (Global Constraints: ONE parser, so
// userinfo tricks and bracket forms cannot diverge between checks).
struct ParsedTarget {
  std::string scheme;
  std::string userinfo; // "" when absent; never inspected, kept for the rewrite
  std::string host;     // lowercase, brackets and zone stripped
  std::string port;     // "" when absent
  std::string path_query;
};

absl::StatusOr<ParsedTarget> ParseTargetUri(absl::string_view uri) {
  const size_t scheme_end = uri.find("://");
  if (scheme_end == absl::string_view::npos || scheme_end == 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "invalid target uri '", uri, "': expected <scheme>://<host>[:<port>]/<path>"));
  }
  ParsedTarget target;
  target.scheme = std::string(uri.substr(0, scheme_end));
  const absl::string_view rest = uri.substr(scheme_end + 3);
  const size_t authority_end = rest.find_first_of("/?#");
  const absl::string_view authority =
      authority_end == absl::string_view::npos ? rest : rest.substr(0, authority_end);
  target.path_query =
      authority_end == absl::string_view::npos ? "" : std::string(rest.substr(authority_end));

  absl::string_view hostport = authority;
  const size_t at = authority.rfind('@');
  if (at != absl::string_view::npos) {
    // Userinfo precedes the LAST '@': the host is what follows it, never the
    // text before it (the userinfo trick).
    target.userinfo = std::string(authority.substr(0, at));
    hostport = authority.substr(at + 1);
  }
  if (hostport.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid target uri '", uri, "': empty host"));
  }
  if (hostport[0] == '[') {
    const size_t close = hostport.find(']');
    if (close == absl::string_view::npos) {
      return absl::InvalidArgumentError(
          absl::StrCat("invalid target uri '", uri, "': unterminated IPv6 literal"));
    }
    target.host = std::string(hostport.substr(1, close - 1));
    const absl::string_view after = hostport.substr(close + 1);
    if (!after.empty()) {
      if (after[0] != ':') {
        return absl::InvalidArgumentError(
            absl::StrCat("invalid target uri '", uri, "': text after the IPv6 literal"));
      }
      target.port = std::string(after.substr(1));
    }
  } else {
    const size_t colon = hostport.rfind(':');
    if (colon != absl::string_view::npos) {
      target.host = std::string(hostport.substr(0, colon));
      target.port = std::string(hostport.substr(colon + 1));
    } else {
      target.host = std::string(hostport);
    }
  }
  target.host = absl::AsciiStrToLower(target.host);
  // Zone id (fe80::1%en0): stripped — the zone is link-local scoping this
  // server has no business following.
  const size_t zone = target.host.find('%');
  if (zone != std::string::npos) {
    target.host = target.host.substr(0, zone);
  }
  if (target.host.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid target uri '", uri, "': empty host"));
  }
  return target;
}

// The default resolver: getaddrinfo, ALL addresses, numeric output.
absl::StatusOr<std::vector<std::string>> DefaultResolveHost(absl::string_view host) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  const int rc = ::getaddrinfo(std::string(host).c_str(), nullptr, &hints, &result);
  if (rc != 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "getaddrinfo(", host, ") failed: ", ::gai_strerror(rc)));
  }
  std::vector<std::string> addresses;
  for (const addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    char buf[INET6_ADDRSTRLEN];
    if (::getnameinfo(ai->ai_addr, ai->ai_addrlen, buf, sizeof(buf), nullptr, 0,
                      NI_NUMERICHOST) == 0) {
      addresses.push_back(buf);
    }
  }
  ::freeaddrinfo(result);
  return addresses;
}

constexpr char kStartRunSchema[] = R"json(
    {"type": "object",
     "properties": {
       "uri": {"type": "string",
               "description": "The target uri (scheme://host[:port]/path). Resolved and checked against the allowed targets (loopback, widened by --allow-target-cidr) before any engine connection; the engine target is rewritten to the resolved IP."},
       "rps": {"type": "integer",
               "description": "The declared global total requests per second. Checked against the load ceiling (--max-rps)."},
       "phases": {"type": "string",
                  "description": "Phase run: comma-separated name:rps:duration[:concurrency[:connections]] entries (e.g. 'warm:100:5:2,spike:400:5:4:64'), replacing duration_s. Each phase rps is a declared global total under the ceiling."},
       "connections": {"type": "integer",
                       "description": "The connection count (HTTP/1)."},
       "duration_s": {"type": "integer",
                      "description": "Run duration in seconds (legacy single-phase run; mutually exclusive with phases)."}},
     "required": ["uri"],
     "additionalProperties": false})json";

constexpr char kStartRunDescription[] =
    "Starts a benchmark run. Args: uri (string, required), rps (integer, "
    "optional; the DECLARED GLOBAL total per second), phases (string, "
    "optional; 'name:rps:duration[:concurrency[:connections]]' entries "
    "replacing duration_s), connections (integer, optional), duration_s "
    "(integer, optional). The target host is resolved and EVERY resolved "
    "address is checked against loopback and --allow-target-cidr BEFORE any "
    "engine connection: an off-box target is refused with the scoping "
    "message, and an allowed target is rewritten to the resolved IP so the "
    "engine cannot re-resolve elsewhere. Redirects are refused for "
    "non-allowlisted targets. A declared total above the --max-rps ceiling "
    "is refused naming the cap. Errors arrive as isError:true with the "
    "reason verbatim (scoping message, ceiling, validator text, engine "
    "rejection). Returns {run_started, session, resolved_target}.";

constexpr char kSteerSchema[] = R"json(
    {"type": "object",
     "properties": {
       "rps": {"type": "integer",
               "description": "The new GLOBAL total rps (the engine divides it across the active workers at fan-out). Checked against the ceiling."},
       "ramp_time_s": {"type": "integer",
                       "description": "New total ramp time in seconds (ramp-plugin runs)."}},
     "additionalProperties": false})json";

constexpr char kSteerDescription[] =
    "Steers the in-flight run. Args: rps (integer, optional; the new GLOBAL "
    "total per second), ramp_time_s (integer, optional; ramp-plugin runs). "
    "At least one is required. The declared total is checked against the "
    "--max-rps ceiling first. One steer in flight at a time: a second steer "
    "while one is pending reaches the engine and is rejected by its "
    "one-in-flight guard (surfaced verbatim, never queued). Waits up to "
    "2x the stats flush interval + 2s (default 4s) for the engine's apply "
    "ack; the ack is the result ({applied, epoch_id, wall_time_unix_ns, "
    "monotonic_ns}); a rejection or the timeout arrives as isError:true with "
    "the reason verbatim ('timed out after <X>s: <what>'). Results match "
    "sends in order.";

constexpr char kCancelRunSchema[] = R"json(
    {"type": "object",
     "properties": {},
     "required": [],
     "additionalProperties": false})json";

constexpr char kCancelRunDescription[] =
    "Cancels the in-flight run with the engine's explicit cancellation arm "
    "(the stream stays alive, so the run's final output still arrives). "
    "Waits up to --timeout + 5s grace (default 35s) for the final output, "
    "then seals the session; expiry arrives as isError:true with 'timed out "
    "after <X>s: <what>'. Returns {cancelled, session}.";

constexpr char kGetStatusSchema[] = R"json(
    {"type": "object",
     "properties": {},
     "required": [],
     "additionalProperties": false})json";

constexpr char kGetStatusDescription[] =
    "Reads the run's structured status strip: run state (in_flight, "
    "completed, session path, resolved_target), the current phase id and "
    "knob epoch, the pending steering state, the phase boundaries (drain, "
    "idle gap, censoring), and the latest counters. The same object the "
    "nighthawk://status resource renders. No args.";

} // namespace

absl::Status ValidateCidrs(const std::vector<std::string>& cidrs) {
  for (const std::string& entry : cidrs) {
    auto parsed = ParseCidr(entry);
    if (!parsed.ok()) {
      return parsed.status();
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> SteeringController::ScopeAndRewriteTarget(absl::string_view uri) {
  auto target = ParseTargetUri(uri);
  if (!target.ok()) {
    return target.status();
  }

  std::vector<Cidr> cidrs;
  cidrs.reserve(options_.allow_target_cidrs.size());
  for (const std::string& entry : options_.allow_target_cidrs) {
    auto parsed = ParseCidr(entry);
    if (!parsed.ok()) {
      return parsed.status();
    }
    cidrs.push_back(*std::move(parsed));
  }

  // Resolve: an IP literal is its own address; anything else goes through
  // the one resolver (getaddrinfo by default) and EVERY address is checked.
  std::vector<std::string> addresses;
  in_addr addr4;
  in6_addr addr6;
  const bool literal_v4 = ::inet_pton(AF_INET, target->host.c_str(), &addr4) == 1;
  const bool literal_v6 = ::inet_pton(AF_INET6, target->host.c_str(), &addr6) == 1;
  if (literal_v4 || literal_v6) {
    int family = AF_UNSPEC;
    addresses.push_back(NormalizeAddress(target->host, &family));
  } else {
    auto resolved = (resolver_ ? resolver_ : DefaultResolveHost)(target->host);
    if (!resolved.ok()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "target '", uri, "' cannot be resolved: ", resolved.status().message(),
          "; refusing before any engine connection"));
    }
    for (std::string& addr : *resolved) {
      int family = AF_UNSPEC;
      addresses.push_back(NormalizeAddress(addr, &family));
    }
  }
  if (addresses.empty()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "target '", uri, "' resolved to no addresses; refusing before any engine connection"));
  }

  for (const std::string& addr : addresses) {
    int family = AF_UNSPEC;
    NormalizeAddress(addr, &family);
    bool allowed = IsLoopbackAddress(addr, family);
    for (const Cidr& cidr : cidrs) {
      if (AddressMatchesCidr(addr, family, cidr)) {
        allowed = true;
        break;
      }
    }
    if (!allowed) {
      return absl::InvalidArgumentError(absl::StrCat(
          "target '", uri, "' resolves to ", addr,
          ", which is outside the allowed targets (loopback only; widened by "
          "--allow-target-cidr); refusing before any engine connection"));
    }
  }

  // Rewrite the engine target to the FIRST resolved IP (all of them passed
  // the check): the engine cannot re-resolve elsewhere.
  int chosen_family = AF_UNSPEC;
  const std::string chosen = NormalizeAddress(addresses.front(), &chosen_family);
  const std::string host_text = chosen_family == AF_INET6 ? absl::StrCat("[", chosen, "]") : chosen;
  return absl::StrCat(target->scheme, "://",
                      target->userinfo.empty() ? "" : target->userinfo + "@", host_text,
                      target->port.empty() ? "" : ":" + target->port, target->path_query);
}

SteeringController::SteeringController(const std::shared_ptr<grpc::ChannelInterface>& channel,
                                       SteeringToolOptions options, HostResolver resolver)
    : options_(std::move(options)),
      resolver_(std::move(resolver)),
      feed_(channel, Callbacks()) {}

Tui::RunFeedCallbacks SteeringController::Callbacks() {
  // The callbacks capture `this`; feed_ is the LAST-initialized member, so
  // by the time a callback can fire every other member is alive (and in the
  // destructor feed_ is destroyed first, so none outlives them).
  Tui::RunFeedCallbacks callbacks;
  callbacks.on_snapshot = [this](const nighthawk::client::LiveSnapshot& snapshot) {
    OnSnapshot(snapshot);
  };
  callbacks.on_phase_event = [this](const nighthawk::client::PhaseBoundaryEvent& event) {
    OnPhaseEvent(event);
  };
  callbacks.on_completed = [this](const nighthawk::client::Output& output) {
    OnCompleted(output);
  };
  callbacks.on_update_result = [this](const nighthawk::client::UpdateResult& result) {
    OnUpdateResult(result);
  };
  callbacks.on_finished = [this](const absl::Status& status) { OnFinished(status); };
  return callbacks;
}

SteeringController::~SteeringController() {
  // feed_ is declared last and destroyed first: its destructor cancels any
  // in-flight run and joins the read thread, whose final callbacks find
  // every other member alive. An open session drains its queued records.
}

void SteeringController::OnSnapshot(const nighthawk::client::LiveSnapshot& snapshot) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    last_snapshot_ = snapshot;
    has_snapshot_ = true;
    // The series resource reads the records the session persists — built
    // once here, kept fresh in memory (the file's sealed prefix lags by the
    // writer's buffered frames) and persisted unchanged.
    nighthawk::client::SessionRecord record;
    *record.mutable_metric_series() = Tui::ToMetricSeries(snapshot);
    series_records_.push_back(record.metric_series());
    if (recorder_.open()) {
      recorder_.AppendSnapshot(std::move(record));
    }
  }
  cv_.notify_all();
}

void SteeringController::OnPhaseEvent(const nighthawk::client::PhaseBoundaryEvent& event) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    boundary_events_.push_back(event);
    if (recorder_.open()) {
      nighthawk::client::SessionRecord record;
      *record.mutable_phase_boundary() = Tui::ToPhaseBoundaryRecord(event);
      recorder_.AppendSnapshot(std::move(record));
    }
  }
  cv_.notify_all();
}

void SteeringController::OnCompleted(const nighthawk::client::Output& output) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    final_output_ = output;
    completed_ = true;
  }
  cv_.notify_all();
}

void SteeringController::OnUpdateResult(const nighthawk::client::UpdateResult& result) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    update_results_.push_back(QueuedResult{++next_seq_, result});
    // The session's knob-epoch record: engine verdicts only — the
    // feed-generated pending surfacing ("run-feed: " prefix) is not one
    // (the TUI pattern). Built once and kept for the timeline resource
    // (WithEpochMarks keeps the truth: only an applied ack marks).
    if (!absl::StartsWith(result.rejection_reason(), "run-feed: ")) {
      nighthawk::client::SessionRecord record;
      *record.mutable_knob_epoch() =
          Tui::ToKnobEpochRecord(result, pending_phase_id_, pending_rps_, pending_ramp_bent_);
      knob_epochs_.push_back(record.knob_epoch());
      if (recorder_.open()) {
        recorder_.AppendSnapshot(std::move(record));
      }
    }
    pending_rps_ = 0;
    pending_phase_id_.clear();
  }
  cv_.notify_all();
}

void SteeringController::OnFinished(const absl::Status& /*status*/) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = true;
    ++finished_count_;
  }
  cv_.notify_all();
}

absl::StatusOr<google::protobuf::Struct>
SteeringController::StartRun(const google::protobuf::Struct& arguments) {
  // 1. The scope check binds at CONNECT: resolve, check all, rewrite —
  //    before anything else touches the engine.
  const Value* uri = FindField(arguments, "uri");
  auto rewritten = ScopeAndRewriteTarget(uri->string_value());
  if (!rewritten.ok()) {
    return rewritten.status();
  }

  // 2. The load ceiling: the declared global total at start_run.
  const Value* rps = FindField(arguments, "rps");
  if (rps != nullptr) {
    const int64_t declared = IntegerOf(*rps);
    if (declared > options_.max_rps) {
      return absl::InvalidArgumentError(absl::StrCat(
          "declared total rps ", declared, " exceeds the load ceiling of ",
          options_.max_rps, " rps (--max-rps)"));
    }
  }
  const Value* phases = FindField(arguments, "phases");
  if (phases != nullptr) {
    auto parsed = Tui::StartRunDialogModel::ParsePhasesField(phases->string_value());
    if (parsed.ok()) {
      for (const nighthawk::client::PhaseSpec& phase : *parsed) {
        if (phase.requests_per_second() > options_.max_rps) {
          return absl::InvalidArgumentError(absl::StrCat(
              "phase '", phase.name(), "' declares total rps ",
              phase.requests_per_second(), " which exceeds the load ceiling of ",
              options_.max_rps, " rps (--max-rps)"));
        }
      }
    }
    // A malformed phases string falls through to the model's verbatim
    // validator error below.
  }

  // 3. The model validation, verbatim (the dialog's own field errors).
  Tui::StartRunDialogModel dialog;
  dialog.set_uri(uri->string_value());
  if (rps != nullptr) {
    dialog.set_rps(static_cast<uint32_t>(IntegerOf(*rps)));
  }
  if (phases != nullptr) {
    dialog.set_phases_field(phases->string_value());
  }
  if (const Value* connections = FindField(arguments, "connections"); connections != nullptr) {
    dialog.set_connections(static_cast<uint32_t>(IntegerOf(*connections)));
  }
  if (const Value* duration = FindField(arguments, "duration_s"); duration != nullptr) {
    dialog.set_duration_seconds(static_cast<uint32_t>(IntegerOf(*duration)));
  }
  for (int field : {Tui::StartRunDialogModel::kUri, Tui::StartRunDialogModel::kRps,
                    Tui::StartRunDialogModel::kDuration, Tui::StartRunDialogModel::kPhases}) {
    const std::string error = Tui::StartRunDialogModel::FieldError(dialog, field);
    if (!error.empty()) {
      return absl::InvalidArgumentError(error);
    }
  }

  nighthawk::client::StartRequest request = dialog.build_request();
  request.mutable_options()->mutable_uri()->set_value(*rewritten);

  // The session file pair opens BEFORE the run starts (the TUI's ordering:
  // once feed_.start() returns, snapshots can arrive on the read thread).
  const int64_t started_unix_ns = absl::ToUnixNanos(absl::Now());
  auto session = recorder_.StartRun(options_.session_prefix, started_unix_ns,
                                    Tui::ToSessionHeader(request, /*session_id=*/"",
                                                         started_unix_ns));
  if (!session.ok()) {
    return absl::InternalError(session.status().message());
  }

  const absl::Status started = feed_.start(request);
  if (!started.ok()) {
    // No run launched; seal the header-only session (the TUI's honesty
    // rule), then surface the start failure verbatim.
    SealSession(/*completed=*/false);
    return absl::InvalidArgumentError(std::string(started.message()));
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = false;
    completed_ = false;
    has_snapshot_ = false;
    last_snapshot_ = {};
    boundary_events_.clear();
    series_records_.clear();
    knob_epochs_.clear();
    update_results_.clear();
    final_output_ = {};
    start_request_ = request;
    resolved_target_ = *rewritten;
  }
  Struct result;
  SetBool(result, "run_started", true);
  SetString(result, "session", recorder_.path());
  SetString(result, "resolved_target", *rewritten);
  return result;
}

absl::StatusOr<google::protobuf::Struct>
SteeringController::Steer(const google::protobuf::Struct& arguments) {
  const Value* rps = FindField(arguments, "rps");
  const Value* ramp_time_s = FindField(arguments, "ramp_time_s");
  if (rps == nullptr && ramp_time_s == nullptr) {
    return absl::InvalidArgumentError(
        "steer requires at least one of 'rps', 'ramp_time_s'");
  }
  if (rps != nullptr) {
    const int64_t declared = IntegerOf(*rps);
    if (declared > options_.max_rps) {
      return absl::InvalidArgumentError(absl::StrCat(
          "declared total rps ", declared, " exceeds the load ceiling of ",
          options_.max_rps, " rps (--max-rps)"));
    }
  }

  nighthawk::client::UpdateRequest request;
  if (rps != nullptr) {
    request.add_deltas()->mutable_requests_per_second()->set_value(
        static_cast<uint32_t>(IntegerOf(*rps)));
  }
  if (ramp_time_s != nullptr) {
    request.add_deltas()->mutable_ramp()->mutable_ramp_time()->set_seconds(
        IntegerOf(*ramp_time_s));
  }

  int64_t bound_s = 4;
  int64_t my_seq = 0;
  bool overlap = false;
  {
    // Send-time context for the session's knob-epoch record; the flush
    // interval (the wait bound's input) copied under the same lock. The
    // waiter registers here too, in send order, so arriving results
    // correlate deterministically (below).
    std::lock_guard<std::mutex> lock(mutex_);
    pending_rps_ = rps != nullptr ? static_cast<uint32_t>(IntegerOf(*rps)) : 0;
    pending_phase_id_ = last_snapshot_.phase_id();
    pending_ramp_bent_ = Tui::RpsTurnBendsRamp(start_request_.options());
    const int64_t flush_s = start_request_.options().has_stats_flush_interval()
                                ? start_request_.options().stats_flush_interval().value()
                                : 1;
    bound_s = 2 * flush_s + 2;
    my_seq = ++next_seq_;
    overlap = !steer_waiters_.empty();
    steer_waiters_.push_back(SteerWaiter{my_seq, overlap});
  }
  // No client-side one-in-flight refusal: a second steer while one is
  // pending reaches the engine and is rejected by its one-in-flight guard
  // (surfaced verbatim below, never queued) — the Global Constraints'
  // posture. The feed's own write errors surface verbatim too.
  const absl::Status sent = feed_.SendUpdate(request);
  if (!sent.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    UnregisterSteerWaiter(my_seq);
    pending_rps_ = 0;
    pending_phase_id_.clear();
    cv_.notify_all();
    return absl::InvalidArgumentError(std::string(sent.message()));
  }

  // The bounded wait: 2x stats_flush_interval + 2s (default 4s), OFF the
  // dispatch thread (the tool is deferred).
  //
  // Result-to-waiter correlation, deterministic under the engine's actual
  // delivery discipline: an arm sent while another was in flight is
  // REJECTED by the one-in-flight guard immediately (in send order), while
  // the oldest arm's applied ack is bound to its applier and lands after
  // them. So: an overlap waiter consumes the oldest queued result once it
  // is the oldest waiter still expecting a rejection; a non-overlap waiter
  // — which can only be the oldest arm — waits until every overlap waiter
  // is served, then takes the oldest result that arrived after its send
  // (a stale result queued before its send — a timed-out steer's late ack —
  // is never misattributed).
  const absl::Time deadline = absl::Now() + absl::Seconds(bound_s);
  nighthawk::client::UpdateResult result;
  bool timed_out = false;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto my_turn = [&] {
      if (overlap) {
        for (const SteerWaiter& waiter : steer_waiters_) {
          if (waiter.overlap) {
            return waiter.seq == my_seq && !update_results_.empty();
          }
        }
        return false;
      }
      for (const SteerWaiter& waiter : steer_waiters_) {
        if (waiter.overlap) {
          return false;
        }
      }
      return !update_results_.empty() && update_results_.front().seq > my_seq;
    };
    if (!cv_.wait_until(lock, absl::ToChronoTime(deadline), my_turn)) {
      timed_out = true;
    } else {
      result = update_results_.front().result;
      update_results_.pop_front();
    }
    UnregisterSteerWaiter(my_seq);
    cv_.notify_all();
  }
  if (timed_out) {
    return absl::InvalidArgumentError(
        absl::StrCat("timed out after ", bound_s, "s: the run's update ack"));
  }
  if (!result.applied()) {
    // The engine's (or the feed's) structured rejection, verbatim.
    return absl::InvalidArgumentError(result.rejection_reason());
  }
  Struct out;
  SetBool(out, "applied", true);
  SetString(out, "epoch_id", result.epoch_id());
  SetInt(out, "wall_time_unix_ns", result.wall_time_unix_ns());
  SetInt(out, "monotonic_ns", result.monotonic_ns());
  return out;
}

void SteeringController::UnregisterSteerWaiter(int64_t seq) {
  // mutex_ held by the caller.
  for (auto it = steer_waiters_.begin(); it != steer_waiters_.end(); ++it) {
    if (it->seq == seq) {
      steer_waiters_.erase(it);
      return;
    }
  }
}

absl::StatusOr<google::protobuf::Struct>
SteeringController::CancelRun(const google::protobuf::Struct& /*arguments*/) {
  int baseline_finished = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    baseline_finished = finished_count_;
  }
  // The explicit arm keeps the stream alive: the run's final output still
  // arrives (unlike a context cancel). Refusals (no run in flight) surface
  // verbatim.
  const absl::Status sent = feed_.SendCancellation();
  if (!sent.ok()) {
    return absl::InvalidArgumentError(std::string(sent.message()));
  }

  // The bounded wait: --timeout + 5s grace for the final output.
  const long long bound_s = options_.timeout_s + 5;
  const absl::Time deadline = absl::Now() + absl::Seconds(bound_s);
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!cv_.wait_until(lock, absl::ToChronoTime(deadline),
                        [&] { return finished_count_ > baseline_finished; })) {
      return absl::InvalidArgumentError(absl::StrCat(
          "timed out after ", bound_s, "s: the run's final output after the cancellation"));
    }
  }
  SealSession(/*completed=*/completed_);
  Struct out;
  SetBool(out, "cancelled", true);
  SetString(out, "session", recorder_.path());
  return out;
}

absl::StatusOr<google::protobuf::Struct>
SteeringController::GetStatus(const google::protobuf::Struct& /*arguments*/) {
  return BuildStatus();
}

google::protobuf::Struct SteeringController::BuildStatus() {
  Struct status;
  std::vector<nighthawk::client::PhaseBoundaryEvent> boundaries;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    Struct run;
    SetBool(run, "in_flight", !finished_);
    SetBool(run, "completed", completed_);
    SetString(run, "session", recorder_.path());
    SetString(run, "resolved_target", resolved_target_);
    (*status.mutable_fields())["run"] = ToValue(run);

    Struct current;
    SetString(current, "phase_id", last_snapshot_.phase_id());
    SetString(current, "epoch_id", last_snapshot_.epoch_id());
    (*status.mutable_fields())["current"] = ToValue(current);

    Struct pending;
    SetBool(pending, "update",
            !update_results_.empty() || !steer_waiters_.empty() || pending_rps_ > 0);
    (*status.mutable_fields())["pending"] = ToValue(pending);

    boundaries = boundary_events_;

    Struct counters;
    for (const auto& [name, value] : last_snapshot_.counters()) {
      SetInt(counters, name, static_cast<int64_t>(value));
    }
    (*status.mutable_fields())["counters"] = ToValue(counters);
  }
  // Force the list kind even for an empty vector: phases answers [], never
  // null (the same empty-list discipline as tools/list).
  Value& phases = (*status.mutable_fields())["phases"];
  phases.mutable_list_value();
  for (const nighthawk::client::PhaseBoundaryEvent& event : boundaries) {
    Struct phase;
    SetInt(phase, "starting_phase_index", event.phase_index());
    SetString(phase, "starting_phase_id", event.phase_id());
    SetInt(phase, "ending_phase_index", event.ending_phase_index());
    SetString(phase, "ending_phase_id", event.ending_phase_id());
    SetDouble(phase, "drain_seconds",
              event.drain_duration().seconds() + event.drain_duration().nanos() / 1e9);
    SetDouble(phase, "idle_gap_seconds",
              event.idle_gap().seconds() + event.idle_gap().nanos() / 1e9);
    SetInt(phase, "worker_count", event.worker_count());
    SetBool(phase, "drain_censored", event.drain_censored());
    SetBool(phase, "next_phase_cold_started", event.next_phase_cold_started());
    *phases.mutable_list_value()->add_values() = ToValue(phase);
  }
  return status;
}

google::protobuf::Struct SteeringController::BuildSeries() {
  std::string session_path;
  std::vector<nighthawk::client::MetricSeriesRecord> records;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_path = recorder_.path();
    records = series_records_;
  }
  return RenderSeriesPayload(session_path, records);
}

google::protobuf::Struct SteeringController::BuildTimeline() {
  nighthawk::client::CommandLineOptions options;
  std::vector<nighthawk::client::PhaseBoundaryEvent> events;
  std::vector<nighthawk::client::PhaseMetadata> metadata;
  std::vector<nighthawk::client::KnobEpochRecord> epochs;
  std::string current_phase;
  std::string session_path;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    options = start_request_.options();
    events = boundary_events_;
    // The post-run source (the Output's phase metadata, complete cells);
    // empty until the run finished, which selects the streamed-event path —
    // BuildPhaseTimeline's source rule.
    metadata.assign(final_output_.phase_metadata().begin(), final_output_.phase_metadata().end());
    epochs = knob_epochs_;
    current_phase = last_snapshot_.phase_id();
    session_path = recorder_.path();
  }
  return RenderTimelinePayload(session_path, options, events, metadata, current_phase, epochs);
}

google::protobuf::Struct SteeringController::BuildSession() {
  const std::string path = recorder_.path();
  auto prefix = recorder_.ReadSealedPrefix();
  if (!prefix.ok()) {
    // No session yet (before the first start_run): an honest empty shape,
    // not a protocol fault — a resource read is not a client error. The
    // reader's reason travels as a note, not as a tail disclosure (nothing
    // is torn; there is no file).
    Struct out;
    Struct provenance;
    SetString(provenance, "session", path);
    SetString(provenance, "source", "the session file's sealed prefix (flushed frames only)");
    (*out.mutable_fields())["provenance"] = ToValue(provenance);
    SetBool(out, "tail_skipped", false);
    SetString(out, "note", prefix.status().message());
    (*out.mutable_fields())["records"].mutable_list_value();
    return out;
  }
  return RenderSessionPayload(path, *prefix);
}

void SteeringController::SealSession(bool completed) {
  if (!recorder_.open()) {
    return;
  }
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(completed);
  run_end.set_ended_unix_ns(absl::ToUnixNanos(absl::Now()));
  (void)recorder_.FlushAndSeal(run_end);
}

void SteeringController::GracefulStop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!finished_) {
      feed_.stop();
      // Bounded: the shutdown path must not hang on a wedged stream.
      cv_.wait_for(lock, std::chrono::seconds(5), [&] { return finished_; });
    }
  }
  SealSession(/*completed=*/completed_);
}

void RegisterSteeringTools(ToolRegistry& registry, SteeringController& controller) {
  ToolSpec start_run;
  start_run.name = "start_run";
  start_run.description = kStartRunDescription;
  start_run.input_schema = ParseJsonObject(kStartRunSchema).value();
  start_run.handler = [&controller](const Struct& arguments) {
    return controller.StartRun(arguments);
  };
  registry.Register(std::move(start_run));

  ToolSpec steer;
  steer.name = "steer";
  steer.description = kSteerDescription;
  steer.input_schema = ParseJsonObject(kSteerSchema).value();
  steer.deferred = true;
  steer.handler = [&controller](const Struct& arguments) { return controller.Steer(arguments); };
  registry.Register(std::move(steer));

  ToolSpec cancel_run;
  cancel_run.name = "cancel_run";
  cancel_run.description = kCancelRunDescription;
  cancel_run.input_schema = ParseJsonObject(kCancelRunSchema).value();
  cancel_run.deferred = true;
  cancel_run.handler = [&controller](const Struct& arguments) {
    return controller.CancelRun(arguments);
  };
  registry.Register(std::move(cancel_run));

  ToolSpec get_status;
  get_status.name = "get_status";
  get_status.description = kGetStatusDescription;
  get_status.input_schema = ParseJsonObject(kGetStatusSchema).value();
  get_status.handler = [&controller](const Struct& arguments) {
    return controller.GetStatus(arguments);
  };
  registry.Register(std::move(get_status));
}

} // namespace Client
} // namespace Nighthawk
