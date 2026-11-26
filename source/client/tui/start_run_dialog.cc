#include "source/client/tui/start_run_dialog.h"

#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"

#include "api/request_source/request_source_plugin.pb.h"

#include "source/client/options_validation_phases.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Trims surrounding whitespace so a fat-fingered submit still works, and a
// whitespace-only field lands on the invalid empty-uri path (routed from the
// Task 5 review).
void StartRunDialogModel::set_uri(std::string uri) {
  uri_ = std::string(absl::StripAsciiWhitespace(uri));
}

void StartRunDialogModel::set_rps(uint32_t rps) { rps_ = rps; }

void StartRunDialogModel::set_duration_seconds(uint32_t seconds) { duration_seconds_ = seconds; }

void StartRunDialogModel::set_connections(uint32_t connections) { connections_ = connections; }

void StartRunDialogModel::set_max_requests_per_connection(uint32_t max_requests_per_connection) {
  max_requests_per_connection_ = max_requests_per_connection;
}

void StartRunDialogModel::set_protocol(nighthawk::client::Protocol::ProtocolOptions protocol) {
  protocol_ = protocol;
}

void StartRunDialogModel::set_request_body_size(uint32_t request_body_size) {
  request_body_size_ = request_body_size;
}

void StartRunDialogModel::set_burst_size(uint32_t burst_size) { burst_size_ = burst_size; }

void StartRunDialogModel::set_open_loop(bool open_loop) { open_loop_ = open_loop; }

void StartRunDialogModel::set_sequencer_idle_strategy(
    nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions strategy) {
  sequencer_idle_strategy_ = strategy;
}

void StartRunDialogModel::set_max_concurrent_streams(uint32_t max_concurrent_streams) {
  max_concurrent_streams_ = max_concurrent_streams;
}

void StartRunDialogModel::set_request_method(envoy::config::core::v3::RequestMethod method) {
  request_method_ = method;
}

void StartRunDialogModel::set_request_mix(
    std::vector<nighthawk::client::RequestOptions> request_mix) {
  request_mix_ = std::move(request_mix);
}

void StartRunDialogModel::set_tls_context(
    envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext tls) {
  tls_context_ = std::move(tls);
}

void StartRunDialogModel::set_worker_series(bool worker_series) { worker_series_ = worker_series; }

void StartRunDialogModel::set_max_pending_requests(uint32_t max_pending_requests) {
  max_pending_requests_ = max_pending_requests;
}

void StartRunDialogModel::set_prefetch_connections(bool prefetch_connections) {
  prefetch_connections_ = prefetch_connections;
}

void StartRunDialogModel::add_request_headers(std::string name, std::string value) {
  request_headers_.emplace_back(std::move(name), std::move(value));
}

void StartRunDialogModel::set_phases(std::vector<nighthawk::client::PhaseSpec> phases) {
  phases_ = std::move(phases);
}

void StartRunDialogModel::set_phases_field(std::string text) {
  phases_field_ = std::string(absl::StripAsciiWhitespace(text));
}

absl::StatusOr<std::vector<nighthawk::client::PhaseSpec>>
StartRunDialogModel::ParsePhasesField(absl::string_view text) {
  std::vector<nighthawk::client::PhaseSpec> phases;
  for (const absl::string_view& raw_entry : absl::StrSplit(text, absl::ByAnyChar(",\n;"),
                                                           absl::SkipEmpty())) {
    const std::string entry(absl::StripAsciiWhitespace(raw_entry));
    if (entry.empty()) {
      continue;
    }
    const std::vector<std::string> parts = absl::StrSplit(entry, ':');
    if (parts.size() < 3 || parts.size() > 6) {
      return absl::InvalidArgumentError(
          absl::StrCat("phase entry \"", entry,
                       "\": expected name:rps:duration[:concurrency[:connections[:weights]]]"));
    }
    nighthawk::client::PhaseSpec phase;
    phase.set_name(parts[0]);
    uint32_t rps = 0;
    uint32_t duration = 0;
    if (!absl::SimpleAtoi(parts[1], &rps) || rps < 1) {
      return absl::InvalidArgumentError(
          absl::StrCat("phase entry \"", entry, "\": rps must be a positive integer"));
    }
    phase.set_requests_per_second(rps);
    if (!absl::SimpleAtoi(parts[2], &duration) || duration < 1) {
      return absl::InvalidArgumentError(
          absl::StrCat("phase entry \"", entry, "\": duration must be a positive integer"));
    }
    phase.mutable_duration()->set_seconds(duration);
    if (parts.size() >= 4) {
      uint32_t workers = 0;
      if (!absl::SimpleAtoi(parts[3], &workers) || workers < 1) {
        return absl::InvalidArgumentError(
            absl::StrCat("phase entry \"", entry, "\": concurrency must be a positive integer"));
      }
      phase.mutable_concurrency()->set_value(std::to_string(workers));
    }
    // NH1: with a weights part, connections may be left empty
    // ("hot:800:20:4::5/1/1/1"): the run-level value is inherited.
    if (parts.size() >= 5 && !(parts.size() == 6 && parts[4].empty())) {
      uint32_t connections = 0;
      if (!absl::SimpleAtoi(parts[4], &connections) || connections < 1) {
        return absl::InvalidArgumentError(
            absl::StrCat("phase entry \"", entry, "\": connections must be a positive integer"));
      }
      phase.mutable_connections()->set_value(connections);
    }
    // NH1: per-worker rate weights, '/'-separated, one per worker of the
    // phase. The reused phase-vector validation checks the count and the
    // whole-number split.
    if (parts.size() == 6) {
      for (const absl::string_view raw_weight : absl::StrSplit(parts[5], '/')) {
        uint32_t weight = 0;
        if (!absl::SimpleAtoi(raw_weight, &weight) || weight < 1) {
          return absl::InvalidArgumentError(absl::StrCat(
              "phase entry \"", entry,
              "\": weights must be '/'-separated positive integers (for example 3/1)"));
        }
        phase.add_worker_weights(weight);
      }
    }
    phases.push_back(std::move(phase));
  }
  if (phases.empty()) {
    return absl::InvalidArgumentError("no phase entries found");
  }
  return phases;
}

bool StartRunDialogModel::valid() const {
  if (uri_.empty() || rps_ < 1) {
    return false;
  }
  // The field must parse when non-empty — a malformed draft never starts.
  if (!phases_field_.empty() && !ParsePhasesField(phases_field_).ok()) {
    return false;
  }
  if (phases_field_.empty() && phases_.empty()) {
    return duration_seconds_ >= 1;
  }
  // Phase run: the top-level duration is replaced by the vector; the vector
  // itself must pass the reused pure validation.
  return validatePhaseVector(build_request().options()).ok();
}

nighthawk::client::StartRequest StartRunDialogModel::build_request() const {
  nighthawk::client::StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_uri()->set_value(uri_);
  options->mutable_requests_per_second()->set_value(rps_);
  // The field is the production source; the programmatic set_phases seam
  // serves tests. An unparsable field builds a legacy request (valid()
  // refuses it before a start can happen — and FieldError says why).
  std::vector<nighthawk::client::PhaseSpec> phases = phases_;
  if (!phases_field_.empty()) {
    if (auto parsed = ParsePhasesField(phases_field_); parsed.ok()) {
      phases = *std::move(parsed);
    } else {
      phases.clear();
    }
  }
  if (phases.empty()) {
    options->mutable_duration()->set_seconds(duration_seconds_);
  } else {
    // Phase run: the vector replaces the mutually-exclusive top-level
    // duration (Task 2's rules, enforced here by construction — the
    // duration field is never written alongside a vector).
    *options->mutable_phases() = {phases.begin(), phases.end()};
  }
  // The steering surface's connection count lands directly on the options
  // (the dialog has no field for it); 0 stays unset — the engine default.
  if (connections_ > 0) {
    options->mutable_connections()->set_value(connections_);
  }
  // The steering surface's churn control: 0 stays unset — the engine
  // default. The tool boundary validates the positive floor before this.
  if (max_requests_per_connection_ > 0) {
    options->mutable_max_requests_per_connection()->set_value(max_requests_per_connection_);
  }
  // N3a: the steering surface's protocol; unset stays unset — the engine
  // default (HTTP/1).
  if (protocol_.has_value()) {
    options->mutable_protocol()->set_value(*protocol_);
  }
  if (request_mix_.empty()) {
    // N3a: the steering surface's request body size, on the static
    // RequestOptions (the CLI's --request-body-size surface).
    if (request_body_size_.has_value()) {
      options->mutable_request_options()->mutable_request_body_size()->set_value(
          *request_body_size_);
    }
    // N3b: the method of the static RequestOptions; unset stays unset (the
    // engine default, GET).
    if (request_method_.has_value()) {
      options->mutable_request_options()->set_request_method(*request_method_);
    }
    // The steering surface's request headers, verbatim (the tool boundary
    // validates the entries; the model is the single builder). They live
    // under the static RequestOptions (the same surface the CLI's
    // --request-header fills).
    if (!request_headers_.empty()) {
      for (const auto& [name, value] : request_headers_) {
        auto* option = options->mutable_request_options()->add_request_headers();
        option->mutable_header()->set_key(name);
        option->mutable_header()->set_value(value);
      }
    }
  } else {
    // N3b: the request mix replaces the static RequestOptions (a oneof on the
    // wire): the in-line options-list source cycles the entries round-robin
    // per worker, with no request cap (num_requests 0).
    nighthawk::request_source::InLineOptionsListRequestSourceConfig config;
    for (const nighthawk::client::RequestOptions& entry : request_mix_) {
      *config.mutable_options_list()->add_options() = entry;
    }
    auto* plugin = options->mutable_request_source_plugin_config();
    plugin->set_name("nighthawk.in-line-options-list-request-source-plugin");
    std::ignore = plugin->mutable_typed_config()->PackFrom(config);
  }
  // N3b: the rest of the request-shape passthrough; unset stays unset.
  if (burst_size_.has_value()) {
    options->mutable_burst_size()->set_value(*burst_size_);
  }
  if (open_loop_.has_value()) {
    options->mutable_open_loop()->set_value(*open_loop_);
  }
  if (sequencer_idle_strategy_.has_value()) {
    options->mutable_sequencer_idle_strategy()->set_value(*sequencer_idle_strategy_);
  }
  if (max_concurrent_streams_.has_value()) {
    options->mutable_max_concurrent_streams()->set_value(*max_concurrent_streams_);
  }
  if (tls_context_.has_value()) {
    // The (deprecated, still honoured) tls_context field rather than a
    // transport socket: the engine keeps adding the protocol's ALPN, the SNI
    // and the QUIC wrapper for h3 around it.
    *options->mutable_tls_context() = *tls_context_;
  }
  // The run-level pending limit (an explicit 0 is written as 0) and
  // prefetching; unset stays unset.
  if (max_pending_requests_.has_value()) {
    options->mutable_max_pending_requests()->set_value(*max_pending_requests_);
  }
  if (prefetch_connections_.has_value()) {
    options->mutable_prefetch_connections()->set_value(*prefetch_connections_);
  }
  // NH1: the per-worker series opt-in; false stays unset.
  if (worker_series_) {
    options->mutable_worker_series()->set_value(true);
  }
  // Presence of live_streaming is the opt-in marker; stats_flush_interval
  // defaults to 1s (the proto bounds it to [1s, 300s]).
  options->mutable_live_streaming();
  options->mutable_stats_flush_interval()->set_value(1);
  return request;
}

std::string StartRunDialogModel::FieldError(const StartRunDialogModel& dialog, int field) {
  switch (field) {
  case kUri:
    return dialog.uri_.empty() ? "uri: must not be empty" : "";
  case kRps:
    return dialog.rps_ >= 1 ? "" : "rps: must be > 0";
  case kDuration:
    if (!dialog.phases_field_.empty() || !dialog.phases_.empty()) {
      // The vector replaces this field: no duration requirement at all.
      return "";
    }
    return dialog.duration_seconds_ >= 1 ? "" : "duration (s): must be > 0";
  case kPhases: {
    // The effective vector: the parsed field when non-empty, else the
    // programmatic seam's vector.
    std::vector<nighthawk::client::PhaseSpec> phases;
    if (!dialog.phases_field_.empty()) {
      auto parsed = ParsePhasesField(dialog.phases_field_);
      if (!parsed.ok()) {
        return absl::StrCat("phases: ", parsed.status().message());
      }
      phases = *std::move(parsed);
    } else {
      phases = dialog.phases_;
    }
    if (phases.empty()) {
      return "";
    }
    // Parsed: the reused validation names the offending phase when it
    // rejects the vector.
    const absl::Status validated = validatePhaseVector([&] {
      nighthawk::client::CommandLineOptions options;
      options.mutable_uri()->set_value(dialog.uri_);
      *options.mutable_phases() = {phases.begin(), phases.end()};
      return options;
    }());
    return validated.ok() ? "" : std::string(validated.message());
  }
  default:
    return "";
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
