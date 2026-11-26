#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "envoy/extensions/transport_sockets/tls/v3/tls.pb.h"

#include "api/client/options.pb.h"
#include "api/client/service.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): pure state machine behind the `n` start-run dialog.
// Holds the three editable fields, validates them, and builds the
// StartRequest. The FTXUI binding is deliberately thin and lives in Task 6's
// app — this model never touches a terminal, so it stays headless-testable.
class StartRunDialogModel {
public:
  void set_uri(std::string uri);
  void set_rps(uint32_t rps);
  void set_duration_seconds(uint32_t seconds);

  // MCP-plan Task 2: the connection-count field, set DIRECTLY on the built
  // CommandLineOptions (the interactive dialog has no field for it; the
  // steering surface does). 0 (the default) leaves the options' connections
  // unset — the engine's default applies. The proto pins the valid range
  // (gte 1, lte 1000000); no dialog-side validation is added here (pure
  // passthrough, tested).
  void set_connections(uint32_t connections);
  uint32_t connections() const { return connections_; }

  // Drive-backlog item 2 (the steering surface's request-shape passthrough).
  // The connection-churn control: build_request() writes it when > 0 (0 =
  // unset, the engine default — the connections pattern); the tool boundary
  // owns the positive-integer floor.
  void set_max_requests_per_connection(uint32_t max_requests_per_connection);
  uint32_t max_requests_per_connection() const { return max_requests_per_connection_; }

  // N3a (the steering surface's protocol and body-size passthrough). The
  // protocol build_request() writes; unset (the default) leaves the options'
  // protocol unset — the engine default (HTTP/1). The tool boundary maps its
  // h1/h2/h3 argument onto this.
  void set_protocol(nighthawk::client::Protocol::ProtocolOptions protocol);
  const std::optional<nighthawk::client::Protocol::ProtocolOptions>& protocol() const {
    return protocol_;
  }

  // The request body size in bytes, written onto the static RequestOptions
  // (the surface the CLI's --request-body-size fills). Unset leaves it
  // unset; an explicit 0 is written as 0 (no body). The tool boundary owns
  // the range floor (the proto pins lte 4194304).
  void set_request_body_size(uint32_t request_body_size);
  const std::optional<uint32_t>& request_body_size() const { return request_body_size_; }

  // The request headers to set on every request, as (name, value) pairs.
  // build_request() appends them verbatim as HeaderValueOptions; the tool
  // boundary owns entry validation (the model stays a pure builder).
  void add_request_headers(std::string name, std::string value);
  const std::vector<std::pair<std::string, std::string>>& request_headers() const {
    return request_headers_;
  }

  // N3b / NH1: the rest of the steering surface's request-shape
  // passthrough. Each is written by build_request() only when set; unset
  // leaves the engine default. The tool boundary owns every range check and
  // every refusal (the model stays a pure builder).
  // - burst size (requests released together per burst);
  void set_burst_size(uint32_t burst_size);
  const std::optional<uint32_t>& burst_size() const { return burst_size_; }
  // - open loop (no backpressure when the client-side limits are hit);
  void set_open_loop(bool open_loop);
  const std::optional<bool>& open_loop() const { return open_loop_; }
  // - the sequencer idle strategy (spin, poll or sleep);
  void set_sequencer_idle_strategy(
      nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions strategy);
  const std::optional<nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions>&
  sequencer_idle_strategy() const {
    return sequencer_idle_strategy_;
  }
  // - the per-connection stream limit (HTTP/2 and HTTP/3);
  void set_max_concurrent_streams(uint32_t max_concurrent_streams);
  const std::optional<uint32_t>& max_concurrent_streams() const { return max_concurrent_streams_; }
  // - the request method of the static RequestOptions;
  void set_request_method(envoy::config::core::v3::RequestMethod method);
  const std::optional<envoy::config::core::v3::RequestMethod>& request_method() const {
    return request_method_;
  }
  // - a request mix: the entries are cycled round-robin per worker through
  //   the in-line options-list request source. Non-empty replaces the static
  //   RequestOptions (headers, method and body size): the two are a oneof on
  //   the wire, so build_request() writes the mix and nothing static;
  void set_request_mix(std::vector<nighthawk::client::RequestOptions> request_mix);
  const std::vector<nighthawk::client::RequestOptions>& request_mix() const { return request_mix_; }
  // - TLS parameters for an https target (the engine still adds the ALPN
  //   and the SNI for the protocol);
  void set_tls_context(envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext tls);
  const std::optional<envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext>&
  tls_context() const {
    return tls_context_;
  }
  // - the run-level pending-request limit (the cluster's pending breaker,
  //   bootstrapped at the maximum over the phases; a phase that does not set
  //   its own max_pending_requests inherits it);
  void set_max_pending_requests(uint32_t max_pending_requests);
  const std::optional<uint32_t>& max_pending_requests() const { return max_pending_requests_; }
  // - HTTP/1 connection prefetching (each worker's pool opens its whole
  //   connection set on its first request);
  void set_prefetch_connections(bool prefetch_connections);
  const std::optional<bool>& prefetch_connections() const { return prefetch_connections_; }
  // - the per-worker series opt-in (NH1).
  void set_worker_series(bool worker_series);
  bool worker_series() const { return worker_series_; }

  // M2: the phase-vector entry. A non-empty vector turns the request into a
  // phase run: build_request() writes the vector and clears the
  // mutually-exclusive top-level duration (Task 2's rules, enforced here by
  // construction), and valid() reuses the pure phase-vector validation
  // (never duplicated here). Run-level rps stays legal alongside a phase
  // vector; the dialog's duration field is ignored for phase runs.
  void set_phases(std::vector<nighthawk::client::PhaseSpec> phases);
  const std::vector<nighthawk::client::PhaseSpec>& phases() const { return phases_; }

  // The dialog's phase entry field (m-fix: the production affordance): one
  // line of comma-separated phase entries, each
  //   name:rps:duration_seconds[:concurrency[:connections[:weights]]]
  // e.g. `warm:100:5:2,spike:400:5:4:64`. NH1: weights are '/'-separated
  // per-worker rate weights, one per worker of the phase, and connections may
  // be left empty before them (`hot:800:20:4::5/1/1/1`). Non-empty means the built request
  // is a phase run carrying the parsed vector; valid() reuses the pure
  // phase-vector validation on the parsed result.
  void set_phases_field(std::string text);
  const std::string& phases_field() const { return phases_field_; }

  // The field's parser: comma/newline-separated entries as above, in phase
  // order. InvalidArgumentError naming the offending entry when any entry is
  // malformed. Pure.
  static absl::StatusOr<std::vector<nighthawk::client::PhaseSpec>>
  ParsePhasesField(absl::string_view text);

  const std::string& uri() const { return uri_; }
  uint32_t rps() const { return rps_; }
  uint32_t duration_seconds() const { return duration_seconds_; }

  // Non-empty uri, rps >= 1, and — for a legacy run — duration >= 1; a
  // phase run additionally requires the phase vector to pass the reused
  // pure validation. The dialog's submit affordance must gate on this;
  // build_request() does not re-check.
  bool valid() const;

  // Builds the request from the current fields: uri, requests_per_second and
  // duration land in the CommandLineOptions; live_streaming is opted into
  // (presence is the marker) and stats_flush_interval defaults to 1s. RunFeed
  // re-asserts both as its shared choke point, so a caller that skips this
  // builder still cannot silently lose live streaming. A phase run carries
  // the vector instead of the top-level duration.
  nighthawk::client::StartRequest build_request() const;

  // The dialog's four fields, in render order (the dialog overlay's field
  // rows and the shell's active-field tracking share this numbering).
  enum DialogField { kUri = 0, kRps = 1, kDuration = 2, kPhases = 3 };

  // The inline error text for one field ("rps: must be > 0"), or "" when the
  // field is fine. Pure; the dialog overlay renders these on the offending
  // field's own row after a rejected submit. A malformed phases field or a
  // vector failing the reused validation surfaces on the phases field's own
  // row with the parser's / the reused validation's message.
  static std::string FieldError(const StartRunDialogModel& dialog, int field);

private:
  std::string uri_;
  uint32_t rps_ = 0;
  uint32_t duration_seconds_ = 0;
  uint32_t connections_ = 0;
  uint32_t max_requests_per_connection_ = 0;
  std::vector<std::pair<std::string, std::string>> request_headers_;
  std::optional<nighthawk::client::Protocol::ProtocolOptions> protocol_;
  std::optional<uint32_t> request_body_size_;
  std::optional<uint32_t> burst_size_;
  std::optional<bool> open_loop_;
  std::optional<nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions>
      sequencer_idle_strategy_;
  std::optional<uint32_t> max_concurrent_streams_;
  std::optional<envoy::config::core::v3::RequestMethod> request_method_;
  std::vector<nighthawk::client::RequestOptions> request_mix_;
  std::optional<envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext> tls_context_;
  std::optional<uint32_t> max_pending_requests_;
  std::optional<bool> prefetch_connections_;
  bool worker_series_ = false;
  std::vector<nighthawk::client::PhaseSpec> phases_;
  std::string phases_field_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
