#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

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
  //   name:rps:duration_seconds[:concurrency[:connections]]
  // e.g. `warm:100:5:2,spike:400:5:4:64`. Non-empty means the built request
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
  std::vector<nighthawk::client::PhaseSpec> phases_;
  std::string phases_field_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
