// FORK-ONLY (We-Amp): the MCP telemetry resources (MCP plan Task 3):
// nighthawk://status|series|timeline|session. Telemetry is DATA: SUT-derived
// strings render only as VALUES in the canonical JSON (escaped by
// construction); there is no provenance prefix line inside the JSON — a
// `provenance` FIELD instead. All URIs are validated at the dispatcher
// (exact-match against the registered URIs — no traversal, no agent-shaped
// ids; Review Focus 3), and the resources read the store: nothing here
// blocks.
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_RESOURCES_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_RESOURCES_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"

#include "api/client/live.pb.h"
#include "api/client/options.pb.h"
#include "api/client/output.pb.h"
#include "api/client/session.pb.h"
#include "google/protobuf/struct.pb.h"

#include "source/client/headless/session_recorder.h"
#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {

// Registers the four telemetry resources on `registry`, bound to
// `controller`:
//   - nighthawk://status   — the canonical status strip (the ONE builder is
//     SteeringController::BuildStatus; get_status renders the same object).
//   - nighthawk://series   — every stamped series the run's snapshots
//     carried, client AND SUT-side alike, each point carrying the monotonic
//     stamp, the wall stamp JOINED through the first-snapshot anchor, and
//     the phase + knob epoch the snapshot was produced under.
//   - nighthawk://timeline — the phase timeline segments plus the
//     boundary/epoch/censor markers (the same builders the TUI strip
//     renders; streamed boundaries pre-run, the Output's phase metadata
//     post-run).
//   - nighthawk://session  — the session file's sealed-prefix records with
//     the torn-tail honesty (tail_skipped + the reader's reason).
// Resource descriptions are specified copy, like the tools'.
void RegisterTelemetryResources(ResourceRegistry& registry, SteeringController& controller);

// --- pure payload renderers (tested directly; the readers above wire them
// to the controller's state) -------------------------------------------------

// The nighthawk://series payload. `records` are the run's metric-series
// records in snapshot order (the first anchors the wall join). Every series
// name the records carried appears — client benchmark.* classes and SUT
// sut.* scrape series side by side, no preference, no re-aggregation.
google::protobuf::Struct RenderSeriesPayload(
    const std::string& session_path,
    const std::vector<nighthawk::client::MetricSeriesRecord>& records);

// The nighthawk://timeline payload. The segment source selection is the
// TUI's (BuildPhaseTimeline): post-run metadata when present, else the
// streamed boundary events; `epochs` (the engine-acked knob-epoch records,
// in ack order) put the +<epoch_id> marks on their phases. The boundaries
// array carries the engine's discrete (wall, monotonic) join keys and the
// censor markers verbatim. Empty phase specs render no segments — the honest
// legacy shape.
google::protobuf::Struct RenderTimelinePayload(
    const std::string& session_path, const nighthawk::client::CommandLineOptions& options,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id,
    const std::vector<nighthawk::client::KnobEpochRecord>& epochs);

// The nighthawk://session payload: every record of the session file's sealed
// prefix in file order, typed, with the torn tail DISCLOSED (tail_skipped +
// tail_reason naming the byte offset) — never silently skipped, never a
// parse error. A profile window's raw profile BYTES are not rendered (they
// are the session file's own content; re-fetch is never needed) — the record
// carries raw_profile_bytes, the byte length.
google::protobuf::Struct RenderSessionPayload(
    const std::string& session_path, const Tui::SessionRecorder::SealedPrefix& prefix);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_RESOURCES_H_
