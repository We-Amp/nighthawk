#pragma once

#include <cstdint>

#include "absl/strings/string_view.h"

#include "api/client/live.pb.h"
#include "api/client/output.pb.h"
#include "api/client/service.pb.h"
#include "api/client/session.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the pure proto conversions between the run feed's wire
// messages and the session file's records. No threading, no I/O — the writer
// thread contract stays in session_writer.h and the callers in tui_app.

// The session file format version ToSessionHeader stamps into the header; the
// reader accepts exactly this version (session_reader.cc).
inline constexpr char kSessionFormatVersion[] = "1";

// Copies a live snapshot into its persisted form, field for field: timestamps
// stay engine-stamped (the rig is the timestamp authority, spec §8.5), the
// phase id stays the constant "main" pre-A1 and an empty epoch id stays empty
// pre-A2 (M0a placeholder semantics).
nighthawk::client::MetricSeriesRecord
ToMetricSeries(const nighthawk::client::LiveSnapshot& snapshot);

// Builds the session header: the format version, the caller's session id, the
// client-host open time (the session's started_unix_ns), and the exact
// StartRequest the run was started with.
nighthawk::client::SessionHeader ToSessionHeader(const nighthawk::client::StartRequest& request,
                                                 absl::string_view session_id,
                                                 int64_t started_unix_ns);

// Builds one PhaseBoundaryRecord for the boundary that ended `phase_id`
// (M2 Task 7). NORMATIVE (the DQ-3 ruling, mirrored on the proto message and
// in status_bar.h): boundary records are METADATA, never synthetic metric
// snapshots. The engine-emitted (wall, monotonic) pair — the boundary's
// PhaseEvent stamps — is the join key for the TUI timeline and window
// alignment; the idle gap sits INSIDE the drain-complete flag for purity
// accounting and is never folded into any percentile series.
//
// `metadata` is the ending phase's merged PhaseMetadata: it supplies the
// boundary timings, the worker-set identity, the claim counts, and the pool
// health (the record's connection aggregates are the per-worker sums).
// `next_phase_id` is empty for the run's final phase (no successor), and
// `next_phase_cold_started` is the SUCCESSOR's new_workers_cold_started
// flag — the record's field 9 describes the transition INTO next_phase_id,
// matching where the timeline renders the cold mark (the ending phase's own
// flag describes its entry boundary, not this one). The plain arguments
// (rather than the engine's PhaseEvent struct) keep the TUI quarantine
// intact — the orchestrator header pulls Envoy-side sources.
nighthawk::client::PhaseBoundaryRecord
ToPhaseBoundaryRecord(uint32_t phase_index, absl::string_view phase_id,
                      absl::string_view next_phase_id, int64_t wall_time_unix_ns,
                      int64_t monotonic_ns, const nighthawk::client::PhaseMetadata& metadata,
                      bool next_phase_cold_started);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
