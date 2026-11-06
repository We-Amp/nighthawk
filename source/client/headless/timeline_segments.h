#pragma once

#include <string>
#include <vector>

#include "absl/strings/string_view.h"

#include "api/client/live.pb.h"
#include "api/client/options.pb.h"
#include "api/client/output.pb.h"
#include "api/client/session.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the phase timeline segment builders (MCP plan Task 0's
// honest seams; moved as-is out of status_bar.{h,cc} for the MCP server's
// nighthawk://timeline resource — the builders are pure model -> model
// functions, but they were declared in an FTXUI-coupled header, which the
// mcp_lib quarantine forbids). The TUI consumes these unchanged; the strip
// and render halves stay in status_bar.
//
// ---------------------------------------------------------------------------
// M2: the phase timeline (spec §4 layout contract). One entry per phase
// span, in run order; the entry also carries the boundary that ENDED the
// phase — its drain/idle gap rendered as a visible gap segment, the pool
// transition into the successor, and the §8.4 non-comparable + cold-start
// marks. The run's final phase carries no boundary at all (it ends at run
// teardown, not at a boundary drain).
//
// NORMATIVE (the DQ-3 ruling, mirrored on PhaseBoundaryRecord in
// api/client/session.proto): the timeline joins on the engine's boundary
// records — the discrete (wall, monotonic) events — and on nothing else.
// Boundary data is METADATA, never a synthetic metric snapshot; the idle gap
// sits INSIDE the drain-complete flag for purity accounting and is rendered
// as a labeled gap, never subtracted from or added to any percentile series.
// ---------------------------------------------------------------------------

// One phase span of the timeline.
struct PhaseTimelineSegment {
  std::string phase_id;
  // The phase's duration in seconds (from its PhaseSpec).
  double duration_seconds = 0.0;
  bool is_current = false;
  // The boundary that ended this phase (absent for the final phase):
  double drain_seconds = 0.0;
  double idle_gap_seconds = 0.0;
  // 8.6 counted-not-hidden: the drain was censored (strip renders the cens
  // mark; the gap is a deadline-anchored lower bound). Known only from the
  // streamed events — PhaseMetadata carries no censor flag.
  bool drain_censored = false;
  // §8.4: the worker set changed at this boundary — the differential pair
  // across it is marked non-comparable.
  bool worker_count_changed = false;
  // Task 6 metadata: workers idle in the prior phase cold-started here.
  bool new_workers_cold_started = false;
  // §8.6 pool-health surfacing at the boundary: the cumulative created
  // DELTA across the boundary (+N; the flat +0 form IS the warm-transfer
  // claim) and the active count. has_pool gates the whole line.
  bool has_pool = false;
  int64_t pool_created_delta = 0;
  uint64_t pool_active = 0;
  // Claim-discipline pair shown next to the current phase (spec 8.2).
  bool has_completion = false;
  double completion_ratio = 0.0;
  bool has_gate = false;
  uint64_t gate_exceeded = 0;
  // M3: the applied knob epochs that landed in this phase, in ack order —
  // the strip renders each as " +<epoch_id>" after the phase's span. Marks
  // arrive ONLY from engine acks (WithEpochMarks); a pending turn marks
  // nothing (spec §4 B timeline-truth rule).
  std::vector<std::string> epoch_ids;
};

// Builds the timeline segments from the run's phase specs (durations) and
// the merged per-phase metadata (boundary timings, worker-set identity, pool
// health, claim counts). `current_phase_id` highlights the phase being run.
// Pool aggregates are the per-worker sums of each phase's pool_health; the
// boundary's created delta is the transition delta into the successor phase:
// successor-vs-ending cumulative sums, full stop (positive for growth, zero
// for the flat warm transfer, negative when a shrinking set takes its
// cumulative counts out of the successor's sum). Pure.
std::vector<PhaseTimelineSegment> PhaseTimelineSegments(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id);

// The LIVE builder: segments from the streamed boundary events (M2 Task 7
// wiring), one segment per spec. Boundary fields (drain/idle/cold) come from
// each event; the §8.4 cmp! mark compares the specs' declared worker counts
// around the boundary (unparseable/absent concurrency = unknown = no mark).
// The pool and claim cells stay unset — the engine computes them in the
// output pass, and the strip renders only what it knows (DQ-3: never a
// fabricated cell). Pure.
std::vector<PhaseTimelineSegment> PhaseTimelineSegmentsFromEvents(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    absl::string_view current_phase_id);

// The render-side source selection: post-run metadata (complete cells) when
// the Output has arrived, else the streamed boundary events. Empty input
// (no phases configured) yields no segments and the bar renders the legacy
// placeholder row. Pure.
std::vector<PhaseTimelineSegment> BuildPhaseTimeline(
    const nighthawk::client::CommandLineOptions& options,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id);

// ---------------------------------------------------------------------------
// M3: the timeline's epoch marks (spec §4 B timeline-truth rule). One mark
// per APPLIED live change — the engine's ack is the ONLY thing that puts a
// mark on the timeline: a user edit marks nothing, a rejected attempt marks
// nothing, and a feed-generated pending-timeout marks nothing. Each mark
// lands on the segment whose phase id matches the ack record's phase_id
// (a record whose phase no segment carries — e.g. a legacy run without a
// phase vector — appends its epoch id to the LAST segment's marks rather
// than vanishing; with no segments at all the marks are dropped, which is
// the honest legacy shape). Pure.
// ---------------------------------------------------------------------------

// Applies the applied-change records to the segments. `epochs` are the
// engine-acked KnobEpochRecords, in ack order.
std::vector<PhaseTimelineSegment> WithEpochMarks(
    const std::vector<PhaseTimelineSegment>& segments,
    const std::vector<nighthawk::client::KnobEpochRecord>& epochs);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
