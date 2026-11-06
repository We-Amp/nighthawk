#include "source/client/headless/timeline_segments.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/strings/numbers.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// The declared worker count of a phase spec (8.4's key); nullopt when the
// spec leaves concurrency unset or non-numeric — an unknown count marks
// nothing.
std::optional<uint32_t> declaredWorkers(const nighthawk::client::PhaseSpec& spec) {
  if (!spec.has_concurrency() || spec.concurrency().value().empty()) {
    return std::nullopt;
  }
  int parsed = 0;
  if (!absl::SimpleAtoi(spec.concurrency().value(), &parsed) || parsed <= 0) {
    return std::nullopt;
  }
  return static_cast<uint32_t>(parsed);
}

// The per-worker pool sums of one phase's metadata (the 8.6 aggregate).
std::pair<uint64_t, uint64_t> poolSums(const nighthawk::client::PhaseMetadata& phase) {
  uint64_t created = 0;
  uint64_t active = 0;
  for (const nighthawk::client::WorkerPoolHealth& health : phase.pool_health()) {
    created += health.connections_created();
    active += health.connections_active();
  }
  return {created, active};
}

} // namespace

std::vector<PhaseTimelineSegment> PhaseTimelineSegments(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id) {
  std::vector<PhaseTimelineSegment> segments;
  segments.reserve(metadata.size());
  for (const nighthawk::client::PhaseMetadata& phase : metadata) {
    PhaseTimelineSegment segment;
    segment.phase_id = phase.phase_id();
    segment.is_current = phase.phase_id() == current_phase_id;
    // Durations live on the specs, matched by the phase's position.
    if (phase.phase_index() < specs.size()) {
      segment.duration_seconds = specs[phase.phase_index()].duration().seconds();
    }

    // The claim-discipline pair always renders from the metadata (spec 8.2:
    // counted, not hidden).
    segment.has_completion = true;
    segment.completion_ratio = phase.completion_ratio();
    segment.has_gate = true;
    segment.gate_exceeded = phase.gate_exceeded();
    // 8.6 counted-not-hidden (M2-close panel major): the metadata path now
    // carries the censoring mark too — the mark must survive the post-run
    // rebuild, not vanish with the streamed events.
    segment.drain_censored = phase.drain_censored();

    // Pool aggregates are the per-worker sums of this phase's entries.
    const bool has_pool = phase.pool_health_size() > 0;
    const auto [created, active] = poolSums(phase);

    // The boundary fields of segment N describe the transition INTO phase
    // N+1 — the boundary that ended this phase: its drain/idle timings come
    // from this phase's metadata; the worker-set change and cold start are
    // recorded on the successor's metadata (they happened at this boundary);
    // the pool delta is the transition delta into the successor.
    if (phase.phase_index() + 1 < metadata.size()) {
      const nighthawk::client::PhaseMetadata& successor = metadata[phase.phase_index() + 1];
      segment.drain_seconds =
          phase.drain_duration().seconds() + phase.drain_duration().nanos() / 1e9;
      segment.idle_gap_seconds =
          phase.has_idle_gap() ? phase.idle_gap().seconds() + phase.idle_gap().nanos() / 1e9 : 0.0;
      segment.worker_count_changed = successor.workers_changed_vs_prior_phase();
      segment.new_workers_cold_started = successor.new_workers_cold_started();
      if (has_pool && successor.pool_health_size() > 0) {
        const auto [successor_created, successor_active] = poolSums(successor);
        segment.has_pool = true;
        segment.pool_created_delta =
            static_cast<int64_t>(successor_created) - static_cast<int64_t>(created);
        segment.pool_active = successor_active;
      }
    }
    segments.push_back(std::move(segment));
  }
  return segments;
}

std::vector<PhaseTimelineSegment> PhaseTimelineSegmentsFromEvents(
    const std::vector<nighthawk::client::PhaseSpec>& specs,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    absl::string_view current_phase_id) {
  if (specs.empty()) {
    return {};
  }
  std::vector<PhaseTimelineSegment> segments;
  segments.reserve(specs.size());
  for (size_t i = 0; i < specs.size(); ++i) {
    PhaseTimelineSegment segment;
    segment.phase_id = specs[i].name();
    segment.duration_seconds = specs[i].duration().seconds();
    segment.is_current = segment.phase_id == current_phase_id;
    // The 8.4 mark from the declared worker counts around the boundary; an
    // unparseable/absent concurrency marks nothing.
    if (i + 1 < specs.size()) {
      const std::optional<uint32_t> here = declaredWorkers(specs[i]);
      const std::optional<uint32_t> next = declaredWorkers(specs[i + 1]);
      segment.worker_count_changed = here.has_value() && next.has_value() && *here != *next;
    }
    segments.push_back(std::move(segment));
  }
  // The streamed boundaries land on their ending phase's segment. DQ-3:
  // metadata only — the pool/claim cells stay unset until the Output lands.
  for (const nighthawk::client::PhaseBoundaryEvent& wire_event : events) {
    if (wire_event.ending_phase_index() >= segments.size()) {
      continue;
    }
    PhaseTimelineSegment& segment = segments[wire_event.ending_phase_index()];
    segment.drain_seconds =
        wire_event.drain_duration().seconds() + wire_event.drain_duration().nanos() / 1e9;
    segment.idle_gap_seconds =
        wire_event.has_idle_gap()
            ? wire_event.idle_gap().seconds() + wire_event.idle_gap().nanos() / 1e9
            : 0.0;
    segment.drain_censored = wire_event.drain_censored();
    segment.new_workers_cold_started = wire_event.next_phase_cold_started();
  }
  return segments;
}

std::vector<PhaseTimelineSegment> BuildPhaseTimeline(
    const nighthawk::client::CommandLineOptions& options,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id) {
  const std::vector<nighthawk::client::PhaseSpec> specs(options.phases().begin(),
                                                        options.phases().end());
  if (specs.empty()) {
    return {};
  }
  // Post-run completeness: the Output's metadata carries the pool and claim
  // cells; before it lands the streamed events are the source.
  if (!metadata.empty()) {
    return PhaseTimelineSegments(specs, metadata, current_phase_id);
  }
  return PhaseTimelineSegmentsFromEvents(specs, events, current_phase_id);
}

std::vector<PhaseTimelineSegment> WithEpochMarks(
    const std::vector<PhaseTimelineSegment>& segments,
    const std::vector<nighthawk::client::KnobEpochRecord>& epochs) {
  std::vector<PhaseTimelineSegment> result = segments;
  if (result.empty()) {
    // No timeline to mark (a legacy run without a phase vector): the marks
    // would have nowhere honest to land — dropped, not faked onto a row.
    return result;
  }
  for (const nighthawk::client::KnobEpochRecord& record : epochs) {
    if (record.epoch_id().empty()) {
      // A rejected attempt carries an empty epoch id — it marks nothing
      // (the controller filters too; both layers keep the timeline truth).
      continue;
    }
    bool landed = false;
    for (PhaseTimelineSegment& segment : result) {
      if (segment.phase_id == record.phase_id()) {
        segment.epoch_ids.push_back(record.epoch_id());
        landed = true;
        break;
      }
    }
    if (!landed) {
      // The bend's phase has no segment (e.g. a phase id the specs never
      // carried): the mark lands on the LAST segment rather than vanishing.
      result.back().epoch_ids.push_back(record.epoch_id());
    }
  }
  return result;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
