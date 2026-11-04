#include "source/client/tui/session_conversion.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

nighthawk::client::MetricSeriesRecord
ToMetricSeries(const nighthawk::client::LiveSnapshot& snapshot) {
  nighthawk::client::MetricSeriesRecord record;
  record.set_wall_time_unix_ns(snapshot.wall_time_unix_ns());
  record.set_monotonic_ns(snapshot.monotonic_ns());
  record.set_phase_id(snapshot.phase_id());
  record.set_epoch_id(snapshot.epoch_id());
  (*record.mutable_counters()) = snapshot.counters();
  *record.mutable_statistics() = snapshot.statistics();
  return record;
}

nighthawk::client::SessionHeader ToSessionHeader(const nighthawk::client::StartRequest& request,
                                                 absl::string_view session_id,
                                                 int64_t started_unix_ns) {
  nighthawk::client::SessionHeader header;
  header.set_format_version(kSessionFormatVersion);
  header.set_session_id(session_id.data(), session_id.size());
  header.set_started_unix_ns(started_unix_ns);
  *header.mutable_start_request() = request;
  return header;
}

nighthawk::client::PhaseBoundaryRecord
ToPhaseBoundaryRecord(uint32_t phase_index, absl::string_view phase_id,
                      absl::string_view next_phase_id, int64_t wall_time_unix_ns,
                      int64_t monotonic_ns, const nighthawk::client::PhaseMetadata& metadata,
                      bool next_phase_cold_started) {
  nighthawk::client::PhaseBoundaryRecord record;
  record.set_phase_index(phase_index);
  record.set_phase_id(phase_id.data(), phase_id.size());
  record.set_next_phase_id(next_phase_id.data(), next_phase_id.size());
  record.set_wall_time_unix_ns(wall_time_unix_ns);
  record.set_monotonic_ns(monotonic_ns);
  // Boundary timings (spec 8.6: recorded, never zero-silent).
  *record.mutable_drain_duration() = metadata.drain_duration();
  *record.mutable_idle_gap() = metadata.idle_gap();
  record.set_worker_count(metadata.worker_count());
  // Field 9 describes the transition INTO the successor (see the proto
  // comment's attribution rule) — the caller passes the successor's flag.
  record.set_new_workers_cold_started(next_phase_cold_started);
  // Pool aggregates: the per-worker sums of the ending phase's entries.
  uint64_t created = 0;
  uint64_t active = 0;
  for (const nighthawk::client::WorkerPoolHealth& health : metadata.pool_health()) {
    created += health.connections_created();
    active += health.connections_active();
  }
  record.set_connections_created(created);
  record.set_connections_active(active);
  // Claim-discipline counts (spec 8.2).
  record.set_completion_ratio(metadata.completion_ratio());
  record.set_gate_exceeded(metadata.gate_exceeded());
  return record;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
