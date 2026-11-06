// FORK-ONLY (We-Amp): see mcp_resources.h.
#include "source/client/mcp/mcp_resources.h"

#include "source/client/headless/series_view.h"
#include "source/client/headless/timeline_segments.h"
#include "source/client/mcp/json_util.h"

namespace Nighthawk {
namespace Client {
namespace {

using google::protobuf::Struct;
using google::protobuf::Value;

// Struct-building helpers (canonical JSON values only; the same discipline
// as mcp_tools.cc).
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

constexpr char kStatusResourceDescription[] =
    "The run's structured status strip: run state (in_flight, completed, "
    "session path, resolved_target), the current phase id and knob epoch, "
    "the pending steering state, the phase boundaries (drain, idle gap, "
    "censoring), and the latest counters. The same object the get_status "
    "tool returns — one builder, two surfaces.";

constexpr char kSeriesResourceDescription[] =
    "Every stamped series the run's snapshots carried — the client "
    "benchmark.* counters AND the SUT-side sut.* scrape series side by side, "
    "no preference, no re-aggregation. Each point is {monotonic_ns, "
    "wall_time_unix_ns, value, phase_id, epoch_id}: the wall stamp is JOINED "
    "through the run's first-snapshot clock anchor (one join key for the "
    "whole view), phase_id is present on every point, epoch_id from the "
    "first applied live change onward. SUT-derived strings render only as "
    "values. Reads the live series the server holds; nighthawk://session "
    "reads the persisted file, whose sealed prefix may lag by the writer's "
    "buffered frames.";

constexpr char kTimelineResourceDescription[] =
    "The phase timeline: one segment per phase with its duration, the "
    "boundary that ended it (drain seconds, idle gap seconds), the censor "
    "mark (drain_censored — counted, not hidden), the worker-set change "
    "(worker_count_changed), the cold-start mark, and the applied "
    "knob-epoch marks (epoch_ids — engine acks only; a rejected or pending "
    "steer marks nothing). The segment source is the TUI's: the Output's "
    "post-run phase metadata once it arrived (complete cells), else the "
    "streamed boundary events. The boundaries array carries the engine's "
    "discrete (wall, monotonic) join keys and the censor markers verbatim. "
    "A run without a phase vector renders no segments — the honest legacy "
    "shape.";

constexpr char kSessionResourceDescription[] =
    "The session file's sealed-prefix records in file order (header first), "
    "each typed (header, metric_series, run_end, phase_boundary, "
    "knob_epoch, profile_window). A torn tail is DISCLOSED as tail_skipped "
    "with tail_reason naming the byte offset — never silently skipped, "
    "never a parse error. Reads only what has reached the disk: records "
    "still in the writer's queue or stream buffer are not yet part of the "
    "prefix. A profile window's raw profile bytes are not rendered; "
    "raw_profile_bytes is their length (the session file itself stays the "
    "self-describing record).";

// One session record, rendered typed. The record's own fields are the
// payload; nothing SUT-derived becomes structure — every value lands as a
// JSON value, escaped by construction.
Struct RenderRecord(const nighthawk::client::SessionRecord& record) {
  Struct out;
  switch (record.record_case()) {
    case nighthawk::client::SessionRecord::kHeader: {
      SetString(out, "type", "header");
      const nighthawk::client::SessionHeader& header = record.header();
      SetString(out, "format_version", header.format_version());
      SetString(out, "session_id", header.session_id());
      SetInt(out, "started_unix_ns", header.started_unix_ns());
      break;
    }
    case nighthawk::client::SessionRecord::kMetricSeries: {
      SetString(out, "type", "metric_series");
      const nighthawk::client::MetricSeriesRecord& series = record.metric_series();
      SetInt(out, "wall_time_unix_ns", series.wall_time_unix_ns());
      SetInt(out, "monotonic_ns", series.monotonic_ns());
      SetString(out, "phase_id", series.phase_id());
      SetString(out, "epoch_id", series.epoch_id());
      Struct counters;
      for (const auto& [name, value] : series.counters()) {
        SetInt(counters, name, static_cast<int64_t>(value));
      }
      (*out.mutable_fields())["counters"] = ToValue(counters);
      break;
    }
    case nighthawk::client::SessionRecord::kRunEnd: {
      SetString(out, "type", "run_end");
      const nighthawk::client::RunEndRecord& run_end = record.run_end();
      SetBool(out, "completed", run_end.completed());
      SetInt(out, "ended_unix_ns", run_end.ended_unix_ns());
      SetInt(out, "dropped_records", static_cast<int64_t>(run_end.dropped_records()));
      break;
    }
    case nighthawk::client::SessionRecord::kPhaseBoundary: {
      SetString(out, "type", "phase_boundary");
      const nighthawk::client::PhaseBoundaryRecord& boundary = record.phase_boundary();
      SetString(out, "phase_id", boundary.phase_id());
      SetString(out, "next_phase_id", boundary.next_phase_id());
      SetInt(out, "phase_index", boundary.phase_index());
      SetInt(out, "wall_time_unix_ns", boundary.wall_time_unix_ns());
      SetInt(out, "monotonic_ns", boundary.monotonic_ns());
      SetDouble(out, "drain_seconds",
                boundary.drain_duration().seconds() + boundary.drain_duration().nanos() / 1e9);
      SetDouble(out, "idle_gap_seconds",
                boundary.idle_gap().seconds() + boundary.idle_gap().nanos() / 1e9);
      SetInt(out, "worker_count", boundary.worker_count());
      SetBool(out, "new_workers_cold_started", boundary.new_workers_cold_started());
      SetInt(out, "connections_created", static_cast<int64_t>(boundary.connections_created()));
      SetInt(out, "connections_active", static_cast<int64_t>(boundary.connections_active()));
      SetDouble(out, "completion_ratio", boundary.completion_ratio());
      SetInt(out, "gate_exceeded", static_cast<int64_t>(boundary.gate_exceeded()));
      SetBool(out, "drain_censored", boundary.drain_censored());
      break;
    }
    case nighthawk::client::SessionRecord::kKnobEpoch: {
      SetString(out, "type", "knob_epoch");
      const nighthawk::client::KnobEpochRecord& epoch = record.knob_epoch();
      SetString(out, "epoch_id", epoch.epoch_id());
      SetString(out, "phase_id", epoch.phase_id());
      SetInt(out, "wall_time_unix_ns", epoch.wall_time_unix_ns());
      SetInt(out, "monotonic_ns", epoch.monotonic_ns());
      SetInt(out, "new_rps", epoch.new_rps());
      SetBool(out, "ramp_bent", epoch.ramp_bent());
      SetString(out, "rejection_note", epoch.rejection_note());
      break;
    }
    case nighthawk::client::SessionRecord::kProfileWindow: {
      SetString(out, "type", "profile_window");
      const nighthawk::client::ProfileWindowRecord& window = record.profile_window();
      SetString(out, "profile_type", window.profile_type());
      SetInt(out, "start_unix_ns", window.start_unix_ns());
      SetInt(out, "end_unix_ns", window.end_unix_ns());
      SetDouble(out, "purity_fraction", window.purity_fraction());
      SetString(out, "phase_id", window.phase_id());
      SetInt(out, "worker_count", static_cast<int64_t>(window.worker_count()));
      SetInt(out, "work_units", static_cast<int64_t>(window.work_units()));
      SetBool(out, "symbolized", window.symbolized());
      SetString(out, "symbolizer_tool", window.symbolizer_tool());
      SetInt(out, "interval_source", window.interval_source());
      SetInt(out, "clock_offset_uncertainty_ns", window.clock_offset_uncertainty_ns());
      SetString(out, "epoch_id", window.epoch_id());
      SetInt(out, "raw_profile_bytes", static_cast<int64_t>(window.raw_profile().size()));
      break;
    }
    case nighthawk::client::SessionRecord::RECORD_NOT_SET:
    default:
      SetString(out, "type", "unset");
      break;
  }
  return out;
}

} // namespace

google::protobuf::Struct RenderSeriesPayload(
    const std::string& session_path,
    const std::vector<nighthawk::client::MetricSeriesRecord>& records) {
  Struct out;
  Struct provenance;
  SetString(provenance, "session", session_path);
  SetString(provenance, "source",
            "the run's live metric-series records (nighthawk://session reads "
            "the persisted sealed prefix)");
  (*out.mutable_fields())["provenance"] = ToValue(provenance);

  const Tui::SeriesView view(records);
  Struct anchor;
  SetInt(anchor, "wall_time_unix_ns", view.anchor().wall_time_unix_ns);
  SetInt(anchor, "monotonic_ns", view.anchor().monotonic_ns);
  (*out.mutable_fields())["anchor"] = ToValue(anchor);

  Value& series_list = (*out.mutable_fields())["series"];
  series_list.mutable_list_value(); // the empty-list discipline: [], never null
  for (const std::string& name : view.SeriesNames()) {
    auto points = view.Series(name);
    if (!points.ok()) {
      continue; // impossible for a SeriesNames() name; never fabricated
    }
    Struct series;
    SetString(series, "name", name);
    Value& point_list = (*series.mutable_fields())["points"];
    point_list.mutable_list_value();
    for (const Tui::StampedPoint& point : *points) {
      Struct rendered;
      SetInt(rendered, "monotonic_ns", point.monotonic_ns);
      SetInt(rendered, "wall_time_unix_ns", point.wall_time_unix_ns);
      SetInt(rendered, "value", static_cast<int64_t>(point.value));
      SetString(rendered, "phase_id", point.phase_id);
      SetString(rendered, "epoch_id", point.epoch_id);
      *point_list.mutable_list_value()->add_values() = ToValue(rendered);
    }
    *series_list.mutable_list_value()->add_values() = ToValue(series);
  }
  return out;
}

google::protobuf::Struct RenderTimelinePayload(
    const std::string& session_path, const nighthawk::client::CommandLineOptions& options,
    const std::vector<nighthawk::client::PhaseBoundaryEvent>& events,
    const std::vector<nighthawk::client::PhaseMetadata>& metadata,
    absl::string_view current_phase_id,
    const std::vector<nighthawk::client::KnobEpochRecord>& epochs) {
  Struct out;
  Struct provenance;
  SetString(provenance, "session", session_path);
  SetString(provenance, "source",
            "the streamed phase boundaries (pre-run) or the Output's phase "
            "metadata (post-run) plus the engine-acked knob epochs");
  SetString(provenance, "current_phase_id", current_phase_id);
  (*out.mutable_fields())["provenance"] = ToValue(provenance);

  const std::vector<Tui::PhaseTimelineSegment> segments =
      Tui::WithEpochMarks(Tui::BuildPhaseTimeline(options, events, metadata, current_phase_id),
                          epochs);
  Value& segment_list = (*out.mutable_fields())["segments"];
  segment_list.mutable_list_value();
  for (const Tui::PhaseTimelineSegment& segment : segments) {
    Struct rendered;
    SetString(rendered, "phase_id", segment.phase_id);
    SetDouble(rendered, "duration_seconds", segment.duration_seconds);
    SetBool(rendered, "is_current", segment.is_current);
    SetDouble(rendered, "drain_seconds", segment.drain_seconds);
    SetDouble(rendered, "idle_gap_seconds", segment.idle_gap_seconds);
    SetBool(rendered, "drain_censored", segment.drain_censored);
    SetBool(rendered, "worker_count_changed", segment.worker_count_changed);
    SetBool(rendered, "new_workers_cold_started", segment.new_workers_cold_started);
    SetBool(rendered, "has_pool", segment.has_pool);
    SetInt(rendered, "pool_created_delta", segment.pool_created_delta);
    SetInt(rendered, "pool_active", static_cast<int64_t>(segment.pool_active));
    SetBool(rendered, "has_completion", segment.has_completion);
    SetDouble(rendered, "completion_ratio", segment.completion_ratio);
    SetBool(rendered, "has_gate", segment.has_gate);
    SetInt(rendered, "gate_exceeded", static_cast<int64_t>(segment.gate_exceeded));
    Value& marks = (*rendered.mutable_fields())["epoch_ids"];
    marks.mutable_list_value();
    for (const std::string& epoch_id : segment.epoch_ids) {
      marks.mutable_list_value()->add_values()->set_string_value(epoch_id);
    }
    *segment_list.mutable_list_value()->add_values() = ToValue(rendered);
  }

  Value& boundary_list = (*out.mutable_fields())["boundaries"];
  boundary_list.mutable_list_value();
  for (const nighthawk::client::PhaseBoundaryEvent& event : events) {
    Struct boundary;
    SetInt(boundary, "ending_phase_index", event.ending_phase_index());
    SetString(boundary, "ending_phase_id", event.ending_phase_id());
    SetString(boundary, "next_phase_id", event.phase_id());
    SetInt(boundary, "wall_time_unix_ns", event.wall_time_unix_ns());
    SetInt(boundary, "monotonic_ns", event.monotonic_ns());
    SetDouble(boundary, "drain_seconds",
              event.drain_duration().seconds() + event.drain_duration().nanos() / 1e9);
    SetDouble(boundary, "idle_gap_seconds",
              event.idle_gap().seconds() + event.idle_gap().nanos() / 1e9);
    SetInt(boundary, "worker_count", event.worker_count());
    SetBool(boundary, "drain_censored", event.drain_censored());
    SetBool(boundary, "next_phase_cold_started", event.next_phase_cold_started());
    *boundary_list.mutable_list_value()->add_values() = ToValue(boundary);
  }
  return out;
}

google::protobuf::Struct RenderSessionPayload(
    const std::string& session_path, const Tui::SessionRecorder::SealedPrefix& prefix) {
  Struct out;
  Struct provenance;
  SetString(provenance, "session", session_path);
  SetString(provenance, "source", "the session file's sealed prefix (flushed frames only)");
  (*out.mutable_fields())["provenance"] = ToValue(provenance);
  SetBool(out, "tail_skipped", prefix.tail_skipped);
  SetString(out, "tail_reason", prefix.tail_reason);
  Value& records = (*out.mutable_fields())["records"];
  records.mutable_list_value();
  for (const nighthawk::client::SessionRecord& record : prefix.records) {
    *records.mutable_list_value()->add_values() = ToValue(RenderRecord(record));
  }
  return out;
}

void RegisterTelemetryResources(ResourceRegistry& registry, SteeringController& controller) {
  ResourceSpec status;
  status.uri = "nighthawk://status";
  status.name = "status";
  status.description = kStatusResourceDescription;
  status.mime_type = "application/json";
  status.reader = [&controller]() { return controller.BuildStatus(); };
  registry.Register(std::move(status));

  ResourceSpec series;
  series.uri = "nighthawk://series";
  series.name = "series";
  series.description = kSeriesResourceDescription;
  series.mime_type = "application/json";
  series.reader = [&controller]() { return controller.BuildSeries(); };
  registry.Register(std::move(series));

  ResourceSpec timeline;
  timeline.uri = "nighthawk://timeline";
  timeline.name = "timeline";
  timeline.description = kTimelineResourceDescription;
  timeline.mime_type = "application/json";
  timeline.reader = [&controller]() { return controller.BuildTimeline(); };
  registry.Register(std::move(timeline));

  ResourceSpec session;
  session.uri = "nighthawk://session";
  session.name = "session";
  session.description = kSessionResourceDescription;
  session.mime_type = "application/json";
  session.reader = [&controller]() { return controller.BuildSession(); };
  registry.Register(std::move(session));
}

} // namespace Client
} // namespace Nighthawk
