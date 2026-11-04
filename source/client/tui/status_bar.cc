#include "source/client/tui/status_bar.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ftxui/dom/elements.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Elapsed run time from the latency series: newest minus oldest monotonic
// stamp, floored to whole seconds. An empty series renders 0 — no snapshot
// has arrived yet.
uint64_t elapsedSeconds(const RunViewModel& model) {
  if (model.latency_band.empty()) {
    return 0;
  }
  const uint64_t elapsed_ns =
      model.latency_band.back().monotonic_ns - model.latency_band.front().monotonic_ns;
  return elapsed_ns / 1000000000ull;
}

std::string modeName(InputMode mode) {
  switch (mode) {
  case InputMode::kNavigate:
    return "navigate";
  case InputMode::kEditingStartRun:
    return "editing (start run)";
  case InputMode::kEditingSearch:
    return "editing (search)";
  case InputMode::kEditingStagedPhase:
    return "editing (staged phase)";
  }
  return "unknown";
}

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

} // namespace

ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri,
                               int duration_seconds, absl::string_view profiler_state,
                               const std::vector<PhaseTimelineSegment>* timeline,
                               int bar_width) {
  const std::string state =
      model.run_state.empty() ? "idle" : model.run_state;
  const bool left_idle = state != "idle";
  const uint64_t elapsed_s = elapsedSeconds(model);
  const std::string elapsed = std::to_string(elapsed_s);
  const uint64_t remaining =
      duration_seconds > 0 && elapsed_s < static_cast<uint64_t>(duration_seconds)
          ? static_cast<uint64_t>(duration_seconds) - elapsed_s
          : 0;

  // Row 0: state + elapsed/remaining + target uri, under the "[editing] "
  // prefix while a dialog owns the keyboard. The target slot is a hard
  // invariant: "(no target)" only ever reads on idle; a left-idle state
  // always carries the accepted target (or "(target pending)" in the
  // sub-second pre-ack window before the first snapshot confirms it).
  std::string target;
  if (left_idle) {
    if (!model.target_uri.empty()) {
      target = model.target_uri;
    } else if (!uri.empty()) {
      target = std::string(uri);
    } else {
      target = "(target pending)";
    }
  } else {
    target = uri.empty() ? "(no target)" : std::string(uri);
  }
  std::string row0;
  if (mode == InputMode::kEditingStartRun) {
    row0 += "[editing] ";
  }
  absl::StrAppend(&row0, state, " | ");
  if (left_idle && duration_seconds > 0) {
    // The remaining readout steers a live run; a finished run's clock is
    // frozen, so only the progress pair renders there.
    absl::StrAppend(&row0, "elapsed ", elapsed, "s/", duration_seconds, "s");
    if (state == "running") {
      absl::StrAppend(&row0, " | remaining ", remaining, "s");
    }
    absl::StrAppend(&row0, " | ");
  } else if (left_idle) {
    absl::StrAppend(&row0, "elapsed ", elapsed, "s (open-ended) | ");
  } else {
    absl::StrAppend(&row0, "elapsed ", elapsed, "s | ");
  }
  absl::StrAppend(&row0, target);

  // Row 1: the phase timeline during phase runs (the strip replaces the
  // placeholder phase cell; the strip is built from the streamed boundary
  // events while the run is live and from the final Output's metadata after
  // it — BuildPhaseTimeline picks the source), else the phase/epoch row.
  // Pre-A1 the phase id is the constant "main" and no steering exists, so
  // the fields are placeholders — labeled as such; the placeholder marker
  // drops when real phase ids flow (M2).
  const std::string phase = model.last_snapshot.phase_id();
  const std::string epoch = model.last_snapshot.epoch_id();
  const bool phase_id_real = !phase.empty() && phase != "main";
  const std::string epoch_cell = absl::StrCat(
      " | epoch: ", epoch.empty() ? "(none)" : epoch,
      epoch.empty() ? "" : " [placeholder]", " | flush ", kStatusBarFlushIntervalSeconds, "s");
  // m1: the strip clips to the bar width minus the tail via
  // RenderPhaseTimeline's truncation — the bar's no-overflow contract.
  // bar_width <= 0 means "unclipped" (callers without a width): pass a
  // bound too large for any real strip rather than 0 (which renders empty).
  const int strip_budget =
      bar_width > 0 ? std::max(20, bar_width - static_cast<int>(epoch_cell.size())) : 4096;
  ftxui::Element row1_element =
      timeline != nullptr && !timeline->empty()
          ? ftxui::hbox({RenderPhaseTimeline(*timeline, strip_budget), ftxui::text(epoch_cell)})
          : ftxui::text(absl::StrCat("phase: ", phase.empty() ? "(none)" : phase,
                                     phase_id_real ? "" : " [pre-phase-engine placeholder]",
                                     epoch_cell));

  // Row 2: session path + input mode + the M1 SUT-stats and profiler cells.
  // The mode cell renders inverse-video while an input mode owns the
  // keyboard. The scrape cells render only when their condition holds: the
  // reset marker uses task 7's folded wording ("reset or label-set change" —
  // the per-name summed scrape deltas make the two indistinguishable), and
  // the histogram-drop count appears once scrapes actually dropped lines.
  const std::string row2_prefix =
      absl::StrCat("session: ", session_path.empty() ? "(none)" : session_path, " | mode: ");
  ftxui::Element mode_cell = ftxui::text(modeName(mode));
  if (mode == InputMode::kEditingStartRun || mode == InputMode::kEditingSearch) {
    mode_cell = mode_cell | ftxui::inverted;
  }
  std::vector<ftxui::Element> row2 = {ftxui::text(row2_prefix), mode_cell};
  if (model.sut_counter_reset_detected) {
    row2.push_back(ftxui::text(" | sut: reset or label-set change"));
  }
  if (model.sut_histogram_line_drops > 0) {
    row2.push_back(ftxui::text(absl::StrCat(" | sut hist drops ",
                                            model.sut_histogram_line_drops)));
  }
  row2.push_back(ftxui::text(
      absl::StrCat(" | profiler: ", profiler_state.empty() ? "idle" : profiler_state)));

  return ftxui::vbox({
      ftxui::text(row0),
      std::move(row1_element),
      ftxui::hbox(std::move(row2)),
  });
}

namespace {

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

std::string PhaseTimelineStrip(const std::vector<PhaseTimelineSegment>& segments) {
  if (segments.empty()) {
    return "";
  }
  std::string strip = "[";
  for (size_t i = 0; i < segments.size(); ++i) {
    const PhaseTimelineSegment& segment = segments[i];
    if (i > 0) {
      absl::StrAppend(&strip, " | ");
    }
    // The phase span; the current phase is wrapped in "*".
    const std::string duration = absl::StrCat(static_cast<uint64_t>(segment.duration_seconds), "s");
    if (segment.is_current) {
      absl::StrAppend(&strip, "*", segment.phase_id, " ", duration, "*");
    } else {
      absl::StrAppend(&strip, segment.phase_id, " ", duration);
    }
    // Claim-discipline pair next to the current phase (spec 8.2).
    if (segment.is_current) {
      if (segment.has_completion) {
        absl::StrAppend(&strip, absl::StrFormat(" ratio=%.2f", segment.completion_ratio));
      }
      if (segment.has_gate) {
        absl::StrAppend(&strip, " gate=", segment.gate_exceeded);
      }
    }
    // The boundary that ended this phase (never on the final phase), as a
    // visible gap segment. The drain renders EXPLICITLY — including 0.0s —
    // never zero-silent (8.6: recorded, never hidden).
    if (i + 1 < segments.size()) {
      absl::StrAppend(&strip, " |");
      absl::StrAppend(&strip, absl::StrFormat(" drain %.1fs", segment.drain_seconds));
      if (segment.idle_gap_seconds > 0) {
        absl::StrAppend(&strip, absl::StrFormat(" gap %.1fs", segment.idle_gap_seconds));
      }
      if (segment.has_pool) {
        // The plan's pool-health vocabulary, with an explicit sign so a
        // shrinking worker set reads as a negative delta.
        const int64_t delta = segment.pool_created_delta;
        absl::StrAppend(&strip, " pool: created=", delta < 0 ? "-" : "+",
                        static_cast<uint64_t>(delta < 0 ? -delta : delta),
                        " active=", segment.pool_active);
      }
      if (segment.worker_count_changed) {
        // 8.4: windows across this boundary are marked non-comparable.
        absl::StrAppend(&strip, " cmp!");
      }
      if (segment.new_workers_cold_started) {
        absl::StrAppend(&strip, " cold");
      }
      if (segment.drain_censored) {
        absl::StrAppend(&strip, " cens");
      }
    }
  }
  absl::StrAppend(&strip, "]");
  return strip;
}

ftxui::Element RenderPhaseTimeline(const std::vector<PhaseTimelineSegment>& segments, int width) {
  if (width <= 0) {
    return ftxui::emptyElement();
  }
  std::string strip = PhaseTimelineStrip(segments);
  if (strip.empty()) {
    return ftxui::emptyElement();
  }
  // The persistent bar's truncation contract: the strip clips to the pane
  // width rather than wrapping or overflowing.
  if (strip.size() > static_cast<size_t>(width)) {
    strip.resize(static_cast<size_t>(width));
  }
  return ftxui::vbox({ftxui::text(strip)});
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

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
