#include "source/client/phase_orchestrator.h"

#include <algorithm>
#include <limits>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

#include "source/common/common/logger.h"

namespace Nighthawk {
namespace Client {

namespace {

int64_t steadyNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

int64_t wallNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

} // namespace

std::vector<PhaseWorkerSubset>
computeWorkerSubsets(const std::vector<nighthawk::client::PhaseSpec>& phases,
                     uint32_t bootstrapped_workers) {
  std::vector<PhaseWorkerSubset> subsets;
  subsets.reserve(phases.size());
  uint32_t prior_count = 0;
  uint32_t high_water_mark = 0;
  for (const nighthawk::client::PhaseSpec& phase : phases) {
    PhaseWorkerSubset subset;
    subset.worker_count = bootstrapped_workers;
    if (phase.has_concurrency()) {
      uint32_t declared = 0;
      if (absl::SimpleAtoi(phase.concurrency().value(), &declared) && declared > 0) {
        subset.worker_count = declared;
      }
    }
    if (!subsets.empty()) {
      subset.workers_changed_vs_prior_phase = subset.worker_count != prior_count;
      subset.new_workers_cold_started = subset.worker_count > high_water_mark;
    }
    prior_count = subset.worker_count;
    high_water_mark = std::max(high_water_mark, subset.worker_count);
    subsets.push_back(subset);
  }
  return subsets;
}

PhaseOrchestrator::PhaseOrchestrator(const std::vector<nighthawk::client::PhaseSpec>& phases,
                                     EventSink event_sink)
    : phases_(phases), event_sink_(std::move(event_sink)) {
  ASSERT(!phases_.empty(), "multi-phase orchestration requires at least one phase");
}

void PhaseOrchestrator::emit(uint32_t phase_index) {
  if (event_sink_ != nullptr) {
    PhaseEvent event;
    event.phase_index = phase_index;
    event.phase_id = phases_[phase_index].name();
    event.wall_time_unix_ns = wallNowNs();
    event.monotonic_ns = steadyNowNs();
    // FORK-ONLY (T7 wiring): only the next-phase-start emit completes a
    // boundary — by then the drain was collected (collectDrainAcks pushed
    // the timing) and the idle gap was recorded against it (immediately
    // above, in markNextPhaseStarted). A FULLY CENSORED boundary completes
    // too — 8.6 counted-not-hidden: it streams with the capped drain
    // duration and its censoring deadline-anchored gap; the reviewer's rule
    // is that the one case that must never vanish is the censored boundary.
    // The drain-complete emit's boundary is not yet idle-timed, and the
    // final-complete emit is the run's own end.
    if (state_ == BoundaryState::kNextPhaseStarted && phase_index > 0 && !timings_.empty() &&
        timings_.back().phase_index == phase_index - 1) {
      const PhaseBoundaryTiming& timing = timings_.back();
      event.boundary_complete = true;
      event.ending_phase_index = timing.phase_index;
      event.ending_phase_id = timing.phase_id;
      event.drain_duration_ns = timing.drain_duration_ns;
      event.idle_gap_ns = timing.idle_gap_ns;
      event.boundary_censored = timing.drain_censored;
    }
    event_sink_(event);
  }
}

BoundaryState PhaseOrchestrator::markSequencerStopped(uint32_t /*phase_index*/) {
  // The orchestrator learns of the stop through the drain acks; this mark
  // exists so the transition table can express the stop state explicitly.
  state_ = BoundaryState::kSequencerStopped;
  return state_;
}

bool PhaseOrchestrator::collectDrainAcks(std::vector<std::future<PhaseDrainAck>>& acks,
                                         std::chrono::milliseconds budget,
                                         uint32_t phase_index) {
  ASSERT(phase_index < phases_.size(), "phase index out of range");
  state_ = BoundaryState::kDraining;
  const auto deadline = std::chrono::steady_clock::now() + budget;
  bool all_acked = true;
  int64_t earliest_stop_ns = std::numeric_limits<int64_t>::max();
  int64_t latest_drain_complete_ns = std::numeric_limits<int64_t>::min();
  for (std::future<PhaseDrainAck>& ack : acks) {
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::milliseconds::zero() ||
        ack.wait_for(remaining) != std::future_status::ready) {
      // Censored: recorded, never hidden. The orchestrator moves on; the
      // worker's drain watch is cancelled by the next begin-phase post.
      all_acked = false;
      continue;
    }
    const PhaseDrainAck value = ack.get();
    earliest_stop_ns = std::min(earliest_stop_ns, value.sequencer_stop_monotonic_ns);
    latest_drain_complete_ns = std::max(latest_drain_complete_ns, value.drain_complete_monotonic_ns);
  }

  PhaseBoundaryTiming timing;
  timing.phase_index = phase_index;
  timing.phase_id = phases_[phase_index].name();
  timing.drain_censored = !all_acked;
  if (latest_drain_complete_ns >= earliest_stop_ns) {
    timing.drain_duration_ns = latest_drain_complete_ns - earliest_stop_ns;
    last_drain_complete_monotonic_ns_ = latest_drain_complete_ns;
  } else {
    // Fully censored (no ack landed, or none carried a usable pair): the
    // bounded wait gave up at the deadline. 8.6 counted-not-hidden, never
    // zero-silent: the recorded drain duration is the full budget, and the
    // censoring deadline anchors the idle-gap measurement.
    timing.drain_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(budget)
                                   .count();
    timing.censored_deadline_monotonic_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch()).count();
  }
  timings_.push_back(std::move(timing));
  state_ = BoundaryState::kDrainComplete;
  emit(phase_index);
  return all_acked;
}

void PhaseOrchestrator::markNextPhaseStarted(uint32_t phase_index) {
  ASSERT(phase_index < phases_.size(), "phase index out of range");
  if (!timings_.empty()) {
    if (last_drain_complete_monotonic_ns_ >= 0) {
      timings_.back().idle_gap_ns = steadyNowNs() - last_drain_complete_monotonic_ns_;
    } else if (timings_.back().censored_deadline_monotonic_ns >= 0) {
      // No completion instant exists (fully censored): the gap is measured
      // from the censoring deadline — an honest lower bound, recorded as
      // metadata like every other boundary number.
      timings_.back().idle_gap_ns =
          steadyNowNs() - timings_.back().censored_deadline_monotonic_ns;
    }
    last_drain_complete_monotonic_ns_ = -1;
  }
  state_ = BoundaryState::kNextPhaseStarted;
  emit(phase_index);
}

void PhaseOrchestrator::markFinalPhaseComplete() {
  const uint32_t last_index = phaseCount() - 1;
  state_ = BoundaryState::kDrainComplete;
  emit(last_index);
}

nighthawk::client::PhaseMetadata
BuildPhaseMetadata(uint32_t phase_index, const nighthawk::client::PhaseSpec& phase_spec,
                   const PhaseBoundaryTiming* boundary, const PhaseWorkerSubset& worker_subset,
                   uint64_t initiated, uint64_t completed, uint64_t stream_resets,
                   uint64_t gate_exceeded, uint64_t inherited_straggler_completions) {
  nighthawk::client::PhaseMetadata metadata;
  metadata.set_phase_id(phase_spec.name());
  metadata.set_phase_index(phase_index);
  if (boundary != nullptr) {
    // Duration.nanos is int32; split the int64 nanosecond stamps into
    // seconds + nanos so multi-second censored drains don't overflow.
    metadata.mutable_drain_duration()->set_seconds(boundary->drain_duration_ns / 1000000000);
    metadata.mutable_drain_duration()->set_nanos(boundary->drain_duration_ns % 1000000000);
    // The idle gap is -1 when the phase has no successor; that is the final
    // boundary and carries no gap.
    if (boundary->idle_gap_ns >= 0) {
      metadata.mutable_idle_gap()->set_seconds(boundary->idle_gap_ns / 1000000000);
      metadata.mutable_idle_gap()->set_nanos(boundary->idle_gap_ns % 1000000000);
    }
  }
  // Task 6's worker-subset bookkeeping: worker_count is the phase's ACTIVE
  // subset, and the boundary's worker-set deltas are surfaced, never silent
  // (a cold start is a joining worker opening its first connections at this
  // boundary; a rejoin within the high-water mark comes back warm and sets
  // only the changed flag).
  metadata.set_worker_count(worker_subset.worker_count);
  metadata.set_workers_changed_vs_prior_phase(worker_subset.workers_changed_vs_prior_phase);
  metadata.set_new_workers_cold_started(worker_subset.new_workers_cold_started);
  metadata.set_initiated(initiated);
  // Straggler completions inherited from a censored predecessor boundary are
  // excluded: the predecessor already counted them in its reset_or_timed_out
  // and they were initiated before this phase began. Without the exclusion
  // they would count twice and this phase's completion ratio could read
  // above 1.
  const uint64_t own_completed = completed > inherited_straggler_completions
                                     ? completed - inherited_straggler_completions
                                     : 0;
  metadata.set_completed(own_completed);
  uint64_t reset_or_timed_out = stream_resets;
  if (boundary != nullptr && boundary->drain_censored) {
    // Censored drain: the requests still in flight when the deadline fired
    // are counted, not hidden (spec 8.2) -- even if they complete later,
    // during the successor phase (whose completed count excludes them).
    // Guarded: the raw completed delta can exceed initiated only via the
    // same censor-to-swap drift the cap below guards against.
    reset_or_timed_out += initiated > completed ? initiated - completed : 0;
  }
  metadata.set_reset_or_timed_out(reset_or_timed_out);
  // Task 5's per-phase gate refusals: starts shed by the client-side gates.
  metadata.set_gate_exceeded(gate_exceeded);
  // The cap is a drift guard: with stragglers excluded, own completions
  // cannot exceed own initiations, so the ratio cannot honestly exceed 1
  // (a tiny censor-to-swap drift could otherwise surface as 1.0000...+eps).
  metadata.set_completion_ratio(initiated > 0
                                    ? std::min(1.0, static_cast<double>(own_completed) /
                                                        static_cast<double>(initiated))
                                    : 1.0);
  return metadata;
}

} // namespace Client
} // namespace Nighthawk
