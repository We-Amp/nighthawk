#include "source/client/phase_orchestrator.h"

#include <algorithm>
#include <limits>

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
  }
  timings_.push_back(std::move(timing));
  state_ = BoundaryState::kDrainComplete;
  emit(phase_index);
  return all_acked;
}

void PhaseOrchestrator::markNextPhaseStarted(uint32_t phase_index) {
  ASSERT(phase_index < phases_.size(), "phase index out of range");
  if (!timings_.empty() && last_drain_complete_monotonic_ns_ >= 0) {
    timings_.back().idle_gap_ns = steadyNowNs() - last_drain_complete_monotonic_ns_;
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

} // namespace Client
} // namespace Nighthawk
