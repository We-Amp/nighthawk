#pragma once

// FORK-ONLY (We-Amp): process-side phase state machine for multi-phase runs
// (M2 A1). Pure bookkeeping: it tracks boundary states and per-boundary
// timings, and performs the bounded drain-ack collection. The threading
// contract lives in ProcessImpl: the orchestrator runs on the run-calling
// thread (the thread that enters ProcessImpl::run() and today joins the
// workers at teardown); all cross-thread traffic is one-way posts into worker
// dispatchers plus promises completed from the worker side, and the
// orchestrator's bounded future collection is the only blocking wait in the
// system -- it can always time out.

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <string>
#include <vector>

#include "nighthawk/client/client_worker.h"

#include "api/client/options.pb.h"

namespace Nighthawk {
namespace Client {

// Engine-emitted phase event (spec 8.5): the boundary emits BOTH stamps, and
// the TUI's phase timeline and window alignment (M2 Task 7) consume exactly
// these, replacing the M1 first-snapshot clock anchor.
struct PhaseEvent {
  uint32_t phase_index;
  std::string phase_id; // == PhaseSpec::name
  int64_t wall_time_unix_ns;
  int64_t monotonic_ns;
};

// Boundary lifecycle states, in the order the orchestrator observes them.
enum class BoundaryState {
  kSequencerStopped,  // the phase's sequencer has stopped (dispatcher kept alive)
  kDraining,          // waiting for in-flight to reach zero, bounded
  kDrainComplete,     // all active workers acked, or the deadline fired (censored)
  kNextPhaseStarted,  // the next phase's first work is scheduled
};

// Per-boundary bookkeeping recorded by the orchestrator. Consumed by the
// per-phase output work (PhaseMetadata, M2 Task 4); spec 8.6: the drain
// duration and the idle gap are recorded, never zero-silent, and censoring is
// counted, not hidden.
struct PhaseBoundaryTiming {
  uint32_t phase_index{0};
  std::string phase_id;
  int64_t drain_duration_ns{0};
  // Last drain completion -> next phase's first scheduled work; -1 when the
  // phase has no successor (final boundary of the run).
  int64_t idle_gap_ns{-1};
  bool drain_censored{false};
};

class PhaseOrchestrator {
public:
  using EventSink = std::function<void(const PhaseEvent&)>;

  // @param phases the validated phase vector (one entry included).
  // @param event_sink invoked synchronously on the run-calling thread at each
  // boundary the orchestrator marks.
  PhaseOrchestrator(const std::vector<nighthawk::client::PhaseSpec>& phases,
                    EventSink event_sink);

  uint32_t phaseCount() const { return static_cast<uint32_t>(phases_.size()); }
  const nighthawk::client::PhaseSpec& phase(uint32_t index) const { return phases_[index]; }
  bool isFinalPhase(uint32_t index) const { return index + 1 == phaseCount(); }
  BoundaryState state() const { return state_; }

  // Marks the phase's sequencer as stopped (the worker dispatcher survives).
  BoundaryState markSequencerStopped(uint32_t phase_index);

  /**
   * Bounded drain-ack collection for one boundary: waits every worker's drain
   * future against a single shared deadline of `budget`. Workers that ack in
   * time fold into the boundary's drain duration; workers that do not are
   * censored (recorded, never hidden) and the orchestrator moves on, so the
   * wait can always time out and the run always completes.
   *
   * @return true iff every future was ready before the deadline.
   */
  bool collectDrainAcks(std::vector<std::future<PhaseDrainAck>>& acks,
                        std::chrono::milliseconds budget, uint32_t phase_index);

  // Marks the next phase's first scheduled work: records the idle gap against
  // the previous boundary's last drain completion and emits the phase-start
  // event for `phase_index`.
  void markNextPhaseStarted(uint32_t phase_index);

  // Marks the final boundary after the run's last phase has been joined.
  void markFinalPhaseComplete();

  const std::vector<PhaseBoundaryTiming>& boundaryTimings() const { return timings_; }

private:
  void emit(uint32_t phase_index);

  std::vector<nighthawk::client::PhaseSpec> phases_;
  EventSink event_sink_;
  BoundaryState state_{BoundaryState::kNextPhaseStarted};
  std::vector<PhaseBoundaryTiming> timings_;
  int64_t last_drain_complete_monotonic_ns_{-1};
};

// FORK-ONLY (We-Amp): builds one phase's metadata record (M2 Task 4, spec
// 8.2/8.4/8.6) from the orchestrator's boundary timing for the phase and the
// phase's claim counts folded across the active workers. Pure bookkeeping,
// unit-testable without a run. `boundary` is null for the run's final phase,
// which ends at the run teardown rather than at a boundary drain. Pool
// health is appended by the caller, which reads the engine's pool counters.
//
// Claim discipline (spec 8.2): reset_or_timed_out counts the phase's stream
// resets, plus -- only when the boundary drain was censored -- the requests
// still in flight when the deadline fired (initiated minus completed; at an
// uncensored boundary in-flight reaches zero before the swap, so the delta
// is zero). Those censored requests stay counted here even when they
// complete later, during the successor phase: the caller passes their count
// as `inherited_straggler_completions` (the predecessor slab's in-flight at
// retire, when the boundary that retired it was censored) and they are
// excluded from THIS phase's completed, so no straggler is counted twice
// and no completion ratio reads above 1. gate_exceeded is Task 5's
// per-phase gate-refusal count (starts refused are shed and counted, never
// queued).
nighthawk::client::PhaseMetadata
BuildPhaseMetadata(uint32_t phase_index, const nighthawk::client::PhaseSpec& phase_spec,
                   const PhaseBoundaryTiming* boundary, uint32_t worker_count, uint64_t initiated,
                   uint64_t completed, uint64_t stream_resets, uint64_t gate_exceeded,
                   uint64_t inherited_straggler_completions);

} // namespace Client
} // namespace Nighthawk
