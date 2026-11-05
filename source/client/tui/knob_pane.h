#pragma once

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "api/client/options.pb.h"
#include "api/client/service.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the knob pane. M0b rendered it display-only; M3's
// Task 8 turns the live set real: the rps knob carries the §4 knob state
// machine (current-acked / pending-apply / rejected-with-inline-reason /
// staged-next-phase, visually distinct), and the display-only banner is
// gone. The steering split stays visible: the live set is rps + ramp
// (spec §3); everything else is phase-gated. RPS VALUES ARE THE PHASE
// TOTAL (the knob speaks global rps; the engine divides across the active
// workers at fan-out) — every rps surface says so.

// Whether a knob joins M3's live steering set or is gated behind the phase
// engine (spec §3 ruling). kStaged marks the M2 staged next-phase preview:
// visually distinct from both applied states, never applied to the current
// phase, discardable.
enum class KnobSteering {
  kLiveM3,
  kPhaseGated,
  kStaged,
};

// One knob line: name, current value, and the steering distinction.
struct KnobDisplay {
  std::string name;  // "rps", "ramp", "concurrency", "connections", "queue bounds"
  std::string value; // current value or "unset"/"not configured" — never empty
  KnobSteering steering;
};

// The five knobs from the run options, in the canonical order above. "ramp"
// reflects the rate_limiter_plugin_config (the linear-ramping-rate-limiter
// plugin is M3's ramping vehicle; a present-but-empty plugin name counts as
// unconfigured); "queue bounds" folds max_pending_requests and
// max_active_requests into one line. Values never render empty: unset fields
// say "unset", an unconfigured ramp says "not configured".
std::vector<KnobDisplay> KnobsFromOptions(const nighthawk::client::CommandLineOptions& options);

// The knobs tab (tab 2): one line per knob under a header that states the
// live set and the phase-total semantics of every rps value (the M3 pane is
// no longer display-only). `width`/`height` are the pane-local dimensions
// (terminal size minus the status bar); a non-positive dimension renders an
// empty pane.
ftxui::Element RenderKnobPane(const std::vector<KnobDisplay>& knobs, int width, int height);

// ---------------------------------------------------------------------------
// M3 Task 8: the live knob-turn state machine (spec §4 B). A live-set turn
// goes pending-apply (the timeline is UNCHANGED until the engine acks) ->
// current-acked (the timeline gains the epoch mark) or
// rejected-with-inline-reason (the service's reason, inline, dismissable);
// a pending turn that resolves without an engine verdict (the feed's
// run-end surfacing) renders as failed-pending with the reason — never
// silently cleared. All four states are visually distinct BY MARKER WORD,
// never by color alone (the five-lens accessibility rule).
// ---------------------------------------------------------------------------

// The knob-turn states. kStaged is M2's staged-next-phase preview, kept
// distinct from every live state.
enum class KnobTurnState {
  kCurrentAcked,  // the engine-acked value; the timeline-stable state
  kPendingApply,  // a turn is in flight; the timeline marks nothing yet
  kRejected,      // the service rejected the last turn; reason inline
  kFailedPending, // the pending turn resolved without an engine verdict
  kStaged,        // the M2 staged-next-phase preview
};

// One live-set knob line's turn state.
struct LiveKnobTurn {
  std::string name;           // "rps"
  std::string acked_value;    // the last engine-acked value; "" before the first ack
  std::string pending_value;  // the value awaiting its ack ("" when none)
  KnobTurnState state = KnobTurnState::kCurrentAcked;
  std::string note; // the rejection / failed-pending reason (kRejected/kFailedPending)
};

// The one-line marker word per turn state — the visual-distinctness anchor
// ("acked" / "pending" / "rejected" / "failed" / "STAGED"). Exposed for the
// distinctness test.
absl::string_view KnobTurnStateMarker(KnobTurnState state);

// The rps knob's turn state machine (pure, no I/O): BeginRpsTurn moves to
// pending-apply; OnUpdateResult resolves it with the feed's ack (applied ->
// current-acked with the new value; an engine rejection -> rejected with the
// reason; a feed-generated result — rejection_reason prefixed "run-feed: ",
// never an engine verdict — -> failed-pending with the reason). Dismiss
// clears a stale rejection/failed note (the u-key affordance). One turn at
// a time: BeginRpsTurn while pending is a no-op (the caller's
// one-outstanding discipline; the controller refuses earlier still).
class KnobTurnTracker {
public:
  // The run's configured rps (the starting acked value — the configured
  // PHASE TOTAL); seeded by the controller at run start. No-op while a turn
  // is pending.
  void SeedAckedValue(uint32_t rps);
  // Begins a turn of the rps knob to `new_rps` (the PHASE TOTAL). A stale
  // rejection/failed note is dismissed by the turn.
  void BeginRpsTurn(uint32_t new_rps);
  // Resolves the pending turn (no-op when nothing is pending; a rejected
  // attempt still resolves the turn).
  void OnUpdateResult(const nighthawk::client::UpdateResult& result);
  // The dismissal affordance for a stale rejection/failed-pending note.
  void DismissOutcome();
  bool has_pending() const { return turn_.state == KnobTurnState::kPendingApply; }
  const LiveKnobTurn& rps() const { return turn_; }

private:
  LiveKnobTurn turn_{"rps", /*acked_value=*/"", /*pending_value=*/"",
                     KnobTurnState::kCurrentAcked, /*note=*/""};
};

// The live knob-turn lines for the knobs tab: the rps turn rendered with its
// state marker (acked/pending/rejected/failed) and, outside the acked state,
// the inline reason or the pending transition; rps values carry the phase-
// total disclosure. Pure.
ftxui::Element RenderLiveKnobTurns(const LiveKnobTurn& rps_turn, int width, int height);

// ---------------------------------------------------------------------------
// M2: the staged next-phase preview (spec §4 knob state machine). While a
// run is active, edits to phase-gated knobs stage into a preview of the NEXT
// phase's config — visually distinct (every line tagged STAGED), discardable,
// and never applied to the current phase. The timeline marks nothing until
// the engine confirms the boundary; rps stages preview-only too until M3's
// ack path exists.
// ---------------------------------------------------------------------------

// The staged preview's knob lines: rps, concurrency, connections, queue
// bounds — the phase-gated vocabulary plus rps (staged preview-only until
// M3). Values come from `staged` (a PhaseSpec, T2's proto); a field the spec
// leaves unset renders the run-level value from `run_options` with an
// explicit " (inherit)" suffix (connections: the run-level value; per-phase
// whitelist inheritance per options.proto). All lines carry kStaged.
std::vector<KnobDisplay> NextPhasePreviewKnobs(const nighthawk::client::PhaseSpec& staged,
                                               const nighthawk::client::CommandLineOptions& run_options);

// The worker-set annotation between the current phase's active workers and
// the staged phase's count: growth -> "starts N new worker(s) (cold)" (the
// new workers start cold per Task 6's metadata — no prefetch pretense);
// shrink -> "M workers folded into phase k"; equality -> "".
std::string WorkerSetAnnotation(uint32_t current_workers, uint32_t staged_workers,
                                uint32_t next_phase_index);

// The staged next-phase preview block: a header naming the staged phase and
// stating the preview is discardable, the staged knob lines, and (when the
// worker count differs) the worker-set annotation. An empty staged spec name
// means NOTHING is staged — renders an empty element (the discarded state).
// Pure; the TuiApp wiring decides when a preview exists.
ftxui::Element RenderNextPhasePreview(const nighthawk::client::PhaseSpec& staged,
                                      const nighthawk::client::CommandLineOptions& run_options,
                                      uint32_t current_workers, uint32_t next_phase_index,
                                      int width, int height);

// The same block from already-built preview lines (the shell composes them
// once per frame via NextPhasePreviewKnobs): the discardable header, the
// STAGED-tagged lines, and the worker-set annotation when non-empty. Empty
// lines render an empty element.
ftxui::Element RenderNextPhasePreviewBlock(const std::vector<KnobDisplay>& preview_knobs,
                                           absl::string_view workers_annotation);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
