#pragma once

#include <string>
#include <vector>

#include "ftxui/dom/elements.hpp"

#include "api/client/options.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the display-only knob pane (M0b). It renders the knob
// vocabulary with its spec §3 steering split — the live steering set is
// rps + ramp (M3); everything else is phase-gated — without implementing any
// steering. A visible banner states that steering lands in M3, so the pane can
// never masquerade as a control surface. Staged-edit/preview/ack states arrive
// with M2/M3.

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

// The knobs tab (tab 1): a display-only banner row followed by one line per
// knob. `width`/`height` are the pane-local dimensions (terminal size minus
// the status bar); a non-positive dimension renders an empty pane.
ftxui::Element RenderKnobPane(const std::vector<KnobDisplay>& knobs, int width, int height);

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

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
