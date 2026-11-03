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
// engine (spec §3 ruling).
enum class KnobSteering {
  kLiveM3,
  kPhaseGated,
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

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
