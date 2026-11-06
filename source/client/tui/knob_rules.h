#pragma once

#include "api/client/options.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the proto-pure knob rules shared by the TUI's knob pane
// and the MCP steering tools (which cannot link knob_pane — it is FTXUI-
// bound). No threading, no rendering, no FTXUI; quarantined like
// client_headless (the standing cquery guard covers the nighthawk_mcp graph).

// Whether an rps turn on this run is a ramp bend (C1): the engine treats a
// live rps delta as ramp re-parameterization ONLY on a run configured with
// the linear-ramping rate limiter plugin — the exact name the engine's
// whitelist validator and fan-out admit on ("nighthawk.linear-ramping-
// rate-limiter-plugin"). Any other named limiter plugin does NOT bend
// (the session record must not claim a bend that never happened). Pure.
bool RpsTurnBendsRamp(const nighthawk::client::CommandLineOptions& options);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
