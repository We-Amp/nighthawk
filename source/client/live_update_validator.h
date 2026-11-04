#pragma once

// FORK-ONLY (We-Amp): service-side live-knob whitelist validation (M3 Task 2,
// spec 4 A2 / 3). Pure: consumes only the proto deltas and the in-flight
// run's declared shape -- no engine types, no engine access. The validator is
// the ONLY authority on which knobs may mutate live: a client (including our
// own TUI) can never mutate a non-whitelisted knob live, and every rejection
// carries a reason naming the offending field.

#include <chrono>
#include <optional>
#include <string>

#include "absl/status/status.h"

#include "api/client/service.pb.h"

namespace Nighthawk {
namespace Client {

// The in-flight run's declared shape, as whitelist validation sees it. Built
// by the engine (ProcessImpl::liveUpdateRunShape()) and handed to the update
// orchestrator; never derived from the update request itself.
struct InFlightRunShape {
  // The run declares a phase vector (M2): the remaining-duration bound is the
  // CURRENT phase's remaining duration.
  bool has_phase_vector{false};
  // The run's rate limiter is the linear-ramping plugin: ramp bends apply,
  // and a live rps delta is a ramp-shape change (it sets the ramp's target
  // frequency, C1).
  bool ramp_plugin_configured{false};
  // The engine's current phase id (the orchestrator checks request targeting
  // against it separately; validation only carries it for context).
  std::string current_phase_id;
  // Remaining time a ramp delta's new ramp time must fit within: the CURRENT
  // phase's remaining duration for phase-vector runs (the limiter is
  // per-phase); the remaining RUN duration for legacy/single-phase runs (the
  // same bound the ramp limiter factory enforces at construction, which
  // remains the run-level backstop). nullopt = unbounded (--no-duration or
  // not yet started).
  std::optional<std::chrono::nanoseconds> remaining_duration;
};

// Validates an update request's knob deltas against the live-knob whitelist
// ({requests_per_second, ramp shape} -- the DQ-1 ruling; queue bounds,
// connections, protocol, TLS and uri have NO live arm: they are phase-gated
// or start-time-only). Returns OK when every delta is whitelisted and
// satisfies its knob's invariants; InvalidArgument whose message names the
// offending field and the reason otherwise. A rejected request must never
// reach the engine apply path.
//
// Note: this returns OK/InvalidArgument only; the normalized delta set the
// apply path will consume is assembled by the task that first needs it (the
// capability fan-out), not by validation.
absl::Status ValidateUpdateDeltas(const nighthawk::client::UpdateRequest& request,
                                  const InFlightRunShape& run_shape);

} // namespace Client
} // namespace Nighthawk
