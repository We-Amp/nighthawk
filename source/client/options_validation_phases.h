#pragma once

// FORK-ONLY (We-Amp): pure validation for the phase vector on
// CommandLineOptions (M2 A1). No Nighthawk state is consulted or mutated; the
// function inspects the proto and reports the first violation found. Every
// error names the offending phase index (0-based, matching the repeated
// field's order).
//
// Rules:
// - 0 phases: legacy run, always valid.
// - Mutual exclusion: whenever `phases` is present (one entry included), the
//   top-level duration and concurrency options are rejected -- never silently
//   ignored. Per-phase durations and concurrency live in the PhaseSpec
//   entries.
// - 1 phase: legacy-equivalent run (a one-element vector); no per-phase
//   validation beyond the mutual exclusion above.
// - 2 or more phases: each phase must carry a unique non-empty name, a
//   duration >= 1s and an rps >= 1. A phase that declares concurrency must
//   declare it as a positive integer, and it may not exceed the maximum
//   concurrency declared over all phases (that maximum is the worker count
//   the process bootstraps with).
// - The PhaseSpec is a whitelist subset (DQ-2 ruling): it carries no
//   protocol/TLS/uri fields, so a cross-protocol phase vector is
//   unrepresentable. Per-phase connections/in-flight limits inherit the
//   run-level values when absent; no cross-protocol validation is needed.

#include "absl/status/status.h"

#include "api/client/options.pb.h"

namespace Nighthawk {
namespace Client {

// Validates the phase vector of `options`. Returns OkStatus for legacy
// (vector-less) runs, single-phase runs and valid multi-phase vectors;
// InvalidArgumentError with a phase-index-bearing message otherwise.
absl::Status validatePhaseVector(const nighthawk::client::CommandLineOptions& options);

} // namespace Client
} // namespace Nighthawk
