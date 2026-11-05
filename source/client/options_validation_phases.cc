#include "source/client/options_validation_phases.h"

#include <algorithm>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {

namespace {

absl::Status phaseError(int index, const absl::string_view message) {
  return absl::InvalidArgumentError(absl::StrCat("phase[", index, "]: ", message));
}

// Parses a per-phase concurrency declaration ("2", "4", ...). Rejects
// 'auto', non-numeric and non-positive values: the bootstrap worker count is
// derived from the declared phase concurrencies, so each declaration must be
// a concrete number.
absl::StatusOr<uint32_t> parsePhaseConcurrency(int index,
                                               const nighthawk::client::PhaseSpec& phase) {
  uint32_t value = 0;
  if (!absl::SimpleAtoi(phase.concurrency().value(), &value) || value == 0) {
    return phaseError(index,
                      "concurrency must be a positive integer ('auto' is not valid for a phase; "
                      "the worker count is derived from the declared phase concurrencies)");
  }
  return value;
}

} // namespace

absl::Status validatePhaseVector(const nighthawk::client::CommandLineOptions& options) {
  const int phase_count = options.phases_size();
  if (phase_count == 0) {
    // No vector: legacy run, identical to today.
    return absl::OkStatus();
  }

  // Mutual exclusion, fail fast with text naming the conflict: when the phase
  // vector is present, the top-level duration and concurrency are rejected --
  // never silently ignored.
  if (options.has_duration()) {
    return absl::InvalidArgumentError(
        "phases and the top-level duration are mutually exclusive; specify "
        "per-phase durations on the PhaseSpec entries instead");
  }
  if (options.has_concurrency()) {
    return absl::InvalidArgumentError(
        "phases and the top-level concurrency are mutually exclusive; specify "
        "per-phase concurrency on the PhaseSpec entries instead");
  }
  // The no_duration arm shares the duration oneof, so phases + no_duration is
  // a well-formed proto that would slip past the duration check -- and
  // silently drop the mandatory per-phase durations (no termination predicate
  // ever fires on duration, every phase boundary censors, and the final phase
  // hangs the run until an external cancel). Fail fast the same way.
  if (options.has_no_duration()) {
    return absl::InvalidArgumentError(
        "phases and the top-level no_duration are mutually exclusive; a phase "
        "vector requires per-phase durations on the PhaseSpec entries instead");
  }

  // A single phase is legacy-equivalent run-wise (a one-element vector,
  // legacy-shaped output), but its declared fields are validated like any
  // other phase: a malformed one-element vector must fail fast, not silently
  // run with a zero duration or zero rps. The >=2-only checks (concurrency
  // within the max over phases, name uniqueness) degrade to trivially true
  // for a single entry, so the loop below can run unconditionally.

  // Per-phase concurrency declarations must be concrete positive integers and
  // may not exceed the maximum declared over all phases (that maximum is the
  // bootstrap worker count).
  std::vector<uint32_t> declared_concurrency(static_cast<size_t>(phase_count), 0);
  uint32_t max_concurrency = 0;
  for (int i = 0; i < phase_count; ++i) {
    const nighthawk::client::PhaseSpec& phase = options.phases(i);
    if (phase.has_concurrency()) {
      absl::StatusOr<uint32_t> value = parsePhaseConcurrency(i, phase);
      if (!value.ok()) {
        return value.status();
      }
      declared_concurrency[i] = *value;
      max_concurrency = std::max(max_concurrency, *value);
    }
  }

  for (int i = 0; i < phase_count; ++i) {
    const nighthawk::client::PhaseSpec& phase = options.phases(i);
    if (phase.name().empty()) {
      return phaseError(i, "name must be non-empty; phase ids derive from it");
    }
    if (phase.duration().seconds() < 1) {
      return phaseError(i, "duration must be >= 1s");
    }
    if (phase.requests_per_second() < 1) {
      return phaseError(i, "requests_per_second must be >= 1");
    }
    if (declared_concurrency[i] > max_concurrency) {
      return phaseError(i, "concurrency exceeds the maximum concurrency declared over all "
                           "phases (that maximum is the bootstrap worker count)");
    }
  }

  // Names must be unique across the vector.
  absl::flat_hash_set<std::string> seen;
  for (int i = 0; i < phase_count; ++i) {
    if (!seen.insert(options.phases(i).name()).second) {
      return phaseError(i, absl::StrCat("duplicate phase name '", options.phases(i).name(),
                                        "'; phase ids derive from names and must be unique"));
    }
  }

  return absl::OkStatus();
}

} // namespace Client
} // namespace Nighthawk
