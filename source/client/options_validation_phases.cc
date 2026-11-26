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

  // NH1: per-worker rate weights. The count must match the phase's worker
  // count (declared, else the maximum over the phases, else 1 -- the same
  // rule determineConcurrency and computeWorkerSubsets apply), and the
  // phase's rate must split into whole per-worker rates.
  for (int i = 0; i < phase_count; ++i) {
    const nighthawk::client::PhaseSpec& phase = options.phases(i);
    if (phase.worker_weights().empty()) {
      continue;
    }
    const uint32_t workers = declared_concurrency[i] > 0
                                 ? declared_concurrency[i]
                                 : (max_concurrency > 0 ? max_concurrency : 1);
    if (static_cast<uint32_t>(phase.worker_weights_size()) != workers) {
      return phaseError(i, absl::StrCat("worker_weights has ", phase.worker_weights_size(),
                                        " entries; it must have one per worker of the phase (",
                                        workers, ")"));
    }
    const absl::Status split = checkWeightedSplit(
        phase.requests_per_second(),
        absl::MakeConstSpan(phase.worker_weights().data(), phase.worker_weights_size()));
    if (!split.ok()) {
      return phaseError(i, split.message());
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

absl::Status checkWeightedSplit(uint32_t rps, absl::Span<const uint32_t> weights) {
  if (weights.empty()) {
    return absl::InvalidArgumentError("worker_weights must not be empty");
  }
  uint64_t sum = 0;
  for (size_t w = 0; w < weights.size(); ++w) {
    if (weights[w] < 1) {
      return absl::InvalidArgumentError(
          absl::StrCat("worker_weights[", w, "] is 0; every weight must be >= 1"));
    }
    sum += weights[w];
  }
  for (size_t w = 0; w < weights.size(); ++w) {
    const uint64_t scaled = static_cast<uint64_t>(rps) * weights[w];
    if (scaled % sum != 0) {
      return absl::InvalidArgumentError(
          absl::StrCat("requests_per_second ", rps,
                       " does not split into whole per-worker rates by "
                       "worker_weights (sum ",
                       sum, "): worker ", w, " would get ", rps, " x ", weights[w], " / ", sum,
                       ", not a whole number; choose a rate where rate x weight is divisible by "
                       "the weight sum for every worker"));
    }
  }
  return absl::OkStatus();
}

std::vector<uint32_t> perWorkerRates(uint32_t rps, uint32_t active_workers,
                                     absl::Span<const uint32_t> weights) {
  std::vector<uint32_t> rates(active_workers, 0);
  if (!weights.empty() && weights.size() == active_workers) {
    uint64_t sum = 0;
    for (const uint32_t weight : weights) {
      sum += weight;
    }
    for (uint32_t w = 0; w < active_workers; ++w) {
      rates[w] = static_cast<uint32_t>(static_cast<uint64_t>(rps) * weights[w] / sum);
    }
    return rates;
  }
  const uint32_t base = rps / active_workers;
  const uint32_t remainder = rps % active_workers;
  for (uint32_t w = 0; w < active_workers; ++w) {
    rates[w] = base > 0 ? base + (w < remainder ? 1u : 0u) : 1u;
  }
  return rates;
}

} // namespace Client
} // namespace Nighthawk
