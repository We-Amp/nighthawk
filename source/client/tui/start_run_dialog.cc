#include "source/client/tui/start_run_dialog.h"

#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"

#include "source/client/options_validation_phases.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Trims surrounding whitespace so a fat-fingered submit still works, and a
// whitespace-only field lands on the invalid empty-uri path (routed from the
// Task 5 review).
void StartRunDialogModel::set_uri(std::string uri) {
  uri_ = std::string(absl::StripAsciiWhitespace(uri));
}

void StartRunDialogModel::set_rps(uint32_t rps) { rps_ = rps; }

void StartRunDialogModel::set_duration_seconds(uint32_t seconds) { duration_seconds_ = seconds; }

void StartRunDialogModel::set_connections(uint32_t connections) { connections_ = connections; }

void StartRunDialogModel::set_phases(std::vector<nighthawk::client::PhaseSpec> phases) {
  phases_ = std::move(phases);
}

void StartRunDialogModel::set_phases_field(std::string text) {
  phases_field_ = std::string(absl::StripAsciiWhitespace(text));
}

absl::StatusOr<std::vector<nighthawk::client::PhaseSpec>>
StartRunDialogModel::ParsePhasesField(absl::string_view text) {
  std::vector<nighthawk::client::PhaseSpec> phases;
  for (const absl::string_view& raw_entry : absl::StrSplit(text, absl::ByAnyChar(",\n;"),
                                                           absl::SkipEmpty())) {
    const std::string entry(absl::StripAsciiWhitespace(raw_entry));
    if (entry.empty()) {
      continue;
    }
    const std::vector<std::string> parts = absl::StrSplit(entry, ':');
    if (parts.size() < 3 || parts.size() > 5) {
      return absl::InvalidArgumentError(absl::StrCat(
          "phase entry \"", entry,
          "\": expected name:rps:duration[:concurrency[:connections]]"));
    }
    nighthawk::client::PhaseSpec phase;
    phase.set_name(parts[0]);
    uint32_t rps = 0;
    uint32_t duration = 0;
    if (!absl::SimpleAtoi(parts[1], &rps) || rps < 1) {
      return absl::InvalidArgumentError(
          absl::StrCat("phase entry \"", entry, "\": rps must be a positive integer"));
    }
    phase.set_requests_per_second(rps);
    if (!absl::SimpleAtoi(parts[2], &duration) || duration < 1) {
      return absl::InvalidArgumentError(
          absl::StrCat("phase entry \"", entry, "\": duration must be a positive integer"));
    }
    phase.mutable_duration()->set_seconds(duration);
    if (parts.size() >= 4) {
      uint32_t workers = 0;
      if (!absl::SimpleAtoi(parts[3], &workers) || workers < 1) {
        return absl::InvalidArgumentError(
            absl::StrCat("phase entry \"", entry, "\": concurrency must be a positive integer"));
      }
      phase.mutable_concurrency()->set_value(std::to_string(workers));
    }
    if (parts.size() >= 5) {
      uint32_t connections = 0;
      if (!absl::SimpleAtoi(parts[4], &connections) || connections < 1) {
        return absl::InvalidArgumentError(
            absl::StrCat("phase entry \"", entry, "\": connections must be a positive integer"));
      }
      phase.mutable_connections()->set_value(connections);
    }
    phases.push_back(std::move(phase));
  }
  if (phases.empty()) {
    return absl::InvalidArgumentError("no phase entries found");
  }
  return phases;
}

bool StartRunDialogModel::valid() const {
  if (uri_.empty() || rps_ < 1) {
    return false;
  }
  // The field must parse when non-empty — a malformed draft never starts.
  if (!phases_field_.empty() && !ParsePhasesField(phases_field_).ok()) {
    return false;
  }
  if (phases_field_.empty() && phases_.empty()) {
    return duration_seconds_ >= 1;
  }
  // Phase run: the top-level duration is replaced by the vector; the vector
  // itself must pass the reused pure validation.
  return validatePhaseVector(build_request().options()).ok();
}

nighthawk::client::StartRequest StartRunDialogModel::build_request() const {
  nighthawk::client::StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_uri()->set_value(uri_);
  options->mutable_requests_per_second()->set_value(rps_);
  // The field is the production source; the programmatic set_phases seam
  // serves tests. An unparsable field builds a legacy request (valid()
  // refuses it before a start can happen — and FieldError says why).
  std::vector<nighthawk::client::PhaseSpec> phases = phases_;
  if (!phases_field_.empty()) {
    if (auto parsed = ParsePhasesField(phases_field_); parsed.ok()) {
      phases = *std::move(parsed);
    } else {
      phases.clear();
    }
  }
  if (phases.empty()) {
    options->mutable_duration()->set_seconds(duration_seconds_);
  } else {
    // Phase run: the vector replaces the mutually-exclusive top-level
    // duration (Task 2's rules, enforced here by construction — the
    // duration field is never written alongside a vector).
    *options->mutable_phases() = {phases.begin(), phases.end()};
  }
  // The steering surface's connection count lands directly on the options
  // (the dialog has no field for it); 0 stays unset — the engine default.
  if (connections_ > 0) {
    options->mutable_connections()->set_value(connections_);
  }
  // Presence of live_streaming is the opt-in marker; stats_flush_interval
  // defaults to 1s (the proto bounds it to [1s, 300s]).
  options->mutable_live_streaming();
  options->mutable_stats_flush_interval()->set_value(1);
  return request;
}

std::string StartRunDialogModel::FieldError(const StartRunDialogModel& dialog, int field) {
  switch (field) {
  case kUri:
    return dialog.uri_.empty() ? "uri: must not be empty" : "";
  case kRps:
    return dialog.rps_ >= 1 ? "" : "rps: must be > 0";
  case kDuration:
    if (!dialog.phases_field_.empty() || !dialog.phases_.empty()) {
      // The vector replaces this field: no duration requirement at all.
      return "";
    }
    return dialog.duration_seconds_ >= 1 ? "" : "duration (s): must be > 0";
  case kPhases: {
    // The effective vector: the parsed field when non-empty, else the
    // programmatic seam's vector.
    std::vector<nighthawk::client::PhaseSpec> phases;
    if (!dialog.phases_field_.empty()) {
      auto parsed = ParsePhasesField(dialog.phases_field_);
      if (!parsed.ok()) {
        return absl::StrCat("phases: ", parsed.status().message());
      }
      phases = *std::move(parsed);
    } else {
      phases = dialog.phases_;
    }
    if (phases.empty()) {
      return "";
    }
    // Parsed: the reused validation names the offending phase when it
    // rejects the vector.
    const absl::Status validated = validatePhaseVector([&] {
      nighthawk::client::CommandLineOptions options;
      options.mutable_uri()->set_value(dialog.uri_);
      *options.mutable_phases() = {phases.begin(), phases.end()};
      return options;
    }());
    return validated.ok() ? "" : std::string(validated.message());
  }
  default:
    return "";
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
