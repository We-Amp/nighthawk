#include "source/client/tui/start_run_dialog.h"

#include "absl/strings/ascii.h"

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

void StartRunDialogModel::set_phases(std::vector<nighthawk::client::PhaseSpec> phases) {
  phases_ = std::move(phases);
}

bool StartRunDialogModel::valid() const {
  if (uri_.empty() || rps_ < 1) {
    return false;
  }
  if (phases_.empty()) {
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
  if (phases_.empty()) {
    options->mutable_duration()->set_seconds(duration_seconds_);
  } else {
    // Phase run: the vector replaces the mutually-exclusive top-level
    // duration (Task 2's rules, enforced here by construction — the
    // duration field is never written alongside a vector).
    *options->mutable_phases() = {phases_.begin(), phases_.end()};
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
    if (!dialog.phases_.empty()) {
      // The vector replaces this field; a failing vector surfaces here with
      // the reused validation's message (it names the offending phase).
      const absl::Status validated = validatePhaseVector(dialog.build_request().options());
      return validated.ok() ? "" : std::string(validated.message());
    }
    return dialog.duration_seconds_ >= 1 ? "" : "duration (s): must be > 0";
  default:
    return "";
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
