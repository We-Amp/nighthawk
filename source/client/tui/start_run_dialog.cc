#include "source/client/tui/start_run_dialog.h"

#include "absl/strings/ascii.h"

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

bool StartRunDialogModel::valid() const {
  return !uri_.empty() && rps_ >= 1 && duration_seconds_ >= 1;
}

nighthawk::client::StartRequest StartRunDialogModel::build_request() const {
  nighthawk::client::StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_uri()->set_value(uri_);
  options->mutable_requests_per_second()->set_value(rps_);
  options->mutable_duration()->set_seconds(duration_seconds_);
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
    return dialog.duration_seconds_ >= 1 ? "" : "duration (s): must be > 0";
  default:
    return "";
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
