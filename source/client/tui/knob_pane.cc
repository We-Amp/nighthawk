#include "source/client/tui/knob_pane.h"

#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr const char* kUnsetValue = "unset";
constexpr const char* kRampUnconfigured = "not configured";
constexpr const char* kLiveTag = "LIVE (M3)";
constexpr const char* kPhaseGatedTag = "PHASE-GATED";
// Column the value starts at (the name field's fixed width); the steering tag
// trails the value after a second fixed-width column.
constexpr int kNameColumn = 14;
constexpr int kValueColumn = 44;

const char* steeringTag(KnobSteering steering) {
  return steering == KnobSteering::kLiveM3 ? kLiveTag : kPhaseGatedTag;
}

// Fixed-width columns keep the knob lines tabular; the steering tag lands at a
// stable column as long as values stay under kValueColumn, and still renders
// readable when one does not.
std::string knobLine(const KnobDisplay& knob) {
  return absl::StrFormat("%-*s%-*s%s", kNameColumn, knob.name, kValueColumn, knob.value,
                         steeringTag(knob.steering));
}

} // namespace

std::vector<KnobDisplay> KnobsFromOptions(const nighthawk::client::CommandLineOptions& options) {
  std::vector<KnobDisplay> knobs;

  // Spec §3 live set, knob 1: requests_per_second.
  knobs.push_back({"rps",
                   options.has_requests_per_second()
                       ? absl::StrCat(options.requests_per_second().value())
                       : kUnsetValue,
                   KnobSteering::kLiveM3});

  // Spec §3 live set, knob 2: the ramp, carried by rate_limiter_plugin_config
  // (the linear-ramping-rate-limiter plugin is M3's ramping vehicle). A
  // present-but-empty plugin name is treated the same as absent: the vocabulary
  // has no bare "configured" value.
  if (options.has_rate_limiter_plugin_config()) {
    const std::string& plugin_name = options.rate_limiter_plugin_config().name();
    knobs.push_back({"ramp", plugin_name.empty() ? std::string(kRampUnconfigured) : plugin_name,
                     KnobSteering::kLiveM3});
  } else {
    knobs.push_back({"ramp", kRampUnconfigured, KnobSteering::kLiveM3});
  }

  // Phase-gated set (M2/M3 phase-engine territory).
  knobs.push_back({"concurrency",
                   options.has_concurrency() && !options.concurrency().value().empty()
                       ? options.concurrency().value()
                       : kUnsetValue,
                   KnobSteering::kPhaseGated});
  knobs.push_back(
      {"connections",
       options.has_connections() ? absl::StrCat(options.connections().value()) : kUnsetValue,
       KnobSteering::kPhaseGated});

  // Queue bounds fold the two limiter knobs into one line; each half says
  // "unset" individually so a partially set pair still reads exactly.
  std::string queue_bounds;
  if (!options.has_max_pending_requests() && !options.has_max_active_requests()) {
    queue_bounds = kUnsetValue;
  } else {
    queue_bounds = absl::StrCat(
        "pending=",
        options.has_max_pending_requests() ? absl::StrCat(options.max_pending_requests().value())
                                           : kUnsetValue,
        " active=",
        options.has_max_active_requests() ? absl::StrCat(options.max_active_requests().value())
                                          : kUnsetValue);
  }
  knobs.push_back({"queue bounds", queue_bounds, KnobSteering::kPhaseGated});

  return knobs;
}

ftxui::Element RenderKnobPane(const std::vector<KnobDisplay>& knobs, int width, int height) {
  if (width <= 0 || height <= 0) {
    return ftxui::emptyElement();
  }
  std::vector<ftxui::Element> rows;
  // The display-only banner: the pane states the steering milestone so no
  // hidden timing semantics can masquerade as implemented controls (spec §3).
  rows.push_back(ftxui::text("knobs (display-only; steering lands in M3)"));
  for (const KnobDisplay& knob : knobs) {
    rows.push_back(ftxui::text(knobLine(knob)));
  }
  return ftxui::vbox(std::move(rows));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
