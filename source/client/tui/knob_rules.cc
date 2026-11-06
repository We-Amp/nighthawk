#include "source/client/tui/knob_rules.h"

#include "absl/strings/match.h"
#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

bool RpsTurnBendsRamp(const nighthawk::client::CommandLineOptions& options) {
  // The engine's C1 rule verbatim (live_update_validator / process_impl):
  // only the linear-ramping rate limiter's runs bend, and on them an rps
  // delta IS the ramp re-parameterization.
  static constexpr absl::string_view kRampingPluginName =
      "nighthawk.linear-ramping-rate-limiter-plugin";
  return options.has_rate_limiter_plugin_config() &&
         options.rate_limiter_plugin_config().name() == kRampingPluginName;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
