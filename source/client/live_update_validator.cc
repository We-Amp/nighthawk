#include "source/client/live_update_validator.h"

#include <algorithm>
#include <optional>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/time/time.h"

namespace Nighthawk {
namespace Client {
namespace {

// The proto oneof arm names, as they appear in rejection reasons.
constexpr absl::string_view kRpsField = "requests_per_second";
constexpr absl::string_view kRampField = "ramp";

absl::Status invalid(absl::string_view message) {
  return absl::InvalidArgumentError(std::string(message));
}

absl::Status duplicateDelta(int index, absl::string_view knob) {
  return invalid(absl::StrCat("deltas[", index, "]: duplicate delta for knob '", knob,
                              "'; a request may update each knob at most once (the order of "
                              "duplicates would be ambiguous)"));
}

std::chrono::nanoseconds durationProtoToNanos(const google::protobuf::Duration& duration) {
  // FORK-ONLY (We-Amp): T2 parked minor -- saturation guard. seconds() is an
  // int64 wire field; the chrono sum overflows for absurd values. Saturate
  // at nanoseconds::max()/min() so the callers' bound checks reject honestly
  // instead of wrapping into a bogus small value.
  constexpr int64_t kNanosPerSecond = 1000000000LL;
  constexpr int64_t kMaxSeconds =
      (std::chrono::nanoseconds::max().count() - (kNanosPerSecond - 1)) / kNanosPerSecond;
  const int64_t seconds = std::clamp<int64_t>(duration.seconds(), -kMaxSeconds, kMaxSeconds);
  return std::chrono::seconds(seconds) + std::chrono::nanoseconds(duration.nanos());
}

} // namespace

absl::Status ValidateUpdateDeltas(const nighthawk::client::UpdateRequest& request,
                                  const InFlightRunShape& run_shape) {
  bool rps_seen = false;
  bool ramp_seen = false;
  for (int i = 0; i < request.deltas_size(); ++i) {
    const nighthawk::client::KnobDelta& delta = request.deltas(i);
    const std::string where = absl::StrCat("deltas[", i, "]");
    switch (delta.delta_case()) {
    case nighthawk::client::KnobDelta::DELTA_NOT_SET:
      // Unknown/absent arms fail validation, never default. Knobs outside
      // the whitelist (queue bounds, connections, protocol, TLS, uri) have
      // no live arm at all (DQ-1), so an unset arm is the only way a
      // non-whitelisted delta can present itself on the wire.
      return invalid(absl::StrCat(
          where, ": no knob field is set; every delta must set exactly one knob from the "
                 "live-knob whitelist ({requests_per_second, ramp shape}); knobs outside "
                 "the whitelist are phase-gated or start-time-only and have no live arm"));
    case nighthawk::client::KnobDelta::kRequestsPerSecond: {
      if (rps_seen) {
        return duplicateDelta(i, kRpsField);
      }
      rps_seen = true;
      const uint32_t value = delta.requests_per_second().value();
      if (value < 1) {
        return invalid(absl::StrCat(where, ": ", kRpsField, ": rps must be >= 1, got ", value));
      }
      if (run_shape.ramp_plugin_configured) {
        // C1: on a ramp-plugin run the release schedule is a function of
        // the ramp's target frequency, so a live rps change IS ramp
        // re-parameterization. It must go through ramp re-anchoring, which
        // is not wired yet -- reject honestly, never silently mis-apply.
        return invalid(absl::StrCat(
            where, ": ", kRpsField,
            ": pending ramp re-anchoring; on a ramp-plugin run a live rps change sets the "
            "ramp's target frequency (a ramp-shape change) and requires ramp re-anchoring, "
            "which is not supported by this build yet"));
      }
      break;
    }
    case nighthawk::client::KnobDelta::kRamp: {
      if (ramp_seen) {
        return duplicateDelta(i, kRampField);
      }
      ramp_seen = true;
      if (!run_shape.ramp_plugin_configured) {
        return invalid(absl::StrCat(
            where, ": ", kRampField,
            ": ramp bends require the linear-ramping rate limiter plugin "
            "(nighthawk.linear-ramping-rate-limiter-plugin) to be configured for the run; "
            "only requests_per_second updates are available on this run"));
      }
      if (delta.ramp().has_ramp_time()) {
        const std::chrono::nanoseconds ramp_time = durationProtoToNanos(delta.ramp().ramp_time());
        if (ramp_time <= std::chrono::nanoseconds::zero()) {
          return invalid(absl::StrCat(where, ": ", kRampField,
                                      ".ramp_time: ramp time must be positive, got ",
                                      ramp_time.count(), "ns"));
        }
        if (run_shape.remaining_duration.has_value() && ramp_time > *run_shape.remaining_duration) {
          // Ramp-time-within-current-phase (the M2 ruling): the limiter is
          // per-phase, so the new ramp must fit the CURRENT phase's
          // remaining duration; in single-phase/legacy runs this bound is
          // the remaining run duration.
          const absl::string_view bound_name = run_shape.has_phase_vector
                                                   ? "the current phase's remaining duration"
                                                   : "the run's remaining duration";
          return invalid(absl::StrCat(
              where, ": ", kRampField, ".ramp_time: ramp time ",
              absl::FormatDuration(absl::FromChrono(ramp_time)), " exceeds ", bound_name, " (",
              absl::FormatDuration(absl::FromChrono(*run_shape.remaining_duration)), ")"));
        }
      }
      if (delta.ramp().has_target_rps() && delta.ramp().target_rps().value() < 1) {
        return invalid(absl::StrCat(where, ": ", kRampField,
                                    ".target_rps: target rps must be >= 1, got ",
                                    delta.ramp().target_rps().value()));
      }
      break;
    }
    default:
      // FORK-ONLY (We-Amp): T2 parked minor -- runtime fail-closed. Unreachable
      // on this proto's wire today (a delta case this build does not know
      // presents as DELTA_NOT_SET), but a future enum arm must land in the
      // same whitelist gate, never silently default.
      return invalid(absl::StrCat(
          where, ": unknown knob delta; it is not in the live-knob whitelist "
                 "({requests_per_second, ramp shape})"));
    }
  }
  return absl::OkStatus();
}

} // namespace Client
} // namespace Nighthawk
