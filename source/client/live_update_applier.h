#pragma once

// FORK-ONLY (We-Amp): the engine-apply seam of the live-update path (M3).
// Extracted from live_update_orchestrator.h into its own header so both
// sides of the dependency graph can name it: the service-side orchestrator
// (nighthawk_service_lib) invokes it, the process-side capability fan-out
// (nighthawk_client_lib, M3 Task 6) implements it -- one apply surface, no
// target-level cycle.

#include <cstdint>
#include <string>

#include "absl/time/time.h"

#include "api/client/service.pb.h"

namespace Nighthawk {
namespace Client {

// The engine clock pair at one instant (spec 8.5 discipline, the same pair
// PhaseEvent emits).
struct EngineInstant {
  int64_t wall_time_unix_ns;
  int64_t monotonic_ns;
};

// The default engine clock: (wall, monotonic) ns pair captured at now.
EngineInstant DefaultEngineInstant();

// The engine-apply step of an update. The Task-6 process-side fan-out is the
// production implementation (LiveCapabilityFanOutApplier); tests substitute
// fakes at this seam.
class LiveUpdateApplier {
public:
  virtual ~LiveUpdateApplier() = default;
  // Applies the request's deltas to the engine. Called on a short-lived
  // orchestrator thread, never on the stream reader thread. MUST honor
  // `deadline`: on expiry it resolves the update as a rejection instead of
  // blocking past it.
  virtual nighthawk::client::UpdateResult
  ApplyDeltas(const nighthawk::client::UpdateRequest& request, const std::string& epoch_id,
              const EngineInstant& bend_instant, absl::Time deadline) = 0;
};

} // namespace Client
} // namespace Nighthawk
