#pragma once

#include <memory>

#include "nighthawk/client/options.h"
#include "nighthawk/client/output_collector.h"
#include "nighthawk/client/process.h"
#include "nighthawk/common/nighthawk_service_client.h"

#include "source/common/common/logger.h"
#include "source/common/nighthawk_service_client_impl.h"

#include "api/client/service.grpc.pb.h"

namespace Nighthawk {
namespace Client {

/**
 * Will delegate execution to a remote nighthawk_service using gRPC.
 */
class RemoteProcessImpl : public Process, public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * @param options Options to send to the remote nighthawk service, as well as
   * containing information to connect to it (which won't be forwarded).
   * @param stub Stub that will be used to communicate with the remote
   * gRPC server.
   */
  RemoteProcessImpl(const Options& options, nighthawk::client::NighthawkService::Stub& stub);
  /**
   * @param collector Collects the output from the remote nighthawk service.
   * @return true iff the remote execution should be considered successful. Unsuccessful execution
   * will log available error details.
   */
  bool run(OutputCollector& collector) override;
  /**
   * Shuts down the service, a no-op in this implementation.
   */
  void shutdown() override {};

  // FORK-ONLY (We-Amp): M3 Task 7 (#380) — writes the CancellationRequest arm
  // on the in-flight run's stream (previously a logged no-op). The explicit
  // arm keeps the stream alive: the run ends and its final response is still
  // delivered to run()'s blocking read, which returns the (shorter) run's
  // output. Returns false when no run is in flight (or the write failed).
  // Safe from any thread at any point in — or between — runs: the handle
  // member below is created once for this object's lifetime and never
  // re-armed or reset from run(), so this path never touches a mutating
  // member (the write closure it reaches is armed/disarmed under the
  // handle's own mutex inside the benchmark).
  bool requestExecutionCancellation() override;

  // FORK-ONLY (We-Amp): live snapshot aggregation is a local-process feature;
  // a remote process produces no local snapshots. Live snapshot streaming of a
  // remote run is not supported in v0, and the service client used here
  // RELEASE_ASSERTs on more than one response anyway.
  nighthawk::client::LiveSnapshot toLiveSnapshot() override { return {}; }

private:
  const Options& options_;
  const std::unique_ptr<NighthawkServiceClientImpl> service_client_;
  nighthawk::client::NighthawkService::Stub& stub_;
  // FORK-ONLY (We-Amp): created once for this object's lifetime — never
  // assigned or reset after construction — so requestExecutionCancellation()
  // (called from the signal-handler dispatch thread) has no member mutation
  // to race, whatever the run lifecycle is doing. Arm/disarm of the write
  // closure happens inside
  // NighthawkServiceClientImpl::PerformNighthawkBenchmarkWithCancellation,
  // under the handle's own mutex: a call outside an in-flight run is a safe
  // false, a call during flight writes the arm, and a call landing in the
  // arming or teardown window reads a valid (disarmed) handle.
  const std::unique_ptr<NighthawkServiceCancellationHandle> cancellation_handle_{
      std::make_unique<NighthawkServiceCancellationHandle>()};
};

} // namespace Client
} // namespace Nighthawk
