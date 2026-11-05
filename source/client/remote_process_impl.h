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
  // Safe from any thread; the single-run-at-a-time contract that run()
  // already imposes on callers makes the handle member's access race-free by
  // construction.
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
  // Armed by run() for the duration of the blocking remote execution; the
  // cancellation arm goes through it from any thread (requestExecutionCancellation).
  std::unique_ptr<NighthawkServiceCancellationHandle> cancellation_handle_;
};

} // namespace Client
} // namespace Nighthawk
