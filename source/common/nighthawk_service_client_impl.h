#pragma once

#include <functional>

#include "nighthawk/common/nighthawk_service_client.h"

#include "absl/synchronization/mutex.h"

#include "source/common/common/logger.h"
#include "source/common/common/statusor.h"
#include "source/common/protobuf/protobuf.h"

#include "api/client/options.pb.h"
#include "api/client/service.grpc.pb.h"

namespace Nighthawk {

// FORK-ONLY (We-Amp): M3 Task 7's remote-cancellation seam (#380, the shape
// DQ-4b confirmed sound by review: the stream handle outlives the blocking
// read; the parallel-control-stub alternative stays rejected). A
// caller-created handle that stays valid across
// NighthawkServiceClientImpl::PerformNighthawkBenchmarkWithCancellation's
// blocking read: while the benchmark is in flight, another thread calls
// RequestCancellation() to write a CancellationRequest arm on the run's
// stream. Unlike cancelling the gRPC context, the explicit arm keeps the
// stream alive, so the run's final response is still delivered to the
// blocked read — the single-response contract survives cancellation.
// Callers that never cancel never create one of these, and their
// PerformNighthawkBenchmark path is byte-identical to the pre-M3 code.
class NighthawkServiceCancellationHandle {
public:
  // Writes the CancellationRequest arm on the in-flight benchmark's stream.
  // Safe from any thread, any number of times; returns false when no
  // benchmark is currently in flight on this handle (never started, already
  // finished, or the write failed). Never blocks on the benchmark itself.
  bool RequestCancellation();

private:
  friend class NighthawkServiceClientImpl;
  // Set while the benchmark's stream is live, cleared under the same mutex
  // before the read thread performs its post-run writes (WritesDone), so a
  // concurrent arm write can never race them. The write goes through a
  // stored closure so this header needs no gRPC stream types.
  absl::Mutex mutex_;
  std::function<bool(const nighthawk::client::CancellationRequest&)> write_arm_
      ABSL_GUARDED_BY(mutex_);
};

/**
 * Real implementation of a helper that opens a channel with the gRPC stub, sends the input, and
 * translates the output or errors into a StatusOr.
 *
 * This class is stateless and may be called from multiple threads. Furthermore, the same gRPC stub
 * is safe to use from multiple threads simultaneously.
 */
class NighthawkServiceClientImpl : public NighthawkServiceClient,
                                   public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  absl::StatusOr<nighthawk::client::ExecutionResponse> PerformNighthawkBenchmark(
      nighthawk::client::NighthawkService::StubInterface* nighthawk_service_stub,
      const nighthawk::client::CommandLineOptions& command_line_options) const override;

  // FORK-ONLY (We-Amp): the cancellation seam of M3 Task 7 (#380). Identical
  // to PerformNighthawkBenchmark except that (a) a non-null
  // |cancellation_handle| is armed with the stream so another thread can
  // write the CancellationRequest arm mid-run via
  // NighthawkServiceCancellationHandle::RequestCancellation, and (b) the
  // client half-close (WritesDone) is deferred until AFTER the blocking read
  // — a mid-run half-close would make every later write, including the
  // cancellation arm, a gRPC error (the same DQ-2 consequence RunFeed's
  // update channel lives under; see the Task 1 report's bidi diagnosis:
  // arms are only readable while the client has not half-closed). The
  // single-response contract is unchanged: the service sends no ack for an
  // accepted cancellation (an update_result from a cancellation arm can only
  // be a targeting rejection, which an empty execution id — the only form
  // the remote caller sends — can never produce; defensively, such a stray
  // ack is logged and skipped, never returned as THE response).
  absl::StatusOr<nighthawk::client::ExecutionResponse> PerformNighthawkBenchmarkWithCancellation(
      nighthawk::client::NighthawkService::StubInterface* nighthawk_service_stub,
      const nighthawk::client::CommandLineOptions& command_line_options,
      NighthawkServiceCancellationHandle* cancellation_handle) const;
};

} // namespace Nighthawk
