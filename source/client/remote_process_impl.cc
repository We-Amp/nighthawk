#include "source/client/remote_process_impl.h"

#include <grpc++/grpc++.h>

#include <memory>

#include "nighthawk/client/output_collector.h"

#include "api/client/options.pb.h"
#include "api/client/output.pb.h"

#include "source/client/options_impl.h"
#include "source/common/nighthawk_service_client_impl.h"
#include "source/common/uri_impl.h"

namespace Nighthawk {
namespace Client {

RemoteProcessImpl::RemoteProcessImpl(const Options& options,
                                     nighthawk::client::NighthawkService::Stub& stub)
    : options_(options), service_client_(std::make_unique<NighthawkServiceClientImpl>()),
      stub_(stub) {}

bool RemoteProcessImpl::run(OutputCollector& collector) {
  Nighthawk::Client::CommandLineOptionsPtr options = options_.toCommandLineOptions();
  // We don't forward the option that requests remote execution. Today,
  // nighthawk_service will ignore the option, but if someone ever changes that this
  // is probably desireable.
  options->mutable_nighthawk_service()->Clear();
  // Note: we do not strip the fork-only live-streaming opt-in here; the shared
  // choke point in NighthawkServiceClientImpl::PerformNighthawkBenchmark does
  // that for every legacy remote consumer, including this one.

  // FORK-ONLY (We-Amp): the cancellation handle exists for this object's
  // whole lifetime (see the member note in the header) and is handed to the
  // blocking execution unchanged (#380), so requestExecutionCancellation()
  // can write the CancellationRequest arm from another thread. The arm keeps
  // the stream alive, so the run's final response still arrives below. The
  // handle's write closure is armed and disarmed INSIDE the benchmark, under
  // the handle's own mutex, so nothing here touches the member at run
  // boundaries: a cancellation request racing execution start or end always
  // reads a valid, armed-or-disarmed handle -- never a freed one.
  const absl::StatusOr<const nighthawk::client::ExecutionResponse> result =
      service_client_->PerformNighthawkBenchmarkWithCancellation(&stub_, *options,
                                                                cancellation_handle_.get());
  if (result.ok()) {
    collector.setOutput(result.value().output());
    return true;
  }
  ENVOY_LOG(error, "Remote execution failure: {}", result.status().message());
  return false;
}

bool RemoteProcessImpl::requestExecutionCancellation() {
  if (cancellation_handle_ == nullptr) {
    // No remote run is in flight on this process (never started, or already
    // finished); there is nothing to cancel.
    return false;
  }
  return cancellation_handle_->RequestCancellation();
}

} // namespace Client
} // namespace Nighthawk
