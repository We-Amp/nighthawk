#include "source/common/nighthawk_service_client_impl.h"

#include "source/common/common/assert.h"

namespace Nighthawk {

absl::StatusOr<nighthawk::client::ExecutionResponse>
NighthawkServiceClientImpl::PerformNighthawkBenchmark(
    nighthawk::client::NighthawkService::StubInterface* nighthawk_service_stub,
    const nighthawk::client::CommandLineOptions& command_line_options) const {
  nighthawk::client::ExecutionRequest request;
  nighthawk::client::ExecutionResponse response;
  // FORK-ONLY (We-Amp): strip the fork-only live-streaming opt-in at this
  // shared choke point, which every legacy remote-execution consumer goes
  // through (the CLI's remote mode, and adaptive load's nighthawk_traffic_template,
  // which carries a full CommandLineOptions). All legacy consumers RELEASE_ASSERT
  // when more than one response arrives, and live snapshot streaming by definition
  // produces many. Live streaming is fork-TUI/probe-only in v0; remote consumers
  // never get it, no matter how the options reached us.
  nighthawk::client::CommandLineOptions options = command_line_options;
  options.clear_live_streaming();
  *request.mutable_start_request()->mutable_options() = options;

  grpc::ClientContext context;
  std::shared_ptr<grpc::ClientReaderWriterInterface<nighthawk::client::ExecutionRequest,
                                                    nighthawk::client::ExecutionResponse>>
      stream(nighthawk_service_stub->ExecutionStream(&context));

  if (!stream->Write(request)) {
    return absl::UnavailableError("Failed to write request to the Nighthawk Service gRPC channel.");
  } else if (!stream->WritesDone()) {
    return absl::InternalError("WritesDone() failed on the Nighthawk Service gRPC channel.");
  }

  bool got_response = false;
  while (stream->Read(&response)) {
    RELEASE_ASSERT(!got_response,
                   "Nighthawk Service has started responding with more than one message.");
    got_response = true;
  }
  if (!got_response) {
    return absl::InternalError("Nighthawk Service did not send a gRPC response.");
  }
  grpc::Status status = stream->Finish();
  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()), status.error_message());
  }
  return response;
}

} // namespace Nighthawk
