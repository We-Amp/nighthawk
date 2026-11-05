#include "source/common/nighthawk_service_client_impl.h"

#include <grpc++/grpc++.h>

#include "source/common/common/assert.h"

namespace Nighthawk {

bool NighthawkServiceCancellationHandle::RequestCancellation() {
  nighthawk::client::CancellationRequest cancellation_request;
  // No execution id: the empty string targets the in-flight run unconditionally
  // (the proto's match rule), so the remote caller — which has no execution id
  // of its own — always hits the in-flight run. The service acks only
  // targeting REJECTIONS of the arm, and an empty id can never be rejected on
  // targeting; an accepted cancellation is silent, and the run's final
  // response still arrives on the stream.
  absl::MutexLock lock(&mutex_);
  if (write_arm_ == nullptr) {
    return false;
  }
  return write_arm_(cancellation_request);
}

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

absl::StatusOr<nighthawk::client::ExecutionResponse>
NighthawkServiceClientImpl::PerformNighthawkBenchmarkWithCancellation(
    nighthawk::client::NighthawkService::StubInterface* nighthawk_service_stub,
    const nighthawk::client::CommandLineOptions& command_line_options,
    NighthawkServiceCancellationHandle* cancellation_handle) const {
  nighthawk::client::ExecutionRequest request;
  nighthawk::client::ExecutionResponse response;
  // The same live-streaming strip as the legacy choke point above: remote
  // consumers never get live snapshot streaming, cancellation or not.
  nighthawk::client::CommandLineOptions options = command_line_options;
  options.clear_live_streaming();
  *request.mutable_start_request()->mutable_options() = options;

  grpc::ClientContext context;
  std::shared_ptr<grpc::ClientReaderWriterInterface<nighthawk::client::ExecutionRequest,
                                                    nighthawk::client::ExecutionResponse>>
      stream(nighthawk_service_stub->ExecutionStream(&context));

  if (!stream->Write(request)) {
    return absl::UnavailableError("Failed to write request to the Nighthawk Service gRPC channel.");
  }
  // NO WritesDone here: the half-close would make every subsequent write —
  // including the cancellation arm — a gRPC error. The half-close happens
  // after the blocking read, below, once the run's single response has been
  // received.
  if (cancellation_handle != nullptr) {
    absl::MutexLock lock(&cancellation_handle->mutex_);
    cancellation_handle->write_arm_ = [stream](const nighthawk::client::CancellationRequest&
                                                   cancellation_request) {
      nighthawk::client::ExecutionRequest arm;
      *arm.mutable_cancellation_request() = cancellation_request;
      return stream->Write(arm);
    };
  }

  bool got_response = false;
  while (stream->Read(&response)) {
    if (response.has_update_result()) {
      // Defensive (see the header note): with the empty execution id the
      // remote caller sends, a cancellation arm is never rejected on
      // targeting, so no ack can legitimately arrive here. Log and skip it;
      // it must never displace the single final response.
      ENVOY_LOG(warn, "Nighthawk Service sent an unexpected update ack; skipping it.");
      continue;
    }
    RELEASE_ASSERT(!got_response,
                   "Nighthawk Service has started responding with more than one message.");
    got_response = true;
    // Single-response contract satisfied: STOP reading here. The service's
    // reader loop blocks in Read() until the client half-closes, so another
    // client Read here would deadlock against it (the Task 1 report's bidi
    // ordering, client side). Half-closing below unblocks the service, its
    // stream status flows back through Finish(), and the loop is not re-entered.
    break;
  }
  bool writes_done_ok = true;
  if (cancellation_handle != nullptr) {
    // Disarm under the same mutex that guards the arm write, and perform the
    // half-close inside the same critical section, so a concurrent
    // RequestCancellation can neither race the write nor fire after the run
    // is over.
    absl::MutexLock lock(&cancellation_handle->mutex_);
    cancellation_handle->write_arm_ = nullptr;
    writes_done_ok = stream->WritesDone();
  } else {
    writes_done_ok = stream->WritesDone();
  }
  if (!got_response) {
    return absl::InternalError("Nighthawk Service did not send a gRPC response.");
  }
  if (!writes_done_ok) {
    ENVOY_LOG(warn, "WritesDone() failed on the Nighthawk Service gRPC channel.");
  }
  grpc::Status status = stream->Finish();
  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()), status.error_message());
  }
  return response;
}

} // namespace Nighthawk
