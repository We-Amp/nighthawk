#include "source/client/live_probe.h"

#include <csignal>
#include <iostream>

#include "grpc++/grpc++.h"

#include "source/common/protobuf/utility.h"

#include "api/client/service.grpc.pb.h"
#include "nighthawk/common/exception.h"

#include "source/common/utility.h"

#include "absl/strings/str_cat.h"
#include "tclap/CmdLine.h"

namespace Nighthawk {
namespace Client {
namespace {

// Set by the SIGINT handler, probed between gRPC reads.
volatile std::sig_atomic_t g_live_probe_interrupted = 0;
// The RPC context the SIGINT handler cancels. A probe process drives a single
// RPC, so a single pointer suffices.
grpc::ClientContext* g_live_probe_context = nullptr;

void liveProbeSignalHandler(int) {
  g_live_probe_interrupted = 1;
  if (g_live_probe_context != nullptr) {
    // Cancel the RPC so the service's cancellation watchdog stops the in-flight
    // run promptly. Not strictly async-signal-safe, but TryCancel() only flips
    // gRPC's cancellation state and wakes the channel; this is the practical
    // way to unblock a synchronous Read() from a signal path.
    g_live_probe_context->TryCancel();
  }
}

// Renders a message as single-line, field-name-preserving JSON.
absl::StatusOr<std::string> toJsonLine(const Envoy::Protobuf::Message& message) {
  return Envoy::MessageUtil::getJsonStringFromMessage(message, /*pretty_print=*/false,
                                                      /*preserve_proto_field_names=*/true);
}

} // namespace

int runLiveProbe(int argc, const char* const* argv, std::ostream& out) {
  TCLAP::CmdLine cmd("nighthawk_live_probe: stream live Nighthawk snapshots as JSON lines", ' ',
                     "1.0");
  TCLAP::ValueArg<std::string> address("", "address",
                                       "Address of the Nighthawk service gRPC endpoint, for "
                                       "example 127.0.0.1:8843.",
                                       true, "", "string", cmd);
  TCLAP::ValueArg<std::string> uri("", "uri", "URI to benchmark.", true, "", "string", cmd);
  TCLAP::ValueArg<uint32_t> rps("", "rps", "Requests per second. Default: 5.", false, 5, "uint32_t",
                                cmd);
  TCLAP::ValueArg<uint32_t> duration("", "duration", "Seconds to run. Default: 10.", false, 10,
                                     "uint32_t", cmd);
  TCLAP::ValueArg<uint32_t> stats_flush_interval("", "stats-flush-interval",
                                                 "Seconds between live snapshots. Default: 1.",
                                                 false, 1, "uint32_t", cmd);
  try {
    Utility::parseCommand(cmd, argc, argv);
  } catch (const MalformedArgvException& e) {
    out << e.what() << "\n";
    return 1;
  } catch (const std::exception& e) {
    out << e.what() << "\n";
    return 1;
  }

  auto channel = grpc::CreateChannel(address.getValue(), grpc::InsecureChannelCredentials());
  auto stub = nighthawk::client::NighthawkService::NewStub(channel);
  grpc::ClientContext context;
  g_live_probe_context = &context;

  nighthawk::client::ExecutionRequest request;
  auto* options = request.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value(uri.getValue());
  options->mutable_requests_per_second()->set_value(rps.getValue());
  options->mutable_duration()->set_seconds(duration.getValue());
  options->mutable_stats_flush_interval()->set_value(stats_flush_interval.getValue());
  // Opt in to live snapshot streaming: presence of the message is the marker.
  options->mutable_live_streaming();

  auto stream = stub->ExecutionStream(&context);
  // SIGINT: cancel the RPC (which the service's cancellation watchdog turns
  // into a prompt run stop) and report the last snapshot received.
  std::signal(SIGINT, liveProbeSignalHandler);
  if (!stream->Write(request) || !stream->WritesDone()) {
    out << "Failed to write the request to the Nighthawk service.\n";
    g_live_probe_context = nullptr;
    return 1;
  }

  nighthawk::client::ExecutionResponse response;
  nighthawk::client::LiveSnapshot last_snapshot;
  bool have_snapshot = false;
  bool got_final = false;
  while (stream->Read(&response)) {
    if (g_live_probe_interrupted) {
      break;
    }
    if (response.has_live_snapshot()) {
      last_snapshot = response.live_snapshot();
      have_snapshot = true;
      absl::StatusOr<std::string> json = toJsonLine(last_snapshot);
      if (json.ok()) {
        out << json.value() << "\n";
        out.flush();
      }
    } else if (response.has_output()) {
      got_final = true;
      break;
    }
  }

  if (!got_final && g_live_probe_interrupted) {
    // gRPC never delivers the server's response after the client cancels the
    // RPC (verified against grpc++ behavior: any server write that races with,
    // or follows, the cancellation is dropped). So after a SIGINT we report
    // the last live snapshot received as the probe's final result instead of
    // waiting for a final response that will never arrive.
    context.TryCancel();
    if (have_snapshot) {
      absl::StatusOr<std::string> json = toJsonLine(last_snapshot);
      if (json.ok()) {
        out << json.value() << "\n";
      }
    } else {
      out << "{\"interrupted\": true, \"snapshot_received\": false}\n";
    }
  }

  g_live_probe_context = nullptr;
  grpc::Status status = stream->Finish();
  if (got_final) {
    absl::StatusOr<std::string> json = toJsonLine(response.output());
    if (json.ok()) {
      out << json.value() << "\n";
    }
  }
  if (got_final || (g_live_probe_interrupted && have_snapshot)) {
    return 0;
  }
  if (!status.ok() && !g_live_probe_interrupted) {
    out << absl::StrCat("Nighthawk service stream failed: ", status.error_message(), "\n");
    return 1;
  }
  return g_live_probe_interrupted ? 0 : 1;
}

} // namespace Client
} // namespace Nighthawk
