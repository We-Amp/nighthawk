#include "source/client/tui/run_feed.h"

#include "absl/status/status.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

namespace {

// grpc::Status -> absl::Status, by code value: the enums align. Same mapping
// as source/sink/nighthawk_sink_client_impl.cc.
absl::Status abslStatusFromGrpc(const grpc::Status& status) {
  if (status.ok()) {
    return absl::OkStatus();
  }
  return absl::Status(static_cast<absl::StatusCode>(status.error_code()), status.error_message());
}

} // namespace

RunFeed::RunFeed(const std::shared_ptr<grpc::ChannelInterface>& channel, RunFeedCallbacks callbacks)
    : callbacks_(std::move(callbacks)),
      stub_(nighthawk::client::NighthawkService::NewStub(channel)) {}

RunFeed::~RunFeed() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (context_ != nullptr) {
      // Promptly ends any in-flight run server-side; the read thread observes
      // the cancelled RPC and unwinds without further ceremony.
      context_->TryCancel();
    }
  }
  if (read_thread_.joinable()) {
    read_thread_.join();
  }
}

absl::Status RunFeed::start(const nighthawk::client::StartRequest& start_request) {
  std::thread to_join;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_.load() || start_in_progress_) {
      return absl::FailedPreconditionError("a run is already in flight on this RunFeed");
    }
    start_in_progress_ = true;
    // Take the previous run's (already finished) thread out of the member so it
    // can be joined with the lock released: joining under the lock would
    // deadlock when the old read thread is inside its on_finished callback and
    // that callback calls stop() (the one sanctioned re-entrant call).
    to_join = std::move(read_thread_);
    // The previous run's stream and context can be released before a fresh
    // pair is opened.
    stream_.reset();
    context_.reset();
  }
  if (to_join.joinable()) {
    to_join.join();
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    nighthawk::client::ExecutionRequest execution_request;
    *execution_request.mutable_start_request() = start_request;
    auto* options = execution_request.mutable_start_request()->mutable_options();
    // Opt in to live snapshot streaming: presence of the message is the marker.
    options->mutable_live_streaming();
    if (!options->has_stats_flush_interval()) {
      options->mutable_stats_flush_interval()->set_value(1);
    }

    context_ = std::make_unique<grpc::ClientContext>();
    // grpc++ requires Finish() on the stream before it is destroyed; the read
    // thread always calls it, and a write failure finishes explicitly below.
    stream_ = stub_->ExecutionStream(context_.get());
    if (!stream_->Write(execution_request) || !stream_->WritesDone()) {
      stream_->Finish();
      stream_.reset();
      context_.reset();
      start_in_progress_ = false;
      return absl::InternalError("failed to write the start request to the Nighthawk service");
    }

    running_.store(true);
    read_thread_ = std::thread([this]() { readLoop(); });
    start_in_progress_ = false;
  }
  return absl::OkStatus();
}

void RunFeed::stop() {
  // TryCancel only flips gRPC's cancellation state and wakes the channel; it
  // is safe from any thread, and the blocking Read() on the read thread
  // returns false as a result.
  std::lock_guard<std::mutex> lock(mutex_);
  if (context_ != nullptr) {
    context_->TryCancel();
  }
}

bool RunFeed::running() const { return running_.load(); }

void RunFeed::readLoop() {
  nighthawk::client::ExecutionResponse response;
  while (stream_->Read(&response)) {
    if (response.has_live_snapshot()) {
      if (callbacks_.on_snapshot) {
        callbacks_.on_snapshot(response.live_snapshot());
      }
    } else if (response.has_phase_event()) {
      if (callbacks_.on_phase_event) {
        callbacks_.on_phase_event(response.phase_event());
      }
    } else if (response.has_output()) {
      if (callbacks_.on_completed) {
        callbacks_.on_completed(response.output());
      }
    }
  }
  const grpc::Status status = stream_->Finish();
  running_.store(false);
  // Exactly once, always last, whatever way the run ended.
  if (callbacks_.on_finished) {
    callbacks_.on_finished(abslStatusFromGrpc(status));
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
