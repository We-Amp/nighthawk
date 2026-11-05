#include "source/client/tui/run_feed.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

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

// Prefix of every feed-GENERATED UpdateResult (the pending-at-stream-end
// surfacing). Anything with this prefix is a RunFeed statement, never an
// engine verdict — spec §4 B honesty rule (never fabricate an ack).
constexpr absl::string_view kFeedGeneratedReasonPrefix = "run-feed: ";

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
    update_pending_count_ = 0;
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
    // FORK-ONLY (We-Amp): deliberately NO WritesDone here (DQ-2, M3 Task 7) —
    // the half-close would make every mid-run write, including the
    // Update/Cancel arms, a gRPC error, and the service only reads arms while
    // the client has not half-closed. The feed half-closes after the final
    // output arrives (readLoop), which is what lets the service's reader loop
    // finish the stream cleanly on natural completion.
    if (!stream_->Write(execution_request)) {
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

absl::Status RunFeed::sendArm(
    const std::function<void(nighthawk::client::ExecutionRequest&)>& populate) {
  nighthawk::client::ExecutionRequest arm;
  populate(arm);
  const bool is_update = arm.has_update_request();
  std::lock_guard<std::mutex> lock(mutex_);
  if (!running_.load()) {
    return absl::FailedPreconditionError("no run is in flight on this RunFeed");
  }
  if (!stream_->Write(arm)) {
    // The write failure may have taken the stream with it; the read thread
    // discovers that and delivers on_finished — no state to repair here.
    return absl::InternalError("failed to write the request to the Nighthawk service");
  }
  if (is_update) {
    update_pending_count_++;
  }
  return absl::OkStatus();
}

absl::Status RunFeed::SendUpdate(const nighthawk::client::UpdateRequest& request) {
  return sendArm([request](nighthawk::client::ExecutionRequest& arm) {
    *arm.mutable_update_request() = request;
  });
}

absl::Status RunFeed::SendCancellation() {
  return sendArm(
      [](nighthawk::client::ExecutionRequest& arm) { arm.mutable_cancellation_request(); });
}

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
    } else if (response.has_update_result()) {
      // An update (or cancellation targeting-rejection) ack: count it
      // against the outstanding updates before dispatching, so the pending
      // surfacing at stream end only fires for updates that were genuinely
      // never acked.
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (update_pending_count_ > 0) {
          update_pending_count_--;
        }
      }
      if (callbacks_.on_update_result) {
        callbacks_.on_update_result(response.update_result());
      }
    } else if (response.has_output()) {
      if (callbacks_.on_completed) {
        callbacks_.on_completed(response.output());
      }
      // The final output IS the last server response on a natural completion.
      // Half-close now so the service's reader loop — which blocks in Read()
      // for the whole stream — observes the end of input and finishes the
      // stream cleanly (the Task 1 report's bidi ordering: this cannot happen
      // any earlier, or the mid-run arms would no longer be readable).
      // Serialized with the arm writes under the feed mutex, so a Write can
      // never race this WritesDone.
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stream_->WritesDone();
        // Clear running_ under the same lock that half-closed the stream: an
        // arm racing in from another thread then fails its running_ check
        // (kFailedPrecondition) instead of attempting a post-half-close
        // write that would fail as a misleading kInternal.
        running_.store(false);
      }
    }
  }
  const grpc::Status status = stream_->Finish();
  bool surface_pending = false;
  int pending_count = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_count = update_pending_count_;
    update_pending_count_ = 0;
    surface_pending = pending_count > 0;
  }
  // The late-rejection drop window (see SendUpdate): updates that the
  // service resolved after run end / accepted cancellation are never acked.
  // Surface that pending state honestly — one feed-generated result,
  // immediately before on_finished — instead of letting the consumer wait
  // for an ack that will never come (or fabricating an engine ack).
  if (surface_pending && callbacks_.on_update_result) {
    nighthawk::client::UpdateResult pending;
    pending.set_applied(false);
    pending.set_rejection_reason(absl::StrCat(
        kFeedGeneratedReasonPrefix, pending_count,
        " update(s) were still pending when the stream ended; the service never acked them "
        "(resolved inside the late-rejection drop window after run end or accepted "
        "cancellation). Not an engine verdict."));
    callbacks_.on_update_result(pending);
  }
  running_.store(false);
  // Exactly once, always last, whatever way the run ended.
  if (callbacks_.on_finished) {
    callbacks_.on_finished(abslStatusFromGrpc(status));
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
