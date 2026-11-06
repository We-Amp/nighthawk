#include "source/client/headless/session_writer.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "source/client/headless/session_framing.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

SessionRecordQueue::SessionRecordQueue(size_t capacity) : capacity_(capacity) {}

void SessionRecordQueue::push(nighthawk::client::SessionRecord record) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      // Post-close pushes are counted drops: never enqueued, never blocking.
      ++dropped_;
      return;
    }
    if (queue_.size() >= capacity_) {
      // Drop-newest: see the class comment for why drop-oldest was rejected.
      ++dropped_;
      return;
    }
    queue_.push_back(std::move(record));
  }
  cv_.notify_one();
}

std::optional<nighthawk::client::SessionRecord> SessionRecordQueue::pop() {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this]() { return !queue_.empty() || closed_; });
  if (queue_.empty()) {
    // Closed and drained.
    return std::nullopt;
  }
  nighthawk::client::SessionRecord record = std::move(queue_.front());
  queue_.pop_front();
  return record;
}

void SessionRecordQueue::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  cv_.notify_all();
}

uint64_t SessionRecordQueue::dropped() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return dropped_;
}

SessionWriter::SessionWriter(std::string session_path, size_t queue_capacity)
    : session_path_(std::move(session_path)), queue_(queue_capacity) {}

absl::StatusOr<std::unique_ptr<SessionWriter>>
SessionWriter::open(absl::string_view session_path, const nighthawk::client::SessionHeader& header,
                    SessionWriterOptions options) {
  std::unique_ptr<SessionWriter> writer(
      new SessionWriter(std::string(session_path), options.queue_capacity));

  writer->data_file_.open(writer->session_path_, std::ios::binary | std::ios::trunc);
  writer->index_file_.open(absl::StrCat(writer->session_path_, ".jsonl"),
                           std::ios::binary | std::ios::trunc);
  if (!writer->data_file_.is_open()) {
    return absl::InternalError(absl::StrCat("cannot create session file: ", writer->session_path_));
  }
  if (!writer->index_file_.is_open()) {
    return absl::InternalError(absl::StrCat("cannot create session index file: ",
                                            writer->session_path_, ".jsonl"));
  }

  nighthawk::client::SessionRecord header_record;
  *header_record.mutable_header() = header;
  absl::Status status = writer->writeRecordLocked(header_record);
  if (!status.ok()) {
    return status;
  }

  writer->writer_thread_ = std::thread([writer_ptr = writer.get()]() { writer_ptr->drainLoop(); });
  return writer;
}

SessionWriter::~SessionWriter() {
  {
    std::lock_guard<std::mutex> lock(close_mutex_);
    closed_ = true;
  }
  queue_.close();
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
  data_file_.close();
  index_file_.close();
}

void SessionWriter::enqueue(nighthawk::client::SessionRecord record) {
  queue_.push(std::move(record));
}

void SessionWriter::drainLoop() {
  while (true) {
    std::optional<nighthawk::client::SessionRecord> record = queue_.pop();
    if (!record.has_value()) {
      return;
    }
    absl::Status status = writeRecordLocked(*record);
    if (!status.ok()) {
      if (write_error_.ok()) {
        write_error_ = status;
      }
      return;
    }
  }
}

absl::Status SessionWriter::close(nighthawk::client::RunEndRecord run_end) {
  std::lock_guard<std::mutex> lock(close_mutex_);
  if (closed_) {
    return absl::FailedPreconditionError("SessionWriter::close called more than once");
  }
  queue_.close();
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
  if (!write_error_.ok()) {
    closed_ = true;
    data_file_.close();
    index_file_.close();
    return write_error_;
  }
  // The writer's queue is authoritative: overwrite whatever the caller
  // passed so a 0 cannot mask queue drops.
  run_end.set_dropped_records(queue_.dropped());
  nighthawk::client::SessionRecord record;
  *record.mutable_run_end() = std::move(run_end);
  absl::Status status = writeRecordLocked(record);
  if (status.ok()) {
    data_file_.flush();
    index_file_.flush();
    if (!data_file_.good() || !index_file_.good()) {
      status = absl::InternalError(absl::StrCat("failed flushing session files: ", session_path_));
    }
  }
  closed_ = true;
  data_file_.close();
  index_file_.close();
  return status;
}

uint64_t SessionWriter::droppedCount() const { return queue_.dropped(); }

absl::Status SessionWriter::writeRecordLocked(const nighthawk::client::SessionRecord& record) {
  std::string payload;
  if (!record.SerializeToString(&payload)) {
    return absl::InternalError("failed serializing session record");
  }
  const uint64_t frame_offset = offset_;
  const uint32_t length = static_cast<uint32_t>(payload.size());
  absl::Status status = writeVarint32(data_file_, length);
  if (!status.ok()) {
    return status;
  }
  data_file_.write(payload.data(), payload.size());
  if (!data_file_.good()) {
    return absl::InternalError(absl::StrCat("failed writing session frame at offset ", frame_offset));
  }
  offset_ += varint32Length(length) + payload.size();

  const char* type = nullptr;
  int64_t monotonic_ns = 0;
  switch (record.record_case()) {
    case nighthawk::client::SessionRecord::kHeader:
      type = "header";
      break;
    case nighthawk::client::SessionRecord::kMetricSeries:
      type = "metric_series";
      monotonic_ns = record.metric_series().monotonic_ns();
      break;
    case nighthawk::client::SessionRecord::kRunEnd:
      type = "run_end";
      break;
    case nighthawk::client::SessionRecord::kProfileWindow:
      type = "profile_window";
      break;
    case nighthawk::client::SessionRecord::kPhaseBoundary:
      type = "phase_boundary";
      monotonic_ns = record.phase_boundary().monotonic_ns();
      break;
    case nighthawk::client::SessionRecord::kKnobEpoch:
      type = "knob_epoch";
      monotonic_ns = record.knob_epoch().monotonic_ns();
      break;
    case nighthawk::client::SessionRecord::RECORD_NOT_SET:
      return absl::InternalError("refusing to write an unset session record");
  }
  index_file_ << "{\"byte_offset\":" << frame_offset << ",\"type\":\"" << type
              << "\",\"monotonic_ns\":" << monotonic_ns << "}\n";
  if (!index_file_.good()) {
    // Divergence note: the data frame above was already written and offset_
    // already advanced past it, so a failed index line leaves the sidecar
    // under-counting relative to the data file (one index line fewer than
    // data frames). Whoever debugs a diverging pair: the data file is the
    // authority; the index is advisory for sequential reads.
    return absl::InternalError(
        absl::StrCat("failed writing session index line at offset ", frame_offset));
  }
  return absl::OkStatus();
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
