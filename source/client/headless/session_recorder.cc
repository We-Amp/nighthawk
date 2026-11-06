#include "source/client/headless/session_recorder.h"

#include <utility>

#include "absl/strings/str_cat.h"

#include "source/client/headless/session_reader.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

absl::StatusOr<std::string> SessionRecorder::StartRun(absl::string_view path_prefix,
                                                      int64_t started_unix_ns,
                                                      nighthawk::client::SessionHeader header) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (writer_ != nullptr) {
    return absl::FailedPreconditionError("a session is already open");
  }
  // The RunController naming contract, unchanged: "<prefix>-<unix_ns>.nhsession".
  const std::string path =
      absl::StrCat(path_prefix, "-", started_unix_ns, ".nhsession");
  header.set_session_id(path);
  auto writer_or = SessionWriter::open(path, header);
  if (!writer_or.ok()) {
    return writer_or.status();
  }
  writer_ = std::move(*writer_or);
  path_ = path;
  return path;
}

void SessionRecorder::AppendSnapshot(nighthawk::client::SessionRecord record) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (writer_ == nullptr) {
    // Post-seal appends are writer drops by contract; with no writer at all
    // there is nothing to count — the record is dropped here.
    return;
  }
  writer_->enqueue(std::move(record));
}

absl::Status SessionRecorder::FlushAndSeal(nighthawk::client::RunEndRecord run_end) {
  std::unique_ptr<SessionWriter> writer;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Move-out makes a second close impossible: after this seal the recorder
    // is closed, and a repeat call falls through to the no-op OkStatus —
    // exactly one run end record per session. path_ stays: the sealed file
    // remains readable through ReadSealedPrefix.
    writer = std::move(writer_);
  }
  if (writer == nullptr) {
    return absl::OkStatus();
  }
  return writer->close(std::move(run_end));
}

bool SessionRecorder::open() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return writer_ != nullptr;
}

std::string SessionRecorder::path() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return path_;
}

absl::StatusOr<SessionRecorder::SealedPrefix> SessionRecorder::ReadSealedPrefix() const {
  std::string path;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    path = path_;
  }
  if (path.empty()) {
    return absl::FailedPreconditionError("no session has been started");
  }
  auto reader_or = SessionReader::open(path);
  if (!reader_or.ok()) {
    return reader_or.status();
  }
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  SealedPrefix prefix;
  for (;;) {
    auto next = reader->next();
    if (!next.ok()) {
      // A torn or unparseable tail frame: the honest disclosure. Everything
      // read so far is the sealed prefix; the reader's reason names the byte
      // offset. Never a failed read, never a silent skip.
      prefix.tail_skipped = true;
      prefix.tail_reason = std::string(next.status().message());
      return prefix;
    }
    if (!next->has_value()) {
      break;
    }
    prefix.records.push_back(**next);
  }
  return prefix;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
