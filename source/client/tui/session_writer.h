#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "api/client/session.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): session recording support for the TUI. The session file
// format is a sequence of frames, each a varint32 byte-length prefix followed
// by a serialized nighthawk.client.SessionRecord, with a JSONL sidecar index
// ("byte_offset"/"type"/"monotonic_ns" per frame) written next to it.
//
// Dependency quarantine: this library and everything it pulls in must stay
// clear of @envoy//source/... C++ targets (see source/client/tui/README.md
// for the standing guard that enforces this).

// Options for SessionWriter::open.
struct SessionWriterOptions {
  // Bounded queue capacity. The queue drops NEWEST when full.
  size_t queue_capacity = 1024;
};

// The varint32 frame-length primitives (writeVarint32, varint32Length,
// readVarint32) live in session_framing.h, shared with the reader.

/**
 * Bounded, thread-safe record queue with a drop-newest policy, used by
 * SessionWriter to hand records from the run feed thread(s) to the file
 * writer thread.
 *
 * Drop policy rationale: drop-newest (drop-oldest was considered and
 * REJECTED — it would silently rewrite the head of the recorded timeline;
 * drop-newest loses only the latest snapshot, which the TUI renders as a gap
 * per spec §4). Pushes after close() count as drops: never enqueued, never
 * blocking. close() ordering: drains already-queued records first (pop()
 * keeps yielding them), then rejects new pushes as drops.
 *
 * Pure sync primitive, unit-tested; SessionWriter wraps it across threads.
 */
class SessionRecordQueue {
public:
  /**
   * @param capacity maximum number of queued records; pushes beyond it are
   * dropped.
   */
  explicit SessionRecordQueue(size_t capacity);

  /**
   * Non-blocking push. Drops (and counts) the record when the queue is full
   * or closed.
   *
   * @param record the record to queue.
   */
  void push(nighthawk::client::SessionRecord record);

  /**
   * Pops the oldest record. Blocks while the queue is empty and not closed;
   * returns nullopt once closed and drained.
   *
   * @return the oldest queued record, or nullopt when closed and empty.
   */
  std::optional<nighthawk::client::SessionRecord> pop();

  /**
   * Closes the queue. Already-queued records remain poppable; new pushes
   * count as drops. Wakes a blocked pop().
   */
  void close();

  /**
   * Authoritative drop count: full-queue drops plus post-close pushes.
   *
   * @return the number of dropped records.
   */
  uint64_t dropped() const;

private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<nighthawk::client::SessionRecord> queue_;
  size_t capacity_;
  bool closed_ = false;
  uint64_t dropped_ = 0;
};

/**
 * Writes a session file plus its JSONL sidecar index. A background thread
 * drains a bounded SessionRecordQueue and appends frames; close() drains
 * everything, appends the run end record, and closes both files.
 *
 * Ordering contract: call close() only AFTER the run feed reports
 * on_finished (TuiApp does this on 'q'). close() overwrites
 * run_end.dropped_records with the queue's authoritative dropped count, so a
 * caller-passed 0 cannot mask queue drops.
 */
class SessionWriter {
public:
  /**
   * Opens a session file pair, writes the header record, and starts the
   * writer thread.
   *
   * @param session_path path of the data file to create (truncated). The
   * sidecar index is created next to it with a ".jsonl" suffix.
   * @param header the header record to write first.
   * @param options queue options.
   * @return the writer, or an error when either file cannot be created.
   */
  static absl::StatusOr<std::unique_ptr<SessionWriter>>
  open(absl::string_view session_path, const nighthawk::client::SessionHeader& header,
       SessionWriterOptions options = {});

  /**
   * Closes the writer on destruction if close() was not called: the queue is
   * closed and drained, but no run end record is appended. Prefer an explicit
   * close().
   */
  ~SessionWriter();

  SessionWriter(const SessionWriter&) = delete;
  SessionWriter& operator=(const SessionWriter&) = delete;

  /**
   * Thread-safe, non-blocking enqueue. Records beyond the queue capacity are
   * dropped and counted.
   *
   * @param record the record to write.
   */
  void enqueue(nighthawk::client::SessionRecord record);

  /**
   * Closes the session: drains all queued records, appends the run end
   * record (with dropped_records overwritten by the queue's authoritative
   * dropped count), flushes and closes both files. Call exactly once, from
   * any thread, after the run feed has finished.
   *
   * @param run_end the run end record to append (dropped_records is
   * overwritten).
   * @return ok, or the first I/O error seen while writing, or an error if
   * close() was already called.
   */
  absl::Status close(nighthawk::client::RunEndRecord run_end);

  /**
   * Authoritative drop count of the underlying queue.
   *
   * @return the number of dropped records.
   */
  uint64_t droppedCount() const;

private:
  SessionWriter(std::string session_path, size_t queue_capacity);

  absl::Status writeRecordLocked(const nighthawk::client::SessionRecord& record);
  void drainLoop();

  const std::string session_path_;
  SessionRecordQueue queue_;
  std::ofstream data_file_;
  std::ofstream index_file_;
  std::thread writer_thread_;
  // Byte offset of the next frame; only touched by the writer thread, and by
  // close() after the thread has been joined.
  uint64_t offset_ = 0;
  // First I/O error seen by the writer thread; read by close() after join.
  absl::Status write_error_ = absl::OkStatus();
  std::mutex close_mutex_;
  bool closed_ = false;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
