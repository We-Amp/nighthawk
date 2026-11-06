#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "api/client/session.pb.h"

#include "source/client/headless/session_writer.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the headless session-file owner (MCP plan Task 0),
// layered on the moved SessionWriter — the exact session lifecycle the TUI's
// RunController drives (open-on-start with the header first, append from the
// feed's read thread, single-call seal strictly after the run's terminal
// event), extracted so a non-TUI consumer (the MCP server) drives the same
// lifecycle unchanged.
//
// NEW here is the honest mid-run read: ReadSealedPrefix() reads the file's
// flushed frame prefix through the SessionReader and DISCLOSES a torn tail —
// a length prefix claiming more bytes than the file holds, exactly what a
// crashed process leaves — as `tail_skipped: true` with the reader's reason,
// never as a parse error and never silently skipped. "Sealed" means flushed
// to disk: records still in the writer's queue or std::ofstream buffer are
// not yet part of the prefix.
//
// Dependency quarantine: this library and everything it pulls in must stay
// clear of @envoy//source/... C++ targets and FTXUI (see source/client/tui/
// README.md — the same standing guard runs over client_headless).
class SessionRecorder {
public:
  // One honest mid-run read of the session file.
  struct SealedPrefix {
    // The records read back in file order (header first, as written).
    std::vector<nighthawk::client::SessionRecord> records;
    // True when the read stopped at a torn/unparseable tail frame; the
    // records above are the sealed prefix only.
    bool tail_skipped = false;
    // The reader's reason naming the byte offset when tail_skipped; empty
    // otherwise.
    std::string tail_reason;
  };

  SessionRecorder() = default;

  SessionRecorder(const SessionRecorder&) = delete;
  SessionRecorder& operator=(const SessionRecorder&) = delete;

  /**
   * Opens the session file pair "<path_prefix>-<started_unix_ns>.nhsession"
   * (plus the ".jsonl" sidecar next to it), stamps `header.session_id` with
   * the concrete path, writes the header as the first frame, and starts the
   * writer thread.
   *
   * @param path_prefix prefix for the session file naming.
   * @param started_unix_ns the session's open stamp (client-host clock); it
   * completes both the file name and the naming contract.
   * @param header the header record to write first (session_id is
   * overwritten with the concrete path).
   * @return the concrete session path, or an error when already open or when
   * either file cannot be created.
   */
  absl::StatusOr<std::string> StartRun(absl::string_view path_prefix, int64_t started_unix_ns,
                                       nighthawk::client::SessionHeader header);

  /**
   * Thread-safe, non-blocking append. A record arriving after FlushAndSeal()
   * counts as a writer drop (the SessionWriter contract) — never a block,
   * never a crash. Callers enqueue complete SessionRecords (the pure
   * conversions build them; this layer persists, it does not translate).
   *
   * @param record the record to persist.
   */
  void AppendSnapshot(nighthawk::client::SessionRecord record);

  /**
   * Single-call seal: drains the writer's queue, appends the run end record
   * (its dropped_records is overwritten by the queue's authoritative drop
   * count), flushes and closes both files, and closes the session so a second
   * call is a no-op success — exactly one run end record can ever be
   * written. The session's path stays queryable afterwards (path() keeps
   * returning it) so a consumer can read the sealed file; open() is what
   * distinguishes an open session from a sealed one.
   *
   * @param run_end the run end record to append.
   * @return ok (also when no session was ever opened), or the writer's first
   * I/O error.
   */
  absl::Status FlushAndSeal(nighthawk::client::RunEndRecord run_end);

  /**
   * Whether a session is open (StartRun succeeded, FlushAndSeal not yet).
   */
  bool open() const;

  /**
   * The current or most recent session's data file path; empty before the
   * first StartRun.
   */
  std::string path() const;

  /**
   * The honest mid-run read: every flushed record in file order, with a torn
   * tail disclosed instead of failing the whole read. Safe to call while the
   * writer runs — it reads only what has reached the disk. kFailedPrecondition
   * before the first StartRun (there is no file to read).
   */
  absl::StatusOr<SealedPrefix> ReadSealedPrefix() const;

private:
  mutable std::mutex mutex_;
  std::unique_ptr<SessionWriter> writer_; // null when no session is open
  // The current/most recent session path; kept after sealing so the sealed
  // file stays readable.
  std::string path_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
