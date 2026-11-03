#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "api/client/session.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

/**
 * Reads a session file written by SessionWriter: a sequence of frames, each a
 * varint32 byte-length prefix followed by a serialized
 * nighthawk.client.SessionRecord. Records are returned in file order. The
 * JSONL sidecar index is not needed for sequential reads.
 *
 * Format gate: open() reads (and stashes) the first frame and requires it to
 * be a header with format_version "1"; an unsupported version is rejected at
 * open with an error naming the found version. The stashed header is returned
 * by the first next() call, so records still come back in file order.
 *
 * Dependency quarantine: this library and everything it pulls in must stay
 * clear of @envoy//source/... C++ targets (see source/client/tui/README.md
 * for the standing guard that enforces this).
 */
class SessionReader {
public:
  /**
   * Opens a session data file and validates its header frame.
   *
   * @param session_path path of the .nhsession data file.
   * @return the reader, or an error when the file cannot be opened, is
   * empty, has a non-header first frame, or declares an unsupported
   * format_version.
   */
  static absl::StatusOr<std::unique_ptr<SessionReader>> open(absl::string_view session_path);

  /**
   * Reads the next record.
   *
   * @return the next record; nullopt at end of file; a kDataLoss error
   * naming the byte offset when a frame is truncated or unparseable.
   */
  absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> next();

private:
  explicit SessionReader(std::ifstream data_file);

  // Reads one length-prefixed frame from the current position; shared by
  // open()'s format gate and next().
  absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> readFrame();

  std::ifstream data_file_;
  uint64_t offset_ = 0;
  // The header frame read by open()'s format gate, handed back by the first
  // next() call so records still come back in file order.
  std::optional<nighthawk::client::SessionRecord> pending_header_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
