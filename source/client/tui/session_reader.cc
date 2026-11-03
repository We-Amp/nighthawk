#include "source/client/tui/session_reader.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "source/client/tui/session_framing.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// The only format version this reader understands; SessionWriter writes the
// same value into its header record.
constexpr const char* kSupportedFormatVersion = "1";

SessionReader::SessionReader(std::ifstream data_file) : data_file_(std::move(data_file)) {}

absl::StatusOr<std::unique_ptr<SessionReader>>
SessionReader::open(absl::string_view session_path) {
  std::ifstream data_file(std::string(session_path), std::ios::binary);
  if (!data_file.is_open()) {
    return absl::NotFoundError(absl::StrCat("cannot open session file: ", session_path));
  }
  std::unique_ptr<SessionReader> reader(new SessionReader(std::move(data_file)));

  // Format gate: peek the first frame. It must be a header declaring a
  // format version this reader understands. The frame is stashed so the
  // first next() call still returns it.
  absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> first = reader->readFrame();
  if (!first.ok()) {
    return first.status();
  }
  if (!first->has_value()) {
    return absl::DataLossError(absl::StrCat("session file is empty: ", session_path));
  }
  if (!(*first)->has_header()) {
    return absl::DataLossError(
        absl::StrCat("first frame of ", session_path, " is not a session header"));
  }
  if ((*first)->header().format_version() != kSupportedFormatVersion) {
    return absl::InvalidArgumentError(absl::StrCat(
        "unsupported session format version \"", (*first)->header().format_version(),
        "\" in ", session_path, " (this reader supports \"", kSupportedFormatVersion, "\")"));
  }
  reader->pending_header_ = std::move(**first);
  return reader;
}

absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> SessionReader::next() {
  if (pending_header_.has_value()) {
    nighthawk::client::SessionRecord record = std::move(*pending_header_);
    pending_header_.reset();
    return std::optional<nighthawk::client::SessionRecord>(std::move(record));
  }
  return readFrame();
}

absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> SessionReader::readFrame() {
  uint32_t length = 0;
  switch (readVarint32(data_file_, length)) {
    case VarintReadResult::kEof:
      return std::nullopt;
    case VarintReadResult::kError:
      return absl::DataLossError(
          absl::StrCat("truncated or malformed length prefix at offset ", offset_));
    case VarintReadResult::kOk:
      break;
  }
  std::string payload(length, '\0');
  if (length > 0) {
    data_file_.read(payload.data(), payload.size());
    if (data_file_.gcount() != static_cast<std::streamsize>(length)) {
      return absl::DataLossError(absl::StrCat("truncated frame at offset ", offset_,
                                              ": expected ", length, " payload bytes, got ",
                                              data_file_.gcount()));
    }
  }
  nighthawk::client::SessionRecord record;
  if (!record.ParseFromString(payload)) {
    return absl::DataLossError(
        absl::StrCat("unparseable frame at offset ", offset_, " (", length, " bytes)"));
  }
  if (record.record_case() == nighthawk::client::SessionRecord::RECORD_NOT_SET) {
    // Length-consistent but content-free: without this check the frame would
    // surface as a silent {} record instead of the corruption it is.
    return absl::DataLossError(
        absl::StrCat("corrupted frame at offset ", offset_, ": parses but sets no record"));
  }
  offset_ += varint32Length(length) + length;
  return std::optional<nighthawk::client::SessionRecord>(std::move(record));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
