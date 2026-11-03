#include "source/client/tui/session_reader.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "source/client/tui/session_writer.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

SessionReader::SessionReader(std::ifstream data_file) : data_file_(std::move(data_file)) {}

absl::StatusOr<std::unique_ptr<SessionReader>>
SessionReader::open(absl::string_view session_path) {
  std::ifstream data_file(std::string(session_path), std::ios::binary);
  if (!data_file.is_open()) {
    return absl::NotFoundError(absl::StrCat("cannot open session file: ", session_path));
  }
  return std::unique_ptr<SessionReader>(new SessionReader(std::move(data_file)));
}

absl::StatusOr<std::optional<nighthawk::client::SessionRecord>> SessionReader::next() {
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
  offset_ += varint32Length(length) + length;
  return std::optional<nighthawk::client::SessionRecord>(std::move(record));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
