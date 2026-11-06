#pragma once

#include <cstdint>
#include <istream>
#include <ostream>

#include "absl/status/status.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Varint32 frame-length primitives shared by the session writer and reader
// (session_writer / session_reader). Header-only on purpose: the functions are
// trivial and both consumers are in this same library target.
//
// Dependency quarantine: this header must stay clear of @envoy//source/...
// C++ targets (see source/client/tui/README.md (the same guard runs over //source/client/headless:client_headless) for the standing guard that
// enforces this).

/**
 * Appends a varint32 encoding of `value` to `out`. The caller checks the
 * stream state afterwards.
 *
 * @param out the output stream to append to.
 * @param value the value to encode.
 * @return ok, or an internal error if the stream is already in a bad state.
 */
inline absl::Status writeVarint32(std::ostream& out, uint32_t value) {
  if (!out.good()) {
    return absl::InternalError("output stream is in a bad state before writing varint");
  }
  while ((value & ~0x7fu) != 0) {
    const char byte = static_cast<char>((value & 0x7fu) | 0x80u);
    out.put(byte);
    value >>= 7;
  }
  out.put(static_cast<char>(value));
  if (!out.good()) {
    return absl::InternalError("failed writing varint length prefix");
  }
  return absl::OkStatus();
}

/**
 * Number of bytes writeVarint32() emits for `value`.
 *
 * @param value the value to measure.
 * @return the encoded length in bytes (1..5).
 */
inline uint32_t varint32Length(uint32_t value) {
  uint32_t length = 1;
  while ((value & ~0x7fu) != 0) {
    value >>= 7;
    ++length;
  }
  return length;
}

/**
 * Outcome of a varint32 read attempt.
 */
enum class VarintReadResult {
  // A value was read into the out-param.
  kOk,
  // Clean EOF: the stream was already at its end before the read started.
  kEof,
  // EOF in the middle of the varint, or the varint exceeds 5 bytes.
  kError,
};

/**
 * Reads one varint32 from `in`.
 *
 * @param in the input stream to read from.
 * @param value receives the decoded value when kOk is returned.
 * @return kOk, kEof, or kError as described above.
 */
inline VarintReadResult readVarint32(std::istream& in, uint32_t& value) {
  value = 0;
  uint32_t shift = 0;
  for (;;) {
    char byte = 0;
    if (!in.get(byte)) {
      if (in.eof() && shift == 0) {
        return VarintReadResult::kEof;
      }
      return VarintReadResult::kError;
    }
    value |= static_cast<uint32_t>(static_cast<uint8_t>(byte) & 0x7fu) << shift;
    if ((byte & 0x80u) == 0) {
      return VarintReadResult::kOk;
    }
    shift += 7;
    if (shift >= 32) {
      return VarintReadResult::kError;
    }
  }
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
