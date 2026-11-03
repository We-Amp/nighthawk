#pragma once

#include <cstdint>

#include "absl/strings/string_view.h"

#include "api/client/live.pb.h"
#include "api/client/service.pb.h"
#include "api/client/session.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the pure proto conversions between the run feed's wire
// messages and the session file's records. No threading, no I/O — the writer
// thread contract stays in session_writer.h and the callers in tui_app.

// The session file format version ToSessionHeader stamps into the header; the
// reader accepts exactly this version (session_reader.cc).
inline constexpr char kSessionFormatVersion[] = "1";

// Copies a live snapshot into its persisted form, field for field: timestamps
// stay engine-stamped (the rig is the timestamp authority, spec §8.5), the
// phase id stays the constant "main" pre-A1 and an empty epoch id stays empty
// pre-A2 (M0a placeholder semantics).
nighthawk::client::MetricSeriesRecord
ToMetricSeries(const nighthawk::client::LiveSnapshot& snapshot);

// Builds the session header: the format version, the caller's session id, the
// client-host open time (the session's started_unix_ns), and the exact
// StartRequest the run was started with.
nighthawk::client::SessionHeader ToSessionHeader(const nighthawk::client::StartRequest& request,
                                                 absl::string_view session_id,
                                                 int64_t started_unix_ns);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
