// FORK-ONLY (We-Amp): the ProfileWindow -> session-record conversion (M1 plan
// task 6; spec §4 session recording). Pure: a field-for-field copy into the
// persisted form, plus the raw staged profile bytes and the symbolizer
// provenance label.
//
// Symbolization is SUBPROCESS-ONLY (Global Constraints): this conversion, and
// everything downstream of the session file, touches symbolization through
// ProfileSymbolizer's `go tool pprof` subprocess only — no ELF/symbol library
// is linked into the TUI for it, and none may be (source/client/tui/README.md
// quarantine rule). The record's symbolizer_tool field carries the --pprof-tool
// argv prefix so a session stays honest about how its trees were built.
#ifndef SOURCE_CLIENT_TUI_PROFILE_SESSION_CONVERSION_H_
#define SOURCE_CLIENT_TUI_PROFILE_SESSION_CONVERSION_H_

#include "absl/strings/string_view.h"

#include "api/client/session.pb.h"
#include "source/client/tui/profile_windows.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Copies one aligned profile window into its session-record form. `raw_bytes`
// are the staged RAW profile bytes verbatim (format boundary: legacy
// gperftools binary for the gperftools SUT, proto-native otherwise — see
// profile_fetcher.h); `symbolizer_tool` is the --pprof-tool argv prefix
// (provenance; may be empty when symbolization was not attempted). An impure
// window converts too, purity_fraction carrying the exclusion mark — a
// session never hides an excluded-and-marked window. The record carries the
// window's interval_source (which clock bounded [start, end]) and its
// clock_offset_uncertainty_ns — clock-domain discipline rides WITH the
// record (M1 final fixes).
nighthawk::client::ProfileWindowRecord ToProfileWindowRecord(
    const ProfileWindow& w, absl::string_view raw_bytes,
    absl::string_view symbolizer_tool);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

#endif // SOURCE_CLIENT_TUI_PROFILE_SESSION_CONVERSION_H_
