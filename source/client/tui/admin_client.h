// FORK-ONLY (We-Amp): minimal blocking HTTP/1.1 client for the SUT admin
// endpoint. Quarantined dependency surface: abseil + POSIX sockets only — no
// Envoy code, no HTTP library. Admin responses are tiny, so the client reads
// the whole response into memory and handles both Content-Length and chunked
// transfer-encoding (the real admin sends chunked bodies).

#ifndef SOURCE_CLIENT_TUI_ADMIN_CLIENT_H_
#define SOURCE_CLIENT_TUI_ADMIN_CLIENT_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Where to reach the SUT admin endpoint: "http://127.0.0.1:9901" locally or
// ":2443"-style rig-forwarded (the "http://" scheme prefix is optional; the
// client is plain HTTP/1.1 regardless).
struct AdminEndpoints {
  std::string admin_base;
};

// Blocking HTTP/1.1 client over the SUT's admin socket. Copyable — the
// ProfileFetcher owns one by value. Not thread-safe: one request at a time;
// callers (the PprofWorkerPool jobs, spec §4) serialize their use.
class AdminClient {
public:
  explicit AdminClient(AdminEndpoints endpoints);

  // Enables/disables the gperftools CPU profiler in the SUT via the admin
  // mutator. NOTE (live-verified against nh-sut-profiled:1.40.2-gperftools,
  // 2026-11-04): the Envoy admin mutators are POST-only — a GET answers 405
  // "Method GET not allowed, POST required."
  // A 500 (the stock failure mode, e.g. double-enable) maps to an error whose
  // message quotes the response body so the failure stays diagnosable from
  // the TUI.
  absl::Status EnableCpuProfiler(bool enable);

  // Auxiliary: the tcmalloc heap-profile bytes from the admin alloc-profiler
  // mutator (same POST-only contract). Non-200 answers map to an error
  // quoting the body (the probed build answers 400 "Allocation profile is
  // not implemented in current build").
  absl::StatusOr<std::string> AllocProfilerBytes();

  // The /stats?format=prometheus body, verbatim (Task 7 parses it).
  absl::StatusOr<std::string> FetchPrometheusStats();

  // The SUT's current wall-clock time as unix ns, from the /server_info
  // clock source. Live-verified finding (2026-11-04, Envoy 1.40.2): the
  // /server_info JSON body carries NO current-time field — the current-time
  // source on that endpoint is the RFC 7231 `Date` response header (second
  // resolution; ns is seconds * 1e9). A response without the header maps to
  // kUnimplemented naming the field path ("date") so the recorded-absence
  // case stays diagnosable.
  absl::StatusOr<int64_t> FetchSutUnixTimeNs();

private:
  struct Response {
    int status = 0;
    std::string body;
    std::string date; // RFC 7231 date header value, empty if absent.
  };

  // Sends one request, reads the full response (up to a fixed timeout).
  absl::StatusOr<Response> Request(absl::string_view method,
                                   absl::string_view path_and_query);

  AdminEndpoints endpoints_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

#endif // SOURCE_CLIENT_TUI_ADMIN_CLIENT_H_
