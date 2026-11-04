// FORK-ONLY (We-Amp): profile-window orchestration + out-of-band retrieval
// over the SUT admin endpoint. See admin_client.h for the live-verified
// admin contract (POST-only mutators, Date-header clock source).
//
// Threading (spec §4): ProfileFetcher is SYNCHRONOUS and owns no thread —
// the blocking work (StopWindowAndFetch) belongs in a PprofWorkerPool job.
// window_open()/StartWindow are single-caller (the orchestrating job).

#ifndef SOURCE_CLIENT_TUI_PROFILE_FETCHER_H_
#define SOURCE_CLIENT_TUI_PROFILE_FETCHER_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "source/client/tui/admin_client.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Out-of-band retrieval seam: the admin API cannot return the CPU profile
// file, so it must be fetched from wherever the SUT left it.
class ProfileRetriever {
public:
  virtual ~ProfileRetriever() = default;

  // Fetches `sut_path` (the path as the SUT sees it, e.g.
  // /var/local/profiling/envoy.prof) and returns a local file path that
  // holds the profile bytes.
  virtual absl::StatusOr<std::string> Retrieve(absl::string_view sut_path) = 0;
};

// Local variant: the SUT's profile directory is bind-mounted into this
// machine, so retrieval is pure path translation (SUT path basename under
// the mount root).
class VolumeMountRetriever : public ProfileRetriever {
public:
  explicit VolumeMountRetriever(std::string mount_root);

  absl::StatusOr<std::string> Retrieve(absl::string_view sut_path) override;

private:
  std::string mount_root_;
};

// Rig variant: scp the profile over the existing SSH forward session's host.
// The host comes from config (the --scp-host flag, Task 6); never hardcoded.
class ScpRetriever : public ProfileRetriever {
public:
  // `scp_host` is the remote scp destination ("user@host"); the SUT path is
  // prefixed with it verbatim.
  explicit ScpRetriever(std::string scp_host);

  // Copies the file to a per-process temp directory and returns its local
  // path. Blocking (a worker-pool job); propagates the scp failure with its
  // stderr quoted.
  absl::StatusOr<std::string> Retrieve(absl::string_view sut_path) override;

private:
  std::string scp_host_;
};

// The metadata the TUI records for one profile window. NOTE (spec §8.5): the
// requested_* timestamps are mac-side call times, METADATA only — the
// authoritative window interval comes from the parsed profile (Task 3/4).
//
// FORMAT BOUNDARY (T2→T3/T4/T6): the staged .pprof is the SUT's RAW CPU
// profile in the legacy gperftools binary format (sample-count header +
// stack records) — NOT gzip and NOT a pprof proto, despite the extension.
// It is readable ONLY through ProfileSymbolizer's go-tool conversion
// (`go tool pprof <sut-binary> <file>`, Task 3); nothing in the TUI may
// parse it directly, and the authoritative window interval (Task 3/4) must
// be read out of that conversion, not from this file.
struct FetchedProfile {
  std::string local_path;
  std::string profile_type; // "cpu" | "alloc"
  int64_t requested_start_unix_ns = 0;
  int64_t requested_end_unix_ns = 0;
};

// Drives one profile window: enable the profiler, then stop-and-fetch.
class ProfileFetcher {
public:
  ProfileFetcher(AdminClient admin, std::unique_ptr<ProfileRetriever> retriever,
                 std::string sut_profile_path, std::string staging_dir);

  // Enables the CPU profiler in the SUT. Fails (kFailedPrecondition) if a
  // window is already open. `window_seconds` is advisory bookkeeping for the
  // caller only — the admin mutator takes no duration; the window ends at
  // StopWindowAndFetch.
  absl::Status StartWindow(int window_seconds);

  // Disables the profiler (ALWAYS before retrieval — a fetch racing a live
  // profiler would read a partial file), retrieves the profile, stages it
  // under staging_dir as <type>-<unix_ns>.pprof and returns the metadata.
  // Blocking — call from a PprofWorkerPool job (Task 6). Fails
  // (kFailedPrecondition) if no window is open; the window is closed even
  // when disable or retrieval fails, so a wedged state never persists.
  //
  // Post-failed-disable recovery: the failure surfaces here and the window
  // is already closed, so the SUT profiler may still be RUNNING. Recovery is
  // the caller's job with its own AdminClient — call
  // AdminClient::EnableCpuProfiler(false) directly (and optionally fetch via
  // Retrieve()) — never by re-entering StopWindowAndFetch, which would fail
  // kFailedPrecondition again.
  absl::StatusOr<FetchedProfile> StopWindowAndFetch();

  bool window_open() const { return window_open_; }

private:
  AdminClient admin_;
  std::unique_ptr<ProfileRetriever> retriever_;
  std::string sut_profile_path_;
  std::string staging_dir_;
  bool window_open_ = false;
  int64_t requested_start_unix_ns_ = 0;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

#endif // SOURCE_CLIENT_TUI_PROFILE_FETCHER_H_
