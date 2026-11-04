// FORK-ONLY (We-Amp). See profile_fetcher.h for the design contract.

#include "source/client/tui/profile_fetcher.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "absl/time/clock.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

std::string Basename(absl::string_view path) {
  size_t slash = path.rfind('/');
  return slash == absl::string_view::npos ? std::string(path)
                                          : std::string(path.substr(slash + 1));
}

bool FileExists(const std::string& path) {
  struct stat st;
  return ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

absl::Status StageFile(const std::string& from, const std::string& to) {
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(to).parent_path(), ec);
  if (ec) {
    return absl::InternalError(
        absl::StrCat("failed to create staging dir: ", ec.message()));
  }
  std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing, ec);
  if (ec) {
    return absl::InternalError(
        absl::StrCat("failed to stage profile \"", from, "\" -> \"", to, "\": ", ec.message()));
  }
  return absl::OkStatus();
}

} // namespace

VolumeMountRetriever::VolumeMountRetriever(std::string mount_root)
    : mount_root_(std::move(mount_root)) {
  while (mount_root_.size() > 1 && mount_root_.back() == '/') {
    mount_root_.pop_back(); // keep trailing-slash normalization trivial
  }
}

absl::StatusOr<std::string> VolumeMountRetriever::Retrieve(absl::string_view sut_path) {
  std::string local = absl::StrCat(mount_root_, "/", Basename(sut_path));
  if (!FileExists(local)) {
    return absl::NotFoundError(absl::StrCat(
        "no profile file under the SUT profile mount: expected \"", local,
        "\" (SUT path \"", sut_path, "\"); the profiler may not have flushed yet"));
  }
  return local;
}

ScpRetriever::ScpRetriever(std::string scp_host) : scp_host_(std::move(scp_host)) {}

absl::StatusOr<std::string> ScpRetriever::Retrieve(absl::string_view sut_path) {
  std::string scratch =
      absl::StrCat("/tmp/nh-scp-fetch-", static_cast<long>(::getpid()));
  std::error_code ec;
  std::filesystem::create_directories(scratch, ec);
  if (ec) {
    return absl::InternalError(absl::StrCat("failed to create scratch dir: ", ec.message()));
  }
  std::string local = absl::StrCat(scratch, "/", Basename(sut_path));
  std::string stderr_path = absl::StrCat(scratch, "/scp.stderr");
  std::string command =
      absl::StrCat("scp -q '", scp_host_, ":", sut_path, "' '", local, "' 2>'", stderr_path,
                   "'");
  int rc = std::system(command.c_str());
  if (rc != 0) {
    std::ifstream err(stderr_path);
    std::string detail((std::istreambuf_iterator<char>(err)),
                       std::istreambuf_iterator<char>());
    ::unlink(stderr_path.c_str());
    return absl::UnavailableError(
        absl::StrCat("scp of \"", sut_path, "\" from \"", scp_host_, "\" failed (rc ", rc,
                     "): ", detail));
  }
  ::unlink(stderr_path.c_str());
  return local;
}

ProfileFetcher::ProfileFetcher(AdminClient admin, std::unique_ptr<ProfileRetriever> retriever,
                               std::string sut_profile_path, std::string staging_dir)
    : admin_(std::move(admin)), retriever_(std::move(retriever)),
      sut_profile_path_(std::move(sut_profile_path)), staging_dir_(std::move(staging_dir)) {}

absl::Status ProfileFetcher::StartWindow(int window_seconds) {
  (void)window_seconds; // advisory only — the admin mutator takes no duration
  if (window_open_.load(std::memory_order_relaxed)) {
    return absl::FailedPreconditionError(
        "a profile window is already open; close it with StopWindow first");
  }
  absl::Status status = admin_.EnableCpuProfiler(true);
  if (!status.ok()) {
    return status;
  }
  requested_start_unix_ns_ = absl::GetCurrentTimeNanos();
  window_open_.store(true, std::memory_order_relaxed);
  return absl::OkStatus();
}

absl::Status ProfileFetcher::StopWindow() {
  if (!window_open_.load(std::memory_order_relaxed)) {
    return absl::FailedPreconditionError("no profile window is open");
  }
  // The window is closed and the interval end stamped BEFORE the disable
  // result is inspected: a wedged state never persists, and the honest
  // interval end is the p-press — never when the pool job later fetches.
  window_open_.store(false, std::memory_order_relaxed);
  requested_end_unix_ns_ = absl::GetCurrentTimeNanos();
  return admin_.EnableCpuProfiler(false);
}

absl::StatusOr<FetchedProfile> ProfileFetcher::FetchProfile() {
  if (window_open_.load(std::memory_order_relaxed)) {
    return absl::FailedPreconditionError(
        "the profiler is still enabled: StopWindow must run before FetchProfile "
        "(a fetch racing a live profiler reads a partial file)");
  }

  auto retrieved = retriever_->Retrieve(sut_profile_path_);
  if (!retrieved.ok()) {
    return retrieved.status();
  }

  // Staged in the legacy gperftools binary format, verbatim from the SUT —
  // NOT gzip, NOT a pprof proto (see FetchedProfile's format boundary):
  // readable only via ProfileSymbolizer's go-tool conversion (Task 3).
  std::string staged = absl::StrCat(staging_dir_, "/cpu-", requested_end_unix_ns_, ".pprof");
  absl::Status staged_status = StageFile(*retrieved, staged);
  if (!staged_status.ok()) {
    return staged_status;
  }

  FetchedProfile fetched;
  fetched.local_path = staged;
  fetched.profile_type = "cpu";
  fetched.requested_start_unix_ns = requested_start_unix_ns_;
  fetched.requested_end_unix_ns = requested_end_unix_ns_;
  return fetched;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
