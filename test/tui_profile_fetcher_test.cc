// FORK-ONLY (We-Amp): profile fetch layer tests. Plain cc_test for the TUI
// quarantine (same reasoning as tui_app_test): the TUI stays off Envoy's C++
// sources and test helpers, so this test carries its own minimal HTTP stub
// server instead of Envoy's admin integration harness.
//
// The stub server stands in for the SUT admin endpoint. Its responses mirror
// the live Envoy 1.40.2 admin contract as probed against
// nh-sut-profiled:1.40.2-gperftools (2026-11-04):
// - /cpuprofiler and /allocprofiler are POST-only (GET answers 405
//   "Method GET not allowed, POST required.")
// - /stats?format=prometheus and /server_info answer GET with chunked bodies
// - the current-time source on /server_info is the RFC 7231 `Date` response
//   header; the JSON body carries no current-time field in this build.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "source/client/tui/profile_fetcher.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Writes a full buffer to an fd; dies via assertion on partial write.
void WriteAllOrDie(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    ASSERT_GE(n, 0) << "stub write failed: " << std::strerror(errno);
    off += static_cast<size_t>(n);
  }
}

// Reads until the end of the HTTP headers, then any advertised body.
std::string ReadRequest(int fd) {
  std::string data;
  char buf[4096];
  while (data.find("\r\n\r\n") == std::string::npos) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n <= 0) {
      return data;
    }
    data.append(buf, static_cast<size_t>(n));
  }
  // No stub route advertises a request body; the admin mutators are POSTs with
  // empty bodies. Anything past the header terminator is ignored.
  return data;
}

std::string StatusReason(int status) {
  switch (status) {
  case 200:
    return "OK";
  case 400:
    return "Bad Request";
  case 405:
    return "Method Not Allowed";
  case 500:
    return "Internal Server Error";
  default:
    return "Unknown";
  }
}

// Builds a raw HTTP/1.1 response with Content-Length framing.
std::string MakeResponse(int status, const std::string& body,
                         const std::vector<std::string>& extra_headers = {}) {
  std::string out = absl::StrCat("HTTP/1.1 ", status, " ", StatusReason(status),
                                 "\r\ncontent-type: text/plain; charset=UTF-8\r\n");
  for (const auto& h : extra_headers) {
    absl::StrAppend(&out, h, "\r\n");
  }
  absl::StrAppend(&out, "content-length: ", body.size(), "\r\n\r\n", body);
  return out;
}

// Builds a chunked HTTP/1.1 response the way Envoy admin does.
std::string MakeChunkedResponse(int status, const std::string& body,
                                const std::vector<std::string>& extra_headers = {}) {
  std::string out = absl::StrCat("HTTP/1.1 ", status, " ", StatusReason(status),
                                 "\r\ncontent-type: text/plain; charset=UTF-8\r\n");
  for (const auto& h : extra_headers) {
    absl::StrAppend(&out, h, "\r\n");
  }
  absl::StrAppend(&out, "transfer-encoding: chunked\r\n\r\n");
  size_t off = 0;
  while (off < body.size()) {
    size_t chunk = std::min<size_t>(body.size() - off, 64);
    absl::StrAppend(&out, absl::Hex(chunk, absl::kZeroPad4), "\r\n");
    out.append(body, off, chunk);
    absl::StrAppend(&out, "\r\n");
    off += chunk;
  }
  absl::StrAppend(&out, "0\r\n\r\n");
  return out;
}

// A single-connection-at-a-time HTTP stub. Each accepted request is answered
// by `handler` and logged with its request line (first line of the request).
class StubAdminServer {
public:
  using Handler = std::function<std::string(const std::string& request_line)>;

  explicit StubAdminServer(Handler handler) : handler_(std::move(handler)) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd_ >= 0);
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    assert(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    assert(::listen(listen_fd_, 4) == 0);
    socklen_t len = sizeof(addr);
    assert(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this]() { AcceptLoop(); });
  }

  ~StubAdminServer() {
    stop_ = true;
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  uint16_t port() const { return port_; }

  std::string admin_base() const {
    return absl::StrCat("http://127.0.0.1:", port_);
  }

  // The request lines received so far, oldest first.
  std::vector<std::string> requests() {
    std::lock_guard<std::mutex> lock(mu_);
    return requests_;
  }

private:
  void AcceptLoop() {
    while (!stop_) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        if (stop_) {
          return;
        }
        continue;
      }
      std::string request = ReadRequest(fd);
      std::string request_line = request.substr(0, request.find("\r\n"));
      {
        std::lock_guard<std::mutex> lock(mu_);
        requests_.push_back(request_line);
      }
      std::string response = handler_(request_line);
      WriteAllOrDie(fd, response);
      ::close(fd);
    }
  }

  Handler handler_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::thread thread_;
  std::mutex mu_;
  std::vector<std::string> requests_;
  std::atomic<bool> stop_{false};
};

// A retriever that records how many requests the stub had already logged when
// Retrieve ran, plus what it was asked for. Lets tests pin the ordering
// contract: the admin must see the profiler disable before retrieval starts.
class RecordingRetriever : public ProfileRetriever {
public:
  // Records the stub's request log at Retrieve time, then delegates to
  // `delegate` (the real VolumeMountRetriever under test) unless `fail` is
  // set — so the ordering contract is observable while the production path
  // translation still runs.
  RecordingRetriever(std::function<std::string()> log_at_retrieve,
                     ProfileRetriever* delegate)
      : log_at_retrieve_(std::move(log_at_retrieve)), delegate_(delegate) {}

  absl::StatusOr<std::string> Retrieve(absl::string_view sut_path) override {
    std::lock_guard<std::mutex> lock(mu_);
    ++calls_;
    asked_for_.emplace_back(sut_path);
    stub_log_at_retrieve_ = log_at_retrieve_();
    if (fail_) {
      return absl::NotFoundError("fake retrieval failure");
    }
    return delegate_->Retrieve(sut_path);
  }

  int calls() {
    std::lock_guard<std::mutex> lock(mu_);
    return calls_;
  }
  std::vector<std::string> asked_for() {
    std::lock_guard<std::mutex> lock(mu_);
    return asked_for_;
  }
  std::string stub_log_at_retrieve() {
    std::lock_guard<std::mutex> lock(mu_);
    return stub_log_at_retrieve_;
  }
  void fail(bool f) { fail_ = f; }

private:
  std::function<std::string()> log_at_retrieve_;
  ProfileRetriever* delegate_;
  std::mutex mu_;
  int calls_ = 0;
  std::vector<std::string> asked_for_;
  std::string stub_log_at_retrieve_;
  std::atomic<bool> fail_{false};
};

// VolumeMountRetriever wired to a real temp directory layout: the SUT path
// /var/local/profiling/envoy.prof maps under the mount by basename, exactly
// the bind-mount translation the production retriever performs.
class VolumeMountTestHarness {
public:
  VolumeMountTestHarness() {
    char mount_dir[] = "/tmp/nh-fetcher-test-mount-XXXXXX";
    char staging_dir[] = "/tmp/nh-fetcher-test-staging-XXXXXX";
    assert(::mkdtemp(mount_dir) != nullptr);
    assert(::mkdtemp(staging_dir) != nullptr);
    mount_root_ = mount_dir;
    staging_root_ = staging_dir;
  }
  ~VolumeMountTestHarness() {
    ::unlink(absl::StrCat(mount_root_, "/envoy.prof").c_str());
    ::rmdir(mount_root_.c_str());
    for (const auto& f : staged_files_) {
      ::unlink(f.c_str());
    }
    ::rmdir(staging_root_.c_str());
  }

  void write_sut_profile(const std::string& content) {
    WriteAllOrDie(
        ::open(absl::StrCat(mount_root_, "/envoy.prof").c_str(), O_WRONLY | O_CREAT | O_TRUNC,
               0644),
        content);
  }
  std::string mount_root() const { return mount_root_; }
  std::string staging_root() const { return staging_root_; }
  std::vector<std::string> staged_files_;

private:
  std::string mount_root_;
  std::string staging_root_;
};

// Handler serving the stock-Envoy failure body for the profiler enable
// request, as verified in M0a: a second enable while the profiler already
// runs fails with a 500 whose body names the failure.
std::string EnableFailureHandler(const std::string& request_line) {
  if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1") {
    return MakeResponse(500, "Profiler is already running.");
  }
  return MakeResponse(404, "not found");
}

TEST(AdminClientTest, EnableFailureSurfacesBody) {
  StubAdminServer server(EnableFailureHandler);
  AdminClient client(AdminEndpoints{server.admin_base()});
  absl::Status status = client.EnableCpuProfiler(true);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(), ::testing::HasSubstr("Profiler is already running."));
}

TEST(AdminClientTest, EnableSuccessAndDisableOrder) {
  StubAdminServer server([](const std::string& request_line) {
    if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1" ||
        request_line == "POST /cpuprofiler?enable=n HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client(AdminEndpoints{server.admin_base()});
  EXPECT_TRUE(client.EnableCpuProfiler(true).ok());
  EXPECT_TRUE(client.EnableCpuProfiler(false).ok());
  EXPECT_THAT(server.requests(),
              ::testing::ElementsAre("POST /cpuprofiler?enable=y HTTP/1.1",
                                     "POST /cpuprofiler?enable=n HTTP/1.1"));
}

TEST(AdminClientTest, AllocProfilerReturnsBytes) {
  const std::string fixture = std::string("\x01\x00\x00\x0dpprof-bytes", 16);
  StubAdminServer server([&fixture](const std::string& request_line) {
    if (request_line == "POST /allocprofiler HTTP/1.1") {
      return MakeResponse(200, fixture);
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client(AdminEndpoints{server.admin_base()});
  absl::StatusOr<std::string> bytes = client.AllocProfilerBytes();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  EXPECT_EQ(*bytes, fixture);
}

TEST(AdminClientTest, FetchPrometheusStatsReturnsBody) {
  const std::string body =
      "# HELP envoy_server_live Server live.\n"
      "envoy_server_live 1\n";
  // Served chunked, mirroring the real admin's transfer-encoding.
  StubAdminServer server([&body](const std::string& request_line) {
    if (request_line == "GET /stats?format=prometheus HTTP/1.1") {
      return MakeChunkedResponse(200, body);
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client(AdminEndpoints{server.admin_base()});
  absl::StatusOr<std::string> stats = client.FetchPrometheusStats();
  ASSERT_TRUE(stats.ok()) << stats.status();
  EXPECT_EQ(*stats, body);
}

TEST(AdminClientTest, ShortContentLengthBodyErrors) {
  StubAdminServer server([](const std::string& request_line) -> std::string {
    if (request_line == "GET /stats?format=prometheus HTTP/1.1") {
      // Advertise more bytes than are actually sent.
      return "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 100\r\n\r\nshort";
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client(AdminEndpoints{server.admin_base()});
  absl::StatusOr<std::string> stats = client.FetchPrometheusStats();
  ASSERT_FALSE(stats.ok());
  EXPECT_THAT(stats.status().message(), ::testing::HasSubstr("truncated"));
}

TEST(AdminClientTest, FetchSutUnixTimeNsParsesOrReports) {
  // With the clock-source field present: the RFC 7231 Date header parses to
  // unix ns. Thursday 1970-01-01 00:00:10 GMT is exactly 10 seconds after
  // the epoch, so the expected value is independent of any host clock.
  StubAdminServer with_date([](const std::string& request_line) {
    if (request_line == "GET /server_info HTTP/1.1") {
      return MakeResponse(200, "{\"version\": \"x\", \"state\": \"LIVE\"}\n",
                          {"date: Thu, 01 Jan 1970 00:00:10 GMT"});
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client_with(AdminEndpoints{with_date.admin_base()});
  absl::StatusOr<int64_t> ns = client_with.FetchSutUnixTimeNs();
  ASSERT_TRUE(ns.ok()) << ns.status();
  EXPECT_EQ(*ns, 10 * 1000000000LL);

  // Without the clock-source field: the recorded-absence case must stay
  // diagnosable — an Unimplemented error naming the field path.
  StubAdminServer without_date([](const std::string& request_line) {
    if (request_line == "GET /server_info HTTP/1.1") {
      return MakeResponse(200, "{\"version\": \"x\", \"state\": \"LIVE\"}\n");
    }
    return MakeResponse(404, "not found");
  });
  AdminClient client_without(AdminEndpoints{without_date.admin_base()});
  absl::StatusOr<int64_t> absent = client_without.FetchSutUnixTimeNs();
  ASSERT_FALSE(absent.ok());
  EXPECT_TRUE(absl::IsUnimplemented(absent.status())) << absent.status();
  EXPECT_THAT(absent.status().message(), ::testing::HasSubstr("date"));
}

TEST(ProfileFetcherTest, StopWindowDisablesThenRetrieves) {
  StubAdminServer server([](const std::string& request_line) {
    if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1" ||
        request_line == "POST /cpuprofiler?enable=n HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    if (request_line == "GET /server_info HTTP/1.1") {
      return MakeResponse(200, "{}\n", {"date: Thu, 01 Jan 1970 00:00:10 GMT"});
    }
    return MakeResponse(404, "not found");
  });

  VolumeMountTestHarness harness;
  // Deliberately NOT gzip / not a pprof proto: the SUT stages its raw CPU
  // profile in the legacy gperftools binary format (see FetchedProfile's
  // format boundary). Contents are opaque to this test.
  const std::string profile_bytes = "fake-legacy-gperftools-profile";
  harness.write_sut_profile(profile_bytes);

  // The recording retriever snapshots the stub's request log at the moment
  // it is invoked, so the ordering contract is observable from the test
  // thread, then delegates to the real VolumeMountRetriever.
  VolumeMountRetriever mount_retriever(harness.mount_root());
  auto retriever = std::make_unique<RecordingRetriever>(
      [&server]() {
        std::string joined;
        for (const auto& r : server.requests()) {
          absl::StrAppend(&joined, r, "\n");
        }
        return joined;
      },
      &mount_retriever);
  RecordingRetriever* retriever_ptr = retriever.get();

  ProfileFetcher fetcher(AdminClient(AdminEndpoints{server.admin_base()}),
                         std::move(retriever), "/var/local/profiling/envoy.prof",
                         harness.staging_root());

  EXPECT_FALSE(fetcher.window_open());
  absl::Status start = fetcher.StartWindow(5);
  ASSERT_TRUE(start.ok()) << start;
  EXPECT_TRUE(fetcher.window_open());

  // The close is two steering-thread calls now (M1 final fixes): StopWindow
  // disables + stamps the interval end; FetchProfile retrieves (the pool
  // job's half).
  absl::Status stopped = fetcher.StopWindow();
  ASSERT_TRUE(stopped.ok()) << stopped;
  EXPECT_FALSE(fetcher.window_open());

  absl::StatusOr<FetchedProfile> fetched = fetcher.FetchProfile();
  ASSERT_TRUE(fetched.ok()) << fetched.status();

  // Ordering: by the time retrieval ran, the stub had already seen the
  // disable request — a fetch must never race a live profiler (it would read
  // a partial file).
  EXPECT_THAT(retriever_ptr->stub_log_at_retrieve(),
              ::testing::HasSubstr("POST /cpuprofiler?enable=n HTTP/1.1"));
  EXPECT_EQ(retriever_ptr->asked_for(),
            std::vector<std::string>{"/var/local/profiling/envoy.prof"});
  EXPECT_EQ(server.requests().size(), 2);
  EXPECT_EQ(server.requests()[0], "POST /cpuprofiler?enable=y HTTP/1.1");
  EXPECT_EQ(server.requests()[1], "POST /cpuprofiler?enable=n HTTP/1.1");

  EXPECT_FALSE(fetcher.window_open());
  EXPECT_EQ(fetched->profile_type, "cpu");
  // The requested-window timestamps are mac-side metadata only (spec §8.5).
  EXPECT_GT(fetched->requested_start_unix_ns, 0);
  EXPECT_GE(fetched->requested_end_unix_ns, fetched->requested_start_unix_ns);

  // Staged under the staging dir as <type>-<unix_ns>.pprof with the mount's
  // profile content.
  EXPECT_THAT(fetched->local_path,
              ::testing::HasSubstr(harness.staging_root()));
  EXPECT_THAT(fetched->local_path,
              ::testing::MatchesRegex(".*/cpu-[0-9]+\\.pprof"));
  int fd = ::open(fetched->local_path.c_str(), O_RDONLY);
  ASSERT_GE(fd, 0);
  char buf[256];
  ssize_t n = ::read(fd, buf, sizeof(buf));
  ::close(fd);
  ASSERT_EQ(n, static_cast<ssize_t>(profile_bytes.size()));
  EXPECT_EQ(std::string(buf, static_cast<size_t>(n)), profile_bytes);
}

TEST(ProfileFetcherTest, SecondStartWhileOpenFails) {
  StubAdminServer server([](const std::string& request_line) {
    if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    if (request_line == "POST /cpuprofiler?enable=n HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    return MakeResponse(404, "not found");
  });
  VolumeMountTestHarness harness;
  VolumeMountRetriever mount_retriever(harness.mount_root());
  ProfileFetcher fetcher(AdminClient(AdminEndpoints{server.admin_base()}),
                         std::make_unique<RecordingRetriever>([]() { return ""; },
                                                              &mount_retriever),
                         "/var/local/profiling/envoy.prof", harness.staging_root());
  ASSERT_TRUE(fetcher.StartWindow(5).ok());
  absl::Status second = fetcher.StartWindow(5);
  EXPECT_FALSE(second.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(second)) << second;
  static_cast<void>(fetcher.StopWindow());
}

TEST(ProfileFetcherTest, StopWithoutStartFails) {
  StubAdminServer server([](const std::string&) { return MakeResponse(200, ""); });
  VolumeMountTestHarness harness;
  ProfileFetcher fetcher(AdminClient(AdminEndpoints{server.admin_base()}),
                         std::make_unique<RecordingRetriever>([]() { return ""; }, nullptr),
                         "/var/local/profiling/envoy.prof", harness.staging_root());
  EXPECT_FALSE(fetcher.window_open());
  absl::Status stopped = fetcher.StopWindow();
  EXPECT_FALSE(stopped.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(stopped)) << stopped;
}

// The ordering contract's job-side half: FetchProfile refuses while a window
// is still open — a retrieval racing a live profiler would read a partial
// file. StopWindow must run first.
TEST(ProfileFetcherTest, FetchProfileWithOpenWindowRefuses) {
  StubAdminServer server([](const std::string& request_line) {
    if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    return MakeResponse(404, "not found");
  });
  VolumeMountTestHarness harness;
  ProfileFetcher fetcher(AdminClient(AdminEndpoints{server.admin_base()}),
                         std::make_unique<RecordingRetriever>([]() { return ""; }, nullptr),
                         "/var/local/profiling/envoy.prof", harness.staging_root());
  ASSERT_TRUE(fetcher.StartWindow(5).ok());
  absl::StatusOr<FetchedProfile> fetched = fetcher.FetchProfile();
  EXPECT_FALSE(fetched.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(fetched.status())) << fetched.status();
  EXPECT_TRUE(fetcher.window_open());
}

TEST(ProfileFetcherTest, RetrievalFailurePropagates) {
  StubAdminServer server([](const std::string& request_line) {
    if (request_line == "POST /cpuprofiler?enable=y HTTP/1.1" ||
        request_line == "POST /cpuprofiler?enable=n HTTP/1.1") {
      return MakeResponse(200, "OK\n");
    }
    return MakeResponse(404, "not found");
  });
  VolumeMountTestHarness harness;
  auto retriever = std::make_unique<RecordingRetriever>([]() { return ""; }, nullptr);
  retriever->fail(true);
  ProfileFetcher fetcher(AdminClient(AdminEndpoints{server.admin_base()}),
                         std::move(retriever), "/var/local/profiling/envoy.prof",
                         harness.staging_root());
  ASSERT_TRUE(fetcher.StartWindow(5).ok());
  ASSERT_TRUE(fetcher.StopWindow().ok());
  absl::StatusOr<FetchedProfile> fetched = fetcher.FetchProfile();
  EXPECT_FALSE(fetched.ok());
  // The window is closed even when retrieval failed, and the stub saw the
  // disable before the failed retrieve attempt.
  EXPECT_FALSE(fetcher.window_open());
  EXPECT_EQ(server.requests().size(), 2);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
