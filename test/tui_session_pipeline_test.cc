#include <grpc++/grpc++.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <netinet/in.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"

#include "absl/strings/str_cat.h"

#include "api/client/service.grpc.pb.h"
#include "api/client/service.pb.h"

#include "ftxui/screen/screen.hpp"

#include "source/client/service_impl.h"
#include "source/client/tui/layout.h"
#include "source/client/tui/run_feed.h"
#include "source/client/tui/run_view_model.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/session_reader.h"
#include "source/client/tui/session_writer.h"
#include "source/client/tui/tui_app.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// FORK-ONLY (We-Amp): headless pipeline integration tests. They drive the
// SAME pipeline the interactive TuiApp drives — feed -> ApplySnapshot/store ->
// SessionWriter via ToMetricSeries, stop key -> feed stop(), close strictly
// after on_finished — minus the FTXUI screen (ScreenInteractive is not
// headless-testable; the shell binding is exercised manually in Task 7).

// The generated protos live in nighthawk::client; the C++ TUI API lives in
// Nighthawk::Client::Tui.
using nighthawk::client::LiveSnapshot;
using nighthawk::client::MetricSeriesRecord;
using nighthawk::client::RunEndRecord;
using nighthawk::client::SessionHeader;
using nighthawk::client::SessionRecord;
using nighthawk::client::StartRequest;

std::string tempPath(const std::string& name) {
  const char* dir = ::getenv("TEST_TMPDIR");
  return absl::StrCat(dir == nullptr ? "/tmp" : dir, "/", name);
}

// A fresh, empty directory unique to the caller, under TEST_TMPDIR (a /tmp
// fallback keeps direct-binary runs working). Tests that create files with a
// name PREFIX must scan only their own directory: SessionWriter pairs every
// session file with a "<path>.jsonl" sidecar, and a scan over the shared
// TEST_TMPDIR both races other files and can match the sidecar by prefix.
std::string uniqueTempDir(const std::string& name) {
  std::string tmpl = tempPath(name) + "-XXXXXX";
  // mkdtemp mutates the template in place; the returned directory exists and
  // is empty.
  const char* made = ::mkdtemp(tmpl.data());
  if (made != nullptr) {
    return std::string(made);
  }
  // mkdtemp failed; fall back to a created (not guaranteed-unique) directory
  // rather than failing the test on directory creation alone.
  const std::string fallback = tempPath(name);
  std::error_code ec;
  std::filesystem::create_directories(fallback, ec);
  return fallback;
}

int64_t nowUnixNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// A minimal HTTP/1.1 server that answers every request with an empty 200
// response, giving the feed's benchmark real upstream traffic. Same pattern as
// test/tui_run_feed_test.cc.
class TestHttpListener {
public:
  TestHttpListener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(listen_fd_, -1);
    int one = 1;
    EXPECT_EQ(::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)), 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    EXPECT_EQ(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    EXPECT_EQ(::listen(listen_fd_, 16), 0);
    socklen_t addr_len = sizeof(addr);
    EXPECT_EQ(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this]() { acceptLoop(); });
  }

  ~TestHttpListener() {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  TestHttpListener(const TestHttpListener&) = delete;
  TestHttpListener& operator=(const TestHttpListener&) = delete;

  std::string uri() const { return fmt::format("http://127.0.0.1:{}/", port_); }

private:
  static constexpr int kNoSignalFlags =
#ifdef MSG_NOSIGNAL
      MSG_NOSIGNAL;
#else
      0;
#endif

  void acceptLoop() {
    while (true) {
      int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        return;
      }
#ifdef SO_NOSIGPIPE
      int one = 1;
      ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
      serve(fd);
    }
  }

  static void serve(int fd) {
    static constexpr absl::string_view kResponse = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    std::string buffer;
    char chunk[4096];
    while (true) {
      size_t request_end;
      while ((request_end = buffer.find("\r\n\r\n")) != std::string::npos) {
        buffer.erase(0, request_end + 4);
        if (!sendAll(fd, kResponse)) {
          ::close(fd);
          return;
        }
      }
      ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
      if (n <= 0) {
        break;
      }
      buffer.append(chunk, static_cast<size_t>(n));
    }
    ::close(fd);
  }

  static bool sendAll(int fd, absl::string_view data) {
    size_t sent = 0;
    while (sent < size_t(data.size())) {
      ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, kNoSignalFlags);
      if (n <= 0) {
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

  int listen_fd_{-1};
  uint16_t port_{0};
  std::thread thread_;
};

StartRequest makeStartRequest(const std::string& uri, uint32_t rps, uint32_t duration_seconds) {
  StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_uri()->set_value(uri);
  options->mutable_requests_per_second()->set_value(rps);
  options->mutable_duration()->set_seconds(duration_seconds);
  // Same opt-in path the dialog builder takes: presence is the marker.
  options->mutable_live_streaming();
  options->mutable_stats_flush_interval()->set_value(1);
  return request;
}

class SessionPipelineTest : public testing::Test {
public:
  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>();
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    channel_ = grpc::CreateChannel(serviceAddress(), grpc::InsecureChannelCredentials());
  }

  void TearDown() override { server_->Shutdown(); }

  std::string serviceAddress() const { return fmt::format("127.0.0.1:{}", grpc_server_port_); }

  const std::shared_ptr<grpc::ChannelInterface>& channel() const { return channel_; }

  TestHttpListener& listener() {
    if (!listener_) {
      listener_ = std::make_unique<TestHttpListener>();
    }
    return *listener_;
  }

private:
  std::unique_ptr<ServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  int grpc_server_port_{0};
  std::shared_ptr<grpc::ChannelInterface> channel_;
  std::unique_ptr<TestHttpListener> listener_;
};

// The stop-key lifecycle, end to end: writer opened with ToSessionHeader, the
// feed's snapshots folded into the store AND enqueued via ToMetricSeries, a
// stop() after the first snapshot, then — strictly after on_finished — the
// single close(completed=false). The file must parse back with every enqueued
// snapshot present (none lost to a close race), no drops, and the run end
// matching both the writer's authoritative drop count and the stop semantics.
TEST_F(SessionPipelineTest, StopKeyPipelineWritesParseableSession) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string session_id = "pipeline-stop-test";
  const int64_t started_unix_ns = nowUnixNs();
  const std::string path = tempPath("tui-pipeline-stop.nhsession");

  SessionHeader header = ToSessionHeader(request, session_id, started_unix_ns);
  auto writer_or = SessionWriter::open(path, header);
  ASSERT_TRUE(writer_or.status().ok());
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);

  SnapshotStore store;
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<int64_t> enqueued_monotonic_ns;
  bool finished = false;
  absl::Status finished_status = absl::UnknownError("not fired");

  RunFeedCallbacks callbacks;
  callbacks.on_snapshot = [&](const LiveSnapshot& snapshot) {
    // The exact snapshot path the shell drives: fold into the model, swap the
    // store, enqueue the persisted record. A null snapshot-notification
    // stands in for the screen's gated redraw post.
    store.store(ApplySnapshot(RunViewModel(), snapshot));
    SessionRecord record;
    *record.mutable_metric_series() = ToMetricSeries(snapshot);
    writer->enqueue(std::move(record));
    std::lock_guard<std::mutex> lock(mutex);
    enqueued_monotonic_ns.push_back(snapshot.monotonic_ns());
    cv.notify_all();
  };
  callbacks.on_finished = [&](const absl::Status& status) {
    std::lock_guard<std::mutex> lock(mutex);
    finished = true;
    finished_status = status;
    cv.notify_all();
  };

  RunFeed feed(channel(), callbacks);
  ASSERT_EQ(feed.start(request), absl::OkStatus());

  // Run is demonstrably in flight.
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 10s, [&]() { return !enqueued_monotonic_ns.empty(); }));
  }

  // The stop key: feed stop() only; the service's watchdog ends the run.
  feed.stop();

  // The ordering contract asserted by construction: nothing is closed until
  // on_finished has landed (close() before on_finished is exactly the bug
  // class this pipeline must make impossible).
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 10s, [&]() { return finished; }));
    EXPECT_FALSE(finished_status.ok());
    EXPECT_EQ(finished_status.code(), absl::StatusCode::kCancelled);
  }
  EXPECT_FALSE(feed.running());

  const std::vector<int64_t> delivered_monotonic_ns = [&]() {
    std::lock_guard<std::mutex> lock(mutex);
    return enqueued_monotonic_ns;
  }();
  ASSERT_FALSE(delivered_monotonic_ns.empty());

  RunEndRecord run_end;
  run_end.set_completed(false);
  run_end.set_ended_unix_ns(nowUnixNs());
  ASSERT_TRUE(writer->close(run_end).ok());
  const uint64_t writer_dropped = writer->droppedCount();

  // Parse it back.
  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  std::vector<MetricSeriesRecord> series;
  bool have_run_end = false;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      header = (*record_or)->header();
      break;
    case SessionRecord::kMetricSeries:
      series.push_back((*record_or)->metric_series());
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      run_end = (*record_or)->run_end();
      break;
    default:
      ADD_FAILURE() << "unexpected unset record frame";
      break;
    }
  }

  ASSERT_TRUE(have_header);
  ASSERT_TRUE(have_run_end);
  ASSERT_GE(series.size(), 1u);

  // Header round-trips the exact start request and metadata.
  EXPECT_EQ(header.format_version(), "1");
  EXPECT_EQ(header.session_id(), session_id);
  EXPECT_EQ(header.started_unix_ns(), started_unix_ns);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(header.start_request(), request));

  // The persisted snapshots are the delivered ones: every enqueued snapshot's
  // monotonic_ns is present in the file (none lost to a close race), and the
  // cumulative counters never go backwards.
  std::multiset<int64_t> file_monotonic_ns;
  for (const MetricSeriesRecord& record : series) {
    file_monotonic_ns.insert(record.monotonic_ns());
    EXPECT_EQ(record.phase_id(), "main");
    EXPECT_EQ(record.epoch_id(), "");
  }
  for (const int64_t monotonic_ns : delivered_monotonic_ns) {
    EXPECT_NE(file_monotonic_ns.find(monotonic_ns), file_monotonic_ns.end())
        << "delivered snapshot " << monotonic_ns << " missing from the session file";
  }
  std::map<std::string, uint64_t> last_counters;
  for (const MetricSeriesRecord& record : series) {
    for (const auto& [name, value] : record.counters()) {
      auto it = last_counters.find(name);
      if (it != last_counters.end()) {
        EXPECT_GE(value, it->second) << "counter " << name << " went backwards";
      }
      last_counters[name] = value;
    }
  }

  // Run end: stopped, nothing dropped, and the file's count equals the
  // writer's authoritative count.
  EXPECT_FALSE(run_end.completed());
  EXPECT_FALSE(run_end.has_output());
  EXPECT_EQ(run_end.dropped_records(), 0u);
  EXPECT_EQ(run_end.dropped_records(), writer_dropped);
}

// A run left to finish on its own closes with completed=true and the final
// output carried into the run end record.
TEST_F(SessionPipelineTest, CompletedRunWritesOutput) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/2);
  const std::string path = tempPath("tui-pipeline-complete.nhsession");

  SessionHeader header = ToSessionHeader(request, "pipeline-complete-test", nowUnixNs());
  auto writer_or = SessionWriter::open(path, header);
  ASSERT_TRUE(writer_or.status().ok());
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);

  SnapshotStore store;
  std::mutex mutex;
  std::condition_variable cv;
  bool finished = false;
  bool completed = false;
  nighthawk::client::Output final_output;

  RunFeedCallbacks callbacks;
  callbacks.on_snapshot = [&](const LiveSnapshot& snapshot) {
    store.store(ApplySnapshot(RunViewModel(), snapshot));
    SessionRecord record;
    *record.mutable_metric_series() = ToMetricSeries(snapshot);
    writer->enqueue(std::move(record));
  };
  callbacks.on_completed = [&](const nighthawk::client::Output& output) {
    std::lock_guard<std::mutex> lock(mutex);
    completed = true;
    final_output = output;
    cv.notify_all();
  };
  callbacks.on_finished = [&](const absl::Status& status) {
    std::lock_guard<std::mutex> lock(mutex);
    finished = status.ok();
    cv.notify_all();
  };

  RunFeed feed(channel(), callbacks);
  ASSERT_EQ(feed.start(request), absl::OkStatus());

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 30s, [&]() { return finished; }));
    EXPECT_TRUE(completed);
  }

  RunEndRecord run_end;
  run_end.set_completed(true);
  *run_end.mutable_output() = final_output;
  run_end.set_ended_unix_ns(nowUnixNs());
  ASSERT_TRUE(writer->close(run_end).ok());
  EXPECT_EQ(writer->droppedCount(), 0u);

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_run_end = false;
  size_t series_count = 0;
  RunEndRecord file_run_end;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    if ((*record_or)->record_case() == SessionRecord::kMetricSeries) {
      series_count++;
    } else if ((*record_or)->record_case() == SessionRecord::kRunEnd) {
      have_run_end = true;
      file_run_end = (*record_or)->run_end();
    }
  }
  ASSERT_TRUE(have_run_end);
  EXPECT_TRUE(file_run_end.completed());
  ASSERT_TRUE(file_run_end.has_output());
  EXPECT_GT(file_run_end.output().results_size(), 0);
  // The natural run's output carries counters (the charts' data source).
  size_t counter_count = 0;
  for (const nighthawk::client::Result& result : file_run_end.output().results()) {
    counter_count += result.counters_size();
  }
  EXPECT_GT(counter_count, 0u);
  EXPECT_GT(series_count, 0u);
}

// The Ctrl-C / EOF teardown (fix round, regression test): the controller is
// destroyed with the run live and finishSession never ran. ~RunController
// cancels and joins the feed FIRST (feed_ is declared last, so members are
// destroyed in the safe order), and the final read-thread callbacks — a last
// onSnapshot enqueuing into the writer, a last onFinished locking the mutex —
// find every other member alive. Pre-fix, the writer and the notification were
// destroyed before the feed, so a live-run teardown raced destroyed objects.
TEST_F(SessionPipelineTest, ControllerTeardownWhileRunIsLiveIsSafe) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string prefix = tempPath("tui-pipeline-teardown");

  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());

  // The session file pair opened before the run: "<prefix>-<unix_ns>.nhsession".
  const std::string session_path = controller->sessionPath();
  EXPECT_NE(session_path.find(prefix), std::string::npos);
  EXPECT_NE(session_path.find(".nhsession"), std::string::npos);

  // The run is demonstrably live and snapshots are flowing (startRun stores
  // the reset model up front, so the real signal is a stored counter point).
  // Note the wait is on the counter series, not the latency band: the live
  // aggregator's statistics order is unspecified (per-worker flat_hash_map
  // fold), so statistics[0] — the band's v0 source — is often a zero-count
  // response-class statistic even under load. Flagged in the task report.
  bool have_snapshot = false;
  for (int i = 0; i < 100 && !have_snapshot; ++i) {
    const std::shared_ptr<const RunViewModel> model = controller->model();
    have_snapshot = model != nullptr && !model->counters.empty();
    if (!have_snapshot) {
      std::this_thread::sleep_for(100ms);
    }
  }
  ASSERT_TRUE(have_snapshot);
  EXPECT_TRUE(controller->runInProgress());

  // Destroy with the run hot: the destructor cancels the RPC, joins the read
  // thread (letting the last callbacks fire into still-alive members), then
  // drains the open session without a run end record. Completing this
  // destruction — promptly, and without crashing — is the assertion.
  controller.reset();

  // The drained session still parses: header + at least one metric series,
  // and (unlike every close() path) no run end record.
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_run_end = false;
  size_t series_count = 0;
  SessionHeader header;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      header = (*record_or)->header();
      break;
    case SessionRecord::kMetricSeries:
      series_count++;
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      break;
    default:
      ADD_FAILURE() << "unexpected unset record frame";
      break;
    }
  }
  ASSERT_TRUE(have_header);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(header.start_request(), request));
  EXPECT_GT(series_count, 0u);
  EXPECT_FALSE(have_run_end);
}

// F2 fix regression: the Ctrl-C loop-exit path must flush like q. FTXUI's
// Ctrl-C / EOF exit returns from ScreenInteractive::Loop WITHOUT the q
// handler's finishSession (the q key is the only in-loop flush), so pre-fix
// the shell only dropped the snapshot notification post-loop and the open
// session drained in the destructor with no run end record — on the real
// binary a 0-byte session file silently lost (finding F2, M0b acceptance).
// The shell now routes the post-loop teardown through
// RunController::handleLoopExit(); this drives the controller the way Ctrl-C
// does — run live, no finishSession ever called — and asserts the session
// left behind parses with the same run end record the q path writes.
TEST_F(SessionPipelineTest, LoopExitWithoutQFlushesSessionWithRunEnd) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string prefix = tempPath("tui-pipeline-ctrlc");

  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  const std::string session_path = controller->sessionPath();

  // Run demonstrably live, snapshots flowing (same signal as the teardown
  // test: the stored counter series, not the latency band).
  bool have_snapshot = false;
  for (int i = 0; i < 100 && !have_snapshot; ++i) {
    const std::shared_ptr<const RunViewModel> model = controller->model();
    have_snapshot = model != nullptr && !model->counters.empty();
    if (!have_snapshot) {
      std::this_thread::sleep_for(100ms);
    }
  }
  ASSERT_TRUE(have_snapshot);
  EXPECT_TRUE(controller->runInProgress());

  // Ctrl-C: the FTXUI loop returned without the q handler having run. The
  // shell's post-loop teardown must run the same flush/close sequence the q
  // path uses (stop-if-running -> wait on_finished -> writer close with a
  // run end record).
  ASSERT_TRUE(controller->handleLoopExit().ok());

  // The session left behind parses: header, every delivered series, and the
  // run end record that pre-fix was lost to the destructor drain.
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_run_end = false;
  size_t series_count = 0;
  RunEndRecord run_end;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      break;
    case SessionRecord::kMetricSeries:
      series_count++;
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      run_end = (*record_or)->run_end();
      break;
    default:
      ADD_FAILURE() << "unexpected unset record frame";
      break;
    }
  }
  ASSERT_TRUE(have_header);
  EXPECT_GT(series_count, 0u);
  ASSERT_TRUE(have_run_end);
  // A Ctrl-C mid-run stop is a stopped run, exactly like the s key then q.
  EXPECT_FALSE(run_end.completed());
  EXPECT_EQ(run_end.dropped_records(), 0u);
}

// F2 final-fix regression (the loop-exit belt alone could not save Ctrl-C):
// on the real binary FTXUI's default signal machinery intercepted Ctrl-C
// before the shell's CatchEvent ever saw it — it records SIGINT and re-raises
// it after PostMain, so the process died before the post-loop
// handleLoopExit() teardown ran (T7 evidence: exit 0, 0-byte session). The
// fix routes Ctrl-C through the shell key map itself (App::ForceHandleCtrlC(false)
// plus a Ctrl-C case in HandleShellEvent): the event reaches the handler
// inside the loop and takes the exact q path — finishSession (flush + close
// with a run end record), then exit. This test drives the real handler the
// CatchEvent installs, with a real controller and a live run, asserting the
// handler consumed the event, requested the loop exit, stopped the run, and
// left a parseable, run-end-stamped session behind.
TEST_F(SessionPipelineTest, CtrlCEventRoutesThroughTheShellKeyMapAndFlushes) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string prefix = tempPath("tui-pipeline-ctrlc-event");

  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  const std::string session_path = controller->sessionPath();

  // Run demonstrably live, snapshots flowing (the stored counter series, as
  // in the sibling teardown tests).
  bool have_snapshot = false;
  for (int i = 0; i < 100 && !have_snapshot; ++i) {
    const std::shared_ptr<const RunViewModel> model = controller->model();
    have_snapshot = model != nullptr && !model->counters.empty();
    if (!have_snapshot) {
      std::this_thread::sleep_for(100ms);
    }
  }
  ASSERT_TRUE(have_snapshot);
  EXPECT_TRUE(controller->runInProgress());

  // What actually reaches the shell now: the CtrlC event, handled inside the
  // loop by the same key map the CatchEvent installs.
  ShellState state;
  bool exit_requested = false;
  const bool consumed = HandleShellEvent(ftxui::Event::CtrlC, state, *controller,
                                         /*force_redraw=*/[] {},
                                         /*exit_loop=*/[&exit_requested]() { exit_requested = true; });

  // The q-equivalent path: consumed, loop exit requested, run stopped, and
  // the session closed by the in-loop flush.
  EXPECT_TRUE(consumed);
  EXPECT_TRUE(exit_requested);
  EXPECT_FALSE(controller->runInProgress());
  EXPECT_TRUE(controller->sessionPath().empty());

  // The session left behind parses with a run end record — pre-fix this
  // reached neither the in-loop flush nor the post-loop belt on the real
  // binary (0-byte file).
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_run_end = false;
  size_t series_count = 0;
  RunEndRecord run_end;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      break;
    case SessionRecord::kMetricSeries:
      series_count++;
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      run_end = (*record_or)->run_end();
      break;
    default:
      ADD_FAILURE() << "unexpected unset record frame";
      break;
    }
  }
  ASSERT_TRUE(have_header);
  EXPECT_GT(series_count, 0u);
  ASSERT_TRUE(have_run_end);
  // A Ctrl-C mid-run stop is a stopped run, exactly like the s key then q.
  EXPECT_FALSE(run_end.completed());
}

// Finding-1 residual (re-review): Ctrl-C must quit from EVERY mode. The
// dialog-owning editing mode swallowed it (the key map only checked Ctrl-C
// in navigate mode), so with the dialog open FTXUI's default machinery
// recorded SIGINT and the re-raise killed the process with the session open
// — a 0-byte file again. The handler must take the same quitShell path
// (flush + exit) with the dialog open; the dialog state is irrelevant to
// shutdown.
TEST_F(SessionPipelineTest, CtrlCWhileEditingDialogFlushesAndExits) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string prefix = tempPath("tui-pipeline-ctrlc-dialog");

  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  const std::string session_path = controller->sessionPath();

  bool have_snapshot = false;
  for (int i = 0; i < 100 && !have_snapshot; ++i) {
    const std::shared_ptr<const RunViewModel> model = controller->model();
    have_snapshot = model != nullptr && !model->counters.empty();
    if (!have_snapshot) {
      std::this_thread::sleep_for(100ms);
    }
  }
  ASSERT_TRUE(have_snapshot);
  EXPECT_TRUE(controller->runInProgress());

  // The dialog is open mid-typing when the user hits C-c.
  ShellState state;
  state.mode = InputMode::kEditingStartRun;
  state.mode_mirror.store(state.mode, std::memory_order_relaxed);
  state.uri_field = listener().uri();
  state.rps_field = "100";
  bool exit_requested = false;
  const bool consumed =
      HandleShellEvent(ftxui::Event::CtrlC, state, *controller, /*force_redraw=*/[] {},
                       /*exit_loop=*/[&exit_requested]() { exit_requested = true; });

  EXPECT_TRUE(consumed);
  EXPECT_TRUE(exit_requested);
  EXPECT_FALSE(controller->runInProgress());
  EXPECT_TRUE(controller->sessionPath().empty());

  // The session still parses with a run end record.
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  bool have_run_end = false;
  size_t series_count = 0;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    if ((*record_or)->record_case() == SessionRecord::kMetricSeries) {
      series_count++;
    } else if ((*record_or)->record_case() == SessionRecord::kRunEnd) {
      have_run_end = true;
    }
  }
  EXPECT_GT(series_count, 0u);
  EXPECT_TRUE(have_run_end);
}


// M0b final-review finding (dialog honesty): an enter whose fields validate
// but whose start the controller refuses (a session already open — the
// steady state after every run until q) must surface as a distinct
// controller-rejection line, NOT the field-error path (which would render a
// "start run - 0 errors" border title on the real shell). Drives the real
// key map with a real controller holding an open session.
TEST_F(SessionPipelineTest, EnterWithOpenSessionSurfacesTheControllerRejection) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  const std::string prefix = tempPath("tui-pipeline-rejected-enter");

  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  const std::string session_path = controller->sessionPath();

  // The dialog as the user left it: valid fields, ready to submit.
  ShellState state;
  state.mode = InputMode::kEditingStartRun;
  state.mode_mirror.store(state.mode, std::memory_order_relaxed);
  state.uri_field = listener().uri();
  state.rps_field = "100";
  state.duration_field = "60";

  const bool consumed =
      HandleShellEvent(ftxui::Event::Return, state, *controller, /*force_redraw=*/[] {},
                       /*exit_loop=*/[] {});
  EXPECT_TRUE(consumed);
  // The dialog stays open, and the failure is the controller-rejection path:
  // no field-error flag, a distinct cannot-start line naming the way out.
  EXPECT_EQ(state.mode, InputMode::kEditingStartRun);
  EXPECT_FALSE(state.show_errors);
  EXPECT_EQ(state.start_rejected, "cannot start: a session is already open (q quits and saves)");
  // The open session is untouched by the refused start.
  EXPECT_EQ(controller->sessionPath(), session_path);

  // Seam-crossing check (re-review): the state above is only user-visible if
  // the shell's per-frame ShellState → OverlayState mapping carries it.
  // Render the frame the same way runInteractive does — via the shared
  // FillOverlayState mapping, not a hand-built overlay — and assert the
  // rejection actually reads on screen.
  OverlayState overlay;
  FillOverlayState(state, &overlay);
  overlay.active_field = StartRunDialogModel::kDuration;
  const ftxui::Element frame_element =
      RenderLayout(100, 30, /*active_tab=*/0, state.mode, RunViewModel{}, state.dialog,
                   controller->sessionPath(), nighthawk::client::CommandLineOptions{}, &overlay);
  ftxui::Screen screen(100, 30);
  ftxui::Render(screen, frame_element);
  const std::string frame_text = screen.ToString();
  EXPECT_NE(frame_text.find("cannot start: a session is already open"), std::string::npos)
      << frame_text;
  EXPECT_NE(frame_text.find("(q quits and saves)"), std::string::npos);
  // And the inverse: a clean state renders no rejection line (the mapping
  // must not leave stale text behind).
  state.start_rejected.clear();
  OverlayState clean_overlay;
  FillOverlayState(state, &clean_overlay);
  clean_overlay.active_field = StartRunDialogModel::kDuration;
  const ftxui::Element clean_element =
      RenderLayout(100, 30, /*active_tab=*/0, state.mode, RunViewModel{}, state.dialog,
                   controller->sessionPath(), nighthawk::client::CommandLineOptions{},
                   &clean_overlay);
  ftxui::Screen clean_screen(100, 30);
  ftxui::Render(clean_screen, clean_element);
  EXPECT_EQ(clean_screen.ToString().find("cannot start:"), std::string::npos);

  // Esc cancels and clears the rejection.
  const bool esc_consumed =
      HandleShellEvent(ftxui::Event::Escape, state, *controller, /*force_redraw=*/[] {},
                       /*exit_loop=*/[] {});
  EXPECT_TRUE(esc_consumed);
  EXPECT_EQ(state.mode, InputMode::kNavigate);
  EXPECT_TRUE(state.start_rejected.empty());
}

// The other half of the dialog honesty split: fields that fail validation
// still take the field-error path (inline errors + the error-count border
// title), with no controller rejection mixed in.
TEST_F(SessionPipelineTest, EnterWithInvalidFieldsTakesTheFieldErrorPath) {
  auto controller = std::make_unique<RunController>(channel(), tempPath("tui-pipeline-bad-enter"));

  ShellState state;
  state.mode = InputMode::kEditingStartRun;
  state.mode_mirror.store(state.mode, std::memory_order_relaxed);
  // Empty uri, unparseable rps/duration: field validation must reject.
  state.uri_field = "";
  state.rps_field = "abc";
  state.duration_field = "";

  const bool consumed =
      HandleShellEvent(ftxui::Event::Return, state, *controller, /*force_redraw=*/[] {},
                       /*exit_loop=*/[] {});
  EXPECT_TRUE(consumed);
  EXPECT_EQ(state.mode, InputMode::kEditingStartRun);
  EXPECT_TRUE(state.show_errors);
  EXPECT_TRUE(state.start_rejected.empty());
  // No start was attempted, so no session opened.
  EXPECT_TRUE(controller->sessionPath().empty());
}

// Conversion: a snapshot's fields land on the record verbatim — including the
// empty epoch id, preserved pre-A2 (constant phase id pre-A1, M0a semantics).
TEST(SessionConversionTest, ToMetricSeriesCarriesPlaceholders) {
  LiveSnapshot snapshot;
  snapshot.set_wall_time_unix_ns(1'000);
  snapshot.set_monotonic_ns(2'000);
  snapshot.set_phase_id("main");
  snapshot.set_epoch_id("");
  (*snapshot.mutable_counters())["benchmark.http_2xx"] = 42;
  nighthawk::client::StatisticSnapshot* stat = snapshot.add_statistics();
  stat->set_id("benchmark.http_latency");
  stat->set_count(42);
  stat->set_p50_ns(100);
  stat->set_p90_ns(200);
  stat->set_p99_ns(300);

  MetricSeriesRecord record = ToMetricSeries(snapshot);
  EXPECT_EQ(record.wall_time_unix_ns(), 1'000);
  EXPECT_EQ(record.monotonic_ns(), 2'000);
  EXPECT_EQ(record.phase_id(), "main");
  EXPECT_TRUE(record.epoch_id().empty());
  ASSERT_EQ(record.counters_size(), 1);
  EXPECT_EQ(record.counters().at("benchmark.http_2xx"), 42);
  ASSERT_EQ(record.statistics_size(), 1);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(record.statistics(0), *stat));
}

// Conversion: the header pins the format version, the session id, the open
// time, and the exact start request.
TEST(SessionConversionTest, ToSessionHeaderCarriesRequestAndMetadata) {
  const StartRequest request = makeStartRequest("http://127.0.0.1:10000/", 100, 60);
  SessionHeader header = ToSessionHeader(request, "sid", 123'456);
  EXPECT_EQ(header.format_version(), "1");
  EXPECT_EQ(header.session_id(), "sid");
  EXPECT_EQ(header.started_unix_ns(), 123'456);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(header.start_request(), request));
}

// ---------------------------------------------------------------------------
// UX batch 2: the T7 findings.
// ---------------------------------------------------------------------------

// F1, the stop->start window: after s (run stopped, session still open until
// q), a valid dialog enter must keep the dialog open and surface the
// controller's rejection on the in-dialog line — the same seam the
// session-already-open steady state uses, not a silent no-op and not a field
// error. Drives the real key map against a real controller mid-window.
TEST_F(SessionPipelineTest, StopThenStartSurfacesTheControllerRejection) {
  const StartRequest request = makeStartRequest(listener().uri(), /*rps=*/10,
                                                /*duration_seconds=*/60);
  auto controller = std::make_unique<RunController>(channel(), tempPath("tui-pipeline-stop-start"));

  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  bool have_snapshot = false;
  for (int i = 0; i < 100 && !have_snapshot; ++i) {
    const std::shared_ptr<const RunViewModel> model = controller->model();
    have_snapshot = model != nullptr && !model->counters.empty();
    if (!have_snapshot) {
      std::this_thread::sleep_for(100ms);
    }
  }
  ASSERT_TRUE(have_snapshot);

  // The stop key: the run winds down (on_finished lands), but the session
  // stays open until q — this is the stop->start window.
  controller->stopRun();
  for (int i = 0; i < 100 && controller->runInProgress(); ++i) {
    std::this_thread::sleep_for(100ms);
  }
  ASSERT_FALSE(controller->runInProgress());
  const std::string session_path = controller->sessionPath();
  ASSERT_FALSE(session_path.empty());

  // A valid dialog enter in the window: refused, visibly, on the rejection
  // line.
  ShellState state;
  state.mode = InputMode::kEditingStartRun;
  state.mode_mirror.store(state.mode, std::memory_order_relaxed);
  state.uri_field = listener().uri();
  state.rps_field = "100";
  state.duration_field = "60";
  const bool consumed =
      HandleShellEvent(ftxui::Event::Return, state, *controller, /*force_redraw=*/[] {},
                       /*exit_loop=*/[] {});
  EXPECT_TRUE(consumed);
  EXPECT_EQ(state.mode, InputMode::kEditingStartRun);
  EXPECT_FALSE(state.show_errors);
  EXPECT_EQ(state.start_rejected, "cannot start: a session is already open (q quits and saves)");
  EXPECT_EQ(controller->sessionPath(), session_path);

  // The rejection actually reads on screen via the shared shell mapping.
  OverlayState overlay;
  FillOverlayState(state, &overlay);
  overlay.active_field = StartRunDialogModel::kDuration;
  const ftxui::Element frame_element =
      RenderLayout(100, 30, /*active_tab=*/0, state.mode, RunViewModel{}, state.dialog,
                   controller->sessionPath(), nighthawk::client::CommandLineOptions{}, &overlay);
  ftxui::Screen screen(100, 30);
  ftxui::Render(screen, frame_element);
  EXPECT_NE(screen.ToString().find("cannot start: a session is already open"), std::string::npos);

  // The refusal is the session gate, not the feed state: the stopped run's
  // session closes cleanly on the way out.
  EXPECT_TRUE(controller->finishSession(/*completed=*/false).ok());
  EXPECT_TRUE(controller->sessionPath().empty());
}

// F3: a start whose RPC write fails must not leave a header-only session
// orphan behind. The writer opens BEFORE the run launches (the ordering the
// snapshot path requires), so the failure path closes the just-opened file
// with a run end record (completed=false) — the file parses as a complete,
// honest empty session, not an orphan. Drives a real controller against a
// listener that accepts and immediately closes connections, so the stream
// write fails fast.
TEST_F(SessionPipelineTest, FailedStartLeavesNoHeaderOnlySession) {
  // A TCP listener that accepts and slams the door: connect succeeds, the
  // HTTP/2 preface write fails, feed start errors.
  int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_NE(listen_fd, -1);
  int one = 1;
  ASSERT_EQ(::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)), 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(::listen(listen_fd, 16), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
  const uint16_t port = ntohs(addr.sin_port);
  std::thread closer([listen_fd]() {
    while (true) {
      const int fd = ::accept(listen_fd, nullptr, nullptr);
      if (fd < 0) {
        return;
      }
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
    }
  });

  auto channel = grpc::CreateChannel(fmt::format("127.0.0.1:{}", port),
                                     grpc::InsecureChannelCredentials());
  // The writer opens "<prefix>-<unix_ns>.nhsession" plus a "<...>.jsonl"
  // sidecar; isolate both in a directory of this test's own so the scan below
  // cannot see any other test's files (or a previous repeat's leftovers).
  const std::string dir = uniqueTempDir("tui-pipeline-failed-start");
  const std::string prefix = absl::StrCat(dir, "/session");
  auto controller = std::make_unique<RunController>(channel, prefix);

  const StartRequest request = makeStartRequest("http://127.0.0.1:9/", /*rps=*/10,
                                                /*duration_seconds=*/60);
  const absl::Status started = controller->startRun(request);
  EXPECT_FALSE(started.ok());

  // No session left open in the controller.
  EXPECT_TRUE(controller->sessionPath().empty());

  // The session file the start opened before failing: it must exist AND carry
  // the run end record — never a header-only orphan. It sits at
  // "<prefix>-<unix_ns>.nhsession"; find it by prefix. The name must END in
  // ".nhsession": a bare substring match would also take the writer's
  // "<...>.nhsession.jsonl" sidecar, and directory order between the pair is
  // unspecified (the sidecar can enumerate first, and reading it as a session
  // file fails).
  static constexpr absl::string_view kSuffix = ".nhsession";
  std::string session_path;
  for (auto it = std::filesystem::directory_iterator(dir);
       it != std::filesystem::directory_iterator(); ++it) {
    const std::string name = it->path().filename().string();
    if (name.rfind("session-", 0) == 0 && name.size() >= kSuffix.size() &&
        name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) == 0) {
      session_path = it->path().string();
      break;
    }
  }
  ::shutdown(listen_fd, SHUT_RDWR);
  ::close(listen_fd);
  closer.join();

  ASSERT_FALSE(session_path.empty()) << "the failed start left no session file at all";
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok());
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_run_end = false;
  size_t series_count = 0;
  RunEndRecord run_end;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok());
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      break;
    case SessionRecord::kMetricSeries:
      series_count++;
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      run_end = (*record_or)->run_end();
      break;
    default:
      ADD_FAILURE() << "unexpected unset record frame";
      break;
    }
  }
  ASSERT_TRUE(have_header);
  ASSERT_TRUE(have_run_end) << "header-only session orphan (finding F3)";
  EXPECT_FALSE(run_end.completed());
  EXPECT_EQ(series_count, 0u);

  // Leave nothing behind: the isolated directory holds only this test's
  // session pair, and the test owns its lifetime.
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

// M2 Task 7 wiring, end to end: a two-phase run through the SAME pipeline the
// TUI drives. The service streams each completed boundary as a phase_event
// response; the controller's intake stores it AND persists it as the
// session's phase_boundary record at boundary time (stream-at-boundary; DQ-3:
// metadata only, never a synthetic snapshot). The final Output's
// PhaseMetadata (in the run end record) is the post-run completeness source.
TEST_F(SessionPipelineTest, PhaseBoundariesStreamAndPersist) {
  StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_uri()->set_value(listener().uri());
  options->mutable_connections()->set_value(4);
  options->mutable_timeout()->set_seconds(5);
  options->mutable_live_streaming();
  options->mutable_stats_flush_interval()->set_value(1);
  const char* names[] = {"p0", "p1"};
  for (uint32_t i = 0; i < 2; ++i) {
    nighthawk::client::PhaseSpec* phase = options->add_phases();
    phase->set_name(names[i]);
    phase->mutable_duration()->set_seconds(2);
    phase->set_requests_per_second(50);
    phase->mutable_concurrency()->set_value("1");
  }

  const std::string prefix = tempPath("tui-pipeline-boundary");
  auto controller = std::make_unique<RunController>(channel(), prefix);
  ASSERT_EQ(controller->startRun(request), absl::OkStatus());
  const std::string session_path = controller->sessionPath();

  // The run completes naturally (4s of phases + teardown).
  for (int i = 0; i < 200 && controller->runInProgress(); ++i) {
    std::this_thread::sleep_for(100ms);
  }
  EXPECT_FALSE(controller->runInProgress());
  ASSERT_TRUE(controller->finishSession(/*completed=*/true).ok());

  // The intake holds the boundary, joined on the engine event: one boundary
  // for the two-phase run, ending p0, next p1.
  const std::vector<nighthawk::client::PhaseBoundaryEvent> events = controller->boundaryEvents();
  ASSERT_EQ(events.size(), 1u);
  EXPECT_EQ(events[0].ending_phase_id(), "p0");
  EXPECT_EQ(events[0].phase_id(), "p1");
  EXPECT_GT(events[0].wall_time_unix_ns(), 0);
  EXPECT_GT(events[0].monotonic_ns(), 0);
  EXPECT_GT(events[0].drain_duration().seconds() + events[0].drain_duration().nanos(), 0);

  // The session carries the persisted record, field-joined to the event.
  auto reader_or = SessionReader::open(session_path);
  ASSERT_TRUE(reader_or.status().ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  bool have_header = false;
  bool have_boundary = false;
  bool have_run_end = false;
  size_t series_count = 0;
  while (true) {
    auto record_or = reader->next();
    ASSERT_TRUE(record_or.status().ok()) << record_or.status();
    if (!record_or->has_value()) {
      break;
    }
    switch ((*record_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      break;
    case SessionRecord::kMetricSeries:
      series_count++;
      break;
    case SessionRecord::kPhaseBoundary: {
      have_boundary = true;
      const auto& got = (*record_or)->phase_boundary();
      EXPECT_EQ(got.phase_id(), "p0");
      EXPECT_EQ(got.next_phase_id(), "p1");
      EXPECT_EQ(got.phase_index(), 0);
      EXPECT_EQ(got.wall_time_unix_ns(), events[0].wall_time_unix_ns());
      EXPECT_EQ(got.monotonic_ns(), events[0].monotonic_ns());
      EXPECT_EQ(got.worker_count(), 1);
      // Stream-at-boundary fields stay default: the Output's PhaseMetadata
      // (in the run end record) is the post-run completeness source.
      EXPECT_EQ(got.connections_created(), 0);
      EXPECT_EQ(got.gate_exceeded(), 0);
      break;
    }
    case SessionRecord::kRunEnd:
      have_run_end = true;
      EXPECT_TRUE((*record_or)->run_end().has_output());
      EXPECT_GT((*record_or)->run_end().output().phase_metadata_size(), 0);
      break;
    case SessionRecord::kProfileWindow:
      // This run opens no profile windows.
      ADD_FAILURE() << "unexpected profile_window frame";
      break;
    case SessionRecord::kKnobEpoch:
      // This run sends no knob turns.
      ADD_FAILURE() << "unexpected knob_epoch frame";
      break;
    case SessionRecord::RECORD_NOT_SET:
      ADD_FAILURE() << "unset record frame";
      break;
    }
  }
  EXPECT_TRUE(have_header);
  EXPECT_TRUE(have_boundary) << "the boundary was not persisted at stream time";
  EXPECT_GT(series_count, 0u);
  EXPECT_TRUE(have_run_end);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
