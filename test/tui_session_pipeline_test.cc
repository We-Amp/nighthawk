#include <grpc++/grpc++.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
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

#include "source/client/service_impl.h"
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

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
