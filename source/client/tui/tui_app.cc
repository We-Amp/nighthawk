#include "source/client/tui/tui_app.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "ftxui/component/component.hpp"
#include "ftxui/component/event.hpp"
#include "ftxui/component/screen_interactive.hpp"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"
#include "tclap/CmdLine.h"

#include "source/client/tui/layout.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/start_run_dialog.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using ftxui::CatchEvent;
using ftxui::Component;
using ftxui::Event;
using ftxui::Input;
using ftxui::Renderer;

// How long finishSession waits for the stopped run's on_finished before
// giving up (and letting the writer drain without a run end record).
constexpr std::chrono::seconds kFinishTimeout{30};

} // namespace

RunController::RunController(const std::shared_ptr<grpc::ChannelInterface>& channel,
                             std::string session_prefix)
    : feed_(channel,
            RunFeedCallbacks{std::bind(&RunController::onSnapshot, this, std::placeholders::_1),
                             std::bind(&RunController::onCompleted, this, std::placeholders::_1),
                             std::bind(&RunController::onFinished, this, std::placeholders::_1)}),
      session_prefix_(std::move(session_prefix)) {}

RunController::~RunController() = default;

void RunController::set_snapshot_notification(std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  snapshot_notification_ = std::move(callback);
}

absl::Status RunController::startRun(const nighthawk::client::StartRequest& request) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_ != nullptr) {
      return absl::FailedPreconditionError("a session is already open; finish it first");
    }
  }

  // Mark the run un-finished before the read thread can race us: start()
  // returns only once the run is in flight, and a very short run could
  // otherwise deliver on_finished before the flag flips.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = false;
    completed_ = false;
    has_output_ = false;
    final_output_ = nighthawk::client::Output{};
    model_ = RunViewModel{};
  }

  // Open the session file pair BEFORE the run starts: once feed_.start()
  // returns, snapshots can arrive on the read thread, and one delivered before
  // a later-opened writer would be lost to the session file. A failure here
  // surfaces without a run ever launching.
  const int64_t unix_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
  const std::string path = absl::StrCat(session_prefix_, "-", unix_ns, ".nhsession");
  auto writer_or =
      SessionWriter::open(path, ToSessionHeader(request, /*session_id=*/path, unix_ns));
  if (!writer_or.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = true;
    return writer_or.status();
  }
  // Publish the writer before the run launches: from the moment feed_.start()
  // returns, snapshots can arrive on the read thread, and every one of them
  // must find an open session to land in.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_path_ = path;
    writer_ = std::move(*writer_or);
  }

  const absl::Status started = feed_.start(request);
  if (!started.ok()) {
    // No run launched; take the writer back and close the header-only session
    // (completed=false, single close), then surface the start failure.
    std::unique_ptr<SessionWriter> writer;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
      writer = std::move(writer_);
      session_path_.clear();
    }
    nighthawk::client::RunEndRecord run_end;
    run_end.set_completed(false);
    run_end.set_ended_unix_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    // A close error on this header-only session is not surfaced (the start
    // failure is the actionable status); the close still happened exactly
    // once.
    (void)writer->close(run_end);
    return started;
  }
  return absl::OkStatus();
}

void RunController::stopRun() { feed_.stop(); }

bool RunController::runInProgress() const { return feed_.running(); }

bool RunController::lastRunEndedCompleted() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return completed_;
}

std::shared_ptr<const RunViewModel> RunController::model() const { return store_.load(); }

const std::string& RunController::sessionPath() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return session_path_;
}

absl::Status RunController::finishSession(bool completed) {
  // Stop-if-running, then wait for on_finished: close() is single-call and
  // strictly after the feed's terminal event, per the session writer's
  // ordering contract.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (feed_.running()) {
      feed_.stop();
    }
  }
  bool finished = false;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    finished = cv_.wait_for(lock, kFinishTimeout, [this]() { return finished_; });
  }
  if (!finished) {
    return absl::InternalError(
        "run feed did not deliver on_finished within the shutdown budget; the open session "
        "drains without a run end record");
  }

  // Single-call close: moving the writer out makes a second close impossible.
  std::unique_ptr<SessionWriter> writer;
  nighthawk::client::Output output;
  bool has_output = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    writer = std::move(writer_);
    session_path_.clear();
    output = final_output_;
    has_output = has_output_;
  }
  if (writer == nullptr) {
    return absl::OkStatus();
  }
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(completed);
  if (completed && has_output) {
    *run_end.mutable_output() = output;
  }
  run_end.set_ended_unix_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count());
  return writer->close(run_end);
}

void RunController::onSnapshot(const nighthawk::client::LiveSnapshot& snapshot) {
  // The snapshot path (spec §4): fold into the model, swap the immutable
  // store copy, persist the record, then ping the shell for exactly one
  // gated redraw.
  model_ = ApplySnapshot(std::move(model_), snapshot);
  store_.store(model_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_ != nullptr) {
      nighthawk::client::SessionRecord record;
      *record.mutable_metric_series() = ToMetricSeries(snapshot);
      writer_->enqueue(std::move(record));
    }
    if (snapshot_notification_ != nullptr) {
      snapshot_notification_();
    }
  }
}

void RunController::onCompleted(const nighthawk::client::Output& output) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    final_output_ = output;
    completed_ = true;
    has_output_ = true;
  }
  // A natural run end only; a cancelled run never sees this callback.
  model_.run_state = "completed";
  store_.store(model_);
}

void RunController::onFinished(const absl::Status& /*status*/) {
  model_.run_state = lastRunEndedCompleted() ? "completed" : "stopped";
  store_.store(model_);
  std::lock_guard<std::mutex> lock(mutex_);
  finished_ = true;
  cv_.notify_all();
}

namespace {

// The interactive shell: RenderLayout composition, the navigate-mode key map
// (c charts, k knobs, n start-run dialog, s stop, q quit) and the dialog
// editor. The lifecycle logic lives in RunController; this layer only owns
// the terminal. FTXUI's ScreenInteractive loop is not headless-testable —
// this binding is exercised manually (Task 7).
int runInteractive(const std::string& address, const std::string& session_prefix) {
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  RunController controller(channel, session_prefix);

  StartRunDialogModel dialog;
  int active_tab = 0;
  InputMode mode = InputMode::kNavigate;
  // The feed thread snapshots the mode to decide whether posting a redraw is
  // allowed (RefreshSuspended); the UI thread updates the mirror on every
  // transition.
  std::atomic<InputMode> mode_mirror{InputMode::kNavigate};
  std::string uri_field;
  std::string rps_field;
  std::string duration_field;

  Component uri_input = Input(&uri_field, "target uri");
  Component rps_input = Input(&rps_field, "requests per second");
  Component duration_input = Input(&duration_field, "duration in seconds");

  // Fullscreen (the alternate-screen buffer) keeps the live TUI from messing
  // up the terminal's scrollback.
  ftxui::App screen = ftxui::App::Fullscreen();

  // Exactly one redraw per stored snapshot, gated by the input mode (spec §4):
  // while an input mode owns the keyboard the panes catch up only on the one
  // forced redraw the dialog close posts.
  controller.set_snapshot_notification([&screen, &mode_mirror]() {
    if (!RefreshSuspended(mode_mirror.load(std::memory_order_relaxed))) {
      screen.PostEvent(Event::Custom);
    }
  });

  Component editor = ftxui::Container::Vertical({uri_input, rps_input, duration_input});
  Component view = Renderer(editor, [&]() {
    static const RunViewModel kIdleModel;
    std::shared_ptr<const RunViewModel> model = controller.model();
    const RunViewModel& view_model = model == nullptr ? kIdleModel : *model;
    if (mode == InputMode::kEditingStartRun) {
      // The dialog editor replaces the tab pane; the status bar stays.
      return ftxui::vbox({
          RenderStatusBar(view_model, mode, controller.sessionPath(), dialog.uri()),
          ftxui::separator(),
          ftxui::text("start a run - enter: start, esc: cancel"),
          ftxui::hbox({ftxui::text("uri: "), uri_input->Render()}),
          ftxui::hbox({ftxui::text("rps: "), rps_input->Render()}),
          ftxui::hbox({ftxui::text("duration (s): "), duration_input->Render()}),
      });
    }
    return RenderLayout(screen.dimx(), screen.dimy(), active_tab, mode, view_model, dialog,
                        controller.sessionPath());
  });

  Component app = CatchEvent(view, [&](Event event) {
    if (mode == InputMode::kEditingStartRun) {
      if (event == Event::Escape) {
        uri_field.clear();
        rps_field.clear();
        duration_field.clear();
        mode = InputMode::kNavigate;
        mode_mirror.store(mode, std::memory_order_relaxed);
        // Exactly one forced redraw so the panes catch up on the state edited
        // under the suspension (input_mode.h's rule).
        screen.PostEvent(Event::Custom);
        return true;
      }
      if (event == Event::Return) {
        dialog.set_uri(uri_field);
        uint32_t rps = 0;
        uint32_t duration_seconds = 0;
        // Unparseable (or empty) fields stay 0, which the dialog rejects.
        const bool parsed = absl::SimpleAtoi(rps_field, &rps) &&
                            absl::SimpleAtoi(duration_field, &duration_seconds);
        dialog.set_rps(rps);
        dialog.set_duration_seconds(duration_seconds);
        if (parsed && dialog.valid() && controller.startRun(dialog.build_request()).ok()) {
          uri_field.clear();
          rps_field.clear();
          duration_field.clear();
          mode = InputMode::kNavigate;
          mode_mirror.store(mode, std::memory_order_relaxed);
          // Exactly one forced redraw on the dialog close (input_mode.h).
          screen.PostEvent(Event::Custom);
        }
        // An invalid field or a declined start stays in editing mode.
        return true;
      }
      return false; // let the text inputs handle the character
    }

    // Navigate mode key map.
    if (event == Event::Character('c')) {
      active_tab = 0;
      return true;
    }
    if (event == Event::Character('k')) {
      active_tab = 1;
      return true;
    }
    if (event == Event::Character('n')) {
      uri_field.clear();
      rps_field.clear();
      duration_field.clear();
      mode = InputMode::kEditingStartRun;
      mode_mirror.store(mode, std::memory_order_relaxed);
      return true;
    }
    if (event == Event::Character('s')) {
      // Stop key: feed stop() only; the service's watchdog ends the run.
      controller.stopRun();
      return true;
    }
    if (event == Event::Character('q')) {
      // Quit: stop-if-running, close the session (completed reflects how the
      // run actually ended), then exit. The last snapshot shown before exit is
      // the final data (gRPC cancel semantics). A close error mid-exit is not
      // actionable: the writer then drains without a run end record at
      // destruction (documented).
      (void)controller.finishSession(controller.lastRunEndedCompleted());
      screen.Exit();
      return true;
    }
    return false;
  });

  screen.Loop(app);
  return 0;
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk

namespace Nighthawk {
namespace Client {

int runTui(int argc, const char* const* argv, std::ostream& err) {
  TCLAP::CmdLine cmd("nighthawk_tui: terminal UI for Nighthawk benchmark runs", ' ', "1.0");
  TCLAP::ValueArg<std::string> address(
      "", "address",
      "Address of the Nighthawk service gRPC endpoint, for example 127.0.0.1:8443. Default: "
      "127.0.0.1:8443.",
      false, "127.0.0.1:8443", "string", cmd);
  TCLAP::ValueArg<std::string> session_prefix(
      "", "session-prefix", "Prefix for Nighthawk session naming. Default: nighthawk-session.",
      false, "nighthawk-session", "string", cmd);
  TCLAP::SwitchArg selftest(
      "", "selftest",
      "Render one static frame into an off-screen screen, print \"tui ok\" and exit.", cmd);
  cmd.setExceptionHandling(false);
  try {
    cmd.parse(argc, argv);
  } catch (const TCLAP::ArgException& e) {
    // Same translation as Utility::parseCommand, minus the MalformedArgvException
    // dependency: write the message and fail.
    err << e.what() << "\n";
    return 1;
  } catch (const TCLAP::ExitException&) {
    // --help or --version: the output has already been written by TCLAP.
    return 0;
  }

  if (selftest.getValue()) {
    // One static frame of the real layout (status bar over the charts tab with
    // an idle model): proves the FTXUI dependency and the render stack without
    // needing a TTY, so CI and scripts can smoke the binary.
    const Tui::RunViewModel idle_model;
    const Tui::StartRunDialogModel dialog;
    ftxui::Element frame =
        Tui::RenderLayout(/*width=*/100, /*height=*/30, /*active_tab=*/0, Tui::InputMode::kNavigate,
                          idle_model, dialog, "selftest");
    ftxui::Screen screen(100, 30);
    ftxui::Render(screen, frame);
    err << "tui ok\n";
    return 0;
  }

  return Tui::runInteractive(address.getValue(), session_prefix.getValue());
}

} // namespace Client
} // namespace Nighthawk
