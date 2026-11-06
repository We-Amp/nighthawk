// FORK-ONLY (We-Amp): see mcp_main.h.
#include "source/client/mcp/mcp_main.h"

#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "grpc++/grpc++.h"
#include "tclap/CmdLine.h"

#include "source/client/mcp/mcp_resources.h"
#include "source/client/mcp/mcp_server.h"
#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {
namespace {

// --- SIGTERM graceful stop --------------------------------------------------
//
// A signal handler may only write(2). The handler drops one byte on a
// self-pipe; the watcher thread (blocked in read(2) on it) performs the
// actual stop — cancel any in-flight run, seal the open session, exit 0.
// 's' = stop the process; 'w' = the main loop ended normally, just wake and
// join. Both shutdown paths (SIGTERM, stdin EOF) run the same
// GracefulStop.
int g_sigterm_pipe_w = -1;

void HandleSigterm(int) {
  const char message[] = "nighthawk_mcp: SIGTERM received; cancelling any run and sealing the "
                         "session\n";
  ssize_t unused = write(2, message, sizeof(message) - 1);
  (void)unused;
  if (g_sigterm_pipe_w >= 0) {
    const char stop = 's';
    ssize_t unused_pipe = write(g_sigterm_pipe_w, &stop, 1);
    (void)unused_pipe;
  }
}

} // namespace

WaiterPool::WaiterPool(int thread_count) {
  threads_.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    threads_.emplace_back([this]() { WorkerLoop(); });
  }
}

WaiterPool::~WaiterPool() { Drain(); }

void WaiterPool::Drain() {
  std::vector<std::thread> to_join;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (drained_) {
      return;
    }
    drained_ = true;
    stopping_ = true;
    to_join = std::move(threads_);
    threads_.clear();
  }
  cv_.notify_all();
  for (std::thread& thread : to_join) {
    thread.join();
  }
}

void WaiterPool::Enqueue(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
}

void WaiterPool::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
      if (tasks_.empty()) {
        return; // stopping_ and drained
      }
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    task();
  }
}

absl::Status ValidateStartupFlags(const McpServerFlags& flags) {
  // The target allowlist: every entry must parse as addr/len.
  const absl::Status cidrs = ValidateCidrs(flags.allow_target_cidrs);
  if (!cidrs.ok()) {
    return cidrs;
  }
  // The load ceiling must be a positive figure (a ceiling of 0 or below
  // would refuse every run with a nonsense message — a misconfiguration).
  if (flags.max_rps <= 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("--max-rps must be > 0, got ", flags.max_rps));
  }
  if (flags.timeout_s <= 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("--timeout must be > 0, got ", flags.timeout_s));
  }
  // The service endpoint: host:port (bracketed IPv6 literals allowed), the
  // port numeric in range — a malformed endpoint would otherwise surface as
  // a confusing gRPC failure on the first tool call.
  {
    absl::string_view hostport = flags.service_address;
    absl::string_view host = hostport;
    absl::string_view port;
    if (!hostport.empty() && hostport[0] == '[') {
      const size_t close = hostport.find(']');
      if (close == absl::string_view::npos) {
        return absl::InvalidArgumentError(absl::StrCat(
            "--service-address '", flags.service_address,
            "' is malformed: unterminated IPv6 literal (expected [host]:port)"));
      }
      host = hostport.substr(1, close - 1);
      port = hostport.substr(close + 1);
      if (!port.empty() && port[0] != ':') {
        return absl::InvalidArgumentError(absl::StrCat(
            "--service-address '", flags.service_address,
            "' is malformed: text after the IPv6 literal"));
      }
      port = port.empty() ? port : port.substr(1);
    } else {
      const size_t colon = hostport.rfind(':');
      if (colon != absl::string_view::npos) {
        host = hostport.substr(0, colon);
        port = hostport.substr(colon + 1);
      }
    }
    if (host.empty()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "--service-address '", flags.service_address, "' is malformed: empty host"));
    }
    int port_number = 0;
    if (!absl::SimpleAtoi(port, &port_number) || port_number < 1 || port_number > 65535) {
      return absl::InvalidArgumentError(absl::StrCat(
          "--service-address '", flags.service_address,
          "' is malformed: the port must be a number in 1..65535"));
    }
  }
  // The profile window default is pull_profile_window's host-blocking bound;
  // the same cap as the tool argument bounds it (kMaxProfileWindowSeconds).
  if (flags.profile.pipeline.window_seconds < 1 ||
      flags.profile.pipeline.window_seconds > kMaxProfileWindowSeconds) {
    return absl::InvalidArgumentError(absl::StrCat(
        "--profile-window-seconds must be in 1..", kMaxProfileWindowSeconds, ", got ",
        flags.profile.pipeline.window_seconds));
  }
  return absl::OkStatus();
}

int RunMcpServer(const McpServerFlags& flags, std::istream& in, std::ostream& out,
                 std::ostream& err) {
  const auto channel = grpc::CreateChannel(flags.service_address, grpc::InsecureChannelCredentials());
  SteeringToolOptions steering;
  steering.max_rps = flags.max_rps;
  steering.timeout_s = flags.timeout_s;
  steering.allow_target_cidrs = flags.allow_target_cidrs;
  steering.session_prefix = "nighthawk-session";
  SteeringController controller(channel, steering);

  // Declared AFTER the steering controller (destroyed FIRST): the profile
  // controller's destructor joins its worker pool while the steering
  // controller it borrows from is still alive.
  ProfileController profile_controller(controller, flags.profile);

  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);
  RegisterSteeringTools(tools, controller);
  RegisterTelemetryResources(resources, controller);
  RegisterProfileTools(tools, profile_controller);

  // The waiter pool: the bounded tool waits (steer 4s, cancel_run 35s) run
  // here, OFF the dispatch loop, so ping answers during a wait.
  WaiterPool pool(2);

  McpServer server(
      tools, resources,
      // The protocol sink: stdout, one canonical JSON line per response,
      // flushed (a misordered/partial line breaks the client's parse).
      [&](absl::string_view line) {
        out << line << "\n";
        out.flush();
      },
      // Diagnostics: stderr only — stdout is the protocol.
      [&](absl::string_view line) {
        err << line << "\n";
        err.flush();
      },
      // Deferred tool handlers off the dispatch loop.
      [&pool](std::function<void()> task) { pool.Enqueue(std::move(task)); });

  err << absl::StrCat("nighthawk_mcp: starting (transport=stdio, address=",
                      flags.address.empty() ? "(none)" : flags.address,
                      ", service_address=", flags.service_address,
                      ", allow_target_cidrs=[", absl::StrJoin(flags.allow_target_cidrs, ","),
                      "]", ", max_rps=", flags.max_rps, ", timeout_s=", flags.timeout_s, ")\n");

  // The graceful-stop self-pipe and its watcher thread (see mcp_main.h).
  int pipe_fds[2];
  if (::pipe(pipe_fds) != 0) {
    err << "nighthawk_mcp: pipe() failed; SIGTERM will not be graceful\n";
    pipe_fds[0] = pipe_fds[1] = -1;
  } else {
    g_sigterm_pipe_w = pipe_fds[1];
    std::signal(SIGTERM, HandleSigterm);
    // A SIGTERM landing after the watcher exited must not kill the process
    // via SIGPIPE on the closed pipe; the EPIPE write failure is the signal.
    std::signal(SIGPIPE, SIG_IGN);
  }

  auto graceful_stop = [&controller, &err]() {
    controller.GracefulStop();
    err << "nighthawk_mcp: graceful stop complete (run cancelled, session sealed)\n";
  };

  std::thread watcher;
  if (pipe_fds[0] >= 0) {
    watcher = std::thread([pipe_r = pipe_fds[0], graceful_stop]() {
      char command = 0;
      while (::read(pipe_r, &command, 1) == 1) {
        if (command == 's') {
          graceful_stop();
          // The session is sealed and the run cancelled; exit 0 per the
          // graceful-stop contract (the stdio loop dies with the process).
          ::_exit(0);
        }
        if (command == 'w') {
          return; // the main loop ended normally; nothing to stop
        }
      }
    });
  }

  std::string line;
  while (std::getline(in, line)) {
    server.HandleLine(line);
  }

  err << "nighthawk_mcp: stdin closed; cancelling any run and sealing the session\n";
  graceful_stop();
  if (pipe_fds[1] >= 0) {
    // Wake the watcher (join path) before closing the pipe.
    const char wake = 'w';
    ssize_t unused = write(pipe_fds[1], &wake, 1);
    (void)unused;
  }
  if (watcher.joinable()) {
    watcher.join();
  }
  if (g_sigterm_pipe_w >= 0) {
    g_sigterm_pipe_w = -1;
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
  }
  // The deferred handlers join HERE, while the server and the steering
  // controller are still alive — their completion must never land in a
  // torn-down server.
  pool.Drain();
  return 0;
}

int RunMcpServerMain(int argc, char* argv[], std::istream& in, std::ostream& out,
                     std::ostream& err) {
  try {
    TCLAP::CmdLine cmd("nighthawk_mcp: MCP stdio server exposing nighthawk live steering "
                       "to agents. Protocol responses on stdout; diagnostics on stderr.",
                       ' ', kMcpServerVersion);
    TCLAP::ValueArg<std::string> address("", "address",
                                         "Reserved: listen address for a future non-stdio "
                                         "transport. The current transport is stdio.",
                                         false, "", "string", cmd);
    TCLAP::ValueArg<std::string> service_address("", "service-address",
                                                 "The Nighthawk service gRPC endpoint the "
                                                 "steering tools drive, e.g. 127.0.0.1:8443.",
                                                 false, "127.0.0.1:8443", "string", cmd);
    TCLAP::MultiArg<std::string> allow_target_cidr(
        "", "allow-target-cidr",
        "Repeatable CIDR allowlist widening the default loopback-only target "
        "posture. Malformed values are a startup failure.",
        false, "cidr", cmd);
    TCLAP::ValueArg<long> max_rps("", "max-rps",
                                  "Load ceiling for the declared global total, enforced at "
                                  "start_run/steer; exceeding it is a tool error naming the cap.",
                                  false, 100000, "rps", cmd);
    TCLAP::ValueArg<long> timeout("", "timeout",
                                  "Bound (seconds) for cancel_run's final-output wait (+5s "
                                  "grace); expiry surfaces as a timed-out tool error.",
                                  false, 30, "seconds", cmd);
    // The M1 profiling flags, mirroring the TUI's surface. Profiling is
    // configured when a profile mount (volume retriever) or scp host (rig
    // variant) is given; without one the profile tools exist and refuse with
    // the honest not-configured error.
    TCLAP::ValueArg<std::string> admin_base(
        "", "admin-base", "SUT admin endpoint base URL. Default: http://127.0.0.1:9901.", false,
        "http://127.0.0.1:9901", "string", cmd);
    TCLAP::ValueArg<std::string> profile_retriever(
        "", "profile-retriever", "Profile retriever kind: mount or scp. Default: mount.", false,
        "mount", "string", cmd);
    TCLAP::ValueArg<std::string> profile_mount(
        "", "profile-mount",
        "Local path of the SUT profile bind mount (the mount retriever reads there). Empty = "
        "profiling unconfigured.",
        false, "", "string", cmd);
    TCLAP::ValueArg<std::string> scp_host(
        "", "scp-host", "scp destination (user@host) for the rig retriever.", false, "", "string",
        cmd);
    TCLAP::ValueArg<std::string> sut_profile_path(
        "", "sut-profile-path",
        "SUT-side path the CPU profiler writes (admin.profile_path). Default: "
        "/var/local/profiling/envoy.prof.",
        false, "/var/local/profiling/envoy.prof", "string", cmd);
    TCLAP::ValueArg<std::string> profile_staging(
        "", "profile-staging",
        "Local staging directory for fetched profiles. Default: /tmp/nh-profiles.", false,
        "/tmp/nh-profiles", "string", cmd);
    TCLAP::ValueArg<std::string> pprof_tool(
        "", "pprof-tool",
        "Symbolizer argv prefix, space separated (SUBPROCESS ONLY - never a linked dependency). "
        "Default: \"go tool pprof\".",
        false, "go tool pprof", "string", cmd);
    TCLAP::ValueArg<std::string> sut_binary(
        "", "sut-binary",
        "SUT binary path inside the PPROF_BINARY_PATH layout (binary reachability for "
        "symbolization). Empty = no symbolization attempted.",
        false, "", "string", cmd);
    TCLAP::ValueArg<long> profile_window_seconds("", "profile-window-seconds",
                                                 "Default profile window length for "
                                                 "pull_profile_window. Default: 5.",
                                                 false, 5, "int", cmd);
    cmd.parse(argc, argv);

    McpServerFlags flags;
    flags.address = address.getValue();
    flags.service_address = service_address.getValue();
    flags.allow_target_cidrs = allow_target_cidr.getValue();
    flags.max_rps = max_rps.getValue();
    flags.timeout_s = timeout.getValue();

    // The profile flag surface (Task 4), the TUI's configured rule. An
    // unknown retriever kind is a startup failure naming the flag (the
    // mount/scp choice is not silently guessed).
    const std::string retriever_kind = profile_retriever.getValue();
    if (retriever_kind != "mount" && retriever_kind != "scp") {
      err << absl::StrCat("nighthawk_mcp: --profile-retriever must be 'mount' or 'scp', got '",
                          retriever_kind, "'\n");
      return 1;
    }
    flags.profile.configured =
        !profile_mount.getValue().empty() || !scp_host.getValue().empty();
    flags.profile.pipeline.admin.admin_base = admin_base.getValue();
    flags.profile.pipeline.retriever_kind =
        (retriever_kind == "scp" || !scp_host.getValue().empty())
            ? Tui::ProfilePipelineConfig::RetrieverKind::kScp
            : Tui::ProfilePipelineConfig::RetrieverKind::kVolumeMount;
    flags.profile.pipeline.mount_root = profile_mount.getValue();
    flags.profile.pipeline.scp_host = scp_host.getValue();
    flags.profile.pipeline.sut_profile_path = sut_profile_path.getValue();
    flags.profile.pipeline.staging_dir = profile_staging.getValue();
    // The argv prefix splits on plain spaces ("go tool pprof").
    flags.profile.pipeline.pprof_tool_argv =
        absl::StrSplit(pprof_tool.getValue(), ' ', absl::SkipEmpty());
    flags.profile.pipeline.sut_binary_path = sut_binary.getValue();
    flags.profile.pipeline.window_seconds = static_cast<int>(profile_window_seconds.getValue());
    const char* binary_layout = std::getenv("PPROF_BINARY_PATH");
    flags.profile.pipeline.pprof_binary_path = binary_layout != nullptr ? binary_layout : "";

    // The startup flag validation (Task 5): malformed values are a startup
    // failure — the reason on stderr, exit non-zero — before any protocol
    // line is spoken. Runs over the FULLY-populated flag surface (the
    // profile window default included).
    const absl::Status startup = ValidateStartupFlags(flags);
    if (!startup.ok()) {
      err << absl::StrCat("nighthawk_mcp: ", startup.message(), "\n");
      return 1;
    }

    return RunMcpServer(flags, in, out, err);
  } catch (TCLAP::ArgException& e) {
    err << absl::StrCat("nighthawk_mcp: flag error: ", e.error(), " '", e.argId(), "'\n");
    return 1;
  }
}

} // namespace Client
} // namespace Nighthawk
