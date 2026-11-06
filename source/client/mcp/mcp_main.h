// FORK-ONLY (We-Amp): the nighthawk_mcp driver — flag parsing, the stdio
// line loop, and graceful-stop wiring. Split from the thin
// source/exe/mcp_main_entry.cc so tests can drive flags + loop in-process
// (same pattern as output_transform_main / live_probe).
//
// Flags (the flag surface the plan specifies; target scoping enforcement
// binds at CONNECT in the steering tools — Task 2 — and the security round
// pins the startup validation):
//   --address            reserved: the listen address for a future
//                        non-stdio transport. The current transport is stdio.
//   --service-address    the Nighthawk service gRPC endpoint the steering
//                        tools drive (default 127.0.0.1:8443, the TUI's
//                        default).
//   --allow-target-cidr  repeatable CIDR allowlist widening the default
//                        loopback-only target posture. Malformed values are
//                        a startup failure.
//   --max-rps            the global-total load ceiling enforced at
//                        start_run/steer (default 100000).
//   --timeout            the bound for cancel_run/final-output waits,
//                        seconds (default 30).
//
// Protocol discipline: stdout is the transport — every byte the server
// writes there is a canonical JSON response line; all diagnostics go to the
// err stream (stderr in production). SIGTERM is a graceful stop: cancel any
// in-flight run, seal the open session, exit 0 (a signal handler may only
// write(2), so the handler drops a byte on a self-pipe and a watcher thread
// performs the stop; the same path runs on stdin EOF).
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_

#include <condition_variable>
#include <deque>
#include <functional>
#include <istream>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {

struct McpServerFlags {
  std::string address;
  // The Nighthawk service gRPC endpoint the steering tools drive (the TUI's
  // --address default; the transport-level --address above stays reserved).
  std::string service_address = "127.0.0.1:8443";
  std::vector<std::string> allow_target_cidrs;
  long long max_rps = 100000;
  long long timeout_s = 30;
  // The profile tool surface (Task 4): the full profile flag surface
  // (--admin-base, --profile-retriever, --profile-mount, --scp-host,
  // --sut-profile-path, --profile-staging, --pprof-tool, --sut-binary,
  // --profile-window-seconds), configured exactly like the TUI's
  // (--profile-mount or --scp-host given = configured). Absent profile
  // flags leave the tools registered but honest (the not-configured error).
  ProfileToolOptions profile;
};

// A tiny fixed-size task pool: the MCP dispatcher's waiter threads run the
// bounded tool waits (steer 4s, cancel_run 35s) OFF the dispatch loop so
// ping keeps answering during a wait (Global Constraints). The destructor
// drains the queue and joins the threads.
class WaiterPool {
public:
  explicit WaiterPool(int thread_count);
  ~WaiterPool();

  WaiterPool(const WaiterPool&) = delete;
  WaiterPool& operator=(const WaiterPool&) = delete;

  void Enqueue(std::function<void()> task);

  // Joins every in-flight task, blocking until the queue drains and the
  // workers idle. Called at the driver's shutdown boundary while the server
  // and the steering controller are still alive — a deferred handler's
  // completion must never land in a torn-down server.
  void Drain();

private:
  void WorkerLoop();

  std::vector<std::thread> threads_;
  std::deque<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_ = false;
  bool drained_ = false;
};

// Runs the stdio transport loop to completion (EOF on `in`). Returns the
// process exit code (0 on a clean shutdown). Protocol responses go to `out`
// as canonical JSON lines; diagnostics go to `err`.
int RunMcpServer(const McpServerFlags& flags, std::istream& in, std::ostream& out,
                 std::ostream& err);

// Parses the flag surface with TCLAP (malformed values are a startup
// failure via TCLAP; a malformed --allow-target-cidr is a startup failure
// with a stderr message), then runs RunMcpServer.
int RunMcpServerMain(int argc, char* argv[], std::istream& in, std::ostream& out,
                     std::ostream& err);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_
