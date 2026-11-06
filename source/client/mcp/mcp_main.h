// FORK-ONLY (We-Amp): the nighthawk_mcp driver — flag parsing, the stdio
// line loop, and graceful-stop wiring. Split from the thin
// source/exe/mcp_main_entry.cc so tests can drive flags + loop in-process
// (same pattern as output_transform_main / live_probe).
//
// Flags (the flag surface the plan specifies; target scoping enforcement
// lands with the steering tools, the security round pins startup
// validation):
//   --address            reserved: the listen address for a future
//                        non-stdio transport. The current transport is stdio.
//   --allow-target-cidr  repeatable CIDR allowlist widening the default
//                        loopback-only target posture. (Parsed here; the
//                        check itself binds at CONNECT, Task 2/5.)
//   --max-rps            the global-total load ceiling enforced at
//                        start_run/steer (default 100000).
//   --timeout            the bound for cancel_run/final-output waits,
//                        seconds (default 30).
//
// Protocol discipline: stdout is the transport — every byte the server
// writes there is a canonical JSON response line; all diagnostics go to the
// err stream (stderr in production). SIGTERM is a graceful stop: today that
// is exit 0 on the read-loop boundary; cancel-any-run + session-seal wiring
// lands with the steering tools (Task 2).
#ifndef NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_
#define NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_

#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace Nighthawk {
namespace Client {

struct McpServerFlags {
  std::string address;
  std::vector<std::string> allow_target_cidrs;
  long long max_rps = 100000;
  long long timeout_s = 30;
};

// Runs the stdio transport loop to completion (EOF on `in`). Returns the
// process exit code (0 on a clean shutdown). Protocol responses go to `out`
// as canonical JSON lines; diagnostics go to `err`.
int RunMcpServer(const McpServerFlags& flags, std::istream& in, std::ostream& out,
                 std::ostream& err);

// Parses the flag surface with TCLAP, installs the SIGTERM graceful-stop
// handler, then runs RunMcpServer. Exits non-zero via TCLAP on malformed
// flags.
int RunMcpServerMain(int argc, char* argv[], std::istream& in, std::ostream& out,
                     std::ostream& err);

} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_CLIENT_MCP_MCP_MAIN_H_
