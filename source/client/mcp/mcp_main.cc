// FORK-ONLY (We-Amp): see mcp_main.h.
#include "source/client/mcp/mcp_main.h"

#include <csignal>
#include <cstdlib>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

#include <unistd.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "tclap/CmdLine.h"

#include "source/client/mcp/mcp_server.h"
#include "source/client/mcp/mcp_tools.h"

namespace Nighthawk {
namespace Client {
namespace {

// SIGTERM graceful stop. Nothing is in flight yet in this milestone step
// (the steering tools own run-cancel + session-seal, Task 2), so the
// graceful stop is exit 0 on the signal boundary; write(2) keeps the
// handler async-signal-safe.
void HandleSigterm(int) {
  const char message[] = "nighthawk_mcp: SIGTERM received, shutting down\n";
  ssize_t unused = write(2, message, sizeof(message) - 1);
  (void)unused;
  _exit(0);
}

} // namespace

int RunMcpServer(const McpServerFlags& flags, std::istream& in, std::ostream& out,
                 std::ostream& err) {
  ToolRegistry tools;
  ResourceRegistry resources;
  RegisterEchoTool(tools);

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
      });

  err << absl::StrCat("nighthawk_mcp: starting (transport=stdio, address=", flags.address.empty() ? "(none)" : flags.address,
                      ", allow_target_cidrs=[", absl::StrJoin(flags.allow_target_cidrs, ","), "]",
                      ", max_rps=", flags.max_rps, ", timeout_s=", flags.timeout_s, ")\n");

  std::string line;
  while (std::getline(in, line)) {
    server.HandleLine(line);
  }
  err << "nighthawk_mcp: stdin closed, shutting down\n";
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
                                  "Bound (seconds) for cancel_run and final-output waits; "
                                  "expiry surfaces as a timed-out tool error.",
                                  false, 30, "seconds", cmd);
    cmd.parse(argc, argv);

    McpServerFlags flags;
    flags.address = address.getValue();
    flags.allow_target_cidrs = allow_target_cidr.getValue();
    flags.max_rps = max_rps.getValue();
    flags.timeout_s = timeout.getValue();

    std::signal(SIGTERM, HandleSigterm);
    return RunMcpServer(flags, in, out, err);
  } catch (TCLAP::ArgException& e) {
    err << absl::StrCat("nighthawk_mcp: flag error: ", e.error(), " '", e.argId(), "'\n");
    return 1;
  }
}

} // namespace Client
} // namespace Nighthawk
