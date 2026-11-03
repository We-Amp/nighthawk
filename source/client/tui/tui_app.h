#pragma once

#include <iosfwd>

namespace Nighthawk {
namespace Client {

/**
 * FORK-ONLY (We-Amp): entry point for nighthawk_tui, the terminal UI that
 * will visualize live Nighthawk benchmark snapshots. Skeleton task: the flags
 * are parsed with TCLAP and interactive mode is not implemented yet.
 *
 * Flags: --address (Nighthawk service gRPC endpoint, default
 * 127.0.0.1:8443), --session-prefix (default nighthawk-session), and
 * --selftest (render one static FTXUI frame into an off-screen
 * ftxui::Screen(100, 30), print "tui ok" on the error stream, and return 0 —
 * a decidable smoke test that works in a non-TTY environment).
 *
 * Dependency quarantine: this library and everything it pulls in must stay
 * clear of @envoy//source/... C++ targets (see source/client/tui/README.md
 * for the standing guard that enforces this).
 *
 * @param argc argument count, in main() form.
 * @param argv argument strings, in main() form.
 * @param err receives diagnostics and the selftest output.
 * @return int process exit code: 0 on success, 1 on failure.
 */
int runTui(int argc, const char* const* argv, std::ostream& err);

} // namespace Client
} // namespace Nighthawk
