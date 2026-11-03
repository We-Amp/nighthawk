#pragma once

#include <iosfwd>

namespace Nighthawk {
namespace Client {

/**
 * FORK-ONLY (We-Amp): entry point for nighthawk_live_probe, a small client
 * that drives one Nighthawk service benchmark run opted in to live snapshot
 * streaming, and prints every received LiveSnapshot as a JSON line on the
 * output stream. When the run completes normally, a final JSON line with the
 * run's Output follows. When the probe is interrupted with SIGINT, it cancels
 * the RPC (which prompts the service to stop the run) and prints the last
 * received snapshot as the final line: gRPC never delivers the server's
 * response to a client that cancelled the RPC, so the last snapshot is the
 * best available result.
 *
 * Flags: --address (Nighthawk service gRPC address, e.g. 127.0.0.1:8843),
 * --uri (benchmark target), --rps, --duration (seconds), and optionally
 * --stats-flush-interval (seconds between snapshots; default 1, so that even
 * short probe runs produce snapshots).
 *
 * @param argc argument count, in main() form.
 * @param argv argument strings, in main() form.
 * @param out receives the JSON lines.
 * @return int process exit code: 0 on success or clean interruption, 1 on failure.
 */
int runLiveProbe(int argc, const char* const* argv, std::ostream& out);

} // namespace Client
} // namespace Nighthawk
