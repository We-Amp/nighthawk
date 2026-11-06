// FORK-ONLY (We-Amp): thin main entry for the nighthawk_mcp binary. The flag
// surface, the stdio loop, and the SIGTERM graceful-stop wiring live in
// source/client/mcp/mcp_main.cc so tests can drive them in-process.
#include <iostream>

#include "source/client/mcp/mcp_main.h"

int main(int argc, char* argv[]) {
  return Nighthawk::Client::RunMcpServerMain(argc, argv, std::cin, std::cout, std::cerr);
}
