// FORK-ONLY (We-Amp): thin main entry for the nighthawk_live_probe binary.
#include <iostream>

#include "absl/debugging/symbolize.h"

#include "source/client/live_probe.h"

int main(int argc, char* argv[]) {
#ifndef __APPLE__
  // absl::Symbolize mostly works without this, but this improves corner case
  // handling, such as running in a chroot jail.
  absl::InitializeSymbolizer(argv[0]);
#endif
  return Nighthawk::Client::runLiveProbe(argc, argv, std::cout);
}
