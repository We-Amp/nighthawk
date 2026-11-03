// FORK-ONLY (We-Amp): thin main entry for the nighthawk_tui binary.
#include <iostream>

#include "source/client/tui/tui_app.h"

int main(int argc, char* argv[]) {
  return Nighthawk::Client::runTui(argc, argv, std::cerr);
}
