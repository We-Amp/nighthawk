#include "source/client/tui/tui_app.h"

#include <iostream>

#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"
#include "tclap/CmdLine.h"

namespace Nighthawk {
namespace Client {

// FORK-ONLY (We-Amp): TCLAP parsing mirrors the house pattern in
// live_probe.cc, but without Utility::parseCommand — that helper lives in
// //source/common, which drags @envoy//source C++ targets into this library's
// dependency graph and would violate the TUI dependency quarantine (see
// source/client/tui/README.md). A two-catch translation of the same logic is
// all the skeleton needs.
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
    // Static placeholder frame: proves the FTXUI dependency renders without
    // needing a TTY, so CI and scripts can smoke the binary.
    ftxui::Element frame = ftxui::vbox({
        ftxui::text("nighthawk_tui"),
        ftxui::separator(),
        ftxui::text("placeholder frame"),
        ftxui::text("interactive mode lands in Task 6"),
    });
    ftxui::Screen screen(100, 30);
    ftxui::Render(screen, frame);
    err << "tui ok\n";
    return 0;
  }

  err << "interactive mode lands in Task 6\n";
  return 1;
}

} // namespace Client
} // namespace Nighthawk
