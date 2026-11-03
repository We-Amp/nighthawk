#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "source/client/tui/tui_app.h"

namespace Nighthawk {
namespace Client {
namespace {

// FORK-ONLY (We-Amp): the --selftest smoke must render one static FTXUI frame
// into an off-screen screen, print "tui ok", and exit 0. This makes the TUI
// decidable in a non-TTY environment (CI, scripts).
TEST(TuiAppTest, SelfTestRendersStaticFrame) {
  std::ostringstream err;
  const char* argv[] = {"nighthawk_tui", "--selftest"};
  EXPECT_EQ(runTui(2, argv, err), 0);
  EXPECT_NE(err.str().find("tui ok"), std::string::npos);
}

} // namespace
} // namespace Client
} // namespace Nighthawk
