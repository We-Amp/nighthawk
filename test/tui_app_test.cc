#include <grpc++/grpc++.h>

#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "source/client/tui/tui_app.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
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

// M1 plan task 6, Review Focus 5's wiring half: the navigate-mode key map,
// pinned AS CODE. The binding table is the single source of truth the help
// overlay renders from; this test asserts it contains EXACTLY the documented
// table — including the M2/M3 collision fix (plain n stays the start-run
// dialog, N opens the noise-floor view, / enters search). The human check of
// the same table on the live terminal is Task 8's demo.
TEST(KeyMapTest, BindingsMatchTheDocumentedTable) {
  const std::vector<KeyBinding> bindings = NavigateKeyBindings();
  ASSERT_FALSE(bindings.empty());

  // (key, action) pairs, exactly as the plan's key map documents them.
  const std::vector<std::pair<const char*, const char*>> expected = {
      {"c", "switch to the charts tab"},
      {"k", "switch to the knobs tab"},
      {"f", "switch to the flamegraph tab"},
      {"n", "open the start-run dialog"},
      {"s", "stop the running run (live runs only)"},
      {"q", "quit; a running run is stopped first"},
      {"?", "toggle help (esc closes it too)"},
      {"Arrows", "flamegraph frame navigation (flame tab)"},
      {"Enter", "zoom to the selected frame (flame tab)"},
      {"Esc/Backspace", "unzoom to the tree root (flame tab)"},
      {"/", "search mode (enter applies, esc cancels)"},
      {"p", "start/stop a profile window"},
      {"d", "differential (default: last two pure)"},
      {"N", "noise-floor view (plain n stays dialog)"},
  };
  ASSERT_EQ(bindings.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(bindings[i].key, expected[i].first) << "row " << i;
    EXPECT_EQ(bindings[i].action, expected[i].second) << "row " << i;
  }
}

// The dispatch half of the same pin: the events actually route to the
// documented actions. Drives the real HandleShellEvent over a real
// RunController (an unroutable channel: none of these keys needs the
// network; p against unconfigured profiling refuses with an honest note).
class KeyMapDispatchTest : public testing::Test {
protected:
  void SetUp() override {
    channel_ = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
    controller_ = std::make_unique<RunController>(channel_, /*session_prefix=*/"km-test");
  }

  bool Press(const ftxui::Event& event) {
    return HandleShellEvent(event, state_, *controller_, /*force_redraw=*/[] {},
                            /*exit_loop=*/[] {});
  }

  ShellState state_;
  std::shared_ptr<grpc::ChannelInterface> channel_;
  std::unique_ptr<RunController> controller_;
};

TEST_F(KeyMapDispatchTest, TabKeys) {
  EXPECT_TRUE(Press(ftxui::Event::Character('f')));
  EXPECT_EQ(state_.active_tab, 1);
  EXPECT_TRUE(Press(ftxui::Event::Character('c')));
  EXPECT_EQ(state_.active_tab, 0);
  EXPECT_TRUE(Press(ftxui::Event::Character('k')));
  EXPECT_EQ(state_.active_tab, 2);
}

TEST_F(KeyMapDispatchTest, PlainNOpensTheStartRunDialog) {
  EXPECT_TRUE(Press(ftxui::Event::Character('n')));
  EXPECT_EQ(state_.mode, InputMode::kEditingStartRun);
  // Esc cancels back to navigate.
  EXPECT_TRUE(Press(ftxui::Event::Escape));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
}

// The collision fix, pinned: shift-n never opens the start-run dialog; it
// dispatches the noise-floor view (refused with a note while there are no
// windows to compare).
TEST_F(KeyMapDispatchTest, ShiftNNeverOpensTheDialog) {
  EXPECT_TRUE(Press(ftxui::Event::Character('N')));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_NE(controller_->flameNote().find("noise floor"), std::string::npos)
      << controller_->flameNote();
  EXPECT_NE(controller_->flameNote().find("pure windows"), std::string::npos)
      << controller_->flameNote();
}

TEST_F(KeyMapDispatchTest, SlashEntersSearchModeAndEscCancels) {
  EXPECT_TRUE(Press(ftxui::Event::Character('/')));
  EXPECT_EQ(state_.mode, InputMode::kEditingSearch);
  // Typed characters edit the search field, not the key map.
  EXPECT_TRUE(Press(ftxui::Event::Character('H')));
  EXPECT_TRUE(Press(ftxui::Event::Character('t')));
  EXPECT_EQ(state_.search_field, "Ht");
  // Esc cancels: back to navigate, query discarded.
  EXPECT_TRUE(Press(ftxui::Event::Escape));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_TRUE(state_.search_field.empty());
}

TEST_F(KeyMapDispatchTest, SearchEnterAppliesTheQuery) {
  EXPECT_TRUE(Press(ftxui::Event::Character('/')));
  EXPECT_TRUE(Press(ftxui::Event::Character('W')));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_EQ(state_.flame_view.search, "W");
}

TEST_F(KeyMapDispatchTest, UnconfiguredProfilingRefusesHonestly) {
  EXPECT_FALSE(controller_->profilingConfigured());
  const absl::Status started = controller_->StartProfileWindow();
  EXPECT_FALSE(started.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(started)) << started;
  const absl::Status stopped = controller_->StopProfileWindowAndFold();
  EXPECT_FALSE(stopped.ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(stopped)) << stopped;
}

// Flame-tab navigation is a no-op on an empty store — consumed, never a
// crash, view state unchanged where there is nothing to address.
TEST_F(KeyMapDispatchTest, FlameNavigationWithoutWindowsIsANoOp) {
  EXPECT_TRUE(Press(ftxui::Event::Character('f')));
  EXPECT_TRUE(Press(ftxui::Event::ArrowDown));
  EXPECT_TRUE(Press(ftxui::Event::ArrowUp));
  EXPECT_TRUE(Press(ftxui::Event::ArrowRight));
  EXPECT_TRUE(Press(ftxui::Event::ArrowLeft));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_TRUE(Press(ftxui::Event::Escape));
  EXPECT_FALSE(state_.flame_view.selected.has_value());
  EXPECT_FALSE(state_.flame_view.zoom_path.has_value());
  // 'd' with fewer than two pure windows refuses with a note, consumed.
  EXPECT_TRUE(Press(ftxui::Event::Character('d')));
  EXPECT_NE(controller_->flameNote().find("pure windows"), std::string::npos)
      << controller_->flameNote();
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
