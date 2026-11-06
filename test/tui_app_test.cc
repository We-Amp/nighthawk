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
      {"e", "stage an edit for the next phase (knobs tab; esc discards)"},
      {"u", "turn the rps knob (knobs tab, live run; the total across workers)"},
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

// The staged next-phase state machine (M2): edits stage into the preview,
// the staged state is inspectable, and esc discards EVERYTHING staged. The
// values are preview-only — nothing here reaches the running engine.
TEST_F(KeyMapDispatchTest, StagedPhaseEditingStagesAndDiscards) {
  // Refusal without a live run: an honest note, never a silent no-op, and
  // the mode stays navigate.
  EXPECT_TRUE(Press(ftxui::Event::Character('k')));
  EXPECT_TRUE(Press(ftxui::Event::Character('e')));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_NE(controller_->flameNote().find("no run to stage for"), std::string::npos)
      << controller_->flameNote();

  // Enter the editing mode directly (the live-run gate is the controller's;
  // the state machine below is the model under test).
  state_.mode = InputMode::kEditingStagedPhase;
  state_.staged_field = 0;
  // Field 0 (concurrency): "4" + Enter stages it.
  EXPECT_TRUE(Press(ftxui::Event::Character('4')));
  EXPECT_EQ(state_.staged_draft, "4");
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_EQ(state_.staged_next_phase.name(), "next");
  EXPECT_EQ(state_.staged_next_phase.concurrency().value(), "4");
  EXPECT_TRUE(state_.staged_draft.empty());
  // Tab cycles to field 1 (connections): "6" + Enter stages it too.
  EXPECT_TRUE(Press(ftxui::Event::Tab));
  EXPECT_EQ(state_.staged_field, 1);
  EXPECT_TRUE(Press(ftxui::Event::Character('6')));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_EQ(state_.staged_next_phase.connections().value(), 6u);
  // Esc discards everything staged: the empty spec name is the discarded
  // state (the preview renders nothing).
  EXPECT_TRUE(Press(ftxui::Event::Escape));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_TRUE(state_.staged_next_phase.name().empty());
  EXPECT_TRUE(state_.staged_draft.empty());
  EXPECT_EQ(state_.staged_field, 0);
}

// A staged edit with a non-numeric draft is refused with a note and stages
// nothing.
TEST_F(KeyMapDispatchTest, StagedEditRefusesNonNumericDraft) {
  state_.mode = InputMode::kEditingStagedPhase;
  EXPECT_TRUE(Press(ftxui::Event::Character('x')));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_TRUE(state_.staged_next_phase.name().empty());
  EXPECT_NE(controller_->flameNote().find("positive integer"), std::string::npos)
      << controller_->flameNote();
}

// The live rps turn (M3): the u key routes through the knobs tab + a live
// run; the editing mode suspends refresh (RefreshSuspended), the draft
// edits like every other field, esc cancels, and a non-numeric draft is
// refused with a note (the value is the PHASE TOTAL). The send itself needs
// a live run — the controller refuses honestly here (no run in flight).
TEST_F(KeyMapDispatchTest, KnobTurnKeyRoutesAndEdits) {
  EXPECT_TRUE(Press(ftxui::Event::Character('k')));
  // No live run: an honest note, never a silent no-op, mode stays navigate.
  EXPECT_TRUE(Press(ftxui::Event::Character('u')));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_NE(controller_->flameNote().find("no run to steer"), std::string::npos)
      << controller_->flameNote();

  // The wrong tab routes with a note too.
  EXPECT_TRUE(Press(ftxui::Event::Character('c')));
  EXPECT_TRUE(Press(ftxui::Event::Character('u')));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_NE(controller_->flameNote().find("knobs tab (k, then u)"), std::string::npos)
      << controller_->flameNote();

  // The editing mode's draft machine (entered directly; the live-run gate
  // is the controller's).
  state_.mode = InputMode::kEditingKnob;
  EXPECT_TRUE(RefreshSuspended(state_.mode));
  EXPECT_TRUE(Press(ftxui::Event::Character('3')));
  EXPECT_TRUE(Press(ftxui::Event::Character('0')));
  EXPECT_TRUE(Press(ftxui::Event::Backspace));
  EXPECT_EQ(state_.knob_draft, "3");
  // A non-numeric draft is refused; the mode stays until esc/enter resolves.
  EXPECT_TRUE(Press(ftxui::Event::Character('x')));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_NE(controller_->flameNote().find("positive integer"), std::string::npos)
      << controller_->flameNote();
  EXPECT_EQ(state_.mode, InputMode::kEditingKnob);
  // Esc cancels: one forced redraw, back to navigate, draft discarded.
  EXPECT_TRUE(Press(ftxui::Event::Escape));
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_TRUE(state_.knob_draft.empty());
  // A VALID draft sends the update; with no live run the controller refuses
  // honestly (rps turn refused), the draft clears, and the mode exits.
  state_.mode = InputMode::kEditingKnob;
  EXPECT_TRUE(Press(ftxui::Event::Character('4')));
  EXPECT_TRUE(Press(ftxui::Event::Return));
  EXPECT_NE(controller_->flameNote().find("rps turn refused"), std::string::npos)
      << controller_->flameNote();
  EXPECT_EQ(state_.mode, InputMode::kNavigate);
  EXPECT_TRUE(state_.knob_draft.empty());
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
