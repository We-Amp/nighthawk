// FORK-ONLY (We-Amp): flamegraph renderer + interaction state machine
// contract tests (M1 plan task 5). Plain cc_test with headless ftxui::Screen
// renders, same quarantine reasoning as the other TUI tests: the TUI keeps
// off Envoy's C++ sources and test helpers.
//
// Exact-render assertions pin the cell grid (screen.at / CellAt), not just
// the ANSI-stripped text, so row-for-row geometry is load-bearing. The diff
// overlay's two hues are pinned BOTH as glyphs (character density survives a
// monochrome paste) and as screen cell colors.

#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"
#include "source/client/tui/flamegraph.h"
#include "source/client/tui/pprof_fold.h"
#include "source/client/tui/profile_windows.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// --- fixture helpers ------------------------------------------------------

// Resolves a runfiles-relative fixture path (same seam as tui_pprof_fold_test).
std::string FixturePath(const std::string& runfiles_rel) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (srcdir != nullptr && workspace != nullptr) {
    return std::string(srcdir) + "/" + workspace + "/" + runfiles_rel;
  }
  return runfiles_rel;
}

// Builds a frame with subtree totals folded up from the children.
FoldedFrame MakeFrame(std::string name, uint64_t self_value,
                      std::vector<FoldedFrame> children = {}) {
  FoldedFrame frame;
  frame.function = std::move(name);
  frame.symbolized = true;
  frame.self_value = self_value;
  frame.children = std::move(children);
  frame.total_value = frame.self_value;
  for (const FoldedFrame& child : frame.children) {
    frame.total_value += child.total_value;
  }
  return frame;
}

FoldedTree MakeTree(std::vector<FoldedFrame> roots) {
  FoldedTree tree;
  tree.root = std::move(roots);
  tree.value_unit = "cpu-nanoseconds";
  for (const FoldedFrame& frame : tree.root) {
    tree.total_value += frame.total_value;
  }
  tree.symbolized = true;
  return tree;
}

// A pure, aligned-looking window for the differential tests (same seam as
// tui_profile_windows_test).
ProfileWindow WindowWith(FoldedTree tree, uint64_t work_units, uint64_t worker_count = 4) {
  ProfileWindow window;
  window.tree = std::move(tree);
  window.start_unix_ns = 1000;
  window.end_unix_ns = 2000;
  window.purity_fraction = 1.0;
  window.phase_id = "main";
  window.worker_count = worker_count;
  window.work_units = work_units;
  window.profile_type = "cpu";
  window.symbolized = true;
  window.interval_source = WindowIntervalSource::kFetchOrchestration;
  return window;
}

bool FrameEqual(const FoldedFrame& a, const FoldedFrame& b) {
  if (a.function != b.function || a.self_value != b.self_value ||
      a.total_value != b.total_value || a.children.size() != b.children.size()) {
    return false;
  }
  for (size_t i = 0; i < a.children.size(); ++i) {
    if (!FrameEqual(a.children[i], b.children[i])) {
      return false;
    }
  }
  return true;
}

bool TreeEqual(const FoldedTree& a, const FoldedTree& b) {
  if (a.total_value != b.total_value || a.root.size() != b.root.size()) {
    return false;
  }
  for (size_t i = 0; i < a.root.size(); ++i) {
    if (!FrameEqual(a.root[i], b.root[i])) {
      return false;
    }
  }
  return true;
}

// --- rendered-frame helper ------------------------------------------------

struct Frame {
  std::unique_ptr<ftxui::Screen> screen;
  std::string text;

  char at(int x, int y) const { return screen->CellAt(x, y).character[0]; }
  ftxui::Color fg(int x, int y) const { return screen->CellAt(x, y).foreground_color; }
  bool inverted(int x, int y) const { return screen->CellAt(x, y).inverted; }
  // The rightmost non-blank column of a row, or -1 when the row is blank.
  // Blank = space OR an empty cell (ftxui leaves empty cells in the region a
  // vbox stretched to fill).
  int extent(int y) const {
    for (int x = screen->dimx() - 1; x >= 0; --x) {
      const std::string& c = screen->CellAt(x, y).character;
      if (!c.empty() && c != " ") {
        return x;
      }
    }
    return -1;
  }
  bool contains(const std::string& needle) const { return text.find(needle) != std::string::npos; }
  bool lacks(const std::string& needle) const { return text.find(needle) == std::string::npos; }
};

// ANSI-stripped text of the screen (colors produce SGR sequences in
// ToString(); the glyph assertions below go through cells directly).
std::string StripAnsi(const std::string& raw) {
  std::string out;
  out.reserve(raw.size());
  for (size_t i = 0; i < raw.size();) {
    if (raw[i] == '\x1b' && i + 1 < raw.size() && raw[i + 1] == '[') {
      size_t j = i + 2;
      while (j < raw.size() && raw[j] != 'm' && raw[j] != '\n') {
        ++j;
      }
      if (j < raw.size() && raw[j] == 'm') {
        i = j + 1;
        continue;
      }
    }
    out.push_back(raw[i]);
    ++i;
  }
  return out;
}

Frame RenderFrame(const FoldedTree& tree, const FlamegraphViewState& state,
                  const DifferentialResult* diff, int width, int height) {
  Frame frame;
  frame.screen = std::make_unique<ftxui::Screen>(width, height);
  ftxui::Render(*frame.screen, RenderFlamegraph(tree, state, diff, width, height));
  frame.text = StripAnsi(frame.screen->ToString());
  return frame;
}

// The tree under test in FlamegraphRendersRowPerDepth:
//   a(total 100: self 40 -> b(total 60: self 20 -> c(total 40)))
FoldedTree ThreeDeepTree() {
  return MakeTree({MakeFrame("a", 40, {MakeFrame("b", 20, {MakeFrame("c", 40)})})});
}

// --- rendering ------------------------------------------------------------

// Render contract: row-per-depth, rectangle width proportional to the
// total_value share of the displayed root; the widest frame lands on the
// first frame row. Cell-grid-exact at 100x20.
TEST(FlameRenderTest, FlamegraphRendersRowPerDepth) {
  FoldedTree tree = ThreeDeepTree();
  Frame frame = RenderFrame(tree, FlamegraphViewState{}, nullptr, 100, 20);

  EXPECT_TRUE(frame.contains("flamegraph total 100 cpu-nanoseconds"));

  // Row 1 = depth 0: frame "a" spans the full width; the closing bracket of
  // the rectangle sits at column 99.
  EXPECT_EQ(frame.at(0, 1), '[');
  EXPECT_EQ(frame.at(1, 1), 'a');
  EXPECT_EQ(frame.at(6, 1), '%');  // "a 100%" label head
  EXPECT_EQ(frame.at(99, 1), ']');
  EXPECT_EQ(frame.extent(1), 99);

  // Row 2 = depth 1: frame "b" spans 60% of the root (60 columns).
  EXPECT_EQ(frame.at(0, 2), '[');
  EXPECT_EQ(frame.at(1, 2), 'b');
  EXPECT_EQ(frame.at(59, 2), ']');
  EXPECT_EQ(frame.extent(2), 59);

  // Row 3 = depth 2: frame "c" spans 40 columns.
  EXPECT_EQ(frame.at(0, 3), '[');
  EXPECT_EQ(frame.at(1, 3), 'c');
  EXPECT_EQ(frame.at(39, 3), ']');
  EXPECT_EQ(frame.extent(3), 39);

  // Widths ordered, widest first; exactly three distinct frame rows.
  EXPECT_GT(frame.extent(1), frame.extent(2));
  EXPECT_GT(frame.extent(2), frame.extent(3));
  EXPECT_EQ(frame.extent(4), -1);
}

// The selected frame is marked (inverted + ">" prefix) and stays inside the
// viewport: scroll_row derives from the selection (the vertical-scroll
// interaction). No selection -> no scroll.
TEST(FlameRenderTest, ScrollKeepsSelectionVisible) {
  // A 30-deep chain: one frame row per stack level.
  FoldedFrame chain = MakeFrame("f29", 1);
  for (int i = 28; i >= 0; --i) {
    chain = MakeFrame("f" + std::to_string(i), 0, {std::move(chain)});
  }
  FoldedTree tree = MakeTree({std::move(chain)});
  ASSERT_EQ(tree.total_value, 1u);

  // No selection: no scroll.
  FlamegraphViewState plain;
  EXPECT_EQ(DeriveScrollRow(tree, plain, /*viewport_rows=*/9), 0);

  // Select the leaf (row 29) in a 10-row viewport (9 frame rows): the scroll
  // must move down so the selected row is the last visible one.
  FlamegraphViewState state;
  FramePath leaf(30, 0);
  state.selected = leaf;
  const int scroll = DeriveScrollRow(tree, state, /*viewport_rows=*/9);
  EXPECT_GT(scroll, 0);
  EXPECT_EQ(scroll, 21);

  Frame frame = RenderFrame(tree, state, nullptr, 60, 10);
  // The selected leaf renders on screen row 9 = 1 (header) + 29 - 21, marked.
  EXPECT_EQ(frame.at(1, 9), '>');  // the selection marker
  EXPECT_EQ(frame.at(2, 9), 'f');
  // The visible window holds levels 21..29; the top of the chain scrolled out.
  EXPECT_NE(frame.text.find("f21"), std::string::npos);
  EXPECT_TRUE(frame.lacks("f0"));
}

// --- zoom / navigation ----------------------------------------------------

// Zoom re-roots (Review Focus 5): the returned tree's root children are the
// zoomed frame's children and the displayed total equals the frame's total.
// Zoom to the root path is the identity; an invalid path is a no-op.
TEST(FlameNavTest, ZoomToFrameRerootsTree) {
  FoldedTree tree = ThreeDeepTree();

  FoldedTree at_b = ZoomToFrame(tree, FramePath{0, 0});
  ASSERT_EQ(at_b.root.size(), 1u);
  EXPECT_EQ(at_b.root[0].function, "c");
  EXPECT_EQ(at_b.total_value, 60u);
  EXPECT_EQ(at_b.value_unit, "cpu-nanoseconds");

  // Root path: identity.
  FoldedTree identity = ZoomToFrame(tree, FramePath{});
  EXPECT_TRUE(TreeEqual(identity, tree));

  // Invalid path: no-op, never a crash.
  FoldedTree invalid = ZoomToFrame(tree, FramePath{0, 5});
  EXPECT_TRUE(TreeEqual(invalid, tree));

  // Selection re-resolves against the re-rooted tree.
  EXPECT_TRUE(ResolveSelection(at_b, FramePath{0}).has_value());
  EXPECT_FALSE(ResolveSelection(at_b, FramePath{1}).has_value());
}

// Navigation is heaviest-first because Task 3's children are total-sorted;
// edges and floors clamp.
TEST(FlameNavTest, NavMovesWithinTreeHeaviestFirst) {
  FoldedTree tree = MakeTree({MakeFrame("big", 20, {MakeFrame("b1", 40), MakeFrame("b2", 20)}),
                              MakeFrame("small", 40)});
  // Root children sorted desc: big(60), small(40); big's children: b1(40), b2(20).

  EXPECT_EQ(NavDown(tree, FramePath{}), FramePath({0}));         // heaviest child
  EXPECT_EQ(NavDown(tree, FramePath({0})), FramePath({0, 0}));   // then its heaviest
  EXPECT_EQ(NavDown(tree, FramePath({0, 0})), FramePath({0, 0}));  // leaf: stays

  EXPECT_EQ(NavSibling(tree, FramePath({0, 0}), 1), FramePath({0, 1}));  // next sibling
  EXPECT_EQ(NavSibling(tree, FramePath({0, 1}), 1), FramePath({0, 1}));  // clamped
  EXPECT_EQ(NavSibling(tree, FramePath({0, 1}), -1), FramePath({0, 0}));
  EXPECT_EQ(NavSibling(tree, FramePath({0}), -1), FramePath({0}));  // clamped at 0
  EXPECT_EQ(NavSibling(tree, FramePath({}), 1), FramePath({}));     // root: no siblings

  EXPECT_EQ(NavUp(tree, FramePath({0, 0})), FramePath({0}));
  EXPECT_EQ(NavUp(tree, FramePath({0})), FramePath({}));
  EXPECT_EQ(NavUp(tree, FramePath({})), FramePath({}));  // root path is the floor

  EXPECT_TRUE(ResolveSelection(tree, FramePath({0, 1})).has_value());
  EXPECT_FALSE(ResolveSelection(tree, FramePath({0, 2})).has_value());
  EXPECT_FALSE(ResolveSelection(tree, FramePath({2})).has_value());
  EXPECT_TRUE(ResolveSelection(tree, FramePath({})).has_value());  // the root itself
}

// --- collapse -------------------------------------------------------------

TEST(FlameCollapseTest, CollapsePrunesBelowThreshold) {
  FoldedTree tree = MakeTree({MakeFrame("big", 99), MakeFrame("tiny", 1)});
  // "tiny" holds 1% of the displayed total; the default threshold is 1% (kept,
  // strictly-below pruned), 5% prunes it.
  FoldedTree collapsed = CollapseBelowThreshold(tree, 0.05);
  ASSERT_EQ(collapsed.root.size(), 1u);
  EXPECT_EQ(collapsed.root[0].function, "big");
  // Total conserved: pruning hides a subtree, it never re-weights the rest.
  EXPECT_EQ(collapsed.total_value, 100u);

  // Nested pruning: a below-threshold child vanishes from the render too.
  FoldedTree nested = MakeTree({MakeFrame("a", 0, {MakeFrame("keep", 95), MakeFrame("drop", 5)})});
  FoldedTree pruned = CollapseBelowThreshold(nested, 0.10);
  ASSERT_EQ(pruned.root.size(), 1u);
  ASSERT_EQ(pruned.root[0].children.size(), 1u);
  EXPECT_EQ(pruned.root[0].children[0].function, "keep");
  EXPECT_EQ(pruned.total_value, 100u);

  // And the pruned frame is gone from the pixels, not just the tree.
  Frame frame = RenderFrame(collapsed, FlamegraphViewState{}, nullptr, 100, 20);
  EXPECT_TRUE(frame.contains("big"));
  EXPECT_TRUE(frame.lacks("tiny"));
}

// --- search ---------------------------------------------------------------

TEST(FlameSearchTest, SearchHighlightsWithoutMutatingTree) {
  FoldedTree tree =
      MakeTree({MakeFrame("a", 20, {MakeFrame("alpha", 40)}), MakeFrame("b", 20, {MakeFrame("beta", 20)})});
  const FoldedTree copy = tree;  // deep copy for the mutation check

  // Case-insensitive substring matches, DFS root-first order.
  EXPECT_EQ(SearchMatches(tree, "alpha"), std::vector<FramePath>({FramePath({0, 0})}));
  EXPECT_EQ(SearchMatches(tree, "LPH"), std::vector<FramePath>({FramePath({0, 0})}));
  EXPECT_EQ(SearchMatches(tree, "beta"), std::vector<FramePath>({FramePath({1, 0})}));
  EXPECT_TRUE(SearchMatches(tree, "zzz").empty());
  EXPECT_TRUE(SearchMatches(tree, "").empty());  // empty query: no hits

  // Pure: search and zoom never mutate the tree.
  EXPECT_TRUE(TreeEqual(tree, copy));
  auto hits = SearchMatches(tree, "alpha");
  ZoomToFrame(tree, FramePath({0}));
  EXPECT_TRUE(TreeEqual(tree, copy));
  EXPECT_EQ(hits.size(), 1u);

  // Render: matching cells invert; nothing else does.
  FlamegraphViewState state;
  state.search = "alpha";
  Frame frame = RenderFrame(tree, state, nullptr, 100, 20);
  // Layout: depth 0 on row 1 (a spans cols 0-59, b 60-99), depth 1 on row 2
  // (alpha spans 0-39, beta 60-79).
  EXPECT_TRUE(frame.inverted(5, 2));   // inside alpha
  EXPECT_TRUE(frame.inverted(30, 2));  // inside alpha
  EXPECT_FALSE(frame.inverted(5, 1));  // frame "a": no match
  EXPECT_FALSE(frame.inverted(70, 1)); // frame "b": no match
  EXPECT_FALSE(frame.inverted(65, 2)); // beta: no match
  EXPECT_NE(frame.text.find("search:alpha"), std::string::npos);
}

// --- differential overlay -------------------------------------------------

// Two flagged frames carry distinct hue cells (screen cell colors), the
// legend names both directions, density encodes the band in glyphs, neutral
// frames keep the base color, and a node new vs base renders "+new" (never
// the raw ~1/eps ratio).
TEST(FlameDiffTest, DiffOverlayRendersTwoHueDelta) {
  // Children in total-sorted order (the FoldedTree invariant), both sides.
  FoldedTree base_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 60),   //
                                                         MakeFrame("cold", 40),  //
                                                         MakeFrame("still", 5)})});
  FoldedTree cur_tree = MakeTree({MakeFrame("main", 0, {MakeFrame("hot", 120),   //
                                                        MakeFrame("fresh", 25),  //
                                                        MakeFrame("cold", 10),   //
                                                        MakeFrame("still", 5)})});
  ProfileWindow base = WindowWith(base_tree, 100);
  ProfileWindow cur = WindowWith(cur_tree, 100);
  auto diff = ComputeDifferential(base, cur);
  ASSERT_TRUE(diff.ok()) << diff.status();
  ASSERT_TRUE(diff->comparable);

  FlamegraphViewState state;
  state.diff_mode = true;
  Frame frame = RenderFrame(cur_tree, state, &*diff, 100, 24);

  // Two-row legend, one per hue.
  EXPECT_NE(frame.text.find("[###] hotter"), std::string::npos);
  EXPECT_NE(frame.text.find("[~~~] cooler"), std::string::npos);

  // Rows: header 0, legend 1-2, depth 0 on row 3, depth 1 on row 4.
  // Depth 1 widths (share of 160, 100 cols): hot 75, fresh 16, cold 6,
  // still 3 (children total-sorted: hot 120, fresh 25, cold 10, still 5).
  //   hot:  +1.0  -> strong hot  (RedLight, full '#')
  //   fresh: new  -> strong hot + "+new" label marker
  //   cold: -0.75 -> medium cool (Green, '~' every other cell)
  //   still: 0.0  -> neutral    (base color)
  EXPECT_EQ(frame.at(0, 4), '[');
  EXPECT_EQ(frame.at(1, 4), 'h');

  EXPECT_EQ(frame.fg(10, 4), ftxui::Color::RedLight);  // strong hot band
  EXPECT_EQ(frame.at(10, 4), '#');                     // full density

  EXPECT_EQ(frame.at(75, 4), '[');  // fresh frame starts at 75
  EXPECT_EQ(frame.at(76, 4), 'f');
  EXPECT_NE(frame.text.find("+new"), std::string::npos);
  EXPECT_EQ(frame.fg(85, 4), ftxui::Color::RedLight);  // new nodes: strong hot

  EXPECT_EQ(frame.at(91, 4), '[');  // cold frame starts at 91
  EXPECT_EQ(frame.at(92, 4), '~');
  EXPECT_EQ(frame.at(93, 4), '_');                  // medium density: off cell
  EXPECT_EQ(frame.fg(92, 4), ftxui::Color::Green);  // medium cool band

  EXPECT_EQ(frame.at(97, 4), '[');  // still frame starts at 97
  EXPECT_EQ(frame.at(98, 4), '_');
  EXPECT_EQ(frame.at(99, 4), ']');
  EXPECT_EQ(frame.fg(98, 4), ftxui::Color::Default);  // neutral: base color

  // Depth 0: main carries +0.6 -> medium hot (Red), not the strong tint.
  EXPECT_EQ(frame.fg(0, 3), ftxui::Color::Red);

  // The hot frame's own delta never prints a raw ratio; percents only.
  EXPECT_TRUE(frame.lacks("1e+12"));
}

// The hard lifetime rule (Task 4 review) is enforced in the pane's data
// model: a DifferentialResult carrying diffs cannot be stored without the
// ProfileWindowStore snapshot that owns its frame pointers.
TEST(FlameDiffTest, DiffOverlayRequiresOwningSnapshot) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100);
  ProfileWindow cur = WindowWith(tree, 100);
  auto diff = ComputeDifferential(base, cur);
  ASSERT_TRUE(diff.ok());

  // Null snapshot: rejected outright — the frames would dangle.
  auto unowned = PairDiffOverlay(/*owning_snapshot=*/nullptr, *diff);
  EXPECT_EQ(unowned.status().code(), absl::StatusCode::kInvalidArgument);

  ProfileWindowStore store;
  std::vector<ProfileWindow> windows;
  windows.push_back(cur);
  store.store(windows);
  auto owned = PairDiffOverlay(store.load(), *diff);
  ASSERT_TRUE(owned.ok()) << owned.status();
  EXPECT_EQ(owned->owning_snapshot, store.load());
  EXPECT_EQ(owned->result.diffs.size(), diff->diffs.size());
}

// 8.4's marked-non-comparable path renders as a mark, never as a delta.
TEST(FlameDiffTest, DiffIncomparableRendersMark) {
  FoldedTree tree = MakeTree({MakeFrame("main", 100)});
  ProfileWindow base = WindowWith(tree, 100, /*worker_count=*/4);
  ProfileWindow cur = WindowWith(tree, 100, /*worker_count=*/8);
  auto diff = ComputeDifferential(base, cur);
  ASSERT_TRUE(diff.ok());
  ASSERT_FALSE(diff->comparable);

  FlamegraphViewState state;
  state.diff_mode = true;
  Frame frame = RenderFrame(tree, state, &*diff, 100, 24);
  EXPECT_NE(frame.text.find("non-comparable:"), std::string::npos);
  EXPECT_TRUE(frame.lacks("[###] hotter"));
}

// --- banner ---------------------------------------------------------------

TEST(FlameBannerTest, UnsymbolizedTreeRendersBanner) {
  FoldedTree tree = MakeTree({MakeFrame("0xdeadbeef", 100)});
  tree.symbolized = false;
  Frame frame = RenderFrame(tree, FlamegraphViewState{}, nullptr, 100, 20);
  EXPECT_NE(frame.text.find("unsymbolized"), std::string::npos);
  EXPECT_NE(frame.text.find("0xdeadbeef"), std::string::npos);  // rows still render
}

// --- the real fixture -----------------------------------------------------

// The committed Task 1 fixture folded by Task 3, rendered row-per-depth at
// the 100x30 minimum: totals match the independent go-tool ground truth,
// the frame rows stack, search highlights real symbols, and a doubled-work
// differential paints the union tree cool. Ground truth: 630ms across 63 x
// 10ms samples (see tui_pprof_fold_test).
TEST(FlameFixtureTest, FixtureTreeRendersAtMinimumSize) {
  auto profile = LoadProfile(FixturePath("test/fixtures/nh_sut_pprof.pb.gz"));
  ASSERT_TRUE(profile.ok()) << profile.status();
  auto folded = FoldProfile(*profile);
  ASSERT_TRUE(folded.ok()) << folded.status();
  FoldedTree tree = *folded;
  ASSERT_EQ(tree.total_samples, 63u);
  ASSERT_EQ(tree.total_value, 630000000u);

  // Solo view. The fixture carries unsymbolized remainder addresses, so the
  // banner renders first and the header follows it.
  FlamegraphViewState solo;
  Frame frame = RenderFrame(tree, solo, nullptr, 100, 30);
  EXPECT_NE(frame.text.find("flamegraph total 630000000 cpu-nanoseconds"), std::string::npos);
  const bool unsymbolized = !tree.symbolized;
  EXPECT_EQ(unsymbolized, frame.text.find("unsymbolized") != std::string::npos);
  // First frame row = first row starting with a bracket; the root row spans
  // (nearly) the full width, and at least three depth rows stack below it.
  int first_frame_row = -1;
  for (int y = 0; y < 30; ++y) {
    if (frame.extent(y) >= 0 && frame.at(0, y) == '[') {
      first_frame_row = y;
      break;
    }
  }
  ASSERT_GE(first_frame_row, 0);
  EXPECT_GE(frame.extent(first_frame_row), 95);
  EXPECT_GT(frame.extent(first_frame_row + 1), 0);
  EXPECT_GT(frame.extent(first_frame_row + 2), 0);

  // Search active: a real symbol from the go-tool ground truth. The key-nav
  // cursor follows the first hit (task 6's wiring), and DeriveScrollRow
  // scrolls the (deep) tree so that hit's row is on screen — the highlight
  // must actually be visible, not scrolled off.
  FlamegraphViewState searching;
  searching.search = "operator";
  const std::vector<FramePath> hits = SearchMatches(tree, "operator");
  ASSERT_FALSE(hits.empty());
  searching.selected = hits.front();
  Frame searched = RenderFrame(tree, searching, nullptr, 100, 30);
  EXPECT_NE(searched.text.find("search:operator"), std::string::npos);
  bool any_inverted = false;
  for (int y = 1; y < 30; ++y) {
    for (int x = 0; x < 100; ++x) {
      if (searched.inverted(x, y)) {
        any_inverted = true;
      }
    }
  }
  EXPECT_TRUE(any_inverted) << "search must highlight at least one visible frame";

  // Differential view: identical tree, work doubled -> uniform -0.5
  // (per-unit-of-work halving, the pinned Task 4 math): every frame paints
  // the medium cool band, and the legend names the cool hue.
  ProfileWindow base = WindowWith(tree, 100);
  ProfileWindow cur = WindowWith(tree, 200);
  auto diff = ComputeDifferential(base, cur);
  ASSERT_TRUE(diff.ok()) << diff.status();
  ASSERT_TRUE(diff->comparable);
  FlamegraphViewState diffing;
  diffing.diff_mode = true;
  Frame diffed = RenderFrame(tree, diffing, &*diff, 100, 30);
  EXPECT_NE(diffed.text.find("[~~~] cooler"), std::string::npos);
  // Every flagged frame paints the medium cool hue: the first FRAME row's
  // leading bracket is green. (The legend rows start with '[' too — skip
  // them by their glyph fill.)
  int diff_first_frame_row = -1;
  for (int y = 0; y < 30; ++y) {
    if (diffed.extent(y) >= 0 && diffed.at(0, y) == '[' && diffed.at(1, y) != '#' &&
        diffed.at(1, y) != '~') {
      diff_first_frame_row = y;
      break;
    }
  }
  ASSERT_GE(diff_first_frame_row, 0);
  EXPECT_EQ(diffed.fg(0, diff_first_frame_row), ftxui::Color::Green);
}

}  // namespace
}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
