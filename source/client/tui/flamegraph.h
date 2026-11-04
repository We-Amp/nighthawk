// FORK-ONLY (We-Amp): the TUI flamegraph pane (M1 plan task 5): row-per-depth
// rendering over the FoldedTree (task 3), heaviest-first navigation, zoom to
// frame (re-rooting), collapse below a share threshold, frame search, and the
// differential color-delta overlay on the union tree (task 4).
//
// Everything here is pure; task 6 owns input wiring and lighting up the tab
// strip. The renderer is ASCII-first and color-additive: the diff overlay's
// two hues are encoded as glyph identity ('#' hot / '~' cool) AND as
// character-density steps, so hue and magnitude survive a monochrome paste.
// Brightness bands are quantized |delta_ratio| steps, documented at
// RenderFlamegraph. FramePath (child indices from the tree root) comes from
// pprof_fold.h and is the single addressing key: the navigation cursor, zoom
// target, search hits, and the overlay all speak it.
//
// Quarantine note: depends only on pprof_fold.h, profile_windows.h, absl, and
// ftxui — no Envoy C++ sources.
#ifndef NIGHTHAWK_TUI_FLAMEGRAPH_H_
#define NIGHTHAWK_TUI_FLAMEGRAPH_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"

#include "source/client/tui/pprof_fold.h"
#include "source/client/tui/profile_windows.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// The flamegraph pane's interaction state. Paths address the tree AFTER zoom
// re-rooting but BEFORE collapse pruning (the uncollapsed displayed tree);
// the renderer translates the selection through the collapse (see
// CollapseTranslatePath), so a cursor whose frame survives collapse keeps
// addressing the same frame even when pruned siblings shift indices.
// ResolveSelection validates a path against the tree it is about to be
// applied to; a pruned-away frame drops the selection.
struct FlamegraphViewState {
  // nullopt = the whole tree; a set path zooms by RE-ROOTING the tree at the
  // addressed frame (the addressed frame's subtree becomes the display).
  std::optional<FramePath> zoom_path;
  // The key-navigation cursor (NavDown/NavUp/NavSibling move it).
  std::optional<FramePath> selected;
  // Collapse frames below this share of the displayed total (default 1%).
  double collapse_threshold_ratio = 0.01;
  // Frame search: highlights matches; NEVER mutates the tree.
  std::string search;
  // Renders the differential overlay; requires a DifferentialResult.
  bool diff_mode = false;
  // Base vertical scroll (frame rows); DeriveScrollRow adjusts it so the
  // selected row stays inside the viewport.
  int scroll_row = 0;
};

// --- navigation (pure; heaviest-first because task 3's children are
// total-sorted by total_value descending) ---

// Into the first (heaviest) child; a childless frame (or an empty tree)
// returns the path unchanged.
FramePath NavDown(const FoldedTree& tree, FramePath path);

// To the parent; the root path (empty) is the floor.
FramePath NavUp(const FoldedTree& tree, FramePath path);

// +/- delta among siblings, clamped to [0, sibling count); the root path has
// no siblings and is returned unchanged.
FramePath NavSibling(const FoldedTree& tree, FramePath path, int delta);

// The path's validity AFTER a re-root or collapse: nullopt when any index
// walks off the tree, the path otherwise. The empty path (the root itself)
// is always valid.
std::optional<FramePath> ResolveSelection(const FoldedTree& tree, FramePath path);

// --- tree transforms ---

// Zoom to frame (Review Focus 5): re-roots the tree so the addressed frame's
// children become the root row and the frame's total_value becomes the
// displayed total. The root path is the identity; an invalid path is a no-op
// (never a crash). Tree metadata (unit, clock stamps, symbolization) carries
// over unchanged.
FoldedTree ZoomToFrame(const FoldedTree& tree, const FramePath& path);

// Prunes every frame whose total_value share of the tree's total is strictly
// below `threshold` (root row included), recursively. Conservation: pruning
// HIDES a subtree — the value stays counted at the surviving ancestors and
// tree.total_value is unchanged; the render shows it as blank gap.
//
// ADDRESSING: pruning shifts sibling indices. For a well-formed (total-sorted)
// tree pruning only removes a suffix, so indices are stable — but nothing in
// this pane may DEPEND on that invariant. Any path expressed against the
// uncollapsed tree (search hits, diff child_paths, the view-state selection)
// must be carried through CollapseTranslatePath before it addresses the
// collapsed tree.
FoldedTree CollapseBelowThreshold(const FoldedTree& tree, double threshold);

// Translates a path expressed against `uncollapsed` into `collapsed`
// coordinates: the collapsed tree's children are a subsequence of the
// uncollapsed children (order preserved; a folded tree has no same-name
// siblings), so each surviving index re-ranks to its post-collapse position.
// nullopt when any level of the path was pruned away (or the path is invalid
// against the uncollapsed tree). The empty path (the root) translates to
// itself. Idempotence is NOT guaranteed in general — always translate from
// the uncollapsed coordinates, never re-translate a translated path.
std::optional<FramePath> CollapseTranslatePath(const FoldedTree& uncollapsed,
                                               const FoldedTree& collapsed,
                                               const FramePath& path);

// All paths whose frame function contains `query` (case-insensitive
// substring), in DFS root-first order. Empty query: no hits.
std::vector<FramePath> SearchMatches(const FoldedTree& tree, absl::string_view query);

// The scroll row that keeps the selected row inside a `viewport_rows`-tall
// viewport: starts from state.scroll_row (clamped to the tree's depth), then
// moves just enough to make the selected row visible. No selection (or the
// root path): the clamped base scroll. `state.selected` is read as a path in
// displayed_tree coordinates (the renderer passes a collapse-translated
// copy; direct callers must do the same). Pass the DISPLAYED tree (zoomed +
// collapsed), the same tree the renderer will draw.
int DeriveScrollRow(const FoldedTree& displayed_tree, const FlamegraphViewState& state,
                    int viewport_rows);

// --- rendering ---

// The DIFF OVERLAY PALETTE (two-hue diverging, brightness = character
// density). Bands are quantized |delta_ratio| steps, shared by both hues:
//
//   strong: |r| >= 1.0        -> fill '#'/'~' on EVERY padding cell
//                                color: RedLight / GreenLight
//   medium: 0.25 <= |r| < 1.0 -> every OTHER padding cell
//                                color: Red / Green
//   weak:   0.05 <= |r| < 0.25 -> every FOURTH padding cell
//                                color: dim Red / dim Green
//   none:   |r| < 0.05        -> no glyph, base color (Color::Default)
//
// '#' marks hotter per-request, '~' cooler. Hue lives in the glyph and the
// color; magnitude lives in the density — a monochrome paste keeps both, so
// nothing in the pane depends on color to be read. Nodes absent from the
// base side (base_per_work == 0) render the strong hot band plus a "+new"
// label marker — their raw delta_ratio (~1/epsilon) is scale-meaningless and
// is never displayed.
//
// Render contract: one row per stack depth, rectangle width proportional to
// total_value's share of the displayed root; frames under three columns (or
// whose label cannot fit) render as '_' blocks with the band's density;
// labels truncate with "..." and RESERVE ONE TRAILING CELL for the band's
// density glyph, so even a fully truncated diff cell keeps its non-color
// hue/band signal (the monochrome-paste promise); the selected frame is
// inverted + bold with a '>' label prefix; search matches invert; diff_mode
// colors the tree per the palette above (mark keys are translated through
// zoom and collapse) and adds the two-row legend; unsymbolized trees render
// the explicit banner first. The header carries overflow honesty cues: rows
// scrolled out of the viewport ("rows N-M of D") and root rectangles wider
// than the terminal ("+K roots hidden"). Headers/legend live on the top
// rows; frame rows scroll per DeriveScrollRow. diff only consulted when
// state.diff_mode is set; an incomparable result renders the
// non-comparable mark instead of the legend.
ftxui::Element RenderFlamegraph(const FoldedTree& tree, const FlamegraphViewState& state,
                                const DifferentialResult* diff, int width, int height);

// --- the overlay's data model (hard lifetime rule from task 4's review) ---

// A DifferentialResult's frame pointers point into the window vector of a
// ProfileWindowStore snapshot; a diff set is therefore only ever held
// TOGETHER WITH the snapshot that owns it. The pairing is TYPE-LEVEL: the
// constructor is private and DiffOverlay::Pair is the only way to build one,
// so a bare result cannot masquerade as an owned overlay. The pane (task 6
// wiring) stores a DiffOverlay, never a bare result.
class DiffOverlay {
 public:
  // Pairs a differential with the store snapshot that owns its frame
  // pointers. kInvalidArgument when the snapshot is null — the renderer
  // would otherwise be handed dangling frames.
  static absl::StatusOr<DiffOverlay> Pair(
      std::shared_ptr<const std::vector<ProfileWindow>> owning_snapshot,
      DifferentialResult result);

  // The owning store snapshot — hold it for as long as result() is used.
  const std::shared_ptr<const std::vector<ProfileWindow>>& snapshot() const;
  // The paired differential. Valid while this DiffOverlay (and its snapshot)
  // is alive.
  const DifferentialResult& result() const;

 private:
  DiffOverlay(std::shared_ptr<const std::vector<ProfileWindow>> owning_snapshot,
              DifferentialResult result);

  std::shared_ptr<const std::vector<ProfileWindow>> owning_snapshot_;
  DifferentialResult result_;
};

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk

#endif  // NIGHTHAWK_TUI_FLAMEGRAPH_H_
