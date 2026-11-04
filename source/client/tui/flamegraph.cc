// FORK-ONLY (We-Amp): the TUI flamegraph pane renderer + interaction state
// machine (M1 plan task 5). Pure functions; see the header for the render
// contract, the diff palette, and the overlay's lifetime pairing.
#include "source/client/tui/flamegraph.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/color.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr char kHotGlyph = '#';   // hotter per-request
constexpr char kCoolGlyph = '~';  // cooler per-request
constexpr char kBlockGlyph = '_'; // the neutral block (too-small frames)

// Brightness bands, quantized |delta_ratio| steps (header contract).
constexpr double kBandStrongRatio = 1.0;
constexpr double kBandMediumRatio = 0.25;
constexpr double kBandWeakRatio = 0.05;

enum class Hue { kNeutral, kHot, kCool };
enum class Band { kNone, kWeak, kMedium, kStrong };

struct Mark {
  Hue hue = Hue::kNeutral;
  Band band = Band::kNone;
  bool is_new = false;  // node absent from the base side
};

Band BandFor(double abs_ratio) {
  if (abs_ratio >= kBandStrongRatio) {
    return Band::kStrong;
  }
  if (abs_ratio >= kBandMediumRatio) {
    return Band::kMedium;
  }
  if (abs_ratio >= kBandWeakRatio) {
    return Band::kWeak;
  }
  return Band::kNone;
}

// The band's density pattern — magnitude lives in how many padding cells the
// direction glyph owns: strong = every cell, medium = every other, weak =
// every fourth.
bool DensityAt(Band band, int position) {
  switch (band) {
    case Band::kStrong:
      return true;
    case Band::kMedium:
      return position % 2 == 0;
    case Band::kWeak:
      return position % 4 == 0;
    default:
      return false;
  }
}

ftxui::Color ColorFor(const Mark& mark, bool* dim) {
  *dim = (mark.band == Band::kWeak);
  switch (mark.band) {
    case Band::kStrong:
      return mark.hue == Hue::kCool ? ftxui::Color::GreenLight : ftxui::Color::RedLight;
    case Band::kMedium:
    case Band::kWeak:
      return mark.hue == Hue::kCool ? ftxui::Color::Green : ftxui::Color::Red;
    default:
      return ftxui::Color::Default;
  }
}

// One rendered run of cells within a row (uniform attributes).
struct Seg {
  std::string text;
  bool inverted = false;
  bool bold = false;
  bool dim = false;
  ftxui::Color color = ftxui::Color::Default;
};

// Depth of the deepest frame row (the number of rows the tree renders).
int DepthOf(const FoldedTree& tree) {
  int max_depth = 0;
  std::vector<std::pair<const FoldedFrame*, int>> stack;
  stack.reserve(tree.root.size());
  for (const FoldedFrame& frame : tree.root) {
    stack.push_back({&frame, 1});
  }
  while (!stack.empty()) {
    const FoldedFrame* frame = stack.back().first;
    const int depth = stack.back().second;
    stack.pop_back();
    max_depth = std::max(max_depth, depth);
    for (const FoldedFrame& child : frame->children) {
      stack.push_back({&child, depth + 1});
    }
  }
  return max_depth;
}

bool ContainsCaseInsensitive(absl::string_view haystack, absl::string_view needle) {
  if (needle.empty()) {
    return false;
  }
  return absl::AsciiStrToLower(haystack).find(absl::AsciiStrToLower(needle)) !=
         std::string::npos;
}

// The children vector of the node at `path` (the tree root for the empty
// path); nullptr when the path walks off the tree.
const std::vector<FoldedFrame>* ChildrenAt(const FoldedTree& tree, const FramePath& path) {
  const std::vector<FoldedFrame>* level = &tree.root;
  for (int index : path) {
    if (index < 0 || static_cast<size_t>(index) >= level->size()) {
      return nullptr;
    }
    level = &(*level)[static_cast<size_t>(index)].children;
  }
  return level;
}

const FoldedFrame* FrameAt(const std::vector<FoldedFrame>& frames, const FramePath& path) {
  const std::vector<FoldedFrame>* level = &frames;
  const FoldedFrame* frame = nullptr;
  for (int index : path) {
    if (index < 0 || static_cast<size_t>(index) >= level->size()) {
      return nullptr;
    }
    frame = &(*level)[static_cast<size_t>(index)];
    level = &frame->children;
  }
  return frame;
}

// Re-expresses a diff's child_path in displayed-tree coordinates: the zoomed
// frame's own row disappears (it became the root), paths above or off the
// zoomed subtree are not displayed at all.
std::optional<FramePath> MapDiffPathToDisplayed(const FramePath& path,
                                                const std::optional<FramePath>& zoom_path) {
  if (!zoom_path.has_value()) {
    return path;
  }
  const FramePath& zoom = *zoom_path;
  if (path.size() <= zoom.size()) {
    return std::nullopt;
  }
  if (!std::equal(zoom.begin(), zoom.end(), path.begin())) {
    return std::nullopt;
  }
  return FramePath(path.begin() + static_cast<std::ptrdiff_t>(zoom.size()), path.end());
}

std::string TruncateToWidth(const std::string& line, int width) {
  if (width <= 0) {
    return "";
  }
  return line.size() <= static_cast<size_t>(width)
             ? line
             : line.substr(0, static_cast<size_t>(width));
}

// One frame's rectangle body: bracketed label when the cell is wide enough,
// a bracketed block run for the too-narrow middle sizes, and a bare block run
// below three columns. The band's glyph/density rides on the padding either
// way; the label truncates with "..." at the cell width.
std::string CellBody(int w, const std::string& label, const Mark& mark) {
  const char glyph = (mark.hue == Hue::kCool) ? kCoolGlyph : kHotGlyph;
  if (w <= 0) {
    return "";
  }
  if (w < 3) {
    std::string body(static_cast<size_t>(w), kBlockGlyph);
    for (int p = 0; p < w; ++p) {
      if (DensityAt(mark.band, p)) {
        body[static_cast<size_t>(p)] = glyph;
      }
    }
    return body;
  }
  const int inner = w - 2;
  if (inner < 6) {
    std::string body;
    body.reserve(static_cast<size_t>(w));
    body.push_back('[');
    for (int p = 0; p < inner; ++p) {
      body.push_back(DensityAt(mark.band, p) ? glyph : kBlockGlyph);
    }
    body.push_back(']');
    return body;
  }
  std::string content = label;
  if (content.size() > static_cast<size_t>(inner)) {
    content = content.substr(0, static_cast<size_t>(inner) - 3) + "...";
  }
  const int pad = inner - static_cast<int>(content.size());
  for (int p = 0; p < pad; ++p) {
    content.push_back(DensityAt(mark.band, p) ? glyph : ' ');
  }
  return "[" + content + "]";
}

void PruneChildren(std::vector<FoldedFrame>* frames, uint64_t total, double threshold) {
  const auto below = [total, threshold](const FoldedFrame& frame) {
    return static_cast<double>(frame.total_value) / static_cast<double>(total) < threshold;
  };
  frames->erase(std::remove_if(frames->begin(), frames->end(), below), frames->end());
  for (FoldedFrame& frame : *frames) {
    PruneChildren(&frame.children, total, threshold);
  }
}

ftxui::Element SegElement(const Seg& seg) {
  ftxui::Element element = ftxui::text(seg.text);
  if (seg.inverted) {
    element |= ftxui::inverted;
  }
  if (seg.bold) {
    element |= ftxui::bold;
  }
  if (seg.dim) {
    element |= ftxui::dim;
  }
  if (seg.color != ftxui::Color::Default) {
    element |= ftxui::color(seg.color);
  }
  return element;
}

}  // namespace

FramePath NavDown(const FoldedTree& tree, FramePath path) {
  const std::vector<FoldedFrame>* children = ChildrenAt(tree, path);
  if (children == nullptr || children->empty()) {
    return path;  // invalid path or childless frame: unchanged
  }
  // Children are total-sorted descending (Task 3): index 0 is the heaviest.
  path.push_back(0);
  return path;
}

FramePath NavUp(const FoldedTree& tree, FramePath path) {
  (void)tree;  // the floor needs no tree walk: pop, clamp-free
  if (!path.empty()) {
    path.pop_back();
  }
  return path;  // the root path is the floor
}

FramePath NavSibling(const FoldedTree& tree, FramePath path, int delta) {
  if (path.empty()) {
    return path;  // the root has no siblings
  }
  const std::vector<FoldedFrame>* siblings = ChildrenAt(tree, FramePath(path.begin(), path.end() - 1));
  if (siblings == nullptr || siblings->empty()) {
    return path;
  }
  path.back() = std::clamp(path.back() + delta, 0, static_cast<int>(siblings->size()) - 1);
  return path;
}

std::optional<FramePath> ResolveSelection(const FoldedTree& tree, FramePath path) {
  if (ChildrenAt(tree, path) == nullptr) {
    return std::nullopt;
  }
  return path;
}

FoldedTree ZoomToFrame(const FoldedTree& tree, const FramePath& path) {
  if (path.empty()) {
    return tree;  // zoom to the root: identity
  }
  const FoldedFrame* frame = FrameAt(tree.root, path);
  if (frame == nullptr) {
    return tree;  // invalid path: no-op, never a crash
  }
  FoldedTree out = tree;  // metadata (unit, clock stamps, symbolization) carries over
  out.root = frame->children;
  out.total_samples = frame->total_samples;
  out.total_value = frame->total_value;
  return out;
}

FoldedTree CollapseBelowThreshold(const FoldedTree& tree, double threshold) {
  FoldedTree out = tree;
  if (threshold <= 0.0 || tree.total_value == 0) {
    return out;
  }
  PruneChildren(&out.root, tree.total_value, threshold);
  return out;
}

std::vector<FramePath> SearchMatches(const FoldedTree& tree, absl::string_view query) {
  std::vector<FramePath> hits;
  if (query.empty()) {
    return hits;
  }
  // DFS root-first, iterative (deep stacks from real profiles).
  struct Item {
    const FoldedFrame* frame;
    FramePath path;
  };
  std::vector<Item> stack;
  stack.reserve(tree.root.size());
  for (size_t i = tree.root.size(); i-- > 0;) {
    stack.push_back({&tree.root[i], FramePath{static_cast<int>(i)}});
  }
  while (!stack.empty()) {
    Item item = std::move(stack.back());
    stack.pop_back();
    if (ContainsCaseInsensitive(item.frame->function, query)) {
      hits.push_back(item.path);
    }
    for (size_t i = item.frame->children.size(); i-- > 0;) {
      FramePath child_path = item.path;
      child_path.push_back(static_cast<int>(i));
      stack.push_back({&item.frame->children[i], std::move(child_path)});
    }
  }
  return hits;
}

int DeriveScrollRow(const FoldedTree& displayed_tree, const FlamegraphViewState& state,
                    int viewport_rows) {
  if (viewport_rows <= 0) {
    return 0;
  }
  const int total_rows = DepthOf(displayed_tree);
  const int max_scroll = std::max(0, total_rows - viewport_rows);
  int scroll = std::clamp(state.scroll_row, 0, max_scroll);
  if (state.selected.has_value() && !state.selected->empty()) {
    const int selected_row = static_cast<int>(state.selected->size()) - 1;
    if (selected_row < scroll) {
      scroll = selected_row;
    }
    if (selected_row >= scroll + viewport_rows) {
      scroll = selected_row - viewport_rows + 1;
    }
    scroll = std::clamp(scroll, 0, max_scroll);
  }
  return scroll;
}

ftxui::Element RenderFlamegraph(const FoldedTree& tree, const FlamegraphViewState& state,
                                const DifferentialResult* diff, int width, int height) {
  if (width <= 0 || height <= 0) {
    return ftxui::emptyElement();
  }

  // The displayed tree: zoom re-roots, collapse prunes. Every path below
  // (selection, search hits, overlay marks) addresses THIS tree.
  const FoldedTree displayed = CollapseBelowThreshold(
      ZoomToFrame(tree, state.zoom_path.value_or(FramePath{})), state.collapse_threshold_ratio);

  // Overlay marks, keyed by displayed-tree path (zoom prefix stripped).
  std::map<FramePath, const DifferentialNodeDiff*> marks;
  if (state.diff_mode && diff != nullptr) {
    for (const DifferentialNodeDiff& node : diff->diffs) {
      auto mapped = MapDiffPathToDisplayed(node.child_path, state.zoom_path);
      if (mapped.has_value()) {
        marks[*mapped] = &node;
      }
    }
  }

  // Header rows.
  std::vector<ftxui::Element> lines;
  if (!tree.symbolized) {
    lines.push_back(ftxui::text(TruncateToWidth(
        "unsymbolized profile - addresses only (no symbol names)", width)));
  }
  std::string header =
      absl::StrCat("flamegraph total ", tree.total_value, " ", tree.value_unit);
  if (state.zoom_path.has_value()) {
    const FoldedFrame* zoom_frame = FrameAt(tree.root, *state.zoom_path);
    if (zoom_frame != nullptr) {
      absl::StrAppend(&header, " zoom:", zoom_frame->function);
    }
  }
  if (!state.search.empty()) {
    absl::StrAppend(&header, " search:", state.search,
                    " hits:", SearchMatches(displayed, state.search).size());
  }
  lines.push_back(ftxui::text(TruncateToWidth(header, width)));

  if (state.diff_mode) {
    if (diff == nullptr) {
      lines.push_back(ftxui::text(TruncateToWidth("diff: no differential loaded", width)));
    } else if (!diff->comparable) {
      lines.push_back(ftxui::text(
          TruncateToWidth("non-comparable: " + diff->incomparable_reason, width)));
    } else {
      // The two-row legend, one row per hue (glyphs survive a monochrome
      // paste; the color is additive).
      lines.push_back(ftxui::text(TruncateToWidth(
          "[###] hotter (per-request; +new = absent in base)", width)));
      lines.push_back(ftxui::text(TruncateToWidth("[~~~] cooler (per-request)", width)));
    }
  }

  const int header_rows = static_cast<int>(lines.size());
  const int viewport_rows = height - header_rows;

  // Rectangle layout: iterative DFS (deep stacks from real profiles).
  // Children stack left-to-right inside the parent's rectangle; every width
  // is the frame's total_value share of the displayed total, rounded, and
  // clamped so nothing exceeds its parent or the terminal.
  struct Placed {
    int depth;
    int x;
    int w;
    const FoldedFrame* frame;
    FramePath path;
  };
  struct Item {
    const FoldedFrame* frame;
    FramePath path;
    int depth;
    int x;
    int max_x;
  };
  std::vector<Item> queue;
  std::vector<Placed> placed;
  const uint64_t denominator = displayed.total_value;
  const auto width_for = [&](uint64_t total) -> int {
    if (denominator == 0) {
      return 0;
    }
    const double share =
        static_cast<double>(total) / static_cast<double>(denominator);
    return std::max(1, static_cast<int>(std::lround(
                            share * static_cast<double>(width))));
  };
  {
    int x = 0;
    for (size_t i = 0; i < displayed.root.size(); ++i) {
      const FoldedFrame& frame = displayed.root[i];
      const int w = std::min(width_for(frame.total_value), width - x);
      if (w <= 0) {
        break;  // widths are total-sorted: later roots overflow too
      }
      FramePath path{static_cast<int>(i)};
      queue.push_back(Item{&frame, std::move(path), 0, x, x + w});
      x += w;
    }
  }
  for (size_t head = 0; head < queue.size(); ++head) {
    const Item item = queue[head];
    const int w = std::min(width_for(item.frame->total_value), item.max_x - item.x);
    if (w <= 0) {
      continue;
    }
    placed.push_back(Placed{item.depth, item.x, w, item.frame, item.path});
    int child_x = item.x;
    for (size_t i = 0; i < item.frame->children.size(); ++i) {
      const FoldedFrame& child = item.frame->children[i];
      const int cw = std::min(width_for(child.total_value), item.max_x - child_x);
      if (cw <= 0) {
        break;  // sorted: later children are narrower
      }
      FramePath child_path = item.path;
      child_path.push_back(static_cast<int>(i));
      queue.push_back(
          Item{&child, std::move(child_path), item.depth + 1, child_x, child_x + cw});
      child_x += cw;
    }
  }

  // Cell runs per depth row.
  const int total_rows = DepthOf(displayed);
  std::vector<std::vector<Seg>> rows(static_cast<size_t>(std::max(0, total_rows)));
  std::vector<std::vector<const Placed*>> by_row(rows.size());
  for (const Placed& p : placed) {
    by_row[static_cast<size_t>(p.depth)].push_back(&p);
  }
  for (size_t d = 0; d < rows.size(); ++d) {
    std::vector<Seg>& row = rows[d];
    std::sort(by_row[d].begin(), by_row[d].end(),
              [](const Placed* a, const Placed* b) { return a->x < b->x; });
    int cursor = 0;
    for (const Placed* p : by_row[d]) {
      if (p->x > cursor) {
        row.push_back(Seg{std::string(static_cast<size_t>(p->x - cursor), ' ')});
      }
      Mark mark;
      const auto mark_it = marks.find(p->path);
      if (mark_it != marks.end()) {
        const DifferentialNodeDiff* node = mark_it->second;
        if (node->base_per_work <= 0.0) {
          // New node: the raw ratio ~1/epsilon is scale-meaningless — render
          // the strong hot band plus the "+new" label marker instead.
          mark = Mark{Hue::kHot, Band::kStrong, true};
        } else {
          mark = Mark{node->delta_ratio >= 0 ? Hue::kHot : Hue::kCool,
                      BandFor(std::fabs(node->delta_ratio)), false};
        }
      }
      const bool selected =
          state.selected.has_value() && *state.selected == p->path;
      const bool match =
          !state.search.empty() &&
          ContainsCaseInsensitive(p->frame->function, state.search);
      const int pct =
          denominator > 0
              ? static_cast<int>(std::lround(
                    100.0 * static_cast<double>(p->frame->total_value) /
                    static_cast<double>(denominator)))
              : 0;
      std::string label =
          (selected ? ">" : "") + p->frame->function + " " + std::to_string(pct) + "%";
      if (mark.is_new) {
        label += " +new";
      }
      Seg seg;
      seg.text = CellBody(p->w, label, mark);
      seg.inverted = match || selected;
      seg.bold = selected;
      seg.color = ColorFor(mark, &seg.dim);
      row.push_back(std::move(seg));
      cursor = p->x + p->w;
    }
    if (cursor < width) {
      row.push_back(Seg{std::string(static_cast<size_t>(width - cursor), ' ')});
    }
  }

  // Frame rows inside the scrolled viewport.
  if (viewport_rows > 0) {
    if (total_rows == 0) {
      lines.push_back(ftxui::text("empty profile"));
    } else {
      const int scroll = DeriveScrollRow(displayed, state, viewport_rows);
      const int last = std::min(total_rows, scroll + viewport_rows);
      for (int d = scroll; d < last; ++d) {
        std::vector<ftxui::Element> cells;
        cells.reserve(rows[static_cast<size_t>(d)].size());
        for (const Seg& seg : rows[static_cast<size_t>(d)]) {
          cells.push_back(SegElement(seg));
        }
        lines.push_back(ftxui::hbox(std::move(cells)));
      }
    }
  }
  return ftxui::vbox(std::move(lines));
}

absl::StatusOr<DiffOverlay> PairDiffOverlay(
    std::shared_ptr<const std::vector<ProfileWindow>> owning_snapshot,
    DifferentialResult result) {
  if (owning_snapshot == nullptr) {
    return absl::InvalidArgumentError(
        "a differential cannot be held without the ProfileWindowStore "
        "snapshot that owns its frame pointers");
  }
  return DiffOverlay{std::move(owning_snapshot), std::move(result)};
}

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
