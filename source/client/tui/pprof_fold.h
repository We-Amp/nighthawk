// FORK-ONLY (We-Amp): pprof proto loading and stack folding for the TUI
// flamegraph pipeline (M1 plan task 3). Consumes the vendored
// perftools.profiles.Profile (//api/pprof) and produces the FoldedTree the
// flamegraph renderer, navigation and search operate on.
//
// Quarantine note: this code depends only on the generated pprof proto (pure
// protobuf C++), absl, and protobuf's gzip stream — no Envoy C++ sources.
#ifndef NIGHTHAWK_TUI_PPROF_FOLD_H_
#define NIGHTHAWK_TUI_PPROF_FOLD_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "api/pprof/profile.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// Child-index path from the tree root to a frame: path[i] is the child index
// at depth i. THE addressing key of the flamegraph pane (M1 task 5) —
// navigation cursor, zoom target, search hits, and the differential
// overlay's node addressing all speak FramePath. Child indices (not
// function-name paths) keep the addressing honest for same-named frames:
// recursion and repeated stacks alias under name paths, never under
// positions in the (total-sorted) tree.
using FramePath = std::vector<int>;

// One row of the folded flamegraph tree.
struct FoldedFrame {
  // Symbolized function name; unsymbolized locations degrade to "0x<addr>"
  // with symbolized=false (never a crash — Review Focus 5).
  std::string function;
  // Best-effort source position from the profile's location line entries.
  std::string file;
  int line = 0;

  // Leaf attribution: samples/values attributed to this frame as a leaf.
  uint64_t self_samples = 0;
  uint64_t self_value = 0;
  // Self plus all descendants.
  uint64_t total_samples = 0;
  uint64_t total_value = 0;

  // Sorted by total_value descending (render + nav + search rely on this;
  // ties break on function name ascending for determinism).
  std::vector<FoldedFrame> children;

  // Whether this frame resolved to a profile Function name.
  bool symbolized = false;
};

// The folded flamegraph for one profile window.
struct FoldedTree {
  uint64_t total_samples = 0;
  uint64_t total_value = 0;
  // The profile's value sample-type unit ("<type>-<unit>", e.g.
  // "cpu-nanoseconds", "bytes").
  std::string value_unit;
  // Profile period, nanoseconds (§8.4 denominators may need it).
  int64_t period_nanos = 0;
  // SUT clock stamp (profile.time_nanos) — the §8.5 join key and the
  // window-interval source.
  int64_t time_nanos = 0;
  // Derived from the samples' timestamp_ns labels:
  // max(timestamp) + period − min(timestamp); 0 when no timestamps exist.
  int64_t duration_nanos = 0;

  // Virtual root: the real stack roots as children (per-stack-root rows, each
  // carrying its full subtree).
  std::vector<FoldedFrame> root;

  // Whether every sampled location resolved to a function name.
  bool symbolized = false;
};

// Reads a pprof proto file — the gzipped `-proto` output of the machine route
// (gzip inflate via protobuf's GzipInputStream) — and parses it into a
// Profile. Plain uncompressed protos are accepted as a fallback; a missing or
// unreadable file is kNotFound, an undecodable file is kInvalidArgument.
absl::StatusOr<perftools::profiles::Profile> LoadProfile(absl::string_view path);

// Folds a parsed profile into a FoldedTree. Pure: reads the profile, returns
// the tree, touches nothing else. Sample-count and value totals are conserved
// exactly (self + children == total at every node).
absl::StatusOr<FoldedTree> FoldProfile(const perftools::profiles::Profile& profile);

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk

#endif  // NIGHTHAWK_TUI_PPROF_FOLD_H_
