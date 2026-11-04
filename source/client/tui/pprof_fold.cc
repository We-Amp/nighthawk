// FORK-ONLY (We-Amp): implementation of pprof loading and stack folding.
// See pprof_fold.h for the contract.
#include "source/client/tui/pprof_fold.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "google/protobuf/io/gzip_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using perftools::profiles::Location;
using perftools::profiles::Profile;

constexpr char kGzipMagic0 = '\x1f';
constexpr char kGzipMagic1 = '\x8b';

std::string ReadFileOrEmpty(absl::string_view path, absl::Status& status) {
  std::ifstream in(std::string(path), std::ios::binary);
  if (!in) {
    status = absl::NotFoundError(
        absl::StrCat("cannot open profile file: ", path));
    return "";
  }
  std::string data((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());
  if (in.bad()) {
    status = absl::InternalError(absl::StrCat("error reading: ", path));
    return "";
  }
  return data;
}

// Internal fold node: identity is the frame key within its parent, so
// recursive frames (same function at several depths) stay distinct while
// same-frame siblings merge, flamegraph-style.
struct FoldNode {
  FoldedFrame frame;
  absl::flat_hash_map<std::string, std::unique_ptr<FoldNode>> children;
};

// Recursively materializes children, sorted by total_value descending with a
// function-name tiebreak for determinism.
void MaterializeChildren(FoldNode& node) {
  node.frame.children.reserve(node.children.size());
  for (auto& [key, child] : node.children) {
    MaterializeChildren(*child);
    node.frame.children.push_back(std::move(child->frame));
  }
  std::sort(node.frame.children.begin(), node.frame.children.end(),
            [](const FoldedFrame& a, const FoldedFrame& b) {
              if (a.total_value != b.total_value) {
                return a.total_value > b.total_value;
              }
              return a.function < b.function;
            });
}

}  // namespace

absl::StatusOr<Profile> LoadProfile(absl::string_view path) {
  absl::Status read_status;
  std::string data = ReadFileOrEmpty(path, read_status);
  if (!read_status.ok()) {
    return read_status;
  }
  if (data.empty()) {
    return absl::InvalidArgumentError(absl::StrCat("empty profile: ", path));
  }

  Profile profile;
  google::protobuf::io::ArrayInputStream raw(data.data(),
                                             static_cast<int>(data.size()));
  if (data.size() >= 2 && data[0] == kGzipMagic0 && data[1] == kGzipMagic1) {
    // The machine route's `-proto` output is gzip-compressed (spec'd by the
    // pprof format: "On-disk, the serialized proto must be gzip-compressed").
    google::protobuf::io::GzipInputStream gzip(
        &raw, google::protobuf::io::GzipInputStream::GZIP);
    if (!profile.ParseFromZeroCopyStream(&gzip)) {
      return absl::InvalidArgumentError(
          absl::StrCat("not a decodable gzipped pprof proto: ", path));
    }
  } else {
    // Fallback: plain uncompressed proto.
    if (!profile.ParseFromZeroCopyStream(&raw)) {
      return absl::InvalidArgumentError(
          absl::StrCat("not a decodable pprof proto: ", path));
    }
  }
  return profile;
}

absl::StatusOr<FoldedTree> FoldProfile(const Profile& profile) {
  const auto& string_table = profile.string_table();
  auto str_of = [&string_table](int64_t idx) -> absl::string_view {
    if (idx >= 0 &&
        static_cast<uint64_t>(idx) < static_cast<uint64_t>(string_table.size())) {
      return string_table[static_cast<size_t>(idx)];
    }
    return {};
  };

  // Index tables: locations and functions by id (pprof ids are 1-based).
  absl::flat_hash_map<uint64_t, const Location*> locations;
  for (const Location& loc : profile.location()) {
    locations[loc.id()] = &loc;
  }
  absl::flat_hash_map<uint64_t, const perftools::profiles::Function*> functions;
  for (const perftools::profiles::Function& fn : profile.function()) {
    functions[fn.id()] = &fn;
  }

  // Value dimension: sample_type[0] is the sample count, sample_type[1] the
  // measurement (time, bytes, ...) when both exist — the pprof convention.
  int sample_idx = 0;
  int value_idx = profile.sample_type_size() >= 2 ? 1 : 0;
  FoldedTree tree;
  if (profile.sample_type_size() > 0) {
    tree.value_unit = absl::StrCat(
        str_of(profile.sample_type(value_idx).type()), "-",
        str_of(profile.sample_type(value_idx).unit()));
  }
  tree.period_nanos = profile.period();
  tree.time_nanos = profile.time_nanos();

  FoldNode virtual_root;
  int64_t min_timestamp = 0;
  int64_t max_timestamp = 0;
  bool have_timestamp = false;
  uint64_t frames_with_names = 0;
  uint64_t frames_total = 0;

  for (const perftools::profiles::Sample& sample : profile.sample()) {
    const int64_t num_samples =
        sample_idx < sample.value_size() ? sample.value(sample_idx) : 0;
    const int64_t value =
        value_idx < sample.value_size() ? sample.value(value_idx) : 0;
    tree.total_samples += static_cast<uint64_t>(num_samples);
    tree.total_value += static_cast<uint64_t>(value);

    for (const auto& label : sample.label()) {
      if (str_of(label.key()) == "timestamp_ns") {
        const int64_t ts = label.num();
        if (!have_timestamp) {
          have_timestamp = true;
          min_timestamp = max_timestamp = ts;
        } else {
          min_timestamp = std::min(min_timestamp, ts);
          max_timestamp = std::max(max_timestamp, ts);
        }
      }
    }

    // pprof stacks are leaf-first; fold root-first.
    FoldNode* node = &virtual_root;
    for (auto it = sample.location_id().rbegin();
         it != sample.location_id().rend(); ++it) {
      const Location* loc = [&] {
        auto hit = locations.find(*it);
        return hit == locations.end() ? nullptr : hit->second;
      }();

      FoldedFrame frame;
      bool named = false;
      if (loc != nullptr && loc->line_size() > 0) {
        auto fn_hit = functions.find(loc->line(0).function_id());
        if (fn_hit != functions.end()) {
          const perftools::profiles::Function* fn = fn_hit->second;
          frame.function = std::string(str_of(fn->name()));
          frame.file = std::string(str_of(fn->filename()));
          frame.line = static_cast<int>(loc->line(0).line());
          named = !frame.function.empty();
        }
      }
      if (!named) {
        // Unsymbolized location: degrade to the address — never a crash.
        const uint64_t address = loc != nullptr ? loc->address() : 0;
        frame.function = absl::StrFormat("0x%x", address);
        frame.symbolized = false;
      } else {
        frame.symbolized = true;
        ++frames_with_names;
      }
      ++frames_total;

      std::string key = frame.function;
      auto [node_it, inserted] = node->children.try_emplace(
          key, std::make_unique<FoldNode>());
      if (inserted) {
        node_it->second->frame = std::move(frame);
      }
      node = node_it->second.get();
      node->frame.total_samples += static_cast<uint64_t>(num_samples);
      node->frame.total_value += static_cast<uint64_t>(value);
    }
    // Leaf attribution.
    node->frame.self_samples += static_cast<uint64_t>(num_samples);
    node->frame.self_value += static_cast<uint64_t>(value);
  }

  if (have_timestamp) {
    tree.duration_nanos = max_timestamp + profile.period() - min_timestamp;
  }
  // A sample with an empty location stack has no frame to attribute to; the
  // virtual root absorbed it. Surface it as a synthetic unsymbolized root so
  // the totals stay conserved.
  if (virtual_root.frame.total_value > 0 ||
      virtual_root.frame.total_samples > 0) {
    FoldedFrame synthetic_root;
    synthetic_root.function = "[unknown stack]";
    synthetic_root.symbolized = false;
    synthetic_root.self_samples = virtual_root.frame.total_samples;
    synthetic_root.self_value = virtual_root.frame.total_value;
    synthetic_root.total_samples = virtual_root.frame.total_samples;
    synthetic_root.total_value = virtual_root.frame.total_value;
    ++frames_total;
    virtual_root.frame.children.push_back(std::move(synthetic_root));
  }
  tree.symbolized = frames_total > 0 && frames_with_names == frames_total;

  MaterializeChildren(virtual_root);
  tree.root = std::move(virtual_root.frame.children);

#ifndef NDEBUG
  // Conservation asserts at every node (debug builds): self + descendants
  // must equal total, and the roots must carry exactly the tree totals.
  uint64_t root_value = 0;
  uint64_t root_samples = 0;
  for (const FoldedFrame& f : tree.root) {
    root_value += f.total_value;
    root_samples += f.total_samples;
  }
  if (root_value != tree.total_value || root_samples != tree.total_samples) {
    return absl::InternalError(
        "fold conservation violated: root totals != profile totals");
  }
#endif

  return tree;
}

}  // namespace Client
}  // namespace Tui
}  // namespace Nighthawk
