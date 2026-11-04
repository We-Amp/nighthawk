// FORK-ONLY (We-Amp): pprof parse/fold/symbolize/worker-pool contract tests.
// Plain cc_test (see BUILD comment): the TUI quarantine keeps this off Envoy's
// C++ sources and test helpers.
//
// The real-profile fixture (test/fixtures/nh_sut_pprof.pb.gz) is the Task 1
// machine-route output: `go tool pprof -proto -output` over the staged SUT
// CPU profile (runbook docs/operations/nighthawk-sut-profiled.md, evidence
// 2026-11-04). Ground truth for its totals comes from the independent
// `go tool pprof -top` run on the same file, NOT from this fold code:
//   total 630ms across 63 samples (10ms period granularity), type
//   cpu/nanoseconds, period 10000000.

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "api/pprof/profile.pb.h"
#include "source/client/tui/pprof_fold.h"
#include "source/client/tui/pprof_worker_pool.h"
#include "source/client/tui/profile_symbolizer.h"

namespace Nighthawk::Client::Tui {
namespace {

// Resolves a runfiles-relative fixture path via Bazel's TEST_SRCDIR /
// TEST_WORKSPACE environment (falls back to a cwd-relative path for scratch
// runs outside bazel).
std::string FixturePath(const std::string& runfiles_rel) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (srcdir != nullptr && workspace != nullptr) {
    return std::string(srcdir) + "/" + workspace + "/" + runfiles_rel;
  }
  return runfiles_rel;
}

// --- synthetic profile helpers -------------------------------------------

// Builds a minimal CPU-shaped profile: sample_type = {sample/count,
// cpu/nanoseconds}, period 10ms, clock stamp at 1_000_000_000 ns.
perftools::profiles::Profile SyntheticCpuProfile() {
  perftools::profiles::Profile p;
  p.add_string_table("");        // 0: pprof requires "" at index 0
  p.add_string_table("sample");  // 1
  p.add_string_table("count");   // 2
  p.add_string_table("cpu");     // 3
  p.add_string_table("nanoseconds");  // 4
  p.add_string_table("main");         // 5
  p.add_string_table("main.cc");      // 6
  p.add_string_table("worker");       // 7
  p.add_string_table("worker.cc");    // 8
  p.add_string_table("leaf");         // 9
  p.add_string_table("leaf.cc");      // 10
  p.add_string_table("timestamp_ns");  // 11
  p.add_string_table("thread_id");     // 12

  auto* st_count = p.add_sample_type();
  st_count->set_type(1);
  st_count->set_unit(2);
  auto* st_cpu = p.add_sample_type();
  st_cpu->set_type(3);
  st_cpu->set_unit(4);

  auto* pt = p.mutable_period_type();
  pt->set_type(3);
  pt->set_unit(4);
  p.set_period(10000000);
  p.set_time_nanos(1000000000);

  auto* f_main = p.add_function();
  f_main->set_id(1);
  f_main->set_name(5);
  f_main->set_filename(6);
  auto* f_worker = p.add_function();
  f_worker->set_id(2);
  f_worker->set_name(7);
  f_worker->set_filename(8);
  auto* f_leaf = p.add_function();
  f_leaf->set_id(3);
  f_leaf->set_name(9);
  f_leaf->set_filename(10);

  auto* loc_main = p.add_location();
  loc_main->set_id(1);
  loc_main->set_address(0x1000);
  auto* l_main = loc_main->add_line();
  l_main->set_function_id(1);
  l_main->set_line(10);
  auto* loc_worker = p.add_location();
  loc_worker->set_id(2);
  loc_worker->set_address(0x2000);
  auto* l_worker = loc_worker->add_line();
  l_worker->set_function_id(2);
  l_worker->set_line(20);
  auto* loc_leaf = p.add_location();
  loc_leaf->set_id(3);
  loc_leaf->set_address(0x3000);
  auto* l_leaf = loc_leaf->add_line();
  l_leaf->set_function_id(3);
  l_leaf->set_line(30);

  // Sample with a timestamp label (the §8.5 join key shape).
  auto label_timestamp = [](perftools::profiles::Sample* s, int64_t ts) {
    auto* lab = s->add_label();
    lab->set_key(11);
    lab->set_num(ts);
  };

  // Thread A + B share the two-frame stack worker <- main... in pprof
  // leaf-first order: location_id[0] is the leaf.
  for (int i = 0; i < 2; ++i) {
    auto* s = p.add_sample();
    s->add_location_id(2);  // leaf: worker
    s->add_location_id(1);  // caller: main
    s->add_value(1);        // sample count
    s->add_value(20000000); // cpu nanoseconds
    label_timestamp(s, 1000000000);
    auto* lab = s->add_label();
    lab->set_key(12);
    lab->set_num(i + 1);  // distinct thread ids
  }
  // Thread C: single-frame stack leaf.
  auto* s_c = p.add_sample();
  s_c->add_location_id(3);
  s_c->add_value(1);
  s_c->add_value(10000000);
  label_timestamp(s_c, 1100000000);
  auto* lab_c = s_c->add_label();
  lab_c->set_key(12);
  lab_c->set_num(3);

  // One zero-label (no labels at all) sample: root-only stack main.
  auto* s_zero = p.add_sample();
  s_zero->add_location_id(1);
  s_zero->add_value(0);
  s_zero->add_value(0);

  return p;
}

// Recursive conservation check: self + descendants == total at every node,
// children sorted by total_value desc (tie: function name asc). Returns the
// node's {total_samples, total_value}.
std::pair<uint64_t, uint64_t> CheckNodeConservation(const FoldedFrame& f) {
  uint64_t child_value = 0;
  uint64_t child_samples = 0;
  for (const FoldedFrame& c : f.children) {
    auto [c_samples, c_value] = CheckNodeConservation(c);
    child_samples += c_samples;
    child_value += c_value;
  }
  EXPECT_EQ(f.self_value + child_value, f.total_value)
      << "conservation violated at " << f.function;
  EXPECT_EQ(f.self_samples + child_samples, f.total_samples)
      << "sample conservation violated at " << f.function;
  for (size_t i = 1; i < f.children.size(); ++i) {
    const FoldedFrame& prev = f.children[i - 1];
    const FoldedFrame& cur = f.children[i];
    EXPECT_TRUE(prev.total_value > cur.total_value ||
                (prev.total_value == cur.total_value &&
                 prev.function <= cur.function))
        << "children not sorted by total_value desc at " << f.function;
  }
  return {f.total_samples, f.total_value};
}

const FoldedFrame* FindFrame(const std::vector<FoldedFrame>& frames,
                             absl::string_view name) {
  for (const FoldedFrame& f : frames) {
    if (f.function == name) {
      return &f;
    }
    const FoldedFrame* hit = FindFrame(f.children, name);
    if (hit != nullptr) {
      return hit;
    }
  }
  return nullptr;
}

// --- tests ---------------------------------------------------------------

// Review Focus 2 (load half): the committed real-profile fixture parses, and
// its totals match the independent `go tool pprof -top` ground truth.
TEST(TuiPprofFoldTest, LoadProfileDecodesCompressedProto) {
  absl::StatusOr<perftools::profiles::Profile> profile =
      LoadProfile(FixturePath("test/fixtures/nh_sut_pprof.pb.gz"));
  ASSERT_TRUE(profile.ok());

  ASSERT_EQ(profile->sample_type_size(), 2);
  EXPECT_EQ(profile->sample(0).value_size(), 2);
  int64_t sample_count = 0;
  int64_t value_sum = 0;
  for (const auto& s : profile->sample()) {
    ASSERT_GE(s.value_size(), 2);
    sample_count += s.value(0);
    value_sum += s.value(1);
  }
  EXPECT_EQ(sample_count, 63) << "vs go tool pprof -top: 63 x 10ms flat";
  EXPECT_EQ(value_sum, 630000000) << "vs go tool pprof -top: 630ms total";
  EXPECT_EQ(profile->period(), 10000000);
  EXPECT_EQ(profile->location_size(), 461) << "runbook: 461 locations";
}

// LoadProfile failure path: missing file is a NotFound, not a crash.
TEST(TuiPprofFoldTest, LoadProfileMissingFileIsNotFound) {
  absl::StatusOr<perftools::profiles::Profile> profile =
      LoadProfile("/nonexistent/nh-sut/no-such-profile.pb.gz");
  EXPECT_EQ(profile.status().code(), absl::StatusCode::kNotFound);
}

// Review Focus 2 (fold half): totals equal the proto's sample/value sums
// exactly, self+children conservation holds at every node. The synthetic
// profile covers 3 threads sharing stacks plus one zero-label sample.
TEST(TuiPprofFoldTest, FoldPreservesSampleAndValueTotals) {
  perftools::profiles::Profile p = SyntheticCpuProfile();
  int64_t sample_count = 0;
  int64_t value_sum = 0;
  for (const auto& s : p.sample()) {
    sample_count += s.value(0);
    value_sum += s.value(1);
  }
  EXPECT_EQ(sample_count, 3);
  EXPECT_EQ(value_sum, 50000000);

  absl::StatusOr<FoldedTree> tree = FoldProfile(p);
  ASSERT_TRUE(tree.ok());
  EXPECT_EQ(tree->total_samples, 3u);
  EXPECT_EQ(tree->total_value, 50000000u);
  EXPECT_EQ(tree->value_unit, "cpu-nanoseconds");

  uint64_t root_value = 0;
  uint64_t root_samples = 0;
  for (const FoldedFrame& f : tree->root) {
    auto [f_samples, f_value] = CheckNodeConservation(f);
    root_samples += f_samples;
    root_value += f_value;
  }
  EXPECT_EQ(root_samples, tree->total_samples);
  EXPECT_EQ(root_value, tree->total_value);
}

// Children ordering is deterministic: total_value desc.
TEST(TuiPprofFoldTest, FoldSortsChildrenByTotalValue) {
  perftools::profiles::Profile p = SyntheticCpuProfile();
  // main is a shared root (40ms under it via worker, plus the 0ms zero-label
  // sample attributed directly to it) and leaf is another root (10ms); the
  // ordering must be deterministic.
  absl::StatusOr<FoldedTree> tree = FoldProfile(p);
  ASSERT_TRUE(tree.ok());
  ASSERT_GE(tree->root.size(), 2u);
  for (size_t i = 1; i < tree->root.size(); ++i) {
    EXPECT_GE(tree->root[i - 1].total_value, tree->root[i].total_value);
  }
  // worker's subtree carries 40ms; it must sort before any lighter sibling.
  const FoldedFrame* worker = FindFrame(tree->root, "worker");
  ASSERT_NE(worker, nullptr);
  const FoldedFrame* main = FindFrame(tree->root, "main");
  ASSERT_NE(main, nullptr);
  EXPECT_EQ(main->total_value, 40000000u);  // two 20ms samples through worker+main
  EXPECT_EQ(worker->total_value, 40000000u);
}

// A 3-deep stack folds into a 3-deep tree (row per depth).
TEST(TuiPprofFoldTest, FoldBuildsRowPerDepth) {
  perftools::profiles::Profile p;
  p.add_string_table("");
  p.add_string_table("sample");
  p.add_string_table("count");
  p.add_string_table("a");
  p.add_string_table("b");
  p.add_string_table("c");
  auto* st = p.add_sample_type();
  st->set_type(1);
  st->set_unit(2);
  for (int i = 0; i < 3; ++i) {
    auto* f = p.add_function();
    f->set_id(i + 1);
    f->set_name(i + 3);
    auto* loc = p.add_location();
    loc->set_id(i + 1);
    auto* line = loc->add_line();
    line->set_function_id(i + 1);
    line->set_line(100 + i);
  }
  auto* s = p.add_sample();
  // Leaf-first: c <- b <- a.
  s->add_location_id(3);
  s->add_location_id(2);
  s->add_location_id(1);
  s->add_value(5);
  s->add_value(0);

  absl::StatusOr<FoldedTree> tree = FoldProfile(p);
  ASSERT_TRUE(tree.ok());
  ASSERT_EQ(tree->root.size(), 1u);
  EXPECT_EQ(tree->root[0].function, "a");
  ASSERT_EQ(tree->root[0].children.size(), 1u);
  EXPECT_EQ(tree->root[0].children[0].function, "b");
  ASSERT_EQ(tree->root[0].children[0].children.size(), 1u);
  EXPECT_EQ(tree->root[0].children[0].children[0].function, "c");
  // Leaf-attribution: only the leaf carries self; totals flow to the root.
  EXPECT_EQ(tree->root[0].self_value, 0u);
  EXPECT_EQ(tree->root[0].children[0].self_value, 0u);
  EXPECT_EQ(tree->root[0].children[0].children[0].self_value, 5u);
  EXPECT_EQ(tree->root[0].total_value, 5u);
  EXPECT_EQ(tree->root[0].children[0].total_value, 5u);
  EXPECT_EQ(tree->root[0].children[0].children[0].total_value, 5u);
}

// Symbolization markers: named functions fill names + symbolized=true;
// locations-only profiles degrade to "0x<addr>" + symbolized=false.
TEST(TuiPprofFoldTest, FoldCarriesSymbolizationMarker) {
  // Half 1: function names present.
  perftools::profiles::Profile named = SyntheticCpuProfile();
  absl::StatusOr<FoldedTree> tree = FoldProfile(named);
  ASSERT_TRUE(tree.ok());
  EXPECT_TRUE(tree->symbolized);
  const FoldedFrame* main = FindFrame(tree->root, "main");
  ASSERT_NE(main, nullptr);
  EXPECT_TRUE(main->symbolized);
  EXPECT_EQ(main->file, "main.cc");
  EXPECT_EQ(main->line, 10);

  // Half 2: locations only — addresses, no functions (the unsymbolized case).
  perftools::profiles::Profile bare;
  bare.add_string_table("");
  bare.add_string_table("sample");
  bare.add_string_table("count");
  auto* st = bare.add_sample_type();
  st->set_type(1);
  st->set_unit(2);
  auto* loc = bare.add_location();
  loc->set_id(1);
  loc->set_address(0xdeadbeef);
  auto* s = bare.add_sample();
  s->add_location_id(1);
  s->add_value(1);
  s->add_value(0);
  absl::StatusOr<FoldedTree> bare_tree = FoldProfile(bare);
  ASSERT_TRUE(bare_tree.ok());
  EXPECT_FALSE(bare_tree->symbolized);
  ASSERT_EQ(bare_tree->root.size(), 1u);
  EXPECT_EQ(bare_tree->root[0].function, "0xdeadbeef");
  EXPECT_FALSE(bare_tree->root[0].symbolized);
}

// §8.5 join key and window-interval source: clock stamp, period, duration.
TEST(TuiPprofFoldTest, FoldCarriesSutClockAndPeriod) {
  perftools::profiles::Profile p = SyntheticCpuProfile();
  absl::StatusOr<FoldedTree> tree = FoldProfile(p);
  ASSERT_TRUE(tree.ok());
  EXPECT_EQ(tree->time_nanos, 1000000000);
  EXPECT_EQ(tree->period_nanos, 10000000);
  // duration = max(timestamp)+period − min(timestamp)
  //   = 1100000000 + 10000000 − 1000000000 = 110000000
  EXPECT_EQ(tree->duration_nanos, 110000000);
}

// Review Focus 5: a tool argv pointing at a nonexistent binary degrades to
// kNotFound (the caller marks the window unsymbolized — never crashes).
TEST(TuiPprofFoldTest, SymbolizeMissingToolDegradesToNotFound) {
  ProfileSymbolizer symbolizer({"/nonexistent/nh-sut/no-such-pprof-tool"},
                               "/tmp/nh-build-id-binaries");
  absl::Status status = symbolizer.Symbolize(
      "/raw/in.prof", "/sut/envoy", "/out/symbolized.pb.gz");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound) << status;
}

// Test seam: the constructed argv + env of the proven machine route.
TEST(TuiPprofFoldTest, SymbolizeBuildsArgvAndEnv) {
  ProfileSymbolizer::Invocation inv = ProfileSymbolizer::BuildInvocation(
      {"go", "tool", "pprof"}, "/tmp/nh-build-id-binaries", "/raw/in.prof",
      "/sut/envoy", "/out/symbolized.pb.gz");
  EXPECT_EQ(
      absl::StrJoin(inv.argv, " "),
      "go tool pprof -proto -output /out/symbolized.pb.gz /sut/envoy "
      "/raw/in.prof");
  bool found = false;
  for (const std::string& entry : inv.env) {
    if (entry == "PPROF_BINARY_PATH=/tmp/nh-build-id-binaries") {
      found = true;
    }
  }
  EXPECT_TRUE(found) << "PPROF_BINARY_PATH missing from child env";
}

// Bounded worker pool: jobs submitted from two threads all run to completion;
// the destructor joins.
TEST(TuiPprofWorkerPoolTest, PoolRunsJobsToCompletion) {
  constexpr int kJobsPerThread = 100;
  constexpr int kThreads = 2;
  std::mutex mu;
  std::set<int> done;
  {
    PprofWorkerPool pool(2);
    std::vector<std::thread> submitters;
    for (int t = 0; t < kThreads; ++t) {
      submitters.emplace_back([&pool, &mu, &done, t] {
        for (int i = 0; i < kJobsPerThread; ++i) {
          int id = t * kJobsPerThread + i;
          pool.Submit([&mu, &done, id] {
            std::lock_guard<std::mutex> lock(mu);
            done.insert(id);
          });
        }
      });
    }
    for (std::thread& t : submitters) {
      t.join();
    }
    // The destructor joins: finishes all queued jobs, then exits.
  }
  std::lock_guard<std::mutex> lock(mu);
  EXPECT_EQ(done.size(), kThreads * kJobsPerThread);
}

// Fixture-fidelity cross-check (task 3 Step 4, made permanent by review):
// the committed fixture IS the Task 1 machine-route output —
// `go tool pprof -proto -output /tmp/out.pb
// pprof-binaries/b2d89e0ae53916f28770771fbdd49c5c99c5c5a2/envoy envoy-1.prof`
// over the staged SUT CPU profile (runbook
// docs/operations/nighthawk-sut-profiled.md, machine-route evidence
// 2026-11-04; fixture sha256 4753dfb270878bda2f729da066684377122002a344e8b71e2
// e63a4f223088c90, byte-identical to that run's /tmp/out.pb). The expected
// values below are the independent `go tool pprof -top` ground truth on that
// same file, not this fold code's output: 630ms total across 63 x 10ms flat
// samples, with named flat rows operator== 20ms and event_active,
// duration_cast, StringResizeAndOverwriteFallback 10ms each (the
// unsymbolized remainder aggregates into -top's [envoy]/[libc.so.6]
// mapping rows, which fold reports per-address instead).
TEST(TuiPprofFoldTest, FoldMatchesPprofTopGroundTruth) {
  absl::StatusOr<perftools::profiles::Profile> profile =
      LoadProfile(FixturePath("test/fixtures/nh_sut_pprof.pb.gz"));
  ASSERT_TRUE(profile.ok());
  absl::StatusOr<FoldedTree> tree = FoldProfile(*profile);
  ASSERT_TRUE(tree.ok());

  EXPECT_EQ(tree->total_samples, 63u);
  EXPECT_EQ(tree->total_value, 630000000u);

  // Root-level conservation.
  uint64_t root_value = 0;
  uint64_t root_samples = 0;
  for (const FoldedFrame& f : tree->root) {
    auto [f_samples, f_value] = CheckNodeConservation(f);
    root_samples += f_samples;
    root_value += f_value;
  }
  EXPECT_EQ(root_samples, 63u);
  EXPECT_EQ(root_value, 630000000u);

  // The four named flat rows from `go tool pprof -top` (self-value sums
  // aggregated over every node carrying the name).
  auto flat_of = [&](const char* name) {
    uint64_t flat = 0;
    std::function<void(const std::vector<FoldedFrame>&)> walk =
        [&](const std::vector<FoldedFrame>& frames) {
          for (const FoldedFrame& f : frames) {
            if (f.function == name) {
              flat += f.self_value;
            }
            walk(f.children);
          }
        };
    walk(tree->root);
    return flat;
  };
  EXPECT_EQ(flat_of("operator=="), 20000000u);
  EXPECT_EQ(flat_of("event_active"), 10000000u);
  EXPECT_EQ(flat_of("duration_cast"), 10000000u);
  EXPECT_EQ(flat_of("StringResizeAndOverwriteFallback"), 10000000u);
}

// Honest check of the default argv against the REAL tool presence on this
// machine: with `go` on PATH, Symbolize must NOT return kNotFound — the
// spawn has to resolve the bare tool name through PATH (a posix_spawn
// instead of posix_spawnp regression fails exactly here, and the argv-seam
// tests cannot see it). The real tool then fails fast on the bogus input
// paths with kInternal. Without `go` on PATH, kNotFound is the expectation.
bool ProgramOnPath(const std::string& program) {
  const char* path_env = std::getenv("PATH");
  if (path_env == nullptr) {
    return false;
  }
  size_t start = 0;
  const std::string path(path_env);
  while (start <= path.size()) {
    size_t end = path.find(':', start);
    if (end == std::string::npos) {
      end = path.size();
    }
    const std::string dir = path.substr(start, end - start);
    if (!dir.empty()) {
      const std::string candidate = dir + "/" + program;
      if (access(candidate.c_str(), X_OK) == 0) {
        return true;
      }
    }
    start = end + 1;
  }
  return false;
}

TEST(TuiPprofSymbolizerTest, SymbolizeDefaultArgvTracksRealToolPresence) {
  const bool go_on_path = ProgramOnPath("go");
  ProfileSymbolizer symbolizer({}, "/tmp/nh-build-id-binaries");
  absl::Status status = symbolizer.Symbolize(
      "no-such-raw.prof", "no-such-binary", "/tmp/nh-tui-symbolize-out.pb.gz");
  if (go_on_path) {
    EXPECT_NE(status.code(), absl::StatusCode::kNotFound)
        << "default argv must resolve through PATH when the tool exists: "
        << status;
    EXPECT_EQ(status.code(), absl::StatusCode::kInternal)
        << "real tool started but must fail fast on the missing inputs: "
        << status;
  } else {
    EXPECT_EQ(status.code(), absl::StatusCode::kNotFound) << status;
  }
}

}  // namespace
}  // namespace Nighthawk::Client::Tui
