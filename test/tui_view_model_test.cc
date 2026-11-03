#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "absl/container/flat_hash_map.h"
#include "api/client/live.pb.h"
#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using nighthawk::client::LiveSnapshot;
using nighthawk::client::StatisticSnapshot;

// FORK-ONLY (We-Amp): the immutable snapshot store and its ApplySnapshot
// reducer. The store is the seam between the feed's read thread (store) and
// the render loop (load); its contract is that every loaded pointer is one
// fully-formed, never-mutated model.

// A synthetic snapshot carrying one latency statistic and one counter.
LiveSnapshot snapshotAt(uint64_t monotonic_ns, uint64_t p50, uint64_t p90, uint64_t p99,
                        uint64_t counter_value) {
  LiveSnapshot snapshot;
  snapshot.set_monotonic_ns(static_cast<int64_t>(monotonic_ns));
  StatisticSnapshot* stat = snapshot.add_statistics();
  stat->set_id("benchmark.http_latency");
  stat->set_count(100);
  stat->set_p50_ns(p50);
  stat->set_p90_ns(p90);
  stat->set_p99_ns(p99);
  (*snapshot.mutable_counters())["benchmark.http_2xx"] = counter_value;
  return snapshot;
}

TEST(RunViewModelTest, ApplySnapshotBuildsMonotonicSeries) {
  RunViewModel model;
  model.run_state = "running";
  model = ApplySnapshot(std::move(model), snapshotAt(1'000'000, 100, 200, 300, 7));
  model = ApplySnapshot(std::move(model), snapshotAt(2'000'000, 110, 210, 310, 9));

  ASSERT_EQ(model.latency_band.size(), 2u);
  EXPECT_LT(model.latency_band[0].monotonic_ns, model.latency_band[1].monotonic_ns);
  EXPECT_EQ(model.latency_band[0].monotonic_ns, 1'000'000u);
  EXPECT_EQ(model.latency_band[0].p50_ns, 100u);
  EXPECT_EQ(model.latency_band[0].p90_ns, 200u);
  EXPECT_EQ(model.latency_band[1].monotonic_ns, 2'000'000u);
  EXPECT_EQ(model.latency_band[1].p99_ns, 310u);

  ASSERT_EQ(model.counters.size(), 1u);
  const std::vector<CounterPoint>& series = model.counters.at("benchmark.http_2xx");
  ASSERT_EQ(series.size(), 2u);
  EXPECT_EQ(series[0].monotonic_ns, 1'000'000u);
  EXPECT_EQ(series[0].value, 7u);
  EXPECT_EQ(series[1].monotonic_ns, 2'000'000u);
  EXPECT_EQ(series[1].value, 9u);

  EXPECT_EQ(model.last_snapshot.monotonic_ns(), 2'000'000);
  EXPECT_EQ(model.run_state, "running");
}

TEST(RunViewModelTest, ApplySnapshotCapsSeriesAt600Points) {
  RunViewModel model;
  for (uint64_t i = 0; i < 610; ++i) {
    model = ApplySnapshot(std::move(model), snapshotAt(i * 1'000'000, 1, 2, 3, i));
  }
  // 600 points = 10 minutes at the default 1s flush; the oldest fall off.
  EXPECT_EQ(model.latency_band.size(), 600u);
  EXPECT_EQ(model.latency_band.front().monotonic_ns, 10'000'000u);
  EXPECT_EQ(model.latency_band.back().monotonic_ns, 609'000'000u);
  ASSERT_EQ(model.counters.size(), 1u);
  const std::vector<CounterPoint>& series = model.counters.at("benchmark.http_2xx");
  EXPECT_EQ(series.size(), 600u);
  EXPECT_EQ(series.front().value, 10u);
  EXPECT_EQ(series.back().value, 609u);
}

TEST(RunViewModelTest, ApplySnapshotSkipsBandWhenNoStatistics) {
  RunViewModel model;
  LiveSnapshot snapshot;
  snapshot.set_monotonic_ns(5'000);
  (*snapshot.mutable_counters())["benchmark.http_2xx"] = 1;
  model = ApplySnapshot(std::move(model), snapshot);
  EXPECT_TRUE(model.latency_band.empty());
  EXPECT_EQ(model.counters.at("benchmark.http_2xx").size(), 1u);
  EXPECT_EQ(model.last_snapshot.monotonic_ns(), 5'000);
}

TEST(RunViewModelTest, ApplySnapshotSkipsZeroCountStatistic) {
  // live.proto: count == 0 means no samples yet, so the percentile fields are
  // meaningless — the warm-up snapshot must leave a band gap, not paint zeros.
  RunViewModel model;
  LiveSnapshot warmup;
  warmup.set_monotonic_ns(1'000);
  StatisticSnapshot* stat = warmup.add_statistics();
  stat->set_id("benchmark.http_latency");
  stat->set_count(0);
  stat->set_p50_ns(999);
  stat->set_p90_ns(999);
  stat->set_p99_ns(999);
  model = ApplySnapshot(std::move(model), warmup);
  EXPECT_TRUE(model.latency_band.empty());

  model = ApplySnapshot(std::move(model), snapshotAt(2'000, 10, 20, 30, 1));
  ASSERT_EQ(model.latency_band.size(), 1u);
  EXPECT_EQ(model.latency_band[0].monotonic_ns, 2'000u);
  EXPECT_EQ(model.latency_band[0].p50_ns, 10u);
  EXPECT_EQ(model.last_snapshot.monotonic_ns(), 2'000);
}

TEST(RunViewModelTest, ApplySnapshotSelectsFirstSampledStatistic) {
  // The live aggregator's statistics order is unspecified (the per-worker
  // fold iterates a flat_hash_map), so a zero-count response-class statistic
  // frequently precedes the sampled one. The band must come from the first
  // statistic WITH samples, not from statistics[0] — under real traffic
  // statistics[0] is often latency_1xx with count==0 while latency_2xx
  // carries the samples.
  RunViewModel model;
  LiveSnapshot snapshot;
  snapshot.set_monotonic_ns(1'000);
  StatisticSnapshot* zero = snapshot.add_statistics();
  zero->set_id("benchmark_http_client.latency_1xx");
  zero->set_count(0);
  zero->set_p50_ns(999);
  zero->set_p90_ns(999);
  zero->set_p99_ns(999);
  StatisticSnapshot* sampled = snapshot.add_statistics();
  sampled->set_id("benchmark_http_client.latency_2xx");
  sampled->set_count(15);
  sampled->set_p50_ns(10);
  sampled->set_p90_ns(20);
  sampled->set_p99_ns(30);
  (*snapshot.mutable_counters())["benchmark.http_2xx"] = 15;

  model = ApplySnapshot(std::move(model), snapshot);
  ASSERT_EQ(model.latency_band.size(), 1u);
  EXPECT_EQ(model.latency_band[0].monotonic_ns, 1'000u);
  EXPECT_EQ(model.latency_band[0].p50_ns, 10u);
  EXPECT_EQ(model.latency_band[0].p90_ns, 20u);
  EXPECT_EQ(model.latency_band[0].p99_ns, 30u);
}

TEST(RunViewModelTest, ConcurrentStoreLoadStaysConsistent) {
  SnapshotStore store;
  std::atomic<bool> done{false};
  std::thread writer([&store, &done] {
    for (int i = 0; i < 1000; ++i) {
      RunViewModel model;
      model.run_state = "running";
      // The counter series carries the sequence number: every loaded model
      // must hold exactly one point whose value equals its own timestamp.
      model.counters["seq"].push_back(
          CounterPoint{static_cast<uint64_t>(i), static_cast<uint64_t>(i)});
      model.latency_band.push_back(PercentilePoint{static_cast<uint64_t>(i), 1, 2, 3});
      store.store(std::move(model));
    }
    done.store(true);
  });

  while (!done.load()) {
    std::shared_ptr<const RunViewModel> view = store.load();
    if (view != nullptr) {
      ASSERT_EQ(view->counters.size(), 1u);
      const std::vector<CounterPoint>& seq = view->counters.at("seq");
      ASSERT_EQ(seq.size(), 1u);
      EXPECT_EQ(seq[0].value, seq[0].monotonic_ns);
      ASSERT_EQ(view->latency_band.size(), 1u);
      EXPECT_EQ(view->latency_band[0].monotonic_ns, seq[0].monotonic_ns);
      EXPECT_EQ(view->run_state, "running");
    }
    std::this_thread::yield();
  }
  writer.join();

  std::shared_ptr<const RunViewModel> view = store.load();
  ASSERT_NE(view, nullptr);
  ASSERT_EQ(view->counters.at("seq").size(), 1u);
  EXPECT_EQ(view->counters.at("seq")[0].value, 999u);
}

TEST(RunViewModelTest, LoadOnFreshStoreReturnsNull) {
  SnapshotStore store;
  EXPECT_EQ(store.load(), nullptr);
}

TEST(RunViewModelTest, LoadReturnsStablePointerBetweenStores) {
  SnapshotStore store;
  RunViewModel model;
  model.run_state = "completed";
  model.counters["seq"].push_back(CounterPoint{1, 1});
  store.store(std::move(model));

  std::shared_ptr<const RunViewModel> first = store.load();
  std::shared_ptr<const RunViewModel> second = store.load();
  EXPECT_EQ(first, second);
  EXPECT_EQ(first->run_state, "completed");

  RunViewModel next;
  next.run_state = "idle";
  store.store(std::move(next));
  EXPECT_NE(store.load(), first);
  // The old model stays alive and unmutated behind the held pointer.
  EXPECT_EQ(first->run_state, "completed");
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
