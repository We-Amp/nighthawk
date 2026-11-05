// FORK-ONLY (We-Amp): the snapshot-acquisition-vs-slab-swap seam (M2 Task 4).
// The aggregator must never fold statistics across phase stamps: a partial is
// the immutable product of one worker dispatcher's slab, and the fold may only
// join partials that carry the same stamp.

#include <map>
#include <string>

#include "gtest/gtest.h"

#include "source/client/live_partial_aggregator.h"

namespace Nighthawk {
namespace Client {
namespace {

nighthawk::client::StatisticSnapshot makeSnapshot(const std::string& id, uint64_t count,
                                                  uint64_t p50, uint64_t p90, uint64_t p99) {
  nighthawk::client::StatisticSnapshot snapshot;
  snapshot.set_id(id);
  snapshot.set_count(count);
  snapshot.set_p50_ns(p50);
  snapshot.set_p90_ns(p90);
  snapshot.set_p99_ns(p99);
  return snapshot;
}

nighthawk::client::WorkerLiveSnapshotPartial
makePartial(uint32_t worker_id, const std::string& phase_id,
            const std::vector<nighthawk::client::StatisticSnapshot>& statistics,
            const std::map<std::string, uint64_t>& counters = {},
            const std::string& epoch_id = "") {
  nighthawk::client::WorkerLiveSnapshotPartial partial;
  partial.set_worker_id(worker_id);
  partial.set_phase_id(phase_id);
  partial.set_epoch_id(epoch_id);
  for (const auto& statistic : statistics) {
    *partial.add_statistics() = statistic;
  }
  for (const auto& [name, value] : counters) {
    (*partial.mutable_counters())[name] = value;
  }
  return partial;
}

const nighthawk::client::StatisticSnapshot*
findStatistic(const nighthawk::client::LiveSnapshot& snapshot, absl::string_view id) {
  for (const nighthawk::client::StatisticSnapshot& statistic : snapshot.statistics()) {
    if (statistic.id() == id) {
      return &statistic;
    }
  }
  return nullptr;
}

TEST(LivePartialAggregatorTest, EmptyStateDefaultsToMainPhase) {
  LivePartialAggregator aggregator;
  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("main", snapshot.phase_id());
  // M3 Task 6: the snapshot's epoch is the run's current knob epoch --
  // empty before the first applied change, never the execution id.
  EXPECT_EQ("", snapshot.epoch_id());
  EXPECT_FALSE(snapshot.epoch_changed());
  EXPECT_EQ(0, snapshot.statistics().size());
  EXPECT_EQ(0, snapshot.counters().size());
}

// The heart of the seam: two partials of the same worker taken across one
// boundary (one pre-swap, one post-swap) arrive in the same collector drain.
// The fold must expose only the post-boundary slab: no summation across the
// stamps, no torn read.
TEST(LivePartialAggregatorTest, FoldAcrossBoundaryNeverMixesSlabs) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("benchmark_http_client.latency_2xx", 100, 10, 20, 30)}));
  aggregator.addPartial(makePartial(0, "p2", {makeSnapshot("benchmark_http_client.latency_2xx", 10, 500, 600, 700)}));

  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("p2", snapshot.phase_id());
  const nighthawk::client::StatisticSnapshot* stat = findStatistic(snapshot, "benchmark_http_client.latency_2xx");
  ASSERT_NE(stat, nullptr);
  EXPECT_EQ(10, stat->count()); // NOT 110: the pre-boundary partial is never joined.
  EXPECT_EQ(500, stat->p50_ns());
  EXPECT_EQ(700, stat->p99_ns());
}

// Same-phase partials from different workers fold together (counts summed,
// percentiles max-over-workers), as in the legacy single-phase behavior.
TEST(LivePartialAggregatorTest, FoldWithinOnePhaseSumsCountsAcrossWorkers) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 100, 10, 20, 30)}));
  aggregator.addPartial(makePartial(1, "p1", {makeSnapshot("s", 40, 50, 60, 70)}));

  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("p1", snapshot.phase_id());
  const nighthawk::client::StatisticSnapshot* stat = findStatistic(snapshot, "s");
  ASSERT_NE(stat, nullptr);
  EXPECT_EQ(140, stat->count());
  EXPECT_EQ(50, stat->p50_ns());
  EXPECT_EQ(70, stat->p99_ns());
}

// A lagging worker whose latest partial is still from the previous phase
// contributes counters (cumulative from run start) but no statistics to the
// new phase's fold -- its stale slab is not joined with the fresh one.
TEST(LivePartialAggregatorTest, LaggingWorkerKeepsCountersButExcludesStaleStatistics) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 100, 10, 20, 30)},
                                    {{"benchmark.pool_overflow", 1}}));
  aggregator.addPartial(makePartial(1, "p1", {makeSnapshot("s", 100, 10, 20, 30)},
                                    {{"benchmark.pool_overflow", 2}}));
  // Worker 0 crossed the boundary; worker 1 has not flushed since.
  aggregator.addPartial(makePartial(0, "p2", {makeSnapshot("s", 10, 500, 600, 700)},
                                    {{"benchmark.pool_overflow", 4}}));

  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("p2", snapshot.phase_id());
  // Only worker 0's fresh slab folds.
  const nighthawk::client::StatisticSnapshot* stat = findStatistic(snapshot, "s");
  ASSERT_NE(stat, nullptr);
  EXPECT_EQ(10, stat->count());
  // Counters stay cumulative over all workers, across the stamp boundary
  // (worker 0's latest is 4, worker 1's latest is 2).
  EXPECT_EQ(6, snapshot.counters().at("benchmark.pool_overflow"));
}

// A straggler partial of an unchanged stamp must not drag the snapshot back
// to an earlier phase label.
TEST(LivePartialAggregatorTest, StragglerSameStampDoesNotRegressPhaseLabel) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 100, 10, 20, 30)}));
  aggregator.addPartial(makePartial(1, "p2", {makeSnapshot("s", 5, 100, 100, 100)}));
  // Worker 0 still flushing p1 after worker 1 advanced to p2.
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 120, 10, 20, 30)}));

  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("p2", snapshot.phase_id());
  const nighthawk::client::StatisticSnapshot* stat = findStatistic(snapshot, "s");
  ASSERT_NE(stat, nullptr);
  EXPECT_EQ(5, stat->count());
}

// Counts of the same phase are monotonic across successive drains.
TEST(LivePartialAggregatorTest, CountsAreMonotonicWithinAPhase) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 10, 1, 1, 1)}));
  const uint64_t first = aggregator.snapshot().statistics(0).count();
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 30, 1, 1, 1)}));
  const uint64_t second = aggregator.snapshot().statistics(0).count();
  EXPECT_LE(first, second);
  // And monotonic stamps survive consecutive snapshots.
  const nighthawk::client::LiveSnapshot a = aggregator.snapshot();
  const nighthawk::client::LiveSnapshot b = aggregator.snapshot();
  EXPECT_LT(a.monotonic_ns(), b.monotonic_ns());
}

// M2 Task 6 review fix: a REMOVED worker's first-ever fold partial carries
// its OLD stamp. Arriving AFTER a survivor's new-phase partial, it must not
// drag latest_phase_id_ backward -- the label would freeze the snapshot's
// percentiles for the rest of the run. The label advances only
// monotonically along the phase order; the removed worker stays excluded
// from the statistics fold (its stamp lags), and its cumulative counters
// still count.
TEST(LivePartialAggregatorTest, FoldPartialCannotDragLatestBackward) {
  LivePartialAggregator aggregator;
  aggregator.setPhaseOrder({"p1", "p2"});
  // The survivor crossed the boundary and flushed in p2.
  aggregator.addPartial(makePartial(1, "p2", {makeSnapshot("s", 5, 100, 100, 100)},
                                    {{"benchmark.http_2xx", 500}}));
  // The removed worker's fold partial (its first and last) arrives after,
  // stamped with the phase it was removed in.
  aggregator.addPartial(makePartial(0, "p1", {makeSnapshot("s", 100, 10, 20, 30)},
                                    {{"benchmark.http_2xx", 300}}));

  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("p2", snapshot.phase_id()) << "fold partial dragged the phase label backward";
  // Statistics fold only from the p2-stamped worker.
  const nighthawk::client::StatisticSnapshot* stat = findStatistic(snapshot, "s");
  ASSERT_NE(stat, nullptr);
  EXPECT_EQ(5, stat->count());
  // Counters are unaffected: cumulative over all workers regardless of stamp.
  EXPECT_EQ(800, snapshot.counters().at("benchmark.http_2xx"));
}

// --- M3 Task 6: knob-epoch stamping + flush-window change markers (spec 8.5) ---

// First-applied wins: the label advances as soon as the first worker's
// post-bend partial carries the new epoch, marks exactly ONE change, and
// stays put afterwards.
TEST(LivePartialAggregatorTest, EpochAdvancesOnFirstStampWithSingleMarker) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {}, {}, ""));
  aggregator.addPartial(makePartial(1, "p1", {}, {}, ""));
  nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("", snapshot.epoch_id());
  EXPECT_FALSE(snapshot.epoch_changed());

  // The bend: worker 0's next partial carries epoch-1 (worker 1's is still
  // the outgoing epoch -- a partial computed within the bend lead window).
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-1"));
  snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-1", snapshot.epoch_id());
  EXPECT_TRUE(snapshot.epoch_changed()) << "exactly one marker per epoch change";

  // Subsequent snapshots carry the new epoch without a marker.
  snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-1", snapshot.epoch_id());
  EXPECT_FALSE(snapshot.epoch_changed());

  // Worker 1 catches up: no second marker.
  aggregator.addPartial(makePartial(1, "p1", {}, {}, "epoch-1"));
  snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-1", snapshot.epoch_id());
  EXPECT_FALSE(snapshot.epoch_changed());
}

// A rolled-back update demotes the label back to the prior epoch -- but only
// once NO worker is still stamped at the demoted epoch (the last
// epoch-stamped worker's post-rollback partial), and that demotion is
// itself one marked change.
TEST(LivePartialAggregatorTest, RollbackDemotesLabelAfterLastStampsPrior) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-1"));
  aggregator.addPartial(makePartial(1, "p1", {}, {}, "epoch-1"));
  aggregator.snapshot(); // consume the epoch-1 marker

  // A second update applies (epoch-2) on both workers.
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-2"));
  nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-2", snapshot.epoch_id());
  EXPECT_TRUE(snapshot.epoch_changed());
  aggregator.addPartial(makePartial(1, "p1", {}, {}, "epoch-2"));
  aggregator.snapshot(); // consume the epoch-2 marker

  // The update fails and rolls back. The rollback re-apply re-stamps worker
  // 0 to the PRIOR epoch; worker 1 still carries epoch-2 (its rollback has
  // not applied yet), so the label must not move yet.
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-1"));
  snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-2", snapshot.epoch_id());
  EXPECT_FALSE(snapshot.epoch_changed());

  // The last epoch-2-stamped worker's post-rollback partial: the demotion.
  aggregator.addPartial(makePartial(1, "p1", {}, {}, "epoch-1"));
  snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-1", snapshot.epoch_id());
  EXPECT_TRUE(snapshot.epoch_changed()) << "the rollback is one marked change";
}

// Retired/idle workers frozen on the empty epoch never block an advance and
// never block a rollback demotion.
TEST(LivePartialAggregatorTest, RetiredWorkerNeverBlocksEpochTransitions) {
  LivePartialAggregator aggregator;
  // Worker 0 is retired: one fold partial, frozen on the empty epoch.
  aggregator.addPartial(makePartial(0, "p1", {}, {}, ""));
  aggregator.addPartial(makePartial(1, "p1", {}, {}, "epoch-1"));
  nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-1", snapshot.epoch_id()) << "the retired worker's empty stamp must not "
                                              "block the advance";
  EXPECT_TRUE(snapshot.epoch_changed());
  aggregator.snapshot(); // consume the marker

  // The active worker rolls back to the empty epoch; the retired worker's
  // frozen empty stamp does not block the demotion either.
  aggregator.addPartial(makePartial(1, "p1", {}, {}, ""));
  snapshot = aggregator.snapshot();
  EXPECT_EQ("", snapshot.epoch_id());
  EXPECT_TRUE(snapshot.epoch_changed());
}

// Epoch ids order NUMERICALLY, never lexicographically ("epoch-10" is
// later than "epoch-9").
TEST(LivePartialAggregatorTest, EpochIdsOrderNumerically) {
  LivePartialAggregator aggregator;
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-9"));
  EXPECT_EQ("epoch-9", aggregator.snapshot().epoch_id());
  aggregator.addPartial(makePartial(0, "p1", {}, {}, "epoch-10"));
  const nighthawk::client::LiveSnapshot snapshot = aggregator.snapshot();
  EXPECT_EQ("epoch-10", snapshot.epoch_id());
  EXPECT_TRUE(snapshot.epoch_changed());
}


} // namespace
} // namespace Client
} // namespace Nighthawk
