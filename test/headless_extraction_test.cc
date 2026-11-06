// FORK-ONLY (We-Amp): the headless extraction contract pins (MCP plan Task 0).
// The three extracted pieces — SessionRecorder (session file owner layered on
// the moved SessionWriter), FoldTracker (the window lifecycle state machine),
// and SeriesView (phase/epoch-stamped series access with the first-snapshot
// wall join) — pinned headlessly, before the TUI re-consumes them. Plain
// cc_test for the same quarantine reason as the tui_* tests: client_headless
// carries no Envoy C++ sources and no test-helper stack.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/container/flat_hash_map.h"
#include "api/client/session.pb.h"

#include "source/client/headless/fold_tracker.h"
#include "source/client/headless/series_view.h"
#include "source/client/headless/session_reader.h"
#include "source/client/headless/session_recorder.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

int64_t nowUnixNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// A distinct scratch prefix per test (the file naming stamps unix ns, so
// concurrent test runs cannot collide).
std::string TempPrefix() {
  static std::atomic<int64_t> seq{0};
  return absl::StrCat("/tmp/nh-headless-test-", nowUnixNs(), "-", seq.fetch_add(1));
}

inline constexpr char kTestSessionFormatVersion[] = "1";

nighthawk::client::SessionHeader TestHeader() {
  nighthawk::client::SessionHeader header;
  header.set_format_version(kTestSessionFormatVersion);
  header.set_started_unix_ns(1'000);
  return header;
}

nighthawk::client::SessionRecord SeriesRecord(int64_t monotonic_ns, uint64_t value_2xx,
                                              uint64_t sut_ticks) {
  nighthawk::client::SessionRecord record;
  auto& series = *record.mutable_metric_series();
  series.set_monotonic_ns(monotonic_ns);
  series.set_phase_id("main");
  (*series.mutable_counters())["benchmark.http_2xx"] = value_2xx;
  (*series.mutable_counters())["sut.cpu_ticks"] = sut_ticks;
  return record;
}

// --- SessionRecorder ---------------------------------------------------------

TEST(SessionRecorderTest, StartRunRoundTripsHeaderSnapshotsAndRunEnd) {
  SessionRecorder recorder;
  const std::string prefix = TempPrefix();
  const int64_t started_unix_ns = nowUnixNs();
  auto path_or = recorder.StartRun(prefix, started_unix_ns, TestHeader());
  ASSERT_TRUE(path_or.ok()) << path_or.status();
  // The naming contract: "<prefix>-<unix_ns>.nhsession".
  const std::string path = absl::StrCat(prefix, "-", started_unix_ns, ".nhsession");
  EXPECT_EQ(*path_or, path);
  EXPECT_TRUE(recorder.open());
  EXPECT_EQ(recorder.path(), path);

  nighthawk::client::SessionRecord first = SeriesRecord(100, 10, 5);
  first.mutable_metric_series()->set_wall_time_unix_ns(1'100);
  nighthawk::client::SessionRecord second = SeriesRecord(200, 20, 7);
  second.mutable_metric_series()->set_wall_time_unix_ns(1'200);
  recorder.AppendSnapshot(first);
  recorder.AppendSnapshot(second);

  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(true);
  run_end.set_ended_unix_ns(2'000);
  EXPECT_TRUE(recorder.FlushAndSeal(run_end).ok());
  EXPECT_FALSE(recorder.open());
  // The sealed session's path stays queryable (sealed-file reads); open()
  // is the open/closed discriminator.
  EXPECT_EQ(recorder.path(), path);

  // Round-trip through the reader: header, both snapshots (in order), run end.
  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::vector<nighthawk::client::SessionRecord> records;
  for (;;) {
    auto next = (*reader_or)->next();
    ASSERT_TRUE(next.ok()) << next.status();
    if (!next->has_value()) {
      break;
    }
    records.push_back(**next);
  }
  ASSERT_EQ(records.size(), 4u);
  ASSERT_TRUE(records[0].has_header());
  EXPECT_EQ(records[0].header().format_version(), kTestSessionFormatVersion);
  // The recorder stamps the header's session id with the concrete path.
  EXPECT_EQ(records[0].header().session_id(), path);
  ASSERT_TRUE(records[1].has_metric_series());
  EXPECT_EQ(records[1].metric_series().monotonic_ns(), 100);
  EXPECT_EQ(records[1].metric_series().counters().at("benchmark.http_2xx"), 10u);
  ASSERT_TRUE(records[2].has_metric_series());
  EXPECT_EQ(records[2].metric_series().monotonic_ns(), 200);
  ASSERT_TRUE(records[3].has_run_end());
  EXPECT_TRUE(records[3].run_end().completed());
  EXPECT_EQ(records[3].run_end().ended_unix_ns(), 2'000);
}

TEST(SessionRecorderTest, StartRunRefusedWhileOpen) {
  SessionRecorder recorder;
  ASSERT_TRUE(recorder.StartRun(TempPrefix(), nowUnixNs(), TestHeader()).ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(
      recorder.StartRun(TempPrefix(), nowUnixNs(), TestHeader()).status()));
}

TEST(SessionRecorderTest, FlushAndSealSingleCall) {
  SessionRecorder recorder;
  auto path_or = recorder.StartRun(TempPrefix(), nowUnixNs(), TestHeader());
  ASSERT_TRUE(path_or.ok()) << path_or.status();
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(false);
  ASSERT_TRUE(recorder.FlushAndSeal(run_end).ok());
  // A second seal after the first is a no-op success (the single-call
  // discipline: the session is already sealed, the file carries one run end).
  EXPECT_TRUE(recorder.FlushAndSeal(run_end).ok());
  auto reader_or = SessionReader::open(*path_or);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  int run_ends = 0;
  for (;;) {
    auto next = (*reader_or)->next();
    ASSERT_TRUE(next.ok()) << next.status();
    if (!next->has_value()) {
      break;
    }
    if ((*next)->has_run_end()) {
      ++run_ends;
    }
  }
  EXPECT_EQ(run_ends, 1);
}

TEST(SessionRecorderTest, FlushAndSealWithoutStartIsOk) {
  SessionRecorder recorder;
  nighthawk::client::RunEndRecord run_end;
  EXPECT_TRUE(recorder.FlushAndSeal(run_end).ok());
}

TEST(SessionRecorderTest, ReadSealedPrefixTornTailSkipped) {
  SessionRecorder recorder;
  auto path_or = recorder.StartRun(TempPrefix(), nowUnixNs(), TestHeader());
  ASSERT_TRUE(path_or.ok()) << path_or.status();
  nighthawk::client::SessionRecord first = SeriesRecord(100, 10, 5);
  recorder.AppendSnapshot(first);
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(true);
  ASSERT_TRUE(recorder.FlushAndSeal(run_end).ok());

  // A crash mid-write left a torn tail: a varint length prefix claiming more
  // bytes than remain, then the stream ends. The sealed prefix must come
  // back untouched, with the tear disclosed — never silently skipped, never
  // surfaced as a parse error.
  {
    std::ofstream torn(*path_or, std::ios::binary | std::ios::app);
    torn << static_cast<char>(0x85);
    torn << "torn!";
  }

  auto prefix_or = recorder.ReadSealedPrefix();
  ASSERT_TRUE(prefix_or.ok()) << prefix_or.status();
  EXPECT_TRUE(prefix_or->tail_skipped);
  EXPECT_FALSE(prefix_or->tail_reason.empty());
  ASSERT_EQ(prefix_or->records.size(), 3u);
  EXPECT_TRUE(prefix_or->records[0].has_header());
  ASSERT_TRUE(prefix_or->records[1].has_metric_series());
  EXPECT_EQ(prefix_or->records[1].metric_series().monotonic_ns(), 100);
  EXPECT_TRUE(prefix_or->records[2].has_run_end());
}

TEST(SessionRecorderTest, ReadSealedPrefixCleanFileHasNoSkip) {
  SessionRecorder recorder;
  auto path_or = recorder.StartRun(TempPrefix(), nowUnixNs(), TestHeader());
  ASSERT_TRUE(path_or.ok()) << path_or.status();
  recorder.AppendSnapshot(SeriesRecord(100, 10, 5));
  nighthawk::client::RunEndRecord run_end;
  ASSERT_TRUE(recorder.FlushAndSeal(run_end).ok());

  auto prefix_or = recorder.ReadSealedPrefix();
  ASSERT_TRUE(prefix_or.ok()) << prefix_or.status();
  EXPECT_FALSE(prefix_or->tail_skipped);
  EXPECT_TRUE(prefix_or->tail_reason.empty());
  EXPECT_EQ(prefix_or->records.size(), 3u); // header + snapshot + run end
}

TEST(SessionRecorderTest, ReadSealedPrefixWithoutSessionRefused) {
  SessionRecorder recorder;
  EXPECT_TRUE(absl::IsFailedPrecondition(recorder.ReadSealedPrefix().status()));
}

// --- FoldTracker -------------------------------------------------------------

TEST(FoldTrackerTest, IdsAreServerAssignedMonotonic) {
  FoldTracker tracker;
  auto first = tracker.Stage();
  ASSERT_TRUE(first.ok()) << first.status();
  EXPECT_EQ(*first, 1);
  auto second_id = tracker.Stage(); // single-flight: 1 is still staged
  EXPECT_TRUE(absl::IsFailedPrecondition(second_id.status()));
  EXPECT_TRUE(tracker.MarkFolding(*first).ok());
  auto third = tracker.Stage(); // single-flight: 1 is folding
  EXPECT_TRUE(absl::IsFailedPrecondition(third.status()));
  EXPECT_TRUE(tracker.MarkReady(*first).ok());
  auto fourth = tracker.Stage(); // 1 is terminal now: the next id
  ASSERT_TRUE(fourth.ok()) << fourth.status();
  EXPECT_EQ(*fourth, 2);
}

TEST(FoldTrackerTest, LifecycleTransitionsPinned) {
  FoldTracker tracker;
  auto id = tracker.Stage();
  ASSERT_TRUE(id.ok());
  auto status = tracker.Status(*id);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(status->state, FoldTracker::State::kStaged);
  // staged -> ready skips the folding state: refused.
  EXPECT_TRUE(absl::IsFailedPrecondition(tracker.MarkReady(*id)));
  EXPECT_TRUE(tracker.MarkFolding(*id).ok());
  EXPECT_EQ(tracker.Status(*id)->state, FoldTracker::State::kFolding);
  EXPECT_TRUE(tracker.MarkReady(*id, "window landed").ok());
  auto done = tracker.Status(*id);
  ASSERT_TRUE(done.ok());
  EXPECT_EQ(done->state, FoldTracker::State::kReady);
  EXPECT_EQ(done->note, "window landed");
  // Terminal: no further transitions.
  EXPECT_TRUE(absl::IsFailedPrecondition(tracker.MarkFolding(*id)));
  EXPECT_TRUE(absl::IsFailedPrecondition(tracker.MarkFailed(*id, "late")));
}

TEST(FoldTrackerTest, FailureCarriesReasonFromStagedOrFolding) {
  FoldTracker tracker;
  auto staged = tracker.Stage();
  ASSERT_TRUE(staged.ok());
  EXPECT_TRUE(tracker.MarkFailed(*staged, "collector arm failed").ok());
  auto status = tracker.Status(*staged);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(status->state, FoldTracker::State::kFailed);
  EXPECT_EQ(status->failure_reason, "collector arm failed");

  auto folding = tracker.Stage();
  ASSERT_TRUE(folding.ok());
  EXPECT_TRUE(tracker.MarkFolding(*folding).ok());
  EXPECT_TRUE(tracker.MarkFailed(*folding, "profile unreadable").ok());
  EXPECT_EQ(tracker.Status(*folding)->state, FoldTracker::State::kFailed);
  EXPECT_EQ(tracker.Status(*folding)->failure_reason, "profile unreadable");
}

TEST(FoldTrackerTest, UnknownIdRefused) {
  FoldTracker tracker;
  EXPECT_TRUE(absl::IsNotFound(tracker.Status(42).status()));
  EXPECT_TRUE(absl::IsNotFound(tracker.MarkFolding(42)));
}

TEST(FoldTrackerTest, BusyAndOpenQueries) {
  FoldTracker tracker;
  EXPECT_FALSE(tracker.busy());
  auto id = tracker.Stage();
  ASSERT_TRUE(id.ok());
  EXPECT_TRUE(tracker.busy());
  EXPECT_EQ(tracker.open_id(), *id);
  EXPECT_EQ(tracker.folding_id(), 0);
  EXPECT_TRUE(tracker.MarkFolding(*id).ok());
  EXPECT_EQ(tracker.open_id(), 0);
  EXPECT_EQ(tracker.folding_id(), *id);
  EXPECT_TRUE(tracker.MarkReady(*id).ok());
  EXPECT_FALSE(tracker.busy());
}

// --- SeriesView --------------------------------------------------------------

// Two snapshots from one run: the first carries the anchor pair, the second
// carries a DIFFERENT wall stamp than its anchor-projected value — proving
// the wall join is the anchor projection, not a passthrough of the record's
// own wall field.
std::vector<nighthawk::client::MetricSeriesRecord> TestRecords() {
  nighthawk::client::MetricSeriesRecord first;
  first.set_wall_time_unix_ns(1'000);
  first.set_monotonic_ns(10'000);
  first.set_phase_id("main");
  first.set_epoch_id("");
  (*first.mutable_counters())["benchmark.http_2xx"] = 10;
  (*first.mutable_counters())["sut.cpu_ticks"] = 5;

  nighthawk::client::MetricSeriesRecord second;
  second.set_wall_time_unix_ns(999'000); // deliberately NOT the joined value
  second.set_monotonic_ns(12'000);
  second.set_phase_id("phase-b");
  second.set_epoch_id("epoch-1");
  (*second.mutable_counters())["benchmark.http_2xx"] = 20;
  (*second.mutable_counters())["sut.cpu_ticks"] = 7;

  return {first, second};
}

TEST(SeriesViewTest, PointsCarryMonotonicWallJoinPhaseAndEpoch) {
  SeriesView view(TestRecords());
  auto series = view.Series("benchmark.http_2xx");
  ASSERT_TRUE(series.ok()) << series.status();
  ASSERT_EQ(series->size(), 2u);
  EXPECT_EQ((*series)[0].monotonic_ns, 10'000);
  EXPECT_EQ((*series)[0].value, 10u);
  EXPECT_EQ((*series)[0].wall_time_unix_ns, 1'000);
  EXPECT_EQ((*series)[0].phase_id, "main");
  EXPECT_EQ((*series)[0].epoch_id, "");
  // The second point's wall is JOINED through the first-snapshot anchor:
  // monotonic 12'000 + (1'000 - 10'000) = 3'000. The record's own (bogus)
  // wall field must NOT leak through.
  EXPECT_EQ((*series)[1].monotonic_ns, 12'000);
  EXPECT_EQ((*series)[1].wall_time_unix_ns, 3'000);
  EXPECT_NE((*series)[1].wall_time_unix_ns, 999'000);
  EXPECT_EQ((*series)[1].phase_id, "phase-b");
  EXPECT_EQ((*series)[1].epoch_id, "epoch-1");
}

TEST(SeriesViewTest, SutSeriesIncludedAlongsideClientSeries) {
  SeriesView view(TestRecords());
  auto names = view.SeriesNames();
  ASSERT_EQ(names.size(), 2u);
  EXPECT_EQ(names[0], "benchmark.http_2xx");
  EXPECT_EQ(names[1], "sut.cpu_ticks");
  auto sut = view.Series("sut.cpu_ticks");
  ASSERT_TRUE(sut.ok()) << sut.status();
  ASSERT_EQ(sut->size(), 2u);
  EXPECT_EQ((*sut)[1].value, 7u);
}

TEST(SeriesViewTest, UnknownSeriesIsNotFound) {
  SeriesView view(TestRecords());
  EXPECT_TRUE(absl::IsNotFound(view.Series("no.such_series").status()));
}

TEST(SeriesViewTest, AnchorIsFirstSnapshotPair) {
  SeriesView view(TestRecords());
  EXPECT_TRUE(view.anchor().valid());
  EXPECT_EQ(view.anchor().wall_time_unix_ns, 1'000);
  EXPECT_EQ(view.anchor().monotonic_ns, 10'000);
}

TEST(SeriesViewTest, EmptyViewIsInvalidAnchorAndNoSeries) {
  SeriesView view({});
  EXPECT_FALSE(view.anchor().valid());
  EXPECT_TRUE(view.SeriesNames().empty());
}

// The latency band's records: the FIRST flush models the unspecified
// statistics order (statistics[0] is a zero-count response-class statistic,
// the sampled latency statistic sits elsewhere), the SECOND carries the
// bend's epoch stamp and a post-bend latency shape. The second record's wall
// stamp is deliberately NOT the anchor-projected value (the join pin).
std::vector<nighthawk::client::MetricSeriesRecord> LatencyRecords() {
  nighthawk::client::MetricSeriesRecord first;
  first.set_wall_time_unix_ns(1'000);
  first.set_monotonic_ns(10'000);
  first.set_phase_id("warm");
  first.set_epoch_id("");
  auto* zero = first.add_statistics();
  zero->set_id("benchmark.latency_1xx"); // count 0: percentiles meaningless
  auto* sampled = first.add_statistics();
  sampled->set_id("benchmark.latency_2xx");
  sampled->set_count(7);
  sampled->set_p50_ns(1'000);
  sampled->set_p90_ns(2'000);
  sampled->set_p99_ns(3'000);
  (*first.mutable_counters())["benchmark.http_2xx"] = 10;

  nighthawk::client::MetricSeriesRecord second;
  second.set_wall_time_unix_ns(999'000); // deliberately NOT the joined value
  second.set_monotonic_ns(12'000);
  second.set_phase_id("spike");
  second.set_epoch_id("epoch-1");
  auto* post = second.add_statistics();
  post->set_id("benchmark.latency_2xx");
  post->set_count(9);
  post->set_p50_ns(1'100);
  post->set_p90_ns(2'200);
  post->set_p99_ns(9'000);

  return {first, second};
}

// The latency band seam (the dogfood gap): MetricSeriesRecord.statistics is
// the same data ApplySnapshot's band consumes — per flush record, the FIRST
// statistic with samples (the zero-count scan rule), its p50/p90/p99 triple
// stamped phase+epoch and anchor-joined wall, exactly like the counters.
TEST(SeriesViewTest, LatencyBandUsesFirstSampledStatisticWithStamps) {
  SeriesView view(LatencyRecords());
  auto band = view.LatencyBand();
  ASSERT_TRUE(band.ok()) << band.status();
  ASSERT_EQ(band->size(), 2u);
  EXPECT_EQ((*band)[0].monotonic_ns, 10'000);
  EXPECT_EQ((*band)[0].wall_time_unix_ns, 1'000);
  EXPECT_EQ((*band)[0].p50_ns, 1'000u);
  EXPECT_EQ((*band)[0].p90_ns, 2'000u);
  EXPECT_EQ((*band)[0].p99_ns, 3'000u);
  EXPECT_EQ((*band)[0].phase_id, "warm");
  EXPECT_EQ((*band)[0].epoch_id, "");
  // The second point's wall is JOINED through the first-snapshot anchor
  // (12'000 + (1'000 - 10'000) = 3'000); the record's own wall field does
  // not leak through.
  EXPECT_EQ((*band)[1].monotonic_ns, 12'000);
  EXPECT_EQ((*band)[1].wall_time_unix_ns, 3'000);
  EXPECT_NE((*band)[1].wall_time_unix_ns, 999'000);
  EXPECT_EQ((*band)[1].phase_id, "spike");
  EXPECT_EQ((*band)[1].epoch_id, "epoch-1");
  EXPECT_EQ((*band)[1].p50_ns, 1'100u);
  EXPECT_EQ((*band)[1].p90_ns, 2'200u);
  EXPECT_EQ((*band)[1].p99_ns, 9'000u);
}

// A flush whose every statistic has count == 0 is a band GAP (the TUI's
// zero-count rule): the band carries only the sampled flushes, never zeros
// painted at the chart's bottom.
TEST(SeriesViewTest, LatencyBandGapsOnZeroCountFlush) {
  auto records = LatencyRecords();
  nighthawk::client::MetricSeriesRecord dry;
  dry.set_wall_time_unix_ns(2'000);
  dry.set_monotonic_ns(11'000);
  dry.set_phase_id("warm");
  dry.add_statistics()->set_id("benchmark.latency_1xx"); // count 0
  records.insert(records.begin() + 1, dry);
  SeriesView view(records);
  auto band = view.LatencyBand();
  ASSERT_TRUE(band.ok()) << band.status();
  ASSERT_EQ(band->size(), 2u);
  EXPECT_EQ((*band)[0].monotonic_ns, 10'000);
  EXPECT_EQ((*band)[1].monotonic_ns, 12'000);
}

// No sampled statistic anywhere: an honest empty band, not a fault.
TEST(SeriesViewTest, LatencyBandEmptyWithoutSampledStatistics) {
  nighthawk::client::MetricSeriesRecord dry;
  dry.set_wall_time_unix_ns(1'000);
  dry.set_monotonic_ns(10'000);
  dry.set_phase_id("main");
  dry.add_statistics()->set_id("benchmark.latency_1xx"); // count 0
  SeriesView view({dry});
  auto band = view.LatencyBand();
  ASSERT_TRUE(band.ok()) << band.status();
  EXPECT_TRUE(band->empty());
}

// The moved completed-work merge keeps its M0b semantics after the extraction
// (the union-stamp last-value-at-or-before sum the §8.4 denominator uses).
TEST(SeriesViewTest, MergeCompletedSeriesSemanticsUnchanged) {
  absl::flat_hash_map<std::string, std::vector<CounterPoint>> counters;
  counters["benchmark.http_2xx"] = {{100, 10}, {300, 30}};
  counters["benchmark.http_5xx"] = {{200, 2}};
  auto merged = MergeCompletedSeries(counters);
  ASSERT_EQ(merged.size(), 3u);
  EXPECT_EQ(merged[0].monotonic_ns, 100u);
  EXPECT_EQ(merged[0].value, 10u);
  EXPECT_EQ(merged[1].monotonic_ns, 200u);
  EXPECT_EQ(merged[1].value, 12u); // 30 carried at-or-before + 2
  EXPECT_EQ(merged[2].monotonic_ns, 300u);
  EXPECT_EQ(merged[2].value, 32u);
  // SUT series are NOT part of the completed-work merge.
  counters["sut.cpu_ticks"] = {{100, 5}};
  merged = MergeCompletedSeries(counters);
  EXPECT_EQ(merged.size(), 3u);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
