#include <cstdint>
#include <fstream>
#include <ios>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "api/client/session.pb.h"
#include "source/client/tui/session_reader.h"
#include "source/client/tui/session_writer.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// The generated protos live in nighthawk::client; the C++ session API lives
// in Nighthawk::Client::Tui.
using nighthawk::client::MetricSeriesRecord;
using nighthawk::client::RunEndRecord;
using nighthawk::client::SessionHeader;
using nighthawk::client::SessionRecord;
using nighthawk::client::StartRequest;
using nighthawk::client::StatisticSnapshot;

// FORK-ONLY (We-Amp): session file format, writer/reader, and queue drop
// contract tests. The drop/close contract is the heart: drop-newest when the
// bounded queue is full, pushes after close() are counted drops, and the
// writer's authoritative dropped count always overwrites the caller's
// run_end.dropped_records at close.

std::string tempPath(const std::string& name) {
  const char* dir = ::getenv("TEST_TMPDIR");
  return absl::StrCat(dir == nullptr ? "/tmp" : dir, "/", name);
}

// Writes a complete valid session (header + 3 metric series + run end) and
// returns the data file path. The sidecar sits next to it with a ".jsonl"
// suffix.
std::string writeSampleSession(const std::string& name) {
  const std::string path = tempPath(name);
  SessionHeader header;
  header.set_format_version("1");
  header.set_session_id("sample");
  header.set_started_unix_ns(1'000);
  header.mutable_start_request()->mutable_options()->mutable_requests_per_second()->set_value(100);

  auto writer_or = SessionWriter::open(path, header);
  if (!writer_or.ok()) {
    ADD_FAILURE() << writer_or.status();
    return path;
  }
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);
  for (int i = 0; i < 3; ++i) {
    MetricSeriesRecord series;
    series.set_wall_time_unix_ns(2'000 + i);
    series.set_monotonic_ns(10 * i);
    series.set_phase_id("main");
    series.set_epoch_id("");
    (*series.mutable_counters())["requests"] = 10 * (i + 1);
    StatisticSnapshot* stat = series.add_statistics();
    stat->set_id("benchmark.http_latency");
    stat->set_count(10 * (i + 1));
    stat->set_p50_ns(100 + i);
    stat->set_p90_ns(200 + i);
    stat->set_p99_ns(300 + i);
    SessionRecord record;
    *record.mutable_metric_series() = std::move(series);
    writer->enqueue(std::move(record));
  }
  RunEndRecord run_end;
  run_end.set_completed(true);
  run_end.set_ended_unix_ns(3'000);
  run_end.set_dropped_records(0);
  auto status = writer->close(std::move(run_end));
  if (!status.ok()) {
    ADD_FAILURE() << status;
  }
  return path;
}

MetricSeriesRecord makeSeries(int64_t monotonic_ns, const std::string& counter_value) {
  MetricSeriesRecord series;
  series.set_wall_time_unix_ns(1);
  series.set_monotonic_ns(monotonic_ns);
  series.set_phase_id("main");
  series.set_epoch_id("");
  (*series.mutable_counters())["marker"] = monotonic_ns;
  series.add_statistics()->set_id(counter_value);
  return series;
}

TEST(TuiSessionTest, RoundTripPreservesRecords) {
  const std::string path = writeSampleSession("roundtrip.nhsession");

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  // Record 1: header, field by field.
  auto r1 = reader->next();
  ASSERT_TRUE(r1.ok()) << r1.status();
  ASSERT_TRUE(r1->has_value());
  EXPECT_TRUE((*r1)->has_header());
  EXPECT_EQ((*r1)->header().format_version(), "1");
  EXPECT_EQ((*r1)->header().session_id(), "sample");
  EXPECT_EQ((*r1)->header().started_unix_ns(), 1'000);
  EXPECT_EQ((*r1)->header().start_request().options().requests_per_second().value(), 100);

  // Records 2..4: metric series, in order, field by field.
  for (int i = 0; i < 3; ++i) {
    auto r = reader->next();
    ASSERT_TRUE(r.ok()) << r.status();
    ASSERT_TRUE(r->has_value());
    ASSERT_TRUE((*r)->has_metric_series());
    const MetricSeriesRecord& series = (*r)->metric_series();
    EXPECT_EQ(series.wall_time_unix_ns(), 2'000 + i);
    EXPECT_EQ(series.monotonic_ns(), 10 * i);
    EXPECT_EQ(series.phase_id(), "main");
    EXPECT_EQ(series.epoch_id(), "");
    ASSERT_EQ(series.counters().at("requests"), 10 * (i + 1));
    ASSERT_EQ(series.statistics_size(), 1);
    EXPECT_EQ(series.statistics(0).id(), "benchmark.http_latency");
    EXPECT_EQ(series.statistics(0).count(), 10 * (i + 1));
    EXPECT_EQ(series.statistics(0).p50_ns(), 100 + i);
    EXPECT_EQ(series.statistics(0).p90_ns(), 200 + i);
    EXPECT_EQ(series.statistics(0).p99_ns(), 300 + i);
  }

  // Record 5: run end.
  auto r5 = reader->next();
  ASSERT_TRUE(r5.ok()) << r5.status();
  ASSERT_TRUE(r5->has_value());
  ASSERT_TRUE((*r5)->has_run_end());
  EXPECT_TRUE((*r5)->run_end().completed());
  EXPECT_EQ((*r5)->run_end().ended_unix_ns(), 3'000);
  EXPECT_EQ((*r5)->run_end().dropped_records(), 0);

  // Then EOF.
  auto r6 = reader->next();
  ASSERT_TRUE(r6.ok()) << r6.status();
  EXPECT_FALSE(r6->has_value());
}

TEST(TuiSessionTest, IndexOffsetsMatchFrames) {
  const std::string path = writeSampleSession("index.nhsession");

  std::ifstream index_file(absl::StrCat(path, ".jsonl"));
  ASSERT_TRUE(index_file.is_open());
  std::vector<uint64_t> offsets;
  std::string line;
  while (std::getline(index_file, line)) {
    if (line.empty()) {
      continue;
    }
    // Format: {"byte_offset":<uint64>,"type":"...","monotonic_ns":<int64>}
    const size_t prefix = line.find("\"byte_offset\":");
    ASSERT_NE(prefix, std::string::npos) << line;
    const size_t start = prefix + strlen("\"byte_offset\":");
    const size_t end = line.find(',', start);
    ASSERT_NE(end, std::string::npos) << line;
    offsets.push_back(std::stoull(line.substr(start, end - start)));
  }
  ASSERT_EQ(offsets.size(), 5u);

  for (uint64_t offset : offsets) {
    std::ifstream data_file(path, std::ios::binary);
    ASSERT_TRUE(data_file.is_open());
    ASSERT_TRUE(data_file.seekg(offset)) << offset;
    // Varint32 length prefix.
    uint32_t length = 0;
    int shift = 0;
    for (;;) {
      char byte = 0;
      ASSERT_TRUE(data_file.get(byte)) << offset;
      length |= static_cast<uint32_t>(static_cast<uint8_t>(byte) & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        break;
      }
      shift += 7;
      ASSERT_LT(shift, 32);
    }
    std::string payload(length, '\0');
    data_file.read(payload.data(), payload.size());
    ASSERT_EQ(data_file.gcount(), static_cast<std::streamsize>(length)) << offset;
    SessionRecord record;
    ASSERT_TRUE(record.ParseFromString(payload)) << offset;
  }
}

TEST(TuiSessionTest, QueueDropsNewestWhenFull) {
  // Review Focus 3: the drop policy must be explicit and countable.
  SessionRecordQueue queue(3);
  for (int i = 0; i < 5; ++i) {
    SessionRecord record;
    record.mutable_metric_series()->set_monotonic_ns(i);
    queue.push(std::move(record));
  }
  EXPECT_EQ(queue.dropped(), 2);
  for (int i = 0; i < 3; ++i) {
    auto popped = queue.pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(popped->metric_series().monotonic_ns(), i);
  }
  queue.close();
  EXPECT_FALSE(queue.pop().has_value());
  EXPECT_EQ(queue.dropped(), 2);
}

TEST(TuiSessionTest, QueueCountsPostClosePushesAsDrops) {
  SessionRecordQueue queue(4);
  SessionRecord first;
  first.mutable_metric_series()->set_monotonic_ns(1);
  queue.push(std::move(first));
  SessionRecord second;
  second.mutable_metric_series()->set_monotonic_ns(2);
  queue.push(std::move(second));
  queue.close();

  // Pushes after close() are counted drops: never enqueued, never blocking.
  for (int i = 0; i < 3; ++i) {
    SessionRecord late;
    late.mutable_metric_series()->set_monotonic_ns(100 + i);
    queue.push(std::move(late));
  }
  EXPECT_EQ(queue.dropped(), 3);

  // close() drains already-queued records first: the two pre-close records
  // still come out, then EOF.
  auto popped = queue.pop();
  ASSERT_TRUE(popped.has_value());
  EXPECT_EQ(popped->metric_series().monotonic_ns(), 1);
  popped = queue.pop();
  ASSERT_TRUE(popped.has_value());
  EXPECT_EQ(popped->metric_series().monotonic_ns(), 2);
  EXPECT_FALSE(queue.pop().has_value());
  EXPECT_EQ(queue.dropped(), 3);
}

TEST(TuiSessionTest, WriterOverwritesCallerDroppedCount) {
  // A caller-passed dropped_records must not mask (or invent) queue drops:
  // the writer's authoritative count always wins at close.
  const std::string path = tempPath("caller-count.nhsession");
  SessionHeader header;
  header.set_format_version("1");
  header.set_session_id("caller-count");
  header.set_started_unix_ns(5'000);
  auto writer_or = SessionWriter::open(path, header);
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);
  for (int i = 0; i < 3; ++i) {
    SessionRecord record;
    *record.mutable_metric_series() = makeSeries(i, "x");
    writer->enqueue(std::move(record));
  }
  RunEndRecord run_end;
  run_end.set_completed(false);
  run_end.set_ended_unix_ns(6'000);
  run_end.set_dropped_records(42); // bogus: nothing was dropped
  ASSERT_TRUE(writer->close(std::move(run_end)).ok());
  EXPECT_EQ(writer->droppedCount(), 0);

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  ASSERT_TRUE(reader->next().ok());
  ASSERT_TRUE(reader->next().ok());
  ASSERT_TRUE(reader->next().ok());
  ASSERT_TRUE(reader->next().ok());
  auto last = reader->next();
  ASSERT_TRUE(last.ok()) << last.status();
  ASSERT_TRUE(last->has_value());
  ASSERT_TRUE((*last)->has_run_end());
  EXPECT_EQ((*last)->run_end().dropped_records(), 0);
  EXPECT_FALSE((*last)->run_end().completed());
}

TEST(TuiSessionTest, WriterMergesAuthoritativeDropCountIntoRunEnd) {
  // Conservation: every enqueued record is either written to the file or
  // counted as a queue drop, and the run end carries the writer's count.
  const std::string path = tempPath("merge-drops.nhsession");
  SessionHeader header;
  header.set_format_version("1");
  header.set_session_id("merge-drops");
  header.set_started_unix_ns(7'000);
  SessionWriterOptions options;
  options.queue_capacity = 64;
  auto writer_or = SessionWriter::open(path, header, options);
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);
  const int kEnqueued = 512;
  for (int i = 0; i < kEnqueued; ++i) {
    SessionRecord record;
    *record.mutable_metric_series() = makeSeries(i, "x");
    writer->enqueue(std::move(record));
  }
  RunEndRecord run_end;
  run_end.set_completed(true);
  run_end.set_ended_unix_ns(8'000);
  run_end.set_dropped_records(0); // caller claims none dropped
  ASSERT_TRUE(writer->close(std::move(run_end)).ok());
  const uint64_t dropped = writer->droppedCount();

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  uint64_t series_seen = 0;
  uint64_t run_end_dropped = 0;
  bool run_end_completed = false;
  bool header_seen = false;
  for (;;) {
    auto r = reader->next();
    ASSERT_TRUE(r.ok()) << r.status();
    if (!r->has_value()) {
      break;
    }
    switch ((*r)->record_case()) {
      case SessionRecord::kHeader:
        ASSERT_FALSE(header_seen) << "header appeared mid-stream";
        header_seen = true;
        break;
      case SessionRecord::kMetricSeries:
        ++series_seen;
        break;
      case SessionRecord::kRunEnd:
        run_end_completed = (*r)->run_end().completed();
        run_end_dropped = (*r)->run_end().dropped_records();
        break;
      case SessionRecord::RECORD_NOT_SET:
        ADD_FAILURE() << "unset record";
        break;
    }
  }
  EXPECT_EQ(series_seen + dropped, kEnqueued);
  EXPECT_EQ(run_end_dropped, dropped);
  EXPECT_TRUE(run_end_completed);
  EXPECT_TRUE(header_seen);
}

TEST(TuiSessionTest, ReaderRejectsTruncatedFrame) {
  const std::string path = writeSampleSession("truncated.nhsession");

  // Chop the last 3 bytes off the valid file.
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  ASSERT_TRUE(in.is_open());
  const std::streamoff size = in.tellg();
  std::string contents(static_cast<size_t>(size), '\0');
  in.seekg(0);
  in.read(contents.data(), size);
  ASSERT_EQ(in.gcount(), size);
  in.close();
  contents.resize(static_cast<size_t>(size) - 3);
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out.write(contents.data(), contents.size());
    ASSERT_TRUE(out.good());
  }

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  // The first 4 records read fine; the damaged 5th frame produces an error
  // naming the offset.
  for (int i = 0; i < 4; ++i) {
    auto r = reader->next();
    ASSERT_TRUE(r.ok()) << r.status();
    EXPECT_TRUE(r->has_value());
  }
  auto r5 = reader->next();
  ASSERT_FALSE(r5.ok());
  EXPECT_EQ(r5.status().code(), absl::StatusCode::kDataLoss);
  EXPECT_NE(r5.status().message().find("offset"), std::string::npos) << r5.status();
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
