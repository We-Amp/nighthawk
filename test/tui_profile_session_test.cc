// FORK-ONLY (We-Amp): profile-window session recording tests (M1 plan task
// 6). Plain cc_test (see BUILD comment): the TUI quarantine keeps this target
// off Envoy's C++ sources and test helpers.
//
// Covers the M1 session gate (spec §4 session recording / §5 M1 row):
// - ProfileWindow -> ProfileWindowRecord conversion, field for field;
// - the record round-trips through the M0b writer/reader untouched;
// - an M0b-era record set still parses fully with the NEW reader (the
//   forward direction of the documented forward-compat asymmetry: new reads
//   old; an old reader hitting a profile_window frame fails at the parked
//   RECORD_NOT_SET rejection — never a silent skip).

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>

#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"

#include "absl/strings/str_cat.h"
#include "api/client/session.pb.h"
#include "source/client/tui/profile_session_conversion.h"
#include "source/client/tui/profile_windows.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/session_reader.h"
#include "source/client/tui/session_writer.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using nighthawk::client::MetricSeriesRecord;
using nighthawk::client::ProfileWindowRecord;
using nighthawk::client::RunEndRecord;
using nighthawk::client::SessionHeader;
using nighthawk::client::SessionRecord;
using nighthawk::client::StartRequest;

std::string tempPath(const std::string& name) {
  const char* dir = ::getenv("TEST_TMPDIR");
  return absl::StrCat(dir == nullptr ? "/tmp" : dir, "/", name);
}

// A minimal but fully-populated window: a tiny folded tree, a contained
// interval, pure, symbolized, with real work units.
ProfileWindow makeWindow() {
  ProfileWindow w;
  w.tree.total_samples = 3;
  w.tree.total_value = 630000000;
  w.tree.value_unit = "cpu-nanoseconds";
  w.tree.period_nanos = 10000000;
  // Legacy gperftools conversion: no profile timestamps (the fetch
  // orchestration is the interval source — the live M1 path).
  w.tree.time_nanos = 0;
  w.tree.duration_nanos = 0;
  w.tree.symbolized = true;
  FoldedFrame root;
  root.function = "main";
  root.total_samples = 3;
  root.total_value = 630000000;
  root.symbolized = true;
  w.tree.root.push_back(root);
  w.start_unix_ns = 1'000'000'000;
  w.end_unix_ns = 6'000'000'000;
  w.purity_fraction = 1.0;
  w.phase_id = "main";
  w.worker_count = 4;
  w.work_units = 1234;
  w.profile_type = "cpu";
  w.symbolized = true;
  w.interval_source = WindowIntervalSource::kFetchOrchestration;
  w.clock_offset_uncertainty_ns = 0;
  return w;
}

// The M0b-era record set (header + metric series + run end), regenerated with
// the same shapes the M0b session test used — the old-format fixture for the
// forward-compat asymmetry.
SessionRecord makeM0bHeader() {
  StartRequest request;
  request.mutable_options()->mutable_uri()->set_value("http://127.0.0.1:10000/");
  SessionRecord record;
  *record.mutable_header() =
      ToSessionHeader(request, /*session_id=*/"m0b-shape", /*started_unix_ns=*/42);
  return record;
}

SessionRecord makeM0bMetricSeries() {
  MetricSeriesRecord series;
  series.set_wall_time_unix_ns(1'000);
  series.set_monotonic_ns(2'000);
  series.set_phase_id("main");
  series.set_epoch_id("");
  (*series.mutable_counters())["benchmark.http_2xx"] = 42;
  SessionRecord record;
  *record.mutable_metric_series() = series;
  return record;
}

TEST(ProfileSessionConversionTest, ToProfileWindowRecordCarriesAllFields) {
  const ProfileWindow w = makeWindow();
  const std::string raw = std::string("\x01\x00\x00\x0d", 4) + "fake-pprof-bytes";
  const ProfileWindowRecord record = ToProfileWindowRecord(w, raw, "go tool pprof");

  EXPECT_EQ(record.profile_type(), "cpu");
  EXPECT_EQ(record.start_unix_ns(), 1'000'000'000);
  EXPECT_EQ(record.end_unix_ns(), 6'000'000'000);
  EXPECT_DOUBLE_EQ(record.purity_fraction(), 1.0);
  EXPECT_EQ(record.phase_id(), "main");
  EXPECT_EQ(record.worker_count(), 4u);
  EXPECT_EQ(record.work_units(), 1234u);
  EXPECT_TRUE(record.symbolized());
  // Provenance: the --pprof-tool argv prefix rides along.
  EXPECT_EQ(record.symbolizer_tool(), "go tool pprof");
  // The raw staged bytes land verbatim: the session is self-describing.
  EXPECT_EQ(record.raw_profile(), raw);
}

TEST(ProfileSessionConversionTest, EmptyToolLabelIsCarriedAsEmpty) {
  const ProfileWindowRecord record =
      ToProfileWindowRecord(makeWindow(), "raw", /*symbolizer_tool=*/"");
  EXPECT_TRUE(record.symbolizer_tool().empty());
}

// The M1 gate: a profile_window record survives the full writer -> file ->
// reader path intact.
TEST(ProfileSessionTest, ProfileWindowRecordRoundTripsThroughWriterReader) {
  const std::string path = tempPath("tui-profile-session-roundtrip.nhsession");

  StartRequest request;
  request.mutable_options()->mutable_uri()->set_value("http://127.0.0.1:10000/");
  auto writer_or = SessionWriter::open(path, ToSessionHeader(request, "sid", 7));
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);

  const std::string raw = "raw-pprof-payload";
  SessionRecord record;
  *record.mutable_profile_window() =
      ToProfileWindowRecord(makeWindow(), raw, "go tool pprof");
  writer->enqueue(record);

  RunEndRecord run_end;
  run_end.set_completed(true);
  run_end.set_ended_unix_ns(99);
  ASSERT_TRUE(writer->close(run_end).ok());
  EXPECT_EQ(writer->droppedCount(), 0u);

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_profile_window = false;
  bool have_run_end = false;
  ProfileWindowRecord got;
  while (true) {
    auto next_or = reader->next();
    ASSERT_TRUE(next_or.status().ok()) << next_or.status();
    if (!next_or->has_value()) {
      break;
    }
    switch ((*next_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      EXPECT_EQ((*next_or)->header().format_version(), "1");
      break;
    case SessionRecord::kProfileWindow:
      have_profile_window = true;
      got = (*next_or)->profile_window();
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      break;
    default:
      ADD_FAILURE() << "unexpected record frame";
      break;
    }
  }
  ASSERT_TRUE(have_header);
  ASSERT_TRUE(have_profile_window);
  ASSERT_TRUE(have_run_end);

  const ProfileWindowRecord want = ToProfileWindowRecord(makeWindow(), raw, "go tool pprof");
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(got, want));
  EXPECT_EQ(got.raw_profile(), raw);
}

// The forward direction of the asymmetry: a session file with ONLY the M0b
// record set still parses fully with the new reader — new reads old.
TEST(ProfileSessionTest, OldSessionStillParses) {
  const std::string path = tempPath("tui-profile-session-old-format.nhsession");

  auto writer_or = SessionWriter::open(path, makeM0bHeader().header());
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);
  writer->enqueue(makeM0bMetricSeries());
  RunEndRecord run_end;
  run_end.set_completed(false);
  run_end.set_ended_unix_ns(55);
  ASSERT_TRUE(writer->close(run_end).ok());

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  bool have_header = false;
  bool have_series = false;
  bool have_run_end = false;
  while (true) {
    auto next_or = reader->next();
    ASSERT_TRUE(next_or.status().ok()) << next_or.status();
    if (!next_or->has_value()) {
      break;
    }
    switch ((*next_or)->record_case()) {
    case SessionRecord::kHeader:
      have_header = true;
      EXPECT_EQ((*next_or)->header().session_id(), "m0b-shape");
      break;
    case SessionRecord::kMetricSeries:
      have_series = true;
      EXPECT_EQ((*next_or)->metric_series().counters().at("benchmark.http_2xx"), 42u);
      break;
    case SessionRecord::kRunEnd:
      have_run_end = true;
      EXPECT_FALSE((*next_or)->run_end().completed());
      break;
    default:
      ADD_FAILURE() << "unexpected record frame";
      break;
    }
  }
  EXPECT_TRUE(have_header);
  EXPECT_TRUE(have_series);
  EXPECT_TRUE(have_run_end);
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
