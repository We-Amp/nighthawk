#include <cstdint>
#include <fstream>
#include <ios>
#include <ostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/screen/screen.hpp"
#include "api/client/output.pb.h"
#include "api/client/session.pb.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/session_reader.h"
#include "source/client/tui/session_writer.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using nighthawk::client::LiveSnapshot;
using nighthawk::client::PhaseBoundaryRecord;
using nighthawk::client::PhaseMetadata;
using nighthawk::client::PhaseSpec;
using nighthawk::client::SessionHeader;
using nighthawk::client::SessionRecord;
using nighthawk::client::WorkerPoolHealth;

// FORK-ONLY (We-Amp): M2 Task 7 phase-1 pure halves — the phase timeline
// strip, the PhaseTimelineSegments builder, and the PhaseBoundaryRecord
// conversion + session round-trip. Assertions work on rendered text / read
// back records so the tests pin what the user gets, not intermediate state.

std::string tempPath(const std::string& name) {
  const char* dir = ::getenv("TEST_TMPDIR");
  return absl::StrCat(dir == nullptr ? "/tmp" : dir, "/", name);
}

PhaseSpec makeSpec(const std::string& name, int64_t duration_seconds, const std::string& workers) {
  PhaseSpec spec;
  spec.set_name(name);
  spec.mutable_duration()->set_seconds(duration_seconds);
  spec.set_requests_per_second(100);
  spec.mutable_concurrency()->set_value(workers);
  return spec;
}

// One phase's metadata: id, index, worker count, per-worker pool entries.
PhaseMetadata makeMetadata(const std::string& id, uint32_t index, uint32_t worker_count,
                           const std::vector<std::pair<uint64_t, uint64_t>>& per_worker_pool) {
  PhaseMetadata metadata;
  metadata.set_phase_id(id);
  metadata.set_phase_index(index);
  metadata.set_worker_count(worker_count);
  for (const auto& [created, active] : per_worker_pool) {
    WorkerPoolHealth* health = metadata.add_pool_health();
    health->set_worker_id(metadata.pool_health_size() - 1);
    health->set_connections_created(created);
    health->set_connections_active(active);
  }
  return metadata;
}

// A two-phase run: phase "warm" (2 workers, ended at a drain of 0.5s with a
// 0.2s idle gap) then "spike" (4 workers, two of them cold-started at the
// boundary). Pool numbers are CUMULATIVE per worker (the engine's pool
// counter), so a surviving worker keeps its count across the boundary.
std::vector<PhaseMetadata> twoPhaseMetadata() {
  PhaseMetadata warm = makeMetadata("warm", 0, 2, {{4, 4}, {4, 4}});
  warm.mutable_drain_duration()->set_nanos(500'000'000);
  warm.mutable_idle_gap()->set_nanos(200'000'000);
  warm.set_completion_ratio(1.0);
  warm.set_gate_exceeded(0);

  PhaseMetadata spike = makeMetadata("spike", 1, 4, {{4, 4}, {4, 4}, {1, 4}, {1, 4}});
  spike.set_workers_changed_vs_prior_phase(true);
  spike.set_new_workers_cold_started(true);
  spike.set_completion_ratio(0.98);
  spike.set_gate_exceeded(7);
  return {warm, spike};
}

std::vector<PhaseSpec> twoPhaseSpecs() {
  return {makeSpec("warm", 2, "2"), makeSpec("spike", 2, "4")};
}

// ---------------------------------------------------------------------------
// The timeline strip: exact-render tests, like the charts.
// ---------------------------------------------------------------------------

std::vector<std::string> screenLines(const ftxui::Element& element, int width, int height) {
  ftxui::Screen screen(width, height);
  ftxui::Render(screen, element);
  std::string text = screen.ToString();
  std::vector<std::string> lines;
  size_t start = 0;
  while (start <= text.size()) {
    size_t newline = text.find('\n', start);
    std::string line =
        (newline == std::string::npos) ? text.substr(start) : text.substr(start, newline - start);
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    lines.push_back(line);
    if (newline == std::string::npos) {
      break;
    }
    start = newline + 1;
  }
  return lines;
}

TEST(PhaseTimelineTest, EmptyPhaseListRendersEmptyStrip) {
  EXPECT_EQ(PhaseTimelineStrip({}), "");
}

// A warm same-count boundary: the pool survives the boundary untouched, so
// the pool line reads the flat +0 form — the warm-transfer claim, rendered.
std::vector<PhaseMetadata> warmTransferMetadata() {
  PhaseMetadata warm = makeMetadata("warm", 0, 2, {{4, 4}, {4, 4}});
  warm.mutable_drain_duration()->set_nanos(500'000'000);
  warm.mutable_idle_gap()->set_nanos(200'000'000);
  warm.set_completion_ratio(1.0);
  warm.set_gate_exceeded(0);
  PhaseMetadata spike = makeMetadata("spike", 1, 2, {{4, 4}, {4, 4}});
  spike.set_completion_ratio(0.98);
  spike.set_gate_exceeded(7);
  return {warm, spike};
}

TEST(PhaseTimelineTest, StripRendersSegmentsBoundariesAndCurrentPhase) {
  std::vector<PhaseSpec> specs = twoPhaseSpecs();
  std::vector<PhaseTimelineSegment> segments =
      PhaseTimelineSegments(specs, warmTransferMetadata(), /*current_phase_id=*/"spike");

  // One entry per phase, in phase order, with the specs' durations.
  ASSERT_EQ(segments.size(), 2u);
  EXPECT_EQ(segments[0].phase_id, "warm");
  EXPECT_EQ(segments[1].phase_id, "spike");
  EXPECT_DOUBLE_EQ(segments[0].duration_seconds, 2.0);
  EXPECT_DOUBLE_EQ(segments[1].duration_seconds, 2.0);
  EXPECT_FALSE(segments[0].is_current);
  EXPECT_TRUE(segments[1].is_current);

  // Segment 0 carries the boundary that ENDED "warm": its drain/idle gap,
  // and the pool transition into "spike" — flat +0 (8 -> 8 cumulative), 8
  // active. No worker-set change at this boundary. The final phase carries
  // no boundary at all (it ends at run teardown).
  EXPECT_DOUBLE_EQ(segments[0].drain_seconds, 0.5);
  EXPECT_DOUBLE_EQ(segments[0].idle_gap_seconds, 0.2);
  EXPECT_TRUE(segments[0].has_pool);
  EXPECT_EQ(segments[0].pool_created_delta, 0);
  EXPECT_EQ(segments[0].pool_active, 8);
  EXPECT_FALSE(segments[0].worker_count_changed);
  EXPECT_FALSE(segments[0].new_workers_cold_started);
  EXPECT_FALSE(segments[1].has_pool);
  EXPECT_FALSE(segments[1].worker_count_changed);

  // The exact strip: phase segments with durations, the boundary's drain and
  // idle gap as a visible gap segment with the pool-health line (the plan's
  // `pool: created=+N active=M` vocabulary), the current phase highlighted
  // with its claim-discipline pair.
  EXPECT_EQ(PhaseTimelineStrip(segments),
            "[warm 2s | drain 0.5s gap 0.2s pool: created=+0 active=8 | *spike 2s* ratio=0.98 "
            "gate=7]");
}

// A shrinking worker set takes workers' cumulative creation counts out of the
// successor's sum, so the boundary pool line renders an explicit negative
// delta form.
TEST(PhaseTimelineTest, StripRendersShrinkingPoolAsNegativeDelta) {
  PhaseMetadata warm = makeMetadata("warm", 0, 3, {{4, 4}, {4, 4}, {4, 4}});
  warm.mutable_drain_duration()->set_nanos(500'000'000);
  PhaseMetadata spike = makeMetadata("spike", 1, 2, {{4, 4}, {4, 4}});

  std::vector<PhaseTimelineSegment> segments =
      PhaseTimelineSegments({makeSpec("warm", 2, "3"), makeSpec("spike", 2, "2")},
                            {warm, spike}, /*current_phase_id=*/"");
  ASSERT_EQ(segments.size(), 2u);
  EXPECT_EQ(segments[0].pool_created_delta, -4);
  EXPECT_EQ(segments[0].pool_active, 8);
  EXPECT_NE(PhaseTimelineStrip(segments).find("pool: created=-4 active=8"), std::string::npos);
}

TEST(PhaseTimelineTest, StripRendersColdAndNonComparableBoundaryMarks) {
  std::vector<PhaseTimelineSegment> segments =
      PhaseTimelineSegments(twoPhaseSpecs(), twoPhaseMetadata(), /*current_phase_id=*/"warm");

  // The worker-set change happens AT the boundary that ends "warm" (the
  // change is recorded on the successor's metadata), so the 8.4
  // non-comparable mark and the cold annotation render on that boundary.
  ASSERT_EQ(segments.size(), 2u);
  EXPECT_TRUE(segments[0].worker_count_changed);
  EXPECT_TRUE(segments[0].new_workers_cold_started);
  // Pool transition into "spike": 8 -> 10 cumulative created (the two cold
  // workers each created one connection), 16 active.
  EXPECT_EQ(segments[0].pool_created_delta, 2);
  EXPECT_EQ(segments[0].pool_active, 16);

  EXPECT_EQ(PhaseTimelineStrip(segments),
            "[*warm 2s* ratio=1.00 gate=0 | drain 0.5s gap 0.2s pool: created=+2 active=16 cmp! "
            "cold | spike 2s]");
}

// The element renderer clips the strip to the pane width — the truncation
// the persistent-bar wiring needs at the 100-column contract minimum.
TEST(PhaseTimelineTest, RenderPhaseTimelineTruncatesToWidth) {
  std::vector<PhaseTimelineSegment> segments =
      PhaseTimelineSegments(twoPhaseSpecs(), twoPhaseMetadata(), /*current_phase_id=*/"spike");
  const std::string strip = PhaseTimelineStrip(segments);
  ASSERT_GT(strip.size(), 40u);

  ftxui::Element element = RenderPhaseTimeline(segments, /*width=*/40);
  std::vector<std::string> lines = screenLines(element, 40, 1);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], strip.substr(0, 40));

  // Non-positive width renders the empty pane (the RenderKnobPane rule):
  // the single screen row carries no content.
  std::vector<std::string> empty = screenLines(RenderPhaseTimeline(segments, 0), 40, 1);
  ASSERT_EQ(empty.size(), 1u);
  EXPECT_EQ(empty[0].find_first_not_of(' '), std::string::npos) << "\"" << empty[0] << "\"";
}

// ---------------------------------------------------------------------------
// The PhaseBoundaryRecord conversion + session round-trip.
// ---------------------------------------------------------------------------

// The record for the boundary that ended a phase carries the EVENT's (wall,
// monotonic) pair, the ENDING phase's metadata aggregate, and the cold-start
// flag of the TRANSITION INTO the successor (the successor's flag — the
// ending phase's own flag describes its entry boundary, not this one; a
// record-joining consumer must be able to reproduce the strip's mark).
TEST(PhaseBoundaryTest, ConversionCarriesEventPairAndMetadataAggregate) {
  std::vector<PhaseMetadata> metadata = twoPhaseMetadata();
  PhaseBoundaryRecord record =
      ToPhaseBoundaryRecord(/*phase_index=*/0, /*phase_id=*/"warm", /*next_phase_id=*/"spike",
                            /*wall_time_unix_ns=*/1'000'000'000, /*monotonic_ns=*/42,
                            metadata[0],
                            /*next_phase_cold_started=*/metadata[1].new_workers_cold_started());

  EXPECT_EQ(record.phase_id(), "warm");
  EXPECT_EQ(record.next_phase_id(), "spike");
  EXPECT_EQ(record.phase_index(), 0);
  EXPECT_EQ(record.wall_time_unix_ns(), 1'000'000'000);
  EXPECT_EQ(record.monotonic_ns(), 42);
  EXPECT_EQ(record.drain_duration().nanos(), 500'000'000);
  EXPECT_EQ(record.idle_gap().nanos(), 200'000'000);
  EXPECT_EQ(record.worker_count(), 2);
  // The warm->spike transition cold-started the successor's two new workers:
  // the record says so even though the ENDING phase itself did not.
  EXPECT_TRUE(record.new_workers_cold_started());
  // Pool aggregates are the per-worker sums (ending-phase totals).
  EXPECT_EQ(record.connections_created(), 8);
  EXPECT_EQ(record.connections_active(), 8);
  EXPECT_DOUBLE_EQ(record.completion_ratio(), 1.0);
  EXPECT_EQ(record.gate_exceeded(), 0);
}

TEST(PhaseBoundaryTest, ConversionCarriesColdStartAndGateCounts) {
  std::vector<PhaseMetadata> metadata = twoPhaseMetadata();
  // The run's final phase has no successor transition: the cold flag is the
  // caller-supplied successor attribute, and with no successor it is false.
  PhaseBoundaryRecord record =
      ToPhaseBoundaryRecord(/*phase_index=*/1, /*phase_id=*/"spike", /*next_phase_id=*/"",
                            /*wall_time_unix_ns=*/2'000'000'000, /*monotonic_ns=*/43,
                            metadata[1], /*next_phase_cold_started=*/false);
  EXPECT_EQ(record.worker_count(), 4);
  EXPECT_FALSE(record.new_workers_cold_started());
  EXPECT_EQ(record.connections_created(), 10);
  EXPECT_EQ(record.connections_active(), 16);
  EXPECT_DOUBLE_EQ(record.completion_ratio(), 0.98);
  EXPECT_EQ(record.gate_exceeded(), 7);
  // Final phase: no successor id, and the zero drain stands (the run's final
  // phase ends at teardown, not at a boundary drain).
  EXPECT_EQ(record.next_phase_id(), "");
  EXPECT_EQ(record.drain_duration().seconds(), 0);
}

TEST(PhaseBoundaryTest, BoundaryRecordRoundTrips) {
  std::vector<PhaseMetadata> metadata = twoPhaseMetadata();
  PhaseBoundaryRecord boundary = ToPhaseBoundaryRecord(
      0, "warm", "spike", 1'000'000'000, 42, metadata[0],
      /*next_phase_cold_started=*/metadata[1].new_workers_cold_started());

  const std::string path = tempPath("boundary-roundtrip.nhsession");
  SessionHeader header;
  header.set_format_version("1");
  header.set_session_id("boundary-roundtrip");
  header.set_started_unix_ns(900);
  auto writer_or = SessionWriter::open(path, header);
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);
  SessionRecord boundary_record;
  *boundary_record.mutable_phase_boundary() = boundary;
  // Wire-level fact an M1-era reader trips on: the record rides oneof
  // branch 5, so the frame's payload starts with the field-5 tag (5<<3|2 =
  // 42 = 0x2A). The old reader has no branch 5 compiled; these bytes land in
  // its unknown-field set and record_case() stays RECORD_NOT_SET — the
  // version-skew rejection below reproduces that path mechanically (a true
  // M1-binary check would need the old generated code linked, which one
  // process cannot do).
  std::string wire_payload;
  ASSERT_TRUE(boundary_record.SerializeToString(&wire_payload));
  ASSERT_FALSE(wire_payload.empty());
  EXPECT_EQ(static_cast<uint8_t>(wire_payload[0]), 0x2A);
  writer->enqueue(std::move(boundary_record));
  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(true);
  ASSERT_TRUE(writer->close(std::move(run_end)).ok());

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  auto header_read = reader->next();
  ASSERT_TRUE(header_read.ok()) << header_read.status();
  ASSERT_TRUE(header_read->has_value());
  EXPECT_TRUE((*header_read)->has_header());

  auto read = reader->next();
  ASSERT_TRUE(read.ok()) << read.status();
  ASSERT_TRUE(read->has_value());
  ASSERT_TRUE((*read)->has_phase_boundary());
  const PhaseBoundaryRecord& got = (*read)->phase_boundary();
  // Field-exact, incl. pool-health and completion-ratio fields, and the
  // successor-attributed cold flag (matching the strip's boundary mark).
  EXPECT_EQ(got.phase_id(), "warm");
  EXPECT_EQ(got.next_phase_id(), "spike");
  EXPECT_EQ(got.phase_index(), 0);
  EXPECT_EQ(got.wall_time_unix_ns(), 1'000'000'000);
  EXPECT_EQ(got.monotonic_ns(), 42);
  EXPECT_EQ(got.drain_duration().nanos(), 500'000'000);
  EXPECT_EQ(got.idle_gap().nanos(), 200'000'000);
  EXPECT_EQ(got.worker_count(), 2);
  EXPECT_TRUE(got.new_workers_cold_started());
  EXPECT_EQ(got.connections_created(), 8);
  EXPECT_EQ(got.connections_active(), 8);
  EXPECT_DOUBLE_EQ(got.completion_ratio(), 1.0);
  EXPECT_EQ(got.gate_exceeded(), 0);

  auto end = reader->next();
  ASSERT_TRUE(end.ok()) << end.status();
  ASSERT_TRUE(end->has_value());
  EXPECT_TRUE((*end)->has_run_end());
  auto eof = reader->next();
  ASSERT_TRUE(eof.ok()) << eof.status();
  EXPECT_FALSE(eof->has_value());
}

// Appends one length-prefixed frame for `record` to `out` (the same varint32
// framing the writer uses), for tests that hand-build files.
void appendFrame(std::ostream& out, const SessionRecord& record) {
  std::string payload;
  ASSERT_TRUE(record.SerializeToString(&payload));
  uint32_t length = payload.size();
  while ((length & ~0x7fu) != 0) {
    out.put(static_cast<char>((length & 0x7fu) | 0x80u));
    length >>= 7;
  }
  out.put(static_cast<char>(length));
  out.write(payload.data(), payload.size());
}

// Appends a raw frame whose payload parses but sets no oneof case the reader
// knows: a lone unknown field. This is exactly what an M1-era reader sees in
// a phase_boundary frame — the wire bytes of oneof branch 5 land in its
// unknown-field set and record_case() stays RECORD_NOT_SET.
void appendUnknownFieldFrame(std::ostream& out, uint32_t field_number, uint64_t value) {
  std::string payload;
  uint64_t tag = (static_cast<uint64_t>(field_number) << 3) | 0; // varint wire type
  while ((tag & ~0x7full) != 0) {
    payload.push_back(static_cast<char>((tag & 0x7f) | 0x80));
    tag >>= 7;
  }
  payload.push_back(static_cast<char>(tag));
  uint64_t v = value;
  while ((v & ~0x7full) != 0) {
    payload.push_back(static_cast<char>((v & 0x7f) | 0x80));
    v >>= 7;
  }
  payload.push_back(static_cast<char>(v));
  uint32_t length = payload.size();
  while ((length & ~0x7fu) != 0) {
    out.put(static_cast<char>((length & 0x7fu) | 0x80u));
    length >>= 7;
  }
  out.put(static_cast<char>(length));
  out.write(payload.data(), payload.size());
}

// The forward-compat asymmetry (re-tested here per the M2 plan): a reader
// that predates the phase_boundary case hits the parked RECORD_NOT_SET
// rejection on an M2 file — new reads old, old does NOT read new. The
// mechanism is the unknown-oneof-case -> unset-record -> version-skew
// DataLoss path; reproduced here with a hand-built frame carrying a field
// number no reader of this file format compiles (the real bytes an old
// reader chokes on — field 5 — are pinned in BoundaryRecordRoundTrips; a
// true M1-binary check would need the old generated code linked, which one
// process cannot do).
TEST(PhaseBoundaryTest, OldReaderReadingNewRecordHitsVersionSkewRejection) {
  const std::string path = tempPath("forward-compat.nhsession");
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    SessionRecord header_record;
    header_record.mutable_header()->set_format_version("1");
    appendFrame(out, header_record);
    // A "future" record: parses fine, sets no case the current reader knows.
    appendUnknownFieldFrame(out, /*field_number=*/99, /*value=*/1);
    ASSERT_TRUE(out.good());
  }

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  auto header = reader->next();
  ASSERT_TRUE(header.ok()) << header.status();
  ASSERT_TRUE(header->has_value());
  auto skewed = reader->next();
  ASSERT_FALSE(skewed.ok());
  EXPECT_EQ(skewed.status().code(), absl::StatusCode::kDataLoss);
  EXPECT_NE(skewed.status().message().find("NEWER writer"), std::string::npos) << skewed.status();
}

// The stream-at-boundary conversion: the wire's PhaseBoundaryEvent becomes
// the session's record, identity joined to the ENDING phase; the pool/claim
// fields stay default (the final Output's PhaseMetadata is the post-run
// completeness source).
TEST(PhaseBoundaryTest, EventConversionJoinsEndingPhaseAndDefaultsClaims) {
  nighthawk::client::PhaseBoundaryEvent event;
  event.set_phase_index(1);
  event.set_phase_id("spike");
  event.set_wall_time_unix_ns(1'000'000'000);
  event.set_monotonic_ns(42);
  event.set_ending_phase_index(0);
  event.set_ending_phase_id("warm");
  event.mutable_drain_duration()->set_nanos(500'000'000);
  event.mutable_idle_gap()->set_seconds(0);
  event.mutable_idle_gap()->set_nanos(200'000'000);
  event.set_worker_count(2);
  event.set_next_phase_cold_started(true);

  PhaseBoundaryRecord record = ToPhaseBoundaryRecord(event);
  EXPECT_EQ(record.phase_id(), "warm");
  EXPECT_EQ(record.next_phase_id(), "spike");
  EXPECT_EQ(record.phase_index(), 0);
  EXPECT_EQ(record.wall_time_unix_ns(), 1'000'000'000);
  EXPECT_EQ(record.monotonic_ns(), 42);
  EXPECT_EQ(record.drain_duration().nanos(), 500'000'000);
  EXPECT_EQ(record.idle_gap().nanos(), 200'000'000);
  EXPECT_EQ(record.worker_count(), 2);
  EXPECT_TRUE(record.new_workers_cold_started());
  // Defaults: post-run completeness comes from the Output's PhaseMetadata.
  EXPECT_EQ(record.connections_created(), 0);
  EXPECT_EQ(record.connections_active(), 0);
  EXPECT_DOUBLE_EQ(record.completion_ratio(), 0.0);
  EXPECT_EQ(record.gate_exceeded(), 0);
}

// 8.6 counted-not-hidden: a censored boundary streams with the flag set, and
// the record carries it (the strip renders the cens mark).
TEST(PhaseBoundaryTest, CensoredBoundaryCarriesTheFlagThroughTheRecord) {
  nighthawk::client::PhaseBoundaryEvent event;
  event.set_phase_index(1);
  event.set_phase_id("spike");
  event.set_wall_time_unix_ns(3'000);
  event.set_monotonic_ns(4'000);
  event.set_ending_phase_index(0);
  event.set_ending_phase_id("warm");
  event.mutable_drain_duration()->set_seconds(1); // the capped full budget
  event.mutable_idle_gap()->set_nanos(100'000);
  event.set_worker_count(2);
  event.set_drain_censored(true);

  PhaseBoundaryRecord record = ToPhaseBoundaryRecord(event);
  EXPECT_TRUE(record.drain_censored());
  EXPECT_EQ(record.drain_duration().seconds(), 1);
  EXPECT_EQ(record.idle_gap().nanos(), 100'000);
}

// Real phase ids flow into the persisted metric series (the placeholder
// marker is dropped when real ids flow); the header conversion is unchanged.
TEST(PhaseBoundaryTest, PhaseIdsFlowToSession) {
  LiveSnapshot snapshot;
  snapshot.set_wall_time_unix_ns(5);
  snapshot.set_monotonic_ns(6);
  snapshot.set_phase_id("spike");
  snapshot.set_epoch_id("");
  nighthawk::client::MetricSeriesRecord series = ToMetricSeries(snapshot);
  EXPECT_EQ(series.phase_id(), "spike");

  nighthawk::client::StartRequest request;
  SessionHeader header = ToSessionHeader(request, "sid", 7);
  EXPECT_EQ(header.format_version(), "1");
  EXPECT_EQ(header.session_id(), "sid");
  EXPECT_EQ(header.started_unix_ns(), 7);
}

// ---------------------------------------------------------------------------
// M2 wiring: the live timeline (streamed events) and the render-side source
// selection.
// ---------------------------------------------------------------------------

using nighthawk::client::PhaseBoundaryEvent;
using nighthawk::client::CommandLineOptions;

PhaseBoundaryEvent makeBoundaryEvent(const std::string& ending, uint32_t ending_index,
                                     const std::string& starting, bool cold) {
  PhaseBoundaryEvent event;
  event.set_phase_index(1);
  event.set_phase_id(starting);
  event.set_wall_time_unix_ns(1'000);
  event.set_monotonic_ns(2'000);
  event.set_ending_phase_index(ending_index);
  event.set_ending_phase_id(ending);
  event.mutable_drain_duration()->set_nanos(500'000'000);
  event.mutable_idle_gap()->set_nanos(200'000'000);
  event.set_worker_count(2);
  event.set_next_phase_cold_started(cold);
  return event;
}

// The live strip: boundary fields render from the streamed events; the pool
// and claim cells stay unset until the Output lands (DQ-3: never a
// fabricated cell).
TEST(PhaseTimelineTest, EventsBuiltSegmentsRenderBoundaryWithoutClaimCells) {
  std::vector<PhaseSpec> specs = twoPhaseSpecs();
  std::vector<PhaseTimelineSegment> segments = PhaseTimelineSegmentsFromEvents(
      specs, {makeBoundaryEvent("warm", 0, "spike", /*cold=*/true)}, /*current_phase_id=*/"spike");

  ASSERT_EQ(segments.size(), 2u);
  EXPECT_EQ(segments[0].phase_id, "warm");
  EXPECT_DOUBLE_EQ(segments[0].drain_seconds, 0.5);
  EXPECT_DOUBLE_EQ(segments[0].idle_gap_seconds, 0.2);
  EXPECT_TRUE(segments[0].new_workers_cold_started);
  // The specs declare 2 -> 4 workers: the 8.4 mark renders. No pool/claim
  // cells (the engine computes them in the output pass).
  EXPECT_TRUE(segments[0].worker_count_changed);
  EXPECT_FALSE(segments[0].has_pool);
  EXPECT_FALSE(segments[0].has_completion);
  EXPECT_FALSE(segments[1].has_pool);

  EXPECT_EQ(PhaseTimelineStrip(segments),
            "[warm 2s | drain 0.5s gap 0.2s cmp! cold | *spike 2s*]");
}

// Source selection: the streamed events are the live source; the final
// Output's metadata takes over post-run (the completeness source).
TEST(PhaseTimelineTest, BuildPhaseTimelinePrefersMetadataWhenOutputArrived) {
  CommandLineOptions options;
  *options.add_phases() = makeSpec("warm", 2, "2");
  *options.add_phases() = makeSpec("spike", 2, "4");

  const std::vector<PhaseMetadata> metadata = twoPhaseMetadata();
  const std::vector<PhaseTimelineSegment> from_metadata =
      BuildPhaseTimeline(options, {makeBoundaryEvent("warm", 0, "spike", true)}, metadata,
                         /*current_phase_id=*/"spike");
  ASSERT_EQ(from_metadata.size(), 2u);
  EXPECT_TRUE(from_metadata[0].has_pool);
  EXPECT_TRUE(from_metadata[1].has_completion);

  const std::vector<PhaseTimelineSegment> from_events =
      BuildPhaseTimeline(options, {makeBoundaryEvent("warm", 0, "spike", true)}, {},
                         /*current_phase_id=*/"spike");
  ASSERT_EQ(from_events.size(), 2u);
  EXPECT_FALSE(from_events[0].has_pool);
  EXPECT_FALSE(from_events[1].has_completion);

  // A legacy run (no phases configured) builds nothing and the bar keeps
  // its placeholder row.
  EXPECT_TRUE(BuildPhaseTimeline(CommandLineOptions{}, {}, {}, "").empty());
}

// The bar renders the strip in row 1 during phase runs (the placeholder
// phase cell is replaced); legacy runs keep the placeholder row, and the
// marker drops once real phase ids flow.
TEST(PhaseTimelineTest, StatusBarRendersTimelineRowAndDropsPlaceholderWhenRealIdsFlow) {
  CommandLineOptions options;
  *options.add_phases() = makeSpec("warm", 2, "2");
  *options.add_phases() = makeSpec("spike", 2, "4");

  RunViewModel model;
  model.run_state = "running";
  model.last_snapshot.set_phase_id("spike");

  const std::vector<PhaseTimelineSegment> timeline =
      BuildPhaseTimeline(options, {makeBoundaryEvent("warm", 0, "spike", true)}, {}, "spike");
  const ftxui::Element bar =
      RenderStatusBar(model, InputMode::kNavigate, "s.nhsession", "http://x", 4, "", &timeline);
  const std::vector<std::string> lines = screenLines(bar, 140, 3);
  ASSERT_EQ(lines.size(), 3u);
  EXPECT_NE(lines[1].find("drain 0.5s"), std::string::npos) << lines[1];
  EXPECT_NE(lines[1].find("*spike 2s*"), std::string::npos) << lines[1];
  EXPECT_EQ(lines[1].find("pre-phase-engine placeholder"), std::string::npos) << lines[1];

  // Legacy row: no timeline; the placeholder marker rides the "main"
  // placeholder id, and a real id without a timeline still drops it.
  model.last_snapshot.set_phase_id("main");
  const ftxui::Element legacy_bar = RenderStatusBar(model, InputMode::kNavigate, "s.nhsession",
                                                    "http://x", 4, "", nullptr);
  const std::vector<std::string> legacy = screenLines(legacy_bar, 140, 3);
  ASSERT_EQ(legacy.size(), 3u);
  EXPECT_NE(legacy[1].find("pre-phase-engine placeholder"), std::string::npos) << legacy[1];
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
