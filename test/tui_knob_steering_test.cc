#include <grpc++/grpc++.h>

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

#include "api/client/session.pb.h"
#include "api/client/service.pb.h"
#include "source/client/tui/knob_pane.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/tui/session_reader.h"
#include "source/client/tui/session_writer.h"
#include "source/client/tui/status_bar.h"
#include "source/client/tui/tui_app.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

using nighthawk::client::KnobEpochRecord;
using nighthawk::client::LiveSnapshot;
using nighthawk::client::MetricSeriesRecord;
using nighthawk::client::SessionHeader;
using nighthawk::client::SessionRecord;
using nighthawk::client::UpdateResult;

// FORK-ONLY (We-Amp): M3 Task 8 — the TUI knob-turn flow. The §4 knob state
// machine (current-acked / pending-apply / rejected-with-inline-reason /
// staged-next-phase, visually distinct), the timeline-truth rule (epoch
// marks land ONLY on engine acks), the pending-timeout honesty rule, and
// the KnobEpochRecord session path.

// Splits an ftxui::Screen into per-row strings (same helper as the other
// TUI tests): ToString() terminates rows with "\r\n"; stripped here.
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

std::string screenText(const ftxui::Element& element, int width, int height) {
  std::string text;
  for (const std::string& line : screenLines(element, width, height)) {
    text += line + "\n";
  }
  return text;
}

UpdateResult appliedAck(const std::string& epoch_id) {
  UpdateResult result;
  result.set_applied(true);
  result.set_epoch_id(epoch_id);
  result.set_wall_time_unix_ns(1'000'000'000);
  result.set_monotonic_ns(42);
  return result;
}

UpdateResult rejectedAck(const std::string& reason) {
  UpdateResult result;
  result.set_applied(false);
  result.set_rejection_reason(reason);
  return result;
}

// One phase segment named `id`.
std::vector<PhaseTimelineSegment> oneSegment(const std::string& id) {
  PhaseTimelineSegment segment;
  segment.phase_id = id;
  segment.duration_seconds = 60;
  return {segment};
}

// ---------------------------------------------------------------------------
// TimelineMarksEpochsOnlyOnEngineAck (Review Focus 4): a user edit marks
// nothing; an applied ack lands exactly one mark carrying the ack's epoch
// id; a rejection marks nothing. Pinned at the pure layer (WithEpochMarks +
// the tracker) AND the controller wiring (synthetic acks through the feed's
// intake seam).
// ---------------------------------------------------------------------------

TEST(TimelineMarksEpochsTest, UserEditMarksNothingAckMarksExactlyOnce) {
  KnobTurnTracker tracker;
  tracker.SeedAckedValue(100);

  // The user edit: the turn goes pending — the timeline is UNCHANGED.
  tracker.BeginRpsTurn(300);
  ASSERT_TRUE(tracker.has_pending());
  std::vector<PhaseTimelineSegment> segments = oneSegment("phase-1");
  std::vector<KnobEpochRecord> no_marks; // nothing acked yet
  std::vector<PhaseTimelineSegment> still = WithEpochMarks(segments, no_marks);
  EXPECT_TRUE(still[0].epoch_ids.empty()) << "a pending turn must not mark the timeline";

  // The engine ack: exactly one mark, carrying the ack's epoch id.
  std::vector<KnobEpochRecord> marks;
  KnobEpochRecord acked = ToKnobEpochRecord(appliedAck("epoch-1"), "phase-1", 300, false);
  ASSERT_FALSE(acked.epoch_id().empty());
  marks.push_back(acked);
  std::vector<PhaseTimelineSegment> marked = WithEpochMarks(segments, marks);
  ASSERT_EQ(marked[0].epoch_ids.size(), 1u);
  EXPECT_EQ(marked[0].epoch_ids[0], "epoch-1");

  // The strip renders the mark.
  const std::string strip = PhaseTimelineStrip(marked);
  EXPECT_NE(strip.find("+epoch-1"), std::string::npos) << strip;
}

TEST(TimelineMarksEpochsTest, RejectionMarksNothingReasonStaysInline) {
  KnobTurnTracker tracker;
  tracker.SeedAckedValue(100);
  tracker.BeginRpsTurn(300);
  // An engine rejection: no mark; the reason renders inline until dismissed.
  tracker.OnUpdateResult(rejectedAck("requests_per_second: not in the live-knob whitelist"));
  EXPECT_FALSE(tracker.has_pending());
  EXPECT_EQ(tracker.rps().state, KnobTurnState::kRejected);
  EXPECT_EQ(tracker.rps().acked_value, "100") << "a rejection changes nothing";

  std::vector<KnobEpochRecord> marks; // the controller records no mark
  std::vector<PhaseTimelineSegment> marked = WithEpochMarks(oneSegment("phase-1"), marks);
  EXPECT_TRUE(marked[0].epoch_ids.empty()) << "a rejection must not mark the timeline";
}

TEST(TimelineMarksEpochsTest, ControllerMarksOnlyOnAppliedAcks) {
  // The wiring seam: the feed's read thread is the production caller; the
  // tests fire synthetic results through the same public intake. (No run is
  // in flight, so no turn is ever pending here — the pending/timeout
  // resolution itself is pinned at the tracker level above; this test pins
  // the MARKS, which depend only on the results.)
  auto channel = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
  RunController controller(channel, /*session_prefix=*/"knob-test");

  EXPECT_TRUE(controller.knobEpochMarks().empty());
  controller.onUpdateResult(appliedAck("epoch-1"));
  ASSERT_EQ(controller.knobEpochMarks().size(), 1u);
  EXPECT_EQ(controller.knobEpochMarks()[0].epoch_id(), "epoch-1");
  EXPECT_EQ(controller.knobEpochMarks()[0].monotonic_ns(), 42);

  controller.onUpdateResult(rejectedAck("queue bounds: not in the live-knob whitelist"));
  EXPECT_EQ(controller.knobEpochMarks().size(), 1u) << "a rejection must not add a mark";
  EXPECT_EQ(controller.rpsTurnState().state, KnobTurnState::kCurrentAcked)
      << "an unsolicited result resolves nothing";

  // A feed-generated pending-timeout result (the run ended with a turn
  // un-acked) adds no mark either — client-side, never an engine verdict.
  controller.onUpdateResult(rejectedAck("run-feed: run ended before the ack"));
  EXPECT_EQ(controller.knobEpochMarks().size(), 1u);
}

// ---------------------------------------------------------------------------
// KnobStatesRenderVisuallyDistinct: all four states into ftxui::Screen —
// distinct MARKER WORDS (never color alone), asserted via the rendered
// cells. Staged renders its own STAGED tag (M2 machinery, kept distinct).
// ---------------------------------------------------------------------------

TEST(KnobTurnRenderTest, KnobStatesRenderVisuallyDistinct) {
  LiveKnobTurn acked{"rps", "100", "", KnobTurnState::kCurrentAcked, ""};
  LiveKnobTurn pending{"rps", "100", "300", KnobTurnState::kPendingApply, ""};
  LiveKnobTurn rejected{"rps", "100", "300", KnobTurnState::kRejected,
                        "requests_per_second: rps must be >= 1"};
  LiveKnobTurn failed{"rps", "100", "300", KnobTurnState::kFailedPending,
                      "run-feed: run ended before the ack"};

  const std::string acked_text = screenText(RenderLiveKnobTurns(acked, 100, 5), 100, 5);
  const std::string pending_text = screenText(RenderLiveKnobTurns(pending, 100, 5), 100, 5);
  const std::string rejected_text = screenText(RenderLiveKnobTurns(rejected, 100, 5), 100, 5);
  const std::string failed_text = screenText(RenderLiveKnobTurns(failed, 100, 5), 100, 5);

  // Each state carries its own marker word.
  EXPECT_NE(acked_text.find("[acked]"), std::string::npos) << acked_text;
  EXPECT_NE(pending_text.find("[pending...]"), std::string::npos) << pending_text;
  EXPECT_NE(rejected_text.find("[rejected"), std::string::npos) << rejected_text;
  EXPECT_NE(failed_text.find("[failed]"), std::string::npos) << failed_text;

  // The marker words are pairwise distinct — no two states share a marker,
  // so the state survives a monochrome paste.
  EXPECT_NE(KnobTurnStateMarker(KnobTurnState::kCurrentAcked),
            KnobTurnStateMarker(KnobTurnState::kPendingApply));
  EXPECT_NE(KnobTurnStateMarker(KnobTurnState::kPendingApply),
            KnobTurnStateMarker(KnobTurnState::kRejected));
  EXPECT_NE(KnobTurnStateMarker(KnobTurnState::kRejected),
            KnobTurnStateMarker(KnobTurnState::kFailedPending));
  EXPECT_NE(KnobTurnStateMarker(KnobTurnState::kFailedPending),
            KnobTurnStateMarker(KnobTurnState::kCurrentAcked));

  // The inline reason renders with the rejected state; the pending
  // transition shows the attempted value; the acked state shows neither.
  EXPECT_NE(rejected_text.find("rps must be >= 1"), std::string::npos) << rejected_text;
  EXPECT_NE(rejected_text.find("(u dismisses)"), std::string::npos) << rejected_text;
  EXPECT_NE(pending_text.find("-> 300"), std::string::npos) << pending_text;
  EXPECT_EQ(acked_text.find("300"), std::string::npos) << acked_text;

  // Every rps surface carries the phase-total disclosure (the knob speaks
  // GLOBAL rps).
  EXPECT_NE(acked_text.find("phase total"), std::string::npos) << acked_text;

  // The staged state (M2 preview) stays distinct from every live state.
  EXPECT_EQ(std::string(KnobTurnStateMarker(KnobTurnState::kStaged)), "STAGED");
  const std::string staged_line = screenText(
      RenderNextPhasePreviewBlock(NextPhasePreviewKnobs([] {
        nighthawk::client::PhaseSpec spec;
        spec.set_name("spike");
        return spec;
      }(), nighthawk::client::CommandLineOptions{}), ""),
      100, 6);
  EXPECT_NE(staged_line.find("STAGED"), std::string::npos) << staged_line;
  EXPECT_EQ(staged_line.find("[acked]"), std::string::npos) << staged_line;
}

// ---------------------------------------------------------------------------
// PendingTimeoutSurfacesNotSilentlyClears: a pending turn that resolves via
// the feed's run-end surfacing renders as failed-pending WITH its reason —
// never silently cleared (spec §4 B honesty rule).
// ---------------------------------------------------------------------------

TEST(PendingTimeoutTest, PendingTimeoutSurfacesNotSilentlyClears) {
  KnobTurnTracker tracker;
  tracker.SeedAckedValue(100);
  tracker.BeginRpsTurn(300);
  ASSERT_TRUE(tracker.has_pending());

  // The feed-generated pending result (rejection_reason carries the
  // "run-feed: " prefix — never an engine verdict).
  tracker.OnUpdateResult(rejectedAck("run-feed: run ended before the ack arrived"));
  EXPECT_FALSE(tracker.has_pending());
  EXPECT_EQ(tracker.rps().state, KnobTurnState::kFailedPending);
  EXPECT_EQ(tracker.rps().note, "run-feed: run ended before the ack arrived");
  EXPECT_EQ(tracker.rps().acked_value, "100") << "no engine verdict: the value is unchanged";

  // The rendered pane carries the failed marker AND the reason.
  const std::string text = screenText(RenderLiveKnobTurns(tracker.rps(), 120, 5), 120, 5);
  EXPECT_NE(text.find("[failed]"), std::string::npos) << text;
  EXPECT_NE(text.find("run ended before the ack"), std::string::npos) << text;

  // Dismissal is the explicit u affordance — not an automatic clear.
  tracker.DismissOutcome();
  EXPECT_EQ(tracker.rps().state, KnobTurnState::kCurrentAcked);
  EXPECT_TRUE(tracker.rps().note.empty());
}

// ---------------------------------------------------------------------------
// KnobEpochRecordRoundTrips: the session record writes and reads back
// field-exact (applied AND rejected shapes); an M2-era reader's view of the
// new oneof branch is the documented version-skew rejection — re-tested.
// ---------------------------------------------------------------------------

std::string tempPath(const std::string& name) {
  const char* dir = ::getenv("TEST_TMPDIR");
  return absl::StrCat(dir == nullptr ? "/tmp" : dir, "/", name);
}

TEST(KnobEpochRecordRoundTripTest, RoundTripsAppliedAndRejected) {
  const std::string path = tempPath("knob-epoch-roundtrip.nhsession");
  SessionHeader header;
  header.set_format_version("1");
  header.set_session_id("knob-epoch");
  auto writer_or = SessionWriter::open(path, header);
  ASSERT_TRUE(writer_or.ok()) << writer_or.status();
  std::unique_ptr<SessionWriter> writer = std::move(*writer_or);

  SessionRecord applied;
  *applied.mutable_knob_epoch() = ToKnobEpochRecord(appliedAck("epoch-1"), "phase-1", 300, true);
  writer->enqueue(std::move(applied));

  SessionRecord rejected;
  *rejected.mutable_knob_epoch() =
      ToKnobEpochRecord(rejectedAck("max_pending_requests: not in the live-knob whitelist"), "",
                        0, false);
  writer->enqueue(std::move(rejected));

  nighthawk::client::RunEndRecord run_end;
  run_end.set_completed(true);
  run_end.set_ended_unix_ns(1);
  ASSERT_TRUE(writer->close(std::move(run_end)).ok());

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);

  auto header_record = reader->next();
  ASSERT_TRUE(header_record.ok()) << header_record.status();
  ASSERT_TRUE((*header_record)->has_header());

  auto got_applied = reader->next();
  ASSERT_TRUE(got_applied.ok()) << got_applied.status();
  ASSERT_TRUE((*got_applied)->has_knob_epoch());
  const KnobEpochRecord& applied_record = (*got_applied)->knob_epoch();
  EXPECT_EQ(applied_record.epoch_id(), "epoch-1");
  EXPECT_EQ(applied_record.phase_id(), "phase-1");
  EXPECT_EQ(applied_record.wall_time_unix_ns(), 1'000'000'000);
  EXPECT_EQ(applied_record.monotonic_ns(), 42);
  EXPECT_EQ(applied_record.new_rps(), 300u);
  EXPECT_TRUE(applied_record.ramp_bent());
  EXPECT_EQ(applied_record.rejection_note(), "");

  auto got_rejected = reader->next();
  ASSERT_TRUE(got_rejected.ok()) << got_rejected.status();
  ASSERT_TRUE((*got_rejected)->has_knob_epoch());
  const KnobEpochRecord& rejected_record = (*got_rejected)->knob_epoch();
  EXPECT_EQ(rejected_record.epoch_id(), "") << "a rejection carries no epoch";
  EXPECT_EQ(rejected_record.phase_id(), "");
  EXPECT_EQ(rejected_record.rejection_note(),
            "max_pending_requests: not in the live-knob whitelist");
  EXPECT_EQ(rejected_record.new_rps(), 0u);

  auto run_end_record = reader->next();
  ASSERT_TRUE(run_end_record.ok()) << run_end_record.status();
  EXPECT_TRUE((*run_end_record)->has_run_end());
  auto eof = reader->next();
  ASSERT_TRUE(eof.ok()) << eof.status();
  EXPECT_FALSE(eof->has_value());
}

// The forward-compat asymmetry, re-tested for the new case (the M2-era
// reader's view of knob_epoch): an unknown oneof case parses but sets no
// known record — the reader reports the version-skew DataLoss, never a
// silent skip. The real field-6 bytes an old reader chokes on are pinned by
// RoundTripsAppliedAndRejected above; the mechanism is reproduced with a
// hand-built unknown-field frame (one process cannot link two proto
// generations).
TEST(KnobEpochRecordRoundTripTest, OldReaderReadingNewRecordHitsVersionSkewRejection) {
  const std::string path = tempPath("knob-epoch-forward-compat.nhsession");
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    SessionRecord header_record;
    header_record.mutable_header()->set_format_version("1");
    // appendFrame equivalent: header is a known case.
    std::string payload;
    ASSERT_TRUE(header_record.SerializeToString(&payload));
    uint32_t length = payload.size();
    while ((length & ~0x7fu) != 0) {
      out.put(static_cast<char>((length & 0x7fu) | 0x80u));
      length >>= 7;
    }
    out.put(static_cast<char>(length));
    out.write(payload.data(), payload.size());
    // A "future" record: field 6 (the knob_epoch branch), varint wire type.
    std::string unknown;
    unknown.push_back(static_cast<char>((6 << 3) | 0));
    unknown.push_back(static_cast<char>(1));
    uint32_t ulength = unknown.size();
    out.put(static_cast<char>(ulength));
    out.write(unknown.data(), unknown.size());
    ASSERT_TRUE(out.good());
  }

  auto reader_or = SessionReader::open(path);
  ASSERT_TRUE(reader_or.ok()) << reader_or.status();
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  auto header = reader->next();
  ASSERT_TRUE(header.ok()) << header.status();
  auto skewed = reader->next();
  ASSERT_FALSE(skewed.ok());
  EXPECT_EQ(skewed.status().code(), absl::StatusCode::kDataLoss);
  EXPECT_NE(skewed.status().message().find("NEWER writer"), std::string::npos) << skewed.status();
}

// ---------------------------------------------------------------------------
// EpochStampsFlowToSession: snapshots carrying epoch ids persist them in
// the MetricSeriesRecord (the dump's grep surface); the KnobEpochRecord
// conversion carries the ack's fields (the session's steering story).
// ---------------------------------------------------------------------------

TEST(EpochStampsFlowTest, SnapshotEpochFlowsToMetricSeriesRecord) {
  LiveSnapshot snapshot;
  snapshot.set_wall_time_unix_ns(1'000);
  snapshot.set_monotonic_ns(10);
  snapshot.set_phase_id("phase-1");
  snapshot.set_epoch_id("epoch-1");
  (*snapshot.mutable_counters())["benchmark.http_2xx"] = 7;

  MetricSeriesRecord record = ToMetricSeries(snapshot);
  EXPECT_EQ(record.epoch_id(), "epoch-1");
  EXPECT_EQ(record.phase_id(), "phase-1");
  EXPECT_EQ(record.wall_time_unix_ns(), 1'000);
  EXPECT_EQ(record.monotonic_ns(), 10);
  EXPECT_EQ(record.counters().at("benchmark.http_2xx"), 7u);

  // Empty epoch stays empty (before the run's first applied change).
  snapshot.set_epoch_id("");
  EXPECT_EQ(ToMetricSeries(snapshot).epoch_id(), "");
}

TEST(EpochStampsFlowTest, AckFlowsToKnobEpochRecord) {
  KnobEpochRecord applied = ToKnobEpochRecord(appliedAck("epoch-3"), "phase-2", 500, false);
  EXPECT_EQ(applied.epoch_id(), "epoch-3");
  EXPECT_EQ(applied.phase_id(), "phase-2");
  EXPECT_EQ(applied.wall_time_unix_ns(), 1'000'000'000);
  EXPECT_EQ(applied.monotonic_ns(), 42);
  EXPECT_EQ(applied.new_rps(), 500u);
  EXPECT_FALSE(applied.ramp_bent());
  EXPECT_EQ(applied.rejection_note(), "");

  KnobEpochRecord rejected =
      ToKnobEpochRecord(rejectedAck("ramp: ramp bend requires the ramp plugin configured"), "",
                        200, false);
  EXPECT_EQ(rejected.epoch_id(), "");
  EXPECT_EQ(rejected.phase_id(), "");
  EXPECT_EQ(rejected.rejection_note(),
            "ramp: ramp bend requires the ramp plugin configured");
}

// ---------------------------------------------------------------------------
// The M3 pane header: the display-only banner is GONE; the phase-total
// semantics render on the pane.
// ---------------------------------------------------------------------------

TEST(KnobPaneBannerTest, BannerGoneAndPhaseTotalStated) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  const std::string text = screenText(RenderKnobPane(KnobsFromOptions(options), 100, 10), 100, 10);
  EXPECT_EQ(text.find("display-only"), std::string::npos) << text;
  EXPECT_EQ(text.find("steering lands in M3"), std::string::npos) << text;
  EXPECT_NE(text.find("phase total"), std::string::npos) << text;
  EXPECT_NE(text.find("LIVE"), std::string::npos) << text;
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
