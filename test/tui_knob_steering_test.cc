#include <grpc++/grpc++.h>

#include <algorithm>
#include <fstream>
#include <ios>
#include <limits>
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
#include "source/client/tui/layout.h"
#include "source/client/headless/run_view_model.h"
#include "source/client/tui/session_conversion.h"
#include "source/client/headless/session_reader.h"
#include "source/client/headless/session_writer.h"
#include "source/client/tui/start_run_dialog.h"
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

  // Every rps surface carries the total disclosure (the knob speaks
  // GLOBAL rps).
  EXPECT_NE(acked_text.find("(total)"), std::string::npos) << acked_text;

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
// The ramp_bent pin (C1, the ENGINE's exact rule): only the linear-ramping
// rate limiter's runs bend — a different named limiter plugin must NOT
// claim a bend in the session record.
// ---------------------------------------------------------------------------

TEST(RampBentRuleTest, OnlyTheLinearRampingPluginBends) {
  nighthawk::client::CommandLineOptions options;
  EXPECT_FALSE(RpsTurnBendsRamp(options)) << "no plugin configured: no bend";

  options.mutable_rate_limiter_plugin_config()->set_name(
      "nighthawk.linear-ramping-rate-limiter-plugin");
  EXPECT_TRUE(RpsTurnBendsRamp(options));

  // A DIFFERENT named limiter plugin: the engine would not treat the rps
  // delta as a bend — the record must not either.
  nighthawk::client::CommandLineOptions other;
  other.mutable_rate_limiter_plugin_config()->set_name("nighthawk.some-other-limiter-plugin");
  EXPECT_FALSE(RpsTurnBendsRamp(other));

  // A present-but-empty name is unconfigured (the pane's own reading).
  nighthawk::client::CommandLineOptions empty_name;
  empty_name.mutable_rate_limiter_plugin_config()->set_name("");
  EXPECT_FALSE(RpsTurnBendsRamp(empty_name));
}

// ---------------------------------------------------------------------------
// The legacy-display honesty fix: the pane's STARTING acked rps is the
// GLOBAL total the engine's live baseline is built from
// (ComputeCurrentScheduleDelta's construction semantics). On a legacy or
// plugin run every worker's limiter is constructed at the top-level flag
// PER WORKER, so the display seed is flag x workers; on a phase run the
// declared phase rps is already global. Seeding the bare flag made a no-op
// knob turn at the displayed value an N-fold load cut on a legacy run.
// ---------------------------------------------------------------------------

TEST(SeedGlobalRpsTest, LegacyRunSeedsFlagTimesWorkers) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/1), 100);
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/4), 400);
  // A zero worker count floors at one worker (the engine's rule).
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/0), 100);
}

TEST(SeedGlobalRpsTest, LegacyOverflowClampsAtUint32Max) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(1000000);
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/100000),
            std::numeric_limits<uint32_t>::max());
}

TEST(SeedGlobalRpsTest, PhaseRunSeedsTheDeclaredPhaseTotalNotTheFlag) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  auto* phase = options.add_phases();
  phase->set_name("spike");
  phase->set_requests_per_second(500);
  auto* sustain = options.add_phases();
  sustain->set_name("sustain");
  sustain->set_requests_per_second(300);
  // Phase 0 is the current phase at seed time; its declared rps is the
  // global baseline (the flag is not).
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/8), 500);
}

TEST(SeedGlobalRpsTest, PhaseWithUnsetRpsFallsBackToTheFlag) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  auto* phase = options.add_phases();
  phase->set_name("coast");
  // The engine's fallback: a phase that leaves rps unset reads the flag.
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/4), 100);
}

TEST(SeedGlobalRpsTest, NoConfiguredRpsSeedsNothing) {
  nighthawk::client::CommandLineOptions options;
  EXPECT_EQ(SeedGlobalRpsBaseline(options, /*worker_count=*/4), 0);
}

// The flow pin: a legacy 4-worker run's pane displays the TOTAL (flag x
// workers), never the bare flag; a phase run's display stays the declared
// global. startRun seeds the tracker before the feed's start attempt, so
// the acked state is checkable over an unroutable channel — the seed is
// what the pane shows in the failed-start state too.
TEST(SeedGlobalRpsTest, ControllerSeedsTheLegacyTotalAndThePhaseGlobal) {
  auto channel = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
  nighthawk::client::StartRequest request;
  auto* options = request.mutable_options();
  options->mutable_requests_per_second()->set_value(100);
  options->mutable_concurrency()->set_value("4");
  options->mutable_uri()->set_value("http://127.0.0.1:1/x");
  {
    RunController controller(channel, tempPath("seed-legacy"));
    (void)controller.startRun(request);
    EXPECT_EQ(controller.rpsTurnState().acked_value, "400");
    const std::string text =
        screenText(RenderLiveKnobTurns(controller.rpsTurnState(), 120, 5), 120, 5);
    EXPECT_NE(text.find("400"), std::string::npos) << text;
    EXPECT_NE(text.find("(total)"), std::string::npos) << text;
    EXPECT_EQ(text.find("100 ("), std::string::npos)
        << "the bare flag must not be the displayed operating point:\n"
        << text;
  }
  // Phase run: the seed is the declared phase rps (the global total), not
  // the flag — the display language is unchanged there.
  auto* phase = options->add_phases();
  phase->set_name("spike");
  phase->set_requests_per_second(500);
  options->clear_concurrency();
  {
    RunController controller(channel, tempPath("seed-phase"));
    (void)controller.startRun(request);
    EXPECT_EQ(controller.rpsTurnState().acked_value, "500");
  }
}

// ---------------------------------------------------------------------------
// Fix round: the failed-pending surfacing must survive the run's END. The
// shell's frame gate used to read runInProgress(); the feed fires its run-end
// pending surfacing exactly when that flag is gone, so the failed frame never
// reached the screen. The gate now reads the TRACKER (the pure
// LiveKnobTurnSurfaceable predicate): the frame renders whenever the tracker
// has anything to say — including a failed-pending outcome that landed after
// on_finished.
// ---------------------------------------------------------------------------

TEST(ShellGateTest, FailedPendingTurnStaysSurfaceableAfterRunEnd) {
  // The pristine (never-seeded) tracker has nothing to say: the gate stays
  // closed and the knobs pane renders the plain vocabulary only.
  KnobTurnTracker pristine;
  EXPECT_FALSE(LiveKnobTurnSurfaceable(pristine.rps()));

  KnobTurnTracker tracker;
  tracker.SeedAckedValue(100);
  EXPECT_TRUE(LiveKnobTurnSurfaceable(tracker.rps())) << "a seeded acked value is surfaceable";
  tracker.BeginRpsTurn(300);
  EXPECT_TRUE(LiveKnobTurnSurfaceable(tracker.rps())) << "a pending turn is surfaceable";

  // The run ends with the turn un-acked: the feed-generated result resolves
  // it failed-pending (this is the exact state the old run-in-flight gate
  // hid). The gate must stay OPEN, and the frame the shell paints must carry
  // the marker AND the reason.
  tracker.OnUpdateResult(rejectedAck("run-feed: 1 update(s) were still pending when the "
                                     "stream ended; the service never acked them"));
  ASSERT_EQ(tracker.rps().state, KnobTurnState::kFailedPending);
  EXPECT_TRUE(LiveKnobTurnSurfaceable(tracker.rps()))
      << "a failed-pending outcome must stay visible after run end";

  OverlayState overlay;
  const ftxui::Element frame =
      RenderLayout(100, 30, /*active_tab=*/2, InputMode::kNavigate, RunViewModel{},
                   StartRunDialogModel{}, /*session_path=*/"",
                   nighthawk::client::CommandLineOptions{}, &overlay,
                   /*flame=*/nullptr, /*profiler_state=*/"",
                   /*timeline=*/nullptr, /*staged_preview=*/nullptr,
                   /*staged_workers_annotation=*/nullptr, /*boundary_stamps=*/nullptr,
                   /*rps_turn=*/&tracker.rps());
  const std::string text = screenText(frame, 100, 30);
  EXPECT_NE(text.find("[failed]"), std::string::npos)
      << "the failed marker must survive the run end:\n"
      << text;
  // The reason survives the run end; at the declared minimum width the long
  // reason renders with its END intact (the width discipline keeps the tail —
  // the reason's last words and the dismissal affordance), so the asserted
  // phrase is one the tail carries.
  EXPECT_NE(text.find("never acked them"), std::string::npos)
      << "the failure reason must survive the run end:\n"
      << text;

  // Dismissal closes the surface honestly: back to the acked line (which
  // stays, the acked value is real), never an empty pane.
  tracker.DismissOutcome();
  EXPECT_EQ(tracker.rps().state, KnobTurnState::kCurrentAcked);
  EXPECT_TRUE(LiveKnobTurnSurfaceable(tracker.rps()));
}

// ---------------------------------------------------------------------------
// Fix round: the knob-turn draft echo. The kEditingKnob draft used to be
// invisible — the operator typed a live steering value blind. The draft line
// renders on the knobs pane while the mode owns the keyboard: knob name,
// typed value, and the pending-send affordance.
// ---------------------------------------------------------------------------

TEST(KnobDraftEchoTest, TypingRendersTheDraftOnTheKnobsPane) {
  OverlayState overlay;
  overlay.knob_draft = "300";

  const ftxui::Element frame =
      RenderLayout(100, 30, /*active_tab=*/2, InputMode::kEditingKnob, RunViewModel{},
                   StartRunDialogModel{}, /*session_path=*/"",
                   nighthawk::client::CommandLineOptions{}, &overlay);
  const std::string text = screenText(frame, 100, 30);
  EXPECT_NE(text.find("rps turn draft"), std::string::npos)
      << "the draft line must name the knob:\n"
      << text;
  EXPECT_NE(text.find("300"), std::string::npos) << "the typed value must echo:\n" << text;
  EXPECT_NE(text.find("enter sends"), std::string::npos)
      << "the pending-send affordance must be visible:\n"
      << text;

  // An empty draft stays honest: the line renders, showing nothing typed yet.
  overlay.knob_draft.clear();
  const std::string empty_text =
      screenText(RenderLayout(100, 30, 2, InputMode::kEditingKnob, RunViewModel{},
                              StartRunDialogModel{}, "", nighthawk::client::CommandLineOptions{},
                              &overlay),
                 100, 30);
  EXPECT_NE(empty_text.find("rps turn draft"), std::string::npos) << empty_text;

  // Outside the editing mode the draft line is gone (no stale echo).
  const std::string navigate_text =
      screenText(RenderLayout(100, 30, 2, InputMode::kNavigate, RunViewModel{},
                              StartRunDialogModel{}, "", nighthawk::client::CommandLineOptions{},
                              &overlay),
                 100, 30);
  EXPECT_EQ(navigate_text.find("rps turn draft"), std::string::npos) << navigate_text;
}

TEST(KnobDraftEchoTest, FillOverlayStateCarriesTheKnobDraft) {
  // The seam-crossing check: the shell's per-frame ShellState -> OverlayState
  // mapping must carry the draft, or a state-level draft assertion stays
  // green while the screen stays blind (the re-review finding's shape).
  ShellState state;
  state.mode = InputMode::kEditingKnob;
  state.knob_draft = "300";
  OverlayState overlay;
  FillOverlayState(state, &overlay);
  EXPECT_EQ(overlay.knob_draft, "300");
}

// ---------------------------------------------------------------------------
// Fix round: the turn line's width discipline. The flat tail-chop hid the
// reason and the "(u dismisses)" affordance behind the pane edge exactly when
// the reason was long enough to matter. An overlong line now keeps the HEAD
// (knob name, values, marker) and the TAIL (the reason's end, the dismissal
// affordance) and ellipsizes the middle.
// ---------------------------------------------------------------------------

TEST(LiveTurnWidthTest, OverlongLineKeepsHeadAndTailEllipsizesTheMiddle) {
  LiveKnobTurn failed{"rps", "100", "300", KnobTurnState::kFailedPending,
                      "run-feed: 1 update(s) were still pending when the stream ended; the "
                      "service never acked them (resolved inside the late-rejection drop "
                      "window after run end or accepted cancellation)"};
  const int width = 60;
  const std::string text = screenText(RenderLiveKnobTurns(failed, width, 5), width, 5);
  EXPECT_NE(text.find("rps: 100"), std::string::npos)
      << "the head (name + acked value) must survive:\n"
      << text;
  EXPECT_NE(text.find("(u dismisses)"), std::string::npos)
      << "the tail (dismissal affordance) must survive:\n"
      << text;
  EXPECT_NE(text.find("..."), std::string::npos) << "the middle must be ellipsized:\n" << text;
  EXPECT_EQ(text.find("resolved inside the late-rejection drop window"), std::string::npos)
      << "the middle must actually be cut, not the old flat tail-chop:\n"
      << text;

  // The ellipsized line fits the pane: no rendered row wider than the width.
  int longest = 0;
  for (const std::string& line : screenLines(RenderLiveKnobTurns(failed, width, 5), width, 5)) {
    longest = std::max(longest, static_cast<int>(line.size()));
  }
  EXPECT_LE(longest, width);

  // A line that fits is untouched (this turn line is ~230 chars; render wide).
  const std::string fits =
      screenText(RenderLiveKnobTurns(failed, 300, 5), 300, 5);
  EXPECT_NE(fits.find("resolved inside the late-rejection drop window"), std::string::npos)
      << fits;
  EXPECT_EQ(fits.find("..."), std::string::npos) << "no ellipsis when the line fits:\n" << fits;
}

// ---------------------------------------------------------------------------
// Fix round: knob-flow notes route to the knobs pane too. NoteFlameStatus
// renders only on the flame tab; a u/e refusal on the knobs tab was invisible
// exactly where the key was pressed. The knobs pane carries the same note.
// ---------------------------------------------------------------------------

TEST(KnobNoteRoutingTest, KnobsTabRendersTheStatusNote) {
  FlamePaneState flame;
  flame.note = "no run to steer: u turns rps while a run is live";
  const ftxui::Element frame =
      RenderLayout(100, 30, /*active_tab=*/2, InputMode::kNavigate, RunViewModel{},
                   StartRunDialogModel{}, /*session_path=*/"",
                   nighthawk::client::CommandLineOptions{}, /*overlay=*/nullptr, &flame);
  const std::string text = screenText(frame, 100, 30);
  EXPECT_NE(text.find("no run to steer: u turns rps while a run is live"), std::string::npos)
      << "the note must render on the knobs pane too:\n"
      << text;

  // No note: no stray line (the empty note renders nothing).
  const std::string quiet =
      screenText(RenderLayout(100, 30, 2, InputMode::kNavigate, RunViewModel{},
                              StartRunDialogModel{}, "", nighthawk::client::CommandLineOptions{},
                              /*overlay=*/nullptr, /*flame=*/nullptr),
                 100, 30);
  EXPECT_EQ(quiet.find("no run to steer"), std::string::npos);
}

// ---------------------------------------------------------------------------
// The M3 pane header: the display-only banner is GONE; the global-total
// semantics render on the pane. The rps language is the total across the
// active workers — true on every run shape now that the seed is the
// construction-honest global (flag x workers on a legacy run, the declared
// phase total on a phase run); the legacy "phase total" vocabulary is gone.
// ---------------------------------------------------------------------------

TEST(KnobPaneBannerTest, BannerGoneAndTotalAcrossWorkersStated) {
  nighthawk::client::CommandLineOptions options;
  options.mutable_requests_per_second()->set_value(100);
  const std::string text = screenText(RenderKnobPane(KnobsFromOptions(options), 100, 10), 100, 10);
  EXPECT_EQ(text.find("display-only"), std::string::npos) << text;
  EXPECT_EQ(text.find("steering lands in M3"), std::string::npos) << text;
  EXPECT_NE(text.find("total across the active workers"), std::string::npos) << text;
  EXPECT_EQ(text.find("phase total"), std::string::npos)
      << "the rps language is the total across workers on every shape:\n"
      << text;
  EXPECT_NE(text.find("LIVE"), std::string::npos) << text;
}

} // namespace
} // namespace Tui
} // namespace Client
} // namespace Nighthawk
