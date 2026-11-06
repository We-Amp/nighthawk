#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "api/client/session.pb.h"

#include "source/client/headless/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// The clock anchor pair (spec §8.5), moved as-is out of tui_app.h: (engine
// wall, engine monotonic) from the FIRST live snapshot of a run. monotonic ~=
// wall - (anchor.wall - anchor.monotonic) maps a wall-clock interval into the
// engine-monotonic domain the completed-work series lives in — and the same
// pair projects a monotonic stamp back onto the wall clock (the M1 pattern
// SeriesView uses).
struct ClockAnchor {
  int64_t wall_time_unix_ns = 0;
  int64_t monotonic_ns = 0;

  bool valid() const { return wall_time_unix_ns != 0 || monotonic_ns != 0; }
};

// The completed-work series for the §8.4 per-unit-of-work denominator, moved
// as-is out of tui_app.{h,cc}: the client-side request counters (the
// benchmark.http_* classes plus stream_resets and pool_connection_failure —
// every sample the run produced counts, per the charts' counts-shown
// disclosure) merged into one ascending monotonic series. At each union stamp
// the merged value is the sum of every source series' last-value-at-or-before
// that stamp (the same convention CompletedWorkInMonotonicRange applies).
// SUT-side series are NOT part of the merge.
std::vector<CounterPoint>
MergeCompletedSeries(const absl::flat_hash_map<std::string, std::vector<CounterPoint>>& counters);

// One point of a named series, fully stamped (MCP plan Task 0): the stamp
// pair the record carries plus the phase and knob epoch the snapshot was
// produced under, and the wall stamp JOINED through the first-snapshot
// anchor — never the record's own wall field for anything but the anchor
// snapshot itself.
struct StampedPoint {
  int64_t monotonic_ns = 0;
  uint64_t value = 0;
  // monotonic_ns + (anchor.wall - anchor.monotonic) — the M1 first-snapshot
  // wall join.
  int64_t wall_time_unix_ns = 0;
  std::string phase_id;
  std::string epoch_id;
};

// The percentile-series names the nighthawk://series resource renders the
// latency band under — the TUI band chart's legend (charts.cc: "# p50
// + p90 . p99 ms"), values in nanoseconds. One series per percentile, the
// same stamps the counters carry.
inline constexpr char kLatencyP50SeriesName[] = "latency.p50";
inline constexpr char kLatencyP90SeriesName[] = "latency.p90";
inline constexpr char kLatencyP99SeriesName[] = "latency.p99";

// One stamped point of the latency band: the p50/p90/p99 triple ONE flush
// carried, with the same stamps as StampedPoint (anchor-joined wall,
// phase id, knob epoch).
struct LatencyBandPoint {
  int64_t monotonic_ns = 0;
  uint64_t p50_ns = 0;
  uint64_t p90_ns = 0;
  uint64_t p99_ns = 0;
  int64_t wall_time_unix_ns = 0;
  std::string phase_id;
  std::string epoch_id;
};

// FORK-ONLY (We-Amp): read-only stamped access to a run's per-series points
// (MCP plan Task 0). Built from the run's persisted MetricSeriesRecords —
// the same records SessionRecorder writes — so a telemetry consumer (the
// MCP server's nighthawk://series resource) reads exactly what the session
// holds. Every counter series the snapshots carried is addressable by name:
// the client benchmark.* classes AND the sut.* scrape series side by side,
// with no preference and no re-aggregation (the raw per-flush values).
//
// Pure and immutable: built once from records, read from anywhere.
//
// Dependency quarantine: generated nighthawk protos and absl only — no
// FTXUI, no @envoy//source C++ targets (see source/client/tui/README.md —
// the same standing guard runs over client_headless).
class SeriesView {
public:
  /**
   * @param records the run's metric-series records, in snapshot order (the
   * first one anchors the wall join).
   */
  explicit SeriesView(std::vector<nighthawk::client::MetricSeriesRecord> records);

  /**
   * Every series name the records carried — client and SUT series alike —
   * ascending.
   */
  std::vector<std::string> SeriesNames() const;

  /**
   * The stamped points of one series, ascending monotonic.
   *
   * @param name the counter name as it appeared in the snapshots
   * (e.g. "benchmark.http_2xx", "sut.cpu_ticks").
   * @return the points, or kNotFound when no record carried that name.
   */
  absl::StatusOr<std::vector<StampedPoint>> Series(absl::string_view name) const;

  /**
   * The run's latency band (the TUI chart's data, rebuilt from the same
   * seam): per flush record, the FIRST statistic with samples — the
   * aggregator's statistics order is unspecified, so the sampled statistic
   * is found by scanning, never assumed at index 0; a flush whose every
   * statistic has count == 0 leaves a band GAP (the zero-count rule — no
   * percentiles are meaningful there, and zeros are never painted). Each
   * point carries the record's p50/p90/p99 triple, stamped like Series().
   */
  absl::StatusOr<std::vector<LatencyBandPoint>> LatencyBand() const;

  /**
   * The first-snapshot anchor pair (invalid when the view is empty).
   */
  const ClockAnchor& anchor() const;

private:
  std::vector<nighthawk::client::MetricSeriesRecord> records_;
  ClockAnchor anchor_;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
