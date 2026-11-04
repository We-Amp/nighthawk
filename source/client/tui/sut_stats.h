// FORK-ONLY (We-Amp): the SUT stats pane's pure layer — Prometheus scrape
// parsing and scrape-pair deltas (spec §4 Live charts: "SUT stats scraped
// from the admin /stats?format=prometheus; phase-local values computed as
// deltas between phase-aligned scrapes"). Quarantined dependency surface:
// abseil strings + the M0b view-model types only — no Prometheus library
// (quarantine rule).
//
// Everything here is pure: timestamps arrive as parameters (the scrape's
// engine-side wall time, §8.5 clock discipline — never the mac-side arrival
// time), and the scrape cadence that drives the layer is Task 6's TuiApp
// wiring. The output maps onto M0b's chart shapes: delta series are keyed by
// `sut.`-prefixed name as single-point CounterPoint vectors, directly
// mergeable into RunViewModel.counters — where RenderCounterLine already
// renders them in the charts pane. No new renderer, no new pane tab: the SUT
// stats pane IS the charts pane gaining `sut.*` series.

#ifndef SOURCE_CLIENT_TUI_SUT_STATS_H_
#define SOURCE_CLIENT_TUI_SUT_STATS_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "source/client/tui/run_view_model.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// One scrape of the SUT admin endpoint: the engine-side wall time the scrape
// belongs to (from AdminClient::FetchSutUnixTimeNs — the clock surface Task 6
// phase-aligns on) and the /stats?format=prometheus body verbatim.
struct SutScrape {
  int64_t wall_time_unix_ns;
  std::string prom_text;
};

// Parses the Prometheus text exposition into counter values only (name →
// value):
//  - Only samples under a `# TYPE <name> counter` family declaration count.
//    Gauges, histogram families (their `_bucket`/`_sum`/`_count` samples),
//    summaries and untyped samples are excluded — deltas of those are
//    meaningless.
//  - Labels are stripped; a name sampled more than once per scrape (per-label
//    series, e.g. per http_conn_manager or response-code class — observed on
//    the live SUT) is SUMMED: the charts pane shows per-name process totals.
//  - Malformed lines (non-numeric or negative values) are dropped.
//  - `histogram_line_drops` (optional out-param) counts histogram-family
//    sample lines dropped; Task 6 turns it into the one-time throttled note
//    (this layer stays pure — it does not log).
std::map<std::string, uint64_t> ParsePrometheusCounters(
    absl::string_view prom_text, int* histogram_line_drops = nullptr);

// The delta layer's output: per-name series carrying the delta for this
// scrape interval, keyed by the `sut.`-prefixed counter name. The shape is
// RunViewModel.counters' own (name → capped-counter-point vectors), so Task
// 6 merges the entries straight into the store the charts pane renders.
struct SutStatsDelta {
  absl::flat_hash_map<std::string, std::vector<CounterPoint>> series;
  // True when any counter decreased vs the previous scrape (a SUT restart
  // mid-run). No series ever carries a negative delta: the reset series
  // reports the post-reset value for this interval, marked by this flag.
  bool counter_reset_detected = false;
};

// Deltas `curr` against `prev`, parsing both bodies. Points are stamped at
// the CURRENT scrape's engine-side wall time. Rules:
//  - rising counter → the difference;
//  - decreased counter → post-reset value, counter_reset_detected set;
//  - first-appearance counter → its full value (it started at zero within
//    the interval — same convention as RenderHealthLine's first-appearance
//    delta);
//  - disappeared counter → dropped (nothing to delta against).
// Errors kInvalidArgument when the scrapes are out of order (curr's wall
// time not strictly after prev's) — a delta interval needs forward time.
absl::StatusOr<SutStatsDelta> DeltaScrapes(const SutScrape& prev, const SutScrape& curr);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

#endif // SOURCE_CLIENT_TUI_SUT_STATS_H_
