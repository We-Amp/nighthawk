// FORK-ONLY (We-Amp): the FTXUI-free profile pipeline core (MCP plan Task 4's
// quarantine need): the pipeline configuration, the pool-job input capture,
// and the ONE fetch -> symbolize -> fold -> align pipeline. Moved AS-IS out of
// tui_app.{h,cc} — tui_lib links FTXUI and the nighthawk_mcp graph must not —
// so both consumers (the TUI's RunController and the MCP profile tools) drive
// the same machinery. The TUI consumes it unchanged; the flamegraph pipeline
// suite pins the behavior end-to-end.
//
// Quarantine: admin_client / profile_fetcher / profile_windows / pprof_fold /
// profile_symbolizer / headless deps only — absl, generated protos, POSIX.
// No FTXUI, no @envoy//source C++ targets (see source/client/tui/README.md —
// the same standing guard runs over this library).
#ifndef NIGHTHAWK_TUI_PROFILE_PIPELINE_H_
#define NIGHTHAWK_TUI_PROFILE_PIPELINE_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"

#include "source/client/headless/series_view.h"
#include "source/client/tui/admin_client.h"
#include "source/client/tui/profile_fetcher.h"
#include "source/client/tui/profile_windows.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// The M1 profiling configuration, gathered from the nighthawk_tui flags (plan
// task 6: --admin-base, --profile-retriever, --profile-mount, --scp-host,
// --sut-profile-path, --profile-staging, --pprof-tool, --sut-binary,
// --profile-window-seconds, plus the PPROF_BINARY_PATH environment for the
// symbolizer child).
struct ProfilePipelineConfig {
  enum class RetrieverKind { kVolumeMount, kScp };

  AdminEndpoints admin;
  RetrieverKind retriever_kind = RetrieverKind::kVolumeMount;
  // Retriever: the mount root (bind-mount translation by basename) or the
  // scp destination ("user@host").
  std::string mount_root;
  std::string scp_host;
  // The SUT-side path the profiler writes (admin.profile_path); the
  // retriever fetches this path.
  std::string sut_profile_path;
  // Local staging directory fetched profiles land in.
  std::string staging_dir;
  // The --pprof-tool argv prefix (symbolizer provenance + invocation).
  std::vector<std::string> pprof_tool_argv{"go", "tool", "pprof"};
  // PPROF_BINARY_PATH value for the symbolizer child env (binary
  // reachability); empty = not provided.
  std::string pprof_binary_path;
  // The SUT binary path inside the layout (--sut-binary); empty = no
  // symbolization attempted (the raw-proto fallback path only).
  std::string sut_binary_path;
  // Default profile window length for the p flow (5s; the dialog variant of
  // the plan sketch was collapsed into this fixed default + flag — see the
  // task report).
  int window_seconds = 5;
  // TEST-ONLY seam (M1 final fixes): delays FetchProfile's retrieval inside
  // the pool job so a test can land the run's end inside the retrieval
  // window (the straddle-belt case). Production keeps 0.
  int test_retriever_delay_ms = 0;
  // TEST-ONLY seam (M2 fix round, same pattern): REPLACES the run's captured
  // boundary events for the split alignment, so a wiring test can place the
  // boundary deterministically (fixed offsets from its own t0) instead of
  // racing a live phase run's emit timing. Production keeps it empty — the
  // real intake is the streamed events.
  std::vector<nighthawk::client::PhaseBoundaryEvent> test_boundary_events;
};

// One pool job's captured inputs: everything StopProfileWindowAndFold grabs
// on the steering thread, so the job itself touches only its own copies plus
// the stores. Captured BEFORE the fetch — a run that ends while the fetch
// runs is exactly the excluded-and-marked straddle case.
struct ProfileJobInput {
  ClockAnchor anchor;
  int64_t run_start_unix_ns = 0;
  bool run_finished = false;
  int64_t run_end_unix_ns = 0;
  uint64_t worker_count = 1;
  std::vector<CounterPoint> completed_series;
  // M2 Task 7: the run's streamed boundary events and the phase specs —
  // non-empty events switch the alignment to the split-at-boundary producer
  // (engine intervals joined on the (wall, monotonic) pairs, DQ-3); empty
  // events keep the M1 legacy single-interval path.
  std::vector<nighthawk::client::PhaseBoundaryEvent> boundary_events;
  std::vector<nighthawk::client::PhaseSpec> phase_specs;
  // M3 Task 8: the run's applied knob-epoch change marks (epoch id + the
  // engine (wall, monotonic) pair at each bend), captured from the live
  // snapshots' epoch-change markers. A window straddling one of these
  // splits AT it (spec 8.5), even in a legacy single-phase run.
  std::vector<EpochMark> epoch_marks;
  // The owning run's identity (drive backlog item 4): the session file's
  // path — unique per start_run. Stamped on every produced window so the
  // same-state rule keys on (run, epoch), not the epoch string alone.
  std::string run_key;
};

// The admin Date header's second resolution (the clock-offset uncertainty
// floor; profile_pipeline.cc and the pool jobs).
inline constexpr int64_t kDateGranularityNs = 1'000'000'000;

// One pool job's outcome: the produced windows (pure, and excluded-and-
// marked; a split window yields up to two parts) plus what the session
// records and the flame-pane note need.
struct ProfileJobOutcome {
  std::vector<ProfileWindow> windows;
  // The staged raw profile bytes (for the session record) and the
  // provenance label.
  std::string raw_bytes;
  std::string symbolizer_tool;
  // Human-facing outcome line for the flame pane note.
  std::string note;
};

// ONE pool-job pipeline (plan task 6): the fetched+retrieved staged profile
// becomes an aligned ProfileWindow (pure) or an excluded-and-marked one
// (impure — Review Focus 3 end-to-end). Never crashes on a bad profile and
// never fabricates an interval: the window interval is the parsed profile's
// own stamps when it carries timestamps (the §8.5 sanctioned join key), the
// fetch orchestration's collector boundary otherwise.
//
// Symbolization path: the machine route runs FIRST when a SUT binary is
// configured; the documented raw-proto fallback (LoadProfile on the staged
// file directly) covers proto-native sources — a staged legacy gperftools
// profile without a working symbolizer degrades to no window (the reason
// lands in the outcome note), never a crash.
ProfileJobOutcome RunProfilePipeline(const ProfilePipelineConfig& config,
                                     const FetchedProfile& fetched,
                                     const ProfileJobInput& input,
                                     int64_t clock_offset_uncertainty_ns);

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

#endif // NIGHTHAWK_TUI_PROFILE_PIPELINE_H_
