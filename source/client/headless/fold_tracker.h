#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): the profile-window lifecycle state machine (MCP plan
// Task 0). Extracted from the TUI's RunController, which tracked a single
// current window with two atomic flags (the "window open <n>s" / "folding..."
// / "idle" status-bar trio); the headless machine keeps those semantics for
// the TUI and adds what a steering client needs: windows are identified by
// SERVER-ASSIGNED monotonic ids (an agent never supplies an index), every
// window's status stays queryable by id after it lands, and a failure is a
// first-class terminal state carrying its reason — a window the pipeline
// "lands nowhere" in the TUI is failed-with-reason here.
//
// Lifecycle: staged (the window is open, collector armed) -> folding (the
// collect boundary passed, the fold job runs) -> ready (windows landed) or
// failed (with the reason). staged -> failed is legal (a collector-arm
// failure); every other skip and every transition out of a terminal state is
// refused. Single-flight: at most one non-terminal window at a time.
//
// Thread-safe: transitions land from the steering thread AND the worker-pool
// job thread.
//
// Dependency quarantine: absl only — no protos, no FTXUI, no Envoy (see
// source/client/tui/README.md — the same standing guard runs over
// client_headless).
class FoldTracker {
public:
  enum class State {
    kStaged,  // open, collector armed, not yet closing
    kFolding, // the fold job is in flight
    kReady,   // landed (windows in the store / session)
    kFailed,  // landed nowhere; failure_reason says why
  };

  struct WindowStatus {
    int64_t id = 0;
    State state = State::kStaged;
    // kFailed only: why the window landed nowhere (validator text, fetch /
    // disable / retrieval / fold failure — verbatim from the pipeline).
    std::string failure_reason;
    // Caller-set terminal note for a ready window (the pipeline's outcome
    // note); empty otherwise.
    std::string note;
  };

  FoldTracker() = default;

  FoldTracker(const FoldTracker&) = delete;
  FoldTracker& operator=(const FoldTracker&) = delete;

  /**
   * Stages a new window and returns its server-assigned id (monotonic from
   * 1). kFailedPrecondition while another window is staged or folding (the
   * single-flight discipline).
   */
  absl::StatusOr<int64_t> Stage();

  /**
   * staged -> folding. kNotFound on an unknown id; kFailedPrecondition from
   * any other state.
   */
  absl::Status MarkFolding(int64_t id);

  /**
   * folding -> ready, with an optional caller-set note. kNotFound on an
   * unknown id; kFailedPrecondition from any other state.
   */
  absl::Status MarkReady(int64_t id, std::string note = "");

  /**
   * staged or folding -> failed, with the reason. kNotFound on an unknown
   * id; kFailedPrecondition from a terminal state.
   */
  absl::Status MarkFailed(int64_t id, std::string reason);

  /**
   * Queryable status per window id. kNotFound on an unknown id.
   */
  absl::StatusOr<WindowStatus> Status(int64_t id) const;

  /**
   * True while a window is staged or folding (the single-flight condition).
   */
  bool busy() const;

  /**
   * The staged (open) window's id, or 0 when none is staged.
   */
  int64_t open_id() const;

  /**
   * The folding window's id, or 0 when none is folding.
   */
  int64_t folding_id() const;

  /**
   * Every window ever staged, ordered by id (history; terminal states stay).
   */
  std::vector<WindowStatus> All() const;

private:
  struct Window {
    int64_t id = 0;
    State state = State::kStaged;
    std::string failure_reason;
    std::string note;
  };

  mutable std::mutex mutex_;
  std::vector<Window> windows_;
  int64_t next_id_ = 1;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
