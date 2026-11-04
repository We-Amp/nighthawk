#include "source/client/tui/status_bar.h"

#include <algorithm>
#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Elapsed run time from the latency series: newest minus oldest monotonic
// stamp, floored to whole seconds. An empty series renders 0 — no snapshot
// has arrived yet.
uint64_t elapsedSeconds(const RunViewModel& model) {
  if (model.latency_band.empty()) {
    return 0;
  }
  const uint64_t elapsed_ns =
      model.latency_band.back().monotonic_ns - model.latency_band.front().monotonic_ns;
  return elapsed_ns / 1000000000ull;
}

std::string modeName(InputMode mode) {
  switch (mode) {
  case InputMode::kNavigate:
    return "navigate";
  case InputMode::kEditingStartRun:
    return "editing (start run)";
  case InputMode::kEditingSearch:
    return "editing (search)";
  }
  return "unknown";
}

} // namespace

ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri,
                               int duration_seconds) {
  const std::string state =
      model.run_state.empty() ? "idle" : model.run_state;
  const bool left_idle = state != "idle";
  const uint64_t elapsed_s = elapsedSeconds(model);
  const std::string elapsed = std::to_string(elapsed_s);
  const uint64_t remaining =
      duration_seconds > 0 && elapsed_s < static_cast<uint64_t>(duration_seconds)
          ? static_cast<uint64_t>(duration_seconds) - elapsed_s
          : 0;

  // Row 0: state + elapsed/remaining + target uri, under the "[editing] "
  // prefix while a dialog owns the keyboard. The target slot is a hard
  // invariant: "(no target)" only ever reads on idle; a left-idle state
  // always carries the accepted target (or "(target pending)" in the
  // sub-second pre-ack window before the first snapshot confirms it).
  std::string target;
  if (left_idle) {
    if (!model.target_uri.empty()) {
      target = model.target_uri;
    } else if (!uri.empty()) {
      target = std::string(uri);
    } else {
      target = "(target pending)";
    }
  } else {
    target = uri.empty() ? "(no target)" : std::string(uri);
  }
  std::string row0;
  if (mode == InputMode::kEditingStartRun) {
    row0 += "[editing] ";
  }
  absl::StrAppend(&row0, state, " | ");
  if (left_idle && duration_seconds > 0) {
    // The remaining readout steers a live run; a finished run's clock is
    // frozen, so only the progress pair renders there.
    absl::StrAppend(&row0, "elapsed ", elapsed, "s/", duration_seconds, "s");
    if (state == "running") {
      absl::StrAppend(&row0, " | remaining ", remaining, "s");
    }
    absl::StrAppend(&row0, " | ");
  } else if (left_idle) {
    absl::StrAppend(&row0, "elapsed ", elapsed, "s (open-ended) | ");
  } else {
    absl::StrAppend(&row0, "elapsed ", elapsed, "s | ");
  }
  absl::StrAppend(&row0, target);

  // Row 1: phase/epoch placeholder markers + flush cadence. Pre-A1 the phase
  // id is the constant "main" and no steering exists, so both fields are
  // placeholders — labeled as such so nobody mistakes them for real phase
  // attribution (that lands with M2's real phase ids).
  const std::string phase = model.last_snapshot.phase_id();
  const std::string epoch = model.last_snapshot.epoch_id();
  const std::string row1 = absl::StrCat(
      "phase: ", phase.empty() ? "(none)" : phase,
      " [pre-phase-engine placeholder] | epoch: ", epoch.empty() ? "(none)" : epoch,
      epoch.empty() ? "" : " [placeholder]", " | flush ", kStatusBarFlushIntervalSeconds, "s");

  // Row 2: session path + input mode. The mode cell renders inverse-video
  // while an input mode owns the keyboard.
  const std::string row2_prefix =
      absl::StrCat("session: ", session_path.empty() ? "(none)" : session_path, " | mode: ");
  ftxui::Element mode_cell = ftxui::text(modeName(mode));
  if (mode == InputMode::kEditingStartRun) {
    mode_cell = mode_cell | ftxui::inverted;
  }

  return ftxui::vbox({
      ftxui::text(row0),
      ftxui::text(row1),
      ftxui::hbox({ftxui::text(row2_prefix), mode_cell}),
  });
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
