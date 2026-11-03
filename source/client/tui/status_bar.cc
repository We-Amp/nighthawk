#include "source/client/tui/status_bar.h"

#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "ftxui/dom/elements.hpp"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Elapsed run time from the latency series: newest minus oldest monotonic
// stamp, floored to whole seconds. An empty series renders 0s — no snapshot
// has arrived yet.
std::string elapsedOf(const RunViewModel& model) {
  if (model.latency_band.empty()) {
    return "0";
  }
  const uint64_t elapsed_ns =
      model.latency_band.back().monotonic_ns - model.latency_band.front().monotonic_ns;
  return std::to_string(elapsed_ns / 1000000000ull);
}

std::string modeName(InputMode mode) {
  switch (mode) {
  case InputMode::kNavigate:
    return "navigate";
  case InputMode::kEditingStartRun:
    return "editing (start run)";
  }
  return "unknown";
}

} // namespace

ftxui::Element RenderStatusBar(const RunViewModel& model, InputMode mode,
                               absl::string_view session_path, absl::string_view uri) {
  // Row 0: state + elapsed + target uri.
  const std::string row0 =
      absl::StrCat(model.run_state.empty() ? "idle" : model.run_state, " | elapsed ",
                   elapsedOf(model), "s | ", uri.empty() ? "(no target)" : uri);

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

  // Row 2: session path + input mode.
  const std::string row2 = absl::StrCat("session: ", session_path.empty() ? "(none)" : session_path,
                                        " | mode: ", modeName(mode));

  return ftxui::vbox({
      ftxui::text(row0),
      ftxui::text(row1),
      ftxui::text(row2),
  });
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
