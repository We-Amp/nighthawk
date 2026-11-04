#include "source/client/tui/knob_pane.h"

#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/numbers.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr const char* kUnsetValue = "unset";
constexpr const char* kRampUnconfigured = "not configured";
constexpr const char* kLiveTag = "LIVE (M3)";
constexpr const char* kPhaseGatedTag = "PHASE-GATED";
constexpr const char* kStagedTag = "STAGED";
constexpr const char* kInheritSuffix = " (inherit)";
// Column the value starts at (the name field's fixed width); the steering tag
// trails the value after a second fixed-width column.
constexpr int kNameColumn = 14;
constexpr int kValueColumn = 44;

const char* steeringTag(KnobSteering steering) {
  switch (steering) {
  case KnobSteering::kLiveM3:
    return kLiveTag;
  case KnobSteering::kPhaseGated:
    return kPhaseGatedTag;
  case KnobSteering::kStaged:
    return kStagedTag;
  }
  return kPhaseGatedTag;
}

// Fixed-width columns keep the knob lines tabular; the steering tag lands at a
// stable column as long as values stay under kValueColumn, and still renders
// readable when one does not.
std::string knobLine(const KnobDisplay& knob) {
  return absl::StrFormat("%-*s%-*s%s", kNameColumn, knob.name, kValueColumn, knob.value,
                         steeringTag(knob.steering));
}

// The staged worker count from a concurrency string, for the worker-set
// annotation. Concurrency may be non-numeric ("auto") — no count, no
// annotation (nullopt).
std::optional<uint32_t> stagedWorkerCount(const nighthawk::client::PhaseSpec& staged) {
  if (!staged.has_concurrency() || staged.concurrency().value().empty()) {
    return std::nullopt;
  }
  const std::string& value = staged.concurrency().value();
  int parsed = 0;
  if (!absl::SimpleAtoi(value, &parsed) || parsed <= 0) {
    return std::nullopt;
  }
  return static_cast<uint32_t>(parsed);
}

} // namespace

std::vector<KnobDisplay> KnobsFromOptions(const nighthawk::client::CommandLineOptions& options) {
  std::vector<KnobDisplay> knobs;

  // Spec §3 live set, knob 1: requests_per_second.
  knobs.push_back({"rps",
                   options.has_requests_per_second()
                       ? absl::StrCat(options.requests_per_second().value())
                       : kUnsetValue,
                   KnobSteering::kLiveM3});

  // Spec §3 live set, knob 2: the ramp, carried by rate_limiter_plugin_config
  // (the linear-ramping-rate-limiter plugin is M3's ramping vehicle). A
  // present-but-empty plugin name is treated the same as absent: the vocabulary
  // has no bare "configured" value.
  if (options.has_rate_limiter_plugin_config()) {
    const std::string& plugin_name = options.rate_limiter_plugin_config().name();
    knobs.push_back({"ramp", plugin_name.empty() ? std::string(kRampUnconfigured) : plugin_name,
                     KnobSteering::kLiveM3});
  } else {
    knobs.push_back({"ramp", kRampUnconfigured, KnobSteering::kLiveM3});
  }

  // Phase-gated set (M2/M3 phase-engine territory).
  knobs.push_back({"concurrency",
                   options.has_concurrency() && !options.concurrency().value().empty()
                       ? options.concurrency().value()
                       : kUnsetValue,
                   KnobSteering::kPhaseGated});
  knobs.push_back(
      {"connections",
       options.has_connections() ? absl::StrCat(options.connections().value()) : kUnsetValue,
       KnobSteering::kPhaseGated});

  // Queue bounds fold the two limiter knobs into one line; each half says
  // "unset" individually so a partially set pair still reads exactly.
  std::string queue_bounds;
  if (!options.has_max_pending_requests() && !options.has_max_active_requests()) {
    queue_bounds = kUnsetValue;
  } else {
    queue_bounds = absl::StrCat(
        "pending=",
        options.has_max_pending_requests() ? absl::StrCat(options.max_pending_requests().value())
                                           : kUnsetValue,
        " active=",
        options.has_max_active_requests() ? absl::StrCat(options.max_active_requests().value())
                                          : kUnsetValue);
  }
  knobs.push_back({"queue bounds", queue_bounds, KnobSteering::kPhaseGated});

  return knobs;
}

ftxui::Element RenderKnobPane(const std::vector<KnobDisplay>& knobs, int width, int height) {
  if (width <= 0 || height <= 0) {
    return ftxui::emptyElement();
  }
  std::vector<ftxui::Element> rows;
  // The display-only banner: the pane states the steering milestone so no
  // hidden timing semantics can masquerade as implemented controls (spec §3).
  rows.push_back(ftxui::text("knobs (display-only; steering lands in M3)"));
  for (const KnobDisplay& knob : knobs) {
    rows.push_back(ftxui::text(knobLine(knob)));
  }
  return ftxui::vbox(std::move(rows));
}

std::vector<KnobDisplay> NextPhasePreviewKnobs(
    const nighthawk::client::PhaseSpec& staged,
    const nighthawk::client::CommandLineOptions& run_options) {
  std::vector<KnobDisplay> preview;
  if (staged.name().empty()) {
    // Nothing staged (the discarded state): no preview lines at all.
    return preview;
  }

  // rps stages preview-only until M3's ack path exists (M2 stages the whole
  // next-phase config; the timeline marks nothing until the engine confirms
  // the boundary).
  preview.push_back({/*name=*/"rps",
                     staged.requests_per_second() > 0
                         ? absl::StrCat(staged.requests_per_second())
                         : absl::StrCat(
                               run_options.has_requests_per_second()
                                   ? run_options.requests_per_second().value()
                                   : 0,
                               kInheritSuffix),
                     KnobSteering::kStaged});

  preview.push_back({/*name=*/"concurrency",
                     staged.has_concurrency() && !staged.concurrency().value().empty()
                         ? staged.concurrency().value()
                         : absl::StrCat(
                               run_options.has_concurrency() &&
                                       !run_options.concurrency().value().empty()
                                   ? run_options.concurrency().value()
                                   : std::string(kUnsetValue),
                               kInheritSuffix),
                     KnobSteering::kStaged});

  // Staged connections value (PhaseSpec field 5, major-1 consistency);
  // unstaged it inherits the run-level value (options.proto's rule).
  preview.push_back(
      {/*name=*/"connections",
       staged.has_connections()
           ? absl::StrCat(staged.connections().value())
           : absl::StrCat(run_options.has_connections()
                              ? absl::StrCat(run_options.connections().value())
                              : std::string(kUnsetValue),
                          kInheritSuffix),
       KnobSteering::kStaged});

  // Queue bounds fold the two limiter knobs into one line; an unstaged half
  // inherits the run-level value.
  const std::string staged_pending = staged.has_max_pending_requests()
                                         ? absl::StrCat(staged.max_pending_requests().value())
                                         : absl::StrCat(
                                               run_options.has_max_pending_requests()
                                                   ? absl::StrCat(
                                                         run_options.max_pending_requests()
                                                             .value())
                                                   : std::string(kUnsetValue),
                                               kInheritSuffix);
  const std::string staged_active = staged.has_max_active_requests()
                                        ? absl::StrCat(staged.max_active_requests().value())
                                        : absl::StrCat(
                                              run_options.has_max_active_requests()
                                                  ? absl::StrCat(run_options.max_active_requests()
                                                                     .value())
                                                  : std::string(kUnsetValue),
                                              kInheritSuffix);
  preview.push_back({/*name=*/"queue bounds",
                     absl::StrCat("pending=", staged_pending, " active=", staged_active),
                     KnobSteering::kStaged});

  return preview;
}

std::string WorkerSetAnnotation(uint32_t current_workers, uint32_t staged_workers,
                                uint32_t next_phase_index) {
  if (staged_workers > current_workers) {
    return absl::StrCat("starts ", staged_workers - current_workers, " new worker(s) (cold)");
  }
  if (staged_workers < current_workers) {
    return absl::StrCat(current_workers - staged_workers, " workers folded into phase ",
                        next_phase_index);
  }
  return "";
}

ftxui::Element RenderNextPhasePreviewBlock(const std::vector<KnobDisplay>& preview_knobs,
                                           absl::string_view workers_annotation) {
  if (preview_knobs.empty()) {
    return ftxui::emptyElement();
  }
  std::vector<ftxui::Element> rows;
  // The header states the preview's contract: next phase only, discardable,
  // nothing marked until the engine confirms the boundary.
  rows.push_back(ftxui::text(
      "[staged: next phase -- applies at the next boundary only; discardable]"));
  for (const KnobDisplay& knob : preview_knobs) {
    rows.push_back(ftxui::text(knobLine(knob)));
  }
  if (!workers_annotation.empty()) {
    rows.push_back(ftxui::text(std::string(workers_annotation)));
  }
  return ftxui::vbox(std::move(rows));
}

ftxui::Element RenderNextPhasePreview(const nighthawk::client::PhaseSpec& staged,
                                      const nighthawk::client::CommandLineOptions& run_options,
                                      uint32_t current_workers, uint32_t next_phase_index,
                                      int width, int height) {
  if (staged.name().empty() || width <= 0 || height <= 0) {
    return ftxui::emptyElement();
  }
  std::vector<ftxui::Element> rows;
  // The header names the staged phase and states the preview's contract:
  // next phase only, discardable, nothing marked until the engine confirms.
  rows.push_back(ftxui::text(absl::StrCat(
      "[staged: ", staged.name(),
      " -- applies at the next phase only; discardable]")));
  for (const KnobDisplay& knob : NextPhasePreviewKnobs(staged, run_options)) {
    rows.push_back(ftxui::text(knobLine(knob)));
  }
  const std::optional<uint32_t> staged_workers = stagedWorkerCount(staged);
  if (staged_workers.has_value()) {
    const std::string workers =
        WorkerSetAnnotation(current_workers, *staged_workers, next_phase_index);
    if (!workers.empty()) {
      rows.push_back(ftxui::text(workers));
    }
  }
  return ftxui::vbox(std::move(rows));
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
