#include "source/client/tui/layout.h"

#include <algorithm>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ftxui/dom/elements.hpp"
#include "ftxui/dom/node.hpp"
#include "ftxui/screen/box.hpp"
#include "ftxui/screen/screen.hpp"

#include "source/client/tui/charts.h"
#include "source/client/tui/knob_pane.h"
#include "source/client/tui/status_bar.h"

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

// Places `overlay_child` on top of `base` at a fixed (col, row), clearing the
// cells under it first. FTXUI's vbox/hbox cannot express absolute placement,
// and dbox-with-spacers would paint blanks over the base, so this is the one
// custom Node in the render stack: the base renders normally, the overlay
// child renders into the sub-box it was assigned in SetBox.
class OverlayNode : public ftxui::Node {
public:
  OverlayNode(ftxui::Element base, ftxui::Element overlay_child, int col, int row)
      : ftxui::Node({std::move(base), std::move(overlay_child)}), col_(col), row_(row) {}

  void ComputeRequirement() override {
    children_[0]->ComputeRequirement();
    children_[1]->ComputeRequirement();
    requirement_ = children_[0]->requirement();
  }

  void SetBox(ftxui::Box box) override {
    ftxui::Node::SetBox(box);
    children_[0]->SetBox(box);
    const ftxui::Requirement req = children_[1]->requirement();
    overlay_box_.x_min = col_;
    overlay_box_.x_max = col_ + std::max(1, req.min_x) - 1;
    overlay_box_.y_min = row_;
    overlay_box_.y_max = row_ + std::max(1, req.min_y) - 1;
    children_[1]->SetBox(overlay_box_);
  }

  void Render(ftxui::Screen& screen) override {
    children_[0]->Render(screen);
    // Clear under the overlay box (as ftxui's clearunder does), then let the
    // child paint its box.
    for (int y = overlay_box_.y_min; y <= overlay_box_.y_max; ++y) {
      for (int x = overlay_box_.x_min; x <= overlay_box_.x_max; ++x) {
        screen.CellAt(x, y) = ftxui::Cell();
        screen.CellAt(x, y).character = " ";
      }
    }
    children_[1]->Render(screen);
  }

private:
  int col_;
  int row_;
  ftxui::Box overlay_box_;
};

ftxui::Element overlayAt(ftxui::Element base, ftxui::Element overlay_child, int col, int row) {
  return std::make_shared<OverlayNode>(std::move(base), std::move(overlay_child), col, row);
}

// A fixed "inner text / border" row helper for the ASCII-bordered overlays:
// "| " + text padded to inner_width + " |".
std::string borderedRow(const std::string& text, int inner_width) {
  std::string padded = text.substr(0, static_cast<size_t>(inner_width));
  padded.resize(static_cast<size_t>(inner_width), ' ');
  return "| " + padded + " |";
}

std::string borderRule(int outer_width) {
  return "+" + std::string(static_cast<size_t>(outer_width) - 2, '-') + "+";
}

constexpr int kDialogInnerWidth = kDialogOverlayWidth - 4;
constexpr int kDialogLabelWidth = 15; // "duration (s): " fits with one trailing space.

// Greedy word-wrap to the dialog's fixed inner width. The controller's
// rejection text can exceed one row; clipping it silently would hide the
// way out (the q hint), so it wraps onto as many rows as it needs.
std::vector<std::string> wrapRows(const std::string& text, int inner_width) {
  std::vector<std::string> rows;
  size_t start = 0;
  while (start < text.size()) {
    size_t end = std::min(start + static_cast<size_t>(inner_width), text.size());
    if (end < text.size()) {
      // Wrap at the last space inside the window when one exists, so a
      // parenthetical like "(q quits and saves)" stays together. When an
      // opening paren sits inside the window, break right before it instead —
      // the default last-space break can land inside "(q" and split the
      // parenthetical anyway.
      size_t space = text.rfind(' ', end - 1);
      const size_t paren = text.find('(', start);
      if (paren != std::string::npos && paren < end && paren > start && text[paren - 1] == ' ') {
        space = paren - 1;
      }
      if (space != std::string::npos && space > start) {
        end = space;
      }
    }
    rows.push_back(text.substr(start, end - start));
    while (end < text.size() && text[end] == ' ') {
      ++end;
    }
    start = end;
  }
  if (rows.empty()) {
    rows.push_back("");
  }
  return rows;
}

ftxui::Element dialogFieldRow(int field, const StartRunDialogModel& dialog,
                              const OverlayState& overlay) {
  const char* label;
  switch (field) {
  case StartRunDialogModel::kUri:
    label = "uri:";
    break;
  case StartRunDialogModel::kRps:
    label = "rps:";
    break;
  default:
    label = "duration (s):";
    break;
  }

  // The draft value: the live Input components when the shell provides them,
  // else the plain string (tests, corpus capture).
  ftxui::Element value;
  switch (field) {
  case StartRunDialogModel::kUri:
    value = overlay.uri_input ? overlay.uri_input->Render() : ftxui::text(overlay.uri_field);
    break;
  case StartRunDialogModel::kRps:
    value = overlay.rps_input ? overlay.rps_input->Render() : ftxui::text(overlay.rps_field);
    break;
  default:
    value = overlay.duration_input ? overlay.duration_input->Render()
                                   : ftxui::text(overlay.duration_field);
    break;
  }
  if (field == overlay.active_field) {
    // The reverse-video caret: inversion marks the active field, and the
    // "[editing] " row-0 prefix carries the same fact through a monochrome
    // paste.
    value = value | ftxui::inverted;
  }

  // Inline error on the offending field's own row (after a rejected enter).
  // The value column shrinks so the full error text fits inside the border.
  const std::string error =
      overlay.show_errors ? StartRunDialogModel::FieldError(dialog, field) : "";
  int value_width = kDialogInnerWidth - kDialogLabelWidth;
  ftxui::Element error_cell = ftxui::text("");
  if (!error.empty()) {
    // "| " + label + value + " " + error + " |" must stay exactly the border
    // width, so the value column gives up its slack first.
    value_width =
        std::max(3, value_width - 1 - static_cast<int>(error.size()));
    error_cell = ftxui::text(" " + error);
  }
  // The value column is exactly value_width wide (clipped, then padded) so
  // every field row meets the right border in the same column.
  value = value | ftxui::size(ftxui::WIDTH, ftxui::LESS_THAN, value_width) |
          ftxui::size(ftxui::WIDTH, ftxui::GREATER_THAN, value_width);

  return ftxui::hbox({ftxui::text("| "), ftxui::text(absl::StrFormat("%-*s", kDialogLabelWidth, label)),
                      value, error_cell, ftxui::text(" |")});
}

} // namespace

ftxui::Element RenderMinSizeMessage(int width, int height) {
  return ftxui::text(absl::StrCat("terminal too small: Nighthawk TUI needs at least ",
                                  kMinTerminalWidth, "x", kMinTerminalHeight, ", got ", width, "x",
                                  height));
}

ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height) {
  // State-aware empty state: an idle shell has never had a run, so the
  // honest message names the user-initiated next action; only a live run
  // before its first flush may claim "waiting for first flush".
  const bool has_run = !model.run_state.empty() && model.run_state != "idle";
  const char* empty_message = has_run ? "waiting for first flush"
                                      : "no run yet - press n to start one";

  std::vector<ftxui::Element> rows;
  const int band_height = std::max(1, height / 2);
  BandAnnotations annotations;
  annotations.spike = model.spike_frames_remaining > 0;
  annotations.spike_ratio = model.spike_ratio;
  annotations.rescaled = model.latency_rescaled;
  rows.push_back(RenderPercentileBand(model.latency_band, width, band_height, annotations,
                                      empty_message));

  // Sorted counter names keep frames deterministic (the model's hash map
  // iterates in unspecified order).
  std::vector<std::string> names;
  names.reserve(model.counters.size());
  for (const auto& [name, points] : model.counters) {
    names.push_back(name);
  }
  std::sort(names.begin(), names.end());

  const int remaining = std::max(0, height - band_height);
  const int per_counter =
      names.empty() ? 0 : std::max(1, remaining / static_cast<int>(names.size()));
  for (const std::string& name : names) {
    rows.push_back(RenderCounterLine(model.counters.at(name), name, width, per_counter,
                                     empty_message));
  }
  return ftxui::vbox(std::move(rows));
}

ftxui::Element RenderKeyBar(InputMode mode, bool running, bool session_open) {
  // Rendered strictly from the shell's real key map (tui_app's navigate-mode
  // handler and the dialog editor's keys) — nothing here invents a binding.
  // While a session is open the n key cannot succeed; the hint says so
  // instead of advertising a key that would silently refuse (M0b review
  // finding).
  const char* n_hint = session_open ? "n new run (unavailable: session open)" : "n new run";
  std::string bar;
  if (mode == InputMode::kEditingStartRun) {
    bar = "tab field | enter apply | esc cancel";
  } else if (running) {
    bar = absl::StrCat(n_hint, " | c charts | k knobs | s stop | q quit (stops run) | ? help");
  } else {
    bar = absl::StrCat(n_hint, " | c charts | k knobs | q quit | ? help");
  }
  return ftxui::text(bar);
}

ftxui::Element RenderHelpOverlay() {
  const int outer_width = 80;
  const int inner_width = outer_width - 4;
  std::vector<ftxui::Element> lines;
  lines.push_back(ftxui::text(borderRule(outer_width)));
  lines.push_back(ftxui::text(borderedRow("help", inner_width)));
  lines.push_back(ftxui::text(borderedRow("keys (navigate)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  n          open the start-run dialog", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  s          stop the running run (live runs only)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  c / k      switch to the charts / knobs tab", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  q          quit; a running run is stopped first", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  ?          toggle this help (esc closes it too)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("keys (start-run dialog)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  tab        move to the next field", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  enter      apply; invalid fields keep the dialog open", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  esc        cancel and close the dialog", inner_width)));
  lines.push_back(ftxui::text(borderedRow("status-bar fields", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  state      idle | running | completed | stopped", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  elapsed    since the first flush; with the run's", inner_width)));
  lines.push_back(ftxui::text(borderedRow("             duration: 39s/60s plus remaining", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  target     uri under load; (target pending) in the", inner_width)));
  lines.push_back(ftxui::text(borderedRow("             sub-second pre-ack window", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  phase      placeholder until M2 real phase ids", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  epoch      placeholder until M3 steering epochs", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  flush 1s   chart cadence: one column per flush", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  session    session recording path", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  mode       navigate, or editing while a dialog owns", inner_width)));
  lines.push_back(ftxui::text(borderedRow("             the keyboard", inner_width)));
  lines.push_back(ftxui::text(borderRule(outer_width)));
  return ftxui::vbox(std::move(lines));
}

ftxui::Element RenderStartRunDialog(const StartRunDialogModel& dialog,
                                    const OverlayState& overlay) {
  // Border summary: the field-error count rides on the top border ("start
  // run - 2 errors") after a rejected enter. Only field-validation failures
  // land here: a controller rejection renders as a line inside the box, so
  // the border never makes a false "0 errors" claim (M0b final-review
  // finding).
  std::string title = "start run";
  if (overlay.show_errors) {
    int errors = 0;
    for (int field = 0; field < 3; ++field) {
      if (!StartRunDialogModel::FieldError(dialog, field).empty()) {
        ++errors;
      }
    }
    if (errors > 0) {
      title = absl::StrCat("start run - ", errors, " errors");
    }
  }

  std::vector<ftxui::Element> lines;
  lines.push_back(ftxui::text(borderRule(kDialogOverlayWidth)));
  lines.push_back(ftxui::text(borderedRow(title, kDialogInnerWidth)));
  lines.push_back(ftxui::text(borderRule(kDialogOverlayWidth)));
  lines.push_back(dialogFieldRow(StartRunDialogModel::kUri, dialog, overlay));
  lines.push_back(dialogFieldRow(StartRunDialogModel::kRps, dialog, overlay));
  lines.push_back(dialogFieldRow(StartRunDialogModel::kDuration, dialog, overlay));
  // A controller rejection (e.g. a session already open) reads as a distinct
  // line inside the box, wrapped to the border width, until esc or a
  // successful start clears it.
  for (const std::string& row : wrapRows(overlay.start_rejected, kDialogInnerWidth)) {
    if (!row.empty()) {
      lines.push_back(ftxui::text(borderedRow(row, kDialogInnerWidth)));
    }
  }
  lines.push_back(ftxui::text(borderedRow("", kDialogInnerWidth)));
  lines.push_back(ftxui::text(borderedRow("tab: next field  enter: start  esc: cancel", kDialogInnerWidth)));
  lines.push_back(ftxui::text(borderRule(kDialogOverlayWidth)));
  return ftxui::vbox(std::move(lines));
}

ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options,
                            const OverlayState* overlay) {
  // The min-size gate wraps everything: below the declared minimum, render
  // the message and nothing else — no partial status bar, no wrapped charts.
  if (width < kMinTerminalWidth || height < kMinTerminalHeight) {
    return RenderMinSizeMessage(width, height);
  }

  ftxui::Element pane;
  switch (active_tab) {
  case 0:
    // Rows 0..2 are the status bar and the last row is the key bar.
    pane = RenderChartsPane(model, width, height - 4);
    break;
  case 1:
    // Tab 1 is the knob pane, rendered from the most recent run's options
    // (unset defaults before any run). Empty options never render an empty
    // pane: KnobsFromOptions yields the five knobs with "unset"/"not
    // configured" values.
    pane = RenderKnobPane(KnobsFromOptions(knob_options), width, height - 4);
    break;
  default:
    pane = ftxui::text(absl::StrCat("unknown tab ", active_tab));
    break;
  }

  // The key bar is pinned to the bottom row via the filler, so it reads from
  // the same row in every frame and every mode.
  ftxui::Element frame = ftxui::vbox({
      RenderStatusBar(model, mode, session_path, dialog.uri(), knob_options.duration().seconds()),
      pane,
      ftxui::filler(),
      RenderKeyBar(mode, model.run_state == "running", /*session_open=*/!session_path.empty()),
  });

  // While the start-run dialog owns the keyboard, the bordered dialog overlay
  // sits over the pane (the status bar and key bar stay visible). `?` opens
  // the help overlay over the same mechanism in navigate mode.
  if (mode == InputMode::kEditingStartRun) {
    static const OverlayState kDefaultOverlay;
    const OverlayState& state = overlay == nullptr ? kDefaultOverlay : *overlay;
    frame = overlayAt(std::move(frame), RenderStartRunDialog(dialog, state), kDialogOverlayCol,
                      kDialogOverlayRow);
  } else if (overlay != nullptr && overlay->help_open) {
    frame = overlayAt(std::move(frame), RenderHelpOverlay(), kHelpOverlayCol, kHelpOverlayRow);
  }
  return frame;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
