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
  case StartRunDialogModel::kDuration:
    label = "duration (s):";
    break;
  default:
    label = "phases:";
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
  case StartRunDialogModel::kDuration:
    value = overlay.duration_input ? overlay.duration_input->Render()
                                   : ftxui::text(overlay.duration_field);
    break;
  default:
    value = overlay.phases_input ? overlay.phases_input->Render()
                                 : ftxui::text(overlay.phases_field);
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

// Thousands-grouped digits ("27,412") for the completion summary's totals.
std::string groupDigits(uint64_t value) {
  std::string digits = absl::StrCat(value);
  std::string grouped;
  for (size_t i = 0; i < digits.size(); ++i) {
    if (i > 0 && (digits.size() - i) % 3 == 0) {
      grouped.push_back(',');
    }
    grouped.push_back(digits[i]);
  }
  return grouped;
}

// The newest value of a counter series, or 0 when the engine never emitted it
// (zero counters are suppressed engine-side; absence IS zero).
uint64_t lastCounterValue(const RunViewModel& model, const std::string& name) {
  const auto it = model.counters.find(name);
  return it == model.counters.end() || it->second.empty() ? 0 : it->second.back().value;
}

// The completion-summary block's lines (backlog item 7). Totals, error ratio,
// and final percentiles come from the same live view the charts draw (the
// last snapshot's cumulative counters and the band's newest point), with the
// per-class counts rendered alongside per §8.1/§8.2 claim discipline: every
// sample the run produced is counted on-screen, none hidden. The saved
// session path closes the loop on where the recording landed.
std::vector<std::string> CompletionSummaryLines(const RunViewModel& model,
                                                absl::string_view session_path) {
  const uint64_t c1xx = lastCounterValue(model, "benchmark.http_1xx");
  const uint64_t c2xx = lastCounterValue(model, "benchmark.http_2xx");
  const uint64_t c3xx = lastCounterValue(model, "benchmark.http_3xx");
  const uint64_t c4xx = lastCounterValue(model, "benchmark.http_4xx");
  const uint64_t c5xx = lastCounterValue(model, "benchmark.http_5xx");
  const uint64_t cxxx = lastCounterValue(model, "benchmark.http_xxx");
  const uint64_t rst = lastCounterValue(model, "benchmark.stream_resets");
  const uint64_t tmo = lastCounterValue(model, "benchmark.pool_connection_failure");

  const uint64_t requests = c1xx + c2xx + c3xx + c4xx + c5xx + cxxx + rst + tmo;
  const uint64_t errors = c4xx + c5xx + cxxx + rst + tmo;
  const double error_pct =
      requests == 0 ? 0.0 : 100.0 * static_cast<double>(errors) / static_cast<double>(requests);

  std::string first = absl::StrCat("run complete");
  if (!model.latency_band.empty()) {
    const uint64_t elapsed_ns = model.latency_band.back().monotonic_ns -
                                model.latency_band.front().monotonic_ns;
    absl::StrAppend(&first, ": ", elapsed_ns / 1000000000ull, "s");
  }
  absl::StrAppend(&first, " | ", groupDigits(requests), " requests | errors ", errors, " (",
                  absl::StrFormat("%.1f%%", error_pct), ")");
  if (!model.latency_band.empty()) {
    const PercentilePoint& final_point = model.latency_band.back();
    absl::StrAppend(&first, " | p50/p90/p99 ", absl::StrFormat("%.1f", static_cast<double>(final_point.p50_ns) / 1e6),
                    "/", absl::StrFormat("%.1f", static_cast<double>(final_point.p90_ns) / 1e6),
                    "/", absl::StrFormat("%.1f", static_cast<double>(final_point.p99_ns) / 1e6),
                    " ms");
  }

  std::string counts = absl::StrCat("counts: 2xx ", groupDigits(c2xx), ", 4xx ", groupDigits(c4xx),
                                    ", 5xx ", groupDigits(c5xx), ", rst ", groupDigits(rst),
                                    ", t/o ", groupDigits(tmo));
  const uint64_t other = c1xx + c3xx + cxxx;
  if (other > 0) {
    absl::StrAppend(&counts, ", other ", groupDigits(other));
  }
  absl::StrAppend(&counts, " - every sample counted, none hidden");

  std::string saved = absl::StrCat("recording saved: ",
                                   session_path.empty() ? "(none)" : std::string(session_path));

  return {first, counts, saved};
}

ftxui::Element RenderChartsPane(const RunViewModel& model, int width, int height,
                                absl::string_view session_path, uint64_t pool_pending_cap,
                                const std::vector<uint64_t>* boundary_stamps) {
  // State-aware empty state: an idle shell has never had a run, so the
  // honest message names the user-initiated next action; only a live run
  // before its first flush may claim "waiting for first flush".
  const bool has_run = !model.run_state.empty() && model.run_state != "idle";
  const char* empty_message = has_run ? "waiting for first flush"
                                      : "no run yet - press n to start one";

  std::vector<ftxui::Element> rows;
  int used_rows = 0;

  // The completion-summary block (backlog item 7): on transition to
  // completed, totals, error ratio, final percentiles, the saved session
  // path, and the §8.2 disclosure ride on top; the charts stay intact below.
  if (model.run_state == "completed") {
    for (const std::string& line : CompletionSummaryLines(model, session_path)) {
      rows.push_back(ftxui::text(line.substr(0, static_cast<size_t>(std::max(0, width)))));
      ++used_rows;
    }
  }

  const int band_height = std::max(1, (height - used_rows) / 2);
  BandAnnotations annotations;
  annotations.spike = model.spike_frames_remaining > 0;
  annotations.spike_ratio = model.spike_ratio;
  annotations.rescaled = model.latency_rescaled;
  static const std::vector<uint64_t> kNoBoundaries;
  const std::vector<uint64_t>& boundaries =
      boundary_stamps != nullptr ? *boundary_stamps : kNoBoundaries;
  rows.push_back(RenderPercentileBand(model.latency_band, width, band_height, annotations,
                                      empty_message, boundaries));

  // The compact health line (backlog item 10): the client-side failure
  // counters as per-flush rates, zeros included — a reset or timeout is THE
  // target-drowning signal, and a steady 0/s says "not drowning" outright.
  int remaining = std::max(0, height - used_rows - band_height);
  const int health_rows = std::min(2, remaining);
  if (health_rows > 0) {
    static const std::vector<CounterPoint> kNoSeries;
    rows.push_back(RenderHealthLine(
        model.counters.count("benchmark.http_4xx") ? model.counters.at("benchmark.http_4xx")
                                                   : kNoSeries,
        model.counters.count("benchmark.stream_resets")
            ? model.counters.at("benchmark.stream_resets")
            : kNoSeries,
        model.counters.count("benchmark.pool_connection_failure")
            ? model.counters.at("benchmark.pool_connection_failure")
            : kNoSeries,
        width, health_rows));
    remaining -= health_rows;
  }

  // Sorted counter names keep frames deterministic (the model's hash map
  // iterates in unspecified order).
  std::vector<std::string> names;
  names.reserve(model.counters.size());
  for (const auto& [name, points] : model.counters) {
    names.push_back(name);
  }
  std::sort(names.begin(), names.end());

  const int per_counter =
      names.empty() ? 0 : std::max(1, remaining / static_cast<int>(names.size()));
  for (const std::string& name : names) {
    // The pool-pending series renders its capacity context (backlog item 12)
    // when the run's config sets the gate: cur/cap readout plus the ceiling
    // rule at cap height.
    const uint64_t cap =
        (pool_pending_cap > 0 && name == "pool_rq_pending_release") ? pool_pending_cap : 0;
    rows.push_back(RenderCounterLine(model.counters.at(name), name, width, per_counter, cap,
                                     empty_message));
  }
  return ftxui::vbox(std::move(rows));
}

ftxui::Element RenderKeyBar(InputMode mode, bool running, bool session_open, int active_tab) {
  // Rendered strictly from the shell's real key map (tui_app's navigate-mode
  // handler and the dialog editor's keys) — nothing here invents a binding.
  // While a session is open the n key cannot succeed; the hint says so
  // instead of advertising a key that would silently refuse (M0b review
  // finding). Labels are compressed to the 100-column minimum: with the M1
  // keys the bars stay under the width so nothing clips.
  const char* n_hint = session_open ? "n new run (session open)" : "n new run";
  std::string bar;
  if (mode == InputMode::kEditingStartRun) {
    bar = "tab field | enter apply | esc cancel";
  } else if (mode == InputMode::kEditingSearch) {
    bar = "type to search | enter apply | esc cancel";
  } else if (mode == InputMode::kEditingKnob) {
    // The M3 rps turn: the typed value is the GLOBAL total (the knob speaks
    // global rps; the engine divides it across the active workers at
    // fan-out).
    bar = "digits: new rps (total across workers) | enter: turn | esc: cancel";
  } else if (active_tab == 1) {
    // The flame tab: the pane's own keys (the key map table's flame rows).
    bar = "Arrows nav | Enter zoom | Esc root | / find | p win | d diff | N noise";
    if (running) {
      bar += " | s stop";
    }
    bar += " | q quit | ? help";
  } else if (running) {
    bar = absl::StrCat(n_hint, " | c charts | f flame | k knobs | s stop | q quit (stops run) | "
                               "? help");
  } else {
    bar = absl::StrCat(n_hint, " | c charts | f flame | k knobs | q quit | ? help");
  }
  return ftxui::text(bar);
}

ftxui::Element RenderTabStrip(int active_tab) {
  // One-row tab strip (backlog item 11), rendered under the status bar: the
  // active tab inverted. As of M1 all three panes exist (charts, flame,
  // knobs); nothing renders dimmed anymore. Tab order matches the key map:
  // c -> charts (0), f -> flame (1), k -> knobs (2).
  const bool charts_active = active_tab == 0;
  const bool flame_active = active_tab == 1;
  const bool knobs_active = active_tab == 2;
  return ftxui::hbox({
      ftxui::text("| "),
      charts_active ? ftxui::text("charts") | ftxui::inverted : ftxui::text("charts"),
      ftxui::text(" | "),
      flame_active ? ftxui::text("flame") | ftxui::inverted : ftxui::text("flame"),
      ftxui::text(" | "),
      knobs_active ? ftxui::text("knobs") | ftxui::inverted : ftxui::text("knobs"),
      ftxui::text(" |"),
  });
}

// The navigate-mode key map, as data (see NavigateKeyBindings in the header):
// the single source of truth the help overlay and the key-map test read;
// HandleShellEvent is written against the same table.
const std::vector<KeyBinding>& KeyBindingTable() {
  static const std::vector<KeyBinding>* const table = new std::vector<KeyBinding>{
      {"c", "switch to the charts tab"},
      {"k", "switch to the knobs tab"},
      {"f", "switch to the flamegraph tab"},
      {"n", "open the start-run dialog"},
      {"s", "stop the running run (live runs only)"},
      {"q", "quit; a running run is stopped first"},
      {"?", "toggle help (esc closes it too)"},
      {"Arrows", "flamegraph frame navigation (flame tab)"},
      {"Enter", "zoom to the selected frame (flame tab)"},
      {"Esc/Backspace", "unzoom to the tree root (flame tab)"},
      {"/", "search mode (enter applies, esc cancels)"},
      {"p", "start/stop a profile window"},
      {"d", "differential (default: last two pure)"},
      {"N", "noise-floor view (plain n stays dialog)"},
      {"e", "stage an edit for the next phase (knobs tab; esc discards)"},
      {"u", "turn the rps knob (knobs tab, live run; the total across workers)"},
  };
  return *table;
}

std::vector<KeyBinding> NavigateKeyBindings() { return KeyBindingTable(); }

ftxui::Element RenderHelpOverlay() {
  // Narrowed to 64 columns (batch-2 polish): the overlay sits at column 10,
  // so its right edge clears the charts' right-margin readout pairs (which
  // start near column 76 at the 100-column minimum) — the readouts behind it
  // are no longer fragmented. Height stays within rows 3..28 so the status
  // bar (rows 0-2) and the key bar (row 29) both stay visible — the M1 keys
  // are therefore GROUPED (every binding covered; the key-map test asserts
  // the one-row-per-binding table and the help's coverage of every key).
  const int outer_width = 64;
  const int inner_width = outer_width - 4;
  std::vector<ftxui::Element> lines;
  lines.push_back(ftxui::text(borderRule(outer_width)));
  lines.push_back(ftxui::text(borderedRow("help (? toggles, esc closes)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("keys (navigate)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  c / k / f       charts / knobs / flame tab", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  n               open the start-run dialog", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  d / N           differential | noise-floor view", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  s / q           stop the run | quit (run stops first)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  u               turn the rps knob (the total, live run)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  Arrows          flame frame navigation (flame tab)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  Enter           zoom to the selected frame", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  Esc/Backspace   unzoom to the tree root", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  /               flame search (enter applies)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  p               start/stop a profile window", inner_width)));
  lines.push_back(ftxui::text(borderedRow("keys (start-run dialog): tab next field | enter apply | esc", inner_width)));
  lines.push_back(ftxui::text(borderedRow("status-bar fields", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  state/elapsed  idle|running|completed|stopped; 39s/60s", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  target         uri under load; (target pending) pre-ack", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  phase/epoch    live phase and knob epoch of the latest flush", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  rps semantics  rps values are TOTALS across all workers", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  flush 1s       chart cadence: one column per flush", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  4xx/rst/t/o    per-flush client-side failure rates", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  session        session recording path", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  mode           navigate, or editing (input owns keys)", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  profiler       flame: idle | window open | folding...", inner_width)));
  lines.push_back(ftxui::text(borderedRow("  sut cells      reset-or-label-set marker; hist drops", inner_width)));
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
    for (int field = 0; field < 4; ++field) {
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
  // The M2 phase-vector entry: one field, comma-separated entries
  // (name:rps:duration[:concurrency[:connections]]); a phase run replaces
  // the duration field (mutual exclusion by construction).
  lines.push_back(dialogFieldRow(StartRunDialogModel::kPhases, dialog, overlay));
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
  lines.push_back(ftxui::text(borderedRow("every rps value is the phase total (split across", kDialogInnerWidth)));
  lines.push_back(ftxui::text(borderedRow("the active workers)", kDialogInnerWidth)));
  lines.push_back(ftxui::text(borderRule(kDialogOverlayWidth)));
  return ftxui::vbox(std::move(lines));
}

ftxui::Element RenderLayout(int width, int height, int active_tab, InputMode mode,
                            const RunViewModel& model, const StartRunDialogModel& dialog,
                            absl::string_view session_path,
                            const nighthawk::client::CommandLineOptions& knob_options,
                            const OverlayState* overlay, const FlamePaneState* flame,
                            absl::string_view profiler_state,
                            const std::vector<PhaseTimelineSegment>* timeline,
                            const std::vector<KnobDisplay>* staged_preview,
                            const std::string* staged_workers_annotation,
                            const std::vector<uint64_t>* boundary_stamps,
                            const LiveKnobTurn* rps_turn) {
  // The min-size gate wraps everything: below the declared minimum, render
  // the message and nothing else — no partial status bar, no wrapped charts.
  if (width < kMinTerminalWidth || height < kMinTerminalHeight) {
    return RenderMinSizeMessage(width, height);
  }

  ftxui::Element pane;
  switch (active_tab) {
  case 0:
    // Rows 0..2 are the status bar, row 3 the tab strip, and the last row
    // the key bar.
    pane = RenderChartsPane(model, width, height - 5, session_path,
                            knob_options.has_max_pending_requests()
                                ? static_cast<uint64_t>(knob_options.max_pending_requests().value())
                                : 0,
                            boundary_stamps);
    break;
  case 1: {
    // Tab 1 is the flamegraph pane (M1): the current window's tree through
    // RenderFlamegraph (which applies zoom + collapse from the view state),
    // under a provenance/note header. The honest empty state when no window
    // has landed (or the shell passed no flame state at all).
    static const FlamePaneState kEmptyFlame;
    const FlamePaneState& state = flame == nullptr ? kEmptyFlame : *flame;
    if (state.tree == nullptr) {
      std::vector<ftxui::Element> rows;
      rows.push_back(ftxui::text("flame: no profile window yet") | ftxui::dim);
      rows.push_back(ftxui::text("press p during a run to pull one; d/N compare pure windows") |
                     ftxui::dim);
      if (!state.note.empty()) {
        rows.push_back(ftxui::text(state.note) | ftxui::dim);
      }
      pane = ftxui::vbox(std::move(rows));
    } else {
      std::vector<ftxui::Element> rows;
      rows.push_back(ftxui::text(state.window_header.substr(
          0, static_cast<size_t>(std::max(0, width)))));
      if (!state.note.empty()) {
        rows.push_back(ftxui::text(
            state.note.substr(0, static_cast<size_t>(std::max(0, width)))));
      }
      // The renderer gets the uncollapsed, unzoomed tree + the view state:
      // zoom, collapse, scroll, search and the diff overlay are all applied
      // (and path-translated) inside RenderFlamegraph.
      const int pane_height = height - 5 - static_cast<int>(rows.size());
      rows.push_back(RenderFlamegraph(*state.tree, state.view, state.diff, width,
                                      std::max(1, pane_height)));
      pane = ftxui::vbox(std::move(rows));
    }
    break;
  }
  case 2: {
    // Tab 2 is the knob pane, rendered from the most recent run's options
    // (unset defaults before any run). Empty options never render an empty
    // pane: KnobsFromOptions yields the five knobs with "unset"/"not
    // configured" values. M3: the live rps turn renders above the knob
    // vocabulary with its state marker (acked/pending/rejected/failed);
    // while a run is active and edits are staged, the staged next-phase
    // preview renders below (visually distinct; discardable; never applied
    // to the current phase).
    const std::vector<KnobDisplay> current = KnobsFromOptions(knob_options);
    std::vector<ftxui::Element> rows;
    if (rps_turn != nullptr) {
      rows.push_back(RenderLiveKnobTurns(*rps_turn, width, height - 5));
    }
    // The knob-turn draft echo (fix round): while the u-key editor owns the
    // keyboard, the typed value renders here. The draft is a live steering
    // input — enter sends an irreversible delta to the engine — so the old
    // no-echo state (typing blind behind the inverted "editing" mode cell)
    // was a defect. The line names the knob, echoes the draft, and states
    // the pending-send affordance.
    if (mode == InputMode::kEditingKnob) {
      static const std::string kEmptyDraft;
      const std::string& draft = overlay == nullptr ? kEmptyDraft : overlay->knob_draft;
      rows.push_back(ftxui::text(absl::StrCat("rps turn draft: ", draft,
                                              "  [enter sends / esc cancels]")
                                     .substr(0, static_cast<size_t>(std::max(0, width)))));
    }
    // The controller's transient note (the same line the flame pane renders)
    // routes to the knobs pane too (fix round): knob-flow refusals surface
    // via NoteFlameStatus, which rendered only on tab 1 — invisible exactly
    // where the u/e key was pressed.
    if (flame != nullptr && !flame->note.empty()) {
      rows.push_back(ftxui::text(flame->note.substr(0, static_cast<size_t>(std::max(0, width)))) |
                     ftxui::dim);
    }
    rows.push_back(RenderKnobPane(current, width, height - 5));
    if (staged_preview != nullptr && !staged_preview->empty()) {
      rows.push_back(RenderNextPhasePreviewBlock(*staged_preview,
                                                 staged_workers_annotation == nullptr
                                                     ? ""
                                                     : *staged_workers_annotation));
    }
    pane = ftxui::vbox(std::move(rows));
    break;
  }
  default:
    pane = ftxui::text(absl::StrCat("unknown tab ", active_tab));
    break;
  }

  // The tab strip (backlog item 11) sits between the status bar and the
  // pane; the key bar is pinned to the bottom row via the filler, so it
  // reads from the same row in every frame and every mode.
  ftxui::Element frame = ftxui::vbox({
      RenderStatusBar(model, mode, session_path, dialog.uri(),
                      knob_options.duration().seconds(), profiler_state, timeline, width),
      RenderTabStrip(active_tab),
      pane,
      ftxui::filler(),
      RenderKeyBar(mode, model.run_state == "running", /*session_open=*/!session_path.empty(),
                   active_tab),
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
