#pragma once

#include <cstdint>
#include <string>

#include "api/client/service.pb.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

// FORK-ONLY (We-Amp): pure state machine behind the `n` start-run dialog.
// Holds the three editable fields, validates them, and builds the
// StartRequest. The FTXUI binding is deliberately thin and lives in Task 6's
// app — this model never touches a terminal, so it stays headless-testable.
class StartRunDialogModel {
public:
  void set_uri(std::string uri);
  void set_rps(uint32_t rps);
  void set_duration_seconds(uint32_t seconds);

  const std::string& uri() const { return uri_; }
  uint32_t rps() const { return rps_; }
  uint32_t duration_seconds() const { return duration_seconds_; }

  // Non-empty uri, rps >= 1, duration >= 1. The dialog's submit affordance
  // must gate on this; build_request() does not re-check.
  bool valid() const;

  // Builds the request from the current fields: uri, requests_per_second and
  // duration land in the CommandLineOptions; live_streaming is opted into
  // (presence is the marker) and stats_flush_interval defaults to 1s. RunFeed
  // re-asserts both as its shared choke point, so a caller that skips this
  // builder still cannot silently lose live streaming.
  nighthawk::client::StartRequest build_request() const;

private:
  std::string uri_;
  uint32_t rps_ = 0;
  uint32_t duration_seconds_ = 0;
};

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
