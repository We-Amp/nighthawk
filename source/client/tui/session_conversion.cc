#include "source/client/tui/session_conversion.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

nighthawk::client::MetricSeriesRecord
ToMetricSeries(const nighthawk::client::LiveSnapshot& snapshot) {
  nighthawk::client::MetricSeriesRecord record;
  record.set_wall_time_unix_ns(snapshot.wall_time_unix_ns());
  record.set_monotonic_ns(snapshot.monotonic_ns());
  record.set_phase_id(snapshot.phase_id());
  record.set_epoch_id(snapshot.epoch_id());
  (*record.mutable_counters()) = snapshot.counters();
  *record.mutable_statistics() = snapshot.statistics();
  return record;
}

nighthawk::client::SessionHeader ToSessionHeader(const nighthawk::client::StartRequest& request,
                                                 absl::string_view session_id,
                                                 int64_t started_unix_ns) {
  nighthawk::client::SessionHeader header;
  header.set_format_version(kSessionFormatVersion);
  header.set_session_id(session_id.data(), session_id.size());
  header.set_started_unix_ns(started_unix_ns);
  *header.mutable_start_request() = request;
  return header;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
