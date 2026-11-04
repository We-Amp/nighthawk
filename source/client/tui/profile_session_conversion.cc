#include "source/client/tui/profile_session_conversion.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

nighthawk::client::ProfileWindowRecord ToProfileWindowRecord(
    const ProfileWindow& w, absl::string_view raw_bytes,
    absl::string_view symbolizer_tool) {
  nighthawk::client::ProfileWindowRecord record;
  record.set_profile_type(w.profile_type);
  record.set_start_unix_ns(w.start_unix_ns);
  record.set_end_unix_ns(w.end_unix_ns);
  record.set_purity_fraction(w.purity_fraction);
  record.set_phase_id(w.phase_id);
  record.set_worker_count(w.worker_count);
  record.set_work_units(w.work_units);
  record.set_symbolized(w.symbolized);
  record.set_symbolizer_tool(symbolizer_tool.data(), symbolizer_tool.size());
  record.set_raw_profile(raw_bytes.data(), raw_bytes.size());
  // Clock-domain discipline (M1 final fixes): the record says which clock
  // bounded its interval, and carries the offset uncertainty.
  record.set_interval_source(w.interval_source == WindowIntervalSource::kProfileTimestamps
                                 ? nighthawk::client::WINDOW_INTERVAL_SOURCE_PROFILE_TIMESTAMPS
                                 : nighthawk::client::WINDOW_INTERVAL_SOURCE_FETCH_ORCHESTRATION);
  record.set_clock_offset_uncertainty_ns(w.clock_offset_uncertainty_ns);
  return record;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk
