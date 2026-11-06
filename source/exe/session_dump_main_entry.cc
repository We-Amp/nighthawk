// FORK-ONLY (We-Amp): entry point for the nighthawk_session_dump binary, the
// headless reader for .nhsession files. Prints one JSON line per record; the
// record's oneof discloses its type naturally ("header", "metricSeries",
// "runEnd" or — since M1 — "profileWindow"). Forward-compat asymmetry (M1):
// this reader reads pre-M1 files fine; a PRE-M1 reader/dump hitting a file
// that carries profile_window records FAILS at the parked RECORD_NOT_SET
// rejection — unknown cases are never silently skipped.
#include <iostream>

#include "google/protobuf/util/json_util.h"
#include "source/client/headless/session_reader.h"
#include "tclap/CmdLine.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

int runSessionDump(int argc, const char* const* argv, std::ostream& err) {
  TCLAP::CmdLine cmd("nighthawk_session_dump: print a .nhsession file as JSON lines", ' ', "1.0");
  TCLAP::ValueArg<std::string> session("", "session", "Path to the .nhsession session file.", true,
                                       "", "string", cmd);
  cmd.setExceptionHandling(false);
  try {
    cmd.parse(argc, argv);
  } catch (const TCLAP::ArgException& e) {
    err << e.what() << "\n";
    return 1;
  } catch (const TCLAP::ExitException&) {
    // --help or --version: the output has already been written by TCLAP.
    return 0;
  }

  auto reader_or = SessionReader::open(session.getValue());
  if (!reader_or.ok()) {
    err << reader_or.status() << "\n";
    return 1;
  }
  std::unique_ptr<SessionReader> reader = std::move(*reader_or);
  for (;;) {
    auto next = reader->next();
    if (!next.ok()) {
      err << next.status() << "\n";
      return 1;
    }
    if (!next->has_value()) {
      break;
    }
    std::string json;
    google::protobuf::util::JsonPrintOptions options;
    // Print default-valued fields too (M1 final fixes): a profileWindow's
    // interval_source = FETCH_ORCHESTRATION and clock_offset_uncertainty_ns
    // = 0 are exactly the values a reader needs to see — proto3 JSON would
    // omit them, and the record's whole point is to say which clock domain
    // it is in.
    options.always_print_fields_with_no_presence = true;
    auto status = google::protobuf::util::MessageToJsonString(**next, &json, options);
    if (!status.ok()) {
      err << status.ToString() << "\n";
      return 1;
    }
    std::cout << json << "\n";
  }
  return 0;
}

} // namespace Tui
} // namespace Client
} // namespace Nighthawk

int main(int argc, char* argv[]) {
  return Nighthawk::Client::Tui::runSessionDump(argc, argv, std::cerr);
}
