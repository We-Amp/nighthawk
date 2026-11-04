// FORK-ONLY (We-Amp): pprof symbolization for the TUI flamegraph pipeline
// (M1 plan task 3).
//
// Symbolization is SUBPROCESS-ONLY (Global Constraints): this component shells
// out to the `go tool pprof` machine route proven in Task 1 Step 3
// (runbook docs/operations/nighthawk-sut-profiled.md):
//
//   <pprof-tool> -proto -output <out_path> <sut_binary_path> <raw_profile_path>
//
// with PPROF_BINARY_PATH=<build-id-keyed binary layout> in the child env
// (binary reachability). No pprof parsing or DWARF reading happens in-process;
// the only allowlist surface this adds to the TUI quarantine is protobuf's
// gzip stream (see pprof_fold.h) — nothing here.
//
// Budget note: a `-proto` run over a full-DWARF binary is SLOW (~40 minutes
// observed on the staged Envoy binary). Call this from a PprofWorkerPool job,
// never from the render or steering paths.
#ifndef NIGHTHAWK_TUI_PROFILE_SYMBOLIZER_H_
#define NIGHTHAWK_TUI_PROFILE_SYMBOLIZER_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace Client {
namespace Tui {

class ProfileSymbolizer {
 public:
  // The exact argv/env the child process gets — exposed as a test seam so the
  // invocation contract is assertable without spawning a (slow) real tool.
  struct Invocation {
    std::vector<std::string> argv;  // argv[0] is the tool path
    std::vector<std::string> env;   // "KEY=VALUE" entries
  };

  // tool_argv: the tool invocation, default {"go", "tool", "pprof"}; the
  //   --pprof-tool flag (M1 task 6) plumbs its override here.
  // binary_path_env: the PPROF_BINARY_PATH value for the child env.
  ProfileSymbolizer(std::vector<std::string> tool_argv,
                    std::string binary_path_env);

  // Builds the machine-route invocation for the given paths.
  static Invocation BuildInvocation(
      const std::vector<std::string>& tool_argv,
      const std::string& binary_path_env, absl::string_view raw_profile_path,
      absl::string_view sut_binary_path, absl::string_view out_path);

  // Blocking; call from a pool job. Runs the tool subprocess and writes the
  // SYMBOLIZED compressed proto to out_path.
  //   Tool absent / not executable → kNotFound (the caller marks the window
  //     unsymbolized — graceful degradation, never a crash).
  //   Tool failure → kInternal whose text carries the child's stderr tail.
  absl::Status Symbolize(absl::string_view raw_profile_path,
                         absl::string_view sut_binary_path,
                         absl::string_view out_path);

 private:
  std::vector<std::string> tool_argv_;
  std::string binary_path_env_;
};

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk

#endif  // NIGHTHAWK_TUI_PROFILE_SYMBOLIZER_H_
