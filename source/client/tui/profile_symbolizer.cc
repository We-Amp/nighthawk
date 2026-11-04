// FORK-ONLY (We-Amp): implementation of the subprocess symbolizer.
// See profile_symbolizer.h for the contract.
#include "source/client/tui/profile_symbolizer.h"

#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <vector>

#include "absl/strings/str_cat.h"

extern "C" char** environ;

namespace Nighthawk {
namespace Client {
namespace Tui {
namespace {

constexpr char kBinaryPathEnvVar[] = "PPROF_BINARY_PATH";
constexpr size_t kStderrTailBytes = 2048;

// Reads at most the last kStderrTailBytes bytes of the child's stderr capture.
std::string ReadStderrTail(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return "";
  }
  in.seekg(0, std::ios::end);
  const std::streamoff size = in.tellg();
  if (size <= 0) {
    return "";
  }
  const std::streamoff skip =
      size > static_cast<std::streamoff>(kStderrTailBytes)
          ? size - static_cast<std::streamoff>(kStderrTailBytes)
          : 0;
  in.seekg(skip, std::ios::beg);
  std::string tail((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());
  return tail;
}

std::string TempFilePath() {
  const char* dir = std::getenv("TMPDIR");
  std::string tmpl =
      absl::StrCat(dir != nullptr && dir[0] != '\0' ? dir : "/tmp",
                   "/nh-pprof-stderr-XXXXXX");
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  const int fd = mkstemp(buf.data());
  if (fd < 0) {
    return "";
  }
  close(fd);
  return std::string(buf.data());
}

}  // namespace

ProfileSymbolizer::ProfileSymbolizer(std::vector<std::string> tool_argv,
                                     std::string binary_path_env)
    : tool_argv_(std::move(tool_argv)), binary_path_env_(std::move(binary_path_env)) {
  if (tool_argv_.empty()) {
    tool_argv_ = {"go", "tool", "pprof"};
  }
}

ProfileSymbolizer::Invocation ProfileSymbolizer::BuildInvocation(
    const std::vector<std::string>& tool_argv,
    const std::string& binary_path_env, absl::string_view raw_profile_path,
    absl::string_view sut_binary_path, absl::string_view out_path) {
  Invocation inv;
  inv.argv = tool_argv;
  inv.argv.push_back("-proto");
  inv.argv.push_back("-output");
  inv.argv.push_back(std::string(out_path));
  inv.argv.push_back(std::string(sut_binary_path));
  inv.argv.push_back(std::string(raw_profile_path));

  // Child env: the parent's environment, plus the binary reachability path
  // (replacing any inherited value).
  for (char** e = environ; e != nullptr && *e != nullptr; ++e) {
    if (std::strncmp(*e, kBinaryPathEnvVar, sizeof(kBinaryPathEnvVar) - 1) ==
            0 &&
        (*e)[sizeof(kBinaryPathEnvVar) - 1] == '=') {
      continue;
    }
    inv.env.push_back(*e);
  }
  inv.env.push_back(
      absl::StrCat(kBinaryPathEnvVar, "=", binary_path_env));
  return inv;
}

absl::Status ProfileSymbolizer::Symbolize(absl::string_view raw_profile_path,
                                          absl::string_view sut_binary_path,
                                          absl::string_view out_path) {
  const Invocation inv =
      BuildInvocation(tool_argv_, binary_path_env_, raw_profile_path,
                      sut_binary_path, out_path);

  std::vector<char*> argv;
  argv.reserve(inv.argv.size() + 1);
  for (const std::string& arg : inv.argv) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);

  std::vector<std::string> env_storage = inv.env;
  std::vector<char*> envp;
  envp.reserve(env_storage.size() + 1);
  for (const std::string& entry : env_storage) {
    envp.push_back(const_cast<char*>(entry.c_str()));
  }
  envp.push_back(nullptr);

  const std::string stderr_path = TempFilePath();
  if (stderr_path.empty()) {
    return absl::InternalError("cannot create stderr capture file");
  }

  posix_spawn_file_actions_t actions;
  if (posix_spawn_file_actions_init(&actions) != 0) {
    return absl::InternalError("posix_spawn_file_actions_init failed");
  }
  posix_spawn_file_actions_addopen(&actions, STDERR_FILENO,
                                   stderr_path.c_str(),
                                   O_WRONLY | O_CREAT | O_TRUNC, 0600);

  posix_spawnattr_t attr;
  posix_spawnattr_init(&attr);
  // The child must not die on terminal-generated signals the TUI session
  // handles itself; default disposition is fine, but reset any handler the
  // parent inherited so the tool behaves as if launched from a shell.
  sigset_t defaults;
  sigemptyset(&defaults);
  sigaddset(&defaults, SIGINT);
  sigaddset(&defaults, SIGTERM);
  sigaddset(&defaults, SIGHUP);
  posix_spawnattr_setsigdefault(&attr, &defaults);
  posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGDEF);

  pid_t pid = -1;
  const int spawn_rc =
      posix_spawn(&pid, argv[0], &actions, &attr, argv.data(), envp.data());
  posix_spawnattr_destroy(&attr);
  posix_spawn_file_actions_destroy(&actions);
  if (spawn_rc == ENOENT || spawn_rc == EACCES || spawn_rc == ENOTDIR) {
    // Tool absent / not executable: graceful degradation (Review Focus 5).
    return absl::NotFoundError(
        absl::StrCat("pprof tool not runnable: ", argv[0]));
  }
  if (spawn_rc != 0) {
    return absl::InternalError(
        absl::StrCat("posix_spawn failed for ", argv[0], ": ",
                     std::strerror(spawn_rc)));
  }

  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    return absl::InternalError("waitpid failed on pprof tool child");
  }

  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    std::ifstream out_check(std::string(out_path), std::ios::binary);
    if (out_check) {
      return absl::OkStatus();
    }
    return absl::InternalError(absl::StrCat(
        "pprof tool succeeded but produced no output at: ", out_path));
  }

  std::string tail = ReadStderrTail(stderr_path);
  std::string why = WIFEXITED(status)
                        ? absl::StrCat("exit status ", WEXITSTATUS(status))
                        : absl::StrCat("killed by signal ", WTERMSIG(status));
  return absl::InternalError(
      absl::StrCat("pprof tool failed (", why, "): ", tail));
}

}  // namespace Tui
}  // namespace Client
}  // namespace Nighthawk
