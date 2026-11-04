#pragma once

#include <semaphore.h>
#include <signal.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <fcntl.h>
#endif

#include <atomic>
#include <functional>
#include <string>
#include <vector>

#include "absl/container/node_hash_map.h"
#include "absl/strings/string_view.h"

#include "nighthawk/client/options.h"
#include "nighthawk/common/uri.h"

#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "source/common/common/posix/thread_impl.h"
#include "source/common/common/statusor.h"
#include "source/common/event/dispatcher_impl.h"
#include "source/common/network/dns_resolver/dns_factory_util.h"

#include "source/common/uri_impl.h"

#include "api/client/options.pb.h"

namespace Nighthawk {

// FORK-ONLY (We-Amp): per-phase resource limits (M2 A1 Task 5). The three
// in-flight ceilings a phase declares (or inherits from the run-level options
// when a field is absent) drive both the breaker bootstrap (at the max across
// phases) and the client-side gates (per phase).
struct PhaseResourceLimits {
  uint32_t connections;
  uint32_t max_pending_requests;
  uint32_t max_active_requests;
};

/**
 * Effective resource limits for one phase: the PhaseSpec's declared value
 * when present, the run-level option's value inherited when absent.
 * `phase == nullptr` yields the run-level limits themselves.
 */
PhaseResourceLimits phaseResourceLimits(const Client::Options& options,
                                        const nighthawk::client::PhaseSpec* phase);

/**
 * The max of each limit across the phase vector (run-level limits for legacy
 * vector-less runs): the values the circuit breakers bootstrap with. The
 * clusters are built once at bootstrap and never touched again at phase
 * boundaries, so every phase's ceiling can never exceed the bootstrapped
 * breaker capacity.
 */
PhaseResourceLimits maxPhaseResourceLimits(const Client::Options& options);

/**
 * Raise-only runtime breaker overrides (M2 Task 1 gate verdict: FEASIBLE-WITH-
 * CAVEATS -- the override REPLACES the effective value, so the raise-only rule
 * is caller-enforced). Produces Envoy runtime entries for cluster
 * `cluster_name` (the worker target clusters are named "0", "1", ...;
 * `circuit_breakers.<cluster>.default.max_connections|max_pending_requests|
 * max_requests`) wherever `desired_limits` exceeds `bootstrap_limits`. Values
 * at or below the bootstrap max produce no entry: merging them would be a
 * no-op at best, and a lowering attempt is refused by omission -- the
 * bootstrapped breaker capacity is never undercut.
 */
absl::node_hash_map<std::string, std::string>
raiseOnlyBreakerOverrides(absl::string_view cluster_name,
                          const PhaseResourceLimits& bootstrap_limits,
                          const PhaseResourceLimits& desired_limits);

/**
 * Creates Envoy bootstrap configuration.
 *
 * The created bootstrap configuration can be used to upstream requests to the
 * specified uris.
 *
 * @param dispatcher is used when resolving hostnames to IP addresses in the
 * bootstrap.
 * @param options are the options this Nighthawk execution was triggered with.
 * @param dns_resolver_factory used to create a DNS resolver to resolve hostnames
 * in the bootstrap.
 * @param typed_dns_resolver_config config used when creating dns_resolver_factory,
 * also needed when creating the resolver.
 * @param number_of_workers indicates how many Nighthawk workers will be
 *        upstreaming requests. A separate cluster is generated for each worker.
 *
 * @return the created bootstrap configuration.
 */
absl::StatusOr<envoy::config::bootstrap::v3::Bootstrap> createBootstrapConfiguration(
    Envoy::Event::Dispatcher& dispatcher, Envoy::Api::Api& api, const Client::Options& options,
    Envoy::Network::DnsResolverFactory& dns_resolver_factory,
    const envoy::config::core::v3::TypedExtensionConfig& typed_dns_resolver_config,
    int number_of_workers);

/**
 * Creates Encapsulation envoy bootstrap configuration.
 *
 * This envoy receives traffic and encapsulates it HTTP
 *
 * @param options are the options this Nighthawk execution was triggered with.
 * @param tunnel_uri URI to the terminating proxy.
 * @param dispatcher is used when resolving hostnames to IP addresses in the
 * bootstrap.
 * @param resolver bootstrap resolver
 *
 * @return the created bootstrap configuration.
 */
absl::StatusOr<envoy::config::bootstrap::v3::Bootstrap>
createEncapBootstrap(const Client::Options& options, UriImpl& tunnel_uri,
                     Envoy::Event::Dispatcher& dispatcher,
                     const Envoy::Network::DnsResolverSharedPtr& resolver);

class EncapsulationSubProcessRunner {
public:
  /**
   * Forks a separate process for Envoy. Both nighthawk and envoy are required to be their own
   * processes
   *
   * @param nighthawk_runner executes nighthawk's workers in current process
   * @param encap_envoy_runner starts up Encapsulation Envoy in a child process.
   * This takes a blocked semaphore which it is responsible for signalling and allowing
   * nighthawk_runner to execute once envoy is ready to serve.
   * Once nighthawk_runner finishes executing, encap_envoy_runner receives a SIGTERM
   *
   */
  EncapsulationSubProcessRunner(std::function<void()> nighthawk_runner,
                                std::function<void(sem_t&)> encap_envoy_runner)
      : nighthawk_runner_(nighthawk_runner), encap_envoy_runner_(encap_envoy_runner) {
#if defined(__APPLE__)
    // macOS does not implement unnamed process-shared semaphores: sem_init() with
    // pshared=1 fails with ENOSYS. Use a named semaphore instead. Because it is
    // opened before fork(), the child inherits the handle and refers to the same
    // semaphore object, preserving the cross-process signalling semantics.
    // The name is unique per process and per construction within the process; on
    // an (unexpected) name collision, retry with a bumped counter, bounded.
    static std::atomic<int> name_counter{0};
    sem_t* sem = SEM_FAILED;
    std::string name;
    for (int attempt = 0; attempt < 3; ++attempt) {
      name = "/nighthawk." + std::to_string(getpid()) + "." + std::to_string(name_counter++);
      sem = sem_open(name.c_str(), O_CREAT | O_EXCL, /*mode=*/0600, /*value=*/0);
      if (sem != SEM_FAILED) {
        break;
      }
      if (errno != EEXIST) {
        break;
      }
    }
    if (sem == SEM_FAILED) {
      throw NighthawkException("Could not initialise semaphore");
    }
    nighthawk_control_sem_ = sem;
    semaphore_name_ = std::move(name);
#else
    nighthawk_control_sem_ = static_cast<sem_t*>(
        mmap(NULL, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));

    // create blocked semaphore for nighthawk to wait on
    int ret = sem_init(nighthawk_control_sem_, /*pshared=*/1, /*count=*/0);
    if (ret != 0) {
      throw NighthawkException("Could not initialise semaphore");
    }
#endif
  };

  ~EncapsulationSubProcessRunner() {
    auto status = TerminateEncapSubProcess();
    if (!status.ok()) {
      ENVOY_LOG_MISC(warn, status.ToString());
    }
#if defined(__APPLE__)
    // Only the process that created the named semaphore cleans it up: the forked
    // child merely inherits the open handle and exits via _exit(). The child is
    // reaped before unlinking, so no other process can still be using the name.
    if (pid_ != 0) {
      sem_close(nighthawk_control_sem_);
      sem_unlink(semaphore_name_.c_str());
    }
#else
    if (pid_ == 0) {
      // Have only parent process destroy semaphore
      sem_destroy(nighthawk_control_sem_);
      munmap(nighthawk_control_sem_, sizeof(sem_t));
    }
#endif
  }
  /**
   * Run functions in parent and child processes. It blocks until nighthawk_runner
   * returns.
   *
   * @return error status for processes
   **/
  absl::Status Run() { return RunWithSubprocess(nighthawk_runner_, encap_envoy_runner_); }

  /**
   * Sends a SIGTERM to the Encap Envoy subprocess and reaps it. Waits up to ~15 seconds
   * for a graceful exit, then escalates to SIGKILL: an unbounded wait here could wedge the
   * caller (for example a gRPC service handler running ProcessImpl::shutdown()) if the
   * subprocess never exits.
   *
   **/
  absl::Status TerminateEncapSubProcess() {
    Envoy::Thread::LockGuard guard(terminate_mutex_);
    if (pid_ == -1 || pid_ == 0) {
      return absl::OkStatus();
    }

    if (kill(pid_, SIGTERM) == -1 && errno != ESRCH) {
      return absl::InternalError("Failed to kill encapsulation subprocess");
    }

    int status = 0;
    pid_t reaped = 0;
    for (int i = 0; i < 150 && reaped == 0; ++i) {
      reaped = waitpid(pid_, &status, WNOHANG);
      if (reaped == 0) {
        // Waiting on a real OS process; there is no injectable time source here.
        usleep(100000); // 100ms per attempt, ~15s in total. NO_CHECK_FORMAT(real_time)
      }
    }
    if (reaped == 0) {
      kill(pid_, SIGKILL);
      reaped = waitpid(pid_, &status, 0);
    }
    pid_ = -1;

    if (reaped < 0) {
      // Nothing to reap (e.g. the child was already reaped elsewhere): not a crash.
      return absl::OkStatus();
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      // Child process did not crash.
      return absl::OkStatus();
    }

    // Child process crashed or had to be hard-killed.
    return absl::InternalError(absl::StrCat("Envoy crashed with code: ", status));
  }

private:
  absl::Status RunWithSubprocess(std::function<void()> nighthawk_runner,
                                 std::function<void(sem_t&)> encap_envoy_runner);

  std::function<void()> nighthawk_runner_;
  std::function<void(sem_t&)> encap_envoy_runner_;
  pid_t pid_ = -1;
  sem_t* nighthawk_control_sem_;
#if defined(__APPLE__)
  std::string semaphore_name_;
#endif
  Envoy::Thread::MutexBasicLockable terminate_mutex_;
};

/**
 * Spins function into thread
 *
 * @param thread_routine executes nighthawk's workers
 *
 * @return thread pointer
 */
Envoy::Thread::PosixThreadPtr createThread(std::function<void()> thread_routine);

} // namespace Nighthawk
