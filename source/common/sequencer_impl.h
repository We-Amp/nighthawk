#pragma once

#include <functional>

#include "absl/status/status.h"
#include "envoy/common/pure.h"
#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"

#include "nighthawk/common/live_tunable_rate_limiter.h"
#include "nighthawk/common/platform_util.h"
#include "nighthawk/common/rate_limiter.h"
#include "nighthawk/common/sequencer.h"
#include "nighthawk/common/statistic.h"
#include "nighthawk/common/termination_predicate.h"

#include "source/common/common/logger.h"

namespace Nighthawk {

namespace {

using namespace std::chrono_literals;

// We shoot for a 40kHz resolution.
constexpr std::chrono::microseconds NighthawkTimerResolution = 25us;

} // namespace

#define ALL_SEQUENCER_STATS(COUNTER) COUNTER(failed_terminations)

struct SequencerStats {
  ALL_SEQUENCER_STATS(GENERATE_COUNTER_STRUCT)
};

/**
 * The Sequencer will drive calls to the SequencerTarget at a pace indicated by the associated
 * RateLimiter. The contract with the target is that it will call the provided callback when it is
 * ready. The target will return true if it was able to proceed, or false if a retry is warranted at
 * a later time (because of being out of required resources, for example).
 * Note that owner of SequencerTarget must outlive the SequencerImpl to avoid use-after-free.
 * Also, the Sequencer implementation is a single-shot design. The general usage pattern is:
 *   SequencerImpl sequencer(...)
 *   sequencer.start();
 *   sequencer.waitForCompletion();
 */
class SequencerImpl : public Sequencer, public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  SequencerImpl(
      const PlatformUtil& platform_util, Envoy::Event::Dispatcher& dispatcher,
      Envoy::TimeSource& time_source, RateLimiterPtr&& rate_limiter, SequencerTarget target,
      StatisticPtr&& latency_statistic, StatisticPtr&& blocked_statistic,
      nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions idle_strategy,
      TerminationPredicatePtr&& termination_predicate, Envoy::Stats::Scope& scope,
      // FORK-ONLY (We-Amp): M3 Task 3 -- the captured pre-decorator
      // live-tunable handle (may be null: a live update then fails honestly).
      LiveTunableRateLimiter* live_tunable_rate_limiter = nullptr);

  /**
   * Starts the Sequencer. Should be followed up with a call to waitForCompletion().
   */
  void start() override;

  /**
   * Blocking call that waits for the Sequencer flow to terminate. Start() must have been called
   * before this.
   */
  void waitForCompletion() override;

  std::chrono::nanoseconds executionDuration() const override { return rate_limiter_->elapsed(); }

  const RateLimiter& rate_limiter() const override { return *rate_limiter_; }

  double completionsPerSecond() const override {
    const double usec =
        std::chrono::duration_cast<std::chrono::microseconds>(executionDuration()).count();

    return usec == 0 ? 0 : ((targets_completed_ / usec) * 1000000);
  }

  StatisticPtrMap statistics() const override;

  const Statistic& blockedStatistic() const { return *blocked_statistic_; }
  const Statistic& latencyStatistic() const { return *latency_statistic_; }

  // FORK-ONLY (We-Amp): multi-phase support (M2 A1). The Sequencer interface
  // above is untouched. A phase boundary needs the sequencer to stop without
  // taking the worker dispatcher down with it: the phase path calls
  // setExitDispatcherOnStop(false) so stop() leaves the dispatcher's event
  // loop running across the boundary, while the final phase and every legacy
  // run keep today's exact exit-at-stop behavior (the default).
  void setExitDispatcherOnStop(bool exit_dispatcher) { exit_dispatcher_on_stop_ = exit_dispatcher; }

  // FORK-ONLY (We-Amp): invoked on the dispatcher thread at the top of stop(),
  // before the dispatcher is (conditionally) exited. The phase path uses this
  // to chain the boundary drain watch; legacy runs leave it unset.
  void setStopHook(std::function<void(bool timed_out)> hook) { stop_hook_ = std::move(hook); }

  // FORK-ONLY (We-Amp): live rps re-parameterization (M3 Task 3). Posts onto
  // this sequencer's OWN dispatcher (limiter state is dispatcher-confined,
  // M0a Review Focus 5 discipline) and applies at t_bend: a future bend is
  // applied by a one-shot timer firing at the bend instant, an at-or-past
  // bend applies immediately. Bends are NOT queued (Task 4 Q4 ruling): a
  // second apply before the first fires fails the new update honestly
  // without touching the limiter. `callback` fires on the worker dispatcher
  // with the per-worker apply status. Callers without a dispatcher running
  // on their side get no callback (the run's final stop exits this
  // dispatcher); the Task 6 orchestrator bounds that window with its run-end
  // resolution. The Task-3-era signature: no staging callback, no epoch id
  // (the stage callback defaults to a no-op, the epoch to empty).
  void applyLiveUpdate(Envoy::MonotonicTime t_bend, const LiveKnobDelta& delta,
                       std::function<void(absl::Status)> callback);

  // FORK-ONLY (We-Amp): the Task 6 fan-out surface (the staging-confirm
  // ruling). Same apply semantics as above, plus `stage_callback`: it fires
  // on the dispatcher BEFORE t_bend for a future bend (at the arming
  // checks): OkStatus = staged and armed, an error = nothing armed, nothing
  // will ever apply. The Task 6 fan-out uses it to surface most failures
  // with NOTHING applied, and to know which staged bends to cancel. The
  // knob epoch does not pass through here: the caller's apply callback
  // wrapper owns the worker-side epoch stamping (M3 Task 6).
  void applyLiveUpdate(Envoy::MonotonicTime t_bend, const LiveKnobDelta& delta,
                       std::function<void(absl::Status)> stage_callback,
                       std::function<void(absl::Status)> callback);

  // FORK-ONLY (We-Amp): cancels the currently pending future bend (Task 6's
  // staging-failure path). Posts onto this sequencer's own dispatcher; if a
  // bend was pending, the timer is disarmed, the pending state is cleared,
  // and the bend's apply callback is invoked on the dispatcher with a
  // CancelledError (its apply will never run). When nothing is pending this
  // is a no-op. Safe to call from any thread; never call from inside the
  // bend timer's own firing (the same re-entrancy trap as timer deletion).
  void cancelPendingLiveUpdate();

protected:
  /**
   * Run is called initially by start() and thereafter by two timers:
   *  - a periodic one running at a 1 ms resolution (the current minimum)
   *  - one to spin on calls to run().
   *
   * Spinning is performed when the Sequencer implementation considers itself idle, where "idle" is
   * defined as:
   * - All benchmark target calls have reported back
   * - Either the rate limiter or the benchmark target is prohibiting initiation of the next
   * benchmark target call.
   *
   * The spinning is performed to improve timelyness when initiating
   * calls to the benchmark targets, and observational data also shows significant improvement of
   * actually latency measurement (most pronounced on non-tuned systems). As a side-effect, spinning
   * keeps the CPU busy, preventing C-state frequency changes. Systems with appropriately cooled
   * processors should not be impacted by thermal throttling. When thermal throttling does occur, it
   * makes sense to first warm up the system to get it into a steady state regarding processor
   * frequency.
   *
   * For more context on the current implementation of how we spin, see the the review discussion:
   * https://github.com/envoyproxy/envoy-perf/pull/49#discussion_r259133387
   *
   * @param from_periodic_timer Indicates if we this is called from the periodic timer.
   * Used to determine if re-enablement of the periodic timer should be performed before returning.
   */
  void run(bool from_periodic_timer);
  void scheduleRun();
  // Legacy one-arg stop: delegates with the dispatcher exit today's behavior
  // is built on. The phase path configures exit_dispatcher_on_stop_ (run()
  // passes it through) so boundaries stop without exiting.
  void stop(bool timed_out);
  void stop(bool timed_out, bool exit_dispatcher);
  void unblockAndUpdateStatisticIfNeeded(const Envoy::MonotonicTime& now);
  void updateStartBlockingTimeIfNeeded();
  // FORK-ONLY (We-Amp): M3 Task 3 -- applyLiveUpdate's dispatcher-side half;
  // always runs on this sequencer's own dispatcher.
  void applyLiveUpdateOnDispatcherThread(Envoy::MonotonicTime t_bend, const LiveKnobDelta& delta,
                                         std::function<void(absl::Status)> stage_callback,
                                         std::function<void(absl::Status)> callback);
  // FORK-ONLY (We-Amp): the bend timer's body. Reads the pending bend from
  // the members below at fire time (the timer lambda captures only `this`),
  // so each firing applies the CURRENTLY pending bend and its own callback
  // -- never a stale capture from the first arming.
  void firePendingBend();

private:
  SequencerTarget target_;
  const PlatformUtil& platform_util_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::TimeSource& time_source_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  StatisticPtr latency_statistic_;
  StatisticPtr blocked_statistic_;
  Envoy::Event::TimerPtr periodic_timer_;
  Envoy::Event::TimerPtr spin_timer_;
  uint64_t targets_initiated_{0};
  uint64_t targets_completed_{0};
  bool running_{};
  bool blocked_{};
  Envoy::MonotonicTime blocked_start_;
  nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions idle_strategy_;
  TerminationPredicatePtr termination_predicate_;
  TerminationPredicate::Status last_termination_status_;
  Envoy::Stats::ScopeSharedPtr scope_;
  SequencerStats sequencer_stats_;
  // FORK-ONLY (We-Amp): multi-phase stop-mode state, see setExitDispatcherOnStop().
  bool exit_dispatcher_on_stop_{true};
  std::function<void(bool timed_out)> stop_hook_;
  // FORK-ONLY (We-Amp): M3 Task 3 -- the captured pre-decorator handle and
  // the pending future-bend timer (null when no bend is armed). Both are
  // touched on this sequencer's own dispatcher only.
  LiveTunableRateLimiter* live_tunable_rate_limiter_{nullptr};
  Envoy::Event::TimerPtr live_update_timer_;
  // True while a future bend is armed (the timer object is reused, so the
  // pointer alone cannot say whether a bend is pending).
  bool bend_timer_pending_{false};
  // The currently pending future bend, dispatched to the limiter when the
  // bend timer fires. Dispatcher-confined like everything above; each
  // successful arming overwrites the previous (bends are not queued, so
  // there is never more than one pending bend).
  Envoy::MonotonicTime pending_bend_time_;
  LiveKnobDelta pending_bend_delta_;
  std::function<void(absl::Status)> pending_bend_callback_;
};

} // namespace Nighthawk
