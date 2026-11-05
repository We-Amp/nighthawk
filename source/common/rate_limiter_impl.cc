#include "source/common/rate_limiter_impl.h"
#include <chrono>
#include <cmath>
#include <cstdint>
#include <memory>

#include "envoy/api/api.h"
#include "envoy/common/exception.h"
#include "nighthawk/client/options.h"
#include "nighthawk/common/exception.h"
#include "source/common/common/macros.h"
#include "source/common/protobuf/protobuf.h"

#include "envoy/registry/registry.h"
#include "nighthawk/common/rate_limiter_plugin_config_factory.h"
#include "source/common/common/assert.h"
#include "source/common/protobuf/utility.h"
#include "source/common/ramp_reanchor.h"

namespace Nighthawk {

using namespace std::chrono_literals;

BurstingRateLimiter::BurstingRateLimiter(RateLimiterPtr&& rate_limiter, const uint64_t burst_size)
    : ForwardingRateLimiterImpl(std::move(rate_limiter)), burst_size_(burst_size) {
  ASSERT(burst_size_ > 0);
}

bool BurstingRateLimiter::tryAcquireOne() {
  // Callers may not be able to actually make use of a successfull acquisition, and
  // call releaseOne() to indicate so. We remember state here so we can use that later when
  // that happens to restore it.
  previously_releasing_ = releasing_;

  if (releasing_) {
    // When releasing_ we drain the accumulated total until there's nothing left. We'll transit to
    // accumulating mode after that.
    accumulated_--;
    releasing_ = accumulated_ > 0;
    return true;
  } else {
    // We are greedy here, which should help with timely releases of bursts when
    // the system is lagging behind.
    while (rate_limiter_->tryAcquireOne()) {
      accumulated_++;
      if ((accumulated_ % burst_size_) == 0) {
        releasing_ = true;
        // We have accumulated the burst size. Enter release mode and recurse.
        return tryAcquireOne();
      }
    }
  }

  previously_releasing_ = std::nullopt;
  return false;
}

void BurstingRateLimiter::releaseOne() {
  ASSERT(accumulated_ < burst_size_);
  ASSERT(previously_releasing_ != std::nullopt && previously_releasing_ == true);
  // The caller wasn't able to put its earlier successfull acquisition to good use, so we restore
  // state to what it was prior to that.
  accumulated_++;
  releasing_ = true; // release_ could only have been set earlier.
  previously_releasing_ = std::nullopt;
}

ScheduledStartingRateLimiter::ScheduledStartingRateLimiter(
    RateLimiterPtr&& rate_limiter, const Envoy::MonotonicTime scheduled_starting_time)
    : ForwardingRateLimiterImpl(std::move(rate_limiter)),
      scheduled_starting_time_(scheduled_starting_time) {
  if (timeSource().monotonicTime() >= scheduled_starting_time_) {
    ENVOY_LOG(error, "Scheduled starting time exceeded. This may cause unintended bursty traffic.");
  }
}

bool ScheduledStartingRateLimiter::tryAcquireOne() {
  if (timeSource().monotonicTime() < scheduled_starting_time_) {
    aquisition_attempted_ = true;
    return false;
  }
  // If we start forwarding right away on the first attempt that is remarkable, so leave a hint
  // about this happening in the logs.
  if (!aquisition_attempted_) {
    aquisition_attempted_ = true;
    ENVOY_LOG(warn, "ScheduledStartingRateLimiter: first acquisition attempt was late");
  }
  return rate_limiter_->tryAcquireOne();
}

void ScheduledStartingRateLimiter::releaseOne() {
  if (timeSource().monotonicTime() < scheduled_starting_time_) {
    throw NighthawkException("Unexpected call to releaseOne()");
  }
  return rate_limiter_->releaseOne();
}

LinearRateLimiter::LinearRateLimiter(Envoy::TimeSource& time_source, const Frequency frequency)
    : RateLimiterBaseImpl(time_source), frequency_(frequency) {
  if (frequency.value() <= 0) {
    throw NighthawkException(fmt::format("frequency must be <= 0, value: {}", frequency.value()));
  }
}

bool LinearRateLimiter::tryAcquireOne() {
  // TODO(oschaaf): consider adding an explicit start() call to the interface.
  // The gate is strictly greater-than: negative credit is never truthy here
  // (M3 Task 3 down-bend clamp contract; see also the ramping limiter's
  // pending Task 5 truthiness fix).
  if (acquireable_count_ > 0) {
    acquireable_count_--;
    acquired_count_++;
    return true;
  }

  // As the common case for configured execution duration is in seconds, we shift phase so that
  // acquisitions timed at one second boundaries will be avoided.
  // For example, at three rps our timings should look like [0.16667s, 0.5s, 0.83333s,...].
  // This helps produce more intuitive/steady counter values.
  // FORK-ONLY (We-Amp): M3 Task 3 -- after a live re-parameterization the
  // schedule is anchored at the re-anchor origin with the acquired-count
  // baseline frozen at the bend (signed: a pre-bend acquisition released
  // after the bend must return its credit, never wrap). Unset, the stock
  // first-acquisition anchoring applies unchanged.
  const Envoy::MonotonicTime now = time_source_.monotonicTime();
  const std::chrono::nanoseconds since_origin =
      live_origin_.has_value() ? now - *live_origin_ : elapsed();
  const int64_t acquired_at_origin =
      live_origin_.has_value() ? static_cast<int64_t>(live_origin_acquired_) : 0;
  const auto phase_shifted = since_origin + (frequency_.interval() / 2);
  acquireable_count_ = static_cast<int64_t>(std::floor(phase_shifted / frequency_.interval())) -
                       (static_cast<int64_t>(acquired_count_) - acquired_at_origin);
  return acquireable_count_ > 0 ? tryAcquireOne() : false;
}

void LinearRateLimiter::releaseOne() {
  acquireable_count_++;
  acquired_count_--;
}

absl::Status LinearRateLimiter::ApplyLiveRps(const Envoy::MonotonicTime t_bend,
                                             const uint32_t new_rps) {
  if (new_rps < 1) {
    return absl::InvalidArgumentError(
        fmt::format("requests_per_second: rps must be >= 1, got {}", new_rps));
  }
  const Frequency new_frequency(new_rps);
  if (start_time_ == std::nullopt) {
    // The schedule never started (no acquisition attempted): there is no
    // operating point to conserve; adopt the new rate directly.
    frequency_ = new_frequency;
    return absl::OkStatus();
  }
  const Envoy::MonotonicTime now = time_source_.monotonicTime();
  if (now + kLiveUpdateBendGrace < t_bend) {
    // Invoked meaningfully ahead of the agreed bend: reject without
    // mutation. The caller must schedule the apply at t_bend (the
    // sequencer's dispatcher-posted apply fires a bend timer).
    return absl::InvalidArgumentError(fmt::format(
        "bend instant is in the future (t_bend - now = {}ns); invoke ApplyLiveRps at the "
        "agreed bend instant t_bend",
        (t_bend - now).count()));
  }
  // Re-anchor (spec 8.7 linear degenerate case): freeze the acquired-count
  // baseline at the actual apply instant (conservation is exact here) and
  // restart the phase-shifted grid from it under the new rate. Any credit
  // left over from the OLD schedule is discarded: carrying it across the
  // bend would hand out the remainder of the old second as a burst.
  live_origin_ = now;
  live_origin_acquired_ = acquired_count_;
  frequency_ = new_frequency;
  acquireable_count_ = 0;
  ENVOY_LOG(debug, "live rps re-parameterization applied: new rps {}", new_rps);
  return absl::OkStatus();
}

absl::Status LinearRateLimiter::ApplyLiveRamp(const Envoy::MonotonicTime /*t_bend*/,
                                              const RampShapeChange& /*change*/) {
  // FORK-ONLY (We-Amp): M3 Task 5 -- the ramp knob requires the ramp plugin.
  // The Task-2 whitelist validator gates it; this is the capability-surface
  // backstop (a live-tunable plain limiter has no ramp to bend).
  return absl::InvalidArgumentError(
      "ramp: ramp bends require the linear-ramping rate limiter plugin to be configured "
      "for the run; only requests_per_second updates are available on this limiter");
}

LinearRampingRateLimiterImpl::LinearRampingRateLimiterImpl(Envoy::TimeSource& time_source,
                                                           const std::chrono::nanoseconds ramp_time,
                                                           const Frequency frequency)
    : RateLimiterBaseImpl(time_source), ramp_time_(ramp_time), frequency_(frequency) {
  if (frequency_.value() <= 0) {
    throw NighthawkException(fmt::format("frequency must be > 0, value: {}", frequency.value()));
  }
  if (ramp_time <= 0ns) {
    throw NighthawkException(
        fmt::format("ramp_time must be positive, value: {}", ramp_time.count()));
  }
}

bool LinearRampingRateLimiterImpl::tryAcquireOne() {
  // FORK-ONLY (We-Amp): M3 Task 5 truthiness fix -- the gate is strictly
  // greater-than, so a negative balance is never truthy (the A2b note's R3;
  // the white-box probe test pins the gate itself). The stock bare-truthiness
  // form would grant an acquisition sourced from negative credit.
  if (acquireable_count_ > 0) {
    acquired_count_++;
    acquireable_count_--;
    return true;
  }
  // FORK-ONLY (We-Amp): M3 Task 5 -- the release count comes from the shared
  // RampReanchor forms (bit-identical to the stock expressions above, one
  // source of truth for the limiter and the re-anchor solver). After a live
  // bend the schedule is anchored at the re-anchor origin with the
  // acquired-count baseline frozen at the bend (the A2b note Q1: cumulative
  // conservation, no windfall, silence bounded, credit >= 0 invariant).
  const Envoy::MonotonicTime now = time_source_.monotonicTime();
  const std::chrono::nanoseconds since_origin =
      live_origin_.has_value() ? now - *live_origin_ : elapsed();
  const int64_t total =
      RampReanchor::CumulativeReleases({ramp_time_, static_cast<double>(frequency_.value())},
                                       since_origin);
  // No baseline offset here, unlike the linear limiter's grid restart: the
  // re-anchored ORIGIN carries the baseline (the solve placed the new
  // schedule where N_B(tau) == acquired at the bend), so the schedule's own
  // count already accounts for the pre-bend acquisitions -- plain credit.
  acquireable_count_ = total - static_cast<int64_t>(acquired_count_);
  return acquireable_count_ > 0 ? tryAcquireOne() : false;
}

void LinearRampingRateLimiterImpl::releaseOne() {
  acquireable_count_++;
  acquired_count_--;
}

absl::Status LinearRampingRateLimiterImpl::ApplyLiveRps(const Envoy::MonotonicTime t_bend,
                                                        const uint32_t new_rps) {
  // FORK-ONLY (We-Amp): C1 -- on this limiter the cumulative release schedule
  // is a function of the target frequency, so a live rps change IS ramp
  // re-parameterization: preserve the ramp time, re-anchor under the new
  // target (the A2b note's Q6 linear corollary, Task 5's real plugin-path
  // rps knob; the Task 3 capture-only interim is superseded).
  return ApplyLiveRamp(t_bend, RampShapeChange{std::nullopt, new_rps});
}

absl::Status LinearRampingRateLimiterImpl::ApplyLiveRamp(const Envoy::MonotonicTime t_bend,
                                                         const RampShapeChange& change) {
  const RampReanchor::RampSchedule next{
      change.ramp_time.value_or(ramp_time_),
      static_cast<double>(change.target_rps.value_or(frequency_.value()))};
  if (next.ramp_time <= 0ns) {
    return absl::InvalidArgumentError(
        fmt::format("ramp.ramp_time: ramp time must be positive, got {}", next.ramp_time.count()));
  }
  if (next.target_rps < 1) {
    return absl::InvalidArgumentError(
        fmt::format("ramp.target_rps: target rps must be >= 1, got {}", next.target_rps));
  }
  if (start_time_ == std::nullopt) {
    // The schedule never started (no acquisition attempted): there is no
    // operating point to conserve; adopt the new schedule directly.
    ramp_time_ = next.ramp_time;
    frequency_ = Frequency(static_cast<uint64_t>(next.target_rps));
    acquireable_count_ = 0;
    return absl::OkStatus();
  }
  const Envoy::MonotonicTime now = time_source_.monotonicTime();
  if (now + kLiveUpdateBendGrace < t_bend) {
    // Invoked meaningfully ahead of the agreed bend: reject without
    // mutation. The caller must schedule the apply at t_bend (the
    // sequencer's dispatcher-posted bend timer fires there).
    return absl::InvalidArgumentError(fmt::format(
        "bend instant is in the future (t_bend - now = {}ns); invoke ApplyLiveRamp at the "
        "agreed bend instant t_bend",
        (t_bend - now).count()));
  }
  // The elapsed-under-the-CURRENT-schedule at the bend: after a previous
  // bend, the current schedule is anchored at the re-anchor origin, not at
  // the run's start -- measuring from the start would place the bend at the
  // wrong point of the outgoing schedule (and trip the bookkeeping sanity
  // check on any second bend onto an already-bent limiter, e.g. a mixed
  // rps+ramp request's rps-then-ramp composition).
  const std::chrono::nanoseconds elapsed_now =
      live_origin_.has_value() ? now - *live_origin_ : elapsed();
  const RampReanchor::RampSchedule current{ramp_time_, static_cast<double>(frequency_.value())};
  auto reanchor = RampReanchor::Reanchor(current, elapsed_now, acquired_count_, next);
  if (!reanchor.ok()) {
    return reanchor.status();
  }
  const double old_rate = RampReanchor::InstantaneousRate(current, elapsed_now);
  ramp_time_ = next.ramp_time;
  frequency_ = Frequency(static_cast<uint64_t>(next.target_rps));
  live_origin_ = now - reanchor->new_origin_offset;
  // The acquired-count baseline is baked into the re-anchored origin itself
  // (N_B(tau) == acquired_count_ at the bend), so tryAcquireOne reads plain
  // credit against it -- no separate baseline member.
  // The outgoing schedule's leftover credit is discarded, never handed out
  // across the bend (carrying it would be a windfall: the 8.7(c) artifact).
  acquireable_count_ = 0;
  ENVOY_LOG(warn, "live ramp bend applied: re-anchored elapsed {}ns, instantaneous rate {} -> {} "
                  "(count conserved at {})",
            reanchor->new_origin_offset.count(), old_rate,
            RampReanchor::InstantaneousRate(next, reanchor->new_origin_offset), acquired_count_);
  return absl::OkStatus();
}

RateLimiterPtr LinearRampingRateLimiterImplFactory::createRateLimiterPlugin(
    const Envoy::Protobuf::Message& typed_config, Envoy::Api::Api& api,
    Envoy::TimeSource& time_source, const Nighthawk::Client::Options& options) {
  UNREFERENCED_PARAMETER(api);
  const auto* any = Envoy::Protobuf::DynamicCastMessage<const Envoy::Protobuf::Any>(&typed_config);
  if (any == nullptr) {
    throw Envoy::EnvoyException("typed_config cannot be cast to an Any proto");
  }
  nighthawk::rate_limiter::LinearRampingRateLimiterConfig config;
  Envoy::MessageUtil::anyConvert(*any, config);

  const uint32_t rps = options.requestsPerSecond();
  const std::chrono::nanoseconds ramp_time = std::chrono::seconds(config.ramp_time().seconds()) +
                                             std::chrono::nanoseconds(config.ramp_time().nanos());

  if (ramp_time <= 0ns) {
    throw NighthawkException("ramp_time must be positive and > 0ns");
  }
  if (!options.noDuration() && ramp_time > options.duration()) {
    throw NighthawkException(
        "ramp_time must be less than or equal to time specified by --duration");
  }

  return std::make_unique<LinearRampingRateLimiterImpl>(time_source, ramp_time, Frequency(rps));
}

REGISTER_FACTORY(LinearRampingRateLimiterImplFactory, RateLimiterPluginConfigFactory);

DelegatingRateLimiterImpl::DelegatingRateLimiterImpl(
    RateLimiterPtr&& rate_limiter, RateLimiterDelegate random_distribution_generator)
    : ForwardingRateLimiterImpl(std::move(rate_limiter)),
      random_distribution_generator_(std::move(random_distribution_generator)) {}

bool DelegatingRateLimiterImpl::tryAcquireOne() {
  const Envoy::MonotonicTime now = timeSource().monotonicTime();
  if (rate_limiter_->tryAcquireOne()) {
    const Envoy::MonotonicTime adjusted = now + random_distribution_generator_();
    // We track a sorted list of timings, where the one at the front is the one that should
    // be applied the soonest.
    distributed_timings_.insert(
        std::upper_bound(distributed_timings_.begin(), distributed_timings_.end(), adjusted),
        adjusted);
  }

  if (!distributed_timings_.empty() && distributed_timings_.front() <= now) {
    distributed_timings_.pop_front();
    sanity_check_pending_release_ = false;
    return true;
  }

  return false;
}

void DelegatingRateLimiterImpl::releaseOne() {
  RELEASE_ASSERT(!sanity_check_pending_release_,
                 "unexpected call to DelegatingRateLimiterImpl::releaseOne()");
  sanity_check_pending_release_ = true;
  rate_limiter_->releaseOne();
}

DistributionSamplingRateLimiterImpl::DistributionSamplingRateLimiterImpl(
    DiscreteNumericDistributionSamplerPtr&& provider, RateLimiterPtr&& rate_limiter)
    : DelegatingRateLimiterImpl(
          std::move(rate_limiter),
          [this]() { return std::chrono::duration<uint64_t, std::nano>(provider_->getValue()); }),
      provider_(std::move(provider)) {}

FilteringRateLimiterImpl::FilteringRateLimiterImpl(RateLimiterPtr&& rate_limiter,
                                                   RateLimiterFilter filter)
    : ForwardingRateLimiterImpl(std::move(rate_limiter)), filter_(std::move(filter)) {}

bool FilteringRateLimiterImpl::tryAcquireOne() {
  return rate_limiter_->tryAcquireOne() ? filter_() : false;
}

GraduallyOpeningRateLimiterFilter::GraduallyOpeningRateLimiterFilter(
    const std::chrono::nanoseconds ramp_time, DiscreteNumericDistributionSamplerPtr&& provider,
    RateLimiterPtr&& rate_limiter)
    : FilteringRateLimiterImpl(
          std::move(rate_limiter),
          [this]() {
            const auto elapsed_time = elapsed();
            if (elapsed_time < ramp_time_) {
              // We want to linearly increase the probability of returning true
              // below. We can derive that from the elapsed fraction of ramp_time.
              const double probability =
                  1.0 - static_cast<double>(ramp_time_.count() - elapsed_time.count()) /
                            (ramp_time_.count() * 1.0);
              // Get a random number r, where 0 < r ≤ 1.
              const double random_between_0_and_1 = 1.0 * provider_->getValue() / provider_->max();
              // Given a uniform distribution, the fraction of the ramp
              // will translate into the probability of opening up we are looking for.
              return random_between_0_and_1 < probability;
            }
            // Ramping is complete, and as such this filter has completely opened up.
            return true;
          }),
      provider_(std::move(provider)), ramp_time_(ramp_time) {
  if (ramp_time <= 0ns) {
    throw NighthawkException("ramp_time must be positive and > 0ns");
  }
  if (provider_->min() != 1) {
    throw NighthawkException("min value of the distribution provider must equal 1");
  }
  if (provider_->max() != 1000000) {
    throw NighthawkException("max value of the distribution provider must equal 1000000");
  }
}

ZipfRateLimiterImpl::ZipfRateLimiterImpl(RateLimiterPtr&& rate_limiter, double q, double v,
                                         ZipfBehavior behavior)
    : FilteringRateLimiterImpl(std::move(rate_limiter),
                               [this]() {
                                 return behavior_ == ZipfBehavior::ZIPF_PSEUDO_RANDOM ? dist_(mt_)
                                                                                      : dist_(g_);
                               }),
      behavior_(behavior) {
  if (v <= 0) {
    throw NighthawkException("v should be > 0");
  }
  if (q <= 1) {
    throw NighthawkException("q should be > 1");
  }
  dist_ = absl::zipf_distribution<uint64_t>(1, q, v);
}

} // namespace Nighthawk
