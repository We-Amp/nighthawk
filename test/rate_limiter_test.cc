#include <chrono>
#include <vector>

#include "nighthawk/common/exception.h"

#include "test/test_common/simulated_time_system.h"

#include "source/common/frequency.h"
#include "source/common/rate_limiter_impl.h"

#include "test/mocks/common/mock_rate_limiter.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;
using namespace testing;

namespace Nighthawk {

class RateLimiterTest : public Test {};

TEST_F(RateLimiterTest, LinearRateLimiterTest) {
  Envoy::Event::SimulatedTimeSystem time_system;
  // Construct a 10/second paced rate limiter.
  LinearRateLimiter rate_limiter(time_system, 10_Hz);

  EXPECT_FALSE(rate_limiter.tryAcquireOne());

  time_system.advanceTimeWait(100ms);
  EXPECT_TRUE(rate_limiter.tryAcquireOne());
  EXPECT_FALSE(rate_limiter.tryAcquireOne());

  time_system.advanceTimeWait(1s);
  for (int i = 0; i < 10; i++) {
    EXPECT_TRUE(rate_limiter.tryAcquireOne());
  }
  EXPECT_FALSE(rate_limiter.tryAcquireOne());
}

TEST_F(RateLimiterTest, LinearRateLimiterInvalidArgumentTest) {
  Envoy::Event::SimulatedTimeSystem time_system;
  EXPECT_THROW(LinearRateLimiter rate_limiter(time_system, 0_Hz), NighthawkException);
}

TEST_F(RateLimiterTest, BurstingRateLimiterTest) {
  const uint64_t burst_size = 3;
  std::unique_ptr<MockRateLimiter> mock_rate_limiter = std::make_unique<MockRateLimiter>();
  MockRateLimiter& unsafe_mock_rate_limiter = *mock_rate_limiter;
  InSequence s;

  EXPECT_CALL(unsafe_mock_rate_limiter, tryAcquireOne)
      .Times(burst_size)
      .WillRepeatedly(Return(true));
  RateLimiterPtr rate_limiter =
      std::make_unique<BurstingRateLimiter>(std::move(mock_rate_limiter), burst_size);

  // On the first acquisition the bursting rate limiter will have accumulated three.
  EXPECT_TRUE(rate_limiter->tryAcquireOne());
  rate_limiter->releaseOne();
  EXPECT_TRUE(rate_limiter->tryAcquireOne());
  EXPECT_TRUE(rate_limiter->tryAcquireOne());

  // Releasing one here should result in one more successfull acquisition, as the
  // BurstingRateLimiter is still releasing and not working to accumulate a new burst.
  rate_limiter->releaseOne();
  EXPECT_TRUE(rate_limiter->tryAcquireOne());
  EXPECT_TRUE(rate_limiter->tryAcquireOne());
  EXPECT_CALL(unsafe_mock_rate_limiter, tryAcquireOne).WillOnce(Return(false));
  EXPECT_FALSE(rate_limiter->tryAcquireOne());
}

TEST_F(RateLimiterTest, ScheduledStartingRateLimiterTest) {
  Envoy::Event::SimulatedTimeSystem time_system;
  const auto schedule_delay = 10ms;
  // We test regular flow, but also the flow where the first acquisition attempt comes after the
  // scheduled delay. This should be business as usual from a functional perspective, but internally
  // this rate limiter specializes on this case to log a warning message, and we want to cover that.
  for (const bool starting_late : std::vector<bool>{false, true}) {
    const Envoy::MonotonicTime scheduled_starting_time =
        time_system.monotonicTime() + schedule_delay;
    std::unique_ptr<MockRateLimiter> mock_rate_limiter = std::make_unique<MockRateLimiter>();
    MockRateLimiter& unsafe_mock_rate_limiter = *mock_rate_limiter;
    InSequence s;

    EXPECT_CALL(unsafe_mock_rate_limiter, timeSource)
        .Times(AtLeast(1))
        .WillRepeatedly(ReturnRef(time_system));
    RateLimiterPtr rate_limiter = std::make_unique<ScheduledStartingRateLimiter>(
        std::move(mock_rate_limiter), scheduled_starting_time);
    EXPECT_CALL(unsafe_mock_rate_limiter, tryAcquireOne)
        .Times(AtLeast(1))
        .WillRepeatedly(Return(true));

    if (starting_late) {
      time_system.advanceTimeWait(schedule_delay);
    }

    // We should expect zero releases until it is time to start.
    while (time_system.monotonicTime() < scheduled_starting_time) {
      EXPECT_FALSE(rate_limiter->tryAcquireOne());
      time_system.advanceTimeWait(1ms);
    }

    // Now that is time to start, the rate limiter should propagate to the mock rate limiter.
    EXPECT_TRUE(rate_limiter->tryAcquireOne());
  }
}

TEST_F(RateLimiterTest, ScheduledStartingRateLimiterTestBadArgs) {
  Envoy::Event::SimulatedTimeSystem time_system;
  // Verify we enforce future-only scheduling.
  for (const auto& timing : std::vector<Envoy::MonotonicTime>{time_system.monotonicTime(),
                                                              time_system.monotonicTime() - 10ms}) {
    std::unique_ptr<MockRateLimiter> mock_rate_limiter = std::make_unique<MockRateLimiter>();
    MockRateLimiter& unsafe_mock_rate_limiter = *mock_rate_limiter;
    EXPECT_CALL(unsafe_mock_rate_limiter, timeSource)
        .Times(AtLeast(1))
        .WillRepeatedly(ReturnRef(time_system));
    EXPECT_NO_THROW(ScheduledStartingRateLimiter(std::move(mock_rate_limiter), timing));
    // TODO(XXX): once we can, verify a warning gets logged while running the line
    // above.
  }
}

class BurstingRateLimiterIntegrationTest : public Test {
public:
  void testBurstSize(const uint64_t burst_size, const Frequency frequency) {
    Envoy::Event::SimulatedTimeSystem time_system;
    RateLimiterPtr rate_limiter = std::make_unique<BurstingRateLimiter>(
        std::make_unique<LinearRateLimiter>(time_system, frequency), burst_size);
    const auto burst_interval_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(frequency.interval() * burst_size);

    int first_burst = -1;
    for (int i = 0; i < 10000; i++) {
      uint64_t burst_acquired = 0;
      while (rate_limiter->tryAcquireOne()) {
        burst_acquired++;
      }
      if (burst_acquired) {
        first_burst = first_burst == -1 ? i : first_burst;
        EXPECT_EQ(burst_acquired, burst_size);
        EXPECT_EQ(i % (burst_interval_ms.count() - first_burst), 0);
      }
      time_system.advanceTimeWait(1ms);
    }
  }
};

TEST_F(BurstingRateLimiterIntegrationTest, BurstingLinearRateLimiterTest) {
  testBurstSize(1, 100_Hz);
  testBurstSize(2, 100_Hz);
  testBurstSize(13, 100_Hz);
  testBurstSize(100, 100_Hz);

  testBurstSize(1, 50_Hz);
  testBurstSize(2, 50_Hz);
  testBurstSize(13, 50_Hz);
  testBurstSize(100, 50_Hz);
}

TEST_F(RateLimiterTest, DistributionSamplingRateLimiterImplTest) {
  const uint64_t tries = 1000;
  auto mock_rate_limiter = std::make_unique<MockRateLimiter>();
  MockRateLimiter& unsafe_mock_rate_limiter = *mock_rate_limiter;
  Envoy::Event::SimulatedTimeSystem time_system;
  EXPECT_CALL(unsafe_mock_rate_limiter, timeSource)
      .Times(AtLeast(1))
      .WillRepeatedly(ReturnRef(time_system));
  auto sampler = std::make_unique<UniformRandomDistributionSamplerImpl>(1);
  EXPECT_EQ(sampler->min(), 0);
  EXPECT_EQ(sampler->max(), 1);
  RateLimiterPtr rate_limiter = std::make_unique<DistributionSamplingRateLimiterImpl>(
      std::move(sampler), std::move(mock_rate_limiter));

  EXPECT_CALL(unsafe_mock_rate_limiter, tryAcquireOne).Times(tries).WillRepeatedly(Return(true));
  // 1 in a billion chance of failure to exceed max_expected_acquisitions.
  const int max_expected_acquisitions = (tries / 2) + 30;
  EXPECT_CALL(unsafe_mock_rate_limiter, releaseOne).Times(AtMost(max_expected_acquisitions));

  int acquisitions = 0;
  // We used a 1ns upper bound. That means we can expect around 50% of acquisitions to succeed as
  // there are only two possibilities: now, or 1ns later in the future.
  for (uint64_t i = 0; i < tries; i++) {
    if (rate_limiter->tryAcquireOne()) {
      acquisitions++;
      // We test the release gets propagated to the mock rate limiter.
      // also, the release will force DelegatingRateLimiterImpl to propagate tryAcquireOne.
      rate_limiter->releaseOne();
    }
  }
  EXPECT_LT(acquisitions, max_expected_acquisitions);
}

// A rate limiter determines when acquisition is allowed, but DistributionSamplingRateLimiterImpl
// may arbitrarily delay that. We test that principle with tests that use this fixture, which
// sets up a distribution sampling rate limiter instance to encapsulate a mock rate limiter,
// relying on simulated time and a mock discrete numberic distribution sampler.
class DistributionSamplingRateLimiterTest : public RateLimiterTest {
public:
  DistributionSamplingRateLimiterTest()
      : tmp_mock_inner_rate_limiter_(std::make_unique<NiceMock<MockRateLimiter>>()),
        mock_inner_rate_limiter_(*tmp_mock_inner_rate_limiter_),
        tmp_mock_discrete_numeric_distribution_sampler_(
            std::make_unique<MockDiscreteNumericDistributionSampler>()),
        mock_discrete_numeric_distribution_sampler_(
            *tmp_mock_discrete_numeric_distribution_sampler_),
        rate_limiter_(std::make_unique<DistributionSamplingRateLimiterImpl>(
            std::move(tmp_mock_discrete_numeric_distribution_sampler_),
            std::move(tmp_mock_inner_rate_limiter_))) {
    EXPECT_CALL(mock_inner_rate_limiter_, timeSource).WillRepeatedly(ReturnRef(time_system_));
  }

  Envoy::Event::SimulatedTimeSystem time_system_;
  std::unique_ptr<NiceMock<MockRateLimiter>> tmp_mock_inner_rate_limiter_;
  MockRateLimiter& mock_inner_rate_limiter_;
  std::unique_ptr<MockDiscreteNumericDistributionSampler>
      tmp_mock_discrete_numeric_distribution_sampler_;
  MockDiscreteNumericDistributionSampler& mock_discrete_numeric_distribution_sampler_;
  RateLimiterPtr rate_limiter_;
};

TEST_F(DistributionSamplingRateLimiterTest, SingleAcquisition) {
  EXPECT_CALL(mock_inner_rate_limiter_, tryAcquireOne)
      .WillOnce(Return(true))
      .WillOnce(Return(false))
      .WillOnce(Return(false));
  // The distribution first yields a 1 ns offset.
  EXPECT_CALL(mock_discrete_numeric_distribution_sampler_, getValue).WillOnce(Return(1));
  // We don't expect to be green lighted without moving time forward.
  EXPECT_FALSE(rate_limiter_->tryAcquireOne());
  time_system_.advanceTimeWait(1ns);
  EXPECT_TRUE(rate_limiter_->tryAcquireOne());
  EXPECT_FALSE(rate_limiter_->tryAcquireOne());
}

TEST_F(DistributionSamplingRateLimiterTest, QueuedAcquisition) {
  EXPECT_CALL(mock_inner_rate_limiter_, tryAcquireOne)
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false))
      .WillOnce(Return(false))
      .WillOnce(Return(false));
  // The distribution yields a 1 ns offset two times.
  EXPECT_CALL(mock_discrete_numeric_distribution_sampler_, getValue)
      .WillOnce(Return(1))
      .WillOnce(Return(1));
  // We do not expect to observe releases because we did not move time forward.
  EXPECT_FALSE(rate_limiter_->tryAcquireOne());
  EXPECT_FALSE(rate_limiter_->tryAcquireOne());
  time_system_.advanceTimeWait(1ns);
  // We moved time forward, release timings that have been queued up earlier should now be observed.
  EXPECT_TRUE(rate_limiter_->tryAcquireOne());
  EXPECT_TRUE(rate_limiter_->tryAcquireOne());
  // This should be all of it, so no further acquisitions are to be expected.
  EXPECT_FALSE(rate_limiter_->tryAcquireOne());
}

TEST_F(DistributionSamplingRateLimiterTest, ReleaseOneFunctionsWhenAcquired) {
  EXPECT_CALL(mock_inner_rate_limiter_, tryAcquireOne).WillOnce(Return(true));
  EXPECT_CALL(mock_discrete_numeric_distribution_sampler_, getValue).WillOnce(Return(0));
  EXPECT_TRUE(rate_limiter_->tryAcquireOne());
  EXPECT_CALL(mock_inner_rate_limiter_, releaseOne);
  rate_limiter_->releaseOne();
}

// Calling releaseOne() without a prior acquisition is invavlid
TEST_F(DistributionSamplingRateLimiterTest, ReleaseOneDiesWhenNotAcquired) {
  EXPECT_DEATH(rate_limiter_->releaseOne(),
               "unexpected call to DelegatingRateLimiterImpl::releaseOne");
  EXPECT_CALL(mock_inner_rate_limiter_, tryAcquireOne).WillOnce(Return(true));
  EXPECT_CALL(mock_discrete_numeric_distribution_sampler_, getValue).WillOnce(Return(0));
  EXPECT_TRUE(rate_limiter_->tryAcquireOne());
  rate_limiter_->releaseOne();
  EXPECT_DEATH(rate_limiter_->releaseOne(),
               "unexpected call to DelegatingRateLimiterImpl::releaseOne");
}

// The DistributionSamplingRateLimiter may queues up timings for deferred release later on. Here we
// verify those are deferred release timings happen at the expected points in time. This is
// important, because the associated distribution sampler may give the
// DistributionSamplingRateLimiter random time offsets as inputs.
TEST_F(DistributionSamplingRateLimiterTest, QueuedAcquisitionCorrectReleaseOrdering) {
  // The vector below defines the sequence of timing offsets that the mock distribution sampler will
  // yield.
  std::vector<uint64_t> input_acquisition_timings_ms = {0, 0, 15000, 7, 3, 700, 2,
                                                        2, 1, 800,   4, 7, 9};
  uint64_t i = 0;
  uint64_t j = 0;
  EXPECT_CALL(mock_inner_rate_limiter_, tryAcquireOne)
      .WillRepeatedly([&i, input_acquisition_timings_ms]() {
        return ++i <= input_acquisition_timings_ms.size() ? true : false;
      });
  EXPECT_CALL(mock_discrete_numeric_distribution_sampler_, getValue)
      .Times(input_acquisition_timings_ms.size())
      .WillRepeatedly(
          [&j, input_acquisition_timings_ms]() { return input_acquisition_timings_ms[j++] * 1e6; });

  // Here we are at T0. The mock rate limiter isn't time dependent when it comes to releasing.
  // So here we iterate over the expected input acquisition timings, and the outer rate limiter
  // will buffer those that indicate an offset > 0. Zero-valued offsets ought to be released
  // immediately.
  std::vector<uint64_t> acquisition_timings;
  for (uint64_t k : input_acquisition_timings_ms) {
    if (k == 0) {
      EXPECT_TRUE(rate_limiter_->tryAcquireOne());
      acquisition_timings.push_back(0);
    } else {
      EXPECT_FALSE(rate_limiter_->tryAcquireOne());
    }
  }

  // Now we will start moving the clock, and see if the accrued deferred releases result in the
  // correct timings.
  const std::chrono::seconds duration = 15s;
  auto total_ms_elapsed = 0ms;
  const auto kClockTick = 1ms;
  do {
    while (rate_limiter_->tryAcquireOne()) {
      acquisition_timings.push_back(total_ms_elapsed.count());
    }
    time_system_.advanceTimeWait(kClockTick);
    total_ms_elapsed += kClockTick;
  } while (total_ms_elapsed <= duration);

  // The observed timings should equal the sorted offsets we had at the input.
  std::sort(input_acquisition_timings_ms.begin(), input_acquisition_timings_ms.end());
  EXPECT_EQ(acquisition_timings, input_acquisition_timings_ms);
}

class LinearRampingRateLimiterImplTest : public Test {
public:
  /**
   * @param frequency The final frequency of the ramp.
   * @param duration The test (and ramp) duration. Frequency will be 0 Hz at the start and
   * linearly increase as time moves forward, up to the specified frequency.
   * @return std::vector<uint64_t> an array containing the acquisition timings
   * in microseconds.
   */
  std::vector<int64_t> checkAcquisitionTimings(const Frequency frequency,
                                               const std::chrono::seconds duration) {
    Envoy::Event::SimulatedTimeSystem time_system;
    std::vector<int64_t> acquisition_timings;
    std::vector<int64_t> control_timings;

    LinearRampingRateLimiterImpl rate_limiter(time_system, duration, frequency);
    auto total_us_elapsed = 0us;
    const auto clock_tick = 10us;
    EXPECT_FALSE(rate_limiter.tryAcquireOne());
    do {
      if (rate_limiter.tryAcquireOne()) {
        EXPECT_FALSE(rate_limiter.tryAcquireOne());
        acquisition_timings.push_back(total_us_elapsed.count());
      }
      // We use the second law of motion to verify results: ½ * a  * t²
      // In this formula, 'a' equates to our ramp speed, and t to elapsed time.
      double t = total_us_elapsed.count() / 1e6;
      double a = (frequency.value() / (duration.count() * 1.0));
      // Finally, figure out the ground that we can expect to be covered.
      uint64_t expected_count = std::round(0.5 * a * t * t);
      if (expected_count > control_timings.size()) {
        control_timings.push_back(total_us_elapsed.count());
      }
      time_system.advanceTimeWait(clock_tick);
      total_us_elapsed += clock_tick;
    } while (total_us_elapsed <= duration);

    // For good measure, verify we saw the expected amount of acquisitions: half
    // of "frequency times duration".
    EXPECT_EQ(std::round(duration.count() * frequency.value() / 2.0), acquisition_timings.size());
    // Sanity check that we have the right number of control timings.
    EXPECT_EQ(control_timings.size(), acquisition_timings.size());
    // Verify that all timings are correct.
    for (uint64_t i = 0; i < acquisition_timings.size(); i++) {
      // We allow one clock tick of slack in timing expectations, as floating
      // point math may introduce small errors in some cases.
      // This is a test only issue: in practice we don't have a fixed microsecond-level step sizes,
      // and the rate limiter computes at nanosecond precision internally. As we want to have
      // microsecond level precision, this should be more then sufficient.
      EXPECT_NEAR(acquisition_timings[i], control_timings[i], clock_tick.count());
    }
    return acquisition_timings;
  }
};

TEST_F(RateLimiterTest, LinearRampingRateLimiterImplInvalidArgumentTest) {
  Envoy::Event::SimulatedTimeSystem time_system;
  // bad frequency
  EXPECT_THROW(LinearRampingRateLimiterImpl rate_limiter(time_system, 1s, 0_Hz);
               , NighthawkException);
  // bad ramp duration
  EXPECT_THROW(LinearRampingRateLimiterImpl rate_limiter(time_system, 0s, 1_Hz);
               , NighthawkException);
  EXPECT_THROW(LinearRampingRateLimiterImpl rate_limiter(time_system, -1s, 1_Hz);
               , NighthawkException);
}

TEST_F(LinearRampingRateLimiterImplTest, TimingVerificationTest) {
  EXPECT_EQ(checkAcquisitionTimings(5_Hz, 5s),
            std::vector<int64_t>({1000010, 1732060, 2236070, 2645760, 3000000, 3316630, 3605560,
                                  3872990, 4123110, 4358900, 4582580, 4795840, 5000000}));
  checkAcquisitionTimings(1_Hz, 3s);
  checkAcquisitionTimings(5_Hz, 3s);
  checkAcquisitionTimings(4_Hz, 2s);
  checkAcquisitionTimings(1000_Hz, 12s);
  checkAcquisitionTimings(40000_Hz, 7s);
}

TEST_F(LinearRampingRateLimiterImplTest, ExtendedDurationGivesCorrectTotalRequests) {
  Envoy::Event::SimulatedTimeSystem time_system;
  const unsigned int ramp_time_sec = 5;
  const unsigned int duration_sec = 12;
  const Frequency frequency = 100_Hz;

  LinearRampingRateLimiterImpl rate_limiter(time_system, std::chrono::seconds(ramp_time_sec),
                                            frequency);

  EXPECT_FALSE(rate_limiter.tryAcquireOne()); // Sets up rate limiter

  unsigned int count = 0;
  const auto clock_tick = 10us; // small step
  auto total_elapsed = 0us;

  do {
    if (rate_limiter.tryAcquireOne()) {
      count++;
    }
    time_system.advanceTimeWait(clock_tick);
    total_elapsed += clock_tick;
  } while (total_elapsed <= std::chrono::seconds(duration_sec));

  EXPECT_EQ(count, (frequency.value() * ramp_time_sec / 2) +
                       (frequency.value() * (duration_sec - ramp_time_sec)));
}

TEST_F(RateLimiterTest, GraduallyOpeningRateLimiterFilterInvalidArgumentTest) {
  // Negative ramp throws.
  EXPECT_THROW(GraduallyOpeningRateLimiterFilter gorl(
                   -1s, std::make_unique<NiceMock<MockDiscreteNumericDistributionSampler>>(),
                   std::make_unique<NiceMock<MockRateLimiter>>());
               , NighthawkException);

  // zero ramp throws.
  EXPECT_THROW(GraduallyOpeningRateLimiterFilter gorl(
                   0s, std::make_unique<NiceMock<MockDiscreteNumericDistributionSampler>>(),
                   std::make_unique<NiceMock<MockRateLimiter>>());
               , NighthawkException);

  // Pass in a badly configured distribution sampler.
  auto bad_distribution_sampler = std::make_unique<MockDiscreteNumericDistributionSampler>();
  EXPECT_CALL(*bad_distribution_sampler, min).WillOnce(Return(0));
  EXPECT_THROW(
      GraduallyOpeningRateLimiterFilter gorl(1s, std::move(bad_distribution_sampler),
                                             std::make_unique<NiceMock<MockRateLimiter>>());
      , NighthawkException);

  bad_distribution_sampler = std::make_unique<MockDiscreteNumericDistributionSampler>();
  // Correct min, but now introduce a bad max.
  EXPECT_CALL(*bad_distribution_sampler, min).WillOnce(Return(1));
  EXPECT_CALL(*bad_distribution_sampler, max).WillOnce(Return(99));
  EXPECT_THROW(
      GraduallyOpeningRateLimiterFilter gorl(1s, std::move(bad_distribution_sampler),
                                             std::make_unique<NiceMock<MockRateLimiter>>());
      , NighthawkException);
}

class GraduallyOpeningRateLimiterFilterTest : public Test {
public:
  std::vector<int64_t> getAcquisitionTimings(const Frequency frequency,
                                             const std::chrono::seconds duration) {
    Envoy::Event::SimulatedTimeSystem time_system;
    std::vector<int64_t> acquisition_timings;
    auto* unsafe_discrete_numeric_distribution_sampler =
        new MockDiscreteNumericDistributionSampler();
    const uint64_t dist_min = 1;
    const uint64_t dist_max = 1000000;
    EXPECT_CALL(*unsafe_discrete_numeric_distribution_sampler, getValue)
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([]() { return (dist_min + dist_max) / 2; }));
    EXPECT_CALL(*unsafe_discrete_numeric_distribution_sampler, min)
        .Times(1)
        .WillOnce(Return(dist_min));
    EXPECT_CALL(*unsafe_discrete_numeric_distribution_sampler, max)
        .Times(AtLeast(1))
        .WillRepeatedly(Return(dist_max));
    RateLimiterPtr rate_limiter = std::make_unique<GraduallyOpeningRateLimiterFilter>(
        duration,
        std::unique_ptr<DiscreteNumericDistributionSampler>(
            unsafe_discrete_numeric_distribution_sampler),
        std::make_unique<LinearRateLimiter>(time_system, frequency));
    auto total_ms_elapsed = 0ms;
    auto clock_tick = 1ms;
    EXPECT_FALSE(rate_limiter->tryAcquireOne());

    do {
      if (rate_limiter->tryAcquireOne()) {
        acquisition_timings.push_back(total_ms_elapsed.count());
        EXPECT_FALSE(rate_limiter->tryAcquireOne());
      }
      time_system.advanceTimeWait(clock_tick);
      total_ms_elapsed += clock_tick;
    } while (total_ms_elapsed <= duration);

    EXPECT_FALSE(rate_limiter->tryAcquireOne());
    time_system.advanceTimeWait(1s);
    // Verify that after the rampup the expected constant pacing is maintained.
    // Calls should be forwarded to the regular linear rate limiter algorithm with its
    // corrective behavior so we can expect to acquire a series with that.
    for (uint64_t i = 0; i < frequency.value(); i++) {
      EXPECT_TRUE(rate_limiter->tryAcquireOne());
    }
    // Verify we acquired everything.
    EXPECT_FALSE(rate_limiter->tryAcquireOne());
    // Verify releaseOne works.
    rate_limiter->releaseOne();
    EXPECT_TRUE(rate_limiter->tryAcquireOne());
    EXPECT_FALSE(rate_limiter->tryAcquireOne());
    return acquisition_timings;
  }
};

TEST_F(GraduallyOpeningRateLimiterFilterTest, TimingVerificationTest) {
  EXPECT_EQ(getAcquisitionTimings(50_Hz, 1s),
            std::vector<int64_t>({510, 530, 550, 570, 590, 610, 630, 650, 670, 690, 710, 730, 750,
                                  770, 790, 810, 830, 850, 870, 890, 910, 930, 950, 970, 990}));
}

class ZipfRateLimiterImplTest : public Test {};

TEST_F(ZipfRateLimiterImplTest, TimingVerificationTest) {
  Envoy::Event::SimulatedTimeSystem time_system;
  const double q = 2.0;
  const double v = 1.0;
  auto rate_limiter = std::make_unique<ZipfRateLimiterImpl>(
      std::make_unique<LinearRateLimiter>(time_system, 10_Hz), q, v,
      ZipfRateLimiterImpl::ZipfBehavior::ZIPF_PSEUDO_RANDOM);
  const std::chrono::seconds duration = 15s;
  std::vector<int64_t> acquisition_timings;
  auto total_ms_elapsed = 0ms;
  auto clock_tick = 1ms;

  do {
    if (rate_limiter->tryAcquireOne()) {
      acquisition_timings.push_back(total_ms_elapsed.count());
    }
    time_system.advanceTimeWait(clock_tick);
    total_ms_elapsed += clock_tick;
  } while (total_ms_elapsed <= duration);
  EXPECT_EQ(acquisition_timings,
            std::vector<int64_t>({450,   750,   1250,  2350,  2850,  3850,  4150,  4350,  4450,
                                  5750,  5950,  6350,  7850,  8350,  8550,  9850,  10150, 10450,
                                  10550, 11950, 12250, 12550, 13250, 13550, 13650, 13750, 13850}));
}

TEST_F(ZipfRateLimiterImplTest, BadArgumentsTest) {
  // Zipf preconditions are q > 1, v > 0, verify we guard appropriately.
  std::list<std::tuple<double, double>> bad_q_v_pairs{
      {1.0, 1.0} /*borderline bad q*/,
      {1.1, 0.0} /*borderline bad v*/,
      {1.0, 0.0} /*borderline bad both*/,
      {0.9, 1.0},
      {1.1, -1.0},
      {-1, 1.0},
  };

  for (const auto& pair : bad_q_v_pairs) {
    EXPECT_THROW(ZipfRateLimiterImpl rate_limiter(std::make_unique<NiceMock<MockRateLimiter>>(),
                                                  std::get<0>(pair), std::get<1>(pair)),
                 NighthawkException);
  }
}

// --- M3 Task 3: the live-tunable capability. Live plain-rps re-
// parameterization on the NON-plugin path (the ramping limiter's capture is
// capture-only; its rejection is pinned in live_update_test.cc's factory
// tests). All of these run on simulated time with 1 ms acquisition ticks.

namespace {
// Acquire greedily; returns the number of acquisitions granted.
uint64_t acquireAll(RateLimiter& limiter) {
  uint64_t n = 0;
  while (limiter.tryAcquireOne()) {
    n++;
  }
  return n;
}

// White-box probe of the credit bookkeeping (the members are protected).
class CreditProbeLinearRateLimiter : public LinearRateLimiter {
public:
  using LinearRateLimiter::LinearRateLimiter;
  void forceCredit(int64_t credit) { acquireable_count_ = credit; }
  int64_t credit() const { return acquireable_count_; }
  uint64_t acquired() const { return acquired_count_; }
};

class CreditProbeRampingRateLimiter : public LinearRampingRateLimiterImpl {
public:
  using LinearRampingRateLimiterImpl::LinearRampingRateLimiterImpl;
  void forceCredit(int64_t credit) { acquireable_count_ = credit; }
  int64_t credit() const { return acquireable_count_; }
  uint64_t acquired() const { return acquired_count_; }
};
} // namespace

// The §8.7 linear degenerate case: a live up-bend conserves the cumulative
// acquired count exactly at the bend, the instantaneous rate steps there
// (left-limit == old 100 ms grid, right-limit == new 50 ms grid, first
// acquisition half a new interval after the bend), and no burst/silence
// beyond one acquisition interval (at most one acquisition per 1 ms tick:
// the OLD schedule's leftover credit is discarded, never handed out).
TEST_F(RateLimiterTest, RpsBendPreservesCumulativeCountAndRate) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRateLimiter bent(time_system, 10_Hz);
  LinearRateLimiter counterfactual(time_system, 10_Hz);

  uint64_t total_bent = 0;
  uint64_t total_cf = 0;
  for (int i = 0; i < 1000; i++) { // one second on the 10 Hz schedule
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_EQ(total_bent, 10u);
  ASSERT_EQ(total_bent, total_cf);

  // Bend at t_b: conservation at the bend is exact.
  const absl::Status status = bent.ApplyLiveRps(time_system.monotonicTime(), 20);
  ASSERT_TRUE(status.ok()) << status.message();
  EXPECT_EQ(total_bent, total_cf);

  uint64_t post_bend = 0;
  std::vector<int64_t> post_bend_acquisition_ms;
  for (int i = 0; i < 1000; i++) { // one more second
    const uint64_t n = acquireAll(bent);
    if (n > 0) {
      EXPECT_EQ(n, 1u); // never a burst: one acquisition per tick at most
      post_bend_acquisition_ms.push_back(i);
    }
    post_bend += n;
    time_system.advanceTimeWait(1ms);
  }
  // One second at the new rate (the one-interval slack absorbs the
  // phase-shifted grid's floating-point boundary rounding).
  EXPECT_LE(total_bent + post_bend, 31u);
  EXPECT_GE(total_bent + post_bend, 29u);
  // Right-limit grid: first acquisition half a new interval (50 ms) after
  // the bend; successive acquisitions one new interval apart.
  ASSERT_FALSE(post_bend_acquisition_ms.empty());
  EXPECT_NEAR(post_bend_acquisition_ms.front(), 25, 1);
  for (size_t i = 1; i < post_bend_acquisition_ms.size(); i++) {
    EXPECT_NEAR(post_bend_acquisition_ms[i] - post_bend_acquisition_ms[i - 1], 50, 1);
  }
}

// A deep down-bend conserves the count and clamps: nothing is handed out
// from negative credit, and the first acquisition under the new (much
// slower) schedule lands only once the new schedule's integral catches up
// (at the half-interval phase point), never as a windfall at the bend.
TEST_F(RateLimiterTest, ApplyLiveRpsDeepDownBendConservesAndClamps) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRateLimiter limiter(time_system, 100_Hz);
  uint64_t total = 0;
  for (int i = 0; i < 1000; i++) { // one second at 100 Hz
    total += acquireAll(limiter);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_EQ(total, 100u);

  const absl::Status status = limiter.ApplyLiveRps(time_system.monotonicTime(), 1);
  ASSERT_TRUE(status.ok()) << status.message();

  int64_t first_acquisition_ms = -1;
  for (int i = 0; i < 1000; i++) { // one more second at 1 Hz
    const uint64_t n = acquireAll(limiter);
    if (n > 0 && first_acquisition_ms == -1) {
      first_acquisition_ms = i;
    }
    total += n;
    time_system.advanceTimeWait(1ms);
  }
  // Conserved + exactly one acquisition under the new schedule.
  EXPECT_EQ(total, 101u);
  // ... which lands half a new interval (500 ms) after the bend, not as a
  // burst at the bend.
  EXPECT_NEAR(first_acquisition_ms, 500, 1);
}

// The down-bend clamp contract (pins the truthiness hazard class): credit
// in the clamp regime must be gated strictly on > 0 -- an acquisition is
// never sourced from negative acquireable_count_.
TEST_F(RateLimiterTest, NegativeCreditClampedNotTruthy) {
  Envoy::Event::SimulatedTimeSystem time_system;
  CreditProbeLinearRateLimiter limiter(time_system, 10_Hz);
  limiter.forceCredit(-5);
  EXPECT_FALSE(limiter.tryAcquireOne());
  // The gate recomputes credit rather than spending the negative balance:
  // no acquisition is sourced from it, and the acquired count is untouched.
  EXPECT_GE(limiter.credit(), 0);
  EXPECT_EQ(limiter.acquired(), 0u);
}

// An apply invoked meaningfully ahead of the agreed bend instant is rejected
// without mutation: the schedule keeps running at the old rate.
TEST_F(RateLimiterTest, ApplyLiveRpsRejectsFutureBendWithoutMutating) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRateLimiter limiter(time_system, 10_Hz);
  uint64_t total = 0;
  for (int i = 0; i < 500; i++) { // half a second at 10 Hz
    total += acquireAll(limiter);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_EQ(total, 5u);

  const absl::Status status = limiter.ApplyLiveRps(time_system.monotonicTime() + 1s, 20);
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), testing::HasSubstr("bend instant is in the future"));

  // Untouched: still on the old 10 Hz grid.
  for (int i = 0; i < 500; i++) {
    total += acquireAll(limiter);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_EQ(total, 10u);
}

// A non-positive target rate is rejected without mutation.
TEST_F(RateLimiterTest, ApplyLiveRpsRejectsNonPositiveRps) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRateLimiter limiter(time_system, 10_Hz);
  const absl::Status status = limiter.ApplyLiveRps(time_system.monotonicTime(), 0);
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), testing::HasSubstr(">= 1"));
}

// C1 superseded (M3 Task 5): the plugin-path rps knob IS ramp
// re-parameterization. Applying an rps delta through the ramping limiter
// re-anchors the schedule: cumulative count conserved at the bend (the bent
// limiter matches the counterfactual up to it), then the cadence follows the
// new target (5 Hz over the next second, vs the counterfactual's 10 Hz) --
// the same update surface as the non-plugin path (Review Focus 2, plugin leg).
TEST_F(RateLimiterTest, RampingLimiterApplyLiveRpsReanchors) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRampingRateLimiterImpl bent(time_system, 200ms, 10_Hz);
  LinearRampingRateLimiterImpl counterfactual(time_system, 200ms, 10_Hz);

  uint64_t total_bent = 0;
  uint64_t total_cf = 0;
  for (int i = 0; i < 600; i++) {
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_GT(total_bent, 0u);
  ASSERT_EQ(total_bent, total_cf); // conserved up to the bend
  const uint64_t at_bend = total_bent;

  const absl::Status status = bent.ApplyLiveRps(time_system.monotonicTime(), 5);
  ASSERT_TRUE(status.ok()) << status.message();

  // Post-bend: the bent limiter releases at 5 Hz (3 grants in the next 600ms,
  // plus/minus the grid), the counterfactual at its unchanged 10 Hz (6).
  for (int i = 0; i < 600; i++) {
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_NEAR(total_bent - at_bend, 3, 1);
  EXPECT_NEAR(total_cf - at_bend, 6, 1);
}

// The ramp-shape bend itself (A2b): collapse a 2s ramp to 500ms mid-ramp at
// the same 10 Hz target. Count conserved at the bend; afterwards the
// instantaneous rate is the re-anchored new curve's (steepened: the collapse
// multiplies the rate by sqrt(T_A/T_B) at the re-anchored point), and the
// schedule reaches the full target much sooner than the counterfactual.
TEST_F(RateLimiterTest, RampingBendPreservesCumulativeCountAndRate) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRampingRateLimiterImpl bent(time_system, 2s, 10_Hz);
  LinearRampingRateLimiterImpl counterfactual(time_system, 2s, 10_Hz);

  uint64_t total_bent = 0;
  uint64_t total_cf = 0;
  for (int i = 0; i < 500; i++) { // 500ms into the 2s ramp
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_GT(total_bent, 0u);
  ASSERT_EQ(total_bent, total_cf); // conserved up to the bend
  const uint64_t at_bend = total_bent;

  const absl::Status status =
      bent.ApplyLiveRamp(time_system.monotonicTime(), RampShapeChange{500ms, std::nullopt});
  ASSERT_TRUE(status.ok()) << status.message();

  // Post-bend over 1750ms: the collapsed ramp completes at 500ms re-anchored
  // elapsed, so the bent limiter is at its full 10 Hz long before the
  // counterfactual (still ramping until its 2s mark). The design note's
  // closed form: r_B(tau) = e_b * sqrt(f_A f_B / (T_A T_B)) ~= 2.5x the old
  // instantaneous rate at the bend.
  for (int i = 0; i < 1750; i++) {
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_GT(total_bent - at_bend, 15); // well into the new curve
  EXPECT_GT(total_cf - at_bend, 0);
  EXPECT_LT(total_cf - at_bend, total_bent - at_bend); // the bend actually steepened
}

// The R3 ruling through the real limiter: a hard down-bend conserves the
// count, never grants from negative credit (credit >= 0 at every probe), and
// the post-bend silence is bounded by two of the new schedule's intervals.
TEST_F(RateLimiterTest, RampingDownBendConservesAndHoldsAtZeroCredit) {
  Envoy::Event::SimulatedTimeSystem time_system;
  CreditProbeRampingRateLimiter bent(time_system, 200ms, 50_Hz);
  LinearRampingRateLimiterImpl counterfactual(time_system, 200ms, 50_Hz);

  uint64_t total_bent = 0;
  uint64_t total_cf = 0;
  for (int i = 0; i < 600; i++) {
    total_bent += acquireAll(bent);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_GT(total_bent, 0u);
  ASSERT_EQ(total_bent, total_cf); // conserved up to the bend

  const absl::Status status =
      bent.ApplyLiveRps(time_system.monotonicTime(), 5); // 50 Hz -> 5 Hz
  ASSERT_TRUE(status.ok()) << status.message();

  int min_credit = 0;
  uint64_t grants_after_bend = 0;
  uint64_t grants_cf = 0;
  bool first_grant_seen = false;
  std::chrono::nanoseconds first_grant_gap{0};
  for (int i = 0; i < 1000; i++) { // 1s at the new 5 Hz: ~5 grants
    const int64_t credit_before = bent.credit();
    min_credit = std::min(min_credit, static_cast<int>(credit_before));
    if (bent.tryAcquireOne()) {
      grants_after_bend++;
      if (!first_grant_seen) {
        first_grant_seen = true;
        first_grant_gap = std::chrono::milliseconds(i);
      }
    }
    grants_cf += counterfactual.tryAcquireOne() ? 1 : 0;
    time_system.advanceTimeWait(1ms);
  }
  // Credit never sourced from, or left at, a negative balance (R3: the
  // origin solve makes negative credit unreachable under the re-anchor).
  EXPECT_EQ(min_credit, 0);
  // The first post-bend grant is bounded by two of the new schedule's
  // intervals (200ms each): 400ms plus slack.
  EXPECT_TRUE(first_grant_seen);
  EXPECT_LE(first_grant_gap, 500ms);
  // ~5 grants at the new rate, ~50 at the counterfactual's unchanged 50 Hz.
  EXPECT_NEAR(grants_after_bend, 5, 2);
  EXPECT_NEAR(grants_cf, 50, 2);
}

// The truthiness-fix pin (M3 Task 5, the A2b note's R3): the ramping
// limiter's credit gate must be strictly greater-than, so an acquisition is
// never sourced from a negative balance. The stock bare-truthiness form
// (`if (acquireable_count_)`) granted from forced negative credit (credit
// dropped to -6); the fixed gate rejects it. The re-anchor mechanism makes
// negative credit unreachable in practice -- this pins the gate itself as
// defense in depth.
TEST_F(RateLimiterTest, RampingLimiterNegativeCreditClamped) {
  Envoy::Event::SimulatedTimeSystem time_system;
  CreditProbeRampingRateLimiter limiter(time_system, 2s, 10_Hz);
  acquireAll(limiter); // start the schedule
  limiter.forceCredit(-5);
  const bool granted = limiter.tryAcquireOne();
  // An acquisition may only be granted from freshly recomputed (non-negative)
  // credit -- never sourced from the negative balance.
  EXPECT_TRUE(!granted || limiter.credit() >= 0)
      << "an acquisition was sourced from negative acquireable_count_";
  EXPECT_EQ(limiter.acquired(), granted ? 1u : 0u);
}

// The bend contract: an apply invoked meaningfully AHEAD of t_bend is
// rejected without mutation; invalid schedule deltas are rejected.
TEST_F(RateLimiterTest, RampingApplyLiveRampRejectsFutureBendAndInvalidDelta) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRampingRateLimiterImpl limiter(time_system, 2s, 10_Hz);
  acquireAll(limiter);
  time_system.advanceTimeWait(100ms);

  const absl::Status future =
      limiter.ApplyLiveRamp(time_system.monotonicTime() + std::chrono::hours(1),
                            RampShapeChange{std::nullopt, 20u});
  EXPECT_EQ(future.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(future.message(), HasSubstr("bend instant is in the future"));

  const absl::Status zero_target =
      limiter.ApplyLiveRamp(time_system.monotonicTime(), RampShapeChange{std::nullopt, 0u});
  EXPECT_EQ(zero_target.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(zero_target.message(), HasSubstr("target rps must be >= 1"));

  const absl::Status zero_ramp =
      limiter.ApplyLiveRamp(time_system.monotonicTime(), RampShapeChange{0ns, 20u});
  EXPECT_EQ(zero_ramp.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(zero_ramp.message(), HasSubstr("ramp time must be positive"));

  const absl::Status zero_rps = limiter.ApplyLiveRps(time_system.monotonicTime(), 0);
  EXPECT_EQ(zero_rps.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(zero_rps.message(), HasSubstr("target rps must be >= 1"));
}

// An un-started schedule has no operating point to conserve: the bend adopts
// the new schedule directly (stock anchoring applies from the first attempt).
TEST_F(RateLimiterTest, RampingApplyLiveRampAdoptsUnstartedSchedule) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRampingRateLimiterImpl limiter(time_system, 2s, 10_Hz);
  const absl::Status status =
      limiter.ApplyLiveRamp(time_system.monotonicTime(), RampShapeChange{std::nullopt, 20u});
  ASSERT_TRUE(status.ok()) << status.message();
  uint64_t total = 0;
  for (int i = 0; i < 1000; i++) { // one second at the new 20 Hz ramp
    total += acquireAll(limiter);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_NEAR(total, 5, 2); // 20 Hz ramping to full over 2s: round(1s * 10Hz / 2) = 5
}

// The ramp knob is plugin-path only: the plain linear limiter's capability
// surface rejects a ramp delta honestly (the validator gates it first; this
// is the backstop).
TEST_F(RateLimiterTest, LinearLimiterApplyLiveRampRejected) {
  Envoy::Event::SimulatedTimeSystem time_system;
  LinearRateLimiter limiter(time_system, 10_Hz);
  acquireAll(limiter);
  time_system.advanceTimeWait(100ms);
  const absl::Status status =
      limiter.ApplyLiveRamp(time_system.monotonicTime(), RampShapeChange{1s, 20u});
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), HasSubstr("linear-ramping rate limiter plugin"));
}

// The Q7 rollback form through the real limiter (M3 Task 6's count-
// conservation assert across the rollback): a worker that applied a bend and
// then got the rollback re-apply of the PRIOR schedule at the rollback
// instant t_r ends up -- cumulative acquisitions identical to the
// never-bent counterfactual at t_r and beyond. This is exactly why the
// rollback must re-anchor at the ACTUAL t_r (solve the prior schedule's
// origin so its integrated releases to t_r equal the acquired count): a
// naive re-apply at the original t_bend would leave the post-bend releases
// double-counted against a diverged acquired count.
TEST_F(RateLimiterTest, RollbackReApplyAtTRConservesAgainstNeverBentCounterfactual) {
  Envoy::Event::SimulatedTimeSystem time_system;
  // The worker's limiter, the never-bent counterfactual, and a stand-in for
  // a worker that never got the failed bend at all.
  LinearRateLimiter worker(time_system, 10_Hz);
  LinearRateLimiter counterfactual(time_system, 10_Hz);

  uint64_t total = 0;
  uint64_t total_cf = 0;
  for (int i = 0; i < 500; i++) { // 500ms at 10 Hz
    total += acquireAll(worker);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_EQ(total, total_cf);

  // The (successful) forward bend at t_bend: 10 -> 20 Hz.
  const absl::Status bend = worker.ApplyLiveRps(time_system.monotonicTime(), 20);
  ASSERT_TRUE(bend.ok()) << bend.message();

  // The bend acts for 300ms: the post-bend releases (about 6 at 20 Hz) put
  // the worker's acquired count on the BENT schedule -- the state a naive
  // rollback at t_bend could never repair.
  for (int i = 0; i < 300; i++) {
    total += acquireAll(worker);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  ASSERT_GT(total, total_cf) << "the bend must have measurably diverged before the rollback";

  // The rollback at t_r (the actual now): re-apply the PRIOR 10 Hz. Q7's
  // conservation at t_r: no release is revoked (the acquired count is frozen
  // as the restored schedule's baseline) and no catch-up credit is granted
  // (the restored schedule re-anchors so its integrated releases continue
  // from the frozen count -- never a windfall that would close the gap).
  const uint64_t gap_at_t_r = total - total_cf;
  const absl::Status rollback = worker.ApplyLiveRps(time_system.monotonicTime(), 10);
  ASSERT_TRUE(rollback.ok()) << rollback.message();
  uint64_t total_right_after_rollback = total;
  for (int i = 0; i < 5; i++) { // the first ticks after the rollback
    total_right_after_rollback += acquireAll(worker);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_LE(total_right_after_rollback - total, 1u)
      << "the rollback itself must neither revoke nor windfall the count";

  // Beyond t_r, the rolled-back worker runs the prior 10 Hz schedule and the
  // bend's count divergence stays frozen (bounded by one acquisition
  // interval of drift): identical rate, no catch-up burst, no silence.
  for (int i = 0; i < 1000; i++) { // 1s at 10 Hz on both
    total += acquireAll(worker);
    total_cf += acquireAll(counterfactual);
    time_system.advanceTimeWait(1ms);
  }
  EXPECT_NEAR(total - total_cf, gap_at_t_r, 1)
      << "the divergence the bend produced must be conserved across the "
         "rollback, never amplified (catch-up burst) or revokes (silence)";
}

} // namespace Nighthawk

