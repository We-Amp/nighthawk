// FORK-ONLY (We-Amp): tests for the phase vector option and its validation
// (M2 A1). The phase vector is a whitelist subset on CommandLineOptions; a
// run without it (or with a single phase) stays legacy-shaped.

#include <memory>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "source/client/options_impl.h"
#include "source/client/options_validation_phases.h"
#include "test/client/utility.h"
#include "test/test_common/proto_matchers.h"
#include "test/test_common/utility.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;

namespace Nighthawk {
namespace Client {

namespace {

// Builds a minimal valid CommandLineOptions carrying `phase_count` phases.
// Phase i gets name "phase<i>", a 2s duration and an rps of 50, so vectors
// built by this helper validate by default; tests mutate from there.
nighthawk::client::CommandLineOptions makeOptionsWithPhases(int phase_count) {
  nighthawk::client::CommandLineOptions cmd;
  cmd.mutable_uri()->set_value("http://127.0.0.1/");
  for (int i = 0; i < phase_count; ++i) {
    nighthawk::client::PhaseSpec* phase = cmd.add_phases();
    phase->set_name(fmt::format("phase{}", i));
    phase->mutable_duration()->set_seconds(2);
    phase->set_requests_per_second(50);
  }
  return cmd;
}

} // namespace

// Review Focus 3: a classic options set without a phase vector parses exactly
// as before, and the accessors report the legacy run.
TEST(OptionsPhasesTest, NoPhaseVectorIsLegacyRun) {
  std::unique_ptr<OptionsImpl> options = TestUtility::createOptionsImpl(std::vector<const char*>{
      "nighthawk_client", "--rps", "4", "--duration", "6", "http://127.0.0.1/"});
  EXPECT_FALSE(options->hasPhaseVector());
  EXPECT_TRUE(options->phases().empty());
  EXPECT_EQ(4, options->requestsPerSecond());
  EXPECT_EQ(6s, options->duration());

  CommandLineOptionsPtr cmd = options->toCommandLineOptions();
  EXPECT_EQ(0, cmd->phases_size());
  // Re-parsing the serialized legacy options keeps reporting no vector.
  OptionsImpl round_trip(*cmd);
  EXPECT_FALSE(round_trip.hasPhaseVector());
}

TEST(OptionsPhasesTest, PhaseVectorParsesAndValidates) {
  nighthawk::client::CommandLineOptions cmd = makeOptionsWithPhases(3);
  cmd.mutable_phases(0)->mutable_concurrency()->set_value("2");
  cmd.mutable_phases(0)->mutable_connections()->set_value(8);
  cmd.mutable_phases(0)->mutable_max_pending_requests()->set_value(16);
  cmd.mutable_phases(0)->mutable_max_active_requests()->set_value(32);
  cmd.mutable_phases(1)->set_name("spike");
  cmd.mutable_phases(1)->mutable_duration()->set_seconds(5);
  cmd.mutable_phases(1)->set_requests_per_second(200);
  cmd.mutable_phases(1)->mutable_concurrency()->set_value("4");
  cmd.mutable_phases(2)->mutable_connections()->set_value(8);

  OptionsImpl options(cmd);
  EXPECT_TRUE(options.hasPhaseVector());
  ASSERT_EQ(3, options.phases().size());
  EXPECT_EQ("phase0", options.phases()[0].name());
  EXPECT_EQ(2, options.phases()[0].duration().seconds());
  EXPECT_EQ(50, options.phases()[0].requests_per_second());
  EXPECT_EQ("2", options.phases()[0].concurrency().value());
  EXPECT_EQ(8, options.phases()[0].connections().value());
  EXPECT_EQ(16, options.phases()[0].max_pending_requests().value());
  EXPECT_EQ(32, options.phases()[0].max_active_requests().value());
  EXPECT_EQ("spike", options.phases()[1].name());
  EXPECT_EQ(5, options.phases()[1].duration().seconds());
  EXPECT_EQ(200, options.phases()[1].requests_per_second());
  EXPECT_EQ(50, options.phases()[2].requests_per_second());

  // The parsed vector round-trips through toCommandLineOptions() and the
  // round-tripped proto validates (no top-level duration/concurrency emitted).
  CommandLineOptionsPtr serialized = options.toCommandLineOptions();
  ASSERT_EQ(3, serialized->phases_size());
  for (int i = 0; i < cmd.phases_size(); ++i) {
    EXPECT_THAT(serialized->phases(i), EqualsProto(cmd.phases(i)));
  }
  EXPECT_FALSE(serialized->has_duration());
  EXPECT_FALSE(serialized->has_concurrency());
  OptionsImpl round_trip(*serialized);
  EXPECT_TRUE(round_trip.hasPhaseVector());
  ASSERT_EQ(3, round_trip.phases().size());
  for (int i = 0; i < cmd.phases_size(); ++i) {
    EXPECT_THAT(round_trip.phases()[i], EqualsProto(cmd.phases(i)));
  }
}

TEST(OptionsPhasesTest, PhaseVectorRejectsDuplicateOrEmptyNames) {
  nighthawk::client::CommandLineOptions dup = makeOptionsWithPhases(2);
  dup.mutable_phases(1)->set_name("phase0");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(dup), MalformedArgvException,
                          "phase\\[1\\].*duplicate");

  nighthawk::client::CommandLineOptions empty = makeOptionsWithPhases(2);
  empty.mutable_phases(0)->set_name("");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(empty), MalformedArgvException,
                          "phase\\[0\\].*name");
}

TEST(OptionsPhasesTest, PhaseVectorRejectsTopLevelDurationConcurrencyConflict) {
  nighthawk::client::CommandLineOptions with_duration = makeOptionsWithPhases(2);
  with_duration.mutable_duration()->set_seconds(5);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(with_duration),
                          MalformedArgvException, "mutually exclusive.*duration");

  nighthawk::client::CommandLineOptions with_concurrency = makeOptionsWithPhases(2);
  with_concurrency.mutable_concurrency()->set_value("4");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(with_concurrency),
                          MalformedArgvException, "mutually exclusive.*concurrency");

  // The exclusion applies whenever the vector is present, single phase included.
  nighthawk::client::CommandLineOptions single_with_duration = makeOptionsWithPhases(1);
  single_with_duration.mutable_duration()->set_seconds(5);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(single_with_duration),
                          MalformedArgvException, "mutually exclusive");
}

TEST(OptionsPhasesTest, PhaseVectorRejectsInvalidDurationOrRps) {
  nighthawk::client::CommandLineOptions short_duration = makeOptionsWithPhases(2);
  short_duration.mutable_phases(1)->mutable_duration()->set_seconds(0);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(short_duration),
                          MalformedArgvException, "phase\\[1\\].*duration");

  nighthawk::client::CommandLineOptions zero_rps = makeOptionsWithPhases(2);
  zero_rps.mutable_phases(1)->set_requests_per_second(0);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(zero_rps), MalformedArgvException,
                          "phase\\[1\\].*requests_per_second");
}

TEST(OptionsPhasesTest, PhaseVectorRejectsInvalidOrAutoConcurrency) {
  nighthawk::client::CommandLineOptions auto_concurrency = makeOptionsWithPhases(2);
  auto_concurrency.mutable_phases(0)->mutable_concurrency()->set_value("auto");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(auto_concurrency),
                          MalformedArgvException, "phase\\[0\\].*concurrency");

  nighthawk::client::CommandLineOptions garbage_concurrency = makeOptionsWithPhases(2);
  garbage_concurrency.mutable_phases(1)->mutable_concurrency()->set_value("foo");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(garbage_concurrency),
                          MalformedArgvException, "phase\\[1\\].*concurrency");

  nighthawk::client::CommandLineOptions zero_concurrency = makeOptionsWithPhases(2);
  zero_concurrency.mutable_phases(0)->mutable_concurrency()->set_value("0");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(zero_concurrency),
                          MalformedArgvException, "phase\\[0\\].*concurrency");

  // Numeric per-phase concurrency below or equal to the max over phases parses.
  nighthawk::client::CommandLineOptions ok_concurrency = makeOptionsWithPhases(2);
  ok_concurrency.mutable_phases(0)->mutable_concurrency()->set_value("4");
  ok_concurrency.mutable_phases(1)->mutable_concurrency()->set_value("2");
  OptionsImpl options(ok_concurrency);
  EXPECT_EQ("4", options.phases()[0].concurrency().value());
  EXPECT_EQ("2", options.phases()[1].concurrency().value());
}

// DQ-2 ruling: M2 phase vectors are same-protocol/same-TLS by construction --
// the PhaseSpec whitelist carries no protocol/TLS/uri fields, so a
// cross-protocol vector is unrepresentable. Pin the whitelist shape here.
TEST(OptionsPhasesTest, PhaseSpecIsWhitelistWithoutProtocolTlsUri) {
  const Envoy::Protobuf::Descriptor* descriptor = nighthawk::client::PhaseSpec::descriptor();
  std::vector<std::string> field_names;
  for (int i = 0; i < descriptor->field_count(); ++i) {
    field_names.push_back(std::string(descriptor->field(i)->name()));
  }
  EXPECT_THAT(field_names,
              testing::ElementsAre("name", "duration", "requests_per_second", "concurrency",
                                   "connections", "max_pending_requests", "max_active_requests"));
}

// One phase is legacy-equivalent: it parses without top-level
// duration/concurrency, and Task 3 will treat it as a one-element vector.
TEST(OptionsPhasesTest, SinglePhaseVectorIsLegacyEquivalent) {
  nighthawk::client::CommandLineOptions cmd = makeOptionsWithPhases(1);
  cmd.mutable_phases(0)->mutable_duration()->set_seconds(5);
  cmd.mutable_phases(0)->set_requests_per_second(5);

  OptionsImpl options(cmd);
  EXPECT_TRUE(options.hasPhaseVector());
  ASSERT_EQ(1, options.phases().size());
  EXPECT_EQ(5, options.phases()[0].duration().seconds());

  CommandLineOptionsPtr serialized = options.toCommandLineOptions();
  ASSERT_EQ(1, serialized->phases_size());
  EXPECT_FALSE(serialized->has_duration());
  EXPECT_FALSE(serialized->has_concurrency());
}

// The duration/rps/concurrency-parse checks apply at any vector size: a
// malformed single-phase vector must fail fast instead of silently running
// with a zero duration or zero rps.
TEST(OptionsPhasesTest, SinglePhaseVectorRejectsInvalidDurationOrRps) {
  nighthawk::client::CommandLineOptions short_duration = makeOptionsWithPhases(1);
  short_duration.mutable_phases(0)->mutable_duration()->set_seconds(0);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(short_duration),
                          MalformedArgvException, "phase\\[0\\].*duration");

  nighthawk::client::CommandLineOptions unset_duration = makeOptionsWithPhases(1);
  unset_duration.mutable_phases(0)->clear_duration();
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(unset_duration),
                          MalformedArgvException, "phase\\[0\\].*duration");

  nighthawk::client::CommandLineOptions zero_rps = makeOptionsWithPhases(1);
  zero_rps.mutable_phases(0)->set_requests_per_second(0);
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(zero_rps), MalformedArgvException,
                          "phase\\[0\\].*requests_per_second");
}

TEST(OptionsPhasesTest, SinglePhaseVectorRejectsInvalidOrAutoConcurrency) {
  nighthawk::client::CommandLineOptions auto_concurrency = makeOptionsWithPhases(1);
  auto_concurrency.mutable_phases(0)->mutable_concurrency()->set_value("auto");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(auto_concurrency),
                          MalformedArgvException, "phase\\[0\\].*concurrency");

  nighthawk::client::CommandLineOptions garbage_concurrency = makeOptionsWithPhases(1);
  garbage_concurrency.mutable_phases(0)->mutable_concurrency()->set_value("foo");
  EXPECT_THROW_WITH_REGEX((void)std::make_unique<OptionsImpl>(garbage_concurrency),
                          MalformedArgvException, "phase\\[0\\].*concurrency");
}

// The pure validation entry point agrees with the OptionsImpl-level behavior.
TEST(OptionsPhasesTest, PureValidationEntry) {
  EXPECT_TRUE(validatePhaseVector(nighthawk::client::CommandLineOptions{}).ok());
  EXPECT_TRUE(validatePhaseVector(makeOptionsWithPhases(1)).ok());
  EXPECT_TRUE(validatePhaseVector(makeOptionsWithPhases(3)).ok());

  nighthawk::client::CommandLineOptions conflict = makeOptionsWithPhases(2);
  conflict.mutable_duration()->set_seconds(5);
  const absl::Status status = validatePhaseVector(conflict);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(std::string(status.message()), testing::HasSubstr("mutually exclusive"));
}

} // namespace Client
} // namespace Nighthawk
