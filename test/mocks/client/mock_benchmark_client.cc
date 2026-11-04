#include "test/mocks/client/mock_benchmark_client.h"

#include <string>
#include <vector>

namespace Nighthawk {
namespace Client {

namespace {
// Static storage for the reference-returning fork-only methods' default
// actions; the references must outlive any mock instance.
std::string& EmptyPhaseId() {
  static std::string* empty = new std::string;
  return *empty;
}
std::vector<PhaseStatisticSlab>& EmptySlabs() {
  static std::vector<PhaseStatisticSlab>* empty = new std::vector<PhaseStatisticSlab>;
  return *empty;
}
} // namespace

MockBenchmarkClient::MockBenchmarkClient() {
  // Reference-returning fork-only methods get safe default actions: calls
  // without expectations return an empty phase id and no slabs.
  ON_CALL(*this, currentPhaseId)
      .WillByDefault([]() -> const std::string& { return EmptyPhaseId(); });
  ON_CALL(*this, phaseSlabs)
      .WillByDefault([]() -> const std::vector<PhaseStatisticSlab>& { return EmptySlabs(); });
}

}
} // namespace Nighthawk