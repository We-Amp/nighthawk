#pragma once

#include "api/client/live.pb.h"

#include "nighthawk/client/output_collector.h"

namespace Nighthawk {
namespace Client {

/**
 * Process context is shared between the CLI and grpc service. It is capable of executing
 * a full Nighthawk test run.
 */
class Process {
public:
  virtual ~Process() = default;

  /**
   * @param collector used to transform output into the desired format.
   * @return bool true iff execution was successfull.
   */
  virtual bool run(OutputCollector& collector) PURE;

  /**
   * Shuts down the worker. Mandatory call before destructing.
   */
  virtual void shutdown() PURE;

  /**
   * Will request all workers to cancel execution asap.
   */
  virtual bool requestExecutionCancellation() PURE;

  /**
   * FORK-ONLY (We-Amp): drains the immutable per-worker live snapshot partials
   * accumulated so far and aggregates them into a LiveSnapshot. Intended to be
   * called periodically from the service's collector thread while a run is in
   * flight; safe to call concurrently with a running benchmark.
   *
   * @return nighthawk::client::LiveSnapshot the aggregated snapshot.
   */
  virtual nighthawk::client::LiveSnapshot toLiveSnapshot() PURE;
};

using ProcessPtr = std::unique_ptr<Process>;

} // namespace Client
} // namespace Nighthawk
