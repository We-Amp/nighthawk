#pragma once
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic warning "-Wunused-parameter"
#endif
#include "api/client/service.grpc.pb.h"
#include "api/request_source/service.grpc.pb.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <thread>

#include "absl/synchronization/mutex.h"

#include "source/client/live_update_orchestrator.h"
#include "source/common/common/logger.h"
#include "source/common/common/thread.h"
#include "source/common/event/real_time_system.h"
#include "source/exe/process_wide.h"

#include "nighthawk/client/process.h"
#include "nighthawk/common/request_source.h"

namespace Nighthawk {
namespace Client {

/**
 * Implements Nighthawk's gRPC service. This service allows load generation to be
 * controlled by gRPC clients.
 */
class ServiceImpl final : public nighthawk::client::NighthawkService::Service,
                          public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {

public:
  /**
   * Constructs a new ServiceImpl instance
   */
  ServiceImpl() : process_wide_(std::make_shared<Envoy::ProcessWide>()) {
    logging_context_ = std::make_unique<Envoy::Logger::Context>(
        spdlog::level::from_str("info"), "[%T.%f][%t][%L] %v", log_lock_, false);
  }

  ServiceImpl(std::unique_ptr<Envoy::Logger::Context>&& logging_context)
      : process_wide_(std::make_shared<Envoy::ProcessWide>()) {
    logging_context_ = std::move(logging_context);
  }

  grpc::Status
  ExecutionStream(grpc::ServerContext* context,
                  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                                           nighthawk::client::ExecutionRequest>* stream) override;

private:
  void handleExecutionRequest(
      const nighthawk::client::ExecutionRequest& request, grpc::ServerContext* context,
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                               nighthawk::client::ExecutionRequest>* stream);
  void writeResponse(
      const nighthawk::client::ExecutionResponse& response,
      grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                               nighthawk::client::ExecutionRequest>* stream);
  grpc::Status finishGrpcStream(const bool success, absl::string_view description = "");
  // FORK-ONLY (We-Amp): live steering (M3). Routes an update_request /
  // cancellation_request arm to the in-flight run. Called on the reader
  // thread. "No run in flight" tears the stream down (the only surviving
  // teardown case); a run in flight keeps the stream up.
  grpc::Status dispatchUpdateOrCancellation(
      const nighthawk::client::ExecutionRequest& request);

  Envoy::Thread::MutexBasicLockable log_lock_;
  std::unique_ptr<Envoy::Logger::Context> logging_context_;
  std::shared_ptr<Envoy::ProcessWide> process_wide_;
  Envoy::Event::RealTimeSystem time_system_; // NO_CHECK_FORMAT(real_time)
  std::future<void> future_;
  // accepted_lock_ and accepted_event_ are used to synchronize the threads
  // when starting up a future to service a test, and ensure the code servicing it
  // in the other thread has acquired busy_lock_.
  Envoy::Thread::MutexBasicLockable accepted_lock_;
  Envoy::Thread::CondVar accepted_event_;
  // busy_lock_ is used to test from the service thread to query if there's
  // an active test being run.
  Envoy::Thread::MutexBasicLockable busy_lock_;
  // FORK-ONLY (We-Amp): serializes every write to the execution stream. Two
  // writers exist when live snapshot streaming is opted in: the run thread
  // (final response) and the live snapshot collector thread (snapshot
  // responses). gRPC ServerReaderWriter::Write is not safe for concurrent
  // use, so every write must take this mutex.
  absl::Mutex stream_write_mutex_;
  // FORK-ONLY (We-Amp): live steering (M3). False while the stream must not
  // carry update acks: the run thread flips it to false under
  // stream_write_mutex_ immediately before the final response is written,
  // and on an accepted cancellation arm — both writes and both flips take
  // the same mutex, so an ack can never land after the final output. Re-armed
  // to true at the start of each run.
  bool acks_allowed_ ABSL_GUARDED_BY(stream_write_mutex_) = false;
  // FORK-ONLY (We-Amp): the in-flight run registry. The run thread registers
  // its per-run orchestrator (owned by the run thread's scope, carrying the
  // ProcessImpl handle via its cancel/phase closures) and clears the entry
  // before process shutdown; the reader thread dispatches update/cancel arms
  // while holding in_flight_mutex_, so a dispatched arm can never race
  // process shutdown.
  absl::Mutex in_flight_mutex_;
  LiveUpdateOrchestrator* in_flight_orchestrator_ ABSL_GUARDED_BY(in_flight_mutex_) = nullptr;
  // FORK-ONLY (We-Amp): test seam — replaces the default applier (which
  // rejects every delta) with a test double. Must be set before the run
  // starts; read once per run on the run thread.
  std::function<std::unique_ptr<LiveUpdateOrchestrator::Applier>()> update_applier_factory_;

public:
  // Test-only: inject a LiveUpdateOrchestrator::Applier factory for the next
  // and following runs of this service instance.
  void SetUpdateApplierFactoryForTest(
      std::function<std::unique_ptr<LiveUpdateOrchestrator::Applier>()> factory) {
    update_applier_factory_ = std::move(factory);
  }
};

/**
 * Dummy implementation of our request-source gRPC service definition, for testing and experimental
 * purposes.
 */
class RequestSourceServiceImpl final
    : public nighthawk::request_source::NighthawkRequestSourceService::Service,
      public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {

public:
  /**
   * Constructs a new RequestSourceServiceImpl instance.
   */
  RequestSourceServiceImpl() {
    logging_context_ = std::make_unique<Envoy::Logger::Context>(
        spdlog::level::from_str("info"), "[%T.%f][%t][%L] %v", log_lock_, false);
  }

  grpc::Status RequestStream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<nighthawk::request_source::RequestStreamResponse,
                               nighthawk::request_source::RequestStreamRequest>* stream) override;

private:
  Envoy::Thread::MutexBasicLockable log_lock_;
  std::unique_ptr<Envoy::Logger::Context> logging_context_;
  RequestSourcePtr createStaticEmptyRequestSource(const uint32_t amount);
};

} // namespace Client
} // namespace Nighthawk
