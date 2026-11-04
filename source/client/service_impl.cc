#include "source/client/service_impl.h"

#include <grpc++/grpc++.h>

#include "envoy/config/core/v3/base.pb.h"

#include "source/client/client.h"
#include "source/client/options_impl.h"
#include "source/client/output_collector_impl.h"
#include "source/common/request_source_impl.h"

#include "absl/strings/str_cat.h"
#include "absl/time/time.h"

#include <atomic>
#include <chrono>
#include <thread>

namespace Nighthawk {
namespace Client {

void ServiceImpl::handleExecutionRequest(
    const nighthawk::client::ExecutionRequest& request, grpc::ServerContext* context,
    grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                             nighthawk::client::ExecutionRequest>* stream) {
  std::unique_ptr<Envoy::Thread::LockGuard> busy_lock;
  {
    // Lock accepted_lock, in case we get here before accepted_event_.wait() is entered.
    auto accepted_lock = std::make_unique<Envoy::Thread::LockGuard>(accepted_lock_);
    // Acquire busy_lock_, and signal that we did so, allowing the service to continue
    // processing inbound requests on the stream.
    busy_lock = std::make_unique<Envoy::Thread::LockGuard>(busy_lock_);
    accepted_event_.notifyOne();
  }
  // FORK-ONLY (We-Amp): re-arm the stream's update-ack gate for this run (M3
  // live steering). It was flipped false by this run's predecessor before its
  // final response, or by an accepted cancellation; this run's updates may
  // ack again from here on.
  {
    absl::MutexLock lock(&stream_write_mutex_);
    acks_allowed_ = true;
  }

  nighthawk::client::ExecutionResponse response;
  OptionsPtr options;
  try {
    options = std::make_unique<OptionsImpl>(request.start_request().options());
  } catch (const MalformedArgvException& e) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    response.mutable_error_detail()->set_message(e.what());
    writeResponse(response, stream);
    return;
  }
  envoy::config::core::v3::TypedExtensionConfig typed_dns_resolver_config;
  Envoy::Network::DnsResolverFactory& dns_resolver_factory =
      Envoy::Network::createDefaultDnsResolverFactory(typed_dns_resolver_config);

  absl::StatusOr<ProcessPtr> process_or_status = ProcessImpl::CreateProcessImpl(
      *options, dns_resolver_factory, std::move(typed_dns_resolver_config), time_system_,
      process_wide_);
  if (!process_or_status.ok()) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    response.mutable_error_detail()->set_message(
        fmt::format("Unable to create ProcessImpl: {}", process_or_status.status().ToString()));
    writeResponse(response, stream);
    return;
  }
  ProcessPtr process = std::move(*process_or_status);
  // FORK-ONLY (We-Amp): ensures shutdown() runs on every exit path. The
  // normal path calls it explicitly below (after the collector and watchdog
  // joins) and marks this guard done; on an exception unwind the guard fires
  // before the ProcessPtr destructor, whose ~ProcessImpl RELEASE_ASSERTs that
  // shutdown was called. Declared immediately after the ProcessPtr, so it
  // unwinds just before it (and after the orchestrator's own teardown).
  struct ShutdownOnExit {
    ~ShutdownOnExit() {
      if (!done && *process != nullptr) {
        (*process)->shutdown();
      }
    }
    ProcessPtr* process;
    bool done = false;
  } shutdown_on_exit{&process};

  // FORK-ONLY (We-Amp): live steering (M3). One update orchestrator per run,
  // owned by this scope; the reader thread reaches it through the in-flight
  // registry below. Its ack writer wraps the result into an ExecutionResponse
  // and writes it under the stream write mutex — the acks_allowed_ gate
  // checked under that same mutex is what keeps an ack from ever landing
  // after the final output (M4's no-ack-after-final-output rule).
  std::unique_ptr<LiveUpdateOrchestrator::Applier> update_applier =
      update_applier_factory_ ? update_applier_factory_() : MakeDefaultUpdateApplier();
  ProcessImpl* process_impl_ptr = static_cast<ProcessImpl*>(process.get());
  LiveUpdateOrchestrator orchestrator(
      update_applier.get(),
      [this, stream](const nighthawk::client::UpdateResult& result) {
        nighthawk::client::ExecutionResponse ack_response;
        *ack_response.mutable_update_result() = result;
        absl::MutexLock lock(&stream_write_mutex_);
        if (!acks_allowed_) {
          return false;
        }
        if (!stream->Write(ack_response)) {
          ENVOY_LOG(warn, "Failed to write update ack to the stream");
          return false;
        }
        return true;
      },
      [process_impl_ptr]() { process_impl_ptr->requestExecutionCancellation(); },
      [process_impl_ptr]() { return process_impl_ptr->currentPhaseId(); },
      options->executionId().value_or(""));
  {
    absl::MutexLock lock(&in_flight_mutex_);
    in_flight_orchestrator_ = &orchestrator;
  }
  // FORK-ONLY (We-Amp): RAII unregister guard (mirrors CollectorJoin /
  // WatchdogJoin). The normal path unregisters explicitly below, before
  // process shutdown, and marks this guard done; if the run unwinds through
  // an exception instead, this guard fires while busy_lock_ is still held —
  // clearing the registry entry (so no dispatched arm can dereference the
  // orchestrator that is about to be destroyed) and flagging the dispatch
  // path to reject arms immediately and honestly.
  struct InFlightUnregister {
    ~InFlightUnregister() {
      if (done) {
        return;
      }
      absl::MutexLock lock(&mutex);
      *orchestrator_slot = nullptr;
      *aborted = true;
    }
    absl::Mutex& mutex;
    LiveUpdateOrchestrator** orchestrator_slot;
    bool* aborted;
    bool done = false;
  } in_flight_unregister{in_flight_mutex_, &in_flight_orchestrator_, &run_dispatch_aborted_};

  // FORK-ONLY (We-Amp): stream-at-boundary phase events (M2 Task 7 wiring,
  // DQ-3 ruling: boundary events are METADATA, never synthetic metric
  // snapshots). The callback fires on the run-calling thread — inside
  // process->run() below — so each boundary response is written before the
  // final output response; the only concurrent writer is the live snapshot
  // collector, and the shared stream-write mutex serializes both. A failed
  // write means the stream is gone (client cancelled); stop delivering.
  if (options->liveStreamingEnabled()) {
    // m8: the collector's write contract — a failed Write means the stream
    // is gone (client cancelled); stop producing boundary responses.
    std::atomic<bool> phase_event_stream_gone{false};
    static_cast<ProcessImpl&>(*process).setPhaseEventCallback(
        [this, stream, &phase_event_stream_gone](const Nighthawk::Client::PhaseEvent& event) {
          if (!event.boundary_complete ||
              phase_event_stream_gone.load(std::memory_order_relaxed)) {
            return;
          }
          nighthawk::client::ExecutionResponse response;
          nighthawk::client::PhaseBoundaryEvent* boundary = response.mutable_phase_event();
          boundary->set_phase_index(event.phase_index);
          boundary->set_phase_id(event.phase_id);
          boundary->set_wall_time_unix_ns(event.wall_time_unix_ns);
          boundary->set_monotonic_ns(event.monotonic_ns);
          boundary->set_ending_phase_index(event.ending_phase_index);
          boundary->set_ending_phase_id(event.ending_phase_id);
          boundary->mutable_drain_duration()->set_seconds(event.drain_duration_ns / 1000000000);
          boundary->mutable_drain_duration()->set_nanos(event.drain_duration_ns % 1000000000);
          if (event.idle_gap_ns >= 0) {
            boundary->mutable_idle_gap()->set_seconds(event.idle_gap_ns / 1000000000);
            boundary->mutable_idle_gap()->set_nanos(event.idle_gap_ns % 1000000000);
          }
          boundary->set_worker_count(event.worker_count);
          boundary->set_next_phase_cold_started(event.next_phase_cold_started);
          boundary->set_drain_censored(event.boundary_censored);
          absl::MutexLock lock(&stream_write_mutex_);
          if (!stream->Write(response)) {
            phase_event_stream_gone.store(true, std::memory_order_relaxed);
          }
        });
  }

  OutputCollectorImpl output_collector(time_system_, *options);

  // FORK-ONLY (We-Amp): when the request opted in to live snapshot streaming,
  // a collector thread emits aggregated LiveSnapshot responses at the stats
  // flush cadence while the run is in flight. Partials arrive from the workers
  // as immutable messages; the collector never reads worker histograms. It is
  // stopped and joined before the final response is written below, so no
  // snapshot can follow the final response on the stream, and nothing from
  // this run can bleed into a follow-up run.
  std::atomic<bool> stop_live_collector{false};
  std::thread live_collector_thread;
  if (options->liveStreamingEnabled()) {
    const Envoy::Protobuf::Duration flush_duration = options->statsFlushIntervalDuration();
    const std::chrono::milliseconds live_interval =
        (flush_duration.seconds() > 0 || flush_duration.nanos() > 0)
            ? std::chrono::milliseconds(flush_duration.seconds() * 1000 +
                                        flush_duration.nanos() / 1000000)
            : std::chrono::seconds(options->statsFlushInterval());
    live_collector_thread =
        std::thread([this, stream, process_ptr = process.get(), live_interval,
                     &stop_live_collector]() {
          auto next_tick = std::chrono::steady_clock::now() + live_interval;
          while (!stop_live_collector.load(std::memory_order_relaxed)) {
            // Sleep in small slices so a stop request joins this thread promptly.
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            if (std::chrono::steady_clock::now() < next_tick) {
              continue;
            }
            next_tick += live_interval;
            nighthawk::client::ExecutionResponse snapshot_response;
            *snapshot_response.mutable_live_snapshot() = process_ptr->toLiveSnapshot();
            {
              // As everywhere else, the stream write goes through the write
              // mutex; a failed write means the stream is gone (e.g. the
              // client cancelled the RPC), so stop producing snapshots.
              absl::MutexLock lock(&stream_write_mutex_);
              if (!stream->Write(snapshot_response)) {
                return;
              }
            }
          }
        });
  }
  // Stops and joins the collector no matter how run() ends, so no snapshot is
  // ever produced after the run finished.
  struct CollectorJoin {
    ~CollectorJoin() {
      stop->store(true, std::memory_order_relaxed);
      if (thread->joinable()) {
        thread->join();
      }
    }
    std::atomic<bool>* stop;
    std::thread* thread;
  };
  CollectorJoin collector_join{&stop_live_collector, &live_collector_thread};
  // Watch the gRPC context while the run is in flight: when the client cancels
  // the RPC, request the workers to stop asap instead of running for the full
  // configured duration. The watchdog is joined again on every exit path of
  // run() before the process shuts down.
  std::atomic<bool> stop_watchdog{false};
  std::thread watchdog_thread;
  if (context != nullptr) {
    watchdog_thread = std::thread([context, process_ptr = process.get(), &stop_watchdog]() {
      while (!stop_watchdog.load(std::memory_order_relaxed)) {
        if (context->IsCancelled()) {
          // The run may have completed on its own in the meantime; calling
          // this is harmless, and it cannot race with process shutdown,
          // which only starts after this thread has been joined.
          process_ptr->requestExecutionCancellation();
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    });
  }
  // Stops and joins the watchdog no matter how run() ends (success, failure or
  // exception), so no thread is ever left running while the process shuts down
  // below.
  struct WatchdogJoin {
    ~WatchdogJoin() {
      stop->store(true, std::memory_order_relaxed);
      if (thread->joinable()) {
        thread->join();
      }
    }
    std::atomic<bool>* stop;
    std::thread* thread;
  };
  const bool ok = [&]() {
    WatchdogJoin watchdog_join{&stop_watchdog, &watchdog_thread};
    // FORK-ONLY (We-Amp): test seam — throws exercise the run-exception
    // unwind path (registry guard, honest arm rejection).
    if (run_failure_injection_) {
      run_failure_injection_();
    }
    return process->run(output_collector);
  }();
  // Stop and join the collector before we fill and write the final response
  // below: no snapshot may follow the final response on the stream. The
  // CollectorJoin guard above remains as an exception-safety backstop, but is
  // a no-op from here on since the thread is joined.
  stop_live_collector.store(true, std::memory_order_relaxed);
  if (live_collector_thread.joinable()) {
    live_collector_thread.join();
  }
  if (!ok) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    // TODO(https://github.com/envoyproxy/nighthawk/issues/181): wire through error descriptions, so
    // we can do better here.
    response.mutable_error_detail()->set_message(
        "Unknown failure. See Nighthawk Service logs. Make sure the URI is well formed and the DNS "
        "name resolves (if applicable). Check the output for problematic counter values. The "
        "default Nighthawk failure predicates report failure if (1) Nighthawk could not connect to "
        "the target (see 'benchmark.pool_connection_failure' counter; check the address and port "
        "number, and try explicitly setting --address-family v4 or v6, especially when using DNS; "
        "instead of localhost try 127.0.0.1 or ::1 explicitly), (2) the protocol was not supported "
        "by the target (see 'benchmark.stream_resets' counter; check http/https in the URI, --h2), "
        "(3) the target returned a 4xx or 5xx HTTP response code (see 'benchmark.http_4xx' and "
        "'benchmark.http_5xx' counters; check the URI path and the server config), or (4) a custom "
        "gRPC RequestSource failed. To relax expectations, set explicit failure predicates in the "
        "benchmark request.");
  }
  *(response.mutable_output()) = output_collector.toProto();
  // FORK-ONLY (We-Amp): from here on the stream must not carry update acks —
  // the final output stays the last response, ever (M4). Both gates flip
  // before the final write: the run-side flag makes pending updates resolve
  // without a write, and the acks_allowed_ flip under the stream write mutex
  // closes the window for any ack already past its orchestrator-side gate.
  orchestrator.OnRunEnded();
  {
    absl::MutexLock lock(&stream_write_mutex_);
    acks_allowed_ = false;
  }
  {
    // Unregister before shutdown: the reader thread dispatches arms while
    // holding in_flight_mutex_, so no arm can be in flight against this
    // process past this point. Marks the RAII guard done (it would otherwise
    // fire again at scope exit, after shutdown).
    absl::MutexLock lock(&in_flight_mutex_);
    in_flight_orchestrator_ = nullptr;
    in_flight_unregister.done = true;
  }
  process->shutdown();
  shutdown_on_exit.done = true;
  // We release before writing the response to avoid a race with the client's follow up request
  // coming in before we release the lock, which would lead up to us declining service when
  // we should not.
  busy_lock.reset();
  writeResponse(response, stream);
  // Join pending ack threads after the final response: they are gated off
  // (orchestrator flags + acks_allowed_) and bounded by the applier deadline,
  // and must be dead before this scope's applier/orchestrator go away. The
  // join deliberately happens after the final write, so a still-pending
  // update's bounded wait never delays the final output.
  orchestrator.JoinPendingAcks();
}

void ServiceImpl::writeResponse(
    const nighthawk::client::ExecutionResponse& response,
    grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                             nighthawk::client::ExecutionRequest>* stream) {
  ENVOY_LOG(debug, "Write response: {}", absl::StrCat(response));
  // Every stream write takes stream_write_mutex_: with live snapshot streaming
  // opted in, the collector thread writes snapshot responses concurrently with
  // this run thread, and gRPC stream writes are not safe for concurrent use.
  // The stream is the one this run was started on, passed down from
  // ExecutionStream: a member would let a second concurrent stream repoint it
  // and deliver this run's responses to another client.
  absl::MutexLock lock(&stream_write_mutex_);
  if (!stream->Write(response)) {
    ENVOY_LOG(warn, "Failed to write response to the stream");
  }
}

grpc::Status ServiceImpl::finishGrpcStream(const bool success, absl::string_view description) {
  // We may get here while there's still an active future in-flight in the error-paths.
  // Allow it to wrap up and put it's response on the stream before finishing the stream.
  if (future_.valid()) {
    future_.wait();
  }
  return success ? grpc::Status::OK
                 : grpc::Status(grpc::StatusCode::INTERNAL, std::string(description));
}

grpc::Status ServiceImpl::dispatchUpdateOrCancellation(
    const nighthawk::client::ExecutionRequest& request) {
  // FORK-ONLY (We-Amp): live steering (M3 Task 1). "Run in flight" is
  // busy_lock_ being held: the run thread holds it from just after accepting
  // a start request until it releases it before its final response. The
  // teardown path below is the only surviving reject-and-teardown case.
  if (busy_lock_.tryLock()) {
    busy_lock_.unlock();
    return finishGrpcStream(false,
                            "No benchmark run is in flight; update and cancellation requests "
                            "are only supported while a run is active.");
  }
  // The run thread registers the orchestrator shortly after acquiring
  // busy_lock_ (process construction happens between the two); wait a
  // bounded moment for the registration instead of failing the arm. If the
  // run aborted instead (exception unwind), reject immediately and honestly.
  // in_flight_mutex_ is held across the whole dispatch below: the run
  // thread's unregister-before-shutdown takes the same mutex, so a dispatched
  // arm can never race process shutdown.
  in_flight_mutex_.Lock();
  in_flight_mutex_.AwaitWithDeadline(absl::Condition(this, &ServiceImpl::ArmDispatchable),
                                     absl::Now() + absl::Seconds(2));
  if (in_flight_orchestrator_ == nullptr) {
    const bool aborted = run_dispatch_aborted_;
    in_flight_mutex_.Unlock();
    return finishGrpcStream(false, aborted ? "The benchmark run failed before it could accept "
                                             "updates or cancellations."
                                           : "Run did not become dispatchable in time.");
  }
  LiveUpdateOrchestrator* orchestrator = in_flight_orchestrator_;
  if (request.has_cancellation_request()) {
    auto rejection = orchestrator->CheckCancellation(request.cancellation_request().execution_id());
    if (rejection.has_value()) {
      // Targeting mismatch: structured ack, the stream stays up. Return OK
      // directly: finishGrpcStream would wait on future_, blocking the
      // reader thread until the in-flight run ends.
      orchestrator->WriteAckSync(*rejection);
      in_flight_mutex_.Unlock();
      return grpc::Status::OK;
    }
    // Flip the ack gate before accepting, so "cancellation accepted" and "no
    // ack from here on" are ordered without a window (M4).
    {
      absl::MutexLock lock(&stream_write_mutex_);
      acks_allowed_ = false;
    }
    orchestrator->AcceptCancellation();
  } else {
    orchestrator->HandleUpdate(request.update_request());
  }
  in_flight_mutex_.Unlock();
  // OK without finishGrpcStream: the run is still in flight, and the reader
  // loop must keep reading (ExecutionStream continues on OK).
  return grpc::Status::OK;
}

// TODO(oschaaf): implement a way to cancel test runs, and update rps config on the fly.
// TODO(oschaaf): unit-test Process, create MockProcess & use in service_test.cc / client_test.cc
// TODO(oschaaf): should we merge incoming request options with defaults?
// TODO(oschaaf): aggregate the client's logs and forward them in the grpc response.
grpc::Status ServiceImpl::ExecutionStream(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                             nighthawk::client::ExecutionRequest>* stream) {
  nighthawk::client::ExecutionRequest request;

  while (stream->Read(&request)) {
    ENVOY_LOG(debug, "Read ExecutionRequest data {}", absl::StrCat(request));
    if (request.has_start_request()) {
      // If busy_lock_ is held we can't start a new benchmark run because one is active already.
      if (busy_lock_.tryLock()) {
        busy_lock_.unlock();
        Envoy::Thread::LockGuard accepted_lock(accepted_lock_);
        // We pass in std::launch::async to avoid lazy evaluation, as we want this to run
        // asap. See: https://en.cppreference.com/w/cpp/thread/async
        future_ = std::future<void>(std::async(
            std::launch::async, &ServiceImpl::handleExecutionRequest, this, request, context,
            stream));
        // Block until the thread associated to the future has acquired busy_lock_
        accepted_event_.wait(accepted_lock_);
      } else {
        return finishGrpcStream(false, "Only a single benchmark session is allowed at a time.");
      }
    } else if (request.has_update_request() || request.has_cancellation_request()) {
      auto status = dispatchUpdateOrCancellation(request);
      if (!status.ok()) {
        return status;
      }
      // The arm was dispatched to the in-flight run; keep reading.
    } else {
      PANIC("not reached");
    }
  }
  return finishGrpcStream(true);
}

namespace {
void addHeader(envoy::config::core::v3::HeaderMap* map, absl::string_view key,
               absl::string_view value) {
  auto* request_header = map->add_headers();
  request_header->set_key(std::string(key));
  request_header->set_value(std::string(value));
}
} // namespace

RequestSourcePtr RequestSourceServiceImpl::createStaticEmptyRequestSource(const uint32_t amount) {
  Envoy::Http::RequestHeaderMapPtr header = Envoy::Http::RequestHeaderMapImpl::create();
  header->addCopy(Envoy::Http::LowerCaseString("x-from-remote-request-source"), "1");
  return std::make_unique<StaticRequestSourceImpl>(std::move(header), amount);
}

grpc::Status RequestSourceServiceImpl::RequestStream(
    grpc::ServerContext* /*context*/,
    grpc::ServerReaderWriter<nighthawk::request_source::RequestStreamResponse,
                             nighthawk::request_source::RequestStreamRequest>* stream) {
  nighthawk::request_source::RequestStreamRequest request;
  bool ok = true;
  while (stream->Read(&request)) {
    ENVOY_LOG(trace, "Inbound RequestStreamRequest {}", absl::StrCat(request));

    // TODO(oschaaf): this is useful for integration testing purposes, but sending
    // these nearly empty headers will basically be a near no-op (note that the client will merge
    // headers we send here into into own header configuration). The client can be configured to
    // connect to a custom grpc service as a remote data source instead of this one, and its workers
    // will comply. That in itself may be useful. But we could offer the following features here:
    // 1. Yet another remote request source, so we balance to-be-replayed headers over workers
    //    and only have a single stream to a remote service here.
    // 2. Read a and dispatch a header stream from disk.
    RequestSourcePtr request_source = createStaticEmptyRequestSource(request.quantity());
    RequestGenerator request_generator = request_source->get();
    RequestPtr request;
    while (ok && (request = request_generator()) != nullptr) {
      HeaderMapPtr headers = request->header();
      nighthawk::request_source::RequestStreamResponse response;
      auto* request_specifier = response.mutable_request_specifier();
      auto* request_headers = request_specifier->mutable_v3_headers();
      headers->iterate([&request_headers](const Envoy::Http::HeaderEntry& header)
                           -> Envoy::Http::HeaderMap::Iterate {
        addHeader(request_headers, header.key().getStringView(), header.value().getStringView());
        return Envoy::Http::RequestHeaderMap::Iterate::Continue;
      });
      // TODO(oschaaf): add static configuration for other fields plus expectations
      ok = ok && stream->Write(response);
    }
    if (!ok) {
      ENVOY_LOG(error, "Failed to send the complete set of replay data.");
      break;
    }
  }
  ENVOY_LOG(trace, "Finishing stream");
  return ok ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INTERNAL, std::string("error"));
}

} // namespace Client
} // namespace Nighthawk
