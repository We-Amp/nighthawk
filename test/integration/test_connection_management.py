"""Test connection management of nighthawk's load generator."""

import json
import logging
import os
import sys
import pytest

from test.integration.integration_test_fixtures import (http_test_server_fixture,
                                                        quic_test_server_fixture, server_config,
                                                        server_config_quic)
from test.integration import asserts
from test.integration import utility


def _run_with_number_of_connections(fixture,
                                    number_of_connections,
                                    expected_connections=-1,
                                    max_pending_requests=0,
                                    requests_per_connection=1024 * 1024,
                                    rps=100,
                                    run_test_expectation=True,
                                    h2=False):
  if expected_connections == -1:
    expected_connections = number_of_connections
  # We add a delay to responses to make sure connections are needed, as the pool creates connections on-demand.
  args = [
      fixture.getTestServerRootUri(), "--rps",
      str(rps), "--duration", "2", "--request-header", "x-envoy-fault-delay-request:500",
      "--max-pending-requests",
      str(max_pending_requests), "--max-requests-per-connection",
      str(requests_per_connection), "--connections",
      str(number_of_connections)
  ]
  if h2:
    args.append("--h2")
  parsed_json, _ = fixture.runNighthawkClient(args)
  counters = fixture.getNighthawkCounterMapFromJson(parsed_json)
  if run_test_expectation:
    asserts.assertCounterEqual(counters, "upstream_cx_total", expected_connections)
  return counters


# A series that tests with queueing disabled
@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_1(http_test_server_fixture):
  """Test http h1 connection management with 1 connection and queueing disabled."""
  _run_with_number_of_connections(http_test_server_fixture, 1)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_2(http_test_server_fixture):
  """Test http h1 connection management with 2 connections and queueing disabled."""
  _run_with_number_of_connections(http_test_server_fixture, 2, run_test_expectation=False)


# A series that tests with queueing enabled
@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_with_queue_1(http_test_server_fixture):
  """Test http h1 connection management with 1 connection and queueing enabled."""
  _run_with_number_of_connections(http_test_server_fixture, 1, max_pending_requests=5)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_with_queue_5(http_test_server_fixture):
  """Test http h1 connection management with 5 connections and queueing enabled."""
  _run_with_number_of_connections(http_test_server_fixture, 5, max_pending_requests=5)


def _connection_management_test_request_per_connection(fixture, requests_per_connection, use_h2):
  max_requests_per_conn = 5
  counters = _run_with_number_of_connections(fixture,
                                             1,
                                             max_pending_requests=1,
                                             requests_per_connection=max_requests_per_conn,
                                             run_test_expectation=False,
                                             h2=use_h2)
  requests = counters["upstream_rq_total"]
  asserts.assertCounterBetweenInclusive(counters, "upstream_cx_total",
                                        (requests / max_requests_per_conn),
                                        (requests / max_requests_per_conn) + 1)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_single_request_per_conn_1(http_test_server_fixture):
  """Test h1 with a single request_per_connection."""
  _connection_management_test_request_per_connection(http_test_server_fixture, 1, False)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_connection_management_single_request_per_conn_5(http_test_server_fixture):
  """Test h1 with a request_per_connection set to 5."""
  _connection_management_test_request_per_connection(http_test_server_fixture, 5, False)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h2_connection_management_single_request_per_conn_1(http_test_server_fixture):
  """Test h2 with a single request_per_connection."""
  _connection_management_test_request_per_connection(http_test_server_fixture, 1, True)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h2_connection_management_single_request_per_conn_1(http_test_server_fixture):
  """Test h2 with a request_per_connection set to 5."""
  _connection_management_test_request_per_connection(http_test_server_fixture, 5, True)


def test_h1_pool_strategy_mru(http_test_server_fixture):
  """Test connection re-use strategies of the http 1 connection pool.

  Test that with the "most recently used" (mru) strategy only the first created connection gets to send requests.
  """
  _, logs = http_test_server_fixture.runNighthawkClient([
      "--rps 5", "-v", "trace", "--duration", "20", "--connections", "2", "--prefetch-connections",
      "--experimental-h1-connection-reuse-strategy", "mru", "--termination-predicate",
      "benchmark.http_2xx:4",
      http_test_server_fixture.getTestServerRootUri()
  ])

  # Expect the second connection to not send any messages
  asserts.assertNotIn("[Tags: \"ConnectionId\":\"1\"] message complete", logs)
  # Expect that we sent some traffic through the first connection
  asserts.assertIn("[Tags: \"ConnectionId\":\"0\"] message complete", logs)


def test_h1_pool_strategy_lru(http_test_server_fixture):
  """Test connection re-use strategies of the http 1 connection pool.

  Test that with the "least recently used" (lru) strategy all connections are used with roughly equal distribution.
  """
  requests = 12
  connections = 3
  _, logs = http_test_server_fixture.runNighthawkClient([
      "--rps",
      "5",
      "-v trace",
      "--duration",
      "20",
      "--connections",
      str(connections),
      "--prefetch-connections",
      "--experimental-h1-connection-reuse-strategy",
      "lru",
      "--termination-predicate",
      # termination-predicate takes affect when it exceeds the limit. Therefore, set the limit to 1 less than the desired number of requests.
      "benchmark.http_2xx:%d" % (requests - 1),
      http_test_server_fixture.getTestServerRootUri()
  ])

  line_counts = []
  for i in range(1, connections):
    line_counts.append(
        float(utility.count_log_lines_with_substring(logs, "[C%d] message complete" % i)))

  average_line_count = sum(line_counts) / len(line_counts)
  for line_count in line_counts:
    asserts.assertBetweenInclusive(
        line_count,
        # provide a little slack. Minimum of 2 as envoy logs "message complete" twice per request.
        average_line_count - 2,
        average_line_count + 2)


# FORK-ONLY (We-Amp): N2 / O-F1. A warm phase and a measure phase in one
# process (--phases) keep their connections across the boundary: the run opens
# exactly the warm phase's connection set and closes none before teardown.
def _warm_measure_phases(concurrency, rps=60, duration="2s"):
  return json.dumps([{
      "name": name,
      "duration": duration,
      "requests_per_second": rps,
      "concurrency": str(concurrency)
  } for name in ("warm", "measure")])


def _assert_one_connection_set(counters, expected_connections):
  asserts.assertCounterEqual(counters, "upstream_cx_total", expected_connections)
  # Counters are read before teardown drains the pools: a close during the run
  # would show here.
  assert "upstream_cx_destroy" not in counters, counters


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_phases_carry_connections_over_boundary(http_test_server_fixture):
  """Test that h1 connections opened in the warm phase serve the measure phase."""
  concurrency = 2
  connections = 3
  parsed_json, _ = http_test_server_fixture.runNighthawkClient([
      http_test_server_fixture.getTestServerRootUri(), "--phases",
      _warm_measure_phases(concurrency), "--connections",
      str(connections), "--prefetch-connections"
  ])
  counters = http_test_server_fixture.getNighthawkCounterMapFromJson(parsed_json)
  _assert_one_connection_set(counters, concurrency * connections)
  # Both phases report their own result and metadata.
  phase_results = [r["name"] for r in parsed_json["results"] if r["name"].startswith("phase_")]
  assert phase_results == ["phase_0_warm", "phase_1_measure"], phase_results
  assert len(parsed_json["phase_metadata"]) == 2, parsed_json["phase_metadata"]


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h2_phases_carry_connections_over_boundary(http_test_server_fixture):
  """Test that each worker's h2 connection serves both phases."""
  concurrency = 2
  parsed_json, _ = http_test_server_fixture.runNighthawkClient([
      "--protocol", "http2",
      http_test_server_fixture.getTestServerRootUri(), "--phases",
      _warm_measure_phases(concurrency), "--max-active-requests", "1"
  ])
  counters = http_test_server_fixture.getNighthawkCounterMapFromJson(parsed_json)
  # One multiplexed connection per worker.
  _assert_one_connection_set(counters, concurrency)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_h3_phases_carry_connections_over_boundary(quic_test_server_fixture):
  """Test that each worker's h3 (QUIC) connection serves both phases."""
  concurrency = 2
  parsed_json, _ = quic_test_server_fixture.runNighthawkClient([
      "--protocol",
      "http3",
      quic_test_server_fixture.getTestServerRootUri(),
      "--phases",
      _warm_measure_phases(concurrency),
      "--max-active-requests",
      "1",
      # Envoy doesn't support disabling certificate verification on Quic
      # connections, so the host in our requests has to match the hostname in
      # the leaf certificate.
      "--request-header",
      "Host:www.lyft.com"
  ])
  counters = quic_test_server_fixture.getNighthawkCounterMapFromJson(parsed_json)
  _assert_one_connection_set(counters, concurrency)



# FORK-ONLY (We-Amp): NH1. Per-worker rate weights split a phase's rate by
# weight, and the per-worker series (--worker-series) shows the realised split
# in the JSON output: one point per worker per stats flush, plus a run-end
# point carrying the worker's final counters.
def _weighted_phase(rps, weights, duration="4s"):
  return json.dumps([{
      "name": "skew",
      "duration": duration,
      "requests_per_second": rps,
      "concurrency": str(len(weights)),
      "worker_weights": weights
  }])


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_weighted_phase_worker_series_shows_split(http_test_server_fixture):
  """Test that weights [3, 1] at 80 rps pace 60 and 20 rps, as the series shows."""
  parsed_json, _ = http_test_server_fixture.runNighthawkClient([
      http_test_server_fixture.getTestServerRootUri(), "--phases",
      _weighted_phase(80, [3, 1]), "--worker-series", "--stats-flush-interval", "1"
  ])
  metadata = parsed_json["phase_metadata"]
  assert len(metadata) == 1, metadata
  assert metadata[0]["worker_requests_per_second"] == [60, 20], metadata[0]

  points = {}
  for point in parsed_json["worker_series"]:
    points.setdefault(int(point.get("worker_id", 0)), []).append(point)
  assert sorted(points) == [0, 1], sorted(points)
  final = {}
  for worker, series in points.items():
    # Proto JSON omits false booleans: only the run-end point carries the mark.
    assert series[-1].get("run_end") is True, series[-1]
    assert all(not p.get("run_end", False) for p in series[:-1])
    stamps = [int(p["monotonic_ns"]) for p in series]
    assert stamps == sorted(stamps), stamps
    final[worker] = int(series[-1]["counters"]["upstream_rq_total"])
    worker_result = next(r for r in parsed_json["results"] if r["name"] == "worker_%d" % worker)
    worker_counters = {c["name"]: int(c["value"]) for c in worker_result["counters"]}
    assert final[worker] == worker_counters["upstream_rq_total"], (worker, final[worker])
  assert final[1] > 0, final
  ratio = final[0] / final[1]
  asserts.assertBetweenInclusive(ratio, 2.5, 3.5)


@pytest.mark.skipif(utility.isSanitizerRun(), reason="Unstable in sanitizer runs")
def test_http_h1_weighted_phase_refuses_uneven_split(http_test_server_fixture):
  """Test that a rate the weights cannot split into whole per-worker rates is refused."""
  _, logs = http_test_server_fixture.runNighthawkClient([
      http_test_server_fixture.getTestServerRootUri(), "--phases",
      _weighted_phase(81, [3, 1])
  ],
                                                        expect_failure=True,
                                                        as_json=False)
  assert "does not split into whole per-worker rates" in logs, logs
