# Profiling a DEBUG SUT through the MCP profile pipeline

The first agent-driven drive against a DEBUG-build Envoy SUT surfaced three
instrument facts worth writing down: the symbolization cliff on DEBUG
binaries, the legacy gperftools staged-profile format, and the 4096-byte
direct-response cap that pushes large-body arms onto a local origin. This
note documents the workarounds; none of them require changes to the
pipeline itself.

## The symbolization cliff on DEBUG binaries

`go tool pprof -proto` against a multi-gigabyte DEBUG binary is unusable in
the fold loop:

- a 57 KB raw CPU profile kept the symbolizer burning more than one core for
  over 15 minutes without finishing; profiles above ~60 KB did not finish in
  an hour;
- `go tool pprof`'s own ELF symtab path resolved NO function names against
  either the full binary or an `objcopy --strip-debug` copy — its symtab
  reader silently fails on this binary shape (fast, but empty).

The workaround that works, per drive measurement (57 KB profile, 771
locations, ~17 s with real demangled names):

1. Sample slower. `CPUPROFILE_FREQUENCY=25` is honored by the gperftools
   profiler in Envoy builds that call plain `ProfilerStart` (verify via the
   profile's `Period: 10000000` ns = 25 Hz). Default 100 Hz just multiplies
   the fold cost for no analytical gain at the drive's rates.
2. Keep a strip-debug copy of the SUT binary alongside the profiled one
   (`objcopy --strip-debug bin/envoy bin/envoy-sym` — typically ~7x smaller).
3. Drive the fold through `--pprof-tool` with a wrapper, not the stock
   `go tool pprof`: convert the staged profile via `go tool pprof -proto`
   against the strip-debug copy (fast), then resolve every remaining
   location against the full binary's symtab (nm bisect + c++filt) and
   inject the Function records into the proto.

Cost of the symtab route: file/line information is lost (every diff row
carries empty `file` and `line=0`); function names are exact. The real fix
is profiling a non-DEBUG SUT or shipping a fast symbolizer in the fold
pipeline — the wrapper is a workaround, not a lane.

## Staged profiles are LEGACY gperftools binary format

For a gperftools SUT, the staged bytes the retriever fetches (and the
session record's `raw_profile` carries) are the LEGACY binary profile, not
a pprof proto. The pipeline's own machine route converts through
`go tool pprof`; any external consumer or symbolizer must too — parsing the
staged bytes in-process only works for proto-native sources (the
documented raw-proto fallback).

## Large-body arms: the 4096-byte direct-response cap

Envoy caps direct-response bodies at 4096 bytes (both `inline_string` and
`filename`; config validation rejects larger). A per-request vs per-byte
scaling scenario cannot use direct responses for 16/64 KB arms.

Local-origin recipe (hermetic, no external network):

1. Serve the body sizes from a static origin on loopback — any static file
   server works (e.g. `nginx` with one `location` per size, or a minimal
   python http server for throwaway drives).
2. Configure the SUT as a proxy: one route per body size, all pointing at a
   single static HTTP/1 cluster on 127.0.0.1.
3. Keep the origin unprofiled and treat it as a shared constant across the
   comparison: every arm traverses the same upstream codec path, so
   cross-arm differentials attribute the body-size axis. Attribution of the
   SUT's own byte path in isolation is weaker than a true direct response —
   expect upstream-codec frames in the diffs; name that confound in the
   reading.

Standing gates (window purity, same-state refusal) apply unchanged to
proxied arms.
