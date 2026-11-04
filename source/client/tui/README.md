# nighthawk_tui — dependency quarantine

## Standing guard

Run this after every change that touches `tui_lib`'s dependency graph (it is
re-run in Task 7 on a clean checkout). **The output must stay empty**:

```bash
a=$(bazel cquery --config=macos 'deps(//source/client/tui:tui_lib)' --output=label 2>/dev/null | sort); b=$(bazel cquery --config=macos 'kind("cc.* rule", @envoy//source/...)' --output=label 2>/dev/null | sort); printf 'tui_deps=%s envoy_cc=%s\n' "$(echo "$a" | wc -l)" "$(echo "$b" | wc -l)" >&2; [ "$(echo "$a" | wc -l)" -gt 0 ] && [ "$(echo "$b" | wc -l)" -gt 0 ] && comm -12 <(echo "$a") <(echo "$b")
```

Both label streams are counted before the intersection is trusted: the counts
go to stderr, and either a zero count or a non-empty `comm` output fails the
check (nonzero exit) — stdout must stay empty. A count of zero means the
cquery itself failed, not that the graph is clean.

**Coverage caveat:** `@envoy//test/...` targets are covered only indirectly —
Envoy test helpers typically link Envoy source libs transitively, so they show
up via `@envoy//source/...`; a header-only `@envoy//test` target with no
transitive source deps would pass the guard as written.

## Threading model (M0b)

Implemented as specified (design spec §4): the FTXUI loop (`nighthawk_tui`) owns
the terminal; `RunFeed` owns its dedicated gRPC read thread (all feed callbacks
fire there); `SessionWriter` owns its own writer thread behind the bounded
`SessionRecordQueue` (drop-newest policy); state crosses threads only as
immutable snapshots (`SnapshotStore`/`RunViewModel`). The lifecycle logic lives
in the headless `RunController` (`tui_app.h`); the interactive shell only
composes `RenderLayout` and the key map on top.

**pprof worker pool: deferred to M1.** This is an inference from the milestone
ladder, not a spec sanction: M0b has no pprof work to fold (the pool's first
consumer is M1's flamegraph pipeline), so introducing it here would be dead
code. The other named-thread contracts above are implemented as specified.

## Profile fetch layer (M1 T2)

`AdminClient` (plain-HTTP/1.1, POST-only admin mutators; /server_info clock
source is the RFC 7231 `Date` response header — the JSON body carries no
current-time field in Envoy 1.40.2) + `ProfileFetcher` (synchronous window
orchestration; disable-before-fetch ordering) + pluggable `ProfileRetriever`
(`VolumeMountRetriever` / `ScpRetriever`). **Format boundary:** the staged
`.pprof` is the SUT's raw profile in the legacy gperftools binary format —
NOT gzip and NOT a pprof proto despite the extension; it is readable only
through ProfileSymbolizer's go-tool conversion (M1 T3), never by TUI code.

## The quarantine rule

The TUI (`nighthawk_tui`, `//source/client/tui:tui_lib`) may depend only on
the allowlist below. It must stay clear of Envoy's C++ sources.

**Why the floor is `@envoy//source/**`:** the generated nighthawk protos and
the `@envoy_api` proto targets compile only protobuf-generated C++ under the
repo's pinned protobuf toolchain — no Envoy sources, no Envoy headers, no mac
rot. The rot lives in Envoy's C++ *sources* (the platform-sensitive code that
needed the `nh-mac-stub` crutch). So `@envoy//source/**` is exactly the
surface the guard must keep empty: an intersection there means the TUI has
started compiling Envoy C++ again, and with it the build fragility the TUI is
supposed to be independent of. The guard is written as an intersection over
`deps(tui_lib)` rather than a direct-deps check so that a violation added
anywhere in the transitive closure — not just in this BUILD file — is caught.

## Session format evolution

Record types in `api/client/session.proto` evolve by adding `oneof` cases: a
reader that hits an unknown case in a `format_version`-"1" file must report it
as an unknown-record version skew — never silent skip, never corruption
(M1's `profile_window` is the first such case; the M1 plan owns the
reader-side implementation). See the header comment there for the rule.

## FTXUI fetch caveat

`bazel fetch @ftxui//...` fails on `@ftxui//:tests`: its googletest
dev-dependency is not wired by Bazel 9 for non-root modules. This is expected,
not a gate failure — the four library targets the TUI consumes
(`@ftxui//:dom`, `@ftxui//:screen`, `@ftxui//:component`, `@ftxui//:ftxui`)
resolve cleanly from bcr.bazel.build.

## Allowlist

Everything else in the TUI's dependency graph is out:

- FTXUI (`@ftxui//:dom`, `@ftxui//:screen`, `@ftxui//:component`, `@ftxui//:ftxui`)
- TCLAP (`@tclap//:tclap`, pinned 1.2.5.bcr.1 — the version already in the
  lockfile graph before this module pinned it explicitly)
- grpc, protobuf, abseil-cpp, googletest (the module-level deps the protos and
  the test pull in)
- `//api/client` generated protos (`grpc_service_lib`, `base_cc_proto`) and
  their transitive `@envoy_api` proto-generated code

Notably NOT allowed: `//source/common` (its `Utility::parseCommand` helper is
re-implemented inline in `tui_app.cc` for exactly this reason), Envoy test
helpers (`//test/test_common`, `@envoy//test/...`), and any
`@envoy//source/...` target.
