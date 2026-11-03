# nighthawk_tui — dependency quarantine

## Standing guard

Run this after every change that touches `tui_lib`'s dependency graph (it is
re-run in Task 7 on a clean checkout). **The output must stay empty**:

```bash
comm -12 \
  <(bazel cquery --config=macos 'deps(//source/client/tui:tui_lib)' --output=label 2>/dev/null | sort) \
  <(bazel cquery --config=macos 'kind("cc.* rule", @envoy//source/...)' --output=label 2>/dev/null | sort)
```

If either cquery stream comes back empty, the check itself failed — do not
treat that as a pass. A non-empty intersection at any point in M0b is a hard
stop: report it, do not widen the allowlist to make it pass.

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
