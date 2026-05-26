# kq Developer Guide

How kq is structured and how to make changes safely.

## Prerequisites

- Bazel 7.x with Bzlmod.
- Rust toolchain provided through `rules_rust` — day-to-day builds go through
  Bazel, never `cargo`.

## Repository Layout

```text
kq/main.rs                  CLI entry point
kq/lib.rs                   Public library re-exports
kq/cli/                     Interactive and batch-mode CLI behavior
kq/engine_setup/            Snapshot loading plus query-engine creation
kq/loader/                  JSON, NDJSON, Arrow IPC, and Parquet loaders
kq/query/                   DataFusion registration and custom UDFs
kq/schema/                  Arrow schemas for Kubernetes resources
kq/synthetic/               Deterministic synthetic snapshot generator
kq/output/                  Table, JSON, CSV, TSV, and compact output formats
kq/memory/                  Memory reporting helpers
kq/tools/                   Developer and operator helper binaries
kq/tests/                   Integration tests and synthetic fixtures
docs/                       Public usage and developer documentation
```

## Data Flow

CLI flags → `engine_setup` → `loader` (format auto-detect) → Arrow
`RecordBatch`es → `query` registers DataFusion `MemTable`s and UDFs →
`output` renders results.

The loader keeps both merged tables and per-source batches so columnar formats
scan as DataFusion partitions without a concat copy — see
[CLAUDE.md](../CLAUDE.md#architecture) for the invariants to preserve.

## Build Targets

```bash
bazel build -c opt //kq:kq                # CLI
bazel build //kq/tools/...                # all helper binaries
```

Helper binaries in `kq/tools/`:

- `synthetic_snapshot` — deterministic NDJSON snapshot generator.
- `snapshot_convert` — NDJSON → Arrow IPC or Parquet.
- `snapshot_correctness` — diff two snapshot directories.
- `synthetic_query_benchmark`, `engine_setup_benchmark`,
  `registration_hotspot_benchmark`, `analyze_loading_phases`,
  `memory_regression_benchmark` — regression smoke tests.

Each binary has a durable purpose. Avoid throwaway experiments with hard-coded
local paths.

## Testing

```bash
bazel test //kq/...
```

For loader / schema / query-registration / output changes, also run the
focused suite documented in
[CLAUDE.md](../CLAUDE.md#when-changing-loader--schema--query-registration--output).

## Validation Workflow

For loader, storage-format, or query-performance changes, run end-to-end
correctness checks on synthetic data:

1. Generate a synthetic NDJSON snapshot with `//kq/tools:synthetic_snapshot`.
2. Convert it to both IPC and Parquet with `//kq/tools:snapshot_convert`.
3. Diff each converted snapshot against the NDJSON source with
   `//kq/tools:snapshot_correctness`.
4. Run a small `//kq/tools:synthetic_query_benchmark` on each.

For the representative 5k-node profile and four-snapshot memory benchmark, see
[BENCHMARKS](BENCHMARKS.md).

## Dependency Updates

1. Add or update the dependency in `Cargo.toml`.
2. Regenerate `Cargo.lock` and the Bazel crate lock:

   ```bash
   CARGO_BAZEL_REPIN=1 bazel sync --only=crates
   ```

3. Add the dependency to the relevant `deps` list in `BUILD.bazel`.
4. Build and test the target that uses the dependency.

## Public-Readiness Checklist

Before sharing a branch publicly, follow the public-repo discipline in
[CONTRIBUTING](../CONTRIBUTING.md#ground-rules) and
[CLAUDE.md](../CLAUDE.md#public-repo-discipline). Confirm README, usage, and
developer docs describe current behavior, and don't commit generated local
benchmark outputs or profile files.
