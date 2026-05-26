See README.md for what kq is and [Usage guide](docs/USAGE.md) for SQL
and snapshot-format details.

## Build and test

Builds go through Bazel (Bzlmod). Never `cargo build` / `cargo test` — Cargo
is the source of truth for crates only. The repo root has no targets;
everything lives under `//kq/...`.

```bash
bazel build -c opt //kq:kq              # optimized CLI
bazel test //kq/...                     # all tests
bazel test //kq/loader:loader_test      # single target
bazel test //kq/... --test_filter=name  # single test within a target
```

## Architecture

Data flow on every CLI invocation:

1. `kq/main.rs` parses snapshot paths + flags.
2. `engine_setup/` builds a `SnapshotLoader` and loads every input.
3. `loader/` auto-detects the format per path: single `.json` / `.json.gz`,
   NDJSON directory, Arrow IPC directory, or Parquet directory. Directory
   snapshots carry a `metadata.json` plus per-resource files (`pods.*`,
   `nodes.*`, `namespaces.*`, `daemonsets.*`).
4. Each resource type becomes an Arrow `RecordBatch`. `SnapshotData` keeps
   **both** merged tables and per-source batches so columnar formats can be
   scanned by DataFusion as partitions without a concat copy — keep that
   invariant when changing the loader.
5. `query/` registers batches as DataFusion `MemTable`s, creates the
   user-facing views (`pods`, `nodes`, `namespaces`, `daemon_sets`), and
   registers Kubernetes-aware UDFs (`parse_cpu`, `parse_memory`,
   `total_cpu_request`, `extract_pool`, …).
6. `output/` renders results as table / JSON / CSV / TSV / compact.

### Gotchas

- Top-level analytic columns (`cluster`, `pool`, `phase`, `namespace`) are
  flattened for fast filters/groupings; nested Kubernetes structs remain
  accessible as `metadata.name`, `spec['nodeName']`, etc.
- **camelCase Kubernetes fields require bracket notation** —
  `spec['nodeName']`, `metadata['creationTimestamp']`. Map keys also use
  brackets: `metadata.labels['app']`.
- The SQL view is `daemon_sets` (snake_case). The on-disk file is
  `daemonsets.ndjson.gz` (no underscore). Don't rename either to match the
  other.

## When changing loader / schema / query registration / output

Before declaring work done, run the focused suite:

```bash
bazel test -c opt //kq:kq_lib_test \
  //kq/cli:cli_test //kq/cli:interactive_test \
  //kq/engine_setup:engine_setup_test \
  //kq/loader:loader_test //kq/memory:memory_test \
  //kq/output:output_test //kq/query:query_test \
  //kq/schema:schema_test //kq/synthetic:synthetic_test \
  //kq/tests:synthetic_snapshot_tests
```

For loader / storage-format / query-perf changes, also run the end-to-end
validation flow in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md#validation-workflow).

## Helper binaries

Each binary in `kq/tools/` has a durable purpose — don't add throwaway
scripts. Full list and purposes:
[docs/DEVELOPMENT.md#build-targets](docs/DEVELOPMENT.md#build-targets).

## Other pointers

- Add a Rust dependency: [docs/DEVELOPMENT.md#dependency-updates](docs/DEVELOPMENT.md#dependency-updates).
- Public-repo discipline (no private hostnames, secrets, real cluster names,
  company labels): [CONTRIBUTING.md#ground-rules](CONTRIBUTING.md#ground-rules).
  Before pushing publicly, grep for
  `internal|private|secret|token|password|api[_-]?key|bearer` and any
  company / domain / cluster names.
