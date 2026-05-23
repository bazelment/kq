# kq Benchmarks

This document is for contributors running synthetic regression benchmarks on
kq itself — loader, query, and memory profiles. If you came here to analyze a
real cluster, that workflow lives in [USAGE](USAGE.md); this page deliberately
uses synthetic data so the numbers are repeatable.

Keep benchmark data synthetic and write outputs under `/tmp` so generated
snapshots and summaries are not committed.

Run every command from the repo root with Bazel. The 5k-node profiles below
generate and convert large snapshots — budget several GB of RAM and a few GB of
free space under `/tmp`. The memory benchmark is Linux/WSL-only (see below).
For the broader end-to-end validation flow, see
[DEVELOPMENT](DEVELOPMENT.md#validation-workflow).

## Representative 5k-Node Cluster

Use this shape when you need a production-sized single-cluster snapshot:

```bash
bazel run -c opt //kq/src/bin:synthetic_snapshot -- \
  --output /tmp/kq-bench-5k-ndjson \
  --cluster bench-5k \
  --nodes 5000 \
  --min-pods-per-node 10 \
  --max-pods-per-node 60 \
  --namespaces 240 \
  --seed 42 \
  --overwrite
```

With these parameters each 5k-node snapshot has roughly 175k pods, depending on
the seed. See [USAGE](USAGE.md#trying-kq-without-a-cluster) for what the
generator produces and why it is deterministic.

Convert the snapshot to the preferred repeated-load format:

```bash
bazel run -c opt //kq/src/bin:snapshot_convert -- \
  --input /tmp/kq-bench-5k-ndjson \
  --output /tmp/kq-bench-5k-ipc \
  --format ipc \
  --overwrite
```

## Four-Snapshot Memory Regression

Run this before landing loader, registration, IPC, Parquet, or Arrow batch
changes. It is self-contained — it generates its own four deterministic 5k-node
clusters under `--output-root` (it does not reuse the snapshot from the profile
above), converts them to IPC, loads them together, registers the query engine,
and samples peak memory during the load/registration window:

```bash
bazel run -c opt //kq/src/bin:memory_regression_benchmark -- \
  --snapshot-count 4 \
  --generated-format ipc \
  --output-root /tmp/kq-memory-regression-5k \
  --nodes 5000 \
  --min-pods-per-node 10 \
  --max-pods-per-node 60 \
  --namespaces 240 \
  --seed 42 \
  --sample-interval-ms 1 \
  --json-output /tmp/kq-memory-regression-5k/summary.json
```

This benchmark runs on Linux or WSL only. Every memory sample reads
`/proc/self/status` for process RSS (`VmRSS` / `VmHWM`), so on macOS — or a
container with a restricted `/proc` — the run aborts at the first sample, even
though the heap and jemalloc-resident figures it also collects come from
`jemalloc_ctl` and would be portable on their own.

To turn it into a regression gate, add threshold flags to the command above;
the run fails if a peak is exceeded:

```text
--max-peak-rss-mb 6000              fail above 6 GB peak process RSS
--max-peak-heap-mb 3000             fail above 3 GB peak heap
--max-peak-jemalloc-resident-mb N   fail above N MB peak jemalloc resident
```

Track these fields across runs. They are keys in the `--json-output` JSON —
`peak.*` are nested under the `peak` object, the rest are top-level:

- `peak.process_rss_bytes`
- `peak.heap_allocated_bytes`
- `peak.jemalloc_resident_bytes`
- `peak_rss_bytes_per_row`
- `peak_heap_bytes_per_row`
- `total_time_s`

The stdout summary carries the same run under its own labels (`peak_*_mb`,
`peak_*_bytes_per_row`, `total_time_s`); thresholding is easiest off the JSON.
Prefer bytes-per-row thresholds for code changes that affect generated snapshot
size, and absolute MB thresholds for fixed representative profiles.

A condensed excerpt from a local run on 2026-05-12 (the binary prints more
lines than shown, including `table_count` and the `json_output` path):

```text
snapshot_count: 4
total_rows: 757678
total_time_s: 0.459
samples: 397
peak_process_rss_mb: 2825.61
peak_heap_allocated_mb: 2604.37
peak_jemalloc_resident_mb: 2864.20
peak_rss_bytes_per_row: 3910.5
peak_heap_bytes_per_row: 3604.3
```

Treat this as a local reference point, not a universal threshold. CI or release
gates should set thresholds from repeated runs on the same worker class.

An 8-snapshot scale check — the same command with `--snapshot-count 8` and a
distinct `--output-root` (e.g. `/tmp/kq-memory-regression-5k-8`) — showed
roughly linear memory growth:

```text
snapshot_count: 8
total_rows: 1517398
peak_process_rss_mb: 5410.73
peak_heap_allocated_mb: 5333.26
peak_jemalloc_resident_mb: 5616.52
peak_rss_bytes_per_row: 3739.0
peak_heap_bytes_per_row: 3685.5
```

The 8-snapshot run did not show nonlinear memory growth compared with the
4-snapshot profile. Treat the absolute peak, around 5.5 GiB RSS on this worker,
as the capacity-planning number for this profile.

## Query Timing And Load Analysis

These commands run against the `/tmp/kq-bench-5k-ipc` directory produced by the
5k-node profile above; substitute any snapshot directory of your own.

For quick repeatable query timings:

```bash
bazel run -c opt //kq/src/bin:synthetic_query_benchmark -- \
  --iterations 10 --warmup 2 /tmp/kq-bench-5k-ipc
```

`analyze_loading_phases` loads a snapshot once and reports total load duration,
per-resource object counts, and a memory breakdown (heap, Arrow tables, string
cache, fragmentation) — a single-run load profile, not a per-phase timing
split:

```bash
bazel run -c opt //kq/src/bin:analyze_loading_phases -- /tmp/kq-bench-5k-ipc
```

## Other Benchmark Binaries

The profiles above use IPC, the fastest repeated-load format. To benchmark
Parquet instead, pass `--format parquet` to `snapshot_convert` and
`--generated-format parquet` to `memory_regression_benchmark`.

Two more binaries in `kq/src/bin` target narrower regressions. Both take one or
more snapshot paths, but their CLIs differ:

```bash
# Times snapshot load plus query-engine setup. Accepts bare paths or --path.
# It writes a CPU profile; CPU_PROFILE sets the path (default: a .pb file in
# the working directory), so point it under /tmp to keep the repo clean.
CPU_PROFILE=/tmp/kq-engine-setup.pb \
  bazel run -c opt //kq/src/bin:engine_setup_benchmark -- /tmp/kq-bench-5k-ipc

# Isolates DataFusion table/UDF registration cost. Prints load and register
# timing to stdout; -o writes a metric,value summary CSV (load_time_s,
# register_time_s, memory_delta_mb, ...).
bazel run -c opt //kq/src/bin:registration_hotspot_benchmark -- \
  /tmp/kq-bench-5k-ipc -o /tmp/kq-registration.csv
```

See [DEVELOPMENT](DEVELOPMENT.md#build-targets) for the full helper-binary list.
