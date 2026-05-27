# Getting Started as a Contributor

A short on-ramp for first-time contributors: what kq is, how it's put
together, and a curated list of small-to-medium projects you can pick up.

## What kq Is, in One Paragraph

kq loads a point-in-time dump of a Kubernetes cluster — pods, nodes,
namespaces, daemonsets — into Apache Arrow tables and lets you query it with
SQL. There is no database to stand up and no live API server load. Capture a
snapshot once, then run as many queries as you want, offline. Label snapshots
at capture time and you can load many at once to compare clusters side by
side.

The project intentionally stays small and focused on snapshot analysis — see
[CONTRIBUTING](../CONTRIBUTING.md#ground-rules) for the ground rules.

## Get the Code Building

```bash
bazel build -c opt //kq:kq
bazel test //kq/...
```

Builds always go through Bazel (Bzlmod). Do not use `cargo build` /
`cargo test` — Cargo is the source of truth for crate metadata only.

If you do not have a real cluster, generate a synthetic snapshot and query
it:

```bash
bazel run -c opt //kq/tools:synthetic_snapshot -- \
  --output /tmp/kq-demo --cluster demo --nodes 100 --namespaces 20 --overwrite
bazel-bin/kq/kq /tmp/kq-demo
```

Full setup and SQL examples live in the [Usage guide](USAGE.md).

## The 60-Second Mental Model

Every CLI invocation walks this path:

```
flags → engine_setup → loader → Arrow RecordBatches → query → output
```

1. `kq/main.rs` parses snapshot paths and flags.
2. `engine_setup/` builds a loader and reads every input.
3. `loader/` auto-detects format per path: single `.json` / `.json.gz`,
   NDJSON directory, Arrow IPC directory, or Parquet directory.
4. Each resource type becomes an Arrow `RecordBatch`.
5. `query/` registers batches as DataFusion `MemTable`s, exposes the
   `pods`, `nodes`, `namespaces`, `daemon_sets` views, and registers
   Kubernetes-aware UDFs (`parse_cpu`, `parse_memory`, `extract_pool`, …).
6. `output/` renders results as table / JSON / CSV / TSV / compact.

The full layout and architectural invariants live in the
[Developer guide](DEVELOPMENT.md) and [CLAUDE.md](../CLAUDE.md#architecture).

## Where to Find Things

| You want to…                             | Look in            |
| ---------------------------------------- | ------------------ |
| Add a new resource type                  | `kq/loader/`, `kq/schema/`, `kq/query/` |
| Add a SQL function                       | `kq/query/`        |
| Add a new output format                  | `kq/output/`       |
| Add a developer helper binary            | `kq/tools/`        |
| Tweak the synthetic generator            | `kq/synthetic/`    |
| Add an integration test                  | `kq/tests/`        |

## Good First Projects

These are sized so a new contributor can land them in one or two PRs. None of
them require touching DataFusion or Arrow internals deeply.

### 1. Add a Kubernetes-aware SQL function (smallest)

kq already exposes UDFs like `parse_cpu`, `parse_memory`, and `extract_pool`.
There is plenty of room for more:

- `parse_duration(string)` for pod age strings.
- `container_image_registry(image)` to pull the registry host out of an image
  reference.
- `is_system_namespace(name)` for the usual `kube-*` set.
- `node_ready(conditions)` reading the Ready condition out of a node's
  status.

Why it's a good first PR: the change is local to `kq/query/`, easy to test
in isolation, and immediately useful to anyone writing queries.

### 2. Add a new output format

Existing formats live in `kq/output/`: table, JSON, CSV, TSV, compact.
Candidates:

- **Markdown table** — easy to paste into PRs and runbooks.
- **JSON Lines (`jsonl`)** — one row per line, easy to pipe into downstream
  tools.

Why it's a good first PR: a self-contained module, mirrors patterns already
in the directory, and the test surface is just "render this batch and check
the bytes."

### 3. Add a new resource type (the "tour" project)

Today the loader handles pods, nodes, namespaces, and daemonsets. Adding a
new type — `deployments`, `services`, `persistentvolumeclaims`, or `events`
— walks you through every layer of the codebase:

- New Arrow schema in `kq/schema/`.
- New loader code path and resource-table entry in `kq/loader/`.
- New view registration plus any flattened analytic columns in `kq/query/`.
- Extend `kq/synthetic/` so the generator can produce the type.
- Add an integration fixture under `kq/tests/`.

Why it's a good second PR: by the end you understand the entire data flow.

### 4. SQL cookbook for common investigations

[`scripts/demo_synthetic_multicluster_queries.sh`](../scripts/demo_synthetic_multicluster_queries.sh)
shows the pattern: generate N synthetic clusters, then run seven typical
fleet queries. There is room for more recipe collections:

- Capacity review: requested vs. allocatable per pool, headroom per node.
- Noisy neighbor: top pods by CPU/memory request per node.
- Hygiene: pods without resource requests, pinned to single nodes, or stuck
  in non-`Running` phases.

Why it's a good first PR: pure docs and SQL — no Rust changes — but the
output is real artifacts that help every user.

### 5. A dedicated snapshot-capture CLI (medium)

Today the README shows a `kubectl ... | jq '{...}'` recipe to produce a
snapshot. It works, but the recipe has sharp edges:

- The top-level `timestamp` is mandatory; forget it and the loader rejects
  the file.
- The `cluster` label has to be set on pods and nodes individually; setting
  it on namespaces or daemonsets is a silent no-op.
- A full-cluster `kubectl get -o json` against a large cluster can OOM.
- The recipe only produces single-file JSON, never the more efficient NDJSON
  or Parquet directory formats — even though kq has writers for both
  (`write_ipc_directory`, `write_parquet_directory`).

A new `kq/tools/snapshot_collect.rs` could:

- Talk to the cluster via the [`kube`](https://crates.io/crates/kube) crate
  instead of shelling out to `kubectl`, paginating list calls so memory
  stays bounded.
- Stamp `timestamp` and per-resource `cluster` labels correctly without the
  user having to remember.
- Pick output format (`--format json|ndjson|parquet`) and reuse the existing
  writers.
- Optionally accept multiple kube contexts (`--context a,b,c`) and label
  each snapshot's pods and nodes with the right cluster name in one run.

Why it's a good medium-sized PR: clear boundaries (read from kube API, hand
off to existing writers), no need to touch the query engine, and it removes
a real source of friction from the getting-started flow.

### 6. Snapshot diff (larger — pick this when you want to stretch)

Two snapshots, one delta: pods that appeared, disappeared, restarted, or
migrated nodes. Either a new `kq/tools/snapshot_diff.rs` binary or a set of
SQL helpers that join two snapshots by `metadata.uid`. This is the most
product-shaped project on the list — discuss the surface in an issue before
writing code.

## Picking Something to Work On

If you have not contributed before, start with **1** or **2** — small,
isolated, mergeable in a sitting. Then **3** is the best way to internalize
the architecture. **4** is great if you are stronger on Kubernetes than on
Rust. **5** is the highest-leverage project on the list once you are
comfortable with the codebase.

Open an issue describing what you plan to do before you start on **3** and
above — it is much easier to give early feedback on direction than on a
finished PR.

## Before You Open a PR

- Run `bazel build -c opt //kq:kq` and `bazel test //kq/...`.
- For loader, schema, query-registration, or output changes, also run the
  focused suite documented in
  [CLAUDE.md](../CLAUDE.md#when-changing-loader--schema--query-registration--output).
- Read the [Pull Request checklist](../CONTRIBUTING.md#pull-request-checklist).
- Confirm any new fixtures are synthetic and safe to publish in a public
  repo.

Welcome aboard.
