# kq Usage Guide

This guide covers the normal kq workflow: capturing a snapshot of your cluster,
running SQL against it, and — when you need faster repeated loads — converting
it to a columnar format.

## Build

Building kq needs [Bazel](https://bazel.build) 7.x; the capture step below
also needs `kubectl` (with cluster credentials) and `jq`. Run every command in
this guide from the repo root, on Linux or macOS (use WSL on Windows).

```bash
bazel build -c opt //kq/src:kq
```

The optimized binary is written to `bazel-bin/kq/src/kq`.

## Capturing a Snapshot

kq queries a snapshot file, not a live cluster. A snapshot is a single JSON
object with a top-level `timestamp` and one array per resource type:

```json
{
  "timestamp": "2026-05-22T12:00:00Z",
  "pods":       [ /* Kubernetes Pod objects */ ],
  "nodes":      [ /* Kubernetes Node objects */ ],
  "namespaces": [ /* Kubernetes Namespace objects */ ],
  "daemonSets": [ /* Kubernetes DaemonSet objects */ ]
}
```

The `timestamp` is required — the loader rejects a snapshot without it. The
recipe below fills it with `jq`'s `now`, i.e. the time you ran the capture on
your machine; substitute a different expression if you need the cluster's own
clock. Capture one straight from a cluster you have `kubectl` access to. The
command needs cluster-wide list/get permission on pods, nodes, namespaces, and
daemonsets; if it fails with a `Forbidden` error, that is an RBAC issue, not a
kq one.

```bash
kubectl get pods,nodes,namespaces,daemonsets --all-namespaces -o json \
  | jq '{
      timestamp: (now | todateiso8601),
      pods:       [.items[] | select(.kind=="Pod")       | .cluster = "prod-us"],
      nodes:      [.items[] | select(.kind=="Node")       | .cluster = "prod-us"],
      namespaces: [.items[] | select(.kind=="Namespace")],
      daemonSets: [.items[] | select(.kind=="DaemonSet")]
    }' \
  > cluster.json
```

Setting `cluster` on the Pod and Node objects is optional: the `pods` and
`nodes` views expose it as a column, which is what makes [Comparing Multiple
Snapshots](#comparing-multiple-snapshots) possible. Other resource types have
no `cluster` column, so tagging them has no effect.

Plain `.json` and gzipped `.json.gz` snapshots both work. Once you have a
snapshot file, every example below treats it the same way — pass its path as
the last argument.

## Running Queries

The examples below use the `cluster.json` from the capture step. If you don't
have a cluster, generate a snapshot first — see [Trying kq Without a
Cluster](#trying-kq-without-a-cluster) — and use its path in place of
`cluster.json`.

### One Query

```bash
bazel-bin/kq/src/kq --query "
SELECT namespace, COUNT(*) AS pods
FROM pods
GROUP BY namespace
ORDER BY pods DESC
LIMIT 20
" cluster.json
```

### Interactive Mode

```bash
bazel-bin/kq/src/kq cluster.json
```

The startup banner lists the loaded views and their row counts. From the
prompt, useful dot commands:

```text
.help            list all commands
.tables          list queryable views
.columns pods    show columns for a view
.format json     switch output format (table, json, csv, compact)
.memory          report current memory usage
.history         show command history
.clear           clear the screen
.quit            exit
```

`DESCRIBE <view>` works too, and so do ad-hoc `information_schema` queries —
for example, `SELECT table_name FROM information_schema.tables WHERE
table_schema = 'public'` to list the user-facing views.

### Batch Mode

Batch mode reads SQL from stdin and prints newline-delimited JSON (NDJSON) —
handy for scripting and pipelines:

```bash
printf 'SELECT COUNT(*) AS pods FROM pods;\n.quit\n' \
  | bazel-bin/kq/src/kq --batch cluster.json
```

The output protocol is fixed, so a parser can rely on it:

- The first line is always `{"ready":true}` — a handshake printed before any
  query runs. Skip it.
- Each statement (terminated by `;`) then produces one compact JSON object on
  its own line; a failed statement produces `{"error": "..."}` instead.
- Batch output is always compact JSON regardless of `--format`; that flag only
  affects interactive and `--query` output.

## Writing SQL

Queries run against four views: `pods`, `nodes`, `namespaces`, and
`daemon_sets`. Note the view is `daemon_sets` (with an underscore) even though
the on-disk file is `daemonsets.ndjson.gz` — `FROM daemonsets` will not resolve.

Top-level analytic columns are flattened for fast filters and groupings:

```sql
SELECT cluster, pool, phase, COUNT(*) AS pods
FROM pods
GROUP BY cluster, pool, phase;
```

Nested Kubernetes structs are reachable in the same query. Use dot notation for
lowercase field names and **bracket notation for camelCase names** — DataFusion
lowercases bare identifiers, so `spec['nodeName']` and
`metadata['creationTimestamp']` are the safe forms:

```sql
SELECT metadata.name, metadata['creationTimestamp'], spec['nodeName'], status.phase
FROM pods
WHERE status.phase = 'Running'
LIMIT 10;
```

Map keys — labels and annotations — are also accessed with brackets:

```sql
SELECT metadata.labels['app'], metadata.annotations['prometheus.io/scrape']
FROM pods
WHERE metadata.labels['app'] IS NOT NULL;
```

## Built-In Functions

kq registers Kubernetes-aware SQL functions:

| Function | Purpose |
| --- | --- |
| `parse_cpu(value)` | Convert a Kubernetes CPU quantity to millicores |
| `parse_memory(value)` | Convert a Kubernetes memory quantity to bytes |
| `container_count(spec.containers)` | Count containers in a pod |
| `container_names(spec.containers)` | Return container names |
| `total_cpu_request(spec.containers)` | Sum container CPU requests |
| `total_memory_request(spec.containers)` | Sum container memory requests |
| `has_sidecar(spec.containers)` | True when a pod has multiple containers |
| `regexp_extract(value, pattern, group)` | Extract a regex capture group |
| `json_extract_str(value, key)` | Extract a string field from JSON text |
| `extract_pool(value)` | Extract `node.kq.dev/pool=<value>` from selector text |

These helpers are column-oriented: each one expects its arguments to be table
columns, not scalar literals, and rejects a literal with an execution error
(the exact wording varies — `parse_cpu` says "Argument must be an array", the
container helpers say "Expected array", and so on). Apply them to a column
(`parse_cpu(some_column)`), not a constant.

`regexp_extract` and `json_extract_str` are a special case. Their `pattern`,
`key`, and `group` arguments must also be columns — but the function reads only
the *first row* of those columns and applies that value to every row. So those
inputs must be **constant across the query**: a column whose value is the same
on every row works; one that varies by row does not, and fails silently by
using row 0's value everywhere.

For example, requested CPU per namespace:

```sql
SELECT metadata.namespace,
       SUM(total_cpu_request(spec.containers)) AS requested_millicores
FROM pods
GROUP BY metadata.namespace
ORDER BY requested_millicores DESC;
```

## Comparing Multiple Snapshots

Pass several snapshot paths and kq queries them as one dataset. kq has no
built-in snapshot identifier, so to tell rows apart you give each capture a
`cluster` value yourself (see [Capturing a Snapshot](#capturing-a-snapshot)).
Pick a value that is **unique per capture** — if you compare two snapshots of
the *same* physical cluster, use distinct labels like `prod-us@monday` and
`prod-us@friday`, or their rows merge indistinguishably. Then group the `pods`
and `nodes` views by `cluster`:

```bash
bazel-bin/kq/src/kq --query "
SELECT cluster, COUNT(*) AS pods
FROM pods
GROUP BY cluster
ORDER BY cluster
" prod-us.json prod-eu.json
```

## Faster Repeated Loads

A single `cluster.json` from the capture step above is loaded directly — no
conversion needed; kq parses it on each run. Conversion is for **NDJSON
directory snapshots**: if you have one and query it often, converting it once
to a columnar format makes every later load faster. kq auto-detects the format,
so your queries don't change.

An NDJSON directory snapshot stores each resource in its own gzipped NDJSON
file alongside a `metadata.json`:

```text
kq-demo/
  metadata.json
  pods.ndjson.gz
  nodes.ndjson.gz
  namespaces.ndjson.gz
  daemonsets.ndjson.gz
```

The synthetic generator ([below](#trying-kq-without-a-cluster)) writes this
layout directly — use its output directory as the `--input` here. Convert it
with `snapshot_convert`:

```bash
# Arrow IPC — fastest local reload.
bazel run -c opt //kq/src/bin:snapshot_convert -- \
  --input /tmp/kq-demo --output /tmp/kq-demo-ipc --format ipc --overwrite

# Parquet — compact, good for durable storage and interchange.
bazel run -c opt //kq/src/bin:snapshot_convert -- \
  --input /tmp/kq-demo --output /tmp/kq-demo-parquet --format parquet --overwrite
```

Converted IPC and Parquet snapshots use the same directory shape, and the
`daemonsets.*` file maps to the SQL view `daemon_sets` (see
[Writing SQL](#writing-sql)).

To sanity-check a converted snapshot against its source — it compares table
names, row counts, schema fields, and key column distributions, not a full
row-by-row equality:

```bash
bazel run -c opt //kq/src/bin:snapshot_correctness -- \
  --expected /tmp/kq-demo --actual /tmp/kq-demo-ipc
```

Then query the converted directory exactly as you would any snapshot:

```bash
bazel-bin/kq/src/kq --query "SELECT COUNT(*) AS pods FROM pods" /tmp/kq-demo-ipc
```

## Trying kq Without a Cluster

To learn the SQL, demo kq, or benchmark loader and query performance without a
real cluster, generate a synthetic snapshot. It produces a directory snapshot
with deterministic, realistic placement, resource requests, labels, node pools,
namespaces, daemonsets, and pod phases:

```bash
bazel run -c opt //kq/src/bin:synthetic_snapshot -- \
  --output /tmp/kq-demo \
  --cluster demo \
  --nodes 1000 \
  --min-pods-per-node 10 \
  --max-pods-per-node 60 \
  --namespaces 80 \
  --seed 42 \
  --overwrite

bazel-bin/kq/src/kq /tmp/kq-demo
```

The same flag set always produces the same cluster topology — placement,
sizing, labels, pools, and phases are fully deterministic. `--seed` fixes the
random draws, but `--cluster` and the size flags (`--nodes`,
`--min-pods-per-node`, `--max-pods-per-node`, `--namespaces`) shape the output
too, so reproducing a snapshot means reusing every flag, not just `--seed`. The
one exception is timestamps: the generator stamps the snapshot with the current
wall-clock time, so `creationTimestamp` and similar fields shift between runs.
With a fixed flag set, synthetic snapshots are safe for tests and reproducible
benchmarks. For representative production-sized profiles, see
[BENCHMARKS](BENCHMARKS.md).
