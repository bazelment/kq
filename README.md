# kq

Fast SQL for Kubernetes cluster snapshots.

kq loads a point-in-time dump of your cluster's resources into Apache Arrow
tables and lets you query it with SQL. Use it for offline cluster analysis,
capacity reviews, debugging large clusters, and — by labelling each capture —
comparing snapshots side by side. No database to stand up, no live API server
load.

## Features

- Query `pods`, `nodes`, `namespaces`, and `daemon_sets` with SQL.
- Work offline from a snapshot file — no cluster connection while you query.
- Load many snapshots at once; the `pods` and `nodes` views carry a `cluster`
  column you set at capture time, so you can group and compare across them.
- Reach nested Kubernetes fields directly: `metadata.name`, `spec['nodeName']`,
  `status.phase`.
- Kubernetes-aware SQL functions for CPU, memory, containers, and node pools.
- Interactive shell, one-shot queries, and machine-readable batch output.

## Quick Start

You need [Bazel](https://bazel.build) 7.x to build kq. To capture a snapshot
from a live cluster you also need `kubectl` with cluster credentials and `jq`;
if you don't have a cluster, skip to [No cluster handy?](#no-cluster-handy)
below. Commands below assume Linux or macOS (use WSL on Windows) and are run
from the repo root.

```bash
# Build the CLI (output: bazel-bin/kq/kq).
bazel build -c opt //kq:kq

# Capture a snapshot of your cluster. The top-level `timestamp` is required.
# Setting `cluster` on pods and nodes (the two views with a `cluster` column)
# lets you tell snapshots apart when you load several at once.
kubectl get pods,nodes,namespaces,daemonsets --all-namespaces -o json \
  | jq '{
      timestamp: (now | todateiso8601),
      pods:       [.items[] | select(.kind=="Pod")       | .cluster = "prod-us"],
      nodes:      [.items[] | select(.kind=="Node")       | .cluster = "prod-us"],
      namespaces: [.items[] | select(.kind=="Namespace")],
      daemonSets: [.items[] | select(.kind=="DaemonSet")]
    }' \
  > cluster.json

# Query it.
bazel-bin/kq/kq --query "
SELECT phase, COUNT(*) AS pods FROM pods GROUP BY phase ORDER BY pods DESC
" cluster.json

# Or explore interactively.
bazel-bin/kq/kq cluster.json
```

Inside the interactive shell — the startup banner lists the loaded views:

```sql
DESCRIBE pods;
SELECT metadata.name, metadata.namespace, spec['nodeName']
FROM pods
LIMIT 10;
```

For the full SQL reference, snapshot formats, and conversion tools, see the
[Usage guide](docs/USAGE.md).

## No cluster handy?

To try kq, learn the SQL, or benchmark without a real cluster, generate a
synthetic snapshot:

```bash
bazel run -c opt //kq/tools:synthetic_snapshot -- \
  --output /tmp/kq-demo --cluster demo --nodes 100 --namespaces 20 --overwrite

bazel-bin/kq/kq /tmp/kq-demo
```

The generator produces deterministic, realistic clusters of any size — see
[Trying kq without a cluster](docs/USAGE.md#trying-kq-without-a-cluster).

## Documentation

- [Usage guide](docs/USAGE.md) — capturing snapshots, CLI modes, SQL syntax,
  conversion tools.
- [Developer guide](docs/DEVELOPMENT.md) — code layout, build/test workflow,
  dependency management.
- [Benchmarks](docs/BENCHMARKS.md) — repeatable loader, query, and memory
  profiles.
- [Contributing](CONTRIBUTING.md) — ground rules, public-data policy, PR
  checklist.

## License

Apache-2.0. See [LICENSE](LICENSE).
