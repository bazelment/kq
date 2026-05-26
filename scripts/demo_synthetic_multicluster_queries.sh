#!/usr/bin/env bash
#
# Generate N synthetic Kubernetes cluster snapshots and run a battery of
# typical investigation queries against all of them at once. Works as both
# a smoke test for new builds and a worked tour of the SQL surface.
#
# Usage:
#   scripts/demo_synthetic_multicluster_queries.sh [options]
#
# Options:
#   -d, --data-dir DIR   Root directory for generated snapshots
#                        (default: /tmp/kq-demo-multicluster)
#   -n, --clusters N     Number of clusters to generate (default: 4)
#       --nodes N        Nodes per cluster (default: 500)
#       --namespaces N   Namespaces per cluster (default: 60)
#       --query-only     Skip generation; just run queries against an
#                        existing data directory
#   -h, --help           Show this help and exit
#
# Each cluster is named bench-<i> with seed <i>, so re-running with the
# same options is deterministic.

set -euo pipefail

DATA_DIR="/tmp/kq-demo-multicluster"
CLUSTERS=4
NODES=500
NAMESPACES=60
QUERY_ONLY=0

usage() {
  sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--data-dir)   DATA_DIR="$2"; shift 2 ;;
    -n|--clusters)   CLUSTERS="$2"; shift 2 ;;
    --nodes)         NODES="$2"; shift 2 ;;
    --namespaces)    NAMESPACES="$2"; shift 2 ;;
    --query-only)    QUERY_ONLY=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! [[ "$CLUSTERS" =~ ^[0-9]+$ ]] || (( CLUSTERS < 1 )); then
  echo "--clusters must be a positive integer (got: $CLUSTERS)" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

cluster_name() { printf 'bench-%s' "$1"; }
cluster_path() { printf '%s/%s' "$DATA_DIR" "$(cluster_name "$1")"; }

# Bazel writes progress/status to stderr; suppress both streams when we
# only care about the produced binary.
quiet_bazel() { bazel "$@" >/dev/null 2>&1; }

SCRIPT_START_NS=$(date +%s%N)

# Build every binary we'll need upfront, in a single Bazel invocation, so
# the remaining steps never pay Bazel analysis overhead. --query-only skips
# the generator since it isn't going to run it.
BUILD_TARGETS=( //kq:kq )
if (( ! QUERY_ONLY )); then
  BUILD_TARGETS+=( //kq/tools:synthetic_snapshot )
fi
echo "==> Building ${#BUILD_TARGETS[@]} target(s): ${BUILD_TARGETS[*]}"
quiet_bazel build -c opt "${BUILD_TARGETS[@]}"
KQ="$REPO_ROOT/bazel-bin/kq/kq"
SYNTH_BIN="$REPO_ROOT/bazel-bin/kq/tools/synthetic_snapshot"

if (( QUERY_ONLY )); then
  echo "==> Query-only mode: reusing snapshots under $DATA_DIR"
  for i in $(seq 1 "$CLUSTERS"); do
    if [[ ! -f "$(cluster_path "$i")/metadata.json" ]]; then
      echo "missing snapshot: $(cluster_path "$i")/metadata.json" >&2
      echo "drop --query-only to regenerate" >&2
      exit 1
    fi
  done
else
  echo "==> Generating $CLUSTERS synthetic clusters under $DATA_DIR"
  echo "    nodes=$NODES namespaces=$NAMESPACES per cluster"
  mkdir -p "$DATA_DIR"
  for i in $(seq 1 "$CLUSTERS"); do
    name="$(cluster_name "$i")"
    out="$(cluster_path "$i")"
    echo "  - $name (seed=$i) -> $out"
    "$SYNTH_BIN" \
      --output "$out" \
      --cluster "$name" \
      --nodes "$NODES" \
      --namespaces "$NAMESPACES" \
      --seed "$i" \
      --overwrite >/dev/null
  done
fi

SNAPS=()
for i in $(seq 1 "$CLUSTERS"); do
  SNAPS+=("$(cluster_path "$i")")
done

QUERY_COUNT=0
QUERY_NS_TOTAL=0

run_query() {
  local title="$1"
  local sql="$2"
  local start end
  echo
  echo "------------------------------------------------------------"
  echo "Q$((++QUERY_COUNT)): $title"
  echo "------------------------------------------------------------"
  start=$(date +%s%N)
  "$KQ" -q "$sql" "${SNAPS[@]}"
  end=$(date +%s%N)
  QUERY_NS_TOTAL=$((QUERY_NS_TOTAL + end - start))
}

run_query "Pods per cluster, broken down by phase" "
SELECT cluster, phase, COUNT(*) AS pods
FROM pods
GROUP BY cluster, phase
ORDER BY cluster, pods DESC
"

run_query "Node capacity by cluster and pool" "
SELECT
  cluster,
  pool,
  COUNT(*)                                                     AS nodes,
  SUM(parse_cpu(status.capacity['cpu']))                       AS cpu_cores,
  ROUND(SUM(parse_memory(status.capacity['memory'])) / 1e9, 1) AS mem_gb
FROM nodes
GROUP BY cluster, pool
ORDER BY cluster, nodes DESC
"

run_query "Top 10 namespaces by Running pods across the fleet" "
SELECT namespace, COUNT(*) AS pods, COUNT(DISTINCT cluster) AS clusters
FROM pods
WHERE phase = 'Running'
GROUP BY namespace
ORDER BY pods DESC
LIMIT 10
"

run_query "Failure hot spots (top 10 cluster+namespace by failed pods)" "
SELECT cluster, namespace, COUNT(*) AS failed_pods
FROM pods
WHERE phase = 'Failed'
GROUP BY cluster, namespace
ORDER BY failed_pods DESC
LIMIT 10
"

run_query "GPU node density (pods per gpu-pool node, per cluster)" "
SELECT
  n.cluster,
  COUNT(DISTINCT n.metadata.name)                  AS gpu_nodes,
  COUNT(p.metadata.name)                           AS pods_on_gpu,
  ROUND(COUNT(p.metadata.name) * 1.0 /
        COUNT(DISTINCT n.metadata.name), 1)        AS pods_per_gpu_node
FROM nodes n
LEFT JOIN pods p
  ON p.cluster = n.cluster
 AND p.spec['nodeName'] = n.metadata.name
WHERE n.pool = 'gpu'
GROUP BY n.cluster
ORDER BY n.cluster
"

run_query "Request-vs-capacity pressure by cluster and pool" "
WITH
  node_cap AS (
    SELECT cluster, pool,
           SUM(parse_cpu(status.capacity['cpu']))       AS cpu_capacity,
           SUM(parse_memory(status.capacity['memory'])) AS mem_capacity
    FROM nodes
    GROUP BY cluster, pool
  ),
  pod_req AS (
    SELECT p.cluster, n.pool,
           SUM(total_cpu_request(p.spec['containers']))    AS cpu_requested,
           SUM(total_memory_request(p.spec['containers'])) AS mem_requested
    FROM pods p
    JOIN nodes n
      ON n.cluster = p.cluster
     AND n.metadata.name = p.spec['nodeName']
    WHERE p.phase IN ('Running', 'Pending')
    GROUP BY p.cluster, n.pool
  )
SELECT
  c.cluster, c.pool,
  ROUND(r.cpu_requested * 100.0 / c.cpu_capacity, 1) AS cpu_pct,
  ROUND(r.mem_requested * 100.0 / c.mem_capacity, 1) AS mem_pct
FROM node_cap c
JOIN pod_req r
  ON r.cluster = c.cluster AND r.pool = c.pool
ORDER BY c.cluster, cpu_pct DESC
"

# Aggregate across clusters: one row per DaemonSet, totals over the fleet.
# Without the GROUP BY this produces N rows per DS (one per cluster) with no
# cluster column to tell them apart.
run_query "DaemonSet rollout across the fleet (aggregated)" "
SELECT
  metadata.name                                                  AS daemonset,
  COUNT(*)                                                       AS clusters,
  SUM(status['desiredNumberScheduled'])                          AS desired,
  SUM(status['numberReady'])                                     AS ready,
  SUM(status['desiredNumberScheduled'] - status['numberReady'])  AS not_ready
FROM daemon_sets
GROUP BY metadata.name
ORDER BY daemonset
"

SCRIPT_END_NS=$(date +%s%N)
total_ms=$(( (SCRIPT_END_NS - SCRIPT_START_NS) / 1000000 ))
query_ms=$(( QUERY_NS_TOTAL / 1000000 ))

echo
echo "============================================================"
echo "Summary"
echo "============================================================"
echo "  clusters:      $CLUSTERS"
echo "  snapshots dir: $DATA_DIR"
echo "  queries run:   $QUERY_COUNT"
echo "  query time:    ${query_ms} ms total (kq runs incl. load+register)"
echo "  wall time:     ${total_ms} ms total"
echo
echo "Re-run with --query-only to skip generation next time."
