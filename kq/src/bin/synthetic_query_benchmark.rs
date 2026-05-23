use anyhow::{Context, Result};
use clap::Parser;
use kq::engine_setup::{load_snapshots_and_prepare_engine, EngineSetupConfig};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
#[command(
    name = "kq-synthetic-query-benchmark",
    about = "Run representative kq analysis queries against synthetic snapshots"
)]
struct Args {
    /// Snapshot directory or file. Pass multiple paths for multi-cluster analysis.
    #[arg(value_name = "SNAPSHOT")]
    snapshots: Vec<PathBuf>,

    /// Timed iterations per query after warmup
    #[arg(long, default_value_t = 10)]
    iterations: usize,

    /// Warmup iterations per query
    #[arg(long, default_value_t = 2)]
    warmup: usize,
}

struct BenchQuery {
    name: &'static str,
    sql: &'static str,
}

const QUERIES: &[BenchQuery] = &[
    BenchQuery {
        name: "cluster_size",
        sql: "SELECT (SELECT COUNT(*) FROM nodes) AS nodes, (SELECT COUNT(*) FROM pods) AS pods, (SELECT COUNT(*) FROM namespaces) AS namespaces",
    },
    BenchQuery {
        name: "pod_phase_distribution",
        sql: "SELECT p.phase AS phase, COUNT(*) AS pod_count FROM pods p GROUP BY p.phase ORDER BY pod_count DESC",
    },
    BenchQuery {
        name: "namespace_distribution",
        sql: "SELECT p.namespace AS namespace, COUNT(*) AS pod_count FROM pods p GROUP BY p.namespace ORDER BY pod_count DESC LIMIT 20",
    },
    BenchQuery {
        name: "node_pool_distribution",
        sql: "SELECT p.pool AS pool, COUNT(*) AS running_pods FROM pods p WHERE p.phase = 'Running' GROUP BY p.pool ORDER BY running_pods DESC",
    },
    BenchQuery {
        name: "container_cpu_by_app",
        sql: "SELECT p.app AS app, SUM(p.cpu_request_total) AS cpu_millis FROM pods p WHERE p.cpu_request_total IS NOT NULL GROUP BY p.app ORDER BY cpu_millis DESC LIMIT 20",
    },
    BenchQuery {
        name: "multi_cluster_rollup",
        sql: "SELECT p.cluster AS cluster, COUNT(*) AS pods FROM pods p GROUP BY p.cluster ORDER BY p.cluster",
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.snapshots.is_empty() {
        anyhow::bail!("at least one snapshot path is required");
    }
    if args.iterations == 0 {
        anyhow::bail!("--iterations must be greater than zero");
    }

    let config = EngineSetupConfig::default();
    let load_start = Instant::now();
    let mut engine = load_snapshots_and_prepare_engine(&args.snapshots, &config)
        .await
        .context("failed to load snapshots")?;
    let load_duration = load_start.elapsed();

    println!("Loaded {} snapshot path(s) in {:.3}s", args.snapshots.len(), load_duration.as_secs_f64());
    println!("query,rows,min_ms,p50_ms,p95_ms,max_ms,avg_ms");

    for query in QUERIES {
        for _ in 0..args.warmup {
            engine
                .execute(query.sql)
                .await
                .with_context(|| format!("warmup query failed: {}", query.name))?;
        }

        let mut timings = Vec::with_capacity(args.iterations);
        let mut rows = 0usize;
        for _ in 0..args.iterations {
            let started = Instant::now();
            let batch = engine
                .execute(query.sql)
                .await
                .with_context(|| format!("query failed: {}", query.name))?;
            timings.push(started.elapsed());
            rows = batch.num_rows();
        }

        print_stats(query.name, rows, &mut timings);
    }

    Ok(())
}

fn print_stats(name: &str, rows: usize, timings: &mut [Duration]) {
    timings.sort_unstable();
    let min = timings[0];
    let max = timings[timings.len() - 1];
    let p50 = percentile(timings, 50);
    let p95 = percentile(timings, 95);
    let total_nanos: u128 = timings.iter().map(Duration::as_nanos).sum();
    let avg = Duration::from_nanos((total_nanos / timings.len() as u128) as u64);

    println!(
        "{},{},{:.3},{:.3},{:.3},{:.3},{:.3}",
        name,
        rows,
        millis(min),
        millis(p50),
        millis(p95),
        millis(max),
        millis(avg)
    );
}

fn percentile(timings: &[Duration], percentile: usize) -> Duration {
    let idx = ((timings.len() - 1) * percentile) / 100;
    timings[idx]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
