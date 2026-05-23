use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use kq::loader::{
    write_ipc_directory, write_parquet_directory, LoaderConfig, NdjsonLoader, SnapshotLoader,
};
use kq::memory::{
    read_memory_sample, MemoryBenchmarkSummary, MemorySampler, MemoryThresholds,
};
use kq::query::QueryEngine;
use kq::synthetic::{generate_ndjson_snapshot, SyntheticSnapshotConfig};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, Parser)]
#[command(
    name = "kq-memory-regression-benchmark",
    about = "Load multiple snapshots while sampling peak RSS and jemalloc memory"
)]
struct Args {
    /// Snapshot paths to load. If omitted, deterministic synthetic snapshots are generated.
    #[arg(value_name = "SNAPSHOT")]
    snapshots: Vec<PathBuf>,

    /// Number of synthetic snapshots to generate when paths are omitted.
    #[arg(long, default_value_t = 4)]
    snapshot_count: usize,

    /// Storage format used for generated snapshots.
    #[arg(long, value_enum, default_value_t = GeneratedFormat::Ipc)]
    generated_format: GeneratedFormat,

    /// Root directory for generated snapshots.
    #[arg(long, default_value = "/tmp/kq-memory-regression")]
    output_root: PathBuf,

    /// Nodes per generated snapshot.
    #[arg(long, default_value_t = 120)]
    nodes: usize,

    /// Minimum pods per generated node.
    #[arg(long, default_value_t = 10)]
    min_pods_per_node: usize,

    /// Maximum pods per generated node.
    #[arg(long, default_value_t = 20)]
    max_pods_per_node: usize,

    /// Namespaces per generated snapshot.
    #[arg(long, default_value_t = 25)]
    namespaces: usize,

    /// Base seed for deterministic synthetic snapshots.
    #[arg(long, default_value_t = 99)]
    seed: u64,

    /// Memory sample interval in milliseconds.
    #[arg(long, default_value_t = 1)]
    sample_interval_ms: u64,

    /// Optional fail threshold for peak process RSS.
    #[arg(long)]
    max_peak_rss_mb: Option<f64>,

    /// Optional fail threshold for peak jemalloc allocated memory.
    #[arg(long)]
    max_peak_heap_mb: Option<f64>,

    /// Optional fail threshold for peak jemalloc resident memory.
    #[arg(long)]
    max_peak_jemalloc_resident_mb: Option<f64>,

    /// Write summary metrics as JSON.
    #[arg(long)]
    json_output: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GeneratedFormat {
    Ndjson,
    Ipc,
    Parquet,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.sample_interval_ms == 0 {
        anyhow::bail!("--sample-interval-ms must be greater than zero");
    }

    let snapshots = if args.snapshots.is_empty() {
        generate_synthetic_inputs(&args)?
    } else {
        args.snapshots.clone()
    };

    println!("Memory regression benchmark");
    println!("  snapshots: {}", snapshots.len());
    for (idx, path) in snapshots.iter().enumerate() {
        println!("  [{}] {}", idx + 1, path.display());
    }

    let baseline = read_memory_sample().context("failed to read baseline memory")?;
    let sampler = MemorySampler::start(Duration::from_millis(args.sample_interval_ms))
        .context("failed to start memory sampler")?;
    let started = Instant::now();

    let loader = SnapshotLoader::with_config(LoaderConfig {
        progress_updates: false,
        ..Default::default()
    });
    let snapshot_data = loader
        .load_and_combine(&snapshots)
        .await
        .context("failed to load snapshots")?;
    let table_names = snapshot_data.list_tables();
    let total_rows: usize = table_names
        .iter()
        .map(|table| snapshot_data.table_row_count(table))
        .sum();

    let engine = QueryEngine::new(snapshot_data)
        .await
        .context("failed to register query engine")?;
    let duration = started.elapsed();
    let peak = sampler.stop()?;
    let final_sample = read_memory_sample().context("failed to read final memory")?;

    let summary = MemoryBenchmarkSummary::new(
        &snapshots,
        generated_format_label(args.generated_format),
        engine.table_count(),
        total_rows,
        duration,
        baseline,
        peak,
        final_sample,
    );
    summary.print();
    if let Some(path) = &args.json_output {
        summary.write_json(path)?;
    }
    MemoryThresholds {
        max_peak_rss_mb: args.max_peak_rss_mb,
        max_peak_heap_mb: args.max_peak_heap_mb,
        max_peak_jemalloc_resident_mb: args.max_peak_jemalloc_resident_mb,
    }
    .check(peak)?;

    Ok(())
}

fn generated_format_label(format: GeneratedFormat) -> &'static str {
    match format {
        GeneratedFormat::Ndjson => "ndjson",
        GeneratedFormat::Ipc => "ipc",
        GeneratedFormat::Parquet => "parquet",
    }
}

fn generate_synthetic_inputs(args: &Args) -> Result<Vec<PathBuf>> {
    if args.snapshot_count == 0 {
        anyhow::bail!("--snapshot-count must be greater than zero");
    }

    std::fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;

    let mut paths = Vec::with_capacity(args.snapshot_count);
    for idx in 0..args.snapshot_count {
        let cluster = format!("memory-regression-{}", idx + 1);
        let ndjson_dir = args.output_root.join(format!("{cluster}-ndjson"));
        let config = SyntheticSnapshotConfig {
            output_dir: ndjson_dir.clone(),
            cluster_name: cluster.clone(),
            node_count: args.nodes,
            min_pods_per_node: args.min_pods_per_node,
            max_pods_per_node: args.max_pods_per_node,
            namespace_count: args.namespaces,
            seed: args.seed + idx as u64,
            overwrite: true,
            timestamp: Utc::now(),
        };
        let summary = generate_ndjson_snapshot(&config)
            .with_context(|| format!("failed to generate synthetic snapshot {cluster}"))?;

        let path = match args.generated_format {
            GeneratedFormat::Ndjson => summary.output_dir,
            GeneratedFormat::Ipc => {
                let output_dir = args.output_root.join(format!("{cluster}-ipc"));
                let (timestamp, tables, _) = NdjsonLoader::new()
                    .load_directory(&summary.output_dir)
                    .with_context(|| format!("failed to load generated snapshot {cluster}"))?;
                if output_dir.exists() {
                    std::fs::remove_dir_all(&output_dir).with_context(|| {
                        format!("failed to remove {}", output_dir.display())
                    })?;
                }
                write_ipc_directory(&output_dir, timestamp, &tables)
                    .with_context(|| format!("failed to write IPC snapshot {cluster}"))?;
                output_dir
            }
            GeneratedFormat::Parquet => {
                let output_dir = args.output_root.join(format!("{cluster}-parquet"));
                let (timestamp, tables, _) = NdjsonLoader::new()
                    .load_directory(&summary.output_dir)
                    .with_context(|| format!("failed to load generated snapshot {cluster}"))?;
                if output_dir.exists() {
                    std::fs::remove_dir_all(&output_dir).with_context(|| {
                        format!("failed to remove {}", output_dir.display())
                    })?;
                }
                write_parquet_directory(&output_dir, timestamp, &tables)
                    .with_context(|| format!("failed to write Parquet snapshot {cluster}"))?;
                output_dir
            }
        };

        paths.push(path);
    }

    Ok(paths)
}
