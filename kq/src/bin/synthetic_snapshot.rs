use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use kq::synthetic::{generate_ndjson_snapshot, SyntheticSnapshotConfig};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "kq-synthetic-snapshot",
    about = "Generate production-shaped synthetic Kubernetes snapshots for kq validation"
)]
struct Args {
    /// Output directory for metadata.json and *.ndjson.gz files
    #[arg(short, long, value_name = "DIR")]
    output: PathBuf,

    /// Synthetic cluster name embedded in labels and annotations
    #[arg(long, default_value = "synthetic-a")]
    cluster: String,

    /// Number of nodes to generate
    #[arg(long, default_value_t = 5_000)]
    nodes: usize,

    /// Minimum pods placed on each node
    #[arg(long, default_value_t = 10)]
    min_pods_per_node: usize,

    /// Maximum pods placed on each node
    #[arg(long, default_value_t = 60)]
    max_pods_per_node: usize,

    /// Number of namespaces to generate
    #[arg(long, default_value_t = 240)]
    namespaces: usize,

    /// Deterministic RNG seed
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Replace an existing output directory
    #[arg(long)]
    overwrite: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = SyntheticSnapshotConfig {
        output_dir: args.output,
        cluster_name: args.cluster,
        node_count: args.nodes,
        min_pods_per_node: args.min_pods_per_node,
        max_pods_per_node: args.max_pods_per_node,
        namespace_count: args.namespaces,
        seed: args.seed,
        overwrite: args.overwrite,
        timestamp: Utc::now(),
    };

    let summary = generate_ndjson_snapshot(&config)?;

    println!("Synthetic snapshot written to {}", summary.output_dir.display());
    println!("  cluster: {}", summary.cluster_name);
    println!("  nodes: {}", summary.node_count);
    println!("  pods: {}", summary.pod_count);
    println!("  namespaces: {}", summary.namespace_count);
    println!("  daemonsets: {}", summary.daemonset_count);
    println!(
        "  pods per node: {}-{}",
        summary.min_pods_per_node, summary.max_pods_per_node
    );
    println!(
        "  phases: Running={}, Pending={}, Succeeded={}, Failed={}, Unknown={}",
        summary.running_pods,
        summary.pending_pods,
        summary.succeeded_pods,
        summary.failed_pods,
        summary.unknown_pods
    );
    println!("  generation time: {:.2}s", summary.generation_seconds);

    Ok(())
}
