/// Analyze time spent in different loading phases
/// Usage: bazel run -c opt //kq/tools:analyze_loading_phases -- /path/to/snapshot ...

use anyhow::Result;
use kq::loader::SnapshotLoader;
use std::path::PathBuf;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with custom filter to reduce noise
    tracing_subscriber::fmt()
        .with_env_filter("info,kq::loader=debug")
        .init();

    // Collect file paths from command line
    let args: Vec<String> = std::env::args().skip(1).collect();
    
    let files: Vec<PathBuf> = args.into_iter().map(PathBuf::from).collect();

    if files.is_empty() {
        eprintln!("Error: provide at least one snapshot path");
        std::process::exit(1);
    }

    println!("\n{}", "=".repeat(80));
    println!("Loading Performance Analysis");
    println!("{}\n", "=".repeat(80));
    
    println!("Analyzing {} files:", files.len());
    for (i, file) in files.iter().enumerate() {
        let metadata = std::fs::metadata(file)?;
        let file_name = file
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| file.display().to_string());
        println!("  {}. {} ({} MB)", 
            i + 1, 
            file_name,
            metadata.len() / 1_000_000);
    }
    println!();

    // Test with default configuration
    println!("Configuration: Default (sequential with optimizations)");
    println!("{}\n", "-".repeat(80));
    
    let start_total = Instant::now();
    let loader = SnapshotLoader::new();
    
    println!("Loading files...");
    let result = loader.load_and_combine(&files).await?;
    
    let total_duration = start_total.elapsed();
    
    println!("\n{}", "=".repeat(80));
    println!("Results");
    println!("{}\n", "=".repeat(80));
    
    // Count objects
    let node_count = table_rows(&result, "nodes");
    let pod_count = table_rows(&result, "pods");
    let ns_count = table_rows(&result, "namespaces");
    let ds_count = table_rows(&result, "daemon_sets");
    let total_objects = node_count + pod_count + ns_count + ds_count;
    
    println!("Total Duration: {:.2}s", total_duration.as_secs_f64());
    println!();
    
    println!("Objects Loaded:");
    println!("  Nodes:       {:>8}", node_count);
    println!("  Pods:        {:>8}", pod_count);
    println!("  Namespaces:  {:>8}", ns_count);
    println!("  DaemonSets:  {:>8}", ds_count);
    println!("  {}", "-".repeat(20));
    println!("  Total:       {:>8}", total_objects);
    println!();
    
    println!("Performance:");
    println!("  Throughput: {:.0} objects/sec", total_objects as f64 / total_duration.as_secs_f64());
    println!("  Per file:   {:.2}s avg", total_duration.as_secs_f64() / files.len() as f64);
    println!();
    
    if let Some(memory) = &result.memory_usage {
        println!("Memory Usage:");
        println!("  Heap allocated:  {}", bytesize::ByteSize(memory.application.heap_allocated));
        println!("  Heap active:     {}", bytesize::ByteSize(memory.application.heap_active));
        println!("  Heap resident:   {}", bytesize::ByteSize(memory.application.heap_resident));
        println!("  Arrow tables:    {}", bytesize::ByteSize(memory.application.arrow_tables_size));
        println!("  String cache:    {}", bytesize::ByteSize(memory.application.string_cache_size));
        println!("  Fragmentation:   {}", bytesize::ByteSize(memory.application.fragmentation_bytes));
        println!();
        
        // Calculate memory efficiency
        if total_objects > 0 {
            let memory_per_object = memory.application.heap_allocated as f64 / total_objects as f64;
            println!("  Memory/object:   {:.1} bytes", memory_per_object);
        } else {
            println!("  Memory/object:   n/a");
        }
    }
    
    println!("{}", "=".repeat(80));

    Ok(())
}

fn table_rows(result: &kq::loader::SnapshotData, table: &str) -> usize {
    result.table_row_count(table)
}
