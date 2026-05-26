use anyhow::Result;
use clap::Parser;
use kq::loader::SnapshotLoader;
use std::time::Instant;
use tracing::{info, Level};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "registration_hotspot_benchmark")]
#[command(about = "Benchmark to identify hotspots in table registration with detailed per-operation timing")]
struct Args {
    /// Input snapshot files (gzipped JSON)
    #[arg(required = true, num_args = 1..)]
    files: Vec<String>,
    
    /// Output CSV file for detailed results
    #[arg(short, long)]
    output: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let args = Args::parse();
    
    println!("\n{}", "=".repeat(70));
    println!("Registration Hotspot Benchmark");
    println!("{}", "=".repeat(70));
    println!("Files: {} files", args.files.len());
    println!("{}", "=".repeat(70));
    
    // Load data
    info!("Loading snapshot data from {} files...", args.files.len());
    let load_start = Instant::now();
    let loader = SnapshotLoader::new();
    let snapshot_data = loader.load_and_combine(&args.files).await?;
    let load_time = load_start.elapsed();
    
    info!("Data loaded in {:.2}s", load_time.as_secs_f64());
    
    // Print table information
    println!("\n{}", "=".repeat(70));
    println!("Table Information");
    println!("{}", "=".repeat(70));
    
    let mut total_rows = 0;
    let mut total_size_mb = 0.0;
    
    for (name, batch) in &snapshot_data.tables {
        let rows = batch.num_rows();
        let cols = batch.num_columns();
        
        // Estimate size
        let mut size_bytes = 0;
        for column in batch.columns() {
            size_bytes += estimate_array_size(column.as_ref());
        }
        let size_mb = size_bytes as f64 / 1024.0 / 1024.0;
        
        println!("  {}: {} rows, {} cols, {:.2} MB", name, rows, cols, size_mb);
        total_rows += rows;
        total_size_mb += size_mb;
    }
    
    println!("{}", "-".repeat(70));
    println!("  Total: {} rows across {} tables, {:.2} MB", 
             total_rows, snapshot_data.tables.len(), total_size_mb);
    println!("{}", "=".repeat(70));
    
    // Get memory stats before registration
    use jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    let mem_before = stats::allocated::read()
        .map_err(|e| anyhow::anyhow!("Failed to read allocated memory: {:?}", e))? as u64;
    
    println!("\nMemory before registration: {:.2} MB", mem_before as f64 / 1024.0 / 1024.0);
    
    // Run registration with detailed instrumentation
    println!("\n{}", "=".repeat(70));
    println!("Starting Registration with Detailed Instrumentation");
    println!("{}", "=".repeat(70));
    println!();
    
    let register_start = Instant::now();
    
    // Create QueryEngine (this triggers the instrumented register_tables)
    use kq::query::QueryEngine;
    let engine = QueryEngine::new(snapshot_data).await?;
    
    let register_duration = register_start.elapsed();
    
    // Get memory stats after registration
    let _ = epoch::advance();
    let mem_after = stats::allocated::read()
        .map_err(|e| anyhow::anyhow!("Failed to read allocated memory: {:?}", e))? as u64;
    let mem_delta = (mem_after as i64 - mem_before as i64) / 1024 / 1024;
    
    println!("\n{}", "=".repeat(70));
    println!("Registration Summary");
    println!("{}", "=".repeat(70));
    println!("  Total registration time: {:.3}s", register_duration.as_secs_f64());
    println!("  Tables registered: {}", engine.table_count());
    println!("  Memory after registration: {:.2} MB", mem_after as f64 / 1024.0 / 1024.0);
    println!("  Memory delta: {} MB", mem_delta);
    println!("{}", "=".repeat(70));
    
    // Write CSV if requested
    if let Some(output_path) = args.output {
        println!("\nWriting detailed results to: {}", output_path);
        // Note: The detailed per-operation timing is already printed via [DETAILED] logs
        // This CSV could contain summary data
        std::fs::write(&output_path, format!(
            "metric,value\n\
             total_files,{}\n\
             total_rows,{}\n\
             total_size_mb,{:.2}\n\
             load_time_s,{:.3}\n\
             register_time_s,{:.3}\n\
             memory_before_mb,{:.2}\n\
             memory_after_mb,{:.2}\n\
             memory_delta_mb,{}\n",
            args.files.len(),
            total_rows,
            total_size_mb,
            load_time.as_secs_f64(),
            register_duration.as_secs_f64(),
            mem_before as f64 / 1024.0 / 1024.0,
            mem_after as f64 / 1024.0 / 1024.0,
            mem_delta
        ))?;
        println!("✓ Results written to {}", output_path);
    }
    
    println!("\n{}", "=".repeat(70));
    println!("Benchmark Complete");
    println!("{}", "=".repeat(70));
    println!("\nNote: Check the [DETAILED] output above for per-operation timing.");
    println!("The operation with the highest percentage is your bottleneck.");
    println!("{}", "=".repeat(70));
    
    Ok(())
}

fn estimate_array_size(array: &dyn arrow::array::Array) -> usize {
    let mut size = 0;
    
    // Add buffer sizes
    for buffer in array.to_data().buffers() {
        size += buffer.len();
    }
    
    // Add null buffer if present
    if let Some(null_buffer) = array.nulls() {
        size += null_buffer.buffer().len();
    }
    
    size
}
