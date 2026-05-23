use anyhow::{Context, Result};
use kq::engine_setup::{EngineSetupConfig, load_snapshots_and_prepare_engine};
use kq::loader::{SnapshotFormat, detect_snapshot_format};
use std::path::PathBuf;
use std::time::Instant;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::time::FormatTime;
use chrono::Local;

// Use jemalloc as the global allocator for better memory tracking
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Custom time formatter: MM-DD HH:MM:SS.mmm (no year, millisecond precision)
struct CompactTime;

impl FormatTime for CompactTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Local::now();
        write!(w, "{}", now.format("%m-%d %H:%M:%S%.3f"))
    }
}

fn parse_args() -> Result<Vec<PathBuf>> {
    let args: Vec<String> = std::env::args().collect();
    let mut paths = Vec::new();
    
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--path" {
            if i + 1 >= args.len() {
                anyhow::bail!("--path requires a value");
            }
            paths.push(PathBuf::from(&args[i + 1]));
            i += 2;
        } else {
            // Treat as a path argument
            paths.push(PathBuf::from(&args[i]));
            i += 1;
        }
    }
    
    if paths.is_empty() {
        anyhow::bail!("At least one path is required. Use --path <PATH> or provide paths as arguments");
    }
    
    Ok(paths)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup debug logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .with_timer(CompactTime)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .context("failed to set tracing subscriber")?;

    // Parse command-line arguments
    let snapshot_paths = parse_args()?;

    // Setup CPU profiling
    let cpu_profile_path = std::env::var("CPU_PROFILE")
        .unwrap_or_else(|_| "engine_setup_cpu_profile.pb".to_string());
    
    info!("Starting engine setup benchmark");
    info!("Snapshot paths: {:?}", snapshot_paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>());
    info!("CPU profile will be saved to: {}", cpu_profile_path);
    
    // Start CPU profiling with better error handling
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(100) // Sample at 100 Hz
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to start CPU profiler: {}", e))?;

    info!("{}", "=".repeat(80));

    // Run unified benchmark - load_snapshots_and_prepare_engine handles all formats automatically
    benchmark_engine_setup(&snapshot_paths).await?;
    
    // Stop profiling and save
    info!("\nStopping CPU profiler and saving profile...");
    match guard.report().build() {
        Ok(report) => {
            match std::fs::File::create(&cpu_profile_path) {
                Ok(file) => {
                    match report.flamegraph(file) {
                        Ok(_) => {
                            info!("CPU profile saved to: {}", cpu_profile_path);
                        }
                        Err(e) => {
                            warn!("Failed to generate flamegraph: {}. Profile data may still be available.", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create CPU profile file {}: {}", cpu_profile_path, e);
                }
            }
        }
        Err(e) => {
            warn!("Failed to build CPU profiling report: {}. This is non-fatal.", e);
        }
    }
    
    info!("\n{}", "=".repeat(80));
    info!("Benchmark completed successfully!");
    info!("{}", "=".repeat(80));
    
    Ok(())
}

async fn benchmark_engine_setup(paths: &[PathBuf]) -> Result<()> {
    info!("{}", "=".repeat(80));
    info!("Engine Setup Performance Benchmark");
    info!("{}", "=".repeat(80));
    info!("Loading {} snapshot(s):", paths.len());
    
    // Show format detection for informational purposes
    for (i, path) in paths.iter().enumerate() {
        let format = detect_snapshot_format(path).unwrap_or(SnapshotFormat::SingleJson);
        let path_str = path.display();
        if path.is_dir() {
            info!("  [{}] {} ({:?})", i + 1, path_str, format);
        } else {
            let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            info!("  [{}] {} ({:?}, {:.2} MB)", 
                i + 1, 
                path_str, 
                format,
                file_size as f64 / 1_000_000.0
            );
        }
    }
    info!("{}", "=".repeat(80));
    
    use kq::memory::MemoryUsageReport;
    
    // Use unified engine setup interface
    let config = EngineSetupConfig {
        loader_config: Default::default(),
        show_memory_report: false, // We'll report it ourselves
    };
    
    let start = Instant::now();
    let memory_before = MemoryUsageReport::current();
    
    let engine = load_snapshots_and_prepare_engine(paths, &config)
        .await
        .context("Failed to load snapshots and prepare engine")?;
    
    let duration = start.elapsed();
    let memory_after = MemoryUsageReport::current();
    
    let memory_delta = memory_after.application.heap_allocated.saturating_sub(memory_before.application.heap_allocated);
    let peak_memory = memory_after.application.heap_allocated.max(memory_before.application.heap_allocated);
    
    info!("\nBenchmark Results:");
    info!("  Total time: {:.3}s", duration.as_secs_f64());
    info!("  Paths loaded: {}", paths.len());
    info!("  Tables created: {}", engine.table_count());
    info!("  Memory delta: {:.2} MB", memory_delta as f64 / 1_000_000.0);
    info!("  Peak memory: {:.2} MB", peak_memory as f64 / 1_000_000.0);
    
    info!("{}", "=".repeat(80));
    
    Ok(())
}
