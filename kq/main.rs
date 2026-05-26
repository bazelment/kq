use anyhow::Result;
use clap::Parser;
use std::io::IsTerminal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::time::FormatTime;

// Use jemalloc as the global allocator for better memory tracking
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Custom time formatter: MM-DD HH:MM:SS.mmm (no year, millisecond precision)
struct CompactTime;

impl FormatTime for CompactTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(w, "{}", now.format("%m-%d %H:%M:%S%.3f"))
    }
}

// Use modules from the kq library crate
use kq::cli::OutputFormat;
use kq::loader::LoaderConfig;

#[derive(Parser)]
#[command(
    name = "kq",
    about = "Lightning-fast SQL queries on Kubernetes cluster snapshots",
    version,
    author = "kq maintainers"
)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Enable debug logging
    #[arg(short, long, global = true)]
    debug: bool,

    /// Generate CPU profile and save to file (e.g., cpu-profile.pb)
    #[arg(long, global = true, value_name = "FILE")]
    cpu_profile: Option<std::path::PathBuf>,

    /// Generate memory profile and save to file (e.g., memory-profile.json)
    #[arg(long, global = true, value_name = "FILE")]
    memory_profile: Option<std::path::PathBuf>,

    /// Number of parallel threads for file processing (default: half CPU cores)
    #[arg(long, global = true, value_name = "N")]
    threads: Option<usize>,

    /// SQL query to execute (optional - if stdin is piped, reads query from stdin)
    #[arg(short, long, value_name = "QUERY")]
    query: Option<String>,

    /// Output format for query results
    #[arg(short, long, default_value = "table")]
    format: OutputFormat,

    /// Limit number of rows in query output
    #[arg(short, long, value_name = "N")]
    limit: Option<usize>,

    /// Show query performance profile
    #[arg(long)]
    profile: bool,

    /// Use simplified progress display (single line)
    #[arg(long)]
    simple_progress: bool,

    /// Batch mode: read queries from stdin line-by-line, output results without prompts/colors
    #[arg(long)]
    batch: bool,

    /// Snapshot file(s) to analyze (launches interactive mode by default, or query mode if stdin is piped)
    #[arg(value_name = "SNAPSHOT_FILE")]
    snapshots: Vec<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Start CPU profiling if requested
    let cpu_guard = if let Some(ref cpu_profile_path) = cli.cpu_profile {
        info!("Starting CPU profiling, will write to: {}", cpu_profile_path.display());
        Some(start_cpu_profiling()?)
    } else {
        None
    };

    // Start memory profiling if requested
    if let Some(ref memory_profile_path) = cli.memory_profile {
        info!("Memory profiling enabled, will write to: {}", memory_profile_path.display());
        start_memory_profiling()?;
    }

    // Initialize logging
    let log_level = if cli.debug {
        Level::DEBUG
    } else if cli.verbose {
        Level::INFO
    } else {
        Level::WARN
    };

    // Detect if stderr is a terminal - disable ANSI colors when redirected to file/pipe
    let use_ansi_colors = std::io::stderr().is_terminal();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(true)  // Show thread IDs
        .with_file(false)
        .with_line_number(false)
        .with_level(false)  // Hide log level (DEBUG/INFO/WARN)
        .with_timer(CompactTime)  // Use compact timestamp format
        .with_ansi(use_ansi_colors)  // Disable ANSI colors when not a terminal
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // Validate snapshots
    if cli.snapshots.is_empty() {
        eprintln!("Error: At least one snapshot file is required");
        eprintln!();
        eprintln!("Usage:");
        eprintln!("  kq <snapshot-file>                    # Interactive mode");
        eprintln!("  echo \"SELECT * FROM pods\" | kq <file>");
        eprintln!("  kq --query \"SELECT * FROM pods\" <file>");
        std::process::exit(1);
    }
    
    // Always use interactive mode - it will handle --query flag and stdin automatically
    run_interactive_mode(&cli).await?;

    // Cleanup: stop profiling if enabled
    if let Some(cpu_guard) = cpu_guard {
        if let Some(ref cpu_profile_path) = cli.cpu_profile {
            stop_cpu_profiling(cpu_guard, cpu_profile_path)?;
            println!("✅ CPU flamegraph written to: {}", cpu_profile_path.display());
        }
    }
    
    if let Some(ref memory_profile_path) = cli.memory_profile {
        stop_memory_profiling(memory_profile_path)?;
        println!("✅ Memory profile written to: {}", memory_profile_path.display());
    }
    
    Ok(())
}


/// Run interactive mode: start REPL (handles both interactive and query modes)
async fn run_interactive_mode(cli: &Cli) -> Result<()> {
    use kq::cli::InteractiveMode;
    use indicatif::{ProgressBar, ProgressStyle};
    
    let query_setup_start = std::time::Instant::now();
    
    let mut loader_config = LoaderConfig::default();
    
    // Apply CLI overrides
    if let Some(threads) = cli.threads {
        info!("Setting parallel threads to: {}", threads);
        loader_config.parallel_threads = threads;
    }
    loader_config.simple_progress = cli.simple_progress;
    
    let engine_config = kq::engine_setup::EngineSetupConfig {
        loader_config,
        show_memory_report: cli.memory_profile.is_some(),
    };
    
    let engine = kq::engine_setup::load_snapshots_and_prepare_engine(&cli.snapshots, &engine_config).await?;
    
    // Prepare interactive mode (schema analysis for completion)
    let is_terminal = std::io::stderr().is_terminal();
    let preparing_pb = if is_terminal {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        pb.set_message("Preparing interactive mode (analyzing schema for completion)...");
        Some(pb)
    } else {
        info!("Preparing interactive mode (analyzing schema for completion)...");
        None
    };
    
    let interactive_start = std::time::Instant::now();
    let mut interactive = InteractiveMode::new_with_options(engine, cli.format, cli.limit, cli.profile, cli.batch)?;
    let interactive_duration = interactive_start.elapsed();
    
    if let Some(pb) = preparing_pb {
        pb.finish_with_message(format!("✓ Ready ({:.1}s)\n", interactive_duration.as_secs_f64()));
    } else {
        info!("✓ Ready ({:.1}s)", interactive_duration.as_secs_f64());
        let query_setup_duration = query_setup_start.elapsed();
        eprintln!();
        eprintln!("{}", "─".repeat(60));
        eprintln!("Query Engine Setup Summary:");
        eprintln!("  Total setup time: {:.2}s", query_setup_duration.as_secs_f64());
        eprintln!("  - Schema analysis: {:.2}s", interactive_duration.as_secs_f64());
        eprintln!("{}", "─".repeat(60));
        eprintln!();
    }
    
    // Run interactive mode - it will handle --query flag and stdin automatically
    interactive.run_with_cli_options(cli.query.as_ref().map(|s| s.as_str())).await?;
    
    Ok(())
}

/// Start CPU profiling using pprof
fn start_cpu_profiling() -> Result<pprof::ProfilerGuard<'static>> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000) // Sample at 1000 Hz
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to start CPU profiler: {}", e))?;
    Ok(guard)
}

/// Stop CPU profiling and write report
fn stop_cpu_profiling(guard: pprof::ProfilerGuard, output_path: &std::path::Path) -> Result<()> {
    match guard.report().build() {
        Ok(report) => {
            // Generate flamegraph (SVG format) - the most useful visualization
            let flamegraph_file = std::fs::File::create(output_path)?;
            report.flamegraph(flamegraph_file)
                .map_err(|e| anyhow::anyhow!("Failed to generate flamegraph: {}", e))?;
            
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("Failed to build profiling report: {}", e)),
    }
}

/// Start memory profiling using jemalloc
fn start_memory_profiling() -> Result<()> {
    use jemalloc_ctl::{epoch, stats};
    
    // Trigger stats update
    epoch::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get epoch MIB: {}", e))?
        .advance()
        .map_err(|e| anyhow::anyhow!("Failed to advance epoch: {}", e))?;
    
    // Get initial stats
    let allocated = stats::allocated::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get allocated MIB: {}", e))?;
    let resident = stats::resident::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get resident MIB: {}", e))?;
    
    let allocated_val = allocated.read()
        .map_err(|e| anyhow::anyhow!("Failed to read allocated: {}", e))?;
    let resident_val = resident.read()
        .map_err(|e| anyhow::anyhow!("Failed to read resident: {}", e))?;
    
    info!("Memory profiling started - Initial allocated: {} bytes, resident: {} bytes",
          allocated_val, resident_val);
    
    Ok(())
}

/// Stop memory profiling and write report
fn stop_memory_profiling(output_path: &std::path::Path) -> Result<()> {
    use jemalloc_ctl::{epoch, stats};
    
    // Trigger final stats update
    epoch::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get epoch MIB: {}", e))?
        .advance()
        .map_err(|e| anyhow::anyhow!("Failed to advance epoch: {}", e))?;
    
    // Get final stats
    let allocated_mib = stats::allocated::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get allocated MIB: {}", e))?;
    let resident_mib = stats::resident::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get resident MIB: {}", e))?;
    let active_mib = stats::active::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get active MIB: {}", e))?;
    let mapped_mib = stats::mapped::mib()
        .map_err(|e| anyhow::anyhow!("Failed to get mapped MIB: {}", e))?;
    
    let allocated = allocated_mib.read()
        .map_err(|e| anyhow::anyhow!("Failed to read allocated: {}", e))?;
    let resident = resident_mib.read()
        .map_err(|e| anyhow::anyhow!("Failed to read resident: {}", e))?;
    let active = active_mib.read()
        .map_err(|e| anyhow::anyhow!("Failed to read active: {}", e))?;
    let mapped = mapped_mib.read()
        .map_err(|e| anyhow::anyhow!("Failed to read mapped: {}", e))?;
    
    // Create memory profile report
    let report = serde_json::json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "allocator": "jemalloc",
        "stats": {
            "allocated_bytes": allocated,
            "resident_bytes": resident,
            "active_bytes": active,
            "mapped_bytes": mapped,
            "allocated_mb": allocated as f64 / 1024.0 / 1024.0,
            "resident_mb": resident as f64 / 1024.0 / 1024.0,
            "active_mb": active as f64 / 1024.0 / 1024.0,
            "mapped_mb": mapped as f64 / 1024.0 / 1024.0,
        },
        "description": {
            "allocated": "Currently allocated memory (application-visible)",
            "resident": "Maximum number of bytes in physically resident data pages",
            "active": "Total number of bytes in active pages",
            "mapped": "Total number of bytes in active extents mapped by the allocator"
        }
    });
    
    // Write report to file
    let mut file = std::fs::File::create(output_path)?;
    std::io::Write::write_all(&mut file, serde_json::to_string_pretty(&report)?.as_bytes())?;
    
    Ok(())
}
