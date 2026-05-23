use anyhow::{Context, Result};
use clap::ValueEnum;
use kq::loader::{write_ipc_directory, write_parquet_directory, NdjsonLoader};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum SnapshotOutputFormat {
    Ipc,
    Parquet,
}

impl SnapshotOutputFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Ipc => "Arrow IPC",
            Self::Parquet => "Parquet",
        }
    }
}

pub struct ConvertOptions {
    pub input: PathBuf,
    pub output: PathBuf,
    pub overwrite: bool,
    pub format: SnapshotOutputFormat,
}

pub fn convert_snapshot(options: ConvertOptions) -> Result<()> {
    if options.output.exists() {
        if !options.overwrite {
            anyhow::bail!(
                "output directory already exists; pass --overwrite to replace: {}",
                options.output.display()
            );
        }
        std::fs::remove_dir_all(&options.output).with_context(|| {
            format!("Failed to remove existing output: {}", options.output.display())
        })?;
    }

    let started = Instant::now();
    let loader = NdjsonLoader::new();
    let (timestamp, tables, timing) = loader
        .load_directory(&options.input)
        .with_context(|| format!("Failed to load NDJSON snapshot: {}", options.input.display()))?;

    match options.format {
        SnapshotOutputFormat::Ipc => {
            write_ipc_directory(&options.output, timestamp, &tables).with_context(|| {
                format!(
                    "Failed to write Arrow IPC snapshot: {}",
                    options.output.display()
                )
            })?
        }
        SnapshotOutputFormat::Parquet => {
            write_parquet_directory(&options.output, timestamp, &tables).with_context(|| {
                format!(
                    "Failed to write Parquet snapshot: {}",
                    options.output.display()
                )
            })?
        }
    }

    println!(
        "{} snapshot written to {}",
        options.format.label(),
        options.output.display()
    );
    println!("  source: {}", options.input.display());
    println!("  tables: {}", tables.len());
    println!("  objects: {}", timing.object_count);
    println!("  conversion time: {:.2}s", started.elapsed().as_secs_f64());

    Ok(())
}
