use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info};

use super::{
    merge_table_batches, parse_snapshot_metadata_timestamp,
    phase_timing::FileTimingDetail, resource_table::RESOURCE_TABLES,
};

/// Loader for directory-based snapshots with per-table Parquet files.
pub struct ParquetLoader {
    batch_size: usize,
}

impl ParquetLoader {
    pub fn new() -> Self {
        Self {
            batch_size: 16 * 1024,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Load a directory-based Parquet snapshot.
    /// Returns (timestamp, tables, timing_detail).
    pub fn load_directory<P: AsRef<Path>>(
        &self,
        dir_path: P,
    ) -> Result<(
        DateTime<Utc>,
        HashMap<String, RecordBatch>,
        FileTimingDetail,
    )> {
        let (timestamp, table_batches, timing_detail) = self.load_directory_batches(dir_path)?;
        let tables = merge_table_batches(table_batches)?;
        Ok((timestamp, tables, timing_detail))
    }

    pub fn load_directory_batches<P: AsRef<Path>>(
        &self,
        dir_path: P,
    ) -> Result<(
        DateTime<Utc>,
        HashMap<String, Vec<RecordBatch>>,
        FileTimingDetail,
    )> {
        let dir_path = dir_path.as_ref();
        let start_time = Instant::now();
        info!("Loading Parquet directory snapshot: {}", dir_path.display());

        let file_io_start = Instant::now();
        let metadata_path = dir_path.join("metadata.json");
        let timestamp = parse_snapshot_metadata_timestamp(&metadata_path).with_context(|| {
            format!("Failed to parse metadata.json from: {}", dir_path.display())
        })?;
        let file_io_duration = file_io_start.elapsed();

        let parquet_start = Instant::now();
        let mut tables = HashMap::new();
        let mut total_objects = 0usize;

        for resource in RESOURCE_TABLES {
            let table_path = dir_path.join(resource.parquet_file);
            if !table_path.exists() {
                continue;
            }

            debug!("Loading {} from: {}", resource.table_name, table_path.display());
            let batches =
                read_parquet_file_batches(&table_path, self.batch_size).with_context(|| {
                    format!("Failed to read Parquet table: {}", table_path.display())
                })?;
            let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if row_count > 0 {
                total_objects += row_count;
                tables.insert(resource.table_name.to_string(), batches);
            }
        }

        let parquet_duration = parquet_start.elapsed();
        let total_duration = start_time.elapsed();

        info!(
            "Parquet directory loading complete: {} tables created from {} objects",
            tables.len(),
            total_objects
        );

        let timing_detail = FileTimingDetail {
            file_name: dir_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            file_index: 0,
            file_io_duration,
            json_parsing_duration: std::time::Duration::ZERO,
            arrow_conversion_duration: parquet_duration,
            total_duration,
            object_count: total_objects,
        };

        Ok((timestamp, tables, timing_detail))
    }
}

impl Default for ParquetLoader {
    fn default() -> Self {
        Self::new()
    }
}

pub fn write_parquet_directory<P: AsRef<Path>>(
    output_dir: P,
    timestamp: DateTime<Utc>,
    tables: &HashMap<String, RecordBatch>,
) -> Result<()> {
    write_parquet_directory_with_compression(output_dir, timestamp, tables, Compression::SNAPPY)
}

pub fn write_parquet_directory_with_compression<P: AsRef<Path>>(
    output_dir: P,
    timestamp: DateTime<Utc>,
    tables: &HashMap<String, RecordBatch>,
    compression: Compression,
) -> Result<()> {
    let output_dir = output_dir.as_ref();
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {}", output_dir.display()))?;

    let metadata = serde_json::json!({
        "format": "parquet",
        "timestamp": timestamp.to_rfc3339(),
        "compression": compression.to_string(),
        "tables": {
            "pods": table_rows(tables, "pods"),
            "nodes": table_rows(tables, "nodes"),
            "namespaces": table_rows(tables, "namespaces"),
            "daemonSets": table_rows(tables, "daemon_sets"),
        }
    });
    std::fs::write(
        output_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )
    .with_context(|| format!("Failed to write metadata.json in {}", output_dir.display()))?;

    for resource in RESOURCE_TABLES {
        if let Some(batch) = tables.get(resource.table_name) {
            write_parquet_file(&output_dir.join(resource.parquet_file), batch, compression)
                .with_context(|| format!("Failed to write table {}", resource.table_name))?;
        }
    }

    Ok(())
}

fn read_parquet_file_batches(file_path: &Path, batch_size: usize) -> Result<Vec<RecordBatch>> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open Parquet file: {}", file_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("Failed to build Parquet reader for: {}", file_path.display()))?
        .with_batch_size(batch_size);
    let reader = builder.build().with_context(|| {
        format!(
            "Failed to create Parquet batch reader for: {}",
            file_path.display()
        )
    })?;

    let mut batches = Vec::new();
    for (idx, batch) in reader.enumerate() {
        batches.push(batch.with_context(|| {
            format!(
                "Failed to read Parquet batch {} from: {}",
                idx,
                file_path.display()
            )
        })?);
    }

    Ok(batches)
}

fn write_parquet_file(
    file_path: &Path,
    batch: &RecordBatch,
    compression: Compression,
) -> Result<()> {
    let file = File::create(file_path)
        .with_context(|| format!("Failed to create Parquet file: {}", file_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| format!("Failed to create Parquet writer for: {}", file_path.display()))?;
    writer
        .write(batch)
        .with_context(|| format!("Failed to write Parquet batch: {}", file_path.display()))?;
    writer
        .close()
        .with_context(|| format!("Failed to close Parquet file: {}", file_path.display()))?;
    Ok(())
}

fn table_rows(tables: &HashMap<String, RecordBatch>, table: &str) -> usize {
    tables.get(table).map_or(0, |batch| batch.num_rows())
}
