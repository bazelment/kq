use anyhow::{Context, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info};

use super::{
    merge_table_batches, parse_snapshot_metadata_timestamp,
    phase_timing::FileTimingDetail, resource_table::RESOURCE_TABLES,
};

/// Loader for directory-based snapshots with per-table Arrow IPC files.
pub struct IpcLoader;

impl IpcLoader {
    pub fn new() -> Self {
        Self
    }

    /// Load a directory-based Arrow IPC snapshot.
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
        info!("Loading Arrow IPC directory snapshot: {}", dir_path.display());

        let file_io_start = Instant::now();
        let metadata_path = dir_path.join("metadata.json");
        let timestamp = parse_snapshot_metadata_timestamp(&metadata_path).with_context(|| {
            format!("Failed to parse metadata.json from: {}", dir_path.display())
        })?;
        let file_io_duration = file_io_start.elapsed();

        let arrow_start = Instant::now();
        let mut tables = HashMap::new();
        let mut total_objects = 0usize;

        for resource in RESOURCE_TABLES {
            let table_path = dir_path.join(resource.ipc_file);
            if !table_path.exists() {
                continue;
            }

            debug!("Loading {} from: {}", resource.table_name, table_path.display());
            let batches = read_ipc_file_batches(&table_path).with_context(|| {
                format!("Failed to read Arrow IPC table: {}", table_path.display())
            })?;
            let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if row_count > 0 {
                total_objects += row_count;
                tables.insert(resource.table_name.to_string(), batches);
            }
        }

        let arrow_duration = arrow_start.elapsed();
        let total_duration = start_time.elapsed();

        info!(
            "Arrow IPC directory loading complete: {} tables created from {} objects",
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
            arrow_conversion_duration: arrow_duration,
            total_duration,
            object_count: total_objects,
        };

        Ok((timestamp, tables, timing_detail))
    }
}

impl Default for IpcLoader {
    fn default() -> Self {
        Self::new()
    }
}

pub fn write_ipc_directory<P: AsRef<Path>>(
    output_dir: P,
    timestamp: DateTime<Utc>,
    tables: &HashMap<String, RecordBatch>,
) -> Result<()> {
    let output_dir = output_dir.as_ref();
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {}", output_dir.display()))?;

    let metadata = serde_json::json!({
        "format": "arrow-ipc",
        "timestamp": timestamp.to_rfc3339(),
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
            write_ipc_file(&output_dir.join(resource.ipc_file), batch)
                .with_context(|| format!("Failed to write table {}", resource.table_name))?;
        }
    }

    Ok(())
}

fn read_ipc_file_batches(file_path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open Arrow IPC file: {}", file_path.display()))?;
    let mut reader = FileReader::try_new_buffered(file, None)
        .with_context(|| format!("Failed to build Arrow IPC reader for: {}", file_path.display()))?;

    let mut batches = Vec::with_capacity(reader.num_batches());
    for (idx, batch) in reader.by_ref().enumerate() {
        batches.push(batch.with_context(|| {
            format!(
                "Failed to read IPC batch {} from: {}",
                idx,
                file_path.display()
            )
        })?);
    }

    Ok(batches)
}

fn write_ipc_file(file_path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(file_path)
        .with_context(|| format!("Failed to create Arrow IPC file: {}", file_path.display()))?;
    let mut writer = FileWriter::try_new_buffered(file, batch.schema_ref())
        .with_context(|| format!("Failed to create Arrow IPC writer for: {}", file_path.display()))?;
    writer
        .write(batch)
        .with_context(|| format!("Failed to write Arrow IPC batch: {}", file_path.display()))?;
    writer
        .finish()
        .with_context(|| format!("Failed to finish Arrow IPC file: {}", file_path.display()))?;
    Ok(())
}

fn table_rows(tables: &HashMap<String, RecordBatch>, table: &str) -> usize {
    tables.get(table).map_or(0, |batch| batch.num_rows())
}
