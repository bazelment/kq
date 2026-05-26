use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

use super::{
    concat_record_batches, parse_snapshot_metadata_timestamp,
    phase_timing::FileTimingDetail, resource_table::RESOURCE_TABLES,
};

/// Loader for directory-based snapshots with per-type ndjson.gz files
pub struct NdjsonLoader {
    buffer_size: usize,
    batch_size: usize,
}

impl NdjsonLoader {
    pub fn new() -> Self {
        Self {
            buffer_size: 8 * 1024 * 1024, // 8MB buffer
            batch_size: 8192, // Default batch size
        }
    }

    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            batch_size: 8192,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Load a directory-based snapshot
    /// Returns (timestamp, tables, timing_detail)
    pub fn load_directory<P: AsRef<Path>>(
        &self,
        dir_path: P,
    ) -> Result<(
        DateTime<Utc>,
        HashMap<String, RecordBatch>,
        FileTimingDetail,
    )> {
        let dir_path = dir_path.as_ref();
        let start_time = Instant::now();
        info!("Loading ndjson directory snapshot: {}", dir_path.display());

        // Phase 1: Parse metadata.json to get timestamp
        let file_io_start = Instant::now();
        let metadata_path = dir_path.join("metadata.json");
        let timestamp = parse_snapshot_metadata_timestamp(&metadata_path)
            .with_context(|| format!("Failed to parse metadata.json from: {}", dir_path.display()))?;
        let file_io_duration = file_io_start.elapsed();

        // Phase 2: Load each resource type file using arrow_json::ReaderBuilder
        let json_parse_start = Instant::now();
        let mut tables = HashMap::new();
        let mut total_objects = 0;

        for resource in RESOURCE_TABLES {
            let table_path = dir_path.join(resource.ndjson_file);
            if !table_path.exists() {
                continue;
            }

            debug!("Loading {} from: {}", resource.table_name, table_path.display());
            let batch = self.load_ndjson_with_arrow(&table_path, resource.schema())?;
            if batch.num_rows() > 0 {
                total_objects += batch.num_rows();
                tables.insert(resource.table_name.to_string(), batch);
            }
        }

        let json_parse_duration = json_parse_start.elapsed();
        // Arrow conversion happens inline with ReaderBuilder, so we track it together
        let arrow_conversion_duration = json_parse_duration; // Same duration since ReaderBuilder does both

        let total_duration = start_time.elapsed();

        info!(
            "Ndjson directory loading complete: {} tables created from {} objects",
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
            json_parsing_duration: json_parse_duration,
            arrow_conversion_duration,
            total_duration,
            object_count: total_objects,
        };

        Ok((timestamp, tables, timing_detail))
    }

    /// Load a gzipped ndjson file using arrow_json::ReaderBuilder
    /// 
    /// This streams the decompressed data directly to ReaderBuilder without loading
    /// everything into memory first. The reader ignores whitespace between JSON values,
    /// including newlines, allowing parsing of newline-delimited JSON (NDJSON).
    /// 
    /// Returns a single RecordBatch containing all rows
    fn load_ndjson_with_arrow(
        &self,
        file_path: &Path,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<RecordBatch> {
        use flate2::read::GzDecoder;

        let file = std::fs::File::open(file_path)
            .with_context(|| format!("Failed to open ndjson file: {}", file_path.display()))?;
        // Optimized buffer chain: single BufReader around GzDecoder
        // GzDecoder doesn't implement BufRead, so we need BufReader for efficient reading
        // We skip the first BufReader since GzDecoder will read from File directly
        let decoder = GzDecoder::new(file);
        let buf_reader = BufReader::with_capacity(self.buffer_size, decoder);

        // Create ReaderBuilder with the provided schema
        // ReaderBuilder::build() accepts any BufRead, so we can stream the decompressed
        // data directly without loading everything into memory first
        let json_reader = ReaderBuilder::new(schema.clone())
            .with_batch_size(self.batch_size) // Process in batches, then concatenate
            .build(buf_reader)
            .with_context(|| format!("Failed to build JSON reader for: {}", file_path.display()))?;

        // Collect all batches and concatenate them
        let mut batches = Vec::new();
        for (idx, batch_result) in json_reader.enumerate() {
            let batch = batch_result
                .with_context(|| format!("Failed to read batch {} from: {} (this usually means schema mismatch with JSON data)", idx, file_path.display()))?;
            batches.push(batch);
        }

        concat_record_batches(schema, batches)
            .with_context(|| format!("Failed to concatenate batches from: {}", file_path.display()))
    }
}

impl Default for NdjsonLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_ndjson_dir() -> Result<(TempDir, std::path::PathBuf)> {
        let temp_dir = TempDir::new()?;
        let dir_path = temp_dir.path().to_path_buf();

        // Create metadata.json
        let metadata_path = dir_path.join("metadata.json");
        let mut metadata_file = std::fs::File::create(&metadata_path)?;
        writeln!(
            metadata_file,
            r#"{{"timestamp": "2024-01-01T00:00:00Z"}}"#
        )?;

        // Create test pods.ndjson.gz
        // Note: The JSON structure needs to match the nested schema structure
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let pods_path = dir_path.join("pods.ndjson.gz");
        let pods_file = std::fs::File::create(&pods_path)?;
        let mut encoder = GzEncoder::new(pods_file, Compression::default());
        // Simplified pod JSON that matches the schema
        writeln!(
            encoder,
            r#"{{"metadata":{{"name":"test-pod","namespace":"default","uid":"pod-uid","creationTimestamp":"2024-01-01T00:00:00Z","labels":null,"annotations":null}},"spec":{{"node_name":null,"restart_policy":null,"scheduler_name":null,"priority_class_name":null,"node_selector":null,"containers":[],"affinity":null,"tolerations":null}},"status":{{"phase":"Running","start_time":null}},"cpu_request_total":null,"memory_request_total":null,"app":null,"product":null,"tenant":null}}"#
        )?;
        encoder.finish()?;

        Ok((temp_dir, dir_path))
    }

    #[test]
    fn test_load_directory() {
        let (temp_dir, dir_path) = create_test_ndjson_dir().unwrap();
        let loader = NdjsonLoader::new();
        let (timestamp, tables, timing) = loader.load_directory(&dir_path).unwrap();

        assert_eq!(timestamp.year(), 2024);
        assert!(tables.contains_key("pods"));
        assert_eq!(timing.object_count, 1);
        assert_eq!(tables.get("pods").unwrap().num_rows(), 1);

        drop(temp_dir); // Cleanup
    }

    #[test]
    fn test_parse_metadata() {
        let (temp_dir, dir_path) = create_test_ndjson_dir().unwrap();
        let metadata_path = dir_path.join("metadata.json");
        let timestamp = parse_snapshot_metadata_timestamp(&metadata_path).unwrap();

        assert_eq!(timestamp.year(), 2024);
        drop(temp_dir);
    }
}
