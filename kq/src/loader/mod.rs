use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use dashmap::DashMap;
use kq_memory::MemoryUsageReport;
use kq_schema::kubernetes::{ClusterSnapshot, SnapshotSummary};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

mod ipc_loader;
mod ndjson_loader;
mod parquet_loader;
mod phase_timing;
mod progress_reporter;
mod resource_table;
mod sax_json_parser;
mod streaming_json_loader;

use phase_timing::FileTimingDetail;
use progress_reporter::{create_progress_reporter, ProgressReporter};
use resource_table::RESOURCE_TABLES;

pub use ipc_loader::{write_ipc_directory, IpcLoader};
pub use ndjson_loader::NdjsonLoader;
pub use parquet_loader::{write_parquet_directory, ParquetLoader};
pub use streaming_json_loader::StreamingJsonLoader;

#[derive(Debug, Clone, PartialEq)]
pub enum LoadingPhase {
    ReadingFile,
    ParsingJSON,
    ConvertingNodes,
    ConvertingPods,
    ConvertingNamespaces,
    ConvertingDaemonSets,
    Finalizing,
}

impl LoadingPhase {
    pub fn description(&self) -> &'static str {
        match self {
            LoadingPhase::ReadingFile => "Reading file",
            LoadingPhase::ParsingJSON => "Parsing JSON",
            LoadingPhase::ConvertingNodes => "Converting nodes",
            LoadingPhase::ConvertingPods => "Converting pods",
            LoadingPhase::ConvertingNamespaces => "Converting namespaces",
            LoadingPhase::ConvertingDaemonSets => "Converting daemonsets",
            LoadingPhase::Finalizing => "Finalizing",
        }
    }
}

/// Snapshot format type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFormat {
    /// Single JSON file (possibly gzipped)
    SingleJson,
    /// Directory with per-type ndjson.gz files
    NdjsonDirectory,
    /// Directory with per-table Arrow IPC files
    ArrowIpcDirectory,
    /// Directory with per-table Parquet files
    ParquetDirectory,
}

/// Detect the snapshot format by checking if the path is a file or directory.
/// Directories with Arrow IPC table files use the IPC fast path; otherwise
/// directories with metadata.json are treated as NDJSON snapshots.
pub fn detect_snapshot_format<P: AsRef<Path>>(path: P) -> Result<SnapshotFormat> {
    let path = path.as_ref();
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Failed to access path: {}", path.display()))?;

    if metadata.is_dir() {
        // Check if it's a supported directory snapshot by looking for metadata.json.
        let metadata_file = path.join("metadata.json");
        if metadata_file.exists() {
            if RESOURCE_TABLES
                .iter()
                .any(|resource| path.join(resource.ipc_file).exists())
            {
                return Ok(SnapshotFormat::ArrowIpcDirectory);
            }
            if RESOURCE_TABLES
                .iter()
                .any(|resource| path.join(resource.parquet_file).exists())
            {
                return Ok(SnapshotFormat::ParquetDirectory);
            }
            return Ok(SnapshotFormat::NdjsonDirectory);
        }
        return Err(anyhow::anyhow!(
            "Directory does not appear to be a supported snapshot (missing metadata.json): {}",
            path.display()
        ));
    } else if metadata.is_file() {
        return Ok(SnapshotFormat::SingleJson);
    }
    Err(anyhow::anyhow!(
        "Path is neither a file nor a directory: {}",
        path.display()
    ))
}

pub fn concat_record_batches(
    schema: arrow_schema::SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => batches
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing single record batch")),
        len if len > 50 => tree_merge_record_batches(schema, batches),
        _ => arrow::compute::concat_batches(&schema, &batches)
            .context("Failed to concatenate record batches"),
    }
}

pub fn merge_table_batches(
    table_batches: std::collections::HashMap<String, Vec<RecordBatch>>,
) -> Result<std::collections::HashMap<String, RecordBatch>> {
    let mut tables = std::collections::HashMap::new();

    for (table_name, batches) in table_batches {
        if let Some(schema) = batches.first().map(RecordBatch::schema) {
            tables.insert(
                table_name,
                concat_record_batches(schema, batches)
                    .context("Failed to merge table batches")?,
            );
        }
    }

    Ok(tables)
}

pub fn single_batch_tables(
    tables: std::collections::HashMap<String, RecordBatch>,
) -> std::collections::HashMap<String, Vec<RecordBatch>> {
    tables
        .into_iter()
        .map(|(table_name, batch)| (table_name, vec![batch]))
        .collect()
}

fn tree_merge_record_batches(
    schema: arrow_schema::SchemaRef,
    mut batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    while batches.len() > 1 {
        let mut merged = Vec::with_capacity(batches.len().div_ceil(2));

        for chunk in batches.chunks(2) {
            if chunk.len() == 2 {
                merged.push(
                    arrow::compute::concat_batches(&schema, chunk)
                        .context("Failed to concatenate batches in tree merge")?,
                );
            } else {
                merged.push(chunk[0].clone());
            }
        }

        batches = merged;
    }

    batches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("tree merge produced no record batch"))
}

pub(crate) fn parse_snapshot_metadata_timestamp(metadata_path: &Path) -> Result<chrono::DateTime<chrono::Utc>> {
    let contents = std::fs::read_to_string(metadata_path)
        .with_context(|| format!("Failed to read metadata.json: {}", metadata_path.display()))?;
    let json: serde_json::Value = serde_json::from_str(&contents)
        .context("Failed to parse metadata.json as JSON")?;

    let timestamp = json
        .get("timestamp")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid timestamp field in metadata.json"))?;

    timestamp
        .parse()
        .context("Failed to parse timestamp")
}

/// Snapshot loader for reading and parsing Kubernetes cluster snapshots
pub struct SnapshotLoader {
    config: LoaderConfig,
}

/// Loaded snapshot data with Arrow tables and per-source table batches.
pub struct SnapshotData {
    pub snapshot: ClusterSnapshot,
    pub tables: std::collections::HashMap<String, RecordBatch>,
    pub table_batches: std::collections::HashMap<String, Vec<RecordBatch>>,
    pub memory_usage: Option<MemoryUsageReport>,
}

/// Optimized loader configuration
#[derive(Debug, Clone)]
pub struct LoaderConfig {
    pub progress_updates: bool,
    pub parallel_threads: usize,
    pub simple_progress: bool,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        let num_cpus = num_cpus::get();
        
        Self {
            progress_updates: {
                use std::io::IsTerminal;
                std::io::stderr().is_terminal()
            },
            parallel_threads: (num_cpus / 2).max(1),
            simple_progress: false,
        }
    }
}

impl SnapshotLoader {
    pub fn new() -> Self {
        Self {
            config: LoaderConfig::default(),
        }
    }

    pub fn with_config(config: LoaderConfig) -> Self {
        Self {
            config,
        }
    }
    /// Load and combine snapshot files/directories using modern batch processing
    /// Supports both single JSON files and NDJSON directories
    /// Always uses scatter-and-gather approach for consistency (even for single paths)
    /// Detects format for each path and combines (supports mixed formats)
    pub async fn load_and_combine<P: AsRef<Path> + Send + Sync>(&self, paths: &[P]) -> Result<SnapshotData> {
        if paths.is_empty() {
            return Err(anyhow::anyhow!("No snapshot files or directories provided"));
        }

        // Always use scatter-and-gather approach for consistency
        // This handles single paths, multiple paths, and mixed formats uniformly
        self.load_and_combine_scatter_gather(paths).await
    }

    /// Load and combine paths with mixed formats (JSON files and NDJSON directories)
    /// Uses scatter-and-gather: detects format for each path, loads in parallel, then combines tables
    /// Works for single paths, multiple paths, and mixed formats
    async fn load_and_combine_scatter_gather<P: AsRef<Path> + Send + Sync>(&self, paths: &[P]) -> Result<SnapshotData> {
        use std::collections::HashMap;
        use rayon::prelude::*;
        
        if paths.is_empty() {
            return Err(anyhow::anyhow!("No snapshot files or directories provided"));
        }

        let num_threads = self.config.parallel_threads;
        
        // Detect format for each path and prepare for parallel processing
        let paths_with_format: Vec<(SnapshotFormat, &P)> = paths.iter()
            .map(|path| {
                let format = detect_snapshot_format(path.as_ref())
                    .unwrap_or_else(|_| {
                        warn!("Failed to detect format for {}, assuming SingleJson", path.as_ref().display());
                        SnapshotFormat::SingleJson
                    });
                (format, path)
            })
            .collect();
        
        // Count formats for logging
        let json_count = paths_with_format.iter().filter(|(f, _)| *f == SnapshotFormat::SingleJson).count();
        let ndjson_count = paths_with_format.iter().filter(|(f, _)| *f == SnapshotFormat::NdjsonDirectory).count();
        let ipc_count = paths_with_format.iter().filter(|(f, _)| *f == SnapshotFormat::ArrowIpcDirectory).count();
        let parquet_count = paths_with_format.iter().filter(|(f, _)| *f == SnapshotFormat::ParquetDirectory).count();
        let all_partitioned_columnar = paths_with_format.iter().all(|(f, _)| {
            matches!(f, SnapshotFormat::ArrowIpcDirectory | SnapshotFormat::ParquetDirectory)
        });
        info!(
            "Format breakdown: {} JSON files, {} NDJSON directories, {} Arrow IPC directories, {} Parquet directories",
            json_count,
            ndjson_count,
            ipc_count,
            parquet_count
        );

        // Create shared concurrent data structures for combining tables
        let shared_tables: Arc<DashMap<String, Mutex<Vec<RecordBatch>>>> = Arc::new(DashMap::new());
        let shared_timestamp: Arc<Mutex<Option<chrono::DateTime<chrono::Utc>>>> = Arc::new(Mutex::new(None));
        let file_timings: Arc<Mutex<Vec<FileTimingDetail>>> = Arc::new(Mutex::new(Vec::new()));

        // Create unified progress reporter
        let mut progress_reporter = create_progress_reporter(self.config.progress_updates, self.config.simple_progress);
        progress_reporter.init(paths.len(), num_threads);
        for (i, path) in paths.iter().enumerate() {
            progress_reporter.register_file(i + 1, path.as_ref());
        }
        let progress_reporter: Arc<Mutex<Box<dyn ProgressReporter>>> = Arc::new(Mutex::new(progress_reporter));

        // Configure rayon thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .context("Failed to create thread pool")?;

        let shared_tables_clone = Arc::clone(&shared_tables);
        let shared_timestamp_clone = Arc::clone(&shared_timestamp);
        let file_timings_clone = Arc::clone(&file_timings);
        let progress_reporter_clone = Arc::clone(&progress_reporter);

        // Process paths in parallel - detect format and use appropriate loader
        let parallel_start = std::time::Instant::now();
        let results: Vec<Result<()>> = pool.install(|| {
            paths_with_format
                .par_iter()
                .enumerate()
                .map(|(file_idx, (format, path))| {
                    let file_position = file_idx + 1;
                    let filename = path.as_ref().file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    
                    debug!("Processing {}/{}: {} (format: {:?})", 
                        file_position, paths.len(), filename, format);
                    
                    // Update progress: Reading file
                    if let Ok(mut reporter) = progress_reporter_clone.lock() {
                        reporter.update_file_phase(
                            file_position,
                            LoadingPhase::ReadingFile,
                            10,
                            format!("Loading {}...", filename)
                        );
                    }
                    
                    // Load based on format
                    let (timestamp, table_batches, timing_detail): (
                        chrono::DateTime<chrono::Utc>,
                        HashMap<String, Vec<RecordBatch>>,
                        FileTimingDetail
                    ) = match format {
                        SnapshotFormat::NdjsonDirectory => {
                            let ndjson_loader = NdjsonLoader::new();
                            let (ts, tbls, timing) = ndjson_loader.load_directory(path.as_ref())
                                .with_context(|| format!("Failed to load NDJSON directory: {}", path.as_ref().display()))?;
                            (ts, single_batch_tables(tbls), timing)
                        }
                        SnapshotFormat::ArrowIpcDirectory => {
                            let ipc_loader = IpcLoader::new();
                            let (ts, tbls, timing) = ipc_loader.load_directory_batches(path.as_ref())
                                .with_context(|| format!("Failed to load Arrow IPC directory: {}", path.as_ref().display()))?;
                            (ts, tbls, timing)
                        }
                        SnapshotFormat::ParquetDirectory => {
                            let parquet_loader = ParquetLoader::new();
                            let (ts, tbls, timing) = parquet_loader.load_directory_batches(path.as_ref())
                                .with_context(|| format!("Failed to load Parquet directory: {}", path.as_ref().display()))?;
                            (ts, tbls, timing)
                        }
                        SnapshotFormat::SingleJson => {
                            // Use streaming SAX parser for compressed JSON files (2.31x faster)
                            let streaming_loader = StreamingJsonLoader::new();
                            let (ts, tbls, timing) = streaming_loader.load_file(path.as_ref())
                                .with_context(|| format!("Failed to load JSON file with streaming loader: {}", path.as_ref().display()))?;
                            (ts, single_batch_tables(tbls), timing)
                        }
                    };
                    
                    // Save timestamp from first snapshot only
                    {
                        let mut ts = shared_timestamp_clone.lock()
                            .map_err(|_| anyhow::anyhow!("Failed to lock timestamp"))?;
                        if ts.is_none() {
                            *ts = Some(timestamp);
                        }
                    }
                    
                    // Collect timing data
                    let mut timing_detail = timing_detail;
                    timing_detail.file_index = file_position;
                    let total_objects = timing_detail.object_count;
                    let duration_secs = timing_detail.total_duration.as_secs_f64();
                    
                    if let Ok(mut timings) = file_timings_clone.lock() {
                        timings.push(timing_detail.clone());
                    }
                    
                    // Push each table's batches to shared map
                    for (table_name, new_batches) in table_batches {
                        let entry = shared_tables_clone.entry(table_name.clone())
                            .or_insert_with(|| Mutex::new(Vec::new()));
                        
                        let mut batches = entry.lock()
                            .map_err(|_| anyhow::anyhow!("Failed to lock table {}", table_name))?;
                        batches.extend(new_batches);
                    }
                    
                    // Finish file progress
                    if let Ok(mut reporter) = progress_reporter_clone.lock() {
                        reporter.finish_file(file_position, duration_secs, total_objects, 0);
                    }
                    
                    debug!("Successfully processed: {}", filename);
                    Ok(())
                })
                .collect()
        });
        
        let wall_clock_time = parallel_start.elapsed();

        // Check for errors
        for (idx, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                // Print full error chain for debugging
                eprintln!("Error loading {}: {:#}", paths[idx].as_ref().display(), e);
                return Err(anyhow::anyhow!(
                    "Failed to load path {}: {:?}",
                    paths[idx].as_ref().display(),
                    e
                ));
            }
        }

        // Merge all batches for legacy JSON/NDJSON inputs. For Arrow IPC and
        // Parquet snapshots, keep one batch per source table and let DataFusion
        // scan them as partitions, avoiding a large copy during startup.
        let mut final_tables = HashMap::new();
        let mut table_batches = HashMap::new();
        for entry in shared_tables.iter() {
            let table_name = entry.key().clone();
            let batches = entry.value().lock()
                .map_err(|_| anyhow::anyhow!("Failed to lock table {} for merging", table_name))?
                .clone();
            
            if batches.is_empty() {
                continue;
            }
            
            if all_partitioned_columnar {
                table_batches.insert(table_name, batches);
            } else if batches.len() == 1 {
                let batch = batches
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing table batch after non-empty check"))?;
                table_batches.insert(table_name.clone(), vec![batch.clone()]);
                final_tables.insert(table_name, batch);
            } else {
                let schema = batches[0].schema();
                let merged = concat_record_batches(schema, batches)
                    .with_context(|| format!("Failed to merge batches for table {}", table_name))?;
                table_batches.insert(table_name.clone(), vec![merged.clone()]);
                final_tables.insert(table_name, merged);
            }
        }

        if all_partitioned_columnar {
            for (table_name, batches) in &table_batches {
                if batches.len() == 1 {
                    final_tables.insert(table_name.clone(), batches[0].clone());
                }
            }
        }

        // Get timestamp from first snapshot
        let timestamp = shared_timestamp.lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock timestamp"))?
            .ok_or_else(|| anyhow::anyhow!("No timestamp found"))?;

        // Update progress: merging
        if let Ok(mut reporter) = progress_reporter.lock() {
            reporter.set_merging(format!("Preparing {} tables...", table_batches.len()));
            reporter.finish_merging(table_batches.len());
            reporter.finish();
        }

        info!("Successfully combined {} tables from {} paths ({:.2}s)",
            table_batches.len(), paths.len(), wall_clock_time.as_secs_f64());

        Ok(SnapshotData {
            snapshot: ClusterSnapshot {
                timestamp,
                nodes: None,
                pods: None,
                namespaces: None,
                daemon_sets: None,
            },
            tables: final_tables,
            table_batches,
            memory_usage: Some(MemoryUsageReport::current()),
        })
    }
}

impl SnapshotData {
    pub fn get_summary(&self) -> SnapshotSummary {
        debug!("get_summary called, delegating to snapshot.get_summary()...");
        let result = self.snapshot.get_summary();
        debug!("get_summary returning.");
        result
    }

    pub fn get_table(&self, name: &str) -> Option<&RecordBatch> {
        self.tables.get(name)
    }

    pub fn get_table_batches(&self, name: &str) -> Option<&[RecordBatch]> {
        self.table_batches.get(name).map(Vec::as_slice)
    }

    pub fn table_row_count(&self, name: &str) -> usize {
        self.table_batches
            .get(name)
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
            .or_else(|| self.tables.get(name).map(RecordBatch::num_rows))
            .unwrap_or(0)
    }

    pub fn table_schema(&self, name: &str) -> Option<arrow_schema::SchemaRef> {
        self.table_batches
            .get(name)
            .and_then(|batches| batches.first().map(RecordBatch::schema))
            .or_else(|| self.tables.get(name).map(RecordBatch::schema))
    }

    pub fn list_tables(&self) -> Vec<String> {
        let mut table_names: Vec<String> = self
            .table_batches
            .keys()
            .chain(self.tables.keys())
            .cloned()
            .collect();
        table_names.sort();
        table_names.dedup();
        table_names
    }
}

impl Default for SnapshotLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_test_snapshot() -> ClusterSnapshot {
        use serde_json::json;
        
        // Use JSON deserialization for simpler, more robust test data
        let snapshot_json = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "nodes": [{
                "metadata": {
                    "name": "test-node",
                    "uid": "node-uid",
                    "labels": {
                        "node.kq.dev/pool": "general"
                    }
                },
                "spec": {
                    "podCIDR": "10.0.0.0/24"
                },
                "status": {
                    "capacity": {
                        "cpu": "4",
                        "memory": "8Gi",
                        "pods": "110"
                    },
                    "allocatable": {
                        "cpu": "3800m",
                        "memory": "7Gi",
                        "pods": "110"
                    },
                    "phase": "Ready"
                }
            }],
            "namespaces": [{
                "metadata": {
                    "name": "default",
                    "uid": "ns-uid"
                },
                "status": {
                    "phase": "Active"
                }
            }],
            "pods": []
        });
        
        serde_json::from_value(snapshot_json).unwrap()
    }

    #[test]
    fn test_snapshot_data_methods() {
        let snapshot = create_test_snapshot();
        let tables = std::collections::HashMap::new();
        let data = SnapshotData {
            snapshot,
            tables,
            table_batches: std::collections::HashMap::new(),
            memory_usage: None,
        };

        let summary = data.get_summary();
        assert_eq!(summary.node_count, 1);
        assert_eq!(summary.namespace_count, 1);

        let table_names = data.list_tables();
        assert_eq!(table_names.len(), 0); // No tables in this test
    }

    // ---------- detect_snapshot_format ----------

    #[test]
    fn detect_format_classifies_single_json_file_as_single_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let file_path = dir.path().join("snapshot.json");
        std::fs::write(&file_path, b"{}").unwrap();

        assert_eq!(
            detect_snapshot_format(&file_path).unwrap(),
            SnapshotFormat::SingleJson
        );
    }

    #[test]
    fn detect_format_classifies_gzipped_json_file_as_single_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let file_path = dir.path().join("snapshot.json.gz");
        std::fs::write(&file_path, b"placeholder").unwrap();

        assert_eq!(
            detect_snapshot_format(&file_path).unwrap(),
            SnapshotFormat::SingleJson
        );
    }

    #[test]
    fn detect_format_classifies_directory_with_only_metadata_as_ndjson() {
        let dir = tempfile::TempDir::new().unwrap();
        let snapshot_dir = dir.path().join("snap");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("metadata.json"), b"{}").unwrap();
        // Presence of ndjson resource files is not required for detection,
        // but include one to mirror the on-disk layout produced by the writer.
        std::fs::write(snapshot_dir.join("pods.ndjson.gz"), b"").unwrap();

        assert_eq!(
            detect_snapshot_format(&snapshot_dir).unwrap(),
            SnapshotFormat::NdjsonDirectory
        );
    }

    #[test]
    fn detect_format_prefers_arrow_ipc_when_directory_has_arrow_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let snapshot_dir = dir.path().join("snap");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("metadata.json"), b"{}").unwrap();
        std::fs::write(snapshot_dir.join("pods.arrow"), b"").unwrap();

        assert_eq!(
            detect_snapshot_format(&snapshot_dir).unwrap(),
            SnapshotFormat::ArrowIpcDirectory
        );
    }

    #[test]
    fn detect_format_classifies_parquet_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let snapshot_dir = dir.path().join("snap");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("metadata.json"), b"{}").unwrap();
        std::fs::write(snapshot_dir.join("pods.parquet"), b"").unwrap();

        assert_eq!(
            detect_snapshot_format(&snapshot_dir).unwrap(),
            SnapshotFormat::ParquetDirectory
        );
    }

    #[test]
    fn detect_format_prefers_arrow_when_directory_has_both_arrow_and_parquet() {
        // Pins the documented routing: Arrow IPC files take precedence over
        // Parquet when both are present alongside metadata.json. Changing this
        // tie-break would silently re-route real snapshots.
        let dir = tempfile::TempDir::new().unwrap();
        let snapshot_dir = dir.path().join("snap");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("metadata.json"), b"{}").unwrap();
        std::fs::write(snapshot_dir.join("pods.arrow"), b"").unwrap();
        std::fs::write(snapshot_dir.join("pods.parquet"), b"").unwrap();

        assert_eq!(
            detect_snapshot_format(&snapshot_dir).unwrap(),
            SnapshotFormat::ArrowIpcDirectory
        );
    }

    #[test]
    fn detect_format_rejects_directory_without_metadata_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let snapshot_dir = dir.path().join("snap");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("pods.ndjson.gz"), b"").unwrap();

        let err = detect_snapshot_format(&snapshot_dir).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("metadata.json"),
            "error should mention the missing metadata.json, got: {message}"
        );
    }

    #[test]
    fn detect_format_returns_error_for_missing_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("does-not-exist");

        let err = detect_snapshot_format(&missing).unwrap_err();
        assert!(
            err.to_string().contains("Failed to access path"),
            "error should mention the inaccessible path, got: {err}"
        );
    }
}
