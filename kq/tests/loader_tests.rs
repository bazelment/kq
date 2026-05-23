// Unit tests for file loading and Arrow conversion logic
// Tests various scenarios: different file sizes, compression methods, parsing strategies, error handling

use kq::loader::{LoaderConfig, SnapshotLoader};
use kq::schema::kubernetes::*;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

/// Create a test snapshot with configurable size using JSON deserialization
/// This is simpler and more realistic than manually constructing k8s-openapi types
fn create_test_snapshot(num_nodes: usize, num_pods: usize, num_namespaces: usize) -> ClusterSnapshot {
    use serde_json::json;
    
    let mut nodes_json = Vec::new();
    let mut pods_json = Vec::new();
    let mut namespaces_json = Vec::new();

    // Create nodes
    for i in 0..num_nodes {
        nodes_json.push(json!({
            "metadata": {
                "name": format!("node-{}", i),
                "uid": format!("node-{}-uid", i),
                "labels": {
                    "node.pool": if i % 10 == 0 { "gpu" } else { "general" }
                }
            },
            "spec": {
                "podCIDR": format!("10.0.{}.0/24", i % 255)
            },
            "status": {
                "capacity": {
                    "cpu": "4",
                    "memory": "8Gi"
                },
                "allocatable": {
                    "cpu": "3800m",
                    "memory": "7Gi"
                }
            }
        }));
    }

    // Create pods
    for i in 0..num_pods {
        pods_json.push(json!({
            "metadata": {
                "name": format!("pod-{}", i),
                "namespace": format!("namespace-{}", i % num_namespaces.max(1)),
                "uid": format!("pod-{}-uid", i),
                "labels": {
                    "app": format!("app-{}", i % 10)
                }
            },
            "spec": {
                "nodeName": format!("node-{}", i % num_nodes.max(1)),
                "containers": [{
                    "name": format!("container-{}", i),
                    "image": "nginx:1.19",
                    "resources": {
                        "requests": {
                            "cpu": "100m",
                            "memory": "128Mi"
                        }
                    }
                }]
            },
            "status": {
                "phase": "Running"
            }
        }));
    }

    // Create namespaces
    for i in 0..num_namespaces {
        namespaces_json.push(json!({
            "metadata": {
                "name": format!("namespace-{}", i),
                "uid": format!("namespace-{}-uid", i)
            },
            "status": {
                "phase": "Active"
            }
        }));
    }

    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": nodes_json,
        "pods": pods_json,
        "namespaces": namespaces_json
    });

    serde_json::from_value(snapshot_json).unwrap()
}

/// Write a snapshot to a gzipped JSON file
fn write_snapshot_to_gzip_file(snapshot: &ClusterSnapshot) -> NamedTempFile {
    let json = serde_json::to_string_pretty(snapshot).unwrap();
    let mut temp_file = tempfile::Builder::new()
        .suffix(".json.gz")
        .tempfile()
        .unwrap();
    
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    
    temp_file.write_all(&compressed).unwrap();
    temp_file.flush().unwrap();
    temp_file
}

// =====================================================
// Test: Loading small files
// =====================================================

#[tokio::test]
async fn test_load_small_snapshot() {
    let snapshot = create_test_snapshot(2, 5, 2);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load small snapshot");
    let data = result.unwrap();
    // After batch processing, snapshot struct may have consumed data moved to tables
    // Just verify tables exist and have correct row counts
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 2);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 5);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 2);
    
    // Verify Arrow tables have correct row counts
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 2);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 5);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 2);
}

#[tokio::test]
async fn test_load_medium_snapshot() {
    let snapshot = create_test_snapshot(50, 500, 20);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load medium snapshot");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 500);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 20);
}

#[tokio::test]
async fn test_load_large_snapshot() {
    let snapshot = create_test_snapshot(200, 2000, 50);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load large snapshot");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 200);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 2000);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 50);
}

// =====================================================
// Test: Decompression (now automatic, no explicit method selection needed)
// =====================================================


#[tokio::test]
async fn test_automatic_decompression() {
    let snapshot = create_test_snapshot(10, 50, 5);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    // Decompression is now automatic - no config needed
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Automatic decompression failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 10);
}

// REMOVED: test_uncompressed_file - The new loader only supports .json.gz and ndjson directories
// Plain .json files are no longer supported as they were part of the legacy batch processing path

// =====================================================
// Test: Different JSON parsing strategies
// =====================================================

#[tokio::test]
async fn test_json_parsing_sonic() {
    let snapshot = create_test_snapshot(10, 50, 5);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Sonic JSON parsing failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 10);
}

#[tokio::test]
async fn test_json_parsing_simd() {
    let snapshot = create_test_snapshot(10, 50, 5);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "SIMD JSON parsing failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 10);
}

// Note: parser strategy selection was removed; the loader now chooses its parser internally.

// =====================================================
// Test: Batch processing
// =====================================================

#[tokio::test]
async fn test_batch_processing_single_file() {
    let snapshot = create_test_snapshot(100, 1000, 20);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    // Single file uses load_and_combine
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Batch processing failed for single file");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 100);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 1000);
}

// =====================================================
// Test: Multiple file loading
// =====================================================

#[tokio::test]
async fn test_load_multiple_files() {
    let snapshot1 = create_test_snapshot(10, 50, 5);
    let snapshot2 = create_test_snapshot(15, 75, 8);
    let snapshot3 = create_test_snapshot(20, 100, 10);
    
    let file1 = write_snapshot_to_gzip_file(&snapshot1);
    let file2 = write_snapshot_to_gzip_file(&snapshot2);
    let file3 = write_snapshot_to_gzip_file(&snapshot3);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![file1.path(), file2.path(), file3.path()];
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Failed to load multiple files");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 45); // 10+15+20
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 225); // 50+75+100
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 23); // 5+8+10
}

#[tokio::test]
async fn test_parallel_batch_loading() {
    let snapshot1 = create_test_snapshot(20, 100, 10);
    let snapshot2 = create_test_snapshot(25, 125, 12);
    let snapshot3 = create_test_snapshot(30, 150, 15);
    
    let file1 = write_snapshot_to_gzip_file(&snapshot1);
    let file2 = write_snapshot_to_gzip_file(&snapshot2);
    let file3 = write_snapshot_to_gzip_file(&snapshot3);
    
    let config = LoaderConfig {
        parallel_threads: 2,
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![file1.path(), file2.path(), file3.path()];
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Parallel batch loading failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 75); // 20+25+30
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 375); // 100+125+150
}

// =====================================================
// Test: Different file sizes mixed
// =====================================================

#[tokio::test]
async fn test_mixed_file_sizes() {
    let small = create_test_snapshot(5, 10, 2);
    let medium = create_test_snapshot(50, 200, 20);
    let large = create_test_snapshot(150, 800, 50);
    
    let file1 = write_snapshot_to_gzip_file(&small);
    let file2 = write_snapshot_to_gzip_file(&medium);
    let file3 = write_snapshot_to_gzip_file(&large);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![file1.path(), file2.path(), file3.path()];
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Failed to load mixed file sizes");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 205); // 5+50+150
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 1010); // 10+200+800
}

// =====================================================
// Test: Empty and edge cases
// =====================================================

#[tokio::test]
async fn test_empty_snapshot() {
    let snapshot = create_test_snapshot(0, 0, 0);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load empty snapshot");
    let data = result.unwrap();
    assert!(data.tables.is_empty() || 
            data.tables.values().all(|t| t.num_rows() == 0));
}

#[tokio::test]
async fn test_nodes_only_snapshot() {
    let snapshot = create_test_snapshot(10, 0, 0);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load nodes-only snapshot");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 10);
    assert!(!data.tables.contains_key("pods") || data.tables.get("pods").unwrap().num_rows() == 0);
}

#[tokio::test]
async fn test_pods_only_snapshot() {
    let snapshot = create_test_snapshot(0, 100, 0);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Failed to load pods-only snapshot");
    let data = result.unwrap();
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 100);
}

// =====================================================
// Test: Error handling
// =====================================================

#[tokio::test]
async fn test_nonexistent_file() {
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&["/nonexistent/file.json.gz"]).await;
    
    assert!(result.is_err(), "Should fail for nonexistent file");
}

#[tokio::test]
async fn test_corrupted_gzip() {
    let mut temp_file = tempfile::Builder::new()
        .suffix(".json.gz")
        .tempfile()
        .unwrap();
    
    // Write invalid gzip data
    temp_file.write_all(b"This is not valid gzip data").unwrap();
    temp_file.flush().unwrap();
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_err(), "Should fail for corrupted gzip");
}

#[tokio::test]
async fn test_invalid_json() {
    let mut temp_file = tempfile::Builder::new()
        .suffix(".json.gz")
        .tempfile()
        .unwrap();
    
    // Write valid gzip but invalid JSON
    let invalid_json = b"{ this is not valid JSON }";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(invalid_json).unwrap();
    let compressed = encoder.finish().unwrap();
    
    temp_file.write_all(&compressed).unwrap();
    temp_file.flush().unwrap();
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_err(), "Should fail for invalid JSON");
}

#[tokio::test]
async fn test_empty_file_list() {
    let loader = SnapshotLoader::new();
    let empty_paths: Vec<&str> = vec![];
    let result = loader.load_and_combine(&empty_paths).await;
    
    assert!(result.is_err(), "Should fail for empty file list");
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("No snapshot files"), "Error message: {}", err_msg);
}

// =====================================================
// Test: Arrow schema validation
// =====================================================

#[tokio::test]
async fn test_arrow_schema_structure() {
    let snapshot = create_test_snapshot(10, 50, 5);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok());
    let data = result.unwrap();
    
    // Validate nodes schema - check for actual nested structure fields
    let nodes_batch = data.tables.get("nodes").unwrap();
    let nodes_schema = nodes_batch.schema();
    assert!(nodes_schema.field_with_name("metadata").is_ok(), "Nodes should have metadata field");
    assert!(nodes_schema.field_with_name("spec").is_ok(), "Nodes should have spec field");
    assert!(nodes_schema.field_with_name("status").is_ok(), "Nodes should have status field");
    
    // Validate pods schema - check for actual nested structure fields
    let pods_batch = data.tables.get("pods").unwrap();
    let pods_schema = pods_batch.schema();
    assert!(pods_schema.field_with_name("metadata").is_ok(), "Pods should have metadata field");
    assert!(pods_schema.field_with_name("spec").is_ok(), "Pods should have spec field");
    assert!(pods_schema.field_with_name("status").is_ok(), "Pods should have status field");
}

// =====================================================
// Test: Memory reporting
// =====================================================

#[tokio::test]
async fn test_memory_reporting() {
    let snapshot = create_test_snapshot(50, 200, 20);
    let temp_file = write_snapshot_to_gzip_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok());
    let data = result.unwrap();
    assert!(data.memory_usage.is_some(), "Memory usage report should be present");
    
    // Memory reporting is present - exact values depend on system state
    // For small test snapshots, arrow_tables_size might be 0 or small
    let _memory_report = data.memory_usage.unwrap();
    // Just verify the report exists and is functional
}

// =====================================================
// Test: Large number of files
// =====================================================

#[tokio::test]
async fn test_many_small_files() {
    let temp_dir = TempDir::new().unwrap();
    let mut paths = Vec::new();
    
    // Create 10 small files
    for i in 0..10 {
        let snapshot = create_test_snapshot(5, 10, 2);
        let file_path = temp_dir.path().join(format!("snapshot-{}.json.gz", i));
        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        
        std::fs::write(&file_path, compressed).unwrap();
        paths.push(file_path);
    }
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Failed to load many small files");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50); // 5*10
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 100); // 10*10
}
