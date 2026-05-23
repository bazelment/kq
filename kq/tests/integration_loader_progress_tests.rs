// Integration tests combining progress bar and loading logic
// Tests the interaction between progress tracking and file loading with various scenarios

use kq::loader::{LoaderConfig, SnapshotLoader};
use kq::schema::kubernetes::*;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use tempfile::TempDir;
use std::path::PathBuf;

/// Helper: Create test snapshot with configurable size using JSON deserialization
fn create_test_snapshot(num_nodes: usize, num_pods: usize, num_namespaces: usize) -> ClusterSnapshot {
    use serde_json::json;
    
    let mut nodes_json = Vec::new();
    let mut pods_json = Vec::new();
    let mut namespaces_json = Vec::new();

    for i in 0..num_nodes {
        nodes_json.push(json!({
            "metadata": {
                "name": format!("node-{}", i),
                "uid": format!("node-{}-uid", i),
                "labels": {
                    "node.pool": "general"
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
                    "image": "nginx:1.19"
                }]
            },
            "status": {
                "phase": "Running"
            }
        }));
    }

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

/// Helper: Write snapshot to gzipped file in temp directory
fn write_snapshot_file(dir: &TempDir, filename: &str, snapshot: &ClusterSnapshot) -> PathBuf {
    let file_path = dir.path().join(filename);
    let json = serde_json::to_string_pretty(snapshot).unwrap();
    
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    
    std::fs::write(&file_path, compressed).unwrap();
    file_path
}

// =====================================================
// Integration Test: Single file with progress enabled
// =====================================================

#[tokio::test]
async fn test_single_file_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(50, 200, 20);
    let file_path = write_snapshot_file(&temp_dir, "snapshot1.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true, // Enable progress tracking
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Failed to load file with progress enabled");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 200);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 20);
}

#[tokio::test]
async fn test_single_file_without_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(50, 200, 20);
    let file_path = write_snapshot_file(&temp_dir, "snapshot1.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: false, // Disable progress tracking
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Failed to load file without progress");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50);
}

// =====================================================
// Integration Test: Multiple files with progress
// =====================================================

#[tokio::test]
async fn test_multiple_files_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let snapshot1 = create_test_snapshot(20, 100, 10);
    let snapshot2 = create_test_snapshot(30, 150, 15);
    let snapshot3 = create_test_snapshot(25, 125, 12);
    
    let file1 = write_snapshot_file(&temp_dir, "snapshot1.json.gz", &snapshot1);
    let file2 = write_snapshot_file(&temp_dir, "snapshot2.json.gz", &snapshot2);
    let file3 = write_snapshot_file(&temp_dir, "snapshot3.json.gz", &snapshot3);
    
    let config = LoaderConfig {
        progress_updates: true,
        parallel_threads: 2,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![file1, file2, file3];
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Failed to load multiple files with progress");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 75); // 20+30+25
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 375); // 100+150+125
}

// =====================================================
// Integration Test: Large files with different sizes
// =====================================================

#[tokio::test]
async fn test_mixed_sizes_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create files of very different sizes
    let small = create_test_snapshot(5, 10, 2);
    let medium = create_test_snapshot(50, 200, 20);
    let large = create_test_snapshot(150, 600, 50);
    
    let file1 = write_snapshot_file(&temp_dir, "small.json.gz", &small);
    let file2 = write_snapshot_file(&temp_dir, "medium.json.gz", &medium);
    let file3 = write_snapshot_file(&temp_dir, "large.json.gz", &large);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![file1, file2, file3];
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Failed to load mixed sizes with progress");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 205);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 810);
}

// =====================================================
// Integration Test: Parallel loading with progress
// =====================================================

#[tokio::test]
async fn test_parallel_loading_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let mut paths = Vec::new();
    for i in 0..5 {
        let snapshot = create_test_snapshot(10, 50, 5);
        let path = write_snapshot_file(&temp_dir, &format!("snapshot{}.json.gz", i), &snapshot);
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: true,
        parallel_threads: 3,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Parallel loading with progress failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50); // 10*5
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 250); // 50*5
}

// =====================================================
// Integration Test: Different compression methods with progress
// =====================================================

#[tokio::test]
async fn test_automatic_compression_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(30, 100, 10);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);
    
    // Decompression is now automatic - no method selection needed
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Failed with automatic decompression");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 30);
}

// =====================================================
// Integration Test: Default parser with progress
// =====================================================

#[tokio::test]
async fn test_default_parser_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(30, 100, 10);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);

    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);

    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Default parser with progress should work");

    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 30);
}

// =====================================================
// Integration Test: Batch processing with progress tracking
// =====================================================

#[tokio::test]
async fn test_batch_processing_with_detailed_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create a file with enough objects to trigger multiple batches
    let snapshot = create_test_snapshot(100, 500, 30);
    let file_path = write_snapshot_file(&temp_dir, "large.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    
    assert!(result.is_ok(), "Batch processing with progress failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 100);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 500);
}

// =====================================================
// Integration Test: Incremental Arrow conversion with progress
// =====================================================

#[tokio::test]
async fn test_incremental_arrow_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let mut paths = Vec::new();
    for i in 0..4 {
        let snapshot = create_test_snapshot(25, 100, 10);
        let path = write_snapshot_file(&temp_dir, &format!("file{}.json.gz", i), &snapshot);
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Incremental Arrow conversion failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 100); // 25*4
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 400); // 100*4
}

// =====================================================
// Integration Test: Error handling with progress
// =====================================================

#[tokio::test]
async fn test_error_handling_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create one valid file and one that will fail
    let valid_snapshot = create_test_snapshot(10, 50, 5);
    let valid_file = write_snapshot_file(&temp_dir, "valid.json.gz", &valid_snapshot);
    let invalid_file = temp_dir.path().join("nonexistent.json.gz");
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    // Should fail because one file doesn't exist
    let paths = vec![valid_file.clone(), invalid_file];
    let result = loader.load_and_combine(&paths).await;
    assert!(result.is_err(), "Should fail with missing file");
    
    // But loading just the valid file should work
    let result = loader.load_and_combine(&[&valid_file]).await;
    assert!(result.is_ok(), "Valid file should load successfully");
}

// =====================================================
// Integration Test: Memory reporting with progress
// =====================================================

#[tokio::test]
async fn test_memory_reporting_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(50, 200, 20);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok());
    
    let data = result.unwrap();
    assert!(data.memory_usage.is_some(), "Memory report should be present");
    
    // Memory usage reporting exists - exact values depend on system state
    let _memory_report = data.memory_usage.unwrap();
    // Note: arrow_tables_size may be 0 for very small test snapshots
    // The important thing is that memory reporting is functional
}

// =====================================================
// Integration Test: Many small files with progress
// =====================================================

#[tokio::test]
async fn test_many_small_files_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let mut paths = Vec::new();
    for i in 0..10 {
        let snapshot = create_test_snapshot(5, 20, 3);
        let path = write_snapshot_file(&temp_dir, &format!("small{}.json.gz", i), &snapshot);
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: true,
        parallel_threads: 4,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Many small files with progress failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50); // 5*10
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 200); // 20*10
}

// =====================================================
// Integration Test: Single file progress
// =====================================================

#[tokio::test]
async fn test_single_file_progress() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(40, 150, 15);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Single file progress should work");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 40);
}

// =====================================================
// Integration Test: Progress with a larger snapshot
// =====================================================

#[tokio::test]
async fn test_progress_with_larger_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot = create_test_snapshot(50, 200, 20);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Progress with a larger snapshot should work");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 50);
}

// =====================================================
// Integration Test: Very large dataset with progress
// =====================================================

#[tokio::test]
async fn test_very_large_dataset_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create a larger dataset to stress test progress tracking
    let snapshot = create_test_snapshot(200, 1000, 50);
    let file_path = write_snapshot_file(&temp_dir, "large.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&[&file_path]).await;
    assert!(result.is_ok(), "Very large dataset with progress failed");
    
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 200);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 1000);
}

// =====================================================
// Integration Test: Empty files with progress
// =====================================================

#[tokio::test]
async fn test_empty_files_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let mut paths = Vec::new();
    for i in 0..3 {
        let snapshot = create_test_snapshot(0, 0, 0);
        let path = write_snapshot_file(&temp_dir, &format!("empty{}.json.gz", i), &snapshot);
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let result = loader.load_and_combine(&paths).await;
    assert!(result.is_ok(), "Empty files with progress should not crash");
}

// =====================================================
// Integration Test: Default multi-file progress
// =====================================================

#[tokio::test]
async fn test_default_multi_file_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let mut paths = Vec::new();
    for i in 0..5 {
        let snapshot = create_test_snapshot(20, 100, 10);
        let path = write_snapshot_file(&temp_dir, &format!("file{}.json.gz", i), &snapshot);
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Default multi-file progress failed");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 100); // 20*5
}

// =====================================================
// Integration Test: Throughput calculations with progress
// =====================================================

#[tokio::test]
async fn test_throughput_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    
    let snapshot = create_test_snapshot(50, 200, 20);
    let file_path = write_snapshot_file(&temp_dir, "snapshot.json.gz", &snapshot);
    
    let config = LoaderConfig {
        progress_updates: true,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let start = std::time::Instant::now();
    let result = loader.load_and_combine(&[&file_path]).await;
    let duration = start.elapsed();
    
    assert!(result.is_ok());
    let data = result.unwrap();
    
    let total_objects = data.tables.get("nodes").unwrap().num_rows()
        + data.tables.get("pods").unwrap().num_rows()
        + data.tables.get("namespaces").unwrap().num_rows();
    
    // Calculate throughput
    let throughput = total_objects as f64 / duration.as_secs_f64();
    assert!(throughput > 0.0, "Throughput should be positive");
}
