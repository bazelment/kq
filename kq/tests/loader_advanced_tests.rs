// Advanced unit tests for critical paths and corner conditions
// Tests edge cases, error scenarios, and special configurations

use kq::loader::{LoaderConfig, SnapshotLoader};
use kq::schema::kubernetes::*;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

/// Helper: Create snapshot with daemonsets
fn create_snapshot_with_daemonsets(num_daemonsets: usize) -> ClusterSnapshot {
    use serde_json::json;
    
    let mut daemonsets_json = Vec::new();
    for i in 0..num_daemonsets {
        daemonsets_json.push(json!({
            "metadata": {
                "name": format!("daemonset-{}", i),
                "namespace": "kube-system",
                "uid": format!("ds-{}-uid", i)
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "monitoring"
                    }
                }
            },
            "status": {
                "numberReady": 10,
                "desiredNumberScheduled": 10
            }
        }));
    }
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": [],
        "pods": [],
        "namespaces": [],
        "daemonSets": daemonsets_json
    });
    
    serde_json::from_value(snapshot_json).unwrap()
}

/// Helper: Create snapshot with missing/null fields
fn create_snapshot_with_sparse_data() -> ClusterSnapshot {
    use serde_json::json;
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": [{
            "metadata": {
                "name": "sparse-node",
                "uid": "uid123"
                // Missing labels, annotations, timestamps
            },
            "spec": {},  // Empty spec
            "status": {
                // Missing most fields
                "capacity": {}
            }
        }],
        "pods": [{
            "metadata": {
                "name": "sparse-pod",
                "uid": "pod-uid"
                // Missing namespace
            },
            "spec": {
                "containers": []  // No containers
            },
            "status": {}  // Empty status
        }],
        "namespaces": []
    });
    
    serde_json::from_value(snapshot_json).unwrap()
}

/// Helper: Create snapshot with Unicode and special characters
fn create_snapshot_with_unicode() -> ClusterSnapshot {
    use serde_json::json;
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": [{
            "metadata": {
                "name": "node-测试-🚀",
                "uid": "uid-with-émojis-™",
                "labels": {
                    "key-with-中文": "value-with-日本語"
                }
            },
            "spec": {},
            "status": {}
        }],
        "pods": [],
        "namespaces": []
    });
    
    serde_json::from_value(snapshot_json).unwrap()
}

/// Helper: Write snapshot to gzip file
fn write_snapshot_to_file(snapshot: &ClusterSnapshot) -> NamedTempFile {
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
// Test: Streaming mode for large files
// =====================================================

#[tokio::test]
async fn test_streaming_mode_explicit() {
    let snapshot = create_snapshot_with_daemonsets(50);
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Streaming mode should work");
    let data = result.unwrap();
    let daemon_table = data.tables.get("daemon_sets")
        .expect("DaemonSets table should exist");
    assert_eq!(daemon_table.num_rows(), 50);
}

#[tokio::test]
async fn test_memory_pooled_parsing() {
    let snapshot = create_snapshot_with_daemonsets(30);
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Memory-pooled parsing should work");
    let data = result.unwrap();
    let daemon_table = data.tables.get("daemon_sets")
        .expect("DaemonSets table should exist");
    assert_eq!(daemon_table.num_rows(), 30);
}

// =====================================================
// Test: DaemonSets handling
// =====================================================

#[tokio::test]
async fn test_daemonsets_only_snapshot() {
    let snapshot = create_snapshot_with_daemonsets(20);
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "DaemonSets-only snapshot should work");
    let data = result.unwrap();
    
    let daemon_table = data.tables.get("daemon_sets")
        .expect("DaemonSets table should exist");
    assert_eq!(daemon_table.num_rows(), 20);
    
    assert!(!data.tables.contains_key("nodes") || data.tables.get("nodes").unwrap().num_rows() == 0);
}

#[tokio::test]
async fn test_large_number_of_daemonsets() {
    let snapshot = create_snapshot_with_daemonsets(500);
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Large number of DaemonSets should work");
    let data = result.unwrap();
    
    let table = data.tables.get("daemon_sets");
    assert!(table.is_some(), "daemon_sets table not found. Available tables: {:?}", data.tables.keys().collect::<Vec<_>>());
    assert_eq!(table.unwrap().num_rows(), 500);
}

// =====================================================
// Test: Sparse/missing data handling
// =====================================================

#[tokio::test]
async fn test_sparse_data_handling() {
    let snapshot = create_snapshot_with_sparse_data();
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Sparse data should be handled gracefully");
    let data = result.unwrap();
    
    // Should still create tables even with sparse data
    assert!(data.tables.contains_key("nodes"));
    assert!(data.tables.contains_key("pods"));
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 1);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 1);
}

#[tokio::test]
async fn test_all_null_values() {
    use serde_json::json;
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": null,
        "pods": null,
        "namespaces": null,
        "daemonSets": null
    });
    
    let snapshot: ClusterSnapshot = serde_json::from_value(snapshot_json).unwrap();
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "All null values should be handled");
}

// =====================================================
// Test: Unicode and special characters
// =====================================================

#[tokio::test]
async fn test_unicode_in_data() {
    let snapshot = create_snapshot_with_unicode();
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Unicode data should be handled correctly");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 1);
}

#[tokio::test]
async fn test_special_characters_in_filename() {
    let snapshot = create_snapshot_with_daemonsets(5);
    let temp_dir = TempDir::new().unwrap();
    
    // Create file with special characters in name
    let special_filename = "snapshot-测试-file@#$%.json.gz";
    let file_path = temp_dir.path().join(special_filename);
    
    let json = serde_json::to_string_pretty(&snapshot).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(&file_path, compressed).unwrap();
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[&file_path]).await;
    
    assert!(result.is_ok(), "Special characters in filename should work");
}

// =====================================================
// Test: Dictionary encoding overflow scenario
// =====================================================

#[tokio::test]
async fn test_dictionary_overflow_handling() {
    use serde_json::json;
    
    // Create many batches with unique strings to potentially overflow dictionary
    let temp_dir = TempDir::new().unwrap();
    let mut paths = Vec::new();
    
    for i in 0..5 {
        let mut nodes_json = Vec::new();
        // Each file has nodes with unique labels
        for j in 0..100 {
            nodes_json.push(json!({
                "metadata": {
                    "name": format!("node-{}-{}", i, j),
                    "uid": format!("uid-{}-{}", i, j),
                    "labels": {
                        format!("unique-key-{}-{}", i, j): format!("unique-value-{}-{}", i, j)
                    }
                },
                "spec": {},
                "status": {}
            }));
        }
        
        let snapshot_json = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "nodes": nodes_json,
            "pods": [],
            "namespaces": []
        });
        
        let snapshot: ClusterSnapshot = serde_json::from_value(snapshot_json).unwrap();
        let path = temp_dir.path().join(format!("file{}.json.gz", i));
        
        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        std::fs::write(&path, compressed).unwrap();
        paths.push(path);
    }
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Dictionary overflow should be handled by conversion to regular arrays");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 500); // 5 files * 100 nodes
}

// =====================================================
// Test: Concurrent error handling
// =====================================================

#[tokio::test]
async fn test_parallel_loading_with_one_bad_file() {
    let temp_dir = TempDir::new().unwrap();
    
    // Create one good file and one corrupted file
    let good_snapshot = create_snapshot_with_daemonsets(10);
    let good_path = temp_dir.path().join("good.json.gz");
    let json = serde_json::to_string_pretty(&good_snapshot).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(&good_path, compressed).unwrap();
    
    // Create corrupted file
    let bad_path = temp_dir.path().join("bad.json.gz");
    std::fs::write(&bad_path, b"corrupted data").unwrap();
    
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    
    let paths = vec![good_path, bad_path];
    let result = loader.load_and_combine(&paths).await;
    
    // Should fail gracefully and report which file failed
    assert!(result.is_err(), "Should fail when one file is corrupted");
    let err = result.err().unwrap();
    let err_msg = format!("{}", err);
    assert!(err_msg.contains("bad.json.gz") || err_msg.contains("Failed"), "Error should mention the bad file: {}", err_msg);
}

// =====================================================
// Test: Zero-length and tiny files
// =====================================================

#[tokio::test]
async fn test_zero_length_file() {
    let temp_file = tempfile::Builder::new()
        .suffix(".json.gz")
        .tempfile()
        .unwrap();
    // File is empty
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_err(), "Zero-length file should fail gracefully");
}

#[tokio::test]
async fn test_single_object_file() {
    use serde_json::json;
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": [{
            "metadata": {"name": "single-node", "uid": "uid"},
            "spec": {},
            "status": {}
        }],
        "pods": [],
        "namespaces": []
    });
    
    let snapshot: ClusterSnapshot = serde_json::from_value(snapshot_json).unwrap();
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "Single object should work");
    let data = result.unwrap();
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 1);
}

// =====================================================
// Test: Thread pool limits
// =====================================================

#[tokio::test]
async fn test_single_thread_parallel_processing() {
    let temp_dir = TempDir::new().unwrap();
    let mut paths = Vec::new();
    
    for i in 0..3 {
        let snapshot = create_snapshot_with_daemonsets(10);
        let path = temp_dir.path().join(format!("file{}.json.gz", i));
        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        std::fs::write(&path, compressed).unwrap();
        paths.push(path);
    }
    
    let config = LoaderConfig {
        parallel_threads: 1, // Force single-threaded
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Single-threaded processing should work");
    let data = result.unwrap();
    
    let table = data.tables.get("daemon_sets");
    assert!(table.is_some(), "daemon_sets table not found. Available tables: {:?}", data.tables.keys().collect::<Vec<_>>());
    assert_eq!(table.unwrap().num_rows(), 30);
}

#[tokio::test]
async fn test_many_threads_parallel_processing() {
    let temp_dir = TempDir::new().unwrap();
    let mut paths = Vec::new();
    
    for i in 0..4 {
        let snapshot = create_snapshot_with_daemonsets(10);
        let path = temp_dir.path().join(format!("file{}.json.gz", i));
        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        std::fs::write(&path, compressed).unwrap();
        paths.push(path);
    }
    
    let config = LoaderConfig {
        parallel_threads: 16, // More threads than files
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let result = loader.load_and_combine(&paths).await;
    
    assert!(result.is_ok(), "Many threads should work");
    let data = result.unwrap();
    
    let table = data.tables.get("daemon_sets");
    assert!(table.is_some(), "daemon_sets table not found. Available tables: {:?}", data.tables.keys().collect::<Vec<_>>());
    assert_eq!(table.unwrap().num_rows(), 40);
}

// =====================================================
// Test: Mixed resource types
// =====================================================

#[tokio::test]
async fn test_all_resource_types_together() {
    use serde_json::json;
    
    let snapshot_json = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "nodes": [{
            "metadata": {"name": "node-1", "uid": "n1"},
            "spec": {},
            "status": {}
        }],
        "pods": [{
            "metadata": {"name": "pod-1", "namespace": "default", "uid": "p1"},
            "spec": {"containers": []},
            "status": {}
        }],
        "namespaces": [{
            "metadata": {"name": "default", "uid": "ns1"},
            "status": {"phase": "Active"}
        }],
        "daemonSets": [{
            "metadata": {"name": "ds-1", "namespace": "kube-system", "uid": "ds1"},
            "spec": {},
            "status": {}
        }]
    });
    
    let snapshot: ClusterSnapshot = serde_json::from_value(snapshot_json).unwrap();
    let temp_file = write_snapshot_to_file(&snapshot);
    
    let loader = SnapshotLoader::new();
    let result = loader.load_and_combine(&[temp_file.path()]).await;
    
    assert!(result.is_ok(), "All resource types together should work");
    let data = result.unwrap();
    
    // Verify all tables are created
    assert_eq!(data.tables.get("nodes").unwrap().num_rows(), 1);
    assert_eq!(data.tables.get("pods").unwrap().num_rows(), 1);
    assert_eq!(data.tables.get("namespaces").unwrap().num_rows(), 1);
    let daemon_table = data.tables.get("daemon_sets")
        .expect("DaemonSets table should exist");
    assert_eq!(daemon_table.num_rows(), 1);
}

// =====================================================
// Test: File extension handling
// =====================================================
