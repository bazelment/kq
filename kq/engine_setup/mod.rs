//! Engine setup module
//! Handles loading snapshots and preparing query engines

use anyhow::Result;
use kq_loader::{LoaderConfig, SnapshotLoader};
use kq_query::QueryEngine;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::info;
use std::io::IsTerminal;
use std::path::PathBuf;

/// Configuration for engine setup
pub struct EngineSetupConfig {
    pub loader_config: LoaderConfig,
    pub show_memory_report: bool,
}

impl Default for EngineSetupConfig {
    fn default() -> Self {
        Self {
            loader_config: LoaderConfig::default(),
            show_memory_report: false,
        }
    }
}

/// Load snapshots and prepare query engine
/// This function handles loading snapshot files and creating a QueryEngine ready for queries
pub async fn load_snapshots_and_prepare_engine(
    snapshot_paths: &[PathBuf],
    config: &EngineSetupConfig,
) -> Result<QueryEngine> {
    info!("Loading snapshots: {:?}", snapshot_paths);
    let loader = SnapshotLoader::with_config(config.loader_config.clone());
    let snapshot_data = loader.load_and_combine(snapshot_paths).await?;

    // Display memory report if enabled
    if config.show_memory_report {
        if let Some(ref memory_report) = snapshot_data.memory_usage {
            println!();
            memory_report.display();
        }
    }

    // Register tables with query engine
    // Skip progress bar in non-terminal mode for better performance
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
        pb.set_message("Registering tables with query engine...");
        Some(pb)
    } else {
        info!("Registering tables with query engine...");
        None
    };
    
    let register_start = std::time::Instant::now();
    let engine = QueryEngine::new(snapshot_data).await?;
    let register_duration = register_start.elapsed();
    
    if let Some(pb) = preparing_pb {
        pb.finish_with_message(format!(
            "✓ Registered {} tables ({:.1}s)", 
            engine.table_count(), 
            register_duration.as_secs_f64()
        ));
    } else {
        info!("✓ Registered {} tables ({:.1}s)", 
            engine.table_count(), 
            register_duration.as_secs_f64());
    }
    
    Ok(engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kq_schema::kubernetes::*;
    use chrono::Utc;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Create a test snapshot with configurable size
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
                        "node.kq.dev/pool": if i % 10 == 0 { "gpu" } else { "general" }
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
                    },
                    "phase": "Ready"
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

    #[tokio::test]
    async fn test_load_snapshots_and_prepare_engine() {
        // Create test snapshot with known data
        let snapshot = create_test_snapshot(5, 10, 2);
        
        // Verify original data
        assert_eq!(snapshot.nodes.as_ref().map_or(0, |n| n.len()), 5);
        assert_eq!(snapshot.pods.as_ref().map_or(0, |p| p.len()), 10);
        assert_eq!(snapshot.namespaces.as_ref().map_or(0, |n| n.len()), 2);
        
        // Write to file
        let temp_file = write_snapshot_to_gzip_file(&snapshot);
        
        // Create config
        let config = EngineSetupConfig::default();
        
        // Load snapshots and prepare engine
        let mut engine = load_snapshots_and_prepare_engine(
            &[temp_file.path().to_path_buf()],
            &config
        ).await.unwrap();
        
        // Verify engine has tables
        assert!(engine.table_count() > 0);
        
        // Run queries to verify data is preserved
        // Test 1: Count nodes
        let result1 = engine.execute("SELECT COUNT(*) as node_count FROM nodes").await.unwrap();
        assert_eq!(result1.num_rows(), 1);
        let node_count = result1.column_by_name("node_count").unwrap();
        let node_count_array = node_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(node_count_array.value(0), 5);
        
        // Test 2: Count pods
        let result2 = engine.execute("SELECT COUNT(*) as pod_count FROM pods").await.unwrap();
        assert_eq!(result2.num_rows(), 1);
        let pod_count = result2.column_by_name("pod_count").unwrap();
        let pod_count_array = pod_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(pod_count_array.value(0), 10);
        
        // Test 3: Count namespaces
        let result3 = engine.execute("SELECT COUNT(*) as ns_count FROM namespaces").await.unwrap();
        assert_eq!(result3.num_rows(), 1);
        let ns_count = result3.column_by_name("ns_count").unwrap();
        let ns_count_array = ns_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(ns_count_array.value(0), 2);
        
        // Test 4: Verify specific node names are preserved (using nested field)
        let result4 = engine.execute("SELECT metadata.name as name FROM nodes WHERE metadata.name = 'node-0'").await.unwrap();
        assert_eq!(result4.num_rows(), 1);
        let name_col = result4.column_by_name("name").unwrap();
        let name_array = name_col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(name_array.value(0), "node-0");
        
        // Test 5: Verify pod namespaces are preserved (using nested field)
        let result5 = engine.execute("SELECT DISTINCT metadata.namespace as namespace FROM pods ORDER BY namespace").await.unwrap();
        assert_eq!(result5.num_rows(), 2);
        let ns_col = result5.column_by_name("namespace").unwrap();
        let ns_array = ns_col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(ns_array.value(0), "namespace-0");
        assert_eq!(ns_array.value(1), "namespace-1");
        
        // Test 6: Verify pod phase is preserved (using nested field)
        let result6 = engine.execute("SELECT COUNT(*) as running_count FROM pods WHERE status.phase = 'Running'").await.unwrap();
        assert_eq!(result6.num_rows(), 1);
        let running_count = result6.column_by_name("running_count").unwrap();
        let running_count_array = running_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(running_count_array.value(0), 10);
    }

    #[tokio::test]
    async fn test_load_snapshots_and_prepare_engine_multiple_files() {
        // Create two test snapshots
        let snapshot1 = create_test_snapshot(3, 5, 1);
        let snapshot2 = create_test_snapshot(2, 5, 1);
        
        // Write to files
        let temp_file1 = write_snapshot_to_gzip_file(&snapshot1);
        let temp_file2 = write_snapshot_to_gzip_file(&snapshot2);
        
        // Create config
        let config = EngineSetupConfig::default();
        
        // Load snapshots and prepare engine
        let mut engine = load_snapshots_and_prepare_engine(
            &[temp_file1.path().to_path_buf(), temp_file2.path().to_path_buf()],
            &config
        ).await.unwrap();
        
        // Verify combined data: should have 3+2=5 nodes, 5+5=10 pods
        let result = engine.execute("SELECT COUNT(*) as node_count FROM nodes").await.unwrap();
        assert_eq!(result.num_rows(), 1);
        let node_count = result.column_by_name("node_count").unwrap();
        let node_count_array = node_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(node_count_array.value(0), 5);
        
        let result = engine.execute("SELECT COUNT(*) as pod_count FROM pods").await.unwrap();
        assert_eq!(result.num_rows(), 1);
        let pod_count = result.column_by_name("pod_count").unwrap();
        let pod_count_array = pod_count.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(pod_count_array.value(0), 10);
    }
}
