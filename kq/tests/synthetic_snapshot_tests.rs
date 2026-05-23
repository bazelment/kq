use arrow_array::{Array, Int64Array, StringArray};
use chrono::Utc;
use kq::loader::{
    write_ipc_directory, write_parquet_directory, LoaderConfig, NdjsonLoader, ParquetLoader,
    SnapshotLoader,
};
use kq::query::QueryEngine;
use kq::synthetic::{generate_ndjson_snapshot, SyntheticSnapshotConfig};
use tempfile::TempDir;

fn test_config(dir: &TempDir, cluster: &str, nodes: usize, seed: u64) -> SyntheticSnapshotConfig {
    SyntheticSnapshotConfig {
        output_dir: dir.path().join(cluster),
        cluster_name: cluster.to_string(),
        node_count: nodes,
        min_pods_per_node: 10,
        max_pods_per_node: 16,
        namespace_count: 18,
        seed,
        overwrite: false,
        timestamp: Utc::now(),
    }
}

async fn load_engine(paths: &[std::path::PathBuf]) -> QueryEngine {
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    let loader = SnapshotLoader::with_config(config);
    let snapshot_data = loader.load_and_combine(paths).await.unwrap();
    QueryEngine::new(snapshot_data).await.unwrap()
}

fn int64_value(batch: &arrow_array::RecordBatch, column: &str, row: usize) -> i64 {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

fn string_value(batch: &arrow_array::RecordBatch, column: &str, row: usize) -> String {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
        .to_string()
}

#[tokio::test]
async fn generated_snapshot_loads_and_supports_representative_queries() {
    let dir = TempDir::new().unwrap();
    let config = test_config(&dir, "synthetic-test-a", 18, 11);
    let summary = generate_ndjson_snapshot(&config).unwrap();

    assert_eq!(summary.node_count, 18);
    assert!(summary.pod_count >= 180);
    assert!(summary.pod_count <= 288);
    assert!(summary.min_pods_per_node >= 10);
    assert!(summary.max_pods_per_node <= 16);
    assert!(summary.running_pods > 0);
    assert!(summary.succeeded_pods + summary.failed_pods + summary.pending_pods > 0);

    let mut engine = load_engine(&[summary.output_dir.clone()]).await;

    let counts = engine
        .execute("SELECT (SELECT COUNT(*) FROM nodes) AS nodes, (SELECT COUNT(*) FROM pods) AS pods, (SELECT COUNT(*) FROM namespaces) AS namespaces")
        .await
        .unwrap();
    assert_eq!(int64_value(&counts, "nodes", 0), summary.node_count as i64);
    assert_eq!(int64_value(&counts, "pods", 0), summary.pod_count as i64);
    assert_eq!(int64_value(&counts, "namespaces", 0), summary.namespace_count as i64);

    let cluster_rollup = engine
        .execute("SELECT metadata.labels['synthetic.kq.dev/cluster'] AS cluster_name, COUNT(*) AS pods FROM pods GROUP BY cluster_name")
        .await
        .unwrap();
    assert_eq!(cluster_rollup.num_rows(), 1);
    assert_eq!(string_value(&cluster_rollup, "cluster_name", 0), summary.cluster_name);
    assert_eq!(int64_value(&cluster_rollup, "pods", 0), summary.pod_count as i64);

    let phase_rollup = engine
        .execute("SELECT status.phase, COUNT(*) AS pods FROM pods GROUP BY status.phase ORDER BY pods DESC")
        .await
        .unwrap();
    assert!(phase_rollup.num_rows() >= 2);

    let node_join = engine
        .execute("SELECT n.metadata.labels['node.kq.dev/pool'] AS node_pool, COUNT(p.metadata.name) AS running_pods FROM pods p JOIN nodes n ON p.spec['nodeName'] = n.metadata.name WHERE p.status.phase = 'Running' GROUP BY n.metadata.labels['node.kq.dev/pool'] ORDER BY running_pods DESC")
        .await
        .unwrap();
    assert!(node_join.num_rows() > 0);
    assert!(int64_value(&node_join, "running_pods", 0) > 0);

    let cpu_by_app = engine
        .execute("SELECT p.app AS app, SUM(p.cpu_request_total) AS cpu_millis FROM pods p WHERE p.cpu_request_total IS NOT NULL GROUP BY p.app ORDER BY cpu_millis DESC LIMIT 10")
        .await
        .unwrap();
    assert!(cpu_by_app.num_rows() > 0);
    assert!(int64_value(&cpu_by_app, "cpu_millis", 0) > 0);

    let fast_pool_rollup = engine
        .execute("SELECT p.pool AS pool, COUNT(*) AS running_pods FROM pods p WHERE p.phase = 'Running' GROUP BY p.pool ORDER BY running_pods DESC")
        .await
        .unwrap();
    assert!(fast_pool_rollup.num_rows() > 0);
    assert!(int64_value(&fast_pool_rollup, "running_pods", 0) > 0);
}

#[tokio::test]
async fn generated_snapshots_support_multi_cluster_rollups() {
    let dir = TempDir::new().unwrap();
    let config_a = test_config(&dir, "synthetic-test-a", 10, 21);
    let config_b = test_config(&dir, "synthetic-test-b", 12, 22);
    let summary_a = generate_ndjson_snapshot(&config_a).unwrap();
    let summary_b = generate_ndjson_snapshot(&config_b).unwrap();

    let mut engine = load_engine(&[summary_a.output_dir.clone(), summary_b.output_dir.clone()]).await;
    let result = engine
        .execute("SELECT cluster, COUNT(*) AS pods FROM pods GROUP BY cluster ORDER BY cluster")
        .await
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    assert_eq!(string_value(&result, "cluster", 0), "synthetic-test-a");
    assert_eq!(string_value(&result, "cluster", 1), "synthetic-test-b");
    assert_eq!(int64_value(&result, "pods", 0), summary_a.pod_count as i64);
    assert_eq!(int64_value(&result, "pods", 1), summary_b.pod_count as i64);
}

#[tokio::test]
async fn generated_snapshot_ipc_roundtrip_preserves_counts_and_queries() {
    let dir = TempDir::new().unwrap();
    let config = test_config(&dir, "synthetic-test-ipc", 14, 31);
    let summary = generate_ndjson_snapshot(&config).unwrap();

    let (timestamp, tables, _) = NdjsonLoader::new()
        .load_directory(&summary.output_dir)
        .unwrap();
    let ipc_dir = dir.path().join("synthetic-test-ipc-arrow");
    write_ipc_directory(&ipc_dir, timestamp, &tables).unwrap();

    let loader = SnapshotLoader::with_config(LoaderConfig {
        progress_updates: false,
        ..Default::default()
    });
    let original = loader.load_and_combine(&[summary.output_dir.clone()]).await.unwrap();
    let converted = loader.load_and_combine(&[ipc_dir.clone()]).await.unwrap();

    for table in ["nodes", "pods", "namespaces", "daemon_sets"] {
        assert_eq!(
            original.table_row_count(table),
            converted.table_row_count(table),
            "row count changed for {table}"
        );
        assert_eq!(
            original.table_schema(table).unwrap().fields(),
            converted.table_schema(table).unwrap().fields(),
            "schema changed for {table}"
        );
    }

    let mut engine = load_engine(&[ipc_dir]).await;
    let counts = engine
        .execute("SELECT (SELECT COUNT(*) FROM nodes) AS nodes, (SELECT COUNT(*) FROM pods) AS pods, (SELECT COUNT(*) FROM namespaces) AS namespaces")
        .await
        .unwrap();
    assert_eq!(int64_value(&counts, "nodes", 0), summary.node_count as i64);
    assert_eq!(int64_value(&counts, "pods", 0), summary.pod_count as i64);
    assert_eq!(int64_value(&counts, "namespaces", 0), summary.namespace_count as i64);
}

#[tokio::test]
async fn generated_snapshot_parquet_roundtrip_preserves_counts_and_queries() {
    let dir = TempDir::new().unwrap();
    let config = test_config(&dir, "synthetic-test-parquet", 14, 32);
    let summary = generate_ndjson_snapshot(&config).unwrap();

    let (timestamp, tables, _) = NdjsonLoader::new()
        .load_directory(&summary.output_dir)
        .unwrap();
    let parquet_dir = dir.path().join("synthetic-test-parquet-tables");
    write_parquet_directory(&parquet_dir, timestamp, &tables).unwrap();

    let loader = SnapshotLoader::with_config(LoaderConfig {
        progress_updates: false,
        ..Default::default()
    });
    let original = loader.load_and_combine(&[summary.output_dir.clone()]).await.unwrap();
    let converted = loader.load_and_combine(&[parquet_dir.clone()]).await.unwrap();

    for table in ["nodes", "pods", "namespaces", "daemon_sets"] {
        assert_eq!(
            original.table_row_count(table),
            converted.table_row_count(table),
            "row count changed for {table}"
        );
        assert_eq!(
            original.table_schema(table).unwrap().fields(),
            converted.table_schema(table).unwrap().fields(),
            "schema changed for {table}"
        );
    }

    let mut engine = load_engine(&[parquet_dir]).await;
    let counts = engine
        .execute("SELECT (SELECT COUNT(*) FROM nodes) AS nodes, (SELECT COUNT(*) FROM pods) AS pods, (SELECT COUNT(*) FROM namespaces) AS namespaces")
        .await
        .unwrap();
    assert_eq!(int64_value(&counts, "nodes", 0), summary.node_count as i64);
    assert_eq!(int64_value(&counts, "pods", 0), summary.pod_count as i64);
    assert_eq!(int64_value(&counts, "namespaces", 0), summary.namespace_count as i64);
}

#[tokio::test]
async fn columnar_multi_snapshot_loads_preserve_source_partitions() {
    let dir = TempDir::new().unwrap();
    let config_a = test_config(&dir, "synthetic-test-columnar-a", 10, 41);
    let config_b = test_config(&dir, "synthetic-test-columnar-b", 12, 42);
    let summary_a = generate_ndjson_snapshot(&config_a).unwrap();
    let summary_b = generate_ndjson_snapshot(&config_b).unwrap();

    let (timestamp_a, tables_a, _) = NdjsonLoader::new()
        .load_directory(&summary_a.output_dir)
        .unwrap();
    let (timestamp_b, tables_b, _) = NdjsonLoader::new()
        .load_directory(&summary_b.output_dir)
        .unwrap();

    let ipc_a = dir.path().join("columnar-a-ipc");
    let ipc_b = dir.path().join("columnar-b-ipc");
    let parquet_a = dir.path().join("columnar-a-parquet");
    let parquet_b = dir.path().join("columnar-b-parquet");
    write_ipc_directory(&ipc_a, timestamp_a.clone(), &tables_a).unwrap();
    write_ipc_directory(&ipc_b, timestamp_b.clone(), &tables_b).unwrap();
    write_parquet_directory(&parquet_a, timestamp_a, &tables_a).unwrap();
    write_parquet_directory(&parquet_b, timestamp_b, &tables_b).unwrap();

    let expected_pods = summary_a.pod_count + summary_b.pod_count;
    for (label, paths) in [
        ("ipc", vec![ipc_a, ipc_b]),
        ("parquet", vec![parquet_a, parquet_b]),
    ] {
        let loader = SnapshotLoader::with_config(LoaderConfig {
            progress_updates: false,
            ..Default::default()
        });
        let snapshot_data = loader.load_and_combine(&paths).await.unwrap();
        let pod_batches = snapshot_data.get_table_batches("pods").unwrap();

        assert_eq!(
            pod_batches.len(),
            2,
            "{label} should keep one pods partition per source snapshot"
        );
        assert_eq!(
            snapshot_data.table_row_count("pods"),
            expected_pods,
            "{label} row count changed"
        );

        let mut engine = QueryEngine::new(snapshot_data).await.unwrap();
        let result = engine
            .execute("SELECT cluster, COUNT(*) AS pods FROM pods GROUP BY cluster ORDER BY cluster")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 2, "{label} cluster rollup changed");
        assert_eq!(int64_value(&result, "pods", 0), summary_a.pod_count as i64);
        assert_eq!(int64_value(&result, "pods", 1), summary_b.pod_count as i64);
    }
}

#[test]
fn parquet_loader_batches_are_not_concatenated_by_reader() {
    let dir = TempDir::new().unwrap();
    let config = test_config(&dir, "synthetic-test-parquet-batches", 16, 51);
    let summary = generate_ndjson_snapshot(&config).unwrap();

    let (timestamp, tables, _) = NdjsonLoader::new()
        .load_directory(&summary.output_dir)
        .unwrap();
    let parquet_dir = dir.path().join("synthetic-test-parquet-batches-tables");
    write_parquet_directory(&parquet_dir, timestamp, &tables).unwrap();

    let (_, table_batches, _) = ParquetLoader::new()
        .with_batch_size(10)
        .load_directory_batches(&parquet_dir)
        .unwrap();
    let pod_batches = table_batches.get("pods").unwrap();

    assert!(
        pod_batches.len() > 1,
        "small Parquet read batch size should produce multiple pods batches"
    );
    assert_eq!(
        pod_batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        summary.pod_count
    );
}
