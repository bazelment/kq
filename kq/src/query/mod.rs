use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use tracing::{debug, info};

use kq_loader::{concat_record_batches, SnapshotData};
use kq_schema::{SchemaInfo, TableInfo};

pub mod functions;

/// Query execution statistics
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub rows_returned: usize,
    pub execution_time_ms: u64,
}

/// Query engine for executing SQL queries on snapshot data
pub struct QueryEngine {
    ctx: SessionContext,
    snapshot_data: SnapshotData,
    last_execution_stats: Option<ExecutionStats>,
}

impl QueryEngine {
    /// Create a new query engine with snapshot data
    pub async fn new(snapshot_data: SnapshotData) -> Result<Self> {
        use std::time::Instant;
        
        let start = Instant::now();
        debug!("Creating SessionContext...");
        // Enable `information_schema` so REPL discovery commands (.tables,
        // .columns) and `SELECT ... FROM information_schema.*` queries work.
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );
        debug!("SessionContext created in {:.3}s", start.elapsed().as_secs_f64());
        
        let start = Instant::now();
        debug!("Creating QueryEngine struct (moving {} tables)...", snapshot_data.tables.len());
        let mut engine = Self {
            ctx,
            snapshot_data,
            last_execution_stats: None,
        };
        debug!("QueryEngine struct created in {:.3}s", start.elapsed().as_secs_f64());

        let start = Instant::now();
        debug!("Registering tables...");
        engine.register_tables().await?;
        debug!("Tables registered in {:.3}s", start.elapsed().as_secs_f64());
        
        let start = Instant::now();
        debug!("Registering custom functions...");
        engine.register_custom_functions()?;
        debug!("Custom functions registered in {:.3}s", start.elapsed().as_secs_f64());

        Ok(engine)
    }

    /// Execute a SQL query and return results
    /// This supports all DataFusion SQL including:
    /// - SELECT queries
    /// - EXPLAIN, EXPLAIN VERBOSE, EXPLAIN ANALYZE
    /// - DDL: CREATE VIEW, DROP VIEW, DESCRIBE
    /// - Configuration: SET, SHOW ALL
    /// - Information schema queries
    pub async fn execute(&mut self, query: &str) -> Result<RecordBatch> {
        use std::time::Instant;

        let started = Instant::now();
        info!("Executing query: {}", query);
        debug!("Query: {}", query);

        let df = self.ctx
            .sql(query)
            .await
            .map_err(|e| {
                // Format the DataFusion error with more context
                let error_msg = format!("{}", e);
                if error_msg.contains("SQL error") || error_msg.contains("ParserError") {
                    anyhow::anyhow!("SQL Parse Error: {}\n\nQuery: {}", error_msg, query)
                } else if error_msg.contains("Schema error") {
                    anyhow::anyhow!("Schema Error: {}\n\nCheck table and column names with information_schema.tables", error_msg)
                } else {
                    anyhow::anyhow!("Query Error: {}", error_msg)
                }
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| {
                // Format execution errors with more context
                let error_msg = format!("{}", e);
                if error_msg.contains("type") || error_msg.contains("cast") {
                    anyhow::anyhow!("Type Error: {}\n\nCheck data types with DESCRIBE <table>", error_msg)
                } else {
                    anyhow::anyhow!("Execution Error: {}", error_msg)
                }
            })?;

        let result = if batches.is_empty() {
            let schema = self.snapshot_data.list_tables().first()
                .and_then(|table| self.snapshot_data.table_schema(table))
                .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
            
            RecordBatch::new_empty(schema)
        } else if batches.len() == 1 {
            batches
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing query result batch"))?
        } else {
            let schema = batches[0].schema();
            concat_record_batches(schema, batches)
                .context("Failed to concatenate result batches")?
        };

        self.last_execution_stats = Some(ExecutionStats {
            rows_returned: result.num_rows(),
            execution_time_ms: started.elapsed().as_millis() as u64,
        });

        Ok(result)
    }

    /// Get query execution plan (for EXPLAIN queries)
    pub async fn explain(&self, query: &str) -> Result<String> {
        info!("Explaining query: {}", query);

        let df = self.ctx
            .sql(query)
            .await
            .map_err(|e| {
                let error_msg = format!("{}", e);
                if error_msg.contains("SQL error") || error_msg.contains("ParserError") {
                    anyhow::anyhow!("SQL Parse Error: {}", error_msg)
                } else {
                    anyhow::anyhow!("Query Error: {}", error_msg)
                }
            })?;

        let plan = df.explain(false, false)?;
        let batches = plan.collect().await?;

        if batches.is_empty() {
            return Ok("No execution plan available".to_string());
        }

        // Convert the explain output to a string
        let mut result = String::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                for col_idx in 0..batch.num_columns() {
                    let array = batch.column(col_idx);
                    if let Some(string_array) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
                        if let Some(value) = string_array.value(row_idx).strip_prefix("plan_type:") {
                            result.push_str(value);
                        } else {
                            result.push_str(string_array.value(row_idx));
                        }
                        result.push('\n');
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get schema information for tables
    pub fn describe_schema(&self, table_name: Option<&str>) -> Result<SchemaInfo> {
        let mut tables = Vec::new();

        match table_name {
            Some(name) => {
                if let Some(schema) = self.snapshot_data.table_schema(name) {
                    tables.push(TableInfo {
                        name: name.to_string(),
                        schema,
                        row_count: self.snapshot_data.table_row_count(name),
                        description: self.get_table_description(name),
                    });
                } else {
                    return Err(anyhow::anyhow!("Table '{}' not found", name));
                }
            }
            None => {
                for table_name in self.snapshot_data.list_tables() {
                    if let Some(schema) = self.snapshot_data.table_schema(&table_name) {
                        tables.push(TableInfo {
                            name: table_name.clone(),
                            schema,
                            row_count: self.snapshot_data.table_row_count(&table_name),
                            description: self.get_table_description(&table_name),
                        });
                    }
                }
            }
        }

        Ok(SchemaInfo { tables })
    }

    pub fn get_execution_stats(&self) -> Option<ExecutionStats> {
        self.last_execution_stats.clone()
    }

    /// Register all tables from snapshot data
    async fn register_tables(&mut self) -> Result<()> {
        use std::time::Instant;
        
        let table_names = self.snapshot_data.list_tables();
        debug!("Starting table registration for {} tables", table_names.len());
        let overall_start = Instant::now();
        
        let mut total_clone_time = 0.0;
        let mut total_memtable_time = 0.0;
        let mut total_register_time = 0.0;
        let mut total_view_prep_time = 0.0;
        let mut total_view_exec_time = 0.0;
        
        for (table_idx, table_name) in table_names.iter().enumerate() {
            let batches = self.snapshot_data.get_table_batches(table_name)
                .ok_or_else(|| anyhow::anyhow!("Table '{}' has no record batches", table_name))?;
            let schema = self.snapshot_data.table_schema(table_name)
                .ok_or_else(|| anyhow::anyhow!("Table '{}' has no schema", table_name))?;
            let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();

            debug!("Processing table {}/{}: {} ({} rows, {} columns)", 
                      table_idx + 1, 
                      table_names.len(),
                      table_name, 
                      row_count,
                      schema.fields().len());
            
            // Time: Clone operation (shallow clone - Arc references)
            let t0 = Instant::now();
            let partitions: Vec<Vec<RecordBatch>> = batches
                .iter()
                .map(|batch| vec![batch.clone()])
                .collect();
            let clone_time = t0.elapsed().as_secs_f64();
            total_clone_time += clone_time;
            debug!("  - Clone: {:.3}s", clone_time);
            
            // Time: MemTable creation
            let t1 = Instant::now();
            let full_table_name = format!("_full_{}", table_name);
            let full_provider = Arc::new(
                datafusion::datasource::memory::MemTable::try_new(
                    schema.clone(),
                    partitions
                )?
            );
            let memtable_time = t1.elapsed().as_secs_f64();
            total_memtable_time += memtable_time;
            debug!("  - MemTable creation: {:.3}s", memtable_time);
            
            // Time: Table registration
            let t2 = Instant::now();
            self.ctx.register_table(&full_table_name, full_provider)?;
            let register_time = t2.elapsed().as_secs_f64();
            total_register_time += register_time;
            debug!("  - Registration: {:.3}s", register_time);
            
            // Time: View creation (split into prep and execution)
            let t3_prep = Instant::now();
            let field_names: Vec<String> = schema.fields()
                .iter()
                .filter(|field| field.name() != ".json")
                .map(|field| format!("\"{}\"", field.name()))
                .collect();
            
            let view_prep_time = t3_prep.elapsed().as_secs_f64();
            total_view_prep_time += view_prep_time;
            debug!("  - View SQL prep: {:.3}s", view_prep_time);
            
            let mut view_exec_time = 0.0;
            if !field_names.is_empty() {
                let view_sql = format!(
                    "CREATE VIEW {} AS SELECT {} FROM {}",
                    table_name,
                    field_names.join(", "),
                    full_table_name
                );
                
                debug!("Creating view: {}", view_sql);
                let t3_exec = Instant::now();
                // Use create_logical_plan and register_table_as_view if available, 
                // but for now we'll optimize the SQL execution
                self.ctx.sql(&view_sql).await?;
                view_exec_time = t3_exec.elapsed().as_secs_f64();
                total_view_exec_time += view_exec_time;
                debug!("  - View SQL exec: {:.3}s", view_exec_time);
            }
            
            let table_total = clone_time + memtable_time + register_time + view_prep_time + view_exec_time;
            debug!("  Table total: {:.3}s", table_total);
        }
        
        let overall_time = overall_start.elapsed().as_secs_f64();
        debug!("Registration complete:");
        debug!("  Total clone time:    {:.3}s ({:.1}%)", 
                  total_clone_time, 
                  total_clone_time / overall_time * 100.0);
        debug!("  Total MemTable time: {:.3}s ({:.1}%)", 
                  total_memtable_time,
                  total_memtable_time / overall_time * 100.0);
        debug!("  Total register time: {:.3}s ({:.1}%)", 
                  total_register_time,
                  total_register_time / overall_time * 100.0);
        debug!("  Total view prep time: {:.3}s ({:.1}%)", 
                  total_view_prep_time,
                  total_view_prep_time / overall_time * 100.0);
        debug!("  Total view exec time: {:.3}s ({:.1}%)", 
                  total_view_exec_time,
                  total_view_exec_time / overall_time * 100.0);
        debug!("  Overall time:        {:.3}s", overall_time);

        info!("Registered {} tables with hidden .json columns", table_names.len());
        Ok(())
    }

    /// Register custom Kubernetes-aware functions
    fn register_custom_functions(&mut self) -> Result<()> {
        // Register Kubernetes-specific UDFs
        functions::register_kubernetes_functions(&self.ctx)?;
        
        debug!("Custom Kubernetes functions registered: regexp_extract, extract_pool, json_extract_str");
        Ok(())
    }

    /// Get the number of tables registered
    pub fn table_count(&self) -> usize {
        self.snapshot_data.list_tables().len()
    }

    /// Get description for a table
    fn get_table_description(&self, table_name: &str) -> String {
        match table_name {
            "pods" => "Kubernetes pods with metadata, spec, and status information".to_string(),
            "nodes" => "Kubernetes nodes with capacity, allocatable resources, and conditions".to_string(),
            "namespaces" => "Kubernetes namespaces with metadata and status".to_string(),
            "daemon_sets" => "Kubernetes DaemonSets with replica status information".to_string(),
            _ => format!("Kubernetes resource table: {}", table_name),
        }
    }

    pub fn get_tables_for_memory_analysis(&self) -> Result<std::collections::HashMap<String, arrow::record_batch::RecordBatch>> {
        let mut tables = std::collections::HashMap::new();

        for table_name in self.snapshot_data.list_tables() {
            if let Some(batch) = self.snapshot_data.get_table(&table_name) {
                tables.insert(table_name, batch.clone());
                continue;
            }

            if let Some(batches) = self.snapshot_data.get_table_batches(&table_name) {
                if let Some(schema) = self.snapshot_data.table_schema(&table_name) {
                    tables.insert(
                        table_name,
                        concat_record_batches(schema, batches.to_vec())
                            .context("Failed to concatenate table batches for memory analysis")?,
                    );
                }
            }
        }

        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use kq_loader::SnapshotLoader;
    use kq_schema::kubernetes::*;
    use chrono::Utc;
    use std::collections::HashMap;

    async fn create_test_engine() -> QueryEngine {
        use serde_json::json;
        use std::io::Write;
        
        // Create test snapshot using JSON deserialization (simpler and more robust)
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
            "pods": [{
                "metadata": {
                    "name": "test-pod",
                    "namespace": "default",
                    "uid": "pod-uid",
                    "labels": {
                        "app": "test"
                    }
                },
                "spec": {
                    "nodeName": "test-node",
                    "restartPolicy": "Always",
                    "containers": [{
                        "name": "test-container",
                        "image": "nginx:latest",
                        "resources": {
                            "requests": {
                                "cpu": "100m",
                                "memory": "128Mi"
                            },
                            "limits": {
                                "cpu": "200m",
                                "memory": "256Mi"
                            }
                        }
                    }]
                },
                "status": {
                    "phase": "Running"
                }
            }]
        });

        // Write to temp file with .json extension (so loader doesn't try to decompress)
        let mut temp_file = tempfile::Builder::new()
            .suffix(".json")
            .tempfile()
            .unwrap();
        temp_file.write_all(snapshot_json.to_string().as_bytes()).unwrap();
        
        let loader = SnapshotLoader::new();
        let snapshot_data = loader.load_and_combine(&[temp_file.path()]).await.unwrap();

        QueryEngine::new(snapshot_data).await.unwrap()
    }

    #[tokio::test]
    async fn test_query_engine_creation() {
        let engine = create_test_engine().await;
        let schema_info = engine.describe_schema(None).unwrap();
        
        assert!(!schema_info.tables.is_empty());
        assert!(schema_info.tables.iter().any(|t| t.name == "nodes"));
        assert!(schema_info.tables.iter().any(|t| t.name == "pods"));
        assert!(schema_info.tables.iter().any(|t| t.name == "namespaces"));
    }

    #[tokio::test]
    async fn test_simple_query() {
        let mut engine = create_test_engine().await;
        
        // Query using available columns (metadata is nested now)
        let result = engine.execute("SELECT pool FROM nodes").await;
        if let Err(e) = &result {
            eprintln!("Query error: {:?}", e);
        }
        assert!(result.is_ok());
        
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    #[tokio::test]
    async fn test_count_query() {
        let mut engine = create_test_engine().await;
        
        let result = engine.execute("SELECT COUNT(*) as node_count FROM nodes").await;
        assert!(result.is_ok());
        
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let stats = engine.get_execution_stats().expect("execution stats should be recorded");
        assert_eq!(stats.rows_returned, 1);
    }

    #[tokio::test]
    async fn test_join_query() {
        let mut engine = create_test_engine().await;
        
        // Simple cross join to test join functionality (use available flat columns)
        let result = engine.execute(
            "SELECT n.pool, ns.app
             FROM nodes n, namespaces ns"
        ).await;
        
        if let Err(e) = &result {
            eprintln!("Join query error: {:?}", e);
        }
        assert!(result.is_ok());
        let batch = result.unwrap();
        // Cross join of 1 node and 1 namespace = 1 row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_explain_query() {
        let engine = create_test_engine().await;
        
        let result = engine.explain("SELECT pool FROM nodes").await;
        assert!(result.is_ok());
        
        let plan = result.unwrap();
        assert!(!plan.is_empty());
        assert!(plan.contains("nodes") || plan.contains("Projection"));
    }

    #[tokio::test]
    async fn test_describe_schema() {
        let engine = create_test_engine().await;
        
        // Test describing all tables
        let schema_info = engine.describe_schema(None).unwrap();
        assert!(!schema_info.tables.is_empty());
        
        // Test describing specific table
        let nodes_schema = engine.describe_schema(Some("nodes")).unwrap();
        assert_eq!(nodes_schema.tables.len(), 1);
        assert_eq!(nodes_schema.tables[0].name, "nodes");
        
        // Test non-existent table
        let result = engine.describe_schema(Some("nonexistent"));
        assert!(result.is_err());
    }



    #[tokio::test]
    async fn test_empty_snapshot_data() {
        let empty_snapshot = SnapshotData {
            snapshot: ClusterSnapshot {
                timestamp: Utc::now(),
                nodes: Some(vec![]),
                namespaces: Some(vec![]),
                daemon_sets: Some(vec![]),
                pods: Some(vec![]),
            },
            tables: HashMap::new(),
            table_batches: HashMap::new(),
            memory_usage: None,
        };
        
        let engine = QueryEngine::new(empty_snapshot).await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_memory_analysis_tables_are_exposed() {
        let engine = create_test_engine().await;
        let tables = engine.get_tables_for_memory_analysis().unwrap();

        assert!(tables.contains_key("nodes"));
        assert!(tables.contains_key("pods"));
    }

    #[tokio::test]
    async fn test_resource_request_udfs_on_nested_containers() {
        let mut engine = create_test_engine().await;
        let result = engine
            .execute(
                "SELECT
                    total_cpu_request(spec.containers) AS cpu,
                    total_memory_request(spec.containers) AS memory
                 FROM pods"
            )
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        let cpu = result
            .column_by_name("cpu")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let memory = result
            .column_by_name("memory")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        assert_eq!(cpu.value(0), 100);
        assert_eq!(memory.value(0), 128 * 1024 * 1024);
    }

    /// Build a query engine over a snapshot whose pods carry caller-controlled
    /// `(name, uid)` pairs. Used by UDF tests to drive string UDFs against
    /// array columns (the UDFs reject scalar literal arguments).
    /// `cpu_request_total` is set to 2 on every pod so tests that need an
    /// Int64 column (e.g. regexp group index) have a column reference to use.
    async fn engine_with_pod_pairs(pairs: &[(&str, &str)]) -> QueryEngine {
        use serde_json::json;
        use std::io::Write;

        let pods: Vec<serde_json::Value> = pairs
            .iter()
            .map(|(name, uid)| {
                json!({
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "uid": uid
                    },
                    "spec": {
                        "nodeName": "n",
                        "containers": [{
                            "name": "c",
                            "image": "i",
                            "resources": {
                                "requests": { "cpu": "10m", "memory": "32Mi" }
                            }
                        }]
                    },
                    "status": { "phase": "Running" },
                    "cpu_request_total": 2i64
                })
            })
            .collect();

        let snapshot_json = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "nodes": [{
                "metadata": { "name": "n", "uid": "n-uid" },
                "spec": { "podCIDR": "10.0.0.0/24" },
                "status": {
                    "capacity": { "cpu": "1", "memory": "1Gi", "pods": "10" },
                    "allocatable": { "cpu": "1", "memory": "1Gi", "pods": "10" },
                    "phase": "Ready"
                }
            }],
            "namespaces": [{
                "metadata": { "name": "default", "uid": "ns-uid" },
                "status": { "phase": "Active" }
            }],
            "daemonSets": [],
            "pods": pods
        });

        let mut temp_file = tempfile::Builder::new()
            .suffix(".json")
            .tempfile()
            .unwrap();
        temp_file.write_all(snapshot_json.to_string().as_bytes()).unwrap();

        let snapshot_data = SnapshotLoader::new()
            .load_and_combine(&[temp_file.path()])
            .await
            .unwrap();
        QueryEngine::new(snapshot_data).await.unwrap()
    }

    /// Convenience helper for tests where the uid is unused.
    async fn engine_with_pod_names(pod_names: &[&str]) -> QueryEngine {
        let pairs: Vec<(&str, &str)> = pod_names
            .iter()
            .enumerate()
            .map(|(i, name)| (*name, ["uid-0", "uid-1", "uid-2", "uid-3"][i]))
            .collect();
        engine_with_pod_pairs(&pairs).await
    }

    fn int64_column<'a>(
        batch: &'a arrow_array::RecordBatch,
        name: &str,
    ) -> &'a arrow_array::Int64Array {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Int64"))
    }

    fn string_column<'a>(
        batch: &'a arrow_array::RecordBatch,
        name: &str,
    ) -> &'a arrow_array::StringArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not Utf8"))
    }

    fn bool_column<'a>(
        batch: &'a arrow_array::RecordBatch,
        name: &str,
    ) -> &'a arrow_array::BooleanArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap_or_else(|| panic!("column {name} is not Boolean"))
    }

    #[tokio::test]
    async fn udf_parse_cpu_returns_millicores_for_each_format() {
        // Drives parse_cpu() through SQL against an array column (pod name).
        let mut engine = engine_with_pod_names(&["500m", "1", "0.5"]).await;
        let result = engine
            .execute(
                "SELECT metadata.name AS s, parse_cpu(metadata.name) AS millicores
                 FROM pods
                 ORDER BY metadata.name",
            )
            .await
            .unwrap();

        // Sorted lexicographically: "0.5", "1", "500m".
        let s = string_column(&result, "s");
        let millicores = int64_column(&result, "millicores");
        assert_eq!(s.value(0), "0.5");
        assert_eq!(millicores.value(0), 500);
        assert_eq!(s.value(1), "1");
        assert_eq!(millicores.value(1), 1000);
        assert_eq!(s.value(2), "500m");
        assert_eq!(millicores.value(2), 500);
    }

    #[tokio::test]
    async fn udf_parse_memory_returns_bytes_for_each_unit() {
        let mut engine = engine_with_pod_names(&["1Gi", "512Mi", "128172060Ki"]).await;
        let result = engine
            .execute(
                "SELECT metadata.name AS s, parse_memory(metadata.name) AS bytes
                 FROM pods
                 ORDER BY metadata.name",
            )
            .await
            .unwrap();

        let s = string_column(&result, "s");
        let bytes = int64_column(&result, "bytes");

        // Verify each row by matching on the string value (sort order is
        // lexicographic and irrelevant to the contract being tested).
        let pairs: Vec<(String, i64)> = (0..result.num_rows())
            .map(|i| (s.value(i).to_string(), bytes.value(i)))
            .collect();
        assert!(pairs.contains(&("1Gi".to_string(), 1024 * 1024 * 1024)));
        assert!(pairs.contains(&("512Mi".to_string(), 512 * 1024 * 1024)));
        assert!(pairs.contains(&("128172060Ki".to_string(), 128_172_060 * 1024)));
    }

    #[tokio::test]
    async fn udf_extract_pool_pulls_node_pool_from_annotation_text() {
        // Synthetic-style annotation strings of the form
        // `scheduler.kq.dev/node-selector: "node.kq.dev/pool=PV"`.
        let mut engine = engine_with_pod_names(&[
            "prefix node.kq.dev/pool=gpu\" extra",
            "no match here",
        ])
        .await;
        let result = engine
            .execute(
                "SELECT metadata.name AS s, extract_pool(metadata.name) AS pool
                 FROM pods
                 ORDER BY metadata.name",
            )
            .await
            .unwrap();

        let s = string_column(&result, "s");
        let pool = string_column(&result, "pool");
        let lookup: std::collections::HashMap<String, Option<String>> = (0..result.num_rows())
            .map(|i| {
                let value = if pool.is_null(i) { None } else { Some(pool.value(i).to_string()) };
                (s.value(i).to_string(), value)
            })
            .collect();

        assert_eq!(
            lookup.get("prefix node.kq.dev/pool=gpu\" extra"),
            Some(&Some("gpu".to_string()))
        );
        assert_eq!(lookup.get("no match here"), Some(&None));
    }

    #[tokio::test]
    async fn udf_regexp_extract_returns_capture_group() {
        // regexp_extract requires ALL args to be array-typed (literals rejected
        // by the UDF). We get three array columns by:
        //   source  = metadata.name
        //   pattern = metadata.uid (constant across rows; UDF reads pattern[0])
        //   group   = cpu_request_total (set to 2 by the fixture)
        let mut engine = engine_with_pod_pairs(&[
            ("worker-abc-123", "([a-z]+)-([a-z]+)"),
            ("router-xyz-9", "([a-z]+)-([a-z]+)"),
        ])
        .await;
        let result = engine
            .execute(
                "SELECT metadata.name AS s,
                        regexp_extract(metadata.name, metadata.uid, cpu_request_total) AS middle
                 FROM pods
                 ORDER BY metadata.name",
            )
            .await
            .unwrap();

        let s = string_column(&result, "s");
        let middle = string_column(&result, "middle");
        let lookup: std::collections::HashMap<String, String> = (0..result.num_rows())
            .map(|i| (s.value(i).to_string(), middle.value(i).to_string()))
            .collect();
        assert_eq!(lookup.get("worker-abc-123"), Some(&"abc".to_string()));
        assert_eq!(lookup.get("router-xyz-9"), Some(&"xyz".to_string()));
    }

    #[tokio::test]
    async fn udf_json_extract_str_returns_named_field() {
        // Same column-vs-scalar story as regexp_extract: pass the key column
        // via `metadata.uid`. The UDF reads key.value(0) so it must be
        // consistent across rows.
        let mut engine = engine_with_pod_pairs(&[
            (r#"{"team":"payments","env":"prod"}"#, "team"),
            (r#"{"team":"search","env":"prod"}"#, "team"),
        ])
        .await;
        let result = engine
            .execute(
                "SELECT metadata.name AS s,
                        json_extract_str(metadata.name, metadata.uid) AS team
                 FROM pods
                 ORDER BY metadata.name",
            )
            .await
            .unwrap();

        let team = string_column(&result, "team");
        let teams: std::collections::HashSet<String> = (0..result.num_rows())
            .map(|i| team.value(i).to_string())
            .collect();
        assert!(teams.contains("payments"));
        assert!(teams.contains("search"));
    }

    #[tokio::test]
    async fn udf_has_sidecar_and_container_count_match_container_list() {
        // The default test engine has exactly one pod with one container, so
        // has_sidecar() should be false and container_count() should be 1.
        let mut engine = create_test_engine().await;
        let result = engine
            .execute(
                "SELECT
                    has_sidecar(spec.containers) AS sidecar,
                    container_count(spec.containers) AS count,
                    container_names(spec.containers) AS names
                 FROM pods",
            )
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(!bool_column(&result, "sidecar").value(0));
        assert_eq!(int64_column(&result, "count").value(0), 1);
        // container_names returns a comma-separated string of container names.
        let names = string_column(&result, "names").value(0);
        assert_eq!(names, "test-container");
    }

    #[tokio::test]
    async fn udf_total_memory_request_sums_container_memory_in_bytes() {
        // Mirrors test_resource_request_udfs_on_nested_containers but isolates
        // total_memory_request specifically — the test_engine has one container
        // requesting 128Mi, so the sum should be exactly 128 * 1024 * 1024.
        let mut engine = create_test_engine().await;
        let result = engine
            .execute(
                "SELECT total_memory_request(spec.containers) AS bytes FROM pods",
            )
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            int64_column(&result, "bytes").value(0),
            128 * 1024 * 1024
        );
    }

    // ---------- View-name pinning ----------

    #[tokio::test]
    async fn view_names_match_documented_user_facing_contract() {
        // The four user-facing SQL views are documented in CLAUDE.md as
        // `pods`, `nodes`, `namespaces`, `daemon_sets`. A rename in either
        // direction (SQL view OR on-disk filename) breaks every saved query.
        // engine_with_pod_pairs includes an empty daemonSets list so all four
        // tables are registered.
        let engine = engine_with_pod_pairs(&[("p", "u")]).await;
        let schema_info = engine.describe_schema(None).unwrap();
        let names: std::collections::HashSet<String> = schema_info
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect();
        for expected in ["pods", "nodes", "namespaces", "daemon_sets"] {
            assert!(
                names.contains(expected),
                "missing user-facing view '{expected}'; got {names:?}"
            );
        }
    }

    #[tokio::test]
    async fn daemon_sets_view_is_queryable_from_daemonsets_file() {
        // Regression test for the snake-case/no-underscore gotcha:
        // on-disk file is daemonsets.* but the SQL view is daemon_sets.
        let mut engine = engine_with_pod_pairs(&[("p", "u")]).await;
        let result = engine
            .execute("SELECT COUNT(*) AS c FROM daemon_sets")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(int64_column(&result, "c").value(0), 0);
    }
}
