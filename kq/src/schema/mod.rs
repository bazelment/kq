use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

pub mod kubernetes;
pub mod nested;

/// Schema information for a table
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub schema: Arc<Schema>,
    pub row_count: usize,
    pub description: String,
}

/// Complete schema information for all tables
#[derive(Debug)]
pub struct SchemaInfo {
    pub tables: Vec<TableInfo>,
}

/// Schema registry for managing Arrow schemas for Kubernetes resources
pub struct SchemaRegistry {
    schemas: HashMap<String, Arc<Schema>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            schemas: HashMap::new(),
        };
        registry.register_default_schemas();
        registry
    }

    fn register_default_schemas(&mut self) {
        self.schemas.insert("pods".to_string(), Self::pods_schema());
        self.schemas.insert("nodes".to_string(), Self::nodes_schema());
        self.schemas.insert("namespaces".to_string(), Self::namespaces_schema());
        self.schemas.insert("daemon_sets".to_string(), Self::daemon_sets_schema());
    }

    pub fn get_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(table_name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Schema for pods table
    pub fn pods_schema() -> Arc<Schema> {
        let fields = vec![
            // Metadata - only name is required, everything else nullable
            Field::new("name", DataType::Utf8, true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
            Field::new("creation_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("labels", DataType::Utf8, true), // JSON string for now
            Field::new("annotations", DataType::Utf8, true), // JSON string for now

            // Spec
            Field::new("node_name", DataType::Utf8, true),
            Field::new("restart_policy", DataType::Utf8, true),
            Field::new("service_account", DataType::Utf8, true),
            Field::new("priority", DataType::Int32, true),

            // Status
            Field::new("phase", DataType::Utf8, true),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Millisecond, None), true),

            // Resources (aggregated from containers)
            Field::new("cpu_request", DataType::Int64, true), // millicores
            Field::new("memory_request", DataType::Int64, true), // bytes
            Field::new("cpu_limit", DataType::Int64, true),
            Field::new("memory_limit", DataType::Int64, true),

            // Container info (simplified as JSON strings for now)
            Field::new("container_names", DataType::Utf8, true),
            Field::new("container_images", DataType::Utf8, true),
            Field::new("container_states", DataType::Utf8, true),

            // Node affinity
            Field::new("node_selector", DataType::Utf8, true), // JSON string for now

            // Workload label columns
            Field::new("app", DataType::Utf8, true), // app
            Field::new("instanceDiscriminator", DataType::Utf8, true), // instanceDiscriminator
            Field::new("product", DataType::Utf8, true), // product
            Field::new("productTag", DataType::Utf8, true), // productTag
            Field::new("liStatefulSetName", DataType::Utf8, true), // liStatefulSetName
            Field::new("tenant", DataType::Utf8, true), // tenant.kq.dev/name
            
            // Custom derived fields
            Field::new("pool", DataType::Utf8, true),
            Field::new("workload_type", DataType::Utf8, true),
            
            // Hidden JSON column - contains full object JSON
            Field::new(".json", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Schema for nodes table
    pub fn nodes_schema() -> Arc<Schema> {
        let fields = vec![
            // Metadata - make all fields nullable for robustness
            Field::new("name", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
            Field::new("creation_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("labels", DataType::Utf8, true), // JSON string for now
            Field::new("annotations", DataType::Utf8, true), // JSON string for now

            // Spec
            Field::new("pod_cidr", DataType::Utf8, true),
            Field::new("provider_id", DataType::Utf8, true),
            Field::new("unschedulable", DataType::Boolean, true),

            // Status
            Field::new("phase", DataType::Utf8, true),

            // Capacity
            Field::new("capacity_cpu", DataType::Int64, true), // millicores
            Field::new("capacity_memory", DataType::Int64, true), // bytes
            Field::new("capacity_pods", DataType::Int32, true),
            Field::new("capacity_storage", DataType::Int64, true), // bytes

            // Allocatable
            Field::new("allocatable_cpu", DataType::Int64, true),
            Field::new("allocatable_memory", DataType::Int64, true),
            Field::new("allocatable_pods", DataType::Int32, true),
            Field::new("allocatable_storage", DataType::Int64, true),

            // Node info
            Field::new("architecture", DataType::Utf8, true),
            Field::new("os_image", DataType::Utf8, true),
            Field::new("kernel_version", DataType::Utf8, true),
            Field::new("kubelet_version", DataType::Utf8, true),

            // Workload label columns
            Field::new("cluster", DataType::Utf8, true), // topology.kq.dev/cluster
            Field::new("mz", DataType::Utf8, true), // node.kq.dev/maintenance-zone
            Field::new("node_profile", DataType::Utf8, true), // node.kq.dev/profile
            Field::new("pool", DataType::Utf8, true), // node.kq.dev/pool
            
            // Custom derived fields
            Field::new("zone", DataType::Utf8, true),
            Field::new("instance_type", DataType::Utf8, true),

            // Conditions
            Field::new("ready", DataType::Boolean, true),
            Field::new("disk_pressure", DataType::Boolean, true),
            Field::new("memory_pressure", DataType::Boolean, true),
            Field::new("pid_pressure", DataType::Boolean, true),
            Field::new("network_unavailable", DataType::Boolean, true),
            
            // Hidden JSON column - contains full object JSON
            Field::new(".json", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Schema for namespaces table
    pub fn namespaces_schema() -> Arc<Schema> {
        let fields = vec![
            // All fields nullable for robustness
            Field::new("name", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
            Field::new("creation_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("labels", DataType::Utf8, true), // JSON string for now
            Field::new("annotations", DataType::Utf8, true), // JSON string for now
            Field::new("phase", DataType::Utf8, true),
            
            // Workload label columns
            Field::new("app", DataType::Utf8, true), // app
            Field::new("product", DataType::Utf8, true), // product
            Field::new("productTag", DataType::Utf8, true), // productTag
            Field::new("tenant", DataType::Utf8, true), // tenant.kq.dev/name
            
            // Hidden JSON column - contains full object JSON
            Field::new(".json", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Schema for daemon_sets table
    pub fn daemon_sets_schema() -> Arc<Schema> {
        let fields = vec![
            // All fields nullable for robustness
            Field::new("name", DataType::Utf8, true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
            Field::new("creation_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("labels", DataType::Utf8, true), // JSON string for now
            Field::new("desired_number_scheduled", DataType::Int32, true),
            Field::new("current_number_scheduled", DataType::Int32, true),
            Field::new("number_ready", DataType::Int32, true),
            Field::new("updated_number_scheduled", DataType::Int32, true),
            Field::new("number_available", DataType::Int32, true),
            
            // Hidden JSON column - contains full object JSON
            Field::new(".json", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registry_creation() {
        let registry = SchemaRegistry::new();
        let tables = registry.list_tables();
        
        assert_eq!(tables.len(), 4);
        assert!(tables.contains(&"pods".to_string()));
        assert!(tables.contains(&"nodes".to_string()));
        assert!(tables.contains(&"namespaces".to_string()));
        assert!(tables.contains(&"daemon_sets".to_string()));
    }

    #[test]
    fn test_pods_schema() {
        let schema = SchemaRegistry::pods_schema();
        assert!(!schema.fields().is_empty());
        
        // Check key fields exist
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("namespace").is_ok());
        assert!(schema.field_with_name("phase").is_ok());
        assert!(schema.field_with_name("node_name").is_ok());
        assert!(schema.field_with_name("cpu_request").is_ok());
        assert!(schema.field_with_name("memory_request").is_ok());
    }

    #[test]
    fn test_nodes_schema() {
        let schema = SchemaRegistry::nodes_schema();
        assert!(!schema.fields().is_empty());
        
        // Check key fields exist
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("capacity_cpu").is_ok());
        assert!(schema.field_with_name("allocatable_cpu").is_ok());
        assert!(schema.field_with_name("pool").is_ok());
        assert!(schema.field_with_name("ready").is_ok());
    }

    #[test]
    fn test_get_schema() {
        let registry = SchemaRegistry::new();
        
        let pods_schema = registry.get_schema("pods");
        assert!(pods_schema.is_some());

        let daemon_sets_schema = registry.get_schema("daemon_sets");
        assert!(daemon_sets_schema.is_some());
        
        let nonexistent_schema = registry.get_schema("nonexistent");
        assert!(nonexistent_schema.is_none());
    }

    #[test]
    fn test_schema_registry_tables() {
        let registry = SchemaRegistry::new();
        let tables = registry.list_tables();
        
        assert!(tables.contains(&"pods".to_string()));
        assert!(tables.contains(&"nodes".to_string()));
        assert!(tables.contains(&"namespaces".to_string()));
        assert!(tables.contains(&"daemon_sets".to_string()));
    }

    #[test]
    fn test_pods_schema_structure() {
        let schema = SchemaRegistry::pods_schema();
        let fields = schema.fields();
        
        // Check that key fields exist
        let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"namespace"));
        assert!(field_names.contains(&"uid"));
        assert!(field_names.contains(&"phase"));
        assert!(field_names.contains(&"node_name"));
        assert!(field_names.contains(&"tenant"));
        assert!(field_names.contains(&"pool"));
    }

    #[test]
    fn test_nodes_schema_structure() {
        let schema = SchemaRegistry::nodes_schema();
        let fields = schema.fields();
        
        // Check that key fields exist
        let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"uid"));
        assert!(field_names.contains(&"phase"));
        assert!(field_names.contains(&"capacity_cpu"));
        assert!(field_names.contains(&"capacity_memory"));
        assert!(field_names.contains(&"pool"));
        assert!(field_names.contains(&"zone"));
    }

    #[test]
    fn test_schema_data_types() {
        let pods_schema = SchemaRegistry::pods_schema();
        let fields = pods_schema.fields();
        
        // Check specific field types
        for field in fields {
            match field.name().as_str() {
                "name" | "namespace" | "uid" | "phase" => {
                    assert_eq!(field.data_type(), &DataType::Utf8);
                }
                "creation_timestamp" | "start_time" => {
                    assert!(matches!(field.data_type(), DataType::Timestamp(TimeUnit::Millisecond, None)));
                }
                "priority" => {
                    assert_eq!(field.data_type(), &DataType::Int32);
                }
                "cpu_request" | "memory_request" | "cpu_limit" | "memory_limit" => {
                    assert_eq!(field.data_type(), &DataType::Int64);
                }
                _ => {
                    // Other fields should have valid types
                    assert!(!matches!(field.data_type(), DataType::Null));
                }
            }
        }
    }
}
