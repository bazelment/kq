use arrow_schema::SchemaRef;
use kq_schema::nested::NestedSchemas;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResourceTable {
    pub table_name: &'static str,
    pub json_key: &'static str,
    pub ndjson_file: &'static str,
    pub ipc_file: &'static str,
    pub parquet_file: &'static str,
    schema: fn() -> SchemaRef,
}

impl ResourceTable {
    pub fn schema(self) -> SchemaRef {
        (self.schema)()
    }
}

pub(crate) const RESOURCE_TABLES: &[ResourceTable] = &[
    ResourceTable {
        table_name: "pods",
        json_key: "pods",
        ndjson_file: "pods.ndjson.gz",
        ipc_file: "pods.arrow",
        parquet_file: "pods.parquet",
        schema: NestedSchemas::pods_nested_schema,
    },
    ResourceTable {
        table_name: "nodes",
        json_key: "nodes",
        ndjson_file: "nodes.ndjson.gz",
        ipc_file: "nodes.arrow",
        parquet_file: "nodes.parquet",
        schema: NestedSchemas::nodes_nested_schema,
    },
    ResourceTable {
        table_name: "namespaces",
        json_key: "namespaces",
        ndjson_file: "namespaces.ndjson.gz",
        ipc_file: "namespaces.arrow",
        parquet_file: "namespaces.parquet",
        schema: NestedSchemas::namespaces_nested_schema,
    },
    ResourceTable {
        table_name: "daemon_sets",
        json_key: "daemonSets",
        ndjson_file: "daemonsets.ndjson.gz",
        ipc_file: "daemonsets.arrow",
        parquet_file: "daemonsets.parquet",
        schema: NestedSchemas::daemon_sets_nested_schema,
    },
];

pub(crate) fn resource_for_json_key(json_key: &str) -> Option<ResourceTable> {
    RESOURCE_TABLES
        .iter()
        .copied()
        .find(|resource| resource.json_key == json_key)
}

pub(crate) fn resource_for_table_name(table_name: &str) -> Option<ResourceTable> {
    RESOURCE_TABLES
        .iter()
        .copied()
        .find(|resource| resource.table_name == table_name)
}
