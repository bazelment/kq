use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Nested schema definitions for Kubernetes resources using Arrow's native nested types
/// This enables Spark-style dot notation queries like: metadata.labels."app"
/// 
/// Based on Kubernetes core/v1 API definitions
pub struct NestedSchemas;

impl NestedSchemas {
    /// Enhanced nodes schema with full nested structure
    /// Based on: kubernetes/staging/src/k8s.io/api/core/v1/generated.proto Node message
    /// Enables queries like: SELECT status.allocatable."hugepages-2Mi" FROM nodes
    pub fn nodes_nested_schema() -> Arc<Schema> {
        let fields = vec![
            // Comprehensive metadata as nested struct - enables metadata.name, metadata.labels.key
            Field::new("metadata", DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("uid", DataType::Utf8, true),
                    Field::new("creationTimestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    Field::new("resourceVersion", DataType::Utf8, true),
                    Field::new("generation", DataType::Int64, true),
                    // Labels as Map for flexible querying: metadata.labels."any.key"
                    Field::new("labels", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false, // keys are not sorted
                    ), true),
                    // Annotations as Map
                    Field::new("annotations", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                ].into()
            ), true),

            // NodeSpec - comprehensive spec as nested struct
            Field::new("spec", DataType::Struct(
                vec![
                    Field::new("podCIDR", DataType::Utf8, true),
                    Field::new("podCIDRs", DataType::List(
                        Arc::new(Field::new("item", DataType::Utf8, false))
                    ), true),
                    Field::new("providerID", DataType::Utf8, true),
                    Field::new("unschedulable", DataType::Boolean, true),
                    // Taints as List of Structs
                    Field::new("taints", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Utf8, true),
                            Field::new("effect", DataType::Utf8, true),
                            Field::new("timeAdded", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                        ].into()), true))
                    ), true),
                ].into()
            ), true),

            // NodeStatus - comprehensive status as nested struct
            Field::new("status", DataType::Struct(
                vec![
                    Field::new("phase", DataType::Utf8, true),
                    // Allocatable resources as Map for flexible resource querying
                    Field::new("allocatable", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                    // Capacity resources as Map
                    Field::new("capacity", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                    // Conditions as List of Structs
                    Field::new("conditions", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("type", DataType::Utf8, true),
                            Field::new("status", DataType::Utf8, true),
                            Field::new("lastHeartbeatTime", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                            Field::new("lastTransitionTime", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                            Field::new("reason", DataType::Utf8, true),
                            Field::new("message", DataType::Utf8, true),
                        ].into()), true))
                    ), true),
                    // Addresses as List of Structs
                    Field::new("addresses", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("type", DataType::Utf8, true),
                            Field::new("address", DataType::Utf8, true),
                        ].into()), true))
                    ), true),
                    // Node system info
                    Field::new("nodeInfo", DataType::Struct(vec![
                        Field::new("machineID", DataType::Utf8, true),
                        Field::new("systemUUID", DataType::Utf8, true),
                        Field::new("bootID", DataType::Utf8, true),
                        Field::new("kernelVersion", DataType::Utf8, true),
                        Field::new("osImage", DataType::Utf8, true),
                        Field::new("containerRuntimeVersion", DataType::Utf8, true),
                        Field::new("kubeletVersion", DataType::Utf8, true),
                        Field::new("kubeProxyVersion", DataType::Utf8, true),
                        Field::new("operatingSystem", DataType::Utf8, true),
                        Field::new("architecture", DataType::Utf8, true),
                    ].into()), true),
                ].into()
            ), true),
            Field::new("hugepages_2mi", DataType::Utf8, true),
            Field::new("hugepages_1gi", DataType::Utf8, true),
            Field::new("cluster", DataType::Utf8, true),
            Field::new("pool", DataType::Utf8, true),
            Field::new("ready", DataType::Boolean, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Enhanced pods schema with full nested structure
    /// Based on: kubernetes/staging/src/k8s.io/api/core/v1/generated.proto Pod message
    /// Enables queries like: SELECT spec.containers[0].resources.requests.memory FROM pods
    pub fn pods_nested_schema() -> Arc<Schema> {
        let fields = vec![
            // Comprehensive metadata struct with labels and annotations
            Field::new("metadata", DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    // Dictionary encoding for namespace (high duplication: 75K pods → ~50 unique)
                    Field::new("namespace", DataType::Utf8, true),
                    Field::new("uid", DataType::Utf8, true),
                    Field::new("creationTimestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    Field::new("resourceVersion", DataType::Utf8, true),
                    Field::new("generation", DataType::Int64, true),
                    // Labels as Map
                    Field::new("labels", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                    // Annotations as Map
                    Field::new("annotations", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                    // Owner references
                    Field::new("ownerReferences", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("apiVersion", DataType::Utf8, true),
                            Field::new("kind", DataType::Utf8, true),
                            Field::new("name", DataType::Utf8, true),
                            Field::new("uid", DataType::Utf8, true),
                            Field::new("controller", DataType::Boolean, true),
                            Field::new("blockOwnerDeletion", DataType::Boolean, true),
                        ].into()), true))
                    ), true),
                ].into()
            ), true),

            // PodSpec - comprehensive spec as nested struct with affinity and containers
            Field::new("spec", DataType::Struct(
                vec![
                    // Dictionary encoding for nodeName (75K pods → 3.5K unique nodes)
                    Field::new("nodeName", DataType::Utf8, true),
                    Field::new("restartPolicy", DataType::Utf8, true),
                    Field::new("schedulerName", DataType::Utf8, true),
                    Field::new("priorityClassName", DataType::Utf8, true),
                    Field::new("priority", DataType::Int32, true),
                    Field::new("serviceAccountName", DataType::Utf8, true),
                    Field::new("terminationGracePeriodSeconds", DataType::Int64, true),
                    Field::new("activeDeadlineSeconds", DataType::Int64, true),
                    Field::new("dnsPolicy", DataType::Utf8, true),
                    Field::new("hostname", DataType::Utf8, true),
                    Field::new("subdomain", DataType::Utf8, true),
                    Field::new("runtimeClassName", DataType::Utf8, true),
                    // Host settings
                    Field::new("hostNetwork", DataType::Boolean, true),
                    Field::new("hostPID", DataType::Boolean, true),
                    Field::new("hostIPC", DataType::Boolean, true),
                    Field::new("shareProcessNamespace", DataType::Boolean, true),
                    // Node selector as Map
                    Field::new("nodeSelector", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ].into()), false)),
                        false,
                    ), true),
                    // Containers as List of Structs
                    Field::new("containers", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("name", DataType::Utf8, true),
                            Field::new("image", DataType::Utf8, true),
                            Field::new("command", DataType::List(
                                Arc::new(Field::new("item", DataType::Utf8, false))
                            ), true),
                            Field::new("args", DataType::List(
                                Arc::new(Field::new("item", DataType::Utf8, false))
                            ), true),
                            Field::new("workingDir", DataType::Utf8, true),
                            Field::new("imagePullPolicy", DataType::Utf8, true),
                            // Ports
                            Field::new("ports", DataType::List(
                                Arc::new(Field::new("item", DataType::Struct(vec![
                                    Field::new("name", DataType::Utf8, true),
                                    Field::new("containerPort", DataType::Int32, true),
                                    Field::new("protocol", DataType::Utf8, true),
                                    Field::new("hostPort", DataType::Int32, true),
                                    Field::new("hostIP", DataType::Utf8, true),
                                ].into()), true))
                            ), true),
                            // Environment variables
                            Field::new("env", DataType::List(
                                Arc::new(Field::new("item", DataType::Struct(vec![
                                    Field::new("name", DataType::Utf8, true),
                                    Field::new("value", DataType::Utf8, true),
                                ].into()), true))
                            ), true),
                            // Resources as nested struct
                            Field::new("resources", DataType::Struct(vec![
                                Field::new("requests", DataType::Map(
                                    Arc::new(Field::new("entries", DataType::Struct(vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", DataType::Utf8, true),
                                    ].into()), false)),
                                    false,
                                ), true),
                                Field::new("limits", DataType::Map(
                                    Arc::new(Field::new("entries", DataType::Struct(vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", DataType::Utf8, true),
                                    ].into()), false)),
                                    false,
                                ), true),
                            ].into()), true),
                            // Volume mounts
                            Field::new("volumeMounts", DataType::List(
                                Arc::new(Field::new("item", DataType::Struct(vec![
                                    Field::new("name", DataType::Utf8, true),
                                    Field::new("mountPath", DataType::Utf8, true),
                                    Field::new("subPath", DataType::Utf8, true),
                                    Field::new("readOnly", DataType::Boolean, true),
                                ].into()), true))
                            ), true),
                        ].into()), true))
                    ), true),
                    // Init containers
                    Field::new("initContainers", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("name", DataType::Utf8, true),
                            Field::new("image", DataType::Utf8, true),
                        ].into()), true))
                    ), true),
                    // Volumes
                    Field::new("volumes", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("name", DataType::Utf8, true),
                            // Could be expanded with volume source types
                        ].into()), true))
                    ), true),
                    // Affinity - full nested structure from protobuf
                    Field::new("affinity", DataType::Struct(vec![
                        Field::new("nodeAffinity", DataType::Struct(vec![
                            // NodeSelector with nodeSelectorTerms array
                            Field::new("requiredDuringSchedulingIgnoredDuringExecution", DataType::Struct(vec![
                                Field::new("nodeSelectorTerms", DataType::List(
                                    Arc::new(Field::new("item", DataType::Struct(vec![
                                        // matchExpressions array
                                        Field::new("matchExpressions", DataType::List(
                                            Arc::new(Field::new("item", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, true),
                                                Field::new("operator", DataType::Utf8, true),
                                                Field::new("values", DataType::List(
                                                    Arc::new(Field::new("item", DataType::Utf8, false))
                                                ), true),
                                            ].into()), true))
                                        ), true),
                                        // matchFields array
                                        Field::new("matchFields", DataType::List(
                                            Arc::new(Field::new("item", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, true),
                                                Field::new("operator", DataType::Utf8, true),
                                                Field::new("values", DataType::List(
                                                    Arc::new(Field::new("item", DataType::Utf8, false))
                                                ), true),
                                            ].into()), true))
                                        ), true),
                                    ].into()), true))
                                ), true),
                            ].into()), true),
                        ].into()), true),
                        // PodAffinity structure
                        Field::new("podAffinity", DataType::Struct(vec![
                            Field::new("requiredDuringSchedulingIgnoredDuringExecution", DataType::List(
                                Arc::new(Field::new("item", DataType::Struct(vec![
                                    // LabelSelector
                                    Field::new("labelSelector", DataType::Struct(vec![
                                        Field::new("matchLabels", DataType::Map(
                                            Arc::new(Field::new("entries", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, true),
                                            ].into()), false)),
                                            false,
                                        ), true),
                                        Field::new("matchExpressions", DataType::List(
                                            Arc::new(Field::new("item", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, true),
                                                Field::new("operator", DataType::Utf8, true),
                                                Field::new("values", DataType::List(
                                                    Arc::new(Field::new("item", DataType::Utf8, false))
                                                ), true),
                                            ].into()), true))
                                        ), true),
                                    ].into()), true),
                                    Field::new("namespaces", DataType::List(
                                        Arc::new(Field::new("item", DataType::Utf8, false))
                                    ), true),
                                    Field::new("topologyKey", DataType::Utf8, true),
                                    Field::new("namespaceSelector", DataType::Struct(vec![
                                        Field::new("matchLabels", DataType::Map(
                                            Arc::new(Field::new("entries", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, true),
                                            ].into()), false)),
                                            false,
                                        ), true),
                                    ].into()), true),
                                ].into()), true))
                            ), true),
                        ].into()), true),
                        // PodAntiAffinity structure (same as PodAffinity)
                        Field::new("podAntiAffinity", DataType::Struct(vec![
                            Field::new("requiredDuringSchedulingIgnoredDuringExecution", DataType::List(
                                Arc::new(Field::new("item", DataType::Struct(vec![
                                    // LabelSelector
                                    Field::new("labelSelector", DataType::Struct(vec![
                                        Field::new("matchLabels", DataType::Map(
                                            Arc::new(Field::new("entries", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, true),
                                            ].into()), false)),
                                            false,
                                        ), true),
                                        Field::new("matchExpressions", DataType::List(
                                            Arc::new(Field::new("item", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, true),
                                                Field::new("operator", DataType::Utf8, true),
                                                Field::new("values", DataType::List(
                                                    Arc::new(Field::new("item", DataType::Utf8, false))
                                                ), true),
                                            ].into()), true))
                                        ), true),
                                    ].into()), true),
                                    Field::new("namespaces", DataType::List(
                                        Arc::new(Field::new("item", DataType::Utf8, false))
                                    ), true),
                                    Field::new("topologyKey", DataType::Utf8, true),
                                    Field::new("namespaceSelector", DataType::Struct(vec![
                                        Field::new("matchLabels", DataType::Map(
                                            Arc::new(Field::new("entries", DataType::Struct(vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new("value", DataType::Utf8, true),
                                            ].into()), false)),
                                            false,
                                        ), true),
                                    ].into()), true),
                                ].into()), true))
                            ), true),
                        ].into()), true),
                    ].into()), true),
                    // Tolerations as List based on protobuf
                    Field::new("tolerations", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("operator", DataType::Utf8, true),
                            Field::new("value", DataType::Utf8, true),
                            Field::new("effect", DataType::Utf8, true),
                            Field::new("tolerationSeconds", DataType::Int64, true),
                        ].into()), true))
                    ), true),
                    // SecurityContext - basic fields
                    Field::new("securityContext", DataType::Struct(vec![
                        Field::new("runAsUser", DataType::Int64, true),
                        Field::new("runAsGroup", DataType::Int64, true),
                        Field::new("runAsNonRoot", DataType::Boolean, true),
                        Field::new("fsGroup", DataType::Int64, true),
                        // SELinux and Windows options can be added if needed
                    ].into()), true),
                ].into()
            ), true),

            // PodStatus - comprehensive status as nested struct
            Field::new("status", DataType::Struct(
                vec![
                    // Dictionary encoding for phase (very few unique values: Running, Pending, etc.)
                    Field::new("phase", DataType::Utf8, true),
                    Field::new("message", DataType::Utf8, true),
                    Field::new("reason", DataType::Utf8, true),
                    Field::new("nominatedNodeName", DataType::Utf8, true),
                    Field::new("hostIP", DataType::Utf8, true),
                    Field::new("podIP", DataType::Utf8, true),
                    Field::new("startTime", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    Field::new("qosClass", DataType::Utf8, true),
                    // Pod IPs
                    Field::new("podIPs", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("ip", DataType::Utf8, true),
                        ].into()), true))
                    ), true),
                    // Conditions
                    Field::new("conditions", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("type", DataType::Utf8, true),
                            Field::new("status", DataType::Utf8, true),
                            Field::new("lastProbeTime", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                            Field::new("lastTransitionTime", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                            Field::new("reason", DataType::Utf8, true),
                            Field::new("message", DataType::Utf8, true),
                        ].into()), true))
                    ), true),
                    // Container statuses
                    Field::new("containerStatuses", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("name", DataType::Utf8, true),
                            Field::new("ready", DataType::Boolean, true),
                            Field::new("restartCount", DataType::Int32, true),
                            Field::new("image", DataType::Utf8, true),
                            Field::new("imageID", DataType::Utf8, true),
                            Field::new("containerID", DataType::Utf8, true),
                            Field::new("started", DataType::Boolean, true),
                        ].into()), true))
                    ), true),
                    // Init container statuses
                    Field::new("initContainerStatuses", DataType::List(
                        Arc::new(Field::new("item", DataType::Struct(vec![
                            Field::new("name", DataType::Utf8, true),
                            Field::new("ready", DataType::Boolean, true),
                            Field::new("restartCount", DataType::Int32, true),
                        ].into()), true))
                    ), true),
                ].into()
            ), true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("node_name", DataType::Utf8, true),
            Field::new("phase", DataType::Utf8, true),
            Field::new("cpu_request_total", DataType::Int64, true),
            Field::new("memory_request_total", DataType::Int64, true),
            Field::new("app", DataType::Utf8, true),
            Field::new("product", DataType::Utf8, true),
            Field::new("tenant", DataType::Utf8, true),
            Field::new("cluster", DataType::Utf8, true),
            Field::new("pool", DataType::Utf8, true),
            Field::new("workload_kind", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Enhanced namespaces schema with nested structure
    pub fn namespaces_nested_schema() -> Arc<Schema> {
        let fields = vec![
            // Full metadata struct
            Field::new("metadata", DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("uid", DataType::Utf8, true),
                    Field::new("creationTimestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    Field::new("labels", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ].into()
                        ), false)),
                        false
                    ), true),
                    Field::new("annotations", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ].into()
                        ), false)),
                        false
                    ), true),
                ].into()
            ), true),

            // Status struct
            Field::new("status", DataType::Struct(
                vec![
                    Field::new("phase", DataType::Utf8, true),
                ].into()
            ), true),
            Field::new("app", DataType::Utf8, true),
            Field::new("product", DataType::Utf8, true),
            Field::new("tenant", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Enhanced daemon_sets schema with nested structure
    pub fn daemon_sets_nested_schema() -> Arc<Schema> {
        let fields = vec![
            // Full metadata struct
            Field::new("metadata", DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("namespace", DataType::Utf8, true),
                    Field::new("uid", DataType::Utf8, true),
                    Field::new("creationTimestamp", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                    Field::new("labels", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ].into()
                        ), false)),
                        false
                    ), true),
                    Field::new("annotations", DataType::Map(
                        Arc::new(Field::new("entries", DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ].into()
                        ), false)),
                        false
                    ), true),
                ].into()
            ), true),

            // Simplified spec struct
            Field::new("spec", DataType::Struct(
                vec![
                    Field::new("revisionHistoryLimit", DataType::Int32, true),
                ].into()
            ), true),

            // Status struct
            Field::new("status", DataType::Struct(
                vec![
                    Field::new("currentNumberScheduled", DataType::Int32, true),
                    Field::new("desiredNumberScheduled", DataType::Int32, true),
                    Field::new("numberReady", DataType::Int32, true),
                ].into()
            ), true),
            Field::new("ready_percentage", DataType::Float32, true),
        ];

        Arc::new(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_json::ReaderBuilder;
    use std::io::BufReader;
    use std::fs::File;
    
    #[test]
    fn test_nested_schemas_creation() {
        let nodes_schema = NestedSchemas::nodes_nested_schema();
        assert!(!nodes_schema.fields().is_empty());
        
        // Check that metadata is a struct
        let metadata_field = nodes_schema.field_with_name("metadata").unwrap();
        assert!(matches!(metadata_field.data_type(), DataType::Struct(_)));
        
        // Check that status is a struct with nested maps
        let status_field = nodes_schema.field_with_name("status").unwrap();
        assert!(matches!(status_field.data_type(), DataType::Struct(_)));
    }

    #[test]
    fn test_pods_nested_schema() {
        let pods_schema = NestedSchemas::pods_nested_schema();
        
        // Check that spec contains containers list
        let spec_field = pods_schema.field_with_name("spec").unwrap();
        assert!(matches!(spec_field.data_type(), DataType::Struct(_)));
        
        // Check that status is a struct
        let status_field = pods_schema.field_with_name("status").unwrap();
        assert!(matches!(status_field.data_type(), DataType::Struct(_)));
    }

    /// Test parsing a synthetic pod JSON with complex affinity rules.
    #[test]
    fn test_parse_golden_pod_with_affinity() {
        let schema = NestedSchemas::pods_nested_schema();
        let golden_file = File::open("kq/tests/golden_pod_with_affinity.json")
            .expect("Golden file should exist");
        let buf_reader = BufReader::new(golden_file);
        
        let json_reader = ReaderBuilder::new(schema.clone())
            .build(buf_reader)
            .expect("Should build JSON reader");
        
        let mut batch_count = 0;
        for batch_result in json_reader {
            let batch = batch_result.expect("Should parse batch successfully");
            assert!(batch.num_rows() > 0, "Batch should have rows");
            batch_count += 1;
        }
        
        assert!(batch_count > 0, "Should have parsed at least one batch");
    }

    /// Test parsing a synthetic node JSON with full resource info.
    #[test]
    fn test_parse_golden_node() {
        let schema = NestedSchemas::nodes_nested_schema();
        let golden_file = File::open("kq/tests/golden_node.json")
            .expect("Golden file should exist");
        let buf_reader = BufReader::new(golden_file);
        
        let json_reader = ReaderBuilder::new(schema.clone())
            .build(buf_reader)
            .expect("Should build JSON reader");
        
        let mut batch_count = 0;
        for batch_result in json_reader {
            let batch = batch_result.expect("Should parse batch successfully");
            assert!(batch.num_rows() > 0, "Batch should have rows");
            batch_count += 1;
        }
        
        assert!(batch_count > 0, "Should have parsed at least one batch");
    }

    /// Test parsing a synthetic namespace JSON.
    #[test]
    fn test_parse_golden_namespace() {
        let schema = NestedSchemas::namespaces_nested_schema();
        let golden_file = File::open("kq/tests/golden_namespace.json")
            .expect("Golden file should exist");
        let buf_reader = BufReader::new(golden_file);
        
        let json_reader = ReaderBuilder::new(schema.clone())
            .build(buf_reader)
            .expect("Should build JSON reader");
        
        let mut batch_count = 0;
        for batch_result in json_reader {
            let batch = batch_result.expect("Should parse batch successfully");
            assert!(batch.num_rows() > 0, "Batch should have rows");
            batch_count += 1;
        }
        
        assert!(batch_count > 0, "Should have parsed at least one batch");
    }

    /// Test parsing a synthetic daemonset JSON.
    #[test]
    fn test_parse_golden_daemonset() {
        let schema = NestedSchemas::daemon_sets_nested_schema();
        let golden_file = File::open("kq/tests/golden_daemonset.json")
            .expect("Golden file should exist");
        let buf_reader = BufReader::new(golden_file);
        
        let json_reader = ReaderBuilder::new(schema.clone())
            .build(buf_reader)
            .expect("Should build JSON reader");
        
        let mut batch_count = 0;
        for batch_result in json_reader {
            let batch = batch_result.expect("Should parse batch successfully");
            assert!(batch.num_rows() > 0, "Batch should have rows");
            batch_count += 1;
        }
        
        assert!(batch_count > 0, "Should have parsed at least one batch");
    }
}
