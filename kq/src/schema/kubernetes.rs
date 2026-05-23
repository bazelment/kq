use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Re-export official Kubernetes API types from k8s-openapi
/// These are auto-generated from the official Kubernetes OpenAPI specifications
/// 
/// This eliminates ~700 lines of manual type definitions and ensures
/// we always have accurate, up-to-date types that match the Kubernetes API.

// Core v1 API - Nodes
pub use k8s_openapi::api::core::v1::{
    Node,
    NodeSpec,
    NodeStatus,
    NodeCondition,
    NodeSystemInfo,
    NodeAddress,
    Taint,
};

// Core v1 API - Pods
pub use k8s_openapi::api::core::v1::{
    Pod,
    PodSpec,
    PodStatus,
    Container,
    ContainerStatus,
    ContainerState,
    ContainerStateWaiting,
    ContainerStateRunning,
    ContainerStateTerminated,
    ResourceRequirements,
};

// Core v1 API - Affinity and Scheduling
pub use k8s_openapi::api::core::v1::{
    Affinity,
    NodeAffinity,
    NodeSelector,
    NodeSelectorTerm,
    NodeSelectorRequirement,
    PreferredSchedulingTerm,
    PodAffinity,
    PodAntiAffinity,
    PodAffinityTerm,
    WeightedPodAffinityTerm,
    Toleration,
};

// Core v1 API - Volumes
pub use k8s_openapi::api::core::v1::{
    Volume,
    ConfigMapVolumeSource,
    SecretVolumeSource,
    PersistentVolumeClaimVolumeSource,
    EmptyDirVolumeSource,
    HostPathVolumeSource,
    KeyToPath,
};

// Core v1 API - Namespaces
pub use k8s_openapi::api::core::v1::{
    Namespace,
    NamespaceStatus,
};

// Apps v1 API - DaemonSets
pub use k8s_openapi::api::apps::v1::{
    DaemonSet,
    DaemonSetSpec,
    DaemonSetStatus,
};

// Meta v1 - Metadata and Labels
pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    ObjectMeta,
    // LabelSelector,
    // LabelSelectorRequirement,
};

// Note: ResourceList is now a BTreeMap<String, Quantity>
// We'll keep a type alias for compatibility with our converters
pub use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
pub type ResourceList = std::collections::BTreeMap<String, Quantity>;

/// Kubernetes cluster snapshot structure
/// 
/// This is our custom wrapper around the official Kubernetes types.
/// All resource types (Node, Pod, etc.) are official k8s-openapi types.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterSnapshot {
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub nodes: Option<Vec<Node>>,
    #[serde(default)]
    pub namespaces: Option<Vec<Namespace>>,
    #[serde(rename = "daemonSets", default)]
    pub daemon_sets: Option<Vec<DaemonSet>>,
    pub pods: Option<Vec<Pod>>,
}

impl ClusterSnapshot {
    /// Get a summary of the snapshot for display
    pub fn get_summary(&self) -> SnapshotSummary {
        let ts = self.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string();
        let node_count = self.nodes.as_ref().map_or(0, |n| n.len());
        let namespace_count = self.namespaces.as_ref().map_or(0, |n| n.len());
        let daemonset_count = self.daemon_sets.as_ref().map_or(0, |d| d.len());
        let pod_count = self.pods.as_ref().map(|p| p.len());
        SnapshotSummary {
            timestamp: ts,
            node_count,
            namespace_count,
            daemonset_count,
            pod_count,
        }
    }
}

#[derive(Debug)]
pub struct SnapshotSummary {
    pub timestamp: String,
    pub node_count: usize,
    pub namespace_count: usize,
    pub daemonset_count: usize,
    pub pod_count: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_snapshot_deserialization() {
        let json = r#"
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [],
            "namespaces": [],
            "daemonSets": [],
            "pods": []
        }
        "#;

        let snapshot: ClusterSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.nodes.as_ref().map_or(0, |n| n.len()), 0);
        assert_eq!(snapshot.namespaces.as_ref().map_or(0, |n| n.len()), 0);
        assert_eq!(snapshot.daemon_sets.as_ref().map_or(0, |d| d.len()), 0);
        assert!(snapshot.pods.is_some());
    }

    #[test]
    fn test_snapshot_summary() {
        let snapshot = ClusterSnapshot {
            timestamp: Utc::now(),
            nodes: Some(vec![]),
            namespaces: Some(vec![]),
            daemon_sets: Some(vec![]),
            pods: Some(vec![]),
        };

        let summary = snapshot.get_summary();
        assert_eq!(summary.node_count, 0);
        assert_eq!(summary.pod_count, Some(0));
    }

    #[test]
    fn test_k8s_openapi_types_available() {
        // Verify that official k8s-openapi types are accessible
        let _node: Option<Node> = None;
        let _pod: Option<Pod> = None;
        let _namespace: Option<Namespace> = None;
        let _daemonset: Option<DaemonSet> = None;
        
        // This test ensures the types compile
        assert!(true);
    }
}
