use anyhow::{Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::{json, Map, Value};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const SYSTEM_NAMESPACES: &[&str] = &[
    "kube-system",
    "kube-public",
    "kube-node-lease",
    "observability",
    "security",
    "ingress",
    "platform",
    "storage",
];

const DAEMONSET_WORKLOADS: &[(&str, &str, &str)] = &[
    ("kube-proxy", "kube-system", "network"),
    ("cni-agent", "kube-system", "network"),
    ("node-exporter", "observability", "metrics"),
    ("log-collector", "observability", "logging"),
    ("security-agent", "security", "security"),
    ("csi-node", "storage", "storage"),
    ("mesh-node", "platform", "mesh"),
    ("dns-node-cache", "kube-system", "dns"),
];

const POOLS: &[(&str, u32)] = &[
    ("general", 58),
    ("cpu", 12),
    ("highmem", 10),
    ("spot", 8),
    ("stateful", 6),
    ("gpu", 3),
    ("infra", 3),
];

const ZONES: &[&str] = &[
    "us-east-1a",
    "us-east-1b",
    "us-east-1c",
    "us-east-1d",
    "us-east-1e",
    "us-east-1f",
];

const TENANTS: &[&str] = &[
    "payments",
    "search",
    "identity",
    "analytics",
    "messaging",
    "ads",
    "platform",
    "ml",
    "growth",
    "infra",
];

const COMPONENTS: &[&str] = &[
    "api",
    "worker",
    "frontend",
    "consumer",
    "scheduler",
    "gateway",
    "processor",
    "indexer",
    "controller",
    "cache",
];

#[derive(Debug, Clone)]
pub struct SyntheticSnapshotConfig {
    pub output_dir: PathBuf,
    pub cluster_name: String,
    pub node_count: usize,
    pub min_pods_per_node: usize,
    pub max_pods_per_node: usize,
    pub namespace_count: usize,
    pub seed: u64,
    pub overwrite: bool,
    pub timestamp: DateTime<Utc>,
}

impl Default for SyntheticSnapshotConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("synthetic-snapshot"),
            cluster_name: "synthetic-a".to_string(),
            node_count: 5_000,
            min_pods_per_node: 10,
            max_pods_per_node: 60,
            namespace_count: 240,
            seed: 42,
            overwrite: false,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyntheticSnapshotSummary {
    pub output_dir: PathBuf,
    pub cluster_name: String,
    pub node_count: usize,
    pub pod_count: usize,
    pub namespace_count: usize,
    pub daemonset_count: usize,
    pub min_pods_per_node: usize,
    pub max_pods_per_node: usize,
    pub running_pods: usize,
    pub pending_pods: usize,
    pub succeeded_pods: usize,
    pub failed_pods: usize,
    pub unknown_pods: usize,
    pub generation_seconds: f64,
}

#[derive(Debug, Clone, Copy)]
enum WorkloadKind {
    Deployment,
    StatefulSet,
    Job,
    CronJob,
    Canary,
    SystemDaemon,
}

#[derive(Debug, Clone, Copy)]
struct NodeShape {
    instance_type: &'static str,
    cpu: u32,
    memory_gib: u32,
    pods: u32,
    ephemeral_gib: u32,
}

#[derive(Debug, Clone)]
struct NodeContext {
    index: usize,
    name: String,
    pool: String,
    zone: String,
    instance_type: &'static str,
    cpu: u32,
    memory_gib: u32,
    pod_capacity: u32,
    ephemeral_gib: u32,
}

#[derive(Debug, Clone)]
struct PodContext {
    ordinal: usize,
    app_id: usize,
    app: String,
    product: String,
    tenant: String,
    component: String,
    namespace: String,
    workload: WorkloadKind,
    phase: &'static str,
    age_minutes: i64,
    intended_lifespan_hours: i64,
    has_sidecar: bool,
    restart_count: i32,
    cpu_millis: u32,
    memory_mib: u32,
    tier: &'static str,
    release: &'static str,
}

#[derive(Debug, Clone)]
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x9e37_79b9_7f4a_7c15 } else { seed };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn range_usize(&mut self, min: usize, max_inclusive: usize) -> usize {
        if min >= max_inclusive {
            return min;
        }
        min + (self.next_u64() as usize % (max_inclusive - min + 1))
    }

    fn range_u32(&mut self, min: u32, max_inclusive: u32) -> u32 {
        self.range_usize(min as usize, max_inclusive as usize) as u32
    }

    fn one_in(&mut self, denominator: usize) -> bool {
        denominator > 0 && self.range_usize(1, denominator) == 1
    }

    fn weighted<'a>(&mut self, values: &'a [(&'a str, u32)]) -> &'a str {
        let total: u32 = values.iter().map(|(_, weight)| *weight).sum();
        let mut pick = self.range_u32(1, total);
        for (value, weight) in values {
            if pick <= *weight {
                return value;
            }
            pick -= *weight;
        }
        values[0].0
    }

    fn pick<'a>(&mut self, values: &'a [&'a str]) -> &'a str {
        values[self.range_usize(0, values.len() - 1)]
    }
}

pub fn generate_ndjson_snapshot(config: &SyntheticSnapshotConfig) -> Result<SyntheticSnapshotSummary> {
    validate_config(config)?;
    prepare_output_dir(config)?;

    let started = Instant::now();
    let mut rng = SimpleRng::new(config.seed);
    let namespace_names = namespace_names(config.namespace_count);

    write_namespaces(config, &namespace_names)?;
    write_daemonsets(config)?;

    let nodes_path = config.output_dir.join("nodes.ndjson.gz");
    let pods_path = config.output_dir.join("pods.ndjson.gz");
    let mut nodes = gzip_line_writer(&nodes_path)?;
    let mut pods = gzip_line_writer(&pods_path)?;

    let mut pod_count = 0usize;
    let mut running_pods = 0usize;
    let mut pending_pods = 0usize;
    let mut succeeded_pods = 0usize;
    let mut failed_pods = 0usize;
    let mut unknown_pods = 0usize;
    let mut observed_min = usize::MAX;
    let mut observed_max = 0usize;

    for node_idx in 0..config.node_count {
        let node = make_node_context(config, &mut rng, node_idx);
        write_json_line(&mut nodes, &make_node(config, &node))?;

        let pods_on_node = pods_for_node(config, &mut rng);
        observed_min = observed_min.min(pods_on_node);
        observed_max = observed_max.max(pods_on_node);

        let daemon_pods = pods_on_node.min(DAEMONSET_WORKLOADS.len());
        for daemon_idx in 0..daemon_pods {
            let pod = make_daemon_pod(config, &node, daemon_idx, pod_count);
            write_json_line(&mut pods, &pod)?;
            pod_count += 1;
            running_pods += 1;
        }

        for _ in daemon_pods..pods_on_node {
            let pod_ctx = make_workload_pod_context(
                config,
                &namespace_names,
                &node,
                &mut rng,
                pod_count,
            );
            match pod_ctx.phase {
                "Running" => running_pods += 1,
                "Pending" => pending_pods += 1,
                "Succeeded" => succeeded_pods += 1,
                "Failed" => failed_pods += 1,
                _ => unknown_pods += 1,
            }
            write_json_line(&mut pods, &make_pod(config, &node, &pod_ctx))?;
            pod_count += 1;
        }
    }

    finish_gzip_writer(nodes)?;
    finish_gzip_writer(pods)?;

    let summary = SyntheticSnapshotSummary {
        output_dir: config.output_dir.clone(),
        cluster_name: config.cluster_name.clone(),
        node_count: config.node_count,
        pod_count,
        namespace_count: namespace_names.len(),
        daemonset_count: DAEMONSET_WORKLOADS.len(),
        min_pods_per_node: observed_min,
        max_pods_per_node: observed_max,
        running_pods,
        pending_pods,
        succeeded_pods,
        failed_pods,
        unknown_pods,
        generation_seconds: started.elapsed().as_secs_f64(),
    };

    write_metadata(config, &summary)?;
    Ok(summary)
}

fn validate_config(config: &SyntheticSnapshotConfig) -> Result<()> {
    if config.node_count == 0 {
        anyhow::bail!("node_count must be greater than zero");
    }
    if config.namespace_count == 0 {
        anyhow::bail!("namespace_count must be greater than zero");
    }
    if config.min_pods_per_node == 0 {
        anyhow::bail!("min_pods_per_node must be greater than zero");
    }
    if config.min_pods_per_node > config.max_pods_per_node {
        anyhow::bail!("min_pods_per_node must be <= max_pods_per_node");
    }
    if config.cluster_name.trim().is_empty() {
        anyhow::bail!("cluster_name must not be empty");
    }
    Ok(())
}

fn prepare_output_dir(config: &SyntheticSnapshotConfig) -> Result<()> {
    if config.output_dir.exists() {
        if !config.output_dir.is_dir() {
            anyhow::bail!("output path exists and is not a directory: {}", config.output_dir.display());
        }

        if config.overwrite {
            fs::remove_dir_all(&config.output_dir)
                .with_context(|| format!("failed to remove existing output directory {}", config.output_dir.display()))?;
        } else if fs::read_dir(&config.output_dir)
            .with_context(|| format!("failed to inspect output directory {}", config.output_dir.display()))?
            .next()
            .is_some()
        {
            anyhow::bail!(
                "output directory is not empty: {} (pass --overwrite to replace it)",
                config.output_dir.display()
            );
        }
    }

    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create output directory {}", config.output_dir.display()))?;
    Ok(())
}

fn write_metadata(config: &SyntheticSnapshotConfig, summary: &SyntheticSnapshotSummary) -> Result<()> {
    let metadata = json!({
        "timestamp": format_timestamp(config.timestamp),
        "generator": "kq_synthetic",
        "format": "ndjson.gz",
        "cluster": summary.cluster_name,
        "nodes": summary.node_count,
        "pods": summary.pod_count,
        "namespaces": summary.namespace_count,
        "daemonSets": summary.daemonset_count,
        "podsPerNode": {
            "min": summary.min_pods_per_node,
            "max": summary.max_pods_per_node
        },
        "seed": config.seed
    });
    let path = config.output_dir.join("metadata.json");
    fs::write(&path, serde_json::to_vec_pretty(&metadata)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn write_namespaces(config: &SyntheticSnapshotConfig, namespace_names: &[String]) -> Result<()> {
    let path = config.output_dir.join("namespaces.ndjson.gz");
    let mut writer = gzip_line_writer(&path)?;
    for (idx, name) in namespace_names.iter().enumerate() {
        let tenant = TENANTS[idx % TENANTS.len()];
        let product = if idx < SYSTEM_NAMESPACES.len() {
            "platform".to_string()
        } else {
            format!("product-{:03}", idx % 160)
        };
        let namespace = json!({
            "metadata": {
                "name": name,
                "uid": stable_uid(&config.cluster_name, "namespace", idx),
                "creationTimestamp": format_timestamp(config.timestamp - Duration::days(180 - (idx % 120) as i64)),
                "labels": {
                    "kubernetes.io/metadata.name": name,
                    "synthetic.kq.dev/cluster": config.cluster_name,
                    "product": product,
                    "tenant.kq.dev/name": tenant,
                    "environment": "production",
                    "cost-center": format!("cc-{:04}", 1000 + idx % 900),
                    "pod-security.kubernetes.io/enforce": if idx < SYSTEM_NAMESPACES.len() { "privileged" } else { "baseline" }
                },
                "annotations": {
                    "owner.kq.dev/team": format!("team-{}", tenant),
                    "quota.kq.dev/cpu": format!("{}", 200 + (idx % 40) * 50),
                    "quota.kq.dev/memory": format!("{}Gi", 512 + (idx % 20) * 128),
                    "linkerd.io/inject": if idx % 5 == 0 { "enabled" } else { "disabled" }
                }
            },
            "status": {
                "phase": "Active"
            },
            "app": name,
            "product": product,
            "tenant": tenant
        });
        write_json_line(&mut writer, &namespace)?;
    }
    finish_gzip_writer(writer)
}

fn write_daemonsets(config: &SyntheticSnapshotConfig) -> Result<()> {
    let path = config.output_dir.join("daemonsets.ndjson.gz");
    let mut writer = gzip_line_writer(&path)?;
    for (idx, (name, namespace, component)) in DAEMONSET_WORKLOADS.iter().enumerate() {
        let unavailable = if idx % 5 == 0 { config.node_count / 200 } else { config.node_count / 1000 };
        let ready = config.node_count.saturating_sub(unavailable).min(i32::MAX as usize) as i32;
        let desired = config.node_count.min(i32::MAX as usize) as i32;
        let daemonset = json!({
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": stable_uid(&config.cluster_name, "daemonset", idx),
                "creationTimestamp": format_timestamp(config.timestamp - Duration::days(120 - (idx % 30) as i64)),
                "labels": {
                    "app": name,
                    "app.kubernetes.io/name": name,
                    "app.kubernetes.io/component": component,
                    "app.kubernetes.io/part-of": "cluster-platform",
                    "synthetic.kq.dev/cluster": config.cluster_name,
                    "workload.kq.dev/kind": "DaemonSet"
                },
                "annotations": {
                    "deployment.kubernetes.io/revision": format!("{}", 10 + idx),
                    "prometheus.io/scrape": "true",
                    "prometheus.io/port": "9100"
                }
            },
            "spec": {
                "revisionHistoryLimit": 10
            },
            "status": {
                "currentNumberScheduled": desired,
                "desiredNumberScheduled": desired,
                "numberReady": ready
            },
            "ready_percentage": if desired > 0 { (ready as f32 / desired as f32) * 100.0 } else { 0.0 }
        });
        write_json_line(&mut writer, &daemonset)?;
    }
    finish_gzip_writer(writer)
}

fn make_node_context(config: &SyntheticSnapshotConfig, rng: &mut SimpleRng, index: usize) -> NodeContext {
    let pool = rng.weighted(POOLS).to_string();
    let zone = rng.pick(ZONES).to_string();
    let shape = node_shape(&pool, rng);
    NodeContext {
        index,
        name: format!("{}-{}-node-{:05}", config.cluster_name, zone.replace('-', ""), index),
        pool,
        zone,
        instance_type: shape.instance_type,
        cpu: shape.cpu,
        memory_gib: shape.memory_gib,
        pod_capacity: shape.pods,
        ephemeral_gib: shape.ephemeral_gib,
    }
}

fn node_shape(pool: &str, rng: &mut SimpleRng) -> NodeShape {
    match pool {
        "gpu" => NodeShape {
            instance_type: "p4d.24xlarge",
            cpu: 96,
            memory_gib: 1152,
            pods: 110,
            ephemeral_gib: 900,
        },
        "highmem" => NodeShape {
            instance_type: "r7i.8xlarge",
            cpu: 32,
            memory_gib: 256,
            pods: 110,
            ephemeral_gib: 450,
        },
        "cpu" => NodeShape {
            instance_type: "c7i.8xlarge",
            cpu: 32,
            memory_gib: 64,
            pods: 110,
            ephemeral_gib: 300,
        },
        "stateful" => NodeShape {
            instance_type: "i4i.8xlarge",
            cpu: 32,
            memory_gib: 256,
            pods: 90,
            ephemeral_gib: 1900,
        },
        "infra" => NodeShape {
            instance_type: "m7i.4xlarge",
            cpu: 16,
            memory_gib: 64,
            pods: 80,
            ephemeral_gib: 250,
        },
        "spot" => NodeShape {
            instance_type: if rng.one_in(2) { "m7i.4xlarge" } else { "c7i.4xlarge" },
            cpu: 16,
            memory_gib: 64,
            pods: 100,
            ephemeral_gib: 250,
        },
        _ => NodeShape {
            instance_type: if rng.one_in(3) { "m7i.8xlarge" } else { "m7i.4xlarge" },
            cpu: if rng.one_in(3) { 32 } else { 16 },
            memory_gib: if rng.one_in(3) { 128 } else { 64 },
            pods: 110,
            ephemeral_gib: 300,
        },
    }
}

fn make_node(config: &SyntheticSnapshotConfig, node: &NodeContext) -> Value {
    let alloc_cpu = node.cpu.saturating_mul(950);
    let alloc_mem = node.memory_gib.saturating_mul(930);
    let mut labels = Map::new();
    insert(&mut labels, "topology.kq.dev/cluster", &config.cluster_name);
    insert(&mut labels, "node.kq.dev/pool", &node.pool);
    insert(&mut labels, "kubernetes.io/hostname", &node.name);
    insert(&mut labels, "kubernetes.io/os", "linux");
    insert(&mut labels, "kubernetes.io/arch", if node.index % 17 == 0 { "arm64" } else { "amd64" });
    insert(&mut labels, "topology.kubernetes.io/region", "us-east-1");
    insert(&mut labels, "topology.kubernetes.io/zone", &node.zone);
    insert(&mut labels, "node.kubernetes.io/instance-type", node.instance_type);
    insert(&mut labels, "synthetic.kq.dev/capacity-class", capacity_class(node));
    if node.pool == "infra" {
        insert(&mut labels, "node-role.kubernetes.io/infra", "");
    } else {
        insert(&mut labels, "node-role.kubernetes.io/worker", "");
    }

    let taints = match node.pool.as_str() {
        "gpu" => json!([{"key": "nvidia.com/gpu", "value": "true", "effect": "NoSchedule"}]),
        "spot" => json!([{"key": "capacity.kq.dev/spot", "value": "true", "effect": "NoSchedule"}]),
        "infra" => json!([{"key": "node-role.kubernetes.io/infra", "value": "true", "effect": "NoSchedule"}]),
        _ => Value::Null,
    };

    json!({
        "metadata": {
            "name": node.name,
            "uid": stable_uid(&config.cluster_name, "node", node.index),
            "creationTimestamp": format_timestamp(config.timestamp - Duration::days(30 + (node.index % 150) as i64)),
            "resourceVersion": format!("{}", 10_000_000 + node.index),
            "generation": 1,
            "labels": labels,
            "annotations": {
                "cluster-autoscaler.kubernetes.io/scale-down-disabled": if node.pool == "infra" { "true" } else { "false" },
                "volumes.kubernetes.io/controller-managed-attach-detach": "true",
                "csi.volume.kubernetes.io/nodeid": format!(r#"{{"ebs.csi.aws.com":"{}"}}"#, node.name),
                "synthetic.kq.dev/generated": "true"
            }
        },
        "spec": {
            "podCIDR": format!("10.{}.{}.0/24", (node.index / 255) % 255, node.index % 255),
            "podCIDRs": [format!("10.{}.{}.0/24", (node.index / 255) % 255, node.index % 255)],
            "providerID": format!("aws:///{}//i-{:016x}", node.zone, 0x1000_0000u64 + node.index as u64),
            "unschedulable": node.index % 503 == 0,
            "taints": taints
        },
        "status": {
            "phase": "Ready",
            "capacity": {
                "cpu": format!("{}", node.cpu),
                "memory": format!("{}Gi", node.memory_gib),
                "pods": format!("{}", node.pod_capacity),
                "ephemeral-storage": format!("{}Gi", node.ephemeral_gib),
                "hugepages-2Mi": if node.pool == "highmem" { "1024Mi" } else { "0" }
            },
            "allocatable": {
                "cpu": format!("{}m", alloc_cpu),
                "memory": format!("{}Mi", alloc_mem),
                "pods": format!("{}", node.pod_capacity.saturating_sub(5)),
                "ephemeral-storage": format!("{}Gi", node.ephemeral_gib.saturating_mul(9) / 10),
                "hugepages-2Mi": if node.pool == "highmem" { "1024Mi" } else { "0" }
            },
            "conditions": [
                {
                    "type": "Ready",
                    "status": if node.index % 997 == 0 { "False" } else { "True" },
                    "lastHeartbeatTime": format_timestamp(config.timestamp - Duration::minutes((node.index % 5) as i64)),
                    "lastTransitionTime": format_timestamp(config.timestamp - Duration::days((node.index % 60) as i64)),
                    "reason": if node.index % 997 == 0 { "KubeletNotReady" } else { "KubeletReady" },
                    "message": if node.index % 997 == 0 { "synthetic node temporarily not ready" } else { "kubelet is posting ready status" }
                },
                {"type": "MemoryPressure", "status": "False", "reason": "KubeletHasSufficientMemory", "message": "kubelet has sufficient memory available"},
                {"type": "DiskPressure", "status": "False", "reason": "KubeletHasNoDiskPressure", "message": "kubelet has no disk pressure"},
                {"type": "PIDPressure", "status": "False", "reason": "KubeletHasSufficientPID", "message": "kubelet has sufficient PID available"}
            ],
            "addresses": [
                {"type": "InternalIP", "address": format!("172.{}.{}.{}", (node.index / 65_000) % 255, (node.index / 255) % 255, node.index % 255)},
                {"type": "Hostname", "address": node.name}
            ],
            "nodeInfo": {
                "machineID": format!("{:032x}", 0xabcdu64 + node.index as u64),
                "systemUUID": stable_uid(&config.cluster_name, "system", node.index),
                "bootID": stable_uid(&config.cluster_name, "boot", node.index),
                "kernelVersion": "6.1.87",
                "osImage": "Ubuntu 22.04.4 LTS",
                "containerRuntimeVersion": "containerd://1.7.20",
                "kubeletVersion": "v1.30.5",
                "kubeProxyVersion": "v1.30.5",
                "operatingSystem": "linux",
                "architecture": if node.index % 17 == 0 { "arm64" } else { "amd64" }
            }
        },
        "hugepages_2mi": if node.pool == "highmem" { "1024Mi" } else { "0" },
        "hugepages_1gi": "0",
        "cluster": config.cluster_name,
        "pool": node.pool,
        "ready": node.index % 997 != 0
    })
}

fn capacity_class(node: &NodeContext) -> &'static str {
    if node.cpu >= 64 {
        "xlarge"
    } else if node.cpu >= 32 {
        "large"
    } else {
        "standard"
    }
}

fn pods_for_node(config: &SyntheticSnapshotConfig, rng: &mut SimpleRng) -> usize {
    let min = config.min_pods_per_node;
    let max = config.max_pods_per_node;
    if min == max {
        return min;
    }

    let spread = max - min;
    match rng.range_usize(0, 99) {
        0..=9 => min + rng.range_usize(0, spread.min(8)),
        10..=79 => min + rng.range_usize(0, spread),
        80..=94 => max.saturating_sub(rng.range_usize(0, (spread / 4).max(1))),
        _ => max,
    }
}

fn make_daemon_pod(
    config: &SyntheticSnapshotConfig,
    node: &NodeContext,
    daemon_idx: usize,
    ordinal: usize,
) -> Value {
    let (name, namespace, component) = DAEMONSET_WORKLOADS[daemon_idx];
    let pod_ctx = PodContext {
        ordinal,
        app_id: daemon_idx,
        app: name.to_string(),
        product: "cluster-platform".to_string(),
        tenant: "infra".to_string(),
        component: component.to_string(),
        namespace: namespace.to_string(),
        workload: WorkloadKind::SystemDaemon,
        phase: "Running",
        age_minutes: 60 * 24 * (20 + (node.index % 120) as i64),
        intended_lifespan_hours: 24 * 180,
        has_sidecar: daemon_idx % 3 == 0,
        restart_count: (node.index % 4) as i32,
        cpu_millis: if component == "metrics" { 80 } else { 120 },
        memory_mib: if component == "logging" { 256 } else { 128 },
        tier: "platform",
        release: "stable",
    };
    make_pod_with_name(
        config,
        node,
        &pod_ctx,
        format!("{}-{:05}", name, node.index),
        Some(("DaemonSet", name)),
    )
}

fn make_workload_pod_context(
    config: &SyntheticSnapshotConfig,
    namespaces: &[String],
    node: &NodeContext,
    rng: &mut SimpleRng,
    ordinal: usize,
) -> PodContext {
    let workload = choose_workload(rng, &node.pool);
    let app_count = (config.node_count / 2).clamp(64, 2_500);
    let app_id = rng.range_usize(0, app_count - 1);
    let tenant = rng.pick(TENANTS).to_string();
    let component = rng.pick(COMPONENTS).to_string();
    let product = format!("{}-svc-{:03}", tenant, app_id % 180);
    let app = format!("{}-{}", product, component);
    let namespace = namespaces[SYSTEM_NAMESPACES.len() + (app_id % (namespaces.len() - SYSTEM_NAMESPACES.len()).max(1))].clone();
    let (phase, age_minutes, intended_lifespan_hours) = lifecycle(rng, workload);
    let (cpu_millis, memory_mib) = resources_for(rng, workload, &node.pool);

    PodContext {
        ordinal,
        app_id,
        app,
        product,
        tenant,
        component,
        namespace,
        workload,
        phase,
        age_minutes,
        intended_lifespan_hours,
        has_sidecar: matches!(workload, WorkloadKind::Deployment | WorkloadKind::Canary) && !rng.one_in(3),
        restart_count: restart_count(rng, phase, workload),
        cpu_millis,
        memory_mib,
        tier: choose_tier(rng),
        release: choose_release(rng, workload),
    }
}

fn choose_workload(rng: &mut SimpleRng, pool: &str) -> WorkloadKind {
    if pool == "stateful" && !rng.one_in(3) {
        return WorkloadKind::StatefulSet;
    }
    if pool == "gpu" && !rng.one_in(4) {
        return WorkloadKind::Job;
    }
    match rng.range_usize(0, 99) {
        0..=69 => WorkloadKind::Deployment,
        70..=78 => WorkloadKind::StatefulSet,
        79..=88 => WorkloadKind::Job,
        89..=95 => WorkloadKind::CronJob,
        _ => WorkloadKind::Canary,
    }
}

fn lifecycle(rng: &mut SimpleRng, workload: WorkloadKind) -> (&'static str, i64, i64) {
    match workload {
        WorkloadKind::Deployment => {
            let phase = rare_non_running(rng, 96);
            let age_hours = if rng.one_in(6) {
                rng.range_usize(24 * 30, 24 * 180) as i64
            } else {
                rng.range_usize(1, 24 * 30) as i64
            };
            (phase, age_hours * 60, 24 * 90)
        }
        WorkloadKind::StatefulSet => {
            let phase = rare_non_running(rng, 98);
            let age_hours = rng.range_usize(24 * 7, 24 * 180) as i64;
            (phase, age_hours * 60, 24 * 180)
        }
        WorkloadKind::Job => match rng.range_usize(0, 99) {
            0..=62 => ("Succeeded", rng.range_usize(5, 36 * 60) as i64, 36),
            63..=79 => ("Running", rng.range_usize(1, 12 * 60) as i64, 24),
            80..=94 => ("Failed", rng.range_usize(5, 48 * 60) as i64, 24),
            _ => ("Pending", rng.range_usize(1, 120) as i64, 24),
        },
        WorkloadKind::CronJob => match rng.range_usize(0, 99) {
            0..=55 => ("Succeeded", rng.range_usize(5, 7 * 24 * 60) as i64, 24),
            56..=79 => ("Running", rng.range_usize(1, 180) as i64, 6),
            80..=91 => ("Failed", rng.range_usize(5, 24 * 60) as i64, 24),
            _ => ("Pending", rng.range_usize(1, 90) as i64, 6),
        },
        WorkloadKind::Canary => {
            let phase = rare_non_running(rng, 93);
            (phase, rng.range_usize(5, 48 * 60) as i64, 48)
        }
        WorkloadKind::SystemDaemon => ("Running", rng.range_usize(30 * 24 * 60, 180 * 24 * 60) as i64, 180 * 24),
    }
}

fn rare_non_running(rng: &mut SimpleRng, running_percent: usize) -> &'static str {
    let pick = rng.range_usize(0, 99);
    if pick < running_percent {
        "Running"
    } else if pick < running_percent + 2 {
        "Pending"
    } else if pick < 99 {
        "Failed"
    } else {
        "Unknown"
    }
}

fn resources_for(rng: &mut SimpleRng, workload: WorkloadKind, pool: &str) -> (u32, u32) {
    match (workload, pool) {
        (WorkloadKind::Job, "gpu") => (rng.range_u32(2_000, 8_000), rng.range_u32(8_192, 65_536)),
        (WorkloadKind::StatefulSet, _) => (rng.range_u32(500, 4_000), rng.range_u32(2_048, 32_768)),
        (WorkloadKind::Job, _) => (rng.range_u32(250, 3_000), rng.range_u32(512, 16_384)),
        (WorkloadKind::Canary, _) => (rng.range_u32(100, 1_000), rng.range_u32(256, 2_048)),
        _ => (rng.range_u32(100, 2_000), rng.range_u32(256, 8_192)),
    }
}

fn restart_count(rng: &mut SimpleRng, phase: &str, workload: WorkloadKind) -> i32 {
    if phase == "Failed" {
        return rng.range_usize(1, 12) as i32;
    }
    if matches!(workload, WorkloadKind::Job | WorkloadKind::CronJob) {
        return rng.range_usize(0, 2) as i32;
    }
    match rng.range_usize(0, 99) {
        0..=84 => 0,
        85..=96 => rng.range_usize(1, 3) as i32,
        _ => rng.range_usize(4, 20) as i32,
    }
}

fn choose_tier(rng: &mut SimpleRng) -> &'static str {
    match rng.range_usize(0, 99) {
        0..=9 => "critical",
        10..=39 => "tier-1",
        40..=74 => "tier-2",
        _ => "batch",
    }
}

fn choose_release(rng: &mut SimpleRng, workload: WorkloadKind) -> &'static str {
    if matches!(workload, WorkloadKind::Canary) {
        return "canary";
    }
    match rng.range_usize(0, 99) {
        0..=79 => "stable",
        80..=91 => "candidate",
        _ => "experiment",
    }
}

fn make_pod(config: &SyntheticSnapshotConfig, node: &NodeContext, pod: &PodContext) -> Value {
    let suffix = format!("{:05x}", pod.ordinal % 0x100000);
    let name = match pod.workload {
        WorkloadKind::StatefulSet => format!("{}-{}", pod.app, pod.ordinal % 16),
        WorkloadKind::Job => format!("{}-job-{}", pod.app, suffix),
        WorkloadKind::CronJob => format!("{}-cron-{}", pod.app, suffix),
        WorkloadKind::Canary => format!("{}-canary-{}", pod.app, suffix),
        _ => format!("{}-{}-{}", pod.app, pod.app_id % 10_000, suffix),
    };
    let owner_name = match pod.workload {
        WorkloadKind::StatefulSet => pod.app.as_str(),
        WorkloadKind::Job | WorkloadKind::CronJob => pod.app.as_str(),
        WorkloadKind::Canary | WorkloadKind::Deployment => pod.app.as_str(),
        WorkloadKind::SystemDaemon => pod.app.as_str(),
    };
    make_pod_with_name(config, node, pod, name, Some((owner_kind(pod.workload), owner_name)))
}

fn make_pod_with_name(
    config: &SyntheticSnapshotConfig,
    node: &NodeContext,
    pod: &PodContext,
    name: String,
    owner: Option<(&str, &str)>,
) -> Value {
    let created_at = config.timestamp - Duration::minutes(pod.age_minutes);
    let started_at = if pod.phase == "Pending" {
        Value::Null
    } else {
        json!(format_timestamp(created_at + Duration::seconds(15 + (pod.ordinal % 90) as i64)))
    };

    let labels = pod_labels(config, node, pod);
    let annotations = pod_annotations(config, node, pod);
    let containers = containers_for(pod);
    let container_statuses = container_statuses_for(pod, &containers);
    let node_selector = if node.pool == "general" && !matches!(pod.workload, WorkloadKind::SystemDaemon) {
        Value::Null
    } else {
        json!({"node.kq.dev/pool": node.pool})
    };

    let (reason, message) = status_reason_message(pod.phase, node);

    json!({
        "metadata": {
            "name": name,
            "namespace": pod.namespace,
            "uid": stable_uid(&config.cluster_name, "pod", pod.ordinal),
            "creationTimestamp": format_timestamp(created_at),
            "resourceVersion": format!("{}", 200_000_000 + pod.ordinal),
            "generation": 1,
            "labels": labels,
            "annotations": annotations,
            "ownerReferences": owner.map(|(kind, owner_name)| json!([{
                "apiVersion": owner_api_version(kind),
                "kind": kind,
                "name": owner_name,
                "uid": stable_uid(&config.cluster_name, kind, pod.app_id),
                "controller": true,
                "blockOwnerDeletion": true
            }])).unwrap_or(Value::Null)
        },
        "spec": {
            "nodeName": if pod.phase == "Pending" { Value::Null } else { json!(node.name) },
            "restartPolicy": if matches!(pod.workload, WorkloadKind::Job | WorkloadKind::CronJob) { "Never" } else { "Always" },
            "schedulerName": "default-scheduler",
            "priorityClassName": if pod.tier == "critical" { "system-cluster-critical" } else { "normal" },
            "priority": if pod.tier == "critical" { 1_000_000 } else { 0 },
            "serviceAccountName": format!("{}-sa", pod.product),
            "terminationGracePeriodSeconds": if matches!(pod.workload, WorkloadKind::Job | WorkloadKind::CronJob) { 30 } else { 90 },
            "dnsPolicy": "ClusterFirst",
            "runtimeClassName": if node.pool == "gpu" { json!("nvidia") } else { Value::Null },
            "hostNetwork": matches!(pod.workload, WorkloadKind::SystemDaemon),
            "hostPID": false,
            "hostIPC": false,
            "shareProcessNamespace": false,
            "nodeSelector": node_selector,
            "containers": containers,
            "initContainers": init_containers_for(pod),
            "volumes": volumes_for(pod),
            "affinity": affinity_for(node, pod),
            "tolerations": tolerations_for(node, pod),
            "securityContext": {
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "runAsNonRoot": !matches!(pod.workload, WorkloadKind::SystemDaemon),
                "fsGroup": 1000
            }
        },
        "status": {
            "phase": pod.phase,
            "reason": reason,
            "message": message,
            "nominatedNodeName": Value::Null,
            "hostIP": format!("172.{}.{}.{}", (node.index / 65_000) % 255, (node.index / 255) % 255, node.index % 255),
            "podIP": if pod.phase == "Pending" { Value::Null } else { json!(format!("10.{}.{}.{}", (pod.ordinal / 65_000) % 255, (pod.ordinal / 255) % 255, pod.ordinal % 255)) },
            "startTime": started_at,
            "qosClass": qos_class(pod),
            "podIPs": if pod.phase == "Pending" { Value::Null } else { json!([{"ip": format!("10.{}.{}.{}", (pod.ordinal / 65_000) % 255, (pod.ordinal / 255) % 255, pod.ordinal % 255)}]) },
            "conditions": conditions_for(config, pod),
            "containerStatuses": container_statuses,
            "initContainerStatuses": Value::Null
        },
        "namespace": pod.namespace,
        "node_name": if pod.phase == "Pending" { Value::Null } else { json!(node.name) },
        "phase": pod.phase,
        "cpu_request_total": total_cpu_millis(pod),
        "memory_request_total": total_memory_bytes(pod),
        "app": pod.app,
        "product": pod.product,
        "tenant": pod.tenant,
        "cluster": config.cluster_name,
        "pool": node.pool,
        "workload_kind": owner_kind(pod.workload)
    })
}

fn pod_labels(config: &SyntheticSnapshotConfig, node: &NodeContext, pod: &PodContext) -> Value {
    json!({
        "app": pod.app,
        "product": pod.product,
        "productTag": format!("{}.production", pod.product),
        "tenant.kq.dev/name": pod.tenant,
        "app.kubernetes.io/name": pod.app,
        "app.kubernetes.io/instance": format!("{}-{}", pod.app, pod.app_id % 10_000),
        "app.kubernetes.io/component": pod.component,
        "app.kubernetes.io/part-of": pod.product,
        "app.kubernetes.io/managed-by": if matches!(pod.workload, WorkloadKind::SystemDaemon) { "platform-operator" } else { "deployment-controller" },
        "app.kubernetes.io/version": format!("{}.{}.{}", 1 + pod.app_id % 5, pod.ordinal % 30, pod.ordinal % 100),
        "pod-template-hash": format!("{:x}", 0x100000 + pod.app_id % 0xfffff),
        "controller-revision-hash": format!("{:x}", 0x200000 + pod.app_id % 0xfffff),
        "release": pod.release,
        "service-tier": pod.tier,
        "workload.kq.dev/kind": owner_kind(pod.workload),
        "synthetic.kq.dev/cluster": config.cluster_name,
        "synthetic.kq.dev/workload-profile": workload_profile(pod.workload),
        "synthetic.kq.dev/lifecycle": pod.phase,
        "node.kq.dev/pool": node.pool,
        "topology.kubernetes.io/zone": node.zone
    })
}

fn pod_annotations(config: &SyntheticSnapshotConfig, node: &NodeContext, pod: &PodContext) -> Value {
    json!({
        "compute.kq.dev/platform-quota-id": stable_uid(&config.cluster_name, "quota", pod.app_id),
        "scheduler.kq.dev/node-selector": format!("node.kq.dev/pool={}", node.pool),
        "prometheus.io/scrape": if matches!(pod.workload, WorkloadKind::Job | WorkloadKind::CronJob) { "false" } else { "true" },
        "prometheus.io/port": if pod.component == "frontend" { "8080" } else { "9090" },
        "checksum/config": format!("{:x}", 0xfeed_cafeu64 + pod.app_id as u64),
        "deployment.kubernetes.io/revision": format!("{}", 1 + pod.ordinal % 200),
        "resource-spec-id": stable_uid(&config.cluster_name, "resource-spec", pod.app_id),
        "orchestration-intent-id": format!("urn:kq:synthetic:{}:{}", config.cluster_name, pod.ordinal),
        "synthetic.kq.dev/intended-lifespan-hours": format!("{}", pod.intended_lifespan_hours),
        "synthetic.kq.dev/snapshot-age-hours": format!("{}", pod.age_minutes / 60),
        "synthetic.kq.dev/applications": format!(
            r#"[{{"product":"{}","application":"{}","instance":"i{:03}","version":"1.{}.{}","container-name":"{}"}}]"#,
            pod.product,
            pod.app,
            pod.app_id % 999,
            pod.app_id % 30,
            pod.ordinal % 100,
            pod.component
        ),
        "sidecar.istio.io/status": if pod.has_sidecar { r#"{"version":"1.22","containers":["istio-proxy"]}"# } else { "" },
        "kubectl.kubernetes.io/restartedAt": if pod.restart_count > 0 { format_timestamp(config.timestamp - Duration::hours((pod.ordinal % 72) as i64)) } else { "".to_string() }
    })
}

fn containers_for(pod: &PodContext) -> Value {
    let mut containers = vec![container_json(
        &pod.component,
        &format!("registry.kq.dev/{}/{}:{}", pod.product, pod.component, pod.ordinal % 1000),
        pod.cpu_millis,
        pod.memory_mib,
        pod,
        8080,
    )];

    if pod.has_sidecar {
        containers.push(container_json(
            "istio-proxy",
            "registry.kq.dev/platform/istio-proxy:1.22.3",
            100,
            128,
            pod,
            15090,
        ));
    }

    if matches!(pod.workload, WorkloadKind::Deployment | WorkloadKind::StatefulSet) && pod.ordinal % 7 == 0 {
        containers.push(container_json(
            "metrics-sidecar",
            "registry.kq.dev/platform/metrics-sidecar:2.8.1",
            50,
            96,
            pod,
            9090,
        ));
    }

    Value::Array(containers)
}

fn container_json(
    name: &str,
    image: &str,
    cpu_millis: u32,
    memory_mib: u32,
    pod: &PodContext,
    port: i32,
) -> Value {
    json!({
        "name": name,
        "image": image,
        "command": ["/bin/app"],
        "args": ["--env=production", format!("--tenant={}", pod.tenant)],
        "workingDir": "/app",
        "imagePullPolicy": "IfNotPresent",
        "ports": [{"name": "http", "containerPort": port, "protocol": "TCP"}],
        "env": [
            {"name": "FABRIC", "value": "prod"},
            {"name": "TENANT", "value": pod.tenant},
            {"name": "PRODUCT", "value": pod.product},
            {"name": "SERVICE_TIER", "value": pod.tier}
        ],
        "resources": {
            "requests": {
                "cpu": format!("{}m", cpu_millis),
                "memory": format!("{}Mi", memory_mib)
            },
            "limits": {
                "cpu": format!("{}m", cpu_millis.saturating_mul(2).max(cpu_millis + 100)),
                "memory": format!("{}Mi", memory_mib.saturating_mul(2))
            }
        },
        "volumeMounts": [
            {"name": "config", "mountPath": "/etc/app", "readOnly": true},
            {"name": "tmp", "mountPath": "/tmp", "readOnly": false}
        ]
    })
}

fn container_statuses_for(pod: &PodContext, containers: &Value) -> Value {
    let Some(items) = containers.as_array() else {
        return Value::Null;
    };
    Value::Array(
        items
            .iter()
            .filter_map(|container| container.get("name").and_then(Value::as_str).map(|name| {
                json!({
                    "name": name,
                    "ready": pod.phase == "Running",
                    "restartCount": pod.restart_count,
                    "image": container.get("image").cloned().unwrap_or(Value::Null),
                    "imageID": format!("registry.kq.dev/sha256:{:064x}", 0xabcdu64 + pod.ordinal as u64),
                    "containerID": if pod.phase == "Pending" { Value::Null } else { json!(format!("containerd://{:064x}", 0x1234u64 + pod.ordinal as u64)) },
                    "started": pod.phase == "Running"
                })
            }))
            .collect(),
    )
}

fn init_containers_for(pod: &PodContext) -> Value {
    if matches!(pod.workload, WorkloadKind::Deployment | WorkloadKind::StatefulSet) && pod.ordinal % 5 == 0 {
        json!([{"name": "init-config", "image": "registry.kq.dev/platform/init-config:1.4.0"}])
    } else {
        Value::Null
    }
}

fn volumes_for(pod: &PodContext) -> Value {
    let mut volumes = vec![json!({"name": "config"}), json!({"name": "tmp"})];
    if matches!(pod.workload, WorkloadKind::StatefulSet) {
        volumes.push(json!({"name": "data"}));
    }
    Value::Array(volumes)
}

fn affinity_for(node: &NodeContext, pod: &PodContext) -> Value {
    if node.pool == "general" && !matches!(pod.workload, WorkloadKind::StatefulSet | WorkloadKind::SystemDaemon) {
        return Value::Null;
    }

    json!({
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{
                    "matchExpressions": [{
                        "key": "node.kq.dev/pool",
                        "operator": "In",
                        "values": [node.pool]
                    }],
                    "matchFields": null
                }]
            }
        },
        "podAffinity": null,
        "podAntiAffinity": null
    })
}

fn tolerations_for(node: &NodeContext, pod: &PodContext) -> Value {
    let mut tolerations = Vec::new();
    if node.pool == "gpu" {
        tolerations.push(json!({"key": "nvidia.com/gpu", "operator": "Equal", "value": "true", "effect": "NoSchedule"}));
    }
    if node.pool == "spot" {
        tolerations.push(json!({"key": "capacity.kq.dev/spot", "operator": "Equal", "value": "true", "effect": "NoSchedule"}));
    }
    if matches!(pod.workload, WorkloadKind::SystemDaemon) || node.pool == "infra" {
        tolerations.push(json!({"key": "node-role.kubernetes.io/infra", "operator": "Exists", "effect": "NoSchedule"}));
        tolerations.push(json!({"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300}));
    }
    if tolerations.is_empty() {
        Value::Null
    } else {
        Value::Array(tolerations)
    }
}

fn conditions_for(config: &SyntheticSnapshotConfig, pod: &PodContext) -> Value {
    let ready = pod.phase == "Running";
    let transition = format_timestamp(config.timestamp - Duration::minutes(pod.age_minutes.min(24 * 60)));
    json!([
        {"type": "PodScheduled", "status": if pod.phase == "Pending" { "False" } else { "True" }, "lastTransitionTime": transition},
        {"type": "Initialized", "status": if pod.phase == "Pending" { "False" } else { "True" }, "lastTransitionTime": transition},
        {"type": "ContainersReady", "status": if ready { "True" } else { "False" }, "lastTransitionTime": transition},
        {"type": "Ready", "status": if ready { "True" } else { "False" }, "lastTransitionTime": transition}
    ])
}

fn status_reason_message(phase: &str, node: &NodeContext) -> (Value, Value) {
    match phase {
        "Pending" => (
            json!("Unschedulable"),
            json!(format!("0/{} nodes available: insufficient cpu or unmatched selector for pool {}", 5_000, node.pool)),
        ),
        "Failed" => (
            json!("Error"),
            json!("synthetic workload exited non-zero after retries"),
        ),
        "Unknown" => (
            json!("NodeLost"),
            json!("synthetic node heartbeat was missed"),
        ),
        _ => (Value::Null, Value::Null),
    }
}

fn owner_kind(workload: WorkloadKind) -> &'static str {
    match workload {
        WorkloadKind::StatefulSet => "StatefulSet",
        WorkloadKind::Job | WorkloadKind::CronJob => "Job",
        WorkloadKind::SystemDaemon => "DaemonSet",
        WorkloadKind::Deployment | WorkloadKind::Canary => "ReplicaSet",
    }
}

fn owner_api_version(kind: &str) -> &'static str {
    match kind {
        "Job" => "batch/v1",
        _ => "apps/v1",
    }
}

fn workload_profile(workload: WorkloadKind) -> &'static str {
    match workload {
        WorkloadKind::Deployment => "long-running",
        WorkloadKind::StatefulSet => "stateful",
        WorkloadKind::Job => "batch",
        WorkloadKind::CronJob => "scheduled-batch",
        WorkloadKind::Canary => "canary",
        WorkloadKind::SystemDaemon => "system-daemon",
    }
}

fn qos_class(pod: &PodContext) -> &'static str {
    if pod.cpu_millis < 150 && pod.memory_mib < 256 {
        "BestEffort"
    } else if matches!(pod.workload, WorkloadKind::StatefulSet) {
        "Guaranteed"
    } else {
        "Burstable"
    }
}

fn total_cpu_millis(pod: &PodContext) -> i64 {
    let mut total = pod.cpu_millis as i64;
    if pod.has_sidecar {
        total += 100;
    }
    if matches!(pod.workload, WorkloadKind::Deployment | WorkloadKind::StatefulSet) && pod.ordinal % 7 == 0 {
        total += 50;
    }
    total
}

fn total_memory_bytes(pod: &PodContext) -> i64 {
    let mut memory_mib = pod.memory_mib as i64;
    if pod.has_sidecar {
        memory_mib += 128;
    }
    if matches!(pod.workload, WorkloadKind::Deployment | WorkloadKind::StatefulSet) && pod.ordinal % 7 == 0 {
        memory_mib += 96;
    }
    memory_mib * 1024 * 1024
}

fn namespace_names(count: usize) -> Vec<String> {
    let mut names = SYSTEM_NAMESPACES
        .iter()
        .take(count)
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();

    let extra = count.saturating_sub(names.len());
    for idx in 0..extra {
        names.push(format!("team-{:03}-prod", idx));
    }

    if names.len() <= SYSTEM_NAMESPACES.len() {
        while names.len() <= SYSTEM_NAMESPACES.len() {
            names.push(format!("team-{:03}-prod", names.len()));
        }
    }
    names
}

fn gzip_line_writer(path: &Path) -> Result<GzEncoder<BufWriter<File>>> {
    let file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    Ok(GzEncoder::new(BufWriter::new(file), Compression::fast()))
}

fn finish_gzip_writer(writer: GzEncoder<BufWriter<File>>) -> Result<()> {
    let mut file = writer.finish().context("failed to finish gzip stream")?;
    file.flush().context("failed to flush gzip stream")
}

fn write_json_line<W: Write>(writer: &mut W, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).context("failed to serialize JSON object")?;
    writer.write_all(b"\n").context("failed to write newline")
}

fn insert(map: &mut Map<String, Value>, key: &str, value: &str) {
    map.insert(key.to_string(), Value::String(value.to_string()));
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn stable_uid(cluster: &str, kind: &str, ordinal: usize) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in cluster.bytes().chain(kind.bytes()).chain(ordinal.to_le_bytes()) {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (hash >> 32) as u32,
        (hash >> 16) as u16,
        hash as u16,
        ((hash >> 48) as u16) | 0x4000,
        hash & 0x0000_ffff_ffff_ffff
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir, name: &str) -> SyntheticSnapshotConfig {
        SyntheticSnapshotConfig {
            output_dir: dir.path().join(name),
            cluster_name: "unit-a".to_string(),
            node_count: 8,
            min_pods_per_node: 3,
            max_pods_per_node: 6,
            namespace_count: 12,
            seed: 7,
            overwrite: false,
            timestamp: "2026-05-12T00:00:00Z".parse().unwrap(),
        }
    }

    fn read_gzip_text(path: &Path) -> String {
        let file = File::open(path).unwrap();
        let mut decoder = GzDecoder::new(file);
        let mut text = String::new();
        decoder.read_to_string(&mut text).unwrap();
        text
    }

    fn read_gzip_json_lines(path: &Path) -> Vec<Value> {
        read_gzip_text(path)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn generates_expected_files_and_counts() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir, "snapshot");

        let summary = generate_ndjson_snapshot(&config).unwrap();
        assert_eq!(summary.node_count, 8);
        assert!(summary.pod_count >= 24);
        assert!(summary.pod_count <= 48);
        assert!(summary.min_pods_per_node >= 3);
        assert!(summary.max_pods_per_node <= 6);
        assert!(config.output_dir.join("metadata.json").exists());
        assert!(config.output_dir.join("nodes.ndjson.gz").exists());
        assert!(config.output_dir.join("pods.ndjson.gz").exists());
        assert!(config.output_dir.join("namespaces.ndjson.gz").exists());
        assert!(config.output_dir.join("daemonsets.ndjson.gz").exists());
    }

    #[test]
    fn rejects_invalid_config_before_creating_output_directory() {
        let dir = TempDir::new().unwrap();
        let config = SyntheticSnapshotConfig {
            node_count: 0,
            ..test_config(&dir, "invalid")
        };

        let err = generate_ndjson_snapshot(&config).unwrap_err();

        assert!(err.to_string().contains("node_count must be greater than zero"));
        assert!(!config.output_dir.exists());
    }

    #[test]
    fn refuses_to_write_into_non_empty_directory_without_overwrite() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir, "existing");
        fs::create_dir_all(&config.output_dir).unwrap();
        let stale_path = config.output_dir.join("stale.txt");
        fs::write(&stale_path, "keep me").unwrap();

        let err = generate_ndjson_snapshot(&config).unwrap_err();

        assert!(err.to_string().contains("output directory is not empty"));
        assert_eq!(fs::read_to_string(stale_path).unwrap(), "keep me");
    }

    #[test]
    fn overwrite_replaces_stale_output_directory() {
        let dir = TempDir::new().unwrap();
        let config = SyntheticSnapshotConfig {
            overwrite: true,
            ..test_config(&dir, "overwrite")
        };
        fs::create_dir_all(&config.output_dir).unwrap();
        let stale_path = config.output_dir.join("stale.txt");
        fs::write(&stale_path, "remove me").unwrap();

        let summary = generate_ndjson_snapshot(&config).unwrap();

        assert_eq!(summary.output_dir, config.output_dir);
        assert!(!stale_path.exists());
        assert!(config.output_dir.join("metadata.json").exists());
    }

    #[test]
    fn same_seed_and_timestamp_generate_same_ndjson_content() {
        let dir = TempDir::new().unwrap();
        let config_a = test_config(&dir, "deterministic-a");
        let config_b = SyntheticSnapshotConfig {
            output_dir: dir.path().join("deterministic-b"),
            ..config_a.clone()
        };

        generate_ndjson_snapshot(&config_a).unwrap();
        generate_ndjson_snapshot(&config_b).unwrap();

        for file in [
            "daemonsets.ndjson.gz",
            "namespaces.ndjson.gz",
            "nodes.ndjson.gz",
            "pods.ndjson.gz",
        ] {
            assert_eq!(
                read_gzip_text(&config_a.output_dir.join(file)),
                read_gzip_text(&config_b.output_dir.join(file)),
                "{file} changed between identical synthetic configs"
            );
        }
    }

    #[test]
    fn summary_phase_counts_match_generated_pod_lines() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir, "phase-counts");

        let summary = generate_ndjson_snapshot(&config).unwrap();
        let pods = read_gzip_json_lines(&config.output_dir.join("pods.ndjson.gz"));

        let count_phase = |phase: &str| {
            pods.iter()
                .filter(|pod| pod["status"]["phase"].as_str() == Some(phase))
                .count()
        };
        assert_eq!(pods.len(), summary.pod_count);
        assert_eq!(count_phase("Running"), summary.running_pods);
        assert_eq!(count_phase("Pending"), summary.pending_pods);
        assert_eq!(count_phase("Succeeded"), summary.succeeded_pods);
        assert_eq!(count_phase("Failed"), summary.failed_pods);
        assert_eq!(count_phase("Unknown"), summary.unknown_pods);
    }
}
