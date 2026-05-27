# 新手贡献者上手指南

给第一次接触 kq 的贡献者：项目是做什么的、代码怎么组织的、有哪些
适合新手切入的小到中等规模的项目。

## 一段话介绍 kq

kq 把一个 Kubernetes 集群的瞬时快照（pods、nodes、namespaces、daemonsets）
加载到 Apache Arrow 表里，然后让你用 SQL 查询。不需要起数据库，不会
对在线 API server 增加压力。抓一次快照，就可以离线跑任意多次查询。
抓的时候打上标签，就可以同时加载多个快照、横向对比集群。

项目刻意保持小而专注，只做快照分析——边界请看
[CONTRIBUTING](../CONTRIBUTING.md#ground-rules)。

## 把代码跑起来

```bash
bazel build -c opt //kq:kq
bazel test //kq/...
```

构建一律走 Bazel（Bzlmod）。**不要**用 `cargo build` / `cargo test`——
Cargo 只是 crate 元数据的事实来源。

如果手头没有真实集群，可以用 synthetic 生成器造一个快照来跑：

```bash
bazel run -c opt //kq/tools:synthetic_snapshot -- \
  --output /tmp/kq-demo --cluster demo --nodes 100 --namespaces 20 --overwrite
bazel-bin/kq/kq /tmp/kq-demo
```

完整的搭建步骤和 SQL 示例在 [Usage 指南](USAGE.md)。

## 60 秒心智模型

每次跑 CLI 都走同一条路径：

```
flags → engine_setup → loader → Arrow RecordBatches → query → output
```

1. `kq/main.rs` 解析快照路径和命令行参数。
2. `engine_setup/` 构造 loader 并读取所有输入。
3. `loader/` 按路径自动识别格式：单文件 `.json` / `.json.gz`、NDJSON
   目录、Arrow IPC 目录、Parquet 目录。
4. 每种资源都变成一个 Arrow `RecordBatch`。
5. `query/` 把 batch 注册成 DataFusion `MemTable`，对外暴露
   `pods` / `nodes` / `namespaces` / `daemon_sets` 四个视图，并注册
   Kubernetes 相关的 UDF（`parse_cpu`、`parse_memory`、
   `extract_pool` 等）。
6. `output/` 把结果渲染成 table / JSON / CSV / TSV / compact。

完整的目录结构和架构不变量在
[Developer 指南](DEVELOPMENT.md) 和
[CLAUDE.md](../CLAUDE.md#architecture)。

## 想做 X 该看哪里

| 你想做的事…                              | 看这里             |
| ---------------------------------------- | ------------------ |
| 加一种新的资源类型                       | `kq/loader/`、`kq/schema/`、`kq/query/` |
| 加一个 SQL 函数                          | `kq/query/`        |
| 加一种新的输出格式                       | `kq/output/`       |
| 加一个开发者辅助工具                     | `kq/tools/`        |
| 改造 synthetic 生成器                    | `kq/synthetic/`    |
| 加一个集成测试                           | `kq/tests/`        |

## 适合新手的方向

下面这些都按"一两个 PR 内能合掉"的尺寸控制过。都不需要深入 DataFusion
或 Arrow 内部。

### 1. 加一个 Kubernetes 相关的 SQL 函数（最小）

kq 已经有 `parse_cpu`、`parse_memory`、`extract_pool` 这类 UDF，还有
不少可以加的：

- `parse_duration(string)` 处理 pod age 这类字符串。
- `container_image_registry(image)` 从 image reference 里抽 registry。
- `is_system_namespace(name)` 判断常见的 `kube-*` 那一票。
- `node_ready(conditions)` 从 node status 里读 Ready 条件。

为什么适合第一个 PR：改动只在 `kq/query/` 内、容易单测、对写查询的
人立刻有用。

### 2. 加一种新的输出格式

现有格式都在 `kq/output/`：table、JSON、CSV、TSV、compact。可以考虑:

- **Markdown 表格** —— 贴 PR 和 runbook 方便。
- **JSON Lines (`jsonl`)** —— 一行一条记录，便于喂给下游工具。

为什么适合第一个 PR：模块自洽，模仿目录里现有的写法即可，测试面就是
"渲染这个 batch、检查字节"。

### 3. 加一种新的资源类型（"游览"项目）

目前 loader 支持 pods、nodes、namespaces、daemonsets。加一种新的
——`deployments`、`services`、`persistentvolumeclaims`、`events` 都行
——会带你过一遍整个代码栈：

- 在 `kq/schema/` 加新的 Arrow schema。
- 在 `kq/loader/` 加一条新的加载路径，并在 resource-table 里登记。
- 在 `kq/query/` 注册新视图，必要时把分析常用列扁平化出来。
- 扩展 `kq/synthetic/`，让生成器能造这种资源。
- 在 `kq/tests/` 加集成测试 fixture。

为什么适合第二个 PR：做完这一圈，整条数据流你就摸通了。

### 4. 常见运维场景的 SQL Cookbook

[`scripts/demo_synthetic_multicluster_queries.sh`](../scripts/demo_synthetic_multicluster_queries.sh)
是这个模式的范例：生成 N 个 synthetic 集群，跑七条典型的 fleet 查询。
还有空间补更多 recipe：

- **容量评审**：每个 pool 的 request vs allocatable、每个 node 的余量。
- **吵闹邻居**：每个 node 上 CPU / 内存 request 前几大的 pod。
- **卫生检查**：没有设 resource request 的 pod、被钉死在单 node 上的
  pod、卡在非 `Running` 阶段的 pod。

为什么适合第一个 PR：纯文档加 SQL、不用动 Rust，但产出对所有用户都
有用。

### 5. 专门的快照采集 CLI（中等）

现在 README 是教用户抄一段 `kubectl ... | jq '{...}'` 来生成快照。能跑，
但有几处尖角：

- 顶层 `timestamp` 必填，忘了 loader 就拒绝。
- `cluster` 标签得分别打在 pods 和 nodes 上；打到 namespaces 或
  daemonsets 上是静默 no-op。
- 在大集群上跑全量 `kubectl get -o json` 可能会 OOM。
- 这条 recipe 只能产出单文件 JSON，没法直接生成更高效的 NDJSON 或
  Parquet 目录格式——尽管 kq 已经有对应的 writer
  （`write_ipc_directory`、`write_parquet_directory`）。

新建一个 `kq/tools/snapshot_collect.rs` 可以做的事：

- 用 [`kube`](https://crates.io/crates/kube) crate 直接访问集群，不再
  shell 调用 `kubectl`；list 调用分页，把内存控住。
- 自动打好 `timestamp` 和每种资源的 `cluster` 标签，用户不用再记。
- 用 `--format json|ndjson|parquet` 选输出格式，直接复用已有 writer。
- 可选支持多个 kube context（`--context a,b,c`），一次跑下来给每个快照
  里的 pods 和 nodes 打上正确的 cluster 名字。

为什么适合做一个中等规模的 PR：边界清楚（从 kube API 读，交给现成的
writer 写），不用碰查询引擎，而且能去掉上手流程里一个实实在在的痛点。

### 6. 快照 diff（更大——想挑战自己的时候做）

两个快照算 delta：哪些 pod 新出现、消失、重启、迁移到了别的 node。
可以做成一个新的 `kq/tools/snapshot_diff.rs` 二进制，也可以做成一组
按 `metadata.uid` join 两个快照的 SQL helper。这个项目最具产品形态——
动手前先开 issue 讨论一下设计面。

## 该挑哪个？

第一次贡献的话，先做 **1** 或 **2**——小、独立、一坐就能合。然后 **3**
是最好的方式把架构吃进脑子里。**4** 适合 Kubernetes 比 Rust 强的人。
**5** 等你对代码库熟了之后做，杠杆最大。

做 **3** 及以上之前请先开 issue 把打算怎么做写出来——在方向上给反馈
比在成品 PR 上给反馈要容易得多。

## 开 PR 之前

- 跑过 `bazel build -c opt //kq:kq` 和 `bazel test //kq/...`。
- 涉及 loader、schema、query-registration、output 的改动，再跑一遍
  [CLAUDE.md](../CLAUDE.md#when-changing-loader--schema--query-registration--output)
  里指定的聚焦套件。
- 读一下 [Pull Request checklist](../CONTRIBUTING.md#pull-request-checklist)。
- 确认新加的 fixture 都是 synthetic 的、可以放在公开仓库里。

欢迎入伙。
