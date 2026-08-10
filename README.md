# 黄区 SIFT1M 多供数离线测试套件

版本：1.4.1

## 1. 目标与边界

本套件用于在 aarch64 EulerOS 黄区完成以下闭环：

```text
SIFT1M
  → Spark、PyIceberg 或 Rust fixture 生成完整 Iceberg v2 snapshot
  → iceberg_catalog.create_table 创建字段级 `vector_dim=128` 的 Catalog 表
  → 将 Catalog `metadata_location/current_snapshot_id` 切换到 fixture
  → IVF-Flat 或 IVF-PQ 索引（默认 IVF-PQ）
  → 串行冒烟或 K×DOP×扫描模式性能矩阵
  → 官方 GT Recall、QPS、p50/p95/p99
```

三条供数路径使用同一数据契约：

- Iceberg schema：`id long`、`embedding list<float>`；
- ID：`1..1,000,000`；
- 表属性：字符串 `vector_dim.embedding=128`，用于审计；
- fixture 定位：最新 metadata 文件的绝对 `file:///` URI；
- 数据库入口：Catalog 主动建表后切换到 fixture metadata，保留原外表和 `relid`。

Catalog 的原生自动映射契约位于 Iceberg schema 字段：

```json
{"id": 2, "name": "embedding", "type": {"type": "list", "element": "float"}, "vector_dim": 128}
```

PyIceberg 0.11.1 和当前 Rust Iceberg SDK 的 `NestedField` 均没有 `vector_dim` 字段，
序列化时无法保留该扩展。Spark Iceberg schema 同样不生成该字段。套件在
`iceberg_catalog.create_table` 的 schema JSON 中提供字段级 `vector_dim=128`，producer
metadata 的 `vector_dim.embedding=128` 保留为数据契约审计属性。

套件不创建、加载或检查 `iceberg_delta` 扩展及其伴生表，也不依赖 Delta。目标 backend
已激活 Delta hook 时，Catalog 在 `create_table` 过程中使用同一份字段级向量 schema；hook
未激活时只创建基础表。两种环境均执行相同的 fixture 接入、数据扫描和向量查询流程。

## 2. 三条供数路径

| 项目 | Spark | PyIceberg | Rust fixture |
|---|---|---|---|
| 主要用途 | 跨引擎兼容性、性能供数 | Python 独立 producer、链路冒烟 | 与 bridge 锁定 SDK 一致的 fixture |
| 前置依赖 | JDK、Spark、Iceberg runtime | Python venv、锁定 wheelhouse | bridge 工作树、Rust 1.96、Cargo 离线缓存 |
| Catalog | HadoopCatalog | 临时 SQLite Catalog | MemoryCatalog + LocalFs |
| 内存策略 | Spark 分区写 | 每批默认 131072 行 | 每批默认 131072 行 |
| 分区 | `bucket(id, N)` | `bucket(id, N)` | `bucket(id, N)` |
| producer metadata 字段级 `vector_dim` | 不支持 | 不支持 | 不支持 |
| 数据库入口 | `create_table` + metadata 切换 | `create_table` + metadata 切换 | `create_table` + metadata 切换 |

PyIceberg 每次 `append`、Rust fixture 每个输入批次都会为涉及的分区生成数据文件。
两条路径的分区表文件数通常多于 Spark。
各 producer 的结果用于验证互操作；性能数值只有在 Parquet 文件数、大小、压缩、
partition spec、snapshot 数、索引参数和硬件一致时才能直接比较。

## 3. 文件结构

```text
sift1m-yellow-mvp/
├── README.md
├── VERSION
├── config/
│   ├── mvp.env.example          # 32 bucket、串行查询和索引构建
│   └── perf.env.example         # 32 bucket、1024 clusters、8 workers
├── requirements/pyiceberg-lock.txt
├── wheelhouse/                  # 离线 Python wheels 和 SHA256SUMS
├── downloads/                   # 四个 SIFT1M 文件
├── checksums/SHA256SUMS
├── state/                       # metadata、日志和结果
├── bin/
│   ├── download-sift1m.sh
│   ├── download-pyiceberg-wheelhouse.sh
│   ├── install-pyiceberg-offline.sh
│   ├── verify-sift1m.sh
│   ├── preflight.sh
│   ├── deploy.sh
│   ├── clean.sh
│   ├── clean-index-artifacts.py
│   ├── supply-data.sh
│   ├── seed-sift1m.sh           # Spark
│   ├── seed-sift1m-pyiceberg.sh
│   ├── seed-sift1m-rust.sh
│   ├── register-table.sh
│   ├── verify-table.sh
│   ├── configure-index.sh
│   ├── build-index.sh
│   ├── test-fullscan.sh
│   ├── test-index.sh
│   ├── run-clean-test.sh
│   ├── benchmark.py
│   ├── run-matrix.py
│   └── make-offline-bundle.sh
├── src/
│   ├── seed_sift1m.py
│   ├── seed_sift1m_pyiceberg.py
│   └── seed_sift1m_rust.rs
└── tests/
```

## 4. 准备离线制品

### 4.1 SIFT1M

GitHub 源码仓库不保存 SIFT1M 数据对象。`downloads/` 保留固定目录及 Git LFS 规则；
黄区将已有数据复制到该目录后，再提交到 CodeHub。四个文件仍须通过本节大小和 SHA-256
门禁。

在联网机器执行：

```bash
bash bin/download-sift1m.sh
```

也可从以下页面下载后放入 `downloads/`：

- <https://huggingface.co/datasets/qbo-odp/sift1m/tree/main>
- <https://gitee.com/hf-datasets/sift1m>

必须包含：

| 文件 | 字节数 |
|---|---:|
| `sift_base.fvecs` | 516000000 |
| `sift_query.fvecs` | 5160000 |
| `sift_groundtruth.ivecs` | 4040000 |
| `sift_learn.fvecs` | 51600000 |

执行 `bash bin/verify-sift1m.sh` 校验大小和 SHA-256。Git LFS 指针、截断文件和错误内容
会立即失败。

### 4.2 PyIceberg wheelhouse

在与黄区相同架构、相同 Python major/minor 和兼容 glibc 的联网机器执行：

```bash
MVP_WHEELHOUSE_PYTHON=/usr/bin/python3 \
  bash bin/download-pyiceberg-wheelhouse.sh
```

脚本只接受 binary wheel，并生成 `wheelhouse/SHA256SUMS`。锁定环境为 PyIceberg 0.11.1、
PyArrow 24.0.0 和 SQLAlchemy 2.0.46。不要在 x86_64 主机下载后传给 aarch64 黄区。

### 4.3 Spark 制品

Spark 路径需要与目标配置相符的 JDK、Spark 和 Iceberg Spark runtime。当前示例为 JDK
17、Spark 3.5.9、Scala 2.12、Iceberg runtime 1.11.0。套件通过绝对路径引用这些制品，
不把 Spark 发行包复制进测试包。

### 4.4 Rust fixture 制品

Rust 路径复用与黄区 bridge 构建相同的完整工作树、Cargo.lock 和本地 SDK path
dependency。联网区先执行一次同一 bridge 的 release 构建并准备 Cargo 离线缓存；黄区
配置 `MVP_BRIDGE_SOURCE`。脚本只在 bridge `examples/` 下创建一个临时符号链接，退出时
删除，不改动 Cargo.toml 和源码。

## 5. 配置与前置检查

套件提供两份用途不同的配置：

| 配置 | 用途 | clusters | 构建 worker | 数据文件目标 | 默认测试规模 |
|---|---|---:|---:|---:|---:|
| `mvp.env.example` | 部署、供数、索引和查询功能回归 | 256 | 1 | 分批供数，文件数可多于 32 | 100 queries |
| `perf.env.example` | 可比较的 SIFT1M 性能与 Recall 测试 | 1024 | 8 | 32 buckets、约 32 个数据文件 | 矩阵每场景 100 queries；正式 Recall 10000 |

功能回归配置：

```bash
cp config/mvp.env.example mvp.env
vi mvp.env
```

性能测试必须从 `perf.env.example` 创建 `mvp.env`，并使用新的 namespace、table 和
空 warehouse 从 0 供数。使用 MVP 配置得到的索引构建时间、查询延迟和 DOP 数据不作为
性能基线。

```bash
cp config/perf.env.example mvp.env
vi mvp.env
bash bin/run-clean-test.sh fresh pyiceberg
python3 bin/run-matrix.py --output-dir state/matrix
```

PyIceberg 是黄区已验证的默认性能供数路径。perf 配置使用一个 1000000 行批次，使
PyIceberg 和 Rust fixture 通常分别生成 32 个分区数据文件；Spark 使用 32 个写入任务。
测试报告必须记录实际 Parquet 文件数和字节数，只有落盘布局一致的结果才能直接比较。

### 5.1 一键测试

首次部署或从 0 重新供数：

```bash
bash bin/run-clean-test.sh fresh spark
# 或：fresh pyiceberg / fresh rust
```

复用已接入的 Iceberg 数据，只清理并重建索引、重跑查询：

```bash
bash bin/run-clean-test.sh reuse spark
# provider 参数用于对应环境预检，不会重新供数
```

两种模式均执行以下流程：

```text
环境预检 → Catalog/FDW 部署检查
  fresh: 全量清理 → producer 供数 → Catalog 接入
  reuse: 结果清理 → 索引清理
→ 表复用门禁 → 全表测试
→ IVF-Flat 建索引和测试 → 索引清理
→ IVF-PQ 建索引和测试
```

流程结束时保留 IVF-PQ 索引和 PQ 配置。总日志写入
`state/run-clean-test.log`，各模块保留独立日志或 JSON 结果。

### 5.2 清理级别

```bash
bash bin/clean.sh index
bash bin/clean.sh results
bash bin/clean.sh all
```

| 级别 | 清理内容 | 保留内容 | 用途 |
|---|---|---|---|
| `index` | Catalog 索引定义、失去引用的 Registry Puffin 和 segment artifact | 外表、Parquet、snapshot、当前空 Registry、查询结果 | Flat/PQ 切换 |
| `results` | benchmark JSON、矩阵和运行日志 | Catalog 表、Iceberg 数据、`metadata_location.txt`、provider/table 定位文件 | 复用供数数据重测 |
| `all` | 当前测试表的 Catalog 记录、两种 producer 表目录、bootstrap metadata、运行状态 | `downloads/` 中的 SIFT1M 原始文件、环境配置 | 从 0 重新供数 |

`index` 使用 `iceberg_catalog.drop_index` 更新 metadata head，再调用
`iceberg_catalog.vacuum_index` 回收可识别的索引文件。随后脚本校验当前 Registry
为空、文件大小/SHA-256/table UUID 和路径边界均正确，并删除维护接口跳过的残留
segment。最终索引目录只保留当前 metadata 引用的空 Registry。`all` 只删除
`MVP_WAREHOUSE_DIR` 下与当前 namespace/table 精确匹配的 Spark/PyIceberg 和 Rust
表目录，并删除当前表的 bootstrap 目录。

同一 SIFT1M 数据集在 schema、分区、压缩和 producer 版本保持一致时可持续复用。
索引参数或 `nprobe` 变化只需要执行 `reuse`。供数布局或 producer 版本变化时执行
`fresh`。

### 5.3 模块入口

| 模块 | 命令 | 作用 |
|---|---|---|
| 环境验证 | `bash bin/preflight.sh <spark\|pyiceberg\|rust\|all>` | 校验架构、producer、数据库、bridge/Catalog 安装副本和 SIFT 文件 |
| 部署 | `bash bin/deploy.sh` | 创建并验证 `iceberg_catalog`、`iceberg_fdw` 及索引清理接口 |
| 清理 | `bash bin/clean.sh <index\|results\|all>` | 按上表清理测试状态 |
| 供数分派 | `bash bin/supply-data.sh <spark\|pyiceberg\|rust>` | 调用对应 producer |
| Catalog 接入 | `bash bin/register-table.sh` | Catalog 建向量表并切换 fixture metadata |
| 表验证 | `bash bin/verify-table.sh` | 校验 head、relid、向量类型及数据范围 |
| 索引切换 | `bash bin/configure-index.sh <flat\|pq>` | 原子更新 mvp.env 中的索引名、类型和 implementation |
| 建索引 | `bash bin/build-index.sh` | 按当前配置建索引并校验 Catalog 状态 |
| 全表测试 | `bash bin/test-fullscan.sh` | 执行串行全扫 Recall 与延迟测试 |
| 索引测试 | `bash bin/test-index.sh <flat\|pq> [quick\|recall]` | 校验当前索引契约后执行快速测试或完整 Recall |
| 总流程 | `bash bin/run-clean-test.sh <fresh\|reuse> <provider>` | 编排上述模块 |

### 5.4 主要参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `MVP_WAREHOUSE_DIR` | 配置文件指定 | 裸绝对 warehouse 路径 |
| `MVP_NAMESPACE` / `MVP_TABLE` | 配置文件指定 | 专用于本次测试的 Catalog 表 |
| `MVP_VECTOR_TYPE` | `floatvector` | 查询 literal cast；建表实际类型由 Catalog 字段级 `vector_dim=128` 决定 |
| `MVP_PARTITION_BUCKETS` | 32 | 三条 producer 共用的 `bucket(id, N)` 分区数；设为 0 创建非分区表 |
| `MVP_DATA_FILES` | 8 / 32 | MVP / perf 的 Spark 写入任务数 |
| `MVP_PYICEBERG_BATCH_ROWS` | 131072 / 1000000 | MVP / perf 的 PyIceberg 输入批行数 |
| `MVP_RUST_BATCH_ROWS` | 131072 / 1000000 | MVP / perf 的 Rust fixture 输入批行数 |
| `MVP_INDEX_NAME` | `idx_sift_ivfpq` | 当前索引名称 |
| `MVP_INDEX_TYPE` | `ivf_pq` | Catalog index type |
| `MVP_INDEX_IMPLEMENTATION` | `ivf_pq` | index ABI implementation |
| `MVP_NUM_CLUSTERS` | 256 / 1024 | MVP 功能回归 / perf 性能基线的聚类数 |
| `MVP_SAMPLE_RATE` | 100000 | 索引训练采样数 |
| `MVP_BUILD_WORKERS` | 1 / 8 | 串行 / 性能配置的构建 worker 数 |
| `MVP_NPROBE` | 10 | 索引查询探测簇数 |
| `MVP_TEST_NQ` | 100 | 一键测试查询数 |
| `MVP_TEST_K` | 10 | 一键测试 Top-K |
| `MVP_TEST_WARMUP` | 5 | 每种扫描模式的预热查询数 |
| `MVP_RECALL_NQ` | 10000 | 正式 Recall 使用的完整 SIFT query 数 |
| `MVP_MATRIX_NQ` | 100 | 性能矩阵每个场景、每轮的查询数 |
| `MVP_MATRIX_ROUNDS` | 1 | 性能矩阵重复轮数 |
| `MVP_MATRIX_K` | 10,100,1000,10000 | 性能矩阵 Top-K 集合 |
| `MVP_MATRIX_DOP` | 1,2,4,8 | 性能矩阵 DOP 集合 |
| `MVP_MATRIX_MODES` | index,fullscan | 性能矩阵扫描模式 |
| `MVP_MATRIX_WARMUP` | 5 | 性能矩阵每个场景的预热 query 数 |

`MVP_PARTITION_BUCKETS` 控制 Iceberg 文件布局和并行任务划分；
`MVP_NUM_CLUSTERS` 控制 IVF 向量聚类。`mvp.env.example` 使用 256 clusters 缩短功能
回归构建时间，`perf.env.example` 使用 1024 clusters 作为性能基线。两份配置均显式
使用 `sample_rate=100000` 和 `nprobe=10`。

蓝区 x86_64 功能验证显式设置 `MVP_ALLOW_NON_AARCH64=1`。黄区保持默认 aarch64
门禁，蓝区结果只用于功能验证。

每条 producer 首次供数使用独立、空的 `MVP_WAREHOUSE_DIR`、`MVP_NAMESPACE` 和 `MVP_TABLE`。
例如 Spark 使用 `sift_spark_part.sift1m_part`，PyIceberg 使用
`sift_pyiceberg_part.sift1m_part`。

按需运行预检：

```bash
bash bin/preflight.sh spark
bash bin/preflight.sh pyiceberg
bash bin/preflight.sh rust
bash bin/preflight.sh all
```

预检会记录架构、producer 版本、数据库连接、runtime jar/bridge/Catalog 哈希和
warehouse 权限。若输出多份 bridge `.so`，先用 `readelf`、`ldd` 和
`/proc/<pid>/maps` 确认实际加载副本。

## 6. Spark 供数

```bash
source mvp.env
bash bin/seed-sift1m.sh
```

脚本使用 `env -u LD_LIBRARY_PATH spark-submit`，按 516 字节定长记录解析 SIFT base，
创建 Iceberg v2 表并写入字符串属性 `vector_dim.embedding=128`。完成后校验行数、属性
和最新 metadata，再写入 `state/metadata_location.txt`。

Spark 表目录已存在时供数器拒绝运行。重复测试使用新目录和新表名。

## 7. PyIceberg 供数

### 7.1 离线安装

```bash
MVP_BASE_PYTHON="$(command -v python3)" bash bin/install-pyiceberg-offline.sh
```

安装器先校验 wheelhouse SHA-256，再以 `--no-index` 创建包内 `.venv`，最后运行
`pip check`。把输出的 Python 路径写入 `mvp.env` 的 `MVP_PYTHON_BIN`。

### 7.2 写入

```bash
source mvp.env
bash bin/seed-sift1m-pyiceberg.sh
```

PyIceberg 供数器执行以下门禁：

1. 校验 base 文件精确大小及每条记录的 128 维头；
2. 拒绝复用已有表目录；
3. 使用临时 SQLite Catalog 和显式本地表 location；
4. 分批构造 `long + list<required float>` Arrow 表；
5. 创建可选的 `bucket(id, N)` 分区；
6. 在创建时写入 `format-version=2` 和审计属性 `vector_dim.embedding=128`；
7. 校验最终 metadata 的 schema、partition spec、snapshot 和属性；
8. 输出 Parquet 文件数、总字节数及最新 metadata URI。

供数器不修改已发布 metadata，也不操作数据库 Catalog。统一接入脚本负责 Catalog 建表和
metadata 切换。

## 8. Rust fixture 供数

```bash
source mvp.env
bash bin/seed-sift1m-rust.sh
```

脚本复用 bridge 的 Cargo.lock，以 `--offline --locked --release` 编译套件内 Rust
example。供数器流式验证 516 字节 fvecs 记录，按 Iceberg `bucket(id, N)` 变换拆分每个
输入批次，为每个分区绑定 `PartitionKey`，再提交一个 Iceberg v2 snapshot。完成后校验
partition spec 和每个 FileScanTask 的 partition 值并输出最新 metadata URI。默认
`MVP_PARTITION_BUCKETS=32`；设为 `0` 时创建非分区表。Rust SDK schema 仍为
`long + list<float>`；`vector_dim.embedding=128` 是表级审计属性。

## 9. 统一 fixture 接入门禁

任一 producer 供数完成后执行：

```bash
bash bin/register-table.sh
```

脚本名为 `register-table.sh` 以保持离线包目录和既有调用方式稳定，实际流程为：

1. 校验 fixture snapshot、`id long`、`embedding list<float>`、`bucket(id, N)` partition spec 和表级审计属性；
2. 在独立 bootstrap 位置调用 `create_table`，schema 字段显式携带 `vector_dim=128`；
3. 保留 Catalog 创建的外表、`relid` 及可选 Delta 伴生表；
4. 将 `tables_internal.metadata_location` 和 `current_snapshot_id` 切换到 fixture；
5. 校验数据范围、基础列类型和 Catalog 表头。

共同通过条件：

- fixture metadata 顶层审计属性是字符串 `vector_dim.embedding=128`；
- Catalog 建表 schema 的 `embedding` 字段含整数 `vector_dim=128`；
- 最终外表是 `id bigint, embedding vector(128)` 或 `floatvector(128)`；
- `tables_internal.relid` 指向最终外表；
- `count=1000000, min(id)=1, max(id)=1000000`。

producer metadata 可以不含字段级 `vector_dim`；向量 SQL 类型来自 Catalog 主动建表 schema。
`MVP_CATALOG_BOOTSTRAP_DIR` 可覆盖空表的临时位置，默认使用
`MVP_WAREHOUSE_DIR/.catalog-bootstrap`。

## 10. 构建索引

index type 与 implementation 使用以下固定映射：

| `MVP_INDEX_TYPE` | `MVP_INDEX_IMPLEMENTATION` | index ABI 选择结果 |
|---|---|---|
| `ivf_flat` | `ivf` | `builtin.ivf_flat@2` |
| `ivf_pq` | `ivf_pq` | `builtin.ivf_pq@1` |
| `btree` | `btree` | `BTree` |

构建脚本拒绝表中未列出的组合。IVF-PQ 是 mvp.env 默认路径。PQ 与 Flat 一键切换命令为：

```bash
bash bin/configure-index.sh pq
bash bin/configure-index.sh flat
```

切换脚本同时更新索引名、type 和 implementation。Flat 使用
`idx_sift_ivfflat + ivf_flat + ivf`，PQ 使用
`idx_sift_ivfpq + ivf_pq + ivf_pq`。

```bash
bash bin/build-index.sh
```

串行冒烟配置保持 256 clusters、100000 sample、1 worker。`config/perf.env.example` 使用
指南基线 1024 clusters、100000 sample、8 workers。索引状态必须为 `active`，Catalog
中的 type/implementation 必须与当前配置一致。构建后门禁读取当前 metadata 指向的
Registry Puffin，校验 Registry 大小、SHA-256、canonical implementation、`active` 状态、
artifact 前缀、文件大小和落盘位置。墙钟耗时分别保存在
`state/build-index-flat.log` 和 `state/build-index-pq.log`。

历史结果与新版性能基线参数不同，不能直接合并。调整 `nprobe` 时固定数据 snapshot 和
索引，只修改外表 option。

## 11. 正确性和性能测试

### 11.1 全扫正确性

模块入口：

```bash
bash bin/test-fullscan.sh
```

等价的细粒度命令：

```bash
python3 bin/benchmark.py \
  --mode fullscan --query-dop 1 --nq 100 --k 10 \
  --output state/fullscan.json
```

前 100 条查询的距离阈值 Recall@10 必须为 1.0。

### 11.2 索引 Recall 和延迟

模块入口：

```bash
bash bin/test-index.sh pq
# 当前配置和活动索引为 Flat 时：bash bin/test-index.sh flat
```

上述快速入口使用 100 条 query。正式 Recall 使用官方 SIFT1M 的全部 10000 条 query
及相同序号的 ground truth：

```bash
bash bin/test-index.sh pq recall
# Flat 索引：bash bin/test-index.sh flat recall
```

等价的细粒度命令：

```bash
python3 bin/benchmark.py \
  --mode index --query-dop 1 --nq 100 --k 10 --nprobe 10 \
  --output state/index-nprobe10.json
```

索引计划必须包含 `Vector Search` 和 `bridge vector index scan`。脚本输出官方 GT 的 ID
Recall、等距容忍 Recall、QPS、mean、p50/p95/p99、逐查询耗时和完整计划。

### 11.3 K×DOP 矩阵

分区表执行：

```bash
python3 bin/run-matrix.py --output-dir state/matrix
```

默认矩阵为 K=`10,100,1000,10000`、DOP=`1,2,4,8`、index/fullscan、每个场景
100 条 query、1 轮和 5 条预热 query。默认值读取 `MVP_MATRIX_*`；需要比较重复轮次
波动时设置 `MVP_MATRIX_ROUNDS=3` 或传入 `--rounds 3`。

DOP>1 默认要求计划出现对应的 `LOCAL GATHER dop: 1/N`。未分区表只执行 `--dop 1`；
`--allow-serial-fallback` 仅用于诊断，带该选项的结果不能声明为并行结果。

SIFT 官方 GT 只包含 top-100。K≤100 计算官方 Recall；K>100 自动使用
`--skip-recall`，只输出性能。报告中不得把 K>100 标记为官方召回率。

完整矩阵开销较高，计划和链路诊断可先执行单查询子集：

```bash
python3 bin/run-matrix.py --k 10,100 --dop 1,8 --rounds 1 --nq 1
```

单查询结果用于功能诊断，不用于 p50/p95/p99 或完整 Recall 结论。

## 12. 结果有效性

| 检查项 | 要求 |
|---|---|
| 数据 | 四个文件大小和 SHA-256 正确 |
| 表 | 1,000,000 行、128 维、一基 ID |
| metadata | 最终 snapshot 的具体绝对 URI |
| 维度 | 数据均为 128 维；记录字段级属性和表级审计属性 |
| SQL 类型 | Catalog `create_table` 原生创建 `vector(128)` 或 `floatvector(128)` |
| 索引 | type/implementation 符合固定映射，`index_status=active` |
| 串行计划 | Vector Search 和实际 bridge scan mode 正确 |
| 并行计划 | DOP>1 出现对应 LOCAL GATHER |
| 快速正确性 | 全扫前 100 条距离阈值 Recall@10=1.0 |
| 正式 Recall | 使用全部 10000 条 query 和官方同序号 ground truth；K≤100 |
| 布局 | 记录 producer、分区、Parquet 文件数/字节数和压缩 |
| 环境 | 记录数据库版本、组件 commit 和运行时 `.so` 哈希 |

`benchmark.py` 的 gsql `\timing` 表示同一会话中的语句端到端时间，包含 bridge I/O。
需要与指南的 `EXPLAIN ANALYZE Total runtime` 比较时，两种口径分别保存，不混合计算。

## 13. 生成完整离线包

下载并校验数据、按需准备 wheelhouse 后执行：

```bash
# 三种 producer
MVP_OFFLINE_PROVIDER=all bash bin/make-offline-bundle.sh

# 仅 Spark
MVP_OFFLINE_PROVIDER=spark bash bin/make-offline-bundle.sh

# 仅 PyIceberg
MVP_OFFLINE_PROVIDER=pyiceberg bash bin/make-offline-bundle.sh

# 仅 Rust fixture
MVP_OFFLINE_PROVIDER=rust bash bin/make-offline-bundle.sh
```

完整包包含 SIFT1M 数据；PyIceberg 模式还强制包含经过 SHA-256 校验的 wheelhouse。
`state/`、`.venv/` 和 Python cache 不进入包。Spark/JDK/runtime 使用 `mvp.env` 中的黄区
绝对路径。

## 14. 常见故障

### 基础表或 Delta 伴生表为 `text`

确认安装的 Catalog 支持 `create_table` schema 字段级 `vector_dim`，并检查
`state/register-table.log`。接入脚本不会从 producer metadata 推导 SQL 类型，也不操作或
检查 Delta。Catalog 主动建表避免基础表与 hook 生成对象分别取自不同 schema；后续数据
扫描和向量查询负责验证实际执行链路。

### DOP 不生效

确认表的默认 partition spec 含 `bucket[32]`，每个 FileScanTask 带 partition 信息，
`query_dop>1`，计划中出现 `LOCAL GATHER dop: 1/N`。未分区表始终是串行基线。

### PyIceberg wheel 无法安装

wheel 必须匹配目标架构、Python ABI 和 glibc。回到同构联网机器重新生成 wheelhouse，
不要在黄区启用网络索引或现场编译 PyArrow。

### Spark 或 PyIceberg 被数据库动态库污染

所有 producer 命令均清除 `LD_LIBRARY_PATH`。数据库环境脚本只用于数据库命令，不在
producer 前重新 source。

### Rust fixture 找不到 example 或依赖

确认 `MVP_BRIDGE_SOURCE` 指向完整 bridge 工作树，Rust 工具链满足其 `rust-version`，并且
联网区已经用同一 Cargo.lock 填充黄区离线缓存。脚本拒绝覆盖 bridge 中已有的同名 example。

## 15. 来源

- [Iceberg 向量索引端到端测试指南](https://github.com/Doreami/infra-compile-scripts/blob/main/%E7%AB%AF%E5%88%B0%E7%AB%AF%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95/%E7%AB%AF%E5%88%B0%E7%AB%AF%E6%B5%8B%E8%AF%95%E6%8C%87%E5%8D%97.md)
- [PyIceberg SqlCatalog API](https://py.iceberg.apache.org/reference/pyiceberg/catalog/sql/)
- [PyIceberg Table append API](https://py.iceberg.apache.org/reference/pyiceberg/table/)
- `../type-and-deployment-contract.md`
- `../spark-supply-guide.md`
