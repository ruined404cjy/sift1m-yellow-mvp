# 黄区 SIFT1M 多供数与 GIST1M 性能测试套件

版本：1.6.0

套件保留 SIFT1M 的既有命令和默认配置，并提供独立的 GIST1M 配置、状态目录及
Spark/Bridge/PyIceberg 性能入口。GIST1M 的准备、参数和执行说明见
[`docs/gist1m-yellow.md`](docs/gist1m-yellow.md)。

## 1. 目标与边界

本套件用于在 aarch64 EulerOS 黄区完成以下闭环：

```text
SIFT1M
  → Spark v3、Bridge ABI v3、PyIceberg v2 或 Rust SDK v2 生成完整 Iceberg snapshot
  → metadata 顶层属性写入 `vector_dim.embedding=128`
  → Catalog #116 原生 register_table 注册 producer metadata
  → IVF-Flat 或 IVF-PQ 索引（默认 IVF-PQ）
  → 串行冒烟或 K×DOP×扫描模式性能矩阵
  → 官方 GT Recall、QPS、p50/p95/p99
```

四条供数路径使用同一数据契约：

- Iceberg schema：`id long`、`embedding list<float>`；
- ID：`1..1,000,000`；
- 表属性：字符串 `vector_dim.embedding=128`，用于 Catalog 向量类型映射和审计；
- fixture 定位：最新 metadata 文件的绝对 `file:///` URI；
- 数据库入口：`iceberg_catalog.register_table(namespace, table, metadata_location)`。

Catalog #116 的注册契约位于 metadata 顶层表属性：

```json
{"properties": {"vector_dim.embedding": "128"}}
```

各 producer 的 Iceberg schema 保持标准 `list<float>`。`register_table` 读取表属性，把
`embedding` 映射为数据库 `vector(128)`，并在 Catalog schema 记录中保存字段维度；producer
metadata 不需要字段级扩展。套件不直接更新 Catalog 内部表。

套件只创建和检查 `iceberg_catalog`、`iceberg_fdw`，不调用 Delta 供数或 Delta 接口。

## 2. 四条供数路径

| 项目 | Spark | Bridge ABI | PyIceberg | Rust SDK |
|---|---|---|---|---|
| 主要用途 | v3 跨引擎基线 | v3 DataInfra ABI 基线 | Python v2 兼容验证 | SDK v2 兼容验证 |
| 前置依赖 | JDK、Spark、Iceberg runtime | bridge 工作树、Rust、Cargo 离线缓存 | Python venv、锁定 wheelhouse | bridge 工作树、Rust、Cargo 离线缓存 |
| producer Catalog | HadoopCatalog | Bridge managed table | 临时 SQLite Catalog | MemoryCatalog + LocalFs |
| 内存策略 | Spark 分区写 | 分批读取并暂存 32 个分区流 | 可配置输入批次 | 可配置输入批次 |
| 分区 | `bucket(id, N)` | `bucket(id, N)` | `bucket(id, N)` | `bucket(id, N)` |
| Iceberg 格式 | v3 | v3 | v2 | v2 |
| 表属性 `vector_dim.embedding` | 支持 | 支持 | 支持 | 支持 |
| 数据库入口 | 原生 `register_table` | 原生 `register_table` | 原生 `register_table` | 原生 `register_table` |

性能基线的文件切分规则为：`bucket(id, 32)` 表在一个 snapshot 中生成 32 个数据
文件，即每个非空 bucket 一个文件。1 GiB 目标文件大小高于 SIFT/GIST 单 bucket 的
未压缩向量载荷。Spark 使用一次 append、Iceberg hash distribution 和 32 个 shuffle
partition；Bridge 先把输入分流为 32 个 Arrow IPC 流，再逐 bucket 调用分区写入 ABI。
两条 v3 路径写完后均强制校验实际文件数为 32。PyIceberg 和 Rust SDK 每个输入批次会为
涉及的 bucket 生成文件，只有单批覆盖全量数据时通常满足 32 文件目标。各 producer 的性能数值
只有在 Parquet 文件数、大小、压缩、partition spec、snapshot 数、索引参数和硬件一致时
才能直接比较。

## 3. 文件结构

```text
sift1m-yellow-mvp/
├── README.md
├── VERSION
├── config/
│   ├── mvp.env.example          # 32 bucket、串行查询和索引构建
│   ├── perf.env.example         # SIFT 32 bucket、1024 clusters、8 workers
│   └── gist-perf.env.example    # GIST 独立性能配置
├── docs/gist1m-yellow.md        # GIST 数据准备、参数和执行说明
├── requirements/pyiceberg-lock.txt
├── wheelhouse/                  # 离线 Python wheels 和 SHA256SUMS
├── downloads/                   # SIFT1M 或 GIST1M 原始文件
├── checksums/SHA256SUMS
├── state/                       # metadata、日志和结果
├── bin/
│   ├── download-sift1m.sh
│   ├── download-gist1m.sh
│   ├── download-pyiceberg-wheelhouse.sh
│   ├── install-pyiceberg-offline.sh
│   ├── verify-sift1m.sh
│   ├── verify-gist1m.sh
│   ├── init-env.sh
│   ├── preflight.sh
│   ├── deploy.sh
│   ├── clean.sh
│   ├── clean-index-artifacts.py
│   ├── supply-data.sh
│   ├── seed-spark.sh            # SIFT/GIST 公共 Spark v3 producer
│   ├── seed-sift1m.sh           # SIFT 兼容入口
│   ├── seed-sift1m-pyiceberg.sh
│   ├── seed-gist1m-pyiceberg.sh
│   ├── seed-sift1m-rust.sh
│   ├── seed-bridge.sh           # SIFT/GIST 公共 Bridge ABI v3 producer
│   ├── register-table.sh
│   ├── verify-table.sh
│   ├── configure-index.sh
│   ├── build-index.sh
│   ├── test-fullscan.sh
│   ├── test-index.sh
│   ├── run-clean-test.sh
│   ├── run-perf.sh
│   ├── run-gist-perf.sh
│   ├── benchmark.py
│   ├── run-matrix.py
│   └── make-offline-bundle.sh
├── src/
│   ├── seed_sift1m.py
│   ├── seed_sift1m_pyiceberg.py
│   ├── seed_sift1m_rust.rs
│   └── seed_fvecs_bridge.rs
└── tests/
```

## 4. 准备离线制品

### 4.1 SIFT1M

GitHub 源码仓库不保存 SIFT1M 数据对象。黄区联网下载或从联网区传入四个文件后，
仍须通过本节大小和 SHA-256 门禁。

在可访问 Hugging Face 的黄区服务器执行：

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

执行 `bash bin/verify-sift1m.sh` 校验大小和 SHA-256。Git LFS 指针、截断文件和错误内容会立即失败。

### 4.2 PyIceberg wheelhouse

在与黄区相同架构、相同 Python major/minor 和兼容 glibc 的联网机器执行：

```bash
MVP_WHEELHOUSE_PYTHON=/usr/bin/python3 \
  bash bin/download-pyiceberg-wheelhouse.sh
```

脚本只接受 binary wheel，并生成 `wheelhouse/SHA256SUMS`。锁定环境为 PyIceberg 0.11.1、PyArrow 24.0.0 和 SQLAlchemy 2.0.46。不要在 x86_64 主机下载后传给 aarch64 黄区。

### 4.3 Spark 制品

Spark 路径需要与目标配置相符的 JDK、Spark 和 Iceberg Spark runtime。当前示例为 JDK 17、Spark 3.5.9、Scala 2.12、Iceberg runtime 1.11.0。套件通过绝对路径引用这些制品，不把 Spark 发行包复制进测试包。

### 4.4 Rust fixture 制品

Rust 路径复用与黄区 bridge 构建相同的完整工作树、Cargo.lock 和本地 SDK path dependency。联网区先执行一次同一 bridge 的 release 构建并准备 Cargo 离线缓存；黄区配置 `MVP_BRIDGE_SOURCE`。脚本只在 bridge `examples/` 下创建一个临时符号链接，退出时删除，不改动 Cargo.toml 和源码。

## 5. 配置与前置检查

套件提供两份用途不同的配置：

| 配置 | 用途 | clusters | 构建 worker | 数据文件目标 | 默认测试规模 |
|---|---|---:|---:|---:|---:|
| `mvp.env.example` | 部署、供数、索引和查询功能回归 | 256 | 1 | Spark 32；分批 producer 可多于 32 | 前 100 条 queries |
| `perf.env.example` | 可比较的 SIFT1M 性能与 Recall 测试 | 1024 | 8 | 32 buckets、约 32 个数据文件 | 全量中等距采样 100 queries；正式 Recall 10000 |

首次使用通过配置初始化入口创建 `mvp.env`。脚本不会覆盖已有配置；当前 shell 已显式导出的同名参数会写入新配置。选择 PyIceberg 且包内已有 `.venv` 时自动配置其 Python 路径；仅有完整 wheelhouse 时自动调用锁定离线安装器；两者均缺失时给出 wheelhouse 准备和安装命令。

```bash
# 创建配置，并立即检查 PyIceberg 路径、数据库和公共依赖。
bash bin/init-env.sh mvp pyiceberg
# 性能环境使用：bash bin/init-env.sh perf pyiceberg
```

无法确定的 producer 路径、GAUSSHOME/gsql、warehouse、namespace 和 table 由脚本列为人工核对项。编辑 `mvp.env` 后重新执行 `bash bin/preflight.sh <provider>`，所有错误均以非零状态退出并给出具体配置项。

功能回归配置：

```bash
bash bin/init-env.sh mvp
vi mvp.env
```

性能测试必须从 `perf.env.example` 创建 `mvp.env`，并使用新的 namespace、table 和空 warehouse 从 0 供数。使用 MVP 配置得到的索引构建时间、查询延迟和 DOP 数据不作为性能基线。

```bash
bash bin/init-env.sh perf
vi mvp.env
bash bin/run-perf.sh
```

SIFT 一键入口继续默认使用 PyIceberg；要求 Iceberg v3 时显式选择 `spark`。perf 配置使
PyIceberg 和 Rust fixture 单批覆盖 1000000 行，通常生成 32 个分区数据文件；Spark
按公共文件切分规则强制生成 32 个文件。perf 的 100-query 测试从官方 10000 条
query 中含首尾等距取序号 `0,101,202,...,9999`，并用同一组序号读取官方 ground truth。
测试报告必须记录实际 Parquet 文件数和字节数。

### 5.1 MVP 与 perf 一键测试

MVP 一键入口默认执行 `fresh pyiceberg`：

```bash
bash bin/run-clean-test.sh
```

首次部署或从 0 重新供数：

```bash
bash bin/run-clean-test.sh fresh pyiceberg
# 或：fresh spark / fresh bridge / fresh rust
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

流程结束时保留 IVF-PQ 索引和 PQ 配置。总日志写入 `state/run-clean-test.log`，各模块保留独立日志或 JSON 结果。

perf 一键入口同样默认执行 `fresh pyiceberg`：

```bash
bash bin/run-perf.sh
# 复用已供数据：bash bin/run-perf.sh reuse pyiceberg
```

perf 流程执行环境检查、部署、供数或复用门禁，然后依次运行 IVF-Flat、IVF-PQ 和无索引全扫。每条路径默认执行 K=`10,100` × DOP=`1,8` 的 4 个场景，共 12 个场景；每个场景从官方 query 中含首尾等距取 100 条，预热 5 条。DOP>1 必须通过 `LOCAL GATHER` 计划门禁。结果分别保存到 `state/perf/flat`、`state/perf/pq` 和 `state/perf/fullscan`，总日志保存到 `state/run-perf.log`。PQ 测试完成后清理索引，再运行全扫；流程结束时不保留索引，索引配置保持 PQ。

`run-perf.sh` 是日常代表性性能验证入口。完整的 K=`10,100,1000,10000` × DOP=`1,2,4,8` × index/fullscan 矩阵继续使用 `run-matrix.py`。

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
| `all` | 当前测试表的 Catalog 记录、producer 表目录、运行状态 | `downloads/` 中的 SIFT1M 原始文件、环境配置 | 从 0 重新供数 |

`index` 使用 `iceberg_catalog.drop_index` 更新 metadata head，确认 Catalog 索引记录归零，
再校验当前 Registry 为空、文件大小/SHA-256/table UUID 和路径边界均正确，并删除残留
segment。当前 Registry v2 的 generation-aware GC 尚未启用，套件不调用
`iceberg_catalog.vacuum_index`。最终索引目录只保留当前 metadata 引用的空 Registry。
`all` 只删除 `MVP_WAREHOUSE_DIR` 下与当前 namespace/table 精确匹配的 producer 表目录。

同一 SIFT1M 数据集在 schema、分区、压缩和 producer 版本保持一致时可持续复用。索引参数或 `nprobe` 变化只需要执行 `reuse`。供数布局或 producer 版本变化时执行 `fresh`。

### 5.3 模块入口

| 模块 | 命令 | 作用 |
|---|---|---|
| 配置初始化 | `bash bin/init-env.sh <mvp\|perf> [provider]` | 创建配置、应用当前 shell 显式参数，并按需立即预检 |
| 环境验证 | `bash bin/preflight.sh <spark\|bridge\|pyiceberg\|rust\|all>` | 校验架构、producer、数据库、bridge/Catalog 安装副本和数据文件 |
| 部署 | `bash bin/deploy.sh` | 创建并验证 `iceberg_catalog`、`iceberg_fdw`、原生注册及索引接口 |
| 清理 | `bash bin/clean.sh <index\|results\|all>` | 按上表清理测试状态 |
| 供数分派 | `bash bin/supply-data.sh <spark\|bridge\|pyiceberg\|rust>` | 调用当前数据集支持的 producer |
| Catalog 接入 | `bash bin/register-table.sh` | 原生注册 producer metadata 并校验向量映射 |
| 表验证 | `bash bin/verify-table.sh` | 校验 head、relid、向量类型及数据范围 |
| 索引切换 | `bash bin/configure-index.sh <flat\|pq>` | 原子更新 mvp.env 中的索引名、类型和 implementation |
| 建索引 | `bash bin/build-index.sh` | 按当前配置建索引并校验 Catalog 状态 |
| 全表测试 | `bash bin/test-fullscan.sh` | 执行串行全扫 Recall 与延迟测试 |
| 索引测试 | `bash bin/test-index.sh <flat\|pq> [quick\|recall]` | 校验当前索引契约后执行快速测试或完整 Recall |
| MVP 总流程 | `bash bin/run-clean-test.sh [fresh\|reuse] [provider]` | 默认 `fresh pyiceberg`，执行全扫、Flat、PQ 功能闭环 |
| perf 总流程 | `bash bin/run-perf.sh [fresh\|reuse] [provider]` | 默认 `fresh pyiceberg`，执行 Flat、PQ、全扫代表性性能矩阵 |

### 5.4 主要参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `MVP_CONFIG_PROFILE` | mvp / perf | 约束总入口只能使用对应类型的配置 |
| `MVP_DATASET` / `MVP_VECTOR_DIM` / `MVP_ROW_COUNT` | 数据集配置指定 | 隔离数据集名称、向量维度和表行数契约 |
| `MVP_QUERY_COUNT` / `MVP_GT_K` | 数据集配置指定 | 校验 query 总数和官方 GT 最大 K |
| `MVP_WAREHOUSE_DIR` | 配置文件指定 | 裸绝对 warehouse 路径 |
| `MVP_NAMESPACE` / `MVP_TABLE` | 配置文件指定 | 专用于本次测试的 Catalog 表 |
| `MVP_VECTOR_TYPE` | `floatvector` | 查询 literal cast；建表实际类型由 Catalog 读取表属性决定 |
| `MVP_PARTITION_BUCKETS` | 32 | 各 producer 共用的 `bucket(id, N)` 分区数；Bridge v3 基线要求正整数 |
| `MVP_DATA_FILES` | 8 / 32 | Spark shuffle partition 数下限；非分区表同时作为目标文件数 |
| `MVP_TARGET_FILE_SIZE_BYTES` | 1073741824 | Spark 单个 Parquet 文件的滚动目标大小 |
| `MVP_SPARK_MASTER` | `local[*]` | 传给 `spark-submit --master` 的本地执行资源配置 |
| `MVP_SPARK_DRIVER_MEMORY` | `8g` | 传给 `spark-submit --driver-memory` 的 JVM 内存配置 |
| `MVP_PYICEBERG_BATCH_ROWS` | 131072 / 1000000 | MVP / perf 的 PyIceberg 输入批行数 |
| `MVP_RUST_BATCH_ROWS` | 131072 / 1000000 | MVP / perf 的 Rust fixture 输入批行数 |
| `MVP_BRIDGE_BATCH_ROWS` | 16384 | Bridge 读取 fvecs 并分流到 Arrow IPC 的批行数 |
| `MVP_INDEX_NAME` | `idx_sift_ivfpq` | 当前索引名称 |
| `MVP_INDEX_TYPE` | `ivf_pq` | Catalog index type |
| `MVP_INDEX_IMPLEMENTATION` | `ivf_pq` | index ABI implementation |
| `MVP_NUM_CLUSTERS` | 256 / 1024 | MVP 功能回归 / perf 性能基线的聚类数 |
| `MVP_SAMPLE_RATE` | 100000 | 索引训练采样数 |
| `MVP_BUILD_WORKERS` | 1 / 8 | 串行 / 性能配置的构建 worker 数 |
| `MVP_NUM_SUB_QUANTIZERS` | 未设置 | IVF-PQ 子向量数 M；设置时必须整除向量维度 |
| `MVP_PQ_NBITS` | 未设置 | IVF-PQ 编码位数，范围 1..8 |
| `MVP_NPROBE` | 10 | 索引查询探测簇数 |
| `MVP_TEST_NQ` | 100 | 一键测试查询数 |
| `MVP_TEST_K` | 10 | 一键测试 Top-K |
| `MVP_TEST_WARMUP` | 5 | 每种扫描模式的预热查询数 |
| `MVP_RECALL_NQ` | 10000 | 正式 Recall 使用的完整 SIFT query 数 |
| `MVP_QUERY_SAMPLING` | first / equidistant | MVP / perf 的 query 选取方式 |
| `MVP_MATRIX_NQ` | 100 | 性能矩阵每个场景、每轮的查询数 |
| `MVP_MATRIX_ROUNDS` | 1 | 性能矩阵重复轮数 |
| `MVP_MATRIX_K` | 10,100,1000,10000 | 性能矩阵 Top-K 集合 |
| `MVP_MATRIX_DOP` | 1,2,4,8 | 性能矩阵 DOP 集合 |
| `MVP_MATRIX_MODES` | index,fullscan | 性能矩阵扫描模式 |
| `MVP_MATRIX_WARMUP` | 5 | 性能矩阵每个场景的预热 query 数 |
| `MVP_PERF_K` | 10,100 | 一键 perf 的 Top-K 集合 |
| `MVP_PERF_DOP` | 1,8 | 一键 perf 的串行与最大并行代表点 |
| `MVP_PERF_NQ` | 100 | 一键 perf 每个场景、每轮的查询数 |
| `MVP_PERF_ROUNDS` | 1 | 一键 perf 每个场景的重复轮数 |
| `MVP_PERF_WARMUP` | 5 | 一键 perf 每个场景的预热 query 数 |

`MVP_PARTITION_BUCKETS` 控制 Iceberg 文件布局和并行任务划分；`MVP_NUM_CLUSTERS` 控制 IVF 向量聚类。`mvp.env.example` 使用 256 clusters 缩短功能回归构建时间，`perf.env.example` 使用 1024 clusters 作为性能基线。两份配置均显式使用 `sample_rate=100000` 和 `nprobe=10`。

### 5.5 参数对性能结果的影响

修改参数后按以下级别重测：

- **重新供数**：MVP 执行 `bash bin/run-clean-test.sh fresh <provider>`；性能测试执行 `bash bin/run-perf.sh fresh <provider>`。
  适用于 schema、压缩、分区和文件布局变化。
- **重建索引**：执行 `bash bin/clean.sh index`，再执行 `bash bin/build-index.sh`。
  适用于索引实现和构建参数变化。
- **重跑查询**：保留数据和索引，执行 `bash bin/clean.sh results` 后重新运行测试模块。
  适用于 nprobe、K、DOP、query sampling 和测量规模变化。

#### 5.5.1 供数和文件布局

| 参数 | 作用 | 影响的结果 | 修改后操作 |
|---|---|---|---|
| `MVP_WAREHOUSE_DIR` | 指定本地 Iceberg warehouse | 路径所在介质的带宽、延迟和缓存状态影响供数、索引构建和查询延迟 | 使用空目录重新供数 |
| `MVP_NAMESPACE` / `MVP_TABLE` | 标识当前测试表 | 本身不调节性能；独立名称防止复用错误的 snapshot 或索引 | 新表重新供数；复用表先执行 `verify-table.sh` |
| `MVP_ID_BASE` | 设置表 ID 和官方 GT 的偏移，SIFT1M 使用 1 | 错误值直接破坏 ID Recall；通常不改变距离计算延迟 | 重新供数并重跑查询 |
| `MVP_COMPRESSION` | 设置 Parquet 压缩编码 | 影响 Parquet 字节数、供数耗时、解码 CPU、扫描延迟和索引构建读取时间 | 重新供数并重建索引 |
| `MVP_PARTITION_BUCKETS` | 设置 `bucket(id, N)` 分区数 | 影响目录和文件布局、task group 数、索引 artifact 数、DOP 扩展性、构建时间、查询延迟；并行 ANN 下也可能影响 Recall | 重新供数并重建索引 |
| `MVP_DATA_FILES` | 设置 Spark shuffle partition 数下限 | bucket 表按 bucket 数校验文件布局；非分区表按该值校验文件数 | Spark 重新供数并重建索引 |
| `MVP_TARGET_FILE_SIZE_BYTES` | 设置 Spark rolling writer 目标大小 | 单 bucket 超过目标时拆分为多个文件；当前 SIFT/GIST 基线固定为 1 GiB | Spark 重新供数并重建索引 |
| `MVP_PYICEBERG_BATCH_ROWS` | 设置每次 PyIceberg append 的输入行数 | 批次越多，通常 snapshot 和每个 bucket 的数据文件越多；影响供数内存、metadata、文件打开开销、扫描和构建时间 | PyIceberg 重新供数并重建索引 |
| `MVP_RUST_BATCH_ROWS` | 设置 Rust fixture 每个输入批次的行数 | 每批按 bucket 拆分文件；影响供数内存、文件数量、扫描和构建时间 | Rust 重新供数并重建索引 |

同一组性能结果必须记录 producer、format version、compression、partition spec、Parquet
文件数和字节数。Spark 和 Bridge producer 对当前 SIFT/GIST 基线强制执行 32 文件门禁；
PyIceberg 和 Rust 的文件数取决于 batch rows。

#### 5.5.2 索引构建和 ANN 查询

| 参数 | 作用 | 影响的结果 | 修改后操作 |
|---|---|---|---|
| `MVP_INDEX_NAME` | 标识 Catalog 索引 | 名称本身不调节性能；同表保留多个活动向量索引会使 FDW 的索引选择不确定 | 清理旧索引后重建 |
| `MVP_INDEX_TYPE` / `MVP_INDEX_IMPLEMENTATION` | 共同选择 Flat、PQ 或 BTree 实现 | 决定索引算法，直接影响构建时间、峰值内存、artifact 大小、查询延迟和 Recall | 清理索引后重建 |
| `MVP_NUM_CLUSTERS` | 设置 IVF 聚类数 | 影响训练和构建资源、索引大小以及每次探测覆盖的数据量；固定 nprobe 时，clusters 增大通常减少扫描比例，Recall 和延迟均可能变化 | 清理索引后重建 |
| `MVP_SAMPLE_RATE` | 设置索引训练采样参数 | 影响训练输入、质心质量、构建时间和内存，进而可能影响 Recall；实际采样量受实现上限约束 | 清理索引后重建 |
| `MVP_BUILD_WORKERS` | 设置索引构建 worker 数 | 主要影响构建墙钟时间、CPU 和峰值内存；不作为查询并行度 | 清理索引后重建 |
| `MVP_NPROBE` | 设置查询时探测的 IVF clusters 数 | 增大通常提高 Recall，同时增加候选计算、I/O 和查询延迟；clusters 不同时相同 nprobe 代表不同扫描比例 | 仅重跑索引查询 |

`MVP_NUM_SUB_QUANTIZERS` 和 `MVP_PQ_NBITS` 仅在配置文件显式设置时写入 PQ 构建参数；SIFT 配置保持实现默认值，GIST 配置固定 M=60、nbits=8。测试报告从 Registry segment 的 `algorithm_details` 核对实际值。

#### 5.5.3 查询规模和统计口径

| 参数 | 作用 | 影响的结果 | 修改后操作 |
|---|---|---|---|
| `MVP_TEST_NQ` | 设置一键快速测试的 query 数 | 不改变单条 SQL 语义；影响总耗时、Recall 样本量及 p50/p95/p99 稳定性 | 重跑查询 |
| `MVP_TEST_K` | 设置快速测试返回的 Top-K | 影响结果行数和 Recall@K；内核按 `max(5×K, 50)` 计算 FetchK，上限 10000，因此也影响 ANN 候选数和延迟 | 重跑查询 |
| `MVP_TEST_WARMUP` | 设置快速测试的预热 query 数 | 预热不计入统计；影响文件页、Parquet metadata 和索引缓存状态，通常影响冷启动后的延迟 | 重跑查询 |
| `MVP_RECALL_NQ` | 设置 `test-index.sh <profile> recall` 的 query 数 | 影响 Recall 估计的覆盖度和总耗时；SIFT1M 正式结果使用全部 10000 条 | 重跑 Recall |
| `MVP_QUERY_SAMPLING` | 选择前 N 条或全量中含首尾等距抽取 N 条 query | query 分布会改变 Recall 样本均值和延迟分布；比较结果时必须使用同一组序号 | 重跑查询 |
| `MVP_MATRIX_K` | 设置矩阵的 K 集合 | 分别影响 FetchK、返回数据量、延迟和 Recall@K；官方 GT 只支持 K≤100 | 重跑矩阵 |
| `MVP_MATRIX_DOP` | 设置矩阵的查询并行度集合 | 影响 worker 和 task group 数、CPU/内存、延迟和 QPS；当前分区 ANN 路径还会改变候选池上限，可能改变 Recall | 重跑矩阵 |
| `MVP_MATRIX_MODES` | 选择 `index`、`fullscan` 或两者 | 决定测量 ANN 索引扫描、精确全扫或两者；两种模式用于计算加速比 | 重跑矩阵 |
| `MVP_MATRIX_NQ` | 设置每个矩阵场景、每轮的 query 数 | 影响每场景总耗时和分位数稳定性；每轮使用 sampling 选定的同一组 query | 重跑矩阵 |
| `MVP_MATRIX_ROUNDS` | 设置每个矩阵场景的重复轮数 | 影响总耗时和跨轮波动观察；增加轮数不增加 unique query 数 | 重跑矩阵 |
| `MVP_MATRIX_WARMUP` | 设置每个矩阵场景的预热 query 数 | 影响缓存热度和后续延迟，不进入分位数样本 | 重跑矩阵 |
| `MVP_PERF_K` / `MVP_PERF_DOP` | 设置一键 perf 的 K 和 DOP 代表点 | 影响一键流程覆盖的延迟、Recall 和并行加速场景；默认覆盖 K=10/100 与 DOP=1/8 | 重跑一键 perf |
| `MVP_PERF_NQ` / `MVP_PERF_ROUNDS` / `MVP_PERF_WARMUP` | 设置一键 perf 每场景的 query、轮次和预热规模 | 影响总耗时、统计稳定性和缓存热度，口径与对应 `MVP_MATRIX_*` 参数一致 | 重跑一键 perf |

`first` 使用序号 `0..N-1`，适合快速功能回归。`equidistant` 在 `[0,total-1]` 上含首尾等距取 N 个序号；SIFT1M 的 `total=10000,N=100` 对应 `0,101,202,...,9999`。结果 JSON 同时记录 `query_sampling` 和完整 `query_indices`，Recall 为 100 条 query 的命中数总和除以 `100×K`，等价于逐 query Recall@K 的算术平均。

当前分区 ANN 路径中，内核为 K=10 设置 FetchK=50，为 K=100 设置 FetchK=500。每个并行 worker 对自己拥有的 partition segments 最多返回一份 FetchK 候选，再由 `LOCAL GATHER` 汇总并执行精确距离 Top-K。32-bucket 表在 DOP=1/2/4/8 时，候选上限可分别达到 1/2/4/8 倍 FetchK。因此 DOP 不只是延迟参数，不同 DOP 的性能结果必须同时报告 Recall；全扫结果用于验证精确 Recall 和并行加速。

### 5.6 环境和工具参数

| 参数 | 作用及结果边界 |
|---|---|
| `JAVA_HOME` / `SPARK_HOME` / `ICEBERG_SPARK_RUNTIME_JAR` | 选择 JDK、Spark 和 Iceberg writer 版本；runtime 1.11.0 要求 JDK 17 或更高版本，版本变化可能改变文件布局、metadata 和供数性能 |
| `MVP_PYTHON_BIN` | 选择 PyIceberg producer Python；包版本变化可能改变 writer 行为。benchmark 和矩阵脚本使用系统 `python3`，Python 启动时间不进入 SQL statement time |
| `MVP_BRIDGE_SOURCE` / `MVP_CARGO_BIN` | 选择 Bridge ABI/Rust SDK producer 的工作树和工具链；版本变化可能改变 metadata 和 writer 行为 |
| `MVP_GSQL_BIN` / `MVP_DB` / `MVP_PORT` | 选择数据库客户端和目标数据库；数据库实例、配置和当前负载属于性能结果环境信息 |
| `MVP_GAUSSHOME` | 选择预检和运行时安装目录；bridge、Catalog、FDW 和内核二进制变化会影响全部功能与性能结果 |
| `MVP_VECTOR_TYPE` | 设置查询向量 literal cast；必须与外表向量类型和 `<->` 运算符匹配，否则可能改变计划或使查询失败 |
| `MVP_ALLOW_NON_AARCH64` | 只用于允许蓝区 x86_64 功能验证，不调节算法；不同架构的耗时结果分别报告 |

蓝区 x86_64 功能验证显式设置 `MVP_ALLOW_NON_AARCH64=1`。黄区保持默认 aarch64 门禁，蓝区结果只用于功能验证。

每条 producer 首次供数使用独立、空的 `MVP_WAREHOUSE_DIR`、`MVP_NAMESPACE` 和 `MVP_TABLE`。例如 Spark 使用 `sift_spark_part.sift1m_part`，PyIceberg 使用 `sift_pyiceberg_part.sift1m_part`。

按需运行预检：

```bash
bash bin/preflight.sh spark
bash bin/preflight.sh bridge
bash bin/preflight.sh pyiceberg
bash bin/preflight.sh rust
bash bin/preflight.sh all
```

预检会记录架构、producer 版本、数据库连接、runtime jar/bridge/Catalog 哈希和 warehouse 权限。若输出多份 bridge `.so`，先用 `readelf`、`ldd` 和 `/proc/<pid>/maps` 确认实际加载副本。

## 6. Spark 供数

```bash
source mvp.env
bash bin/seed-sift1m.sh
# GIST：MVP_ENV_FILE=gist.env MVP_STATE_DIR=state/gist1m bash bin/seed-spark.sh
```

`bin/seed-spark.sh` 从当前配置读取 dataset、dimension、rows 和 base 文件，使用
`env -u LD_LIBRARY_PATH spark-submit` 调用公共 Python producer，并传入
`MVP_SPARK_MASTER`、`MVP_SPARK_DRIVER_MEMORY`。producer 创建 Iceberg
v3 表，写入 `vector_dim.embedding=<dimension>`，并校验 metadata 格式版本、行数和文件
布局。`bin/seed-sift1m.sh` 保留为 SIFT 兼容入口。

Spark 表目录已存在时供数器拒绝运行。重复测试使用新目录和新表名。

## 7. PyIceberg 供数

### 7.1 离线安装

```bash
MVP_BASE_PYTHON="$(command -v python3)" bash bin/install-pyiceberg-offline.sh
```

安装器先校验 wheelhouse SHA-256，再以 `--no-index` 创建包内 `.venv`，最后运行 `pip check`。把输出的 Python 路径写入 `mvp.env` 的 `MVP_PYTHON_BIN`。

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

供数器不修改已发布 metadata，也不操作数据库 Catalog。统一接入脚本负责原生注册。

## 8. Rust 与 Bridge 供数

### 8.1 Rust SDK v2

```bash
source mvp.env
bash bin/seed-sift1m-rust.sh
```

脚本复用 bridge 的 Cargo.lock，以 `--offline --locked --release` 编译套件内 Rust example。供数器流式验证 516 字节 fvecs 记录，按 Iceberg `bucket(id, N)` 变换拆分每个输入批次，为每个分区绑定 `PartitionKey`，再提交一个 Iceberg v2 snapshot。完成后校验 partition spec 和每个 FileScanTask 的 partition 值并输出最新 metadata URI。默认 `MVP_PARTITION_BUCKETS=32`；设为 `0` 时创建非分区表。Rust SDK schema 仍为 `long + list<float>`；`vector_dim.embedding=128` 是表级审计属性。

### 8.2 Bridge ABI v3

```bash
source mvp.env
bash bin/seed-bridge.sh
# GIST：MVP_ENV_FILE=gist.env MVP_STATE_DIR=state/gist1m bash bin/seed-bridge.sh
```

公共 Bridge provider 读取 SIFT/GIST 配置，通过
`iceberg_bridge_table_create` 创建 v3 表，并写入 `vector_dim.embedding=<dimension>`。
输入按 `MVP_BRIDGE_BATCH_ROWS` 分批解析，通过 SDK 的 Iceberg bucket transform 分流到 32 个
临时 Arrow IPC 流，随后逐 bucket 调用
`iceberg_bridge_table_write_partitioned_data_files`，最后调用
`iceberg_bridge_table_fast_append_commit`。完成后使用 `iceberg_bridge_table_load` 重载并校验
format version、snapshot、分区和 32 文件布局。IPC 暂存控制 GIST 宽向量的常驻内存，
临时目录随进程退出清理。

## 9. 统一 fixture 接入门禁

任一 producer 供数完成后执行：

```bash
bash bin/register-table.sh
```

脚本流程为：

1. 校验 fixture 的 v2/v3 format、UUID、snapshot、标准 schema、partition spec 和表属性；
2. 创建扩展及缺失的 namespace；
3. 调用三参数 `iceberg_catalog.register_table` 注册最终 metadata URI；
4. 校验数据范围、SQL 向量类型、Catalog UUID、metadata、snapshot、`relid` 和 schema 维度。

共同通过条件：

- fixture metadata 顶层审计属性是字符串 `vector_dim.embedding=128`；
- Catalog schema 记录的 `embedding.field_vector_dim=128`；
- 最终外表是 `id bigint, embedding vector(128)` 或 `floatvector(128)`；
- `tables_internal.relid` 指向最终外表；
- `count=1000000, min(id)=1, max(id)=1000000`。

producer metadata 可以不含字段级 `vector_dim`；向量 SQL 类型来自 Catalog #116 对表属性
`vector_dim.embedding` 的解析。注册流程不创建 bootstrap 表，也不更新
`iceberg_catalog.tables_internal`。

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

切换脚本同时更新索引名、type 和 implementation。Flat 使用 `idx_sift_ivfflat + ivf_flat + ivf`，PQ 使用 `idx_sift_ivfpq + ivf_pq + ivf_pq`。

```bash
bash bin/build-index.sh
```

串行冒烟配置保持 256 clusters、100000 sample、1 worker。`config/perf.env.example` 使用指南基线 1024 clusters、100000 sample、8 workers。索引状态必须为 `active`，Catalog 中的 type/implementation 必须与当前配置一致。构建后门禁读取当前 metadata 指向的 Registry Puffin，校验 Registry 大小、SHA-256、canonical implementation、`active` 状态、artifact 前缀、文件大小和落盘位置。墙钟耗时分别保存在 `state/build-index-flat.log` 和 `state/build-index-pq.log`。

历史结果与新版性能基线参数不同，不能直接合并。调整 `nprobe` 时固定数据 snapshot 和索引，只修改外表 option。

## 11. 正确性和性能测试

### 11.1 全扫正确性

模块入口：

```bash
bash bin/test-fullscan.sh
```

等价的细粒度命令：

```bash
python3 bin/benchmark.py \
  --mode fullscan --query-dop 1 --nq 100 --query-sampling equidistant --k 10 \
  --output state/fullscan.json
```

perf 配置等距采样 100 条查询，MVP 配置使用前 100 条查询；全扫距离阈值 Recall@10 必须为 1.0。

### 11.2 索引 Recall 和延迟

模块入口：

```bash
bash bin/test-index.sh pq
# 当前配置和活动索引为 Flat 时：bash bin/test-index.sh flat
```

上述快速入口使用 100 条 query。正式 Recall 使用官方 SIFT1M 的全部 10000 条 query 及相同序号的 ground truth：

```bash
bash bin/test-index.sh pq recall
# Flat 索引：bash bin/test-index.sh flat recall
```

等价的细粒度命令：

```bash
python3 bin/benchmark.py \
  --mode index --query-dop 1 --nq 100 --query-sampling equidistant \
  --k 10 --nprobe 10 \
  --output state/index-nprobe10.json
```

索引计划必须包含 `Vector Search` 和 `bridge vector index scan`。脚本输出官方 GT 的 ID Recall、等距容忍 Recall、QPS、mean、p50/p95/p99、逐查询耗时和完整计划。

### 11.3 K×DOP 矩阵

分区表执行：

```bash
python3 bin/run-matrix.py --output-dir state/matrix
```

默认矩阵为 K=`10,100,1000,10000`、DOP=`1,2,4,8`、index/fullscan、每个场景 100 条 query、1 轮和 5 条预热 query。perf 配置默认使用 `equidistant`；可通过 `--query-sampling first|equidistant` 显式覆盖。默认值读取 `MVP_MATRIX_*`；需要比较重复轮次波动时设置 `MVP_MATRIX_ROUNDS=3` 或传入 `--rounds 3`。矩阵使用 100 条不同 query 统计延迟分布；单条 query 重复执行只反映该 query 的运行波动，两种结果不能直接比较。

DOP>1 默认要求计划出现对应的 `LOCAL GATHER dop: 1/N`。未分区表只执行 `--dop 1`；`--allow-serial-fallback` 仅用于诊断，带该选项的结果不能声明为并行结果。

SIFT 官方 GT 只包含 top-100。K≤100 计算官方 Recall；K>100 自动使用 `--skip-recall`，只输出性能。报告中不得把 K>100 标记为官方召回率。

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
| 维度 | 数据均为 128 维；metadata 含表级维度属性，Catalog schema 记录字段维度 |
| SQL 类型 | Catalog `register_table` 原生创建 `vector(128)` 或 `floatvector(128)` |
| 索引 | type/implementation 符合固定映射，`index_status=active` |
| 串行计划 | Vector Search 和实际 bridge scan mode 正确 |
| 并行计划 | DOP>1 出现对应 LOCAL GATHER |
| 快速正确性 | 全扫前 100 条距离阈值 Recall@10=1.0 |
| 正式 Recall | 使用全部 10000 条 query 和官方同序号 ground truth；K≤100 |
| 布局 | 记录 producer、分区、Parquet 文件数/字节数和压缩 |
| 环境 | 记录数据库版本、组件 commit 和运行时 `.so` 哈希 |

`benchmark.py` 的 gsql `\timing` 表示同一会话中的语句端到端时间，包含 bridge I/O。需要与指南的 `EXPLAIN ANALYZE Total runtime` 比较时，两种口径分别保存，不混合计算。

套件自身回归使用锁定 PyIceberg 环境执行，确保 PyIceberg/PyArrow API 集成用例参与测试：

```bash
.venv/bin/python -m pip check
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
bash -n bin/*.sh
```

系统 `python3` 未安装 `pyiceberg`、`pyarrow` 或 `sqlalchemy` 时会跳过两项 API 集成用例，不作为完整回归结果。

## 13. 生成完整离线包

下载并校验数据、按需准备 wheelhouse 后执行：

```bash
# 全部 producer
MVP_OFFLINE_PROVIDER=all bash bin/make-offline-bundle.sh

# 仅 Spark
MVP_OFFLINE_PROVIDER=spark bash bin/make-offline-bundle.sh

# 仅 PyIceberg
MVP_OFFLINE_PROVIDER=pyiceberg bash bin/make-offline-bundle.sh

# 仅 Rust fixture
MVP_OFFLINE_PROVIDER=rust bash bin/make-offline-bundle.sh

# 仅 Bridge ABI v3
MVP_OFFLINE_PROVIDER=bridge bash bin/make-offline-bundle.sh
```

完整包包含 SIFT1M 数据；PyIceberg 模式还强制包含经过 SHA-256 校验的 wheelhouse。`state/`、`.venv/` 和 Python cache 不进入包。Spark/JDK/runtime 使用 `mvp.env` 中的黄区绝对路径。

GIST1M Spark 离线包携带 GIST 数据；Spark/JDK/runtime 继续使用黄区绝对路径：

```bash
MVP_OFFLINE_DATASET=gist1m MVP_OFFLINE_PROVIDER=spark \
  bash bin/make-offline-bundle.sh
```

## 14. 常见故障

### 基础表或 Delta 伴生表为 `text`

确认已安装 Catalog #116，三参数 `register_table` 可用，并检查 producer metadata 顶层
`properties.vector_dim.embedding` 与 `state/register-table.log`。套件不操作或检查 Delta。

### DOP 不生效

确认表的默认 partition spec 含 `bucket[32]`，每个 FileScanTask 带 partition 信息，`query_dop>1`，计划中出现 `LOCAL GATHER dop: 1/N`。未分区表始终是串行基线。

### PyIceberg wheel 无法安装

wheel 必须匹配目标架构、Python ABI 和 glibc。回到同构联网机器重新生成 wheelhouse，不要在黄区启用网络索引或现场编译 PyArrow。

### Producer 被数据库构建环境污染

producer 命令清除数据库 `LD_LIBRARY_PATH`；Bridge runner 同时清除 `CC/CXX`。数据库环境
脚本只用于数据库命令。

### Rust fixture 找不到 example 或依赖

确认 `MVP_BRIDGE_SOURCE` 指向完整 bridge 工作树，Rust 工具链满足其 `rust-version`，并且联网区已经用同一 Cargo.lock 填充黄区离线缓存。脚本拒绝覆盖 bridge 中已有的同名 example。

## 15. 来源

- [openGauss-Catalog PR #116：register_table 表级向量维度支持](https://github.com/DataInfraLab/openGauss-Catalog/pull/116)
- [Iceberg 向量索引端到端测试指南](https://github.com/Doreami/infra-compile-scripts/blob/main/%E7%AB%AF%E5%88%B0%E7%AB%AF%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95/%E7%AB%AF%E5%88%B0%E7%AB%AF%E6%B5%8B%E8%AF%95%E6%8C%87%E5%8D%97.md)
- [PyIceberg SqlCatalog API](https://py.iceberg.apache.org/reference/pyiceberg/catalog/sql/)
- [PyIceberg Table append API](https://py.iceberg.apache.org/reference/pyiceberg/table/)
- `../type-and-deployment-contract.md`
- `../spark-supply-guide.md`
