# 黄区 SIFT1M 多供数离线测试套件

版本：1.4.1

## 1. 目标与边界

本套件用于在 aarch64 EulerOS 黄区完成以下闭环：

```text
SIFT1M
  → Spark、PyIceberg 或 Rust fixture 生成完整 Iceberg v2 snapshot
  → iceberg_catalog.create_table 创建字段级 `vector_dim=128` 的 Catalog 表
  → 将 Catalog `metadata_location/current_snapshot_id` 切换到 fixture
  → IVF-PQ 索引
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
| 主要用途 | 跨引擎兼容性、性能供数 | Python 独立 producer、链路冒烟 | 与 bridge 锁定 SDK 一致的串行 fixture |
| 前置依赖 | JDK、Spark、Iceberg runtime | Python venv、锁定 wheelhouse | bridge 工作树、Rust 1.96、Cargo 离线缓存 |
| Catalog | HadoopCatalog | 临时 SQLite Catalog | MemoryCatalog + LocalFs |
| 内存策略 | Spark 分区写 | 每批默认 131072 行 | 每批默认 131072 行 |
| 分区 | `bucket(id, N)` | `bucket(id, N)` | 仅非分区 |
| producer metadata 字段级 `vector_dim` | 不支持 | 不支持 | 不支持 |
| 数据库入口 | `create_table` + metadata 切换 | `create_table` + metadata 切换 | `create_table` + metadata 切换 |

PyIceberg 每次 `append` 会为涉及的分区生成数据文件。分区表的文件数通常多于 Spark。
各 producer 的结果用于验证互操作；性能数值只有在 Parquet 文件数、大小、压缩、
partition spec、snapshot 数、索引参数和硬件一致时才能直接比较。

## 3. 文件结构

```text
sift1m-yellow-mvp/
├── README.md
├── VERSION
├── config/
│   ├── mvp.env.example          # 非分区串行冒烟
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
│   ├── seed-sift1m.sh           # Spark
│   ├── seed-sift1m-pyiceberg.sh
│   ├── seed-sift1m-rust.sh
│   ├── register-table.sh
│   ├── build-index.sh
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

串行冒烟：

```bash
cp config/mvp.env.example mvp.env
vi mvp.env
```

分区性能基线：

```bash
cp config/perf.env.example mvp.env
vi mvp.env
```

每个 producer 使用独立、空的 `MVP_WAREHOUSE_DIR`、`MVP_NAMESPACE` 和 `MVP_TABLE`。
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

Rust fixture 仅用于非分区串行表。先将 `MVP_PARTITION_BUCKETS` 设为 `0`，再执行：

```bash
source mvp.env
bash bin/seed-sift1m-rust.sh
```

脚本复用 bridge 的 Cargo.lock，以 `--offline --locked --release` 编译套件内 Rust
example。供数器流式验证 516 字节 fvecs 记录，按批次生成 Parquet 文件，提交一个 Iceberg
v2 snapshot，并输出最新 metadata URI。Rust SDK schema 仍为 `long + list<float>`；
`vector_dim.embedding=128` 是表级审计属性。

## 9. 统一 fixture 接入门禁

任一 producer 供数完成后执行：

```bash
bash bin/register-table.sh
```

脚本名为 `register-table.sh` 以保持离线包目录和既有调用方式稳定，实际流程为：

1. 校验 fixture snapshot、`id long`、`embedding list<float>` 和表级审计属性；
2. 在独立 bootstrap 位置调用 `create_table`，schema 字段显式携带 `vector_dim=128`；
3. 保留 Catalog 创建的外表、`relid` 及可选 Delta 伴生表；
4. 将 `tables_internal.metadata_location` 和 `current_snapshot_id` 切换到 fixture；
5. 校验数据范围、基础列类型、Catalog 表头，以及已存在的 Delta 伴生列类型。

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

```bash
bash bin/build-index.sh
```

串行冒烟配置保持 256 clusters、100000 sample、1 worker。`config/perf.env.example` 使用
指南基线 1024 clusters、100000 sample、8 workers。索引状态必须为 `active`，最终参数和
墙钟耗时保存在 `state/build-index.log`。

历史结果与新版性能基线参数不同，不能直接合并。调整 `nprobe` 时固定数据 snapshot 和
索引，只修改外表 option。

## 11. 正确性和性能测试

### 11.1 全扫正确性

```bash
python3 bin/benchmark.py \
  --mode fullscan --query-dop 1 --nq 100 --k 10 \
  --output state/fullscan.json
```

前 100 条查询的距离阈值 Recall@10 必须为 1.0。

### 11.2 索引 Recall 和延迟

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
python3 bin/run-matrix.py \
  --k 10,100,1000,10000 \
  --dop 1,2,4,8 \
  --modes index,fullscan \
  --rounds 3 --nq 1 \
  --output-dir state/matrix
```

DOP>1 默认要求计划出现对应的 `LOCAL GATHER dop: 1/N`。未分区表只执行 `--dop 1`；
`--allow-serial-fallback` 仅用于诊断，带该选项的结果不能声明为并行结果。

SIFT 官方 GT 只包含 top-100。K≤100 计算官方 Recall；K>100 自动使用
`--skip-recall`，只输出性能。报告中不得把 K>100 标记为官方召回率。

完整矩阵开销较高，可先执行：

```bash
python3 bin/run-matrix.py --k 10,100 --dop 1,8 --rounds 3 --nq 1
```

## 12. 结果有效性

| 检查项 | 要求 |
|---|---|
| 数据 | 四个文件大小和 SHA-256 正确 |
| 表 | 1,000,000 行、128 维、一基 ID |
| metadata | 最终 snapshot 的具体绝对 URI |
| 维度 | 数据均为 128 维；记录字段级属性和表级审计属性 |
| SQL 类型 | Catalog `create_table` 原生创建 `vector(128)` 或 `floatvector(128)` |
| 索引 | `index_status=active` |
| 串行计划 | Vector Search 和实际 bridge scan mode 正确 |
| 并行计划 | DOP>1 出现对应 LOCAL GATHER |
| 正确性 | 全扫前 100 条距离阈值 Recall@10=1.0 |
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
