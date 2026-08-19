# 黄区 GIST1M 性能测试

## 1. 测试范围

GIST1M 套件在同一代码库中复用 SIFT1M 已验证的 Catalog 接入、索引构建、计划门禁、
Recall 和延迟统计链路。GIST 使用独立配置文件 `gist.env`、独立 Catalog 表、独立
warehouse 和 `state/gist1m/` 状态目录。SIFT 的 `mvp.env` 和 `state/` 保持原有语义。

GIST 默认性能流程为：

```text
官方 GIST1M fvecs/ivecs
  → Spark 写入 Iceberg v3 bucket(id, 32)，每个 bucket 一个数据文件
  → Catalog #116 原生 register_table 创建 vector(960) 外表
  → IVF-Flat（ivf_flat + ivf）
  → IVF-PQ（ivf_pq + ivf_pq）
  → 清理索引后 FullScan
  → 官方 GT Recall、QPS、p50/p95/p99
```

GIST 同时提供 Bridge ABI v3 和 PyIceberg v2 入口；Rust SDK v2 仅支持 SIFT。Spark 与 SIFT 复用
`src/seed_sift1m.py` 的定长 fvecs 解析、Iceberg 建表、写入和 metadata 门禁，GIST 的
配置、状态目录、Catalog 表和测试结果保持独立。

### 1.1 复用入口与阅读顺序

GIST 只保留数据集契约、下载校验、供数入口和配置隔离，执行框架以根目录
[`README.md`](../README.md) 描述的 SIFT 公共流程为准。维护或审查 GIST 流程时按以下
顺序阅读：

1. [`README.md` 的目标与边界](../README.md#1-目标与边界)：了解 producer snapshot、
   表级向量维度属性和 Catalog #116 原生注册契约；
2. [`README.md` 的 MVP 与 perf 一键测试](../README.md#51-mvp-与-perf-一键测试)和
   [模块入口](../README.md#53-模块入口)：了解公共执行顺序和模块职责；
3. [`config/gist-perf.env.example`](../config/gist-perf.env.example)和
   [`bin/run-gist-perf.sh`](../bin/run-gist-perf.sh)：了解 GIST 固定契约、独立配置和
   `state/gist1m/` 状态边界；
4. [`bin/run-perf.sh`](../bin/run-perf.sh)：了解 `fresh`、`reuse`、Flat、PQ 和
   FullScan 的实际编排；
5. [`bin/supply-data.sh`](../bin/supply-data.sh)、[`bin/seed-spark.sh`](../bin/seed-spark.sh)
   和 [`src/seed_sift1m.py`](../src/seed_sift1m.py)：了解默认 Spark v3 分派、960 维参数、
   文件布局及公共 writer；PyIceberg 兼容路径再阅读
   [`bin/seed-gist1m-pyiceberg.sh`](../bin/seed-gist1m-pyiceberg.sh)和
   [`src/seed_sift1m_pyiceberg.py`](../src/seed_sift1m_pyiceberg.py)；
6. [`README.md` 的统一 fixture 接入门禁](../README.md#9-统一-fixture-接入门禁)、
   [`bin/register-table.sh`](../bin/register-table.sh)和
   [`bin/verify-table.sh`](../bin/verify-table.sh)：了解原生注册和复用门禁；
7. [`bin/configure-index.sh`](../bin/configure-index.sh)、
   [`bin/build-index.sh`](../bin/build-index.sh)、
   [`bin/run-matrix.py`](../bin/run-matrix.py)和
   [`bin/benchmark.py`](../bin/benchmark.py)：了解索引路由、构建校验、计划门禁和指标口径。

一键入口的实际调用链为：

```text
bin/run-gist-perf.sh
  → bin/run-perf.sh fresh|reuse spark
    → bin/preflight.sh → bin/deploy.sh
    → fresh: bin/clean.sh all
             → bin/supply-data.sh spark
             → bin/seed-spark.sh
             → src/seed_sift1m.py
             → bin/register-table.sh
      reuse: bin/clean.sh results → bin/clean.sh index
    → bin/verify-table.sh
    → bin/configure-index.sh flat → bin/build-index.sh → bin/run-matrix.py
    → bin/clean.sh index
    → bin/configure-index.sh pq → bin/build-index.sh → bin/run-matrix.py
    → bin/clean.sh index → bin/run-matrix.py fullscan
```

`bin/register-table.sh` 校验 producer metadata 的 `vector_dim.embedding=960` 后，调用
三参数 `iceberg_catalog.register_table`。Catalog #116 将标准 `list<float>` 映射为
`vector(960)`，并保存 UUID、metadata、snapshot、`relid` 和 schema 字段维度。GIST 复用
这份公共脚本，维度、namespace、table、warehouse 和状态目录均来自 `gist.env` 及入口
导出的 `MVP_ENV_FILE`、`MVP_STATE_DIR`。

## 2. 数据契约

| 文件 | 记录数 | 记录宽度 | 字节数 |
|---|---:|---:|---:|
| `gist_base.fvecs` | 1,000,000 | 960 个 float32 | 3,844,000,000 |
| `gist_query.fvecs` | 1,000 | 960 个 float32 | 3,844,000 |
| `gist_groundtruth.ivecs` | 1,000 | 100 个 int32 | 404,000 |

表 ID 使用 `1..1,000,000`。官方 ground truth 为零基 ID，benchmark 按 `MVP_ID_BASE=1`
转换。代表性测试从 1,000 条 query 中含首尾等距抽取 100 条；正式 Recall 使用全部
1,000 条 query。官方 GT 仅支持 K≤100，K=1,000 和 K=10,000 场景只统计性能。

GIST 与 SIFT 行数相同，维度为 SIFT 的 7.5 倍。结果主要反映宽向量 Parquet 解码、
跨层复制、距离计算和内存带宽。跨数据集比较需要分别记录维度、query 集合、
Parquet 文件布局和索引参数；Recall 只在同一数据集、同一 GT 和同一 K 下比较。

## 3. 数据与依赖准备

在可访问 Hugging Face 的黄区服务器执行：

```bash
bash bin/download-gist1m.sh
```

脚本优先使用 `hf` 从公开镜像的固定 revision 下载；未安装 `hf` 时使用 TexMex FTP。
当前环境无法访问 TexMex FTP 时先安装 Hugging Face CLI：

```bash
python3 -m pip install --user -U huggingface_hub hf_xet
```

可通过 `MVP_GIST_URL` 显式指定其他下载入口。下载后先按镜像的 Git LFS OID 校验压缩包
大小 `2,740,172,684` 和 SHA-256
`01469a7f1c3768853525e543d537e2dfa1adece927616405e360952e3f67df73`，再提取三个
数据文件，校验固定大小和所有记录头，并生成 `checksums/GIST1M_SHA256SUMS`。文件
清单固定记录官方文件摘要，后续重复执行下载脚本时直接校验已有文件。

Spark v3 路径需要配置中锁定的 JDK、Spark 3.5 和 Iceberg Spark runtime。PyIceberg v2
兼容路径继续复用现有 wheelhouse，wheel 必须匹配黄区 aarch64、Python ABI 和 glibc。
生成 GIST Spark 离线包：

```bash
MVP_OFFLINE_DATASET=gist1m MVP_OFFLINE_PROVIDER=spark \
  bash bin/make-offline-bundle.sh
```

## 4. 黄区配置

```bash
cp config/gist-perf.env.example gist.env
vi gist.env
bash bin/verify-gist1m.sh
```

至少填写以下绝对路径：

- `SPARK_HOME`：黄区 Spark 3.5 安装目录；
- `ICEBERG_SPARK_RUNTIME_JAR`：与 Spark/Scala 匹配的 Iceberg runtime；
- `JAVA_HOME`：JDK 17 或更高版本安装目录；
- `MVP_GAUSSHOME`：目标 GaussDB 安装目录；
- `MVP_WAREHOUSE_DIR`：NVMe/XFS 上新的空目录。

Bridge v3 路径还需填写 `MVP_BRIDGE_SOURCE` 和 `MVP_CARGO_BIN`。

`MVP_NAMESPACE`、`MVP_TABLE`、warehouse 和索引名均使用 GIST 专用值。`gist.env` 与
`mvp.env` 分离；`run-gist-perf.sh` 通过 `MVP_ENV_FILE` 复用公共脚本，并将结果写入
`state/gist1m/`。

## 5. 固定基线参数

| 层次 | 参数 | 初始值 |
|---|---|---:|
| 数据 | dimension / rows | 960 / 1,000,000 |
| Iceberg | partition / compression | bucket(id, 32) / uncompressed |
| Spark | shuffle partitions / target file size | 32 / 1 GiB |
| IVF | clusters / sample rate | 1,024 / 100,000 |
| 构建 | workers | 8 |
| IVF-PQ | M / nbits | 60 / 8 |
| 查询 | nprobe | 10 |
| 代表性矩阵 | K / DOP / nq | 10,100 / 1,8 / 100 |
| 扩展矩阵 | DOP | 1,2,4,8,16,32,64 |

`M=60` 使每个 PQ 子向量包含 16 维。索引路由以 ABI 组合判定：IVF-Flat 使用
`type=ivf_flat, implementation=ivf`，解析为 `builtin.ivf_flat@2`；IVF-PQ 使用
`type=ivf_pq, implementation=ivf_pq`，解析为 `builtin.ivf_pq@1`。报告同时保存
Registry segment 的 `algorithm_details`，核对实际 clusters、M 和 nbits。

Spark 对全量数据执行一次 append，并使用 Iceberg hash distribution。每个 bucket 的
未压缩向量载荷约 120 MB，低于 1 GiB rolling target，因此文件切分规则要求生成 32 个
Parquet 文件；producer 在完成后核对实际文件数，偏离即失败。PyIceberg 兼容路径只有
单批覆盖全量数据时通常得到相同布局，进程会持有约 3.84 GB 的原始 Float32 buffer。
Bridge v3 路径按 16384 行分批解析并分流到 32 个临时 Arrow IPC 流，再逐 bucket 调用
Bridge 分区写入 ABI，常驻内存由批大小和单 bucket 流控制；完成后同样强制校验 32 个文件。
数据库 `max_process_memory` 约 12 GiB、`shared_buffers` 约 1 GiB、`work_mem` 64 MiB，索引构建
先固定 8 workers；提高 workers 前单独采集 gaussdb 峰值 RSS 和内存错误。无 Swap
环境中出现 OOM 或内存门禁失败时，该轮结果标记失败。

代表性 DOP 保持 1 和 8，便于与 SIFT 基线对齐。扩展矩阵增加 16、32、64，观察
256 CPU、8 NUMA 环境下宽向量扫描扩展性。`query_dop` 不保证线程固定在特定 NUMA
节点；当前 gaussdb affinity 覆盖 0-255 且线程池关闭，报告记录实际计划和进程绑定，
不把 DOP 数值直接解释为 NUMA 节点数。

## 6. 执行

从空 warehouse 完整运行：

```bash
bash bin/run-gist-perf.sh fresh spark
```

复用已接入的 GIST Iceberg 表，清理索引和结果后重跑：

```bash
bash bin/run-gist-perf.sh reuse spark
```

执行顺序固定为 IVF-Flat、IVF-PQ、FullScan。流程结束时不保留索引，配置保持 PQ。
需要复核既有 PyIceberg v2 数据时，把第二个参数改为 `pyiceberg`。
需要验证 DataInfra Bridge v3 写入链路时，把第二个参数改为 `bridge`。
结果目录为：

```text
state/gist1m/
├── preflight.log
├── seed-spark.log 或 seed-bridge.log
├── register-table.log
├── build-index-flat.log
├── build-index-pq.log
├── perf/flat/
├── perf/pq/
├── perf/fullscan/
└── run-perf.log
```

运行扩展矩阵前先选择并构建目标索引，再执行：

```bash
export MVP_ENV_FILE="$PWD/gist.env"
export MVP_STATE_DIR="$PWD/state/gist1m"
bash bin/configure-index.sh pq
bash bin/build-index.sh
python3 bin/run-matrix.py --modes index \
  --output-dir state/gist1m/matrix/index
```

Flat 矩阵将 `pq` 改为 `flat`，清理当前索引后重新构建。FullScan 扩展矩阵先执行
`bash bin/clean.sh index`，再将 `--modes` 改为 `fullscan`。DOP>1 场景必须通过
`LOCAL GATHER dop: 1/N` 计划门禁。

## 7. 结果门禁

每轮结果满足以下条件：

- 原始文件大小、SHA-256 和记录头校验通过；
- Iceberg 表为 1,000,000 行、960 维、一基 ID、bucket[32]；
- producer、compression、snapshot、Parquet 文件数和字节数完整记录；
- Flat 和 PQ 的 Catalog type、implementation、canonical implementation 与 Registry
  `algorithm_details` 一致；
- 索引计划包含 Vector Search 和 bridge vector index scan；
- FullScan 计划不含 bridge vector index scan；
- K≤100 使用同序号官方 GT 计算 ID Recall 和距离阈值 Recall；
- 记录数据库版本、内存参数、线程池状态、CPU/NUMA、文件系统和块设备 ROTA。

## 8. 来源

- [openGauss-Catalog PR #116：register_table 表级向量维度支持](https://github.com/DataInfraLab/openGauss-Catalog/pull/116)
- [TexMex GIST 数据集](ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz)
- [GIST1M 镜像压缩包 Git LFS OID](https://huggingface.co/datasets/fzliu/gist1m/commit/a98d7415dba638216300552059013cc627293409)
- [ANN Benchmarks 数据集说明](https://ann-benchmarks.com/)
- 套件根目录 [`README.md`](../README.md)
