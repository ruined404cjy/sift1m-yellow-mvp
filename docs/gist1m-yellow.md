# 黄区 GIST1M 性能测试

## 1. 测试范围

GIST1M 套件在同一代码库中复用 SIFT1M 已验证的 Catalog 接入、索引构建、计划门禁、
Recall 和延迟统计链路。GIST 使用独立配置文件 `gist.env`、独立 Catalog 表、独立
warehouse 和 `state/gist1m/` 状态目录。SIFT 的 `mvp.env` 和 `state/` 保持原有语义。

首版 GIST 流程固定为：

```text
官方 GIST1M fvecs/ivecs
  → PyIceberg 写入 Iceberg v2 bucket(id, 32)
  → Catalog 创建 floatvector(960) 表并接入 snapshot
  → IVF-Flat（ivf_flat + ivf）
  → IVF-PQ（ivf_pq + ivf_pq）
  → 清理索引后 FullScan
  → 官方 GT Recall、QPS、p50/p95/p99
```

Spark 和 Rust 供数路径不参与首版 GIST 基线。该边界将新增代码集中在定长 fvecs
读取和 PyIceberg 公共链路，避免引入第二套 Catalog、索引和 benchmark 实现。

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

GIST 首版复用套件现有 PyIceberg wheelhouse。wheel 必须匹配黄区 aarch64、Python ABI
和 glibc。生成仅含 GIST 数据与 PyIceberg 制品的离线包：

```bash
MVP_OFFLINE_DATASET=gist1m MVP_OFFLINE_PROVIDER=pyiceberg \
  bash bin/make-offline-bundle.sh
```

## 4. 黄区配置

```bash
cp config/gist-perf.env.example gist.env
vi gist.env
bash bin/install-pyiceberg-offline.sh
bash bin/verify-gist1m.sh
```

至少填写以下绝对路径：

- `MVP_PYTHON_BIN`：套件隔离虚拟环境中的 Python；
- `MVP_GAUSSHOME`：目标 GaussDB 安装目录；
- `MVP_WAREHOUSE_DIR`：NVMe/XFS 上新的空目录。

`MVP_NAMESPACE`、`MVP_TABLE`、warehouse 和索引名均使用 GIST 专用值。`gist.env` 与
`mvp.env` 分离；`run-gist-perf.sh` 通过 `MVP_ENV_FILE` 复用公共脚本，并将结果写入
`state/gist1m/`。

## 5. 固定基线参数

| 层次 | 参数 | 初始值 |
|---|---|---:|
| 数据 | dimension / rows | 960 / 1,000,000 |
| Iceberg | partition / compression | bucket(id, 32) / uncompressed |
| PyIceberg | batch rows | 1,000,000 |
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

PyIceberg 单批供数用于稳定生成约 32 个分区数据文件，进程会持有约 3.84 GB 的原始
Float32 buffer，并产生 Arrow/分区写入开销。黄区主机内存容量可覆盖该路径。数据库
`max_process_memory` 约 12 GiB、`shared_buffers` 约 1 GiB、`work_mem` 64 MiB，索引构建
先固定 8 workers；提高 workers 前单独采集 gaussdb 峰值 RSS 和内存错误。无 Swap
环境中出现 OOM 或内存门禁失败时，该轮结果标记失败。

代表性 DOP 保持 1 和 8，便于与 SIFT 基线对齐。扩展矩阵增加 16、32、64，观察
256 CPU、8 NUMA 环境下宽向量扫描扩展性。`query_dop` 不保证线程固定在特定 NUMA
节点；当前 gaussdb affinity 覆盖 0-255 且线程池关闭，报告记录实际计划和进程绑定，
不把 DOP 数值直接解释为 NUMA 节点数。

## 6. 执行

从空 warehouse 完整运行：

```bash
bash bin/run-gist-perf.sh fresh
```

复用已接入的 GIST Iceberg 表，清理索引和结果后重跑：

```bash
bash bin/run-gist-perf.sh reuse
```

执行顺序固定为 IVF-Flat、IVF-PQ、FullScan。流程结束时不保留索引，配置保持 PQ。
结果目录为：

```text
state/gist1m/
├── preflight.log
├── seed-pyiceberg.log
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

- [TexMex GIST 数据集](ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz)
- [GIST1M 镜像压缩包 Git LFS OID](https://huggingface.co/datasets/fzliu/gist1m/commit/a98d7415dba638216300552059013cc627293409)
- [ANN Benchmarks 数据集说明](https://ann-benchmarks.com/)
- 套件根目录 [`README.md`](../README.md)
