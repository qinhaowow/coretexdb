# CoreTexDB

A multimodal vector database for AI applications, built in Rust.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Rust](https://img.shields.io/badge/Rust-2021-blue.svg)](https://www.rust-lang.org/)
[![Build](https://github.com/qinhaowow/coretexdb/actions/workflows/build.yml/badge.svg)](https://github.com/qinhaowow/coretexdb/actions/workflows/build.yml)

> **⚠️ 当前状态**：本仓库目前**只有向量存储与检索基座经过端到端验证**。
> 权威契约见 [`SCOPE-v0.1.md`](SCOPE-v0.1.md)，构建与开发说明见 [`DEVELOPMENT.md`](DEVELOPMENT.md)。
> 下文 Features 中标 🔬 的模块**存在且能编译，但未经端到端验证**，不应视为可用。

## Features

✅ = 已验证（有测试或端到端证据）　🔬 = 未验证

- ✅ **Vector Search**: 精确索引（brute-force）默认；HNSW 为近似索引，需显式指定
- ✅ **Durability**: 追加写日志 + CRC32 校验 + 崩溃恢复 + compaction + TTL
- ✅ **Metadata Filtering**: `$and` / `$or` / `$not` / `$gt` / `$in` / `$regex` 等
- ✅ **Multi-language**: REST API + CLI
- 🔬 **Indexing**: IVF（当前不可用）、Scalar、PQ 量化
- 🔬 **Multi-modal Embedding**: Text, image, audio, video, point cloud support
- 🔬 **Hybrid Retrieval**: BM25 full-text + vector search with RRF fusion
- 🔬 **SQL Interface**: SQL parser, optimizer, and executor（`ORDER BY` 与聚合有已知缺陷）
- 🔬 **Distributed**: 2PC transactions, Raft consensus, edge computing
- 🔬 **Security**: TLS, AES-256-GCM encryption, RBAC, audit logging
- 🔬 **Real-time**: CDC, WAL, transactions with isolation levels
- 🔬 **Geospatial**: 2D/3D spatial indexing with RTree
- 🔬 **Time Series**: Temporal indexing with aggregation and rolling windows
- 🔬 **Graph**: Graph database with path queries
- 🔬 **Observability**: Prometheus metrics, distributed tracing, alerting
- 🔬 **APIs**: gRPC, GraphQL, WebSocket, Python bindings

## Quick Start

### Install from Source

```bash
# Clone the repository
git clone https://github.com/qinhaowow/coretexdb.git
cd coretexdb

# Build（默认 features 足够跑通基座）
cargo build --release

# Windows 交叉编译（从 Linux/WSL，详见 DEVELOPMENT.md）
cargo build --release --target x86_64-pc-windows-gnu
```

> `--features full`（rocksdb / onnx / tantivy / tls-gen）**未经验证**：
> rocksdb 需要 C++ 工具链，onnx 需要下载模型运行时，目前不保证能构建。

### Run the Server

```bash
# REST + gRPC + WebSocket
# 注意：地址/端口是 -a / -p，不是 --host / --port
./target/release/coretex server -a 0.0.0.0 -p 5000

# 或直接用脚本
./run-server.sh        # Linux / WSL
run-server.bat         # Windows
```

### CLI Usage

CLI 每条命令是独立进程，共享同一个 `--data-dir`（默认 `./coretex_data`）。
集合名与向量都是**位置参数**：

```bash
B=./target/release/coretex

# Create a collection（名称位置参数；-d 维度，-m 度量，-i 索引）
$B collection create vectors -d 128 -m cosine

# Insert vectors（<collection> <id> <vector>）
$B vector insert vectors v1 "0.1,0.2,0.3" -m '{"tag":"red"}'

# Search（<vector> 位置参数；-c 集合，-k 数量）
$B search "0.1,0.2,0.3" -c vectors -k 10 --filter '{"tag":"red"}'

# 其他
$B vector count vectors
$B collection list -v
$B doctor
```

## Python Client

### Install

```bash
pip install coretexdb
```

### Usage

```python
from coretexdb import CortexDBClient

client = CortexDBClient(host="localhost", port=5000)

# Create collection
client.create_collection(name="my_vectors", dimension=128)

# Insert vectors
import numpy as np
vectors = np.random.randn(1000, 128).astype(np.float32)
client.insert("my_vectors", vectors)

# Search
query = np.random.randn(128).astype(np.float32)
results = client.search("my_vectors", query, k=10)
for r in results:
    print(f"ID: {r.id}, Score: {r.score}")
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│              API Layer (REST/gRPC/GraphQL/WS)    │
├─────────────────────────────────────────────────┤
│              CLI Layer (clap-based)              │
├─────────────────────────────────────────────────┤
│          Data Layer (DataManager + WAL)          │
├─────────────────────────────────────────────────┤
│       Index Layer (HNSW/IVF/BF/Scalar/BM25)     │
├─────────────────────────────────────────────────┤
│      Storage Layer (Memory / RocksDB optional)   │
└─────────────────────────────────────────────────┘
```

## Feature Flags

持久化**不再依赖** `rocksdb`：默认的 `FileStorage` 是纯 Rust 实现，开箱即用。

| Feature | Description | Default |
| --- | --- | --- |
| `tokio` | Async runtime + WebSocket | Yes |
| `serde` | JSON serialization | Yes |
| `compression` | RLE, delta, quantization | Yes |
| `metrics` | Prometheus monitoring | Yes |
| `rocksdb` | RocksDB 存储引擎（可选，非必需）🔬 | No |
| `onnx` | ONNX model inference 🔬 | No |
| `tls-gen` | Self-signed TLS certificates | No |
| `python` | PyO3 Python bindings 🔬 | No |
| `full` | 上述全部 🔬 **未验证，不保证可构建** | No |

## Configuration

Configuration files are in `config/`:

- `cortex_config.yaml` - Client database config
- `model_config.yaml` - Embedding, LLM, and RAG config
- `robot_config.yaml` - Robot hardware integration config

## Development

**先读 [`DEVELOPMENT.md`](DEVELOPMENT.md)** —— 里面有构建、测试、运行、已知问题与下一步。

```bash
# 基座对拍测试（15 项，应全过）
cargo test --test persistence_and_search

# 存储引擎单测（15 项）
cargo test --lib coretex_storage::file_store

# 全量（注意：存在既有的既有失败，见 SCOPE-v0.1.md §7）
cargo test --no-fail-fast

cargo check
cargo clippy
```

> `--features full` 未经验证，见上方说明。

## Project Structure

```text
src/
├── lib.rs                  # Library root
├── main.rs                 # CLI entry point
├── coretex_core/           # Core types and errors
├── coretex_storage/        # Storage engines (Memory, FileStorage, RocksDB)
│   └── file_store.rs       # ★ v0.1 持久化引擎
├── coretex_index/          # Vector indices (HNSW, IVF, BF, Scalar)
├── coretex_data/           # Data management + transactions
├── coretex_query/          # Query planner and optimizer
├── coretex_sql/            # SQL parser/executor
├── coretex_search_pipeline/ # End-to-end search pipeline
├── coretex_hybrid/         # Multi-modal hybrid retrieval
├── coretex_embedding/      # Embedding service (text/image/audio/video)
├── coretex_bm25.rs         # BM25 full-text search
├── coretex_rerank/         # 2-stage reranking
├── coretex_distributed/    # 2PC distributed transactions
├── coretex_failover.rs     # Raft consensus
├── coretex_grpc/           # gRPC service + client
├── coretex_api/            # REST + GraphQL API
├── coretex_cli/            # CLI commands
├── coretex_security/       # TLS, encryption, ACL, KMS
├── coretex_auth/           # JWT authentication
├── coretex_transaction.rs  # ACID transactions + WAL
├── coretex_gis/            # 2D/3D geospatial
├── coretex_timeseries/     # Time series indexing
├── coretex_graph.rs        # Graph database
├── coretex_document/       # Document parsing
├── coretex_lakehouse/      # Hot/warm/cold tiering
├── coretex_monitoring/     # Prometheus + Grafana
├── coretex_backup.rs       # Backup/restore
├── coretex_compression/    # Vector compression
├── coretex_bio.rs          # Bioinformatics (k-mer)
└── tests_integration.rs    # Integration tests

tests/
└── persistence_and_search.rs  # ★ v0.1 差分对拍测试（与独立暴力参考实现比对）

python/
├── coretexdb/              # Python package
├── core/                   # Robot memory client
├── llm/                    # LLM decision engine
├── rag/                    # RAG pipeline
└── ros2_integration/       # ROS2 robotics nodes
```

## License

AGPL-3.0 - See [LICENSE](LICENSE) for details.
