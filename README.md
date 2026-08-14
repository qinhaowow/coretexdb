# CoreTexDB

A multimodal vector database for AI applications, built in Rust.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Rust](https://img.shields.io/badge/Rust-2021-blue.svg)](https://www.rust-lang.org/)
[![Build](https://github.com/cerebros/CoretexDB/actions/workflows/build.yml/badge.svg)](https://github.com/cerebros/CoretexDB/actions/workflows/build.yml)

## Features

- **Vector Search**: HNSW, IVF, BruteForce, Scalar indexing with auto-tuning
- **Multi-modal Embedding**: Text, image, audio, video, point cloud support
- **Hybrid Retrieval**: BM25 full-text + vector search with RRF fusion
- **SQL Interface**: SQL parser, optimizer, and executor
- **Distributed**: 2PC transactions, Raft consensus, edge computing
- **Security**: TLS, AES-256-GCM encryption, RBAC, audit logging
- **Real-time**: CDC, WAL, transactions with isolation levels
- **Geospatial**: 2D/3D spatial indexing with RTree
- **Time Series**: Temporal indexing with aggregation and rolling windows
- **Graph**: Graph database with path queries
- **Observability**: Prometheus metrics, distributed tracing, alerting
- **Multi-language**: REST, gRPC, GraphQL, WebSocket APIs + Python bindings

## Quick Start

### Install from Source

```bash
# Clone the repository
git clone https://github.com/cerebros/CoretexDB.git
cd CoretexDB

# Build with default features
cargo build --release

# Build with all features (RocksDB, ONNX, TLS, metrics)
cargo build --release --features full
```

### Run the Server

```bash
# Start the server (REST + gRPC + GraphQL)
./target/release/coretex server --host 0.0.0.0 --port 5000
```

### CLI Usage

```bash
# Create a collection
./target/release/coretex collection create --name vectors --dimension 128

# Insert vectors
./target/release/coretex vector insert --collection vectors --file data.json

# Search
./target/release/coretex search --collection vectors --query "[0.1, 0.2, ...]" --k 10
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

| Feature | Description | Default |
|---------|-------------|---------|
| `tokio` | Async runtime + WebSocket | Yes |
| `serde` | JSON serialization | Yes |
| `compression` | RLE, delta, quantization | Yes |
| `metrics` | Prometheus monitoring | Yes |
| `rocksdb` | Persistent storage engine | No |
| `onnx` | ONNX model inference | No |
| `tls-gen` | Self-signed TLS certificates | No |
| `python` | PyO3 Python bindings | No |
| `full` | All features enabled | No |

## Configuration

Configuration files are in `config/`:

- `cortex_config.yaml` - Client database config
- `model_config.yaml` - Embedding, LLM, and RAG config
- `robot_config.yaml` - Robot hardware integration config

## Development

```bash
# Run tests
cargo test --features full

# Run benchmarks
cargo bench

# Check for errors
cargo check --features full

# Lint
cargo clippy --features full
```

## Project Structure

```
src/
├── lib.rs                  # Library root
├── main.rs                 # CLI entry point
├── coretex_core/           # Core types and errors
├── coretex_storage/        # Storage engines (Memory, RocksDB)
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

python/
├── coretexdb/              # Python package
│   ├── core.py             # Standalone DB class
│   ├── client.py           # HTTP client (sync/async)
│   └── grpc_client.py      # gRPC client (sync/async)
├── core/                   # Robot memory client
├── llm/                    # LLM decision engine
├── rag/                    # RAG pipeline
└── ros2_integration/       # ROS2 robotics nodes
```

## License

AGPL-3.0 - See [LICENSE](LICENSE) for details.
