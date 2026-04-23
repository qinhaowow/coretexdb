# CortexDB - 多模态向量数据库

<p align="center">
  <img src="https://img.shields.io/badge/Rust-1.70+-orange" alt="Rust">
  <img src="https://img.shields.io/badge/Python-3.8+-blue" alt="Python">
  <img src="https://img.shields.io/badge/License-AGPL--3.0-green" alt="License">
  <img src="https://img.shields.io/badge/Version-1.0.0-yellow" alt="Version">
</p>

CortexDB 是一个高性能的多模态向量数据库，专为 AI 应用设计。它支持向量存储、全文搜索、混合查询、事务处理和边缘部署。

## ✨ 特性

### 核心功能
- **向量索引**: BruteForce, HNSW, IVF, PQ 多种索引算法
- **混合查询**: 向量相似度 + 元数据过滤 + BM25 全文搜索
- **CRUD 操作**: Create, Read, Update, Delete, Upsert, Bulk
- **实时索引**: 增量索引更新，无需重建

### 数据同步
- **CDC**: 变更数据捕获，支持 PostgreSQL, MySQL, MongoDB
- **WAL**: 预写日志，保证数据持久性

### 事务与版本控制
- **MVCC**: 多版本并发控制
- **时间旅行**: 查询历史版本数据
- **ACID**: 原子性、一致性、隔离性、持久性

### 部署模式
- **服务端**: gRPC/REST API 服务模式
- **嵌入式**: 轻量级嵌入式模式，适合边缘设备
- **WASM**: WebAssembly 支持，浏览器运行

### 安全
- **AES-256-GCM**: 端到端加密
- **TLS/SSL**: 传输层加密
- **ACL**: 访问控制列表
- **审计日志**: 安全事件追踪

## 🚀 快速开始

### Rust 使用

```rust
use coretex_core::{CoreTexDB, DbConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = CoreTexDB::new();
    
    // 创建集合
    db.create_collection("documents", 128, "cosine").await?;
    
    // 插入向量
    let vectors = vec![
        ("doc1", vec![0.1; 128]),
        ("doc2", vec![0.2; 128]),
    ];
    db.insert_vectors("documents", vectors).await?;
    
    // 搜索
    let results = db.search("documents", vec![0.1; 128], 5).await?;
    println!("Found {} results", results.len());
    
    Ok(())
}
```

### Python 使用

```python
from coretexdb import CortexDBGrpcClient
import numpy as np

# 连接服务器
client = CortexDBGrpcClient(host="localhost", port=50051)

# 创建集合
client.create_collection("test", dimension=128, metric="cosine")

# 插入向量
vectors = [np.random.rand(128).astype(np.float32) for _ in range(10)]
client.insert_vectors("test", vectors)

# 搜索
query = np.random.rand(128).astype(np.float32)
results = client.search("test", query, k=3)
```

### 边缘部署

```rust
use coretex_edge::EdgeDB;

// 创建嵌入式数据库
let db = EdgeDB::in_memory();
db.init().await?;

// 使用方式相同
db.create_collection("local", 128).await?;
db.insert("local", "vec1", vector).await?;
```

## 📊 性能

| 索引类型 | 内存占用 | 搜索速度 | 召回率 |
|---------|---------|---------|--------|
| BruteForce | 高 | 慢 | 100% |
| HNSW | 高 | 快 | ~95% |
| IVF | 中 | 中 | ~90% |
| PQ | 低 | 中 | 可控 |

## 🏗️ 项目结构

```
CoretexDB/
├── src/
│   ├── coretex_core/        # 核心数据结构
│   ├── coretex_index/       # 向量索引 (HNSW, IVF, PQ)
│   ├── coretex_storage/     # 存储引擎
│   ├── coretex_query/       # 查询处理
│   ├── coretex_bm25/        # BM25 全文搜索
│   ├── coretex_incremental/ # 增量索引
│   ├── coretex_cdc/        # CDC 数据同步
│   ├── coretex_transaction/ # 事务与版本控制
│   ├── coretex_edge/        # 边缘部署
│   ├── coretex_security/    # 安全加密
│   ├── coretex_grpc/       # gRPC 服务
│   └── coretex_onnx/       # ONNX 推理
├── python/
│   └── coretexdb/          # Python SDK
├── benches/                 # 性能测试
└── examples/               # 示例代码
```

## 🔧 构建

```bash
# 构建 Rust
cargo build --release

# 构建 Python
cd python
pip install -e .
```

## 📝 License

AGPL-3.0 - see [LICENSE](LICENSE) for details.

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
