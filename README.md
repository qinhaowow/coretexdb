# CoretexDB - 企业级多模态向量数据库

<div align="center">

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust Version](https://img.shields.io/badge/Rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![CI Status](https://img.shields.io/github/actions/workflow/status/cortexdb/cortexdb/Ci.yaml?branch=main)](https://github.com/cortexdb/cortexdb/actions)
[![Coverage](https://img.shields.io/codecov/c/github/cortexdb/cortexdb)](https://codecov.io/gh/cortexdb/cortexdb)
[![Documentation](https://img.shields.io/docsrs/cortexdb)](https://docs.rs/cortexdb)

**CortexDB** 是一个企业级分布式向量数据库，专为 AI 应用设计，支持多模态数据存储与检索。

[English](README.md) | [中文](README_CN.md)

</div>

## 特性

### 核心特性

- **高性能向量搜索**：支持十亿级向量毫秒级检索
- **多模态支持**：文本、图像、音频等任意向量数据
- **分布式架构**：水平扩展，支持多节点集群
- **强一致性**：基于 Raft 协议的分布式共识
- **企业级安全**：RBAC 权限管理，数据加密

### 高级特性

| 模块 | 功能 |
|------|------|
| **AI 引擎** | 自动调参、模型仓库、联邦学习、隐私计算 |
| **聚类分析** | Streaming K-Means、BIRCH、ANN 索引 |
| **调度系统** | 优先级调度、公平分享、令牌桶策略 |
| **服务发现** | etcd/Consul 集成、动态注册 |
| **负载均衡** | 一致性哈希、轮询、最少连接 |

## 快速开始

### 安装

```bash
# 从源码编译
git clone https://github.com/cortexdb/cortexdb.git
cd cortexdb
cargo install --path . --features full

# 或者使用 Docker
docker run -p 8080:8080 cortexdb/cortexdb:latest
```

### 使用示例

```rust
use cortexdb::{CortexDB, Vector};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = CortexDB::new().await?;
    
    // 存储向量
    let vector = Vector::new(vec![0.1, 0.2, 0.3, 0.4]);
    db.store("doc1", &vector, &serde_json::json!({
        "text": "Hello, CortexDB!"
    })).await?;
    
    // 相似性搜索
    let results = db.search(&[0.1, 0.2, 0.3, 0.4], 10).await?;
    
    Ok(())
}
```

### Python API

```python
pip install cortexdb

from cortexdb import CortexDB

db = CortexDB()
db.store("doc1", [0.1, 0.2, 0.3, 0.4], {"text": "Hello!"})
results = db.search([0.1, 0.2, 0.3, 0.4], k=10)
```

## 项目架构

```
CortexDB/
├── src/
│   ├── cortex_core/      # 核心类型、配置、模式
│   ├── cortex_storage/    # 存储引擎
│   ├── cortex_index/      # 索引管理
│   ├── cortex_query/      # 查询处理
│   ├── cortex_api/        # REST/gRPC API
│   ├── cortex_distributed/ # 分布式协调
│   ├── cortex_security/   # 安全认证
│   ├── cortex_monitor/    # 监控日志
│   ├── cortex_backup/     # 备份恢复
│   ├── scheduler/         # 任务调度
│   ├── worker/           # 工作节点
│   ├── clustering/       # 聚类算法
│   ├── sharding/         # 数据分片
│   ├── discovery/        # 服务发现
│   └── coordinator/      # 领导选举
├── python/               # Python SDK
├── deploy/              # 部署配置
├── .github/workflows/    # CI/CD 流水线
└── tests/               # 测试用例
```

## 功能模块

### 存储层

| 模块 | 功能 | 状态 |
|------|------|------|
| MemoryStorage | 内存存储 | ✅ |
| PersistentStorage | 持久化存储 | ✅ |
| RocksDB 集成 | 磁盘存储 | ✅ |
| LSM Tree | 日志结构合并树 | ✅ |
| 列式存储 | 分析查询优化 | ✅ |

### 索引层

| 索引类型 | 算法 | 状态 |
|----------|------|------|
| 向量索引 | HNSW | ✅ |
| | DiskANN | ✅ |
| | BruteForce | ✅ |
| 标量索引 | B-Tree | ✅ |
| | Hash | ✅ |
| 全文搜索 | Tantivy | ✅ |
| 图索引 | HNSW | ✅ |

### API 层

| 协议 | 实现 | 状态 |
|------|------|------|
| REST | Axum | ✅ |
| gRPC | Tonic | ✅ |
| WebSocket | Tungstenite | ✅ |
| PostgreSQL | Postgres Wire | ✅ |
| GraphQL | Juniper | ✅ |

### 分布式层

| 功能 | 实现 | 状态 |
|------|------|------|
| 集群管理 | Gossip | ✅ |
| 分片策略 | 一致性哈希 | ✅ |
| 领导选举 | Raft | ✅ |
| 服务发现 | etcd/Consul | ✅ |
| 负载均衡 | 多种策略 | ✅ |

## 构建配置

### 特性标志

```bash
# 基础安装
cargo build --release

# 完整功能
cargo build --release --features full

# 指定特性
cargo build --release \
    --features "grpc,distributed,postgres"

# Python 支持
cargo build --release --features python
```

### Docker 构建

```bash
# 构建镜像
docker build -t cortexdb:latest .

# 多架构构建
docker buildx build -t cortexdb:latest \
    --platform linux/amd64,linux/arm64 \
    --push .
```

## 测试

```bash
# 运行所有测试
cargo test --workspace

# 运行集成测试
cargo test --test integration

# 运行基准测试
cargo bench

# 代码覆盖率
cargo tarpaulin --workspace
```

## 文档

- [用户指南](docs/user-guide.md)
- [API 文档](https://docs.cortexdb.io/api)
- [架构设计](docs/architecture.md)
- [部署指南](docs/deployment.md)
- [最佳实践](docs/best-practices.md)

## 性能基准

| 数据集 | 向量维度 | 向量数量 | QPS | P99 延迟 |
|--------|----------|----------|-----|----------|
| SIFT-1M | 128 | 1,000,000 | 50,000 | 2ms |
| GIST-1M | 960 | 1,000,000 | 10,000 | 5ms |
| Deep-1B | 96 | 1,000,000,000 | 1,000 | 50ms |

## 贡献指南

欢迎贡献代码！请阅读 [贡献指南](CONTRIBUTING.md)。

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

## 路线图

### v0.3.0 (Q2 2024)
- [ ] 多租户支持
- [ ] 实时复制
- [ ] 增量备份
- [ ] 自动化运维 API

### v0.4.0 (Q3 2024)
- [ ] 图数据库支持
- [ ] 时序数据支持
- [ ] 边缘计算集成
- [ ] 多云部署

## 许可证

本项目采用 Apache License 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

## 社区

- [Discord](https://discord.gg/cortexdb)
- [Twitter](https://twitter.com/cortexdb_io)
- [LinkedIn](https://linkedin.com/company/cortexdb)

## 致谢

感谢所有贡献者的支持！

<div align="center">

**用 ❤️ 构建**

</div>
