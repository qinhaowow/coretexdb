# CortexDB - 版本信息

**CortexDB** 是一个企业级多模态向量数据库，专为 AI 应用设计。

---

## 开源版

### 许可证

**Apache License 2.0** - 商业友好型开源许可证

### 核心功能

#### 存储层 ✅

| 模块 | 功能 | 状态 |
|------|------|------|
| MemoryStorage | 内存存储 | ✅ |
| PersistentStorage | 持久化存储 | ✅ |
| RocksDB 集成 | 磁盘存储 | ✅ |
| LSM Tree | 日志结构合并树 | ✅ |
| 列式存储 | 分析查询优化 | ✅ |

#### 索引层 ✅

| 索引类型 | 算法 | 状态 |
|----------|------|------|
| 向量索引 | HNSW | ✅ |
| | DiskANN | ✅ |
| | BruteForce | ✅ |
| 标量索引 | B-Tree | ✅ |
| | Hash | ✅ |
| 全文搜索 | Tantivy | ✅ |
| 图索引 | HNSW | ✅ |

#### API 层 ✅

| 协议 | 实现 | 状态 |
|------|------|------|
| REST | Axum | ✅ |
| gRPC | Tonic | ✅ |
| PostgreSQL | Postgres Wire | ✅ |
| GraphQL | Juniper | ✅ |

#### 分布式层 ✅

| 功能 | 实现 | 状态 |
|------|------|------|
| 集群管理 | Gossip | ✅ |
| 分片策略 | 一致性哈希 | ✅ |
| 服务发现 | etcd/Consul | ✅ |
| 负载均衡 | RoundRobin | ✅ |

---

## 构建命令

```bash
# 基础安装
cargo build --release

# 指定特性
cargo build --release --features "api,grpc,distributed"

# Python 支持
cargo build --release --features python
```

---

## 版本历史

### v0.2.0

- 完整向量数据库核心功能
- 多种索引支持 (HNSW, DiskANN, BruteForce)
- REST/gRPC/PostgreSQL API
- 分布式集群支持
- 完整的测试套件
- CI/CD 自动化

### v0.1.0

- 初始版本
- 基础存储引擎
- 简单查询功能

---

## 许可证

本项目采用 Apache License 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

---

## 贡献

欢迎贡献代码！请阅读 CONTRIBUTING.md 了解详情。
