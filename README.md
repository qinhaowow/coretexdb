# CoreTexDB V0.2.4 — 向量数据库 · 操作手册

[![Build](https://github.com/qinhaowow/coretexdb/actions/workflows/build.yml/badge.svg)](https://github.com/qinhaowow/coretexdb/actions/workflows/build.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-blue.svg)](https://www.rust-lang.org/)
![Tests](https://img.shields.io/badge/tests-537%20passed%20%2F%200%20failed-brightgreen)

> 开发者: qinhaowo@126.com
> 辅助工具: MiMo v2.5 大语言模型

---

**CoreTexDB** is a vector database written in Rust. It ships as a
**single `coretex` binary** and provides ANN indexes (`brute_force`, `hnsw`,
`ivf`, `pq`), append-only-log durability, index persistence with
checksum-verified load-or-rebuild, TTL, metadata filtering, B-C-D-D (`.cdb`)
encryption, and **REST + gRPC** server interfaces.

On durability: every write lands in an append-only log under `data/coretex/store/`,
fsynced by default (`DbConfig::sync_writes`). A separate WAL journal also exists
and is selected by `--wal-dir`, but it is **off by default** — the store log
already is the durability mechanism, so a second journal would only duplicate it.
Last-write-wins recovery is what you get; there is no multi-version rollback.

**Not every module in this repository is reachable from the CLI, REST or gRPC.**
Read [§0 能力状态](#0-能力状态) before trusting a feature name you saw in the
source tree.

源码仓库：[GitHub](https://github.com/qinhaowow/coretexdb) ·
[Gitee 镜像](https://gitee.com/HaoqinOW/coretexdb)

### 快速入口 / Quick links

| 想做什么 | 去哪里 |
| --- | --- |
| 30 秒跑起来 | [`cargo run --example mvp`](examples/mvp.rs) — 最小可运行链路，每步带断言 |
| 其他示例 | [`examples/quickstart.rs`](examples/quickstart.rs) · [`filter_search`](examples/filter_search.rs) · [`persistence`](examples/persistence.rs) |
| 过滤搜索、持久化示例 | [`examples/`](examples/) |
| 了解分层、数据流与锁顺序 | [`docs/architecture.md`](docs/architecture.md) |
| 参与开发 / 找活干 | [`CONTRIBUTING.md`](CONTRIBUTING.md) · [`docs/roadmap.md`](docs/roadmap.md) |
| 变更历史 | [`CHANGELOG.md`](CHANGELOG.md) |
| 报告安全漏洞（勿开 issue） | [`SECURITY.md`](SECURITY.md) |

---

## 目录

0. [能力状态（哪些真能用）](#0-能力状态)
1. [安装与启动](#1-安装与启动)
2. [核心概念](#2-核心概念)
3. [服务器模式（服务端 + 客户端连接）](#3-服务器模式)
4. [CLI 命令大全](#4-cli-命令大全)
5. [REST API](#5-rest-api)
6. [GraphQL API（⚠️ 未接线）](#6-graphql-api-)
7. [gRPC API](#7-grpc-api)
8. [SQL 查询](#8-sql-查询)
9. [B-C-D-D 加密（.cdb 文件）](#9-b-c-d-d-加密)
10. [数据目录结构](#10-数据目录结构)
11. [常见问题排查](#11-常见问题排查)

---

## 0. 能力状态

仓库里有相当多模块尚未接线。这里按「你能不能真的用上」分类，不按代码量分类。
未接线的模块仍在 `src/` 里、仍有 doc comment 和单元测试，甚至从 `lib.rs`
`pub use` 导出——但**没有任何 CLI / REST / gRPC 入口能到达它们**。看到源码里有
某个类型，不等于产品里有这个功能。

### ✅ 可用

| 能力 | 入口 |
| --- | --- |
| 向量索引 `brute_force` / `hnsw` / `ivf` / `pq` | CLI · REST · gRPC |
| append-only log 持久化、启动恢复 | 自动 |
| 索引持久化（校验和匹配则加载，否则重建） | 自动 · `coretex index save\|list` |
| TTL `set` / `remove` / `purge` | CLI · REST |
| metadata 过滤 + 倒排过滤索引 | CLI · REST |
| hybrid 搜索（向量 + BM25 → RRF 融合） | REST `/hybrid-search` |
| 两阶段 rerank | `CoreTexDB::hybrid_search_reranked`（库 API，CLI/REST 尚未暴露 `--rerank`） |
| SQL 查询 | CLI `coretex sql` |
| 备份 / 恢复 | CLI · REST |
| 认证 + 限流 | REST · gRPC |
| B-C-D-D（`.cdb`）加密 | CLI `encrypt` / `decrypt` |
| REST · gRPC 服务端 | `coretex server` |
| C FFI（13 个 `extern "C"`） | `include/coretexdb.h` + `libcoretexdb` |
| Python SDK | `python/` |
| 复制**读**接口（主库侧） | REST `/replication/{status,snapshot,entries}` |

### ⚠️ 未接线（代码在仓库里，够不到）

| 模块 | 现状 |
| --- | --- |
| GraphQL API | `start_graphql_server` 无调用者，`coretex server` 不启动它 |
| WebSocket | 无 TCP listener；`handle_search` 恒返回空，`handle_insert`/`handle_delete` 不落库（结构体里没有 `db` 字段）；认证只判断 token 非空 |
| Raft / 主备切换 | `FailoverManager` / `LogReplicator` 从未被实例化。**原 `POST /raft/append_entries` 是个返回硬编码 `success: true` 的假桩（且免认证），已从路由表移除**；`request_vote` / `heartbeat` / `install_snapshot` 从未注册 |
| 冷热分层 lakehouse | `DataManager.lakehouse` 恒为 `None`，`attach_lakehouse` 无调用者 |
| Prometheus / `/metrics` | 无该路由；`coretex admin metrics` 是手写 `println!` |
| 多模态：GIS / 时序 / embedding / 文档解析 / 生信 | 无任何入口调用。5 个模态的 embedding 实现是 `hash` 冒充模型；UDF 对未知函数静默返回 `args.sum()` |
| 增量索引 `IncrementalIndex` | `IndexType::HNSW/IVF/PQ` 三个实现体是 `BruteForceIndex` 的复制粘贴，构造参数全丢弃 |
| GRPO 强化学习 | "梯度" = `权重 × loss × 0.01`，非反向传播；`kl_divergence`、`learning_progress` 恒为 0 |
| CDC（MySQL / PostgreSQL / MongoDB） | binlog / 逻辑复制 / OP_MSG 的**协议解析是真的**，但 `connect()` 只 `eprintln!("connected")` 并伪造位点，从不建连 |
| 端侧 `EdgeDB` | `flush()` 真落盘，但**没有任何 load 路径**，重启必空；`EdgeConfig` 五个 builder 字段无消费者 |
| 审计日志 | `data/logs/audit/` **会被创建，但永远是空的**——`AuditLogger`（`coretex_security`）没有任何入口实例化，且它的 `storage_path` 是相对路径 `audit_log.json`，没走 `--data-dir`。想看操作记录得接 `coretex_security`，目前没有 |
| ONNX 推理 | `ort` 版本与代码 API 不匹配（`cannot find Session in ort`），**从 `full` 移出**。`coretex_onnx` 与 `coretex_embedding/text_embedding.rs` 的 ONNX 引擎都无法编译 |
| Tantivy 全文检索 | tantivy 0.22 API 变化（`Option`→`Result`、`Field` 无 `Default`），**从 `full` 移出**。`coretex_tantivy` 296 行、零调用点 |
| 端侧/wasm（`embedded`/`wasm` feature） | `coretex_edge` 编译不过（本 crate 的 `Result<T>` 别名遮蔽 + 缺 `From<EdgeError>`），**从 `full` 移出** |

> **为什么 `full` 不再包含所有 feature**：`onnx` / `tantivy` / `embedded` / `wasm`
> 这四组的代码**从未通过编译**，错误已存在很久无人发现（`full` 的 CI job 此前
> 因 `s3` 编译错误先失败，`cargo test` 那一步从未执行到）。对应的模块又全部
> 零生产调用点，为它们做 API 迁移等于给死代码做维护。修好后应加回，并给
> `full` 补一条「必须编译通过」的门禁，否则同类腐化会再次静默发生。

逐项进度与取舍见 [`docs/roadmap.md`](docs/roadmap.md)。

> 上面的分类不是免责声明，而是待办清单：`coretex_cli` 对尚未实现的参数一律
> 显式报错（见 `--compression` / `--incremental` / `--target-time`），宁可报错也
> 不收下参数然后忽略。这里沿用同一条原则——README 不该承诺够不到的东西。

---

## 1. 安装与启动

### 1.1 获取可执行文件

发布包为完整安装根（解压即用，或经 `scripts/install.sh` 装到 `/opt/CoreTexDB-V0.2.4`）：

```
CoreTexDB-V0.2.4/                      # 程序安装根目录
├── bin/                               # 可执行文件
│   ├── coretex                        # 唯一主程序（Windows: coretex.exe）
│   └──                                #   子命令 server·backup·doctor·search...
│
├── lib/                               # 动态/静态库
│   ├── libcoretexdb.so                # 核心引擎（Linux）
│   ├── libcoretexdb.dylib             # 核心引擎（macOS）
│   ├── libcoretexdb.a                 # 静态库（供 C++ 链接）
│   ├── coretexdb.dll                  # 动态库（Windows）
│   └── coretexdb.lib                  # 导入库（Windows）
│
├── include/                           # C/C++ 头文件
│   └── coretexdb.h
│
├── config/                            # 配置文件
│   ├── coretex.toml                   # 主配置
│   ├── logging.yaml                   # 日志配置
│   ├── backup.toml / security.toml / metrics.toml
│   └── {dev,staging,prod}/overrides.toml
│
├── share/                             # 静态资源
│   ├── doc/                           # INSTALL / ADMIN_GUIDE / SECURITY
│   └── examples/                      # rust / cpp 示例
│
├── scripts/                           # 管理脚本
│   ├── start.sh / stop.sh / status.sh
│   ├── install.sh / upgrade.sh / uninstall.sh
│   ├── backup.sh / restore.sh
│   └── healthcheck.sh / secure_setup.sh
│
├── systemd/ + logrotate/              # 服务与日志轮转（Linux）
├── VERSION / README.md / LICENSE / RELEASE_NOTES.md
│
└── data/                              # 数据根目录（可通过配置更改）
    ├── coretex/
    │   ├── collections/
    │   ├── indexes/{vector,scalar}/
    │   ├── metadata/
    │   └── store/
    ├── wal/
    ├── backup/{full,incremental,snapshots}/
    ├── logs/audit/          # 创建但恒为空（审计模块未接线，见 §0）
    ├── temp/
    └── versions/
```

Windows 快速验证：解压后执行 `bin\coretex.exe --version`。

### 1.2 验证安装

```cmd
coretex.exe --version
coretex.exe --help
```

### 1.3 全局选项

所有命令都支持：

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--data-dir <路径>` | 数据库目录 | `./coretex_data` |
| `-h, --help` | 查看帮助 | — |
| `-V, --version` | 查看版本 | — |

> **数据目录查找顺序**：若 `--data-dir` 未指定，优先使用已存在的 `~/.coretexdb/data`，否则使用 `./coretex_data`。

---

## 2. 核心概念

| 概念 | 说明 |
|------|------|
| **Collection（集合）** | 一组同维度向量的容器，类似"表" |
| **Vector（向量）** | 一条数据：ID + 浮点数组 + 可选 JSON 元数据 |
| **Dimension（维度）** | 向量长度，创建集合时固定（如 128、384） |
| **Metric（距离度量）** | `cosine` / `euclidean` / `dotproduct` |
| **Index（索引）** | `brute_force`（精确，默认）/ `hnsw` / `ivf` / `scalar` |

---

## 3. 服务器模式

`coretex server` 启动一个**等待客户端连接**的服务端进程，同时提供 REST + gRPC 两种协议。

### 3.1 启动服务器

```cmd
:: 默认：REST :5000 + gRPC :50051
coretex.exe server

:: 自定义端口与参数
coretex.exe server -a 0.0.0.0 -p 8080 --grpc-port 50051 --auth

:: 关闭 gRPC（仅 REST）
coretex.exe server --grpc-port 0

:: 指定数据目录
coretex.exe server --data-dir D:\mydb
```

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `-a, --address` | 监听地址 | `0.0.0.0` |
| `-p, --port` | REST 端口 | `5000` |
| `--grpc-port` | gRPC 端口（0=禁用） | `50051` |
| `--ws-port` | WebSocket 端口 — **接受但未实现**，见 §0 | `8080` |
| `--auth` | 启用认证 | 关闭 |
| `--rate-limit` | 每分钟请求限制（0=不限） | `0` |

启动成功输出示例：

```
Starting CoreTexDB server on 0.0.0.0:5000
gRPC: port 50051
Starting gRPC server on 0.0.0.0:50051
Starting CoreTexDB API server on http://0.0.0.0:5000
```

### 3.2 客户端如何连接

服务器启动后会**一直等待**，需要另开终端用客户端连接。

#### 方式一：REST 客户端（curl）

```cmd
:: 健康检查
curl http://localhost:5000/health

:: 列出集合
curl http://localhost:5000/api/collections

:: 创建集合
curl -X POST http://localhost:5000/api/collections ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"mydb\",\"dimension\":128,\"metric\":\"cosine\"}"

:: 插入向量
curl -X POST http://localhost:5000/api/collections/mydb/vectors ^
  -H "Content-Type: application/json" ^
  -d "{\"id\":\"doc1\",\"vector\":[0.1,0.2,...],\"metadata\":{\"title\":\"测试\"}}"

:: 搜索
curl -X POST http://localhost:5000/api/collections/mydb/search ^
  -H "Content-Type: application/json" ^
  -d "{\"vector\":[0.1,0.2,...],\"k\":10}"
```

#### 方式二：CLI 客户端（另开终端）

```cmd
:: 注意：CLI 直接操作数据目录，不走网络
:: 需指向服务器使用的同一数据目录
coretex.exe --data-dir ./coretex_data collection list
coretex.exe --data-dir ./coretex_data vector count -c mydb
```

> **重要**：CLI 的 collection/vector/search 等命令是**本地模式**，直接读写 `--data-dir` 指向的文件，不连接服务器。服务器模式用于提供 HTTP/gRPC 远程接口。

#### 方式三：gRPC 客户端

使用 gRPC 定义文件 `src/coretex_grpc/coretex.proto` 生成客户端代码，连接 `localhost:50051`。

### 3.3 验证服务是否正常

```cmd
:: 1. 检查端口是否监听
netstat -ano | findstr :5000
netstat -ano | findstr :50051

:: 2. REST 健康检查
curl http://localhost:5000/health

:: 3. 查看进程
tasklist | findstr coretex
```

---

## 4. CLI 命令大全

> 以下示例中 `coretex` 在 Windows 下为 `coretex.exe`。
> 所有命令均可加 `--data-dir <路径>` 指定数据目录。

### 4.1 服务器

```cmd
coretex server [选项]
```

见 [第 3 节](#3-服务器模式)。

### 4.2 集合管理（collection）

```cmd
:: 创建集合
coretex collection create --name mydb --dimension 128 --metric cosine --index hnsw
coretex collection create -n docs -d 384 -m cosine -i brute_force

:: 列出集合
coretex collection list
coretex collection list --verbose          # 详细模式

:: 查看集合信息
coretex collection info --name mydb

:: 查看集合统计
coretex collection stats --name mydb

:: 重命名集合
coretex collection rename --name mydb --to newdb

:: 删除集合（需确认）
coretex collection delete --name mydb
coretex collection delete --name mydb --force   # 跳过确认
```

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `-n, --name` | 集合名 | 必填 |
| `-d, --dimension` | 向量维度 | `384` |
| `-m, --metric` | `cosine` / `euclidean` / `dotproduct` | `cosine` |
| `-i, --index` | `brute_force` / `hnsw` / `ivf` / `scalar` | `brute_force` |

### 4.3 向量操作（vector）

```cmd
:: 插入单条向量
coretex vector insert -c mydb --id doc1 --vector 0.1,0.2,0.3 --metadata "{\"title\":\"测试\"}"

:: 插入/更新（存在则替换）
coretex vector upsert -c mydb --id doc1 --vector 0.1,0.2,0.3

:: 查询向量
coretex vector get -c mydb --id doc1

:: 更新向量（只更新元数据）
coretex vector update -c mydb --id doc1 --metadata "{\"title\":\"更新后\"}"

:: 更新向量（只更新向量值）
coretex vector update -c mydb --id doc1 --vector 0.4,0.5,0.6

:: 按 ID 删除
coretex vector delete -c mydb --ids doc1,doc2,doc3

:: 按元数据过滤删除
coretex vector delete -c mydb --filter "{\"title\":\"测试\"}"

:: 列出向量
coretex vector list -c mydb --limit 10 --offset 0 --with-metadata
coretex vector list -c mydb --format json

:: 统计向量数
coretex vector count -c mydb

:: 清空集合（需确认）
coretex vector clear -c mydb --force

:: 批量导入
coretex vector import -c mydb --file data.json --format json

:: 导出
coretex vector export -c mydb --output data.json --format json
coretex vector export -c mydb --output data.csv --format csv

:: 批量插入（从文件）
coretex vector insert -c mydb --batch vectors.json
```

| 选项 | 说明 |
|------|------|
| `-c, --collection` | 集合名（部分命令支持位置参数） |
| `-i, --id` | 向量 ID |
| `-v, --vector` | 逗号分隔的浮点值 |
| `-m, --metadata` | JSON 元数据（注意 Windows cmd 下用 `\"` 转义双引号） |
| `--filter` | JSON 过滤条件 |
| `--batch` | 批量导入文件路径 |

> **Windows cmd 单引号问题**：cmd 不识别单引号，JSON 参数请用双引号+转义：
> `--metadata "{\"key\":\"value\"}"`
> 或 PowerShell 下直接用单引号：
> `--metadata '{\"key\":\"value\"}'`

### 4.4 向量搜索（search）

```cmd
:: 基本搜索
coretex search -c mydb --vector 0.1,0.2,0.3 --k 10

:: 带元数据过滤
coretex search -c mydb --vector 0.1,0.2,0.3 -k 5 --filter "{\"category\":\"tech\"}"

:: 包含元数据结果
coretex search -c mydb --vector 0.1,0.2,0.3 --with-metadata

:: JSON 输出
coretex search -c mydb --vector 0.1,0.2,0.3 --format json
```

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `-c, --collection` | 集合名 | `default` |
| `-v, --vector` | 查询向量（必填） | — |
| `-k` | 返回结果数 | `10` |
| `--filter` | 元数据过滤 JSON | — |
| `--with-metadata` | 结果包含元数据 | 关闭 |
| `--format` | `text` / `json` | `text` |

### 4.5 SQL 查询（sql）

```cmd
:: 查询
coretex sql "SELECT * FROM mydb WHERE id = 'doc1'"
coretex sql "SELECT COUNT(*) FROM mydb"
coretex sql "SELECT * FROM mydb WHERE category = 'tech' ORDER BY id LIMIT 10"

:: 插入
coretex sql "INSERT INTO mydb (id, title) VALUES ('doc1', '测试')"

:: 更新
coretex sql "UPDATE mydb SET title = '更新' WHERE id = 'doc1'"

:: 删除
coretex sql "DELETE FROM mydb WHERE id = 'doc1'"

:: 从文件读取 SQL
coretex sql --file query.sql

:: JSON / CSV 输出
coretex sql "SELECT * FROM mydb" --format json
coretex sql "SELECT * FROM mydb" --format csv
```

### 4.6 备份恢复（backup / restore）

```cmd
:: 创建备份
coretex backup --name mybackup --output ./backups
coretex backup --incremental                    # 增量备份

:: 恢复
coretex restore --name mybackup --input ./backups --force

:: PITR 时间点恢复
coretex restore --name mybackup --input ./backups --target-time 1700000000
```

### 4.7 数据迁移（migrate）

```cmd
coretex migrate --source /path/to/source --target /path/to/target
coretex migrate --source /path/a --target /path/b --collection mydb --batch-size 5000
```

### 4.8 管理员（admin）

```cmd
:: 用户管理
coretex admin user create --username admin --password 123456 --role admin
coretex admin user list
coretex admin user delete --username user1
coretex admin user grant --username user1 --role reader
coretex admin user revoke --username user1 --role reader

:: 数据库统计
coretex admin stats

:: 健康检查
coretex admin health

:: 统计信息（手写输出，非 Prometheus 格式；见 §0）
coretex admin metrics

:: 配置查看/设置
coretex admin config
coretex admin config --key server.port
coretex admin config --set server.port 8383
```

### 4.9 Token 管理（token）

```cmd
:: 创建 Token（TTL 默认 86400 秒）
coretex token create --username admin --password 123456 --ttl 3600

:: 验证 Token
coretex token verify --token <token>

:: 吊销 Token
coretex token revoke --token <token>
```

### 4.10 集群管理（cluster）

```cmd
:: 查看集群状态
coretex cluster status

:: 添加节点
coretex cluster add-node --node-id node2 --address 192.168.1.100:50051

:: 移除节点
coretex cluster remove-node --node-id node2

:: 重平衡分片
coretex cluster rebalance

:: 故障转移
coretex cluster failover --target node2 --reason "manual"
```

### 4.11 REPL 交互模式

```cmd
coretex repl
coretex repl --history C:\my_history
```

### 4.12 诊断与工具

```cmd
:: 诊断检查
coretex doctor

:: 版本信息
coretex version
coretex version --verbose

:: 基准测试
coretex benchmark --count 1000 --dimension 128 --queries 100 --k 10

:: 查看存储/WAL 文件（可读格式）
coretex dump store
coretex dump store --file /path/to/store-000000.log --limit 100
coretex dump store --output json
coretex dump wal
coretex dump wal --limit 20 --output json

:: 注意：`store` / `wal` 是**位置参数**，不是 `--type`：
::   正确 → coretex dump store --file xxx.log
::   错误 → coretex dump --type store --file xxx.log
```

---

## 5. REST API

服务器启动后，REST API 在 `http://<address>:<port>` 提供服务。

### 5.1 端点列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/metrics` | Prometheus 指标（文本格式 0.0.4；开启 `--auth` 时需 token） |
| GET | `/console` | 浏览器控制台 |
| POST | `/api/auth/login` | 用户登录 |
| POST | `/api/auth/register` | 用户注册 |
| GET | `/api/collections` | 列出所有集合 |
| POST | `/api/collections` | 创建集合 |
| GET | `/api/collections/:name` | 获取集合信息 |
| DELETE | `/api/collections/:name` | 删除集合 |
| GET | `/api/collections/:name/stats` | 集合统计 |
| POST | `/api/collections/:name/vectors` | 插入向量 |
| PUT | `/api/collections/:name/vectors` | 更新向量 |
| GET | `/api/collections/:name/vectors` | 列出向量 |
| GET | `/api/collections/:name/vectors/:id` | 获取单条向量 |
| DELETE | `/api/collections/:name/vectors/:id` | 删除单条向量 |
| DELETE | `/api/collections/:name/vectors` | 批量删除 |
| POST | `/api/collections/:name/vectors/upsert` | 插入/更新 |
| DELETE | `/api/collections/:name/vectors/clear` | 清空集合 |
| PUT | `/api/collections/:name/rename` | 重命名集合 |
| POST | `/api/collections/:name/search` | 搜索向量 |
| POST | `/api/collections/:name/batch-search` | 批量搜索 |
| GET | `/api/collections/:name/count` | 向量计数 |
| POST | `/api/admin/backup` | 创建备份 |
| POST | `/api/admin/restore` | 恢复备份 |
| GET | `/api/admin/backup/list` | 列出备份 |
| POST | `/api/collections/:name/hybrid-search` | 混合搜索（向量 + BM25 → RRF） |
| GET | `/replication/status` | 本节点日志位点 / 是否只读 |
| GET | `/replication/snapshot` | 全量同步载荷 |
| GET | `/replication/entries?since=N` | 增量日志尾部 |

> 复制接口只覆盖**主库侧的读取**。`ReplicaSync` 客户端只被集成测试调用，
> 二进制里不会跑——即「能当主库被拉，不能当从库去拉」。Raft 主备切换未实现，
> 见 §0。

> `/raft/*` **不存在**。此前 `/raft/append_entries` 硬编码返回 `success: true`
> 并在认证白名单内，等于告诉 leader「日志已复制」而实际什么都没写。该端点已
> 删除而非打桩：没有任何代码构造 `FailoverManager`，handler 只能回显常量。

### 5.2 调用示例

```cmd
:: 健康检查
curl http://localhost:5000/health

:: Prometheus 指标（开启 --auth 时需带 token）
curl http://localhost:5000/metrics ^
  -H "Authorization: Bearer <token>"

:: 创建集合
curl -X POST http://localhost:5000/api/collections ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"mydb\",\"dimension\":128,\"metric\":\"cosine\"}"

:: 插入向量
curl -X POST http://localhost:5000/api/collections/mydb/vectors ^
  -H "Content-Type: application/json" ^
  -d "{\"id\":\"doc1\",\"vector\":[0.1,0.2,0.3],\"metadata\":{\"title\":\"测试\"}}"

:: 搜索
curl -X POST http://localhost:5000/api/collections/mydb/search ^
  -H "Content-Type: application/json" ^
  -d "{\"vector\":[0.1,0.2,0.3],\"k\":5}"
```

---

## 6. GraphQL API（⚠️ 未接线）

schema 已实现（`src/coretex_api/graphql/`），但 **`coretex server` 不会启动它**——
`start_graphql_server` 在全仓库没有调用者。下面是 schema 提供的方法，
**当前没有任何方式访问**（无端口、无 CLI 开关、无 REST 转发）。需要时请自行接线，
或直接用 REST / gRPC。

| 类型 | 操作 |
|------|------|
| **Query** | `collections`, `collection`, `vector`, `search`, `batchSearch` |
| **Mutation** | `createCollection`, `deleteCollection`, `insertVectors`, `deleteVectors`, `updateMetadata`, `createUser`, `login`, `deleteUser`, `assignRole`, `revokeToken`, `renameCollection` |
| **Subscription** | `dataChanges`, `allDataChanges` |

---

## 7. gRPC API

- 默认端口：`50051`
- Proto 文件：`src/coretex_grpc/coretex.proto`
- 服务方法：`CreateCollection`, `DeleteCollection`, `ListCollections`, `InsertVectors`, `SearchVectors`, `GetVector`, `DeleteVectors`, `GetCollectionInfo`, `HealthCheck`
- 支持：JWT 认证拦截器（已接入）、指标采集（已接入，每 60 秒打印一次 `[gRPC Metrics]`）、TLS、优雅关闭
- **限流未接入**：`GrpcConfig::rate_limit_per_minute` 与 CLI `--rate-limit` 目前只作用于 REST；gRPC 侧的限流拦截器已实现但没有挂进服务链，启动横幅打印的 `Rate limit:` 数值不生效

> 指标说明：gRPC 指标走 tower `MetricsLayer`，按请求统计 total / success / failed /
> avg_latency。它只统计**到达 gRPC 服务的方法调用**；未认证请求被
> `AuthInterceptor` 挡下时不计入（拦截器在内层）。要判断服务是否被调用过，
> 看 `total` 即可。

---

## 8. SQL 查询

支持语法：

```sql
SELECT columns FROM table [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n]
SELECT COUNT(*) FROM table
SELECT aggregate(col) FROM table [GROUP BY ...] [HAVING ...]
INSERT INTO table (col1, col2) VALUES (val1, val2)
UPDATE table SET col = val [WHERE ...]
DELETE FROM table [WHERE ...]
```

支持 JOIN：`INNER JOIN`, `LEFT JOIN`

---

## 9. B-C-D-D 加密（.cdb 文件）

CoreTexDB 实现 B-C-D-D 四阶段加密协议，产出 `.cdb` 加密文件。

### 9.1 生成密钥

```cmd
coretex crypto keygen
:: 输出: 32字节十六进制密钥（如 a1b2c3...）
```

### 9.2 加密文件

```cmd
coretex crypto encrypt input.txt --key <hexkey> --output output.cdb --cipher aes
coretex crypto encrypt data.bin -k <hexkey> -o data.cdb -c chacha
```

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `-k, --key` | 32字节十六进制密钥（必填） | — |
| `-o, --output` | 输出 `.cdb` 路径 | 自动命名 |
| `-c, --cipher` | `aes`（AES-256-GCM）/ `chacha`（ChaCha20-Poly1305） | `aes` |

### 9.3 解密文件

```cmd
coretex crypto decrypt output.cdb --key <hexkey> --output restored.txt
```

### 9.4 查看 .cdb 文件头

```cmd
coretex crypto info output.cdb
```

显示：魔数 `CTDB`、版本、加密算法、创建时间、会话 ID、发送方公钥等。

### 9.5 完整流程示例

```cmd
:: 1. 生成密钥
coretex crypto keygen
:: → KEY=a1b2c3d4e5f6...

:: 2. 加密
coretex crypto encrypt secret.txt -k %KEY% -o secret.cdb -c aes

:: 3. 查看文件头
coretex crypto info secret.cdb

:: 4. 解密验证
coretex crypto decrypt secret.cdb -k %KEY% -o restored.txt

:: 5. 错误密钥将被拒绝
coretex crypto decrypt secret.cdb -k 0000000000000000000000000000000000000000000000000000000000000000 -o fail.txt
:: → 错误：密钥不匹配
```

---

## 10. 数据目录结构

`--data-dir` 指向**安装根目录**（如 `CoreTexDB-V0.2.4/`），数据统一落在其下的 `data/`：

```
CoreTexDB-V0.2.4/                      # 程序安装根目录（--data-dir）
├── bin/                               # 可执行文件
│   └── coretex / coretex.exe          # 唯一主程序（server/backup/doctor/...）
│
├── lib/                               # 动态/静态库
│   ├── libcoretexdb.so / .dylib / .a
│   └── coretexdb.dll / coretexdb.lib
│
├── include/
│   └── coretexdb.h                    # C/C++ 头文件
│
├── config/
│   ├── coretex.toml                   # 主配置
│   ├── logging.yaml                   # 日志配置
│   └── backup.toml / security.toml / metrics.toml
│
├── share/
│   ├── doc/                           # 文档
│   └── examples/                      # 示例代码
│
├── scripts/
│   ├── start.sh / stop.sh
│   ├── backup.sh / restore.sh
│   └── install.sh / upgrade.sh
│
├── systemd/ + logrotate/              # Linux 服务与轮转
│
└── data/                              # 数据根目录（可通过配置更改）
    ├── coretex/                       # 数据库主数据
    │   ├── collections/               # 集合数据
    │   │   ├── products/
    │   │   └── users/
    │   ├── indexes/                   # 索引数据
    │   │   ├── vector/
    │   │   └── scalar/
    │   ├── metadata/                  # 元数据
    │   │   ├── metadata.json
    │   │   ├── config.toml
    │   │   └── auth.json
    │   └── store/
    │       └── store-000000.log       # 向量数据（二进制追加日志）
    │
    ├── wal/                           # 预写日志
    │   ├── wal-000001.log
    │   └── wal-000002.log
    │
    ├── backup/                        # 备份目录
    │   ├── full/                      # 全量备份
    │   │   └── backup-20260924/
    │   ├── incremental/               # 增量备份
    │   │   └── backup-20260924-001/
    │   └── snapshots/
    │
    ├── logs/                          # 运行日志
    │   ├── coretex.log
    │   ├── error.log
    │   ├── slow_query.log
    │   └── audit/
    │
    ├── temp/                          # 临时文件
    └── versions/                      # 版本快照
```

> 查看二进制文件内容请用：
> ```cmd
> coretex dump store
> coretex dump wal
> ```

---

## 11. 常见问题排查

### Q1: 启动服务器后，客户端怎么连？

服务器启动后**等待客户端**，不会自动连接。验证方式：

```cmd
:: 终端1：启动
coretex server

:: 终端2：测试
curl http://localhost:5000/health
```

CLI 的 `collection`/`vector`/`search` 是**本地模式**，直接读写数据目录，不走网络。

### Q2: 端口被占用

```cmd
netstat -ano | findstr :5000
taskkill /F /PID <pid>
```

或换端口：`coretex server -p 8080 --grpc-port 50052`

### Q3: JSON 参数解析失败（Windows cmd）

cmd 不支持单引号，用双引号+转义：

```cmd
:: 错误
coretex vector insert -c mydb -i doc1 -v 1,2,3 -m '{"key":"val"}'

:: 正确
coretex vector insert -c mydb -i doc1 -v 1,2,3 -m "{\"key\":\"val\"}"
```

### Q4: 维度不匹配

集合创建时维度已固定（如 128），插入向量必须同维度：

```cmd
coretex collection create -n mydb -d 128       # 创建时固定 128
coretex vector insert -c mydb -i doc1 -v 1,2,...（必须128个值）
```

### Q5: 找不到数据

确认 `--data-dir` 指向正确目录：

```cmd
coretex --data-dir ./coretex_data collection list
coretex doctor                                 # 诊断检查
```

### Q6: 如何查看 WAL / 存储内容

```cmd
coretex dump store --limit 50
coretex dump wal --limit 20 --output json
```

### Q7: gRPC 端口没有监听

确认启动参数 `--grpc-port` 非 0，且输出中有：

```
Starting gRPC server on 0.0.0.0:50051
```

验证：

```cmd
netstat -ano | findstr :50051
```

---

## 开发信息

- 版本：V0.2.4
- 开发者：qinhaowo@126.com
- 辅助工具：MiMo v2.5 大语言模型
- 测试状态：537 通过 / 0 失败（`cargo test --features full`，含 455 单元 + 82 集成）
- CI 门禁：10 个 feature 逐个编译 + `full --all-targets`；4 个 example 真跑
- 许可证：[AGPL-3.0](LICENSE)
- 贡献指南：[CONTRIBUTING.md](CONTRIBUTING.md) · 行为准则：[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- 架构与设计：[docs/architecture.md](docs/architecture.md) · 路线图：[docs/roadmap.md](docs/roadmap.md)
- 变更日志：[CHANGELOG.md](CHANGELOG.md) · 发布说明：[RELEASE_NOTES.md](RELEASE_NOTES.md)

> 本会话记忆文件 `AGENTS.md` / `CLAUDE.md` 供自动化工具使用，不是面向贡献者的文档。
