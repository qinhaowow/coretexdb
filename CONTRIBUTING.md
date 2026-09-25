# Contributing to CoreTexDB

Thanks for your interest in improving CoreTexDB — a multimodal vector database
written in Rust.

> 中文用户请先读 [`README.md`](README.md)（完整中文操作手册）。本文件描述
> 协作流程与工程规范，两种语言的内容是一致的。

---

## 1. Ways to contribute

| 类型 | 说明 |
| --- | --- |
| **Bug reports** | 用 [issue 模板](.github/ISSUE_TEMPLATE/bug_report.yml)，附最小复现与环境信息 |
| **Feature ideas** | 用 [feature 模板](.github/ISSUE_TEMPLATE/feature_request.yml)，先讨论再动手 |
| **Code** | 从 [`docs/roadmap.md`](docs/roadmap.md) 里挑 `good first issue` |
| **Docs** | 补充 `docs/`、示例 `examples/`、翻译 README 的英文小节 |
| **Tests** | 提升覆盖率、补边界与故障注入用例 |

---

## 2. Development environment

### Prerequisites

- **Rust stable**（[rustup](https://rustup.rs/) 管理），CI 使用 stable；
- **protoc**（gRPC 代码生成，`apt install protobuf-compiler` / `brew install protobuf`）；
- **C toolchain + libclang**（`rocksdb`、`ort` 等可选依赖）：Linux 上
  `build-essential clang libclang-dev`；
- 可选：Docker（构建镜像）、Python 3.10+（`python/` 绑定，需要 `maturin`）。

### Build and run

```bash
cargo build                      # 默认 features（最快，日常开发用）
cargo build --release --features full   # 全功能，与发布产物一致
cargo run -- --help              # 查看全部子命令
cargo run -- server              # 启动服务端
```

单二进制分发：一个 `coretex` 可执行文件按 `argv[0]`/子命令分担
`server`、`backup`、`doctor`、`init`、`vector`、`index`、`ttl` 等角色，
详见 README「CLI 命令大全」。

### Test — 这是硬性要求

```bash
cargo test                  # 默认 features，必须全绿
cargo test --features full  # 全功能，提交/推送前必须全绿
```

**规则：每次 push 之前，本地全量 `cargo test` 必须 0 failed。**
只跑单个定向测试是不够的——历史上曾因此把红测试推上远端。

---

## 3. Making a change

### Branching

- 默认分支是 **`master`**（`origin/HEAD → master`）；
- 日常开发在 `release/<version>-base` 与 `feature/<topic>` 分支上进行；
- Pull request 以 `master` 为目标分支。

### Commit messages

采用 [Conventional Commits](https://www.conventionalcommits.org/)：

```
feat(index): persist PQ index codes across restarts
fix(storage): purge memory and index rows when a TTL expires
test(cli): cover `coretex ttl set/remove/purge`
docs(readme): add English quick start
```

类型：`feat` / `fix` / `test` / `docs` / `refactor` / `perf` / `chore`；
范围用模块名（`index`、`storage`、`data`、`cli`、`api`、`grpc`…）。
**正文说明"为什么"，而不是复述"改了什么"。**

### Pull request checklist

- [ ] `cargo test` 与 `cargo test --features full` 全绿；
- [ ] 新功能带测试，修 bug 带回归测试；
- [ ] 公开接口有文档注释（`cargo doc` 能生成）；
- [ ] 改动了行为或命令行，同步更新 `README.md` 与 `docs/`；
- [ ] 一次 PR 只做一件事（拆小更容易审）。

---

## 4. Code style

- `cargo fmt`（配置见 [`rustfmt.toml`](rustfmt.toml)）、`cargo clippy`；
- 异步代码注意**锁顺序**（新增锁请在注释里写明顺序，否则会死锁）：
  - PQ 索引：`training → original_vectors → vectors`（`persist`/`load` 同序）；
  - HNSW：`vectors → entry_point → graph`（`remove`/`clear`/持久化同样遵守）；
  - 索引管理器：先取索引读锁，再进入索引内部锁；
- 新模块必须接到调用链上（`src/lib.rs` 导出 → `CoreTexDB`/CLI/REST 暴露），
  孤立模块是本项目已知的历史问题，见 [`docs/roadmap.md`](docs/roadmap.md)。

---

## 5. Project layout

```
src/
  lib.rs                CoreTexDB 门面：配置、初始化、对外 API
  main.rs               argv[0]/子命令分发（单二进制多角色）
  coretex_core/         核心类型、配置、错误
  coretex_storage/      存储引擎：Memory / File / RocksDB
  coretex_data/         数据管理：集合、写入路径、WAL、恢复、TTL
  coretex_index/        ANN 索引：brute_force / hnsw / ivf / pq + 持久化
  coretex_api/          REST / GraphQL / WebSocket
  coretex_grpc/         gRPC 服务与生成代码
  coretex_cli/          CLI 命令实现
  coretex_backup/       备份与恢复
  coretex_query/        查询与 SQL
tests/                  端到端集成测试
examples/               可运行示例
docs/                   架构与设计文档、路线图
```

---

## 6. Good first contributions

- 补 `examples/` 中的用法示例并加进文档；
- 为 `docs/roadmap.md` 中阶段 A（正确性收口）的剩余项补测试；
- 把 README 的英文快速开始补全；
- 报告一个可复现的边界条件 bug。

---

## 7. Community and conduct

- 行为准则：[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- 安全漏洞请**不要**开公开 issue，见 [`SECURITY.md`](SECURITY.md)
- 变更历史：[`CHANGELOG.md`](CHANGELOG.md)

`AGENTS.md` / `CLAUDE.md` 是本仓库的**会话记忆**（记录版本、分支、网络与
偏好设置），不是给贡献者读的文档，仅供自动化工具参考。

---

## 中文速览

1. 装好 Rust stable + protoc；
2. `cargo build` / `cargo run -- --help` 跑起来；
3. **push 前 `cargo test` 必须全绿**（默认 features 与 `--features full` 都要）；
4. Conventional Commits，PR 拆小、带测试、带文档；
5. 新功能必须真正接线到 CLI/REST/API，不留孤立模块。
