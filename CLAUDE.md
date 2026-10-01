# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-10-01）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.4**（`VERSION` / `Cargo.toml` / `Cargo.lock` / `RELEASE_NOTES.md` 一致） |
| 工作分支 | `release/v0.2.1-base`（默认分支仍 `master`） |
| 最新 commit | `8a6e6ee` — `perf(data): inverted metadata index makes filtered pre-selection sublinear`（B5，本地未推送） |
| tag | `v0.2.4` → `53292cd`（指向 `f0032b1`，已推远端，release.yml `v*` 触发）；历史 `v0.2.3`→`c14e486`、`v0.2.2`→`e12b6f6` |
| 工作区 | **本地领先 origin 5 提交未推**：`ee5b02e` B2a hybrid → `e122d80` B2b REST/CLI → `91353e9` B1 FFI+rerank → `663b295` B6-B8 Python → `8a6e6ee` B5 过滤索引；另一会话 9 文件 clippy WIP（cli/compression/crypto/http_rpc/index/s3/persistence/security/sql）未收口 |

### 近期完成（2026-10-01，B 线主体收口，均已本地提交未推送）
1. **B1 完整 C FFI** ✅ `91353e9`：手写 `include/coretexdb.h`（13 `extern "C"` + 状态码宏 + 所有权/线程约定）+ `src/coretex_ffi.rs`（`CoreTexDbHandle{db, rt}` 2-worker runtime、`catch_unwind`、线程局部 last_error）+ `tests/ffi_api.rs` 7 例（头↔源符号一致性守护）+ `share/examples/c/main.c` 真编译真运行 + `scripts/build_ffi_example.sh`；重复建集合经死变体 `CollectionAlreadyExists` 源映射为 `-3`
2. **B2 rerank 收口** ✅ 同批：`TwoStageSearchPipeline::search_with_callback` 原喂硬编码 mock——新增 `search_with_documents` 真回调 + `CoreTexDB::hybrid_search_reranked`（细排对真实 metadata 文本、每次新建 pipeline、无文本查询逐位透传 RRF）；`tests/rerank_search.rs` 3 例 + pipeline 单测 3 例
3. **B6/B7/B8 Python 三件** ✅ `663b295`：PEP 621 `pyproject.toml`（setuptools 后端 + dynamic version=1.0.12 单源，实测 `pip install -e .` 拿到元数据；CI 的 rm-pyproject 空操作删除）；类名 `CortexDB*`→`CoreTexDB*` 主名 + 旧名同对象别名至 1.0（测试断言 `alias is canonical`）；示例品牌 16 处 + 方法签名核对 + 修过时 `bin coretex-server` 启动命令；16 单测绿
4. **B5 过滤索引** ✅ `8a6e6ee`：`coretex_data/filter_index.rs` metadata 倒排（(字段,规范值)→ids + 存在集），`data_version` 在 data.read 锁内校验；`scan` 出候选超集（等值/$in/单 $ne/$exists 精确、范围/$regex 收窄存在集、$and/$or 交并、$not 回退），`search_filtered`/`delete_vectors_where` 候选迭代 + 精筛保精确；单测 38 种 filter 形状对拍超集性质 + 1000 条窄查询 100 候选断言；集成 6 例 + A3 回归 6 例绿
5. **B4 用户拍板暂缓**：三孤立模块（ann/graph/tantivy 3.8k 行）零调用点、文档零承诺——C/D 后再定接线或删除

### 之前完成（2026-09-25）
1. **PQ 真量化索引**（`d3f78c7`）：clone_box 共享 Arc、`layout_for` 自适应维度、惰性 `maybe_train`、码本 `decode`、`IndexType::PQ` 接线
2. **开源项目整备**（`1881474` fix + `a2f19bb` chore(oss)）：
   - 修复 5 处未编译/未检查缺陷：`.gitignore` 根锚定 `/coretex_data/`、`benches/vector_index.rs` 从未编译、Python 包名/CLI 用法过时、5 个 deny 级 clippy、坏 doc 链接
   - 治理文档：`CONTRIBUTING.md` `SECURITY.md` `CODE_OF_CONDUCT.md` `CHANGELOG.md` `.editorconfig` `rustfmt.toml` issue 表单×3 PR 模板
   - CI 新增 `lint`（clippy deny 级 + rustdoc）与 `examples`（编译+实跑）job，PR 触发补 `release/*`
   - `examples/{quickstart,filter_search,persistence}.rs` 本地实跑验证；`Cargo.toml` repository 改 `github.com/qinhaowow/coretexdb`
3. **V0.2.4 发布**（`f0032b1` + tag `v0.2.4`）：17 文件 35 处版本号、`RELEASE_NOTES.md` 重写、`CHANGELOG.md` `[0.2.4] - 2026-09-25`、`SECURITY.md` 版本表
4. **A3 过滤搜索性能** ✅ `25899d9`：`search_filtered` 三路径——候选 ≤ `max(256, k*16)` 或无索引走精确扫描；宽过滤让 ANN 索引过采样提案再过滤+同一距离函数重算；存活提案 < k 回退精确扫描（**过滤永远不能让查询变短**）。锁序 data.read → 索引内部，候选借引用不 clone。`tests/filtered_search.rs` 6 例（含"提案全被拒绝必须回退拿满 k"回归）
5. **B2a hybrid 搜索接线（本次，待提交）**：`CoreTexDB::hybrid_search`（向量路 + BM25 文本路 → RRF 融合，单侧可用；filter 两侧生效、文本 rank 过滤后赋值）；BM25 缓存按 `DataManager::data_version` 失效（`write_data()` helper 统一接管 17 处写锁拿锁即 bump）；`BM25Index::add_documents` 批量建 O(n)（原循环单加是 O(n²)）；修 `rrf_fusion` sources 按 id 收集 + 并列分 id tie-break；**0 分命中过滤**（BM25 对无词文档打 0 分仍占 top-k，会污染融合）。`tests/hybrid_search.rs` 7 例
6. （0.2.3 时代）索引持久化接线：`VectorIndex::persist` + `IndexManager::load_index`、原子写+校验和防陈旧索引、`restore_from_storage` 两阶段、CLI `coretex index save|list`
- Windows 验收：`E:\Ubuntn24042\wintest` 脚本 `windows_acceptance.ps1 -Root <dir>`，**19/19 全绿**（0.2.3 单 exe 构建）
- Linux 测试基线：**491** 全绿（485 + A3 新增 6）；clippy `--all-targets` 0 error；rustdoc 0 warning

### 之前完成（V0.2.2 发布线）
1. `release.yml`：多 bin + `--target` 正确产物路径 + sha256 + GitHub Release
2. 版本号全线 0.2.1 → 0.2.2（含 systemd/scripts/docs/install 默认 `/opt/CoreTexDB-V0.2.2`）
3. 完整安装根架构打进 Release：`bin/ lib/ include/ config/ share/ scripts/ systemd/ logrotate/ data/` 骨架
4. `include/coretexdb.h` PATCH 已改为 2
5. 本地编译缓存已清（`target/` 3.9G、`package/`）

### 用户偏好 / 约束
- **不要主动 push / 打 tag**，除非明确要求
- 中文沟通
- 清理过本地编译垃圾；`~/.cargo/registry`（~710M）可保留加速重编

## 环境要点

- **代码在 WSL**：`\\wsl.localhost\Ubuntu\home\qh\CoreTexDB` 或 WSL 内 `/home/qh/CoreTexDB`
- **GitHub HTTPS 被墙**：DNS 污染 `github.com→127.0.0.1`；只能 **SSH over 443**
- SSH 配置：`~/.ssh/config` → `HostName 20.205.243.160` Port 443（IP 会变，用 `refresh-github-ssh-ip.sh` 刷新）
- 推送命令需：`export GIT_SSH_COMMAND='ssh -p 443 -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes'`
- 偶发 `remote: Internal Server Error`：重试即可
- GitHub API/HTTPS 本机不可用，Actions 状态需用户在浏览器看
- PowerShell 调 WSL 时 **避免复杂引号/`$(...)`**，改写脚本文件执行
- **ruflo 智能体平台**（2026-10-01 起用户要求用它辅助开发）：Windows npm 全局 3.49.0 + shim `C:\Users\QH\AppData\Roaming\npm\ruflo.cmd`（PATH 直接 `ruflo` 可用）；**WSL 内严禁跑 `/mnt/c` 下的 ruflo 包**（9p 跨文件系统加载卡死 0 输出），用 `npx -y ruflo@3.49.0`（缓存 WSL 原生）；交互命令需 PTY → `printf '<答案>' | script -qec "npx -y ruflo@3.49.0 init wizard" /dev/null`；LLM provider 全部未配置，key 由**用户自己** `ruflo providers configure -p openai -e <endpoint> -k <key> -m <model>` 填（不进对话）

## 架构速查

### 安装根（V0.2.4）
```
CoreTexDB-V0.2.4/
  bin/     coretex（单二进制，argv[0] 分发：改名/硬链接即变 coretexd|backup|healthcheck）
  lib/     libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib（crate-type: rlib+cdylib+staticlib）
  include/ coretexdb.h（手写 FFI 头，13 函数；`tests/ffi_api.rs` 守护头↔源一致）
  config/  coretex.toml, logging.yaml, backup/security/metrics + dev|staging|prod
  share/   doc/, examples/
  scripts/ start/stop/install/upgrade/backup/...
  systemd/ logrotate/
  data/    coretex/{collections,indexes,metadata,store}, wal, backup, logs, temp, versions
```

### Cargo bins
单 `[[bin]] coretex`（`src/main.rs` 按 argv[0] 分发子命令：`server/backup/doctor/...`）  
默认 features: `tokio, serde, compression, metrics`（`full` 含 rocksdb/onnx 等，CI 用默认）

### 版本号来源
- 运行时：`env!("CARGO_PKG_VERSION")` → 读 `Cargo.toml`
- 安装默认路径：scripts/systemd 里的 `/opt/CoreTexDB-V0.2.2`
- Python SDK：`python/coretexdb/version.py` = `1.0.12`（独立线，勿混改）

## 关键文件

- 发布：`.github/workflows/release.yml`（`workflow_dispatch` + tag `v*`）
- 安装：`scripts/install.sh`
- 文档：`README.md` §1.1 §10、`RELEASE_NOTES.md`、`share/doc/INSTALL.md`

## 下次可能任务

- [ ] **推送 5 个本地提交**（`ee5b02e`→`8a6e6ee`）：等另一会话 9 文件 clippy WIP 收口 → 本地全量 `cargo test` 全绿（基线 491 + ffi 7 + rerank 3+3 + filter_index 4+6 = **514** 待确认）→ `export GIT_SSH_COMMAND='ssh -p 443 -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes'` 后 SSH443 push
- [ ] **B3 余项**：分页参数、错误码统一；CLI/REST `--rerank` 标志（`coretex_cli/mod.rs`、`coretex_api/rest/mod.rs` 全字段字面量构造处——属另一会话 WIP，错峰补）
- [ ] **C1-C5 系统能力**（全新）：复制、分片、Pub-Sub、快照、INFO——见 roadmap C 节
- [ ] **D1-D5 生产化**：观测/SIMD/测试/文档/CI 门禁（D5 clippy 归另一会话，~100 warning 进行中）
- [ ] B4 孤立模块：**暂缓**（用户拍板，C/D 后再定接线或删除）
- [ ] 确认 Actions 是否 green（tag `v0.2.4` 触发 release.yml；GitHub HTTPS 被墙，需用户看网页）
- [ ] 是否把分支改名 `release/v0.2.4`（现仍叫 `v0.2.1-base`）
- [ ] 遗留：`insert_vectors` 持 `data.write()` 跨 storage IO；事务 abort 无 undo

## Git 身份

```
user.name=qinhaowow
user.email=qinhaowo@126.com   # commit 时显式 -c 指定
```
