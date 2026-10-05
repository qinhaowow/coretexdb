# 故障恢复演练 / Recovery Runbook

本文是**动手操作手册**：每一步都给出命令、预期输出、以及"输出不同说明什么"。
设计原理见 [`architecture.md`](architecture.md)，测试证据见
`tests/fault_injection.rs` 与 `tests/differential.rs`。

> **前提认知**：断电时数据不会"停在原地"。`FileStorage` 与 WAL 都是追加日志，
> 恢复规则是**重放到最后一条完整记录**——断电点上正在写的那一条会丢失，
> 之前的全部保留。要在任何演练后确认这一点，跑第 5 节的核对。

---

## 1. 三层持久化，各自的承诺

| 层 | 格式 | 断了会怎样 | 谁负责恢复 |
|---|---|---|---|
| `FileStorage` | 长度前缀 + CRC32 记录，按段分文件 | **尾部半条记录丢弃**，从头损坏则该段停止恢复 | `FileStorage::recover` |
| WAL | `checksum\|json` 行，按段分文件 | **坏行跳过并计数**，其余行照常重放 | `RecoveryManager::recover_storage_entries` |
| manifest | `metadata/metadata.json` | 丢失/为空则 schema 从 WAL 的 `CreateCollection` 条目重建 | `CoreTexDB::init` |

关键性质：**manifest 丢了不会丢数据**——schema 存在于 WAL 条目里，恢复时按
日志重建，包括度量与索引类型。这是 `recover_from_wal` 先应用 schema 条目、再
折叠数据条目的原因（数据落库需要集合已存在且度量正确）。

---

## 2. 演练一：断电时正在批量写入

模拟：写入 1000 行时进程被杀。

```bash
cd /home/qh/CoreTexDB
cargo test --test fault_injection crash_between_wal_and_storage_is_recovered_from_the_log
```

**原理**：`insert_vectors` 的顺序是 WAL → storage → memory → index，
WAL 一次 fsync 落整批（见 D2）。所以断电只可能让 WAL 比 storage **多**，
恢复方向永远是"重放一遍已确认的写"。

**自查方法**（生产目录同样适用）：

```bash
# 1. 记录行数
./coretex vector count --data-dir /opt/CoreTexDB --collection c
# 2. 停服，模拟断电
# 3. 重启后再数一次
./coretex vector count --data-dir /opt/CoreTexDB --collection c
```

差异只可能是**断电时正在写的那一批**。若少了整整一批，说明 WAL 与 storage
的顺序被改坏了——这是严重回归，参考 `tests/fault_injection.rs` 第一个用例的
断言形态（`crash_between_wal_and_storage_is_recovered_from_the_log`）。

---

## 3. 演练二：存储段被截断

模拟：磁盘写满或强制断电导致段文件尾部不完整。

```bash
cargo test --test fault_injection truncated_storage_tail_is_discarded_at_every_offset
cargo test --test fault_injection storage_checksum_break_stops_recovery_there
```

**手工演练**（在副本上做，勿用生产库）：

```bash
STORE=/opt/CoreTexDB/data/coretex/store
cp -r "$STORE" /tmp/store-backup
# 砍掉最后一个段的尾部
truncate -s -64 "$STORE/store-000003.log"
./coretex server --data-dir /opt/CoreTexDB/data/coretex   # 应正常启动
```

**预期**：
- 启动**成功**，不报错——尾部半条记录被丢弃并打印
  `discarding N trailing bytes in …`
- 该段最后一条记录对应的行消失，之前的都在
- 段文件被自动截断到最后一个完整记录处

**若启动失败**：说明恢复遇到了段**中部**损坏而非尾部。CRC 校验通过但记录
长度错误的情况应表现为"停止恢复该段"，不是 panic；panic 说明 `read_record`
的边界处理被破坏。

---

## 4. 演练三：日志被截断 / WAL 段丢失

```bash
cargo test --test fault_injection corrupt_wal_line_is_skipped_and_neighbours_replay
cargo test --test fault_injection torn_wal_tail_does_not_hide_earlier_entries
```

**手工演练**：

```bash
WAL=/opt/CoreTexDB/data/wal
# 坏行必须被跳过，且它前后的行不受影响
sed -i '3s/"metadata":{}/"metadata":{"tampered":1}/' "$WAL/wal-000002.log"
./coretex doctor --data-dir /opt/CoreTexDB   # 看 corrupted_entries 是否增加
```

**关键区分**（两种情况的处理完全不同）：

| 现象 | 含义 | 处理 |
|---|---|---|
| `corrupted_entries` 增加，行数不变 | 单行损坏，**安全** | 无需处理，监控即可 |
| 行的 `sequence` 出现**空洞** | 段被删除或日志被重置 | **必须**做全量同步；见第 6 节 |

段丢失会让 `read_entries_since` 返回 `truncated = true`——这是复制与集群
协议的"请回退到全量"信号，不是错误。

---

## 5. 演练四：三条恢复路径对拍（最重要）

**这一节是整个演练的核心。** 同一个数据集分别用五条路径恢复，逐字段比对。
任何一条路径与参考状态不一致，都是 bug——而且这种 bug 靠单条路径自己的测试
永远发现不了（副本能忠实重放一个本身错了的主库，快照能把错误一起保存下来）。

```bash
cargo test --test differential every_restore_path_agrees
cargo test --test differential different_write_shapes_recover_identically
cargo test --test differential incremental_and_bulk_replication_converge
```

覆盖的五条路径：

1. 普通重启（manifest + storage + WAL 重放）
2. 快照恢复（`SnapshotArchive::restore_into`）
3. 复制同步（`ReplicaSync` 全量/增量）
4. 压实日志回放（`compact_wal` → 新库重放）
5. 纯 manifest + storage，**完全无 WAL**

四种度量（euclidean / cosine / dotproduct / manhattan）各跑一遍——因为
SIMD 内核与标量参考的浮点累加顺序不同，只有多度量对拍才能确认没有哪条路径
落到了不同的度量实现上。

**这一节历史上抓到过一个真实缺陷**：结果只按距离排序，等距向量的返回顺序
取决于 HashMap 的迭代序——重启后相同数据返回不同顺序，两个副本可能排名不一致。
现在 8 处排序都以 id 作 tie-break。

---

## 6. 恢复演练：生产环境的正确顺序

灾难恢复的顺序有讲究，**先恢复日志、再压实、最后快照**：

```
1. 停服（确保没有写入）
2. 备份当前目录（哪怕是坏的——它是唯一证据）
     cp -r /opt/CoreTexDB/data /data-recovery-$(date +%F)
3. 冷启动一次，让它自己恢复
     ./coretex server --data-dir /opt/CoreTexDB/data/coretex &
4. 核对行数（对照第 2 步备份里的预期）
5. 只有核对通过，才压实日志（压实会重写日志，出错就没有第二次机会）
     compact_wal → 新目录 → 切换 wal_dir
6. 立刻做一次快照存档
     SnapshotArchive::save_auto
7. 再核对一次（第 5、6 步都碰了持久层）
8. 才恢复正常服务
```

**为什么压实放在核对之后**：`compact_wal` 把日志折叠成"每 key 最终状态"并写到
**新目录**，它不碰活跃日志——这是有意的设计，但如果恢复本身就是错的，压实会把
错误固化进一份看起来整洁的日志里，再也分不清哪些数据是对的。

---

## 7. 快照损坏

```bash
cargo test --test snapshot damaged_snapshot_file_is_refused
```

快照容器是 `CTSNAP01 + 长度 + CRC32 + payload`，任何一项不符都会被拒绝并
指明失败项：

| 报错 | 含义 |
|---|---|
| `snapshot truncated: N bytes, header alone needs 16` | 文件被截断 |
| `not a snapshot file: magic "…"` | 不是快照（选错文件） |
| `snapshot length mismatch: header says N, file carries M` | 长度字段与实际不符 |
| `snapshot checksum mismatch: header …, payload …` | 校验和不符（位翻转/介质损坏） |

**恢复策略**：快照损坏**不影响数据库本身**——它只是一份备份。用上一份快照
（第 6 节第 6 步保证了你有上一份），或者从 WAL 重放。

---

## 8. 副本的恢复语义

副本（`ReplicaSync`）与主库的恢复语义不同，值得单独说明：

- 副本的本地 WAL 记录的是**重放结果**，不是主库的传输日志。所以副本的 WAL
  永远比主库短，恢复到的是"上次同步到的位置"，不是最新数据。
- **副本重启后必须再同步一次**，才会追平主库。`replica_state.json` 记着已应用
  的日志位置：位置可用则走增量，位置已被主库截断则自动回退全量。
- 副本的 `persist_manifest` 由 `ReplicaSync` 在每次 apply 后调用——否则 schema
  只在内存里，重启就没了。

```bash
cargo test --test replication replica_restart_recovers_from_its_own_wal
cargo test --test replication state_file_survives_replica_restarts
```

---

## 9. 每次演练后必须核对的三件事

无论演练哪种故障，结束前跑这三项：

```bash
# 1. 行数符合预期？
./coretex vector count --data-dir ... --collection c

# 2. 查询结果合理？（不只是行数对，内容也要对）
./coretex search --data-dir ... --collection c --vector '[0.1,0.2,0.3]' --k 5

# 3. 无待处理的损坏？
./coretex doctor --data-dir /opt/CoreTexDB
```

`doctor` 是最后一关：它会报告 WAL 的 `corrupted_entries`、索引文件校验和
是否匹配陈旧索引、以及 manifest 与实际集合是否一致。三项都干净，才算演练成功。

---

## 10. 相关测试索引

| 主题 | 测试 |
|---|---|
| 存储截断 / 校验和 / WAL 损坏 / manifest 丢失 / 重启幂等 | `tests/fault_injection.rs`（8 例） |
| 五条恢复路径对拍（四度量） | `tests/differential.rs`（3 例） |
| 快照恢复与损坏拒绝 | `tests/snapshot.rs`（5 例） |
| WAL 压实回放等价 | `tests/snapshot.rs::wal_compaction_preserves_state` |
| 副本恢复与续传 | `tests/replication.rs`（8 例） |
| SIMD 内核 vs 标量参考 | `src/coretex_simd.rs` 单测 |