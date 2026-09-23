# CoreTexDB V0.2.1 — 多模态向量数据库

> 开发者: qinhaowo@126.com
> 辅助工具: MiMo v2.5 大语言模型

## 快速开始

```bash
# 启动 REPL（首次运行自动创建 ~/.coretexdb/）
coretex repl

# 查看所有命令
coretex --help
```

## CLI 命令一览

### 向量操作
```bash
# 插入向量
coretex vector insert mydb --id doc1 --dim 128 --metadata '{"title":"测试"}'

# 查询向量
coretex vector get mydb --id doc1

# 更新向量
coretex vector update mydb --id doc1 --metadata '{"title":"更新后"}'

# 搜索向量
coretex vector search mydb --dim 128 -k 10

# 批量插入
coretex batch insert mydb --count 1000 --dim 128

# 查看向量数
coretex vector count mydb

# 导出/导入
coretex vector export mydb --format json --output data.json
coretex vector import mydb --file data.json
```

### SQL 模式
```bash
coretex sql "SELECT * FROM mydb WHERE id = 'doc1'"
coretex sql "SELECT COUNT(*) FROM mydb"
coretex sql "INSERT INTO mydb (id, title) VALUES ('doc1', '测试')"
coretex sql "UPDATE mydb SET title = '更新' WHERE id = 'doc1'"
coretex sql "DELETE FROM mydb WHERE id = 'doc1'"
```

### 集合管理
```bash
coretex collection list
coretex collection create mydb 128 cosine hnsw
coretex collection delete mydb
coretex collection rename mydb new_name
coretex collection info mydb
coretex collection stats mydb
```

### 备份恢复
```bash
# 备份
coretex backup --name mybackup --dest /path/to/backup

# 恢复
coretex restore --snapshot /path/to/backup

# 数据迁移
coretex migrate --source /path/to/source --target /path/to/target
```

### 存储与 WAL
```bash
coretex storage info
coretex wal status
coretex wal replay
coretex wal gc
```

### 用户管理
```bash
# 创建用户
coretex admin user create --username admin --password 123456 --role admin

# 列出用户
coretex admin user list

# 删除用户
coretex admin user delete --username user1

# 分配角色
coretex admin user grant --username user1 --permission reader
```

### Token 管理
```bash
# 创建 Token
coretex token create --username admin --password 123456

# 验证 Token
coretex token verify --token <token>

# 吊销 Token
coretex token revoke --token <token>
```

### 集群管理
```bash
# 查看集群状态
coretex cluster status

# 添加节点
coretex cluster add-node --node-id node2 --address 192.168.1.100:50051

# 移除节点
coretex cluster remove-node --node-id node2

# 重平衡分片
coretex cluster rebalance
```

### 监控与配置
```bash
# 查看指标
coretex admin metrics

# 查看配置
coretex admin config

# 设置配置
coretex admin config --set key value

# 诊断检查
coretex doctor

# 版本信息
coretex version
```

### 文件导出
```bash
# 查看存储文件内容（可读格式）
coretex dump store
coretex dump store -f /path/to/store-000000.log -n 100

# 查看 WAL 日志内容
coretex dump wal
coretex dump wal -n 20

# JSON 格式输出
coretex dump store -o json
coretex dump wal -o json
```

## REST API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /health | 健康检查 |
| POST | /api/auth/login | 用户登录 |
| POST | /api/auth/register | 用户注册 |
| GET | /api/collections | 列出所有集合 |
| POST | /api/collections | 创建集合 |
| GET | /api/collections/:name | 获取集合信息 |
| DELETE | /api/collections/:name | 删除集合 |
| GET | /api/collections/:name/stats | 集合统计 |
| POST | /api/collections/:name/vectors | 插入向量 |
| PUT | /api/collections/:name/vectors | 更新向量 |
| GET | /api/collections/:name/vectors | 列出向量 |
| POST | /api/collections/:name/vectors/upsert | 插入/更新向量 |
| DELETE | /api/collections/:name/vectors/clear | 清空集合 |
| PUT | /api/collections/:name/rename | 重命名集合 |
| POST | /api/collections/:name/search | 搜索向量 |
| POST | /api/collections/:name/batch-search | 批量搜索 |
| POST | /api/admin/backup | 创建备份 |
| POST | /api/admin/restore | 恢复备份 |
| GET | /api/admin/backup/list | 列出备份 |

## GraphQL API

- Query: collections, collection, vector, search, batchSearch
- Mutation: createCollection, deleteCollection, insertVectors, deleteVectors, updateMetadata, createuser, login, deleteUser, assignRole, revokeToken, renameCollection
- Subscription: dataChanges, allDataChanges

## 环境变量

```bash
export CORETEX_PORT=8383          # REST API 端口
export CORETEX_GRPC_PORT=50051    # gRPC 端口
export CORETEX_ENABLE_WAL=true    # 启用 WAL
export CORETEX_DATA_DIR=~/.coretexdb/data
```

## 特性

- 完整 CRUD: 集合创建/列表/删除/重命名, 向量插入/查询/更新/删除
- 支持中英文混合向量和元数据
- L2/余弦/IP 距离计算
- HNSW/IVF 索引加速搜索
- WAL 预写日志（崩溃恢复 + 检查点）
- REST (Axum 0.7) + GraphQL (async-graphql) + gRPC (Tonic)
- SQL 查询引擎 (SELECT/INSERT/UPDATE/DELETE/JOIN/WHERE)
- JWT 认证 + RBAC 权限控制
- 备份恢复（快照 + 增量）
- 数据迁移工具
- 并发安全（RwLock + DashMap）
- 内置 REPL 交互模式
- 内置基准测试
- 内置诊断工具
- 支持元数据过滤（JSON 路径表达式）
- TTL 过期清理
- WebSocket 实时推送

## 开发信息

- 开发者: qinhaowo@126.com
- 辅助工具: MiMo v2.5 大语言模型
- 版本: V0.2.1
