# Security Policy

## Supported versions

本项目采用滚动发布，**最新 release 分支上的版本获得安全更新**。
更早版本请升级后再报告问题。

| 版本 | 支持状态 |
| --- | --- |
| `release/v0.2.1-base`（当前开发线） | ✅ 安全更新 |
| `0.2.3` 及之后的 tag | ✅ 安全更新 |
| `< 0.2.2` | ❌ 请升级 |

## Reporting a vulnerability

**请勿公开提交 issue、讨论或提交记录。** 按以下方式私下报告：

1. 发邮件至 **qinhaowo@126.com**，主题前缀 `[SECURITY] CoreTexDB`；
2. 或使用 GitHub 的
   [private vulnerability reporting](https://github.com/qinhaowow/coretexdb/security/advisories/new)；
3. Gitee 镜像：`https://gitee.com/HaoqinOW/coretexdb` 同样可提交私密报告。

### 请包含

- 受影响的版本 / commit SHA；
- 攻击前提（是否需要认证、是否可远程触发）；
- 最小复现步骤或 PoC；
- 预期影响（数据泄露、未授权写入、拒绝服务、加密弱点…）。

### 我们会做什么

| 阶段 | 目标时间 |
| --- | --- |
| 确认收到 | 3 个工作日内 |
| 初步评估 | 7 个工作日内 |
| 修复与发布 | 视严重程度，高危优先 |
| 公开披露 | 修复发布后与你协商时间 |

在修复发布前，我们不会公开漏洞细节。若你希望在 `CHANGELOG.md` 与
安全公告中署名，报告时请说明。

## Scope

特别欢迎报告以下方向的问题：

- **WAL / 恢复**：重放时的数据一致性、last-write-wins 之外的竞态；
- **索引持久化**：校验和绕过导致加载陈旧索引、静默漏检；
- **TTL / purge**：过期数据未真正删除（存储、内存、索引三者不一致）；
- **备份与恢复**：恢复失败回滚不完整、路径穿越；
- **认证与限流**：REST 认证绕过、B-C-D-D 加密密钥处理；
- **FFI / 绑定**：`staticlib`/`cdylib` 导出接口的内存安全。

## Non-security bugs

普通缺陷请走 [bug report 模板](.github/ISSUE_TEMPLATE/bug_report.yml)。
