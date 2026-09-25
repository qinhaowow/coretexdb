## 变更说明 / What does this PR do?

<!-- 用 1-3 句说明"为什么做这个改动"，而不是复述 diff。
     Describe the *why* in 1-3 sentences. -->

## 类型 / Type

- [ ] `feat` 新功能
- [ ] `fix` 缺陷修复
- [ ] `test` 测试
- [ ] `docs` 文档
- [ ] `refactor` / `perf` 重构或性能
- [ ] `chore` 构建、CI、依赖

## 关联 / Links

<!-- 修复的问题：Closes #123
     设计文档：见 docs/ -->

## 检查清单 / Checklist

- [ ] `cargo test` 本地全绿（**必须**）
- [ ] `cargo test --features full` 本地全绿（**必须**）
- [ ] 新功能/修复带测试（含回归测试）
- [ ] 公开接口写了文档注释，`cargo doc` 可生成
- [ ] 改动了 CLI/行为 → 已同步 `README.md` 与 `docs/`
- [ ] 新增代码已真正接线（非孤立模块）
- [ ] 提交信息遵循 Conventional Commits

## 测试输出 / Test output

```
<!-- 粘贴 cargo test 的 test result 行，例如：
test result: ok. 435 passed; 0 failed; ... -->
```
