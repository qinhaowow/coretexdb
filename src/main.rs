//! CoreTexDB CLI
//!
//! Command-line interface for CoreTexDB.
//! Provides commands for database operations, server management, and administration.

use coretexdb::run_cli;

// 注意：这里刻意 *不* 用 `#[tokio::main]`。
// `run_cli()` 是同步入口，内部自行创建 tokio Runtime 并 block_on；
// 若 main 本身也是异步（已处于运行时中），再 block_on 会直接 panic：
//   "Cannot start a runtime from within a runtime"
// 因此 main 保持同步，由 run_cli 独占运行时的创建。
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // CoreTexError 实现了 std::error::Error + Send + Sync，
    // 用 `?` 借 From 转换到 main 的 Box<dyn Error> 返回类型。
    run_cli()?;
    Ok(())
}
