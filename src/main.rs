//! CoreTexDB CLI
//!
//! Command-line interface for CoreTexDB.
//! Provides commands for database operations, server management, and administration.
//!
//! 单文件分发：本二进制根据**自身文件名**（argv[0]）自动分发，因此把
//! `coretex.exe` 改名（或建硬链接）为 `coretexd.exe` / `coretex-backup.exe` /
//! `coretex-healthcheck.exe` / `coretex-migrate.exe` / `coretex-cli.exe`
//! 即等价于对应程序，无需真的存在第二个可执行文件。

use coretexdb::run_cli_with_args;
use std::ffi::OsString;

// 注意：这里刻意 *不* 用 `#[tokio::main]`。
// `run_cli_with_args()` 是同步入口，内部自行创建 tokio Runtime 并 block_on；
// 若 main 本身也是异步（已处于运行时中），再 block_on 会直接 panic：
//   "Cannot start a runtime from within a runtime"
// 因此 main 保持同步，由它独占运行时的创建。
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let argv: Vec<OsString> = std::env::args_os().collect();

    // basename without extension, e.g. ".../bin/coretexd.exe" -> "coretexd"
    let exe = argv
        .first()
        .map(|a| {
            std::path::Path::new(a)
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_lowercase()
        })
        .unwrap_or_default();

    // 每个壳程序原本注入的子命令，在这里按文件名重现（与 src/bin/* 壳一致）。
    let mut args: Vec<OsString> = vec!["coretex".into()];
    match exe.as_str() {
        "coretexd" => {
            args.push("server".into());
            args.extend(argv[1..].iter().cloned());
        }
        "coretex-backup" => {
            // 壳逻辑：未显式给出 backup/restore 子命令时默认注入 backup
            let rest = &argv[1..];
            let has_sub = rest
                .iter()
                .any(|a| a.to_string_lossy() == "backup" || a.to_string_lossy() == "restore");
            if !has_sub {
                args.push("backup".into());
            }
            args.extend(rest.iter().cloned());
        }
        "coretex-healthcheck" => {
            args.push("doctor".into());
            args.extend(argv[1..].iter().cloned());
        }
        "coretex-migrate" => {
            println!("coretex-migrate {}", env!("CARGO_PKG_VERSION"));
            println!("Migration entrypoint: use 'coretex dump/restore' for store/WAL moves.");
            println!("See share/doc/INSTALL.md for upgrade procedure.");
            return Ok(());
        }
        _ => {
            // "coretex" / "coretex-cli" / 其它自定义名：参数原样透传
            args.extend(argv[1..].iter().cloned());
        }
    }

    run_cli_with_args(args)?;
    Ok(())
}
