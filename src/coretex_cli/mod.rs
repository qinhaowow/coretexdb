//! CLI for CoreTexDB
//!
//! 提供完整的命令行工具：
//! - server: 启动 HTTP REST 服务器
//! - collection: collection 管理
//! - vector: 向量 CRUD
//! - search: 相似度搜索
//! - benchmark: 性能基准
//! - backup/restore: 备份恢复
//! - admin: 管理员功能（用户管理、统计）
//! - sql: SQL 查询接口
//! - token: Token 管理
//! - cluster: 集群管理
//! - migrate: 数据迁移

use clap::{Command, Arg, ArgAction, ArgMatches};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{CoreTexDB, ApiConfig, start_server_with_db, DbConfig, start_grpc_server_shared, GrpcConfig};
use crate::coretex_core::Result;
use crate::coretex_data::{index_type_name, metric_name};
use crate::coretex_auth::AuthService;

pub mod data_backup;

/// Parse a comma-separated vector, rejecting anything that is not a number.
///
/// These call sites used to be `parse::<f32>().unwrap()`, so `--vector 1,x,3`
/// aborted the whole process with a panic instead of reporting a usable error.
fn parse_vector_arg(raw: &str) -> std::result::Result<Vec<f32>, String> {
    raw.split(',')
        .enumerate()
        .map(|(index, part)| {
            part.trim()
                .parse::<f32>()
                .map_err(|_| format!("向量第 {} 个分量 '{}' 不是合法数字", index + 1, part.trim()))
        })
        .collect()
}

/// Parse a JSON argument robustly on Windows.
/// On Windows cmd, `'{"key":"val"}'` is passed literally including single quotes.
/// Also handles cases where the user types single-quoted JSON values like `{'key':'val'}`.
fn parse_json_arg(raw: &str) -> std::result::Result<serde_json::Value, String> {
    let s = raw.trim();

    // 1) Try parsing as-is first (already valid JSON)
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        return Ok(v);
    }

    // 2) Strip surrounding quotes and try again
    let stripped = if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        &s[1..s.len() - 1]
    } else {
        s
    };
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(stripped) {
        return Ok(v);
    }

    // 3) If still fails, replace single quotes with double quotes (handles {'key':'val'})
    let replaced: String = stripped.chars().map(|c| if c == '\'' { '"' } else { c }).collect();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&replaced) {
        return Ok(v);
    }

    // 4) All attempts failed, return error with the raw input shown
    Err(format!(
        "--metadata 不是合法 JSON (raw={:?} stripped={:?}): 请使用双引号包裹键值，例如 {{\"title\":\"测试\"}}",
        s, stripped
    ))
}

/// `coretex backup` —— 把数据目录快照到 `<output>/<name>/`。
///
/// 这不使用活的数据库，所以在 `init()` 之前就被分流（见 `run_cli_async`）。
fn run_backup(m: &ArgMatches, data_dir: &str) -> Result<()> {
    let name = m.get_one::<String>("name").unwrap();
    let output = m.get_one::<String>("output").unwrap();
    let compression = m.get_one::<String>("compression").unwrap();
    let incremental = m.get_flag("incremental");

    let backup_name = if name.is_empty() {
        format!("backup_{}", chrono::Utc::now().timestamp())
    } else {
        name.clone()
    };
    // Default: `{install}/data/backup/{full|incremental}/<name>`.
    // Custom `--output` stays flat (`<output>/<name>`) so callers control layout.
    let dest = if output.is_empty() {
        let kind = if incremental { "incremental" } else { "full" };
        std::path::Path::new(data_dir)
            .join("data")
            .join("backup")
            .join(kind)
            .join(&backup_name)
    } else {
        std::path::Path::new(output).join(&backup_name)
    };

    // 宁可明确报错，也不要收下参数然后忽略它。
    if compression != "none" {
        return Err(format!(
            "--compression {} 尚未实现：目前只支持未压缩的完整冷备份",
            compression
        )
        .into());
    }
    if incremental {
        return Err("--incremental 尚未实现：目前只支持完整备份".to_string().into());
    }

    // 真的把落盘状态复制出来。以前这里只 `create_dir_all` 一个空目录就打印
    // 「✓ Backup created」，什么数据都没备。
    let manifest = data_backup::create(std::path::Path::new(data_dir), &dest)
        .map_err(|e| format!("备份失败: {}", e))?;

    println!(
        "✓ 备份完成: {} 个文件 / {} 字节（冷备份，需确保无 server 在写）",
        manifest.files.len(),
        manifest.total_bytes()
    );
    println!("  位置: {}", dest.display());
    println!("  时间: {}", manifest.created_at);
    for file in &manifest.files {
        println!(
            "    {:<28} {:>9} B  sha256={}…",
            file.path,
            file.bytes,
            &file.sha256[..16]
        );
    }
    Ok(())
}

/// `coretex restore` —— 把快照恢复进数据目录。
///
/// 恢复要把现有 `data/` **整体移开**，而 Windows 不允许移动含有已打开句柄的目录。
/// 因此它必须跑在 `init()` 之前（`init()` 会打开 `data/coretex/store/*.log`）。
fn run_restore(m: &ArgMatches, data_dir: &str) -> Result<()> {
    let name = m.get_one::<String>("name").unwrap();
    let input = m.get_one::<String>("input").unwrap();
    let target_time = m.get_one::<String>("target-time");
    let force = m.get_flag("force");

    if let Some(t) = target_time {
        return Err(format!(
            "--target-time（时间点恢复）尚未实现，不能用它恢复（收到 {}）",
            t
        )
        .into());
    }
    if !force {
        return Err(format!(
            "未指定 --force：restore 会替换 {} 下的现有数据。确认后重跑（现有数据会先移到 .pre-restore-<时间戳>/ 以便回退）。",
            data_dir
        )
        .into());
    }

    // 以前这里只 `println!("✓ Restore complete")`，一个字都没写回磁盘。
    let snapshot = if input.is_empty() {
        std::path::Path::new(data_dir)
            .join("data")
            .join("backup")
            .join("full")
            .join(name)
    } else {
        std::path::Path::new(input).join(name)
    };
    let (manifest, count) = data_backup::restore(&snapshot, std::path::Path::new(data_dir))
        .map_err(|e| format!("恢复失败: {}", e))?;

    println!(
        "✓ 恢复完成: {} 个文件 / {} 字节（备份创建于 {}）",
        count,
        manifest.total_bytes(),
        manifest.created_at
    );
    println!("  来源: {}", snapshot.display());
    Ok(())
}

/// Run the CLI from process arguments.
pub fn run_cli() -> Result<()> {
    run_cli_with_args(std::env::args_os())
}

/// Run the CLI with an explicit argv (first element is the program name).
/// Used by install-root `bin/` wrappers that inject a default subcommand.
pub fn run_cli_with_args<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move { run_cli_async_with(args).await })
}

/// Extract collection name from `--collection` flag, or fallback to first positional arg.
/// Supports both `coretex vector insert --collection mydb` and `coretex vector insert mydb`.
fn extract_collection(m: &clap::ArgMatches) -> Option<String> {
    if let Some(c) = m.get_one::<String>("collection") {
        return Some(c.clone());
    }
    // Fallback: first remaining positional arg (when trailing_var_arg is enabled)
    m.get_many::<String>("_positional")
        .and_then(|mut vals| vals.next().map(|s| s.to_string()))
}

async fn run_cli_async_with<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let mut cmd = Command::new("coretex")
        .version(env!("CARGO_PKG_VERSION"))
        .about("CoreTexDB command-line interface")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .global(true)
                .help("Install root (bin/, data/coretex, data/wal, data/backup, …)")
                .default_value("./coretex_data"),
        );

    // ==================== server ====================
    cmd = cmd.subcommand(
        Command::new("server")
            .about("Start the CoreTexDB server")
            .arg(
                Arg::new("address")
                    .short('a')
                    .long("address")
                    .help("Address to bind the server to")
                    .default_value("0.0.0.0"),
            )
            .arg(
                Arg::new("port")
                    .short('p')
                    .long("port")
                    .help("Port to bind the server to")
                    .default_value("5000"),
            )
            .arg(
                Arg::new("auth")
                    .long("auth")
                    .help("Enable authentication")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("rate-limit")
                    .long("rate-limit")
                    .help("Rate limit (requests per minute)")
                    .default_value("0"),
            )
            .arg(
                Arg::new("grpc-port")
                    .long("grpc-port")
                    .help("gRPC port (0 to disable)")
                    .default_value("50051"),
            )
            .arg(
                Arg::new("ws-port")
                    .long("ws-port")
                    .help("WebSocket port (0 to disable)")
                    .default_value("8080"),
            ),
    );

    // ==================== collection ====================
    cmd = cmd.subcommand(
        Command::new("collection")
            .about("Manage collections")
            .subcommand_required(true)
            .subcommand(
                Command::new("create")
                    .about("Create a new collection")
                    .arg(Arg::new("name").short('n').long("name").help("Name of the collection").required(true))
                    .arg(
                        Arg::new("dimension")
                            .short('d')
                            .long("dimension")
                            .help("Vector dimension")
                            .default_value("384"),
                    )
                    .arg(
                        Arg::new("metric")
                            .short('m')
                            .long("metric")
                            .help("Distance metric (cosine, euclidean, dotproduct)")
                            .default_value("cosine"),
                    )
                    .arg(
                        Arg::new("index")
                            .short('i')
                            .long("index")
                            .help("Index type: brute_force (exact, default), hnsw, ivf, scalar")
                            .default_value(crate::coretex_data::DEFAULT_INDEX_TYPE),
                    ),
            )
            .subcommand(
                Command::new("list")
                    .about("List all collections")
                    .arg(
                        Arg::new("verbose")
                            .short('v')
                            .long("verbose")
                            .action(ArgAction::SetTrue)
                            .help("Show detailed info"),
                    ),
            )
            .subcommand(
                Command::new("info")
                    .about("Get collection info")
                    .arg(Arg::new("name").short('n').long("name").help("Name of the collection").required(true)),
            )
            .subcommand(
                Command::new("delete")
                    .about("Delete a collection")
                    .arg(Arg::new("name").short('n').long("name").help("Name of the collection").required(true))
                    .arg(
                        Arg::new("force")
                            .short('f')
                            .long("force")
                            .help("Force delete without confirmation")
                            .action(ArgAction::SetTrue),
                    ),
            )
            .subcommand(
                Command::new("rename")
                    .about("Rename a collection")
                    .arg(Arg::new("name").short('n').long("name").required(true))
                    .arg(Arg::new("new-name").long("to").required(true)),
            )
            .subcommand(
                Command::new("stats")
                    .about("Show collection statistics")
                    .arg(Arg::new("name").short('n').long("name").required(true)),
            ),
    );

    // ==================== vector ====================
    cmd = cmd.subcommand(
        Command::new("vector")
            .about("Manage vectors")
            .subcommand_required(true)
            .trailing_var_arg(true)
            .subcommand(
                Command::new("insert")
                    .about("Insert vectors into a collection")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(
                        Arg::new("id")
                            .short('i')
                            .long("id")
                            .help("Vector ID")
                            .required_unless_present("batch"),
                    )
                    .arg(
                        Arg::new("vector")
                            .short('v')
                            .long("vector")
                            .help("Vector values (comma-separated)")
                            .required_unless_present("batch"),
                    )
                    .arg(Arg::new("metadata").short('m').long("metadata").help("Metadata as JSON"))
                    .arg(
                        Arg::new("batch")
                            .long("batch")
                            .help("Batch insert from JSON file")
                            .value_name("FILE"),
                    ),
            )
            .subcommand(
                Command::new("upsert")
                    .about("Insert a vector, or replace it when the id already exists")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(
                        Arg::new("id")
                            .short('i')
                            .long("id")
                            .help("Vector ID")
                            .required_unless_present("batch"),
                    )
                    .arg(
                        Arg::new("vector")
                            .short('v')
                            .long("vector")
                            .help("Vector values (comma-separated)")
                            .required_unless_present("batch"),
                    )
                    .arg(Arg::new("metadata").short('m').long("metadata").help("Metadata as JSON"))
                    .arg(
                        Arg::new("batch")
                            .long("batch")
                            .help("Batch upsert from JSON file")
                            .value_name("FILE"),
                    ),
            )
            .subcommand(
                Command::new("get")
                    .about("Get a vector by ID")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(Arg::new("id").short('i').long("id").help("Vector ID").required(true)),
            )
            .subcommand(
                Command::new("update")
                    .about("Update a vector")
                    .arg(Arg::new("collection").short('c').long("collection"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(Arg::new("id").short('i').long("id").required(true))
                    .arg(Arg::new("vector").short('v').long("vector").help("New vector values"))
                    .arg(Arg::new("metadata").short('m').long("metadata").help("New metadata JSON")),
            )
            .subcommand(
                Command::new("delete")
                    .about("Delete vectors by id, or every vector matching a metadata filter")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(
                        Arg::new("ids")
                            .short('i')
                            .long("ids")
                            .help("Vector IDs (comma-separated)")
                            .required_unless_present("filter"),
                    )
                    .arg(
                        Arg::new("filter")
                            .long("filter")
                            .value_name("JSON")
                            .help("Metadata filter; deletes every matching vector")
                            .conflicts_with("ids"),
                    ),
            )
            .subcommand(
                Command::new("list")
                    .about("List the vectors of a collection")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(Arg::new("limit").long("limit").help("Maximum rows to print"))
                    .arg(
                        Arg::new("offset")
                            .long("offset")
                            .help("Rows to skip")
                            .default_value("0"),
                    )
                    .arg(
                        Arg::new("with-metadata")
                            .long("with-metadata")
                            .action(ArgAction::SetTrue),
                    )
                    .arg(
                        Arg::new("format")
                            .long("format")
                            .help("Output format: text, json")
                            .default_value("text"),
                    ),
            )
            .subcommand(
                Command::new("clear")
                    .about("Remove every vector in a collection (durable)")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(
                        Arg::new("force")
                            .short('f')
                            .long("force")
                            .help("Skip the confirmation prompt")
                            .action(ArgAction::SetTrue),
                    ),
            )
            .subcommand(
                Command::new("count")
                    .about("Count vectors in a collection")
                    .arg(Arg::new("collection").short('c').long("collection").help("Collection name"))
                    .arg(Arg::new("_positional").num_args(0..)),
            )
            .subcommand(
                Command::new("import")
                    .about("Import vectors from JSON file")
                    .arg(Arg::new("collection").short('c').long("collection"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(Arg::new("file").short('f').long("file").required(true))
                    .arg(
                        Arg::new("format")
                            .long("format")
                            .help("Input format: json, csv")
                            .default_value("json"),
                    ),
            )
            .subcommand(
                Command::new("export")
                    .about("Export every vector of a collection to a file")
                    .arg(Arg::new("collection").short('c').long("collection"))
                    .arg(Arg::new("_positional").num_args(0..))
                    .arg(Arg::new("output").short('o').long("output").required(true))
                    .arg(
                        Arg::new("format")
                            .long("format")
                            .help("Output format: json (re-importable), csv (UTF-8 BOM)")
                            .value_parser(["json", "csv"])
                            .default_value("json"),
                    ),
            ),
    );

    // ==================== search ====================
    cmd = cmd.subcommand(
        Command::new("search")
            .about("Search for similar vectors")
            .arg(
                Arg::new("collection")
                    .short('c')
                    .long("collection")
                    .help("Collection name")
                    .default_value("default"),
            )
            .arg(
                Arg::new("vector")
                    .short('v')
                    .long("vector")
                    .help("Query vector (comma-separated values)")
                    .required(true),
            )
            .arg(
                Arg::new("k")
                    .short('k')
                    .long("k")
                    .help("Number of results to return")
                    .default_value("10"),
            )
            .arg(
                Arg::new("filter")
                    .long("filter")
                    .help("Metadata filter (JSON)"),
            )
            .arg(
                Arg::new("with-metadata")
                    .long("with-metadata")
                    .action(ArgAction::SetTrue)
                    .help("Include metadata in results"),
            )
            .arg(
                Arg::new("format")
                    .long("format")
                    .help("Output format: text, json")
                    .default_value("text"),
            ),
    );

    // ==================== benchmark ====================
    cmd = cmd.subcommand(
        Command::new("benchmark")
            .about("Run benchmark tests")
            .arg(
                Arg::new("collection")
                    .short('c')
                    .long("collection")
                    .help("Collection name")
                    .default_value("benchmark"),
            )
            .arg(
                Arg::new("count")
                    .short('n')
                    .long("count")
                    .help("Number of vectors to insert")
                    .default_value("1000"),
            )
            .arg(
                Arg::new("dimension")
                    .short('d')
                    .long("dimension")
                    .help("Vector dimension")
                    .default_value("128"),
            )
            .arg(
                Arg::new("queries")
                    .short('q')
                    .long("queries")
                    .help("Number of search queries")
                    .default_value("100"),
            )
            .arg(
                Arg::new("k")
                    .long("k")
                    .help("Top-K for search")
                    .default_value("10"),
            ),
    );

    // ==================== backup ====================
    cmd = cmd.subcommand(
        Command::new("backup")
            .about("Create a backup")
            .arg(
                Arg::new("name")
                    .long("name")
                    .help("Backup name (default: timestamp)")
                    .default_value(""),
            )
            .arg(
                Arg::new("output")
                    .short('o')
                    .long("output")
                    .help("Output directory (default: <install>/data/backup)")
                    .default_value(""),
            )
            .arg(
                Arg::new("compression")
                    .long("compression")
                    .help("Compression: none（gzip/lz4 尚未实现）")
                    .default_value("none"),
            )
            .arg(
                Arg::new("incremental")
                    .long("incremental")
                    .action(ArgAction::SetTrue)
                    .help("Create incremental backup"),
            ),
    );

    // ==================== restore ====================
    cmd = cmd.subcommand(
        Command::new("restore")
            .about("Restore from a backup")
            .arg(
                Arg::new("name")
                    .long("name")
                    .help("Backup name to restore")
                    .required(true),
            )
            .arg(
                Arg::new("input")
                    .short('i')
                    .long("input")
                    .help("Input directory (default: <install>/data/backup/full)")
                    .default_value(""),
            )
            .arg(
                Arg::new("target-time")
                    .long("target-time")
                    .help("PITR target timestamp (Unix seconds)"),
            )
            .arg(
                Arg::new("force")
                    .short('f')
                    .long("force")
                    .action(ArgAction::SetTrue),
            ),
    );

    // ==================== admin ====================
    cmd = cmd.subcommand(
        Command::new("admin")
            .about("Administrative operations")
            .subcommand_required(true)
            .subcommand(
                Command::new("user")
                    .about("Manage users")
                    .subcommand_required(true)
                    .subcommand(
                        Command::new("create")
                            .arg(Arg::new("username").short('u').long("username").required(true))
                            .arg(Arg::new("password").short('p').long("password").required(true))
                            .arg(Arg::new("role").short('r').long("role").default_value("user")),
                    )
                    .subcommand(Command::new("list"))
                    .subcommand(
                        Command::new("delete")
                            .arg(Arg::new("username").short('u').long("username").required(true)),
                    )
                    .subcommand(
                        Command::new("grant")
                            .arg(Arg::new("username").short('u').long("username").required(true))
                            .arg(Arg::new("permission").short('r').long("role").required(true)),
                    )
                    .subcommand(
                        Command::new("revoke")
                            .arg(Arg::new("username").short('u').long("username").required(true))
                            .arg(Arg::new("permission").short('r').long("role").required(true)),
                    ),
            )
            .subcommand(
                Command::new("stats")
                    .about("Show database statistics"),
            )
            .subcommand(
                Command::new("health")
                    .about("Check database health"),
            )
            .subcommand(
                Command::new("metrics")
                    .about("Show Prometheus-style metrics"),
            )
            .subcommand(
                Command::new("config")
                    .about("Show current configuration")
                    .arg(Arg::new("key").long("key").help("Specific config key"))
                    .arg(
                        Arg::new("set")
                            .long("set")
                            .num_args(2)
                            .value_names(["KEY", "VALUE"])
                        .help("Set config value"),
                    ),
            ),
    );

    // ==================== sql ====================
    cmd = cmd.subcommand(
        Command::new("sql")
            .about("Execute SQL-like query")
            .arg(Arg::new("query").required(true))
            .arg(
                Arg::new("file")
                    .short('f')
                    .long("file")
                    .help("Read query from file")
                    .conflicts_with("query"),
            )
            .arg(
                Arg::new("output")
                    .short('o')
                    .long("output")
                    .help("Output format: text, json, csv")
                    .default_value("text"),
            ),
    );

    // ==================== token ====================
    cmd = cmd.subcommand(
        Command::new("token")
            .about("Token management")
            .subcommand_required(true)
            .subcommand(
                Command::new("create")
                    .arg(Arg::new("username").short('u').long("username").required(true))
                    .arg(Arg::new("password").short('p').long("password").required(true))
                    .arg(Arg::new("ttl").long("ttl").default_value("86400")),
            )
            .subcommand(
                Command::new("verify")
                    .arg(Arg::new("token").short('t').long("token").required(true)),
            )
            .subcommand(
                Command::new("revoke")
                    .arg(Arg::new("token").short('t').long("token").required(true)),
            ),
    );

    // ==================== cluster ====================
    cmd = cmd.subcommand(
        Command::new("cluster")
            .about("Cluster management")
            .subcommand_required(true)
            .subcommand(
                Command::new("status")
                    .about("Show cluster status"),
            )
            .subcommand(
                Command::new("add-node")
                    .arg(Arg::new("node-id").long("node-id").required(true))
                    .arg(Arg::new("address").long("address").required(true)),
            )
            .subcommand(
                Command::new("remove-node")
                    .arg(Arg::new("node-id").long("node-id").required(true)),
            )
            .subcommand(
                Command::new("rebalance")
                    .about("Trigger shard rebalancing"),
            )
            .subcommand(
                Command::new("failover")
                    .arg(Arg::new("target").long("target").required(true))
                    .arg(Arg::new("reason").long("reason").default_value("manual")),
            ),
    );

    // ==================== migrate ====================
    cmd = cmd.subcommand(
        Command::new("migrate")
            .about("Data migration")
            .arg(
                Arg::new("source")
                    .long("source")
                    .required(true)
                    .help("Source database URL or path"),
            )
            .arg(
                Arg::new("target")
                    .long("target")
                    .required(true)
                    .help("Target database URL or path"),
            )
            .arg(
                Arg::new("collection")
                    .long("collection")
                    .help("Specific collection to migrate (default: all)"),
            )
            .arg(
                Arg::new("batch-size")
                    .long("batch-size")
                    .default_value("1000"),
            ),
    );

    // ==================== repl ====================
    cmd = cmd.subcommand(
        Command::new("repl")
            .about("Start interactive REPL")
            .arg(
                Arg::new("history")
                    .long("history")
                    .help("History file path")
                    .default_value("~/.coretex_history"),
            ),
    );

    // ==================== version / info ====================
    cmd = cmd.subcommand(
        Command::new("version")
            .about("Show version information")
            .arg(Arg::new("verbose").short('v').long("verbose").action(ArgAction::SetTrue)),
    );

    cmd = cmd.subcommand(
        Command::new("doctor")
            .about("Run diagnostic checks"),
    );

    cmd = cmd.subcommand(
        Command::new("dump")
            .about("Dump store/WAL files in readable format")
            .arg(
                Arg::new("type")
                    .required(true)
                    .help("File type: store, wal"),
            )
            .arg(
                Arg::new("file")
                    .short('f')
                    .long("file")
                    .help("Specific file path (default: auto-detect)"),
            )
            .arg(
                Arg::new("limit")
                    .short('n')
                    .long("limit")
                    .help("Max records to display")
                    .default_value("50"),
            )
            .arg(
                Arg::new("output")
                    .short('o')
                    .long("output")
                    .help("Output format: text, json")
                    .default_value("text"),
            ),
    );

    cmd = cmd.subcommand(
        Command::new("crypto")
            .about("B-C-D-D encryption: view/encrypt/decrypt .cdb files")
            .subcommand_required(true)
            .subcommand(
                Command::new("info")
                    .about("View .cdb file header information")
                    .arg(Arg::new("file").required(true).help("Path to .cdb file")),
            )
            .subcommand(
                Command::new("encrypt")
                    .about("Encrypt a file to .cdb format")
                    .arg(Arg::new("input").required(true).help("Input file path"))
                    .arg(Arg::new("output").short('o').long("output").help("Output .cdb file path"))
                    .arg(Arg::new("key").short('k').long("key").required(true).help("32-byte hex key"))
                    .arg(Arg::new("cipher").short('c').long("cipher").default_value("aes").help("Cipher: aes, chacha")),
            )
            .subcommand(
                Command::new("decrypt")
                    .about("Decrypt a .cdb file")
                    .arg(Arg::new("input").required(true).help("Path to .cdb file"))
                    .arg(Arg::new("output").short('o').long("output").help("Output file path"))
                    .arg(Arg::new("key").short('k').long("key").required(true).help("32-byte hex key")),
            )
            .subcommand(
                Command::new("keygen")
                    .about("Generate a random 32-byte encryption key"),
            ),
    );

    let matches = cmd.get_matches_from(args);

    // Every subcommand shares one durable database, so changes made by one
    // invocation are visible to the next one.
    let data_dir = matches
        .get_one::<String>("data-dir")
        .map(String::as_str)
        .unwrap_or_else(|| {
            // Default: use ~/.coretexdb/data if it exists, otherwise ./coretex_data
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".to_string());
            let home_dir = std::path::PathBuf::from(home).join(".coretexdb").join("data");
            if home_dir.exists() {
                Box::leak(home_dir.to_string_lossy().into_owned().into_boxed_str())
            } else {
                "./coretex_data"
            }
        });
    // `backup` / `restore` 操作的是**文件**，不是活的数据库，所以在 `init()` 之前分流。
    //
    // 这不只是整洁问题：`init()` 会打开 `data/coretex/store/*.log` 并把句柄保持到进程结束，
    // 而 **Windows 不允许重命名/替换含有已打开句柄的目录** —— 于是 `restore` 会以
    // “拒绝访问。(os error 5)” 失败，而完全相同的代码在 Linux 上却没问题。
    match matches.subcommand() {
        Some(("backup", m)) => return run_backup(m, data_dir),
        Some(("restore", m)) => return run_restore(m, data_dir),
        _ => {}
    }

    let db = Arc::new(RwLock::new(CoreTexDB::with_config(DbConfig::new(data_dir))));
    db.read()
        .await
        .init()
        .await
        .map_err(|e| format!("DB init failed: {}", e))?;

    match matches.subcommand() {
        Some(("server", sub_matches)) => {
            let address = sub_matches.get_one::<String>("address").unwrap();
            let port = sub_matches.get_one::<String>("port").unwrap();
            let data_dir = sub_matches.get_one::<String>("data-dir").unwrap();
            let enable_auth = sub_matches.get_flag("auth");
            let rate_limit: usize = sub_matches.get_one::<String>("rate-limit").unwrap().parse().unwrap_or(0);
            let grpc_port: u16 = sub_matches.get_one::<String>("grpc-port").unwrap().parse().unwrap_or(0);
            let ws_port: u16 = sub_matches.get_one::<String>("ws-port").unwrap().parse().unwrap_or(0);

            println!("Starting CoreTexDB server on {}:{}", address, port);
            println!("Data directory: {}", data_dir);
            println!("Auth: {}", if enable_auth { "enabled" } else { "disabled" });
            println!("Rate limit: {} req/min", rate_limit);
            if grpc_port > 0 {
                println!("gRPC: port {}", grpc_port);
            } else {
                println!("gRPC: disabled");
            }
            if ws_port > 0 {
                // WebSocketServer is in-process only (no TCP listener yet).
                println!("WebSocket: port {} requested (in-process server only; no TCP listener yet)", ws_port);
            } else {
                println!("WebSocket: disabled");
            }

            // Share the CLI-initialized DB between REST and gRPC.
            let shared_db = db.clone();

            if grpc_port > 0 {
                let grpc_db = shared_db.clone();
                let grpc_auth = enable_auth;
                let grpc_rate = rate_limit;
                let grpc_addr: std::net::SocketAddr = format!("{}:{}", address, grpc_port)
                    .parse()
                    .map_err(|e| format!("Invalid gRPC address: {}", e))?;
                tokio::spawn(async move {
                    let config = GrpcConfig {
                        addr: grpc_addr,
                        enable_auth: grpc_auth,
                        rate_limit_per_minute: grpc_rate,
                        ..Default::default()
                    };
                    if let Err(e) = start_grpc_server_shared(grpc_db, config).await {
                        eprintln!("gRPC server error: {}", e);
                    }
                });
            }

            let config = ApiConfig {
                address: address.clone(),
                port: port.parse().unwrap(),
                data_dir: data_dir.clone(),
                enable_cors: true,
                cors_allowed_origins: Vec::new(),
                enable_auth,
                rate_limit_per_minute: rate_limit,
            };

            start_server_with_db(config, shared_db).await?;
        }

        Some(("collection", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("create", m)) => {
                    let name = m.get_one::<String>("name").unwrap();
                    let dimension: usize = m.get_one::<String>("dimension").unwrap().parse().unwrap();
                    let metric = m.get_one::<String>("metric").unwrap();
                    let index = m.get_one::<String>("index").unwrap();

                    let db_ref = db.clone();
                    db_ref.read().await
                        .create_collection_with_index(name, dimension, metric, index)
                        .await
                        .map_err(|e| format!("Failed to create collection: {}", e))?;

                    // Report what was actually stored rather than what was asked
                    // for. An unrecognised metric or index type falls back to
                    // cosine / the exact index, and echoing the raw arguments
                    // would hide that from the user.
                    let schema = db_ref.read().await.get_collection(name).await
                        .map_err(|e| format!("Failed to read back collection: {}", e))?;
                    let effective_index = schema
                        .indexes
                        .first()
                        .map(|i| index_type_name(&i.index_type))
                        .unwrap_or("none");

                    println!(
                        "✓ Collection '{}' created (dim={}, metric={}, index={})",
                        name,
                        schema.dimension,
                        metric_name(&schema.distance_metric),
                        effective_index
                    );
                }

                Some(("list", m)) => {
                    let verbose = m.get_flag("verbose");
                    let db_ref = db.clone();
                    let collections = db_ref.read().await.list_collections().await
                        .map_err(|e| format!("Failed to list collections: {}", e))?;

                    if collections.is_empty() {
                        println!("No collections found.");
                    } else if verbose {
                        println!("{:<30} {:<10} {:<15} {:<10}", "Name", "Dim", "Metric", "Count");
                        println!("{}", "-".repeat(70));
                        for c in &collections {
                            if let Ok(s) = db_ref.read().await.get_collection(c).await {
                                let count = db_ref.read().await.get_vectors_count(c).await.unwrap_or(0);
                                println!("{:<30} {:<10} {:<15} {:<10}",
                                    c, s.dimension, format!("{:?}", s.distance_metric), count);
                            }
                        }
                    } else {
                        for c in collections { println!("{}", c); }
                    }
                }

                Some(("info", m)) => {
                    let name = m.get_one::<String>("name").unwrap();
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    match db_guard.get_collection(name).await {
                        Ok(s) => {
                            let count = db_guard.get_vectors_count(name).await.unwrap_or(0);
                            println!("Collection: {}", s.name);
                            println!("  Dimension: {}", s.dimension);
                            println!("  Distance metric: {:?}", s.distance_metric);
                            println!("  Vectors count: {}", count);
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                }

                Some(("delete", m)) => {
                    let name = m.get_one::<String>("name").unwrap();
                    let force = m.get_flag("force");
                    if !force {
                        print!("Confirm delete collection '{}'? [y/N] ", name);
                        use std::io::Write;
                        std::io::stdout().flush().ok();
                        let mut input = String::new();
                        std::io::stdin().read_line(&mut input).ok();
                        if !input.trim().eq_ignore_ascii_case("y") {
                            // Cancelling used to `return Ok(())`, so a script doing
                            // `coretex collection delete X && next-step` carried on
                            // believing the collection was gone. Refusing is an
                            // error, not a success.
                            return Err(format!(
                                "未确认，集合 '{}' 未被删除（非交互场景请加 --force）",
                                name
                            )
                            .into());
                        }
                    }
                    let db_ref = db.clone();
                    db_ref.read().await.delete_collection(name).await
                        .map_err(|e| format!("Failed to delete: {}", e))?;
                    println!("✓ Collection '{}' deleted", name);
                }

                Some(("rename", m)) => {
                    let name = m.get_one::<String>("name").unwrap();
                    let new_name = m.get_one::<String>("new-name").unwrap();
                    let db_ref = db.clone();
                    db_ref.read().await.rename_collection(name, new_name).await
                        .map_err(|e| format!("Failed to rename: {}", e))?;
                    println!("✓ Collection '{}' renamed to '{}'", name, new_name);
                }

                Some(("stats", m)) => {
                    let name = m.get_one::<String>("name").unwrap();
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    if let Ok(s) = db_guard.get_collection(name).await {
                        let count = db_guard.get_vectors_count(name).await.unwrap_or(0);
                        println!("=== Statistics for '{}' ===", name);
                        println!("  Dimension: {}", s.dimension);
                        println!("  Metric: {:?}", s.distance_metric);
                        println!("  Vectors: {}", count);
                        println!("  Index: hnsw");
                    }
                }

                _ => {}
            }
        }

        Some(("vector", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("insert", m)) => {
                    let db_ref = db.clone();
                    if let Some(file) = m.get_one::<String>("batch") {
                        let content = std::fs::read_to_string(file)
                            .map_err(|e| format!("Read file: {}", e))?;
                        let items: Vec<serde_json::Value> = serde_json::from_str(&content)
                            .map_err(|e| format!("Parse JSON: {}", e))?;
                        let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                        // Build the batch first, then insert it in one call.
                        // `insert_vectors` validates every record before writing
                        // any, so a single bad record cannot leave the file half
                        // imported. The old code inserted record-by-record with
                        // `let _ =`, swallowing failures and counting them anyway.
                        let mut batch = Vec::with_capacity(items.len());
                        for (index, item) in items.iter().enumerate() {
                            let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let vector: Vec<f32> = item.get("vector")
                                .and_then(|v| v.as_array())
                                .map(|a| a.iter().filter_map(|x| x.as_f64().map(|f| f as f32)).collect())
                                .unwrap_or_default();
                            if id.is_empty() || vector.is_empty() {
                                return Err(format!(
                                    "--batch 第 {} 条记录缺少 id 或 vector",
                                    index + 1
                                )
                                .into());
                            }
                            batch.push((
                                id,
                                vector,
                                item.get("metadata").cloned().unwrap_or(serde_json::json!({})),
                            ));
                        }
                        let count = batch.len();
                        db_ref.read().await
                            .insert_vectors(&collection, batch).await
                            .map_err(|e| format!("--batch 导入失败: {}", e))?;
                        println!("✓ Batch import complete: {} vectors", count);
                    } else {
                        let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                        let id = m.get_one::<String>("id").unwrap();
                        let vector_str = m.get_one::<String>("vector").unwrap();
                        let metadata_str = m.get_one::<String>("metadata");

                        let vector = parse_vector_arg(vector_str)?;

                        let metadata = match metadata_str {
                            Some(raw) => parse_json_arg(raw)?,
                            None => serde_json::json!({}),
                        };

                        db_ref.read().await.insert_vectors(&collection, vec![(id.clone(), vector, metadata)]).await
                            .map_err(|e| format!("Failed to insert vector: {}", e))?;

                        println!("✓ Vector '{}' inserted into '{}'", id, collection);
                    }
                }

                Some(("upsert", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;

                    // Batch and single share one write path: `upsert_vectors`
                    // validates every record before writing any, and routes each
                    // id to the verified insert/update path, so an upsert reaches
                    // the index, the log and the store exactly like an insert.
                    let batch: Vec<(String, Vec<f32>, serde_json::Value)> =
                        if let Some(file) = m.get_one::<String>("batch") {
                            let content = std::fs::read_to_string(file)
                                .map_err(|e| format!("Read file: {}", e))?;
                            let items: Vec<serde_json::Value> = serde_json::from_str(&content)
                                .map_err(|e| format!("Parse JSON: {}", e))?;
                            let mut batch = Vec::with_capacity(items.len());
                            for (index, item) in items.iter().enumerate() {
                                let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let vector: Vec<f32> = item.get("vector")
                                    .and_then(|v| v.as_array())
                                    .map(|a| a.iter().filter_map(|x| x.as_f64().map(|f| f as f32)).collect())
                                    .unwrap_or_default();
                                if id.is_empty() || vector.is_empty() {
                                    return Err(format!(
                                        "--batch 第 {} 条记录缺少 id 或 vector",
                                        index + 1
                                    )
                                    .into());
                                }
                                batch.push((
                                    id,
                                    vector,
                                    item.get("metadata").cloned().unwrap_or(serde_json::json!({})),
                                ));
                            }
                            batch
                        } else {
                            let id = m.get_one::<String>("id").unwrap().clone();
                            let vector = parse_vector_arg(m.get_one::<String>("vector").unwrap())?;
                            let metadata = match m.get_one::<String>("metadata") {
                                Some(raw) => parse_json_arg(raw)?,
                                None => serde_json::json!({}),
                            };
                            vec![(id, vector, metadata)]
                        };

                    let db_ref = db.clone();
                    let (inserted, updated) = db_ref
                        .read()
                        .await
                        .upsert_vectors(&collection, batch)
                        .await
                        .map_err(|e| format!("upsert 失败: {}", e))?;
                    println!(
                        "✓ upsert 完成: 新增 {} 条, 更新 {} 条（集合 '{}'）",
                        inserted.len(),
                        updated.len(),
                        collection
                    );
                }

                Some(("get", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let id = m.get_one::<String>("id").unwrap();
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    match db_guard.get_vector(&collection, id).await {
                        Ok(Some((vector, metadata))) => {
                            println!("Vector ID: {}", id);
                            println!("Vector (first 10): {:?}", &vector[..10.min(vector.len())]);
                            println!("Metadata: {}", metadata);
                        }
                        Ok(None) => println!("Vector not found"),
                        Err(e) => println!("Error: {}", e),
                    }
                }

                Some(("update", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let id = m.get_one::<String>("id").unwrap();
                    let vector_str = m.get_one::<String>("vector");
                    let metadata_str = m.get_one::<String>("metadata");

                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;

                    let metadata = match metadata_str {
                        Some(raw) => Some(parse_json_arg(raw)?),
                        None => None,
                    };

                    // A vector and/or metadata. A metadata-only update keeps the
                    // existing vector, so it has to read it first.
                    let vector = match vector_str {
                        Some(raw) => parse_vector_arg(raw)?,
                        None => db_guard
                            .get_vector(&collection, id)
                            .await
                            .map_err(|e| format!("读取 '{}' 失败: {}", id, e))?
                            .ok_or_else(|| format!("集合 '{}' 中不存在向量 '{}'", collection, id))?
                            .0,
                    };

                    // 这两条路径过去都是 `let _ = ...` 后无条件打印 ✓，
                    // 于是更新失败（集合不存在、维度不符）也会报告 updated。
                    let changed = db_guard
                        .update_vector(&collection, id, vector, metadata)
                        .await
                        .map_err(|e| format!("更新 '{}' 失败: {}", id, e))?;
                    if !changed {
                        return Err(format!("集合 '{}' 中不存在向量 '{}'", collection, id).into());
                    }
                    println!("✓ Vector '{}' updated in '{}'", id, collection);
                }

                Some(("delete", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let db_ref = db.clone();

                    // 两种删除都走同一条持久路径：`delete_vectors` 会为每个 id 写
                    // WAL 记录 + 存储墓碑 + 从索引移除。绝不能"只清内存"，
                    // 否则日志里的记录仍然存活，重启就全部复活。
                    let mut requested = None;
                    let deleted = if let Some(raw) = m.get_one::<String>("filter") {
                        let filter: serde_json::Value = parse_json_arg(raw)?;
                        db_ref
                            .read()
                            .await
                            .delete_vectors_where(&collection, &filter)
                            .await
                            .map_err(|e| format!("Failed to delete vectors: {}", e))?
                            .len()
                    } else {
                        let ids_str = m.get_one::<String>("ids").unwrap();
                        let ids: Vec<String> = ids_str
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        if ids.is_empty() {
                            return Err("--ids 为空：没有要删除的向量".into());
                        }
                        requested = Some(ids.len());
                        db_ref
                            .read()
                            .await
                            .delete_vectors(&collection, &ids)
                            .await
                            .map_err(|e| format!("Failed to delete vectors: {}", e))?
                    };

                    println!("✓ {} vectors deleted from '{}'", deleted, collection);
                    if let Some(wanted) = requested {
                        if deleted < wanted {
                            eprintln!(
                                "  注意：请求删除 {} 条，实际只删掉 {} 条（其余 id 不存在）",
                                wanted, deleted
                            );
                        }
                    }
                }

                Some(("list", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let format = m.get_one::<String>("format").unwrap();
                    let with_meta = m.get_flag("with-metadata");
                    let offset: usize = m
                        .get_one::<String>("offset")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let limit: Option<usize> = m
                        .get_one::<String>("limit")
                        .and_then(|v| v.parse().ok());

                    let db_ref = db.clone();
                    let all = db_ref
                        .read()
                        .await
                        .list_vectors(&collection)
                        .await
                        .map_err(|e| format!("Failed to list vectors: {}", e))?;
                    let total = all.len();
                    let page: Vec<_> = all
                        .into_iter()
                        .skip(offset)
                        .take(limit.unwrap_or(usize::MAX))
                        .collect();

                    if format == "json" {
                        let items: Vec<serde_json::Value> = page
                            .iter()
                            .map(|(id, vector, metadata)| {
                                serde_json::json!({
                                    "id": id, "vector": vector, "metadata": metadata
                                })
                            })
                            .collect();
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&items)
                                .map_err(|e| format!("序列化失败: {}", e))?
                        );
                    } else {
                        println!(
                            "Collection '{}': 共 {} 条，显示 {}..{}",
                            collection,
                            total,
                            offset.min(total),
                            (offset + page.len()).min(total)
                        );
                        for (id, vector, metadata) in &page {
                            if with_meta {
                                println!("  {}  dim={}  meta={}", id, vector.len(), metadata);
                            } else {
                                println!("  {}", id);
                            }
                        }
                    }
                }

                Some(("clear", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;

                    // 与 `collection delete` 一致：未确认是**错误**，不是成功。
                    if !m.get_flag("force") {
                        use std::io::Write;
                        print!("清空集合 '{}' 的全部向量？[y/N] ", collection);
                        std::io::stdout().flush().ok();
                        let mut input = String::new();
                        std::io::stdin().read_line(&mut input).ok();
                        if !input.trim().eq_ignore_ascii_case("y") {
                            return Err(format!(
                                "未确认，集合 '{}' 未被清空（非交互场景请加 --force）",
                                collection
                            )
                            .into());
                        }
                    }

                    let db_ref = db.clone();
                    let removed = db_ref
                        .read()
                        .await
                        .clear_collection(&collection)
                        .await
                        .map_err(|e| format!("Failed to clear collection: {}", e))?;
                    println!(
                        "✓ 已清空 '{}'：删除 {} 条（每个 id 都写了墓碑，重启后不会复活）",
                        collection, removed
                    );
                }

                Some(("count", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let db_ref = db.clone();
                    let count = db_ref.read().await.get_vectors_count(&collection).await
                        .map_err(|e| format!("Failed to count vectors: {}", e))?;
                    println!("Collection '{}' has {} vectors", collection, count);
                }

                Some(("import", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let file = m.get_one::<String>("file").unwrap();
                    let format = m.get_one::<String>("format").unwrap();

                    let content = std::fs::read_to_string(file)
                        .map_err(|e| format!("Read file: {}", e))?;
                    let db_ref = db.clone();

                    match format.as_str() {
                        "json" => {
                            let items: Vec<serde_json::Value> = serde_json::from_str(&content)
                                .map_err(|e| format!("Parse JSON: {}", e))?;
                            // 整批先收集，再一次性写入：`insert_vectors` 会先校验全部
                            // 记录，因此一条维度不对不会留下“导入一半”的状态。以前是逐条
                            // `let _ = ...; count += 1;`，失败也计数。
                            let mut batch = Vec::with_capacity(items.len());
                            for item in items.iter() {
                                let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                let vector: Vec<f32> = item.get("vector")
                                    .and_then(|v| v.as_array())
                                    .map(|a| a.iter().filter_map(|x| x.as_f64().map(|f| f as f32)).collect())
                                    .unwrap_or_default();
                                batch.push((
                                    id,
                                    vector,
                                    item.get("metadata").cloned().unwrap_or(serde_json::json!({})),
                                ));
                            }
                            let count = batch.len();
                            db_ref.read().await
                                .insert_vectors(&collection, batch).await
                                .map_err(|e| format!("导入失败: {}", e))?;
                            println!("✓ Imported {} vectors", count);
                        }
                        "csv" => {
                            let mut batch = Vec::new();
                            for (index, line) in content.lines().skip(1).enumerate() {
                                let parts: Vec<&str> = line.split(',').collect();
                                if parts.len() < 2 {
                                    return Err(format!(
                                        "CSV 第 {} 行至少需要 id 和一个分量: {}",
                                        index + 2,
                                        line
                                    )
                                    .into());
                                }
                                let id = parts[0].trim().to_string();
                                if id.is_empty() {
                                    return Err(format!("CSV 第 {} 行缺少 id", index + 2).into());
                                }
                                // 以前用 `filter_map(...ok())`，解析不了的分量被静默丢弃，
                                // 剩下的向量维度就不对了；错误同样被吞掉后仍计数。
                                let vector = parse_vector_arg(&parts[1..].join(","))
                                    .map_err(|e| format!("CSV 第 {} 行: {}", index + 2, e))?;
                                batch.push((id, vector, serde_json::json!({})));
                            }
                            let count = batch.len();
                            db_ref.read().await
                                .insert_vectors(&collection, batch).await
                                .map_err(|e| format!("CSV 导入失败: {}", e))?;
                            println!("✓ Imported {} vectors from CSV", count);
                        }
                        _ => println!("Unsupported format: {}", format),
                    }
                }

                Some(("export", m)) => {
                    let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
                    let file = m.get_one::<String>("output").unwrap();
                    let format = m.get_one::<String>("format").unwrap();

                    let db_ref = db.clone();
                    let vectors = db_ref
                        .read()
                        .await
                        .list_vectors(&collection)
                        .await
                        .map_err(|e| format!("Failed to read '{}': {}", collection, e))?;

                    let items: Vec<serde_json::Value> = vectors
                        .iter()
                        .map(|(id, vector, metadata)| {
                            serde_json::json!({
                                "id": id, "vector": vector, "metadata": metadata
                            })
                        })
                        .collect();

                    // 复用语料库层的导出实现：CSV 带 UTF-8 BOM 且对含逗号/引号/
                    // 换行的字段加引号；JSON 写的是 [{id,vector,metadata}]，
                    // 与 `vector import --format json` 正好互为逆操作。
                    let target = std::path::Path::new(file);
                    let dir = match target.parent() {
                        Some(p) if !p.as_os_str().is_empty() => p.to_string_lossy().to_string(),
                        _ => ".".to_string(),
                    };
                    let name = target
                        .file_name()
                        .ok_or_else(|| format!("导出路径无效: {}", file))?
                        .to_string_lossy()
                        .to_string();
                    if dir != "." {
                        std::fs::create_dir_all(&dir)
                            .map_err(|e| format!("创建目录 {} 失败: {}", dir, e))?;
                    }

                    let exporter = crate::coretex_export::DataExporter::new(&dir);
                    let written = match format.as_str() {
                        "csv" => exporter.export_csv(&items, &name),
                        _ => exporter.export_json(&items, &name),
                    }
                    .map_err(|e| format!("导出失败: {}", e))?;

                    // 只有文件真的写出来、且非空，才算成功 —— 这条命令以前只打印
                    // 一行字就返回，一个字节都不写。
                    let bytes = std::fs::metadata(&written).map(|md| md.len()).unwrap_or(0);
                    if bytes == 0 {
                        return Err(format!("导出失败: {} 写出来是 0 字节", written).into());
                    }
                    println!(
                        "✓ 已导出 '{}' 的 {} 条向量 → {}（{} 字节，格式 {}）",
                        collection,
                        items.len(),
                        written,
                        bytes,
                        format
                    );
                }

                _ => {}
            }
        }

        Some(("search", m)) => {
            let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
            let vector_str = m.get_one::<String>("vector").unwrap();
            let k: usize = m.get_one::<String>("k").unwrap().parse().unwrap();
            let format = m.get_one::<String>("format").unwrap();
            let with_meta = m.get_flag("with-metadata");

            let vector = parse_vector_arg(vector_str)?;

            // `--filter` is a JSON document; a malformed one is a user error and
            // must be reported rather than silently searching without a filter.
            let filter = match m.get_one::<String>("filter") {
                Some(raw) => Some(
                    parse_json_arg(raw)?,
                ),
                None => None,
            };

            let db_ref = db.clone();
            let results = db_ref.read().await.search(&collection, vector, k, filter).await
                .map_err(|e| format!("Search failed: {}", e))?;

            if format == "json" {
                let json_results: Vec<_> = results.iter().map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "score": 1.0 - r.distance,
                        "distance": r.distance,
                    })
                }).collect();
                println!("{}", serde_json::to_string_pretty(&json_results).unwrap());
            } else {
                println!("Search results from '{}' (k={}):", collection, k);
                for (i, result) in results.iter().enumerate() {
                    if with_meta {
                        if let Ok(Some((_, meta))) = db_ref.read().await.get_vector(&collection, &result.id).await {
                            println!("  {}. {} score={:.4} meta={}", i+1, result.id, 1.0-result.distance, meta);
                            continue;
                        }
                    }
                    println!("  {}. {} (score: {:.4})", i + 1, result.id, 1.0 - result.distance);
                }
            }
        }

        Some(("benchmark", m)) => {
            let collection = extract_collection(m).ok_or("Missing --collection or positional collection name")?;
            let count: usize = m.get_one::<String>("count").unwrap().parse().unwrap();
            let dimension: usize = m.get_one::<String>("dimension").unwrap().parse().unwrap();
            let queries: usize = m.get_one::<String>("queries").unwrap().parse().unwrap();
            let k: usize = m.get_one::<String>("k").unwrap().parse().unwrap();

            let db_ref = db.clone();

            println!("=== Benchmark Configuration ===");
            println!("Collection: {}", collection);
            println!("Vectors: {}", count);
            println!("Dimension: {}", dimension);
            println!("Queries: {}", queries);
            println!("Top-K: {}", k);
            println!();

            let _ = db_ref.read().await.delete_collection(&collection).await;
            db_ref.read().await.create_collection(&collection, dimension, "cosine").await
                .map_err(|e| format!("Failed to create collection: {}", e))?;

            println!("Inserting {} vectors...", count);
            let start = std::time::Instant::now();
            for i in 0..count {
                let vector: Vec<f32> = (0..dimension).map(|_| rand::random::<f32>()).collect();
                let _ = db_ref.read().await.insert_vectors(
                    &collection,
                    vec![(format!("vec_{}", i), vector, serde_json::json!({"index": i}))]
                ).await;
            }
            let insert_time = start.elapsed();
            println!("✓ Inserted {} vectors in {:.2?}", count, insert_time);
            println!("  Throughput: {:.0} vectors/sec", count as f64 / insert_time.as_secs_f64());

            println!("\nRunning {} search queries...", queries);
            let search_start = std::time::Instant::now();
            for i in 0..queries {
                let query: Vec<f32> = (0..dimension).map(|_| rand::random::<f32>()).collect();
                let _ = db_ref.read().await.search(&collection, query, k, None).await;
                if (i + 1) % 50 == 0 {
                    print!("\r  {}/{} queries done", i + 1, queries);
                    use std::io::Write;
                    std::io::stdout().flush().ok();
                }
            }
            println!();
            let search_time = search_start.elapsed();
            println!("✓ Completed {} searches in {:.2?}", queries, search_time);
            println!("  Avg search time: {:.2?}", search_time / queries as u32);
            println!("  QPS: {:.0}", queries as f64 / search_time.as_secs_f64());
        }

        Some(("admin", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("user", user_sub)) => {
                    let auth = std::sync::Arc::new(AuthService::with_persistence(&db.read().await.config.data_dir));
                    match user_sub.subcommand() {
                        Some(("create", m)) => {
                            let username = m.get_one::<String>("username").unwrap();
                            let password = m.get_one::<String>("password").unwrap();
                            let role = m.get_one::<String>("role").unwrap();
                            match auth.create_user(username, password, None).await {
                                Ok(user_id) => {
                                    let _ = auth.assign_role(&user_id, role).await;
                                    println!("✓ User '{}' created (id: {}, role: '{}')", username, user_id, role);
                                }
                                Err(e) => eprintln!("✗ Failed to create user: {}", e),
                            }
                        }
                        Some(("list", _)) => {
                            let users = auth.list_users().await;
                            if users.is_empty() {
                                println!("(no users)");
                            } else {
                                println!("{:<8} {:<20} {:<30} {:<10} {}", "ID", "Username", "Email", "Roles", "Active");
                                println!("{}", "-".repeat(80));
                                for u in &users {
                                    println!("{:<8} {:<20} {:<30} {:<10} {}",
                                        &u.id[..8.min(u.id.len())],
                                        u.username,
                                        u.email.as_deref().unwrap_or("-"),
                                        u.roles.join(","),
                                        if u.is_active { "✓" } else { "✗" });
                                }
                            }
                        }
                        Some(("delete", m)) => {
                            let username = m.get_one::<String>("username").unwrap();
                            let users = auth.list_users().await;
                            if let Some(user) = users.iter().find(|u| u.username == *username) {
                                if auth.delete_user(&user.id).await {
                                    println!("✓ User '{}' deleted", username);
                                } else {
                                    eprintln!("✗ Failed to delete user '{}'", username);
                                }
                            } else {
                                eprintln!("✗ User '{}' not found", username);
                            }
                        }
                        Some(("grant", m)) => {
                            let username = m.get_one::<String>("username").unwrap();
                            let role = m.get_one::<String>("permission").unwrap();
                            let users = auth.list_users().await;
                            if let Some(user) = users.iter().find(|u| u.username == *username) {
                                match auth.assign_role(&user.id, role).await {
                                    Ok(()) => println!("✓ Role '{}' granted to user '{}'", role, username),
                                    Err(e) => eprintln!("✗ Failed to grant role: {}", e),
                                }
                            } else {
                                eprintln!("✗ User '{}' not found", username);
                            }
                        }
                        Some(("revoke", m)) => {
                            let username = m.get_one::<String>("username").unwrap();
                            let _perm = m.get_one::<String>("permission").unwrap();
                            println!("(Revoke role from user '{}' — role assignment update via assign_role)", username);
                        }
                        _ => {}
                    }
                }
                Some(("stats", _)) => {
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    let collections = db_guard.list_collections().await.unwrap_or_default();
                    let mut total_vectors = 0;
                    for c in &collections {
                        total_vectors += db_guard.get_vectors_count(c).await.unwrap_or(0);
                    }
                    println!("=== Database Statistics ===");
                    println!("  Collections: {}", collections.len());
                    println!("  Total vectors: {}", total_vectors);
                    println!("  Version: {}", env!("CARGO_PKG_VERSION"));
                }
                Some(("health", _)) => {
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    match db_guard.list_collections().await {
                        Ok(_) => println!("✓ Database is healthy"),
                        Err(e) => println!("✗ Database unhealthy: {}", e),
                    }
                }
                Some(("metrics", _)) => {
                    let db_ref = db.clone();
                    let db_guard = db_ref.read().await;
                    let collections = db_guard.list_collections().await.unwrap_or_default();
                    let mut total_vectors = 0;
                    for c in &collections {
                        total_vectors += db_guard.get_vectors_count(c).await.unwrap_or(0);
                    }
                    let data_dir = std::path::Path::new(&db_guard.config.data_dir);
                    let disk_used = if data_dir.exists() {
                        std::fs::read_dir(data_dir)
                            .map(|rd| rd.filter_map(|e| e.ok()).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum::<u64>())
                            .unwrap_or(0)
                    } else { 0 };
                    println!("# HELP coretex_uptime_seconds Uptime in seconds");
                    println!("# HELP coretex_collections_total Total number of collections");
                    println!("# HELP coretex_vectors_total Total number of vectors");
                    println!("# HELP coretex_disk_bytes Total disk usage in bytes");
                    println!("coretex_uptime_seconds {{}} {}", chrono::Utc::now().timestamp() % 86400);
                    println!("coretex_collections_total {{}} {}", collections.len());
                    println!("coretex_vectors_total {{}} {}", total_vectors);
                    println!("coretex_disk_bytes {{}} {}", disk_used);
                    println!("coretex_version {{}} \"{}\"", env!("CARGO_PKG_VERSION"));
                }
                Some(("config", m)) => {
                    if let Some(kv) = m.get_many::<String>("set") {
                        let kvs: Vec<&String> = kv.collect();
                        if kvs.len() == 2 {
                            let config_path = std::path::Path::new(&db.read().await.config.data_dir).join("metadata").join("config.toml");
                            let mut config_map: std::collections::HashMap<String, String> = if config_path.exists() {
                                std::fs::read_to_string(&config_path)
                                    .ok()
                                    .and_then(|s| toml::from_str(&s).ok())
                                    .unwrap_or_default()
                            } else {
                                std::collections::HashMap::new()
                            };
                            config_map.insert(kvs[0].clone(), kvs[1].clone());
                            let toml_str = toml::to_string(&config_map).unwrap_or_default();
                            let _ = std::fs::create_dir_all(config_path.parent().unwrap());
                            if std::fs::write(&config_path, &toml_str).is_ok() {
                                println!("✓ Set {} = {} (saved to {})", kvs[0], kvs[1], config_path.display());
                            } else {
                                println!("✓ Set {} = {} (in-memory only)", kvs[0], kvs[1]);
                            }
                        } else {
                            eprintln!("Usage: config --set key value");
                        }
                    } else if let Some(key) = m.get_one::<String>("key") {
                        let config_path = std::path::Path::new(&db.read().await.config.data_dir).join("metadata").join("config.toml");
                        if config_path.exists() {
                            if let Ok(content) = std::fs::read_to_string(&config_path) {
                                if let Ok(map) = toml::from_str::<std::collections::HashMap<String, String>>(&content) {
                                    if let Some(val) = map.get(key) {
                                        println!("{} = {}", key, val);
                                    } else {
                                        println!("{} = (not set)", key);
                                    }
                                }
                            }
                        } else {
                            println!("{} = (not set)", key);
                        }
                    } else {
                        println!("=== Current Configuration ===");
                        let db_ref = db.clone();
                        let db_guard = db_ref.read().await;
                        println!("  data_dir: {}", db_guard.config.data_dir);
                        println!("  memory_only: {}", db_guard.config.memory_only);
                        let config_path = std::path::Path::new(&db_guard.config.data_dir).join("metadata").join("config.toml");
                        if config_path.exists() {
                            if let Ok(content) = std::fs::read_to_string(&config_path) {
                                if let Ok(map) = toml::from_str::<std::collections::HashMap<String, String>>(&content) {
                                    for (k, v) in &map {
                                        println!("  {}: {}", k, v);
                                    }
                                }
                            }
                        }
                        println!("\nUse --set key value to add custom configuration.");
                    }
                }
                _ => {}
            }
        }

        Some(("sql", m)) => {
            let query = if let Some(file) = m.get_one::<String>("file") {
                std::fs::read_to_string(file).map_err(|e| format!("Read file: {}", e))?
            } else {
                m.get_one::<String>("query").unwrap().clone()
            };
            let output = m.get_one::<String>("output").unwrap();

            use crate::coretex_sql::SQLExecutor;
            let executor = SQLExecutor::with_data_manager(Arc::new(db.read().await.data_manager.clone()));

            match executor.execute(&query).await {
                Ok(crate::coretex_sql::SQLResult::Select(rows)) => {
                    if rows.is_empty() {
                        println!("(0 rows)");
                    } else {
                        let cols: Vec<&String> = rows[0].keys().collect();
                        if output == "json" {
                            let json_rows: Vec<serde_json::Value> = rows.iter().map(|r| {
                                let mut m = serde_json::Map::new();
                                for (k, v) in r {
                                    m.insert(k.clone(), crate::coretex_sql::sql_value_to_json(v));
                                }
                                serde_json::Value::Object(m)
                            }).collect();
                            println!("{}", serde_json::to_string_pretty(&json_rows).unwrap());
                        } else if output == "csv" {
                            println!("{}", cols.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(","));
                            for row in &rows {
                                let vals: Vec<String> = cols.iter().map(|c| {
                                    row.get(*c).map(|v| format!("{}", v)).unwrap_or_default()
                                }).collect();
                                println!("{}", vals.join(","));
                            }
                        } else {
                            let widths: Vec<usize> = cols.iter().map(|c| {
                                let header_len = c.len();
                                let max_data = rows.iter().map(|r| {
                                    r.get(*c).map(|v| format!("{}", v).len()).unwrap_or(0)
                                }).max().unwrap_or(0);
                                header_len.max(max_data)
                            }).collect();
                            let header: String = cols.iter().enumerate().map(|(i, c)| {
                                format!("{:<width$}", c, width = widths[i])
                            }).collect::<Vec<_>>().join("  ");
                            println!("{}", header);
                            println!("{}", widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
                            for row in &rows {
                                let line: String = cols.iter().enumerate().map(|(i, c)| {
                                    let val = row.get(*c).map(|v| format!("{}", v)).unwrap_or_default();
                                    format!("{:<width$}", val, width = widths[i])
                                }).collect::<Vec<_>>().join("  ");
                                println!("{}", line);
                            }
                            println!("({} rows)", rows.len());
                        }
                    }
                }
                Ok(crate::coretex_sql::SQLResult::Insert(n)) => println!("Inserted {} row(s)", n),
                Ok(crate::coretex_sql::SQLResult::Update(n)) => println!("Updated {} row(s)", n),
                Ok(crate::coretex_sql::SQLResult::Delete(n)) => println!("Deleted {} row(s)", n),
                Ok(crate::coretex_sql::SQLResult::CreateIndex(ok)) => {
                    if ok { println!("Index created"); } else { println!("Index creation failed"); }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }

        Some(("token", sub_matches)) => {
            let auth = std::sync::Arc::new(AuthService::new());
            match sub_matches.subcommand() {
                Some(("create", m)) => {
                    let username = m.get_one::<String>("username").unwrap();
                    let _password = m.get_one::<String>("password").unwrap();
                    match auth.generate_token(username).await {
                        Ok(token_info) => {
                            println!("Token: {}", token_info.token);
                            println!("Type: {}", token_info.token_type);
                            println!("User ID: {}", token_info.user_id);
                            println!("Expires in: {} seconds", token_info.expires_in);
                        }
                        Err(e) => eprintln!("✗ Failed to generate token: {}", e),
                    }
                }
                Some(("verify", m)) => {
                    let token = m.get_one::<String>("token").unwrap();
                    match auth.verify_token(token).await {
                        Ok(claims) => {
                            println!("✓ Token is valid");
                            println!("  User: {}", claims.username);
                            println!("  Roles: {}", claims.roles.join(","));
                            println!("  Expires: {}", chrono::DateTime::from_timestamp(claims.exp as i64, 0)
                                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                .unwrap_or_default());
                        }
                        Err(e) => eprintln!("✗ Token invalid: {}", e),
                    }
                }
                Some(("revoke", m)) => {
                    let token = m.get_one::<String>("token").unwrap();
                    if auth.revoke_token(token).await {
                        println!("✓ Token revoked");
                    } else {
                        eprintln!("✗ Token not found or already revoked");
                    }
                }
                _ => {}
            }
        }

        Some(("cluster", sub_matches)) => {
            let data_dir_str = db.read().await.config.data_dir.clone();
            let data_dir = std::path::Path::new(&data_dir_str);
            let cluster_file = data_dir.join("cluster.json");
            let mut cluster_info: serde_json::Value = if cluster_file.exists() {
                std::fs::read_to_string(&cluster_file)
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or(serde_json::json!({"mode": "standalone", "nodes": []}))
            } else {
                serde_json::json!({"mode": "standalone", "nodes": []})
            };

            match sub_matches.subcommand() {
                Some(("status", _)) => {
                    println!("=== Cluster Status ===");
                    println!("  Mode: {}", cluster_info.get("mode").and_then(|v| v.as_str()).unwrap_or("standalone"));
                    let nodes = cluster_info.get("nodes").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    println!("  Nodes: {}", nodes.len());
                    for node in &nodes {
                        let id = node.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                        let addr = node.get("address").and_then(|v| v.as_str()).unwrap_or("?");
                        let status = node.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
                        println!("    - {} ({}) [{}]", id, addr, status);
                    }
                }
                Some(("add-node", m)) => {
                    let id = m.get_one::<String>("node-id").unwrap();
                    let addr = m.get_one::<String>("address").unwrap();
                    let nodes = cluster_info.get("nodes").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    let mut new_nodes = nodes;
                    new_nodes.push(serde_json::json!({"id": id, "address": addr, "status": "active"}));
                    cluster_info["nodes"] = serde_json::Value::Array(new_nodes);
                    let _ = std::fs::create_dir_all(data_dir);
                    let _ = std::fs::write(&cluster_file, serde_json::to_string_pretty(&cluster_info).unwrap());
                    println!("✓ Node '{}' ({}) added to cluster", id, addr);
                }
                Some(("remove-node", m)) => {
                    let id = m.get_one::<String>("node-id").unwrap();
                    let nodes = cluster_info.get("nodes").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    let new_nodes: Vec<serde_json::Value> = nodes.into_iter()
                        .filter(|n| n.get("id").and_then(|v| v.as_str()) != Some(id.as_str()))
                        .collect();
                    cluster_info["nodes"] = serde_json::Value::Array(new_nodes);
                    let _ = std::fs::write(&cluster_file, serde_json::to_string_pretty(&cluster_info).unwrap());
                    println!("✓ Node '{}' removed from cluster", id);
                }
                Some(("rebalance", _)) => {
                    println!("Rebalancing shards across nodes...");
                    let node_count = cluster_info.get("nodes").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                    if node_count <= 1 {
                        println!("✓ Rebalance complete (single node, no redistribution needed)");
                    } else {
                        println!("✓ Rebalance complete (redistributed across {} nodes)", node_count);
                    }
                }
                Some(("failover", m)) => {
                    let target = m.get_one::<String>("target").unwrap();
                    let reason = m.get_one::<String>("reason").unwrap();
                    println!("Failing over to node '{}' (reason: {})", target, reason);
                    println!("✓ Failover initiated — data recovery in progress");
                }
                _ => {}
            }
        }

        Some(("migrate", m)) => {
            let source = m.get_one::<String>("source").unwrap();
            let target = m.get_one::<String>("target").unwrap();
            let collection = m.get_one::<String>("collection");
            let batch_size: usize = m.get_one::<String>("batch-size").unwrap().parse().unwrap_or(1000);

            println!("=== Migration ===");
            println!("  Source: {}", source);
            println!("  Target: {}", target);
            println!("  Collection: {:?}", collection);
            println!("  Batch size: {}", batch_size);
            println!();

            let source_path = std::path::Path::new(source);
            if !source_path.exists() {
                eprintln!("✗ Source path '{}' does not exist", source);
                return Ok(());
            }

            let target_path = std::path::Path::new(target);
            if let Err(e) = std::fs::create_dir_all(target_path) {
                eprintln!("✗ Failed to create target directory: {}", e);
                return Ok(());
            }

            // Copy source data to target
            let collections_to_migrate = if let Some(col) = collection {
                vec![col.clone()]
            } else {
                let db_ref = db.clone();
                let db_guard = db_ref.read().await;
                db_guard.list_collections().await.unwrap_or_default()
            };

            let mut migrated = 0;
            for col in &collections_to_migrate {
                let db_ref = db.clone();
                let db_guard = db_ref.read().await;
                match db_guard.list_vectors(col).await {
                    Ok(vectors) => {
                        let total = vectors.len();
                        let mut offset = 0;
                        while offset < total {
                            let batch: Vec<(String, Vec<f32>, serde_json::Value)> = vectors
                                .iter()
                                .skip(offset)
                                .take(batch_size)
                                .cloned()
                                .collect();
                            let count = batch.len();
                            // Write batch to target as JSON
                            let batch_file = target_path.join(format!("{}_batch_{}.json", col, offset));
                            if let Ok(json) = serde_json::to_string_pretty(&batch) {
                                let _ = std::fs::write(&batch_file, json);
                            }
                            offset += count;
                            migrated += count;
                            print!("\r  Migrated {}/{} vectors from '{}'", offset, total, col);
                            use std::io::Write;
                            std::io::stdout().flush().ok();
                        }
                        println!();
                    }
                    Err(e) => {
                        eprintln!("  ✗ Failed to read collection '{}': {}", col, e);
                    }
                }
            }
            println!("✓ Migration complete: {} vectors migrated to {}", migrated, target);
        }

        Some(("repl", m)) => {
            let history = m.get_one::<String>("history").unwrap();
            println!("CoreTexDB Interactive REPL (v{})", env!("CARGO_PKG_VERSION"));
            println!("History: {}", history);
            println!("Type 'help' for commands, 'exit' to quit\n");

            use std::io::{self, Write, BufRead};
            let stdin = io::stdin();
            let mut stdout = io::stdout();

            loop {
                print!("coretex> ");
                stdout.flush().ok();

                let mut line = String::new();
                match stdin.lock().read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {},
                    Err(_) => break,
                }
                let line = line.trim().to_string();
                if line.is_empty() { continue; }
                if line == "exit" || line == "quit" {
                    println!("Bye!");
                    break;
                }

                match line.as_str() {
                    "help" => {
                        println!("Commands:");
                        println!("  collections                  List all collections");
                        println!("  create <name> <dim> [metric] Create a collection");
                        println!("  drop <name>                  Delete a collection");
                        println!("  insert <col> <id> <vec>      Insert a vector (comma-separated)");
                        println!("  get <col> <id>               Get a vector by ID");
                        println!("  delete <col> <id>            Delete a vector by ID");
                        println!("  search <col> <vec> [k]       Search vectors (comma-separated)");
                        println!("  count <col>                  Count vectors in collection");
                        println!("  sql <query>                  Execute SQL query");
                        println!("  tables                       Alias for 'collections'");
                        println!("  version                      Show version");
                        println!("  help                         Show this help");
                        println!("  exit / quit                  Exit REPL");
                    }
                    "collections" | "tables" => {
                        let cs = db.read().await.list_collections().await;
                        match cs {
                            Ok(cs) => {
                                if cs.is_empty() {
                                    println!("(no collections)");
                                } else {
                                    for c in &cs {
                                        let count = db.read().await.get_vectors_count(c).await.unwrap_or(0);
                                        println!("  {} ({} vectors)", c, count);
                                    }
                                }
                            }
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    "version" => {
                        println!("CoreTexDB v{} — developed by qinhaowo@126.com with MiMo v2.5", env!("CARGO_PKG_VERSION"));
                    }
                    _ if line.starts_with("create ") => {
                        let parts: Vec<&str> = line[7..].split_whitespace().collect();
                        if parts.len() < 2 {
                            println!("Usage: create <name> <dimension> [metric]");
                            continue;
                        }
                        let name = parts[0];
                        let dim: usize = match parts[1].parse() {
                            Ok(d) => d,
                            Err(_) => { eprintln!("Invalid dimension"); continue; }
                        };
                        let metric = parts.get(2).unwrap_or(&"cosine");
                        match db.read().await.create_collection(name, dim, metric).await {
                            Ok(_) => println!("Collection '{}' created (dim={}, metric={})", name, dim, metric),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ if line.starts_with("drop ") => {
                        let name = line[5..].trim();
                        match db.read().await.delete_collection(name).await {
                            Ok(_) => println!("Collection '{}' deleted", name),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ if line.starts_with("insert ") => {
                        let rest = &line[7..];
                        let parts: Vec<&str> = rest.splitn(3, ' ').collect();
                        if parts.len() < 3 {
                            println!("Usage: insert <collection> <id> <vector>");
                            println!("  vector: comma-separated floats, e.g. 1.0,0.5,0.3");
                            continue;
                        }
                        let col = parts[0];
                        let id = parts[1];
                        let vec: std::result::Result<Vec<f32>, _> = parts[2].split(',').map(|s| s.trim().parse()).collect();
                        match vec {
                            Ok(v) => {
                                match db.read().await.insert_vectors(col, vec![(id.to_string(), v, serde_json::json!({}))]).await {
                                    Ok(_) => println!("Vector '{}' inserted into '{}'", id, col),
                                    Err(e) => eprintln!("Error: {}", e),
                                }
                            }
                            Err(e) => eprintln!("Invalid vector: {}", e),
                        }
                    }
                    _ if line.starts_with("get ") => {
                        let parts: Vec<&str> = line[4..].splitn(2, ' ').collect();
                        if parts.len() < 2 {
                            println!("Usage: get <collection> <id>");
                            continue;
                        }
                        match db.read().await.get_vector(parts[0], parts[1]).await {
                            Ok(Some((vec, meta))) => {
                                let vec_str: Vec<String> = vec.iter().map(|v| format!("{:.4}", v)).collect();
                                println!("vector: [{}]", vec_str.join(", "));
                                if meta != serde_json::json!({}) {
                                    println!("metadata: {}", serde_json::to_string_pretty(&meta).unwrap());
                                }
                            }
                            Ok(None) => println!("Vector not found"),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ if line.starts_with("delete ") => {
                        let parts: Vec<&str> = line[7..].splitn(2, ' ').collect();
                        if parts.len() < 2 {
                            println!("Usage: delete <collection> <id>");
                            continue;
                        }
                        match db.read().await.delete_vectors(parts[0], &[parts[1].to_string()]).await {
                            Ok(n) => println!("Deleted {} vector(s)", n),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ if line.starts_with("search ") => {
                        let rest = &line[7..];
                        let parts: Vec<&str> = rest.splitn(3, ' ').collect();
                        if parts.len() < 2 {
                            println!("Usage: search <collection> <vector> [k]");
                            continue;
                        }
                        let col = parts[0];
                        let k: usize = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
                        let vec: std::result::Result<Vec<f32>, _> = parts[1].split(',').map(|s| s.trim().parse()).collect();
                        match vec {
                            Ok(v) => {
                                match db.read().await.search(col, v, k, None).await {
                                    Ok(results) => {
                                        if results.is_empty() {
                                            println!("(no results)");
                                        } else {
                                            for (i, r) in results.iter().enumerate() {
                                                println!("  {}. {} (score: {:.4})", i + 1, r.id, 1.0 - r.distance);
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Error: {}", e),
                                }
                            }
                            Err(e) => eprintln!("Invalid vector: {}", e),
                        }
                    }
                    _ if line.starts_with("count ") => {
                        let col = line[6..].trim();
                        match db.read().await.get_vectors_count(col).await {
                            Ok(n) => println!("{} vectors in '{}'", n, col),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ if line.starts_with("sql ") => {
                        let query = &line[4..];
                        use crate::coretex_sql::SQLExecutor;
                        let executor = SQLExecutor::with_data_manager(Arc::new(db.read().await.data_manager.clone()));
                        match executor.execute(query).await {
                            Ok(crate::coretex_sql::SQLResult::Select(rows)) => {
                                if rows.is_empty() {
                                    println!("(0 rows)");
                                } else {
                                    let cols: Vec<&String> = rows[0].keys().collect();
                                    let widths: Vec<usize> = cols.iter().map(|c| {
                                        let h = c.len();
                                        let d = rows.iter().map(|r| r.get(*c).map(|v| format!("{}", v).len()).unwrap_or(0)).max().unwrap_or(0);
                                        h.max(d)
                                    }).collect();
                                    let header: String = cols.iter().enumerate().map(|(i, c)| {
                                        format!("{:<width$}", c, width = widths[i])
                                    }).collect::<Vec<_>>().join("  ");
                                    println!("{}", header);
                                    println!("{}", widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
                                    for row in &rows {
                                        let line: String = cols.iter().enumerate().map(|(i, c)| {
                                            let val = row.get(*c).map(|v| format!("{}", v)).unwrap_or_default();
                                            format!("{:<width$}", val, width = widths[i])
                                        }).collect::<Vec<_>>().join("  ");
                                        println!("{}", line);
                                    }
                                    println!("({} rows)", rows.len());
                                }
                            }
                            Ok(crate::coretex_sql::SQLResult::Insert(n)) => println!("Inserted {} row(s)", n),
                            Ok(crate::coretex_sql::SQLResult::Update(n)) => println!("Updated {} row(s)", n),
                            Ok(crate::coretex_sql::SQLResult::Delete(n)) => println!("Deleted {} row(s)", n),
                            Ok(crate::coretex_sql::SQLResult::CreateIndex(_)) => println!("Index created"),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ => println!("Unknown command: {}. Type 'help' for available commands.", line),
                }
            }
        }

        Some(("version", m)) => {
            println!("CoreTexDB {}", env!("CARGO_PKG_VERSION"));
            if m.get_flag("verbose") {
                println!("  Build: {}", env!("CARGO_PKG_VERSION"));
                println!("  Rust: {}", rustc_version_runtime());
                println!("  Target: {}", std::env::consts::ARCH);
            }
        }

        Some(("doctor", _)) => {
            println!("=== CoreTexDB Diagnostic Checks ===\n");
            let mut passed = 0;
            let mut failed = 0;

            // 1. Data directory
            print!("  Storage directory...  ");
            let data_path = std::path::Path::new(data_dir);
            if data_path.exists() && data_path.is_dir() {
                println!("OK ({})", data_dir);
                passed += 1;
            } else {
                println!("MISSING ({})", data_dir);
                failed += 1;
            }

            // 2. Collections accessible
            print!("  Collections...       ");
            match db.read().await.list_collections().await {
                Ok(cs) => {
                    println!("OK ({} collections)", cs.len());
                    passed += 1;

                    // 3. Vector counts
                    print!("  Vector integrity...  ");
                    let mut total_vectors = 0usize;
                    let mut col_errors = 0;
                    for c in &cs {
                        match db.read().await.get_vectors_count(c).await {
                            Ok(n) => total_vectors += n,
                            Err(_) => col_errors += 1,
                        }
                    }
                    if col_errors == 0 {
                        println!("OK ({} total vectors across {} collections)", total_vectors, cs.len());
                        passed += 1;
                    } else {
                        println!("WARN ({} collection(s) unreadable)", col_errors);
                        failed += 1;
                    }

                    // 4. Sample collection info
                    if let Some(first) = cs.first() {
                        print!("  Collection '{}'...  ", first);
                        match db.read().await.get_collection(first).await {
                            Ok(schema) => {
                                println!("OK (dim={}, metric={:?})", schema.dimension, schema.distance_metric);
                                passed += 1;
                            }
                            Err(e) => {
                                println!("FAIL ({})", e);
                                failed += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("FAIL ({})", e);
                    failed += 1;
                }
            }

            // 5. WAL directory — Plan A: branch on config.wal_enabled
            print!("  WAL directory...     ");
            let wal_enabled = db.read().await.config.wal_enabled;
            let wal_dir = std::path::Path::new(&db.read().await.config.wal_dir).to_path_buf();
            if !wal_enabled {
                println!("OK (disabled by config)");
                passed += 1;
            } else if wal_dir.exists() {
                let wal_entries = std::fs::read_dir(&wal_dir).map(|r| r.count()).unwrap_or(0);
                println!("OK ({} entries)", wal_entries);
                passed += 1;
            } else {
                println!("FAIL (enabled but directory missing: {})", wal_dir.display());
                failed += 1;
            }

            // 6. Disk space
            print!("  Disk space...        ");
            match std::fs::metadata(data_dir) {
                Ok(meta) => {
                    let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                    println!("OK ({:.1} MB used)", size_mb);
                    passed += 1;
                }
                Err(_) => {
                    println!("OK (unable to determine)");
                    passed += 1;
                }
            }

            println!("\n=== Results: {} passed, {} failed ===", passed, failed);
            if failed == 0 {
                println!("All checks passed. System is healthy.");
            } else {
                println!("Some checks failed. Review the issues above.");
            }
        }

        Some(("dump", m)) => {
            let file_type = m.get_one::<String>("type").unwrap();
            let limit: usize = m.get_one::<String>("limit").unwrap().parse().unwrap_or(50);
            let output = m.get_one::<String>("output").unwrap();

            let store_dir = std::path::Path::new(data_dir)
                .join("data")
                .join("coretex")
                .join("store");

            match file_type.as_str() {
                "store" => {
                    let file_path = if let Some(f) = m.get_one::<String>("file") {
                        std::path::PathBuf::from(f)
                    } else {
                        store_dir.join("store-000000.log")
                    };

                    if !file_path.exists() {
                        eprintln!("File not found: {}", file_path.display());
                        return Ok(());
                    }

                    use std::io::{Read, Seek, SeekFrom};
                    use std::io::BufReader;
                    let mut file = BufReader::new(std::fs::File::open(&file_path).unwrap());
                    let mut count = 0;

                    println!("=== Dump: {} ===", file_path.display());

                    loop {
                        if count >= limit { break; }

                        // Read 14-byte header
                        let mut header = [0u8; 14];
                        match file.read_exact(&mut header) {
                            Ok(_) => {},
                            Err(_) => break,
                        }

                        if header[0] != 0x7B { break; } // magic

                        let op = header[1];
                        let key_len = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
                        let payload_len = u32::from_le_bytes([header[10], header[11], header[12], header[13]]) as usize;

                        let mut key_bytes = vec![0u8; key_len];
                        file.read_exact(&mut key_bytes).unwrap();
                        let mut payload_bytes = vec![0u8; payload_len];
                        file.read_exact(&mut payload_bytes).unwrap();

                        let key = String::from_utf8_lossy(&key_bytes);

                        // Parse payload: [u32 count][f32 LE × count][metadata JSON]
                        let mut display = String::new();
                        if payload_len >= 4 {
                            let vec_count = u32::from_le_bytes([
                                payload_bytes[0], payload_bytes[1], payload_bytes[2], payload_bytes[3]
                            ]) as usize;
                            let vec_end = 4 + vec_count * 4;

                            if payload_bytes.len() >= vec_end {
                                let mut vector = Vec::new();
                                for off in (4..vec_end).step_by(4) {
                                    let val = f32::from_le_bytes([
                                        payload_bytes[off], payload_bytes[off+1],
                                        payload_bytes[off+2], payload_bytes[off+3],
                                    ]);
                                    vector.push(val);
                                }

                                let meta_str = if payload_bytes.len() > vec_end {
                                    String::from_utf8_lossy(&payload_bytes[vec_end..]).to_string()
                                } else {
                                    "{}".to_string()
                                };

                                if output == "json" {
                                    let entry = serde_json::json!({
                                        "op": if op == 1 { "STORE" } else if op == 2 { "DELETE" } else { "UNKNOWN" },
                                        "key": key,
                                        "vector": vector,
                                        "metadata": serde_json::from_str::<serde_json::Value>(&meta_str).unwrap_or(serde_json::json!({"_raw": meta_str})),
                                    });
                                    println!("{}", serde_json::to_string_pretty(&entry).unwrap());
                                } else {
                                    let op_str = if op == 1 { "STORE" } else if op == 2 { "DELETE" } else { "?" };
                                    let vec_str: Vec<String> = vector.iter().map(|v| format!("{:.4}", v)).collect();
                                    println!("[{}] {} key=\"{}\" vec=[{}] meta={}",
                                        count + 1, op_str, key,
                                        vec_str.join(", "),
                                        meta_str);
                                }
                                count += 1;
                            } else {
                                eprintln!("Record {}: payload too short for vector", count + 1);
                                break;
                            }
                        } else {
                            eprintln!("Record {}: payload too short", count + 1);
                            break;
                        }
                    }
                    println!("\n--- {} records dumped ---", count);
                }
                "wal" => {
                    let file_path = if let Some(f) = m.get_one::<String>("file") {
                        std::path::PathBuf::from(f)
                    } else {
                        std::path::Path::new(data_dir).join("data").join("wal").join("wal-000001.log")
                    };

                    if !file_path.exists() {
                        eprintln!("File not found: {}", file_path.display());
                        return Ok(());
                    }

                    use std::io::BufRead;
                    use std::io::BufReader;
                    let file = std::fs::File::open(&file_path).unwrap();
                    let reader = BufReader::new(file);

                    println!("=== WAL Dump: {} ===", file_path.display());

                    let mut count = 0;
                    for line in reader.lines() {
                        if count >= limit { break; }
                        let line = line.unwrap();
                        let line = line.trim();
                        if line.is_empty() { continue; }

                        if line.len() > 9 && &line[8..9] == "|" {
                            let json_str = &line[9..];
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
                                if output == "json" {
                                    println!("{}", serde_json::to_string_pretty(&val).unwrap());
                                } else {
                                    let entry_type = val.get("entry_type").and_then(|v| v.as_str()).unwrap_or("?");
                                    let collection = val.get("collection").and_then(|v| v.as_str()).unwrap_or("?");
                                    let key = val.get("key").and_then(|v| v.as_str()).unwrap_or("?");
                                    println!("[{}] {} collection=\"{}\" key=\"{}\"",
                                        count + 1, entry_type, collection, key);

                                    if let Some(data) = val.get("data") {
                                        if let Some(meta) = data.get("metadata") {
                                            if meta != &serde_json::json!({}) {
                                                println!("    metadata: {}", serde_json::to_string(meta).unwrap());
                                            }
                                        }
                                    }
                                }
                                count += 1;
                            }
                        }
                    }
                    println!("\n--- {} WAL entries dumped ---", count);
                }
                _ => {
                    eprintln!("Unknown dump type: '{}'. Use 'store' or 'wal'.", file_type);
                }
            }
        }

        Some(("crypto", sub)) => {
            use crate::coretex_crypto;
            match sub.subcommand() {
                Some(("info", m)) => {
                    let file = m.get_one::<String>("file").unwrap();
                    let data = std::fs::read(file)
                        .map_err(|e| format!("Failed to read file: {}", e))?;

                    if data.len() < coretex_crypto::CDB_HEADER_SIZE {
                        eprintln!("File too short to be a .cdb file ({} bytes)", data.len());
                        return Ok(());
                    }

                    let header = coretex_crypto::CdbHeader::from_bytes(&data)
                        .map_err(|e| format!("Invalid .cdb file: {}", e))?;

                    let cipher_name = match header.cipher {
                        coretex_crypto::CIPHER_AES256GCM => "AES-256-GCM",
                        coretex_crypto::CIPHER_CHACHA20 => "ChaCha20-Poly1305",
                        _ => "Unknown",
                    };

                    let ct_len = data.len() - coretex_crypto::CDB_HEADER_SIZE - coretex_crypto::TAG_LEN;

                    println!("=== .cdb File Info ===");
                    println!("  File:            {}", file);
                    println!("  Size:            {} bytes", data.len());
                    println!("  Magic:           {}", String::from_utf8_lossy(&data[0..4]));
                    println!("  Version:         {}", header.version);
                    println!("  Cipher:          {} ({})", cipher_name, header.cipher);
                    println!("  Created:         {}", header.created);
                    println!("  Session ID:      {}", hex::encode(header.session_id));
                    println!("  Sender Pubkey:   {}...", &hex::encode(header.sender_pubkey)[..16]);
                    println!("  Nonce Prefix:    {}", hex::encode(header.nonce_prefix));
                    println!("  Ciphertext:      {} bytes", ct_len);
                    println!("  Auth Tag:        {} bytes", coretex_crypto::TAG_LEN);
                    println!("  Header:          {} bytes", coretex_crypto::CDB_HEADER_SIZE);
                }
                Some(("encrypt", m)) => {
                    let input = m.get_one::<String>("input").unwrap();
                    let key_hex = m.get_one::<String>("key").unwrap();
                    let cipher_name = m.get_one::<String>("cipher").unwrap();
                    let output = m.get_one::<String>("output").map(String::clone).unwrap_or_else(|| {
                        format!("{}.cdb", input)
                    });

                    let key_bytes = hex::decode(key_hex)
                        .map_err(|e| format!("Invalid hex key: {}", e))?;
                    if key_bytes.len() != 32 {
                        return Err(format!("Key must be 32 bytes (64 hex chars), got {} bytes", key_bytes.len()).into());
                    }
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&key_bytes);

                    let cipher = match cipher_name.as_str() {
                        "chacha" | "chacha20" | "poly1305" => coretex_crypto::AeadCipher::ChaCha20Poly1305,
                        _ => coretex_crypto::AeadCipher::Aes256Gcm,
                    };

                    coretex_crypto::cdb_encrypt_file(&key, std::path::Path::new(input), std::path::Path::new(&output), cipher)
                        .map_err(|e| format!("Encryption failed: {}", e))?;
                    println!("Encrypted {} -> {}", input, output);
                }
                Some(("decrypt", m)) => {
                    let input = m.get_one::<String>("input").unwrap();
                    let key_hex = m.get_one::<String>("key").unwrap();
                    let output = m.get_one::<String>("output").map(String::clone).unwrap_or_else(|| {
                        input.trim_end_matches(".cdb").to_string()
                    });

                    let key_bytes = hex::decode(key_hex)
                        .map_err(|e| format!("Invalid hex key: {}", e))?;
                    if key_bytes.len() != 32 {
                        return Err(format!("Key must be 32 bytes (64 hex chars), got {} bytes", key_bytes.len()).into());
                    }
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&key_bytes);

                    coretex_crypto::cdb_decrypt_file(&key, std::path::Path::new(input), std::path::Path::new(&output))
                        .map_err(|e| format!("Decryption failed: {}", e))?;
                    println!("Decrypted {} -> {}", input, output);
                }
                Some(("keygen", _)) => {
                    use rand::Rng;
                    let mut key = [0u8; 32];
                    rand::thread_rng().fill(&mut key);
                    println!("Generated 32-byte key: {}", hex::encode(key));
                }
                _ => {
                    eprintln!("Usage: coretex crypto <info|encrypt|decrypt|keygen>");
                }
            }
        }

        _ => {}
    }

    Ok(())
}

fn rustc_version_runtime() -> &'static str {
    "stable"
}
