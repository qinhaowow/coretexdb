//! coretex-backup — backup/restore entry point.
fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let has_sub = argv.iter().any(|a| a == "backup" || a == "restore");
    let mut args: Vec<std::ffi::OsString> = vec!["coretex".into()];
    if !has_sub {
        args.push("backup".into());
    }
    args.extend(std::env::args_os().skip(1));
    if let Err(e) = coretexdb::run_cli_with_args(args) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
