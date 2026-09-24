//! coretex-healthcheck — health check entry point (`doctor`).
fn main() {
    let mut args: Vec<std::ffi::OsString> = vec!["coretex".into(), "doctor".into()];
    args.extend(std::env::args_os().skip(1));
    if let Err(e) = coretexdb::run_cli_with_args(args) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
