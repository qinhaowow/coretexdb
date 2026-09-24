//! coretexd — database server entry point.
fn main() {
    let mut args: Vec<std::ffi::OsString> = vec![
        "coretex".into(),
        "server".into(),
    ];
    args.extend(std::env::args_os().skip(1));
    if let Err(e) = coretexdb::run_cli_with_args(args) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
