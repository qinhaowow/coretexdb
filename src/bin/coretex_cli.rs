//! coretex-cli — command-line client (same as `coretex`).
fn main() {
    if let Err(e) = coretexdb::run_cli() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
