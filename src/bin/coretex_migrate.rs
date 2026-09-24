//! coretex-migrate — data migration tool.
fn main() {
    println!("coretex-migrate {}", env!("CARGO_PKG_VERSION"));
    println!("Migration entrypoint: use 'coretex-cli dump/restore' for store/WAL moves.");
    println!("See share/doc/INSTALL.md for upgrade procedure.");
}
