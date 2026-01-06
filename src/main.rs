//! CortexDB CLI
//!
//! Command-line interface for CortexDB.
//! Provides commands for database operations, server management, and administration.

use cortexdb::run_cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_cli()
}