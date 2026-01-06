//! Command-line interface module for CortexDB.
//!
//! This module provides the command-line interface for CortexDB, including:
//! - Command parsing and execution
//! - Collection management commands
//! - Document management commands
//! - Search and query commands
//! - System management commands
//! - Help and usage information

pub mod commands;
pub mod parser;
pub mod utils;

pub use commands::*;
pub use parser::*;
pub use utils::*;
