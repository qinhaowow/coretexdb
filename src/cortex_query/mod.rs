//! Query system module for CortexDB.
//!
//! This module provides query management and execution for CortexDB, including:
//! - Query builder for constructing queries
//! - Query executor for executing queries
//! - Query planner for optimizing query execution
//! - Query optimizer for improving query performance
//! - Support for vector similarity search and scalar queries

pub mod builder;
pub mod executor;
pub mod planner;
pub mod optimizer;

pub use builder::*;
pub use executor::*;
pub use planner::*;
pub use optimizer::*;
