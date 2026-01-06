//! Index system module for CortexDB.
//!
//! This module provides index management and implementation for CortexDB, including:
//! - Vector index abstraction and implementations
//! - Scalar index implementation
//! - Index manager for creating and managing indexes
//! - Support for different index types and algorithms

pub mod vector;
pub mod brute_force;
pub mod scalar;
pub mod manager;

pub use vector::*;
pub use brute_force::*;
pub use scalar::*;
pub use manager::*;
