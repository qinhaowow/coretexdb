//! ID generator utilities for CortexDB.
//!
//! This module provides ID generation functionality for CortexDB, including:
//! - UUID v4 generation
//! - Snowflake ID generation
//! - Custom ID generation with prefixes
//! - ID validation utilities
//! - ID format conversion

use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// ID generator traits
pub trait IdGenerator {
    /// Generate a new ID
    fn generate(&self) -> String;
    
    /// Validate an ID
    fn validate(&self, id: &str) -> bool;
}

/// UUID v4 generator
#[derive(Debug, Clone)]
pub struct UuidGenerator;

impl UuidGenerator {
    /// Create a new UUID generator
    pub fn new() -> Self {
        Self
    }
}

impl IdGenerator for UuidGenerator {
    fn generate(&self) -> String {
        let mut rng = rand::thread_rng();
        
        let time_low = rng.next_u32();
        let time_mid = rng.next_u16();
        let time_hi_and_version = (rng.next_u16() & 0x0FFF) | 0x4000;
        let clock_seq_hi_and_reserved = (rng.next_u8() & 0x3F) | 0x80;
        let clock_seq_low = rng.next_u8();
        let node = rng.next_u64() & 0xFFFF_FFFF_FFFF;
        
        format!(
            "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:012x}",
            time_low,
            time_mid,
            time_hi_and_version,
            clock_seq_hi_and_reserved,
            clock_seq_low,
            node
        )
    }
    
    fn validate(&self, id: &str) -> bool {
        // Basic UUID v4 validation
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 5 {
            return false;
        }
        
        // Check version (should be 4)
        let version_part = parts[2];
        if version_part.len() != 4 {
            return false;
        }
        
        let version_char = version_part.chars().nth(0).unwrap_or('0');
        version_char == '4'
    }
}

/// Snowflake ID generator
#[derive(Debug, Clone)]
pub struct SnowflakeGenerator {
    /// Timestamp bits (41 bits)
    epoch: u64,
    /// Machine ID bits (10 bits)
    machine_id: u64,
    /// Sequence bits (12 bits)
    sequence: AtomicU64,
    /// Last timestamp
    last_timestamp: AtomicU64,
}

impl SnowflakeGenerator {
    /// Create a new Snowflake generator
    pub fn new(machine_id: u64) -> Self {
        // Use a custom epoch (2024-01-01 00:00:00 UTC)
        let epoch = SystemTime::new()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            epoch,
            machine_id: machine_id & 0x3FF, // 10 bits
            sequence: AtomicU64::new(0),
            last_timestamp: AtomicU64::new(0),
        }
    }
    
    /// Create a Snowflake generator with default machine ID
    pub fn with_default_machine_id() -> Self {
        // Generate a random machine ID
        let machine_id = rand::thread_rng().gen_range(0..1024);
        Self::new(machine_id)
    }
}

impl IdGenerator for SnowflakeGenerator {
    fn generate(&self) -> String {
        let mut sequence = self.sequence.load(Ordering::SeqCst);
        let mut last_timestamp = self.last_timestamp.load(Ordering::SeqCst);
        
        let mut current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Handle clock drift
        if current_timestamp < last_timestamp {
            current_timestamp = last_timestamp;
        }
        
        // Increment sequence if timestamp is the same as last
        if current_timestamp == last_timestamp {
            sequence = (sequence + 1) & 0xFFF; // 12 bits
            if sequence == 0 {
                // Sequence overflow, wait for next millisecond
                while current_timestamp <= last_timestamp {
                    current_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                }
            }
        } else {
            sequence = 0;
        }
        
        // Update last timestamp and sequence
        self.last_timestamp.store(current_timestamp, Ordering::SeqCst);
        self.sequence.store(sequence, Ordering::SeqCst);
        
        // Build snowflake ID
        let timestamp = current_timestamp - self.epoch;
        let snowflake = (timestamp << 22) | (self.machine_id << 12) | sequence;
        
        snowflake.to_string()
    }
    
    fn validate(&self, id: &str) -> bool {
        // Check if ID is a valid number
        if let Ok(snowflake) = id.parse::<u64>() {
            // Check timestamp part
            let timestamp = (snowflake >> 22) + self.epoch;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            
            // Timestamp should not be in the future
            timestamp <= now
        } else {
            false
        }
    }
}

/// Prefixed ID generator
#[derive(Debug, Clone)]
pub struct PrefixedIdGenerator {
    /// Base generator
    base_generator: Box<dyn IdGenerator>,
    /// ID prefix
    prefix: String,
    /// Separator
    separator: String,
}

impl PrefixedIdGenerator {
    /// Create a new prefixed ID generator
    pub fn new(base_generator: Box<dyn IdGenerator>, prefix: &str, separator: &str) -> Self {
        Self {
            base_generator,
            prefix: prefix.to_string(),
            separator: separator.to_string(),
        }
    }
}

impl IdGenerator for PrefixedIdGenerator {
    fn generate(&self) -> String {
        let base_id = self.base_generator.generate();
        format!("{}{}{}", self.prefix, self.separator, base_id)
    }
    
    fn validate(&self, id: &str) -> bool {
        if id.starts_with(&self.prefix) {
            let base_id = id.strip_prefix(&format!("{}{}", self.prefix, self.separator))
                .unwrap_or(id);
            self.base_generator.validate(base_id)
        } else {
            false
        }
    }
}

/// Document ID generator
#[derive(Debug, Clone)]
pub struct DocumentIdGenerator {
    /// Underlying generator
    generator: PrefixedIdGenerator,
}

impl DocumentIdGenerator {
    /// Create a new document ID generator
    pub fn new() -> Self {
        let base_generator = Box::new(UuidGenerator::new());
        let prefixed_generator = PrefixedIdGenerator::new(base_generator, "doc", "_");
        Self {
            generator: prefixed_generator,
        }
    }
}

impl IdGenerator for DocumentIdGenerator {
    fn generate(&self) -> String {
        self.generator.generate()
    }
    
    fn validate(&self, id: &str) -> bool {
        self.generator.validate(id)
    }
}

/// Collection ID generator
#[derive(Debug, Clone)]
pub struct CollectionIdGenerator {
    /// Underlying generator
    generator: PrefixedIdGenerator,
}

impl CollectionIdGenerator {
    /// Create a new collection ID generator
    pub fn new() -> Self {
        let base_generator = Box::new(UuidGenerator::new());
        let prefixed_generator = PrefixedIdGenerator::new(base_generator, "col", "_");
        Self {
            generator: prefixed_generator,
        }
    }
}

impl IdGenerator for CollectionIdGenerator {
    fn generate(&self) -> String {
        self.generator.generate()
    }
    
    fn validate(&self, id: &str) -> bool {
        self.generator.validate(id)
    }
}

/// Generate a random string of specified length
pub fn generate_random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Validate a UUID string
pub fn validate_uuid(uuid: &str) -> bool {
    let generator = UuidGenerator::new();
    generator.validate(uuid)
}

/// Validate a Snowflake ID
pub fn validate_snowflake(id: &str) -> bool {
    let generator = SnowflakeGenerator::with_default_machine_id();
    generator.validate(id)
}

/// Format a UUID as a compact string (without hyphens)
pub fn format_uuid_compact(uuid: &str) -> String {
    uuid.replace('-', "")
}

/// Format a compact UUID with hyphens
pub fn format_uuid_with_hyphens(compact_uuid: &str) -> String {
    if compact_uuid.len() != 32 {
        return compact_uuid.to_string();
    }
    
    format!(
        "{}-{}-{}-{}-{}",
        &compact_uuid[0..8],
        &compact_uuid[8..12],
        &compact_uuid[12..16],
        &compact_uuid[16..20],
        &compact_uuid[20..32]
    )
}

/// Generate a short ID (8 characters)
pub fn generate_short_id() -> String {
    generate_random_string(8)
}

/// Generate a medium ID (16 characters)
pub fn generate_medium_id() -> String {
    generate_random_string(16)
}

/// Generate a long ID (32 characters)
pub fn generate_long_id() -> String {
    generate_random_string(32)
}

/// Test utilities for ID generation
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_generator() {
        let generator = UuidGenerator::new();
        
        let id = generator.generate();
        println!("Generated UUID: {}", id);
        assert!(generator.validate(&id));
        assert_eq!(id.len(), 36);
    }

    #[test]
    fn test_snowflake_generator() {
        let generator = SnowflakeGenerator::with_default_machine_id();
        
        let id1 = generator.generate();
        let id2 = generator.generate();
        
        println!("Generated Snowflake IDs: {}, {}", id1, id2);
        assert!(generator.validate(&id1));
        assert!(generator.validate(&id2));
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_prefixed_id_generator() {
        let base_generator = Box::new(UuidGenerator::new());
        let generator = PrefixedIdGenerator::new(base_generator, "user", "_");
        
        let id = generator.generate();
        println!("Generated prefixed ID: {}", id);
        assert!(generator.validate(&id));
        assert!(id.starts_with("user_"));
    }

    #[test]
    fn test_document_id_generator() {
        let generator = DocumentIdGenerator::new();
        
        let id = generator.generate();
        println!("Generated document ID: {}", id);
        assert!(generator.validate(&id));
        assert!(id.starts_with("doc_"));
    }

    #[test]
    fn test_collection_id_generator() {
        let generator = CollectionIdGenerator::new();
        
        let id = generator.generate();
        println!("Generated collection ID: {}", id);
        assert!(generator.validate(&id));
        assert!(id.starts_with("col_"));
    }

    #[test]
    fn test_random_string_generation() {
        let short = generate_short_id();
        assert_eq!(short.len(), 8);
        
        let medium = generate_medium_id();
        assert_eq!(medium.len(), 16);
        
        let long = generate_long_id();
        assert_eq!(long.len(), 32);
    }

    #[test]
    fn test_uuid_formatting() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let compact = format_uuid_compact(uuid);
        assert_eq!(compact, "123e4567e89b12d3a456426614174000");
        
        let formatted = format_uuid_with_hyphens(&compact);
        assert_eq!(formatted, uuid);
    }

    #[test]
    fn test_id_validation() {
        let uuid = UuidGenerator::new().generate();
        assert!(validate_uuid(&uuid));
        
        let snowflake = SnowflakeGenerator::with_default_machine_id().generate();
        assert!(validate_snowflake(&snowflake));
    }
}
