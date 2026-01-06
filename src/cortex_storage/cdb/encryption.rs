//! Encryption support for CDB file format.
//!
//! This module provides encryption utilities for the CDB file format, including:
//! - Encryption header management
//! - AES-256-GCM encryption/decryption
//! - Key management utilities
//! - Encryption statistics tracking

use crate::cortex_core::error::{CortexError, StorageError};
use aes_gcm::{
    aead::{Aead, NewAead},
    Aes256Gcm, Key, Nonce,
};
use bytes::{Bytes, BytesMut};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Encryption algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    /// No encryption
    None,
    /// AES-256-GCM encryption
    Aes256Gcm,
}

/// Encryption header for CDB files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionHeader {
    /// Encryption algorithm used
    algorithm: EncryptionAlgorithm,
    /// Key derivation salt
    salt: [u8; 16],
    /// Initialization vector
    iv: [u8; 12],
    /// Authentication tag
    tag: [u8; 16],
    /// Encryption timestamp
    timestamp: u64,
    /// Key identifier
    key_id: Option<String>,
}

/// Encryption statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EncryptionStats {
    /// Number of encrypted blocks
    encrypted_blocks: u64,
    /// Number of decrypted blocks
    decrypted_blocks: u64,
    /// Total encryption time (nanoseconds)
    encryption_time_ns: u64,
    /// Total decryption time (nanoseconds)
    decryption_time_ns: u64,
    /// Average encryption time per block (nanoseconds)
    avg_encryption_time_ns: f64,
    /// Average decryption time per block (nanoseconds)
    avg_decryption_time_ns: f64,
}

/// Encryption manager for CDB files
pub struct EncryptionManager {
    /// Encryption key
    key: Option<[u8; 32]>,
    /// Encryption algorithm
    algorithm: EncryptionAlgorithm,
    /// Encryption statistics
    stats: EncryptionStats,
}

impl EncryptionManager {
    /// Create a new encryption manager with optional key
    pub fn new(key: Option<&[u8]>, algorithm: EncryptionAlgorithm) -> Result<Self, CortexError> {
        let key = if let Some(key) = key {
            if key.len() != 32 {
                return Err(CortexError::Storage(StorageError::InvalidEncryptionKey(
                    "Encryption key must be 32 bytes for AES-256-GCM".to_string(),
                )));
            }
            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(key);
            Some(key_array)
        } else if algorithm != EncryptionAlgorithm::None {
            return Err(CortexError::Storage(StorageError::InvalidEncryptionKey(
                "Encryption key is required for non-None algorithms".to_string(),
            )));
        } else {
            None
        };

        Ok(Self {
            key,
            algorithm,
            stats: EncryptionStats::default(),
        })
    }

    /// Generate a random encryption key
    pub fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    /// Encrypt data
    pub fn encrypt(&mut self, data: &[u8]) -> Result<(Bytes, EncryptionHeader), CortexError> {
        match self.algorithm {
            EncryptionAlgorithm::None => {
                let header = EncryptionHeader {
                    algorithm: EncryptionAlgorithm::None,
                    salt: [0u8; 16],
                    iv: [0u8; 12],
                    tag: [0u8; 16],
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                    key_id: None,
                };
                Ok((Bytes::copy_from_slice(data), header))
            }
            EncryptionAlgorithm::Aes256Gcm => {
                let start_time = std::time::Instant::now();
                
                let key = self.key.ok_or_else(|| {
                    CortexError::Storage(StorageError::InvalidEncryptionKey(
                        "Encryption key not set".to_string(),
                    ))
                })?;

                let mut salt = [0u8; 16];
                rand::thread_rng().fill_bytes(&mut salt);

                let mut iv = [0u8; 12];
                rand::thread_rng().fill_bytes(&mut iv);

                let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
                let nonce = Nonce::<Aes256Gcm>::from_slice(&iv);

                let ciphertext = cipher
                    .encrypt(nonce, data.as_ref())
                    .map_err(|e| {
                        CortexError::Storage(StorageError::EncryptionError(format!(
                            "Failed to encrypt data: {}",
                            e
                        )))
                    })?;

                // For simplicity, we'll use a fixed tag size (in practice, AEAD returns the tag appended)
                let mut tag = [0u8; 16];
                if ciphertext.len() >= 16 {
                    tag.copy_from_slice(&ciphertext[ciphertext.len() - 16..]);
                }

                let header = EncryptionHeader {
                    algorithm: EncryptionAlgorithm::Aes256Gcm,
                    salt,
                    iv,
                    tag,
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                    key_id: None,
                };

                let encryption_time = start_time.elapsed().as_nanos() as u64;
                self.stats.encrypted_blocks += 1;
                self.stats.encryption_time_ns += encryption_time;
                self.stats.avg_encryption_time_ns =
                    self.stats.encryption_time_ns as f64 / self.stats.encrypted_blocks as f64;

                Ok((Bytes::from(ciphertext), header))
            }
        }
    }

    /// Decrypt data
    pub fn decrypt(&mut self, data: &[u8], header: &EncryptionHeader) -> Result<Bytes, CortexError> {
        match header.algorithm {
            EncryptionAlgorithm::None => Ok(Bytes::copy_from_slice(data)),
            EncryptionAlgorithm::Aes256Gcm => {
                let start_time = std::time::Instant::now();
                
                let key = self.key.ok_or_else(|| {
                    CortexError::Storage(StorageError::InvalidEncryptionKey(
                        "Encryption key not set".to_string(),
                    ))
                })?;

                let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
                let nonce = Nonce::<Aes256Gcm>::from_slice(&header.iv);

                let plaintext = cipher
                    .decrypt(nonce, data.as_ref())
                    .map_err(|e| {
                        CortexError::Storage(StorageError::DecryptionError(format!(
                            "Failed to decrypt data: {}",
                            e
                        )))
                    })?;

                let decryption_time = start_time.elapsed().as_nanos() as u64;
                self.stats.decrypted_blocks += 1;
                self.stats.decryption_time_ns += decryption_time;
                self.stats.avg_decryption_time_ns =
                    self.stats.decryption_time_ns as f64 / self.stats.decrypted_blocks as f64;

                Ok(Bytes::from(plaintext))
            }
        }
    }

    /// Get encryption statistics
    pub fn stats(&self) -> &EncryptionStats {
        &self.stats
    }

    /// Reset encryption statistics
    pub fn reset_stats(&mut self) {
        self.stats = EncryptionStats::default();
    }

    /// Check if encryption is enabled
    pub fn is_encrypted(&self) -> bool {
        self.algorithm != EncryptionAlgorithm::None
    }

    /// Get encryption algorithm
    pub fn algorithm(&self) -> EncryptionAlgorithm {
        self.algorithm
    }
}

/// Test utilities for encryption
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_roundtrip() {
        let test_data = b"Hello, encrypted world!";
        let key = EncryptionManager::generate_key();
        
        let mut encryptor = EncryptionManager::new(Some(&key), EncryptionAlgorithm::Aes256Gcm)
            .expect("Failed to create encryption manager");
        
        let (encrypted, header) = encryptor.encrypt(test_data).expect("Failed to encrypt");
        
        let mut decryptor = EncryptionManager::new(Some(&key), EncryptionAlgorithm::Aes256Gcm)
            .expect("Failed to create decryption manager");
        
        let decrypted = decryptor.decrypt(&encrypted, &header).expect("Failed to decrypt");
        
        assert_eq!(test_data, decrypted.as_ref());
    }

    #[test]
    fn test_no_encryption() {
        let test_data = b"Hello, unencrypted world!";
        
        let mut manager = EncryptionManager::new(None, EncryptionAlgorithm::None)
            .expect("Failed to create encryption manager");
        
        let (encrypted, header) = manager.encrypt(test_data).expect("Failed to encrypt");
        
        assert_eq!(test_data, encrypted.as_ref());
        assert_eq!(header.algorithm, EncryptionAlgorithm::None);
        
        let decrypted = manager.decrypt(&encrypted, &header).expect("Failed to decrypt");
        
        assert_eq!(test_data, decrypted.as_ref());
    }

    #[test]
    fn test_invalid_key() {
        let test_data = b"Test data";
        let invalid_key = [0u8; 16]; // Too short
        
        let result = EncryptionManager::new(Some(&invalid_key), EncryptionAlgorithm::Aes256Gcm);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_stats_tracking() {
        let test_data = b"Test data for stats";
        let key = EncryptionManager::generate_key();
        
        let mut manager = EncryptionManager::new(Some(&key), EncryptionAlgorithm::Aes256Gcm)
            .expect("Failed to create encryption manager");
        
        // Initial stats should be zero
        assert_eq!(manager.stats().encrypted_blocks, 0);
        assert_eq!(manager.stats().decrypted_blocks, 0);
        
        // Encrypt data
        let (encrypted, header) = manager.encrypt(test_data).expect("Failed to encrypt");
        
        assert_eq!(manager.stats().encrypted_blocks, 1);
        assert!(manager.stats().encryption_time_ns > 0);
        
        // Decrypt data
        manager.decrypt(&encrypted, &header).expect("Failed to decrypt");
        
        assert_eq!(manager.stats().decrypted_blocks, 1);
        assert!(manager.stats().decryption_time_ns > 0);
        
        // Reset stats
        manager.reset_stats();
        
        assert_eq!(manager.stats().encrypted_blocks, 0);
        assert_eq!(manager.stats().decrypted_blocks, 0);
    }
}
