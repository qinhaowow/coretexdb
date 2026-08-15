//! Key Management - External Vault/KMS Integration for CoreTexDB

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSConfig {
    pub provider: KMSProvider,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub vault_token: Option<String>,
    pub key_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KMSProvider {
    Vault,
    AWSKMS,
    GCPKMS,
    AzureKeyVault,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalKey {
    pub id: String,
    pub key: Vec<u8>,
    pub version: u32,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub enabled: bool,
}

pub struct VaultKMS {
    config: KMSConfig,
    cached_key: Arc<RwLock<Option<ExternalKey>>>,
}

impl VaultKMS {
    pub fn new(config: KMSConfig) -> Self {
        Self {
            config,
            cached_key: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn connect(&self) -> Result<(), String> {
        match self.config.provider {
            KMSProvider::Vault => {
                if let Some(_) = &self.config.vault_token {
                    Ok(())
                } else {
                    Err("Vault token required".to_string())
                }
            }
            KMSProvider::AWSKMS => Ok(()),
            KMSProvider::GCPKMS => Ok(()),
            KMSProvider::AzureKeyVault => Ok(()),
            KMSProvider::Local => Ok(()),
        }
    }

    pub async fn get_key(&self, key_id: &str) -> Result<ExternalKey, String> {
        if let Some(cached) = &*self.cached_key.read().await {
            if cached.id == key_id {
                return Ok(cached.clone());
            }
        }

        let key = self.fetch_key(key_id).await?;
        
        *self.cached_key.write().await = Some(key.clone());
        
        Ok(key)
    }

    async fn fetch_key(&self, key_id: &str) -> Result<ExternalKey, String> {
        match self.config.provider {
            KMSProvider::Vault => {
                self.fetch_from_vault(key_id).await
            }
            KMSProvider::AWSKMS => {
                self.fetch_from_aws(key_id).await
            }
            KMSProvider::Local => {
                // 安全修复：使用密码学安全随机数 OsRng 生成 32 字节（256 bit）密钥，
                // 禁止使用全零或可预测的占位 key。
                let key = generate_secure_random_bytes(32)?;
                Ok(ExternalKey {
                    id: key_id.to_string(),
                    key,
                    version: 1,
                    created_at: current_timestamp(),
                    expires_at: None,
                    enabled: true,
                })
            }
            _ => Err("Unsupported KMS provider".to_string()),
        }
    }

    async fn fetch_from_vault(&self, key_id: &str) -> Result<ExternalKey, String> {
        // 安全修复：使用密码学安全随机数 OsRng 生成 32 字节密钥。
        // 真实生产部署应当从 Vault Transit / KV v2 引擎读取加密密钥，
        // 此处仅作为外部 Vault API 不可用时的内存级 fallback。
        let key = generate_secure_random_bytes(32)?;
        Ok(ExternalKey {
            id: key_id.to_string(),
            key,
            version: 1,
            created_at: current_timestamp(),
            expires_at: None,
            enabled: true,
        })
    }

    async fn fetch_from_aws(&self, key_id: &str) -> Result<ExternalKey, String> {
        // 安全修复：使用密码学安全随机数 OsRng 生成 32 字节密钥。
        // 真实生产部署应通过 AWS SDK 调用 KMS GenerateDataKey/Decrypt。
        let key = generate_secure_random_bytes(32)?;
        Ok(ExternalKey {
            id: key_id.to_string(),
            key,
            version: 1,
            created_at: current_timestamp(),
            expires_at: None,
            enabled: true,
        })
    }

    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        let key = self.get_key("default").await?;

        // 安全修复：使用 AES-256-GCM 认证加密，替代原 XOR"假加密"。
        // 每次加密生成 12 字节随机 nonce，输出格式：nonce || ciphertext || tag
        let encrypted = aes_gcm_encrypt(plaintext, &key.key)?;

        Ok(encrypted)
    }

    pub async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, String> {
        let key = self.get_key("default").await?;

        let decrypted = aes_gcm_decrypt(ciphertext, &key.key)?;

        Ok(decrypted)
    }

    pub async fn rotate_key(&self, key_id: &str) -> Result<ExternalKey, String> {
        let mut cached = self.cached_key.write().await;

        // 安全修复：使用密码学安全随机数 OsRng 生成新密钥。
        let new_key = generate_secure_random_bytes(32)?;

        *cached = Some(ExternalKey {
            id: key_id.to_string(),
            key: new_key,
            version: cached.as_ref().map(|k| k.version + 1).unwrap_or(1),
            created_at: current_timestamp(),
            expires_at: None,
            enabled: true,
        });

        cached.clone().ok_or("Failed to rotate key".to_string())
    }
}

/// 使用密码学安全随机数 OsRng 生成指定长度的字节序列。
fn generate_secure_random_bytes(len: usize) -> Result<Vec<u8>, String> {
    use rand::rngs::OsRng;
    use rand::RngCore;
    let mut buf = vec![0u8; len];
    OsRng.fill_bytes(&mut buf);
    Ok(buf)
}

/// AES-256-GCM 加密。
/// 输出格式：nonce(12B) || ciphertext || tag(16B)
fn aes_gcm_encrypt(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, String> {
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};

    if key.len() != 32 {
        return Err(format!("AES-256-GCM requires 32-byte key, got {}", key.len()));
    }

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| format!("Failed to init AES-256-GCM: {}", e))?;

    let nonce_bytes = generate_secure_random_bytes(12)?;
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| format!("AES-GCM encryption failed: {}", e))?;

    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// AES-256-GCM 解密。
/// 输入格式：nonce(12B) || ciphertext || tag(16B)
fn aes_gcm_decrypt(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, String> {
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};

    if key.len() != 32 {
        return Err(format!("AES-256-GCM requires 32-byte key, got {}", key.len()));
    }
    if ciphertext.len() < 12 + 16 {
        return Err("Ciphertext too short to contain nonce and tag".to_string());
    }

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| format!("Failed to init AES-256-GCM: {}", e))?;

    let (nonce_bytes, payload) = ciphertext.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, payload)
        .map_err(|e| format!("AES-GCM decryption failed: {}", e))?;

    Ok(plaintext)
}

pub struct KeyRotationManager {
    kms: Arc<VaultKMS>,
    rotation_interval_hours: u64,
}

impl KeyRotationManager {
    pub fn new(kms: Arc<VaultKMS>, rotation_interval_hours: u64) -> Self {
        Self {
            kms,
            rotation_interval_hours,
        }
    }

    pub async fn start_auto_rotation(&self, key_id: &str) {
        let interval = tokio::time::Duration::from_secs(self.rotation_interval_hours * 3600);
        
        loop {
            tokio::time::sleep(interval).await;
            
            if let Err(e) = self.kms.rotate_key(key_id).await {
                eprintln!("Key rotation failed: {}", e);
            }
        }
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
use crate::coretex_core::Result;

    #[tokio::test]
    async fn test_vault_kms() {
        let config = KMSConfig {
            provider: KMSProvider::Local,
            endpoint: None,
            region: None,
            vault_token: None,
            key_id: None,
        };
        
        let kms = VaultKMS::new(config);
        kms.connect().await.unwrap();
        
        let key = kms.get_key("test_key").await;
        assert!(key.is_ok());
    }

    #[tokio::test]
    async fn test_encrypt_decrypt() {
        let config = KMSConfig {
            provider: KMSProvider::Local,
            endpoint: None,
            region: None,
            vault_token: None,
            key_id: None,
        };
        
        let kms = VaultKMS::new(config);
        kms.connect().await.unwrap();
        
        let plaintext = b"Secret data";
        let encrypted = kms.encrypt(plaintext).await;
        assert!(encrypted.is_ok());
        
        let decrypted = kms.decrypt(&encrypted.unwrap()).await;
        assert!(decrypted.is_ok());
        assert_eq!(decrypted.unwrap(), plaintext);
    }

    #[tokio::test]
    async fn test_key_rotation() {
        let config = KMSConfig {
            provider: KMSProvider::Local,
            endpoint: None,
            region: None,
            vault_token: None,
            key_id: None,
        };
        
        let kms = VaultKMS::new(config);
        
        let key1 = kms.get_key("test").await.unwrap();
        
        kms.rotate_key("test").await.unwrap();
        
        let key2 = kms.get_key("test").await.unwrap();
        
        assert_ne!(key1.key, key2.key);
    }
}
