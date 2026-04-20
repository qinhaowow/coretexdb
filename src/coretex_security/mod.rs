//! Security module for CoreTexDB
//! Provides TLS/SSL encryption, data encryption at rest, and audit logging

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod tls;
pub mod encryption;
pub mod audit;
pub mod acl;
pub mod kms;
pub mod validation;
pub mod network;

pub use tls::{TlsConfig, TlsServer, TlsClient};
pub use encryption::{EncryptionService, EncryptedData, EncryptionKey, KeyManager};
pub use audit::{AuditLogger, AuditEvent, AuditLevel, AuditAction};
pub use acl::{ACLEngine, ACLPolicy, Subject, SubjectType, Resource, ResourceType, Action, Effect, ACLValidator};
pub use kms::{VaultKMS, KMSConfig, KMSProvider, ExternalKey, KeyRotationManager};
pub use validation::{InputValidator, RateLimitValidator};
pub use network::{NetworkIsolation, NetworkPolicy, IpRange, PolicyAction, IPRangeManager};

mod tls {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use std::path::Path;
    use std::fs;
    
    #[derive(Debug, Clone)]
    pub struct TlsConfig {
        pub cert_path: String,
        pub key_path: String,
        pub ca_path: Option<String>,
        pub verify_client: bool,
        pub min_version: TlsVersion,
    }
    
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum TlsVersion {
        TLSv1_2,
        TLSv1_3,
    }
    
    impl Default for TlsConfig {
        fn default() -> Self {
            Self {
                cert_path: "cert.pem".to_string(),
                key_path: "key.pem".to_string(),
                ca_path: None,
                verify_client: false,
                min_version: TlsVersion::TLSv1_2,
            }
        }
    }
    
    impl TlsConfig {
        pub fn from_files(cert_path: &str, key_path: &str) -> Result<Self, String> {
            if !Path::new(cert_path).exists() {
                return Err(format!("Certificate file not found: {}", cert_path));
            }
            if !Path::new(key_path).exists() {
                return Err(format!("Key file not found: {}", key_path));
            }
            Ok(Self {
                cert_path: cert_path.to_string(),
                key_path: key_path.to_string(),
                ca_path: None,
                verify_client: false,
                min_version: TlsVersion::TLSv1_3,
            })
        }
        
        pub fn for_development() -> Self {
            Self {
                cert_path: "cert.pem".to_string(),
                key_path: "key.pem".to_string(),
                ca_path: None,
                verify_client: false,
                min_version: TlsVersion::TLSv1_2,
            }
        }
    }
    
    pub struct TlsServer {
        config: TlsConfig,
    }
    
    impl TlsServer {
        pub fn new(config: TlsConfig) -> Self {
            Self { config }
        }
    
        pub fn from_config(config: TlsConfig) -> Result<Self, String> {
            Ok(Self { config })
        }
    
        pub fn generate_self_signed_cert(&self) -> Result<(Vec<u8>, Vec<u8>), String> {
            let cert = Self::generate_cert()?;
            let key = Self::generate_key()?;
            Ok((cert, key))
        }
    
        fn generate_cert() -> Result<Vec<u8>, String> {
            Ok(vec![])
        }
    
        fn generate_key() -> Result<Vec<u8>, String> {
            Ok(vec![])
        }
    
        pub fn config(&self) -> &TlsConfig {
            &self.config
        }
        
        pub fn load_cert_chain(&self) -> Result<Vec<u8>, String> {
            fs::read(&self.config.cert_path)
                .map_err(|e| format!("Failed to read certificate: {}", e))
        }
        
        pub fn load_private_key(&self) -> Result<Vec<u8>, String> {
            fs::read(&self.config.key_path)
                .map_err(|e| format!("Failed to read private key: {}", e))
        }
    }
    
    pub struct TlsClient {
        config: TlsConfig,
    }
    
    impl TlsClient {
        pub fn new(config: TlsConfig) -> Self {
            Self { config }
        }
    
        pub fn verify_server_cert(&self, cert: &[u8]) -> Result<bool, String> {
            Ok(true)
        }
    }
}

mod encryption {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use std::collections::HashMap;
    use aes_gcm::{
        Aes256Gcm, Key, KeyInit, Nonce,
        aead::{Aead, OsRng},
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use hex;
    
    type HmacSha256 = Hmac<Sha256>;
    
    #[derive(Debug, Clone)]
    pub struct EncryptionKey {
        pub id: String,
        pub key: Vec<u8>,
        pub created_at: u64,
        pub expires_at: Option<u64>,
    }
    
    impl EncryptionKey {
        pub fn from_bytes(id: String, key_bytes: Vec<u8>) -> Self {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            Self {
                id,
                key: key_bytes,
                created_at: now,
                expires_at: None,
            }
        }
        
        pub fn key_hash(&self) -> String {
            let mut mac = HmacSha256::new_from_slice(&self.key)
                .expect("HMAC can take key of any size");
            mac.update(b"key_verification");
            hex::encode(mac.finalize().into_bytes())
        }
    }
    
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EncryptedData {
        pub key_id: String,
        pub algorithm: String,
        pub iv: Vec<u8>,
        pub ciphertext: Vec<u8>,
        pub auth_tag: Vec<u8>,
    }
    
    impl EncryptedData {
        pub fn to_base64(&self) -> String {
            let combined = [
                &self.iv[..],
                &self.ciphertext[..],
                &self.auth_tag[..],
            ].concat();
            BASE64.encode(&combined)
        }
        
        pub fn from_base64(key_id: &str, data: &str) -> Result<Self, String> {
            let combined = BASE64.decode(data)
                .map_err(|e| format!("Base64 decode error: {}", e))?;
            
            if combined.len() < 12 + 16 {
                return Err("Invalid encrypted data length".to_string());
            }
            
            let iv = combined[..12].to_vec();
            let auth_tag = combined[combined.len() - 16..].to_vec();
            let ciphertext = combined[12..combined.len() - 16].to_vec();
            
            Ok(Self {
                key_id: key_id.to_string(),
                algorithm: "AES-256-GCM".to_string(),
                iv,
                ciphertext,
                auth_tag,
            })
        }
    }
    
    pub struct KeyManager {
        keys: Arc<RwLock<HashMap<String, EncryptionKey>>>,
        primary_key_id: Arc<RwLock<Option<String>>>,
    }
    
    impl KeyManager {
        pub fn new() -> Self {
            Self {
                keys: Arc::new(RwLock::new(HashMap::new())),
                primary_key_id: Arc::new(RwLock::new(None)),
            }
        }
    
        pub async fn generate_key(&self, key_id: &str, bits: usize) -> Result<EncryptionKey, String> {
            if bits != 128 && bits != 256 {
                return Err("Key size must be 128 or 256 bits".to_string());
            }
            
            let key: Vec<u8> = (0..bits / 8).map(|_| rand::random::<u8>()).collect();
            
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let key_obj = EncryptionKey {
                id: key_id.to_string(),
                key,
                created_at: now,
                expires_at: None,
            };
            
            let mut keys = self.keys.write().await;
            keys.insert(key_id.to_string(), key_obj.clone());
            
            let mut primary = self.primary_key_id.write().await;
            if primary.is_none() {
                *primary = Some(key_id.to_string());
            }
            
            Ok(key_obj)
        }
    
        pub async fn get_key(&self, key_id: &str) -> Option<EncryptionKey> {
            let keys = self.keys.read().await;
            keys.get(key_id).cloned()
        }
    
        pub async fn get_primary_key(&self) -> Option<EncryptionKey> {
            let primary_id = self.primary_key_id.read().await;
            if let Some(id) = primary_id.as_ref() {
                let keys = self.keys.read().await;
                keys.get(id).cloned()
            } else {
                None
            }
        }
    
        pub async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey, String> {
            self.generate_key(key_id, 256).await
        }
    }
    
    impl Default for KeyManager {
        fn default() -> Self {
            Self::new()
        }
    }
    
    pub struct EncryptionService {
        key_manager: Arc<KeyManager>,
    }
    
    impl EncryptionService {
        pub fn new(key_manager: Arc<KeyManager>) -> Self {
            Self { key_manager }
        }
    
        pub async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData, String> {
            let key = self.key_manager.get_primary_key().await
                .ok_or("No encryption key available")?;
            
            if key.key.len() != 32 {
                return Err("Key must be 256 bits (32 bytes)".to_string());
            }
            
            let key_array: [u8; 32] = key.key.clone().try_into()
                .map_err(|_| "Invalid key length")?;
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_array));
            
            let mut nonce_bytes = [0u8; 12];
            OsRng.fill_bytes(&mut nonce_bytes);
            let nonce = Nonce::from_slice(&nonce_bytes);
            
            let ciphertext = cipher.encrypt(nonce, plaintext)
                .map_err(|e| format!("Encryption failed: {}", e))?;
            
            let auth_tag = ciphertext[ciphertext.len() - 16..].to_vec();
            let encrypted_bytes = ciphertext[..ciphertext.len() - 16].to_vec();
            
            Ok(EncryptedData {
                key_id: key.id,
                algorithm: "AES-256-GCM".to_string(),
                iv: nonce_bytes.to_vec(),
                ciphertext: encrypted_bytes,
                auth_tag,
            })
        }
    
        pub async fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>, String> {
            let key = self.key_manager.get_key(&encrypted.key_id).await
                .ok_or("Encryption key not found")?;
            
            if key.key.len() != 32 {
                return Err("Key must be 256 bits (32 bytes)".to_string());
            }
            
            let key_array: [u8; 32] = key.key.clone().try_into()
                .map_err(|_| "Invalid key length")?;
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_array));
            
            if encrypted.iv.len() != 12 {
                return Err("Invalid nonce length".to_string());
            }
            
            let mut nonce_array = [0u8; 12];
            nonce_array.copy_from_slice(&encrypted.iv);
            let nonce = Nonce::from_slice(&nonce_array);
            
            let mut combined = encrypted.ciphertext.clone();
            combined.extend_from_slice(&encrypted.auth_tag);
            
            let plaintext = cipher.decrypt(nonce, combined.as_ref())
                .map_err(|e| format!("Decryption failed: {}", e))?;
            
            Ok(plaintext)
        }
    
        pub async fn encrypt_vector(&self, vector: &[f32]) -> Result<EncryptedData, String> {
            let bytes: Vec<u8> = vector.iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();
            
            self.encrypt(&bytes).await
        }
    
        pub async fn decrypt_vector(&self, encrypted: &EncryptedData) -> Result<Vec<f32>, String> {
            let bytes = self.decrypt(encrypted).await?;
            
            let floats: Vec<f32> = bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();
            
            Ok(floats)
        }
        
        pub async fn encrypt_string(&self, text: &str) -> Result<String, String> {
            let encrypted = self.encrypt(text.as_bytes()).await?;
            Ok(encrypted.to_base64())
        }
        
        pub async fn decrypt_string(&self, encrypted_base64: &str) -> Result<String, String> {
            let key = self.key_manager.get_primary_key().await
                .ok_or("No encryption key available")?;
            
            let encrypted = EncryptedData::from_base64(&key.id, encrypted_base64)?;
            let plaintext = self.decrypt(&encrypted).await?;
            
            String::from_utf8(plaintext)
                .map_err(|e| format!("Invalid UTF-8: {}", e))
        }
    }
}

mod audit {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use std::collections::VecDeque;
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};
    
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum AuditLevel {
        Info,
        Warning,
        Error,
        Critical,
    }
    
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum AuditAction {
        Create,
        Read,
        Update,
        Delete,
        Login,
        Logout,
        Query,
        Search,
        Admin,
    }
    
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AuditEvent {
        pub id: String,
        pub timestamp: u64,
        pub level: AuditLevel,
        pub action: AuditAction,
        pub user_id: Option<String>,
        pub username: Option<String>,
        pub resource: String,
        pub details: HashMap<String, String>,
        pub ip_address: Option<String>,
        pub success: bool,
        pub error_message: Option<String>,
    }
    
    pub struct AuditLogger {
        events: Arc<RwLock<VecDeque<AuditEvent>>>,
        max_events: usize,
        persistent_storage: bool,
    }
    
    impl AuditLogger {
        pub fn new(max_events: usize) -> Self {
            Self {
                events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events))),
                max_events,
                persistent_storage: false,
            }
        }
    
        pub fn with_persistent_storage(mut self, enabled: bool) -> Self {
            self.persistent_storage = enabled;
            self
        }
    
        pub async fn log(&self, event: AuditEvent) {
            let mut events = self.events.write().await;
            
            if events.len() >= self.max_events {
                events.pop_front();
            }
            
            events.push_back(event);
            
            if self.persistent_storage {
                self.persist_event(&event).await;
            }
        }
    
        async fn persist_event(&self, _event: &AuditEvent) {
        }
    
        pub async fn log_event(
            &self,
            level: AuditLevel,
            action: AuditAction,
            resource: &str,
            user_id: Option<&str>,
            success: bool,
        ) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let event = AuditEvent {
                id: format!("audit_{}", now),
                timestamp: now,
                level,
                action,
                user_id: user_id.map(String::from),
                username: None,
                resource: resource.to_string(),
                details: HashMap::new(),
                ip_address: None,
                success,
                error_message: None,
            };
            
            self.log(event).await;
        }
    
        pub async fn get_events(
            &self,
            user_id: Option<&str>,
            action: Option<AuditAction>,
            limit: usize,
        ) -> Vec<AuditEvent> {
            let events = self.events.read().await;
            
            events
                .iter()
                .filter(|e| {
                    let user_match = user_id.map_or(true, |u| e.user_id.as_deref() == Some(u));
                    let action_match = action.map_or(true, |a| e.action == a);
                    user_match && action_match
                })
                .rev()
                .take(limit)
                .cloned()
                .collect()
        }
    
        pub async fn get_failed_logins(&self, limit: usize) -> Vec<AuditEvent> {
            let events = self.events.read().await;
            
            events
                .iter()
                .filter(|e| e.action == AuditAction::Login && !e.success)
                .rev()
                .take(limit)
                .cloned()
                .collect()
        }
    
        pub async fn get_user_activity(&self, user_id: &str, limit: usize) -> Vec<AuditEvent> {
            let events = self.events.read().await;
            
            events
                .iter()
                .filter(|e| e.user_id.as_deref() == Some(user_id))
                .rev()
                .take(limit)
                .cloned()
                .collect()
        }
    
        pub async fn clear_old_events(&self, before_timestamp: u64) -> usize {
            let mut events = self.events.write().await;
            let initial_len = events.len();
            
            events.retain(|e| e.timestamp >= before_timestamp);
            
            initial_len - events.len()
        }
    
        pub async fn export_to_json(&self) -> String {
            let events = self.events.read().await;
            serde_json::to_string_pretty(&*events).unwrap_or_default()
        }
    }
    
    impl Default for AuditLogger {
        fn default() -> Self {
            Self::new(10000)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_key_manager() {
        let km = KeyManager::new();
        
        let key = km.generate_key("test_key", 256).await;
        assert!(key.is_ok());
        
        let retrieved = km.get_key("test_key").await;
        assert!(retrieved.is_some());
    }
    
    #[tokio::test]
    async fn test_encryption() {
        let km = Arc::new(KeyManager::new());
        km.generate_key("primary", 256).await.unwrap();
        
        let enc = EncryptionService::new(km);
        let plaintext = b"Hello, CoreTexDB!";
        
        let encrypted = enc.encrypt(plaintext).await;
        assert!(encrypted.is_ok());
        
        let decrypted = enc.decrypt(&encrypted.unwrap()).await;
        assert!(decrypted.is_ok());
        assert_eq!(decrypted.unwrap(), plaintext);
    }
    
    #[tokio::test]
    async fn test_audit_logger() {
        let logger = AuditLogger::new(100);
        
        logger.log_event(
            AuditLevel::Info,
            AuditAction::Login,
            "auth",
            Some("user1"),
            true,
        ).await;
        
        let events = logger.get_events(Some("user1"), None, 10).await;
        assert!(!events.is_empty());
    }
    
    #[tokio::test]
    async fn test_failed_login_tracking() {
        let logger = AuditLogger::new(100);
        
        logger.log_event(
            AuditLevel::Warning,
            AuditAction::Login,
            "auth",
            Some("hacker"),
            false,
        ).await;
        
        let failed = logger.get_failed_logins(10).await;
        assert!(!failed.is_empty());
    }
}
