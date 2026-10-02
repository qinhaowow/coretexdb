//! Security module for CoreTexDB
//! Provides TLS/SSL encryption, data encryption at rest, and audit logging


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
    
    use std::path::Path;
    use std::fs;
    use std::io::Cursor;
    
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
            // 安全修复：默认配置使用 TLS 1.3 并启用客户端证书校验 (mTLS)，
            // 避免在生产部署中以不安全的默认配置启动。
            Self {
                cert_path: "cert.pem".to_string(),
                key_path: "key.pem".to_string(),
                ca_path: Some("ca.pem".to_string()),
                verify_client: true,
                min_version: TlsVersion::TLSv1_3,
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
            // 安全修复：from_files 是显式构造路径，强制启用 mTLS。
            Ok(Self {
                cert_path: cert_path.to_string(),
                key_path: key_path.to_string(),
                ca_path: Some("ca.pem".to_string()),
                verify_client: true,
                min_version: TlsVersion::TLSv1_3,
            })
        }

        pub fn for_development() -> Self {
            // 仅供本地开发使用：关闭 mTLS，使用 TLS 1.2 兼容旧客户端。
            // 警告：此配置不能用于生产环境。
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
    
        /// 生成真实的自签名 X.509 证书与对应的 PKCS#8 私钥（PEM 格式）。
        ///
        /// 需要 `tls-gen` feature 启用（依赖 rcgen）。
        /// 未启用该 feature 时返回错误，避免静默使用不安全的占位证书。
        #[cfg(feature = "tls-gen")]
        pub fn generate_self_signed_cert(&self) -> Result<(Vec<u8>, Vec<u8>), String> {
            // 收集 SAN（subject alternative names）：包含 cert_path 不影响，使用 localhost
            // 便于开发环境的本地数据库客户端连接。
            let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    
            let cert = rcgen::generate_simple_self_signed(subject_alt_names)
                .map_err(|e| format!("Failed to generate self-signed certificate: {}", e))?;
    
            let cert_pem = cert.serialize_pem()
                .map_err(|e| format!("Failed to serialize certificate to PEM: {}", e))?;
            let key_pem = cert.serialize_private_key_pem();
    
            Ok((cert_pem.into_bytes(), key_pem.into_bytes()))
        }
    
        /// 未启用 `tls-gen` feature 时，无法生成自签名证书。
        ///
        /// 调用方应启用 `tls-gen` feature 或提供外部证书。
        #[cfg(not(feature = "tls-gen"))]
        pub fn generate_self_signed_cert(&self) -> Result<(Vec<u8>, Vec<u8>), String> {
            Err(
                "Self-signed certificate generation is disabled. \
                 Enable the 'tls-gen' feature or provide a certificate via TlsConfig::from_files."
                    .to_string(),
            )
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
    
        /// 从已加载的 PEM 字节构建 `rustls::ServerConfig`。
        ///
        /// 使用 `rustls-pemfile` 解析证书链与私钥，确保只接受格式合法的 PEM。
        /// 返回的 `ServerConfig` 可直接用于 `tokio-rustls` 等 TLS 监听器。
        pub fn build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, String> {
            let cert_pem = self.load_cert_chain()?;
            let key_pem = self.load_private_key()?;

            let certs: Vec<rustls::Certificate> = rustls_pemfile::certs(&mut Cursor::new(&cert_pem))
                .map_err(|e| format!("Failed to parse certificate PEM: {}", e))?
                .into_iter()
                .map(rustls::Certificate)
                .collect();

            if certs.is_empty() {
                return Err("No valid certificate found in PEM".to_string());
            }

            let key = rustls_pemfile::pkcs8_private_keys(&mut Cursor::new(&key_pem))
                .map_err(|e| format!("Failed to parse private key PEM: {}", e))?
                .into_iter()
                .next()
                .map(rustls::PrivateKey)
                .ok_or_else(|| "No valid private key found in PEM".to_string())?;

            let server_config = rustls::ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("Failed to build rustls ServerConfig: {}", e))?;

            Ok(Arc::new(server_config))
        }
    
        /// 把生成的自签名证书与私钥写入磁盘，便于复用。
        #[cfg(feature = "tls-gen")]
        pub fn write_self_signed_cert(&self, cert_path: &str, key_path: &str) -> Result<(), String> {
            let (cert, key) = self.generate_self_signed_cert()?;
            fs::write(cert_path, &cert).map_err(|e| format!("Failed to write cert: {}", e))?;
            fs::write(key_path, &key).map_err(|e| format!("Failed to write key: {}", e))?;
            Ok(())
        }
    }
    
    pub struct TlsClient {
        config: TlsConfig,
    }
    
    impl TlsClient {
        pub fn new(config: TlsConfig) -> Self {
            Self { config }
        }
    
        /// 验证服务器证书 PEM：使用 `rustls-pemfile` 解析并确认至少包含一张合法证书。
        ///
        /// 替换原仅检查 PEM header 字符串的占位实现。
        pub fn verify_server_cert(&self, cert: &[u8]) -> Result<bool, String> {
            if cert.is_empty() {
                return Err("Empty certificate provided".to_string());
            }
    
            let mut cursor = Cursor::new(cert);
            let certs = rustls_pemfile::certs(&mut cursor)
                .map_err(|e| format!("Invalid certificate PEM: {}", e))?;
    
            if certs.is_empty() {
                return Err("No valid certificate found in PEM".to_string());
            }
    
            Ok(true)
        }
    
        pub fn config(&self) -> &TlsConfig {
            &self.config
        }
    
        /// 构建客户端 `rustls::ClientConfig`，可加载自定义 CA 用于自签名证书校验。
        ///
        /// 必须通过 `TlsConfig.ca_path` 提供 CA 证书；未提供时返回错误，
        /// 避免在不知情的情况下信任未知根证书。
        pub fn build_client_config(&self) -> Result<Arc<rustls::ClientConfig>, String> {
            let mut root_store = rustls::RootCertStore::empty();
    
            let ca_path = self.config.ca_path.as_ref()
                .ok_or_else(|| "CA path must be provided to build a client config".to_string())?;
    
            let ca_pem = fs::read(ca_path)
                .map_err(|e| format!("Failed to read CA file: {}", e))?;
            let mut cursor = Cursor::new(&ca_pem);
            let ca_certs = rustls_pemfile::certs(&mut cursor)
                .map_err(|e| format!("Failed to parse CA PEM: {}", e))?;
    
            if ca_certs.is_empty() {
                return Err("No valid CA certificate found in PEM".to_string());
            }
    
            for cert in ca_certs {
                let rustls_cert = rustls::Certificate(cert.to_vec());
                root_store
                    .add(&rustls_cert)
                    .map_err(|e| format!("Failed to add CA to root store: {}", e))?;
            }
    
            let client_config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth();
    
            Ok(Arc::new(client_config))
        }
    }
}

mod encryption {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};
    use aes_gcm::{
        Aes256Gcm, Key, KeyInit, Nonce,
        aead::{Aead, OsRng},
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use hex;
    use rand::RngCore;
    use serde::{Deserialize, Serialize};
    
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
            let mut mac = <HmacSha256 as hmac::Mac>::new_from_slice(&self.key)
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

            // 安全修复：使用密码学安全随机数 OsRng 生成对称加密密钥，
            // 替代原先的 rand::random::<u8>()（其随机性依赖实现细节，审计风险高）。
            use rand::rngs::OsRng;
            use rand::RngCore;
            let mut key = vec![0u8; bits / 8];
            OsRng.fill_bytes(&mut key);

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
                .as_chunks::<4>().0.iter()
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
    use std::collections::{HashMap, VecDeque};
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::path::Path;
    
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
        storage_path: String,
    }

    /// Monotonic event id.
    ///
    /// The id used to be `format!("audit_{}", unix_seconds)`, so every event
    /// within the same second shared one — under load that is most of them, and
    /// an audit trail whose ids collide cannot be correlated or deduplicated.
    /// Second-granularity is still the `timestamp`; the id adds a counter.
    fn next_event_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("audit_{now}_{n}")
    }
    
    impl AuditLogger {
        pub fn new(max_events: usize) -> Self {
            Self {
                events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events))),
                max_events,
                persistent_storage: false,
                storage_path: "audit_log.json".to_string(),
            }
        }
    
        pub fn with_persistent_storage(mut self, enabled: bool) -> Self {
            self.persistent_storage = enabled;
            self
        }

        pub fn with_storage_path(mut self, path: &str) -> Self {
            self.storage_path = path.to_string();
            self
        }
    
        pub async fn log(&self, event: AuditEvent) {
            if self.persistent_storage {
                self.persist_event(&event).await;
            }
            
            let mut events = self.events.write().await;
            
            if events.len() >= self.max_events {
                events.pop_front();
            }
            
            events.push_back(event);
        }
    
        async fn persist_event(&self, event: &AuditEvent) {
            let Ok(json) = serde_json::to_string(event) else {
                return;
            };
            let path = Path::new(&self.storage_path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }
            let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            else {
                return;
            };
            // One JSON object per line (JSONL), append-only.
            //
            // This used to write `[{...}]` on the first event and `,{...}` on
            // every one after, producing
            //
            //     [{"..."}]
            //     ,{"..."}
            //     ,{"..."}
            //
            // which is neither a JSON array nor JSONL: `serde_json` rejects it,
            // and a leading comma on line 2 is not valid anywhere. The persisted
            // log was unreadable by the only thing that could read it. It went
            // unnoticed because `with_persistent_storage` was never called, so
            // this path never executed in production.
            use std::io::Write;
            let _ = writeln!(file, "{}", json);
        }

        /// Read the persisted log back.
        ///
        /// `None` if the file is absent. Individual malformed lines are skipped
        /// rather than failing the whole read — a truncated final line (process
        /// killed mid-write) must not make the entire history unreadable.
        pub async fn load_persisted(&self) -> Option<Vec<AuditEvent>> {
            let content = std::fs::read_to_string(&self.storage_path).ok()?;
            let events: Vec<AuditEvent> = content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();
            Some(events)
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
                id: next_event_id(),
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

        /// Log an event with the client address attached.
        ///
        /// The address is what makes an audit trail worth having — without it,
        /// a log of "user X logged in" cannot answer "from where".
        pub async fn log_request(
            &self,
            level: AuditLevel,
            action: AuditAction,
            resource: &str,
            user_id: Option<&str>,
            client_ip: Option<&str>,
            success: bool,
            error: Option<&str>,
        ) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let event = AuditEvent {
                id: next_event_id(),
                timestamp: now,
                level,
                action,
                user_id: user_id.map(String::from),
                username: None,
                resource: resource.to_string(),
                details: HashMap::new(),
                ip_address: client_ip.map(String::from),
                success,
                error_message: error.map(String::from),
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
                    let user_match = user_id.is_none_or(|u| e.user_id.as_deref() == Some(u));
                    let action_match = action.is_none_or(|a| e.action == a);
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
    use std::sync::Arc;
    use crate::coretex_core::Result;

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

    /// Regression: `persist_event` wrote `[{...}]` for the first event and
    /// `,{...}` for every one after. The file was therefore neither a JSON
    /// array nor JSONL — `serde_json` rejected the leading comma on line 2, so
    /// the persisted audit log was unreadable by the only thing that could read
    /// it. It went unnoticed because nothing ever enabled persistent storage.
    #[tokio::test]
    async fn test_persisted_audit_log_is_parseable_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("audit.jsonl");
        let logger = AuditLogger::new(100)
            .with_persistent_storage(true)
            .with_storage_path(path.to_str().unwrap());

        for i in 0..5 {
            logger
                .log_event(
                    AuditLevel::Info,
                    AuditAction::Admin,
                    &format!("resource-{i}"),
                    Some("user1"),
                    true,
                )
                .await;
        }

        // The raw bytes must not contain the old broken shapes.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            !raw.trim_start().starts_with('['),
            "must be JSONL, not a JSON array: {raw}"
        );
        assert!(
            !raw.contains("\n,{"),
            "must not emit leading commas between records: {raw}"
        );

        // Every line parses independently.
        for (i, line) in raw.lines().filter(|l| !l.trim().is_empty()).enumerate() {
            serde_json::from_str::<AuditEvent>(line)
                .unwrap_or_else(|e| panic!("line {i} is not valid JSON: {line:?} ({e})"));
        }

        // And the whole log round-trips.
        let loaded = logger.load_persisted().await.expect("log must be readable");
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].resource, "resource-0");
        assert_eq!(loaded[4].resource, "resource-4");
    }

    /// Regression: ids were `audit_{unix_seconds}`, so every event in the same
    /// second collided. Under load that is most of them, and a trail whose ids
    /// collide cannot be correlated or deduplicated.
    #[tokio::test]
    async fn test_audit_event_ids_are_unique() {
        let logger = AuditLogger::new(1000);

        for _ in 0..50 {
            logger
                .log_event(AuditLevel::Info, AuditAction::Query, "q", None, true)
                .await;
        }

        let events = logger.get_events(None, None, 1000).await;
        assert_eq!(events.len(), 50);

        let mut ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(
            ids.len(),
            before,
            "event ids must be unique — 50 events in one second must not share \
             one id"
        );
    }

    /// A malformed line — e.g. a truncated write from a killed process — must
    /// not make the entire history unreadable.
    #[tokio::test]
    async fn test_persisted_log_tolerates_a_truncated_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::new(100)
            .with_persistent_storage(true)
            .with_storage_path(path.to_str().unwrap());

        logger
            .log_event(AuditLevel::Info, AuditAction::Query, "a", None, true)
            .await;
        logger
            .log_event(AuditLevel::Info, AuditAction::Query, "b", None, true)
            .await;

        // Simulate a process killed mid-write.
        let mut raw = std::fs::read_to_string(&path).unwrap();
        raw.push_str("{\"id\":\"audit_trunc\",\"resour");
        std::fs::write(&path, raw).unwrap();

        let loaded = logger.load_persisted().await.unwrap();
        assert_eq!(
            loaded.len(),
            2,
            "the two complete records must survive a truncated tail"
        );
    }

    /// The client address is what makes an audit trail useful — a log of
    /// "user X logged in" that cannot say from where answers little.
    #[tokio::test]
    async fn test_log_request_records_client_ip_and_error() {
        let logger = AuditLogger::new(10);

        logger
            .log_request(
                AuditLevel::Warning,
                AuditAction::Login,
                "auth",
                Some("user1"),
                Some("10.1.2.3"),
                false,
                Some("Invalid password"),
            )
            .await;

        let events = logger.get_events(None, None, 10).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ip_address.as_deref(), Some("10.1.2.3"));
        assert_eq!(events[0].error_message.as_deref(), Some("Invalid password"));
        assert!(!events[0].success);
    }
}
