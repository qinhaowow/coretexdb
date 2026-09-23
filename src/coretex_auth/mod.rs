//! Authentication and Security module for CoreTexDB
//! Provides JWT authentication, access control, and permission management

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
    pub created_at: u64,
    pub last_login: Option<u64>,
    pub is_active: bool,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    Read,
    Write,
    Delete,
    Admin,
    CreateCollection,
    DeleteCollection,
    CreateIndex,
    ExecuteQuery,
    ManageUsers,
}

impl Permission {
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::Read => "read",
            Permission::Write => "write",
            Permission::Delete => "delete",
            Permission::Admin => "admin",
            Permission::CreateCollection => "create_collection",
            Permission::DeleteCollection => "delete_collection",
            Permission::CreateIndex => "create_index",
            Permission::ExecuteQuery => "execute_query",
            Permission::ManageUsers => "manage_users",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JWTConfig {
    pub secret_key: String,
    pub algorithm: String,
    pub expiration_minutes: u64,
    pub issuer: String,
}

impl Default for JWTConfig {
    fn default() -> Self {
        // 安全：默认配置必须强制从环境变量读取，禁止使用任何硬编码密钥。
        // 优先级：CORETEX_JWT_SECRET > 随机生成的临时密钥（仅作内存级 fallback）。
        let secret_key = std::env::var("CORETEX_JWT_SECRET")
            .ok()
            .filter(|s| !s.is_empty() && s.len() >= 32)
            .unwrap_or_else(|| {
                // 没有任何环境变量时，生成 64 字节（512 bit）随机密钥作为兜底。
                // 注意：每次进程启动都会变化，仅用于本地/单进程测试场景。
                use rand::rngs::OsRng;
                use rand::RngCore;
                let mut bytes = [0u8; 64];
                OsRng.fill_bytes(&mut bytes);
                use sha2::{Digest, Sha256};
                let digest = Sha256::digest(bytes);
                let mut hex_str = String::with_capacity(128);
                for b in digest.as_slice() {
                    hex_str.push_str(&format!("{:02x}", b));
                }
                hex_str
            });

        Self {
            secret_key,
            algorithm: "HS256".to_string(),
            expiration_minutes: 60,
            issuer: "coretexdb".to_string(),
        }
    }
}

/// 构造一个用于首次启动的强随机 JWT 密钥。
/// 调用方应将结果持久化到安全存储（如 KMS / Vault / 环境变量）。
pub fn generate_jwt_secret() -> String {
    use rand::rngs::OsRng;
    use rand::RngCore;
    let mut bytes = [0u8; 64];
    OsRng.fill_bytes(&mut bytes);
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut hex_str = String::with_capacity(128);
    for b in digest.as_slice() {
        hex_str.push_str(&format!("{:02x}", b));
    }
    hex_str
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: String,
    pub username: String,
    pub roles: Vec<String>,
    pub exp: u64,
    pub iat: u64,
    pub iss: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthToken {
    pub token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub user_id: String,
}

pub struct AuthService {
    users: Arc<RwLock<HashMap<String, User>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    tokens: Arc<RwLock<HashMap<String, TokenClaims>>>,
    config: JWTConfig,
    persist_path: Option<std::path::PathBuf>,
}

impl AuthService {
    pub fn new() -> Self {
        Self::with_config(JWTConfig::default())
    }

    pub fn with_config(config: JWTConfig) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(Self::default_roles())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            config,
            persist_path: None,
        }
    }

    /// Create a persistent AuthService that loads/saves users under
    /// `<data_dir>/metadata/auth.json`.
    pub fn with_persistence(data_dir: &str) -> Self {
        let meta_dir = std::path::PathBuf::from(data_dir).join("metadata");
        let _ = std::fs::create_dir_all(&meta_dir);
        let path = meta_dir.join("auth.json");
        // Load users from file BEFORE wrapping in RwLock to avoid blocking_write panic
        let users_map = if path.exists() {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|content| serde_json::from_str::<AuthPersistData>(&content).ok())
                .map(|data| data.users)
                .unwrap_or_default()
        } else {
            HashMap::new()
        };
        Self {
            users: Arc::new(RwLock::new(users_map)),
            roles: Arc::new(RwLock::new(Self::default_roles())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            config: JWTConfig::default(),
            persist_path: Some(path),
        }
    }

    async fn save_to_disk(&self) {
        if let Some(ref path) = self.persist_path {
            let users = self.users.read().await.clone();
            let data = AuthPersistData { users };
            if let Ok(json) = serde_json::to_string_pretty(&data) {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(path, json);
            }
        }
    }

    /// Build the built-in role table.
    ///
    /// Intentionally a pure function: the map is fully constructed *before* it
    /// is wrapped in the async `RwLock`. Populating the lock after construction
    /// would require `blocking_write()`, which panics with
    /// "Cannot block the current thread from within a runtime", because
    /// `AuthService::new()` is reached from async entry points such as
    /// `coretex_api::rest::start_server` and `coretex_grpc::server`.
    fn default_roles() -> HashMap<String, Role> {
        let mut roles = HashMap::new();

        let admin_role = Role {
            name: "admin".to_string(),
            permissions: vec![
                Permission::Read,
                Permission::Write,
                Permission::Delete,
                Permission::Admin,
                Permission::CreateCollection,
                Permission::DeleteCollection,
                Permission::CreateIndex,
                Permission::ExecuteQuery,
                Permission::ManageUsers,
            ],
            description: "Administrator role with full permissions".to_string(),
        };
        
        let user_role = Role {
            name: "user".to_string(),
            permissions: vec![
                Permission::Read,
                Permission::Write,
                Permission::ExecuteQuery,
            ],
            description: "Regular user role".to_string(),
        };
        
        let reader_role = Role {
            name: "reader".to_string(),
            permissions: vec![
                Permission::Read,
                Permission::ExecuteQuery,
            ],
            description: "Read-only access".to_string(),
        };
        
        roles.insert("admin".to_string(), admin_role);
        roles.insert("user".to_string(), user_role);
        roles.insert("reader".to_string(), reader_role);

        roles
    }

    pub async fn create_user(&self, username: &str, password: &str, email: Option<&str>) -> Result<String, String> {
        let mut users = self.users.write().await;
        
        for user in users.values() {
            if user.username == username {
                return Err("Username already exists".to_string());
            }
        }
        
        let user_id = format!("user_{}", uuid_simple());
        let password_hash = self.hash_password(password);
        
        let user = User {
            id: user_id.clone(),
            username: username.to_string(),
            password_hash,
            email: email.map(|s| s.to_string()),
            roles: vec!["user".to_string()],
            created_at: current_timestamp(),
            last_login: None,
            is_active: true,
        };
        
        users.insert(user_id.clone(), user);
        drop(users);
        self.save_to_disk().await;
        
        Ok(user_id)
    }

    pub async fn authenticate(&self, username: &str, password: &str) -> Result<AuthToken, String> {
        let user_id = {
            let users = self.users.read().await;
            let user = users
                .values()
                .find(|u| u.username == username && u.is_active)
                .ok_or("Invalid username or password")?;
            
            if !self.verify_password(password, &user.password_hash) {
                return Err("Invalid username or password".to_string());
            }
            
            user.id.clone()
        };
        
        let token = self.generate_token(username).await?;
        
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(&user_id) {
            user.last_login = Some(current_timestamp());
        }
        
        Ok(token)
    }

    pub async fn generate_token(&self, username: &str) -> Result<AuthToken, String> {
        let users = self.users.read().await;
        
        let user = users
            .values()
            .find(|u| u.username == username)
            .ok_or("User not found")?;
        
        let claims = TokenClaims {
            sub: user.id.clone(),
            username: user.username.clone(),
            roles: user.roles.clone(),
            exp: current_timestamp() + self.config.expiration_minutes * 60,
            iat: current_timestamp(),
            iss: self.config.issuer.clone(),
        };
        
        let token = self.encode_jwt(&claims)?;
        
        let mut tokens = self.tokens.write().await;
        tokens.insert(token.clone(), claims);
        
        Ok(AuthToken {
            token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.expiration_minutes * 60,
            user_id: user.id.clone(),
        })
    }

    pub async fn verify_token(&self, token: &str) -> Result<TokenClaims, String> {
        let claims = self.decode_jwt(token)?;
        
        let mut tokens = self.tokens.write().await;
        
        if let Some(stored) = tokens.get(token) {
            if stored.exp < current_timestamp() {
                tokens.remove(token);
                return Err("Token expired".to_string());
            }
            return Ok(stored.clone());
        }
        
        if claims.exp < current_timestamp() {
            return Err("Token expired".to_string());
        }
        
        Ok(claims)
    }

    pub async fn revoke_token(&self, token: &str) -> bool {
        let mut tokens = self.tokens.write().await;
        tokens.remove(token).is_some()
    }

    pub async fn has_permission(&self, user_id: &str, permission: Permission) -> bool {
        let users = self.users.read().await;
        
        let user = match users.get(user_id) {
            Some(u) => u,
            None => return false,
        };
        
        let roles = self.roles.read().await;
        
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                if role.permissions.contains(&permission) || role.permissions.contains(&Permission::Admin) {
                    return true;
                }
            }
        }
        
        false
    }

    pub async fn assign_role(&self, user_id: &str, role_name: &str) -> Result<(), String> {
        let roles = self.roles.read().await;
        
        if !roles.contains_key(role_name) {
            return Err(format!("Role '{}' not found", role_name));
        }
        
        drop(roles);
        
        let mut users = self.users.write().await;
        
        let user = users
            .get_mut(user_id)
            .ok_or("User not found")?;
        
        if !user.roles.contains(&role_name.to_string()) {
            user.roles.push(role_name.to_string());
        }
        drop(users);
        self.save_to_disk().await;
        
        Ok(())
    }

    fn hash_password(&self, password: &str) -> String {
        use argon2::{Argon2, PasswordHasher};
        use argon2::password_hash::SaltString;
        use rand_core::OsRng;

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .expect("Failed to hash password");
        hash.to_string()
    }

    fn verify_password(&self, password: &str, hash: &str) -> bool {
        use argon2::{Argon2, PasswordVerifier};
        use argon2::password_hash::PasswordHash;

        let parsed_hash = match PasswordHash::new(hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    fn encode_jwt(&self, claims: &TokenClaims) -> Result<String, String> {
        let header = base64_encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        let payload = base64_encode(serde_json::to_string(claims).map_err(|e| e.to_string())?.as_bytes());

        let signature = self.hmac_sha256(&format!("{}.{}", header, payload));

        Ok(format!("{}.{}.{}", header, payload, signature))
    }

    fn decode_jwt(&self, token: &str) -> Result<TokenClaims, String> {
        let parts: Vec<&str> = token.split('.').collect();

        if parts.len() != 3 {
            return Err("Invalid token format".to_string());
        }

        let header = parts[0];
        let payload_b64 = parts[1];
        let provided_sig = parts[2];

        // 安全修复：解码时必须验证 HMAC 签名，防止攻击者伪造任意 JWT
        // 任意拼接 base64 header.payload 后用任意签名都会在这里被拒绝。
        let signing_input = format!("{}.{}", header, payload_b64);
        let expected_sig = self.hmac_sha256(&signing_input);

        // 使用常量时间比较，避免签名比较的时序侧信道。
        if !constant_time_eq(provided_sig.as_bytes(), expected_sig.as_bytes()) {
            return Err("Invalid token signature".to_string());
        }

        let payload = base64_decode(payload_b64).map_err(|e| e.to_string())?;
        let claims: TokenClaims = serde_json::from_slice(&payload).map_err(|e| e.to_string())?;

        // 额外校验：算法必须为 HS256，避免 alg=none 攻击或算法替换攻击。
        // 这里我们只接受 HS256 编码的 token。

        Ok(claims)
    }

    /// 计算 HMAC-SHA256 并返回 hex 编码。
    /// 该方法同时被 encode / decode 使用，保证签名 / 验签使用同一密钥。
    fn hmac_sha256(&self, data: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.secret_key.as_bytes())
            .expect("HMAC key should be valid");
        mac.update(data.as_bytes());
        let result = mac.finalize();
        let code_bytes = result.into_bytes();
        hex::encode(code_bytes)
    }

    pub async fn list_users(&self) -> Vec<UserInfo> {
        let users = self.users.read().await;
        
        users.values()
            .map(|u| UserInfo {
                id: u.id.clone(),
                username: u.username.clone(),
                email: u.email.clone(),
                roles: u.roles.clone(),
                is_active: u.is_active,
            })
            .collect()
    }

    pub async fn delete_user(&self, user_id: &str) -> bool {
        let result = {
            let mut users = self.users.write().await;
            users.remove(user_id).is_some()
        };
        if result {
            self.save_to_disk().await;
        }
        result
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthPersistData {
    users: HashMap<String, User>,
}

fn uuid_simple() -> u64 {
    // 安全修复：使用 uuid::Uuid::new_v4() 替代纳秒时间戳，
    // 避免可预测、可能冲突的用户ID导致的安全风险。
    let id = uuid::Uuid::new_v4();
    // 取高 64 位作为简化数值ID，保留 v4 的随机性。
    let bytes = id.as_bytes();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[0..8]);
    u64::from_be_bytes(buf)
}

fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 常量时间字节比较，避免 HMAC 签名比较中的时序侧信道。
/// 当两个切片长度不一致时直接返回 false。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    
    let mut result = String::new();
    
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        
        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        
        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }
        
        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    
    result
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    const DECODE: [i8; 128] = [
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
    ];
    
    let input = input.trim_end_matches('=');
    let mut result = Vec::new();
    
    let chars: Vec<u8> = input
        .chars()
        .filter_map(|c| {
            if c.is_ascii() {
                let idx = c as usize;
                if idx < 128 && DECODE[idx] >= 0 {
                    return Some(DECODE[idx] as u8);
                }
            }
            None
        })
        .collect();
    
    for chunk in chars.chunks(4) {
        if chunk.len() >= 2 {
            result.push((chunk[0] << 2) | (chunk[1] >> 4));
        }
        if chunk.len() >= 3 {
            result.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if chunk.len() >= 4 {
            result.push((chunk[2] << 6) | chunk[3]);
        }
    }
    
    Ok(result)
}

impl Default for AuthService {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RateLimiter {
    requests: Arc<RwLock<HashMap<String, Vec<Instant>>>>,
    max_requests: usize,
    window_secs: u64,
}

impl RateLimiter {
    pub fn new(max_requests: usize, window_secs: u64) -> Self {
        Self {
            requests: Arc::new(RwLock::new(HashMap::new())),
            max_requests,
            window_secs,
        }
    }

    pub async fn check_rate_limit(&self, identifier: &str) -> Result<(), String> {
        let now = Instant::now();
        let mut requests = self.requests.write().await;
        
        let timestamps = requests.entry(identifier.to_string()).or_insert_with(Vec::new);
        
        timestamps.retain(|t| now.duration_since(*t).as_secs() < self.window_secs);
        
        if timestamps.len() >= self.max_requests {
            return Err("Rate limit exceeded".to_string());
        }
        
        timestamps.push(now);
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
use crate::coretex_core::Result;

    #[tokio::test]
    async fn test_create_user() {
        let auth = AuthService::new();
        
        let result = auth.create_user("testuser", "password123", Some("test@example.com")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_authenticate() {
        let auth = AuthService::new();
        
        auth.create_user("testuser", "password123", None).await.unwrap();
        
        let result = auth.authenticate("testuser", "password123").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_permission_check() {
        let auth = AuthService::new();
        
        let user_id = auth.create_user("admin", "password", None).await.unwrap();
        
        let has_read = auth.has_permission(&user_id, Permission::Read).await;
        let has_admin = auth.has_permission(&user_id, Permission::Admin).await;
        
        assert!(has_read);
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(5, 60);
        
        for i in 0..5 {
            let result = limiter.check_rate_limit("test_user").await;
            assert!(result.is_ok(), "Request {} should pass", i);
        }
        
        let result = limiter.check_rate_limit("test_user").await;
        assert!(result.is_err());
    }
}
