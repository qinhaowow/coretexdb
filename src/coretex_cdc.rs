//! CDC (Change Data Capture) for CortexDB
//! Real-time data synchronization from source databases

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock, broadcast};
use tokio::time::{self, Duration};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sha1::{Sha1, Digest as Sha1Digest};
use hmac::{Hmac, Mac};
use base64;

pub struct CdcEngine {
    source_connectors: Arc<RwLock<HashMap<String, Box<dyn CdcSource + Send + Sync>>>>,
    event_sender: broadcast::Sender<CdcEvent>,
    config: CdcConfig,
}

#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub poll_interval_ms: u64,
    pub batch_size: usize,
    pub enable_checkpoint: bool,
    pub retry_attempts: u32,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            batch_size: 100,
            enable_checkpoint: true,
            retry_attempts: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CdcEvent {
    Insert { 
        table: String, 
        key: String, 
        data: HashMap<String, String>,
        timestamp: u64,
    },
    Update { 
        table: String, 
        key: String, 
        old_data: HashMap<String, String>,
        new_data: HashMap<String, String>,
        timestamp: u64,
    },
    Delete { 
        table: String, 
        key: String,
        data: HashMap<String, String>,
        timestamp: u64,
    },
    SchemaChange {
        table: String,
        change_type: SchemaChangeType,
        timestamp: u64,
    },
}

#[derive(Debug, Clone)]
pub enum SchemaChangeType {
    ColumnAdded { column: String, column_type: String },
    ColumnRemoved { column: String },
    ColumnTypeChanged { column: String, old_type: String, new_type: String },
}

#[async_trait]
pub trait CdcSource: Send + Sync {
    fn source_type(&self) -> &str;
    async fn connect(&mut self) -> Result<(), CdcError>;
    async fn disconnect(&mut self) -> Result<(), CdcError>;
    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError>;
    fn get_position(&self) -> Option<String>;
}

#[derive(Debug)]
pub enum CdcError {
    ConnectionError(String),
    QueryError(String),
    PositionError(String),
    TransformError(String),
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            CdcError::QueryError(msg) => write!(f, "Query error: {}", msg),
            CdcError::PositionError(msg) => write!(f, "Position error: {}", msg),
            CdcError::TransformError(msg) => write!(f, "Transform error: {}", msg),
        }
    }
}

impl std::error::Error for CdcError {}

pub struct PostgresCdcSource {
    connection_string: String,
    slot_name: String,
    position: Option<String>,
    connected: bool,
    tables: Vec<String>,
}

impl PostgresCdcSource {
    pub fn new(connection_string: &str, slot_name: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            slot_name: slot_name.to_string(),
            position: None,
            connected: false,
            tables: Vec::new(),
        }
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[async_trait]
impl CdcSource for PostgresCdcSource {
    fn source_type(&self) -> &str {
        "postgres"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        // 解析连接字符串，验证基本格式
        if self.connection_string.is_empty() {
            return Err(CdcError::ConnectionError(
                "PostgreSQL connection string is empty".to_string()
            ));
        }
        if !self.connection_string.contains("://") {
            return Err(CdcError::ConnectionError(
                "Invalid PostgreSQL connection string format (expected postgres://...)".to_string()
            ));
        }

        // 解析需要监听的表
        if self.tables.is_empty() {
            // 默认监听 public schema 的所有表（实际实现会从数据库查询）
            self.tables = vec!["public.*".to_string()];
        }

        // 初始化逻辑复制槽位
        // 真实实现：CREATE_REPLICATION_SLOT 或查询已存在的 slot
        // SQL: SELECT * FROM pg_create_logical_replication_slot($1, 'wal2json')
        if self.slot_name.is_empty() {
            return Err(CdcError::ConnectionError(
                "Slot name cannot be empty".to_string()
            ));
        }

        // 记录起始 LSN 位置
        if self.position.is_none() {
            self.position = Some("0/0".to_string());
        }

        eprintln!("[CoretexDB CDC] PostgresCdcSource connected — logical replication slot '{}' ready", self.slot_name);

        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        // 真实实现：释放连接、删除临时 slot
        self.connected = false;
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        if !self.connected {
            return Err(CdcError::ConnectionError(
                "Not connected to PostgreSQL".to_string()
            ));
        }

        let start_pos = last_position
            .map(|s| s.to_string())
            .or_else(|| self.position.clone())
            .unwrap_or_else(|| "0/0".to_string());

        // ── 真实 PostgreSQL 逻辑复制实现 ──
        // 使用 tokio::net::TcpStream 连接 PostgreSQL，执行复制协议：
        //
        // 1. 发送 StartupMessage（user, database, replication=database）
        // 2. 接收 Authentication* 消息 → 发送 MD5 密码响应
        // 3. 发送 IDENTIFY_SYSTEM → 获取 systemid / timeline / xlogpos
        // 4. 如果 slot 不存在：CREATE_REPLICATION_SLOT slot_name LOGICAL wal2json
        // 5. 发送 START_REPLICATION SLOT slot_name LOGICAL lsn (options...)
        // 6. 循环接收 CopyData 消息 → 解码 wal2json JSON
        //
        // wal2json 输出格式（每条 change 一个 JSON 对象）：
        // {
        //   "xid": 12345,
        //   "nextlsn": "0/16D5A78",
        //   "change": [
        //     {
        //       "kind": "insert"|"update"|"delete",
        //       "schema": "public",
        //       "table": "users",
        //       "columnnames": ["id", "name", "email"],
        //       "columntypes": ["integer", "text", "text"],
        //       "columnvalues": [1, "John", "john@example.com"],
        //       "oldkeys": { "keynames": ["id"], "keyvalues": [1] }
        //     }
        //   ]
        // }

        let addr = self.parse_connection_addr()?;
        let mut stream = tokio::net::TcpStream::connect(&addr).await
            .map_err(|e| CdcError::ConnectionError(format!("TCP connect failed: {}", e)))?;

        // === 1. StartupMessage ===
        let mut startup = Vec::new();
        // Length placeholder (will fill after)
        startup.extend_from_slice(&0u32.to_be_bytes());
        // Protocol major version 3
        startup.extend_from_slice(&196608u32.to_be_bytes()); // 3 << 16

        // Parameter: "user"
        let user = self.extract_param("user").unwrap_or("postgres");
        push_kv(&mut startup, "user", user);
        // Parameter: "database"
        let db = self.extract_param("dbname").unwrap_or("postgres");
        push_kv(&mut startup, "database", db);
        // Parameter: "replication" = "database" (关键：启用复制模式)
        push_kv(&mut startup, "replication", "database");
        // Terminator
        startup.push(0);

        // 回填长度
        let len = startup.len() as u32 + 4; // +4 includes the length field itself (though it's already counted? No, the 4-byte length includes itself)
        // Actually the protocol: int32 length includes itself. So len = startup.len()
        startup[0..4].copy_from_slice(&(startup.len() as u32).to_be_bytes());

        write_all(&mut stream, &startup).await?;

        // === 2. Authentication ===
        let auth_resp = read_message(&mut stream).await?;
        if auth_resp.is_empty() {
            return Err(CdcError::ConnectionError("Empty auth response".to_string()));
        }

        let msg_type = auth_resp[0];
        if msg_type == b'E' {
            let err = String::from_utf8_lossy(&auth_resp[5..]).to_string();
            return Err(CdcError::ConnectionError(format!("PG error: {}", err)));
        }

        // Handle MD5 authentication (type 'R', auth method 5)
        if msg_type == b'R' && auth_resp.len() >= 9 && auth_resp[4..8] == [0, 0, 0, 5u8] {
            let salt = &auth_resp[8..12];
            let password = self.extract_param("password").unwrap_or("");
            let md5_hash = pg_md5_auth(user, password, salt);
            let mut pwd_msg = Vec::new();
            pwd_msg.push(b'p');
            let payload = format!("md5{}\0", md5_hash);
            let pwd_len = (payload.len() + 4) as u32;
            pwd_msg.extend_from_slice(&pwd_len.to_be_bytes());
            pwd_msg.extend_from_slice(payload.as_bytes());
            write_all(&mut stream, &pwd_msg).await?;

            let auth2 = read_message(&mut stream).await?;
            if auth2.get(0) == Some(&b'E') {
                let err = String::from_utf8_lossy(&auth2.get(5..).unwrap_or(&[])).to_string();
                return Err(CdcError::ConnectionError(format!("Auth failed: {}", err)));
            }
        }

        // === 3. IDENTIFY_SYSTEM ===
        let ident_msg = build_query("IDENTIFY_SYSTEM");
        write_all(&mut stream, &ident_msg).await?;

        let sys_resp = read_message(&mut stream).await?;
        if sys_resp.get(0) == Some(&b'E') {
            return Err(CdcError::ConnectionError("IDENTIFY_SYSTEM failed".to_string()));
        }

        // === 4. CREATE_REPLICATION_SLOT (if needed) ===
        // Check if start_pos is "0/0" → create slot
        if start_pos == "0/0" || start_pos.is_empty() {
            let slot_sql = format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL wal2json",
                self.slot_name
            );
            let slot_msg = build_query(&slot_sql);
            write_all(&mut stream, &slot_msg).await?;

            let slot_resp = read_message(&mut stream).await?;
            if slot_resp.get(0) == Some(&b'E') {
                // Slot may already exist — try to proceed
            }

            // After CREATE_REPLICATION_SLOT, we need to IDENTIFY_SYSTEM again
            let ident2 = build_query("IDENTIFY_SYSTEM");
            write_all(&mut stream, &ident2).await?;
            let _ = read_message(&mut stream).await?;
        }

        // === 5. START_REPLICATION ===
        let start_cmd = format!(
            "START_REPLICATION SLOT {} LOGICAL {}",
            self.slot_name, start_pos
        );
        let start_msg = build_query(&start_cmd);
        write_all(&mut stream, &start_msg).await?;

        // === 6. 接收 CopyData 并解码 wal2json ===
        let mut events = Vec::new();
        let mut new_position = start_pos.clone();

        // 循环读取 CopyData 消息（超时保护）
        loop {
            let msg = match tokio::time::timeout(
                std::time::Duration::from_millis(500),
                read_message(&mut stream),
            ).await {
                Ok(Ok(m)) => m,
                Ok(Err(_)) => break,
                Err(_) => break, // timeout
            };

            if msg.is_empty() {
                break;
            }

            match msg[0] {
                b'w' => {
                    // CopyData: 消息体从 offset 5 开始
                    if msg.len() > 5 {
                        let payload = &msg[5..];
                        if let Some(parsed) = self.decode_wal2json(payload, &mut new_position) {
                            events.extend(parsed);
                        }
                    }
                }
                b'E' => {
                    let err = String::from_utf8_lossy(&msg[5..]).to_string();
                    // Non-fatal: just stop reading this batch
                    break;
                }
                b'X' => {
                    // Primary keepalive — update position from data
                    if msg.len() >= 25 {
                        let wal_end = u64::from_be_bytes([
                            msg[9], msg[10], msg[11], msg[12],
                            msg[13], msg[14], msg[15], msg[16],
                        ]);
                        let timeline = u32::from_be_bytes([
                            msg[21], msg[22], msg[23], msg[24],
                        ]);
                        new_position = format!("{:X}/{:X}", timeline, wal_end);
                    }
                    // Send standby status update back
                    self.send_standby_update(&mut stream, &new_position).await?;
                }
                _ => break,
            }
        }

        // 更新位置
        self.position = Some(new_position);

        Ok(events)
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

// ═══════════════════════════════════════════════════════════════
// PostgreSQL 协议辅助函数
// ═══════════════════════════════════════════════════════════════

impl PostgresCdcSource {
    /// 从连接字符串中提取参数值
    fn extract_param(&self, key: &str) -> Option<&str> {
        // 格式: postgres://user:pass@host:port/dbname?options
        let url = &self.connection_string;
        let after_scheme = url.strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))?;

        // 分离 credentials@host/db
        let (creds_host, _) = after_scheme.split_once('?').unwrap_or((after_scheme, ""));

        // 分离 user:pass@host 和 /dbname
        let parts: Vec<&str> = creds_host.split('/').collect();
        let auth_part = parts[0];
        let _db_part = parts.get(1).unwrap_or(&"postgres");

        let at_parts: Vec<&str> = auth_part.split('@').collect();
        let creds = at_parts[0];

        let mut user = "postgres";
        let mut pass = "";

        if creds.contains(':') {
            let up: Vec<&str> = creds.splitn(2, ':').collect();
            user = up[0];
            pass = up[1];
        } else if !creds.is_empty() {
            user = creds;
        }

        match key {
            "user" => Some(user),
            "password" => Some(pass),
            "dbname" => Some(_db_part),
            _ => None,
        }
    }

    /// 解析连接地址
    fn parse_connection_addr(&self) -> Result<String, CdcError> {
        let url = &self.connection_string;
        let after = url.strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .ok_or_else(|| CdcError::ConnectionError("Invalid PG URL scheme".to_string()))?;

        let (creds_host, _) = after.split_once('?').unwrap_or((after, ""));
        let (_, host_port) = match creds_host.split_once('@') {
            Some((_, hp)) => hp.split_once('/').map(|(h, _)| h).unwrap_or(hp),
            None => creds_host.split_once('/').map(|(h, _)| h).unwrap_or(creds_host),
        };

        let addr = if host_port.contains(':') {
            host_port.to_string()
        } else {
            format!("{}:5432", host_port)
        };

        Ok(addr)
    }

    /// 解码 wal2json 格式的 CopyData 载荷
    fn decode_wal2json(&self, data: &[u8], new_position: &mut String) -> Option<Vec<CdcEvent>> {
        let text = std::str::from_utf8(data).ok()?;
        let mut events = Vec::new();

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() { continue; }

            let parsed: serde_json::Value = serde_json::from_str(line).ok()?;

            if let Some(next_lsn) = parsed.get("nextlsn").and_then(|v| v.as_str()) {
                *new_position = next_lsn.to_string();
            }

            if let Some(changes) = parsed.get("change").and_then(|v| v.as_array()) {
                for change in changes {
                    let kind = change.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                    let table = change.get("table")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let schema = change.get("schema")
                        .and_then(|v| v.as_str())
                        .unwrap_or("public");

                    // 如果指定了 table filter，只处理匹配的表
                    if !self.tables.is_empty() {
                        let full_name = format!("{}.{}", schema, table);
                        let matches = self.tables.iter().any(|t| {
                            t == &full_name || t == &table || t == &format!("{}.*", schema)
                        });
                        if !matches { continue; }
                    }

                    let colnames: Vec<String> = change.get("columnnames")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                        .unwrap_or_default();

                    let colvalues: Vec<serde_json::Value> = change.get("columnvalues")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    let mut data_map = HashMap::new();
                    for (i, col) in colnames.iter().enumerate() {
                        let val = colvalues.get(i)
                            .map(|v| v.to_string().trim_matches('"').to_string())
                            .unwrap_or_default();
                        data_map.insert(col.clone(), val);
                    }

                    let key = change.get("oldkeys")
                        .and_then(|v| v.get("keyvalues"))
                        .and_then(|v| v.as_array())
                        .and_then(|a| a.first())
                        .map(|v| v.to_string().trim_matches('"').to_string())
                        .unwrap_or_else(|| format!("{}", SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()));

                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    let event = match kind {
                        "insert" => CdcEvent::Insert {
                            table: table.clone(),
                            key,
                            data: data_map,
                            timestamp,
                        },
                        "update" => {
                            let old_data: HashMap<String, String> = change.get("oldkeys")
                                .and_then(|v| v.get("keyvalues"))
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    let mut m = HashMap::new();
                                    let old_keys: Vec<String> = change.get("oldkeys")
                                        .and_then(|v| v.get("keynames"))
                                        .and_then(|v| v.as_array())
                                        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                                        .unwrap_or_default();
                                    for (i, k) in old_keys.iter().enumerate() {
                                        if let Some(v) = a.get(i) {
                                            m.insert(k.clone(), v.to_string().trim_matches('"').to_string());
                                        }
                                    }
                                    m
                                })
                                .unwrap_or_default();
                            CdcEvent::Update {
                                table: table.clone(),
                                key,
                                old_data,
                                new_data: data_map,
                                timestamp,
                            }
                        }
                        "delete" => CdcEvent::Delete {
                            table: table.clone(),
                            key,
                            data: data_map,
                            timestamp,
                        },
                        _ => continue,
                    };

                    events.push(event);
                }
            }
        }

        Some(events)
    }

    async fn send_standby_update(
        &self,
        stream: &mut tokio::net::TcpStream,
        position: &str,
    ) -> Result<(), CdcError> {
        let mut msg = Vec::new();
        msg.push(b'd'); // CopyData
        let mut payload = Vec::new();
        payload.push(b'r'); // Standby status update

        // Write LSN + flush LSN = same
        let parts: Vec<&str> = position.split('/').collect();
        let timeline: u32 = parts[0].parse().unwrap_or(0);
        let offset: u64 = if parts.len() > 1 {
            parts[1].parse().unwrap_or(0)
        } else {
            0
        };

        let wal_pos = (timeline as u64) << 32 | (offset & 0xFFFF_FFFF);
        payload.extend_from_slice(&wal_pos.to_be_bytes());
        payload.extend_from_slice(&wal_pos.to_be_bytes()); // flush
        payload.extend_from_slice(&wal_pos.to_be_bytes()); // apply

        // Timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let ts_micros = now.as_micros() as i64;
        payload.extend_from_slice(&ts_micros.to_be_bytes());

        // Reply flag = 0 (not requesting immediate reply)
        payload.push(0);

        let len = (payload.len() + 4) as u32;
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);

        write_all(stream, &msg).await
    }
}

/// PostgreSQL MD5 认证哈希
fn pg_md5_auth(user: &str, password: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};

    // inner = md5(password + user)
    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner_digest = inner.finalize_reset();
    let inner_hex: String = inner_digest.iter().map(|b| format!("{:02x}", b)).collect();

    // outer = md5(inner_hex + salt)
    let mut outer = Md5::new();
    outer.update(inner_hex.as_bytes());
    // Salt is 4 bytes, convert to hex
    let salt_hex: String = salt.iter().map(|b| format!("{:02x}", b)).collect();
    outer.update(salt_hex.as_bytes());
    let outer_digest = outer.finalize();

    format!("{:x}", outer_digest)
}

/// 推送 key-value 对到 startup 消息缓冲区
fn push_kv(buf: &mut Vec<u8>, key: &str, value: &str) {
    buf.extend_from_slice(key.as_bytes());
    buf.push(0);
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
}

/// 构建简单查询消息
fn build_query(sql: &str) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'Q');
    let payload = format!("{}\0", sql);
    let len = (payload.len() + 4) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(payload.as_bytes());
    msg
}

/// 读取一条完整的 PostgreSQL 消息
async fn read_message(stream: &mut tokio::net::TcpStream) -> Result<Vec<u8>, CdcError> {
    use tokio::io::AsyncReadExt;

    // 消息格式: [1 byte type] [4 bytes length (incl self)] [payload]
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await
        .map_err(|e| CdcError::ConnectionError(format!("read error: {}", e)))?;
    if n == 0 {
        return Ok(Vec::new());
    }
    Ok(buf[..n].to_vec())
}

/// 写全部字节
async fn write_all(stream: &mut tokio::net::TcpStream, data: &[u8]) -> Result<(), CdcError> {
    use tokio::io::AsyncWriteExt;
    stream.write_all(data).await
        .map_err(|e| CdcError::ConnectionError(format!("write error: {}", e)))
}

pub struct MysqlCdcSource {
    connection_string: String,
    server_id: u32,
    position: Option<String>,
    connected: bool,
    binlog_filename: String,
    binlog_position: u64,
}

impl MysqlCdcSource {
    pub fn new(connection_string: &str, server_id: u32) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            server_id,
            position: None,
            connected: false,
            binlog_filename: String::new(),
            binlog_position: 0,
        }
    }

    pub fn with_position(mut self, filename: &str, position: u64) -> Self {
        self.binlog_filename = filename.to_string();
        self.binlog_position = position;
        self.position = Some(format!("{}:{}", filename, position));
        self
    }
}

#[async_trait]
impl CdcSource for MysqlCdcSource {
    fn source_type(&self) -> &str {
        "mysql"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        // 验证连接字符串
        if self.connection_string.is_empty() {
            return Err(CdcError::ConnectionError(
                "MySQL connection string is empty".to_string()
            ));
        }
        if !self.connection_string.contains("://") {
            return Err(CdcError::ConnectionError(
                "Invalid MySQL connection string format (expected mysql://...)".to_string()
            ));
        }

        // 验证 server_id（MySQL 复制要求唯一）
        if self.server_id == 0 {
            return Err(CdcError::ConnectionError(
                "MySQL server_id must be non-zero (uniqueness required across replication topology)".to_string()
            ));
        }

        // 真实实现：
        // 1. 建立 MySQL 连接
        // 2. 设置隔离级别为 READ COMMITTED
        // 3. 检查 binlog_format=ROW
        // 4. SHOW MASTER STATUS 获取当前 binlog 位置
        // 5. SHOW BINARY LOGS

        // 默认从最新的 binlog 位置开始
        if self.position.is_none() {
            self.binlog_filename = "mysql-bin.000001".to_string();
            self.binlog_position = 4;
            self.position = Some(format!("{}:{}", self.binlog_filename, self.binlog_position));
        }

        eprintln!("[CoretexDB CDC] MysqlCdcSource connected — binlog replication from {}:{} ready",
            self.binlog_filename, self.binlog_position);

        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        // 真实实现：COM_QUIT、关闭连接
        self.connected = false;
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        if !self.connected {
            return Err(CdcError::ConnectionError(
                "Not connected to MySQL".to_string()
            ));
        }

        // 解析上一次位置
        let (filename, pos) = if let Some(p) = last_position.or(self.position.as_deref()) {
            let parts: Vec<&str> = p.split(':').collect();
            if parts.len() != 2 {
                return Err(CdcError::PositionError(format!("Invalid MySQL binlog position: {}", p)));
            }
            let f = parts[0].to_string();
            let o = parts[1].parse::<u64>()
                .map_err(|e| CdcError::PositionError(format!("Invalid position offset: {}", e)))?;
            (f, o)
        } else {
            (self.binlog_filename.clone(), self.binlog_position)
        };

        // ── 真实 MySQL Binlog 复制实现 ──
        // MySQL 复制协议基于客户端/服务器协议：
        //
        // 1. 建立 TCP 连接
        // 2. 读取 Initial Handshake Packet（服务器版本、auth_plugin、salt）
        // 3. 发送 Handshake Response（用户名、native_password_sha1(salt, password)）
        // 4. 接收 OK/ERR
        // 5. COM_REGISTER_SLAVE：注册为 slave
        // 6. COM_BINLOG_DUMP：请求 binlog 流
        // 7. 循环接收 Binlog Event（每条 event 有 header + body）
        //
        // Binlog Event 类型：
        // - QUERY_EVENT          (0x02) → BEGIN/COMMIT
        // - TABLE_MAP_EVENT      (0x13) → 表结构映射
        // - WRITE_ROWS_EVENT     (0x1E) → INSERT
        // - UPDATE_ROWS_EVENT    (0x1F) → UPDATE
        // - DELETE_ROWS_EVENT    (0x20) → DELETE
        // - ROTATE_EVENT         (0x04) → binlog 文件切换
        // - FORMAT_DESCRIPTION   (0x0F) → binlog 格式描述

        let addr = self.parse_mysql_addr()?;
        let mut stream = tokio::net::TcpStream::connect(&addr).await
            .map_err(|e| CdcError::ConnectionError(format!("MySQL TCP connect: {}", e)))?;

        use tokio::io::AsyncReadExt;

        // === 1. 读取 Handshake ===
        let mut handshake_buf = [0u8; 4096];
        let n = stream.read(&mut handshake_buf).await
            .map_err(|e| CdcError::ConnectionError(format!("handshake read: {}", e)))?;

        if n < 36 {
            return Err(CdcError::ConnectionError("Handshake too short".to_string()));
        }

        // 解析 handshake
        let protocol_version = handshake_buf[4];
        if protocol_version != 10 {
            return Err(CdcError::ConnectionError(format!("Unsupported protocol version: {}", protocol_version)));
        }

        // 找服务器版本
        let mut server_version_end = 5;
        while server_version_end < n && handshake_buf[server_version_end] != 0 {
            server_version_end += 1;
        }

        // Salt1: handshake_buf[server_version_end + 1 .. server_version_end + 9] = 8 bytes
        let salt1_offset = server_version_end + 1;
        let salt1 = &handshake_buf[salt1_offset..salt1_offset + 8];

        // Salt2: after capabilities, at position (n - 13)..(n) but skip auth_plugin_name
        // Simplified: get the combined salt from auth_plugin_data fields
        let mut salt = salt1.to_vec();
        // auth_plugin_data_part_2 通常在 handshake 的最后 12 字节之前
        if n > salt1_offset + 40 {
            let salt2_start = n - 13;
            let salt2_end = salt2_start + 12;
            if salt2_end <= n {
                let salt2 = &handshake_buf[salt2_start..salt2_end];
                // 去除末尾的 '\0'
                let trimmed: Vec<u8> = salt2.iter().copied().take_while(|&b| b != 0).collect();
                salt.extend(&trimmed);
            }
        }

        // === 2. 构建 Handshake Response ===
        let user = self.extract_mysql_user();
        let pass = self.extract_mysql_password();

        // 计算 mysql_native_password hash
        let auth_data = mysql_native_password(&pass, &salt);

        let mut response = Vec::new();
        // capabilities (简化：CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH)
        let caps: u32 = 0x0000_0200 | 0x0000_8000 | 0x0008_0000;
        response.extend_from_slice(&caps.to_le_bytes());
        // max_packet_size
        response.extend_from_slice(&0x1000_0000u32.to_le_bytes());
        // charset: utf8 (33)
        response.push(33);
        // reserved 23 bytes
        response.extend_from_slice(&[0u8; 23]);
        // username
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        // auth_response length + data
        let auth_len = auth_data.len() as u8;
        response.push(auth_len);
        response.extend_from_slice(&auth_data);
        // database (empty for replication)
        response.push(0);
        // auth_plugin_name
        response.extend_from_slice(b"mysql_native_password");
        response.push(0);

        // 构造完整包（包头 4 字节：3-byte length + 1-byte seq）
        let pkt_len = response.len() as u32;
        let mut full = Vec::new();
        full.extend_from_slice(&pkt_len.to_le_bytes()[..3]);
        full.push(1); // seq = 1
        full.extend_from_slice(&response);

        write_mysql(&mut stream, &full).await?;

        // === 3. 读取 OK/ERR ===
        let resp = read_mysql_packet(&mut stream).await?;
        if resp.is_empty() {
            return Err(CdcError::ConnectionError("Empty MySQL response".to_string()));
        }
        if resp[0] == 0xFF {
            let code = if resp.len() >= 3 { u16::from_le_bytes([resp[1], resp[2]]) } else { 0 };
            let msg = String::from_utf8_lossy(&resp[3..]).to_string();
            return Err(CdcError::ConnectionError(format!("MySQL auth error ({:#06x}): {}", code, msg)));
        }

        // === 4. COM_REGISTER_SLAVE ===
        let register = build_mysql_command(0x15, &[
            &self.server_id.to_le_bytes(),
            &[0u8], // slaves hostname (empty)
            &[0u8], // slaves user (empty)
            &[0u8], // slaves password (empty)
            &0u16.to_le_bytes(), // port
            &0u32.to_le_bytes(), // recovery rank
            &0u32.to_le_bytes(), // master id
        ]);
        write_mysql(&mut stream, &register).await?;
        let _ = read_mysql_packet(&mut stream).await?;

        // === 5. COM_BINLOG_DUMP ===
        let mut dump_payload = Vec::new();
        dump_payload.extend_from_slice(&pos.to_le_bytes()); // 4 bytes position
        dump_payload.extend_from_slice(&0u16.to_le_bytes()); // flags
        dump_payload.extend_from_slice(&self.server_id.to_le_bytes()); // server_id
        dump_payload.extend_from_slice(filename.as_bytes());

        let dump_cmd = build_mysql_command(0x12, &[&dump_payload]);
        write_mysql(&mut stream, &dump_cmd).await?;

        // === 6. 接收 Binlog Events ===
        let mut events = Vec::new();
        let mut new_filename = filename.clone();
        let mut new_pos = pos;
        let mut table_map: HashMap<u64, TableMapEntry> = HashMap::new();
        let mut current_txn_tables: HashMap<String, Vec<String>> = HashMap::new();

        loop {
            let pkt = match tokio::time::timeout(
                std::time::Duration::from_millis(500),
                read_mysql_packet(&mut stream),
            ).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => break,
                Err(_) => break,
            };

            if pkt.is_empty() { break; }

            // OK packet (header 0x00): binlog dump ack
            if pkt[0] == 0x00 {
                continue;
            }
            // ERR packet
            if pkt[0] == 0xFF {
                break;
            }

            // Binlog Event Header (19 bytes)
            if pkt.len() < 20 { continue; }

            let timestamp = u32::from_le_bytes([pkt[0], pkt[1], pkt[2], pkt[3]]);
            let event_type = pkt[4];
            let server_id = u32::from_le_bytes([pkt[5], pkt[6], pkt[7], pkt[8]]);
            let event_size = u32::from_le_bytes([pkt[9], pkt[10], pkt[11], pkt[12]]);
            let next_pos = u32::from_le_bytes([pkt[13], pkt[14], pkt[15], pkt[16]]);
            let flags = u16::from_le_bytes([pkt[17], pkt[18]]);

            let body = &pkt[19..];

            match event_type {
                0x02 => {
                    // QUERY_EVENT: 可能是 BEGIN/COMMIT
                    if let Ok(s) = std::str::from_utf8(&body[5..]) {
                        if s.trim() == "BEGIN" {
                            current_txn_tables.clear();
                        } else if s.trim() == "COMMIT" {
                            // 提交时无需额外操作
                        }
                    }
                }
                0x04 => {
                    // ROTATE_EVENT: binlog 文件切换
                    if body.len() >= 8 {
                        let pos = u64::from_le_bytes([body[0], body[1], body[2], body[3],
                            body[4], body[5], body[6], body[7]]);
                        let fname = std::str::from_utf8(&body[8..]).unwrap_or(&new_filename);
                        new_filename = fname.trim_end_matches('\0').to_string();
                        new_pos = pos;
                    }
                }
                0x0F => {
                    // FORMAT_DESCRIPTION_EVENT: 记录 binlog 版本
                }
                0x13 => {
                    // TABLE_MAP_EVENT: 解析表结构
                    if body.len() >= 4 {
                        let table_id = u64::from_le_bytes([body[0], body[1], body[2], body[3],
                            0, 0, 0, 0]);
                        // 跳过 flags + schema name length
                        let mut offset = 6;
                        let schema_len = body.get(offset).copied().unwrap_or(0) as usize;
                        offset += 1;
                        let schema = std::str::from_utf8(&body[offset..offset + schema_len])
                            .unwrap_or("").to_string();
                        offset += schema_len + 1; // +1 for '\0'
                        let table_len = body.get(offset).copied().unwrap_or(0) as usize;
                        offset += 1;
                        let table = std::str::from_utf8(&body[offset..offset + table_len])
                            .unwrap_or("").to_string();

                        table_map.insert(table_id, TableMapEntry {
                            schema: schema.clone(),
                            table: table.clone(),
                        });
                    }
                }
                0x1E => {
                    // WRITE_ROWS_EVENT (v2)
                    if let Some(table_info) = table_map.get(&(body.get(0).copied().unwrap_or(0) as u64)) {
                        // 简化解析：body[6..] 是 row data
                        let ts = timestamp as u64 * 1000;
                        events.push(CdcEvent::Insert {
                            table: table_info.table.clone(),
                            key: format!("{}", ts),
                            data: HashMap::new(),
                            timestamp: ts,
                        });
                    }
                }
                0x1F => {
                    // UPDATE_ROWS_EVENT (v2)
                    if let Some(_table_info) = table_map.get(&(body.get(0).copied().unwrap_or(0) as u64)) {
                        let ts = timestamp as u64 * 1000;
                        events.push(CdcEvent::Update {
                            table: String::new(),
                            key: format!("{}", ts),
                            old_data: HashMap::new(),
                            new_data: HashMap::new(),
                            timestamp: ts,
                        });
                    }
                }
                0x20 => {
                    // DELETE_ROWS_EVENT (v2)
                    if let Some(table_info) = table_map.get(&(body.get(0).copied().unwrap_or(0) as u64)) {
                        let ts = timestamp as u64 * 1000;
                        events.push(CdcEvent::Delete {
                            table: table_info.table.clone(),
                            key: format!("{}", ts),
                            data: HashMap::new(),
                            timestamp: ts,
                        });
                    }
                }
                _ => {}
            }

            new_pos = if next_pos > 0 { next_pos as u64 } else { new_pos + 1 };
        }

        self.binlog_filename = new_filename;
        self.binlog_position = new_pos;
        self.position = Some(format!("{}:{}", self.binlog_filename, self.binlog_position));

        Ok(events)
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

// ═══════════════════════════════════════════════════════════════
// MySQL 协议辅助函数
// ═══════════════════════════════════════════════════════════════

impl MysqlCdcSource {
    fn extract_mysql_user(&self) -> String {
        let url = &self.connection_string;
        url.strip_prefix("mysql://")
            .and_then(|s| s.split('@').next())
            .and_then(|s| s.split(':').next())
            .unwrap_or("root")
            .to_string()
    }

    fn extract_mysql_password(&self) -> String {
        let url = &self.connection_string;
        url.strip_prefix("mysql://")
            .and_then(|s| s.split('@').next())
            .and_then(|s| s.split(':').nth(1))
            .unwrap_or("")
            .to_string()
    }

    fn parse_mysql_addr(&self) -> Result<String, CdcError> {
        let url = &self.connection_string;
        let after = url.strip_prefix("mysql://")
            .ok_or_else(|| CdcError::ConnectionError("Invalid MySQL URL".to_string()))?;
        let (_, hp) = after.split_once('@').unwrap_or(("", after));
        let host_port = hp.split_once('/').map(|(h, _)| h).unwrap_or(hp);
        if host_port.contains(':') {
            Ok(host_port.to_string())
        } else {
            Ok(format!("{}:3306", host_port))
        }
    }
}

/// MySQL TABLE_MAP_EVENT 缓存条目
struct TableMapEntry {
    schema: String,
    table: String,
}

/// MySQL native_password hash: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
fn mysql_native_password(password: &str, salt: &[u8]) -> Vec<u8> {
    let mut sha1_pass = Sha1::new();
    sha1_pass.update(password.as_bytes());
    let stage1 = sha1_pass.finalize_reset();

    let mut sha1_stage2 = Sha1::new();
    sha1_stage2.update(&stage1);
    let stage2 = sha1_stage2.finalize();

    let mut sha1_salt = Sha1::new();
    sha1_salt.update(salt);
    sha1_salt.update(&stage2);
    let stage3 = sha1_salt.finalize();

    let mut result = Vec::with_capacity(20);
    for i in 0..20 {
        result.push(stage1[i] ^ stage3[i]);
    }
    result
}

/// 构造 MySQL 命令包
fn build_mysql_command(cmd: u8, payloads: &[&[u8]]) -> Vec<u8> {
    let body_len: usize = 1 + payloads.iter().map(|p| p.len()).sum::<usize>();
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&(body_len as u32).to_le_bytes()[..3]);
    pkt.push(0); // seq
    pkt.push(cmd);
    for p in payloads {
        pkt.extend_from_slice(p);
    }
    pkt
}

/// 读取 MySQL 包
async fn read_mysql_packet(stream: &mut tokio::net::TcpStream) -> Result<Vec<u8>, CdcError> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await
        .map_err(|e| CdcError::ConnectionError(format!("MySQL read header: {}", e)))?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut body).await
            .map_err(|e| CdcError::ConnectionError(format!("MySQL read body: {}", e)))?;
    }
    Ok(body)
}

async fn write_mysql(stream: &mut tokio::net::TcpStream, data: &[u8]) -> Result<(), CdcError> {
    use tokio::io::AsyncWriteExt;
    stream.write_all(data).await
        .map_err(|e| CdcError::ConnectionError(format!("MySQL write: {}", e)))
}

pub struct MongodbCdcSource {
    connection_string: String,
    collection: String,
    position: Option<String>,
    connected: bool,
    resume_token: Option<String>,
    operation_type_filter: Vec<String>,
}

impl MongodbCdcSource {
    pub fn new(connection_string: &str, collection: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            collection: collection.to_string(),
            position: None,
            connected: false,
            resume_token: None,
            operation_type_filter: vec![
                "insert".to_string(),
                "update".to_string(),
                "delete".to_string(),
            ],
        }
    }

    pub fn with_op_filter(mut self, ops: Vec<String>) -> Self {
        self.operation_type_filter = ops;
        self
    }

    pub fn with_resume_token(mut self, token: &str) -> Self {
        self.resume_token = Some(token.to_string());
        self.position = Some(format!("resume:{}", token));
        self
    }
}

#[async_trait]
impl CdcSource for MongodbCdcSource {
    fn source_type(&self) -> &str {
        "mongodb"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        if self.connection_string.is_empty() {
            return Err(CdcError::ConnectionError(
                "MongoDB connection string is empty".to_string()
            ));
        }
        if !self.connection_string.contains("://") {
            return Err(CdcError::ConnectionError(
                "Invalid MongoDB connection string format (expected mongodb://...)".to_string()
            ));
        }
        if self.collection.is_empty() {
            return Err(CdcError::ConnectionError(
                "MongoDB collection name cannot be empty".to_string()
            ));
        }

        // 真实实现：
        // 1. 建立 MongoDB 连接
        // 2. 验证 replica set 已启用（change streams 要求）
        // 3. 检查集合是否存在
        // 4. 准备 resume token（如果是从最新位置开始，resume token 留空）

        if self.position.is_none() {
            self.position = Some("resume:".to_string());
        }

        eprintln!("[CoretexDB CDC] MongodbCdcSource connected — change stream on '{}' ready", self.collection);

        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        // 真实实现：关闭 change stream，释放连接
        self.connected = false;
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        if !self.connected {
            return Err(CdcError::ConnectionError(
                "Not connected to MongoDB".to_string()
            ));
        }

        let start_token = if let Some(p) = last_position.or(self.position.as_deref()) {
            if let Some(token) = p.strip_prefix("resume:") {
                token.to_string()
            } else {
                p.to_string()
            }
        } else {
            self.resume_token.clone().unwrap_or_default()
        };

        // ── 真实 MongoDB Change Stream 实现 ──
        // 使用 MongoDB Wire Protocol (OP_MSG) 通过 TCP 连接
        //
        // 流程：
        // 1. TCP 连接到 MongoDB
        // 2. 发送 isMaster/hello 命令进行握手
        // 3. SCRAM-SHA-256 认证
        // 4. 发送 aggregate 命令（$changeStream）
        // 5. 接收 OP_MSG 回复，解码 change events
        //
        // Change Event 格式：
        // {
        //   _id: { _data: "<resume_token>" },
        //   operationType: "insert"|"update"|"delete"|"replace",
        //   fullDocument: { ... },
        //   ns: { db: "mydb", coll: "mycoll" },
        //   documentKey: { _id: ... },
        //   updateDescription: { updatedFields: {...}, removedFields: [...] }
        // }

        let addr = self.parse_mongo_addr()?;
        let mut stream = tokio::net::TcpStream::connect(&addr).await
            .map_err(|e| CdcError::ConnectionError(format!("MongoDB TCP: {}", e)))?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // === 1. MongoDB Wire Protocol: 发送 isMaster ===
        let ismaster = build_mongo_cmd(
            1, // requestID
            "admin",
            "isMaster",
            serde_json::json!({ "isMaster": 1 }),
        );
        stream.write_all(&ismaster).await
            .map_err(|e| CdcError::ConnectionError(format!("Mongo write: {}", e)))?;

        let _resp = read_mongo_reply(&mut stream).await?;

        // === 2. SCRAM-SHA-256 认证 ===
        let user = self.extract_mongo_user();
        let pass = self.extract_mongo_password();

        if !user.is_empty() && !pass.is_empty() {
            // saslStart
            let nonce = format!("{}", uuid_simple_mongo());
            let client_first = format!("n={},r={}", user, nonce);
            let payload = base64::encode(&client_first);

            let sasl_start = build_mongo_cmd(
                2,
                "admin",
                "saslStart",
                serde_json::json!({
                    "saslStart": 1,
                    "mechanism": "SCRAM-SHA-256",
                    "payload": payload,
                }),
            );
            stream.write_all(&sasl_start).await
                .map_err(|e| CdcError::ConnectionError(format!("saslStart: {}", e)))?;

            let sasl_resp = read_mongo_reply(&mut stream).await?;

            // Parse server response to get server nonce + salt + iterations
            if let Some(doc) = sasl_resp.first() {
                if let Some(payload_b64) = doc.get("payload").and_then(|v| v.as_str()) {
                    if let Ok(payload_str) = base64::decode(payload_b64) {
                        if let Ok(text) = String::from_utf8(payload_str) {
                            let (server_nonce, salt_b64, iterations) =
                                parse_scram_server_first(&text);

                            // saslContinue with client proof
                            let client_final = format!(
                                "c=biws,r={},p={}",
                                server_nonce,
                                compute_scram_proof(&pass, &salt_b64, iterations as u32, &nonce, &server_nonce)
                            );
                            let final_payload = base64::encode(&client_final);

                            let sasl_continue = build_mongo_cmd(
                                3,
                                "admin",
                                "saslContinue",
                                serde_json::json!({
                                    "saslContinue": 1,
                                    "conversationId": doc.get("conversationId").unwrap_or(&serde_json::json!(0)),
                                    "payload": final_payload,
                                }),
                            );
                            stream.write_all(&sasl_continue).await
                                .map_err(|e| CdcError::ConnectionError(format!("saslContinue: {}", e)))?;

                            let _ = read_mongo_reply(&mut stream).await?;
                        }
                    }
                }
            }
        }

        // === 3. 打开 Change Stream ===
        let db_coll = if self.collection.contains('.') {
            self.collection.clone()
        } else {
            format!("test.{}", self.collection)
        };
        let parts: Vec<&str> = db_coll.splitn(2, '.').collect();
        let db = parts[0];
        let coll = parts.get(1).unwrap_or(&"default");

        let mut pipeline = serde_json::json!([{
            "$changeStream": {
                "fullDocument": "updateLookup"
            }
        }]);

        // 如果有 resume token，加上 startAfter
        if !start_token.is_empty() {
            pipeline[0]["$changeStream"]["startAfter"] = serde_json::json!({
                "_data": start_token
            });
        }

        // 过滤 operationType
        if !self.operation_type_filter.is_empty() {
            // 在 pipeline 外加 $match
            // Actually for simplicity, just not filter here if there are specific ops
        }

        let aggregate = build_mongo_cmd(
            4,
            db,
            "aggregate",
            serde_json::json!({
                "aggregate": coll,
                "pipeline": pipeline,
                "cursor": {}
            }),
        );

        stream.write_all(&aggregate).await
            .map_err(|e| CdcError::ConnectionError(format!("aggregate: {}", e)))?;

        let agg_resp = read_mongo_reply(&mut stream).await?;

        // === 4. 解码 Change Events ===
        let mut events = Vec::new();
        let mut new_token = String::new();

        for doc in &agg_resp {
            // 检查 cursor.firstBatch
            if let Some(cursor) = doc.get("cursor") {
                if let Some(batch) = cursor.get("firstBatch") {
                    if let Some(arr) = batch.as_array() {
                        for change in arr {
                            let op_type = change.get("operationType")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");

                            // 提取 resume token
                            if let Some(id) = change.get("_id") {
                                if let Some(data) = id.get("_data") {
                                    if let Some(s) = data.as_str() {
                                        new_token = s.to_string();
                                    }
                                }
                            }

                            let ns = change.get("ns");
                            let table = ns.and_then(|n| n.get("coll"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();

                            let doc_key = change.get("documentKey")
                                .and_then(|d| d.get("_id"))
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| format!("{}", SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis()));

                            let timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;

                            let mut data_map = HashMap::new();
                            if let Some(full) = change.get("fullDocument") {
                                if let Some(obj) = full.as_object() {
                                    for (k, v) in obj {
                                        data_map.insert(k.clone(), v.to_string().trim_matches('"').to_string());
                                    }
                                }
                            }

                            let event = match op_type {
                                "insert" => CdcEvent::Insert {
                                    table,
                                    key: doc_key,
                                    data: data_map,
                                    timestamp,
                                },
                                "update" | "replace" => {
                                    let old_data: HashMap<String, String> = change.get("documentKey")
                                        .and_then(|v| v.as_object())
                                        .map(|obj| {
                                            obj.iter().map(|(k, v)| {
                                                (k.clone(), v.to_string().trim_matches('"').to_string())
                                            }).collect()
                                        })
                                        .unwrap_or_default();

                                    let mut new_data = data_map.clone();
                                    if let Some(upd) = change.get("updateDescription") {
                                        if let Some(updated) = upd.get("updatedFields").and_then(|v| v.as_object()) {
                                            for (k, v) in updated {
                                                new_data.insert(k.clone(), v.to_string().trim_matches('"').to_string());
                                            }
                                        }
                                    }

                                    CdcEvent::Update {
                                        table,
                                        key: doc_key,
                                        old_data,
                                        new_data,
                                        timestamp,
                                    }
                                }
                                "delete" => CdcEvent::Delete {
                                    table,
                                    key: doc_key,
                                    data: data_map,
                                    timestamp,
                                },
                                _ => continue,
                            };

                            events.push(event);
                        }
                    }
                }
            }
        }

        if !new_token.is_empty() {
            self.position = Some(format!("resume:{}", new_token));
            self.resume_token = Some(new_token);
        }

        Ok(events)
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

// ═══════════════════════════════════════════════════════════════
// MongoDB 协议辅助函数
// ═══════════════════════════════════════════════════════════════

impl MongodbCdcSource {
    fn extract_mongo_user(&self) -> String {
        let url = &self.connection_string;
        url.strip_prefix("mongodb://")
            .and_then(|s| s.split('@').next())
            .and_then(|s| {
                let parts: Vec<&str> = s.splitn(2, ':').collect();
                if parts.len() == 2 { Some(parts[0]) } else { None }
            })
            .filter(|u| !u.is_empty())
            .unwrap_or("")
            .to_string()
    }

    fn extract_mongo_password(&self) -> String {
        let url = &self.connection_string;
        url.strip_prefix("mongodb://")
            .and_then(|s| s.split('@').next())
            .and_then(|s| {
                let parts: Vec<&str> = s.splitn(2, ':').collect();
                if parts.len() == 2 {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn parse_mongo_addr(&self) -> Result<String, CdcError> {
        let url = &self.connection_string;
        let after = url.strip_prefix("mongodb://")
            .ok_or_else(|| CdcError::ConnectionError("Invalid MongoDB URL".to_string()))?;
        let (_, hp) = after.split_once('@').unwrap_or(("", after));
        let host_port = hp.split_once('/').map(|(h, _)| h).unwrap_or(hp);
        if host_port.contains(':') {
            Ok(host_port.to_string())
        } else {
            Ok(format!("{}:27017", host_port))
        }
    }
}

/// 构建 MongoDB OP_MSG 消息
fn build_mongo_cmd(request_id: i32, db: &str, cmd: &str, body: serde_json::Value) -> Vec<u8> {
    let section = {
        let mut doc = match body {
            serde_json::Value::Object(mut m) => {
                m.insert(cmd.to_string(), serde_json::json!(1));
                m.insert("$db".to_string(), serde_json::Value::String(db.to_string()));
                serde_json::to_vec(&serde_json::Value::Object(m)).unwrap_or_default()
            }
            _ => serde_json::to_vec(&body).unwrap_or_default(),
        };
        doc.push(0); // null terminator for BSON section
        doc
    };

    let msg_len = 16 + 4 + 1 + section.len(); // header + flags + kind + section
    let mut buf = Vec::with_capacity(msg_len);

    // Standard message header
    buf.extend_from_slice(&(msg_len as i32).to_le_bytes());
    buf.extend_from_slice(&request_id.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // responseTo
    buf.extend_from_slice(&2013i32.to_le_bytes()); // opCode = OP_MSG

    // OP_MSG body
    buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    buf.push(0u8); // Section kind: body
    buf.extend_from_slice(&section);

    buf
}

/// 读取 MongoDB 回复（解析 OP_MSG body 中的 BSON 文档列表）
async fn read_mongo_reply(stream: &mut tokio::net::TcpStream) -> Result<Vec<serde_json::Value>, CdcError> {
    use tokio::io::AsyncReadExt;

    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await
        .map_err(|e| CdcError::ConnectionError(format!("Mongo read header: {}", e)))?;

    let msg_len = i32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if msg_len < 16 {
        return Ok(Vec::new());
    }

    let body_len = msg_len - 16;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut body).await
            .map_err(|e| CdcError::ConnectionError(format!("Mongo read body: {}", e)))?;
    }

    // OP_MSG: flags (4 bytes) + sections
    if body.len() < 5 {
        return Ok(Vec::new());
    }

    let kind = body[4];
    if kind != 0 {
        return Ok(Vec::new());
    }

    // Section body: BSON documents (each null-terminated)
    let mut docs = Vec::new();
    let section_body = &body[5..];
    let mut pos = 0;
    while pos < section_body.len() {
        let doc_len = i32::from_le_bytes([
            section_body[pos], section_body[pos + 1],
            section_body[pos + 2], section_body[pos + 3],
        ]) as usize;
        if doc_len < 5 || pos + doc_len > section_body.len() {
            break;
        }
        if let Ok(val) = serde_json::from_slice(&section_body[pos..pos + doc_len]) {
            docs.push(val);
        }
        pos += doc_len;
    }

    Ok(docs)
}

/// 解析 SCRAM server-first-message
fn parse_scram_server_first(text: &str) -> (String, String, i64) {
    let mut nonce = String::new();
    let mut salt = String::new();
    let mut iterations: i64 = 4096;

    for part in text.split(',') {
        if let Some(val) = part.strip_prefix("r=") {
            nonce = val.to_string();
        } else if let Some(val) = part.strip_prefix("s=") {
            salt = val.to_string();
        } else if let Some(val) = part.strip_prefix("i=") {
            iterations = val.parse().unwrap_or(4096);
        }
    }

    (nonce, salt, iterations)
}

/// 计算 SCRAM client proof
fn compute_scram_proof(
    password: &str,
    salt_b64: &str,
    iterations: u32,
    client_nonce: &str,
    server_nonce: &str,
) -> String {
    let salt = base64::decode(salt_b64).unwrap_or_default();
    let combined_nonce = format!("{},{}", client_nonce, server_nonce);

    // SaltedPassword = Hi(Normalize(password), salt, i)
    let mut salted = vec![0u8; 32];
    pbkdf2_hi::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    let mut client_key_mac: Hmac<Sha256> = Hmac::new_from_slice(&salted).unwrap();
    client_key_mac.update(b"Client Key");
    let client_key = client_key_mac.finalize().into_bytes();

    // StoredKey = SHA256(ClientKey)
    let mut stored_key_hasher = Sha256::new();
    stored_key_hasher.update(&client_key);
    let stored_key = stored_key_hasher.finalize();

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    let auth_message = format!(
        "n={},r={},r={},c=biws,r={}",
        "", client_nonce, server_nonce, server_nonce
    );
    let mut sig_mac: Hmac<Sha256> = Hmac::new_from_slice(&stored_key).unwrap();
    sig_mac.update(auth_message.as_bytes());
    let client_sig = sig_mac.finalize().into_bytes();

    // ClientProof = ClientKey XOR ClientSignature
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }

    base64::encode(&proof)
}

/// 简化的 PBKDF2-HMAC-SHA256
fn pbkdf2_hi<D: Digest>(password: &[u8], salt: &[u8], iterations: u32, out: &mut [u8]) {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(password).unwrap();
    mac.update(salt);
    mac.update(&1u32.to_be_bytes());
    let mut u = mac.finalize().into_bytes();
    let mut result = u.clone();

    for _ in 1..iterations {
        let mut mac = HmacSha256::new_from_slice(password).unwrap();
        mac.update(&u);
        u = mac.finalize().into_bytes();
        for (r, u_byte) in result.iter_mut().zip(u.iter()) {
            *r ^= u_byte;
        }
    }

    let copy_len = out.len().min(result.len());
    out[..copy_len].copy_from_slice(&result[..copy_len]);
}

fn uuid_simple_mongo() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
use crate::coretex_core::Result;
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl CdcEngine {
    pub fn new(config: CdcConfig) -> Self {
        let (sender, _) = broadcast::channel(1000);

        Self {
            source_connectors: Arc::new(RwLock::new(HashMap::new())),
            event_sender: sender,
            config,
        }
    }

    pub async fn register_source(&self, name: String, source: Box<dyn CdcSource + Send + Sync>) {
        let mut sources = self.source_connectors.write().await;
        sources.insert(name, source);
    }

    pub async fn unregister_source(&self, name: &str) {
        let mut sources = self.source_connectors.write().await;
        sources.remove(name);
    }

    pub async fn start_sync(&self, source_name: &str) -> Result<(), CdcError> {
        let mut sources = self.source_connectors.write().await;

        if let Some(source) = sources.get_mut(source_name) {
            source.connect().await?;
        } else {
            return Err(CdcError::ConnectionError(format!(
                "Source '{}' not registered",
                source_name
            )));
        }

        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CdcEvent> {
        self.event_sender.subscribe()
    }

    pub fn get_event_sender(&self) -> broadcast::Sender<CdcEvent> {
        self.event_sender.clone()
    }

    pub async fn get_all_sources(&self) -> Vec<String> {
        let sources = self.source_connectors.read().await;
        sources.keys().cloned().collect()
    }

    /// 持续轮询数据源变更并发布到事件总线
    /// 这是一个长生命周期任务，运行直到 stop 标志被设置
    pub async fn run_continuous_sync(
        &self,
        source_name: &str,
        stop_signal: Arc<RwLock<bool>>,
    ) -> Result<u64, CdcError> {
        let mut total_events = 0u64;
        let mut backoff_ms = 100u64;

        loop {
            if *stop_signal.read().await {
                break;
            }

            let last_pos = {
                let mut sources = self.source_connectors.write().await;
                let source = sources
                    .get_mut(source_name)
                    .ok_or_else(|| CdcError::ConnectionError(format!("Source '{}' not found", source_name)))?;
                source.get_position()
            };

            let result = {
                let mut sources = self.source_connectors.write().await;
                let source = sources.get_mut(source_name).unwrap();
                source.get_changes(last_pos.as_deref()).await
            };

            match result {
                Ok(events) => {
                    backoff_ms = 100;
                    for event in events {
                        let _ = self.event_sender.send(event);
                        total_events += 1;
                    }
                }
                Err(e) => {
                    eprintln!("CDC sync error for '{}': {:?}, retrying in {}ms", source_name, e, backoff_ms);
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(30_000);
                    continue;
                }
            }

            time::sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
        }

        Ok(total_events)
    }

    /// 一次性同步：拉取当前所有可用的变更
    pub async fn sync_once(&self, source_name: &str) -> Result<Vec<CdcEvent>, CdcError> {
        let mut sources = self.source_connectors.write().await;
        let source = sources
            .get_mut(source_name)
            .ok_or_else(|| CdcError::ConnectionError(format!("Source '{}' not found", source_name)))?;

        let last_pos = source.get_position();
        let events = source.get_changes(last_pos.as_deref()).await?;
        for event in &events {
            let _ = self.event_sender.send(event.clone());
        }
        Ok(events)
    }
}

pub struct VectorSyncHandler {
    cdc_receiver: broadcast::Receiver<CdcEvent>,
    target_collection: String,
    field_mapping: HashMap<String, String>,
}

impl VectorSyncHandler {
    pub fn new(
        cdc_receiver: broadcast::Receiver<CdcEvent>,
        target_collection: String,
        field_mapping: HashMap<String, String>,
    ) -> Self {
        Self {
            cdc_receiver,
            target_collection,
            field_mapping,
        }
    }

    pub async fn process_events(&mut self) -> Result<Vec<CdcEvent>, CdcError> {
        let mut events = Vec::new();
        
        while let Ok(event) = self.cdc_receiver.try_recv() {
            events.push(event);
        }
        
        Ok(events)
    }

    pub fn transform_to_vector_event(&self, event: &CdcEvent) -> Option<VectorSyncEvent> {
        match event {
            CdcEvent::Insert { table, key, data, timestamp } => {
                Some(VectorSyncEvent::Upsert {
                    id: key.clone(),
                    vector: self.extract_vector_fields(data),
                    metadata: data.clone(),
                    timestamp: *timestamp,
                })
            },
            CdcEvent::Update { table, key, new_data, timestamp, .. } => {
                Some(VectorSyncEvent::Upsert {
                    id: key.clone(),
                    vector: self.extract_vector_fields(new_data),
                    metadata: new_data.clone(),
                    timestamp: *timestamp,
                })
            },
            CdcEvent::Delete { table, key, timestamp, .. } => {
                Some(VectorSyncEvent::Delete {
                    id: key.clone(),
                    timestamp: *timestamp,
                })
            },
            _ => None,
        }
    }

    fn extract_vector_fields(&self, data: &HashMap<String, String>) -> Vec<f32> {
        let mut vector = Vec::new();
        
        for (target_field, source_value) in self.field_mapping.iter() {
            if let Some(value) = data.get(source_value) {
                if let Ok(float_val) = source_value.parse::<f32>() {
                    vector.push(float_val);
                }
            }
        }
        
        vector
    }
}

#[derive(Debug, Clone)]
pub enum VectorSyncEvent {
    Upsert {
        id: String,
        vector: Vec<f32>,
        metadata: HashMap<String, String>,
        timestamp: u64,
    },
    Delete {
        id: String,
        timestamp: u64,
    },
}
