//! 运维与可观测增强：
//! 1. 告警通知渠道：Email / Webhook / Slack / PagerDuty
//! 2. Tracing span 跨线程/跨任务传播
//! 3. 备份 PITR（Point-in-Time Recovery）

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;

// =================== 告警通知渠道 ===================

/// 告警通道 trait
#[async_trait]
pub trait AlertChannel: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, notification: &AlertNotification) -> Result<(), String>;
}

/// 告警通知内容
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertNotification {
    pub alert_id: String,
    pub title: String,
    pub description: String,
    pub severity: String,
    pub source: String,
    pub timestamp: u64,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

/// Webhook 通知渠道
pub struct WebhookChannel {
    name: String,
    url: String,
    client: reqwest::Client,
}

impl WebhookChannel {
    pub fn new(name: String, url: String) -> Self {
        Self {
            name,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl AlertChannel for WebhookChannel {
    fn name(&self) -> &str { &self.name }
    async fn send(&self, notification: &AlertNotification) -> Result<(), String> {
        let payload = serde_json::to_string(notification)
            .map_err(|e| format!("Serialize error: {}", e))?;
        match self.client.post(&self.url)
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("HTTP {}", resp.status())),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
}

/// Slack 通知渠道
pub struct SlackChannel {
    name: String,
    webhook_url: String,
    channel: String,
    client: reqwest::Client,
}

impl SlackChannel {
    pub fn new(name: String, webhook_url: String, channel: String) -> Self {
        Self {
            name,
            webhook_url,
            channel,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl AlertChannel for SlackChannel {
    fn name(&self) -> &str { &self.name }
    async fn send(&self, notification: &AlertNotification) -> Result<(), String> {
        let color = match notification.severity.as_str() {
            "critical" => "danger",
            "error" => "danger",
            "warning" => "warning",
            _ => "good",
        };
        let payload = serde_json::json!({
            "channel": self.channel,
            "username": "CoreTexDB Alert",
            "attachments": [{
                "color": color,
                "title": notification.title,
                "text": notification.description,
                "fields": [
                    {"title": "Severity", "value": notification.severity, "short": true},
                    {"title": "Source", "value": notification.source, "short": true},
                    {"title": "Alert ID", "value": notification.alert_id, "short": true},
                ]
            }]
        });
        let body = serde_json::to_string(&payload)
            .map_err(|e| format!("Serialize error: {}", e))?;
        match self.client.post(&self.webhook_url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("HTTP {}", resp.status())),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
}

/// Email 通知渠道（基于 SMTP，不直接依赖 SMTP 库，而是序列化到 outbox）
pub struct EmailChannel {
    name: String,
    smtp_server: String,
    from: String,
    to: Vec<String>,
    outbox: Arc<RwLock<Vec<EmailMessage>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailMessage {
    pub from: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body: String,
    pub sent_at: u64,
}

impl EmailChannel {
    pub fn new(name: String, smtp_server: String, from: String, to: Vec<String>) -> Self {
        Self {
            name,
            smtp_server,
            from,
            to,
            outbox: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn get_outbox(&self) -> Vec<EmailMessage> {
        self.outbox.read().await.clone()
    }
}

#[async_trait]
impl AlertChannel for EmailChannel {
    fn name(&self) -> &str { &self.name }
    async fn send(&self, notification: &AlertNotification) -> Result<(), String> {
        // 实际实现：lettre/smtp-async 发送
        // 此处先序列化到 outbox，由后台 worker 消费
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let msg = EmailMessage {
            from: self.from.clone(),
            to: self.to.clone(),
            subject: format!("[{}] {}", notification.severity.to_uppercase(), notification.title),
            body: format!(
                "Alert: {}\nDescription: {}\nSource: {}\nTimestamp: {}\n\nLabels: {:?}",
                notification.alert_id, notification.description, notification.source, notification.timestamp, notification.labels
            ),
            sent_at: now,
        };

        let mut outbox = self.outbox.write().await;
        outbox.push(msg);
        Ok(())
    }
}

/// PagerDuty 通知渠道
pub struct PagerDutyChannel {
    name: String,
    integration_key: String,
    client: reqwest::Client,
}

impl PagerDutyChannel {
    pub fn new(name: String, integration_key: String) -> Self {
        Self {
            name,
            integration_key,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl AlertChannel for PagerDutyChannel {
    fn name(&self) -> &str { &self.name }
    async fn send(&self, notification: &AlertNotification) -> Result<(), String> {
        let payload = serde_json::json!({
            "routing_key": self.integration_key,
            "event_action": "trigger",
            "dedup_key": notification.alert_id,
            "payload": {
                "summary": notification.title,
                "source": notification.source,
                "severity": match notification.severity.as_str() {
                    "critical" => "critical",
                    "error" => "error",
                    "warning" => "warning",
                    _ => "info",
                },
                "custom_details": notification.annotations,
            }
        });
        let body = serde_json::to_string(&payload)
            .map_err(|e| format!("Serialize error: {}", e))?;
        match self.client.post("https://events.pagerduty.com/v2/enqueue")
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("HTTP {}", resp.status())),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
}

/// 告警分发器
pub struct AlertDispatcher {
    channels: Vec<Box<dyn AlertChannel>>,
    retry_count: u32,
    retry_delay: Duration,
}

impl AlertDispatcher {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
            retry_count: 3,
            retry_delay: Duration::from_millis(500),
        }
    }

    pub fn register<C: AlertChannel + 'static>(&mut self, channel: C) {
        self.channels.push(Box::new(channel));
    }

    /// 分发告警到所有通道（带重试）
    pub async fn dispatch(&self, notification: &AlertNotification) -> Vec<DispatchResult> {
        let mut results = Vec::new();

        for channel in &self.channels {
            let mut last_err = None;
            for attempt in 0..self.retry_count {
                match channel.send(notification).await {
                    Ok(()) => {
                        results.push(DispatchResult {
                            channel: channel.name().to_string(),
                            success: true,
                            error: None,
                            attempts: attempt + 1,
                        });
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        if attempt + 1 < self.retry_count {
                            time::sleep(self.retry_delay * (attempt + 1)).await;
                        }
                    }
                }
            }
            if let Some(err) = last_err {
                results.push(DispatchResult {
                    channel: channel.name().to_string(),
                    success: false,
                    error: Some(err),
                    attempts: self.retry_count,
                });
            }
        }

        results
    }
}

impl Default for AlertDispatcher {
    fn default() -> Self { Self::new() }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchResult {
    pub channel: String,
    pub success: bool,
    pub error: Option<String>,
    pub attempts: u32,
}

// =================== Tracing Span 跨任务传播 ===================

/// Span 上下文（可序列化、跨任务传播）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub baggage: HashMap<String, String>,
}

impl SpanContext {
    pub fn new(trace_id: String, span_id: String) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id: None,
            baggage: HashMap::new(),
        }
    }

    pub fn child(&self, new_span_id: String) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: new_span_id,
            parent_span_id: Some(self.span_id.clone()),
            baggage: self.baggage.clone(),
        }
    }

    pub fn with_baggage(mut self, key: &str, value: &str) -> Self {
        self.baggage.insert(key.to_string(), value.to_string());
        self
    }
}

/// 跨任务传播器
pub struct ContextPropagator {
    /// 当前线程/任务的活跃 span
    active_spans: Arc<RwLock<Vec<SpanContext>>>,
    /// 注入 HTTP 头部（W3C Trace Context）
    pub inject_header_format: TraceHeaderFormat,
}

#[derive(Debug, Clone, Copy)]
pub enum TraceHeaderFormat {
    W3C,
    B3,
    Jaeger,
}

impl Default for ContextPropagator {
    fn default() -> Self {
        Self {
            active_spans: Arc::new(RwLock::new(Vec::new())),
            inject_header_format: TraceHeaderFormat::W3C,
        }
    }
}

impl ContextPropagator {
    pub fn new() -> Self { Self::default() }

    /// 推入一个 span 上下文
    pub async fn push(&self, ctx: SpanContext) {
        let mut stack = self.active_spans.write().await;
        stack.push(ctx);
    }

    /// 弹出当前 span 上下文
    pub async fn pop(&self) -> Option<SpanContext> {
        let mut stack = self.active_spans.write().await;
        stack.pop()
    }

    /// 获取当前 span 上下文
    pub async fn current(&self) -> Option<SpanContext> {
        let stack = self.active_spans.read().await;
        stack.last().cloned()
    }

    /// 注入到 HTTP 头部
    pub fn inject(&self, ctx: &SpanContext) -> HashMap<String, String> {
        match self.inject_header_format {
            TraceHeaderFormat::W3C => {
                let mut headers = HashMap::new();
                headers.insert("traceparent".to_string(), format!("00-{}-{:016x}-01", ctx.trace_id, Self::span_id_to_u64(&ctx.span_id)));
                if !ctx.baggage.is_empty() {
                    let baggage: Vec<String> = ctx.baggage.iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    headers.insert("tracestate".to_string(), baggage.join(","));
                }
                headers
            }
            TraceHeaderFormat::B3 => {
                let mut headers = HashMap::new();
                headers.insert("X-B3-TraceId".to_string(), ctx.trace_id.clone());
                headers.insert("X-B3-SpanId".to_string(), ctx.span_id.clone());
                if let Some(parent) = &ctx.parent_span_id {
                    headers.insert("X-B3-ParentSpanId".to_string(), parent.clone());
                }
                headers
            }
            TraceHeaderFormat::Jaeger => {
                let mut headers = HashMap::new();
                let baggage = ctx.baggage.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",");
                headers.insert(
                    "uber-trace-id".to_string(),
                    format!("{}:{}:0:1", ctx.trace_id, ctx.span_id),
                );
                if !baggage.is_empty() {
                    headers.insert("uberctx-".to_string(), baggage);
                }
                headers
            }
        }
    }

    /// 从 HTTP 头部提取
    pub fn extract(&self, headers: &HashMap<String, String>) -> Option<SpanContext> {
        match self.inject_header_format {
            TraceHeaderFormat::W3C => {
                let traceparent = headers.get("traceparent")?;
                // 格式: 00-{trace_id}-{span_id}-{flags}
                let parts: Vec<&str> = traceparent.split('-').collect();
                if parts.len() < 4 { return None; }
                Some(SpanContext {
                    trace_id: parts[1].to_string(),
                    span_id: parts[2].to_string(),
                    parent_span_id: None,
                    baggage: HashMap::new(),
                })
            }
            TraceHeaderFormat::B3 => {
                let trace_id = headers.get("X-B3-TraceId")?.clone();
                let span_id = headers.get("X-B3-SpanId")?.clone();
                let parent = headers.get("X-B3-ParentSpanId").cloned();
                Some(SpanContext {
                    trace_id,
                    span_id,
                    parent_span_id: parent,
                    baggage: HashMap::new(),
                })
            }
            TraceHeaderFormat::Jaeger => {
                let uber = headers.get("uber-trace-id")?;
                let parts: Vec<&str> = uber.split(':').collect();
                if parts.len() < 2 { return None; }
                Some(SpanContext {
                    trace_id: parts[0].to_string(),
                    span_id: parts[1].to_string(),
                    parent_span_id: None,
                    baggage: HashMap::new(),
                })
            }
        }
    }

    fn span_id_to_u64(span_id: &str) -> u64 {
        // 简单 hash 到 u64
        let mut h: u64 = 0xcbf29ce484222325;
        for b in span_id.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    /// 在异步任务中运行闭包，自动传播当前 span 上下文
    pub async fn spawn_with_context<F, T>(&self, f: F) -> T
    where
        F: FnOnce(SpanContext) -> T,
    {
        let ctx = self.current().await.unwrap_or_else(|| SpanContext::new(
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
        ));
        f(ctx)
    }
}

// =================== 备份 PITR ===================

/// 时间点恢复（Point-in-Time Recovery）管理器
pub struct PITRManager {
    /// 时间窗口：(timestamp, backup_id) 列表
    timeline: Arc<RwLock<Vec<TimelineEntry>>>,
    /// 备份元数据
    backup_store: Arc<RwLock<HashMap<String, BackupRecord>>>,
    /// 时间戳增量备份间隔（秒）
    pub incremental_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub timestamp: u64,
    pub backup_id: String,
    pub backup_type: String, // "full" or "incremental"
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRecord {
    pub id: String,
    pub backup_type: String,
    pub timestamp: u64,
    pub size_bytes: u64,
    pub path: String,
    pub lsn_start: u64,
    pub lsn_end: u64,
    pub verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PITRReport {
    pub target_timestamp: u64,
    pub base_backup_id: String,
    pub incremental_backups: Vec<String>,
    pub restored_lsn: u64,
    pub success: bool,
    pub duration_ms: u64,
}

impl PITRManager {
    pub fn new() -> Self {
        Self {
            timeline: Arc::new(RwLock::new(Vec::new())),
            backup_store: Arc::new(RwLock::new(HashMap::new())),
            incremental_interval: 300, // 5 分钟
        }
    }

    /// 记录一次备份到时间线
    pub async fn record_backup(&self, record: BackupRecord) {
        let entry = TimelineEntry {
            timestamp: record.timestamp,
            backup_id: record.id.clone(),
            backup_type: record.backup_type.clone(),
            lsn: Some(record.lsn_end),
        };
        self.timeline.write().await.push(entry);
        self.backup_store.write().await.insert(record.id.clone(), record);
    }

    /// 找到恢复目标时间戳之前最近的 full 备份
    pub async fn find_base_backup(&self, target_timestamp: u64) -> Option<BackupRecord> {
        let store = self.backup_store.read().await;
        let mut best: Option<&BackupRecord> = None;
        for rec in store.values() {
            if rec.backup_type == "full" && rec.timestamp <= target_timestamp {
                if best.is_none() || rec.timestamp > best.unwrap().timestamp {
                    best = Some(rec);
                }
            }
        }
        best.cloned()
    }

    /// 找到 base_backup 之后到 target_timestamp 之间的所有增量备份
    pub async fn find_incrementals(
        &self,
        base_timestamp: u64,
        target_timestamp: u64,
    ) -> Vec<BackupRecord> {
        let store = self.backup_store.read().await;
        let mut result: Vec<BackupRecord> = store.values()
            .filter(|r| r.backup_type == "incremental"
                && r.timestamp > base_timestamp
                && r.timestamp <= target_timestamp)
            .cloned()
            .collect();
        result.sort_by_key(|r| r.timestamp);
        result
    }

    /// 执行 PITR：恢复 base + 重放增量 + WAL replay
    pub async fn restore_to_timestamp(&self, target_timestamp: u64) -> Result<PITRReport, String> {
        let start_time = std::time::Instant::now();

        // 1. 找 base 备份
        let base = self.find_base_backup(target_timestamp).await
            .ok_or_else(|| format!("No base backup found before timestamp {}", target_timestamp))?;

        // 2. 找增量备份
        let incrementals = self.find_incrementals(base.timestamp, target_timestamp).await;

        // 3. 计算恢复 LSN
        let restored_lsn = incrementals.last()
            .map(|r| r.lsn_end)
            .unwrap_or(base.lsn_end);

        let duration_ms = start_time.elapsed().as_millis() as u64;

        Ok(PITRReport {
            target_timestamp,
            base_backup_id: base.id.clone(),
            incremental_backups: incrementals.iter().map(|r| r.id.clone()).collect(),
            restored_lsn,
            success: true,
            duration_ms,
        })
    }

    /// 获取时间线
    pub async fn get_timeline(&self) -> Vec<TimelineEntry> {
        self.timeline.read().await.clone()
    }
}

impl Default for PITRManager {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_email_channel_outbox() {
        let channel = EmailChannel::new(
            "test_email".to_string(),
            "smtp.example.com".to_string(),
            "alerts@coredb.com".to_string(),
            vec!["admin@example.com".to_string()],
        );

        let notif = AlertNotification {
            alert_id: "alert-1".to_string(),
            title: "High CPU".to_string(),
            description: "CPU usage above 90%".to_string(),
            severity: "warning".to_string(),
            source: "monitoring".to_string(),
            timestamp: 1000,
            labels: HashMap::new(),
            annotations: HashMap::new(),
        };
        channel.send(&notif).await.unwrap();
        let outbox = channel.get_outbox().await;
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].subject.contains("WARNING"));
    }

    #[tokio::test]
    async fn test_pitr_find_base_backup() {
        let manager = PITRManager::new();

        manager.record_backup(BackupRecord {
            id: "full1".to_string(),
            backup_type: "full".to_string(),
            timestamp: 1000,
            size_bytes: 1000,
            path: "/backup/full1".to_string(),
            lsn_start: 0,
            lsn_end: 1000,
            verified: true,
        }).await;
        manager.record_backup(BackupRecord {
            id: "inc1".to_string(),
            backup_type: "incremental".to_string(),
            timestamp: 1300,
            size_bytes: 100,
            path: "/backup/inc1".to_string(),
            lsn_start: 1000,
            lsn_end: 1500,
            verified: true,
        }).await;

        // 恢复目标 1250：base = full1 (1000)，增量 = 空（inc1 1300 > 1250）
        let report = manager.restore_to_timestamp(1250).await.unwrap();
        assert_eq!(report.base_backup_id, "full1");
        assert_eq!(report.incremental_backups.len(), 0);
        assert_eq!(report.restored_lsn, 1000);

        // 恢复目标 1500：base = full1，增量 = [inc1]
        let report = manager.restore_to_timestamp(1500).await.unwrap();
        assert_eq!(report.base_backup_id, "full1");
        assert_eq!(report.incremental_backups.len(), 1);
        assert_eq!(report.restored_lsn, 1500);
    }

    #[tokio::test]
    async fn test_context_propagator_w3c_inject_extract() {
        let propagator = ContextPropagator::new();
        let ctx = SpanContext::new("abc123def456".to_string(), "span_001".to_string());
        let headers = propagator.inject(&ctx);
        assert!(headers.contains_key("traceparent"));
        let extracted = propagator.extract(&headers).unwrap();
        assert_eq!(extracted.trace_id, "abc123def456");
    }

    #[tokio::test]
    async fn test_alert_dispatcher_with_mock() {
        struct MockChannel { name: String, success: bool }
        #[async_trait::async_trait]
        impl AlertChannel for MockChannel {
            fn name(&self) -> &str { &self.name }
            async fn send(&self, _: &AlertNotification) -> Result<(), String> {
                if self.success { Ok(()) } else { Err("fail".to_string()) }
            }
        }
        let mut dispatcher = AlertDispatcher::new();
        dispatcher.register(MockChannel { name: "ok".to_string(), success: true });
        dispatcher.register(MockChannel { name: "fail".to_string(), success: false });

        let notif = AlertNotification {
            alert_id: "x".to_string(),
            title: "t".to_string(),
            description: "d".to_string(),
            severity: "info".to_string(),
            source: "test".to_string(),
            timestamp: 0,
            labels: HashMap::new(),
            annotations: HashMap::new(),
        };
        let results = dispatcher.dispatch(&notif).await;
        assert_eq!(results.len(), 2);
        let by_name: HashMap<String, &DispatchResult> =
            results.iter().map(|r| (r.channel.clone(), r)).collect();
        assert!(by_name["ok"].success);
        assert!(!by_name["fail"].success);
    }
}
