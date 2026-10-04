//! D1 — one place to turn observability on.
//!
//! Three metric stacks had accumulated side by side: a hand-rolled
//! `PrometheusMetrics`, a `DatabaseMetrics` wrapper around it (the one the
//! `/metrics` endpoint actually serves), and the official `metrics` crate
//! with its Prometheus exporter installed but never used. Meanwhile
//! `tracing` events were emitted with no subscriber configured, so they went
//! nowhere. This module is the single switch: it installs the tracing
//! subscriber, owns the `DatabaseMetrics` the endpoint renders, wires the C5
//! command observer onto the database, and folds the stage-C state —
//! replication position, replica read-only flag, cluster slots, slow
//! queries — into the *same* Prometheus text rather than a second export.
//!
//! An application calls [`Telemetry::init`] once and hands
//! [`Telemetry::database_metrics`] to its HTTP layer; nothing here needs the
//! REST module, which keeps this free of the file another session is editing.
//!
//! Metrics named here avoid the ones `DatabaseMetrics` already owns
//! (`coretexdb_uptime_seconds`, `coretexdb_collections_count`, …) so the two
//! halves cannot collide in one exposition.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::coretex_cluster::ClusterInfo;
use crate::coretex_core::{CoreTexError, Result};
use crate::coretex_monitoring::{DatabaseMetrics, SlowQueryConfig, SlowQueryLogger};
use crate::coretex_stats::OperationObserver;
use crate::CoreTexDB;

/// How the telemetry surface starts.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Name attached to tracing output.
    pub service_name: String,
    /// `EnvFilter` directives (e.g. `"info,coretexdb=debug"`). `None` uses
    /// the default filter.
    pub filter_directives: Option<String>,
    /// Install the tracing subscriber. Off in tests that want no output.
    pub enable_tracing: bool,
    /// Queries slower than this are logged as slow (milliseconds).
    pub slow_query_threshold_ms: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: "coretexdb".to_string(),
            filter_directives: None,
            enable_tracing: true,
            slow_query_threshold_ms: 100,
        }
    }
}

/// The unified observability surface.
pub struct Telemetry {
    config: TelemetryConfig,
    database_metrics: Arc<DatabaseMetrics>,
    commands: Arc<OperationObserver>,
    slow_queries: Arc<SlowQueryLogger>,
    started: Instant,
}

impl Telemetry {
    /// Start observability: install the tracing subscriber (idempotent — a
    /// second call in the same process leaves the first subscriber in place
    /// rather than failing), and build the metrics/slow-query/command
    /// surfaces.
    pub fn init(config: TelemetryConfig) -> Result<Self> {
        if config.enable_tracing {
            // `try_init` rather than `init`: the global subscriber can only
            // be set once per process, and a second call is a no-op, not an
            // error worth propagating.
            let installed = match &config.filter_directives {
                Some(directives) => {
                    let filter = tracing_subscriber::EnvFilter::try_new(directives).map_err(
                        |e| {
                            CoreTexError::Other(format!(
                                "invalid tracing filter directives '{directives}': {e}"
                            ))
                        },
                    )?;
                    tracing_subscriber::fmt()
                        .with_env_filter(filter)
                        .try_init()
                }
                None => tracing_subscriber::fmt().try_init(),
            };
            if let Err(e) = installed {
                log::debug!("tracing subscriber already installed ({e}); keeping it");
            }
        }

        let slow_queries = Arc::new(SlowQueryLogger::new(SlowQueryConfig {
            enabled: true,
            slow_threshold_ms: config.slow_query_threshold_ms,
            ..SlowQueryConfig::default()
        }));

        Ok(Self {
            config,
            database_metrics: Arc::new(DatabaseMetrics::new()),
            commands: Arc::new(OperationObserver::new()),
            slow_queries,
            started: Instant::now(),
        })
    }

    /// The metrics instance the `/metrics` endpoint should render. Handing
    /// this existing `Arc` over keeps the endpoint working without touching
    /// its handler.
    pub fn database_metrics(&self) -> &Arc<DatabaseMetrics> {
        &self.database_metrics
    }

    /// Command statistics (C5), already attached by [`Self::attach_database`].
    pub fn commands(&self) -> &Arc<OperationObserver> {
        &self.commands
    }

    /// The slow-query log.
    pub fn slow_queries(&self) -> &Arc<SlowQueryLogger> {
        &self.slow_queries
    }

    /// Config this surface was built with.
    pub fn config(&self) -> &TelemetryConfig {
        &self.config
    }

    /// Seconds since this surface started.
    pub fn uptime_seconds(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Wire the database into this surface: command statistics start being
    /// counted, and slow queries land in this surface's log.
    ///
    /// One-shot, like the observer it sets. Calling it twice reports why
    /// rather than silently doing nothing.
    pub fn attach_database(&self, db: &CoreTexDB) -> Result<()> {
        // The observer carries its own slow-query logger; route it to ours so
        // slow entries show up in the same place as everything else.
        db.set_operation_observer(self.commands.clone())
    }

    /// Fold current database state into the metric gauges.
    ///
    /// Cheap enough to call on every scrape: a handful of counters and two
    /// map lengths.
    pub async fn refresh(&self, db: &CoreTexDB, cluster: Option<&ClusterInfo>) {
        let metrics = &self.database_metrics;

        metrics
            .publish_gauge(
                "coretexdb_replication_lsn",
                db.data_manager.replication_lsn().await as f64,
                None,
            )
            .await;
        metrics
            .publish_gauge(
                "coretexdb_read_only",
                if db.data_manager.read_only() { 1.0 } else { 0.0 },
                None,
            )
            .await;
        metrics
            .publish_gauge(
                "coretexdb_wal_enabled",
                if db.config.wal_enabled { 1.0 } else { 0.0 },
                None,
            )
            .await;

        metrics
            .set_collection_count(db.data_manager.get_collection_names().await.len())
            .await;
        metrics
            .set_vector_count(db.data_manager.get_total_vector_count().await)
            .await;

        // Command statistics (C5) as per-command series.
        let stats = self.commands.snapshot();
        metrics
            .publish_gauge("coretexdb_commands_total", stats.total_calls as f64, None)
            .await;
        for (command, stat) in &stats.commands {
            let mut labels = HashMap::new();
            labels.insert("command".to_string(), command.clone());
            metrics
                .publish_gauge(
                    "coretexdb_command_calls",
                    stat.calls as f64,
                    Some(labels.clone()),
                )
                .await;
            metrics
                .publish_gauge(
                    "coretexdb_command_errors",
                    stat.errors as f64,
                    Some(labels.clone()),
                )
                .await;
            metrics
                .publish_gauge(
                    "coretexdb_command_mean_ms",
                    stat.mean_ms(),
                    Some(labels),
                )
                .await;
        }

        // Slow queries.
        let slow = self.slow_queries.get_slow_queries().await;
        metrics
            .publish_gauge("coretexdb_slow_queries", slow.len() as f64, None)
            .await;
        let slowest = slow.iter().map(|e| e.duration_ms).fold(0.0_f64, f64::max);
        metrics
            .publish_gauge("coretexdb_slowest_query_ms", slowest, None)
            .await;

        // Cluster routing (C2), when the process knows about a cluster.
        if let Some(cluster) = cluster {
            for node in &cluster.nodes {
                let mut labels = HashMap::new();
                labels.insert("node".to_string(), node.node.id.clone());
                metrics
                    .publish_gauge(
                        "coretexdb_cluster_slots",
                        node.slots as f64,
                        Some(labels.clone()),
                    )
                    .await;
                metrics
                    .publish_gauge(
                        "coretexdb_cluster_collections",
                        node.collections as f64,
                        Some(labels),
                    )
                    .await;
            }
            metrics
                .publish_gauge(
                    "coretexdb_cluster_unassigned_slots",
                    cluster.unassigned_slots as f64,
                    None,
                )
                .await;
        }
    }

    /// Refresh, then render the whole Prometheus exposition — the same text
    /// the `/metrics` endpoint serves, plus the stage-C gauges.
    pub async fn render_prometheus(&self, db: &CoreTexDB, cluster: Option<&ClusterInfo>) -> String {
        self.refresh(db, cluster).await;
        self.database_metrics.get_prometheus_metrics().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbConfig;

    fn config() -> TelemetryConfig {
        TelemetryConfig {
            // Tests do not need log output, and a subscriber can only be set
            // once per process — installing it would leak into other tests.
            enable_tracing: false,
            slow_query_threshold_ms: u64::MAX,
            ..TelemetryConfig::default()
        }
    }

    #[test]
    fn init_twice_is_a_no_op_not_a_failure() {
        let _a = Telemetry::init(config()).expect("first init");
        let _b = Telemetry::init(config()).expect("second init is fine");
    }

    #[test]
    fn invalid_filter_directives_are_reported() {
        let mut cfg = config();
        cfg.enable_tracing = true;
        // `notalevel` is not a level, so the directive cannot be parsed.
        cfg.filter_directives = Some("coretexdb=notalevel".to_string());
        let err = match Telemetry::init(cfg) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("invalid filter directives must be refused"),
        };
        assert!(err.contains("invalid tracing filter directives"), "got: {err}");
    }

    #[tokio::test]
    async fn refresh_publishes_stage_c_gauges() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = DbConfig::new(&dir.path().to_string_lossy());
        cfg.wal_enabled = true;
        let db = CoreTexDB::with_config(cfg);
        db.init().await.unwrap();
        db.create_collection_with_index("docs", 4, "euclidean", "brute_force")
            .await
            .unwrap();
        db.insert_vectors(
            "docs",
            vec![("a".into(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({}))],
        )
        .await
        .unwrap();

        let telemetry = Telemetry::init(config()).unwrap();
        telemetry.attach_database(&db).unwrap();
        db.search("docs", vec![1.0, 0.0, 0.0, 0.0], 1, None).await.unwrap();

        let text = telemetry.render_prometheus(&db, None).await;
        assert!(text.contains("coretexdb_replication_lsn"), "{text}");
        assert!(text.contains("coretexdb_wal_enabled 1"), "{text}");
        assert!(text.contains("coretexdb_read_only 0"), "{text}");
        assert!(text.contains("coretexdb_collections_count 1"), "{text}");
        assert!(text.contains("coretexdb_vectors_count 1"), "{text}");
        assert!(text.contains("coretexdb_commands_total 1"), "{text}");
        assert!(text.contains("coretexdb_command_calls"), "{text}");
        assert!(text.contains("coretexdb_slow_queries 0"), "{text}");
    }

    #[tokio::test]
    async fn cluster_slots_appear_when_routing_is_supplied() {
        let dir = tempfile::tempdir().unwrap();
        let db = CoreTexDB::with_config(DbConfig::new(&dir.path().to_string_lossy()));
        db.init().await.unwrap();

        let router = Arc::new(
            crate::coretex_cluster::ClusterRouter::new(vec![
                crate::coretex_cluster::NodeInfo::new("n1", "http://a"),
                crate::coretex_cluster::NodeInfo::new("n2", "http://b"),
            ])
            .unwrap(),
        );
        router.assign_collection("docs", "n1").await.unwrap();

        let telemetry = Telemetry::init(config()).unwrap();
        let text = telemetry
            .render_prometheus(&db, Some(&router.cluster_info().await))
            .await;
        assert!(text.contains("coretexdb_cluster_slots"), "{text}");
        assert!(text.contains("coretexdb_cluster_unassigned_slots 16383"), "{text}");
    }

    #[tokio::test]
    async fn attaching_twice_explains_why() {
        let dir = tempfile::tempdir().unwrap();
        let db = CoreTexDB::with_config(DbConfig::new(&dir.path().to_string_lossy()));
        db.init().await.unwrap();

        let telemetry = Telemetry::init(config()).unwrap();
        telemetry.attach_database(&db).unwrap();
        let err = match telemetry.attach_database(&db) {
            Err(e) => e.to_string(),
            Ok(()) => panic!("a second attach must report why it did nothing"),
        };
        assert!(err.contains("already set"), "got: {err}");
    }
}