//! C5 — command statistics, slow-query logging and the INFO surface.
//!
//! `SlowQueryLogger` has been complete for a long time and nothing ever
//! called it; the same goes for the counters behind `/metrics`. This module
//! supplies the missing piece — an observer the library's own entry points
//! report to — and turns the accumulated numbers into an INFO report a human
//! or a script can read.
//!
//! Cost when nothing is attached: one `Option` check per entry point.
//! [`OperationObserver::timer`] returns `None` and the caller's `finish`
//! line becomes a no-op, so an unobserved database pays no clock reads and
//! no locking.

use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::coretex_cluster::ClusterInfo;
use crate::coretex_monitoring::{SlowQueryConfig, SlowQueryLogger};
use crate::CoreTexDB;

/// Counters for one command (an operation name such as `search`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CommandStat {
    /// Successful calls.
    pub calls: u64,
    /// Calls that returned an error.
    pub errors: u64,
    /// Sum of durations, milliseconds.
    pub total_ms: f64,
    /// Slowest single call, milliseconds.
    pub max_ms: f64,
}

impl CommandStat {
    /// Mean duration, milliseconds (0 when never called).
    pub fn mean_ms(&self) -> f64 {
        let total = self.calls + self.errors;
        if total == 0 {
            0.0
        } else {
            self.total_ms / total as f64
        }
    }
}

/// A point-in-time copy of the counters.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CommandStats {
    pub commands: HashMap<String, CommandStat>,
    pub total_calls: u64,
    pub uptime_seconds: u64,
}

/// Counters plus the slow-query log, shared across entry points.
pub struct OperationObserver {
    commands: StdRwLock<HashMap<String, CommandStat>>,
    started: Instant,
    slow_queries: Option<Arc<SlowQueryLogger>>,
}

impl Default for OperationObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationObserver {
    /// An observer that only counts.
    pub fn new() -> Self {
        Self {
            commands: StdRwLock::new(HashMap::new()),
            started: Instant::now(),
            slow_queries: None,
        }
    }

    /// Also record queries slower than `config.slow_threshold_ms` into a
    /// `SlowQueryLogger` (which also writes them to `config.log_path`).
    pub fn with_slow_query_logging(mut self, config: SlowQueryConfig) -> Self {
        self.slow_queries = Some(Arc::new(SlowQueryLogger::new(config)));
        self
    }

    /// The slow-query log, when one is attached.
    pub fn slow_queries(&self) -> Option<&Arc<SlowQueryLogger>> {
        self.slow_queries.as_ref()
    }

    /// Start observing one operation. `None` means no observer is attached and
    /// the caller should skip the matching `finish`. The strings are owned so
    /// a timer can outlive the borrow of the collection name it describes.
    pub fn timer(
        &self,
        command: &str,
        collection: &str,
        params: &str,
    ) -> Option<OperationTimer<'_>> {
        Some(OperationTimer {
            observer: self,
            command: command.to_string(),
            collection: collection.to_string(),
            params: params.to_string(),
            started: Instant::now(),
        })
    }

    /// Record one finished operation.
    pub async fn record(
        &self,
        command: &str,
        started: Instant,
        collection: &str,
        params: &str,
        ok: bool,
    ) {
        let elapsed = started.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        {
            let mut commands = match self.commands.write() {
                Ok(guard) => guard,
                // A poisoned statistics lock must not fail a query.
                Err(poisoned) => poisoned.into_inner(),
            };
            let stat = commands.entry(command.to_string()).or_default();
            if ok {
                stat.calls += 1;
            } else {
                stat.errors += 1;
            }
            stat.total_ms += ms;
            if ms > stat.max_ms {
                stat.max_ms = ms;
            }
        }
        if let Some(logger) = &self.slow_queries {
            logger
                .record_query(command, ms, collection, params)
                .await;
        }
    }

    /// Current counters.
    pub fn snapshot(&self) -> CommandStats {
        let commands = match self.commands.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let total_calls = commands
            .values()
            .map(|s| s.calls + s.errors)
            .sum();
        CommandStats {
            commands,
            total_calls,
            uptime_seconds: self.started.elapsed().as_secs(),
        }
    }
}

/// In-flight operation handle produced by [`OperationObserver::timer`].
pub struct OperationTimer<'a> {
    observer: &'a OperationObserver,
    command: String,
    collection: String,
    params: String,
    started: Instant,
}

impl OperationTimer<'_> {
    /// Record the operation's outcome. Awaited once, at the end of the
    /// operation.
    pub async fn finish(self, ok: bool) {
        self.observer
            .record(
                &self.command,
                self.started,
                &self.collection,
                &self.params,
                ok,
            )
            .await;
    }
}

/// One collection's keyspace line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub vectors: usize,
}

/// The INFO report: server identity, keyspace, statistics, replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub version: String,
    /// `standalone` for a single node; the cluster section carries the rest.
    pub mode: String,
    /// Whether this node refuses writes (a replica).
    pub read_only: bool,
    pub uptime_seconds: u64,
    pub wal_enabled: bool,
    /// Replication log position (0 without a WAL).
    pub lsn: u64,
    pub total_vectors: usize,
    pub collections: Vec<CollectionInfo>,
    pub stats: CommandStats,
    pub slow_query_count: usize,
    pub slowest_query_ms: f64,
    /// Present when the caller supplied cluster routing information.
    pub cluster: Option<ClusterInfo>,
}

impl ServerInfo {
    /// Redis-style text rendering: `# Section` headers and `key:value` lines.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str("# Server\n");
        out.push_str(&format!("coretexdb_version:{}\n", self.version));
        out.push_str(&format!("mode:{}\n", self.mode));
        out.push_str(&format!("read_only:{}\n", self.read_only));
        out.push_str(&format!("uptime_in_seconds:{}\n", self.uptime_seconds));

        out.push_str("# Replication\n");
        out.push_str(&format!("wal_enabled:{}\n", self.wal_enabled));
        out.push_str(&format!("replication_lsn:{}\n", self.lsn));

        out.push_str("# Keyspace\n");
        out.push_str(&format!("total_vectors:{}\n", self.total_vectors));
        out.push_str(&format!("collection_count:{}\n", self.collections.len()));
        for collection in &self.collections {
            out.push_str(&format!(
                "coretexdb_collection:{}:vectors={}\n",
                collection.name, collection.vectors
            ));
        }

        out.push_str("# Stats\n");
        out.push_str(&format!(
            "total_commands_processed:{}\n",
            self.stats.total_calls
        ));
        let mut names: Vec<&String> = self.stats.commands.keys().collect();
        names.sort();
        for name in names {
            let stat = &self.stats.commands[name];
            out.push_str(&format!(
                "cmdstat_{}:calls={},errors={},mean_ms={:.3},max_ms={:.3}\n",
                name,
                stat.calls,
                stat.errors,
                stat.mean_ms(),
                stat.max_ms
            ));
        }
        out.push_str(&format!("slow_query_count:{}\n", self.slow_query_count));
        out.push_str(&format!("slowest_query_ms:{:.3}\n", self.slowest_query_ms));

        if let Some(cluster) = &self.cluster {
            out.push_str("# Cluster\n");
            for node in &cluster.nodes {
                out.push_str(&format!(
                    "cluster_node:{}:slots={},collections={}\n",
                    node.node.id, node.slots, node.collections
                ));
            }
            out.push_str(&format!(
                "cluster_unassigned_slots:{}\n",
                cluster.unassigned_slots
            ));
        }

        out
    }
}

/// Collect the INFO report for `db`. `observer` supplies the statistics
/// (without one, every counter reads zero); `cluster` adds the routing
/// section when the process knows about a cluster.
pub async fn collect_info(
    db: &CoreTexDB,
    observer: Option<&OperationObserver>,
    cluster: Option<ClusterInfo>,
) -> ServerInfo {
    let mut collections = Vec::new();
    let mut total_vectors = 0usize;
    for name in db.data_manager.get_collection_names().await {
        let vectors = db
            .data_manager
            .get_vectors_count(&name)
            .await
            .unwrap_or(0);
        total_vectors += vectors;
        collections.push(CollectionInfo { name, vectors });
    }
    collections.sort_by(|a, b| a.name.cmp(&b.name));

    let (slow_query_count, slowest_query_ms) = match observer.and_then(|o| o.slow_queries()) {
        Some(logger) => {
            let entries = logger.get_slow_queries().await;
            let slowest = entries.iter().map(|e| e.duration_ms).fold(0.0_f64, f64::max);
            (entries.len(), slowest)
        }
        None => (0, 0.0),
    };

    let stats = observer.map(|o| o.snapshot()).unwrap_or_default();
    let uptime = stats.uptime_seconds;

    ServerInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        mode: "standalone".to_string(),
        read_only: db.data_manager.read_only(),
        uptime_seconds: uptime,
        wal_enabled: db.config.wal_enabled,
        lsn: db.data_manager.replication_lsn().await,
        total_vectors,
        collections,
        stats,
        slow_query_count,
        slowest_query_ms,
        cluster,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn counters_track_calls_errors_and_duration() {
        let observer = OperationObserver::new();
        observer
            .record("search", Instant::now(), "docs", "k=10", true)
            .await;
        observer
            .record("search", Instant::now(), "docs", "k=10", false)
            .await;
        observer
            .record("insert", Instant::now(), "docs", "n=3", true)
            .await;

        let stats = observer.snapshot();
        assert_eq!(stats.total_calls, 3);
        let search = &stats.commands["search"];
        assert_eq!(search.calls, 1);
        assert_eq!(search.errors, 1);
        assert_eq!(stats.commands["insert"].calls, 1);
        assert!(stats.commands["search"].mean_ms() >= 0.0);
    }

    #[tokio::test]
    async fn slow_queries_respect_the_threshold() {
        let mut config = SlowQueryConfig::default();
        config.enabled = true;
        config.slow_threshold_ms = 0; // everything is slow
        config.log_path = std::env::temp_dir()
            .join("coretex-stats-test.log")
            .to_string_lossy()
            .to_string();
        let observer = OperationObserver::new().with_slow_query_logging(config);

        observer
            .record("search", Instant::now(), "docs", "k=10", true)
            .await;

        let logger = observer.slow_queries().expect("logger attached");
        let entries = logger.get_slow_queries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].query_type, "search");
        assert_eq!(entries[0].collection, "docs");
        assert_eq!(entries[0].query_params, "k=10");
    }

    #[test]
    fn info_text_carries_the_sections() {
        let info = ServerInfo {
            version: "0.2.4".to_string(),
            mode: "standalone".to_string(),
            read_only: true,
            uptime_seconds: 42,
            wal_enabled: true,
            lsn: 7,
            total_vectors: 3,
            collections: vec![CollectionInfo {
                name: "docs".to_string(),
                vectors: 3,
            }],
            stats: CommandStats {
                commands: HashMap::from([(
                    "search".to_string(),
                    CommandStat {
                        calls: 2,
                        errors: 0,
                        total_ms: 4.0,
                        max_ms: 3.0,
                    },
                )]),
                total_calls: 2,
                uptime_seconds: 42,
            },
            slow_query_count: 1,
            slowest_query_ms: 12.5,
            cluster: Some(ClusterInfo {
                nodes: vec![crate::coretex_cluster::NodeRouting {
                    node: crate::coretex_cluster::NodeInfo::new("n1", "http://x"),
                    slots: 2,
                    collections: 1,
                }],
                unassigned_slots: 16382,
            }),
        };

        let text = info.to_text();
        assert!(text.contains("# Server"));
        assert!(text.contains("coretexdb_version:0.2.4"));
        assert!(text.contains("read_only:true"));
        assert!(text.contains("coretexdb_collection:docs:vectors=3"));
        assert!(text.contains("cmdstat_search:calls=2"));
        assert!(text.contains("cluster_node:n1:slots=2"));
    }
}