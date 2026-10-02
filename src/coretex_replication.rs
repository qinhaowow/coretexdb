//! Primary → replica replication over the real WAL (C1).
//!
//! Three pieces, deliberately independent of the half-wired Raft code in
//! [`crate::coretex_failover`]:
//!
//! * [`ReplicationSnapshot`] — a point-in-time full copy plus the log
//!   position (`lsn`) it corresponds to. Produced by
//!   [`crate::coretex_data::DataManager::replication_snapshot`], whose read
//!   order (position → schemas → records) guarantees everything not in the
//!   copy lies after `lsn` in the log.
//! * [`EntriesBatch`] — the incremental tail after an `lsn`, with a
//!   `truncated` flag: when the primary can no longer answer continuously
//!   (segments discarded, log reset) the replica must fall back to a full
//!   resync instead of silently skipping history.
//! * [`ReplicaSync`] — the replica-side state machine: fetch → apply →
//!   persist position. The local database is switched to read-only on
//!   construction; only replication replay (and startup recovery) may write.
//!
//! Transport is a trait so the same state machine drives an HTTP replica
//! ([`HttpTransport`]) and a same-process pair ([`InProcessTransport`],
//! used heavily by the integration tests).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::coretex_core::{CollectionSchema, CoreTexError, Result};
use crate::coretex_data::VectorRecord;
use crate::coretex_utils::wal::WalEntry;
use crate::CoreTexDB;

/// Full-sync payload: every collection and record, with the primary log
/// position they are consistent with.
///
/// Applying a snapshot is destructive (the replica is wiped first) and the
/// position is taken *before* the data is copied, so entries in
/// `(lsn, ∞)` may overlap what is already in the copy — every apply path
/// is idempotent to make that safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSnapshot {
    /// Primary log position consistent with this copy (0 = no WAL).
    pub lsn: u64,
    /// All collection schemas as of the snapshot.
    pub collections: Vec<CollectionSchema>,
    /// `collection → id → record`, as of the snapshot.
    pub records:
        std::collections::HashMap<String, std::collections::HashMap<String, VectorRecord>>,
}

/// Incremental transport unit: the log tail after a position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntriesBatch {
    /// Entries with `sequence > since`, in order.
    pub entries: Vec<WalEntry>,
    /// `false` = the batch is continuous from the requested position;
    /// `true` = history is missing and the replica must take a snapshot.
    pub truncated: bool,
    /// Primary position *at read time*. Derived from the entries themselves
    /// (`last sequence`, or the requested position when the batch is empty),
    /// never from a separately sampled counter — a watermark sampled after
    /// the entries could advertise a sequence that was never shipped.
    pub lsn: u64,
}

/// Point-in-time answer to "where is this primary, and is it a replica?".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    /// Current log position (`last_sequence`), 0 when WAL is disabled.
    pub lsn: u64,
    /// Whether this node refuses mutations.
    pub read_only: bool,
    /// Number of collections.
    pub collections: usize,
    /// Total records across collections.
    pub records: usize,
}

impl ReplicationStatus {
    /// Collect the status of `db` (cheap: counters only, no data copy).
    pub async fn collect(db: &CoreTexDB) -> Self {
        let lsn = db.data_manager.replication_lsn().await;
        Self {
            lsn,
            read_only: db.data_manager.read_only(),
            collections: db.data_manager.get_collection_names().await.len(),
            records: db.data_manager.get_total_vector_count().await,
        }
    }
}

// ── Transport ──────────────────────────────────────────────────────

/// How a replica reaches its primary.
#[async_trait]
pub trait ReplicationTransport: Send + Sync {
    /// Full copy of the primary.
    async fn fetch_snapshot(&self) -> Result<ReplicationSnapshot>;
    /// Log tail after `since`, with the continuity flag.
    async fn fetch_entries(&self, since: u64) -> Result<EntriesBatch>;
}

/// Pull replication over HTTP from the primary's `/replication/*` REST
/// endpoints.
pub struct HttpTransport {
    client: reqwest::Client,
    base_url: String,
}

impl HttpTransport {
    /// `base_url` is the primary's REST origin, e.g. `http://127.0.0.1:8080`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

#[async_trait]
impl ReplicationTransport for HttpTransport {
    async fn fetch_snapshot(&self) -> Result<ReplicationSnapshot> {
        self.client
            .get(self.url("/replication/snapshot"))
            .send()
            .await
            .map_err(|e| CoreTexError::Other(format!("replication fetch snapshot: {e}")))?
            // Check the status *before* decoding. Without this, an HTTP error
            // is handed straight to `.json()` and surfaces as
            // "replication decode snapshot: expected value at line 1" — a
            // complaint about parsing that says nothing about the real cause,
            // sending the reader after their JSON instead of after the primary.
            //
            // It is worse than a misleading message when the error body happens
            // to be shaped like a valid payload (a gateway that echoes a
            // previous 200, a proxy that wraps the upstream response). The
            // replica would then apply a bogus snapshot, jump its position past
            // data it never received, and report success. Verified by
            // `http_transport_does_not_treat_an_error_status_as_success`.
            .error_for_status()
            .map_err(|e| CoreTexError::Other(format!("replication snapshot status: {e}")))?
            .json::<ReplicationSnapshot>()
            .await
            .map_err(|e| CoreTexError::Other(format!("replication decode snapshot: {e}")))
    }

    async fn fetch_entries(&self, since: u64) -> Result<EntriesBatch> {
        self.client
            .get(self.url(&format!("/replication/entries?since={since}")))
            .send()
            .await
            .map_err(|e| CoreTexError::Other(format!("replication fetch entries: {e}")))?
            .error_for_status()
            .map_err(|e| CoreTexError::Other(format!("replication entries status: {e}")))?
            .json::<EntriesBatch>()
            .await
            .map_err(|e| CoreTexError::Other(format!("replication decode entries: {e}")))
    }
}

/// Same-process transport: the "primary" is just another [`CoreTexDB`]
/// handle. Used by tests and single-process double-instance setups; no
/// serialization happens on this path, but the payload shapes are the ones
/// the HTTP endpoints ship, so the state machine behaves identically.
pub struct InProcessTransport {
    primary: Arc<CoreTexDB>,
}

impl InProcessTransport {
    pub fn new(primary: Arc<CoreTexDB>) -> Self {
        Self { primary }
    }
}

#[async_trait]
impl ReplicationTransport for InProcessTransport {
    async fn fetch_snapshot(&self) -> Result<ReplicationSnapshot> {
        Ok(self.primary.data_manager.replication_snapshot().await)
    }

    async fn fetch_entries(&self, since: u64) -> Result<EntriesBatch> {
        let (entries, truncated) = self
            .primary
            .data_manager
            .read_replication_entries(since)
            .await?;
        // Same watermark rule as the REST endpoint: derive from the shipped
        // entries, never from a separately sampled counter.
        let lsn = entries.last().map(|e| e.sequence).unwrap_or(since);
        Ok(EntriesBatch {
            entries,
            truncated,
            lsn,
        })
    }
}

// ── Replica state ──────────────────────────────────────────────────

/// What one [`ReplicaSync::sync_once`] cycle accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// Everything replaced by a fresh snapshot (first sync, or the tail
    /// was truncated).
    FullSync { lsn: u64 },
    /// `applied` entries changed state; the position is now `lsn`.
    Incremental { applied: u32, lsn: u64 },
    /// The primary had nothing after the current position.
    UpToDate { lsn: u64 },
}

/// Persist the applied position (`{"last_lsn": N}`, atomic rename).
fn save_state(path: &Path, lsn: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(&serde_json::json!({ "last_lsn": lsn }))?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Read back a persisted position; `None` (→ 0) when missing or corrupt,
/// which simply forces a full resync.
fn load_state(path: &Path) -> Option<u64> {
    let bytes = std::fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("last_lsn")?.as_u64()
}

/// Replica side of the pipeline: pulls from a primary through a
/// [`ReplicationTransport`] and applies to the local database.
///
/// Construction flips the local database to read-only. From then on every
/// external mutation — REST, FFI, direct library calls — is rejected, while
/// [`Self::sync_once`] continues to apply what the primary sends through
/// the guard-exempt replay paths.
pub struct ReplicaSync {
    db: Arc<CoreTexDB>,
    transport: Arc<dyn ReplicationTransport>,
    state_path: PathBuf,
    last_lsn: AtomicU64,
}

impl ReplicaSync {
    /// Wrap `db` as a replica of whatever `transport` points at.
    ///
    /// `state_path` is the JSON file remembering the applied position across
    /// restarts; a missing/corrupt file means "start from a snapshot".
    pub fn new(
        db: Arc<CoreTexDB>,
        transport: Arc<dyn ReplicationTransport>,
        state_path: impl Into<PathBuf>,
    ) -> Self {
        let state_path = state_path.into();
        let last_lsn = load_state(&state_path).unwrap_or(0);
        db.data_manager.set_read_only(true);
        Self {
            db,
            transport,
            state_path,
            last_lsn: AtomicU64::new(last_lsn),
        }
    }

    /// Position of the last entry this replica applied (0 = never synced).
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::Acquire)
    }

    /// The local database handle (read-only while this syncer owns it).
    pub fn db(&self) -> &Arc<CoreTexDB> {
        &self.db
    }

    /// One pull → apply → persist cycle.
    ///
    /// * position 0 → full snapshot;
    /// * `truncated` tail → full snapshot;
    /// * otherwise apply the tail and advance the position.
    ///
    /// On error the position is left untouched, so the next cycle retries
    /// from the same place; every apply is idempotent, making overlap free.
    pub async fn sync_once(&self) -> Result<SyncOutcome> {
        let since = self.last_lsn();

        if since == 0 {
            let snapshot = self.transport.fetch_snapshot().await?;
            self.db
                .data_manager
                .apply_replication_snapshot(&snapshot)
                .await?;
            // Schemas live in memory after the wipe; the manifest is what a
            // restart reads back, so persist it before advancing position.
            self.db.persist_manifest().await?;
            return Ok(self.finish_full_sync(snapshot.lsn));
        }

        let batch = self.transport.fetch_entries(since).await?;
        if batch.truncated {
            let snapshot = self.transport.fetch_snapshot().await?;
            self.db
                .data_manager
                .apply_replication_snapshot(&snapshot)
                .await?;
            self.db.persist_manifest().await?;
            return Ok(self.finish_full_sync(snapshot.lsn));
        }

        if batch.entries.is_empty() {
            return Ok(SyncOutcome::UpToDate { lsn: since });
        }

        let applied = self
            .db
            .data_manager
            .apply_replicated_entries(&batch.entries)
            .await?;
        if applied > 0 {
            // Schema entries (create/delete collections, self-heals) only
            // exist in memory until the manifest is rewritten.
            self.db.persist_manifest().await?;
        }
        // Trust only what was shipped: `entries.last()` is a position the
        // replica demonstrably has, whatever the batch claims.
        let lsn = batch
            .entries
            .last()
            .map(|e| e.sequence)
            .unwrap_or(since);
        save_state(&self.state_path, lsn)?;
        self.last_lsn.store(lsn, Ordering::Release);
        Ok(SyncOutcome::Incremental { applied, lsn })
    }

    fn finish_full_sync(&self, lsn: u64) -> SyncOutcome {
        // Snapshot read order guarantees nothing after `lsn` is missing.
        if let Err(e) = save_state(&self.state_path, lsn) {
            log::warn!("replication: failed to persist position {lsn}: {e}");
        }
        self.last_lsn.store(lsn, Ordering::Release);
        SyncOutcome::FullSync { lsn }
    }

    /// Spawn a background task syncing every `interval`.
    ///
    /// Returns the task handle and a stop switch: send `true` to break the
    /// loop. Transport/apply errors are logged and retried next tick — a
    /// replica must ride out primary restarts without operator action.
    pub fn spawn_loop(
        self: Arc<Self>,
        interval: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            loop {
                if let Err(e) = self.sync_once().await {
                    log::warn!("replication: sync cycle failed: {e}");
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        (handle, stop_tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_roundtrip_and_corruption_forces_resync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replica_state.json");

        // Missing file → 0 → first sync takes a snapshot.
        assert_eq!(load_state(&path), None);

        save_state(&path, 42).unwrap();
        assert_eq!(load_state(&path), Some(42));

        // Atomic rename must not leave the temporary behind.
        assert!(!path.with_extension("json.tmp").exists());

        // Corrupt content degrades to "no position" rather than panicking:
        // the next cycle falls back to a full snapshot.
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(load_state(&path), None);
    }
}
