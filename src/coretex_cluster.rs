//! C2 — cluster slot routing, node discovery and collection migration.
//!
//! The sharding unit is a **collection**, not a single vector: an index is
//! built per collection, so splitting one collection across nodes would mean
//! merging partial ANN results for every query — a distributed retrieval
//! problem this phase deliberately does not take on. Routing is therefore
//! `collection → slot → node`, Redis-style: 16384 slots, CRC16 with
//! `{hashtag}` support, and a `MOVED`-shaped error when nothing owns a
//! collection yet.
//!
//! Three pieces:
//!
//! * [`ClusterRouter`] — the slot table plus node directory, and health
//!   probing over [`ClusterTransport`].
//! * [`CollectionChunk`] — one collection (schema + rows + log position) as
//!   a transportable unit, produced by
//!   [`crate::coretex_data::DataManager::export_collection`] and consumed by
//!   [`crate::coretex_data::DataManager::import_collection`].
//! * [`ClusterMigrator`] — copy a collection between nodes, and only then
//!   move the slot: the route never points at a node that does not have the
//!   data.
//!
//! HTTP wiring (node-side endpoints for export/import, `MOVED` responses for
//! the REST layer) is deliberately absent: `src/coretex_api/rest/mod.rs`
//! currently carries another session's work, and mixing changes into a file
//! someone else is editing has burned us before. [`LocalNodeTransport`]
//! covers same-process clusters and tests today; an HTTP transport follows
//! the endpoints.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::coretex_core::{CollectionSchema, CoreTexError, Result};
use crate::coretex_data::VectorRecord;
use crate::coretex_replication::ReplicationStatus;
use crate::CoreTexDB;

/// Slot count, matching Redis Cluster so operators can reuse slot tooling.
pub const SLOT_COUNT: usize = 16384;

/// Sentinel in the slot table for "unassigned".
const NO_OWNER: u16 = u16::MAX;

/// CRC16/XMODEM — the function Redis Cluster hashes with.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// The slot a collection belongs to.
///
/// `{tag}` in the name pins the hash to the tag, so `"{user:42}:profile"` and
/// `"{user:42}:orders"` land together — the same escape hatch Redis offers
/// for multi-key commands. An empty or unterminated `{` is hashed whole.
pub fn slot_of(collection: &str) -> u16 {
    let bytes = collection.as_bytes();
    if let Some(open) = bytes.iter().position(|b| *b == b'{') {
        if let Some(rel_close) = bytes[open + 1..].iter().position(|b| *b == b'}') {
            let close = open + 1 + rel_close;
            if close > open + 1 {
                return crc16(&bytes[open + 1..close]) % (SLOT_COUNT as u16);
            }
        }
    }
    crc16(bytes) % (SLOT_COUNT as u16)
}

/// One collection as a self-contained unit of migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionChunk {
    /// Schema verbatim — dimension, metric and index type travel with the
    /// rows so the receiving node indexes them exactly like the original.
    pub schema: CollectionSchema,
    /// `id → record`, as of the export.
    pub records: HashMap<String, VectorRecord>,
    /// Source log position the chunk corresponds to (0 = no WAL).
    pub lsn: u64,
}

impl CollectionChunk {
    /// Number of rows in the chunk.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the chunk carries no rows.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Stable identifier, unique within the cluster.
    pub id: String,
    /// REST origin used by HTTP transports (`http://host:port`).
    pub base_url: String,
}

impl NodeInfo {
    pub fn new(id: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            base_url: base_url.into(),
        }
    }
}

/// Result of probing one node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeHealth {
    /// The node that was probed.
    pub node: NodeInfo,
    /// Whether it answered.
    pub alive: bool,
    /// Its reported replication status (collection/record counts, log
    /// position) when it answered.
    pub status: Option<ReplicationStatus>,
    /// Why the probe failed.
    pub error: Option<String>,
}

/// Per-node routing summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRouting {
    pub node: NodeInfo,
    /// Slots owned by this node.
    pub slots: usize,
    /// Collections assigned to this node.
    pub collections: usize,
}

/// Whole-cluster routing summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub nodes: Vec<NodeRouting>,
    /// Slots with no owner yet.
    pub unassigned_slots: usize,
}

/// Slot ownership table plus node directory.
#[derive(Debug)]
pub struct ClusterRouter {
    nodes: RwLock<Vec<NodeInfo>>,
    /// `slots[s]` indexes `nodes`; [`NO_OWNER`] means unassigned.
    slots: RwLock<Vec<u16>>,
    /// Reverse index for administration: which collections were assigned, and
    /// to which slot. Without it a node's collection list could only be
    /// recovered by asking every node.
    assigned: RwLock<HashMap<String, u16>>,
}

impl ClusterRouter {
    /// Build a router over `nodes` with every slot unassigned.
    pub fn new(nodes: Vec<NodeInfo>) -> Result<Self> {
        let mut seen = std::collections::HashSet::new();
        for node in &nodes {
            if node.id.is_empty() {
                return Err(CoreTexError::Other(
                    "cluster node id must not be empty".to_string(),
                ));
            }
            if !seen.insert(node.id.clone()) {
                return Err(CoreTexError::Other(format!(
                    "duplicate cluster node id '{}'",
                    node.id
                )));
            }
        }
        Ok(Self {
            nodes: RwLock::new(nodes),
            slots: RwLock::new(vec![NO_OWNER; SLOT_COUNT]),
            assigned: RwLock::new(HashMap::new()),
        })
    }

    /// The node directory.
    pub async fn nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.clone()
    }

    /// Look up a node by id.
    pub async fn node(&self, id: &str) -> Option<NodeInfo> {
        self.nodes
            .read()
            .await
            .iter()
            .find(|n| n.id == id)
            .cloned()
    }

    fn index_of(nodes: &[NodeInfo], id: &str) -> Option<u16> {
        nodes
            .iter()
            .position(|n| n.id == id)
            .map(|i| i as u16)
    }

    /// Point `collection`'s slot at `node_id`. Re-assignment is the last
    /// half of a migration and is deliberately a separate call from moving
    /// the data.
    pub async fn assign_collection(&self, collection: &str, node_id: &str) -> Result<u16> {
        let nodes = self.nodes.read().await;
        let index = Self::index_of(&nodes, node_id).ok_or_else(|| {
            CoreTexError::Other(format!("unknown cluster node '{node_id}'"))
        })?;
        drop(nodes);

        let slot = slot_of(collection);
        self.slots.write().await[slot as usize] = index;
        self.assigned
            .write()
            .await
            .insert(collection.to_string(), slot);
        Ok(slot)
    }

    /// Assign `[start, end]` (inclusive) to `node_id` — the bulk form used
    /// when bootstrapping a cluster from a static layout.
    pub async fn assign_range(
        &self,
        node_id: &str,
        start: usize,
        end: usize,
    ) -> Result<usize> {
        if start > end || end >= SLOT_COUNT {
            return Err(CoreTexError::Other(format!(
                "invalid slot range {start}..={end} (0..={})",
                SLOT_COUNT - 1
            )));
        }
        let nodes = self.nodes.read().await;
        let index = Self::index_of(&nodes, node_id).ok_or_else(|| {
            CoreTexError::Other(format!("unknown cluster node '{node_id}'"))
        })?;
        drop(nodes);

        let mut slots = self.slots.write().await;
        for slot in slots.iter_mut().take(end + 1).skip(start) {
            *slot = index;
        }
        Ok(end - start + 1)
    }

    /// The node owning a slot, if any.
    pub async fn owner_of_slot(&self, slot: u16) -> Option<NodeInfo> {
        let index = self.slots.read().await[slot as usize];
        if index == NO_OWNER {
            return None;
        }
        self.nodes.read().await.get(index as usize).cloned()
    }

    /// The node that serves `collection`.
    ///
    /// The error carries the slot, so a caller can answer with a `MOVED`
    /// style redirect once HTTP wiring exists.
    pub async fn lookup(&self, collection: &str) -> Result<NodeInfo> {
        let slot = slot_of(collection);
        self.owner_of_slot(slot).await.ok_or_else(|| {
            CoreTexError::Other(format!(
                "no node owns collection '{collection}' (slot {slot})"
            ))
        })
    }

    /// Collections assigned to `node_id` (via the reverse index; ranges
    /// assigned without collections simply contribute no names).
    pub async fn collections_of(&self, node_id: &str) -> Result<Vec<String>> {
        let node = self.node(node_id).await.ok_or_else(|| {
            CoreTexError::Other(format!("unknown cluster node '{node_id}'"))
        })?;
        let index = Self::index_of(&self.nodes.read().await, &node.id).unwrap_or(NO_OWNER);
        let slots = self.slots.read().await;
        Ok(self
            .assigned
            .read()
            .await
            .iter()
            .filter(|(_, slot)| slots[**slot as usize] == index)
            .map(|(name, _)| name.clone())
            .collect())
    }

    /// Full routing summary.
    pub async fn cluster_info(&self) -> ClusterInfo {
        let nodes = self.nodes.read().await;
        let slots = self.slots.read().await;
        let assigned = self.assigned.read().await;

        let mut nodes_out = Vec::with_capacity(nodes.len());
        for (index, node) in nodes.iter().enumerate() {
            let owned = slots.iter().filter(|s| **s == index as u16).count();
            let collections = assigned
                .iter()
                .filter(|(_, slot)| slots[**slot as usize] == index as u16)
                .count();
            nodes_out.push(NodeRouting {
                node: node.clone(),
                slots: owned,
                collections,
            });
        }
        ClusterInfo {
            nodes: nodes_out,
            unassigned_slots: slots.iter().filter(|s| **s == NO_OWNER).count(),
        }
    }

    /// Probe every node through its transport. A node with no registered
    /// transport is reported down rather than skipped — a silent gap here
    /// would look like a healthy cluster.
    pub async fn probe_all(
        &self,
        transports: &HashMap<String, Arc<dyn ClusterTransport>>,
    ) -> Vec<ClusterNodeHealth> {
        let mut out = Vec::new();
        for node in self.nodes.read().await.iter() {
            let Some(transport) = transports.get(&node.id) else {
                out.push(ClusterNodeHealth {
                    node: node.clone(),
                    alive: false,
                    status: None,
                    error: Some("no transport registered for node".to_string()),
                });
                continue;
            };
            match transport.status().await {
                Ok(status) => out.push(ClusterNodeHealth {
                    node: node.clone(),
                    alive: true,
                    status: Some(status),
                    error: None,
                }),
                Err(e) => out.push(ClusterNodeHealth {
                    node: node.clone(),
                    alive: false,
                    status: None,
                    error: Some(e.to_string()),
                }),
            }
        }
        out
    }
}

// ── Node transport ─────────────────────────────────────────────────

/// What the cluster asks of a node.
#[async_trait]
pub trait ClusterTransport: Send + Sync {
    /// Node status; also the liveness probe.
    async fn status(&self) -> Result<ReplicationStatus>;

    /// Export one collection (`None` when the node does not have it).
    async fn export_collection(&self, name: &str) -> Result<Option<CollectionChunk>>;

    /// Load a collection into the node, replacing any collection of that
    /// name. Implementations must persist the manifest before returning, so
    /// the imported collection survives a restart.
    async fn import_collection(&self, chunk: &CollectionChunk) -> Result<usize>;
}

/// Same-process node: talks to another [`CoreTexDB`] handle directly.
pub struct LocalNodeTransport {
    db: Arc<CoreTexDB>,
}

impl LocalNodeTransport {
    pub fn new(db: Arc<CoreTexDB>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl ClusterTransport for LocalNodeTransport {
    async fn status(&self) -> Result<ReplicationStatus> {
        Ok(ReplicationStatus::collect(&self.db).await)
    }

    async fn export_collection(&self, name: &str) -> Result<Option<CollectionChunk>> {
        self.db.data_manager.export_collection(name).await
    }

    async fn import_collection(&self, chunk: &CollectionChunk) -> Result<usize> {
        let imported = self.db.data_manager.import_collection(chunk).await?;
        // The manifest is what a restart reads schemas from; without this the
        // imported collection would only exist in memory.
        self.db.persist_manifest().await?;
        Ok(imported)
    }
}

// ── Migration ──────────────────────────────────────────────────────

/// What one migration accomplished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOutcome {
    pub collection: String,
    pub slot: u16,
    pub from: String,
    pub to: String,
    /// Rows delivered to the target.
    pub records: usize,
    /// The source keeps its copy: migration is copy-then-switch, so cleanup
    /// is an explicit follow-up (`delete_collection` on the source), never a
    /// silent side effect of a routing change.
    pub source_retained: bool,
}

/// Moves collections between nodes and flips the route afterwards.
pub struct ClusterMigrator {
    router: Arc<ClusterRouter>,
    transports: HashMap<String, Arc<dyn ClusterTransport>>,
}

impl ClusterMigrator {
    pub fn new(
        router: Arc<ClusterRouter>,
        transports: HashMap<String, Arc<dyn ClusterTransport>>,
    ) -> Self {
        Self { router, transports }
    }

    fn transport(&self, node_id: &str) -> Result<&Arc<dyn ClusterTransport>> {
        self.transports.get(node_id).ok_or_else(|| {
            CoreTexError::Other(format!("no transport registered for node '{node_id}'"))
        })
    }

    /// Copy `collection` from one node to another, then move its slot.
    ///
    /// Order matters: the export or import failing leaves the route
    /// untouched, so clients keep reaching the node that still has the data.
    /// Callers are responsible for quiescing writes to the collection across
    /// the copy — rows written to the source after its export are not
    /// carried over.
    pub async fn migrate(
        &self,
        collection: &str,
        from: &str,
        to: &str,
    ) -> Result<MigrationOutcome> {
        if from == to {
            return Err(CoreTexError::Other(format!(
                "migration source and target are both '{from}'"
            )));
        }
        let source = self.transport(from)?.clone();
        let target = self.transport(to)?.clone();

        let chunk = source
            .export_collection(collection)
            .await?
            .ok_or_else(|| {
                CoreTexError::Other(format!(
                    "collection '{collection}' not found on node '{from}'"
                ))
            })?;

        let records = target.import_collection(&chunk).await?;

        // Data is in place — now the route may move.
        let slot = self.router.assign_collection(collection, to).await?;

        Ok(MigrationOutcome {
            collection: collection.to_string(),
            slot,
            from: from.to_string(),
            to: to.to_string(),
            records,
            source_retained: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_is_stable_and_hashtag_aware() {
        // Same name, same slot, every time.
        assert_eq!(slot_of("docs"), slot_of("docs"));
        assert!(slot_of("docs") < SLOT_COUNT as u16);

        // A tag pins related collections together…
        assert_eq!(slot_of("{user:42}:profile"), slot_of("{user:42}:orders"));
        // …and separates different tags.
        assert_ne!(slot_of("{user:42}:profile"), slot_of("{user:43}:profile"));
        // The tag content, not the whole string, decides the slot.
        assert_eq!(slot_of("{user:42}"), slot_of("{user:42}:anything"));

        // Degenerate tags fall back to hashing the whole name.
        assert_eq!(slot_of("{}docs"), slot_of("{}docs"));
        assert_ne!(slot_of("{}docs"), slot_of("{user:42}:profile"));
        assert_eq!(slot_of("{unterminated"), slot_of("{unterminated"));
        assert_ne!(slot_of("{unterminated"), slot_of("unterminated"));
    }

    #[test]
    fn slot_distribution_spans_the_table() {
        // A hash that only ever produced a few hundred slots would make the
        // 16384-slot table pointless.
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            seen.insert(slot_of(&format!("collection-{i}")));
        }
        assert!(
            seen.len() > 600,
            "1000 names landed in only {} distinct slots",
            seen.len()
        );
    }

    #[tokio::test]
    async fn assignment_lookup_and_reverse_index() {
        let router = ClusterRouter::new(vec![
            NodeInfo::new("n1", "http://127.0.0.1:7001"),
            NodeInfo::new("n2", "http://127.0.0.1:7002"),
        ])
        .unwrap();

        // Nothing is routed before assignment: the error names the slot so a
        // caller can turn it into a MOVED response.
        let err = router.lookup("docs").await.unwrap_err().to_string();
        assert!(err.contains("no node owns collection 'docs'"), "got: {err}");
        assert!(err.contains(&format!("slot {}", slot_of("docs"))), "got: {err}");

        let slot = router.assign_collection("docs", "n1").await.unwrap();
        assert_eq!(slot, slot_of("docs"));
        assert_eq!(router.lookup("docs").await.unwrap().id, "n1");

        // Unknown nodes are refused rather than silently creating entries.
        let err = router
            .assign_collection("other", "nope")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown cluster node 'nope'"), "got: {err}");

        // Re-assignment (the last half of a migration) just moves the slot.
        router.assign_collection("docs", "n2").await.unwrap();
        assert_eq!(router.lookup("docs").await.unwrap().id, "n2");
        assert_eq!(router.collections_of("n2").await.unwrap(), vec!["docs"]);
        assert!(router.collections_of("n1").await.unwrap().is_empty());

        let info = router.cluster_info().await;
        assert_eq!(info.nodes.len(), 2);
        assert_eq!(info.nodes[1].slots, 1);
        assert_eq!(info.nodes[1].collections, 1);
        assert_eq!(info.nodes[0].slots, 0);
        assert_eq!(info.unassigned_slots, SLOT_COUNT - 1);
    }

    #[tokio::test]
    async fn ranges_and_duplicate_ids_are_validated() {
        let router = ClusterRouter::new(vec![NodeInfo::new("n1", "http://x")]).unwrap();
        assert_eq!(router.assign_range("n1", 0, 99).await.unwrap(), 100);
        let info = router.cluster_info().await;
        assert_eq!(info.nodes[0].slots, 100);
        assert_eq!(info.unassigned_slots, SLOT_COUNT - 100);

        // A range that runs past the table would panic on indexing.
        assert!(router.assign_range("n1", 0, SLOT_COUNT).await.is_err());
        assert!(router.assign_range("n1", 10, 9).await.is_err());

        // Duplicate ids would make `node()` ambiguous.
        let err = ClusterRouter::new(vec![
            NodeInfo::new("n1", "http://a"),
            NodeInfo::new("n1", "http://b"),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("duplicate cluster node id 'n1'"), "got: {err}");
    }
}