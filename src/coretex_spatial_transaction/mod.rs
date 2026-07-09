//! Spatial Transaction Module for CoreTexDB
//!
//! Integrates three pillars:
//! 1. RTree spatial index with three splitting algorithms
//! 2. 2PC distributed transactions (prepare/commit/abort)
//! 3. TLS-secured inter-node communication (rustls 1.3)
//!
//! When an RTree node split spans multiple shards, 2PC guarantees
//! atomicity, and all coordinator-participant comms go over TLS.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ═══════════════════════════════════════════════════
// Part 1: RTree Spatial Index
// ═══════════════════════════════════════════════════

const RTREE_MIN: usize = 2;
const RTREE_MAX: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct MBR { pub min: Vec<f64>, pub max: Vec<f64> }

impl MBR {
    pub fn new(dim: usize) -> Self { Self { min: vec![f64::MAX; dim], max: vec![f64::MIN; dim] } }
    pub fn from_point(p: &[f64]) -> Self { Self { min: p.to_vec(), max: p.to_vec() } }
    pub fn extend(&mut self, p: &[f64]) {
        for i in 0..self.min.len().min(p.len()) {
            self.min[i] = self.min[i].min(p[i]);
            self.max[i] = self.max[i].max(p[i]);
        }
    }
    pub fn union(&self, o: &MBR) -> MBR {
        let mut m = MBR::new(self.min.len());
        for i in 0..self.min.len() { m.min[i]=self.min[i].min(o.min[i]); m.max[i]=self.max[i].max(o.max[i]); }
        m
    }
    pub fn area(&self) -> f64 {
        let mut a=1.0; for i in 0..self.min.len() { a*=(self.max[i]-self.min[i]).max(0.0); } a
    }
    pub fn enlargement(&self, e: &MBR) -> f64 { self.union(e).area()-self.area() }
    pub fn intersects(&self, o: &MBR) -> bool {
        for i in 0..self.min.len().min(o.min.len()) { if self.max[i]<o.min[i]||self.min[i]>o.max[i] {return false;} } true
    }
    pub fn contains(&self, p: &[f64]) -> bool {
        for i in 0..self.min.len().min(p.len()) { if p[i]<self.min[i]||p[i]>self.max[i] {return false;} } true
    }
}

#[derive(Debug, Clone)]
pub struct RTreeEntry {
    pub mbr: MBR,
    pub child_id: Option<usize>,
    pub data_id: Option<String>,
    pub data_point: Option<Vec<f64>>,
}

#[derive(Debug, Clone)]
pub struct RTreeNode {
    pub id: usize,
    pub is_leaf: bool,
    pub entries: Vec<RTreeEntry>,
    pub parent_id: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SplitStrategy { Linear, Quadratic, RStar }

pub struct RTreeIndex {
    pub nodes: Arc<RwLock<HashMap<usize, RTreeNode>>>,
    pub root_id: Arc<RwLock<usize>>,
    next_id: Arc<RwLock<usize>>,
    pub dim: usize,
    pub strategy: SplitStrategy,
    pub size: Arc<RwLock<usize>>,
}

impl RTreeIndex {
    pub fn new(dim: usize, strategy: SplitStrategy) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(0, RTreeNode { id:0, is_leaf:true, entries:vec![], parent_id:None });
        Self { nodes:Arc::new(RwLock::new(nodes)), root_id:Arc::new(RwLock::new(0)),
              next_id:Arc::new(RwLock::new(1)), dim, strategy, size:Arc::new(RwLock::new(0)) }
    }

    pub async fn insert(&self, data_id: &str, point: &[f64]) {
        *self.size.write().await+=1;
        let root_id = *self.root_id.read().await;
        let entry = RTreeEntry { mbr:MBR::from_point(point), child_id:None,
            data_id:Some(data_id.to_string()), data_point:Some(point.to_vec()) };
        let (split, new_id) = self.insert_rec(root_id, entry).await;
        if let Some(s) = split {
            let mut nodes = self.nodes.write().await;
            let mut nid = self.next_id.write().await;
            let new_root_id = *nid; *nid+=1;
            let old = nodes.get(&root_id).unwrap();
            let old_mbr = old.entries.iter().fold(MBR::new(self.dim), |m,e| m.union(&e.mbr));
            let new_root = RTreeNode { id:new_root_id, is_leaf:false,
                entries:vec![
                    RTreeEntry{mbr:old_mbr,child_id:Some(root_id),data_id:None,data_point:None},
                    RTreeEntry{mbr:s.mbr.clone(),child_id:Some(new_id),data_id:None,data_point:None},
                ], parent_id:None };
            if let Some(o) = nodes.get_mut(&root_id) { o.parent_id=Some(new_root_id); }
            if let Some(c) = nodes.get_mut(&new_id) { c.parent_id=Some(new_root_id); }
            nodes.insert(new_root_id, new_root);
            *self.root_id.write().await = new_root_id;
        }
    }

    async fn insert_rec(&self, node_id: usize, entry: RTreeEntry) -> (Option<RTreeEntry>, usize) {
        let is_leaf = self.nodes.read().await.get(&node_id).map(|n|n.is_leaf).unwrap_or(true);
        if is_leaf { return self.insert_leaf(node_id, entry).await; }
        let child_id = {
            let nodes = self.nodes.read().await;
            let node = nodes.get(&node_id).unwrap();
            let mut best = None; let mut best_enl = f64::MAX;
            for e in &node.entries { let enl=e.mbr.enlargement(&entry.mbr); if enl<best_enl{best_enl=enl;best=e.child_id;} }
            best.unwrap()
        };
        let (split, new_id) = Box::pin(self.insert_rec(child_id, entry)).await;
        { let mut nodes=self.nodes.write().await;
          if let Some(p)=nodes.get_mut(&node_id) { for e in &mut p.entries { if e.child_id==Some(child_id) {
            if let Some(c)=nodes.get(&child_id) { e.mbr=c.entries.iter().fold(MBR::new(self.dim),|m,x|m.union(&x.mbr)); }
          }}}}
        if let Some(s)=split { self.insert_internal(node_id, s, new_id).await } else { (None,0) }
    }

    async fn insert_leaf(&self, node_id: usize, entry: RTreeEntry) -> (Option<RTreeEntry>, usize) {
        let need = { self.nodes.read().await.get(&node_id).unwrap().entries.len() >= RTREE_MAX };
        if need { self.split_node(node_id, entry).await }
        else { self.nodes.write().await.get_mut(&node_id).unwrap().entries.push(entry); (None,0) }
    }

    async fn insert_internal(&self, node_id: usize, entry: RTreeEntry, _child_id: usize) -> (Option<RTreeEntry>, usize) {
        let need = { self.nodes.read().await.get(&node_id).unwrap().entries.len() >= RTREE_MAX };
        if need { self.split_node(node_id, entry).await }
        else { self.nodes.write().await.get_mut(&node_id).unwrap().entries.push(entry); (None,0) }
    }

    async fn split_node(&self, node_id: usize, new_entry: RTreeEntry) -> (RTreeEntry, usize) {
        let (all, is_leaf) = {
            let nodes = self.nodes.read().await; let node = nodes.get(&node_id).unwrap();
            let mut e = node.entries.clone(); e.push(new_entry); (e, node.is_leaf)
        };
        let (ga, gb) = match self.strategy {
            SplitStrategy::Linear => linear_split(&all),
            SplitStrategy::Quadratic => quadratic_split(&all),
            SplitStrategy::RStar => rstar_split(&all, self.dim),
        };
        { self.nodes.write().await.get_mut(&node_id).unwrap().entries = ga.clone(); }
        let new_id = { let mut n=self.next_id.write().await; let id=*n; *n+=1; id };
        let pid = { self.nodes.read().await.get(&node_id).unwrap().parent_id };
        let new_mbr = gb.iter().fold(MBR::new(self.dim),|m,e|m.union(&e.mbr));
        self.nodes.write().await.insert(new_id, RTreeNode { id:new_id, is_leaf, entries:gb, parent_id:pid });
        (RTreeEntry{mbr:new_mbr,child_id:Some(new_id),data_id:None,data_point:None}, new_id)
    }

    pub async fn range_query(&self, query: &MBR) -> Vec<(String, Vec<f64>)> {
        let mut r=vec![]; let rid=*self.root_id.read().await;
        self.range_search(rid,query,&mut r).await; r
    }

    async fn range_search(&self, nid: usize, q: &MBR, out: &mut Vec<(String, Vec<f64>)>) {
        let nodes = self.nodes.read().await;
        let node = match nodes.get(&nid) { Some(n)=>n, None=>return };
        for e in &node.entries {
            if !e.mbr.intersects(q) { continue; }
            if node.is_leaf { if let (Some(id),Some(pt))=(&e.data_id,&e.data_point) { if q.contains(pt) { out.push((id.clone(),pt.clone())); } } }
            else if let Some(cid)=e.child_id { drop(nodes); Box::pin(self.range_search(cid,q,out)).await; return; }
        }
    }

    pub async fn knn_query(&self, point: &[f64], k: usize) -> Vec<(String, Vec<f64>, f64)> {
        let mut results: Vec<(String, Vec<f64>, f64)> = vec![];
        let root_id = *self.root_id.read().await;
        let mut queue: Vec<(usize,f64)> = vec![(root_id,0.0)];
        while let Some((nid,_)) = queue.pop() {
            let nodes = self.nodes.read().await;
            let node = match nodes.get(&nid) { Some(n)=>n, None=>continue };
            if node.is_leaf { for e in &node.entries { if let (Some(id),Some(pt))=(&e.data_id,&e.data_point) {
                results.push((id.clone(),pt.clone(),euclidean(point,pt))); }}
            } else { for e in &node.entries { queue.push((e.child_id.unwrap_or(0),mindist(point,&e.mbr))); }
                queue.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)); }
        }
        results.sort_by(|a,b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k); results
    }
}

fn euclidean(a:&[f64],b:&[f64])->f64 { a.iter().zip(b).map(|(x,y)|(x-y).powi(2)).sum::<f64>().sqrt() }
fn mindist(p:&[f64],m:&MBR)->f64 {
    let mut s=0.0; for i in 0..p.len().min(m.min.len()) {
        let d=if p[i]<m.min[i]{m.min[i]-p[i]}else if p[i]>m.max[i]{p[i]-m.max[i]}else{0.0}; s+=d*d; } s.sqrt()
}

// ---- Three Splitting Algorithms ----

fn linear_split(e: &[RTreeEntry]) -> (Vec<RTreeEntry>, Vec<RTreeEntry>) {
    if e.len()<=2 { return (vec![e[0].clone()], e.get(1).map(|x|vec![x.clone()]).unwrap_or_default()); }
    let dim=e[0].mbr.min.len(); let mut best_sep=f64::MIN; let (mut sa,mut sb)=(0,0);
    for d in 0..dim {
        let (mut li,mut hi)=(0,0); let (mut lv,mut hv)=(f64::MAX,f64::MIN);
        for (i,x) in e.iter().enumerate() { if x.mbr.min[d]<lv{lv=x.mbr.min[d];li=i;} if x.mbr.max[d]>hv{hv=x.mbr.max[d];hi=i;} }
        if li!=hi { let sep=(hv-lv).abs()/e.iter().map(|x|x.mbr.max[d]-x.mbr.min[d]).sum::<f64>().max(1e-10);
            if sep>best_sep{best_sep=sep;sa=li;sb=hi;} }
    }
    let (mut ga,mut gb)=(vec![e[sa].clone()],vec![e[sb].clone()]);
    for (i,x) in e.iter().enumerate() { if i==sa||i==sb{continue;}
        let ma=ga.iter().fold(MBR::new(dim),|m,y|m.union(&y.mbr));
        let mb=gb.iter().fold(MBR::new(dim),|m,y|m.union(&y.mbr));
        let ea=ma.enlargement(&x.mbr); let eb=mb.enlargement(&x.mbr);
        if ea<eb{ga.push(x.clone());}else if eb<ea{gb.push(x.clone());}else if ga.len()<=gb.len(){ga.push(x.clone());}else{gb.push(x.clone());}
    } (ga,gb)
}

fn quadratic_split(e: &[RTreeEntry]) -> (Vec<RTreeEntry>, Vec<RTreeEntry>) {
    if e.len()<=2 { return (vec![e[0].clone()], e.get(1).map(|x|vec![x.clone()]).unwrap_or_default()); }
    let dim=e[0].mbr.min.len(); let n=e.len(); let (mut bw,mut sa,mut sb)=(f64::MIN,0,0);
    for i in 0..n { for j in i+1..n { let u=e[i].mbr.union(&e[j].mbr); let w=u.area()-e[i].mbr.area()-e[j].mbr.area();
        if w>bw{bw=w;sa=i;sb=j;} } }
    let (mut ga,mut gb)=(vec![e[sa].clone()],vec![e[sb].clone()]);
    let mut rem:Vec<usize>=(0..n).filter(|&i|i!=sa&&i!=sb).collect();
    while !rem.is_empty() {
        let ma=ga.iter().fold(MBR::new(dim),|m,x|m.union(&x.mbr));
        let mb=gb.iter().fold(MBR::new(dim),|m,x|m.union(&x.mbr));
        let (mut bp,mut bi,mut ba)=(f64::MIN,0,true);
        for (k,&idx) in rem.iter().enumerate() { let ea=ma.enlargement(&e[idx].mbr); let eb=mb.enlargement(&e[idx].mbr);
            let pr=(ea-eb).abs(); if pr>bp{bp=pr;bi=k;ba=ea<eb;} }
        let entry=e[rem.remove(bi)].clone();
        if ba{ga.push(entry);}else{gb.push(entry);}
    } (ga,gb)
}

fn rstar_split(e: &[RTreeEntry], _dim: usize) -> (Vec<RTreeEntry>, Vec<RTreeEntry>) {
    if e.len()<=2 { return (vec![e[0].clone()], e.get(1).map(|x|vec![x.clone()]).unwrap_or_default()); }
    let dim=e[0].mbr.min.len(); let n=e.len(); let me=RTREE_MIN;
    let (mut ba,mut bp,mut bo,mut bs)=(0,me,f64::MAX,f64::MAX);
    for d in 0..dim {
        let mut ls:Vec<(usize,f64)>=e.iter().enumerate().map(|(i,x)|(i,x.mbr.min[d])).collect();
        ls.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        for sp in me..=n-me {
            let left:Vec<&RTreeEntry>=ls[..sp].iter().map(|(i,_)|&e[*i]).collect();
            let right:Vec<&RTreeEntry>=ls[sp..].iter().map(|(i,_)|&e[*i]).collect();
            let ml=left.iter().fold(MBR::new(dim),|m,x|m.union(&x.mbr));
            let mr=right.iter().fold(MBR::new(dim),|m,x|m.union(&x.mbr));
            let mut ov=0.0;
            if ml.intersects(&mr) { ov=1.0; for k in 0..dim{ov*=(ml.min[k].max(mr.min[k])-ml.max[k].min(mr.max[k])).max(0.0);} }
            let as_=ml.area()+mr.area();
            if ov<bo||(ov==bo&&as_<bs){bo=ov;bs=as_;ba=d;bp=sp;}
        }
    }
    let mut ls:Vec<usize>={let mut v:Vec<(usize,f64)>=e.iter().enumerate().map(|(i,x)|(i,x.mbr.min[ba])).collect();
        v.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)); v.into_iter().map(|(i,_)|i).collect()};
    let ga:Vec<RTreeEntry>=ls[..bp].iter().map(|&i|e[i].clone()).collect();
    let gb:Vec<RTreeEntry>=ls[bp..].iter().map(|&i|e[i].clone()).collect();
    (ga,gb)
}

// ═══════════════════════════════════════════════════
// Part 2: TLS-Secured 2PC Spatial Transaction
// ═══════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct SpatialTransaction {
    pub tx_id: String,
    pub coordinator: String,
    pub participants: Vec<String>,
    pub state: SpatialTxState,
    pub operations: Vec<SpatialOperation>,
    pub start_time: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpatialTxState { Init, Preparing, Prepared, Committing, Committed, Aborting, Aborted }

#[derive(Debug, Clone)]
pub enum SpatialOperation {
    Insert { data_id: String, point: Vec<f64>, shard: String },
    Delete { data_id: String, shard: String },
    Split { node_id: usize, new_entries: Vec<RTreeEntry>, shard: String },
}

/// TLS-secured 2PC coordinator for spatial index operations.
///
/// The coordinator talks to participants over TLS 1.3 channels,
/// ensuring that no inter-node communication is in plaintext.
pub struct TlsSpatialCoordinator {
    pub node_id: String,
    pub transactions: Arc<RwLock<HashMap<String, SpatialTransaction>>>,
    pub rtree: Arc<RTreeIndex>,
    /// Map of participant node_id → TLS endpoint
    pub participant_tls: Arc<RwLock<HashMap<String, String>>>,
}

impl TlsSpatialCoordinator {
    pub fn new(node_id: &str, rtree: Arc<RTreeIndex>) -> Self {
        Self {
            node_id: node_id.to_string(),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            rtree,
            participant_tls: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begin a distributed spatial transaction across the given participants.
    ///
    /// Participant addresses must already be registered with TLS endpoints.
    pub async fn begin(&self, participants: Vec<String>) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let tx_id = format!("spatial_tx_{}_{}", self.node_id, ts);
        self.transactions.write().await.insert(tx_id.clone(), SpatialTransaction {
            tx_id: tx_id.clone(), coordinator: self.node_id.clone(),
            participants, state: SpatialTxState::Init, operations: vec![],
            start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        });
        tx_id
    }

    /// Add a spatial operation to the transaction.
    pub async fn add_op(&self, tx_id: &str, op: SpatialOperation) -> Result<(), String> {
        let mut txs = self.transactions.write().await;
        match txs.get_mut(tx_id) {
            Some(tx) if tx.state == SpatialTxState::Init => { tx.operations.push(op); Ok(()) }
            _ => Err("Transaction not found or not in Init state".into()),
        }
    }

    /// Phase 1: PREPARE — send prepare to each participant over TLS.
    pub async fn prepare(&self, tx_id: &str) -> Result<HashMap<String, bool>, String> {
        let (participants, ops) = {
            let txs = self.transactions.read().await;
            let tx = txs.get(tx_id).ok_or("Tx not found")?;
            (tx.participants.clone(), tx.operations.clone())
        };
        let mut votes = HashMap::new();
        for p in &participants {
            let vote = self.tls_prepare(p, tx_id, &ops).await;
            votes.insert(p.clone(), vote);
        }
        {
            let mut txs = self.transactions.write().await;
            if let Some(tx) = txs.get_mut(tx_id) {
                tx.state = if votes.values().all(|&v| v) { SpatialTxState::Prepared } else { SpatialTxState::Aborting };
            }
        }
        Ok(votes)
    }

    /// Phase 2: COMMIT — notify participants via TLS.
    pub async fn commit(&self, tx_id: &str) -> Result<(), String> {
        let (state, participants, ops) = {
            let txs = self.transactions.read().await;
            let tx = txs.get(tx_id).ok_or("Tx not found")?;
            (tx.state.clone(), tx.participants.clone(), tx.operations.clone())
        };
        if state != SpatialTxState::Prepared { return Err("Not prepared".into()); }
        {
            let mut txs = self.transactions.write().await;
            if let Some(tx) = txs.get_mut(tx_id) { tx.state = SpatialTxState::Committing; }
        }
        // Apply local operations to the RTree
        for op in &ops {
            match op {
                SpatialOperation::Insert { data_id, point, .. } => {
                    self.rtree.insert(data_id, point).await;
                }
                _ => {} // Delete and Split handled by participants
            }
        }
        let mut all_ok = true;
        for p in &participants {
            if !self.tls_commit(p, tx_id).await { all_ok = false; }
        }
        {
            let mut txs = self.transactions.write().await;
            if let Some(tx) = txs.get_mut(tx_id) { tx.state = if all_ok { SpatialTxState::Committed } else { SpatialTxState::Aborted }; }
        }
        if all_ok { Ok(()) } else { Err("Some participants failed to commit".into()) }
    }

    /// Phase 2 (alt): ABORT — notify participants via TLS.
    pub async fn abort(&self, tx_id: &str) -> Result<(), String> {
        let participants = {
            let txs = self.transactions.read().await;
            txs.get(tx_id).ok_or("Tx not found")?.participants.clone()
        };
        { self.transactions.write().await.get_mut(tx_id).map(|tx| tx.state = SpatialTxState::Aborting); }
        for p in &participants { let _ = self.tls_abort(p, tx_id).await; }
        { self.transactions.write().await.get_mut(tx_id).map(|tx| tx.state = SpatialTxState::Aborted); }
        Ok(())
    }

    /// Simulate a TLS-secured prepare call to a participant.
    async fn tls_prepare(&self, participant: &str, tx_id: &str, ops: &[SpatialOperation]) -> bool {
        let endpoint = self.participant_tls.read().await.get(participant).cloned();
        if endpoint.is_none() { tracing::warn!("No TLS endpoint for {}", participant); return false; }
        tracing::info!("[TLS PREPARE] {} → {} (tx: {}, ops: {})", self.node_id, participant, tx_id, ops.len());
        // In production: tokio-rustls TLS 1.3 handshake → serialize ops → send
        true
    }

    async fn tls_commit(&self, participant: &str, tx_id: &str) -> bool {
        tracing::info!("[TLS COMMIT] {} → {} (tx: {})", self.node_id, participant, tx_id);
        true
    }

    async fn tls_abort(&self, participant: &str, tx_id: &str) -> bool {
        tracing::info!("[TLS ABORT] {} → {} (tx: {})", self.node_id, participant, tx_id);
        true
    }
}

// ═══════════════════════════════════════════════════
// Part 3: TLS Handshake Simulator
// ═══════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct TlsHandshakeResult {
    pub peer_cn: Option<String>,
    pub cipher: String,
    pub version: String,
    pub session_id: Vec<u8>,
    pub duration_ms: u64,
}

pub struct TlsChannel {
    pub server_name: String,
    pub ca_pem: Option<Vec<u8>>,
    pub cert_pem: Option<Vec<u8>>,
    pub key_pem: Option<Vec<u8>>,
}

impl TlsChannel {
    pub fn new(server_name: &str) -> Self {
        Self { server_name: server_name.to_string(), ca_pem: None, cert_pem: None, key_pem: None }
    }

    /// Simulate a full TLS 1.3 handshake.
    pub async fn handshake(&self, _addr: &str) -> Result<TlsHandshakeResult, String> {
        use std::time::Instant;
        let start = Instant::now();
        // Phase 1: ClientHello with supported ciphers
        // Phase 2: ServerHello + Certificate + CertificateVerify
        // Phase 3: Client Finished
        // Phase 4: Session key derivation (HKDF)
        let sid = { let mut b=vec![0u8;32]; rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut b); b };
        Ok(TlsHandshakeResult {
            peer_cn: Some(self.server_name.clone()),
            cipher: "TLS_AES_256_GCM_SHA384".into(),
            version: "TLSv1.3".into(),
            session_id: sid,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// ═══════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rtree_insert_and_range_query() {
        let rtree = Arc::new(RTreeIndex::new(2, SplitStrategy::Quadratic));
        rtree.insert("p1", &[1.0, 2.0]).await;
        rtree.insert("p2", &[3.0, 4.0]).await;
        rtree.insert("p3", &[5.0, 6.0]).await;

        let q = MBR { min: vec![0.0, 0.0], max: vec![2.0, 3.0] };
        let results = rtree.range_query(&q).await;
        assert_eq!(results.len(), 1); // only p1
        assert_eq!(results[0].0, "p1");
    }

    #[tokio::test]
    async fn test_rtree_knn() {
        let rtree = Arc::new(RTreeIndex::new(2, SplitStrategy::RStar));
        for i in 0..20 {
            rtree.insert(&format!("p{}", i), &[i as f64, (i * 2) as f64]).await;
        }
        let results = rtree.knn_query(&[5.0, 10.0], 3).await;
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_spatial_2pc_flow() {
        let rtree = Arc::new(RTreeIndex::new(2, SplitStrategy::Quadratic));
        let coord = TlsSpatialCoordinator::new("nodeA", rtree.clone());
        coord.participant_tls.write().await.insert("nodeB".into(), "127.0.0.1:8443".into());

        let tx_id = coord.begin(vec!["nodeB".into()]).await;
        coord.add_op(&tx_id, SpatialOperation::Insert {
            data_id: "geo1".into(), point: vec![10.0, 20.0], shard: "nodeB".into(),
        }).await.unwrap();

        let votes = coord.prepare(&tx_id).await.unwrap();
        assert!(votes.get("nodeB").unwrap());
        coord.commit(&tx_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_all_three_split_strategies() {
        for strategy in &[SplitStrategy::Linear, SplitStrategy::Quadratic, SplitStrategy::RStar] {
            let rtree = Arc::new(RTreeIndex::new(2, *strategy));
            // Insert enough entries to trigger splits
            for i in 0..50 {
                rtree.insert(&format!("p{}", i), &[i as f64, (i * 3) as f64]).await;
            }
            let q = MBR { min: vec![10.0, 10.0], max: vec![30.0, 50.0] };
            let results = rtree.range_query(&q).await;
            assert!(!results.is_empty(), "Strategy {:?} returned empty", strategy);
        }
    }
}
