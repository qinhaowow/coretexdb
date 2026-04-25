//! Graph Query Engine for CoreTexDB
//! Provides graph storage and query capabilities for knowledge graphs and social networks

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    pub label: String,
    pub weight: f64,
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct GraphPath {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub total_weight: f64,
}

pub struct GraphDatabase {
    nodes: Arc<RwLock<HashMap<String, GraphNode>>>,
    edges: Arc<RwLock<HashMap<String, GraphEdge>>>,
    adjacency: Arc<RwLock<HashMap<String, Vec<String>>>>,
    reverse_adjacency: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl GraphDatabase {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            adjacency: Arc::new(RwLock::new(HashMap::new())),
            reverse_adjacency: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_node(&self, id: &str, label: &str, properties: HashMap<String, serde_json::Value>) -> Result<(), GraphError> {
        let node = GraphNode {
            id: id.to_string(),
            label: label.to_string(),
            properties,
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(id.to_string(), node);

        let mut adj = self.adjacency.write().await;
        adj.entry(id.to_string()).or_insert_with(Vec::new);

        let mut rev = self.reverse_adjacency.write().await;
        rev.entry(id.to_string()).or_insert_with(Vec::new);

        Ok(())
    }

    pub async fn add_edge(
        &self,
        id: &str,
        source: &str,
        target: &str,
        label: &str,
        weight: f64,
        properties: HashMap<String, serde_json::Value>,
    ) -> Result<(), GraphError> {
        let nodes = self.nodes.read().await;
        if !nodes.contains_key(source) {
            return Err(GraphError::NodeNotFound(source.to_string()));
        }
        if !nodes.contains_key(target) {
            return Err(GraphError::NodeNotFound(target.to_string()));
        }
        drop(nodes);

        let edge = GraphEdge {
            id: id.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            label: label.to_string(),
            weight,
            properties,
        };

        let mut edges = self.edges.write().await;
        edges.insert(id.to_string(), edge);

        let mut adj = self.adjacency.write().await;
        adj.entry(source.to_string())
            .or_insert_with(Vec::new)
            .push(id.to_string());

        let mut rev = self.reverse_adjacency.write().await;
        rev.entry(target.to_string())
            .or_insert_with(Vec::new)
            .push(id.to_string());

        Ok(())
    }

    pub async fn get_node(&self, id: &str) -> Option<GraphNode> {
        let nodes = self.nodes.read().await;
        nodes.get(id).cloned()
    }

    pub async fn get_edge(&self, id: &str) -> Option<GraphEdge> {
        let edges = self.edges.read().await;
        edges.get(id).cloned()
    }

    pub async fn get_neighbors(&self, node_id: &str) -> Vec<GraphNode> {
        let adj = self.adjacency.read().await;
        let edges = self.edges.read().await;
        let nodes = self.nodes.read().await;

        let neighbor_ids: Vec<String> = adj
            .get(node_id)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|eid| edges.get(eid).map(|e| e.target.clone()))
                    .collect()
            })
            .unwrap_or_default();

        neighbor_ids
            .iter()
            .filter_map(|id| nodes.get(id).cloned())
            .collect()
    }

    pub async fn get_incoming_neighbors(&self, node_id: &str) -> Vec<GraphNode> {
        let rev = self.reverse_adjacency.read().await;
        let edges = self.edges.read().await;
        let nodes = self.nodes.read().await;

        let neighbor_ids: Vec<String> = rev
            .get(node_id)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|eid| edges.get(eid).map(|e| e.source.clone()))
                    .collect()
            })
            .unwrap_or_default();

        neighbor_ids
            .iter()
            .filter_map(|id| nodes.get(id).cloned())
            .collect()
    }

    pub async fn bfs(&self, start: &str, max_depth: usize) -> Vec<GraphPath> {
        let mut paths = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, Vec<String>, Vec<String>, f64)> = VecDeque::new();

        queue.push_back((start.to_string(), vec![], vec![], 0.0));
        visited.insert(start.to_string());

        while let Some((current, node_ids, edge_ids, weight)) = queue.pop_front() {
            if node_ids.len() > max_depth {
                continue;
            }

            if !node_ids.is_empty() {
                let nodes: Vec<GraphNode> = node_ids
                    .iter()
                    .filter_map(|id| self.get_node(id).await)
                    .collect();
                let edges: Vec<GraphEdge> = edge_ids
                    .iter()
                    .filter_map(|id| self.get_edge(id).await)
                    .collect();

                if !nodes.is_empty() {
                    paths.push(GraphPath {
                        nodes,
                        edges,
                        total_weight: weight,
                    });
                }
            }

            let neighbors = self.get_neighbors(&current).await;
            for neighbor in neighbors {
                if !visited.contains(&neighbor.id) || node_ids.len() < max_depth - 1 {
                    let mut new_node_ids = node_ids.clone();
                    new_node_ids.push(current.clone());

                    let mut new_edge_ids = edge_ids.clone();
                    if let Some(edge) = self.find_edge(&current, &neighbor.id).await {
                        new_edge_ids.push(edge.id);
                    }

                    let new_weight = weight
                        + self
                            .find_edge(&current, &neighbor.id)
                            .await
                            .map(|e| e.weight)
                            .unwrap_or(0.0);

                    visited.insert(neighbor.id.clone());
                    queue.push_back((
                        neighbor.id,
                        new_node_ids,
                        new_edge_ids,
                        new_weight,
                    ));
                }
            }
        }

        paths
    }

    pub async fn dfs(&self, start: &str, max_depth: usize) -> Vec<GraphPath> {
        let mut paths = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();

        self.dfs_recursive(start, max_depth, vec![], vec![], 0.0, &mut visited, &mut paths)
            .await;

        paths
    }

    async fn dfs_recursive(
        &self,
        current: &str,
        max_depth: usize,
        mut node_ids: Vec<String>,
        mut edge_ids: Vec<String>,
        mut weight: f64,
        visited: &mut HashSet<String>,
        paths: &mut Vec<GraphPath>,
    ) {
        if node_ids.len() >= max_depth {
            if !node_ids.is_empty() {
                let mut all_nodes = node_ids.clone();
                all_nodes.push(current.to_string());

                let nodes: Vec<GraphNode> = all_nodes
                    .iter()
                    .filter_map(|id| self.get_node(id).await)
                    .collect();
                let edges: Vec<GraphEdge> = edge_ids
                    .iter()
                    .filter_map(|id| self.get_edge(id).await)
                    .collect();

                if !nodes.is_empty() {
                    paths.push(GraphPath {
                        nodes,
                        edges,
                        total_weight: weight,
                    });
                }
            }
            return;
        }

        visited.insert(current.to_string());

        let neighbors = self.get_neighbors(current).await;
        for neighbor in neighbors {
            if !visited.contains(&neighbor.id) || node_ids.len() < max_depth - 1 {
                if let Some(edge) = self.find_edge(current, &neighbor.id).await {
                    node_ids.push(current.to_string());
                    edge_ids.push(edge.id.clone());
                    weight += edge.weight;

                    self.dfs_recursive(
                        &neighbor.id,
                        max_depth,
                        node_ids.clone(),
                        edge_ids.clone(),
                        weight,
                        visited,
                        paths,
                    )
                    .await;

                    node_ids.pop();
                    edge_ids.pop();
                    weight -= edge.weight;
                }
            }
        }

        visited.remove(current);
    }

    async fn find_edge(&self, source: &str, target: &str) -> Option<GraphEdge> {
        let adj = self.adjacency.read().await;
        let edges = self.edges.read().await;

        adj.get(source)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|eid| edges.get(eid))
                    .find(|e| e.target == target)
                    .cloned()
            })
            .flatten()
    }

    pub async fn shortest_path(&self, start: &str, end: &str) -> Option<GraphPath> {
        let mut distances: HashMap<String, f64> = HashMap::new();
        let mut previous: HashMap<String, (String, String)> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        distances.insert(start.to_string(), 0.0);
        queue.push_back(start.to_string());

        while let Some(current) = queue.pop_front() {
            if current == end {
                break;
            }

            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            let neighbors = self.get_neighbors(&current).await;
            for neighbor in neighbors {
                if let Some(edge) = self.find_edge(&current, &neighbor.id).await {
                    let new_dist = distances.get(&current).unwrap_or(&f64::MAX) + edge.weight;

                    let existing_dist = distances.entry(neighbor.id.clone()).or_insert(f64::MAX);
                    if new_dist < *existing_dist {
                        *existing_dist = new_dist;
                        previous.insert(neighbor.id.clone(), (current.clone(), edge.id.clone()));
                        queue.push_back(neighbor.id);
                    }
                }
            }
        }

        if !previous.contains_key(end) && start != end {
            return None;
        }

        let mut path_nodes = vec![end.to_string()];
        let mut path_edges = vec![];
        let mut current = end.to_string();

        while let Some((prev, edge_id)) = previous.get(&current) {
            path_nodes.push(prev.clone());
            path_edges.push(edge_id.clone());
            current = prev.clone();
        }

        path_nodes.reverse();
        path_edges.reverse();

        let nodes: Vec<GraphNode> = path_nodes
            .iter()
            .filter_map(|id| self.get_node(id).await)
            .collect();
        let edges: Vec<GraphEdge> = path_edges
            .iter()
            .filter_map(|id| self.get_edge(id).await)
            .collect();
        let total_weight = *distances.get(end).unwrap_or(&0.0);

        Some(GraphPath {
            nodes,
            edges,
            total_weight,
        })
    }

    pub async fn delete_node(&self, id: &str) -> Result<(), GraphError> {
        let mut nodes = self.nodes.write().await;
        if !nodes.contains_key(id) {
            return Err(GraphError::NodeNotFound(id.to_string()));
        }
        nodes.remove(id);

        let mut edges = self.edges.write().await;
        let adj = self.adjacency.read().await;
        let rev = self.reverse_adjacency.read().await;

        let edges_to_remove: Vec<String> = adj
            .get(id)
            .map(|e| e.clone())
            .unwrap_or_default()
            .into_iter()
            .chain(
                rev.get(id)
                    .map(|e| e.clone())
                    .unwrap_or_default()
                    .into_iter(),
            )
            .collect();

        for edge_id in edges_to_remove {
            edges.remove(&edge_id);
        }

        let mut adj = self.adjacency.write().await;
        adj.remove(id);

        let mut rev = self.reverse_adjacency.write().await;
        rev.remove(id);

        Ok(())
    }

    pub async fn delete_edge(&self, id: &str) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        if let Some(edge) = edges.remove(id) {
            let mut adj = self.adjacency.write().await;
            if let Some(list) = adj.get_mut(&edge.source) {
                list.retain(|e| e != id);
            }

            let mut rev = self.reverse_adjacency.write().await;
            if let Some(list) = rev.get_mut(&edge.target) {
                list.retain(|e| e != id);
            }

            Ok(())
        } else {
            Err(GraphError::EdgeNotFound(id.to_string()))
        }
    }

    pub async fn get_node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    pub async fn get_edge_count(&self) -> usize {
        self.edges.read().await.len()
    }

    pub async fn find_nodes_by_label(&self, label: &str) -> Vec<GraphNode> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.label == label)
            .cloned()
            .collect()
    }

    pub async fn find_edges_by_label(&self, label: &str) -> Vec<GraphEdge> {
        let edges = self.edges.read().await;
        edges
            .values()
            .filter(|e| e.label == label)
            .cloned()
            .collect()
    }
}

impl Default for GraphDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum GraphError {
    NodeNotFound(String),
    EdgeNotFound(String),
    NodeAlreadyExists(String),
    EdgeAlreadyExists(String),
    InvalidOperation(String),
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphError::NodeNotFound(id) => write!(f, "Node not found: {}", id),
            GraphError::EdgeNotFound(id) => write!(f, "Edge not found: {}", id),
            GraphError::NodeAlreadyExists(id) => write!(f, "Node already exists: {}", id),
            GraphError::EdgeAlreadyExists(id) => write!(f, "Edge already exists: {}", id),
            GraphError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
    }
}

impl std::error::Error for GraphError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_node() {
        let graph = GraphDatabase::new();
        let props = HashMap::new();
        graph.add_node("user1", "person", props).await.unwrap();

        let node = graph.get_node("user1").await;
        assert!(node.is_some());
        assert_eq!(node.unwrap().label, "person");
    }

    #[tokio::test]
    async fn test_add_edge() {
        let graph = GraphDatabase::new();

        let props = HashMap::new();
        graph.add_node("user1", "person", props.clone()).await.unwrap();
        graph.add_node("user2", "person", props.clone()).await.unwrap();

        graph
            .add_edge("e1", "user1", "user2", "friend", 1.0, props)
            .await
            .unwrap();

        let neighbors = graph.get_neighbors("user1").await;
        assert_eq!(neighbors.len(), 1);
    }

    #[tokio::test]
    async fn test_shortest_path() {
        let graph = GraphDatabase::new();
        let props = HashMap::new();

        graph.add_node("A", "city", props.clone()).await.unwrap();
        graph.add_node("B", "city", props.clone()).await.unwrap();
        graph.add_node("C", "city", props.clone()).await.unwrap();

        graph
            .add_edge("e1", "A", "B", "road", 1.0, props.clone())
            .await
            .unwrap();
        graph
            .add_edge("e2", "B", "C", "road", 1.0, props.clone())
            .await
            .unwrap();
        graph
            .add_edge("e3", "A", "C", "road", 5.0, props.clone())
            .await
            .unwrap();

        let path = graph.shortest_path("A", "C").await.unwrap();
        assert_eq!(path.nodes.len(), 3);
        assert!((path.total_weight - 2.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_bfs() {
        let graph = GraphDatabase::new();
        let props = HashMap::new();

        graph.add_node("A", "node", props.clone()).await.unwrap();
        graph.add_node("B", "node", props.clone()).await.unwrap();
        graph.add_node("C", "node", props.clone()).await.unwrap();

        graph
            .add_edge("e1", "A", "B", "link", 1.0, props.clone())
            .await
            .unwrap();
        graph
            .add_edge("e2", "A", "C", "link", 1.0, props.clone())
            .await
            .unwrap();

        let paths = graph.bfs("A", 2).await;
        assert!(!paths.is_empty());
    }
}
