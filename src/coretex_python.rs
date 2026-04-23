//! CortexDB Python bindings using pyo3

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{CoreTexDB, DbConfig, SearchResult};

#[pyclass]
pub struct PyCortexDB {
    db: Arc<RwLock<CoreTexDB>>,
}

#[pymethods]
impl PyCortexDB {
    #[new]
    fn new(data_dir: Option<String>, memory_only: Option<bool>) -> Self {
        let config = DbConfig {
            data_dir: data_dir.unwrap_or_else(|| "./data".to_string()),
            memory_only: memory_only.unwrap_or(false),
            max_vectors_per_collection: 1000000,
        };
        
        Self {
            db: Arc::new(RwLock::new(CoreTexDB::with_config(config))),
        }
    }

    fn init(&self) -> PyResult<()> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.init().await.map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        Ok(())
    }

    fn create_collection(&self, name: &str, dimension: usize, metric: Option<&str>) -> PyResult<()> {
        let db = self.db.clone();
        let name = name.to_string();
        let metric = metric.unwrap_or("cosine").to_string();
        
        pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.create_collection(&name, dimension, &metric)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        Ok(())
    }

    fn delete_collection(&self, name: &str) -> PyResult<()> {
        let db = self.db.clone();
        let name = name.to_string();
        
        pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.delete_collection(&name)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        Ok(())
    }

    fn list_collections(&self) -> PyResult<Vec<String>> {
        let db = self.db.clone();
        
        let result = pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.list_collections().await.map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        
        Ok(result)
    }

    fn insert_vectors(
        &self,
        collection: &str,
        vectors: Vec<Vec<f32>>,
        metadata: Option<Vec<HashMap<String, String>>>,
        ids: Option<Vec<String>>,
    ) -> PyResult<Vec<String>> {
        let db = self.db.clone();
        let collection = collection.to_string();
        
        let vectors_data: Vec<(String, Vec<f32>, serde_json::Value)> = vectors
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let id = ids.as_ref()
                    .and_then(|ids| ids.get(i))
                    .map(|s| s.clone())
                    .unwrap_or_else(|| format!("vec_{}", i));
                let meta = metadata.as_ref()
                    .and_then(|m| m.get(i))
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::json!({}));
                (id, v, meta)
            })
            .collect();
        
        let result = pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.insert_vectors(&collection, vectors_data)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        
        Ok(result)
    }

    fn search(
        &self,
        collection: &str,
        query: Vec<f32>,
        k: usize,
    ) -> PyResult<Vec<PySearchResult>> {
        let db = self.db.clone();
        let collection = collection.to_string();
        
        let results = pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.search(&collection, query, k, None)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        
        Ok(results
            .into_iter()
            .map(|r| PySearchResult {
                id: r.id,
                distance: r.distance,
            })
            .collect())
    }

    fn get_vector(
        &self,
        collection: &str,
        id: &str,
    ) -> PyResult<Option<(Vec<f32>, HashMap<String, String>)>> {
        let db = self.db.clone();
        let collection = collection.to_string();
        let id = id.to_string();
        
        let result = pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.get_vector(&collection, &id)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        
        match result {
            Some((vector, metadata)) => {
                let meta_map: HashMap<String, String> = serde_json::from_value(metadata)
                    .unwrap_or_default();
                Ok(Some((vector, meta_map)))
            }
            None => Ok(None),
        }
    }

    fn delete_vectors(&self, collection: &str, ids: Vec<String>) -> PyResult<usize> {
        let db = self.db.clone();
        let collection = collection.to_string();
        
        let result = pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            db.delete_vectors(&collection, &ids)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        
        Ok(result)
    }

    fn get_collection_info(&self, name: &str) -> PyResult<PyCollectionInfo> {
        let db = self.db.clone();
        let name = name.to_string();
        
        pyo3_asyncio::tokio::run(async move {
            let db = db.read().await;
            let schema = db.get_collection(&name)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))?;
            let count = db.get_vectors_count(&name)
                .await
                .unwrap_or(0);
            
            Ok(PyCollectionInfo {
                name: schema.name,
                dimension: schema.dimension,
                metric: format!("{:?}", schema.distance_metric),
                vector_count: count,
            })
        })
    }
}

#[pyclass]
pub struct PySearchResult {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub distance: f32,
}

#[pyclass]
pub struct PyCollectionInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub dimension: usize,
    #[pyo3(get)]
    pub metric: String,
    #[pyo3(get)]
    pub vector_count: usize,
}

#[pyclass]
pub struct PyCoreTexError {
    message: String,
}

impl PyCoreTexError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Debug for PyCoreTexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Display for PyCoreTexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PyCoreTexError {}

impl From<PyCoreTexError> for PyErr {
    fn from(err: PyCoreTexError) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(err.message)
    }
}

#[pymodule]
pub fn coretexdb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyCortexDB>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyCollectionInfo>()?;
    m.add_class::<PyCoreTexError>()?;
    Ok(())
}
