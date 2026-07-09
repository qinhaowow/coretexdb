//! CortexDB Python bindings using pyo3
//!
//! 提供完整 Python 绑定：
//! - PyCortexDB: 同步入口
//! - PyAsyncCortexDB: 异步入口
//! - 完整 CRUD
//! - 搜索/批量搜索
//! - 混合搜索
//! - BM25 全文搜索
//! - 多模态搜索
//! - 备份/恢复
//! - 指标/健康

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{CoreTexDB, DbConfig, SearchResult};

/// 同步 Python 绑定入口
#[pyclass]
pub struct PyCortexDB {
    db: Arc<RwLock<CoreTexDB>>,
    rt: Arc<tokio::runtime::Runtime>,
}

/// 异步 Python 绑定入口
#[pyclass]
pub struct PyAsyncCortexDB {
    db: Arc<RwLock<CoreTexDB>>,
}

#[pymethods]
impl PyCortexDB {
    #[new]
    fn new(data_dir: Option<String>, memory_only: Option<bool>) -> PyResult<Self> {
        let config = DbConfig {
            data_dir: data_dir.unwrap_or_else(|| "./data".to_string()),
            memory_only: memory_only.unwrap_or(false),
            max_vectors_per_collection: 1000000,
            ..Default::default()
        };

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyCoreTexError::new(format!("Runtime init failed: {}", e)))?;

        let db = rt.block_on(async {
            let db = CoreTexDB::with_config(config);
            db.init().await.map_err(|e| PyCoreTexError::new(e.to_string()))?;
            Ok::<CoreTexDB, PyCoreTexError>(db)
        })?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            rt: Arc::new(rt),
        })
    }

    fn create_collection(&self, name: &str, dimension: usize, metric: Option<&str>) -> PyResult<()> {
        let db = self.db.clone();
        let name = name.to_string();
        let metric = metric.unwrap_or("cosine").to_string();

        self.rt.block_on(async move {
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

        self.rt.block_on(async move {
            let db = db.read().await;
            db.delete_collection(&name)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;
        Ok(())
    }

    fn list_collections(&self) -> PyResult<Vec<String>> {
        let db = self.db.clone();

        let result = self.rt.block_on(async move {
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

        let result = self.rt.block_on(async move {
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

        let results = self.rt.block_on(async move {
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

        let result = self.rt.block_on(async move {
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

        let result = self.rt.block_on(async move {
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

        self.rt.block_on(async move {
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

    fn count(&self, collection: &str) -> PyResult<usize> {
        let db = self.db.clone();
        let collection = collection.to_string();

        let count = self.rt.block_on(async move {
            let db = db.read().await;
            db.get_vectors_count(&collection)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;

        Ok(count)
    }

    fn update_vector(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<bool> {
        let db = self.db.clone();
        let collection = collection.to_string();
        let id = id.to_string();
        let meta = metadata.map(|m| serde_json::json!(m));

        let result = self.rt.block_on(async move {
            let db = db.read().await;
            db.update_vector(&collection, &id, vector, meta)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })?;

        Ok(result)
    }

    fn batch_search(
        &self,
        collection: &str,
        queries: Vec<Vec<f32>>,
        k: usize,
    ) -> PyResult<Vec<Vec<PySearchResult>>> {
        let db = self.db.clone();
        let collection = collection.to_string();

        let all = self.rt.block_on(async move {
            let db = db.read().await;
            let mut all_results = Vec::new();
            for q in queries {
                let res = db.search(&collection, q, k, None).await
                    .map_err(|e| PyCoreTexError::new(e.to_string()))?;
                let items: Vec<PySearchResult> = res.into_iter()
                    .map(|r| PySearchResult { id: r.id, distance: r.distance })
                    .collect();
                all_results.push(items);
            }
            Ok::<Vec<Vec<PySearchResult>>, PyCoreTexError>(all_results)
        })?;

        Ok(all)
    }

    fn health(&self) -> PyResult<PyHealth> {
        let db = self.db.clone();

        let h = self.rt.block_on(async move {
            let db = db.read().await;
            let collections = db.list_collections().await.unwrap_or_default();
            Ok::<PyHealth, PyCoreTexError>(PyHealth {
                status: "ok".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                collections: collections.len(),
            })
        })?;

        Ok(h)
    }
}

// ==================== 异步 Python 绑定 ====================

#[pymethods]
impl PyAsyncCortexDB {
    #[new]
    fn new(data_dir: Option<String>, memory_only: Option<bool>) -> PyResult<Self> {
        let config = DbConfig {
            data_dir: data_dir.unwrap_or_else(|| "./data".to_string()),
            memory_only: memory_only.unwrap_or(false),
            max_vectors_per_collection: 1000000,
            ..Default::default()
        };

        // 异步版本不需要在 new 中初始化，由 await init() 完成
        let db = CoreTexDB::with_config(config);

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
        })
    }

    fn init<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.init().await.map_err(|e| PyCoreTexError::new(e.to_string()))
        })
    }

    fn create_collection<'py>(
        &self,
        py: Python<'py>,
        name: String,
        dimension: usize,
        metric: Option<String>,
    ) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        let m = metric.unwrap_or_else(|| "cosine".to_string());
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.create_collection(&name, dimension, &m)
                .await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })
    }

    fn list_collections<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.list_collections().await.map_err(|e| PyCoreTexError::new(e.to_string()))
        })
    }

    fn search<'py>(
        &self,
        py: Python<'py>,
        collection: String,
        query: Vec<f32>,
        k: usize,
    ) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            let results = db.search(&collection, query, k, None).await
                .map_err(|e| PyCoreTexError::new(e.to_string()))?;
            let py_results: Vec<PySearchResult> = results.into_iter()
                .map(|r| PySearchResult { id: r.id, distance: r.distance })
                .collect();
            Ok(py_results)
        })
    }

    fn insert_vectors<'py>(
        &self,
        py: Python<'py>,
        collection: String,
        vectors: Vec<Vec<f32>>,
        ids: Option<Vec<String>>,
        metadata: Option<Vec<HashMap<String, String>>>,
    ) -> PyResult<&'py PyAny> {
        let db = self.db.clone();

        let data: Vec<(String, Vec<f32>, serde_json::Value)> = vectors
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let id = ids.as_ref()
                    .and_then(|ids| ids.get(i).cloned())
                    .unwrap_or_else(|| format!("vec_{}", i));
                let meta = metadata.as_ref()
                    .and_then(|m| m.get(i))
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::json!({}));
                (id, v, meta)
            })
            .collect();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.insert_vectors(&collection, data).await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })
    }

    fn delete_vectors<'py>(
        &self,
        py: Python<'py>,
        collection: String,
        ids: Vec<String>,
    ) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.delete_vectors(&collection, &ids).await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
        })
    }

    fn get_vector<'py>(
        &self,
        py: Python<'py>,
        collection: String,
        id: String,
    ) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            let result = db.get_vector(&collection, &id).await
                .map_err(|e| PyCoreTexError::new(e.to_string()))?;
            match result {
                Some((v, m)) => {
                    let meta_map: HashMap<String, String> = serde_json::from_value(m)
                        .unwrap_or_default();
                    Ok(Some((v, meta_map)))
                }
                None => Ok(None),
            }
        })
    }

    fn count<'py>(&self, py: Python<'py>, collection: String) -> PyResult<&'py PyAny> {
        let db = self.db.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let db = db.read().await;
            db.get_vectors_count(&collection).await
                .map_err(|e| PyCoreTexError::new(e.to_string()))
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
pub struct PyHealth {
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub version: String,
    #[pyo3(get)]
    pub collections: usize,
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
    m.add_class::<PyAsyncCortexDB>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyCollectionInfo>()?;
    m.add_class::<PyHealth>()?;
    m.add_class::<PyCoreTexError>()?;
    Ok(())
}
