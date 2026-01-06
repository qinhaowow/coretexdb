//! Parallel query execution for performance optimization

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ParallelConfig {
    pub max_parallel_queries: usize,
    pub timeout: Duration,
    pub batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryTask {
    pub node_id: String,
    pub params: crate::cortex_query::QueryParams,
    pub timeout: Duration,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResult {
    pub node_id: String,
    pub result: Option<crate::cortex_query::QueryResult>,
    pub error: Option<String>,
    pub execution_time: f64,
}

#[derive(Debug)]
pub struct ParallelQueryExecutor {
    config: ParallelConfig,
    active_tasks: Arc<RwLock<usize>>,
}

impl ParallelQueryExecutor {
    pub fn new() -> Self {
        Self {
            config: ParallelConfig {
                max_parallel_queries: 10,
                timeout: Duration::from_seconds(30),
                batch_size: 5,
            },
            active_tasks: Arc::new(RwLock::new(0)),
        }
    }

    pub fn with_config(mut self, config: ParallelConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn execute_parallel(&self, tasks: Vec<QueryTask>) -> Result<Vec<TaskResult>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        let mut task_batches = Vec::new();

        // Split tasks into batches
        for chunk in tasks.chunks(self.config.batch_size) {
            task_batches.push(chunk.to_vec());
        }

        // Execute each batch in parallel
        for batch in task_batches {
            let batch_results = self.execute_batch(batch).await?;
            results.extend(batch_results);
        }

        Ok(results)
    }

    async fn execute_batch(&self, tasks: Vec<QueryTask>) -> Result<Vec<TaskResult>, Box<dyn std::error::Error>> {
        let mut tasks = tasks;
        let mut results = Vec::new();
        let mut handles = Vec::new();

        // Execute tasks in parallel with concurrency limit
        for task in tasks {
            // Wait if we've reached max parallel queries
            let mut active_tasks = self.active_tasks.write().await;
            while *active_tasks >= self.config.max_parallel_queries {
                drop(active_tasks);
                tokio::time::sleep(Duration::from_millis(10)).await;
                active_tasks = self.active_tasks.write().await;
            }
            *active_tasks += 1;
            drop(active_tasks);

            // Spawn task
            let cloned_self = self.clone();
            let handle = tokio::spawn(async move {
                let result = cloned_self.execute_task(task).await;
                // Decrement active tasks counter
                let mut active_tasks = cloned_self.active_tasks.write().await;
                *active_tasks -= 1;
                result
            });

            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            if let Ok(result) = handle.await {
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn execute_task(&self, task: QueryTask) -> TaskResult {
        let start_time = std::time::Instant::now();

        // In a real implementation, we would send the query to the remote node
        // For this example, we'll simulate the execution with a delay
        let result = timeout(task.timeout, async {
            // Simulate network latency
            tokio::time::sleep(Duration::from_millis(50)).await;
            
            // Create a dummy query processor for simulation
            let dummy_processor = crate::cortex_query::DefaultQueryProcessor::new(
                Arc::new(crate::cortex_index::IndexManager::new())
            );
            
            dummy_processor.process(task.params).await
        }).await;

        let execution_time = start_time.elapsed().as_millis() as f64;

        match result {
            Ok(Ok(query_result)) => TaskResult {
                node_id: task.node_id,
                result: Some(query_result),
                error: None,
                execution_time,
            },
            Ok(Err(e)) => TaskResult {
                node_id: task.node_id,
                result: None,
                error: Some(e.to_string()),
                execution_time,
            },
            Err(_) => TaskResult {
                node_id: task.node_id,
                result: None,
                error: Some("Task timed out".to_string()),
                execution_time,
            },
        }
    }

    pub async fn get_active_tasks(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let active_tasks = self.active_tasks.read().await;
        Ok(*active_tasks)
    }
}

impl Clone for ParallelQueryExecutor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            active_tasks: self.active_tasks.clone(),
        }
    }
}
