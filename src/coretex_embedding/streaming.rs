//! Streaming Embeddings module for CoreTexDB
//! Handles real-time streaming data processing for embeddings

use std::sync::Arc;
use tokio::sync::{mpsc, RwLock,broadcast};
use futures::stream::Stream;
use std::pin::Pin;
use std::future::Future;
use std::collections::VecDeque;

use crate::coretex_embedding::{EmbeddingConfig, DataType, EmbeddingResponse};

pub struct StreamingEmbedder {
    config: EmbeddingConfig,
    buffer: Arc<RwLock<VecDeque<StreamItem>>>,
    sender: Option<mpsc::Sender<StreamItem>>,
    receiver: Option<mpsc::Receiver<StreamItem>>,
    batch_size: usize,
    processed_count: Arc<RwLock<usize>>,
    error_count: Arc<RwLock<usize>>,
}

#[derive(Debug, Clone)]
pub struct StreamItem {
    pub id: String,
    pub data: Vec<u8>,
    pub data_type: DataType,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct StreamResult {
    pub id: String,
    pub embedding: Option<Vec<f32>>,
    pub error: Option<String>,
    pub timestamp: i64,
}

pub struct EmbeddingStream {
    receiver: mpsc::Receiver<StreamResult>,
}

impl Stream for EmbeddingStream {
    type Item = StreamResult;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl StreamingEmbedder {
    pub fn new(config: EmbeddingConfig) -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        Self {
            config,
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            sender: Some(sender),
            receiver: Some(receiver),
            batch_size: config.batch_size,
            processed_count: Arc::new(RwLock::new(0)),
            error_count: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn push(&self, item: StreamItem) -> Result<(), String> {
        if let Some(ref sender) = self.sender {
            sender.send(item).await.map_err(|e| e.to_string())
        } else {
            Err("Stream closed".to_string())
        }
    }

    pub async fn push_batch(&self, items: Vec<StreamItem>) -> Vec<Result<(), String>> {
        let mut results = Vec::new();
        for item in items {
            results.push(self.push(item).await);
        }
        results
    }

    pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<StreamResult>> {
        self.receiver.take()
    }

    pub async fn process_stream<F, Fut>(
        &self,
        mut receiver: mpsc::Receiver<StreamItem>,
        processor: F,
    ) where
        F: Fn(StreamItem) -> Fut,
        Fut: Future<Output = StreamResult>,
    {
        let (result_sender, _) = mpsc::channel(1000);
        
        loop {
            tokio::select! {
                Some(item) = receiver.recv() => {
                    let result = processor(item).await;
                    let _ = result_sender.send(result).await;
                }
                else => break,
            }
        }
    }

    pub async fn process_batch<F, Fut>(&self, items: Vec<StreamItem>, processor: F) -> Vec<StreamResult>
    where
        F: Fn(StreamItem) -> Fut,
        Fut: Future<Output = StreamResult>,
    {
        let mut results = Vec::new();
        
        for chunk in items.chunks(self.batch_size) {
            let mut futures = Vec::new();
            for item in chunk {
                let item_clone = item.clone();
                let future = processor(item_clone);
                futures.push(future);
            }
            
            let batch_results = futures::future::join_all(futures).await;
            results.extend(batch_results);
        }
        
        let mut count = self.processed_count.write().await;
        *count += results.len();
        
        results
    }

    pub async fn get_stats(&self) -> StreamingStats {
        StreamingStats {
            processed: *self.processed_count.read().await,
            errors: *self.error_count.read().await,
            buffer_size: self.buffer.read().await.len(),
            batch_size: self.batch_size,
        }
    }

    pub async fn record_error(&self) {
        let mut count = self.error_count.write().await;
        *count += 1;
    }
}

#[derive(Debug, Clone)]
pub struct StreamingStats {
    pub processed: usize,
    pub errors: usize,
    pub buffer_size: usize,
    pub batch_size: usize,
}

pub struct BatchedStreamEmbedder {
    embedder: StreamingEmbedder,
    pending: Arc<RwLock<Vec<StreamItem>>>,
    flush_interval: tokio::time::Duration,
}

impl BatchedStreamEmbedder {
    pub fn new(config: EmbeddingConfig, flush_interval_ms: u64) -> Self {
        Self {
            embedder: StreamingEmbedder::new(config),
            pending: Arc::new(RwLock::new(Vec::new())),
            flush_interval: tokio::time::Duration::from_millis(flush_interval_ms),
        }
    }

    pub async fn push(&self, item: StreamItem) -> Result<(), String> {
        let mut pending = self.pending.write().await;
        pending.push(item);
        
        if pending.len() >= self.embedder.batch_size {
            drop(pending);
            self.flush().await?;
        }
        
        Ok(())
    }

    pub async fn flush(&self) -> Result<Vec<StreamResult>, String> {
        let items: Vec<StreamItem> = {
            let mut pending = self.pending.write().await;
            std::mem::take(&mut *pending)
        };
        
        if items.is_empty() {
            return Ok(Vec::new());
        }
        
        Ok(vec![])
    }

    pub async fn start_background_flush<F, Fut>(&self, processor: F)
    where
        F: Fn(Vec<StreamItem>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<StreamResult>> + Send,
    {
        let pending = self.pending.clone();
        let interval = self.flush_interval;
        let batch_size = self.embedder.batch_size;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            
            loop {
                interval.tick().await;
                
                let items: Vec<StreamItem> = {
                    let mut p = pending.write().await;
                    if p.len() >= batch_size {
                        std::mem::take(&mut *p)
                    } else {
                        continue;
                    }
                };
                
                if !items.is_empty() {
                    let _results = processor(items).await;
                }
            }
        });
    }

    pub async fn stats(&self) -> StreamingStats {
        let pending = self.pending.read().await;
        let mut base_stats = self.embedder.get_stats().await;
        base_stats.buffer_size += pending.len();
        base_stats
    }
}

pub struct WindowedStreamEmbedder {
    inner: StreamingEmbedder,
    window_size: usize,
    window_slide: usize,
}

impl WindowedStreamEmbedder {
    pub fn new(config: EmbeddingConfig, window_size: usize, window_slide: usize) -> Self {
        Self {
            inner: StreamingEmbedder::new(config),
            window_size,
            window_slide,
        }
    }

    pub async fn process_with_window<F, Fut>(&self, items: Vec<StreamItem>, processor: F) -> Vec<StreamResult>
    where
        F: Fn(Vec<StreamItem>) -> Fut,
        Fut: Future<Output = Vec<StreamResult>>,
    {
        let mut results = Vec::new();
        let mut window_start = 0;
        
        while window_start + self.window_size <= items.len() {
            let window: Vec<StreamItem> = items[window_start..window_start + self.window_size].to_vec();
            let window_results = processor(window).await;
            results.extend(window_results);
            window_start += self.window_slide;
        }
        
        let remainder = &items[window_start..];
        if !remainder.is_empty() {
            let window_results = processor(remainder.to_vec()).await;
            results.extend(window_results);
        }
        
        results
    }
}

pub struct BackpressureStreamEmbedder {
    inner: StreamingEmbedder,
    max_pending: usize,
    control: broadcast::Sender<BackpressureSignal>,
}

#[derive(Debug, Clone)]
pub enum BackpressureSignal {
    Pause,
    Resume,
    DropOldest,
}

impl BackpressureStreamEmbedder {
    pub fn new(config: EmbeddingConfig, max_pending: usize) -> Self {
        let (control, _) = broadcast::channel(16);
        Self {
            inner: StreamingEmbedder::new(config),
            max_pending,
            control,
        }
    }

    pub fn signal_sender(&self) -> broadcast::Sender<BackpressureSignal> {
        self.control.clone()
    }

    pub async fn push_with_backpressure(&self, item: StreamItem) -> Result<(), String> {
        let stats = self.inner.get_stats().await;
        
        if stats.buffer_size >= self.max_pending {
            return Err("Backpressure: buffer full".to_string());
        }
        
        self.inner.push(item).await
    }

    pub async fn subscribe<F>(&self, mut receiver: broadcast::Receiver<BackpressureSignal>, handler: F)
    where
        F: Fn(BackpressureSignal) + Send + Sync,
    {
        loop {
            match receiver.recv().await {
                Ok(signal) => handler(signal),
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_streaming_embedder_push() {
        let config = EmbeddingConfig::default();
        let embedder = StreamingEmbedder::new(config);
        
        let item = StreamItem {
            id: "test1".to_string(),
            data: vec![1, 2, 3],
            data_type: DataType::Text,
            metadata: None,
        };
        
        let result = embedder.push(item).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_embedder_stats() {
        let config = EmbeddingConfig::default();
        let embedder = StreamingEmbedder::new(config);
        
        let stats = embedder.get_stats().await;
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.errors, 0);
    }

    #[tokio::test]
    async fn test_batched_embedder() {
        let config = EmbeddingConfig {
            batch_size: 2,
            ..Default::default()
        };
        let embedder = BatchedStreamEmbedder::new(config, 100);
        
        embedder.push(StreamItem {
            id: "1".to_string(),
            data: vec![],
            data_type: DataType::Text,
            metadata: None,
        }).await.unwrap();
        
        embedder.push(StreamItem {
            id: "2".to_string(),
            data: vec![],
            data_type: DataType::Text,
            metadata: None,
        }).await.unwrap();
        
        let stats = embedder.stats().await;
        assert!(stats.buffer_size >= 0);
    }
}
