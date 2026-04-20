//! Multimodal embedding service for CoreTexDB

pub mod text_embedding;
pub mod image_embedding;
pub mod audio_embedding;
pub mod video_embedding;
pub mod pointcloud_embedding;
pub mod embedding_router;
pub mod streaming;

pub use text_embedding::TextEmbeddingService;
pub use image_embedding::ImageEmbeddingService;
pub use audio_embedding::AudioEmbeddingService;
pub use video_embedding::VideoEmbeddingService;
pub use pointcloud_embedding::PointCloudEmbeddingService;
pub use embedding_router::{EmbeddingRouter, EmbeddingRequest, EmbeddingResponse, DataType};
pub use streaming::{StreamingEmbedder, StreamItem, StreamResult, EmbeddingStream, StreamingStats, BatchedStreamEmbedder, WindowedStreamEmbedder, BackpressureStreamEmbedder, BackpressureSignal};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    pub text_model: String,
    pub image_model: String,
    pub audio_model: String,
    pub video_model: String,
    pub pointcloud_model: String,
    pub device: String,
    pub batch_size: usize,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            text_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            image_model: "clip-vit-base-patch32".to_string(),
            audio_model: "wavlm-base".to_string(),
            video_model: "videoclip".to_string(),
            pointcloud_model: "pointnet2".to_string(),
            device: "cpu".to_string(),
            batch_size: 32,
        }
    }
}
