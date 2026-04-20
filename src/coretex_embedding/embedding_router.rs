//! Embedding router -统一多模态嵌入服务

use serde::{Deserialize, Serialize};
use std::error::Error;

use crate::coretex_embedding::{
    TextEmbeddingService,
    ImageEmbeddingService,
    AudioEmbeddingService,
    VideoEmbeddingService,
    PointCloudEmbeddingService,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Text,
    Image,
    Audio,
    Video,
    PointCloud,
    VoxelGrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub data_type: DataType,
    pub data: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingResponse {
    pub embedding: Vec<f32>,
    pub dimension: usize,
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub struct EmbeddingRouter {
    text_service: TextEmbeddingService,
    image_service: ImageEmbeddingService,
    audio_service: AudioEmbeddingService,
    video_service: VideoEmbeddingService,
    pointcloud_service: PointCloudEmbeddingService,
}

impl EmbeddingRouter {
    pub fn new() -> Self {
        Self {
            text_service: TextEmbeddingService::with_defaults(),
            image_service: ImageEmbeddingService::with_defaults(),
            audio_service: AudioEmbeddingService::with_defaults(),
            video_service: VideoEmbeddingService::with_defaults(),
            pointcloud_service: PointCloudEmbeddingService::with_defaults(),
        }
    }

    pub fn embed(&self, request: &EmbeddingRequest) -> Result<EmbeddingResponse, Box<dyn Error + Send + Sync>> {
        match request.data_type {
            DataType::Text => {
                let embedding = self.text_service.embed_text(&request.data)?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.text_service.get_dimension(),
                    data_type: "text".to_string(),
                })
            }
            DataType::Image => {
                let image_data = std::fs::read(&request.data)?;
                let embedding = self.image_service.embed_image(&image_data)?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.image_service.get_dimension(),
                    data_type: "image".to_string(),
                })
            }
            DataType::Audio => {
                let embedding = self.audio_service.embed_audio_path(&request.data)?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.audio_service.get_dimension(),
                    data_type: "audio".to_string(),
                })
            }
            DataType::Video => {
                let embedding = self.video_service.embed_video_path(&request.data)?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.video_service.get_dimension(),
                    data_type: "video".to_string(),
                })
            }
            DataType::PointCloud => {
                let embedding = self.pointcloud_service.embed_point_cloud_path(&request.data)?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.pointcloud_service.get_dimension(),
                    data_type: "pointcloud".to_string(),
                })
            }
            DataType::VoxelGrid => {
                let voxel_data = std::fs::read(&request.data)?;
                let embedding = self.pointcloud_service.embed_voxel_grid(&voxel_data, (32, 32, 32))?;
                Ok(EmbeddingResponse {
                    embedding,
                    dimension: self.pointcloud_service.get_dimension(),
                    data_type: "voxel".to_string(),
                })
            }
        }
    }

    pub fn embed_text(&self, text: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        self.text_service.embed_text(text)
    }

    pub fn embed_image(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let image_data = std::fs::read(path)?;
        self.image_service.embed_image(&image_data)
    }

    pub fn embed_audio(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        self.audio_service.embed_audio_path(path)
    }

    pub fn embed_video(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        self.video_service.embed_video_path(path)
    }

    pub fn embed_pointcloud(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        self.pointcloud_service.embed_point_cloud_path(path)
    }

    pub fn get_dimension(&self, data_type: &DataType) -> usize {
        match data_type {
            DataType::Text => self.text_service.get_dimension(),
            DataType::Image => self.image_service.get_dimension(),
            DataType::Audio => self.audio_service.get_dimension(),
            DataType::Video => self.video_service.get_dimension(),
            DataType::PointCloud | DataType::VoxelGrid => self.pointcloud_service.get_dimension(),
        }
    }
}

impl Default for EmbeddingRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embed_text() {
        let router = EmbeddingRouter::new();
        let embedding = router.embed_text("Hello world").unwrap();
        
        assert_eq!(embedding.len(), 384);
    }

    #[test]
    fn test_embed_request_text() {
        let router = EmbeddingRouter::new();
        let request = EmbeddingRequest {
            data_type: DataType::Text,
            data: "Test text".to_string(),
            metadata: None,
        };
        
        let response = router.embed(&request).unwrap();
        
        assert_eq!(response.dimension, 384);
        assert_eq!(response.data_type, "text");
    }
}
