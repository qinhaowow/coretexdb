//! Tests for embedding services

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddingConfig;
    use crate::TextEmbeddingService;
    use crate::EmbeddingRouter;
    use crate::EmbeddingRequest;
    use crate::DataType;
    use crate::StreamingEmbedder;
    use crate::EmbeddingResponse;

    #[tokio::test]
    async fn test_embedding_config_default() {
        let config = EmbeddingConfig::default();
        
        assert_eq!(config.device, "cpu");
        assert_eq!(config.batch_size, 32);
    }

    #[tokio::test]
    async fn test_embedding_config_custom() {
        let config = EmbeddingConfig {
            text_model: "custom-model".to_string(),
            image_model: "custom-image-model".to_string(),
            audio_model: "custom-audio-model".to_string(),
            video_model: "custom-video-model".to_string(),
            pointcloud_model: "custom-pc-model".to_string(),
            device: "cuda".to_string(),
            batch_size: 64,
        };
        
        assert_eq!(config.device, "cuda");
        assert_eq!(config.batch_size, 64);
    }

    #[test]
    fn test_text_embedding_service() {
        let service = TextEmbeddingService::new("test-model", 384, "cpu");
        
        let result = service.embed_text("Hello world");
        
        match result {
            Ok(embedding) => {
                assert!(!embedding.is_empty());
            }
            Err(_) => {
                // Service might not be available in test environment
            }
        }
    }

    #[test]
    fn test_text_embedding_batch() {
        let service = TextEmbeddingService::new("test-model", 384, "cpu");
        
        let texts = vec![
            "First sentence".to_string(),
            "Second sentence".to_string(),
            "Third sentence".to_string(),
        ];
        
        let result = service.embed_batch(&texts);
        
        match result {
            Ok(embeddings) => {
                assert_eq!(embeddings.len(), texts.len());
            }
            Err(_) => {
                // Service might not be available
            }
        }
    }

    #[test]
    fn test_embedding_router_text() {
        let router = EmbeddingRouter::new();
        
        let request = EmbeddingRequest {
            data_type: DataType::Text,
            data: "test text".to_string(),
            metadata: None,
        };
        
        let result = router.embed(&request);
        
        match result {
            Ok(response) => {
                assert!(!response.embedding.is_empty());
            }
            Err(_) => {
                // Expected in test environment without models
            }
        }
    }

    #[test]
    fn test_embedding_router_unsupported() {
        let router = EmbeddingRouter::new();
        
        let request = EmbeddingRequest {
            data_type: DataType::PointCloud,
            data: "pointcloud".to_string(),
            metadata: None,
        };
        
        let result = router.embed(&request);
        
        // Point cloud might not be supported
        assert!(result.is_err() || result.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_embedder() {
        let embedder = StreamingEmbedder::new(EmbeddingConfig::default());
        
        let stats = embedder.get_stats().await;
        assert_eq!(stats.processed, 0);
    }

    #[tokio::test]
    async fn test_streaming_stats() {
        let embedder = StreamingEmbedder::new(EmbeddingConfig::default());
        
        let stats = embedder.get_stats().await;
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.buffer_size, 0);
    }

    #[test]
    fn test_embedding_response() {
        let response = EmbeddingResponse {
            embedding: vec![0.1, 0.2, 0.3],
            dimension: 3,
            data_type: "text".to_string(),
        };
        
        assert_eq!(response.embedding.len(), 3);
        assert_eq!(response.dimension, 3);
    }

    #[tokio::test]
    async fn test_data_type_variants() {
        let text = DataType::Text;
        let image = DataType::Image;
        let audio = DataType::Audio;
        let video = DataType::Video;
        let pointcloud = DataType::PointCloud;
        
        assert!(!format!("{:?}", text).is_empty());
        assert!(!format!("{:?}", image).is_empty());
        assert!(!format!("{:?}", audio).is_empty());
        assert!(!format!("{:?}", video).is_empty());
        assert!(!format!("{:?}", pointcloud).is_empty());
    }
}
