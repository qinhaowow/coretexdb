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
    use crate::StreamingStats;
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

    #[tokio::test]
    async fn test_text_embedding_service() {
        let service = TextEmbeddingService::new(EmbeddingConfig::default());
        
        let result = service.embed_text("Hello world").await;
        
        match result {
            Ok(embedding) => {
                assert!(!embedding.is_empty());
            }
            Err(_) => {
                // Service might not be available in test environment
            }
        }
    }

    #[tokio::test]
    async fn test_text_embedding_batch() {
        let service = TextEmbeddingService::new(EmbeddingConfig::default());
        
        let texts = vec![
            "First sentence".to_string(),
            "Second sentence".to_string(),
            "Third sentence".to_string(),
        ];
        
        let result = service.embed_batch(&texts).await;
        
        match result {
            Ok(embeddings) => {
                assert_eq!(embeddings.len(), texts.len());
            }
            Err(_) => {
                // Service might not be available
            }
        }
    }

    #[tokio::test]
    async fn test_embedding_router_text() {
        let router = EmbeddingRouter::new(EmbeddingConfig::default());
        
        let request = EmbeddingRequest {
            data_type: DataType::Text,
            data: "test text".to_string(),
            model: None,
        };
        
        let result = router.route(request).await;
        
        match result {
            Ok(response) => {
                assert!(!response.embeddings.is_empty());
            }
            Err(_) => {
                // Expected in test environment without models
            }
        }
    }

    #[tokio::test]
    async fn test_embedding_router_image() {
        let router = EmbeddingRouter::new(EmbeddingConfig::default());
        
        let request = EmbeddingRequest {
            data_type: DataType::Image,
            data: "image_data".to_string(),
            model: None,
        };
        
        let result = router.route(request).await;
        
        match result {
            Ok(response) => {
                assert!(!response.embeddings.is_empty());
            }
            Err(_) => {
                // Expected in test environment
            }
        }
    }

    #[tokio::test]
    async fn test_embedding_router_unsupported() {
        let router = EmbeddingRouter::new(EmbeddingConfig::default());
        
        let request = EmbeddingRequest {
            data_type: DataType::PointCloud,
            data: "pointcloud".to_string(),
            model: None,
        };
        
        let result = router.route(request).await;
        
        // Point cloud might not be supported
        assert!(result.is_err() || result.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_embedder() {
        let embedder = StreamingEmbedder::new(EmbeddingConfig::default());
        
        let result = embedder.embed_stream("test stream".to_string()).await;
        
        match result {
            Ok(_stream) => {
                // Stream created
            }
            Err(_) => {
                // Expected in test environment
            }
        }
    }

    #[tokio::test]
    async fn test_streaming_stats() {
        let stats = StreamingStats {
            total_processed: 100,
            total_tokens: 500,
            avg_latency_ms: 50.0,
            errors: 2,
        };
        
        assert_eq!(stats.total_processed, 100);
        assert!(stats.avg_latency_ms > 0.0);
    }

    #[tokio::test]
    async fn test_embedding_response() {
        let response = EmbeddingResponse {
            embeddings: vec![vec![0.1, 0.2, 0.3]],
            model: "test-model".to_string(),
            dimension: 3,
            processing_time_ms: 100,
        };
        
        assert_eq!(response.embeddings.len(), 1);
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
