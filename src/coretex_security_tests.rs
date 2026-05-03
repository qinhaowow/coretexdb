//! Tests for security module (encryption, ACL, audit)

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::KeyManager;
    use crate::EncryptionService;
    use crate::EncryptedData;
    use crate::TlsConfig;
    use crate::TlsServer;
    use crate::TlsClient;

    #[tokio::test]
    async fn test_encryption_service_basic() {
        let key_manager = Arc::new(KeyManager::new());
        key_manager.generate_key("test-key", 256).await.unwrap();
        
        let encryption = Arc::new(EncryptionService::new(key_manager));
        
        let plaintext = b"Hello, CoreTexDB!";
        let encrypted = encryption.encrypt(plaintext).await.unwrap();
        
        assert_eq!(encrypted.algorithm, "AES-256-GCM");
        assert_eq!(encrypted.iv.len(), 12);
        
        let decrypted = encryption.decrypt(&encrypted).await.unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encryption_service_vector() {
        let key_manager = Arc::new(KeyManager::new());
        key_manager.generate_key("test-key", 256).await.unwrap();
        
        let encryption = Arc::new(EncryptionService::new(key_manager));
        
        let vector = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let encrypted = encryption.encrypt_vector(&vector).await.unwrap();
        
        let decrypted = encryption.decrypt_vector(&encrypted).await.unwrap();
        
        assert_eq!(decrypted.len(), vector.len());
        for (a, b) in decrypted.iter().zip(vector.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[tokio::test]
    async fn test_encryption_service_string() {
        let key_manager = Arc::new(KeyManager::new());
        key_manager.generate_key("test-key", 256).await.unwrap();
        
        let encryption = Arc::new(EncryptionService::new(key_manager));
        
        let original = "Test string for encryption";
        let encrypted = encryption.encrypt_string(original).await.unwrap();
        
        let decrypted = encryption.decrypt_string(&encrypted).await.unwrap();
        assert_eq!(decrypted, original);
    }

    #[tokio::test]
    async fn test_encryption_no_key() {
        let key_manager = Arc::new(KeyManager::new());
        
        let encryption = Arc::new(EncryptionService::new(key_manager));
        
        let result = encryption.encrypt(b"test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_key_manager_generate() {
        let key_manager = KeyManager::new();
        
        let key = key_manager.generate_key("key1", 256).await.unwrap();
        
        assert_eq!(key.id, "key1");
        assert_eq!(key.key.len(), 32);
    }

    #[tokio::test]
    async fn test_key_manager_invalid_size() {
        let key_manager = KeyManager::new();
        
        let result = key_manager.generate_key("key1", 128).await;
        assert!(result.is_ok());
        
        let result = key_manager.generate_key("key2", 512).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_key_manager_get_key() {
        let key_manager = KeyManager::new();
        
        key_manager.generate_key("key1", 256).await.unwrap();
        
        let key = key_manager.get_key("key1").await;
        assert!(key.is_some());
        
        let key = key_manager.get_key("nonexistent").await;
        assert!(key.is_none());
    }

    #[tokio::test]
    async fn test_key_manager_primary_key() {
        let key_manager = KeyManager::new();
        
        key_manager.generate_key("key1", 256).await.unwrap();
        key_manager.generate_key("key2", 256).await.unwrap();
        
        let primary = key_manager.get_primary_key().await;
        assert!(primary.is_some());
    }

    #[tokio::test]
    async fn test_key_manager_rotate() {
        let key_manager = KeyManager::new();
        
        let key1 = key_manager.generate_key("key1", 256).await.unwrap();
        let key1_hash = key1.key_hash();
        
        let key2 = key_manager.rotate_key("key1").await.unwrap();
        let key2_hash = key2.key_hash();
        
        assert_ne!(key1_hash, key2_hash);
    }

    #[tokio::test]
    async fn test_encrypted_data_base64() {
        let key_manager = Arc::new(KeyManager::new());
        key_manager.generate_key("test-key", 256).await.unwrap();
        
        let encryption = Arc::new(EncryptionService::new(key_manager.clone()));
        
        let plaintext = b"Test data";
        let encrypted = encryption.encrypt(plaintext).await.unwrap();
        
        let base64 = encrypted.to_base64();
        assert!(!base64.is_empty());
        
        let key = key_manager.get_primary_key().await.unwrap();
        let decrypted = EncryptedData::from_base64(&key.id, &base64).unwrap();
        
        let result = encryption.decrypt(&decrypted).await.unwrap();
        assert_eq!(&result, plaintext);
    }

    #[tokio::test]
    async fn test_encrypted_data_invalid_base64() {
        let result = EncryptedData::from_base64("key", "invalid-base64!!!");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_encrypted_data_short_length() {
        let key_manager = Arc::new(KeyManager::new());
        key_manager.generate_key("test-key", 256).await.unwrap();
        
        let encryption = Arc::new(EncryptionService::new(key_manager));
        
        let result = EncryptedData::from_base64("key", "YWJjZA==");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_encryption_decryption_wrong_key() {
        let key_manager1 = Arc::new(KeyManager::new());
        key_manager1.generate_key("key1", 256).await.unwrap();
        
        let key_manager2 = Arc::new(KeyManager::new());
        key_manager2.generate_key("key2", 256).await.unwrap();
        
        let encryption1 = Arc::new(EncryptionService::new(key_manager1));
        let encryption2 = Arc::new(EncryptionService::new(key_manager2));
        
        let plaintext = b"Secret message";
        let encrypted = encryption1.encrypt(plaintext).await.unwrap();
        
        let result = encryption2.decrypt(&encrypted).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tls_config_default() {
        let config = TlsConfig::default();
        
        assert_eq!(config.cert_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
        assert!(config.ca_path.is_none());
    }

    #[tokio::test]
    async fn test_tls_config_for_development() {
        let config = TlsConfig::for_development();
        
        assert!(!config.verify_client);
    }

    #[tokio::test]
    async fn test_tls_server_config() {
        let config = TlsConfig::default();
        let server = TlsServer::new(config.clone());
        
        assert_eq!(server.config().cert_path, "cert.pem");
    }

    #[tokio::test]
    async fn test_tls_client_verify() {
        let config = TlsConfig::default();
        let client = TlsClient::new(config);
        
        let result = client.verify_server_cert(b"test_cert");
        assert!(result.is_ok());
    }
}
