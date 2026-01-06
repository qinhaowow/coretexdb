//! Token management functionality for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use std::time::Duration;
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenClaims {
    pub sub: String, // User ID
    pub role: String,
    pub exp: u64,
    pub iat: u64,
    pub jti: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenConfig {
    pub secret_key: String,
    pub token_expiry: Duration,
    pub algorithm: Algorithm,
}

#[derive(Debug)]
pub struct TokenManager {
    config: TokenConfig,
    valid_tokens: Arc<RwLock<Vec<String>>>,
}

impl TokenManager {
    pub fn new() -> Self {
        Self {
            config: TokenConfig {
                secret_key: "your-secret-key-here".to_string(), // In production, this should be from environment variables
                token_expiry: Duration::from_hours(24),
                algorithm: Algorithm::HS256,
            },
            valid_tokens: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_config(mut self, config: TokenConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn generate_token(&self, user_id: &str, role: &str) -> Result<String, Box<dyn std::error::Error>> {
        let now = chrono::Utc::now();
        let exp = (now + chrono::Duration::from_std(self.config.token_expiry)?).timestamp() as u64;
        let iat = now.timestamp() as u64;
        let jti = uuid::Uuid::new_v4().to_string();

        let claims = TokenClaims {
            sub: user_id.to_string(),
            role: role.to_string(),
            exp,
            iat,
            jti,
        };

        let token = encode(&Header::new(self.config.algorithm), &claims, &EncodingKey::from_secret(self.config.secret_key.as_bytes()))?;

        // Add token to valid tokens
        let mut valid_tokens = self.valid_tokens.write().await;
        valid_tokens.push(token.clone());

        Ok(token)
    }

    pub async fn validate_token(&self, token: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Check if token is in valid tokens
        let valid_tokens = self.valid_tokens.read().await;
        if !valid_tokens.contains(&token.to_string()) {
            return Err("Token not found".into());
        }
        drop(valid_tokens);

        // Validate token
        let decoding_key = DecodingKey::from_secret(self.config.secret_key.as_bytes());
        let validation = Validation::new(self.config.algorithm);
        let decoded = decode::<TokenClaims>(token, &decoding_key, &validation)?;

        // Check if token is expired
        let now = chrono::Utc::now().timestamp() as u64;
        if decoded.claims.exp < now {
            // Remove expired token
            let mut valid_tokens = self.valid_tokens.write().await;
            valid_tokens.retain(|t| t != token);
            return Err("Token expired".into());
        }

        Ok(true)
    }

    pub async fn revoke_token(&self, token: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut valid_tokens = self.valid_tokens.write().await;
        let token_index = valid_tokens.iter().position(|t| t == token);

        if let Some(index) = token_index {
            valid_tokens.remove(index);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn get_user_id_from_token(&self, token: &str) -> Result<String, Box<dyn std::error::Error>> {
        let decoding_key = DecodingKey::from_secret(self.config.secret_key.as_bytes());
        let validation = Validation::new(self.config.algorithm);
        let decoded = decode::<TokenClaims>(token, &decoding_key, &validation)?;

        Ok(decoded.claims.sub)
    }

    pub async fn get_token_expiry(&self, token: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let decoding_key = DecodingKey::from_secret(self.config.secret_key.as_bytes());
        let validation = Validation::new(self.config.algorithm);
        let decoded = decode::<TokenClaims>(token, &decoding_key, &validation)?;

        Ok(decoded.claims.exp)
    }

    pub async fn cleanup_expired_tokens(&self) -> Result<(), Box<dyn std::error::Error>> {
        let now = chrono::Utc::now().timestamp() as u64;
        let mut valid_tokens = self.valid_tokens.write().await;
        
        let mut expired_tokens = Vec::new();
        for token in &*valid_tokens {
            if let Ok(expiry) = self.get_token_expiry(token).await {
                if expiry < now {
                    expired_tokens.push(token.clone());
                }
            }
        }
        
        // Remove expired tokens
        for token in expired_tokens {
            valid_tokens.retain(|t| t != &token);
        }
        
        Ok(())
    }

    pub async fn start_cleanup_task(&self) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_hours(1)).await;
                if let Err(e) = cloned_self.cleanup_expired_tokens().await {
                    eprintln!("Error cleaning up expired tokens: {}", e);
                }
            }
        });
    }
}

impl Clone for TokenManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            valid_tokens: self.valid_tokens.clone(),
        }
    }
}
