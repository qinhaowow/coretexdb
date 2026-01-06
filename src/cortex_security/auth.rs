//! Authentication functionality for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use argon2::{Argon2, PasswordHash, PasswordVerifier};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub role: String,
    pub created_at: u64,
    pub last_login: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthResponse {
    pub token: String,
    pub user: User,
    pub expires_at: u64,
}

#[derive(Debug)]
pub struct AuthManager {
    users: Arc<RwLock<Vec<User>>>,
    token_manager: Arc<crate::cortex_security::token::TokenManager>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(Vec::new())),
            token_manager: Arc::new(crate::cortex_security::token::TokenManager::new()),
        }
    }

    pub fn with_token_manager(mut self, token_manager: Arc<crate::cortex_security::token::TokenManager>) -> Self {
        self.token_manager = token_manager;
        self
    }

    pub async fn register_user(&self, username: &str, password: &str, role: &str) -> Result<User, Box<dyn std::error::Error>> {
        // Check if user already exists
        let users = self.users.read().await;
        if users.iter().any(|u| u.username == username) {
            return Err("User already exists".into());
        }
        drop(users);

        // Hash password
        let password_hash = argon2::PasswordHasher::hash(&Argon2::default(), password.as_bytes())?;

        // Create user
        let user = User {
            id: uuid::Uuid::new_v4().to_string(),
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            role: role.to_string(),
            created_at: chrono::Utc::now().timestamp() as u64,
            last_login: None,
        };

        // Add user to list
        let mut users = self.users.write().await;
        users.push(user.clone());

        Ok(user)
    }

    pub async fn authenticate(&self, request: AuthRequest) -> Result<AuthResponse, Box<dyn std::error::Error>> {
        // Find user
        let users = self.users.read().await;
        let user = users.iter().find(|u| u.username == request.username)
            .ok_or("User not found")?;

        // Verify password
        let password_hash = PasswordHash::new(&user.password_hash)?;
        Argon2::default().verify_password(request.password.as_bytes(), &password_hash)?;

        // Generate token
        let token = self.token_manager.generate_token(&user.id, &user.role).await?;
        let expires_at = self.token_manager.get_token_expiry(&token).await?;

        // Update last login
        drop(users);
        let mut users = self.users.write().await;
        if let Some(user_mut) = users.iter_mut().find(|u| u.username == request.username) {
            user_mut.last_login = Some(chrono::Utc::now().timestamp() as u64);
        }

        Ok(AuthResponse {
            token,
            user: user.clone(),
            expires_at,
        })
    }

    pub async fn get_user(&self, user_id: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
        let users = self.users.read().await;
        Ok(users
            .iter()
            .find(|u| u.id == user_id)
            .cloned())
    }

    pub async fn list_users(&self) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let users = self.users.read().await;
        Ok(users.clone())
    }

    pub async fn delete_user(&self, user_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut users = self.users.write().await;
        let user_index = users.iter().position(|u| u.id == user_id);

        if let Some(index) = user_index {
            users.remove(index);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn update_user_role(&self, user_id: &str, role: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut users = self.users.write().await;
        if let Some(user) = users.iter_mut().find(|u| u.id == user_id) {
            user.role = role.to_string();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn validate_token(&self, token: &str) -> Result<bool, Box<dyn std::error::Error>> {
        self.token_manager.validate_token(token).await
    }

    pub async fn get_user_from_token(&self, token: &str) -> Result<Option<User>, Box<dyn std::error::Error>> {
        if let Ok(user_id) = self.token_manager.get_user_id_from_token(token).await {
            self.get_user(&user_id).await
        } else {
            Ok(None)
        }
    }
}

impl Clone for AuthManager {
    fn clone(&self) -> Self {
        Self {
            users: self.users.clone(),
            token_manager: self.token_manager.clone(),
        }
    }
}
