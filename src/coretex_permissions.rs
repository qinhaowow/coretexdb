//! Fine-grained permission control module for CoreTexDB
//! Provides column-level, field-level, and row-level access control

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    pub id: String,
    pub name: String,
    pub resource_type: PermissionResource,
    pub actions: Vec<PermissionAction>,
    pub effect: PermissionEffect,
    pub conditions: Vec<PermissionCondition>,
    pub priority: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionResource {
    Collection,
    Vector,
    Field,
    Column,
    Row,
    Index,
    Query,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionAction {
    Create,
    Read,
    Update,
    Delete,
    Search,
    Query,
    Admin,
    Execute,
    Export,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionCondition {
    pub field: String,
    pub operator: ConditionOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Contains,
    StartsWith,
    EndsWith,
    In,
    NotIn,
    Between,
    Regex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionScope {
    pub collection: Option<String>,
    pub vector_ids: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
    pub metadata_filter: Option<MetadataPermissionFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataPermissionFilter {
    pub conditions: Vec<MetadataCondition>,
    pub combine: FilterCombine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCondition {
    pub field: String,
    pub operator: ConditionOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterCombine {
    And,
    Or,
}

pub struct FineGrainedPermissionEngine {
    roles: Arc<RwLock<HashMap<String, Role>>>,
    users: Arc<RwLock<HashMap<String, User>>>,
    permissions: Arc<RwLock<HashMap<String, Permission>>>,
    role_permissions: Arc<RwLock<HashMap<String, Vec<String>>>>,
    user_roles: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub id: String,
    pub name: String,
    pub description: String,
    pub permissions: Vec<String>,
    pub inherits_from: Vec<String>,
    pub is_system: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub email: String,
    pub roles: Vec<String>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub is_active: bool,
}

impl FineGrainedPermissionEngine {
    pub fn new() -> Self {
        Self {
            roles: Arc::new(RwLock::new(HashMap::new())),
            users: Arc::new(RwLock::new(HashMap::new())),
            permissions: Arc::new(RwLock::new(HashMap::new())),
            role_permissions: Arc::new(RwLock::new(HashMap::new())),
            user_roles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_permission(&self, permission: Permission) -> Result<(), String> {
        let mut permissions = self.permissions.write().await;
        permissions.insert(permission.id.clone(), permission);
        Ok(())
    }

    pub async fn get_permission(&self, id: &str) -> Option<Permission> {
        let permissions = self.permissions.read().await;
        permissions.get(id).cloned()
    }

    pub async fn assign_permission_to_role(&self, role_id: &str, permission_id: &str) -> Result<(), String> {
        let mut role_perms = self.role_permissions.write().await;
        role_perms
            .entry(role_id.to_string())
            .or_insert_with(Vec::new)
            .push(permission_id.to_string());
        Ok(())
    }

    pub async fn assign_role_to_user(&self, user_id: &str, role_id: &str) -> Result<(), String> {
        let mut user_roles = self.user_roles.write().await;
        user_roles
            .entry(user_id.to_string())
            .or_insert_with(Vec::new)
            .push(role_id.to_string());
        Ok(())
    }

    pub async fn check_permission(
        &self,
        user_id: &str,
        resource_type: PermissionResource,
        action: PermissionAction,
        scope: &PermissionScope,
    ) -> Result<bool, String> {
        let user_roles = self.user_roles.read().await;
        let roles = self.roles.read().await;
        let permissions = self.permissions.read().await;
        let role_perms = self.role_permissions.read().await;
        
        let user_role_ids = user_roles.get(user_id).cloned().unwrap_or_default();
        
        let mut all_permission_ids = Vec::new();
        
        for role_id in &user_role_ids {
            if let Some(perms) = role_perms.get(role_id) {
                all_permission_ids.extend(perms.clone());
            }
            
            if let Some(role) = roles.get(role_id) {
                for inherited in &role.inherits_from {
                    if let Some(inherited_perms) = role_perms.get(inherited) {
                        all_permission_ids.extend(inherited_perms.clone());
                    }
                }
            }
        }
        
        let mut has_permission = false;
        let mut has_deny = false;
        
        for perm_id in all_permission_ids {
            if let Some(perm) = permissions.get(&perm_id) {
                if perm.resource_type == resource_type && perm.actions.contains(&action) {
                    if perm.effect == PermissionEffect::Deny {
                        has_deny = true;
                    } else {
                        has_permission = true;
                    }
                    
                    if let Some(ref filter) = scope.metadata_filter {
                        if !Self::check_metadata_conditions(filter, scope) {
                            continue;
                        }
                    }
                }
            }
        }
        
        Ok(has_permission && !has_deny)
    }

    fn check_metadata_conditions(filter: &MetadataPermissionFilter, scope: &PermissionScope) -> bool {
        if let Some(ref metadata) = scope.metadata_filter {
            for cond in &metadata.conditions {
                let field_value = metadata.conditions.iter()
                    .find(|c| c.field == cond.field)
                    .map(|c| c.value.clone());
                
                if let Some(value) = field_value {
                    let matches = match cond.operator {
                        ConditionOperator::Equals => value == cond.value,
                        ConditionOperator::NotEquals => value != cond.value,
                        ConditionOperator::Contains => {
                            if let (Some(v), Some(c)) = (value.as_str(), cond.value.as_str()) {
                                v.contains(c)
                            } else {
                                false
                            }
                        }
                        _ => false,
                    };
                    
                    if metadata.combine == FilterCombine::And && !matches {
                        return false;
                    }
                    if metadata.combine == FilterCombine::Or && matches {
                        return true;
                    }
                }
            }
        }
        true
    }

    pub async fn create_role(&self, role: Role) -> Result<(), String> {
        let mut roles = self.roles.write().await;
        
        if roles.contains_key(&role.id) {
            return Err(format!("Role {} already exists", role.id));
        }
        
        roles.insert(role.id.clone(), role);
        Ok(())
    }

    pub async fn create_user(&self, user: User) -> Result<(), String> {
        let mut users = self.users.write().await;
        
        if users.contains_key(&user.id) {
            return Err(format!("User {} already exists", user.id));
        }
        
        users.insert(user.id.clone(), user);
        Ok(())
    }

    pub async fn get_user(&self, user_id: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(user_id).cloned()
    }

    pub async fn get_role(&self, role_id: &str) -> Option<Role> {
        let roles = self.roles.read().await;
        roles.get(role_id).cloned()
    }

    pub async fn get_user_permissions(&self, user_id: &str) -> Vec<Permission> {
        let user_roles = self.user_roles.read().await;
        let roles = self.roles.read().await;
        let permissions = self.permissions.read().await;
        let role_perms = self.role_permissions.read().await;
        
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        if let Some(role_ids) = user_roles.get(user_id) {
            for role_id in role_ids {
                if let Some(perm_ids) = role_perms.get(role_id) {
                    for perm_id in perm_ids {
                        if !seen.contains(perm_id) {
                            if let Some(perm) = permissions.get(perm_id) {
                                result.push(perm.clone());
                                seen.insert(perm_id.clone());
                            }
                        }
                    }
                }
            }
        }
        
        result
    }

    pub async fn revoke_permission_from_role(&self, role_id: &str, permission_id: &str) -> Result<(), String> {
        let mut role_perms = self.role_permissions.write().await;
        if let Some(perms) = role_perms.get_mut(role_id) {
            perms.retain(|p| p != permission_id);
        }
        Ok(())
    }

    pub async fn revoke_role_from_user(&self, user_id: &str, role_id: &str) -> Result<(), String> {
        let mut user_roles = self.user_roles.write().await;
        if let Some(roles) = user_roles.get_mut(user_id) {
            roles.retain(|r| r != role_id);
        }
        Ok(())
    }
}

impl Default for FineGrainedPermissionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_permission_engine_new() {
        let engine = FineGrainedPermissionEngine::new();
        
        let stats = engine.get_user_permissions("user1").await;
        assert!(stats.is_empty());
    }

    #[tokio::test]
    async fn test_create_permission() {
        let engine = FineGrainedPermissionEngine::new();
        
        let permission = Permission {
            id: "perm1".to_string(),
            name: "Read Access".to_string(),
            resource_type: PermissionResource::Collection,
            actions: vec![PermissionAction::Read],
            effect: PermissionEffect::Allow,
            conditions: vec![],
            priority: 0,
        };
        
        engine.create_permission(permission).await.unwrap();
        
        let perm = engine.get_permission("perm1").await;
        assert!(perm.is_some());
    }

    #[tokio::test]
    async fn test_create_role() {
        let engine = FineGrainedPermissionEngine::new();
        
        let role = Role {
            id: "role1".to_string(),
            name: "Admin".to_string(),
            description: "Administrator role".to_string(),
            permissions: vec![],
            inherits_from: vec![],
            is_system: true,
        };
        
        engine.create_role(role).await.unwrap();
        
        let retrieved = engine.get_role("role1").await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_create_user() {
        let engine = FineGrainedPermissionEngine::new();
        
        let user = User {
            id: "user1".to_string(),
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            roles: vec![],
            metadata: HashMap::new(),
            is_active: true,
        };
        
        engine.create_user(user).await.unwrap();
        
        let retrieved = engine.get_user("user1").await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_assign_role_to_user() {
        let engine = FineGrainedPermissionEngine::new();
        
        let user = User {
            id: "user1".to_string(),
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            roles: vec![],
            metadata: HashMap::new(),
            is_active: true,
        };
        engine.create_user(user).await.unwrap();
        
        let role = Role {
            id: "role1".to_string(),
            name: "Admin".to_string(),
            description: "Admin role".to_string(),
            permissions: vec![],
            inherits_from: vec![],
            is_system: false,
        };
        engine.create_role(role).await.unwrap();
        
        engine.assign_role_to_user("user1", "role1").await.unwrap();
        
        let perms = engine.get_user_permissions("user1").await;
        assert!(perms.is_empty());
    }

    #[tokio::test]
    async fn test_check_permission() {
        let engine = FineGrainedPermissionEngine::new();
        
        let permission = Permission {
            id: "perm1".to_string(),
            name: "Read Access".to_string(),
            resource_type: PermissionResource::Collection,
            actions: vec![PermissionAction::Read],
            effect: PermissionEffect::Allow,
            conditions: vec![],
            priority: 0,
        };
        engine.create_permission(permission).await.unwrap();
        
        let role = Role {
            id: "role1".to_string(),
            name: "Reader".to_string(),
            description: "Reader role".to_string(),
            permissions: vec!["perm1".to_string()],
            inherits_from: vec![],
            is_system: false,
        };
        engine.create_role(role).await.unwrap();
        
        let user = User {
            id: "user1".to_string(),
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            roles: vec!["role1".to_string()],
            metadata: HashMap::new(),
            is_active: true,
        };
        engine.create_user(user).await.unwrap();
        
        let scope = PermissionScope {
            collection: Some("test".to_string()),
            vector_ids: None,
            fields: None,
            metadata_filter: None,
        };
        
        let has_perm = engine.check_permission(
            "user1",
            PermissionResource::Collection,
            PermissionAction::Read,
            &scope,
        ).await.unwrap();
        
        assert!(has_perm);
    }

    #[tokio::test]
    async fn test_check_permission_denied() {
        let engine = FineGrainedPermissionEngine::new();
        
        let permission = Permission {
            id: "perm1".to_string(),
            name: "Deny Delete".to_string(),
            resource_type: PermissionResource::Collection,
            actions: vec![PermissionAction::Delete],
            effect: PermissionEffect::Deny,
            conditions: vec![],
            priority: 0,
        };
        engine.create_permission(permission).await.unwrap();
        
        let role = Role {
            id: "role1".to_string(),
            name: "User".to_string(),
            description: "User role".to_string(),
            permissions: vec!["perm1".to_string()],
            inherits_from: vec![],
            is_system: false,
        };
        engine.create_role(role).await.unwrap();
        
        let user = User {
            id: "user1".to_string(),
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            roles: vec!["role1".to_string()],
            metadata: HashMap::new(),
            is_active: true,
        };
        engine.create_user(user).await.unwrap();
        
        let scope = PermissionScope {
            collection: Some("test".to_string()),
            vector_ids: None,
            fields: None,
            metadata_filter: None,
        };
        
        let has_perm = engine.check_permission(
            "user1",
            PermissionResource::Collection,
            PermissionAction::Delete,
            &scope,
        ).await.unwrap();
        
        assert!(!has_perm);
    }

    #[tokio::test]
    async fn test_revoke_role() {
        let engine = FineGrainedPermissionEngine::new();
        
        let user = User {
            id: "user1".to_string(),
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            roles: vec!["role1".to_string()],
            metadata: HashMap::new(),
            is_active: true,
        };
        engine.create_user(user).await.unwrap();
        
        engine.revoke_role_from_user("user1", "role1").await.unwrap();
        
        let perms = engine.get_user_permissions("user1").await;
        assert!(perms.is_empty());
    }
}
