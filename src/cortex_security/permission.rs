//! Permission management functionality for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Permission {
    pub id: String,
    pub name: String,
    pub description: String,
    pub resource: String,
    pub action: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Role {
    pub id: String,
    pub name: String,
    pub description: String,
    pub permissions: Vec<String>, // Permission IDs
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RolePermission {
    pub role_id: String,
    pub permission_id: String,
}

#[derive(Debug)]
pub struct PermissionManager {
    permissions: Arc<RwLock<Vec<Permission>>>,
    roles: Arc<RwLock<Vec<Role>>>,
    role_permissions: Arc<RwLock<Vec<RolePermission>>>,
}

impl PermissionManager {
    pub fn new() -> Self {
        let mut permissions = Vec::new();
        let mut roles = Vec::new();
        let mut role_permissions = Vec::new();

        // Add default permissions
        let read_permission = Permission {
            id: uuid::Uuid::new_v4().to_string(),
            name: "read".to_string(),
            description: "Read access to resources".to_string(),
            resource: "*".to_string(),
            action: "read".to_string(),
        };
        permissions.push(read_permission.clone());

        let write_permission = Permission {
            id: uuid::Uuid::new_v4().to_string(),
            name: "write".to_string(),
            description: "Write access to resources".to_string(),
            resource: "*".to_string(),
            action: "write".to_string(),
        };
        permissions.push(write_permission.clone());

        let admin_permission = Permission {
            id: uuid::Uuid::new_v4().to_string(),
            name: "admin".to_string(),
            description: "Administrative access".to_string(),
            resource: "*".to_string(),
            action: "*".to_string(),
        };
        permissions.push(admin_permission.clone());

        // Add default roles
        let user_role = Role {
            id: uuid::Uuid::new_v4().to_string(),
            name: "user".to_string(),
            description: "Regular user role".to_string(),
            permissions: vec![read_permission.id.clone()],
        };
        roles.push(user_role.clone());

        let writer_role = Role {
            id: uuid::Uuid::new_v4().to_string(),
            name: "writer".to_string(),
            description: "Writer role with read/write access".to_string(),
            permissions: vec![read_permission.id.clone(), write_permission.id.clone()],
        };
        roles.push(writer_role.clone());

        let admin_role = Role {
            id: uuid::Uuid::new_v4().to_string(),
            name: "admin".to_string(),
            description: "Administrator role with full access".to_string(),
            permissions: vec![read_permission.id.clone(), write_permission.id.clone(), admin_permission.id.clone()],
        };
        roles.push(admin_role.clone());

        // Add role permissions
        role_permissions.push(RolePermission {
            role_id: user_role.id.clone(),
            permission_id: read_permission.id.clone(),
        });

        role_permissions.push(RolePermission {
            role_id: writer_role.id.clone(),
            permission_id: read_permission.id.clone(),
        });

        role_permissions.push(RolePermission {
            role_id: writer_role.id.clone(),
            permission_id: write_permission.id.clone(),
        });

        role_permissions.push(RolePermission {
            role_id: admin_role.id.clone(),
            permission_id: read_permission.id.clone(),
        });

        role_permissions.push(RolePermission {
            role_id: admin_role.id.clone(),
            permission_id: write_permission.id.clone(),
        });

        role_permissions.push(RolePermission {
            role_id: admin_role.id.clone(),
            permission_id: admin_permission.id.clone(),
        });

        Self {
            permissions: Arc::new(RwLock::new(permissions)),
            roles: Arc::new(RwLock::new(roles)),
            role_permissions: Arc::new(RwLock::new(role_permissions)),
        }
    }

    pub async fn add_permission(&self, permission: Permission) -> Result<Permission, Box<dyn std::error::Error>> {
        let mut permissions = self.permissions.write().await;
        permissions.push(permission.clone());
        Ok(permission)
    }

    pub async fn add_role(&self, role: Role) -> Result<Role, Box<dyn std::error::Error>> {
        let mut roles = self.roles.write().await;
        roles.push(role.clone());
        Ok(role)
    }

    pub async fn assign_permission_to_role(&self, role_id: &str, permission_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Check if role exists
        let roles = self.roles.read().await;
        if !roles.iter().any(|r| r.id == role_id) {
            return Err("Role not found".into());
        }
        drop(roles);

        // Check if permission exists
        let permissions = self.permissions.read().await;
        if !permissions.iter().any(|p| p.id == permission_id) {
            return Err("Permission not found".into());
        }
        drop(permissions);

        // Check if already assigned
        let role_permissions = self.role_permissions.read().await;
        if role_permissions.iter().any(|rp| rp.role_id == role_id && rp.permission_id == permission_id) {
            return Err("Permission already assigned to role".into());
        }
        drop(role_permissions);

        // Assign permission to role
        let mut role_permissions = self.role_permissions.write().await;
        role_permissions.push(RolePermission {
            role_id: role_id.to_string(),
            permission_id: permission_id.to_string(),
        });

        // Update role's permissions
        let mut roles = self.roles.write().await;
        if let Some(role) = roles.iter_mut().find(|r| r.id == role_id) {
            role.permissions.push(permission_id.to_string());
        }

        Ok(true)
    }

    pub async fn check_permission(&self, role_name: &str, resource: &str, action: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Find role by name
        let roles = self.roles.read().await;
        let role = roles.iter().find(|r| r.name == role_name)
            .ok_or("Role not found")?;
        drop(roles);

        // Get role's permissions
        let role_id = role.id.clone();
        let role_permissions = self.role_permissions.read().await;
        let permission_ids: Vec<String> = role_permissions
            .iter()
            .filter(|rp| rp.role_id == role_id)
            .map(|rp| rp.permission_id.clone())
            .collect();
        drop(role_permissions);

        // Check if any permission allows access
        let permissions = self.permissions.read().await;
        for permission_id in permission_ids {
            if let Some(permission) = permissions.iter().find(|p| p.id == permission_id) {
                // Check resource and action
                if (permission.resource == "*" || permission.resource == resource) &&
                   (permission.action == "*" || permission.action == action) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub async fn get_role_permissions(&self, role_id: &str) -> Result<Vec<Permission>, Box<dyn std::error::Error>> {
        let role_permissions = self.role_permissions.read().await;
        let permission_ids: Vec<String> = role_permissions
            .iter()
            .filter(|rp| rp.role_id == role_id)
            .map(|rp| rp.permission_id.clone())
            .collect();
        drop(role_permissions);

        let permissions = self.permissions.read().await;
        let role_perms: Vec<Permission> = permissions
            .iter()
            .filter(|p| permission_ids.contains(&p.id))
            .cloned()
            .collect();

        Ok(role_perms)
    }

    pub async fn list_permissions(&self) -> Result<Vec<Permission>, Box<dyn std::error::Error>> {
        let permissions = self.permissions.read().await;
        Ok(permissions.clone())
    }

    pub async fn list_roles(&self) -> Result<Vec<Role>, Box<dyn std::error::Error>> {
        let roles = self.roles.read().await;
        Ok(roles.clone())
    }

    pub async fn remove_permission_from_role(&self, role_id: &str, permission_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Remove from role_permissions
        let mut role_permissions = self.role_permissions.write().await;
        let initial_count = role_permissions.len();
        role_permissions.retain(|rp| !(rp.role_id == role_id && rp.permission_id == permission_id));
        let removed = role_permissions.len() < initial_count;
        drop(role_permissions);

        // Update role's permissions
        if removed {
            let mut roles = self.roles.write().await;
            if let Some(role) = roles.iter_mut().find(|r| r.id == role_id) {
                role.permissions.retain(|p| p != permission_id);
            }
        }

        Ok(removed)
    }
}

impl Clone for PermissionManager {
    fn clone(&self) -> Self {
        Self {
            permissions: self.permissions.clone(),
            roles: self.roles.clone(),
            role_permissions: self.role_permissions.clone(),
        }
    }
}
