//! ACL - Fine-grained Access Control List for CoreTexDB

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ACLPolicy {
    pub id: String,
    pub name: String,
    pub description: String,
    pub subjects: Vec<Subject>,
    pub resources: Vec<Resource>,
    pub actions: Vec<Action>,
    pub effect: Effect,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subject {
    pub subject_type: SubjectType,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubjectType {
    User,
    Role,
    Group,
    ServiceAccount,
    IPAddress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub resource_type: ResourceType,
    pub id: Option<String>,
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    Collection,
    Vector,
    Index,
    Query,
    Admin,
    API,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Create,
    Read,
    Update,
    Delete,
    Search,
    Query,
    Admin,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub attribute: String,
    pub operator: ConditionOperator,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    Contains,
    In,
    GreaterThan,
    LessThan,
    RegexMatch,
}

pub struct ACLEngine {
    policies: Arc<RwLock<HashMap<String, ACLPolicy>>>,
    default_deny: bool,
}

impl ACLEngine {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            default_deny: true,
        }
    }

    pub fn with_default_allow(mut self) -> Self {
        self.default_deny = false;
        self
    }

    pub async fn add_policy(&self, policy: ACLPolicy) {
        let mut policies = self.policies.write().await;
        policies.insert(policy.id.clone(), policy);
    }

    pub async fn remove_policy(&self, policy_id: &str) -> bool {
        let mut policies = self.policies.write().await;
        policies.remove(policy_id).is_some()
    }

    pub async fn check_permission(
        &self,
        subject: &Subject,
        resource: &Resource,
        action: Action,
    ) -> Result<bool, String> {
        let policies = self.policies.read().await;
        
        for policy in policies.values() {
            if self.matches_subject(&policy.subjects, subject)
                && self.matches_resource(&policy.resources, resource)
                && self.matches_action(&policy.actions, action)
                && self.check_conditions(&policy.conditions, subject)
            {
                return Ok(policy.effect == Effect::Allow);
            }
        }
        
        Ok(!self.default_deny)
    }

    fn matches_subject(&self, subjects: &[Subject], subject: &Subject) -> bool {
        subjects.iter().any(|s| {
            s.subject_type == subject.subject_type && (s.id == "*" || s.id == subject.id)
        })
    }

    fn matches_resource(&self, resources: &[Resource], resource: &Resource) -> bool {
        resources.iter().any(|r| {
            r.resource_type == resource.resource_type
                && (r.id.as_ref().map_or(true, |id| id == "*" || id == resource.id.as_ref().unwrap_or(&String::new())))
        })
    }

    fn matches_action(&self, actions: &[Action], action: Action) -> bool {
        actions.iter().any(|a| *a == Action::All || *a == action)
    }

    fn check_conditions(&self, _conditions: &[Condition], _subject: &Subject) -> bool {
        true
    }

    pub async fn list_policies(&self) -> Vec<ACLPolicy> {
        let policies = self.policies.read().await;
        policies.values().cloned().collect()
    }

    pub async fn get_user_permissions(&self, user_id: &str) -> HashMap<String, Vec<Action>> {
        let mut perms = HashMap::new();
        
        let subject = Subject {
            subject_type: SubjectType::User,
            id: user_id.to_string(),
        };
        
        let resources = vec![
            ResourceType::Collection,
            ResourceType::Vector,
            ResourceType::Query,
            ResourceType::Admin,
        ];
        
        for resource_type in resources {
            let resource = Resource {
                resource_type,
                id: None,
                pattern: None,
            };
            
            let actions = vec![
                Action::Create,
                Action::Read,
                Action::Update,
                Action::Delete,
                Action::Search,
            ];
            
            let mut allowed = Vec::new();
            for action in actions {
                if let Ok(true) = self.check_permission(&subject, &resource, action).await {
                    allowed.push(action);
                }
            }
            
            if !allowed.is_empty() {
                perms.insert(format!("{:?}", resource_type), allowed);
            }
        }
        
        perms
    }
}

impl Default for ACLEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ACLValidator;

impl ACLValidator {
    pub fn validate_policy(policy: &ACLPolicy) -> Result<(), String> {
        if policy.name.is_empty() {
            return Err("Policy name cannot be empty".to_string());
        }
        
        if policy.subjects.is_empty() {
            return Err("Policy must have at least one subject".to_string());
        }
        
        if policy.resources.is_empty() {
            return Err("Policy must have at least one resource".to_string());
        }
        
        if policy.actions.is_empty() {
            return Err("Policy must have at least one action".to_string());
        }
        
        Ok(())
    }
}
