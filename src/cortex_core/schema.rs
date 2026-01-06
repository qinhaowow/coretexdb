//! Collection schema management for CortexDB
//! 
//! This module handles the schema definition and validation for collections,
//! including field definitions, constraints, and schema evolution.

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Field type enum for schema definition
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum FieldType {
    /// String field
    String,
    /// Integer field
    Integer,
    /// Float field
    Float,
    /// Boolean field
    Boolean,
    /// Array field with element type
    Array(Box<FieldType>),
    /// Object field with nested fields
    Object(HashMap<String, FieldDefinition>),
    /// Vector field with dimension
    Vector(usize),
    /// Embedding field (alias for Vector)
    Embedding(usize),
}

/// Field definition for schema
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct FieldDefinition {
    /// Field type
    pub r#type: FieldType,
    /// Whether the field is required
    pub required: bool,
    /// Default value (optional)
    pub default: Option<serde_json::Value>,
    /// Description of the field
    pub description: Option<String>,
    /// Whether the field should be indexed
    pub indexed: bool,
}

/// Collection schema definition
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CollectionSchema {
    /// Schema version
    pub version: u32,
    /// Field definitions
    pub fields: HashMap<String, FieldDefinition>,
    /// Primary key field
    pub primary_key: Option<String>,
    /// Vector field name for similarity search
    pub vector_field: Option<String>,
    /// Distance metric for vector similarity
    pub distance_metric: Option<String>,
    /// Description of the collection
    pub description: Option<String>,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub updated_at: u64,
}

/// Schema manager for handling schema operations
#[derive(Debug)]
pub struct SchemaManager {
    schemas: HashMap<String, CollectionSchema>,
}

impl SchemaManager {
    /// Create a new schema manager
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Create a new collection schema
    pub fn create_schema(
        &mut self,
        collection_name: &str,
        fields: HashMap<String, FieldDefinition>,
        vector_field: Option<String>,
        distance_metric: Option<String>,
    ) -> Result<CollectionSchema, SchemaError> {
        // Check if collection already exists
        if self.schemas.contains_key(collection_name) {
            return Err(SchemaError::CollectionAlreadyExists(collection_name.to_string()));
        }

        // Validate vector field if specified
        if let Some(vector_field_name) = &vector_field {
            if !fields.contains_key(vector_field_name) {
                return Err(SchemaError::VectorFieldNotFound(vector_field_name.to_string()));
            }
            
            let field_def = &fields[vector_field_name];
            match &field_def.r#type {
                FieldType::Vector(_) | FieldType::Embedding(_) => {},
                _ => return Err(SchemaError::InvalidVectorFieldType(vector_field_name.to_string())),
            }
        }

        // Create schema
        let now = chrono::Utc::now().timestamp() as u64;
        let schema = CollectionSchema {
            version: 1,
            fields,
            primary_key: None,
            vector_field,
            distance_metric,
            description: None,
            created_at: now,
            updated_at: now,
        };

        // Store schema
        self.schemas.insert(collection_name.to_string(), schema.clone());

        Ok(schema)
    }

    /// Get collection schema
    pub fn get_schema(&self, collection_name: &str) -> Result<&CollectionSchema, SchemaError> {
        self.schemas.get(collection_name)
            .ok_or(SchemaError::CollectionNotFound(collection_name.to_string()))
    }

    /// Update collection schema (schema evolution)
    pub fn update_schema(
        &mut self,
        collection_name: &str,
        new_fields: HashMap<String, FieldDefinition>,
    ) -> Result<CollectionSchema, SchemaError> {
        let schema = self.schemas.get_mut(collection_name)
            .ok_or(SchemaError::CollectionNotFound(collection_name.to_string()))?;

        // Update fields (add new fields, do not modify existing ones)
        for (field_name, field_def) in new_fields {
            if schema.fields.contains_key(&field_name) {
                return Err(SchemaError::FieldAlreadyExists(field_name));
            }
            schema.fields.insert(field_name, field_def);
        }

        // Update version and timestamp
        schema.version += 1;
        schema.updated_at = chrono::Utc::now().timestamp() as u64;

        Ok(schema.clone())
    }

    /// Delete collection schema
    pub fn delete_schema(&mut self, collection_name: &str) -> Result<bool, SchemaError> {
        if !self.schemas.contains_key(collection_name) {
            return Err(SchemaError::CollectionNotFound(collection_name.to_string()));
        }

        self.schemas.remove(collection_name);
        Ok(true)
    }

    /// List all collection schemas
    pub fn list_schemas(&self) -> Vec<(String, CollectionSchema)> {
        self.schemas.iter()
            .map(|(name, schema)| (name.clone(), schema.clone()))
            .collect()
    }

    /// Validate document against schema
    pub fn validate_document(
        &self,
        collection_name: &str,
        document: &serde_json::Value,
    ) -> Result<(), SchemaError> {
        let schema = self.get_schema(collection_name)?;

        // Validate required fields
        for (field_name, field_def) in &schema.fields {
            if field_def.required {
                if !document.contains_key(field_name) {
                    return Err(SchemaError::MissingRequiredField(field_name.to_string()));
                }
            }
        }

        // Validate field types
        for (field_name, field_def) in &schema.fields {
            if let Some(value) = document.get(field_name) {
                if let Err(err) = self.validate_field_type(value, &field_def.r#type) {
                    return Err(SchemaError::InvalidFieldType(format!("{}: {}", field_name, err)));
                }
            }
        }

        Ok(())
    }

    /// Validate field value against field type
    fn validate_field_type(&self, value: &serde_json::Value, field_type: &FieldType) -> Result<(), String> {
        match (value, field_type) {
            (serde_json::Value::String(_), FieldType::String) => Ok(()),
            (serde_json::Value::Number(n), FieldType::Integer) if n.is_i64() => Ok(()),
            (serde_json::Value::Number(n), FieldType::Float) if n.is_f64() => Ok(()),
            (serde_json::Value::Bool(_), FieldType::Boolean) => Ok(()),
            (serde_json::Value::Array(arr), FieldType::Array(element_type)) => {
                for item in arr {
                    self.validate_field_type(item, element_type)?;
                }
                Ok(())
            }
            (serde_json::Value::Object(obj), FieldType::Object(field_defs)) => {
                for (field_name, field_def) in field_defs {
                    if let Some(value) = obj.get(field_name) {
                        self.validate_field_type(value, &field_def.r#type)?;
                    } else if field_def.required {
                        return Err(format!("Missing required nested field: {}", field_name));
                    }
                }
                Ok(())
            }
            (serde_json::Value::Array(arr), FieldType::Vector(dim)) if arr.len() == *dim => {
                for item in arr {
                    if !item.is_number() {
                        return Err("Vector elements must be numbers".to_string());
                    }
                }
                Ok(())
            }
            (serde_json::Value::Array(arr), FieldType::Embedding(dim)) if arr.len() == *dim => {
                for item in arr {
                    if !item.is_number() {
                        return Err("Embedding elements must be numbers".to_string());
                    }
                }
                Ok(())
            }
            _ => Err(format!("Value does not match expected type: {:?}", field_type)),
        }
    }
}

/// Schema error types
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SchemaError {
    /// Collection already exists
    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),
    
    /// Collection not found
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),
    
    /// Vector field not found in schema
    #[error("Vector field not found: {0}")]
    VectorFieldNotFound(String),
    
    /// Invalid vector field type
    #[error("Invalid vector field type: {0}")]
    InvalidVectorFieldType(String),
    
    /// Field already exists
    #[error("Field already exists: {0}")]
    FieldAlreadyExists(String),
    
    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingRequiredField(String),
    
    /// Invalid field type
    #[error("Invalid field type: {0}")]
    InvalidFieldType(String),
}

/// Default schema for common use cases
pub fn default_vector_schema(dimension: usize) -> HashMap<String, FieldDefinition> {
    let mut fields = HashMap::new();
    
    // Default id field
    fields.insert(
        "id".to_string(),
        FieldDefinition {
            r#type: FieldType::String,
            required: true,
            default: None,
            description: Some("Document ID".to_string()),
            indexed: true,
        },
    );
    
    // Default text field
    fields.insert(
        "text".to_string(),
        FieldDefinition {
            r#type: FieldType::String,
            required: false,
            default: None,
            description: Some("Document text".to_string()),
            indexed: false,
        },
    );
    
    // Default vector field
    fields.insert(
        "vector".to_string(),
        FieldDefinition {
            r#type: FieldType::Vector(dimension),
            required: true,
            default: None,
            description: Some("Embedding vector".to_string()),
            indexed: true,
        },
    );
    
    fields
}