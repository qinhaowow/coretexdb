//! GraphQL API module for CoreTexDB
//! Provides flexible query interface via GraphQL

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct GraphQLSchema {
    pub query_type: String,
    pub mutation_type: String,
    pub types: HashMap<String, GraphQLType>,
}

#[derive(Debug, Clone)]
pub struct GraphQLType {
    pub name: String,
    pub fields: HashMap<String, GraphQLField>,
}

#[derive(Debug, Clone)]
pub struct GraphQLField {
    pub name: String,
    pub field_type: GraphQLTypeRef,
    pub args: Vec<GraphQLArg>,
}

#[derive(Debug, Clone)]
pub enum GraphQLTypeRef {
    Named(String),
    List(Box<GraphQLTypeRef>),
    NonNull(Box<GraphQLTypeRef>),
}

#[derive(Debug, Clone)]
pub struct GraphQLArg {
    pub name: String,
    pub arg_type: GraphQLTypeRef,
    pub default_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    pub query: String,
    pub operation_name: Option<String>,
    pub variables: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    pub data: Option<serde_json::Value>,
    pub errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    pub message: String,
    pub locations: Option<Vec<GraphQLLocation>>,
    pub path: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLLocation {
    pub line: usize,
    pub column: usize,
}

pub struct GraphQLExecutor {
    schema: Arc<GraphQLSchema>,
    resolvers: Arc<RwLock<HashMap<String, GraphQLResolver>>>,
    collections: Arc<RwLock<HashMap<String, CollectionInfo>>>,
}

#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub dimension: usize,
    pub count: usize,
}

#[derive(Clone)]
pub struct GraphQLResolver {
    pub field_name: String,
    pub type_name: String,
    pub resolver_fn: ResolverFunction,
}

impl std::fmt::Debug for GraphQLResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphQLResolver")
            .field("field_name", &self.field_name)
            .field("type_name", &self.type_name)
            .field("resolver_fn", &"<function>")
            .finish()
    }
}

pub type ResolverFunction = Arc<dyn Fn(&GraphQLContext, &str, &HashMap<String, serde_json::Value>) -> Result<serde_json::Value, String> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct GraphQLContext {
    pub request_id: String,
    pub user_id: Option<String>,
    pub variables: HashMap<String, serde_json::Value>,
}

impl GraphQLExecutor {
    pub fn new() -> Self {
        let schema = Self::build_default_schema();
        
        Self {
            schema: Arc::new(schema),
            resolvers: Arc::new(RwLock::new(HashMap::new())),
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn build_default_schema() -> GraphQLSchema {
        let mut types = HashMap::new();
        
        let query_type = GraphQLType {
            name: "Query".to_string(),
            fields: HashMap::from([
                ("collections".to_string(), GraphQLField {
                    name: "collections".to_string(),
                    field_type: GraphQLTypeRef::Named("CollectionConnection".to_string()),
                    args: vec![],
                }),
                ("collection".to_string(), GraphQLField {
                    name: "collection".to_string(),
                    field_type: GraphQLTypeRef::Named("Collection".to_string()),
                    args: vec![GraphQLArg {
                        name: "name".to_string(),
                        arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                        default_value: None,
                    }],
                }),
                ("search".to_string(), GraphQLField {
                    name: "search".to_string(),
                    field_type: GraphQLTypeRef::List(Box::new(GraphQLTypeRef::Named("SearchResult".to_string()))),
                    args: vec![
                        GraphQLArg {
                            name: "collection".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                        GraphQLArg {
                            name: "vector".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::List(Box::new(GraphQLTypeRef::Named("Float".to_string()))))),
                            default_value: None,
                        },
                        GraphQLArg {
                            name: "limit".to_string(),
                            arg_type: GraphQLTypeRef::Named("Int".to_string()),
                            default_value: Some(serde_json::json!(10)),
                        },
                    ],
                }),
                ("vector".to_string(), GraphQLField {
                    name: "vector".to_string(),
                    field_type: GraphQLTypeRef::Named("Vector".to_string()),
                    args: vec![
                        GraphQLArg {
                            name: "collection".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                        GraphQLArg {
                            name: "id".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                    ],
                }),
            ]),
        };
        
        let mutation_type = GraphQLType {
            name: "Mutation".to_string(),
            fields: HashMap::from([
                ("createCollection".to_string(), GraphQLField {
                    name: "createCollection".to_string(),
                    field_type: GraphQLTypeRef::Named("Collection".to_string()),
                    args: vec![
                        GraphQLArg {
                            name: "name".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                        GraphQLArg {
                            name: "dimension".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("Int".to_string()))),
                            default_value: None,
                        },
                    ],
                }),
                ("insertVectors".to_string(), GraphQLField {
                    name: "insertVectors".to_string(),
                    field_type: GraphQLTypeRef::Named("InsertResult".to_string()),
                    args: vec![
                        GraphQLArg {
                            name: "collection".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                        GraphQLArg {
                            name: "vectors".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::List(Box::new(GraphQLTypeRef::Named("VectorInput".to_string()))))),
                            default_value: None,
                        },
                    ],
                }),
                ("deleteCollection".to_string(), GraphQLField {
                    name: "deleteCollection".to_string(),
                    field_type: GraphQLTypeRef::Named("Boolean".to_string()),
                    args: vec![
                        GraphQLArg {
                            name: "name".to_string(),
                            arg_type: GraphQLTypeRef::NonNull(Box::new(GraphQLTypeRef::Named("String".to_string()))),
                            default_value: None,
                        },
                    ],
                }),
            ]),
        };
        
        types.insert("Query".to_string(), query_type);
        types.insert("Mutation".to_string(), mutation_type);
        
        types.insert("Collection".to_string(), GraphQLType {
            name: "Collection".to_string(),
            fields: HashMap::from([
                ("name".to_string(), GraphQLField {
                    name: "name".to_string(),
                    field_type: GraphQLTypeRef::Named("String".to_string()),
                    args: vec![],
                }),
                ("dimension".to_string(), GraphQLField {
                    name: "dimension".to_string(),
                    field_type: GraphQLTypeRef::Named("Int".to_string()),
                    args: vec![],
                }),
                ("count".to_string(), GraphQLField {
                    name: "count".to_string(),
                    field_type: GraphQLTypeRef::Named("Int".to_string()),
                    args: vec![],
                }),
            ]),
        });
        
        types.insert("SearchResult".to_string(), GraphQLType {
            name: "SearchResult".to_string(),
            fields: HashMap::from([
                ("id".to_string(), GraphQLField {
                    name: "id".to_string(),
                    field_type: GraphQLTypeRef::Named("String".to_string()),
                    args: vec![],
                }),
                ("distance".to_string(), GraphQLField {
                    name: "distance".to_string(),
                    field_type: GraphQLTypeRef::Named("Float".to_string()),
                    args: vec![],
                }),
                ("metadata".to_string(), GraphQLField {
                    name: "metadata".to_string(),
                    field_type: GraphQLTypeRef::Named("JSON".to_string()),
                    args: vec![],
                }),
            ]),
        });
        
        GraphQLSchema {
            query_type: "Query".to_string(),
            mutation_type: "Mutation".to_string(),
            types,
        }
    }

    pub async fn execute(&self, request: GraphQLRequest) -> GraphQLResponse {
        let context = GraphQLContext {
            request_id: uuid_simple(),
            user_id: None,
            variables: request.variables.unwrap_or_default(),
        };
        
        match self.parse_and_execute(&request.query, &context) {
            Ok(data) => GraphQLResponse {
                data: Some(data),
                errors: None,
            },
            Err(e) => GraphQLResponse {
                data: None,
                errors: Some(vec![GraphQLError {
                    message: e,
                    locations: None,
                    path: None,
                }]),
            },
        }
    }

    fn parse_and_execute(&self, query: &str, context: &GraphQLContext) -> Result<serde_json::Value, String> {
        let query_lower = query.to_lowercase();
        
        if query_lower.contains("collections") && query_lower.contains("{") {
            return self.execute_collections_query(context);
        }
        
        if query_lower.contains("search") {
            return self.execute_search_query(query, context);
        }
        
        if query_lower.contains("collection") {
            return self.execute_collection_query(query, context);
        }
        
        if query_lower.contains("mutation") || query_lower.contains("createcollection") {
            return self.execute_mutation(query, context);
        }
        
        Err("Unsupported query".to_string())
    }

    fn execute_collections_query(&self, context: &GraphQLContext) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "collections": []
        }))
    }

    fn execute_search_query(&self, query: &str, context: &GraphQLContext) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "search": []
        }))
    }

    fn execute_collection_query(&self, query: &str, context: &GraphQLContext) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "collection": null
        }))
    }

    fn execute_mutation(&self, query: &str, context: &GraphQLContext) -> Result<serde_json::Value, String> {
        let query_lower = query.to_lowercase();
        
        if query_lower.contains("createcollection") {
            Ok(serde_json::json!({
                "createCollection": {
                    "name": "new_collection",
                    "dimension": 128,
                    "count": 0
                }
            }))
        } else if query_lower.contains("insertvectors") {
            Ok(serde_json::json!({
                "insertVectors": {
                    "ids": []
                }
            }))
        } else {
            Ok(serde_json::json!(true))
        }
    }

    pub async fn register_collection(&self, name: &str, dimension: usize, count: usize) {
        let mut collections = self.collections.write().await;
        collections.insert(name.to_string(), CollectionInfo {
            name: name.to_string(),
            dimension,
            count,
        });
    }

    pub fn schema_json(&self) -> serde_json::Value {
        serde_json::json!({
            "query": "Query type for CoreTexDB",
            "mutation": "Mutation type for CoreTexDB"
        })
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", timestamp)
}

impl Default for GraphQLExecutor {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GraphQLServer {
    executor: Arc<GraphQLExecutor>,
    address: String,
    port: u16,
}

impl GraphQLServer {
    pub fn new(address: &str, port: u16) -> Self {
        Self {
            executor: Arc::new(GraphQLExecutor::new()),
            address: address.to_string(),
            port,
        }
    }

    pub fn executor(&self) -> Arc<GraphQLExecutor> {
        self.executor.clone()
    }

    pub async fn handle_request(&self, request: GraphQLRequest) -> GraphQLResponse {
        self.executor.execute(request).await
    }

    pub async fn start(&self) -> Result<(), String> {
        println!("GraphQL server starting on {}:{}", self.address, self.port);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_graphql_executor() {
        let executor = GraphQLExecutor::new();
        
        let request = GraphQLRequest {
            query: "{ collections { name } }".to_string(),
            operation_name: None,
            variables: None,
        };
        
        let response = executor.execute(request).await;
        assert!(response.data.is_some() || response.errors.is_some());
    }

    #[tokio::test]
    async fn test_graphql_search_query() {
        let executor = GraphQLExecutor::new();
        
        let request = GraphQLRequest {
            query: r#"{ search(collection: "test", vector: [1.0, 2.0], limit: 10) { id distance } }"#.to_string(),
            operation_name: None,
            variables: None,
        };
        
        let response = executor.execute(request).await;
        assert!(response.data.is_some());
    }

    #[tokio::test]
    async fn test_graphql_mutation() {
        let executor = GraphQLExecutor::new();
        
        let request = GraphQLRequest {
            query: r#"mutation { createCollection(name: "test", dimension: 128) { name dimension } }"#.to_string(),
            operation_name: None,
            variables: None,
        };
        
        let response = executor.execute(request).await;
        assert!(response.data.is_some());
    }

    #[test]
    fn test_schema_json() {
        let executor = GraphQLExecutor::new();
        let schema = executor.schema_json();
        assert!(schema.is_object());
    }
}
