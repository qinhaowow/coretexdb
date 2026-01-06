//! Command-line argument parser for CortexDB CLI.
//!
//! This module provides command-line argument parsing functionality for the CortexDB CLI, including:
//! - Command definition and parsing
//! - Subcommand support
//! - Argument validation
//! - Help text generation
//! - Version information

use clap::{Arg, ArgAction, Command};
use std::env;

/// CLI command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    /// Collection commands
    Collection {
        /// Subcommand
        subcommand: CollectionSubcommand,
    },
    /// Document commands
    Document {
        /// Subcommand
        subcommand: DocumentSubcommand,
    },
    /// Search commands
    Search {
        /// Subcommand
        subcommand: SearchSubcommand,
    },
    /// Index commands
    Index {
        /// Subcommand
        subcommand: IndexSubcommand,
    },
    /// System commands
    System {
        /// Subcommand
        subcommand: SystemSubcommand,
    },
    /// Help command
    Help,
    /// Version command
    Version,
}

/// Collection subcommands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionSubcommand {
    /// List collections
    List,
    /// Create collection
    Create {
        /// Collection name
        name: String,
        /// Collection schema (optional)
        schema: Option<String>,
    },
    /// Get collection
    Get {
        /// Collection name
        name: String,
    },
    /// Delete collection
    Delete {
        /// Collection name
        name: String,
    },
    /// Update collection
    Update {
        /// Collection name
        name: String,
        /// Update data
        data: String,
    },
}

/// Document subcommands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentSubcommand {
    /// List documents
    List {
        /// Collection name
        collection: String,
        /// Limit
        limit: Option<usize>,
        /// Offset
        offset: Option<usize>,
    },
    /// Insert documents
    Insert {
        /// Collection name
        collection: String,
        /// Documents file
        file: String,
        /// Overwrite existing
        overwrite: bool,
    },
    /// Get document
    Get {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
    },
    /// Delete document
    Delete {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
    },
    /// Update document
    Update {
        /// Collection name
        collection: String,
        /// Document ID
        id: String,
        /// Document data
        data: String,
    },
}

/// Search subcommands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchSubcommand {
    /// Vector search
    Vector {
        /// Collection name
        collection: String,
        /// Query vector
        query: String,
        /// Limit
        limit: Option<usize>,
        /// Threshold
        threshold: Option<f32>,
    },
    /// Scalar query
    Scalar {
        /// Collection name
        collection: String,
        /// Filter
        filter: String,
        /// Limit
        limit: Option<usize>,
        /// Offset
        offset: Option<usize>,
    },
    /// Hybrid search
    Hybrid {
        /// Collection name
        collection: String,
        /// Query vector
        query: String,
        /// Filter
        filter: String,
        /// Limit
        limit: Option<usize>,
        /// Threshold
        threshold: Option<f32>,
    },
}

/// Index subcommands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexSubcommand {
    /// List indexes
    List {
        /// Collection name
        collection: String,
    },
    /// Create index
    Create {
        /// Collection name
        collection: String,
        /// Index name
        name: String,
        /// Index type
        type: String,
        /// Index config
        config: String,
    },
    /// Get index
    Get {
        /// Collection name
        collection: String,
        /// Index name
        name: String,
    },
    /// Delete index
    Delete {
        /// Collection name
        collection: String,
        /// Index name
        name: String,
    },
}

/// System subcommands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemSubcommand {
    /// Health check
    Health,
    /// Status check
    Status,
    /// Metrics
    Metrics,
    /// Config
    Config {
        /// Config action
        action: String,
        /// Config key
        key: Option<String>,
        /// Config value
        value: Option<String>,
    },
    /// Shutdown
    Shutdown,
}

/// Parse CLI arguments
pub fn parse_args() -> CliCommand {
    let matches = build_cli().get_matches();

    match matches.subcommand() {
        Some(("collection", sub_matches)) => {
            let subcommand = match sub_matches.subcommand() {
                Some(("list", _)) => CollectionSubcommand::List,
                Some(("create", create_matches)) => CollectionSubcommand::Create {
                    name: create_matches.get_one::<String>("name").unwrap().clone(),
                    schema: create_matches.get_one::<String>("schema").cloned(),
                },
                Some(("get", get_matches)) => CollectionSubcommand::Get {
                    name: get_matches.get_one::<String>("name").unwrap().clone(),
                },
                Some(("delete", delete_matches)) => CollectionSubcommand::Delete {
                    name: delete_matches.get_one::<String>("name").unwrap().clone(),
                },
                Some(("update", update_matches)) => CollectionSubcommand::Update {
                    name: update_matches.get_one::<String>("name").unwrap().clone(),
                    data: update_matches.get_one::<String>("data").unwrap().clone(),
                },
                _ => panic!("Unknown collection subcommand"),
            };
            CliCommand::Collection { subcommand }
        }
        Some(("document", sub_matches)) => {
            let subcommand = match sub_matches.subcommand() {
                Some(("list", list_matches)) => DocumentSubcommand::List {
                    collection: list_matches.get_one::<String>("collection").unwrap().clone(),
                    limit: list_matches.get_one::<usize>("limit").cloned(),
                    offset: list_matches.get_one::<usize>("offset").cloned(),
                },
                Some(("insert", insert_matches)) => DocumentSubcommand::Insert {
                    collection: insert_matches.get_one::<String>("collection").unwrap().clone(),
                    file: insert_matches.get_one::<String>("file").unwrap().clone(),
                    overwrite: insert_matches.get_flag("overwrite"),
                },
                Some(("get", get_matches)) => DocumentSubcommand::Get {
                    collection: get_matches.get_one::<String>("collection").unwrap().clone(),
                    id: get_matches.get_one::<String>("id").unwrap().clone(),
                },
                Some(("delete", delete_matches)) => DocumentSubcommand::Delete {
                    collection: delete_matches.get_one::<String>("collection").unwrap().clone(),
                    id: delete_matches.get_one::<String>("id").unwrap().clone(),
                },
                Some(("update", update_matches)) => DocumentSubcommand::Update {
                    collection: update_matches.get_one::<String>("collection").unwrap().clone(),
                    id: update_matches.get_one::<String>("id").unwrap().clone(),
                    data: update_matches.get_one::<String>("data").unwrap().clone(),
                },
                _ => panic!("Unknown document subcommand"),
            };
            CliCommand::Document { subcommand }
        }
        Some(("search", sub_matches)) => {
            let subcommand = match sub_matches.subcommand() {
                Some(("vector", vector_matches)) => SearchSubcommand::Vector {
                    collection: vector_matches.get_one::<String>("collection").unwrap().clone(),
                    query: vector_matches.get_one::<String>("query").unwrap().clone(),
                    limit: vector_matches.get_one::<usize>("limit").cloned(),
                    threshold: vector_matches.get_one::<f32>("threshold").cloned(),
                },
                Some(("scalar", scalar_matches)) => SearchSubcommand::Scalar {
                    collection: scalar_matches.get_one::<String>("collection").unwrap().clone(),
                    filter: scalar_matches.get_one::<String>("filter").unwrap().clone(),
                    limit: scalar_matches.get_one::<usize>("limit").cloned(),
                    offset: scalar_matches.get_one::<usize>("offset").cloned(),
                },
                Some(("hybrid", hybrid_matches)) => SearchSubcommand::Hybrid {
                    collection: hybrid_matches.get_one::<String>("collection").unwrap().clone(),
                    query: hybrid_matches.get_one::<String>("query").unwrap().clone(),
                    filter: hybrid_matches.get_one::<String>("filter").unwrap().clone(),
                    limit: hybrid_matches.get_one::<usize>("limit").cloned(),
                    threshold: hybrid_matches.get_one::<f32>("threshold").cloned(),
                },
                _ => panic!("Unknown search subcommand"),
            };
            CliCommand::Search { subcommand }
        }
        Some(("index", sub_matches)) => {
            let subcommand = match sub_matches.subcommand() {
                Some(("list", list_matches)) => IndexSubcommand::List {
                    collection: list_matches.get_one::<String>("collection").unwrap().clone(),
                },
                Some(("create", create_matches)) => IndexSubcommand::Create {
                    collection: create_matches.get_one::<String>("collection").unwrap().clone(),
                    name: create_matches.get_one::<String>("name").unwrap().clone(),
                    type: create_matches.get_one::<String>("type").unwrap().clone(),
                    config: create_matches.get_one::<String>("config").unwrap().clone(),
                },
                Some(("get", get_matches)) => IndexSubcommand::Get {
                    collection: get_matches.get_one::<String>("collection").unwrap().clone(),
                    name: get_matches.get_one::<String>("name").unwrap().clone(),
                },
                Some(("delete", delete_matches)) => IndexSubcommand::Delete {
                    collection: delete_matches.get_one::<String>("collection").unwrap().clone(),
                    name: delete_matches.get_one::<String>("name").unwrap().clone(),
                },
                _ => panic!("Unknown index subcommand"),
            };
            CliCommand::Index { subcommand }
        }
        Some(("system", sub_matches)) => {
            let subcommand = match sub_matches.subcommand() {
                Some(("health", _)) => SystemSubcommand::Health,
                Some(("status", _)) => SystemSubcommand::Status,
                Some(("metrics", _)) => SystemSubcommand::Metrics,
                Some(("config", config_matches)) => SystemSubcommand::Config {
                    action: config_matches.get_one::<String>("action").unwrap().clone(),
                    key: config_matches.get_one::<String>("key").cloned(),
                    value: config_matches.get_one::<String>("value").cloned(),
                },
                Some(("shutdown", _)) => SystemSubcommand::Shutdown,
                _ => panic!("Unknown system subcommand"),
            };
            CliCommand::System { subcommand }
        }
        Some(("help", _)) => CliCommand::Help,
        Some(("version", _)) => CliCommand::Version,
        _ => CliCommand::Help,
    }
}

/// Build the CLI command structure
fn build_cli() -> Command {
    Command::new("cortexdb")
        .about("CortexDB CLI - A multimodal vector database for AI applications")
        .version("0.1.0")
        .author("CortexDB Team")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommands(vec![
            // Collection commands
            Command::new("collection")
                .about("Manage collections")
                .subcommands(vec![
                    Command::new("list")
                        .about("List all collections"),
                    Command::new("create")
                        .about("Create a new collection")
                        .arg(Arg::new("name")
                            .help("Collection name")
                            .required(true)),
                    Command::new("get")
                        .about("Get collection details")
                        .arg(Arg::new("name")
                            .help("Collection name")
                            .required(true)),
                    Command::new("delete")
                        .about("Delete a collection")
                        .arg(Arg::new("name")
                            .help("Collection name")
                            .required(true)),
                    Command::new("update")
                        .about("Update a collection")
                        .arg(Arg::new("name")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("data")
                            .help("Update data (JSON)")
                            .required(true)),
                ]),
            // Document commands
            Command::new("document")
                .about("Manage documents")
                .subcommands(vec![
                    Command::new("list")
                        .about("List documents in a collection")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("limit")
                            .help("Limit number of results")
                            .long("limit")
                            .value_parser(clap::value_parser!(usize))),
                    Command::new("insert")
                        .about("Insert documents into a collection")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("file")
                            .help("Path to JSON file with documents")
                            .required(true))
                        .arg(Arg::new("overwrite")
                            .help("Overwrite existing documents")
                            .long("overwrite")
                            .action(ArgAction::SetTrue)),
                    Command::new("get")
                        .about("Get a document")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("id")
                            .help("Document ID")
                            .required(true)),
                    Command::new("delete")
                        .about("Delete a document")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("id")
                            .help("Document ID")
                            .required(true)),
                    Command::new("update")
                        .about("Update a document")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("id")
                            .help("Document ID")
                            .required(true))
                        .arg(Arg::new("data")
                            .help("Document data (JSON)")
                            .required(true)),
                ]),
            // Search commands
            Command::new("search")
                .about("Search and query data")
                .subcommands(vec![
                    Command::new("vector")
                        .about("Perform vector similarity search")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("query")
                            .help("Query vector (JSON array)")
                            .required(true))
                        .arg(Arg::new("limit")
                            .help("Limit number of results")
                            .long("limit")
                            .value_parser(clap::value_parser!(usize))),
                    Command::new("scalar")
                        .about("Perform scalar filter query")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("filter")
                            .help("Filter conditions (JSON)")
                            .required(true))
                        .arg(Arg::new("limit")
                            .help("Limit number of results")
                            .long("limit")
                            .value_parser(clap::value_parser!(usize))),
                    Command::new("hybrid")
                        .about("Perform hybrid search (vector + scalar)")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("query")
                            .help("Query vector (JSON array)")
                            .required(true))
                        .arg(Arg::new("filter")
                            .help("Filter conditions (JSON)")
                            .required(true))
                        .arg(Arg::new("limit")
                            .help("Limit number of results")
                            .long("limit")
                            .value_parser(clap::value_parser!(usize))),
                ]),
            // Index commands
            Command::new("index")
                .about("Manage indexes")
                .subcommands(vec![
                    Command::new("list")
                        .about("List indexes for a collection")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true)),
                    Command::new("create")
                        .about("Create an index")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("name")
                            .help("Index name")
                            .required(true))
                        .arg(Arg::new("type")
                            .help("Index type (vector/scalar)")
                            .required(true))
                        .arg(Arg::new("config")
                            .help("Index configuration (JSON)")
                            .required(true)),
                    Command::new("get")
                        .about("Get index details")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("name")
                            .help("Index name")
                            .required(true)),
                    Command::new("delete")
                        .about("Delete an index")
                        .arg(Arg::new("collection")
                            .help("Collection name")
                            .required(true))
                        .arg(Arg::new("name")
                            .help("Index name")
                            .required(true)),
                ]),
            // System commands
            Command::new("system")
                .about("System management commands")
                .subcommands(vec![
                    Command::new("health")
                        .about("Check system health"),
                    Command::new("status")
                        .about("Check system status"),
                    Command::new("metrics")
                        .about("Get system metrics"),
                    Command::new("config")
                        .about("Manage system configuration")
                        .arg(Arg::new("action")
                            .help("Config action (get/set/list)")
                            .required(true))
                        .arg(Arg::new("key")
                            .help("Config key")
                            .long("key")),
                    Command::new("shutdown")
                        .about("Shutdown the system"),
                ]),
            // Help command
            Command::new("help")
                .about("Print help information"),
            // Version command
            Command::new("version")
                .about("Print version information"),
        ])
}

/// Print help message
pub fn print_help() {
    build_cli().print_help().unwrap();
    println!();
}

/// Print version information
pub fn print_version() {
    println!("cortexdb {}", env!("CARGO_PKG_VERSION"));
    println!("A multimodal vector database for AI applications");
    println!("Copyright (c) 2024 CortexDB Team");
}
