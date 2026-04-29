//! CLI for CoreTexDB

use clap::{Command, Arg, ArgAction};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::net::SocketAddr;

use crate::{CoreTexDB, DbConfig, ApiConfig, start_server};

/// Run the CLI
pub fn run_cli() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        run_cli_async().await
    })
}

async fn run_cli_async() -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = Arc::new(RwLock::new(CoreTexDB::new()));
    db.read().await.init().await.map_err(|e| format!("DB init failed: {}", e))?;

    let mut cmd = Command::new("coretex")
        .version(env!("CARGO_PKG_VERSION"))
        .about("CoreTexDB command-line interface")
        .subcommand_required(true)
        .arg_required_else_help(true);

    cmd = cmd.subcommand(
        Command::new("server")
            .about("Start the CoreTexDB server")
            .arg(
                Arg::new("address")
                    .short('a')
                    .long("address")
                    .help("Address to bind the server to")
                    .default_value("0.0.0.0"),
            )
            .arg(
                Arg::new("port")
                    .short('p')
                    .long("port")
                    .help("Port to bind the server to")
                    .default_value("5000"),
            )
            .arg(
                Arg::new("data-dir")
                    .short('d')
                    .long("data-dir")
                    .help("Directory to store data")
                    .default_value("./data"),
            ),
    );

    cmd = cmd.subcommand(
        Command::new("collection")
            .about("Manage collections")
            .subcommand_required(true)
            .subcommand(
                Command::new("create")
                    .about("Create a new collection")
                    .arg(Arg::new("name").help("Name of the collection").required(true))
                    .arg(
                        Arg::new("dimension")
                            .short('d')
                            .long("dimension")
                            .help("Vector dimension")
                            .default_value("384"),
                    )
                    .arg(
                        Arg::new("metric")
                            .short('m')
                            .long("metric")
                            .help("Distance metric (cosine, euclidean, dotproduct)")
                            .default_value("cosine"),
                    ),
            )
            .subcommand(
                Command::new("list")
                    .about("List all collections"),
            )
            .subcommand(
                Command::new("info")
                    .about("Get collection info")
                    .arg(Arg::new("name").help("Name of the collection").required(true)),
            )
            .subcommand(
                Command::new("delete")
                    .about("Delete a collection")
                    .arg(Arg::new("name").help("Name of the collection").required(true))
                    .arg(
                        Arg::new("force")
                            .short('f')
                            .long("force")
                            .help("Force delete without confirmation"),
                    ),
            ),
    );

    cmd = cmd.subcommand(
        Command::new("vector")
            .about("Manage vectors")
            .subcommand_required(true)
            .subcommand(
                Command::new("insert")
                    .about("Insert vectors into a collection")
                    .arg(Arg::new("collection").help("Collection name").required(true))
                    .arg(Arg::new("id").help("Vector ID").required(true))
                    .arg(Arg::new("vector").help("Vector values (comma-separated)").required(true))
                    .arg(Arg::new("metadata").short('m').long("metadata").help("Metadata as JSON")),
            )
            .subcommand(
                Command::new("get")
                    .about("Get a vector by ID")
                    .arg(Arg::new("collection").help("Collection name").required(true))
                    .arg(Arg::new("id").help("Vector ID").required(true)),
            )
            .subcommand(
                Command::new("delete")
                    .about("Delete vectors")
                    .arg(Arg::new("collection").help("Collection name").required(true))
                    .arg(Arg::new("ids").help("Vector IDs (comma-separated)").required(true)),
            )
            .subcommand(
                Command::new("count")
                    .about("Count vectors in a collection")
                    .arg(Arg::new("collection").help("Collection name").required(true)),
            ),
    );

    cmd = cmd.subcommand(
        Command::new("search")
            .about("Search for similar vectors")
            .arg(
                Arg::new("collection")
                    .short('c')
                    .long("collection")
                    .help("Collection name")
                    .default_value("default"),
            )
            .arg(
                Arg::new("vector")
                    .help("Query vector (comma-separated values)")
                    .required(true),
            )
            .arg(
                Arg::new("k")
                    .short('k')
                    .long("k")
                    .help("Number of results to return")
                    .default_value("10"),
            ),
    );

    cmd = cmd.subcommand(
        Command::new("benchmark")
            .about("Run benchmark tests")
            .arg(
                Arg::new("collection")
                    .short('c')
                    .long("collection")
                    .help("Collection name")
                    .default_value("benchmark"),
            )
            .arg(
                Arg::new("count")
                    .short('n')
                    .long("count")
                    .help("Number of vectors to insert")
                    .default_value("1000"),
            )
            .arg(
                Arg::new("dimension")
                    .short('d')
                    .long("dimension")
                    .help("Vector dimension")
                    .default_value("128"),
            ),
    );

    let matches = cmd.get_matches();

    match matches.subcommand() {
        Some(("server", sub_matches)) => {
            let address = sub_matches.get_one::<String>("address").unwrap();
            let port = sub_matches.get_one::<String>("port").unwrap();
            let data_dir = sub_matches.get_one::<String>("data-dir").unwrap();

            println!("Starting CoreTexDB server on {}:{}", address, port);
            println!("Data directory: {}", data_dir);

            let config = ApiConfig {
                address: address.clone(),
                port: port.parse().unwrap(),
                enable_cors: true,
            };

            start_server(config).await?;
        }

        Some(("collection", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("create", create_matches)) => {
                    let name = create_matches.get_one::<String>("name").unwrap();
                    let dimension: usize = create_matches.get_one::<String>("dimension").unwrap().parse().unwrap();
                    let metric = create_matches.get_one::<String>("metric").unwrap();

                    let db_ref = db.clone();
                    db_ref.read().await.create_collection(name, dimension, metric).await
                        .map_err(|e| format!("Failed to create collection: {}", e))?;

                    println!("✓ Collection '{}' created (dimension: {}, metric: {})", name, dimension, metric);
                }

                Some(("list", _)) => {
                    let db_ref = db.clone();
                    let collections = db_ref.read().await.list_collections().await
                        .map_err(|e| format!("Failed to list collections: {}", e))?;

                    if collections.is_empty() {
                        println!("No collections found.");
                    } else {
                        println!("Collections:");
                        for coll in collections {
                            println!("  - {}", coll);
                        }
                    }
                }

                Some(("info", info_matches)) => {
                    let name = info_matches.get_one::<String>("name").unwrap();
                    let db_ref = db.clone();
                    
                    match db_ref.read().await.get_collection(name).await {
                        Ok(schema) => {
                            let count = db_ref.read().await.get_vectors_count(name).await.unwrap_or(0);
                            println!("Collection: {}", schema.name);
                            println!("  Dimension: {}", schema.dimension);
                            println!("  Distance metric: {:?}", schema.distance_metric);
                            println!("  Vectors count: {}", count);
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }

                Some(("delete", delete_matches)) => {
                    let name = delete_matches.get_one::<String>("name").unwrap();
                    let _force = delete_matches.get_one::<bool>("force");

                    let db_ref = db.clone();
                    db_ref.read().await.delete_collection(name).await
                        .map_err(|e| format!("Failed to delete collection: {}", e))?;

                    println!("✓ Collection '{}' deleted", name);
                }

                _ => {}
            }
        }

        Some(("vector", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("insert", insert_matches)) => {
                    let collection = insert_matches.get_one::<String>("collection").unwrap();
                    let id = insert_matches.get_one::<String>("id").unwrap();
                    let vector_str = insert_matches.get_one::<String>("vector").unwrap();
                    let metadata_str = insert_matches.get_one::<String>("metadata");

                    let vector: Vec<f32> = vector_str.split(',')
                        .map(|s| s.trim().parse::<f32>().unwrap())
                        .collect();

                    let metadata = match metadata_str {
                        Some(m) => serde_json::from_str(m).unwrap_or(serde_json::json!({})),
                        None => serde_json::json!({}),
                    };

                    let db_ref = db.clone();
                    db_ref.read().await.insert_vectors(collection, vec![(id.clone(), vector, metadata)]).await
                        .map_err(|e| format!("Failed to insert vector: {}", e))?;

                    println!("✓ Vector '{}' inserted into '{}'", id, collection);
                }

                Some(("get", get_matches)) => {
                    let collection = get_matches.get_one::<String>("collection").unwrap();
                    let id = get_matches.get_one::<String>("id").unwrap();

                    let db_ref = db.clone();
                    match db_ref.read().await.get_vector(collection, id).await {
                        Ok(Some((vector, metadata))) => {
                            println!("Vector ID: {}", id);
                            println!("Vector: {:?}", &vector[..10.min(vector.len())]);
                            println!("Metadata: {}", metadata);
                        }
                        Ok(None) => {
                            println!("Vector not found");
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }

                Some(("delete", delete_matches)) => {
                    let collection = delete_matches.get_one::<String>("collection").unwrap();
                    let ids_str = delete_matches.get_one::<String>("ids").unwrap();
                    let ids: Vec<String> = ids_str.split(',').map(|s| s.trim().to_string()).collect();

                    let db_ref = db.clone();
                    let count = db_ref.read().await.delete_vectors(collection, &ids).await
                        .map_err(|e| format!("Failed to delete vectors: {}", e))?;

                    println!("✓ {} vectors deleted from '{}'", count, collection);
                }

                Some(("count", count_matches)) => {
                    let collection = count_matches.get_one::<String>("collection").unwrap();

                    let db_ref = db.clone();
                    let count = db_ref.read().await.get_vectors_count(collection).await
                        .map_err(|e| format!("Failed to count vectors: {}", e))?;

                    println!("Collection '{}' has {} vectors", collection, count);
                }

                _ => {}
            }
        }

        Some(("search", sub_matches)) => {
            let collection = sub_matches.get_one::<String>("collection").unwrap();
            let vector_str = sub_matches.get_one::<String>("vector").unwrap();
            let k: usize = sub_matches.get_one::<String>("k").unwrap().parse().unwrap();

            let vector: Vec<f32> = vector_str.split(',')
                .map(|s| s.trim().parse::<f32>().unwrap())
                .collect();

            let db_ref = db.clone();
            let results = db_ref.read().await.search(collection, vector, k, None).await
                .map_err(|e| format!("Search failed: {}", e))?;

            println!("Search results from '{}' (k={}):", collection, k);
            for (i, result) in results.iter().enumerate() {
                println!("  {}. {} (score: {:.4})", i + 1, result.id, 1.0 - result.distance);
            }
        }

        Some(("benchmark", sub_matches)) => {
            let collection = sub_matches.get_one::<String>("collection").unwrap();
            let count: usize = sub_matches.get_one::<String>("count").unwrap().parse().unwrap();
            let dimension: usize = sub_matches.get_one::<String>("dimension").unwrap().parse().unwrap();

            let db_ref = db.clone();
            
            println!("Creating collection '{}'...", collection);
            let _ = db_ref.read().await.delete_collection(collection).await;
            db_ref.read().await.create_collection(collection, dimension, "cosine").await
                .map_err(|e| format!("Failed to create collection: {}", e))?;

            println!("Inserting {} vectors...", count);
            let start = std::time::Instant::now();
            
            for i in 0..count {
                let vector: Vec<f32> = (0..dimension).map(|_| rand::random::<f32>()).collect();
                let _ = db_ref.read().await.insert_vectors(
                    collection,
                    vec![(format!("vec_{}", i), vector, serde_json::json!({"index": i}))]
                ).await;

                if (i + 1) % 100 == 0 {
                    println!("  Inserted {}/{} vectors", i + 1, count);
                }
            }

            let insert_time = start.elapsed();
            println!("✓ Inserted {} vectors in {:.2?}", count, insert_time);
            println!("  Throughput: {:.0} vectors/sec", count as f64 / insert_time.as_secs_f64());

            println!("\nRunning search benchmark...");
            let search_start = std::time::Instant::now();
            
            for i in 0..10.min(count) {
                let query: Vec<f32> = (0..dimension).map(|_| rand::random::<f32>()).collect();
                let _ = db_ref.read().await.search(collection, query, 10, None).await;
            }

            let search_time = search_start.elapsed();
            println!("✓ Completed 10 searches in {:.2?}", search_time);
            println!("  Avg search time: {:.2?}", search_time / 10);
        }

        _ => {}
    }

    Ok(())
}
