//! Basic performance test for CortexDB

use cortexdb::{CortexDB, Vector, DistanceMetric, IndexType};
use std::time::Instant;
use rand::Rng;

fn generate_random_vector(dim: usize) -> Vector {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

async fn test_index_performance(index_type: IndexType, dim: usize, n_vectors: usize, k: usize) {
    println!("Testing {} index with {} vectors of dimension {}", index_type, n_vectors, dim);
    
    // Create database
    let mut db = CortexDB::new();
    
    // Create collection
    let collection_name = format!("test_{}", index_type);
    db.create_collection(&collection_name, dim, DistanceMetric::Cosine, IndexType::BruteForce).await.unwrap();
    
    // Insert vectors
    let start = Instant::now();
    for i in 0..n_vectors {
        let vector = generate_random_vector(dim);
        let id = format!("vec_{}", i);
        db.insert(&collection_name, &id, &vector, None).await.unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?} ({:.2} vectors/s)", insert_time, n_vectors as f64 / insert_time.as_secs_f64());
    
    // Build index
    let start = Instant::now();
    db.build_index(&collection_name).await.unwrap();
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);
    
    // Search
    let start = Instant::now();
    let n_queries = 100;
    for _ in 0..n_queries {
        let query = generate_random_vector(dim);
        db.search(&collection_name, &query, k).await.unwrap();
    }
    let search_time = start.elapsed();
    println!("Search time: {:?} ({:.2} queries/s)", search_time, n_queries as f64 / search_time.as_secs_f64());
    
    println!();
}

#[tokio::main]
async fn main() {
    // Test different index types
    let dim = 128;
    let n_vectors = 10_000;
    let k = 10;
    
    test_index_performance(IndexType::BruteForce, dim, n_vectors, k).await;
    test_index_performance(IndexType::HNSW, dim, n_vectors, k).await;
    test_index_performance(IndexType::IVF, dim, n_vectors, k).await;
}
