//! Simple performance test for CortexDB index implementations

use cortexdb::{cortex_index::{BruteForceIndex, HNSWIndex, IVFIndex, ScalarIndex}, StorageEngine, MemoryStorage};
use std::time::Instant;
use rand::Rng;
use tokio::main;

fn generate_random_vector(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

async fn test_brute_force_index(dim: usize, n_vectors: usize, k: usize) {
    println!("Testing BruteForceIndex with {} vectors of dimension {}", n_vectors, dim);
    
    // Create index
    let index = BruteForceIndex::new("cosine");
    
    // Insert vectors
    let start = Instant::now();
    for i in 0..n_vectors {
        let vector = generate_random_vector(dim);
        let id = format!("vec_{}", i);
        index.add(&id, &vector).await.unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?} ({:.2} vectors/s)", insert_time, n_vectors as f64 / insert_time.as_secs_f64());
    
    // Search
    let start = Instant::now();
    let n_queries = 100;
    for _ in 0..n_queries {
        let query = generate_random_vector(dim);
        index.search(&query, k).await.unwrap();
    }
    let search_time = start.elapsed();
    println!("Search time: {:?} ({:.2} queries/s)", search_time, n_queries as f64 / search_time.as_secs_f64());
    
    println!();
}

async fn test_hnsw_index(dim: usize, n_vectors: usize, k: usize) {
    println!("Testing HNSWIndex with {} vectors of dimension {}", n_vectors, dim);
    
    // Create index
    let index = HNSWIndex::new("cosine");
    
    // Insert vectors
    let start = Instant::now();
    for i in 0..n_vectors {
        let vector = generate_random_vector(dim);
        let id = format!("vec_{}", i);
        index.add(&id, &vector).await.unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?} ({:.2} vectors/s)", insert_time, n_vectors as f64 / insert_time.as_secs_f64());
    
    // Build index
    let start = Instant::now();
    index.build().await.unwrap();
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);
    
    // Search
    let start = Instant::now();
    let n_queries = 100;
    for _ in 0..n_queries {
        let query = generate_random_vector(dim);
        index.search(&query, k).await.unwrap();
    }
    let search_time = start.elapsed();
    println!("Search time: {:?} ({:.2} queries/s)", search_time, n_queries as f64 / search_time.as_secs_f64());
    
    println!();
}

async fn test_ivf_index(dim: usize, n_vectors: usize, k: usize) {
    println!("Testing IVFIndex with {} vectors of dimension {}", n_vectors, dim);
    
    // Create index
    let index = IVFIndex::new("cosine");
    
    // Insert vectors
    let start = Instant::now();
    for i in 0..n_vectors {
        let vector = generate_random_vector(dim);
        let id = format!("vec_{}", i);
        index.add(&id, &vector).await.unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?} ({:.2} vectors/s)", insert_time, n_vectors as f64 / insert_time.as_secs_f64());
    
    // Build index
    let start = Instant::now();
    index.build().await.unwrap();
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);
    
    // Search
    let start = Instant::now();
    let n_queries = 100;
    for _ in 0..n_queries {
        let query = generate_random_vector(dim);
        index.search(&query, k).await.unwrap();
    }
    let search_time = start.elapsed();
    println!("Search time: {:?} ({:.2} queries/s)", search_time, n_queries as f64 / search_time.as_secs_f64());
    
    println!();
}

async fn test_scalar_index(n_vectors: usize, k: usize) {
    println!("Testing ScalarIndex with {} scalars", n_vectors);
    
    // Create index
    let index = ScalarIndex::new();
    
    // Insert scalars (as single-element vectors)
    let start = Instant::now();
    let mut rng = rand::thread_rng();
    for i in 0..n_vectors {
        let scalar = rng.gen_range(0.0..1000.0);
        let vector = vec![scalar];
        let id = format!("vec_{}", i);
        index.add(&id, &vector).await.unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?} ({:.2} scalars/s)", insert_time, n_vectors as f64 / insert_time.as_secs_f64());
    
    // Build index
    let start = Instant::now();
    index.build().await.unwrap();
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);
    
    // Search
    let start = Instant::now();
    let n_queries = 100;
    for _ in 0..n_queries {
        let target = rng.gen_range(0.0..1000.0);
        let query = vec![target];
        index.search(&query, k).await.unwrap();
    }
    let search_time = start.elapsed();
    println!("Search time: {:?} ({:.2} queries/s)", search_time, n_queries as f64 / search_time.as_secs_f64());
    
    println!();
}

#[main]
async fn main() {
    // Test parameters
    let dim = 128;
    let n_vectors = 1000;
    let k = 10;
    
    // Run tests
    test_brute_force_index(dim, n_vectors, k).await;
    test_hnsw_index(dim, n_vectors, k).await;
    test_ivf_index(dim, n_vectors, k).await;
    test_scalar_index(n_vectors, k).await;
    
    println!("Performance test completed!");
}
