//! Vector indexing benchmarks for CortexDB

use cortexdb::coretex_index::{BruteForceIndex, HNSWIndex, VectorIndex};
use rand::Rng;
use std::time::Instant;

fn generate_random_vectors(dim: usize, count: usize) -> Vec<(String, Vec<f32>)> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let vector: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            (format!("vec_{}", i), vector)
        })
        .collect()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    1.0 - dot / (norm_a * norm_b)
}

async fn benchmark_brute_force(dim: usize, n_vectors: usize, n_queries: usize, k: usize) {
    println!("\n=== Brute Force Index ===");
    println!("Dimensions: {}, Vectors: {}, Queries: {}", dim, n_vectors, n_queries);

    let vectors = generate_random_vectors(dim, n_vectors);

    // Build index
    let start = Instant::now();
    let index = BruteForceIndex::new("cosine".to_string());
    for (id, vector) in &vectors {
        let _ = index.add(id, vector).await;
    }
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);

    // Search
    let queries = generate_random_vectors(dim, n_queries);
    let start = Instant::now();
    for (id, query) in &queries {
        let _ = index.search(query, k).await;
    }
    let search_time = start.elapsed();
    println!("Search time: {:?}", search_time);
    println!(
        "Throughput: {:.2} queries/sec",
        n_queries as f64 / search_time.as_secs_f64()
    );
}

async fn benchmark_hnsw(dim: usize, n_vectors: usize, n_queries: usize, k: usize) {
    println!("\n=== HNSW Index ===");
    println!("Dimensions: {}, Vectors: {}, Queries: {}", dim, n_vectors, n_queries);

    let vectors = generate_random_vectors(dim, n_vectors);

    // Build index
    let start = Instant::now();
    let index = HNSWIndex::new("cosine".to_string(), 16, 100, 50, 10);
    for (id, vector) in &vectors {
        let _ = index.add(id, vector).await;
    }
    let _ = index.build().await;
    let build_time = start.elapsed();
    println!("Build time: {:?}", build_time);

    // Search
    let queries = generate_random_vectors(dim, n_queries);
    let start = Instant::now();
    for (id, query) in &queries {
        let _ = index.search(query, k).await;
    }
    let search_time = start.elapsed();
    println!("Search time: {:?}", search_time);
    println!(
        "Throughput: {:.2} queries/sec",
        n_queries as f64 / search_time.as_secs_f64()
    );
}

#[tokio::main]
async fn main() {
    let dim = 128;
    let n_vectors = 10000;
    let n_queries = 1000;
    let k = 10;

    println!("CortexDB Vector Index Benchmarks");
    println!("=================================");

    benchmark_brute_force(dim, n_vectors, n_queries, k).await;
    benchmark_hnsw(dim, n_vectors, n_queries, k).await;

    println!("\n=== Summary ===");
    println!("Test completed successfully!");
}
