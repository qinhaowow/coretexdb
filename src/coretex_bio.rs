//! Bioinformatics Module for CortexDB
//! DNA/Protein k-mer embedding and biological sequence processing

use std::collections::{HashMap, HashSet};

pub struct KmerIndexer {
    k: usize,
    alphabet: HashMap<char, usize>,
}

impl KmerIndexer {
    pub fn new(k: usize) -> Self {
        let alphabet = Self::default_alphabet();
        Self { k, alphabet }
    }

    pub fn with_alphabet(mut self, alphabet: HashMap<char, usize>) -> Self {
        self.alphabet = alphabet;
        self
    }

    fn default_alphabet() -> HashMap<char, usize> {
        let mut map = HashMap::new();
        map.insert('A', 0); map.insert('a', 0);
        map.insert('C', 1); map.insert('c', 1);
        map.insert('G', 2); map.insert('g', 2);
        map.insert('T', 3); map.insert('t', 3);
        map.insert('U', 3); map.insert('u', 3);
        map
    }

    pub fn dna_alphabet() -> HashMap<char, usize> {
        let mut map = HashMap::new();
        map.insert('A', 0); map.insert('a', 0);
        map.insert('C', 1); map.insert('c', 1);
        map.insert('G', 2); map.insert('g', 2);
        map.insert('T', 3); map.insert('t', 3);
        map.insert('U', 3); map.insert('u', 3);
        map.insert('N', 4); map.insert('n', 4);
        map
    }

    pub fn protein_alphabet() -> HashMap<char, usize> {
        let amino_acids = "ACDEFGHIKLMNPQRSTVWY";
        let mut map = HashMap::new();
        for (i, aa) in amino_acids.chars().enumerate() {
            map.insert(aa, i);
            map.insert(aa.to_ascii_lowercase(), i);
        }
        map
    }

    pub fn extract_kmers(&self, sequence: &str) -> Vec<String> {
        let clean_seq: String = sequence
            .chars()
            .filter(|c| self.alphabet.contains_key(c))
            .collect();

        if clean_seq.len() < self.k {
            return Vec::new();
        }

        (0..=clean_seq.len() - self.k)
            .map(|i| clean_seq[i..i + self.k].to_string())
            .collect()
    }

    pub fn kmer_counts(&self, sequence: &str) -> HashMap<String, usize> {
        let kmers = self.extract_kmers(sequence);
        let mut counts = HashMap::new();
        for kmer in kmers {
            *counts.entry(kmer).or_insert(0) += 1;
        }
        counts
    }

    pub fn kmer_frequency(&self, sequence: &str) -> HashMap<String, f64> {
        let counts = self.kmer_counts(sequence);
        let total: usize = counts.values().sum();
        
        if total == 0 {
            return HashMap::new();
        }

        counts
            .into_iter()
            .map(|(kmer, count)| (kmer, count as f64 / total as f64))
            .collect()
    }

    pub fn sequence_to_vector(&self, sequence: &str) -> Vec<f32> {
        let frequency = self.kmer_frequency(sequence);
        let n_possible = self.alphabet.len().pow(self.k as u32);
        let mut vector = vec![0.0; n_possible];

        for (kmer, freq) in frequency {
            let index = self.kmer_to_index(&kmer);
            if let Some(idx) = index {
                vector[idx] = freq as f32;
            }
        }

        vector
    }

    pub fn kmer_to_index(&self, kmer: &str) -> Option<usize> {
        let mut index = 0;
        let base = self.alphabet.len();

        for (i, c) in kmer.chars().rev().enumerate() {
            if let Some(&val) = self.alphabet.get(&c) {
                index += val * base.pow(i as u32);
            } else {
                return None;
            }
        }

        Some(index)
    }

    pub fn reverse_complement(&self, sequence: &str) -> String {
        let complement = |c: char| -> char {
            match c.to_ascii_uppercase() {
                'A' => 'T',
                'T' | 'U' => 'A',
                'G' => 'C',
                'C' => 'G',
                'N' => 'N',
                c => c,
            }
        };

        sequence
            .chars()
            .rev()
            .map(complement)
            .collect()
    }

    pub fn gc_content(&self, sequence: &str) -> f64 {
        let clean: String = sequence
            .chars()
            .filter(|c| matches!(c.to_ascii_uppercase(), 'G' | 'C' | 'A' | 'T'))
            .collect();

        if clean.is_empty() {
            return 0.0;
        }

        let gc_count = clean
            .chars()
            .filter(|c| matches!(c.to_ascii_uppercase(), 'G' | 'C'))
            .count();

        gc_count as f64 / clean.len() as f64
    }
}

pub struct SequenceChunker {
    chunk_size: usize,
    overlap: usize,
}

impl SequenceChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size, overlap: 0 }
    }

    pub fn with_overlap(mut self, overlap: usize) -> Self {
        self.overlap = overlap;
        self
    }

    pub fn chunk(&self, sequence: &str) -> Vec<SequenceChunk> {
        let clean: String = sequence
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        if clean.is_empty() || self.chunk_size == 0 {
            return Vec::new();
        }

        let step = self.chunk_size.saturating_sub(self.overlap);
        let mut chunks = Vec::new();
        let mut start = 0;

        while start < clean.len() {
            let end = (start + self.chunk_size).min(clean.len());
            chunks.push(SequenceChunk {
                sequence: clean[start..end].to_string(),
                start_position: start,
                end_position: end,
                chunk_index: chunks.len(),
            });

            if end == clean.len() {
                break;
            }
            start += step;
        }

        chunks
    }

    pub fn chunk_with_metadata(&self, sequence: &str, metadata: HashMap<String, String>) -> Vec<SequenceChunkWithMeta> {
        self.chunk(sequence)
            .into_iter()
            .map(|chunk| SequenceChunkWithMeta {
                chunk,
                metadata: metadata.clone(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct SequenceChunk {
    pub sequence: String,
    pub start_position: usize,
    pub end_position: usize,
    pub chunk_index: usize,
}

#[derive(Debug, Clone)]
pub struct SequenceChunkWithMeta {
    pub chunk: SequenceChunk,
    pub metadata: HashMap<String, String>,
}

pub struct BinaryVector {
    data: Vec<u8>,
    dimension: usize,
}

impl BinaryVector {
    pub fn new(dimension: usize) -> Self {
        let byte_size = (dimension + 7) / 8;
        Self {
            data: vec![0u8; byte_size],
            dimension,
        }
    }

    pub fn from_floats(floats: &[f32], threshold: f32) -> Self {
        let dimension = floats.len();
        let mut vec = Self::new(dimension);

        for (i, &f) in floats.iter().enumerate() {
            if f >= threshold {
                vec.set_bit(i, true);
            }
        }

        vec
    }

    pub fn set_bit(&mut self, index: usize, value: bool) {
        if index >= self.dimension {
            return;
        }
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        
        if value {
            self.data[byte_idx] |= 1 << bit_idx;
        } else {
            self.data[byte_idx] &= !(1 << bit_idx);
        }
    }

    pub fn get_bit(&self, index: usize) -> bool {
        if index >= self.dimension {
            return false;
        }
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn hamming_distance(&self, other: &BinaryVector) -> usize {
        if self.dimension != other.dimension {
            return usize::MAX;
        }

        let mut distance = 0;
        for (a, b) in self.data.iter().zip(other.data.iter()) {
            distance += (a ^ b).count_ones() as usize;
        }

        distance
    }

    pub fn jaccard_similarity(&self, other: &BinaryVector) -> f64 {
        if self.dimension != other.dimension {
            return 0.0;
        }

        let mut intersection = 0usize;
        let mut union = 0usize;

        for i in 0..self.dimension {
            let a = self.get_bit(i);
            let b = other.get_bit(i);
            if a || b {
                union += 1;
            }
            if a && b {
                intersection += 1;
            }
        }

        if union == 0 {
            return 1.0;
        }

        intersection as f64 / union as f64
    }
}

pub struct IntegerVector {
    data: Vec<i32>,
    dimension: usize,
}

impl IntegerVector {
    pub fn new(dimension: usize) -> Self {
        Self {
            data: vec![0; dimension],
            dimension,
        }
    }

    pub fn from_slice(data: &[i32]) -> Self {
        let dimension = data.len();
        Self {
            data: data.to_vec(),
            dimension,
        }
    }

    pub fn get(&self, index: usize) -> i32 {
        self.data.get(index).copied().unwrap_or(0)
    }

    pub fn set(&mut self, index: usize, value: i32) {
        if index < self.dimension {
            self.data[index] = value;
        }
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn l2_distance(&self, other: &IntegerVector) -> i64 {
        if self.dimension != other.dimension {
            return i64::MAX;
        }

        let mut sum: i64 = 0;
        for (a, b) in self.data.iter().zip(other.data.iter()) {
            let diff = *a as i64 - *b as i64;
            sum += diff * diff;
        }

        (sum as f64).sqrt() as i64
    }

    pub fn cosine_similarity(&self, other: &IntegerVector) -> f64 {
        if self.dimension != other.dimension {
            return 0.0;
        }

        let mut dot: i64 = 0;
        let mut norm_a: i64 = 0;
        let mut norm_b: i64 = 0;

        for (a, b) in self.data.iter().zip(other.data.iter()) {
            dot += *a as i64 * *b as i64;
            norm_a += (*a as i64) * (*a as i64);
            norm_b += (*b as i64) * (*b as i64);
        }

        let denom = ((norm_a as f64) * (norm_b as f64)).sqrt();
        if denom == 0.0 {
            return 0.0;
        }

        dot as f64 / denom
    }
}

pub struct SpacetimeIndex {
    data: Vec<SpacetimePoint>,
    x_bins: usize,
    y_bins: usize,
    z_bins: usize,
    t_bins: usize,
}

#[derive(Debug, Clone)]
pub struct SpacetimePoint {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub t: f64,
    pub vector: Vec<f32>,
    pub metadata: HashMap<String, String>,
}

impl SpacetimeIndex {
    pub fn new(x_bins: usize, y_bins: usize, z_bins: usize, t_bins: usize) -> Self {
        Self {
            data: Vec::new(),
            x_bins,
            y_bins,
            z_bins,
            t_bins,
        }
    }

    pub fn insert(&mut self, point: SpacetimePoint) {
        self.data.push(point);
    }

    pub fn query_box(
        &self,
        x_min: f64, x_max: f64,
        y_min: f64, y_max: f64,
        z_min: f64, z_max: f64,
        t_min: f64, t_max: f64,
    ) -> Vec<&SpacetimePoint> {
        self.data
            .iter()
            .filter(|p| {
                p.x >= x_min && p.x <= x_max
                    && p.y >= y_min && p.y <= y_max
                    && p.z >= z_min && p.z <= z_max
                    && p.t >= t_min && p.t <= t_max
            })
            .collect()
    }

    pub fn query_range(
        &self,
        center: &SpacetimePoint,
        spatial_radius: f64,
        temporal_radius: f64,
    ) -> Vec<&SpacetimePoint> {
        self.data
            .iter()
            .filter(|p| {
                let spatial_dist = ((p.x - center.x).powi(2)
                    + (p.y - center.y).powi(2)
                    + (p.z - center.z).powi(2)).sqrt();
                let temporal_dist = (p.t - center.t).abs();

                spatial_dist <= spatial_radius && temporal_dist <= temporal_radius
            })
            .collect()
    }

    pub fn knn_spatiotemporal(
        &self,
        query: &SpacetimePoint,
        k: usize,
        spatial_weight: f64,
        temporal_weight: f64,
    ) -> Vec<(f64, &SpacetimePoint)> {
        let mut distances: Vec<(f64, &SpacetimePoint)> = self.data
            .iter()
            .map(|p| {
                let spatial_dist = ((p.x - query.x).powi(2)
                    + (p.y - query.y).powi(2)
                    + (p.z - query.z).powi(2)).sqrt();
                let temporal_dist = (p.t - query.t).abs();
                let combined = spatial_dist * spatial_weight + temporal_dist * temporal_weight;
                (combined, p)
            })
            .collect();

        distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        distances.truncate(k);
        distances
    }
}

pub struct UserDefinedFunction {
    name: String,
    func_type: UdfType,
    parameters: Vec<UdfParameter>,
    body: String,
}

#[derive(Debug, Clone)]
pub enum UdfType {
    Scalar,
    Aggregate,
    Vectorize,
    Filter,
}

#[derive(Debug, Clone)]
pub struct UdfParameter {
    pub name: String,
    pub param_type: UdfParamType,
}

#[derive(Debug, Clone)]
pub enum UdfParamType {
    Float,
    Integer,
    String,
    Vector,
    Binary,
}

impl UserDefinedFunction {
    pub fn new(name: &str, func_type: UdfType) -> Self {
        Self {
            name: name.to_string(),
            func_type,
            parameters: Vec::new(),
            body: String::new(),
        }
    }

    pub fn with_parameters(mut self, params: Vec<UdfParameter>) -> Self {
        self.parameters = params;
        self
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    pub fn execute_float(&self, args: &[f32]) -> f32 {
        match self.name.as_str() {
            "normalize" => {
                let norm = args.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    args.iter().map(|x| x / norm).sum()
                } else {
                    0.0
                }
            },
            "sigmoid" => {
                if args.is_empty() { return 0.0; }
                1.0 / (1.0 + (-args[0]).exp())
            },
            "relu" => {
                if args.is_empty() { return 0.0; }
                args[0].max(0.0)
            },
            _ => args.iter().sum(),
        }
    }

    pub fn execute_vector(&self, args: &[Vec<f32>]) -> Vec<f32> {
        match self.name.as_str() {
            "elementwise_add" => {
                if args.is_empty() { return Vec::new(); }
                let len = args[0].len();
                (0..len)
                    .map(|i| args.iter().map(|v| v[i]).sum())
                    .collect()
            },
            "elementwise_mul" => {
                if args.is_empty() { return Vec::new(); }
                let len = args[0].len();
                (0..len)
                    .map(|i| args.iter().map(|v| v[i]).product::<f32>())
                    .collect()
            },
            _ => args.first().cloned().unwrap_or_default(),
        }
    }
}

pub struct UdfRegistry {
    functions: HashMap<String, UserDefinedFunction>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    pub fn register(&mut self, udf: UserDefinedFunction) {
        self.functions.insert(udf.name.clone(), udf);
    }

    pub fn get(&self, name: &str) -> Option<&UserDefinedFunction> {
        self.functions.get(name)
    }

    pub fn list(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    pub fn call(&self, name: &str, args: &[f32]) -> Option<f32> {
        self.functions.get(name).map(|f| f.execute_float(args))
    }
}
