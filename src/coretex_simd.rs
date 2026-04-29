//! SIMD-accelerated vector operations for CoreTexDB
//! Provides high-performance implementations using CPU SIMD instructions

#[cfg(target_arch = "x86_64")]
pub mod simd_utils {
    use std::arch::x86_64::*;
    use std::cmp::Ordering;

    #[inline]
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }

        let dot = dot_product(a, b);
        let norm_a = euclidean_norm(a);
        let norm_b = euclidean_norm(b);

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    #[inline]
    pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }

        let len = a.len();
        let mut sum = 0.0_f32;
        let mut i = 0;

        if is_x86_feature_detected!("fma") && is_x86_feature_detected!("avx") {
            let mut acc0 = _mm256_setzero_ps();
            let mut acc1 = _mm256_setzero_ps();

            while i + 16 <= len {
                let a0 = _mm256_loadu_ps(&a[i]);
                let b0 = _mm256_loadu_ps(&b[i]);
                let a1 = _mm256_loadu_ps(&a[i + 8]);
                let b1 = _mm256_loadu_ps(&b[i + 8]);

                acc0 = _mm256_fmadd_ps(a0, b0, acc0);
                acc1 = _mm256_fmadd_ps(a1, b1, acc1);

                i += 16;
            }

            let sum256 = _mm256_add_ps(acc0, acc1);
            let mut result = [0.0_f32; 8];
            _mm256_storeu_ps(&mut result, sum256);
            sum += result.iter().sum::<f32>();
        } else if is_x86_feature_detected!("sse") {
            let mut acc0 = _mm_setzero_ps();
            let mut acc1 = _mm_setzero_ps();

            while i + 8 <= len {
                let a0 = _mm_loadu_ps(&a[i]);
                let b0 = _mm_loadu_ps(&b[i]);
                let a1 = _mm_loadu_ps(&a[i + 4]);
                let b1 = _mm_loadu_ps(&b[i + 4]);

                acc0 = _mm_mul_ps(a0, b0);
                acc1 = _mm_mul_ps(a1, b1);

                i += 8;
            }

            let mut result = [0.0_f32; 4];
            let sum128 = _mm_add_ps(acc0, acc1);
            _mm_storeu_ps(&mut result, sum128);
            sum += result.iter().sum::<f32>();
        }

        while i < len {
            sum += a[i] * b[i];
            i += 1;
        }

        sum
    }

    #[inline]
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        euclidean_distance_squared(a, b).sqrt()
    }

    #[inline]
    pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }

        let len = a.len();
        let mut sum = 0.0_f32;
        let mut i = 0;

        if is_x86_feature_detected!("fma") && is_x86_feature_detected!("avx") {
            let mut acc0 = _mm256_setzero_ps();
            let mut acc1 = _mm256_setzero_ps();

            while i + 16 <= len {
                let a0 = _mm256_loadu_ps(&a[i]);
                let b0 = _mm256_loadu_ps(&b[i]);
                let a1 = _mm256_loadu_ps(&a[i + 8]);
                let b1 = _mm256_loadu_ps(&b[i + 8]);

                let diff0 = _mm256_sub_ps(a0, b0);
                let diff1 = _mm256_sub_ps(a1, b1);

                acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
                acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);

                i += 16;
            }

            let sum256 = _mm256_add_ps(acc0, acc1);
            let mut result = [0.0_f32; 8];
            _mm256_storeu_ps(&mut result, sum256);
            sum += result.iter().sum::<f32>();
        } else if is_x86_feature_detected!("sse") {
            let mut acc0 = _mm_setzero_ps();
            let mut acc1 = _mm_setzero_ps();

            while i + 8 <= len {
                let a0 = _mm_loadu_ps(&a[i]);
                let b0 = _mm_loadu_ps(&b[i]);
                let a1 = _mm_loadu_ps(&a[i + 4]);
                let b1 = _mm_loadu_ps(&b[i + 4]);

                let diff0 = _mm_sub_ps(a0, b0);
                let diff1 = _mm_sub_ps(a1, b1);

                acc0 = _mm_mul_ps(diff0, diff0);
                acc1 = _mm_mul_ps(diff1, diff1);

                i += 8;
            }

            let mut result = [0.0_f32; 4];
            let sum128 = _mm_add_ps(acc0, acc1);
            _mm_storeu_ps(&mut result, sum128);
            sum += result.iter().sum::<f32>();
        }

        while i < len {
            let diff = a[i] - b[i];
            sum += diff * diff;
            i += 1;
        }

        sum
    }

    #[inline]
    pub fn euclidean_norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    #[inline]
    pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }

        let len = a.len();
        let mut sum = 0.0_f32;
        let mut i = 0;

        if is_x86_feature_detected!("avx") {
            let mut acc0 = _mm256_setzero_ps();
            let mut acc1 = _mm256_setzero_ps();

            while i + 16 <= len {
                let a0 = _mm256_loadu_ps(&a[i]);
                let b0 = _mm256_loadu_ps(&b[i]);
                let a1 = _mm256_loadu_ps(&a[i + 8]);
                let b1 = _mm256_loadu_ps(&b[i + 8]);

                let diff0 = _mm256_sub_ps(a0, b0);
                let diff1 = _mm256_sub_ps(a1, b1);

                let abs0 = _mm256_andnot_ps(diff0, diff0);
                let abs1 = _mm256_andnot_ps(diff1, diff1);

                acc0 = _mm256_add_ps(acc0, abs0);
                acc1 = _mm256_add_ps(acc1, abs1);

                i += 16;
            }

            let sum256 = _mm256_add_ps(acc0, acc1);
            let mut result = [0.0_f32; 8];
            _mm256_storeu_ps(&mut result, sum256);
            sum += result.iter().sum::<f32>();
        } else if is_x86_feature_detected!("sse") {
            let mut acc0 = _mm_setzero_ps();
            let mut acc1 = _mm_setzero_ps();

            while i + 8 <= len {
                let a0 = _mm_loadu_ps(&a[i]);
                let b0 = _mm_loadu_ps(&b[i]);
                let a1 = _mm_loadu_ps(&a[i + 4]);
                let b1 = _mm_loadu_ps(&b[i + 4]);

                let diff0 = _mm_sub_ps(a0, b0);
                let diff1 = _mm_sub_ps(a1, b1);

                let abs0 = _mm_andnot_ps(diff0, diff0);
                let abs1 = _mm_andnot_ps(diff1, diff1);

                acc0 = _mm_add_ps(acc0, abs0);
                acc1 = _mm_add_ps(acc1, abs1);

                i += 8;
            }

            let mut result = [0.0_f32; 4];
            let sum128 = _mm_add_ps(acc0, acc1);
            _mm_storeu_ps(&mut result, sum128);
            sum += result.iter().sum::<f32>();
        }

        while i < len {
            sum += (a[i] - b[i]).abs();
            i += 1;
        }

        sum
    }

    pub fn has_avx() -> bool {
        is_x86_feature_detected!("avx")
    }

    pub fn has_avx2() -> bool {
        is_x86_feature_detected!("avx2")
    }

    pub fn has_fma() -> bool {
        is_x86_feature_detected!("fma")
    }

    pub fn has_sse() -> bool {
        is_x86_feature_detected!("sse4.1")
    }

    pub fn get_capabilities() -> SimdCapabilities {
        SimdCapabilities {
            has_avx: has_avx(),
            has_avx2: has_avx2(),
            has_fma: has_fma(),
            has_sse: has_sse(),
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub mod simd_utils {

    #[inline]
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    #[inline]
    pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    #[inline]
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        euclidean_distance_squared(a, b).sqrt()
    }

    #[inline]
    pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }
        a.iter().zip(b.iter()).map(|(x, y)| {
            let diff = x - y;
            diff * diff
        }).sum()
    }

    #[inline]
    pub fn euclidean_norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    #[inline]
    pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum()
    }

    pub fn has_avx() -> bool { false }
    pub fn has_avx2() -> bool { false }
    pub fn has_fma() -> bool { false }
    pub fn has_sse() -> bool { false }

    pub fn get_capabilities() -> SimdCapabilities {
        SimdCapabilities {
            has_avx: false,
            has_avx2: false,
            has_fma: false,
            has_sse: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimdCapabilities {
    pub has_avx: bool,
    pub has_avx2: bool,
    pub has_fma: bool,
    pub has_sse: bool,
}

impl SimdCapabilities {
    pub fn is_fast(&self) -> bool {
        self.has_avx || self.has_sse
    }

    pub fn summary(&self) -> String {
        let mut caps = Vec::new();
        if self.has_sse { caps.push("SSE"); }
        if self.has_avx { caps.push("AVX"); }
        if self.has_avx2 { caps.push("AVX2"); }
        if self.has_fma { caps.push("FMA"); }

        if caps.is_empty() {
            "Scalar".to_string()
        } else {
            caps.join("+")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];

        let similarity = simd_utils::cosine_similarity(&a, &b);
        assert!((similarity - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        let similarity = simd_utils::cosine_similarity(&a, &b);
        assert!((similarity - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];

        let dot = simd_utils::dot_product(&a, &b);
        assert!((dot - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];

        let dist = simd_utils::euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_norm() {
        let v = vec![3.0, 4.0, 0.0];

        let norm = simd_utils::euclidean_norm(&v);
        assert!((norm - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];

        let dist = simd_utils::manhattan_distance(&a, &b);
        assert!((dist - 6.0).abs() < 1e-6);
    }

    #[test]
    fn test_simd_capabilities() {
        let caps = simd_utils::get_capabilities();
        let summary = caps.summary();
        assert!(!summary.is_empty());
    }
}
