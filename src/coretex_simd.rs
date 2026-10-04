//! SIMD distance kernels.
//!
//! The vectorised paths live in `#[target_feature]` functions and the CPU is
//! probed once per process (D2). Both details matter for speed: kernels in a
//! function the compiler cannot assume AVX for degrade into opaque intrinsic
//! calls, which measured *slower* than scalar code, and per-call feature
//! detection put a branch and a cache lookup in the innermost loop.
//!
//! Every kernel has a scalar tail, so a vector length that is not a multiple
//! of the lane width is handled without a second code path for the caller.

#[cfg(target_arch = "x86_64")]
pub mod simd_utils {
    use std::arch::x86_64::*;
    use std::sync::OnceLock;

    /// Which kernel the CPU supports, resolved once per process.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Kernel {
        Scalar,
        Sse,
        Avx,
    }

    fn kernel() -> Kernel {
        static KERNEL: OnceLock<Kernel> = OnceLock::new();
        *KERNEL.get_or_init(|| {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                Kernel::Avx
            } else if is_x86_feature_detected!("sse2") {
                Kernel::Sse
            } else {
                Kernel::Scalar
            }
        })
    }

    /// Name of the kernel actually in use: `"avx+fma"`, `"sse2"` or `"scalar"`.
    pub fn active_kernel() -> &'static str {
        match kernel() {
            Kernel::Avx => "avx+fma",
            Kernel::Sse => "sse2",
            Kernel::Scalar => "scalar",
        }
    }

    // ── AVX + FMA kernels ───────────────────────────────────────────

    #[target_feature(enable = "avx,fma")]
    unsafe fn dot_avx(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut i = 0;
        // Four independent accumulators: without them the FMA chain serialises
        // on a single dependency and the throughput gain mostly disappears.
        while i + 32 <= len {
            acc0 = _mm256_fmadd_ps(
                _mm256_loadu_ps(&a[i]),
                _mm256_loadu_ps(&b[i]),
                acc0,
            );
            acc1 = _mm256_fmadd_ps(
                _mm256_loadu_ps(&a[i + 8]),
                _mm256_loadu_ps(&b[i + 8]),
                acc1,
            );
            acc2 = _mm256_fmadd_ps(
                _mm256_loadu_ps(&a[i + 16]),
                _mm256_loadu_ps(&b[i + 16]),
                acc2,
            );
            acc3 = _mm256_fmadd_ps(
                _mm256_loadu_ps(&a[i + 24]),
                _mm256_loadu_ps(&b[i + 24]),
                acc3,
            );
            i += 32;
        }
        while i + 8 <= len {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(&a[i]), _mm256_loadu_ps(&b[i]), acc0);
            i += 8;
        }
        let sum = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += a[i] * b[i];
            i += 1;
        }
        total
    }

    /// Absolute value by masking the sign bit out. `andnot` complements its
    /// *first* operand, so `andnot(diff, diff)` — the obvious spelling — is
    /// zero on every lane.
    #[inline]
    unsafe fn abs256(v: __m256) -> __m256 {
        _mm256_andnot_ps(_mm256_set1_ps(-0.0), v)
    }

    #[target_feature(enable = "avx,fma")]
    unsafe fn manhattan_avx(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 16 <= len {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(&a[i]), _mm256_loadu_ps(&b[i]));
            let d1 = _mm256_sub_ps(
                _mm256_loadu_ps(&a[i + 8]),
                _mm256_loadu_ps(&b[i + 8]),
            );
            acc0 = _mm256_add_ps(acc0, abs256(d0));
            acc1 = _mm256_add_ps(acc1, abs256(d1));
            i += 16;
        }
        let sum = _mm256_add_ps(acc0, acc1);
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += (a[i] - b[i]).abs();
            i += 1;
        }
        total
    }

    #[target_feature(enable = "avx,fma")]
    unsafe fn euclidean_squared_avx(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 32 <= len {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(&a[i]), _mm256_loadu_ps(&b[i]));
            let d1 = _mm256_sub_ps(
                _mm256_loadu_ps(&a[i + 8]),
                _mm256_loadu_ps(&b[i + 8]),
            );
            let d2 = _mm256_sub_ps(
                _mm256_loadu_ps(&a[i + 16]),
                _mm256_loadu_ps(&b[i + 16]),
            );
            let d3 = _mm256_sub_ps(
                _mm256_loadu_ps(&a[i + 24]),
                _mm256_loadu_ps(&b[i + 24]),
            );
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            acc1 = _mm256_fmadd_ps(d1, d1, acc1);
            acc2 = _mm256_fmadd_ps(d2, d2, acc2);
            acc3 = _mm256_fmadd_ps(d3, d3, acc3);
            i += 32;
        }
        while i + 8 <= len {
            let d = _mm256_sub_ps(_mm256_loadu_ps(&a[i]), _mm256_loadu_ps(&b[i]));
            acc0 = _mm256_fmadd_ps(d, d, acc0);
            i += 8;
        }
        let sum = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            let d = a[i] - b[i];
            total += d * d;
            i += 1;
        }
        total
    }

    #[target_feature(enable = "avx,fma")]
    unsafe fn norm_squared_avx(v: &[f32]) -> f32 {
        let len = v.len();
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 16 <= len {
            let a0 = _mm256_loadu_ps(&v[i]);
            let a1 = _mm256_loadu_ps(&v[i + 8]);
            acc0 = _mm256_fmadd_ps(a0, a0, acc0);
            acc1 = _mm256_fmadd_ps(a1, a1, acc1);
            i += 16;
        }
        let sum = _mm256_add_ps(acc0, acc1);
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), sum);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += v[i] * v[i];
            i += 1;
        }
        total
    }

    // ── SSE kernels ─────────────────────────────────────────────────

    #[target_feature(enable = "sse2")]
    unsafe fn dot_sse(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc0 = _mm_setzero_ps();
        let mut acc1 = _mm_setzero_ps();
        let mut i = 0;
        while i + 8 <= len {
            acc0 = _mm_add_ps(
                acc0,
                _mm_mul_ps(_mm_loadu_ps(&a[i]), _mm_loadu_ps(&b[i])),
            );
            acc1 = _mm_add_ps(
                acc1,
                _mm_mul_ps(_mm_loadu_ps(&a[i + 4]), _mm_loadu_ps(&b[i + 4])),
            );
            i += 8;
        }
        let mut lanes = [0.0f32; 4];
        _mm_storeu_ps(lanes.as_mut_ptr(), _mm_add_ps(acc0, acc1));
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += a[i] * b[i];
            i += 1;
        }
        total
    }

    #[inline]
    unsafe fn abs128(v: __m128) -> __m128 {
        _mm_andnot_ps(_mm_set1_ps(-0.0), v)
    }

    #[target_feature(enable = "sse2")]
    unsafe fn manhattan_sse(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc = _mm_setzero_ps();
        let mut i = 0;
        while i + 4 <= len {
            let d = _mm_sub_ps(_mm_loadu_ps(&a[i]), _mm_loadu_ps(&b[i]));
            acc = _mm_add_ps(acc, abs128(d));
            i += 4;
        }
        let mut lanes = [0.0f32; 4];
        _mm_storeu_ps(lanes.as_mut_ptr(), acc);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += (a[i] - b[i]).abs();
            i += 1;
        }
        total
    }

    #[target_feature(enable = "sse2")]
    unsafe fn euclidean_squared_sse(a: &[f32], b: &[f32]) -> f32 {
        let len = a.len();
        let mut acc = _mm_setzero_ps();
        let mut i = 0;
        while i + 4 <= len {
            let d = _mm_sub_ps(_mm_loadu_ps(&a[i]), _mm_loadu_ps(&b[i]));
            acc = _mm_add_ps(acc, _mm_mul_ps(d, d));
            i += 4;
        }
        let mut lanes = [0.0f32; 4];
        _mm_storeu_ps(lanes.as_mut_ptr(), acc);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            let d = a[i] - b[i];
            total += d * d;
            i += 1;
        }
        total
    }

    #[target_feature(enable = "sse2")]
    unsafe fn norm_squared_sse(v: &[f32]) -> f32 {
        let len = v.len();
        let mut acc = _mm_setzero_ps();
        let mut i = 0;
        while i + 4 <= len {
            let x = _mm_loadu_ps(&v[i]);
            acc = _mm_add_ps(acc, _mm_mul_ps(x, x));
            i += 4;
        }
        let mut lanes = [0.0f32; 4];
        _mm_storeu_ps(lanes.as_mut_ptr(), acc);
        let mut total: f32 = lanes.iter().sum();
        while i < len {
            total += v[i] * v[i];
            i += 1;
        }
        total
    }

    // ── Public API: thin dispatch over the kernels above ────────────

    #[inline]
    pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        match kernel() {
            // SAFETY: guarded by the same detection that selected the kernel.
            Kernel::Avx => unsafe { dot_avx(a, b) },
            Kernel::Sse => unsafe { dot_sse(a, b) },
            Kernel::Scalar => a.iter().zip(b).map(|(x, y)| x * y).sum(),
        }
    }

    #[inline]
    pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }
        match kernel() {
            Kernel::Avx => unsafe { euclidean_squared_avx(a, b) },
            Kernel::Sse => unsafe { euclidean_squared_sse(a, b) },
            Kernel::Scalar => a
                .iter()
                .zip(b)
                .map(|(x, y)| {
                    let d = x - y;
                    d * d
                })
                .sum(),
        }
    }

    #[inline]
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        euclidean_distance_squared(a, b).sqrt()
    }

    #[inline]
    pub fn euclidean_norm(v: &[f32]) -> f32 {
        match kernel() {
            Kernel::Avx => unsafe { norm_squared_avx(v).sqrt() },
            Kernel::Sse => unsafe { norm_squared_sse(v).sqrt() },
            Kernel::Scalar => v.iter().map(|x| x * x).sum::<f32>().sqrt(),
        }
    }

    #[inline]
    pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }
        match kernel() {
            Kernel::Avx => unsafe { manhattan_avx(a, b) },
            Kernel::Sse => unsafe { manhattan_sse(a, b) },
            Kernel::Scalar => a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum(),
        }
    }

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

    pub fn get_capabilities() -> super::SimdCapabilities {
        super::SimdCapabilities {
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

    /// No vector units are dispatched here; every kernel is scalar.
    pub fn active_kernel() -> &'static str {
        "scalar"
    }

    pub fn has_avx() -> bool { false }
    pub fn has_avx2() -> bool { false }
    pub fn has_fma() -> bool { false }
    pub fn has_sse() -> bool { false }

    pub fn get_capabilities() -> super::SimdCapabilities {
        super::SimdCapabilities {
            has_avx: false,
            has_avx2: false,
            has_fma: false,
            has_sse: false,
        }
    }
}

use serde::{Deserialize, Serialize};

/// CPU vector capabilities, for diagnostics and the INFO surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimdCapabilities {
    pub has_avx: bool,
    pub has_avx2: bool,
    pub has_fma: bool,
    pub has_sse: bool,
}

impl Default for SimdCapabilities {
    fn default() -> Self {
        simd_utils::get_capabilities()
    }
}

#[cfg(test)]
mod tests {
    use super::simd_utils::*;

    /// Every kernel must agree with a straightforward scalar reference to
    /// floating-point tolerance, at every awkward length (D2).
    fn reference_dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    fn reference_sq(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    fn reference_norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    fn data(dim: usize) -> (Vec<f32>, Vec<f32>) {
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        };
        (
            (0..dim).map(|_| next()).collect(),
            (0..dim).map(|_| next()).collect(),
        )
    }

    #[test]
    fn kernels_match_the_scalar_reference_at_every_length() {
        // Lengths around the lane widths, including odd tails.
        for dim in [0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 129] {
            let (a, b) = data(dim);
            let tolerance = 1e-4;

            let dot = dot_product(&a, &b);
            assert!(
                (dot - reference_dot(&a, &b)).abs() <= tolerance,
                "dot dim={dim}: {dot} vs {}",
                reference_dot(&a, &b)
            );

            let sq = euclidean_distance_squared(&a, &b);
            assert!(
                (sq - reference_sq(&a, &b)).abs() <= tolerance,
                "euclidean^2 dim={dim}: {sq} vs {}",
                reference_sq(&a, &b)
            );

            let manhattan = manhattan_distance(&a, &b);
            let reference_manhattan: f32 = a.iter().zip(&b).map(|(x, y)| (x - y).abs()).sum();
            assert!(
                (manhattan - reference_manhattan).abs() <= tolerance,
                "manhattan dim={dim}: {manhattan} vs {reference_manhattan}"
            );

            let norm = euclidean_norm(&a);
            assert!(
                (norm - reference_norm(&a)).abs() <= tolerance,
                "norm dim={dim}: {norm} vs {}",
                reference_norm(&a)
            );
        }
    }

    #[test]
    fn cosine_matches_the_reference_including_degenerate_vectors() {
        for dim in [1usize, 4, 8, 17, 64] {
            let (a, b) = data(dim);
            let dot = reference_dot(&a, &b);
            let expected = dot / (reference_norm(&a) * reference_norm(&b));
            let got = cosine_similarity(&a, &b);
            assert!((got - expected).abs() <= 1e-5, "cosine dim={dim}: {got} vs {expected}");
        }

        // A zero vector has no direction: 0, not NaN.
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]), 0.0);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), 0.0);
    }

    #[test]
    fn mismatched_lengths_report_infinite_distance() {
        // The guard is on the squared distance; `euclidean_distance` takes its
        // square root, so the sentinel arrives as sqrt(f32::MAX) — still
        // effectively "infinitely far", and larger than any real distance.
        assert_eq!(euclidean_distance_squared(&[1.0], &[1.0, 2.0]), f32::MAX);
        assert!(euclidean_distance(&[1.0], &[1.0, 2.0]) > 1e19);
        assert_eq!(manhattan_distance(&[1.0], &[1.0, 2.0]), f32::MAX);
        assert_eq!(dot_product(&[1.0], &[1.0, 2.0]), 0.0);
    }

    #[test]
    fn the_active_kernel_is_reported() {
        let name = active_kernel();
        assert!(
            ["avx+fma", "sse2", "scalar"].contains(&name),
            "unexpected kernel {name}"
        );
    }
}