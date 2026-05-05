//! Fast vector math helpers (scalar + SIMD runtime dispatch).
//!
//! Used by filter functions like `dotProduct`, `cosineSimilarity`, and `euclideanDistance`.
//!
//! Design goals:
//! - **No user configuration**: runtime dispatch uses SIMD when available.
//! - **Portable**: scalar fallback on all platforms.
//! - **Safe call sites**: SIMD functions are `unsafe` + guarded by feature detection.

/// Type alias for SIMD kernel function pointers (f64).
#[cfg(target_arch = "x86_64")]
type SimdKernelF64 = unsafe fn(&[f64], &[f64]) -> f64;

/// Type alias for SIMD kernel function pointers (f32).
#[cfg(target_arch = "x86_64")]
type SimdKernelF32 = unsafe fn(&[f32], &[f32]) -> f32;

/// Below this length, scalar tends to win (dispatch/reduction overhead dominates).
///
/// This threshold is intentionally conservative; tune with real workloads.
const SIMD_LEN_THRESHOLD: usize = 256;

#[inline]
pub fn dot_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len());

    if a.len() < SIMD_LEN_THRESHOLD {
        return dot_f64_scalar(a, b);
    }

    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;

        // Cache dispatch once per process (no user flags).
        static DOT_KERNEL: OnceLock<SimdKernelF64> = OnceLock::new();
        let f = *DOT_KERNEL.get_or_init(|| {
            // AVX (not AVX2) is sufficient for f64 mul/add.
            if std::arch::is_x86_feature_detected!("avx") {
                dot_f64_avx
            } else {
                // SSE2 is baseline on x86_64.
                dot_f64_sse2
            }
        });
        // SAFETY: DOT_KERNEL only stores functions whose target_feature
        // requirements were checked at init time.
        unsafe { f(a, b) }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // NEON/ASIMD is baseline on aarch64.
        unsafe { dot_f64_neon(a, b) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    dot_f64_scalar(a, b)
}

#[inline]
pub fn l2_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len());

    if a.len() < SIMD_LEN_THRESHOLD {
        return l2_f64_scalar(a, b);
    }

    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;

        static L2_KERNEL: OnceLock<SimdKernelF64> = OnceLock::new();
        let f = *L2_KERNEL.get_or_init(|| {
            if std::arch::is_x86_feature_detected!("avx") {
                l2_f64_avx
            } else {
                l2_f64_sse2
            }
        });
        unsafe { f(a, b) }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { l2_f64_neon(a, b) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    l2_f64_scalar(a, b)
}

#[inline]
pub fn cosine_f64(a: &[f64], b: &[f64]) -> Option<f64> {
    debug_assert_eq!(a.len(), b.len());

    // We compute dot + squared magnitudes in a single pass for cache efficiency.
    let (dot, mag_a2, mag_b2) = dot_mag2_f64(a, b);
    if mag_a2 == 0.0 || mag_b2 == 0.0 {
        None
    } else {
        Some(dot / (mag_a2.sqrt() * mag_b2.sqrt()))
    }
}

#[inline]
fn dot_mag2_f64(a: &[f64], b: &[f64]) -> (f64, f64, f64) {
    debug_assert_eq!(a.len(), b.len());

    // SIMD variants could be added, but start with a scalar single-pass loop.
    // This already saves an extra pass vs separate dot + norm loops.
    let mut dot = 0.0;
    let mut mag_a2 = 0.0;
    let mut mag_b2 = 0.0;
    for i in 0..a.len() {
        let x = a[i];
        let y = b[i];
        dot += x * y;
        mag_a2 += x * x;
        mag_b2 += y * y;
    }
    (dot, mag_a2, mag_b2)
}

#[inline]
fn dot_f64_scalar(a: &[f64], b: &[f64]) -> f64 {
    let mut acc = 0.0;
    for i in 0..a.len() {
        acc += a[i] * b[i];
    }
    acc
}

#[inline]
fn l2_f64_scalar(a: &[f64], b: &[f64]) -> f64 {
    let mut acc = 0.0;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        acc += d * d;
    }
    acc.sqrt()
}

// =============================================================================
// x86_64 SIMD
// =============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn dot_f64_sse2(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut acc = _mm_setzero_pd();
    let mut i = 0usize;
    let n = a.len();
    while i + 2 <= n {
        let va = _mm_loadu_pd(a.as_ptr().add(i));
        let vb = _mm_loadu_pd(b.as_ptr().add(i));
        acc = _mm_add_pd(acc, _mm_mul_pd(va, vb));
        i += 2;
    }

    // Horizontal sum
    let mut tmp = [0f64; 2];
    _mm_storeu_pd(tmp.as_mut_ptr(), acc);
    let mut sum = tmp[0] + tmp[1];

    // Tail
    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_f64_avx(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut acc = _mm256_setzero_pd();
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = _mm256_loadu_pd(a.as_ptr().add(i));
        let vb = _mm256_loadu_pd(b.as_ptr().add(i));
        acc = _mm256_add_pd(acc, _mm256_mul_pd(va, vb));
        i += 4;
    }

    // Horizontal sum of 4 lanes.
    let hi = _mm256_extractf128_pd(acc, 1);
    let lo = _mm256_castpd256_pd128(acc);
    let sum2 = _mm_add_pd(lo, hi);
    let mut tmp = [0f64; 2];
    _mm_storeu_pd(tmp.as_mut_ptr(), sum2);
    let mut sum = tmp[0] + tmp[1];

    // Tail
    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn l2_f64_sse2(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut acc = _mm_setzero_pd();
    let mut i = 0usize;
    let n = a.len();
    while i + 2 <= n {
        let va = _mm_loadu_pd(a.as_ptr().add(i));
        let vb = _mm_loadu_pd(b.as_ptr().add(i));
        let d = _mm_sub_pd(va, vb);
        acc = _mm_add_pd(acc, _mm_mul_pd(d, d));
        i += 2;
    }

    let mut tmp = [0f64; 2];
    _mm_storeu_pd(tmp.as_mut_ptr(), acc);
    let mut sum = tmp[0] + tmp[1];

    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }

    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn l2_f64_avx(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let mut acc = _mm256_setzero_pd();
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = _mm256_loadu_pd(a.as_ptr().add(i));
        let vb = _mm256_loadu_pd(b.as_ptr().add(i));
        let d = _mm256_sub_pd(va, vb);
        acc = _mm256_add_pd(acc, _mm256_mul_pd(d, d));
        i += 4;
    }

    let hi = _mm256_extractf128_pd(acc, 1);
    let lo = _mm256_castpd256_pd128(acc);
    let sum2 = _mm_add_pd(lo, hi);
    let mut tmp = [0f64; 2];
    _mm_storeu_pd(tmp.as_mut_ptr(), sum2);
    let mut sum = tmp[0] + tmp[1];

    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }

    sum.sqrt()
}

// =============================================================================
// aarch64 SIMD (NEON)
// =============================================================================

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_f64_neon(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::aarch64::*;

    let mut acc = vdupq_n_f64(0.0);
    let mut i = 0usize;
    let n = a.len();
    while i + 2 <= n {
        let va = vld1q_f64(a.as_ptr().add(i));
        let vb = vld1q_f64(b.as_ptr().add(i));
        acc = vaddq_f64(acc, vmulq_f64(va, vb));
        i += 2;
    }

    let mut sum = vaddvq_f64(acc);
    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_f64_neon(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::aarch64::*;

    let mut acc = vdupq_n_f64(0.0);
    let mut i = 0usize;
    let n = a.len();
    while i + 2 <= n {
        let va = vld1q_f64(a.as_ptr().add(i));
        let vb = vld1q_f64(b.as_ptr().add(i));
        let d = vsubq_f64(va, vb);
        acc = vaddq_f64(acc, vmulq_f64(d, d));
        i += 2;
    }

    let mut sum = vaddvq_f64(acc);
    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }
    sum.sqrt()
}

// =============================================================================
// f32 variants — 2x SIMD throughput vs f64 on same register width
// =============================================================================

/// Below this length, scalar tends to win for f32 (same rationale as f64).
const SIMD_LEN_THRESHOLD_F32: usize = 128;

#[inline]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    if a.len() < SIMD_LEN_THRESHOLD_F32 {
        return dot_f32_scalar(a, b);
    }

    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;

        static DOT_F32_KERNEL: OnceLock<SimdKernelF32> = OnceLock::new();
        let f = *DOT_F32_KERNEL.get_or_init(|| {
            if std::arch::is_x86_feature_detected!("avx") {
                dot_f32_avx
            } else {
                dot_f32_sse
            }
        });
        unsafe { f(a, b) }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { dot_f32_neon(a, b) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    dot_f32_scalar(a, b)
}

#[inline]
pub fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    if a.len() < SIMD_LEN_THRESHOLD_F32 {
        return l2_f32_scalar(a, b);
    }

    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;

        static L2_F32_KERNEL: OnceLock<SimdKernelF32> = OnceLock::new();
        let f = *L2_F32_KERNEL.get_or_init(|| {
            if std::arch::is_x86_feature_detected!("avx") {
                l2_f32_avx
            } else {
                l2_f32_sse
            }
        });
        unsafe { f(a, b) }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { l2_f32_neon(a, b) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    l2_f32_scalar(a, b)
}

#[inline]
pub fn cosine_f32(a: &[f32], b: &[f32]) -> Option<f32> {
    debug_assert_eq!(a.len(), b.len());

    let (dot, mag_a2, mag_b2) = dot_mag2_f32(a, b);
    if mag_a2 == 0.0 || mag_b2 == 0.0 {
        None
    } else {
        Some(dot / (mag_a2.sqrt() * mag_b2.sqrt()))
    }
}

#[inline]
fn dot_mag2_f32(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    debug_assert_eq!(a.len(), b.len());

    let mut dot = 0.0f32;
    let mut mag_a2 = 0.0f32;
    let mut mag_b2 = 0.0f32;
    for i in 0..a.len() {
        let x = a[i];
        let y = b[i];
        dot += x * y;
        mag_a2 += x * x;
        mag_b2 += y * y;
    }
    (dot, mag_a2, mag_b2)
}

#[inline]
fn dot_f32_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0f32;
    for i in 0..a.len() {
        acc += a[i] * b[i];
    }
    acc
}

#[inline]
fn l2_f32_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        acc += d * d;
    }
    acc.sqrt()
}

// f32 x86_64 SIMD

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn dot_f32_sse(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let mut acc = _mm_setzero_ps();
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = _mm_loadu_ps(a.as_ptr().add(i));
        let vb = _mm_loadu_ps(b.as_ptr().add(i));
        acc = _mm_add_ps(acc, _mm_mul_ps(va, vb));
        i += 4;
    }

    // Horizontal sum of 4 lanes
    // NOTE: avoid SSE3 intrinsics so this is safe on baseline x86_64.
    let tmp = _mm_movehl_ps(acc, acc); // [a2, a3, a2, a3]
    let sum2 = _mm_add_ps(acc, tmp); // [a0+a2, a1+a3, ..]
    let tmp2 = _mm_shuffle_ps(sum2, sum2, 0x55); // lane1 replicated
    let sum1 = _mm_add_ss(sum2, tmp2); // (a0+a2) + (a1+a3)
    let mut sum = _mm_cvtss_f32(sum1);

    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_f32_avx(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let mut acc = _mm256_setzero_ps();
    let mut i = 0usize;
    let n = a.len();
    while i + 8 <= n {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        acc = _mm256_add_ps(acc, _mm256_mul_ps(va, vb));
        i += 8;
    }

    // Horizontal sum: reduce 8 → 4 → scalar
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let sum4 = _mm_add_ps(lo, hi);
    let tmp = _mm_movehl_ps(sum4, sum4);
    let sum2 = _mm_add_ps(sum4, tmp);
    let tmp2 = _mm_shuffle_ps(sum2, sum2, 0x55);
    let sum1 = _mm_add_ss(sum2, tmp2);
    let mut sum = _mm_cvtss_f32(sum1);

    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn l2_f32_sse(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let mut acc = _mm_setzero_ps();
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = _mm_loadu_ps(a.as_ptr().add(i));
        let vb = _mm_loadu_ps(b.as_ptr().add(i));
        let d = _mm_sub_ps(va, vb);
        acc = _mm_add_ps(acc, _mm_mul_ps(d, d));
        i += 4;
    }

    let tmp = _mm_movehl_ps(acc, acc);
    let sum2 = _mm_add_ps(acc, tmp);
    let tmp2 = _mm_shuffle_ps(sum2, sum2, 0x55);
    let sum1 = _mm_add_ss(sum2, tmp2);
    let mut sum = _mm_cvtss_f32(sum1);

    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }
    sum.sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn l2_f32_avx(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let mut acc = _mm256_setzero_ps();
    let mut i = 0usize;
    let n = a.len();
    while i + 8 <= n {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        let d = _mm256_sub_ps(va, vb);
        acc = _mm256_add_ps(acc, _mm256_mul_ps(d, d));
        i += 8;
    }

    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let sum4 = _mm_add_ps(lo, hi);
    let tmp = _mm_movehl_ps(sum4, sum4);
    let sum2 = _mm_add_ps(sum4, tmp);
    let tmp2 = _mm_shuffle_ps(sum2, sum2, 0x55);
    let sum1 = _mm_add_ss(sum2, tmp2);
    let mut sum = _mm_cvtss_f32(sum1);

    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }
    sum.sqrt()
}

// f32 aarch64 SIMD (NEON)

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_f32_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let mut acc = vdupq_n_f32(0.0);
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        acc = vaddq_f32(acc, vmulq_f32(va, vb));
        i += 4;
    }

    let mut sum = vaddvq_f32(acc);
    while i < n {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_f32_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let mut acc = vdupq_n_f32(0.0);
    let mut i = 0usize;
    let n = a.len();
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        let d = vsubq_f32(va, vb);
        acc = vaddq_f32(acc, vmulq_f32(d, d));
        i += 4;
    }

    let mut sum = vaddvq_f32(acc);
    while i < n {
        let d = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += d * d;
        i += 1;
    }
    sum.sqrt()
}

// =============================================================================
// Tests (scalar correctness)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_matches_scalar() {
        let a = vec![0.6, 0.5, -1.0, 2.0, 3.25];
        let b = vec![0.7, -0.25, 4.0, 1.5, 0.5];
        let expected = dot_f64_scalar(&a, &b);
        let got = dot_f64(&a, &b);
        assert!(
            (got - expected).abs() < 1e-12,
            "got {got}, expected {expected}"
        );
    }

    #[test]
    fn l2_matches_scalar() {
        let a = vec![0.6, 0.5, -1.0, 2.0, 3.25];
        let b = vec![0.7, -0.25, 4.0, 1.5, 0.5];
        let expected = l2_f64_scalar(&a, &b);
        let got = l2_f64(&a, &b);
        assert!(
            (got - expected).abs() < 1e-12,
            "got {got}, expected {expected}"
        );
    }

    #[test]
    fn cosine_handles_zero_vectors() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 2.0];
        assert_eq!(cosine_f64(&a, &b), None);
    }

    // f32 tests

    #[test]
    fn dot_f32_matches_scalar() {
        let a = vec![0.6f32, 0.5, -1.0, 2.0, 3.25];
        let b = vec![0.7f32, -0.25, 4.0, 1.5, 0.5];
        let expected = dot_f32_scalar(&a, &b);
        let got = dot_f32(&a, &b);
        assert!(
            (got - expected).abs() < 1e-5,
            "got {got}, expected {expected}"
        );
    }

    #[test]
    fn l2_f32_matches_scalar() {
        let a = vec![0.6f32, 0.5, -1.0, 2.0, 3.25];
        let b = vec![0.7f32, -0.25, 4.0, 1.5, 0.5];
        let expected = l2_f32_scalar(&a, &b);
        let got = l2_f32(&a, &b);
        assert!(
            (got - expected).abs() < 1e-5,
            "got {got}, expected {expected}"
        );
    }

    #[test]
    fn cosine_f32_handles_zero_vectors() {
        let a = vec![0.0f32, 0.0];
        let b = vec![1.0f32, 2.0];
        assert_eq!(cosine_f32(&a, &b), None);
    }

    #[test]
    fn cosine_f32_unit_vectors_match_dot() {
        // For unit vectors, cosine = dot product
        let inv_sqrt2 = 1.0f32 / 2.0f32.sqrt();
        let a = vec![inv_sqrt2, inv_sqrt2];
        let b = vec![1.0f32, 0.0];
        let cos = cosine_f32(&a, &b).unwrap();
        let dot = dot_f32(&a, &b);
        assert!((cos - dot).abs() < 1e-5, "cosine={cos}, dot={dot}");
    }

    #[test]
    fn dot_f32_large_vector() {
        // Test with a vector large enough to trigger SIMD path
        let n = 768;
        let a: Vec<f32> = (0..n).map(|i| (i as f32) * 0.001).collect();
        let b: Vec<f32> = (0..n).map(|i| ((n - i) as f32) * 0.001).collect();
        let expected = dot_f32_scalar(&a, &b);
        let got = dot_f32(&a, &b);
        assert!(
            (got - expected).abs() < 0.01,
            "got {got}, expected {expected}"
        );
    }
}
