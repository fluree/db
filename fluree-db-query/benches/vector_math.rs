//! Benchmarks for vector math functions (dot product, L2 distance, cosine similarity).
//!
//! Compares:
//! - NEW: SIMD-accelerated functions from vector_math.rs
//! - OLD: Scalar baseline (prior implementation)
//!
//! Run with: cargo bench -p fluree-db-query --bench vector_math

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// Import the NEW SIMD vector math functions from the crate
use fluree_db_query::expression::vector_math::{cosine_f64, dot_f64, l2_f64};

// =============================================================================
// OLD scalar implementations (copied from prior commit for baseline comparison)
// =============================================================================

/// Old scalar dot product (prior implementation)
fn dot_scalar(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Old scalar L2 distance (prior implementation)
fn l2_scalar(a: &[f64], b: &[f64]) -> f64 {
    let sum_sq: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum();
    sum_sq.sqrt()
}

/// Old scalar cosine similarity (prior implementation - 3 separate passes)
fn cosine_scalar(a: &[f64], b: &[f64]) -> Option<f64> {
    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        None
    } else {
        Some(dot / (mag_a * mag_b))
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Generate random f64 vectors of given dimension (deterministic for reproducibility).
fn random_vectors(dim: usize) -> (Vec<f64>, Vec<f64>) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    let mut a = Vec::with_capacity(dim);
    let mut b = Vec::with_capacity(dim);

    for i in 0..dim {
        i.hash(&mut hasher);
        let h1 = hasher.finish();
        a.push((h1 as f64) / (u64::MAX as f64) * 2.0 - 1.0);

        (i + 1000).hash(&mut hasher);
        let h2 = hasher.finish();
        b.push((h2 as f64) / (u64::MAX as f64) * 2.0 - 1.0);
    }

    (a, b)
}

// =============================================================================
// Benchmarks: dot product
// =============================================================================

fn bench_dot_product(c: &mut Criterion) {
    let mut group = c.benchmark_group("dot_product");

    // Common embedding dimensions
    for dim in [128, 384, 768, 1536, 3072] {
        let (a, b) = random_vectors(dim);
        group.throughput(Throughput::Elements(dim as u64));

        // NEW: SIMD implementation
        group.bench_with_input(BenchmarkId::new("simd", dim), &dim, |bench, _| {
            bench.iter(|| dot_f64(black_box(&a), black_box(&b)));
        });

        // OLD: Scalar baseline
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| dot_scalar(black_box(&a), black_box(&b)));
        });
    }

    group.finish();
}

// =============================================================================
// Benchmarks: L2 distance
// =============================================================================

fn bench_l2_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("l2_distance");

    for dim in [128, 384, 768, 1536, 3072] {
        let (a, b) = random_vectors(dim);
        group.throughput(Throughput::Elements(dim as u64));

        // NEW: SIMD implementation
        group.bench_with_input(BenchmarkId::new("simd", dim), &dim, |bench, _| {
            bench.iter(|| l2_f64(black_box(&a), black_box(&b)));
        });

        // OLD: Scalar baseline
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| l2_scalar(black_box(&a), black_box(&b)));
        });
    }

    group.finish();
}

// =============================================================================
// Benchmarks: cosine similarity
// =============================================================================

fn bench_cosine_similarity(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_similarity");

    for dim in [128, 384, 768, 1536, 3072] {
        let (a, b) = random_vectors(dim);
        group.throughput(Throughput::Elements(dim as u64));

        // NEW: Single-pass implementation
        group.bench_with_input(BenchmarkId::new("single_pass", dim), &dim, |bench, _| {
            bench.iter(|| cosine_f64(black_box(&a), black_box(&b)));
        });

        // OLD: Three-pass scalar baseline
        group.bench_with_input(BenchmarkId::new("three_pass", dim), &dim, |bench, _| {
            bench.iter(|| cosine_scalar(black_box(&a), black_box(&b)));
        });
    }

    group.finish();
}

// =============================================================================
// Benchmarks: batch ranking (realistic workload)
// =============================================================================

fn bench_batch_ranking(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_ranking");

    let dim = 768; // Common embedding size (BERT-like)
    let n_vectors = 1000;

    let (query, _) = random_vectors(dim);
    let corpus: Vec<Vec<f64>> = (0..n_vectors)
        .map(|i| {
            let (v, _) = random_vectors(dim);
            v.into_iter()
                .enumerate()
                .map(|(j, x)| x + (i as f64 * 0.0001) + (j as f64 * 0.00001))
                .collect()
        })
        .collect();

    group.throughput(Throughput::Elements(n_vectors as u64));

    // Dot product comparison
    group.bench_function("dot_simd_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| dot_f64(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    group.bench_function("dot_scalar_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| dot_scalar(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    // Cosine comparison
    group.bench_function("cosine_single_pass_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| cosine_f64(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    group.bench_function("cosine_three_pass_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| cosine_scalar(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    // L2 comparison
    group.bench_function("l2_simd_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| l2_f64(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    group.bench_function("l2_scalar_1000", |bench| {
        bench.iter(|| {
            corpus
                .iter()
                .map(|v| l2_scalar(black_box(&query), black_box(v)))
                .collect::<Vec<_>>()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_dot_product,
    bench_l2_distance,
    bench_cosine_similarity,
    bench_batch_ranking,
);
criterion_main!(benches);
