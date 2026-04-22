// End-to-end fulltext query benchmarks.
//
// Measures throughput of inline `fulltext()` scoring through the full
// Fluree JSON-LD query engine with realistic paragraph-length documents.
//
// ## Scenarios
//
// 1. `fulltext_scan_all` — Score every `@fulltext` document against a query
//    using `fulltext(?content, "query")`. Pure single-property scan + BM25.
//
// 2. `fulltext_scan_filtered` — Filter docs by category first, then score
//    remaining docs. Measures graph-pattern filter + fulltext scoring.
//
// 3. `fulltext_scan_all_indexed` — Same as (1) but after building a binary
//    index (arena BM25 path instead of TF-saturation fallback).
//
// 4. `fulltext_scan_filtered_indexed` — Same as (2) after indexing.
//
// Each scenario runs at 1K, 5K, 10K, and 50K documents with paragraph-sized
// text (~30-60 words per document), targeting sub-1-second query time.
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench fulltext_query
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --bench fulltext_query -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use rand::prelude::*;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

use fluree_db_api::admin::ReindexOptions;

fn init_tracing_for_bench() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        if std::env::var("FLUREE_BENCH_TRACING").ok().as_deref() != Some("1") {
            return;
        }
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info");
        }
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_level(true)
            .try_init()
            .ok();
    });
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Articles per insert transaction.
const BATCH_SIZE: usize = 200;

/// Dataset sizes to benchmark.
const DATASET_SIZES: &[usize] = &[1_000, 5_000, 10_000, 50_000];

/// Categories for filtered queries — ~25% pass rate per category.
const CATEGORIES: &[&str] = &["science", "technology", "history", "culture"];

// ---------------------------------------------------------------------------
// Paragraph corpus — realistic short paragraphs (~30-60 words each)
// ---------------------------------------------------------------------------

/// Paragraph templates with domain vocabulary. Each has placeholders that get
/// filled with random terms to create unique documents.
const PARAGRAPH_TEMPLATES: &[&str] = &[
    "The rapid advancement of distributed database systems has fundamentally \
     transformed how organizations manage and query large-scale data. Modern \
     approaches leverage columnar storage, immutable ledgers, and semantic \
     graph models to achieve both performance and correctness guarantees \
     that were previously unattainable.",
    "Machine learning algorithms continue to reshape scientific research \
     across multiple disciplines. From protein folding predictions to climate \
     modeling, neural networks provide powerful tools for pattern recognition \
     in complex datasets that defy traditional analytical methods.",
    "Sustainable energy infrastructure requires careful integration of \
     renewable sources with existing power grids. Battery storage technology, \
     smart grid management, and demand response systems form the backbone \
     of modern energy transition strategies in urban environments.",
    "The evolution of programming languages reflects changing priorities \
     in software engineering. Memory safety, concurrency primitives, and \
     type system expressiveness have become critical design considerations \
     as systems grow more complex and security threats intensify.",
    "Quantum computing research has reached an inflection point where \
     practical applications begin to emerge alongside theoretical advances. \
     Error correction techniques and hybrid classical-quantum algorithms \
     show promise for optimization problems in logistics and cryptography.",
    "Urban planning in the twenty-first century must balance population \
     growth with environmental sustainability. Mixed-use development, \
     public transit investment, and green infrastructure provide frameworks \
     for creating resilient cities that serve diverse communities.",
    "Genomic medicine is revolutionizing healthcare through personalized \
     treatment protocols based on individual genetic profiles. Advances in \
     sequencing technology and bioinformatics tools enable clinicians to \
     identify disease markers and therapeutic targets with unprecedented precision.",
    "The intersection of artificial intelligence and natural language \
     processing has produced remarkable advances in text understanding. \
     Large language models demonstrate emergent capabilities in reasoning, \
     summarization, and knowledge synthesis across diverse domains.",
    "Ocean conservation efforts increasingly rely on satellite monitoring \
     and underwater sensor networks to track marine ecosystem health. \
     Real-time data collection enables rapid response to pollution events \
     and supports evidence-based fishery management policies.",
    "Cybersecurity frameworks must evolve continuously to address emerging \
     threat vectors in cloud-native architectures. Zero trust principles, \
     supply chain verification, and automated incident response form the \
     foundation of modern defensive security postures.",
    "Archaeological discoveries continue to reshape our understanding of \
     ancient civilizations and their technological achievements. Advanced \
     imaging techniques and isotope analysis reveal migration patterns, \
     trade networks, and cultural exchanges spanning millennia.",
    "The global semiconductor industry faces unprecedented demand driven \
     by artificial intelligence workloads and Internet of Things devices. \
     Advanced fabrication processes at nanometer scales push the boundaries \
     of materials science and precision manufacturing.",
    "Blockchain technology extends beyond cryptocurrency to enable verifiable \
     credentials, supply chain transparency, and decentralized governance. \
     Immutable ledger architectures provide audit trails and trust frameworks \
     for multi-party transactions without centralized intermediaries.",
    "Climate science models integrate atmospheric, oceanic, and terrestrial \
     data to project future environmental conditions with increasing accuracy. \
     Ensemble methods and high-resolution simulations help policymakers \
     understand risks and plan adaptation strategies.",
    "Robotic systems in healthcare settings assist surgeons with precision \
     procedures and support rehabilitation through adaptive therapy programs. \
     Advances in haptic feedback and computer vision enable safer and more \
     effective human-robot collaboration in clinical environments.",
    "Digital humanities scholarship applies computational methods to literary, \
     historical, and cultural analysis. Text mining, network visualization, \
     and geospatial mapping tools reveal patterns in archives and collections \
     that would be impossible to identify through manual review alone.",
];

/// Extra vocabulary injected randomly to ensure document uniqueness.
const EXTRA_VOCAB: &[&str] = &[
    "performance",
    "optimization",
    "distributed",
    "concurrent",
    "scalable",
    "efficient",
    "robust",
    "innovative",
    "comprehensive",
    "fundamental",
    "architecture",
    "infrastructure",
    "methodology",
    "implementation",
    "evaluation",
    "framework",
    "algorithm",
    "protocol",
    "specification",
    "integration",
    "verification",
    "validation",
    "deployment",
    "monitoring",
    "analysis",
    "synthesis",
    "transformation",
    "processing",
    "computation",
    "visualization",
    "simulation",
    "approximation",
    "calibration",
    "aggregation",
];

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type BenchFluree = fluree_db_api::Fluree;
type BenchLedger = fluree_db_api::LedgerState;

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// Generate a unique paragraph by combining a template with random extra words.
fn random_paragraph(rng: &mut impl Rng) -> String {
    let template = PARAGRAPH_TEMPLATES[rng.gen_range(0..PARAGRAPH_TEMPLATES.len())];
    // Append 2-4 random vocabulary words to make each doc unique
    let n_extra = rng.gen_range(2..=4);
    let extras: Vec<&str> = (0..n_extra)
        .map(|_| EXTRA_VOCAB[rng.gen_range(0..EXTRA_VOCAB.len())])
        .collect();
    format!("{} Keywords: {}.", template, extras.join(", "))
}

/// Create Fluree instance, insert `n_docs` @fulltext documents, return
/// everything needed to run benchmark queries.
async fn setup_dataset(n_docs: usize) -> (BenchFluree, BenchLedger, JsonValue, JsonValue, usize) {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = format!("bench/ft-{n_docs}:main");
    let mut ledger = fluree.create_ledger(&alias).await.unwrap();

    let mut rng = StdRng::seed_from_u64(42);

    let ctx = json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "ex": "http://example.org/ns/",
        "f": "https://ns.flur.ee/db#"
    });

    // Large novelty limit to avoid backpressure during bulk setup.
    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let mut n_science = 0usize;
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for i in 0..n_docs {
        let content = random_paragraph(&mut rng);
        let category = CATEGORIES[rng.gen_range(0..CATEGORIES.len())];
        if category == "science" {
            n_science += 1;
        }

        batch.push(json!({
            "@id": format!("ex:doc-{}", i),
            "@type": "ex:Document",
            "ex:title": format!("Document {}", i),
            "ex:content": {"@value": content, "@type": "@fulltext"},
            "ex:category": category
        }));

        if batch.len() >= BATCH_SIZE || i == n_docs - 1 {
            let txn = json!({
                "@context": ctx,
                "@graph": batch
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_config,
                )
                .await
                .unwrap();
            ledger = result.ledger;
            batch = Vec::with_capacity(BATCH_SIZE);
        }
    }

    // -----------------------------------------------------------------------
    // Query 1: Score ALL @fulltext docs (single-property scan + BM25)
    // -----------------------------------------------------------------------
    let query_all = json!({
        "@context": ctx,
        "select": ["?doc", "?score"],
        "where": [
            {"@id": "?doc", "ex:content": "?content"},
            ["bind", "?score", "(fulltext ?content \"distributed database systems performance\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    // -----------------------------------------------------------------------
    // Query 2: Filter by category FIRST, then score remaining docs
    // -----------------------------------------------------------------------
    let query_filtered = json!({
        "@context": ctx,
        "select": ["?doc", "?score"],
        "where": [
            {"@id": "?doc", "ex:content": "?content", "ex:category": "?cat"},
            ["filter", "(= ?cat \"science\")"],
            ["bind", "?score", "(fulltext ?content \"machine learning neural networks research\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    (fluree, ledger, query_all, query_filtered, n_science)
}

/// Like `setup_dataset` but also builds a binary index (arena BM25 path).
async fn setup_dataset_indexed(
    n_docs: usize,
) -> (
    BenchFluree,
    fluree_db_api::view::GraphDb,
    JsonValue,
    JsonValue,
    usize,
) {
    let (fluree, _ledger, query_all, query_filtered, n_science) = setup_dataset(n_docs).await;

    let ledger_id = format!("bench/ft-{n_docs}:main");

    fluree
        .reindex(&ledger_id, ReindexOptions::default())
        .await
        .unwrap();

    let view = fluree.db(&ledger_id).await.unwrap();

    (fluree, view, query_all, query_filtered, n_science)
}

// ---------------------------------------------------------------------------
// Benchmark: scan all docs (novelty-only, TF-saturation fallback)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_all(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_all");
    group.sample_size(10);

    for &n in DATASET_SIZES {
        eprintln!("  [setup] Inserting {n} @fulltext docs...");
        let (fluree, ledger, query, _, _) = rt.block_on(setup_dataset(n));
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| rt.block_on(async { black_box(fluree.query(&db, &query).await.unwrap()) }));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: scan all docs (indexed, arena BM25)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_all_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_all_indexed");
    group.sample_size(10);

    for &n in DATASET_SIZES {
        eprintln!("  [setup] Inserting {n} @fulltext docs + building index...");
        let (_fluree, snapshot, query, _, _) = rt.block_on(setup_dataset_indexed(n));

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(async { black_box(_fluree.query(&snapshot, &query).await.unwrap()) })
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: filter by category, then score (novelty-only)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_filtered(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_filtered");
    group.sample_size(10);

    for &n in DATASET_SIZES {
        eprintln!("  [setup] Inserting {n} @fulltext docs...");
        let (fluree, ledger, _, query, n_science) = rt.block_on(setup_dataset(n));
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        eprintln!(
            "  {} of {} docs pass category filter (~{:.0}%)",
            n_science,
            n,
            n_science as f64 / n as f64 * 100.0
        );

        group.throughput(Throughput::Elements(n_science as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_science),
            &n,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async { black_box(fluree.query(&db, &query).await.unwrap()) })
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: filter by category, then score (indexed, arena BM25)
// ---------------------------------------------------------------------------

fn bench_fulltext_scan_filtered_indexed(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fulltext_scan_filtered_indexed");
    group.sample_size(10);

    for &n in DATASET_SIZES {
        eprintln!("  [setup] Inserting {n} @fulltext docs + building index...");
        let (_fluree, snapshot, _, query, n_science) = rt.block_on(setup_dataset_indexed(n));
        eprintln!(
            "  {} of {} docs pass category filter (~{:.0}%)",
            n_science,
            n,
            n_science as f64 / n as f64 * 100.0
        );

        group.throughput(Throughput::Elements(n_science as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{n}_total"), n_science),
            &n,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        black_box(_fluree.query(&snapshot, &query).await.unwrap())
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_fulltext_scan_all,
    bench_fulltext_scan_filtered,
    bench_fulltext_scan_all_indexed,
    bench_fulltext_scan_filtered_indexed,
);
criterion_main!(benches);
