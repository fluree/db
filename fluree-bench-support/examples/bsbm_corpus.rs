//! Emit a BSBM-shape Turtle corpus for the Tier-2 conversion matrix.
//!
//! A second SHAPE, not a second size. The synthetic corpus is uniform,
//! subject-major and single-prefix, which flatters a parser; BSBM's four
//! interlinked entity types with mixed datatypes and a wider predicate
//! vocabulary exercise a different path. An ordering that survives both is
//! worth more than one measured twice.
//!
//! Turtle ONLY, deliberately. The generator has no native N-Triples emitter,
//! and producing the NT half by converting this file with our own writer would
//! benchmark every tool on input shaped by the tool under test — the exact
//! circularity that removed RiverBench's Turtle column from the corpus set.
//! The matrix prints N/A with that reason instead.
//!
//! Usage: cargo run --release -p fluree-bench-support --example bsbm_corpus -- <n_products> <out.ttl>

fn main() {
    let mut args = std::env::args().skip(1);
    let n_products: usize = args
        .next()
        .and_then(|a| a.parse().ok())
        .expect("usage: bsbm_corpus <n_products> <out.ttl>");
    let out = args
        .next()
        .expect("usage: bsbm_corpus <n_products> <out.ttl>");

    let data = fluree_bench_support::gen::bsbm::generate_dataset(n_products);
    let turtle = fluree_bench_support::gen::bsbm::bsbm_data_to_turtle(&data);
    std::fs::write(&out, turtle).expect("write corpus");

    eprintln!("wrote {out} ({} products)", n_products);
}
