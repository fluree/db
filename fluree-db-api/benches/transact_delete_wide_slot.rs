//! DELETE staging latency over one wide `(subject, predicate)` slot.
//!
//! One subject holds `n` values of one predicate, in a bulk-imported
//! (indexed) ledger, and the measured transaction deletes all of them. The
//! other delete benches spread their facts over many subjects, one value
//! per slot (`transact_filtered_delete`, `transact_upsert_replace`), so a
//! cost that grows with the values of one slot times the intents naming it
//! does not show there. This one exists for that shape: a resolver that
//! scans the slot once per intent turns the delete quadratic.
//!
//! ## Scenarios
//!
//! 1. `jsonld_delete_all` — a JSON-LD delete naming every value. Each value
//!    is an intent matched against the slot's stored facts.
//! 2. `delete_where_listed` — `where`/`delete` over `?o`, on a ledger that
//!    stores one `@list` elsewhere. The template is witnessed by the WHERE,
//!    and with a list stored in the ledger each row reads its slot for a
//!    list position, so every row is matched against the slot.
//!
//! Both assert in setup that the fixture is indexed and on the intended
//! side of the list gate.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → values in the slot
//!              (Tiny=1k, Small=5k, Medium=10k, Large=20k)
//!   metric:    ns/delete (criterion default; no Throughput, so a
//!              superlinear curve is not hidden behind a growing
//!              denominator)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench transact_delete_wide_slot
//!   cargo bench -p fluree-db-api --bench transact_delete_wide_slot -- --test
//!   FLUREE_BENCH_SCALE=large cargo bench -p fluree-db-api --bench transact_delete_wide_slot

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, LedgerState, TxnOpts};
use serde_json::json;
use std::io::Write;

/// Map BenchScale to the number of values in the slot.
fn scale_values(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 1_000,
        BenchScale::Small => 5_000,
        BenchScale::Medium => 10_000,
        BenchScale::Large => 20_000,
    }
}

const CTX_PREFIX: &str = "http://example.org/ns/";

/// The measured delete must run over the imported index, not fold into it.
fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

/// Turtle for the imported base: `ex:s ex:p` with `n` values, plus an
/// untouched fact, and with `with_list` one RDF collection elsewhere.
fn base_turtle(n: usize, with_list: bool) -> String {
    let mut out = String::with_capacity(n * 16 + 128);
    out.push_str("@prefix ex: <");
    out.push_str(CTX_PREFIX);
    out.push_str("> .\nex:s ex:name 'keep' .\n");
    for i in 0..n {
        out.push_str(&format!("ex:s ex:p 'v{i}' .\n"));
    }
    if with_list {
        out.push_str("ex:listed ex:items ( 'a' 'b' 'c' ) .\n");
    }
    out
}

/// A JSON-LD delete naming each of the `n` values.
fn delete_all(n: usize) -> serde_json::Value {
    let values: Vec<_> = (0..n).map(|i| json!(format!("v{i}"))).collect();
    json!({
        "@context": {"ex": CTX_PREFIX},
        "delete": {"@id": "ex:s", "ex:p": values}
    })
}

/// A delete of every value the WHERE binds: witnessed by its WHERE triple.
fn delete_where() -> serde_json::Value {
    json!({
        "@context": {"ex": CTX_PREFIX},
        "where": {"@id": "ex:s", "ex:p": "?o"},
        "delete": {"@id": "ex:s", "ex:p": "?o"}
    })
}

/// Everything one measured iteration consumes. The tempdirs ride along so
/// the file-backed store outlives the timed block.
struct Fixture {
    _db_dir: tempfile::TempDir,
    _data_dir: tempfile::TempDir,
    fluree: fluree_db_api::Fluree,
    ledger: LedgerState,
}

/// Bulk-import the slot. Asserts the fixture is indexed and on the
/// intended side of the list gate, so a fixture that drifted fails the
/// bench instead of measuring another path.
fn build_fixture(rt: &tokio::runtime::Runtime, ttl: &str, expect_list_meta: bool) -> Fixture {
    rt.block_on(async {
        let db_dir = tempfile::tempdir().expect("db tmpdir");
        let data_dir = tempfile::tempdir().expect("data tmpdir");
        let ttl_path = data_dir.path().join("base.ttl");
        let mut f = std::fs::File::create(&ttl_path).expect("create ttl");
        f.write_all(ttl.as_bytes()).expect("write ttl");
        drop(f);

        let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
            .build()
            .expect("build file-backed Fluree");
        let alias = next_ledger_alias("tdws");
        let result = fluree
            .create(&alias)
            .import(&ttl_path)
            .cleanup(false)
            .execute()
            .await
            .expect("bulk import");
        assert!(result.root_id.is_some(), "fixture needs an indexed base");
        let ledger = fluree.ledger(&alias).await.expect("load imported ledger");
        assert_eq!(
            ledger.snapshot.has_list_meta,
            Some(expect_list_meta),
            "fixture must land on the intended side of the list gate"
        );
        Fixture {
            _db_dir: db_dir,
            _data_dir: data_dir,
            fluree,
            ledger,
        }
    })
}

fn bench_transact_delete_wide_slot(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = scale_values(scale);

    let plain_ttl = base_turtle(n, false);
    let listed_ttl = base_turtle(n, true);
    eprintln!(
        "  [transact_delete_wide_slot] scale={} values={n}",
        scale.as_str()
    );

    let mut group = c.benchmark_group("transact_delete_wide_slot");
    group.sample_size(profile.sample_size());
    // The fixture is rebuilt per iteration because the measured op is
    // destructive; Flat keeps criterion from assuming a cheap routine.
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, ttl, has_lists, txn) in [
        ("jsonld_delete_all", &plain_ttl, false, delete_all(n)),
        ("delete_where_listed", &listed_ttl, true, delete_where()),
    ] {
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &n, |b, &n| {
            b.iter_batched(
                // Setup: import. NOT measured.
                || build_fixture(&rt, ttl, has_lists),
                // Measured: one delete of every value in the slot.
                |fixture| {
                    rt.block_on(async {
                        let result = fixture
                            .fluree
                            .update_with_opts(
                                fixture.ledger,
                                &txn,
                                TxnOpts::default(),
                                CommitOpts::default(),
                                &index_config(),
                            )
                            .await
                            .expect("delete");
                        assert_eq!(result.receipt.retract_count, n, "every value retracted");
                        black_box(result.ledger);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_transact_delete_wide_slot);
criterion_main!(benches);
