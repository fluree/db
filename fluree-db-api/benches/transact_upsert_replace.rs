//! Upsert staging latency when the payload replaces values the ledger holds.
//!
//! An upsert retracts every current value of each `(graph, subject,
//! predicate)` its payload names, then asserts the payload. That retraction
//! wave runs on every upsert of an existing entity, and nothing else in the
//! suite exercises it: `insert_formats` upserts only new subjects and
//! `transact_commit` measures inserts.
//!
//! ## Scenarios
//!
//! Each scenario upserts one payload into a freshly built ledger. N comes
//! from the scale (below); every subject named is replaced unless stated.
//!
//! 1. `default_novelty` — N subjects × 1 value, default graph, nothing
//!    indexed.
//! 2. `default_indexed` — the same, indexed, novelty empty.
//! 3. `named_novelty` — the same in a named graph, nothing indexed. The
//!    lane that walked the graph's whole novelty once per slot.
//! 4. `named_indexed` — the named graph, indexed.
//! 5. `lang_list` — each subject holds two language-tagged values and a
//!    two-entry `@list`, indexed; the payload replaces both predicates.
//!    Language tags and list positions are exactly what the wave must
//!    retract as stored.
//! 6. `wide_predicates` — N/4 subjects × 16 predicates, indexed: the
//!    per-slot cost shows up here, where one subject costs 16 lookups.
//! 7. `novelty_heavy` — the indexed base of (2) plus 4×N unrelated
//!    novelty flakes. A lookup that translates or walks the whole overlay
//!    per call (the #1722 floor) turns into a slope here.
//! 8. `new_subjects` — the payload names N subjects the ledger has never
//!    seen: the #1549 absence skip, which must stay flat.
//!
//! Every scenario asserts in setup that it reached the lane it names
//! (indexed or not, graph registered, novelty size), and after each
//! measured upsert that the receipt's retract/assert counts are the ones
//! the scenario implies. A fixture that drifts fails instead of reporting
//! a number for the wrong path.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → N (Tiny=500, Small=2k, Medium=8k, Large=24k)
//!   metric:    ns/upsert (criterion default; no Throughput — the guard is
//!              on the curve, like `transact_filtered_delete`)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench transact_upsert_replace
//!   cargo bench -p fluree-db-api --bench transact_upsert_replace -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench transact_upsert_replace
//!
//! ## Cargo.toml + budget already wired (see fluree-db-api/Cargo.toml,
//! regression-budget.json).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, IndexConfig, LedgerState, ReindexOptions, TxnOpts,
};
use serde_json::{json, Value as JsonValue};

/// Map BenchScale to N, the number of subjects the upsert replaces.
fn scale_n(scale: BenchScale) -> usize {
    match scale {
        // Keep tiny tiny so PR-gated runs finish quickly.
        BenchScale::Tiny => 500,
        BenchScale::Small => 2_000,
        BenchScale::Medium => 8_000,
        BenchScale::Large => 24_000,
    }
}

const CTX_PREFIX: &str = "http://example.org/ns/";
const GRAPH: &str = "http://example.org/ns/bench-graph";
/// Predicates per subject in `wide_predicates`.
const WIDE: usize = 16;
/// Subjects per base-load commit.
const BATCH: usize = 500;

/// Novelty must survive the fixture: a reindex would fold it into the base.
fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

fn ctx() -> JsonValue {
    json!({"ex": CTX_PREFIX})
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    DefaultNovelty,
    DefaultIndexed,
    NamedNovelty,
    NamedIndexed,
    LangList,
    WidePredicates,
    NoveltyHeavy,
    NewSubjects,
}

impl Scenario {
    const ALL: [Scenario; 8] = [
        Scenario::DefaultNovelty,
        Scenario::DefaultIndexed,
        Scenario::NamedNovelty,
        Scenario::NamedIndexed,
        Scenario::LangList,
        Scenario::WidePredicates,
        Scenario::NoveltyHeavy,
        Scenario::NewSubjects,
    ];

    fn name(self) -> &'static str {
        match self {
            Scenario::DefaultNovelty => "default_novelty",
            Scenario::DefaultIndexed => "default_indexed",
            Scenario::NamedNovelty => "named_novelty",
            Scenario::NamedIndexed => "named_indexed",
            Scenario::LangList => "lang_list",
            Scenario::WidePredicates => "wide_predicates",
            Scenario::NoveltyHeavy => "novelty_heavy",
            Scenario::NewSubjects => "new_subjects",
        }
    }

    fn indexed(self) -> bool {
        !matches!(self, Scenario::DefaultNovelty | Scenario::NamedNovelty)
    }

    fn graph(self) -> Option<&'static str> {
        match self {
            Scenario::NamedNovelty | Scenario::NamedIndexed => Some(GRAPH),
            _ => None,
        }
    }

    /// Subjects in the base (and, except for `new_subjects`, in the upsert).
    fn subjects(self, n: usize) -> usize {
        match self {
            Scenario::WidePredicates => (n / 4).max(1),
            _ => n,
        }
    }

    /// Retractions and assertions the measured upsert must commit.
    fn expected_counts(self, n: usize) -> (usize, usize) {
        let subjects = self.subjects(n);
        match self {
            // Two tagged values + two list entries out, one of each in.
            Scenario::LangList => (4 * subjects, 2 * subjects),
            Scenario::WidePredicates => (WIDE * subjects, WIDE * subjects),
            Scenario::NewSubjects => (0, subjects),
            _ => (subjects, subjects),
        }
    }
}

/// One node of the base load or of the measured upsert.
fn node(scenario: Scenario, i: usize, replacement: bool) -> JsonValue {
    let id = if scenario == Scenario::NewSubjects && replacement {
        format!("ex:new{i}")
    } else {
        format!("ex:s{i}")
    };
    let mut node = match scenario {
        Scenario::LangList if replacement => json!({
            "ex:label": {"@value": format!("b{i}"), "@language": "en"},
            "ex:items": {"@list": [format!("z{i}")]},
        }),
        Scenario::LangList => json!({
            // Distinct lexical forms per tag, so the count check below
            // reads the same whether or not the retractions keep the tag.
            "ex:label": [
                {"@value": format!("a{i}"), "@language": "en"},
                {"@value": format!("c{i}"), "@language": "fr"},
            ],
            "ex:items": {"@list": [format!("x{i}"), format!("y{i}")]},
        }),
        Scenario::WidePredicates => {
            let tag = if replacement { "n" } else { "o" };
            let mut obj = serde_json::Map::new();
            for k in 0..WIDE {
                obj.insert(format!("ex:p{k}"), json!(format!("{tag}{i}-{k}")));
            }
            JsonValue::Object(obj)
        }
        _ => {
            let tag = if replacement { "new" } else { "old" };
            json!({"ex:v": format!("{tag}{i}")})
        }
    };
    let obj = node.as_object_mut().expect("object");
    obj.insert("@id".to_string(), json!(id));
    if let Some(g) = scenario.graph() {
        obj.insert("@graph".to_string(), json!(g));
    }
    node
}

fn payload(nodes: Vec<JsonValue>) -> JsonValue {
    json!({"@context": ctx(), "@graph": nodes})
}

/// The measured upsert.
fn upsert_payload(scenario: Scenario, n: usize) -> JsonValue {
    payload(
        (0..scenario.subjects(n))
            .map(|i| node(scenario, i, true))
            .collect(),
    )
}

/// Everything one measured iteration consumes. The measured routine hands
/// the tempdir and the instance back to criterion, which drops them after
/// the clock stops: deleting the file-backed store costs more than the
/// upsert itself, and its cost follows the filesystem, not the code.
struct Fixture {
    db_dir: tempfile::TempDir,
    fluree: Fluree,
    ledger: LedgerState,
}

async fn insert_batches(
    fluree: &Fluree,
    mut ledger: LedgerState,
    count: usize,
    mut make: impl FnMut(usize) -> JsonValue,
) -> LedgerState {
    let mut from = 0usize;
    while from < count {
        let to = (from + BATCH).min(count);
        let r = fluree
            .insert_with_opts(
                ledger,
                &payload((from..to).map(&mut make).collect()),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config(),
            )
            .await
            .expect("fixture insert");
        ledger = r.ledger;
        from = to;
    }
    ledger
}

/// Load the base, index it when the scenario says so, and add the
/// unrelated novelty of `novelty_heavy`. Asserts the lane on the way out.
fn build_fixture(rt: &tokio::runtime::Runtime, scenario: Scenario, n: usize) -> Fixture {
    rt.block_on(async {
        let db_dir = tempfile::tempdir().expect("db tmpdir");
        let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
            .build()
            .expect("build file-backed Fluree");
        let alias = next_ledger_alias("tur");
        let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
        let mut ledger = insert_batches(&fluree, ledger, scenario.subjects(n), |i| {
            node(scenario, i, false)
        })
        .await;

        if scenario.indexed() {
            drop(ledger);
            fluree
                .reindex(&alias, ReindexOptions::default())
                .await
                .expect("reindex");
            ledger = fluree.ledger(&alias).await.expect("reload after reindex");
            assert!(
                ledger.snapshot.range_provider.is_some() && ledger.novelty.is_empty(),
                "{}: fixture needs an indexed base and empty novelty",
                scenario.name()
            );
        } else {
            assert!(
                ledger.snapshot.range_provider.is_none(),
                "{}: fixture must stay unindexed",
                scenario.name()
            );
        }

        if scenario == Scenario::NoveltyHeavy {
            ledger = insert_batches(
                &fluree,
                ledger,
                4 * n,
                |i| json!({"@id": format!("ex:u{i}"), "ex:w": format!("w{i}")}),
            )
            .await;
            assert!(
                ledger.novelty.len() >= 4 * n,
                "novelty_heavy: the unrelated novelty must survive the fixture"
            );
        }

        if let Some(g) = scenario.graph() {
            assert!(
                ledger.snapshot.graph_registry.graph_id_for_iri(g).is_some(),
                "{}: the named graph must be registered before the upsert",
                scenario.name()
            );
        }

        Fixture {
            db_dir,
            fluree,
            ledger,
        }
    })
}

fn bench_transact_upsert_replace(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = scale_n(scale);

    eprintln!("  [transact_upsert_replace] scale={} n={n}", scale.as_str());

    let mut group = c.benchmark_group("transact_upsert_replace");
    group.sample_size(profile.sample_size());
    // The fixture is rebuilt per iteration because the measured upsert
    // changes it; Flat keeps criterion from assuming a cheap, repeatable
    // routine.
    group.sampling_mode(criterion::SamplingMode::Flat);

    for scenario in Scenario::ALL {
        let upsert = upsert_payload(scenario, n);
        let (retracts, asserts) = scenario.expected_counts(n);
        group.bench_with_input(
            BenchmarkId::new(scenario.name(), scale.as_str()),
            &n,
            |b, &n| {
                b.iter_batched(
                    // Setup: base load (+ reindex, + novelty). NOT measured.
                    || build_fixture(&rt, scenario, n),
                    // Measured: one upsert over every subject named. The
                    // store goes back to criterion to drop untimed.
                    |fixture| {
                        let Fixture {
                            db_dir,
                            fluree,
                            ledger,
                        } = fixture;
                        let ledger = rt.block_on(async {
                            let result = fluree
                                .upsert_with_opts(
                                    ledger,
                                    &upsert,
                                    TxnOpts::default(),
                                    CommitOpts::default(),
                                    &index_config(),
                                )
                                .await
                                .expect("upsert");
                            assert_eq!(
                                (result.receipt.retract_count, result.receipt.assert_count),
                                (retracts, asserts),
                                "{}: unexpected (retracts, asserts)",
                                scenario.name()
                            );
                            result.ledger
                        });
                        black_box((db_dir, fluree, ledger))
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_transact_upsert_replace);
criterion_main!(benches);
