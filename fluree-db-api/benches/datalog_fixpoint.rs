//! Datalog fixpoint cost: the first reasoning query after a commit.
//!
//! Derived facts are cached per database state, so a warm query measures a
//! cache hit. Each sample therefore commits one unrelated fact (untimed) and
//! then times a query that has to rematerialize the whole rule program.
//!
//! Four programs over the same chain graph (`n_chains` disjoint `ex:parent`
//! chains of `CHAIN_LEN` nodes):
//!
//! 1. **`linear_recursion`** — `ancestor(x,z) :- parent(x,y), ancestor(y,z)`.
//!    A transitive closure, evaluated as one property path.
//! 2. **`nonlinear_recursion`** — `ancestor(x,z) :- ancestor(x,y),
//!    ancestor(y,z)`. Also a closure, evaluated as one path.
//! 3. **`recursion_with_acyclic_rules`** — the linear program plus
//!    `ACYCLIC_RULES` non-recursive rules reading its output, which run once
//!    after the recursion converges.
//! 4. **`iterated_linear_recursion`** — the linear step with a filter that
//!    always holds on chains but keeps the rule from compiling to a path. It
//!    iterates one pass per chain hop, each re-reading only the previous
//!    pass's facts.
//!
//! Every query carries an explicit reasoning budget sized for its scale, so
//! the default 30-second ceiling cannot truncate a larger run. Before timing,
//! each derived relation is counted and the total derived-fact count is
//! checked; every timed sample is then rejected if materialization was capped
//! or derived a different number of facts. A run that derives less fails
//! instead of looking faster.
//!
//!   inputs:    BenchScale → n_chains
//!              (Tiny=10, Small=100, Medium=500, Large=2_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench datalog_fixpoint
//!   cargo bench -p fluree-db-api --bench datalog_fixpoint -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench datalog_fixpoint

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GraphSnapshot, IndexConfig, TrackingOptions, TxnOpts,
};
use serde_json::{json, Value};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

const CHAIN_LEN: usize = 16;
const ACYCLIC_RULES: usize = 12;

fn scale_n_chains(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 10,
        BenchScale::Small => 100,
        BenchScale::Medium => 500,
        BenchScale::Large => 2_000,
    }
}

/// Reasoning time ceiling per query. Generous on purpose: it exists so the
/// default ceiling cannot truncate a timing run, and a sample that reaches it
/// fails the bench rather than being timed.
fn scale_max_seconds(scale: BenchScale) -> u64 {
    match scale {
        BenchScale::Tiny => 60,
        BenchScale::Small => 300,
        BenchScale::Medium => 1_200,
        BenchScale::Large => 3_600,
    }
}

struct Program {
    name: &'static str,
    rules: Vec<Value>,
    /// Each derived predicate and the number of facts it holds per chain.
    relations: Vec<(String, usize)>,
}

impl Program {
    fn derived_facts(&self, n_chains: usize) -> u64 {
        let per_chain: usize = self.relations.iter().map(|(_, n)| n).sum();
        (per_chain * n_chains) as u64
    }
}

fn rule(where_: Value, insert: Value) -> Value {
    json!({"@context": {"ex": "http://example.org/"}, "where": where_, "insert": insert})
}

fn ancestor_base() -> Value {
    rule(
        json!({"@id": "?x", "ex:parent": "?y"}),
        json!({"@id": "?x", "ex:ancestor": {"@id": "?y"}}),
    )
}

fn linear_step() -> Value {
    rule(
        json!([{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:ancestor": "?z"}]),
        json!({"@id": "?x", "ex:ancestor": {"@id": "?z"}}),
    )
}

fn filtered_linear_step() -> Value {
    rule(
        json!([
            {"@id": "?x", "ex:parent": "?y"},
            {"@id": "?y", "ex:ancestor": "?z"},
            ["filter", "(!= ?x ?z)"]
        ]),
        json!({"@id": "?x", "ex:ancestor": {"@id": "?z"}}),
    )
}

fn programs() -> Vec<Program> {
    let pairs_per_chain = CHAIN_LEN * (CHAIN_LEN - 1) / 2;
    let ancestor = || ("ex:ancestor".to_string(), pairs_per_chain);
    let mut with_acyclic = vec![ancestor_base(), linear_step()];
    let mut with_acyclic_relations = vec![ancestor()];
    for k in 0..ACYCLIC_RULES {
        with_acyclic_relations.push((format!("ex:reaches{k}"), CHAIN_LEN - 1));
        with_acyclic.push(rule(
            json!([
                {"@id": "?x", "ex:ancestor": "?y"},
                {"@id": "?y", "@type": format!("ex:Marker{k}")}
            ]),
            json!({"@id": "?x", format!("ex:reaches{k}"): {"@id": "?y"}}),
        ));
    }
    vec![
        Program {
            name: "linear_recursion",
            rules: vec![ancestor_base(), linear_step()],
            relations: vec![ancestor()],
        },
        Program {
            name: "nonlinear_recursion",
            rules: vec![
                ancestor_base(),
                rule(
                    json!([{"@id": "?x", "ex:ancestor": "?y"}, {"@id": "?y", "ex:ancestor": "?z"}]),
                    json!({"@id": "?x", "ex:ancestor": {"@id": "?z"}}),
                ),
            ],
            relations: vec![ancestor()],
        },
        Program {
            name: "recursion_with_acyclic_rules",
            rules: with_acyclic,
            relations: with_acyclic_relations,
        },
        Program {
            name: "iterated_linear_recursion",
            rules: vec![ancestor_base(), filtered_linear_step()],
            relations: vec![ancestor()],
        },
    ]
}

/// `n_chains` disjoint parent chains; each chain's last node carries every
/// marker type the acyclic rules look for.
fn chains_turtle(n_chains: usize) -> String {
    let mut ttl = String::with_capacity(n_chains * CHAIN_LEN * 64);
    ttl.push_str("@prefix ex: <http://example.org/> .\n");
    for c in 0..n_chains {
        for i in 0..CHAIN_LEN - 1 {
            let _ = writeln!(ttl, "ex:n{c}_{i} ex:parent ex:n{c}_{} .", i + 1);
        }
        for k in 0..ACYCLIC_RULES {
            let _ = writeln!(ttl, "ex:n{c}_{} a ex:Marker{k} .", CHAIN_LEN - 1);
        }
    }
    ttl
}

async fn setup_indexed(program: &Program, n_chains: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let alias = next_ledger_alias(&format!("datalog-fixpoint-{}", program.name));
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let ledger = fluree
        .insert_turtle_with_opts(
            ledger,
            &chains_turtle(n_chains),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("populate insert")
        .ledger;

    let stored_rules: Vec<Value> = program
        .rules
        .iter()
        .enumerate()
        .map(|(i, rule)| {
            json!({"@id": format!("ex:rule{i:02}"), "f:rule": {"@type": "@json", "@value": rule}})
        })
        .collect();
    let rules_txn = json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "@graph": stored_rules
    });
    fluree
        .insert(ledger, &rules_txn)
        .await
        .expect("insert rules");

    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");
    (db_dir, fluree, alias)
}

static TICK: AtomicU64 = AtomicU64::new(0);

/// Commit one fact no rule reads, so the next reasoning query misses the
/// derived-facts cache.
async fn commit_tick(fluree: &Fluree, alias: &str) {
    let ledger = fluree.ledger(alias).await.expect("ledger");
    let tick = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:tick",
        "ex:seq": TICK.fetch_add(1, Ordering::Relaxed)
    });
    fluree.insert(ledger, &tick).await.expect("tick commit");
}

fn count_query(predicate: &str, max_facts: u64, max_seconds: u64) -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?y)"],
        "where": {"@id": "?x", predicate: "?y"},
        "reasoning": "datalog",
        "reasoningBudget": {"maxFacts": max_facts, "maxSeconds": max_seconds}
    })
}

/// Run `query` and return its result, panicking unless materialization ran
/// to completion and derived exactly `expected_facts`.
async fn run_complete(
    snapshot: &GraphSnapshot<'_>,
    query: &Value,
    expected_facts: u64,
    program: &str,
) -> Value {
    let time_only = TrackingOptions {
        track_time: true,
        track_fuel: false,
        track_policy: false,
        max_fuel: None,
    };
    let response = snapshot
        .query()
        .jsonld(query)
        .tracking(time_only)
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("{program}: query failed: {e:?}"));
    let reasoning = response
        .reasoning
        .unwrap_or_else(|| panic!("{program}: no reasoning outcome was reported"));
    assert!(
        !reasoning.capped,
        "{program}: materialization was capped ({:?}) after {} facts",
        reasoning.capped_reason, reasoning.derived_facts
    );
    assert_eq!(
        reasoning.derived_facts, expected_facts,
        "{program}: derived-fact count changed"
    );
    response.result
}

fn bench_datalog_fixpoint(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_chains = scale_n_chains(scale);
    let max_seconds = scale_max_seconds(scale);

    eprintln!(
        "  [datalog_fixpoint] scale={} n_chains={} chain_len={} max_seconds={}",
        scale.as_str(),
        n_chains,
        CHAIN_LEN,
        max_seconds
    );

    let mut group = c.benchmark_group("datalog_fixpoint");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for program in programs() {
        let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(&program, n_chains));
        let expected_facts = program.derived_facts(n_chains);
        let max_facts = expected_facts * 2;

        rt.block_on(async {
            let snapshot = fluree.graph(&alias).load().await.expect("graph load");
            for (predicate, per_chain) in &program.relations {
                let counted = run_complete(
                    &snapshot,
                    &count_query(predicate, max_facts, max_seconds),
                    expected_facts,
                    program.name,
                )
                .await;
                assert_eq!(
                    counted,
                    json!([[(per_chain * n_chains) as u64]]),
                    "{}: {predicate} holds the wrong number of facts",
                    program.name
                );
            }
        });

        let (timed_predicate, _) = &program.relations[0];
        let query = count_query(timed_predicate, max_facts, max_seconds);
        group.bench_with_input(
            BenchmarkId::new(program.name, scale.as_str()),
            &n_chains,
            |b, _| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            commit_tick(&fluree, &alias).await;
                            fluree.graph(&alias).load().await.expect("graph load")
                        })
                    },
                    |snapshot| {
                        rt.block_on(async {
                            let result =
                                run_complete(&snapshot, &query, expected_facts, program.name).await;
                            black_box(result);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_datalog_fixpoint);
criterion_main!(benches);
