//! Differential fast-path correctness harness (audit roadmap Phase 0.7,
//! `docs/audit/2026-06-architecture-audit.md`).
//!
//! The planner has 30+ `detect_*` fast paths whose results must be
//! indistinguishable from the generic operator pipeline, and the engine has
//! three physical representations of the same logical data (binary index,
//! index + novelty overlay, pure novelty) whose query results must be
//! indistinguishable from each other. Both contracts are exactly where the
//! historical correctness bugs lived. This harness pins them:
//!
//! **Axis 1 — fast vs generic.** Every catalog query runs twice per
//! condition: once normally and once with the planner kill switch
//! (`fluree_db_api::set_fast_paths_disabled`) forcing the generic pipeline.
//! Results must match exactly (ordered queries) or as sets (unordered).
//!
//! **Axis 2 — representation equivalence.** The same logical dataset is
//! materialized three ways — `base` (everything reindexed; epoch 0),
//! `overlay` (base indexed, churn + new entities trailing in novelty),
//! `novelty` (never indexed) — and the *generic* results must agree across
//! all three conditions.
//!
//! The dataset deliberately includes overlay churn that exercises the
//! brittle lanes: upserts that retract-and-replace indexed values (price /
//! label changes whose new values dominate `ORDER BY DESC` top-k), plus
//! novelty-only subjects and strings (tail products + reviews).
//!
//! **A mismatch here is a finding, not a flaky test.** Investigate which
//! side is wrong against first principles before touching the harness —
//! the audit's bug-class history says the fast/overlay side is usually the
//! suspect, but the generic path is not presumed correct either.
//!
//! ## Finding history
//!
//! Cases marked `known_divergence` REPORT their mismatch to stderr instead
//! of failing, so the harness stays green while pinning the contract for
//! everything else; when a fix lands, the marker is removed and the case
//! becomes enforced. The harness's first run (2026-06-11) found three
//! divergence classes, all since fixed and now enforced:
//!
//! - **FD-1 `avg_numeric`** — FIXED. The AVG fast path accumulated
//!   Kahan-f64 and emitted xsd:double; the generic pipeline emits exact
//!   xsd:decimal for integer inputs (SPARQL semantics). Fix: the fast
//!   path mirrors the generic accumulator per number kind — exact i64 +
//!   BigDecimal division at the shared AVG_DECIMAL_PRECISION for
//!   integers, plain (non-Kahan, bit-matching) f64 for doubles, fallback
//!   for other numeric encodings (`fast_predicate_scalar_agg.rs`).
//! - **FD-2 `max_label` / `min_label`** — FIXED. The MIN/MAX-string fast
//!   path took per-leaflet first/last directory keys as candidates,
//!   which are extremes by StringId, not by lexicographic value; sound
//!   only under the bulk-import `lex_sorted_string_ids` invariant. Fix:
//!   the same lex-order gate `fast_string_prefix_count_all` already used
//!   (`fast_min_max_string.rs`, `minmax_string_dict_post`).
//! - **FD-3 `group_by_predicate_count`** — FIXED. The path answered from
//!   `StatsView` planner estimates, which count duplicate re-asserts,
//!   track `rdf:type` asymmetrically between indexer-built and
//!   novelty-accumulated stats, and apply novelty deltas inconsistently.
//!   Fix: rewritten to count exactly from POST leaf-directory metadata
//!   at epoch 0 (`stats_query.rs`), falling back to the generic pipeline
//!   under overlay/time-travel/policy/multi-ledger.

#![cfg(feature = "native")]

use fluree_bench_support::gen::bsbm::{bsbm_data_to_turtle, generate_dataset, BsbmData};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig,
    TxnOpts,
};
use serde_json::Value;

const N_PRODUCTS: usize = 200;
const N_TAIL: usize = 20;
const N_CHURNED: usize = 5;

// -----------------------------------------------------------------------------
// Dataset: base + churn (upsert retract/replace) + tail (novelty-only subjects)
// -----------------------------------------------------------------------------

fn base_turtle() -> String {
    bsbm_data_to_turtle(&generate_dataset(N_PRODUCTS))
}

/// Upsert that retracts and replaces price + label on the first
/// `N_CHURNED` products. New prices are unique and far above the
/// generator's 1000–50999 range, so `ORDER BY DESC(?price)` top-k MUST
/// surface these churned values — in the overlay condition that means the
/// winner rows come from novelty retract/assert pairs over indexed rows.
fn churn_turtle() -> String {
    let mut buf = String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix bsbm: <http://example.org/bsbm/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n",
    );
    for i in 0..N_CHURNED {
        buf.push_str(&format!(
            "ex:product-{i:06} bsbm:price \"{}\"^^xsd:integer ;\n    bsbm:label \"Churned Product {i:06}\" .\n\n",
            90_001 + i * 7,
        ));
    }
    buf
}

/// New products `[N_PRODUCTS, N_PRODUCTS + N_TAIL)` with their reviews —
/// novelty-only subjects and strings in the overlay condition. Vendors and
/// persons from the larger generation are included wholesale; re-asserting
/// already-present facts is itself a realistic overlay shape (set-semantics
/// dedup) and keeps the slice robust to generator-internal count rules.
fn tail_entities_turtle() -> String {
    let full = generate_dataset(N_PRODUCTS + N_TAIL);
    bsbm_data_to_turtle(&BsbmData {
        vendors: full.vendors.clone(),
        persons: full.persons.clone(),
        products: full.products[N_PRODUCTS..].to_vec(),
        reviews: Vec::new(),
    })
}

fn tail_reviews_turtle() -> String {
    let full = generate_dataset(N_PRODUCTS + N_TAIL);
    bsbm_data_to_turtle(&BsbmData {
        vendors: Vec::new(),
        persons: Vec::new(),
        products: Vec::new(),
        reviews: full.reviews[N_PRODUCTS * 3..].to_vec(),
    })
}

// -----------------------------------------------------------------------------
// Conditions
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Condition {
    /// All commits applied, then reindexed: everything behind the binary
    /// index at epoch 0 (including the churn's retraction history).
    Base,
    /// Base data reindexed; churn + tail commits trail in novelty.
    Overlay,
    /// Indexing disabled; the whole dataset lives in novelty.
    Novelty,
}

impl Condition {
    fn name(self) -> &'static str {
        match self {
            Condition::Base => "base",
            Condition::Overlay => "overlay",
            Condition::Novelty => "novelty",
        }
    }
}

/// Build a file-backed ledger in the given condition. All three conditions
/// hold the same logical data (base + churn + tail), differing only in how
/// much of it sits behind the binary index.
async fn setup(cond: Condition) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let mut builder = FlureeBuilder::file(path);
    if cond == Condition::Novelty {
        builder = builder.without_indexing();
    }
    let fluree = builder.build().expect("build Fluree");

    let alias = format!("diff:{}", cond.name());
    let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // Thresholds high enough that no commit triggers foreground/background
    // indexing on its own — the only index points are the explicit
    // `reindex` calls below, which is what defines each condition.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };

    let r = fluree
        .insert_turtle_with_opts(
            ledger,
            &base_turtle(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("insert base");
    ledger = r.ledger;

    if cond == Condition::Overlay {
        // Index the base only; everything after this trails in novelty.
        fluree
            .reindex(&alias, ReindexOptions::default())
            .await
            .expect("reindex base");
    }

    let r = fluree
        .upsert_turtle_with_opts(
            ledger,
            &churn_turtle(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("upsert churn");
    ledger = r.ledger;
    for (label, turtle) in [
        ("insert tail entities", tail_entities_turtle()),
        ("insert tail reviews", tail_reviews_turtle()),
    ] {
        let r = fluree
            .insert_turtle_with_opts(
                ledger,
                &turtle,
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect(label);
        ledger = r.ledger;
    }
    let _ = ledger;

    if cond == Condition::Base {
        // Index everything: epoch 0 with the churn in index history.
        fluree
            .reindex(&alias, ReindexOptions::default())
            .await
            .expect("reindex all");
    }

    (dir, fluree, alias)
}

// -----------------------------------------------------------------------------
// Query catalog
// -----------------------------------------------------------------------------

struct Case {
    name: &'static str,
    /// Compare rows exactly (ORDER BY with unique sort keys) vs as a
    /// sorted multiset.
    ordered: bool,
    /// `Some(finding_id)` — a documented, reproducible fast-vs-generic
    /// divergence (see module docs). Mismatches are reported to stderr
    /// instead of failing until the fix lands and the marker is removed.
    known_divergence: Option<&'static str>,
    sparql: &'static str,
}

const PREFIX: &str = "PREFIX bsbm: <http://example.org/bsbm/>\n";

/// Shapes chosen to trigger specific `detect_*` fast paths (see
/// `fluree-db-query/src/execute/operator_tree.rs`); each comment names the
/// intended target. Detection is shape-based, so a case that stops matching
/// after planner changes silently degrades to generic-vs-generic — keep
/// shapes in sync with the detectors when they evolve.
fn catalog() -> Vec<Case> {
    vec![
        // detect_predicate_object_count (bound-object count via POST FIRSTs)
        Case {
            name: "count_class",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(?s) AS ?c) WHERE { ?s a bsbm:Product }",
        },
        // detect_predicate_count_rows
        Case {
            name: "count_predicate_rows",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(?s) AS ?c) WHERE { ?s bsbm:price ?o }",
        },
        // detect_count_triples
        Case {
            name: "count_all_triples",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
        },
        // detect_count_distinct_position
        Case {
            name: "count_distinct_subjects",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }",
        },
        // detect_predicate_count_distinct_object
        Case {
            name: "count_distinct_objects",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s bsbm:productType ?o }",
        },
        // detect_predicate_count_rows_numeric_compare — the FILTER bound
        // (25000) sits inside the base price range and below the churned
        // 90k+ prices, so overlay retract/assert churn shifts the count.
        Case {
            name: "count_numeric_filter",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(?s) AS ?c) WHERE { ?s bsbm:price ?o . FILTER(?o > 25000) }",
        },
        // detect_fused_scan_sum / scalar agg — SUM shifts when churn
        // replaces five prices.
        Case {
            name: "sum_numeric",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (SUM(?o) AS ?total) WHERE { ?s bsbm:price ?o }",
        },
        // detect_predicate_avg_numeric
        Case {
            name: "avg_numeric",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (AVG(?o) AS ?avg) WHERE { ?s bsbm:price ?o }",
        },
        // detect_predicate_minmax_string — churned labels sort after
        // "Product ..." ("Churned ..." sorts before), and MAX over labels
        // includes tail products' novelty-only strings.
        Case {
            name: "max_label",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (MAX(?o) AS ?m) WHERE { ?s bsbm:label ?o }",
        },
        Case {
            name: "min_label",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (MIN(?o) AS ?m) WHERE { ?s bsbm:label ?o }",
        },
        // detect_post_order_desc_limit — reverse-POST tail walk. The seven
        // winners include all five churned prices (90k+), which in the
        // overlay condition exist only as novelty retract/assert pairs over
        // indexed rows. Prices are unique by construction → fully ordered.
        Case {
            name: "order_desc_limit",
            ordered: true,
            known_divergence: None,
            sparql: "SELECT ?s ?o WHERE { ?s bsbm:price ?o } ORDER BY DESC(?o) LIMIT 7",
        },
        // detect_predicate_group_by_object_count_topk — LIMIT exceeds the
        // five product types so the full group set returns and ties in the
        // count order don't make the row *set* ambiguous (compared unordered).
        Case {
            name: "group_by_object_count",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT ?o (COUNT(?s) AS ?c) WHERE { ?s bsbm:productType ?o } GROUP BY ?o ORDER BY DESC(?c) LIMIT 10",
        },
        // detect_stats_count_by_predicate (StatsView-answered when indexed)
        Case {
            name: "group_by_predicate_count",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT ?p (COUNT(?s) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p",
        },
        // Property-join star fusion + ORDER BY (generic-path internal fast
        // lanes; also a Q5-shape sanity anchor). Unique prices → ordered.
        Case {
            name: "star_join_ordered",
            ordered: true,
            known_divergence: None,
            sparql: "SELECT ?product ?label ?vendorLabel ?price WHERE {\n\
                     ?product a bsbm:Product ; bsbm:label ?label ; bsbm:vendor ?vendor ; bsbm:price ?price .\n\
                     ?vendor bsbm:label ?vendorLabel .\n\
                     FILTER(?price >= 5000 && ?price <= 25000)\n\
                     } ORDER BY ?price LIMIT 10",
        },
        // detect_label_regex_type / string-prefix lanes. Churn renames
        // products 0–4 ("Churned ..."), so the overlay's retractions must
        // suppress their old "Product 0000xx" labels for the count to agree.
        Case {
            name: "strstarts_label",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT ?s WHERE { ?s a bsbm:Product ; bsbm:label ?l . FILTER(STRSTARTS(?l, \"Product 0000\")) }",
        },
        // detect_string_prefix_count_all
        Case {
            name: "count_strstarts",
            ordered: false,
            known_divergence: None,
            sparql: "SELECT (COUNT(?s) AS ?c) WHERE { ?s bsbm:label ?l . FILTER(STRSTARTS(?l, \"Churned\")) }",
        },
    ]
}

// -----------------------------------------------------------------------------
// Execution + comparison
// -----------------------------------------------------------------------------

fn normalize(rows: &Value, ordered: bool) -> Vec<String> {
    let arr = rows
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array of rows, got: {rows}"));
    let mut out: Vec<String> = arr
        .iter()
        .map(|r| serde_json::to_string(r).expect("serialize row"))
        .collect();
    if !ordered {
        out.sort();
    }
    out
}

async fn run_query(fluree: &Fluree, alias: &str, sparql: &str) -> Value {
    let full = format!("{PREFIX}{sparql}");
    let snapshot = fluree
        .graph(alias)
        .load()
        .await
        .unwrap_or_else(|e| panic!("load {alias}: {e}"));
    snapshot
        .query()
        .sparql(&full)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query [{alias}] {sparql}: {e}"))
}

/// RAII guard: the kill switch is process-global; restore on every exit
/// path (including panics) so a failure here can't poison other tests.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test]
async fn differential_fastpath_and_condition_matrix() {
    let _guard = FastPathGuard;
    let conditions = [Condition::Base, Condition::Overlay, Condition::Novelty];
    let cases = catalog();
    let mut failures: Vec<String> = Vec::new();

    // generic results per (case, condition) for the cross-condition axis.
    let mut generic_results: Vec<Vec<Vec<String>>> = vec![Vec::new(); cases.len()];

    for cond in conditions {
        let (_dir, fluree, alias) = setup(cond).await;

        for (ci, case) in cases.iter().enumerate() {
            set_fast_paths_disabled(false);
            let fast = run_query(&fluree, &alias, case.sparql).await;
            set_fast_paths_disabled(true);
            let generic = run_query(&fluree, &alias, case.sparql).await;
            set_fast_paths_disabled(false);

            let fast_n = normalize(&fast, case.ordered);
            let generic_n = normalize(&generic, case.ordered);
            if fast_n != generic_n {
                let detail = format!(
                    "[fast≠generic] case={} condition={}\n  fast:    {:?}\n  generic: {:?}",
                    case.name,
                    cond.name(),
                    fast_n,
                    generic_n,
                );
                match case.known_divergence {
                    Some(finding) => {
                        eprintln!("KNOWN DIVERGENCE {finding} (not enforced): {detail}\n");
                    }
                    None => failures.push(detail),
                }
            }
            generic_results[ci].push(generic_n);
        }
    }

    // Axis 2: the generic pipeline must see identical logical data in all
    // three physical representations.
    for (ci, case) in cases.iter().enumerate() {
        let per_cond = &generic_results[ci];
        for (i, cond) in conditions.iter().enumerate().skip(1) {
            if per_cond[i] != per_cond[0] {
                failures.push(format!(
                    "[condition≠condition] case={} {}≠{}\n  {}: {:?}\n  {}: {:?}",
                    case.name,
                    cond.name(),
                    conditions[0].name(),
                    conditions[0].name(),
                    per_cond[0],
                    cond.name(),
                    per_cond[i],
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "differential harness found {} mismatch(es):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
