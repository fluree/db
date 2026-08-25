//! Regression pins for issue #1652: three indexed-lane fast paths that
//! returned plausible wrong numbers where the generic pipeline (the
//! `FLUREE_DISABLE_QUERY_FAST_PATHS` kill-switch reference) is correct.
//!
//! * **Star top-k bag multiplicity** — `GroupByObjectStarTopKOperator`
//!   treated the star's filter triples as an existence semi-join, so
//!   `GROUP BY ?o (COUNT(?s))` lost the filter predicates' join
//!   multiplicity (a subject with 3 signatures contributed 1, not 3).
//! * **Chain-fold POST group split** — `PostObjectGroupCountIter` restarted
//!   an object group at every leaflet boundary; `execute_chain`'s
//!   forward-only PSOT seek then dropped the second fragment's rows.
//!   Layout-dependent, so this file's chain ledger is indexed with 3-row
//!   leaflets to force boundary-straddling groups at test scale.
//! * **Composite `(s,o)` join identity** — `count_composite_join_pairs`
//!   merge-joined on `(s_id, o_type, o_key)`, but a `NUM_BIG_OVERFLOW`
//!   `o_key` is a per-predicate arena handle (every `xsd:decimal`, any
//!   `xsd:integer` beyond i64), so equal big values under the two
//!   predicates never matched. Now declined by directory metadata (list
//!   rows too — their generic join semantics depend on `o_i`).
//!
//! Each case asserts three things: the fast lane's answer, the generic
//! pipeline's answer (both against a hand-pinned value, so a generic
//! regression is caught as loudly as a fast-path one), and the engine's
//! `fast-path outcome` stamps — the fixed shapes must still `proceed` on
//! their site (the fix must not silently disable the lane), and the
//! NumBig/list composite shapes must NOT proceed on `count-plan`.
//!
//! Own test binary: toggles the process-global kill switch AND asserts
//! fast-path routing via span capture, so it must not share a process with
//! other tests.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig,
    TxnOpts,
};
use fluree_db_indexer::IndexerConfig;
use serde_json::Value;

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

const N_PUB: usize = 60;
const N_A: usize = 40;
const N_B: usize = 25;

/// Star ledger: `bibtexType` (group pred, 4 rotating classes + a second value
/// on every 11th subject) with `hasSignature` (0–3 IRI objects) and `hasTag`
/// (0–2 string objects) as filter-star predicates. Subjects with `i % 4 == 0`
/// have a type but NO signatures, pinning that they drop out of the
/// signature-star group counts entirely.
///
/// `refA` / `refB` are the shared-object-variable probe: every subject carries
/// two of each with exactly one value in common, so a filter pair joined on one
/// variable has per-subject multiplicity 1 while the product of counts is 4.
/// Every 17th subject gets disjoint values instead — generically it drops out
/// of the join, while a product of counts still credits it.
fn star_turtle() -> String {
    let mut buf = String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n",
    );
    for i in 0..N_PUB {
        let ty = match i % 4 {
            0 => "ex:Inproceedings",
            1 => "ex:Book",
            2 => "ex:Incollection",
            _ => "ex:Article",
        };
        buf.push_str(&format!("ex:pub-{i:04} ex:bibtexType {ty} "));
        let sigs = i % 4;
        if sigs > 0 {
            buf.push_str("; ex:hasSignature ");
            let list: Vec<String> = (0..sigs).map(|k| format!("ex:sig-{i:04}-{k}")).collect();
            buf.push_str(&list.join(", "));
        }
        let tags = i % 3;
        if tags > 0 {
            buf.push_str("; ex:hasTag ");
            let list: Vec<String> = (0..tags).map(|k| format!("\"tag-{i:04}-{k}\"")).collect();
            buf.push_str(&list.join(", "));
        }
        if i % 11 == 0 {
            buf.push_str("; ex:bibtexType ex:Misc ");
        }
        if i % 17 == 0 {
            // Disjoint refA/refB: the generic join drops these subjects
            // entirely, where a product of counts still contributes.
            buf.push_str("; ex:refA ex:r-0 ; ex:refB ex:r-2 ");
        } else {
            buf.push_str("; ex:refA ex:r-0, ex:r-1 ; ex:refB ex:r-1, ex:r-2 ");
        }
        buf.push_str(" .\n");
    }
    buf
}

/// Chain ledger: `?a ex:sigPub ?b . ?b rdf:type ?c` with a `rdfs:subClassOf`
/// tail on `?c` (classes c-0/c-1 have 2/1 superclasses, the rest none), plus a
/// mixed-object `ex:kind` second hop and literal objects sprinkled into
/// `sigPub` itself. Indexed with 3-row leaflets so object groups of the POST
/// walk straddle leaflet boundaries — the shape that lost rows.
fn chain_turtle() -> String {
    let mut buf = String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n",
    );
    buf.push_str(
        "ex:c-0 rdfs:subClassOf ex:Top1, ex:Top2 .\n\
         ex:c-1 rdfs:subClassOf ex:Top1 .\n\n",
    );
    for j in 0..N_B {
        buf.push_str(&format!("ex:b-{j:04} rdf:type ex:c-{} ", j % 8));
        if j % 5 == 0 {
            buf.push_str(&format!(", ex:c-{} ", (j + 3) % 8));
        }
        buf.push_str(&format!("; ex:kind \"kind-{}\" ", j % 6));
        if j % 7 == 0 {
            buf.push_str(&format!("; ex:kind ex:c-{} ", j % 8));
        }
        buf.push_str(" .\n");
    }
    for i in 0..N_A {
        buf.push_str(&format!("ex:a-{i:04} ex:sigPub ex:b-{:04} ", i % N_B));
        if i % 6 == 0 {
            buf.push_str(&format!(", ex:b-{:04} ", (i + 5) % N_B));
        }
        if i % 9 == 0 {
            buf.push_str(&format!(", \"lit-{i}\" "));
        }
        buf.push_str(" .\n");
    }
    buf
}

/// Composite-join ledger WITHOUT arena-routed values: the `(s, o)` merge-join
/// stays on its fast path and must count matching pairs across IRIs, strings,
/// inline integers/decimals, and language-tagged strings (`@en` vs `@fr` must
/// NOT match).
fn composite_clean_turtle() -> String {
    String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n\
         ex:d-0 ex:createdBy ex:p-1 ; ex:authoredBy ex:p-1 .\n\
         ex:d-1 ex:createdBy ex:p-1, ex:p-2 ; ex:authoredBy ex:p-2 .\n\
         ex:d-2 ex:createdBy \"alice\" ; ex:authoredBy \"alice\" .\n\
         ex:d-3 ex:createdBy \"7\"^^xsd:integer ; ex:authoredBy \"7\"^^xsd:integer .\n\
         ex:d-4 ex:createdBy \"bob\"@en ; ex:authoredBy \"bob\"@en .\n\
         ex:d-5 ex:createdBy \"bob\"@en ; ex:authoredBy \"bob\"@fr .\n\
         ex:d-6 ex:createdBy ex:p-3 ; ex:authoredBy ex:p-4 .\n",
    )
}

/// Composite-join ledger WITH arena-routed values (`xsd:decimal`, overflow
/// `xsd:integer`), where `createdBy` carries a wider big-value set than
/// `authoredBy` so the per-predicate arena handles for the shared values
/// disagree. The fast lane must decline; the counted answer includes every
/// big-value match.
fn composite_numbig_turtle() -> String {
    let mut buf = String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n",
    );
    let bigs = [
        "10000000000000000000.1",
        "20000000000000000000.2",
        "30000000000000000000.3",
        "40000000000000000000.4",
        "50000000000000000000.5",
        "60000000000000000000.6",
    ];
    // createdBy-only extras widen createdBy's arena so shared values land on
    // different handles.
    for (i, v) in bigs.iter().enumerate().take(3) {
        buf.push_str(&format!("ex:x-{i} ex:createdBy \"{v}\"^^xsd:decimal .\n"));
    }
    for (i, v) in bigs.iter().enumerate().skip(3) {
        buf.push_str(&format!(
            "ex:x-{i} ex:createdBy \"{v}\"^^xsd:decimal ; ex:authoredBy \"{v}\"^^xsd:decimal .\n"
        ));
    }
    let ints = [
        "100000000000000000000",
        "200000000000000000000",
        "300000000000000000000",
    ];
    buf.push_str(&format!(
        "ex:y-0 ex:createdBy \"{}\"^^xsd:integer .\n",
        ints[0]
    ));
    for (i, v) in ints.iter().enumerate().skip(1) {
        buf.push_str(&format!(
            "ex:y-{i} ex:createdBy \"{v}\"^^xsd:integer ; ex:authoredBy \"{v}\"^^xsd:integer .\n"
        ));
    }
    buf.push_str(
        "ex:z-0 ex:createdBy ex:p-1 ; ex:authoredBy ex:p-1 .\n\
         ex:z-1 ex:createdBy \"7\"^^xsd:integer ; ex:authoredBy \"7\"^^xsd:integer .\n\
         ex:z-2 ex:createdBy \"1.5\"^^xsd:decimal ; ex:authoredBy \"1.5\"^^xsd:decimal .\n\
         ex:z-3 ex:createdBy \"abc\" ; ex:authoredBy \"abc\" .\n",
    );
    buf
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// Which ledger a case runs against.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ledger {
    Star,
    Chain,
    CompositeClean,
    CompositeNumBig,
    CompositeList,
}

/// Routing assertion against the engine's `fast-path outcome` stamps.
#[derive(Clone, Copy)]
enum Routing {
    /// This site must `proceed` — the fix must keep the lane, not disable it.
    MustFire(&'static str),
    /// This site must NOT `proceed` — the shape is one the lane cannot answer.
    MustNotFire(&'static str),
}

struct Case {
    name: &'static str,
    ledger: Ledger,
    sparql: &'static str,
    /// Hand-pinned rows as `normalize` renders them (sorted JSON row strings).
    /// Both the fast lane and the generic pipeline must produce exactly this.
    expected: &'static [&'static str],
    routing: Routing,
}

fn cases() -> Vec<Case> {
    use Ledger::{Chain, CompositeClean, CompositeList, CompositeNumBig, Star};
    use Routing::{MustFire, MustNotFire};
    const STAR_SITE: &str = "group_by_object_star_topk";
    const PLAN_SITE: &str = "count-plan";
    vec![
        // ---- Case 1: star top-k bag multiplicity ---------------------------
        // Inproceedings subjects (i % 4 == 0) have no signatures: absent.
        // Counts are sums of per-subject signature counts, not subject counts.
        Case {
            name: "star topk COUNT over signature star",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:hasSignature ?o2 .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",45]",
                "[\"ex:Book\",15]",
                "[\"ex:Incollection\",30]",
                "[\"ex:Misc\",9]",
            ],
            routing: MustFire(STAR_SITE),
        },
        Case {
            name: "star topk COUNT over string-tag star",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:hasTag ?o2 .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",15]",
                "[\"ex:Book\",15]",
                "[\"ex:Incollection\",15]",
                "[\"ex:Inproceedings\",15]",
                "[\"ex:Misc\",6]",
            ],
            routing: MustFire(STAR_SITE),
        },
        // Two filter predicates: multiplicity is the PRODUCT of per-subject
        // counts (signatures x tags), exercising the intersection-map lane.
        Case {
            name: "star topk COUNT over two-predicate star",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:hasSignature ?o2 . ?s ex:hasTag ?o3 .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",45]",
                "[\"ex:Book\",15]",
                "[\"ex:Incollection\",30]",
                "[\"ex:Misc\",11]",
            ],
            routing: MustFire(STAR_SITE),
        },
        // Multi-aggregate: MIN/MAX stay subject-level (duplicate-insensitive)
        // while COUNT carries the multiplicity — the mixed-correctness shape
        // from the report (wrong counts, right min/max).
        Case {
            name: "star topk COUNT+MIN+MAX",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) (MIN(?s) AS ?mn) (MAX(?s) AS ?mx) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:hasSignature ?o2 .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",45,\"ex:pub-0003\",\"ex:pub-0059\"]",
                "[\"ex:Book\",15,\"ex:pub-0001\",\"ex:pub-0057\"]",
                "[\"ex:Incollection\",30,\"ex:pub-0002\",\"ex:pub-0058\"]",
                "[\"ex:Misc\",9,\"ex:pub-0011\",\"ex:pub-0055\"]",
            ],
            routing: MustFire(STAR_SITE),
        },
        // Two filter triples sharing an object variable are a join on it, so
        // the per-subject multiplicity is the size of the refA/refB value
        // intersection (1), not the product of their counts (4). The operator
        // has no way to express that, so the detector must decline.
        Case {
            name: "star topk shared filter object var declines",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:refA ?x . ?s ex:refB ?x .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",14]",
                "[\"ex:Book\",14]",
                "[\"ex:Incollection\",14]",
                "[\"ex:Inproceedings\",14]",
                "[\"ex:Misc\",5]",
            ],
            routing: MustNotFire(STAR_SITE),
        },
        // Distinct object vars over filter predicates stay eligible — there the
        // product of per-subject counts IS the join multiplicity.
        Case {
            name: "star topk distinct filter object vars keep the lane",
            ledger: Star,
            sparql: "SELECT ?o1 (COUNT(?s) AS ?count) WHERE {\n\
                     ?s ex:bibtexType ?o1 . ?s ex:refA ?x . ?s ex:refB ?y .\n\
                     } GROUP BY ?o1 ORDER BY DESC(?count) LIMIT 10",
            expected: &[
                "[\"ex:Article\",57]",
                "[\"ex:Book\",57]",
                "[\"ex:Incollection\",57]",
                "[\"ex:Inproceedings\",57]",
                "[\"ex:Misc\",21]",
            ],
            routing: MustFire(STAR_SITE),
        },
        // ---- Case 2: chain fold across leaflet boundaries ------------------
        // The inner join loses rows too when a POST object group straddles a
        // leaflet — the OPTIONAL/MINUS asymmetry in the report was downstream
        // of the same split.
        Case {
            name: "chain COUNT(*) plain",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b rdf:type ?c . }",
            expected: &["[57]"],
            routing: MustFire(PLAN_SITE),
        },
        Case {
            name: "chain COUNT(*) OPTIONAL tail",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b rdf:type ?c .\n\
                     OPTIONAL { ?c rdfs:subClassOf ?d . } }",
            expected: &["[67]"],
            routing: MustFire(PLAN_SITE),
        },
        Case {
            name: "chain COUNT(*) MINUS tail",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b rdf:type ?c .\n\
                     MINUS { ?c rdfs:subClassOf ?d . } }",
            expected: &["[41]"],
            routing: MustFire(PLAN_SITE),
        },
        Case {
            name: "chain COUNT(*) EXISTS tail",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b rdf:type ?c .\n\
                     FILTER EXISTS { ?c rdfs:subClassOf ?d . } }",
            expected: &["[16]"],
            routing: MustFire(PLAN_SITE),
        },
        // Mixed-object second hop (`ex:kind` holds strings AND IRIs): with
        // 3-row leaflets some are homogeneous non-IRI, which the POST walk
        // must SKIP, not treat as end-of-stream.
        Case {
            name: "chain COUNT(*) mixed-object hop",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b ex:kind ?c . }",
            expected: &["[54]"],
            routing: MustFire(PLAN_SITE),
        },
        Case {
            name: "chain COUNT(*) mixed-object hop, OPTIONAL tail",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b ex:kind ?c .\n\
                     OPTIONAL { ?c rdfs:subClassOf ?d . } }",
            expected: &["[56]"],
            routing: MustFire(PLAN_SITE),
        },
        Case {
            name: "chain COUNT(*) mixed-object hop, MINUS tail",
            ledger: Chain,
            sparql: "SELECT (COUNT(*) AS ?count) WHERE { ?a ex:sigPub ?b . ?b ex:kind ?c .\n\
                     MINUS { ?c rdfs:subClassOf ?d . } }",
            expected: &["[52]"],
            routing: MustFire(PLAN_SITE),
        },
        // ---- Case 3: composite (s,o) join ---------------------------------
        // d-0 (IRI), d-1 (p-2 of two createdBy), d-2 (string), d-3 (inline
        // int), d-4 (@en/@en). d-5 (@en/@fr) and d-6 (different IRIs) must not
        // match.
        Case {
            name: "composite join, no arena values (keeps fast path)",
            ledger: CompositeClean,
            sparql:
                "SELECT (COUNT(*) AS ?count) WHERE { ?s ex:createdBy ?o . ?s ex:authoredBy ?o }",
            expected: &["[5]"],
            routing: MustFire(PLAN_SITE),
        },
        // 9 = 3 big decimals + 2 big ints + z-0..z-3's four matches; the six
        // big-value matches are exactly what the per-predicate arena handles
        // missed (fast said 3).
        Case {
            name: "composite join, arena values (declines to generic)",
            ledger: CompositeNumBig,
            sparql:
                "SELECT (COUNT(*) AS ?count) WHERE { ?s ex:createdBy ?o . ?s ex:authoredBy ?o }",
            expected: &["[9]"],
            routing: MustNotFire(PLAN_SITE),
        },
        // List rows (`o_i`): the generic join treats every list element as its
        // own fact, so a var-object join is a bag join over them today — s2's
        // two `q-1` entries on each side pair up 2×2 = 4, s3 pairs its single
        // shared `r-1` once, and s1 never matches (see the fixture). This pins
        // what the generic pipeline yields, not a ruling: whether list
        // position belongs to a literal's identity is still open (#1676), and
        // settling it may change this number. What the case does assert is
        // that the count lane cannot express the shape in an
        // (s, o_type, o_key) key, so it must decline and agree with whatever
        // the generic join answers. (The membership lane would answer as a
        // semi-join — one row per driving row, 3 — which the planner's
        // driving-size gate now keeps it away from at this fixture's size;
        // the lane's answer at or above the gate is unpinned.)
        Case {
            name: "composite join, list rows (declines to generic)",
            ledger: CompositeList,
            sparql:
                "SELECT (COUNT(*) AS ?count) WHERE { ?s ex:createdBy ?o . ?s ex:authoredBy ?o }",
            expected: &["[5]"],
            routing: MustNotFire(PLAN_SITE),
        },
    ]
}

// ---------------------------------------------------------------------------
// Setup + runner
// ---------------------------------------------------------------------------

/// Insert + reindex with an explicit leaflet size. `leaflet_rows = 3` forces
/// multi-leaflet predicates at test scale (the chain ledger's boundary-split
/// shape); the default keeps single-leaflet layout.
async fn setup_turtle(
    alias: &str,
    turtle: &str,
    leaflet_rows: Option<usize>,
) -> (tempfile::TempDir, Fluree) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let ledger = fluree.create_ledger(alias).await.expect("create_ledger");
    // Thresholds high enough that no commit self-indexes; the explicit
    // reindex below is the only index point.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("insert");
    let mut opts = ReindexOptions::default();
    if let Some(rows) = leaflet_rows {
        opts = opts.with_indexer_config(IndexerConfig::default().with_leaflet_rows(rows));
    }
    fluree.reindex(alias, opts).await.expect("reindex");
    (dir, fluree)
}

/// The list ledger needs JSON-LD (`@list` has no Turtle surface for `o_i`).
/// Bare strings inside the `@list` arrays are string literals (the terms are
/// not `@type: @id`), while s1's `ex:authoredBy` is an IRI — so s1 never
/// joins (string vs IRI). s2: duplicate lists on both sides (bag join: 2×2 =
/// 4). s3: one shared value at different positions (1).
async fn setup_list_ledger(alias: &str) -> (tempfile::TempDir, Fluree) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let ledger0 = fluree.create_ledger(alias).await.expect("create_ledger");
    let insert1 = serde_json::json!({
        "@context": [
            {"ex": "http://example.org/ns/"},
            {"ex:createdBy": {"@container": "@list"}}
        ],
        "@id": "ex:s1",
        "ex:createdBy": ["ex:p-1", "ex:p-1"],
        "ex:authoredBy": {"@id": "ex:p-1"}
    });
    let insert2 = serde_json::json!({
        "@context": [
            {"ex": "http://example.org/ns/"},
            {"ex:createdBy": {"@container": "@list"}, "ex:authoredBy": {"@container": "@list"}}
        ],
        "@id": "ex:s2",
        "ex:createdBy": ["ex:q-1", "ex:q-1"],
        "ex:authoredBy": ["ex:q-1", "ex:q-1"]
    });
    let insert3 = serde_json::json!({
        "@context": [
            {"ex": "http://example.org/ns/"},
            {"ex:createdBy": {"@container": "@list"}, "ex:authoredBy": {"@container": "@list"}}
        ],
        "@id": "ex:s3",
        "ex:createdBy": ["ex:r-other", "ex:r-1"],
        "ex:authoredBy": ["ex:r-1", "ex:r-x"]
    });
    let r1 = fluree.insert(ledger0, &insert1).await.expect("insert s1");
    let r2 = fluree.insert(r1.ledger, &insert2).await.expect("insert s2");
    let _ = fluree.insert(r2.ledger, &insert3).await.expect("insert s3");
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    (dir, fluree)
}

async fn run_query(fluree: &Fluree, alias: &str, sparql: &str) -> Value {
    let full = format!("{PREFIX}{sparql}");
    let snapshot = fluree.graph(alias).load().await.expect("load");
    snapshot
        .query()
        .sparql(&full)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query {sparql}: {e}"))
}

fn normalize(rows: &Value) -> Vec<String> {
    let mut out: Vec<String> = rows
        .as_array()
        .expect("array of rows")
        .iter()
        .map(|r| serde_json::to_string(r).expect("serialize row"))
        .collect();
    out.sort();
    out
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn issue_1652_shapes_match_generic_and_keep_their_fast_paths() {
    // The kill switch OR's with this env var; with it set, the fast phase
    // below runs generically and every assertion is vacuous.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let (_d1, star) = setup_turtle("r1652:star", &star_turtle(), None).await;
    let (_d2, chain) = setup_turtle("r1652:chain", &chain_turtle(), Some(3)).await;
    let (_d3, clean) = setup_turtle("r1652:clean", &composite_clean_turtle(), None).await;
    let (_d4, numbig) = setup_turtle("r1652:numbig", &composite_numbig_turtle(), None).await;
    let (_d5, list) = setup_list_ledger("r1652:list").await;
    let env = |l: Ledger| -> (&Fluree, &'static str) {
        match l {
            Ledger::Star => (&star, "r1652:star"),
            Ledger::Chain => (&chain, "r1652:chain"),
            Ledger::CompositeClean => (&clean, "r1652:clean"),
            Ledger::CompositeNumBig => (&numbig, "r1652:numbig"),
            Ledger::CompositeList => (&list, "r1652:list"),
        }
    };
    let cases = cases();

    // Phase 1 — fast paths on, under span capture so each answer can be
    // attributed to the site that served it.
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut fast: Vec<Vec<String>> = Vec::new();
    let mut proceeded: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let (fluree, alias) = env(c.ledger);
        let before = store.find_events("fast-path outcome").len();
        fast.push(normalize(&run_query(fluree, alias, c.sparql).await));
        proceeded.push(
            store.find_events("fast-path outcome")[before..]
                .iter()
                .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                .filter_map(|e| e.fields.get("site").cloned())
                .collect(),
        );
    }
    drop(tracing_guard);

    // Phase 2 — generic pipeline (the kill-switch reference).
    set_fast_paths_disabled(true);
    let mut generic: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let (fluree, alias) = env(c.ledger);
        generic.push(normalize(&run_query(fluree, alias, c.sparql).await));
    }
    set_fast_paths_disabled(false);

    let mut failures: Vec<String> = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        let expected: Vec<String> = c.expected.iter().map(|s| (*s).to_string()).collect();
        if fast[i] != expected {
            failures.push(format!(
                "{}: fast lane returned {:?}, expected {:?} [proceeded: {:?}]",
                c.name, fast[i], expected, proceeded[i]
            ));
        }
        if generic[i] != expected {
            failures.push(format!(
                "{}: generic pipeline returned {:?}, expected {:?} — the \
                 hand-pinned answer is wrong or the general pipeline regressed",
                c.name, generic[i], expected
            ));
        }
        match c.routing {
            Routing::MustFire(site) => {
                if !proceeded[i].iter().any(|s| s == site) {
                    failures.push(format!(
                        "{}: expected site `{site}` to proceed — the fix must \
                         keep the fast path, not disable it [proceeded: {:?}]",
                        c.name, proceeded[i]
                    ));
                }
            }
            Routing::MustNotFire(site) => {
                if proceeded[i].iter().any(|s| s == site) {
                    failures.push(format!(
                        "{}: site `{site}` proceeded on a shape it cannot \
                         answer [proceeded: {:?}]",
                        c.name, proceeded[i]
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "issue #1652 regression pins found {} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
