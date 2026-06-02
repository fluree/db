//! Regression tests for class stats and txn-meta output of the bulk import path
//! (`fluree.create(id).import(path).execute()`).
//!
//! These tests demonstrate two distinct bugs observed on enterprise ontologies:
//!
//! 1. `ref-classes` capped at 64 distinct target classes (`ClassBitsetTable`),
//!    producing incomplete schema-discovery output on ledgers with many classes.
//! 2. `txn-meta` graph reports 0 flakes in per-graph stats even though the
//!    import path builds a g_id=1 meta chunk. This test splits the question
//!    into "is the data queryable?" vs "do stats reflect it?" so we know
//!    whether the bug is in named-graph routing or in stats collection.
//!
//! Both tests are expected to FAIL on the current code and PASS once the fixes
//! land.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::{
    range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
    TXN_META_GRAPH_ID,
};
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};
use std::collections::HashSet;
use std::io::Write;

fn write_file(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create file");
    f.write_all(content.as_bytes()).expect("write file");
    path
}

/// Generate a TTL string with `n_classes` distinct classes, each with one
/// instance, and ref edges that form a cycle across all classes.
///
/// Class `C0` → `C1`, `C1` → `C2`, …, `C_{n-1}` → `C0`. This guarantees that
/// every class is referenced as a target by exactly one property, so an
/// uncapped ref-class rollup must see `n` distinct target classes. With the
/// 64-cap, only the first ~64 (in encounter order) will appear.
fn generate_many_class_ttl(n_classes: usize) -> String {
    let mut out = String::new();
    out.push_str("@prefix ex: <http://example.org/> .\n");
    out.push_str("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n");
    for i in 0..n_classes {
        let next = (i + 1) % n_classes;
        out.push_str(&format!(
            "ex:s{i} a ex:C{i} ;\n    ex:linksTo ex:s{next} .\n\n",
        ));
    }
    out
}

// =============================================================================
// Test 1: ref-classes coverage on >64-class ledgers
// =============================================================================

/// Regression: bulk import of a ledger with more than 64 distinct classes
/// must report ref-class targets for every referenced class, not just the
/// first 64 encountered.
///
/// Currently fails: `ClassBitsetTable` caps the bitset width at 64, so only
/// the first ~64 target classes ever appear in any `ref_classes` value.
#[tokio::test]
async fn bulk_import_ref_classes_covers_more_than_64_classes() {
    const N_CLASSES: usize = 80;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    let ttl = generate_many_class_ttl(N_CLASSES);
    let ttl_path = write_file(data_dir.path(), "many_classes.ttl", &ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");

    fluree
        .create("test/many-classes:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(256)
        .collect_id_stats(true)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree.ledger("test/many-classes:main").await.expect("load");
    let stats = ledger
        .snapshot
        .stats
        .as_ref()
        .expect("stats should be present");
    let graphs = stats.graphs.as_ref().expect("graphs should be present");
    let default_graph = graphs
        .iter()
        .find(|g| g.g_id == 0)
        .expect("default graph should be present");
    let classes = default_graph
        .classes
        .as_ref()
        .expect("default graph should have classes");

    // Sanity: we got at least N_CLASSES class entries (one per distinct rdf:type).
    assert!(
        classes.len() >= N_CLASSES,
        "expected at least {N_CLASSES} class entries, got {}",
        classes.len()
    );

    // Collect every distinct (source, property, target) class triple across the
    // ref_classes rollup. The cap shows up as "fewer than N_CLASSES distinct
    // targets" — every class is the target of `ex:linksTo` exactly once, so an
    // uncapped rollup must show all N_CLASSES distinct targets.
    let mut distinct_targets: HashSet<Sid> = HashSet::new();
    let mut total_target_appearances = 0usize;
    for class in classes {
        for prop in &class.properties {
            for target in &prop.ref_classes {
                distinct_targets.insert(target.class_sid.clone());
                total_target_appearances += 1;
            }
        }
    }

    assert!(
        distinct_targets.len() >= N_CLASSES,
        "expected ref-class rollup to cover all {N_CLASSES} target classes, \
         got {} distinct targets ({} total appearances). \
         This is the 64-class ClassBitsetTable cap leaking into stats.",
        distinct_targets.len(),
        total_target_appearances
    );
}

// =============================================================================
// Test 2: txn-meta queryable AND counted
// =============================================================================

/// Regression: after a bulk import, the `#txn-meta` named graph must
/// (a) contain queryable commit-metadata flakes, and
/// (b) be reflected in per-graph stats with non-zero flake counts.
///
/// Splitting the question: if (a) passes but (b) fails, the bug is in stats
/// collection only; if (a) also fails, the meta chunk isn't landing in the
/// final index at all.
#[tokio::test]
async fn bulk_import_emits_queryable_txn_meta_with_stats() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice a ex:Person ;
    ex:name \"Alice\" .

ex:bob a ex:Person ;
    ex:name \"Bob\" .
";
    let ttl_path = write_file(data_dir.path(), "people.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");

    let result = fluree
        .create("test/txn-meta-import:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(256)
        .collect_id_stats(true)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    assert!(result.t > 0, "import should produce at least one commit");
    let import_t = result.t;

    let ledger = fluree
        .ledger("test/txn-meta-import:main")
        .await
        .expect("load");

    // (a) The txn-meta named graph must contain queryable commit-metadata.
    // We probe the POST index for `db:t == import_t` scoped to the txn-meta
    // graph id, matching the pattern in it_graph_commit.rs.
    let predicate = Sid::new(FLUREE_DB, fluree_vocab::db::T);
    let range_match = RangeMatch::predicate_object(predicate, FlakeValue::Long(import_t));
    let opts = RangeOptions::default()
        .with_to_t(ledger.t())
        .with_flake_limit(16);
    let flakes = range_with_overlay(
        &ledger.snapshot,
        TXN_META_GRAPH_ID,
        ledger.novelty.as_ref(),
        IndexType::Post,
        RangeTest::Eq,
        range_match,
        opts,
    )
    .await
    .expect("txn-meta POST lookup");

    let queryable_meta = flakes.iter().any(|f| {
        f.p.namespace_code == FLUREE_DB
            && f.p.name.as_ref() == fluree_vocab::db::T
            && f.o == FlakeValue::Long(import_t)
            && f.s.namespace_code == FLUREE_COMMIT
    });
    assert!(
        queryable_meta,
        "after bulk import, the txn-meta graph should contain a `db:t` flake \
         for the import commit (t={import_t}); got {} flakes: {flakes:?}",
        flakes.len()
    );

    // (b) Per-graph stats must reflect those flakes.
    let stats = ledger.snapshot.stats.as_ref().expect("stats");
    let graphs = stats.graphs.as_ref().expect("graphs");
    let txn_meta_entry = graphs.iter().find(|g| g.g_id == TXN_META_GRAPH_ID);
    assert!(
        txn_meta_entry.is_some(),
        "per-graph stats should contain an entry for txn-meta (g_id={TXN_META_GRAPH_ID})"
    );
    let txn_meta_entry = txn_meta_entry.unwrap();
    assert!(
        txn_meta_entry.flakes > 0,
        "per-graph stats for txn-meta should report > 0 flakes after bulk import, got {}. \
         If the queryable assertion above passed, this is a stats omission \
         in the bulk-import path (meta chunk produced but not counted).",
        txn_meta_entry.flakes
    );
}

// =============================================================================
// Test 3: per-class stats for user-defined NAMED graphs
// =============================================================================

/// Regression: a TriG bulk import must populate per-class stats for
/// user-defined named graphs (g_id >= FIRST_USER_GRAPH_ID), not just the
/// default graph.
///
/// Before the SpotClassStats merge, each named-graph build pass computed its
/// class stats and then discarded them (`let (ng_result, _) = …`), so
/// `stats.graphs[named].classes` was `None`. This test imports a named graph
/// with two classes and a same-graph reference and asserts the class stats —
/// including the resolved ref-target class — land on the named graph's entry.
#[tokio::test]
async fn bulk_import_populates_named_graph_class_stats() {
    use fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // Default graph: one Person. Named graph <…/g/hr>: an Employee that
    // `worksAt` a Company (a same-graph ref → must resolve to a target class).
    let trig = "\
@prefix ex: <http://example.org/> .

ex:alice a ex:Person ;
    ex:name \"Alice\" .

GRAPH <http://example.org/g/hr> {
    ex:bob a ex:Employee ;
        ex:name \"Bob\" ;
        ex:worksAt ex:acme .
    ex:acme a ex:Company ;
        ex:name \"Acme\" .
}
";
    let trig_path = write_file(data_dir.path(), "named.trig", trig);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");

    fluree
        .create("test/named-class-stats:main")
        .import(&trig_path)
        .threads(1)
        .memory_budget_mb(256)
        .collect_id_stats(true)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree
        .ledger("test/named-class-stats:main")
        .await
        .expect("load");
    let stats = ledger.snapshot.stats.as_ref().expect("stats present");
    let graphs = stats.graphs.as_ref().expect("graphs present");

    // Default graph keeps exactly its own class (Person), unaffected.
    let g0 = graphs.iter().find(|g| g.g_id == 0).expect("default graph");
    let g0_classes = g0.classes.as_ref().expect("default graph has classes");
    assert_eq!(
        g0_classes.len(),
        1,
        "default graph should have 1 class (Person)"
    );

    // The user-defined named graph must now carry its own class stats.
    let named = graphs
        .iter()
        .find(|g| g.g_id >= FIRST_USER_GRAPH_ID)
        .expect("a user-defined named graph entry should exist");
    let named_classes = named
        .classes
        .as_ref()
        .expect("named graph should have class stats (was None before the merge)");
    assert_eq!(
        named_classes.len(),
        2,
        "named graph should have 2 classes (Employee, Company); got {named_classes:?}"
    );

    // The same-graph ref (Employee ex:worksAt → Company) must resolve to a
    // target class exactly once. Exactly one class (Employee) has a property
    // with a non-empty ref-class rollup.
    let ref_rollup: Vec<&fluree_db_core::ClassRefCount> = named_classes
        .iter()
        .flat_map(|c| c.properties.iter())
        .flat_map(|p| p.ref_classes.iter())
        .collect();
    assert_eq!(
        ref_rollup.len(),
        1,
        "exactly one same-graph ref-target class (Employee.worksAt → Company); got {ref_rollup:?}"
    );
    assert_eq!(
        ref_rollup[0].count, 1,
        "worksAt → Company should be counted once"
    );
}

// =============================================================================
// Test 3: rdf:type inner-star COUNT(*) served from per-(class,property) stats
// =============================================================================

/// The rdf:type-star metadata fold serves `?s rdf:type ?o1 . ?s P ?o2` COUNT(*) =
/// Σ_C Σ_dt classStat[C][P].count directly from the per-(class,property) datatype
/// stats (no scan). Bulk import produces exact stats here.
/// ex:a types {C1,C2} p={1,2,3} => 2*3=6 ; ex:b type {C1} p={4} => 1 ;
/// ex:c type {C2} no p => 0 ; ex:d p={5} no type => 0  => 7.
#[tokio::test]
async fn bulk_import_rdf_type_star_count_from_class_stats() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    let ttl = "@prefix ex: <http://example.org/> .\n\
               @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
               ex:a a ex:C1, ex:C2 ; ex:p 1, 2, 3 .\n\
               ex:b a ex:C1 ; ex:p 4 .\n\
               ex:c a ex:C2 .\n\
               ex:d ex:p 5 .\n";
    let ttl_path = write_file(data_dir.path(), "typestar.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");
    fluree
        .create("test/typestar:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(256)
        .collect_id_stats(true)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger("test/typestar:main").await.expect("load");

    let q = "PREFIX ex: <http://example.org/>\n\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 . ?s ex:p ?o2 }";
    let result = support::query_sparql(&fluree, &ledger, q)
        .await
        .expect("typestar query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&jsonld),
        support::normalize_rows(&serde_json::json!([[7]])),
        "Σ_C count(C, ex:p) = C1:(3+1) + C2:(3) = 7"
    );
}
