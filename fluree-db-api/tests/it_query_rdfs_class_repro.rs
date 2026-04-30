//! Regression for: COUNT `?s rdf:type rdfs:Class` returning 0 on bulk-imported
//! data while the parallel COUNT for `rdf:Property` and FILTER rewrites return
//! the correct count.
//!
//! Tracked in fluree/db#1208. The bug lives in `count_bound_object_v6` in
//! `fluree-db-query/src/fast_group_count_firsts.rs` — a POST-leaflet
//! directory-skip optimization that incorrectly skipped any leaflet whose
//! first key sorted below the target value, even when the leaflet contained
//! target rows mixed with other distinct objects. A second, related issue
//! around mixed-predicate leaflets is exercised by
//! `count_mixed_predicate_leaflet_regression` below.
#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::io::Write;
use tempfile::TempDir;

fn write_ttl(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create ttl file");
    f.write_all(content.as_bytes()).expect("write ttl");
    path
}

/// Synthetic RDFS ontology shape: classes declared with `a rdfs:Class`, a
/// few `rdf:Property` declarations, and a handful of instance triples that
/// share storage with the schema declarations after bulk import.
fn ontology_ttl() -> &'static str {
    r#"
@prefix ex:   <http://example.org/ns/> .
@prefix exd:  <http://example.org/data/> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

# Classes
ex:ClassA a rdfs:Class ; rdfs:label "Class A" .
ex:ClassB a rdfs:Class ; rdfs:label "Class B" .
ex:ClassC a rdfs:Class ; rdfs:label "Class C" .
ex:ClassD a rdfs:Class ; rdfs:label "Class D" .
ex:ClassE a rdfs:Class ; rdfs:label "Class E" .

# Properties
ex:propX a rdf:Property ;
    rdfs:label "Prop X" ;
    rdfs:domain ex:ClassA ;
    rdfs:range xsd:string .
ex:propY a rdf:Property ;
    rdfs:label "Prop Y" ;
    rdfs:domain ex:ClassA ;
    rdfs:range ex:ClassB .
ex:propZ a rdf:Property ;
    rdfs:label "Prop Z" ;
    rdfs:domain ex:ClassA ;
    rdfs:range ex:ClassC .

# Instance rows so the index has real `rdf:type` flakes mixed with the
# schema declarations.
exd:item-1 a ex:ClassA ; ex:propX "value-1" .
exd:item-2 a ex:ClassA ; ex:propX "value-2" .
exd:item-3 a ex:ClassA ; ex:propX "value-3" .
"#
}

async fn bulk_import_ontology() -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    write_ttl(data_dir.path(), "00-ontology.ttl", ontology_ttl());

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/rdfs-class-repro:main".to_string();

    let result = fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");
    assert!(result.t > 0);

    (db_dir, data_dir, fluree, ledger_id)
}

#[tokio::test]
async fn sparql_select_class_prefixed() {
    // SELECT ?c WHERE { ?c a rdfs:Class }
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?c WHERE { ?c a rdfs:Class }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(arr.len(), 5, "expected 5 classes, got {}", arr.len());
}

#[tokio::test]
async fn sparql_select_class_full_iris() {
    // SELECT ?c WHERE { ?c <rdf:type-IRI> <rdfs:Class-IRI> }
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        SELECT ?c WHERE {
            ?c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
               <http://www.w3.org/2000/01/rdf-schema#Class> .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(arr.len(), 5, "expected 5 classes, got {}", arr.len());
}

#[tokio::test]
async fn sparql_filter_rewrite_returns_same_count() {
    // FILTER form — semantically equivalent to the bound-object form;
    // returns the correct count even before the fix.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?c WHERE { ?c rdf:type ?t . FILTER(?t = rdfs:Class) }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(arr.len(), 5, "expected 5 classes, got {}", arr.len());
}

#[tokio::test]
async fn sparql_select_property_works() {
    // Control: the parallel COUNT/SELECT shape for rdf:Property — works
    // because rdf:Property happens to be the first row of its leaflet.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?p WHERE { ?p a rdf:Property }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(arr.len(), 3, "expected 3 properties, got {}", arr.len());
}

#[tokio::test]
async fn sparql_select_user_class_works() {
    // Control: a user-defined class — works for the same reason.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?c WHERE { ?c a ex:ClassA }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(arr.len(), 3, "expected 3 instances, got {}", arr.len());
}

#[tokio::test]
async fn jsonld_select_star_class() {
    // The exact JSON-LD shape originally reported as broken.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let q = json!({
        "@context": {
            "id": "@id",
            "type": "@type",
            "ex": "http://example.org/ns/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "where": {"@id": "?s", "@type": "rdfs:Class"},
        "select": {"?s": ["*"]}
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        support::normalize_rows(&rows).len(),
        5,
        "expected 5 classes (jsonld), got: {rows}"
    );
}

/// Regression for the leaflet first-key skip bug in `count_bound_object_v6`.
///
/// The fast path used to skip any leaflet whose first prefix sorted below the
/// target — but a leaflet's rows span `[first_prefix, next_leaflet_first_prefix)`,
/// so a target value can sit *inside* a leaflet whose first row is some smaller
/// value. The shape that triggers it: a single leaflet contains rdf:type rows
/// for multiple classes (e.g. `rdf:Property`, `rdfs:Class`, plus user classes).
/// A COUNT for any class that isn't the very first row of the leaflet would
/// silently return 0.
#[tokio::test]
async fn count_bound_object_first_key_skip_regression() {
    let db_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let mut ttl = String::from(
        "@prefix ex:   <http://example.org/ns/> .\n\
         @prefix exd:  <http://example.org/data/> .\n\
         @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n",
    );
    // Two well-known classes that come first in POST order (RDF/RDFS small
    // namespace codes encode to the smallest SubjectIds for rdf:type's range).
    ttl.push_str("ex:p1 a rdf:Property .\n");
    ttl.push_str("ex:p2 a rdf:Property .\n");
    ttl.push_str("ex:Class1 a rdfs:Class .\n");
    ttl.push_str("ex:Class2 a rdfs:Class .\n");
    ttl.push_str("ex:Class3 a rdfs:Class .\n");
    // User class with many instances, to push the rdf:type predicate's POST
    // range across multiple leaflets and put the small-cardinality classes at
    // a leaflet boundary.
    for i in 0..3000 {
        ttl.push_str(&format!("exd:big-{i} a ex:BigClass .\n"));
    }
    write_ttl(data_dir.path(), "00.ttl", &ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "test/first-key-skip:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(128)
        .leaflet_rows(1024)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.unwrap();

    for (label, sparql, want) in [
        (
            "rdfs:Class",
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
             SELECT (COUNT(?c) AS ?n) WHERE { ?c a rdfs:Class }",
            3,
        ),
        (
            "rdf:Property",
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
             SELECT (COUNT(?p) AS ?n) WHERE { ?p a rdf:Property }",
            2,
        ),
        (
            "ex:BigClass",
            "PREFIX ex: <http://example.org/ns/> \
             SELECT (COUNT(?s) AS ?n) WHERE { ?s a ex:BigClass }",
            3000,
        ),
    ] {
        let r = support::query_sparql(&fluree, &ledger, sparql)
            .await
            .unwrap()
            .to_sparql_json(&ledger.snapshot)
            .unwrap();
        let got: i64 = r["results"]["bindings"][0]["n"]["value"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(got, want, "COUNT {label}: expected {want}, got {got}");
    }
}

/// Regression for the mixed-predicate leaflet branch of `count_bound_object_v6`
/// and `group_count_v6`.
///
/// When a leaflet straddles a `p_id` boundary, its directory entry has
/// `p_const = None`. The pre-fix code skipped any such leaflet via
/// `entry.p_const != Some(p_id)`, which dropped target rows that lived in
/// boundary leaflets. The fix lets mixed-predicate leaflets fall through to
/// the row-level scan with a per-row `p_id` check. This test forces that
/// shape by interleaving multiple predicates in the smallest possible
/// dataset: a small number of `rdf:type` rows immediately followed by a
/// small number of `rdfs:label` rows — small enough that they share a single
/// leaflet, with `p_const = None`.
#[tokio::test]
async fn count_mixed_predicate_leaflet_regression() {
    let db_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let mut ttl = String::from(
        "@prefix ex:   <http://example.org/ns/> .\n\
         @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n",
    );
    // 5 rdf:type rows + 5 rdfs:label rows on the same subjects. PSOT sorts by
    // (p_id, s_id, ...); rdf:type and rdfs:label use distinct p_ids that are
    // adjacent in the predicate dictionary, so a single leaflet spanning both
    // predicates is plausible. POST sorts by (p_id, o_type, o_key, s_id),
    // putting rdf:type rows and rdfs:label rows in the same predicate region
    // when one leaflet straddles the boundary.
    for i in 1..=5 {
        ttl.push_str(&format!(
            "ex:item-{i} a rdfs:Class ; rdfs:label \"Item {i}\" .\n"
        ));
    }
    write_ttl(data_dir.path(), "00.ttl", &ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "test/mixed-predicate-leaflet:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(128)
        // Tiny leaflet target so the 10 total rows land in one leaflet that
        // spans rdf:type AND rdfs:label predicates.
        .leaflet_rows(64)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.unwrap();

    // COUNT for rdfs:Class — bound predicate is `rdf:type`, bound object is
    // `rdfs:Class`. Exercises `count_bound_object_v6` against a leaflet that
    // may straddle p_id=rdf:type and p_id=rdfs:label.
    let r = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
         SELECT (COUNT(?c) AS ?n) WHERE { ?c a rdfs:Class }",
    )
    .await
    .unwrap()
    .to_sparql_json(&ledger.snapshot)
    .unwrap();
    let got: i64 = r["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        got, 5,
        "COUNT rdfs:Class across mixed leaflet: expected 5, got {got}"
    );

    // SELECT form for the same shape (sanity check via the BinaryScanOperator
    // path, which doesn't depend on the V6 fast path).
    let bindings = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
         SELECT ?c WHERE { ?c a rdfs:Class }",
    )
    .await
    .unwrap()
    .to_sparql_json(&ledger.snapshot)
    .unwrap();
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "SELECT rdfs:Class across mixed leaflet: expected 5, got {}",
        arr.len()
    );
}
