//! Reproduction for: querying `?s rdf:type rdfs:Class` via SPARQL after a
//! TTL bulk import returns 0, even though the triples are present.
//! Source notes: /tmp/fluree-bug-rdfs-class-invisibility.md
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

/// Simulate the Hometap ontology shape: classes declared with `a rdfs:Class`
/// and properties declared with `a rdf:Property`, mixed with instance data.
fn ontology_ttl() -> &'static str {
    r#"
@prefix ht:   <https://ns.hometap.com/v1#> .
@prefix htd:  <https://data.hometap.com/> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

# Classes
ht:CalendarDay a rdfs:Class ; rdfs:label "Calendar Day" .
ht:Inquiry a rdfs:Class ; rdfs:label "Inquiry" .
ht:Opportunity a rdfs:Class ; rdfs:label "Opportunity" .
ht:Person a rdfs:Class ; rdfs:label "Person" .
ht:Home a rdfs:Class ; rdfs:label "Home" .

# Properties
ht:friendlyId a rdf:Property ;
    rdfs:label "Friendly ID" ;
    rdfs:domain ht:Inquiry ;
    rdfs:range xsd:string .
ht:home a rdf:Property ;
    rdfs:label "Home" ;
    rdfs:domain ht:Inquiry ;
    rdfs:range ht:Home .
ht:person a rdf:Property ;
    rdfs:label "Person" ;
    rdfs:domain ht:Inquiry ;
    rdfs:range ht:Person .

# A handful of instance triples to mirror the user's scenario where there
# are real Inquiry rows sharing storage with the rdfs:Class declarations.
htd:inquiry-1 a ht:Inquiry ; ht:friendlyId "I-1" .
htd:inquiry-2 a ht:Inquiry ; ht:friendlyId "I-2" .
htd:inquiry-3 a ht:Inquiry ; ht:friendlyId "I-3" .
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
async fn sparql_a_prefixed_a_rdfs_class() {
    // Query A: SELECT ?c WHERE { ?c a rdfs:Class }
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
    eprintln!("[A] = {}", serde_json::to_string_pretty(&bindings).unwrap());
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "expected 5 classes via Query A, got {}",
        arr.len()
    );
}

#[tokio::test]
async fn sparql_b_full_iri_rdf_type_rdfs_class() {
    // Query B: SELECT ?c WHERE { ?c <rdf:type> <rdfs:Class> } (full IRIs)
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
    eprintln!("[B] = {}", serde_json::to_string_pretty(&bindings).unwrap());
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "expected 5 classes via Query B, got {}",
        arr.len()
    );
}

#[tokio::test]
async fn sparql_c_filter_rewrite_works() {
    // Query C (the rewrite that the user reports works): FILTER form.
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
    eprintln!("[C] = {}", serde_json::to_string_pretty(&bindings).unwrap());
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "expected 5 classes via Query C, got {}",
        arr.len()
    );
}

#[tokio::test]
async fn sparql_d_group_by_type() {
    // Query D: ?s ?p ?o GROUP BY ?t — should show rdfs:Class and rdf:Property
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?t (COUNT(?s) AS ?n) WHERE { ?s rdf:type ?t } GROUP BY ?t
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[D] = {}", serde_json::to_string_pretty(&bindings).unwrap());
}

#[tokio::test]
async fn sparql_e_describe_inquiry_class() {
    // Query E: get all triples for ht:Inquiry (the class itself)
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX ht: <https://ns.hometap.com/v1#>
        SELECT ?p ?o WHERE { ht:Inquiry ?p ?o }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[E] = {}", serde_json::to_string_pretty(&bindings).unwrap());
}

#[tokio::test]
async fn sparql_f_a_rdf_property_works() {
    // Query F (control): ?p a rdf:Property — the user reports this WORKS.
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
    eprintln!("[F] = {}", serde_json::to_string_pretty(&bindings).unwrap());
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        3,
        "expected 3 properties via Query F, got {}",
        arr.len()
    );
}

/// Regression for the COUNT-by-class fast-path leaflet-skip bug:
/// `count_bound_object_v6` (in `fast_group_count_firsts.rs`) used to skip
/// any leaflet whose first prefix was below the target, even when the
/// leaflet contained target rows mixed with other distinct objects.
///
/// The shape that triggers it: a single leaflet contains rdf:type rows for
/// multiple classes — e.g. `rdf:Property`, `rdfs:Class`, plus several user
/// classes. Querying COUNT for any class that isn't first in the leaflet
/// would return 0.
#[tokio::test]
async fn count_bound_object_mixed_leaflet_regression() {
    // Build a TTL where many classes share a single leaflet (POST sorts by
    // (p_id, o_type, o_key); rdf:type ns_codes are encoded as RDF/RDFS/owl
    // with small SubjectIds and user namespaces with larger ones).
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
    // User classes with many instances each, to force the rdf:type predicate's
    // POST range across multiple leaflets and put the small-cardinality classes
    // at a leaflet boundary.
    for i in 0..3000 {
        ttl.push_str(&format!("exd:big-{i} a ex:BigClass .\n"));
    }
    write_ttl(data_dir.path(), "00.ttl", &ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "test/mixed-leaflet-regression:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(128)
        .leaflet_rows(1024) // small leaflets so rdfs:Class lands inside a mixed-class leaflet
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.unwrap();

    // The fast-path COUNT operator: bound predicate (rdf:type), bound object.
    let count_class = r"
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(?c) AS ?n) WHERE { ?c a rdfs:Class }
    ";
    let r = support::query_sparql(&fluree, &ledger, count_class)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let n: i64 = r["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n, 3, "expected COUNT rdfs:Class = 3, got {n}");

    let count_prop = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT (COUNT(?p) AS ?n) WHERE { ?p a rdf:Property }
    ";
    let r = support::query_sparql(&fluree, &ledger, count_prop)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let n: i64 = r["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n, 2, "expected COUNT rdf:Property = 2, got {n}");

    let count_user = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT (COUNT(?s) AS ?n) WHERE { ?s a ex:BigClass }
    ";
    let r = support::query_sparql(&fluree, &ledger, count_user)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let n: i64 = r["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n, 3000, "expected COUNT ex:BigClass = 3000, got {n}");
}

#[tokio::test]
async fn sparql_g_a_user_class_works() {
    // Query G (control): ?c a ht:Inquiry — user-defined class, user reports works.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let sparql = r"
        PREFIX ht: <https://ns.hometap.com/v1#>
        SELECT ?c WHERE { ?c a ht:Inquiry }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[G] = {}", serde_json::to_string_pretty(&bindings).unwrap());
    let arr = bindings["results"]["bindings"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        3,
        "expected 3 instances via Query G, got {}",
        arr.len()
    );
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_diagnostic() {
    use fluree_db_core::Sid;
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        return;
    }

    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    let ledger_id = "test/hometap-diag:main";
    fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let snap = &ledger.snapshot;

    // 1) Show the snapshot's namespace_codes registration for the relevant prefixes.
    eprintln!("\n=== namespace_codes ===");
    for code in [3u16, 4u16] {
        eprintln!("  code {} → {:?}", code, snap.namespaces().get(&code));
    }
    // Also dump any *-allocated codes whose prefix mentions rdf or rdf-schema
    for (code, prefix) in snap.namespaces() {
        if prefix.contains("rdf") {
            eprintln!("  code {code} → {prefix:?}");
        }
    }

    // 2) Encode the two object IRIs and print the Sid each yields.
    let class_iri = "http://www.w3.org/2000/01/rdf-schema#Class";
    let property_iri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
    let class_sid = snap.encode_iri(class_iri);
    let property_sid = snap.encode_iri(property_iri);
    eprintln!("\n=== encode_iri at query time ===");
    eprintln!("  rdfs:Class    → {class_sid:?}");
    eprintln!("  rdf:Property → {property_sid:?}");

    // 3) Look at the stored class_counts (subject/o_key index) to see how the
    //    importer encoded it on the way in. If we can directly look at one of
    //    the actual stored type triples for ht:Inquiry, we can compare.
    let s_inquiry = snap.encode_iri("https://ns.hometap.com/v1#Inquiry");
    eprintln!("  ht:Inquiry    → {s_inquiry:?}");

    // 4) Scan the OPST cursor for (rdf:type, rdfs:Class) and dump what we find.
    use fluree_db_core::{
        range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest,
    };
    let rdf_type_sid = snap
        .encode_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        .unwrap();
    let class_sid_unwrapped = class_sid.clone().unwrap();
    let property_sid_unwrapped = property_sid.clone().unwrap();
    eprintln!("\n=== rdf:type SID ===");
    eprintln!("  rdf:type → {rdf_type_sid:?}");

    let novelty = ledger.novelty.as_ref();

    let opts_cap = RangeOptions::default().with_flake_limit(20);

    // OPST scan for the specific (rdf:type, rdfs:Class) shape via OPST
    let class_range = range_with_overlay(
        snap,
        0u16,
        novelty,
        IndexType::Opst,
        RangeTest::Eq,
        RangeMatch {
            p: Some(rdf_type_sid.clone()),
            o: Some(FlakeValue::Ref(class_sid_unwrapped.clone())),
            ..Default::default()
        },
        opts_cap.clone(),
    )
    .await
    .expect("opst class");
    eprintln!(
        "\n=== OPST(rdf:type, rdfs:Class) range_with_overlay → {} flakes (cap 20) ===",
        class_range.len()
    );
    for f in class_range.iter().take(5) {
        eprintln!("  flake: s={:?} p={:?} o={:?}", f.s, f.p, f.o);
    }

    // OPST scan for (rdf:type, rdf:Property) — the working case
    let prop_range = range_with_overlay(
        snap,
        0u16,
        novelty,
        IndexType::Opst,
        RangeTest::Eq,
        RangeMatch {
            p: Some(rdf_type_sid.clone()),
            o: Some(FlakeValue::Ref(property_sid_unwrapped.clone())),
            ..Default::default()
        },
        opts_cap.clone(),
    )
    .await
    .expect("opst prop");
    eprintln!(
        "\n=== OPST(rdf:type, rdf:Property) range_with_overlay → {} flakes (cap 20) ===",
        prop_range.len()
    );
    for f in prop_range.iter().take(5) {
        eprintln!("  flake: s={:?} p={:?} o={:?}", f.s, f.p, f.o);
    }

    // 4b) Check find_subject_id_by_parts directly — this is what
    //     value_to_otype_okey_simple uses for FlakeValue::Ref encoding.
    let store_te = ledger.binary_store.as_ref();
    if let Some(te) = store_te {
        if let Ok(bs) =
            te.0.clone()
                .downcast::<fluree_db_binary_index::BinaryIndexStore>()
        {
            eprintln!("\n=== find_subject_id_by_parts ===");
            let r1 = bs.find_subject_id_by_parts(
                class_sid_unwrapped.namespace_code,
                &class_sid_unwrapped.name,
            );
            eprintln!("  rdfs:Class (4, Class)      → {r1:?}");
            let r2 = bs.find_subject_id_by_parts(
                property_sid_unwrapped.namespace_code,
                &property_sid_unwrapped.name,
            );
            eprintln!("  rdf:Property (3, Property) → {r2:?}");
            let r3 = bs.find_subject_id_by_parts(13, "Inquiry");
            eprintln!("  ht:Inquiry  (13, Inquiry)  → {r3:?}");

            // CRITICAL: Compare snapshot.encode_iri vs store.encode_iri.
            // The decode path uses store.encode_iri(decoded_iri) to produce
            // FlakeValue::Ref. If that encoding doesn't match the bound_o
            // (which uses snapshot.encode_iri), the row filter at
            // binary_scan.rs:1168 will reject every row.
            eprintln!("\n=== encode_iri comparison (snapshot vs store) ===");
            for iri in [class_iri, property_iri, "https://ns.hometap.com/v1#Inquiry"] {
                let snap_sid = snap.encode_iri(iri);
                let store_sid = bs.encode_iri(iri);
                eprintln!("  iri = {iri}");
                eprintln!("    snapshot.encode_iri → {snap_sid:?}");
                eprintln!("    store.encode_iri    → {store_sid:?}");
                if snap_sid.as_ref() != Some(&store_sid) {
                    eprintln!("    *** MISMATCH ***");
                }
            }
            // Also: does the store's namespace_reverse table contain rdfs/rdf?
            eprintln!("\n=== store namespace registrations ===");
            for iri in [
                "http://www.w3.org/2000/01/rdf-schema#",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "https://ns.hometap.com/v1#",
            ] {
                use fluree_db_core::ns_encoding::NsLookup;
                eprintln!("  {iri:60} → {:?}", bs.code_for_prefix(iri));
            }
        }
    } else {
        eprintln!("  no binary_store available");
    }

    // 5) Scan POST for ALL `rdf:type` triples (no object) and group by object Sid
    let all_types = range_with_overlay(
        snap,
        0u16,
        novelty,
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch {
            p: Some(rdf_type_sid.clone()),
            ..Default::default()
        },
        RangeOptions::default().with_flake_limit(2_000_000),
    )
    .await
    .expect("post all rdf:type");
    eprintln!(
        "\n=== POST(rdf:type, *) → {} total flakes ===",
        all_types.len()
    );
    let mut counts: std::collections::HashMap<Sid, u64> = std::collections::HashMap::new();
    for f in &all_types {
        if let FlakeValue::Ref(o) = &f.o {
            *counts.entry(o.clone()).or_insert(0) += 1;
        }
    }
    let mut sorted: Vec<_> = counts.iter().collect();
    sorted.sort_by_key(|(_sid, n)| std::cmp::Reverse(**n));
    for (sid, n) in sorted.iter().take(20) {
        eprintln!(
            "   ({}) sid={:?} → decoded: {:?}",
            n,
            sid,
            snap.decode_sid(sid)
        );
    }
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_a_count_only() {
    // ONLY runs Query A (COUNT) so we can isolate the operator-tree decisions.
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        return;
    }
    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let ledger_id = "test/hometap-count-a:main";
    fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .unwrap();

    let ledger = fluree.ledger(ledger_id).await.unwrap();
    eprintln!("\n========== Query A only (COUNT) ==========\n");
    let q_a = r"
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(?c) AS ?n) WHERE { ?c a rdfs:Class }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, q_a)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!(
        "\n[A only] = {}",
        serde_json::to_string_pretty(&bindings).unwrap()
    );
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_a_select_only() {
    // Same as Query A but as SELECT ?c (no COUNT) so we hit BinaryScanOperator
    // directly rather than fast_count.
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        return;
    }
    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let ledger_id = "test/hometap-select:main";
    fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .unwrap();

    let ledger = fluree.ledger(ledger_id).await.unwrap();

    let q_a = r"
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?c WHERE { ?c a rdfs:Class }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, q_a)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr_a = bindings["results"]["bindings"].as_array().unwrap();
    eprintln!("\n=== A SELECT result count = {} ===", arr_a.len());

    let q_f = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?p WHERE { ?p a rdf:Property }
    ";
    let bindings_f = support::query_sparql(&fluree, &ledger, q_f)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    let arr_f = bindings_f["results"]["bindings"].as_array().unwrap();
    eprintln!("\n=== F SELECT result count = {} ===", arr_f.len());

    eprintln!("A={} F={}", arr_a.len(), arr_f.len());
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_a_with_reasoning_off() {
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        return;
    }
    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let ledger_id = "test/hometap-noreason:main";
    fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .unwrap();

    let ledger = fluree.ledger(ledger_id).await.unwrap();

    // Same Query A, but with reasoning explicitly disabled.
    let q = json!({
        "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
        "where": {"@id": "?c", "@type": "rdfs:Class"},
        "select": "?c",
        "reasoning": "none"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    eprintln!(
        "[A reasoning:none] = {}",
        serde_json::to_string_pretty(&rows).unwrap()
    );
    let arr = rows.as_array().expect("array");
    assert_eq!(
        arr.len(),
        16,
        "expected 16 classes with reasoning off, got {}",
        arr.len()
    );
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_a_with_explain() {
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        return;
    }
    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let ledger_id = "test/hometap-explain:main";
    fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .unwrap();

    let ledger = fluree.ledger(ledger_id).await.unwrap();

    // Query A: explain
    let q_a = json!({
        "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
        "where": {"@id": "?c", "@type": "rdfs:Class"},
        "select": "?c"
    });
    let q_f = json!({
        "@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        "where": {"@id": "?p", "@type": "rdf:Property"},
        "select": "?p"
    });

    for (label, q) in [("A rdfs:Class", q_a), ("F rdf:Property", q_f)] {
        eprintln!("\n=== EXPLAIN {label} ===");
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let res = fluree.explain(&db, &q).await;
        eprintln!("explain result = {res:#?}");
    }
}

#[tokio::test]
#[ignore = "uses local user data at ~/Downloads/hometap"]
async fn user_actual_dataset_full() {
    let data_dir = std::path::PathBuf::from(format!(
        "{}/Downloads/hometap",
        std::env::var("HOME").unwrap()
    ));
    if !data_dir.exists() {
        eprintln!("data dir missing, skipping");
        return;
    }

    let db_dir = TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/hometap-real:main";
    let result = fluree
        .create(ledger_id)
        .import(&data_dir)
        .threads(2)
        .memory_budget_mb(1024)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    eprintln!("imported t={}, flakes={}", result.t, result.flake_count);

    let ledger = fluree.ledger(ledger_id).await.expect("load");

    // Query A: SELECT ?c WHERE { ?c a rdfs:Class }
    let q_a = r"
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(?c) AS ?n) WHERE { ?c a rdfs:Class }
    ";
    let a = support::query_sparql(&fluree, &ledger, q_a)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[A real] = {}", serde_json::to_string_pretty(&a).unwrap());

    // Query B: full IRIs
    let q_b = r"
        SELECT (COUNT(?c) AS ?n) WHERE {
            ?c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
               <http://www.w3.org/2000/01/rdf-schema#Class> .
        }
    ";
    let b = support::query_sparql(&fluree, &ledger, q_b)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[B real] = {}", serde_json::to_string_pretty(&b).unwrap());

    // Query C: FILTER form (works for user)
    let q_c = r"
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(?c) AS ?n) WHERE { ?c rdf:type ?t . FILTER(?t = rdfs:Class) }
    ";
    let c = support::query_sparql(&fluree, &ledger, q_c)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[C real] = {}", serde_json::to_string_pretty(&c).unwrap());

    // Query F (control): ?p a rdf:Property — works for user
    let q_f = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT (COUNT(?p) AS ?n) WHERE { ?p a rdf:Property }
    ";
    let f = support::query_sparql(&fluree, &ledger, q_f)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[F real] = {}", serde_json::to_string_pretty(&f).unwrap());

    // Query D: GROUP BY type to confirm object IRI seen by query engine
    let q_d = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?t (COUNT(?s) AS ?n) WHERE { ?s rdf:type ?t } GROUP BY ?t
    ";
    let d = support::query_sparql(&fluree, &ledger, q_d)
        .await
        .unwrap()
        .to_sparql_json(&ledger.snapshot)
        .unwrap();
    eprintln!("[D real] = {}", serde_json::to_string_pretty(&d).unwrap());

    let count_of = |v: &serde_json::Value| -> i64 {
        v["results"]["bindings"][0]["n"]["value"]
            .as_str()
            .unwrap_or("0")
            .parse()
            .unwrap_or(0)
    };
    let a_n = count_of(&a);
    let b_n = count_of(&b);
    let c_n = count_of(&c);
    let f_n = count_of(&f);
    eprintln!("counts: A={a_n} B={b_n} C={c_n} F={f_n}");

    // We expect A, B, and C to all match — they're semantically identical.
    assert_eq!(
        a_n, c_n,
        "Query A ({a_n}) should match FILTER rewrite C ({c_n})"
    );
    assert_eq!(
        b_n, c_n,
        "Query B ({b_n}) should match FILTER rewrite C ({c_n})"
    );
}

#[tokio::test]
async fn jsonld_form_user_query() {
    // The exact JSON-LD form the user reported broken in the original message.
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_ontology().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let q = json!({
        "@context": {
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "where": {"@id": "?s", "@type": "rdfs:Class"},
        "select": {"?s": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("jsonld query")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    eprintln!(
        "[jsonld] = {}",
        serde_json::to_string_pretty(&result).unwrap()
    );
    let normalized = support::normalize_rows(&result);
    assert_eq!(
        normalized.len(),
        5,
        "expected 5 rdfs:Class subjects (jsonld), got {}: {:?}",
        normalized.len(),
        normalized
    );
}
