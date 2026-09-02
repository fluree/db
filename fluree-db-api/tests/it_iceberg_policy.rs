//! View-policy enforcement on an Iceberg (R2RML) graph source.
//!
//! Drives the committed local fixture `tests/fixtures/iceberg/silver/people`
//! (5 rows: alice..erin, `id`/`name`/`score`) through the same query builder
//! the server uses, with inline policies in `opts`, and checks every static
//! policy shape against the SAME policy on a native ledger holding identical
//! data — the native scan is the oracle for what a virtual source must return.
//!
//! `f:query` is the deliberate exception: a virtual source has no graph to run
//! a policy query against, so it fails closed where native would evaluate it.

#![cfg(all(feature = "iceberg", feature = "native"))]

use fluree_db_api::{FlureeBuilder, R2rmlCreateConfig};
use serde_json::{json, Value};

const PEOPLE_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#PeopleMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ; rr:class ex:Person ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:score ; rr:objectMap [ rr:column "score" ] ] .
"#;

/// `rdf:type` derived from a column (one class per person), no `rr:class`.
const KINDS_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#KindMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate rdf:type ;
            rr:objectMap [ rr:template "http://example.org/kind/{name}" ; rr:termType rr:IRI ]
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

const GS: &str = "local-people:main";
const KINDS_GS: &str = "local-kinds:main";
const NATIVE: &str = "native-people:main";

fn table_location() -> String {
    format!(
        "file://{}/tests/fixtures/iceberg/silver/people",
        env!("CARGO_MANIFEST_DIR")
    )
}

async fn setup() -> fluree_db_api::Fluree {
    let loc = table_location();
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        loc.strip_prefix("file://").unwrap_or(&loc),
    );
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, mapping) in [("local-people", PEOPLE_R2RML), ("local-kinds", KINDS_R2RML)] {
        let cfg = R2rmlCreateConfig::new_direct(name, &loc, mapping)
            .with_mapping_media_type("text/turtle");
        fluree
            .create_r2rml_graph_source(cfg)
            .await
            .expect("create graph source");
    }

    // The native twin of the fixture (same IRIs, classes, values) is the oracle.
    let ledger = fluree.create_ledger(NATIVE).await.expect("ledger");
    let people = [
        (1, "alice", 90),
        (2, "bob", 85),
        (3, "carol", 70),
        (4, "dave", 60),
        (5, "erin", 95),
    ];
    let graph: Vec<Value> = people
        .iter()
        .map(|(id, name, score)| {
            json!({
                "@id": format!("http://example.org/person/{id}"),
                "@type": "http://example.org/Person",
                "http://example.org/name": name,
                "http://example.org/score": score,
            })
        })
        .collect();
    fluree
        .insert(ledger, &json!({ "@graph": graph }))
        .await
        .expect("seed native twin");
    fluree
}

fn context() -> Value {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    })
}

fn deny(id: &str, targeting: Value) -> Value {
    let mut p =
        json!({ "@id": id, "@type": "f:AccessPolicy", "f:action": "f:view", "f:allow": false });
    for (k, v) in targeting.as_object().unwrap() {
        p[k] = v.clone();
    }
    p
}

fn allow(id: &str, targeting: Value) -> Value {
    let mut p =
        json!({ "@id": id, "@type": "f:AccessPolicy", "f:action": "f:view", "f:allow": true });
    for (k, v) in targeting.as_object().unwrap() {
        p[k] = v.clone();
    }
    p
}

fn on_property(iri: &str) -> Value {
    json!({ "f:onProperty": [{ "@id": iri }] })
}

fn on_class(iri: &str) -> Value {
    json!({ "f:onClass": [{ "@id": iri }] })
}

fn on_subject(iri: &str) -> Value {
    json!({ "f:onSubject": [{ "@id": iri }] })
}

/// Run `where`/`select` against `from` under `policies`, returning the rows
/// sorted so two sources can be compared regardless of scan order.
async fn run(
    fluree: &fluree_db_api::Fluree,
    from: &str,
    policies: Vec<Value>,
    default_allow: bool,
    select: Value,
    r#where: Value,
) -> Vec<String> {
    let q = json!({
        "@context": context(),
        "from": from,
        "opts": { "policy": policies, "default-allow": default_allow },
        "select": select,
        "where": r#where,
    });
    let rows = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query against {from} failed: {e}"));
    // The native formatter compacts IRIs against the query context; the
    // virtual source emits them in full. Expand so row sets compare equal.
    let mut out: Vec<String> = rows
        .as_array()
        .expect("array result")
        .iter()
        .map(|r| {
            r.to_string()
                .replace("\"ex:", "\"http://example.org/")
                .replace("\"rdf:", "\"http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        })
        .collect();
    out.sort();
    out
}

/// Assert the virtual source and the native twin agree on a policy + query.
async fn assert_parity(
    fluree: &fluree_db_api::Fluree,
    label: &str,
    policies: Vec<Value>,
    default_allow: bool,
    select: Value,
    r#where: Value,
    expected_rows: usize,
) {
    let native = run(
        fluree,
        NATIVE,
        policies.clone(),
        default_allow,
        select.clone(),
        r#where.clone(),
    )
    .await;
    let virt = run(fluree, GS, policies, default_allow, select, r#where).await;
    assert_eq!(
        virt, native,
        "[{label}] virtual source must match native twin"
    );
    assert_eq!(
        native.len(),
        expected_rows,
        "[{label}] oracle row count: {native:?}"
    );
}

fn name_query() -> (Value, Value) {
    (
        json!(["?s", "?name"]),
        json!({ "@id": "?s", "ex:name": "?name" }),
    )
}

#[tokio::test]
async fn static_policies_match_native_twin() {
    let fluree = setup().await;
    let (sel, wh) = name_query();

    // Baseline: no policy inputs => everything (the probe that found the gap).
    let q = json!({ "@context": context(), "from": GS, "select": sel, "where": wh });
    let rows = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("baseline");
    assert_eq!(rows.as_array().unwrap().len(), 5);

    // f:onProperty
    assert_parity(
        &fluree,
        "deny name, default deny",
        vec![deny("ex:p", on_property("http://example.org/name"))],
        false,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "deny name, default allow",
        vec![deny("ex:p", on_property("http://example.org/name"))],
        true,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "allow name only, default deny",
        vec![allow("ex:p", on_property("http://example.org/name"))],
        false,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;
    assert_parity(
        &fluree,
        "allow name only, default deny: score hidden",
        vec![allow("ex:p", on_property("http://example.org/name"))],
        false,
        json!(["?s", "?score"]),
        json!({ "@id": "?s", "ex:score": "?score" }),
        0,
    )
    .await;

    // f:onClass — the class is static per triples map (rr:class), so the deny
    // covers every predicate the map declares, with no per-row work.
    assert_parity(
        &fluree,
        "deny Person, default allow",
        vec![deny("ex:c", on_class("http://example.org/Person"))],
        true,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "allow Person, default deny",
        vec![allow("ex:c", on_class("http://example.org/Person"))],
        false,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;
    assert_parity(
        &fluree,
        "deny other class, default allow",
        vec![deny("ex:c", on_class("http://example.org/Other"))],
        true,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;
    // onClass + onProperty intersect.
    assert_parity(
        &fluree,
        "allow Person.name only, default deny",
        vec![allow("ex:cp", json!({ "f:onClass": [{ "@id": "http://example.org/Person" }], "f:onProperty": [{ "@id": "http://example.org/name" }] }))],
        false,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;
    assert_parity(
        &fluree,
        "allow Person.name only, default deny: score hidden",
        vec![allow("ex:cp", json!({ "f:onClass": [{ "@id": "http://example.org/Person" }], "f:onProperty": [{ "@id": "http://example.org/name" }] }))],
        false,
        json!(["?s", "?score"]),
        json!({ "@id": "?s", "ex:score": "?score" }),
        0,
    )
    .await;

    // f:onSubject — per-row subject IRI compare.
    assert_parity(
        &fluree,
        "allow person/1 only",
        vec![allow("ex:s", on_subject("http://example.org/person/1"))],
        false,
        sel.clone(),
        wh.clone(),
        1,
    )
    .await;
    assert_parity(
        &fluree,
        "deny person/1, default allow",
        vec![deny("ex:s", on_subject("http://example.org/person/1"))],
        true,
        sel.clone(),
        wh.clone(),
        4,
    )
    .await;

    // Untargeted / default-only.
    assert_parity(
        &fluree,
        "no policies, default deny",
        vec![],
        false,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "no policies, default allow",
        vec![],
        true,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;
    assert_parity(
        &fluree,
        "untargeted deny",
        vec![deny("ex:d", json!({}))],
        true,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "untargeted allow",
        vec![allow("ex:a", json!({}))],
        false,
        sel.clone(),
        wh.clone(),
        5,
    )
    .await;

    // f:required gates.
    assert_parity(
        &fluree,
        "required deny beats allow",
        vec![allow("ex:a", on_property("http://example.org/name")), {
            let mut p = deny("ex:r", on_property("http://example.org/name"));
            p["f:required"] = json!(true);
            p
        }],
        true,
        sel.clone(),
        wh.clone(),
        0,
    )
    .await;
}

#[tokio::test]
async fn pattern_shapes_match_native_twin() {
    let fluree = setup().await;
    let deny_score = vec![deny("ex:p", on_property("http://example.org/score"))];

    // Same-subject star: a row needs every member viewable.
    assert_parity(
        &fluree,
        "star with hidden member",
        deny_score.clone(),
        true,
        json!(["?s", "?name", "?score"]),
        json!({ "@id": "?s", "ex:name": "?name", "ex:score": "?score" }),
        0,
    )
    .await;
    // OPTIONAL on the hidden member keeps the row, unbound.
    assert_parity(
        &fluree,
        "optional hidden member",
        deny_score.clone(),
        true,
        json!(["?s", "?name", "?score"]),
        json!([{ "@id": "?s", "ex:name": "?name" }, ["optional", { "@id": "?s", "ex:score": "?score" }]]),
        5,
    )
    .await;
    // Class-selected subject scan.
    assert_parity(
        &fluree,
        "class scan under class deny",
        vec![deny("ex:c", on_class("http://example.org/Person"))],
        true,
        json!(["?s"]),
        json!({ "@id": "?s", "@type": "ex:Person" }),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "class scan under property deny",
        deny_score.clone(),
        true,
        json!(["?s"]),
        json!({ "@id": "?s", "@type": "ex:Person" }),
        5,
    )
    .await;
    // Projected type var.
    assert_parity(
        &fluree,
        "type projection under class deny",
        vec![deny("ex:c", on_class("http://example.org/Person"))],
        true,
        json!(["?s", "?t"]),
        json!({ "@id": "?s", "@type": "?t" }),
        0,
    )
    .await;
    // Constant object.
    assert_parity(
        &fluree,
        "constant object under deny",
        vec![deny("ex:p", on_property("http://example.org/name"))],
        true,
        json!(["?s"]),
        json!({ "@id": "?s", "ex:name": "alice" }),
        0,
    )
    .await;
    assert_parity(
        &fluree,
        "constant object under allow",
        vec![allow("ex:p", on_property("http://example.org/name"))],
        false,
        json!(["?s"]),
        json!({ "@id": "?s", "ex:name": "alice" }),
        1,
    )
    .await;
    // Aggregates take the row-filtering path (the fused fold declines).
    assert_parity(
        &fluree,
        "count under deny",
        vec![deny("ex:p", on_property("http://example.org/name"))],
        true,
        json!(["(count ?s)"]),
        json!({ "@id": "?s", "ex:name": "?name" }),
        1,
    )
    .await;
    let counted = run(
        &fluree,
        GS,
        vec![allow("ex:s", on_subject("http://example.org/person/1"))],
        false,
        json!(["(count ?s)"]),
        json!({ "@id": "?s", "ex:name": "?name" }),
    )
    .await;
    assert_eq!(
        counted,
        vec!["[1]".to_string()],
        "count sees only the allowed subject"
    );
}

#[tokio::test]
async fn wildcard_scan_hides_denied_predicates() {
    let fluree = setup().await;
    let sel = json!(["?p", "?o"]);
    let wh = json!({ "@id": "http://example.org/person/1", "?p": "?o" });
    assert_parity(
        &fluree,
        "wildcard under score deny",
        vec![deny("ex:p", on_property("http://example.org/score"))],
        true,
        sel.clone(),
        wh.clone(),
        2, // rdf:type + ex:name
    )
    .await;
    assert_parity(
        &fluree,
        "wildcard under class deny",
        vec![deny("ex:c", on_class("http://example.org/Person"))],
        true,
        sel,
        wh,
        0,
    )
    .await;
}

#[tokio::test]
async fn query_policies_fail_closed() {
    let fluree = setup().await;
    let (sel, wh) = name_query();
    let gate = json!({
        "@id": "ex:q", "@type": "f:AccessPolicy", "f:action": "f:view",
        "f:onProperty": [{ "@id": "http://example.org/name" }],
        "f:query": { "@type": "@json", "@value": {
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?$this", "ex:name": "alice" }]
        }}
    });
    // Native evaluates the query: alice's name is visible.
    let native = run(
        &fluree,
        NATIVE,
        vec![gate.clone()],
        true,
        sel.clone(),
        wh.clone(),
    )
    .await;
    assert_eq!(native.len(), 1, "oracle: {native:?}");
    // The virtual source cannot run it: the targeted flakes are denied.
    let virt = run(
        &fluree,
        GS,
        vec![gate.clone()],
        true,
        sel.clone(),
        wh.clone(),
    )
    .await;
    assert!(
        virt.is_empty(),
        "f:query must fail closed on a virtual source: {virt:?}"
    );
    // A static allow ahead of it still grants (allow-overrides, in order), as
    // native. (Listed after the gate, the failing query would deny first on
    // both sides for every subject the query rejects.)
    let both = vec![allow("ex:a", on_property("http://example.org/name")), gate];
    assert_parity(&fluree, "static allow + query", both, false, sel, wh, 5).await;
}

#[tokio::test]
async fn column_derived_classes_are_enforced_per_row() {
    let fluree = setup().await;
    let q = |policies: Vec<Value>, default_allow: bool| {
        json!({
            "@context": context(),
            "from": KINDS_GS,
            "opts": { "policy": policies, "default-allow": default_allow },
            "select": ["?s", "?name"],
            "where": { "@id": "?s", "ex:name": "?name" },
        })
    };
    let count = |v: Value| v.as_array().map_or(0, Vec::len);

    let all = fluree
        .query_from()
        .jsonld(&q(vec![], true))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(count(all), 5);
    // Deny one row's class: only that row disappears.
    let hidden = fluree
        .query_from()
        .jsonld(&q(
            vec![deny("ex:c", on_class("http://example.org/kind/alice"))],
            true,
        ))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(count(hidden.clone()), 4, "{hidden}");
    assert!(!hidden.to_string().contains("alice"));
    // Allow one row's class under default deny: only that row survives.
    let only = fluree
        .query_from()
        .jsonld(&q(
            vec![allow("ex:c", on_class("http://example.org/kind/bob"))],
            false,
        ))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(count(only.clone()), 1, "{only}");
    assert!(only.to_string().contains("bob"));
}

#[tokio::test]
async fn dataset_mode_enforces_per_graph() {
    let fluree = setup().await;
    // FROM NAMED + GRAPH over the virtual source, joined with the native twin.
    let q = json!({
        "@context": context(),
        "from": NATIVE,
        "from-named": [GS],
        "opts": {
            "policy": [deny("ex:p", on_property("http://example.org/score"))],
            "default-allow": true
        },
        "select": ["?s", "?score"],
        "where": [["graph", GS, { "@id": "?s", "ex:score": "?score" }]],
    });
    let rows = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("dataset query");
    assert_eq!(rows.as_array().unwrap().len(), 0, "{rows}");
    let q_allowed = {
        let mut q = q.clone();
        q["opts"]["policy"] = json!([allow("ex:p", on_property("http://example.org/score"))]);
        q["opts"]["default-allow"] = json!(false);
        q
    };
    let rows = fluree
        .query_from()
        .jsonld(&q_allowed)
        .execute_formatted()
        .await
        .expect("dataset query");
    assert_eq!(rows.as_array().unwrap().len(), 5, "{rows}");
}

/// A source registered with `--model`: its policies and class hierarchy come
/// from the model ledger's default graph, through the same cross-ledger
/// resolver a native ledger's `f:policySource` / `f:schemaSource` use.
#[tokio::test]
async fn model_ledger_supplies_policies_and_hierarchy() {
    let fluree = setup().await;
    const MODEL: &str = "governance:main";
    const GOVERNED: &str = "local-governed:main";

    // M: Person ⊑ Agent; the baseline (f:AccessPolicy) rule hides Agents;
    // a reader policy class grants Person.name; one identity holds it.
    let ledger = fluree.create_ledger(MODEL).await.expect("model ledger");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    { "@id": "ex:Person", "@type": "rdfs:Class", "rdfs:subClassOf": { "@id": "ex:Agent" } },
                    { "@id": "ex:Agent", "@type": "rdfs:Class" },
                    { "@id": "ex:hideAgents", "@type": "f:AccessPolicy", "f:action": { "@id": "f:view" },
                      "f:onClass": [{ "@id": "ex:Agent" }], "f:allow": false },
                    { "@id": "ex:readNames", "@type": "ex:ReaderPolicy", "f:action": { "@id": "f:view" },
                      "f:onClass": [{ "@id": "ex:Person" }], "f:onProperty": [{ "@id": "ex:name" }],
                      "f:allow": true },
                    { "@id": "http://example.org/users/reader", "f:policyClass": { "@id": "ex:ReaderPolicy" } }
                ]
            }),
        )
        .await
        .expect("seed model ledger");

    let cfg = R2rmlCreateConfig::new_direct("local-governed", table_location(), PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle")
        .with_model(MODEL);
    fluree
        .create_r2rml_graph_source(cfg)
        .await
        .expect("governed source");

    let query = |opts: Value, select: Value, r#where: Value| {
        let mut q =
            json!({ "@context": context(), "from": GOVERNED, "select": select, "where": r#where });
        if !opts.is_null() {
            q["opts"] = opts;
        }
        q
    };
    let rows = |v: Value| v.as_array().map_or(0, Vec::len);
    let (sel, wh) = name_query();

    // No policy inputs on the request: unrestricted, as for a native ledger.
    let r = fluree
        .query_from()
        .jsonld(&query(json!(null), sel.clone(), wh.clone()))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(rows(r), 5);

    // Anonymous governed request: the baseline f:AccessPolicy rules apply, and
    // `ex:Agent` reaches `ex:Person` through M's subclass hierarchy.
    let r = fluree
        .query_from()
        .jsonld(&query(
            json!({ "default-allow": true }),
            sel.clone(),
            wh.clone(),
        ))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(
        rows(r.clone()),
        0,
        "subclass-expanded deny from the model: {r}"
    );

    // Identity known to M: its f:policyClass (looked up in M) selects the rules.
    let reader = json!({ "identity": "http://example.org/users/reader", "default-allow": false });
    let r = fluree
        .query_from()
        .jsonld(&query(reader.clone(), sel.clone(), wh.clone()))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(rows(r.clone()), 5, "reader sees names: {r}");
    let r = fluree
        .query_from()
        .jsonld(&query(
            reader,
            json!(["?s", "?score"]),
            json!({ "@id": "?s", "ex:score": "?score" }),
        ))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(rows(r.clone()), 0, "reader does not see scores: {r}");

    // Explicit policy-class selects the same rules without an identity.
    let r = fluree
        .query_from()
        .jsonld(&query(
            json!({ "policy-class": ["http://example.org/ReaderPolicy"], "default-allow": false }),
            sel.clone(),
            wh.clone(),
        ))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(rows(r.clone()), 5, "{r}");

    // Identity unknown to M selects nothing; default-allow governs.
    let nobody = json!({ "identity": "http://example.org/users/nobody", "default-allow": false });
    let r = fluree
        .query_from()
        .jsonld(&query(nobody, sel, wh))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(rows(r.clone()), 0, "{r}");
}

/// `--model` is validated when the source is registered, and policies the
/// source will never be able to evaluate are reported up front.
#[tokio::test]
async fn registration_validates_model_and_warns_on_query_policies() {
    let fluree = setup().await;
    let cfg = |name: &str, model: &str| {
        R2rmlCreateConfig::new_direct(name, table_location(), PEOPLE_R2RML)
            .with_mapping_media_type("text/turtle")
            .with_model(model)
    };

    let err = fluree
        .create_r2rml_graph_source(cfg("bad-model", "nope:main"))
        .await
        .expect_err("unknown model ledger must be refused");
    assert!(err.to_string().contains("not found"), "{err}");

    let err = fluree
        .create_r2rml_graph_source(cfg("gs-model", GS))
        .await
        .expect_err("a graph source is not a model");
    assert!(err.to_string().contains("graph source"), "{err}");

    let ledger = fluree.create_ledger("gated:main").await.expect("model");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": { "ex": "http://example.org/", "f": "https://ns.flur.ee/db#" },
                "@graph": [
                    { "@id": "ex:owners", "@type": "f:AccessPolicy", "f:action": { "@id": "f:view" },
                      "f:onClass": [{ "@id": "ex:Person" }],
                      "f:query": { "@type": "@json", "@value": { "where": [{ "@id": "?$this", "ex:owner": "?$identity" }] } } },
                    { "@id": "ex:plain", "@type": "f:AccessPolicy", "f:action": { "@id": "f:view" },
                      "f:onProperty": [{ "@id": "ex:name" }], "f:allow": true }
                ]
            }),
        )
        .await
        .expect("seed gated model");
    let created = fluree
        .create_r2rml_graph_source(cfg("gated-people", "gated:main"))
        .await
        .expect("registers with warnings");
    assert_eq!(
        created.model_warnings.len(),
        1,
        "{:?}",
        created.model_warnings
    );
    assert!(created.model_warnings[0].contains("http://example.org/owners"));
    assert!(created.model_warnings[0].contains("f:query"));
}

/// The tracked response names the policies the virtual source could not
/// evaluate, so a caller can tell a fail-closed `f:query` from an empty table.
#[tokio::test]
async fn tracked_response_lists_unevaluable_policies() {
    let fluree = setup().await;
    let (sel, wh) = name_query();
    let gate = json!({
        "@id": "ex:q", "@type": "f:AccessPolicy", "f:action": "f:view",
        "f:onProperty": [{ "@id": "http://example.org/name" }],
        "f:query": { "@type": "@json", "@value": { "where": [{ "@id": "?$this", "ex:name": "alice" }] } }
    });
    let q = json!({
        "@context": context(),
        "from": GS,
        "opts": { "policy": [gate], "default-allow": true, "meta": { "policy": true } },
        "select": sel,
        "where": wh,
    });
    let response = fluree
        .query_from()
        .jsonld(&q)
        .execute_tracked()
        .await
        .expect("tracked query");
    assert_eq!(response.result.as_array().map_or(0, Vec::len), 0);
    let enforcement = response.policy_enforcement.expect("enforced");
    assert!(enforcement.enforced);
    // Ids are reported as the policy wrote them (here the compact form).
    assert_eq!(enforcement.unevaluable_policies, vec!["ex:q".to_string()]);
}

/// A source's own `default-allow` keeps it readable to authenticated callers
/// that match no policy, without attaching a model.
#[tokio::test]
async fn source_default_allow_keeps_model_less_source_readable() {
    let fluree = setup().await;
    let cfg = R2rmlCreateConfig::new_direct("local-open", table_location(), PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle")
        .with_default_allow(true);
    fluree
        .create_r2rml_graph_source(cfg)
        .await
        .expect("open source");

    let (sel, wh) = name_query();
    let q = |from: &str| {
        json!({
            "@context": context(),
            "from": from,
            "opts": { "identity": "http://example.org/users/anyone" },
            "select": sel,
            "where": wh,
        })
    };
    let open = fluree
        .query_from()
        .jsonld(&q("local-open:main"))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(open.as_array().map_or(0, Vec::len), 5, "{open}");
    let closed = fluree
        .query_from()
        .jsonld(&q(GS))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(closed.as_array().map_or(0, Vec::len), 0, "{closed}");
}
