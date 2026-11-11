//! A class-targeted view policy over an R2RML graph source is decided on the
//! classes the subject holds in the graph the mapping defines, as on a native
//! ledger holding the same triples under the same policy.
//!
//! Each case maps committed local Iceberg fixtures (and, with the `sql`
//! feature, a fake SQL endpoint holding the same rows), builds the native
//! twin from the triples the mapping defines, and compares the answers on
//! SPARQL and JSON-LD, with fast paths on and off.
//!
//! Own binary: it flips the process-wide fast-path switch and sets
//! `FLUREE_ICEBERG_LOCAL_ROOTS`.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[cfg(feature = "sql")]
#[path = "support/fake_sql.rs"]
mod fake_sql;

use fluree_db_api::{
    set_fast_paths_disabled, Fluree, FlureeBuilder, GovernanceOptions, R2rmlCreateConfig,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Every test flips the process-wide fast-path switch.
static SERIAL: Mutex<()> = Mutex::const_new(());

const EX: &str = "http://example.org/";

const PREFIXES: &str = r"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix owl: <http://www.w3.org/2002/07/owl#> .
    @prefix ex: <http://example.org/> .
    @prefix f: <https://ns.flur.ee/db#> .
";

/// `silver.people`: `(id, name)`.
const PEOPLE: [(i64, &str); 5] = [
    (1, "alice"),
    (2, "bob"),
    (3, "carol"),
    (4, "dave"),
    (5, "erin"),
];

/// `sets.customers_us` and `sets.customers_eu`: `(id, name, city)`.
const US: [(i64, &str, &str); 2] = [(7, "Ada", "Miami"), (8, "Bo", "Austin")];
const EU: [(i64, &str, &str); 2] = [(7, "Ada L.", "Lyon"), (9, "Cy", "Paris")];

/// `sets.customers_history`: `(customer_id, name, city)`.
const HISTORY: [(Option<i64>, &str, Option<&str>); 11] = [
    (Some(1), "alice", Some("Boston")),
    (Some(2), "bob", Some("Austin")),
    (Some(3), "carol", Some("Chicago")),
    (Some(1), "alice", Some("Denver")),
    (Some(5), "erin", Some("Miami")),
    (Some(2), "robert", Some("Austin")),
    (Some(2), "robert", Some("Austin")),
    (Some(4), "dave", None),
    (Some(1), "alice", Some("Denver")),
    (Some(5), "erin-m", Some("Tampa")),
    (None, "ghost", Some("Nowhere")),
];

fn ex(local: &str) -> String {
    format!("{EX}{local}")
}

fn allow_fixture_roots() {
    // SAFETY: set before any storage or scan is built; every test in this
    // binary sets the same value.
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR")),
    );
}

fn root(namespace: &str) -> String {
    format!(
        "file://{}/tests/fixtures/iceberg/{namespace}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn policy(id: &str, allow: bool, targeting: Value) -> Value {
    let mut p = json!({
        "@id": id,
        "@type": "https://ns.flur.ee/db#AccessPolicy",
        "https://ns.flur.ee/db#action": { "@id": "https://ns.flur.ee/db#view" },
        "https://ns.flur.ee/db#allow": allow,
    });
    for (k, v) in targeting.as_object().expect("targeting is an object") {
        p[k] = v.clone();
    }
    p
}

fn on_class(class: &str) -> Value {
    json!({ "https://ns.flur.ee/db#onClass": [{ "@id": class }] })
}

fn on_class_property(class: &str, property: &str) -> Value {
    json!({
        "https://ns.flur.ee/db#onClass": [{ "@id": class }],
        "https://ns.flur.ee/db#onProperty": [{ "@id": property }],
    })
}

/// A graph source over the fixtures, with the triples its mapping defines.
struct Source {
    alias: String,
    namespace: &'static str,
    mapping: String,
    /// The native twin's `@graph`.
    graph: Vec<Value>,
    /// The fake SQL endpoint's tables: `(name, columns, rows)`.
    #[allow(dead_code)]
    tables: Vec<(
        &'static str,
        Vec<(&'static str, &'static str)>,
        Vec<Vec<Value>>,
    )>,
}

fn people_table() -> (
    &'static str,
    Vec<(&'static str, &'static str)>,
    Vec<Vec<Value>>,
) {
    (
        "silver.people",
        vec![("id", "bigint"), ("name", "varchar")],
        PEOPLE
            .iter()
            .map(|(id, name)| vec![json!(id), json!(name)])
            .collect(),
    )
}

/// The lanes a case runs on: the Iceberg source and, with `sql`, its SQL twin.
async fn register(fluree: &Fluree, source: &Source) -> Vec<String> {
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct(&source.alias, root(source.namespace), &source.mapping)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .unwrap_or_else(|e| panic!("create {}: {e}", source.alias));
    let native = fluree
        .create_ledger(&format!("{}-native:main", source.alias))
        .await
        .expect("native ledger");
    fluree
        .insert(native, &json!({ "@graph": source.graph }))
        .await
        .expect("seed native twin");
    #[allow(unused_mut)]
    let mut lanes = vec![format!("{}:main", source.alias)];
    #[cfg(feature = "sql")]
    {
        let mut server = fake_sql::FakeSql::new();
        for (name, cols, rows) in &source.tables {
            server = server.table(fake_sql::Table::new(name, cols, rows.clone()));
        }
        let server = server.mount().await;
        let alias = format!("{}-sql", source.alias);
        let mut cfg =
            fluree_db_api::SqlCreateConfig::new(&alias, server.uri(), source.mapping.clone());
        cfg.allow_duplicate_subjects = true;
        fluree
            .create_sql_graph_source(cfg)
            .await
            .unwrap_or_else(|e| panic!("create {alias}: {e}"));
        // The server lives on this test's runtime; dropping it would stop it.
        std::mem::forget(server);
        lanes.push(format!("{alias}:main"));
    }
    lanes
}

/// A result's rows as text, sorted (duplicates kept); an error as one row.
fn rows_of(result: Result<Value, String>) -> Vec<String> {
    let v = match result {
        Ok(v) => v,
        Err(e) => return vec![format!("ERROR {e}")],
    };
    let mut rows: Vec<String> = if let Some(b) = v["results"]["bindings"].as_array() {
        b.iter()
            .map(|row| {
                let mut cells: Vec<(String, String)> = row
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, c)| (k.clone(), c["value"].as_str().unwrap_or("").to_string()))
                    .collect();
                cells.sort();
                cells
                    .into_iter()
                    .map(|(_, c)| c)
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
            .collect()
    } else {
        v.as_array()
            .unwrap_or_else(|| panic!("not a result: {v}"))
            .iter()
            .map(|row| match row {
                Value::Array(cells) => cells
                    .iter()
                    .map(|c| c.as_str().map_or_else(|| c.to_string(), str::to_string))
                    .collect::<Vec<_>>()
                    .join(" | "),
                other => other.to_string(),
            })
            .collect()
    };
    rows.sort();
    rows
}

/// One query shape on both surfaces.
struct Shape {
    label: &'static str,
    /// SPARQL `WHERE` body and select list.
    sparql: (&'static str, &'static str),
    /// JSON-LD `select` and `where`.
    jsonld: (Value, Value),
}

fn names() -> Shape {
    Shape {
        label: "?s ex:name ?n",
        sparql: ("?s ?n", "?s <http://example.org/name> ?n"),
        jsonld: (
            json!(["?s", "?n"]),
            json!({ "@id": "?s", "http://example.org/name": "?n" }),
        ),
    }
}

async fn run(
    fluree: &Fluree,
    from: &str,
    shape: &Shape,
    policies: &[Value],
    default_allow: bool,
    jsonld: bool,
) -> Vec<String> {
    let result = if jsonld {
        let q = json!({
            "from": from,
            "opts": { "policy": policies, "default-allow": default_allow },
            "select": shape.jsonld.0,
            "where": shape.jsonld.1,
        });
        fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .map_err(|e| e.to_string())
    } else {
        // The SPARQL select list sorted, so its cells line up with JSON-LD's.
        let sparql = format!(
            "SELECT {} FROM <{from}> WHERE {{ {} }}",
            shape.sparql.0, shape.sparql.1
        );
        fluree
            .query_from()
            .sparql(&sparql)
            .connection_opts(GovernanceOptions {
                policy: Some(json!(policies)),
                default_allow: Some(default_allow),
                ..GovernanceOptions::default()
            })
            .execute_formatted()
            .await
            .map_err(|e| e.to_string())
    };
    rows_of(result)
}

/// Every disagreement between each lane and the native twin.
async fn disagreements(
    fluree: &Fluree,
    source: &Source,
    lanes: &[String],
    shapes: &[Shape],
    policies: &[Value],
    default_allow: bool,
) -> Vec<String> {
    let native = format!("{}-native:main", source.alias);
    let mut wrong = Vec::new();
    for shape in shapes {
        for jsonld in [false, true] {
            let surface = if jsonld { "JSON-LD" } else { "SPARQL" };
            let want = run(fluree, &native, shape, policies, default_allow, jsonld).await;
            for lane in lanes {
                for fast in [true, false] {
                    set_fast_paths_disabled(!fast);
                    let got = run(fluree, lane, shape, policies, default_allow, jsonld).await;
                    set_fast_paths_disabled(false);
                    if got != want {
                        wrong.push(format!(
                            "{lane} {} {surface} fast={fast}: {got:?}, native {want:?}",
                            shape.label
                        ));
                    }
                }
            }
        }
    }
    wrong
}

fn person(id: i64) -> String {
    ex(&format!("person/{id}"))
}

fn kind(name: &str) -> String {
    ex(&format!("kind/{name}"))
}

/// People typed from their name column, keyed by `id`.
fn kinds_source(alias: &str) -> Source {
    Source {
        alias: alias.to_string(),
        namespace: "silver",
        mapping: format!(
            r#"{PREFIXES}
            <http://example.org/mapping#Kinds> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/person/{{id}}" ] ;
                rr:predicateObjectMap [
                    rr:predicate rdf:type ;
                    rr:objectMap [ rr:template "http://example.org/kind/{{name}}" ; rr:termType rr:IRI ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            "#
        ),
        graph: PEOPLE
            .iter()
            .map(|(id, name)| json!({ "@id": person(*id), "@type": kind(name), ex("name"): name }))
            .collect(),
        tables: vec![people_table()],
    }
}

/// R2: a combined class and property restriction decides a map whose class
/// comes from a column.
#[tokio::test]
async fn a_class_and_property_policy_decides_a_column_typed_map() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let source = kinds_source("cp-kinds");
    let lanes = register(&fluree, &source).await;
    let policies = [policy(
        "ex:p",
        false,
        on_class_property(&kind("alice"), &ex("name")),
    )];
    let wrong = disagreements(&fluree, &source, &lanes, &[names()], &policies, true).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// R3: a class the map derives through a reference to another map's
/// subjects is a class of the row's subject.
#[tokio::test]
async fn a_class_derived_through_a_reference_is_the_subjects() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let mut graph: Vec<Value> = PEOPLE
        .iter()
        .map(|(id, name)| json!({ "@id": person(*id), "@type": kind(name), ex("name"): name }))
        .collect();
    graph.extend(
        PEOPLE
            .iter()
            .map(|(_, name)| json!({ "@id": kind(name), ex("label"): name })),
    );
    let source = Source {
        alias: "ref-kinds".to_string(),
        namespace: "silver",
        mapping: format!(
            r#"{PREFIXES}
            <http://example.org/mapping#People> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/person/{{id}}" ] ;
                rr:predicateObjectMap [
                    rr:predicate rdf:type ;
                    rr:objectMap [
                        rr:parentTriplesMap <http://example.org/mapping#Kind> ;
                        rr:joinCondition [ rr:child "id" ; rr:parent "id" ]
                    ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            <http://example.org/mapping#Kind> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/kind/{{name}}" ] ;
                rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "name" ] ] .
            "#
        ),
        graph,
        tables: vec![people_table()],
    };
    let lanes = register(&fluree, &source).await;
    let policies = [policy("ex:p", false, on_class(&kind("alice")))];
    let wrong = disagreements(&fluree, &source, &lanes, &[names()], &policies, true).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// R4: a predicate one classed map and one column-typed map both mint is
/// decided on each subject's own classes, not credited to the classed map's.
#[tokio::test]
async fn a_shared_predicate_is_decided_on_each_subjects_classes() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let mut graph: Vec<Value> = PEOPLE
        .iter()
        .map(|(id, name)| json!({ "@id": person(*id), "@type": ex("Customer"), ex("name"): name }))
        .collect();
    graph.extend(PEOPLE.iter().map(|(id, name)| {
        json!({ "@id": ex(&format!("other/{id}")), "@type": kind(name), ex("name"): name })
    }));
    let source = Source {
        alias: "shared-name".to_string(),
        namespace: "silver",
        mapping: format!(
            r#"{PREFIXES}
            <http://example.org/mapping#Customers> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/person/{{id}}" ; rr:class ex:Customer ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            <http://example.org/mapping#Others> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/other/{{id}}" ] ;
                rr:predicateObjectMap [
                    rr:predicate rdf:type ;
                    rr:objectMap [ rr:template "http://example.org/kind/{{name}}" ; rr:termType rr:IRI ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            "#
        ),
        graph,
        tables: vec![people_table()],
    };
    let lanes = register(&fluree, &source).await;
    let policies = [policy("ex:p", true, on_class(&ex("Customer")))];
    let wrong = disagreements(&fluree, &source, &lanes, &[names()], &policies, false).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// R5: a map declaring a schema class beside a data class decides each
/// `rdf:type` triple on the class it states.
#[tokio::test]
async fn each_type_triple_is_decided_on_the_class_it_states() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let secret = "https://example.org/Secret";
    let owl_class = "http://www.w3.org/2002/07/owl#Class";
    let source = Source {
        alias: "schema-typed".to_string(),
        namespace: "silver",
        mapping: format!(
            r#"{PREFIXES}
            <http://example.org/mapping#Secrets> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "silver.people" ; f:key ( "id" ) ] ;
                rr:subjectMap [
                    rr:template "http://example.org/person/{{id}}" ;
                    rr:class owl:Class , <{secret}>
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            "#
        ),
        graph: PEOPLE
            .iter()
            .map(|(id, name)| {
                json!({ "@id": person(*id), "@type": [owl_class, secret], ex("name"): name })
            })
            .collect(),
        tables: vec![people_table()],
    };
    let lanes = register(&fluree, &source).await;
    let shapes = [
        Shape {
            label: "?s a <Secret>",
            sparql: ("?s", "?s a <https://example.org/Secret>"),
            jsonld: (json!(["?s"]), json!({ "@id": "?s", "@type": secret })),
        },
        Shape {
            label: "?s a ?t",
            sparql: ("?s ?t", "?s a ?t"),
            jsonld: (json!(["?s", "?t"]), json!({ "@id": "?s", "@type": "?t" })),
        },
        Shape {
            label: "?s ?p ?o",
            sparql: ("?o ?p ?s", "?s ?p ?o"),
            jsonld: (
                json!(["?o", "?p", "?s"]),
                json!({ "@id": "?s", "?p": "?o" }),
            ),
        },
    ];
    let policies = [policy("ex:p", false, on_class(secret))];
    let wrong = disagreements(&fluree, &source, &lanes, &shapes, &policies, true).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// R6: a subject two maps mint, each with its own `rr:class`, holds both
/// classes.
#[tokio::test]
#[ignore = "fixed by the class-policy decisions change"]
async fn a_subject_two_maps_mint_holds_both_maps_classes() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let regional = |name: &str, table: &str, class: &str| {
        format!(
            r#"
            <http://example.org/mapping#{name}> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "{table}" ; f:key ( "id" ) ] ;
                rr:subjectMap [ rr:template "http://example.org/customer/{{id}}" ; rr:class ex:{class} ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            "#
        )
    };
    let table = |name: &'static str, rows: &[(i64, &str, &str)]| {
        (
            name,
            vec![("id", "bigint"), ("name", "varchar"), ("city", "varchar")],
            rows.iter()
                .map(|(i, n, c)| vec![json!(i), json!(n), json!(c)])
                .collect(),
        )
    };
    let mut graph = Vec::new();
    for (class, rows) in [("Customer", &US), ("Restricted", &EU)] {
        for (id, name, _) in rows.iter() {
            graph.push(json!({
                "@id": ex(&format!("customer/{id}")),
                "@type": ex(class),
                ex("name"): name
            }));
        }
    }
    let source = Source {
        alias: "two-classes".to_string(),
        namespace: "sets",
        mapping: format!(
            "{PREFIXES}{}{}",
            regional("Us", "sets.customers_us", "Customer"),
            regional("Eu", "sets.customers_eu", "Restricted")
        ),
        graph,
        tables: vec![
            table("sets.customers_us", &US),
            table("sets.customers_eu", &EU),
        ],
    };
    let lanes = register(&fluree, &source).await;
    let mut wrong = Vec::new();
    for (policies, default_allow) in [
        (
            vec![policy("ex:p", false, on_class(&ex("Restricted")))],
            true,
        ),
        (vec![policy("ex:p", true, on_class(&ex("Customer")))], false),
    ] {
        wrong.extend(
            disagreements(
                &fluree,
                &source,
                &lanes,
                &[names()],
                &policies,
                default_allow,
            )
            .await,
        );
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// R6: a subject several rows mint, each deriving a class from a column,
/// holds every row's class.
#[tokio::test]
#[ignore = "fixed by the class-policy decisions change"]
async fn a_subject_several_rows_mint_holds_every_rows_class() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let mut graph = Vec::new();
    for (id, name, city) in HISTORY {
        let Some(id) = id else { continue };
        let mut node = json!({ "@id": ex(&format!("customer/{id}")), ex("name"): name });
        if let Some(c) = city {
            node["@type"] = json!(ex(&format!("city/{c}")));
        }
        graph.push(node);
    }
    let source = Source {
        alias: "history-classes".to_string(),
        namespace: "sets",
        mapping: format!(
            r#"{PREFIXES}
            <http://example.org/mapping#History> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
                rr:subjectMap [ rr:template "http://example.org/customer/{{customer_id}}" ] ;
                rr:predicateObjectMap [
                    rr:predicate rdf:type ;
                    rr:objectMap [ rr:template "http://example.org/city/{{city}}" ; rr:termType rr:IRI ]
                ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
            "#
        ),
        graph,
        tables: vec![(
            "sets.customers_history",
            vec![
                ("customer_id", "bigint"),
                ("name", "varchar"),
                ("city", "varchar"),
            ],
            HISTORY
                .iter()
                .map(|(id, n, c)| vec![json!(id), json!(n), json!(c)])
                .collect(),
        )],
    };
    let lanes = register(&fluree, &source).await;
    let policies = [policy("ex:p", false, on_class(&ex("city/Boston")))];
    let wrong = disagreements(&fluree, &source, &lanes, &[names()], &policies, true).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
