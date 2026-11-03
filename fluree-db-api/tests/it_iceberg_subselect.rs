//! Sub-selects over an Iceberg graph source.
//!
//! A sub-select inside a graph-source scope is evaluated by lowering its body
//! through the R2RML rewrite like any other nested scope, so its patterns
//! become scans of the source's tables. Before that, the body ran against the
//! source's native index, which is empty, so the rewrite refused every
//! sub-select rather than let an ungrouped `COUNT` answer 0.
//!
//! The source is the committed two-table fixture under
//! `tests/fixtures/iceberg/glue/objects/warehouse/glue_it.db` (50 customers,
//! 200 orders, four orders per customer; see `scripts/glue-local/seed.py`),
//! read catalog-less as a Direct warehouse root, with the orders' customer
//! column mapped as a referencing object map (and a second one, joined on the
//! order number, that dangles for most orders). Every answer is checked against
//! a native ledger holding the same rows (copied out of the source through
//! plain scans, which need no sub-select), and against a fixed fact about the
//! fixture so the two cannot agree on a wrong answer such as "no rows".
//!
//! Each shape runs on SPARQL and JSON-LD, through `FROM <source>` and through
//! the source's own graph handle, with and without an explicit graph block,
//! and mixed with a native ledger.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{Fluree, FlureeBuilder, R2rmlCreateConfig};
use serde_json::{json, Value};

/// The graph source, its native twin, and a native ledger the mixed queries
/// join against. `{G}` in a query stands for the source or the twin.
const LAKE: &str = "shop-lake:main";
const TWIN: &str = "shop-twin:main";
const CRM: &str = "crm:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

const SHOP_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Customer>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "glue_it.customers" ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{customer_id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:country ; rr:objectMap [ rr:column "country" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "glue_it.orders" ] ;
        rr:subjectMap [
            rr:template "http://example.org/order/{order_id}" ;
            rr:class ex:Order
        ] ;
        # Typed, so amounts compare and order as numbers: an un-annotated
        # column is the string of its value, and "988.2" > 900 is false.
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:double ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:customer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Customer> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "customer_id" ]
            ]
        ] ;
        # An order and the customer with its number: orders 1..=50 resolve,
        # orders 51..=200 dangle (no such customer), so they have no triple.
        rr:predicateObjectMap [
            rr:predicate ex:numberedLike ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Customer> ;
                rr:joinCondition [ rr:child "order_id" ; rr:parent "customer_id" ]
            ]
        ] .
"#;

/// Local tables are fail-closed behind an allowlist captured on first use, so
/// every test in this binary installs the same value before building storage.
fn allow_fixture_roots() {
    // SAFETY: set before any storage or scan is built, to the same value in
    // every test of this binary.
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR")),
    );
}

/// The warehouse root holding `customers/` and `orders/`.
fn warehouse_root() -> String {
    format!(
        "file://{}/tests/fixtures/iceberg/glue/objects/warehouse/glue_it.db",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// The source, its native twin, and the CRM ledger (a tier for customers
/// 1..=12: gold when the id is a multiple of three, else silver).
async fn setup() -> Fluree {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("shop-lake", warehouse_root(), SHOP_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create the graph source");
    seed_twin(&fluree).await;

    let crm = fluree.create_ledger(CRM).await.expect("crm ledger");
    let tiers: Vec<Value> = (1..=12)
        .map(|id| {
            json!({
                "@id": format!("http://example.org/customer/{id}"),
                "http://example.org/tier": if id % 3 == 0 { "gold" } else { "silver" },
            })
        })
        .collect();
    fluree
        .insert(crm, &json!({ "@graph": tiers }))
        .await
        .expect("seed crm");
    fluree
}

/// Copy the source's rows into a native ledger through plain scans.
async fn seed_twin(fluree: &Fluree) {
    let customers = sparql_json(
        fluree,
        &format!("SELECT ?c ?n ?k FROM <{LAKE}> WHERE {{ ?c ex:name ?n ; ex:country ?k }}"),
    )
    .await
    .expect("read customers");
    let orders = sparql_json(
        fluree,
        &format!("SELECT ?o ?c ?a FROM <{LAKE}> WHERE {{ ?o ex:customer ?c ; ex:amount ?a }}"),
    )
    .await
    .expect("read orders");
    let numbered = sparql_json(
        fluree,
        &format!("SELECT ?o ?c FROM <{LAKE}> WHERE {{ ?o ex:numberedLike ?c }}"),
    )
    .await
    .expect("read numbered-like edges");
    let customers = customers["results"]["bindings"].as_array().expect("rows");
    let orders = orders["results"]["bindings"].as_array().expect("rows");
    let numbered = numbered["results"]["bindings"].as_array().expect("rows");
    assert_eq!(customers.len(), 50, "the fixture has 50 customers");
    assert_eq!(orders.len(), 200, "the fixture has 200 orders");
    assert_eq!(numbered.len(), 50, "orders 51..=200 dangle");

    let ex = "http://example.org/";
    let mut graph: Vec<Value> = customers
        .iter()
        .map(|b| {
            json!({
                "@id": b["c"]["value"],
                "@type": format!("{ex}Customer"),
                format!("{ex}name"): b["n"]["value"],
                format!("{ex}country"): b["k"]["value"],
            })
        })
        .collect();
    graph.extend(orders.iter().map(|b| {
        json!({
            "@id": b["o"]["value"],
            "@type": format!("{ex}Order"),
            format!("{ex}customer"): {"@id": b["c"]["value"]},
            format!("{ex}amount"): {"@value": b["a"]["value"], "@type": b["a"]["datatype"]},
        })
    }));
    graph.extend(numbered.iter().map(|b| {
        json!({
            "@id": b["o"]["value"],
            format!("{ex}numberedLike"): {"@id": b["c"]["value"]},
        })
    }));
    let twin = fluree.create_ledger(TWIN).await.expect("twin ledger");
    fluree
        .insert(twin, &json!({ "@graph": graph }))
        .await
        .expect("seed twin");
}

async fn sparql_json(fluree: &Fluree, body: &str) -> Result<Value, String> {
    fluree
        .query_from()
        .sparql(&format!("{PREFIX}{body}"))
        .execute_formatted()
        .await
        .map_err(|e| e.to_string())
}

/// SPARQL JSON as sorted rows of `var=value` (variables in alphabetical order).
fn rows_of(v: &Value) -> Vec<String> {
    let mut vars: Vec<String> = v["head"]["vars"]
        .as_array()
        .expect("vars")
        .iter()
        .filter_map(|x| x.as_str().map(String::from))
        .collect();
    vars.sort();
    let mut rows: Vec<String> = v["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("not SPARQL JSON: {v}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|var| format!("{var}={}", b[var]["value"].as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    rows.sort();
    rows
}

/// JSON-LD array results as sorted serialized rows, with every IRI written out
/// in full. The JSON-LD formatter compacts a native ledger's IRIs against the
/// query's `@context` but writes a graph source's IRIs as they are (only SPARQL
/// JSON compacts those today), so `ex:order/1` and
/// `http://example.org/order/1` are the same node here.
fn jsonld_rows(v: &Value) -> Vec<String> {
    let mut rows: Vec<String> = v
        .as_array()
        .unwrap_or_else(|| panic!("not an array result: {v}"))
        .iter()
        .map(|row| row.to_string().replace("\"ex:", "\"http://example.org/"))
        .collect();
    rows.sort();
    rows
}

/// Run a `FROM <{G}>` SPARQL query on the source and on the twin; the rows
/// must agree. Returns them for the caller's fixed-fact checks.
async fn sparql_agrees(fluree: &Fluree, what: &str, body: &str) -> Vec<String> {
    let lake = sparql_json(fluree, &body.replace("{G}", LAKE))
        .await
        .unwrap_or_else(|e| panic!("{what} (source): {e}"));
    let twin = sparql_json(fluree, &body.replace("{G}", TWIN))
        .await
        .unwrap_or_else(|e| panic!("{what} (twin): {e}"));
    let (lake, twin) = (rows_of(&lake), rows_of(&twin));
    assert_eq!(lake, twin, "{what}: the source disagrees with its twin");
    lake
}

/// The JSON-LD twin of [`sparql_agrees`]: `{G}` anywhere in the query.
async fn jsonld_agrees(fluree: &Fluree, what: &str, query: &Value) -> Vec<String> {
    let run = |g: &str| {
        let q: Value = serde_json::from_str(&query.to_string().replace("{G}", g)).expect("json");
        async move {
            fluree
                .query_from()
                .jsonld(&q)
                .execute_formatted()
                .await
                .map_err(|e| e.to_string())
        }
    };
    let lake = run(LAKE)
        .await
        .unwrap_or_else(|e| panic!("{what} (source): {e}"));
    let twin = run(TWIN)
        .await
        .unwrap_or_else(|e| panic!("{what} (twin): {e}"));
    let (lake, twin) = (jsonld_rows(&lake), jsonld_rows(&twin));
    assert_eq!(lake, twin, "{what}: the source disagrees with its twin");
    lake
}

fn ctx() -> Value {
    json!({"ex": "http://example.org/"})
}

/// The issue's query: a per-entity grouped `COUNT` joined back to the outer
/// pattern. Every customer has four orders.
const GROUPED: &str = "SELECT ?n ?k FROM <{G}> WHERE { ?c ex:name ?n \
     { SELECT ?c (COUNT(?o) AS ?k) WHERE { ?o ex:customer ?c } GROUP BY ?c } }";

#[tokio::test]
async fn a_grouped_sub_select_joins_its_counts_to_the_outer_rows() {
    let fluree = setup().await;
    let rows = sparql_agrees(&fluree, "SPARQL grouped", GROUPED).await;
    assert_eq!(rows.len(), 50, "{rows:?}");
    assert!(rows.iter().all(|r| r.starts_with("k=4 ")), "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD grouped",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?n", "?k"],
            "where": [
                {"@id": "?c", "ex:name": "?n"},
                ["query", {
                    "@context": ctx(),
                    "select": ["?c", "(as (count ?o) ?k)"],
                    "where": {"@id": "?o", "ex:customer": "?c"},
                    "groupBy": ["?c"]
                }]
            ]
        }),
    )
    .await;
    assert_eq!(rows.len(), 50, "{rows:?}");
    assert!(rows.iter().all(|r| r.ends_with(",4]")), "{rows:?}");
}

/// An ungrouped `COUNT` is the silently-wrong case the refusal guarded: over
/// the empty native index it answers 0, as a success.
#[tokio::test]
async fn an_ungrouped_count_sub_select_counts_the_table() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL ungrouped",
        "SELECT ?k FROM <{G}> WHERE { { SELECT (COUNT(?o) AS ?k) WHERE { ?o ex:customer ?c } } }",
    )
    .await;
    assert_eq!(rows, ["k=200"]);

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD ungrouped",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?k"],
            "where": [["query", {
                "@context": ctx(),
                "select": ["(as (count ?o) ?k)"],
                "where": {"@id": "?o", "ex:customer": "?c"}
            }]]
        }),
    )
    .await;
    assert_eq!(rows, ["[200]"]);
}

/// `DISTINCT` inside the sub-select, then a join on its projected key.
#[tokio::test]
async fn a_distinct_sub_select_joins_on_its_key() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL distinct",
        "SELECT ?n ?k FROM <{G}> WHERE { \
           { SELECT DISTINCT ?c WHERE { ?o ex:customer ?c ; ex:amount ?a FILTER(?a > 900) } } \
           ?c ex:name ?n ; ex:country ?k }",
    )
    .await;
    assert!(!rows.is_empty() && rows.len() < 50, "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD distinct",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?n", "?k"],
            "where": [
                ["query", {
                    "@context": ctx(),
                    "select": ["?c"],
                    "distinct": true,
                    "where": [
                        {"@id": "?o", "ex:customer": "?c", "ex:amount": "?a"},
                        ["filter", "(> ?a 900)"]
                    ]
                }],
                {"@id": "?c", "ex:name": "?n", "ex:country": "?k"}
            ]
        }),
    )
    .await;
    assert!(!rows.is_empty() && rows.len() < 50, "{rows:?}");
}

/// `ORDER BY … LIMIT` inside the sub-select. A SPARQL sub-select is evaluated
/// independently (the five largest orders overall, then joined). A JSON-LD
/// sub-select is seeded per outer row, so the same shape is top-k per group:
/// the two largest orders of each customer.
#[tokio::test]
async fn a_top_k_sub_select_keeps_its_slice() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL top-k",
        "SELECT ?n ?o ?a FROM <{G}> WHERE { \
           { SELECT ?o ?c ?a WHERE { ?o ex:customer ?c ; ex:amount ?a } ORDER BY DESC(?a) LIMIT 5 } \
           ?c ex:name ?n }",
    )
    .await;
    assert_eq!(rows.len(), 5, "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD top-k per group",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?n", "?o", "?a"],
            "where": [
                {"@id": "?c", "ex:name": "?n"},
                ["query", {
                    "@context": ctx(),
                    "select": ["?c", "?o", "?a"],
                    "where": {"@id": "?o", "ex:customer": "?c", "ex:amount": "?a"},
                    "orderBy": "(desc ?a)",
                    "limit": 2
                }]
            ]
        }),
    )
    .await;
    assert_eq!(rows.len(), 100, "two orders for each of 50 customers");
}

/// `ORDER BY … LIMIT` over a scan its input already constrains, with no
/// sub-select: a native ledger binds `?c` outside the source's graph block, or
/// a `VALUES` binds it inside the source's scope. The scan joins its rows to
/// those bindings, so it must not first cut the table to the top rows of the
/// whole table (the three largest orders belong to customers 34..=36, none of
/// them a CRM customer or customer 1 or 2).
#[tokio::test]
async fn a_constrained_scan_orders_only_its_matching_rows() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL native rows joined into the source's block",
        &format!(
            "SELECT ?c ?o ?a FROM <{CRM}> FROM NAMED <{{G}}> WHERE {{ ?c ex:tier ?t \
               GRAPH <{{G}}> {{ ?o ex:customer ?c ; ex:amount ?a }} }} ORDER BY DESC(?a) LIMIT 3"
        ),
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD native rows joined into the source's block",
        &json!({
            "@context": ctx(),
            "from": CRM,
            "fromNamed": ["{G}"],
            "select": ["?c", "?o", "?a"],
            "where": [
                {"@id": "?c", "ex:tier": "?t"},
                ["graph", "{G}", {"@id": "?o", "ex:customer": "?c", "ex:amount": "?a"}]
            ],
            "orderBy": "(desc ?a)",
            "limit": 3
        }),
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");

    let rows = sparql_agrees(
        &fluree,
        "SPARQL VALUES in the source's scope",
        "SELECT ?o ?a FROM <{G}> WHERE { VALUES ?c { ex:customer/1 ex:customer/2 } \
           ?o ex:customer ?c ; ex:amount ?a } ORDER BY DESC(?a) LIMIT 3",
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD VALUES in the source's scope",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?o", "?a"],
            "where": [
                ["values", ["?c", [{"@id": "ex:customer/1"}, {"@id": "ex:customer/2"}]]],
                {"@id": "?o", "ex:customer": "?c", "ex:amount": "?a"}
            ],
            "orderBy": "(desc ?a)",
            "limit": 3
        }),
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");
}

/// A sub-select inside a sub-select: both bodies are lowered.
#[tokio::test]
async fn a_nested_sub_select_is_lowered_at_every_level() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL nested",
        "SELECT ?n ?k FROM <{G}> WHERE { ?c ex:name ?n \
           { SELECT ?c ?k WHERE { \
               { SELECT ?c (COUNT(?o) AS ?k) WHERE { ?o ex:customer ?c ; ex:amount ?a FILTER(?a > 500) } \
                 GROUP BY ?c } \
               FILTER(?k >= 2) } } }",
    )
    .await;
    assert!(!rows.is_empty() && rows.len() < 50, "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD nested",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?n", "?k"],
            "where": [
                {"@id": "?c", "ex:name": "?n"},
                ["query", {
                    "@context": ctx(),
                    "select": ["?c", "?k"],
                    "where": [
                        ["query", {
                            "@context": ctx(),
                            "select": ["?c", "(as (count ?o) ?k)"],
                            "where": [
                                {"@id": "?o", "ex:customer": "?c", "ex:amount": "?a"},
                                ["filter", "(> ?a 500)"]
                            ],
                            "groupBy": ["?c"]
                        }],
                        ["filter", "(>= ?k 2)"]
                    ]
                }]
            ]
        }),
    )
    .await;
    assert!(!rows.is_empty() && rows.len() < 50, "{rows:?}");
}

/// The source's own graph handle, with no `FROM`: once with the query wrapped
/// into the source's graph automatically, once with the user's own graph block
/// around the sub-select. Both answer what `FROM <source>` answers.
#[tokio::test]
async fn the_graph_handle_answers_with_and_without_a_graph_block() {
    let fluree = setup().await;
    let expected = sparql_agrees(&fluree, "SPARQL grouped", GROUPED).await;
    let graph = fluree.graph(LAKE);
    for (what, body) in [
        (
            "no graph block",
            "SELECT ?n ?k WHERE { ?c ex:name ?n \
             { SELECT ?c (COUNT(?o) AS ?k) WHERE { ?o ex:customer ?c } GROUP BY ?c } }"
                .to_string(),
        ),
        (
            "an explicit graph block",
            format!(
                "SELECT ?n ?k WHERE {{ GRAPH <{LAKE}> {{ ?c ex:name ?n \
                 {{ SELECT ?c (COUNT(?o) AS ?k) WHERE {{ ?o ex:customer ?c }} GROUP BY ?c }} }} }}"
            ),
        ),
    ] {
        let sparql = format!("{PREFIX}{body}");
        let rows = graph
            .query()
            .with_r2rml()
            .sparql(&sparql)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        assert_eq!(rows_of(&rows), expected, "{what}");
    }

    let query = json!({
        "@context": ctx(),
        "select": ["?n", "?k"],
        "where": [["graph", LAKE,
            {"@id": "?c", "ex:name": "?n"},
            ["query", {
                "@context": ctx(),
                "select": ["?c", "(as (count ?o) ?k)"],
                "where": {"@id": "?o", "ex:customer": "?c"},
                "groupBy": ["?c"]
            }]
        ]]
    });
    let rows = graph
        .query()
        .with_r2rml()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("JSON-LD graph block");
    let rows = jsonld_rows(&rows);
    assert_eq!(rows.len(), 50, "{rows:?}");
    assert!(rows.iter().all(|r| r.ends_with(",4]")), "{rows:?}");
}

/// Mixing a native ledger with the source. Inside the source's graph block a
/// sub-select reads the source, as every pattern there does (SPARQL's `GRAPH`
/// scopes a sub-select's body too); outside it, a sub-select reads the native
/// default graph and reaches the source through a graph block of its own.
#[tokio::test]
async fn a_sub_select_mixes_a_native_ledger_through_graph_blocks() {
    let fluree = setup().await;
    let rows = sparql_agrees(
        &fluree,
        "SPARQL sub-select inside the source's block",
        &format!(
            "SELECT ?n ?t ?k FROM <{CRM}> FROM NAMED <{{G}}> WHERE {{ ?c ex:tier ?t \
               GRAPH <{{G}}> {{ ?c ex:name ?n \
                 {{ SELECT ?c (COUNT(?o) AS ?k) WHERE {{ ?o ex:customer ?c }} GROUP BY ?c }} }} }}"
        ),
    )
    .await;
    assert_eq!(rows.len(), 12, "{rows:?}");
    assert!(rows.iter().all(|r| r.starts_with("k=4 ")), "{rows:?}");

    let rows = sparql_agrees(
        &fluree,
        "SPARQL native sub-select with a source block",
        &format!(
            "SELECT ?c ?k FROM <{CRM}> FROM NAMED <{{G}}> WHERE {{ \
               {{ SELECT ?c (COUNT(?o) AS ?k) WHERE {{ ?c ex:tier \"gold\" \
                   GRAPH <{{G}}> {{ ?o ex:customer ?c }} }} GROUP BY ?c }} }}"
        ),
    )
    .await;
    assert_eq!(rows.len(), 4, "customers 3, 6, 9, 12: {rows:?}");
    assert!(rows.iter().all(|r| r.ends_with(" k=4")), "{rows:?}");

    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD sub-select inside the source's block",
        &json!({
            "@context": ctx(),
            "from": CRM,
            "fromNamed": ["{G}"],
            "select": ["?n", "?t", "?k"],
            "where": [
                {"@id": "?c", "ex:tier": "?t"},
                ["graph", "{G}",
                    {"@id": "?c", "ex:name": "?n"},
                    ["query", {
                        "@context": ctx(),
                        "select": ["?c", "(as (count ?o) ?k)"],
                        "where": {"@id": "?o", "ex:customer": "?c"},
                        "groupBy": ["?c"]
                    }]
                ]
            ]
        }),
    )
    .await;
    assert_eq!(rows.len(), 12, "{rows:?}");
}

/// What a sub-select's body cannot lower still refuses, on both surfaces —
/// never an empty answer from the source's native index.
#[tokio::test]
async fn what_a_sub_select_cannot_lower_still_refuses() {
    let fluree = setup().await;

    // A transitive path inside the body.
    let err = sparql_json(
        &fluree,
        &format!(
            "SELECT ?c FROM <{LAKE}> WHERE {{ {{ SELECT ?c WHERE {{ ?c ex:customer+ ?d }} }} }}"
        ),
    )
    .await
    .expect_err("a path inside a sub-select must refuse");
    assert!(
        err.contains("cannot be evaluated over a virtual dataset") && err.contains("property path"),
        "{err}"
    );

    // A variable predicate with a bound object inside the body.
    let err = sparql_json(
        &fluree,
        &format!(
            "SELECT ?c FROM <{LAKE}> WHERE {{ {{ SELECT ?c WHERE {{ ?c ?p \"Customer 001\" }} }} }}"
        ),
    )
    .await
    .expect_err("an unconvertible triple inside a sub-select must refuse");
    assert!(err.contains("variable predicate"), "{err}");
    let query = json!({
        "@context": ctx(),
        "from": LAKE,
        "select": ["?c"],
        "where": [["query", {
            "@context": ctx(),
            "select": ["?c"],
            "where": {"@id": "?c", "?p": "Customer 001"}
        }]]
    });
    let err = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect_err("JSON-LD: an unconvertible triple inside a sub-select must refuse")
        .to_string();
    assert!(err.contains("variable predicate"), "{err}");

    // A sub-select outside every graph scope: the user wrote a graph block of
    // their own, so nothing wraps the query, and nothing lowers this body.
    let sparql = format!(
        "{PREFIX}SELECT ?n ?k WHERE {{ GRAPH <{LAKE}> {{ ?c ex:name ?n }} \
         {{ SELECT ?c (COUNT(?o) AS ?k) WHERE {{ ?o ex:customer ?c }} GROUP BY ?c }} }}"
    );
    let err = fluree
        .graph(LAKE)
        .query()
        .with_r2rml()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .expect_err("a sub-select outside the graph block must refuse")
        .to_string();
    assert!(
        err.contains("subquery") && err.contains("graph block"),
        "{err}"
    );
}

/// A JSON-LD sub-select with a slice is planned after the `VALUES` that binds
/// its key and seeded with it per outer row, which is sound only because a
/// scan binds that variable in every row it emits: a row leaving it unbound
/// would take the seed's value instead. Each scan shape that could leave one
/// unbound is seeded here, under a `LIMIT` that cuts nothing: a referencing
/// object map that dangles for most rows (customer 60 must not acquire
/// orders), a type variable, and a variable-predicate wildcard's predicate.
#[tokio::test]
async fn a_seeded_json_ld_sub_select_joins_on_its_scan_variables() {
    let fluree = setup().await;
    let customers = |ids: &[u32]| -> Value {
        json!(ids
            .iter()
            .map(|i| json!({"@id": format!("http://example.org/customer/{i}")}))
            .collect::<Vec<_>>())
    };

    // The dangling referencing object map, joined on its object.
    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD join on a dangling FK's object",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?c", "?o"],
            "where": [
                ["values", ["?c", customers(&[1, 2, 60])]],
                ["query", {
                    "@context": ctx(),
                    "select": ["?c", "?o"],
                    "where": {"@id": "?o", "ex:numberedLike": "?c"},
                    "limit": 1000
                }]
            ]
        }),
    )
    .await;
    assert_eq!(
        rows,
        [
            r#"["http://example.org/customer/1","http://example.org/order/1"]"#,
            r#"["http://example.org/customer/2","http://example.org/order/2"]"#
        ],
        "customer 60 does not exist"
    );

    // A type variable.
    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD join on a type variable",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?t", "?n"],
            "where": [
                ["values", ["?t", [{"@id": "ex:Customer"}, {"@id": "ex:Order"}]]],
                ["query", {
                    "@context": ctx(),
                    "select": ["?t", "(as (count ?s) ?n)"],
                    "where": {"@id": "?s", "@type": "?t"},
                    "groupBy": ["?t"],
                    "limit": 1000
                }]
            ]
        }),
    )
    .await;
    assert_eq!(
        rows,
        [
            r#"["http://example.org/Customer",50]"#,
            r#"["http://example.org/Order",200]"#
        ]
    );

    // A variable-predicate wildcard, joined on its predicate.
    let rows = jsonld_agrees(
        &fluree,
        "JSON-LD join on a wildcard's predicate",
        &json!({
            "@context": ctx(),
            "from": "{G}",
            "select": ["?p", "?n"],
            "where": [
                ["values", ["?p", [{"@id": "ex:name"}, {"@id": "ex:numberedLike"}]]],
                ["query", {
                    "@context": ctx(),
                    "select": ["?p", "(as (count ?v) ?n)"],
                    "where": {"@id": "?s", "?p": "?v"},
                    "groupBy": ["?p"],
                    "limit": 1000
                }]
            ]
        }),
    )
    .await;
    assert_eq!(
        rows,
        [
            r#"["http://example.org/name",50]"#,
            r#"["http://example.org/numberedLike",50]"#
        ]
    );
}

/// How many times each shape reads its tables, counted from the scan spans,
/// against the same patterns written without a sub-select. A SPARQL sub-select
/// is evaluated once. A JSON-LD sub-select correlated on a variable its scans
/// bind in every row is evaluated once and hash-joined, so its reads do not
/// grow with the outer rows; were that variable not a join key it would be
/// re-evaluated, unseeded, once per outer row.
#[tokio::test(flavor = "current_thread")]
async fn scans_per_shape() {
    let fluree = setup().await;
    let (store, _guard) = span_capture::init_test_tracing();
    let count = || store.find_spans("r2rml.scan_table").len();

    let before = count();
    sparql_json(
        &fluree,
        &format!("SELECT ?n ?o FROM <{LAKE}> WHERE {{ ?c ex:name ?n . ?o ex:customer ?c }}"),
    )
    .await
    .expect("flat");
    let flat = count() - before;

    let before = count();
    sparql_json(&fluree, &GROUPED.replace("{G}", LAKE))
        .await
        .expect("grouped");
    let grouped = count() - before;

    let jsonld_seeded = |n: usize| {
        let ids: Vec<Value> = (1..=n)
            .map(|i| json!({"@id": format!("http://example.org/customer/{i}")}))
            .collect();
        json!({
            "@context": ctx(),
            "from": LAKE,
            "select": ["?n", "?o"],
            "where": [
                ["values", ["?c", ids]],
                {"@id": "?c", "ex:name": "?n"},
                ["query", {
                    "@context": ctx(),
                    "select": ["?c", "?o"],
                    "where": {"@id": "?o", "ex:customer": "?c"}
                }]
            ]
        })
    };
    let mut seeded = Vec::new();
    let mut millis = Vec::new();
    for n in [3usize, 12] {
        let started = std::time::Instant::now();
        let before = count();
        let rows = fluree
            .query_from()
            .jsonld(&jsonld_seeded(n))
            .execute_formatted()
            .await
            .expect("seeded");
        assert_eq!(rows.as_array().map(Vec::len), Some(4 * n), "{rows}");
        seeded.push((n, count() - before));
        millis.push(started.elapsed().as_millis());
    }
    // The same sub-select under a parent that is only a `VALUES`: planned
    // after it, so correlated on `?c`.
    let values_only = |n: usize| {
        let mut q = jsonld_seeded(n);
        q["select"] = json!(["?c", "?o"]);
        q["where"].as_array_mut().expect("where").remove(1);
        q
    };
    let mut correlated = Vec::new();
    for n in [3usize, 12] {
        let started = std::time::Instant::now();
        let before = count();
        let rows = fluree
            .query_from()
            .jsonld(&values_only(n))
            .execute_formatted()
            .await
            .expect("values only");
        assert_eq!(rows.as_array().map(Vec::len), Some(4 * n), "{rows}");
        correlated.push((n, count() - before));
        millis.push(started.elapsed().as_millis());
    }
    eprintln!(
        "scans: flat = {flat}; SPARQL grouped = {grouped}; \
         JSON-LD seeded (outer rows, scans) = {seeded:?}; \
         JSON-LD under VALUES only = {correlated:?}; ms = {millis:?}"
    );
    assert!(flat > 0, "the scan spans are captured");
    assert_eq!(
        grouped, flat,
        "a grouped sub-select reads what its patterns read"
    );
    assert_eq!(
        seeded,
        [(3, flat), (12, flat)],
        "a seeded JSON-LD sub-select is evaluated once, whatever the outer rows"
    );
}
