//! Stored lists in export.
//!
//! A list is stored as items that carry a list index (a JSON-LD `@list`, or a
//! Turtle `( … )` in object position), and the index scan reaches them in
//! object order. Export writes each list in its stored order: Turtle and TriG
//! as `( … )` and JSON-LD as `{"@list": […]}`, which re-insert as the same
//! list, and N-Triples and N-Quads, which have no list syntax, as the
//! `rdf:first`/`rdf:rest` spine a collection denotes.

use crate::support;
use fluree_db_api::export::ExportFormat;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const RDF_FIRST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
const RDF_REST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
const RDF_NIL: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
const NAMED: &str = "http://example.org/g";

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn export(fluree: &fluree_db_api::Fluree, ledger_id: &str, format: ExportFormat) -> String {
    let mut builder = fluree.export(ledger_id).format(format).context(&ctx());
    if matches!(format, ExportFormat::TriG | ExportFormat::NQuads) {
        builder = builder.all_graphs();
    }
    let mut out = Vec::new();
    builder.write_to(&mut out).await.expect("export");
    String::from_utf8(out).expect("UTF-8 export")
}

async fn insert_jsonld(fluree: &fluree_db_api::Fluree, ledger_id: &str, data: &JsonValue) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(data)
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{data}"));
}

async fn insert_text(fluree: &fluree_db_api::Fluree, ledger_id: &str, text: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(text)
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{text}"));
}

/// `subject`'s properties as a JSON-LD crawl returns them: a list in order.
async fn crawl(fluree: &fluree_db_api::Fluree, ledger_id: &str, subject: &str) -> JsonValue {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let query = json!({"@context": ctx(), "selectOne": {subject: ["*"]}});
    support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("crawl")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld")
}

/// Every default-graph value, sorted: what a list's items are, in any order.
/// A JSON-LD query, which sees a list's members as values; SPARQL sees its
/// cells, whose labels a ledger names in its own order.
async fn triples(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<JsonValue> {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let query = json!({
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let rows = support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&rows)
}

/// The items of the one list `subject predicate` names in an N-Triples
/// export, read by walking its spine.
fn spine_items(ntriples: &str, subject: &str, predicate: &str) -> Vec<String> {
    let triples: Vec<(&str, &str, &str)> = ntriples
        .lines()
        .filter_map(|line| {
            let (s, rest) = line.strip_suffix(" .")?.split_once(' ')?;
            let (p, o) = rest.split_once(' ')?;
            Some((s, p, o))
        })
        .collect();
    let objects = |s: &str, p: &str| -> Vec<&str> {
        triples
            .iter()
            .filter(|t| t.0 == s && t.1 == p)
            .map(|t| t.2)
            .collect()
    };
    let one = |s: &str, p: &str| match objects(s, p)[..] {
        [o] => o,
        ref found => panic!("{s} {p} has one object, not {found:?}: {ntriples}"),
    };
    let heads: Vec<&str> = objects(subject, predicate)
        .into_iter()
        .filter(|o| o.starts_with("_:"))
        .collect();
    let [mut cell] = heads[..] else {
        panic!("one list head, not {heads:?}: {ntriples}");
    };
    let mut items = Vec::new();
    while cell != RDF_NIL {
        assert!(
            cell.starts_with("_:"),
            "a spine cell is a blank node: {cell}"
        );
        items.push(one(cell, RDF_FIRST).to_string());
        cell = one(cell, RDF_REST);
    }
    items
}

/// A Turtle export's subject blocks, sorted: a copy assigns subject ids, and
/// so orders its blocks, by its own insert order.
fn subject_blocks(ttl: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    for line in ttl.lines().filter(|l| !l.is_empty()) {
        match blocks.last_mut() {
            Some(block) if line.starts_with(' ') => {
                block.push('\n');
                block.push_str(line);
            }
            _ => blocks.push(line.to_string()),
        }
    }
    blocks.sort();
    blocks
}

/// Each format writes every list in stored order, and the Turtle, TriG and
/// JSON-LD exports re-insert as ledgers holding the same lists, types and
/// annotations — from novelty (where most rows miss V3 translation), from the
/// index, and from novelty over an index, where one list mixes rows that
/// translate with a decimal, which does not. JSON-LD's `@type` holds IRIs
/// only, so its export writes an annotated or list-valued type as `rdf:type`.
#[tokio::test]
async fn lists_export_in_order_and_reinsert_as_the_same_lists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let source = "it/export-lists/source:main";
    fluree.create_ledger(source).await.expect("create");
    insert_jsonld(
        &fluree,
        source,
        &json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:s", "ex:items": {"@list": ["c", "a", "b", "a"]}},
                {"@id": "ex:t", "ex:refs": {"@list": [{"@id": "ex:z"}, {"@id": "ex:y"}]}},
                {
                    "@id": "ex:u",
                    "ex:name": "u",
                    "ex:mixed": {"@list": ["b", {"@value": "2.5", "@type": "xsd:decimal"}, "a"]}
                }
            ]
        }),
    )
    .await;
    insert_text(
        &fluree,
        source,
        "@prefix ex: <http://example.org/> .
         ex:w a ex:A ~ ex:r {| ex:since 2020 |} , ex:B , ( ex:C ex:D ) ;
              ex:label \"w\" ~ ex:q {| ex:since 2021 |} .",
    )
    .await;

    let mut subjects = vec!["ex:s", "ex:t", "ex:u", "ex:w", "ex:r", "ex:q"];
    for lane in ["novelty", "index", "overlay"] {
        match lane {
            "index" => support::rebuild_and_publish_index(&fluree, source).await,
            "overlay" => {
                insert_jsonld(
                    &fluree,
                    source,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:v",
                        "ex:mixed": {"@list": ["z", {"@value": "1.5", "@type": "xsd:decimal"}, "y"]}
                    }),
                )
                .await;
                subjects.push("ex:v");
            }
            _ => {}
        }
        assert_eq!(
            crawl(&fluree, source, "ex:s").await["ex:items"],
            json!(["c", "a", "b", "a"]),
            "{lane}: the source holds the list in order"
        );

        let ttl = export(&fluree, source, ExportFormat::Turtle).await;
        assert!(
            ttl.contains(r#"ex:items ( "c" "a" "b" "a" )"#),
            "{lane}: {ttl}"
        );
        assert!(ttl.contains("ex:refs ( ex:z ex:y )"), "{lane}: {ttl}");
        let jsonld = export(&fluree, source, ExportFormat::JsonLd).await;
        assert!(
            jsonld.contains(r#""ex:items": {"@list":["c","a","b","a"]}"#),
            "{lane}: {jsonld}"
        );
        let nt = export(&fluree, source, ExportFormat::NTriples).await;
        assert_eq!(
            spine_items(&nt, "<http://example.org/s>", "<http://example.org/items>"),
            [r#""c""#, r#""a""#, r#""b""#, r#""a""#],
            "{lane}"
        );
        assert_eq!(
            spine_items(&nt, "<http://example.org/u>", "<http://example.org/mixed>"),
            [
                r#""b""#,
                r#""2.5"^^<http://www.w3.org/2001/XMLSchema#decimal>"#,
                r#""a""#
            ],
            "{lane}"
        );
        if lane == "overlay" {
            assert_eq!(
                spine_items(&nt, "<http://example.org/v>", "<http://example.org/mixed>"),
                [
                    r#""z""#,
                    r#""1.5"^^<http://www.w3.org/2001/XMLSchema#decimal>"#,
                    r#""y""#
                ],
            );
        }

        let trig = export(&fluree, source, ExportFormat::TriG).await;
        for (name, text) in [("ttl", &ttl), ("trig", &trig), ("jsonld", &jsonld)] {
            let copy = format!("it/export-lists/{lane}-{name}:main");
            fluree.create_ledger(&copy).await.expect("create copy");
            match name {
                "jsonld" => {
                    let doc: JsonValue = serde_json::from_str(text).expect("JSON-LD export");
                    insert_jsonld(&fluree, &copy, &doc).await;
                }
                _ => insert_text(&fluree, &copy, text).await,
            }
            assert_eq!(
                triples(&fluree, &copy).await,
                triples(&fluree, source).await,
                "{lane} {name}: the same items"
            );
            for subject in &subjects {
                assert_eq!(
                    crawl(&fluree, &copy, subject).await,
                    crawl(&fluree, source, subject).await,
                    "{lane} {name}: {subject}'s lists in the same order"
                );
            }
        }

        // N-Triples re-inserts as the spine it spells out.
        let copy = format!("it/export-lists/{lane}-nt:main");
        fluree.create_ledger(&copy).await.expect("create copy");
        insert_text(&fluree, &copy, &nt).await;
        let ledger = fluree.ledger(&copy).await.expect("load");
        let rows = support::query_sparql(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/>
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             SELECT ?first WHERE { ex:s ex:items/rdf:first ?first }",
        )
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
        assert_eq!(support::normalize_rows(&rows), [json!(["c"])], "{lane} nt");
    }
}

/// A list in a named graph round-trips through TriG, and in N-Quads each
/// graph's spine has cells of its own: a dataset's blank-node labels are
/// shared by all its graphs.
#[tokio::test]
async fn named_graph_lists_keep_their_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let source = "it/export-lists/named:main";
    fluree.create_ledger(source).await.expect("create");
    insert_text(
        &fluree,
        source,
        &format!(
            "@prefix ex: <http://example.org/> .
             ex:s ex:items ( \"b\" \"a\" ) .
             GRAPH <{NAMED}> {{ ex:s ex:items ( \"y\" \"x\" ) . }}"
        ),
    )
    .await;

    let trig = export(&fluree, source, ExportFormat::TriG).await;
    let copy = "it/export-lists/named-copy:main";
    fluree.create_ledger(copy).await.expect("create copy");
    insert_text(&fluree, copy, &trig).await;
    assert_eq!(export(&fluree, copy, ExportFormat::TriG).await, trig);
    let (default, named) = trig.split_once("GRAPH").expect("a GRAPH block");
    assert!(default.contains(r#"ex:items ( "b" "a" )"#), "{trig}");
    assert!(named.contains(r#"ex:items ( "y" "x" )"#), "{trig}");

    let nq = export(&fluree, source, ExportFormat::NQuads).await;
    let mut cells: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for line in nq.lines() {
        let Some(cell) = line.strip_prefix("_:") else {
            continue;
        };
        let label = cell.split(' ').next().unwrap_or_default();
        let graph = if line.ends_with(&format!("<{NAMED}> .")) {
            "named"
        } else {
            "default"
        };
        assert_eq!(
            *cells.entry(label).or_insert(graph),
            graph,
            "cell {label} is in one graph: {nq}"
        );
    }
    assert_eq!(cells.len(), 4, "two cells a list: {nq}");
}

/// A predicate can hold a list and plain values side by side, and an
/// annotation names the plain edge, never the list item with the same object:
/// the list index is part of the edge. Every format writes both, the marker
/// once, and the Turtle and JSON-LD exports re-insert as the same ledger.
#[tokio::test]
async fn a_list_and_an_annotated_value_share_a_predicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let source = "it/export-lists/shared:main";
    fluree.create_ledger(source).await.expect("create");
    insert_text(
        &fluree,
        source,
        "@prefix ex: <http://example.org/> .
         ex:s ex:items \"a\" ~ ex:r {| ex:since 2020 |} , ( \"b\" \"a\" ) .",
    )
    .await;

    let stats = |format| {
        let fluree = &fluree;
        async move {
            let mut sink = Vec::new();
            fluree
                .export(source)
                .format(format)
                .write_to(&mut sink)
                .await
                .expect("export")
        }
    };
    let ttl = export(&fluree, source, ExportFormat::Turtle).await;
    assert!(ttl.contains(r#"ex:items "a" ~ ex:r ;"#), "{ttl}");
    assert!(ttl.contains(r#"ex:items ( "b" "a" ) ."#), "{ttl}");
    assert_eq!(ttl.matches("~ ex:r").count(), 1, "{ttl}");
    let nt = export(&fluree, source, ExportFormat::NTriples).await;
    assert_eq!(
        spine_items(&nt, "<http://example.org/s>", "<http://example.org/items>"),
        [r#""b""#, r#""a""#]
    );
    assert_eq!(nt.matches("#reifies>").count(), 1, "{nt}");
    let jsonld = export(&fluree, source, ExportFormat::JsonLd).await;
    assert_eq!(jsonld.matches("@annotation").count(), 1, "{jsonld}");
    for format in [
        ExportFormat::Turtle,
        ExportFormat::NTriples,
        ExportFormat::JsonLd,
    ] {
        let stats = stats(format).await;
        assert_eq!(stats.annotations_unresolved, 0, "{format:?}");
        assert_eq!(
            stats.triples_written, 4,
            "{format:?}: three items and the since"
        );
    }

    for (name, text) in [("ttl", &ttl), ("jsonld", &jsonld)] {
        let copy = format!("it/export-lists/shared-{name}:main");
        fluree.create_ledger(&copy).await.expect("create copy");
        match name {
            "jsonld" => {
                let doc: JsonValue = serde_json::from_str(text).expect("JSON-LD export");
                insert_jsonld(&fluree, &copy, &doc).await;
            }
            _ => insert_text(&fluree, &copy, text).await,
        }
        assert_eq!(
            subject_blocks(&export(&fluree, &copy, ExportFormat::Turtle).await),
            subject_blocks(&ttl),
            "{name}"
        );
    }
}

/// Novelty holds each fact's whole lifecycle, and export writes a fact's
/// latest state: a fact asserted and then deleted since the index was built
/// is not exported, and neither are the members a SPARQL edit re-stored as
/// triples. From a ledger with no index, and over one.
#[tokio::test]
async fn facts_deleted_since_the_index_are_not_exported() {
    let fluree = FlureeBuilder::memory().build_memory();
    for indexed in [false, true] {
        let id = format!("it/export-lists/deleted-{indexed}:main");
        fluree.create_ledger(&id).await.expect("create");
        insert_text(
            &fluree,
            &id,
            "@prefix ex: <http://example.org/> .
             ex:s ex:name \"s\" ; ex:items ( \"a\" \"b\" ) .",
        )
        .await;
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        insert_jsonld(
            &fluree,
            &id,
            &json!({"@context": ctx(), "@id": "ex:t", "ex:name": "t"}),
        )
        .await;
        fluree
            .graph(&id)
            .transact()
            .update(&json!({"@context": ctx(), "delete": {"@id": "ex:t", "ex:name": "t"}}))
            .commit()
            .await
            .expect("delete");
        fluree
            .graph(&id)
            .transact()
            .sparql_update(
                "PREFIX ex: <http://example.org/>
                 INSERT { ?h ex:note \"x\" } WHERE { ex:s ex:items ?h }",
            )
            .commit()
            .await
            .expect("note the list's head");

        for format in [
            ExportFormat::NTriples,
            ExportFormat::Turtle,
            ExportFormat::JsonLd,
        ] {
            let text = export(&fluree, &id, format).await;
            assert!(
                !text.contains("example.org/t") && !text.contains("ex:t"),
                "{indexed} {format:?}: {text}"
            );
            for member in ["\"a\"", "\"b\""] {
                assert_eq!(
                    text.matches(member).count(),
                    1,
                    "{indexed} {format:?}: {member} once, in the re-stored list: {text}"
                );
            }
        }
    }
}
