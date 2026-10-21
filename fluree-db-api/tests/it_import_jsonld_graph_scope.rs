//! Bulk JSON-LD import writes graph-scoped data where a transaction does.
//!
//! Each document is written by `insert` and by a bulk import, and the
//! statements of every graph are compared with the expected ones. The
//! import is read three ways: from the index it built, after a full rebuild
//! from its commits, and with no index at all, where a load replays the
//! commits and finds each named graph through the commit's `graph_delta`.

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};
use std::path::Path;

const EX: &str = "http://example.org/";
/// One ledger id for both lanes (each has its own storage root), so a
/// document that names the ledger's own address means the same on both.
const TXN: &str = "it/jsonld-scope:main";
const IMPORT: &str = TXN;

fn fluree_at(dir: &Path) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree")
}

fn ctx() -> Value {
    json!({"ex": EX})
}

/// `graph s p o` for every statement in the default graph and in `ex:g` and
/// `ex:h`, sorted, IRIs under `ex:` written `ex:` and blank nodes `_:`.
async fn statements(fluree: &Fluree, id: &str) -> Vec<String> {
    let ledger = fluree.ledger(id).await.expect("load");
    let mut out = Vec::new();
    for (label, from) in [
        ("default", id.to_string()),
        ("g", format!("{id}#{EX}g")),
        ("h", format!("{id}#{EX}h")),
    ] {
        let query = json!({
            "from": from,
            "select": ["?s", "?p", "?o"],
            "where": {"@id": "?s", "?p": "?o"}
        });
        let rows = match fluree.query_connection(&query).await {
            Ok(rows) => rows.to_jsonld(&ledger.snapshot).expect("jsonld"),
            // A graph nothing was written to is not registered.
            Err(e) if e.to_string().contains("not found") => continue,
            Err(e) => panic!("{label}: {e}"),
        };
        for row in rows.as_array().expect("rows") {
            let cells: Vec<String> = row
                .as_array()
                .expect("row")
                .iter()
                .map(|cell| {
                    let cell = match cell {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if cell.starts_with("_:") {
                        "_:".to_string()
                    } else {
                        cell.replace(EX, "ex:")
                    }
                })
                .collect();
            out.push(format!("{label}: {}", cells.join(" ")));
        }
    }
    out.sort();
    out
}

async fn transact(doc: &Value) -> Result<Vec<String>, String> {
    let dir = tempfile::tempdir().unwrap();
    let fluree = fluree_at(dir.path());
    let ledger = fluree.create_ledger(TXN).await.unwrap();
    fluree
        .insert(ledger, doc)
        .await
        .map_err(|e| e.to_string())?;
    Ok(statements(&fluree_at(dir.path()), TXN).await)
}

/// Import `files` into a fresh ledger at `dir`: the one file, or a
/// directory holding them all. With `build_index` off the import writes its
/// commits only.
async fn import_into(
    dir: &Path,
    files: &[(&str, String)],
    build_index: bool,
) -> Result<(), String> {
    let data = tempfile::tempdir().unwrap();
    for (name, body) in files {
        std::fs::write(data.path().join(name), body).unwrap();
    }
    let source = match files {
        [(name, _)] => data.path().join(name),
        _ => data.path().to_path_buf(),
    };
    fluree_at(dir)
        .create(IMPORT)
        .import(&source)
        .threads(1)
        .memory_budget_mb(256)
        .build_index(build_index)
        .execute()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Import `files` and read the statements back. The statements read from
/// the index the import built must equal those read after a full rebuild of
/// its commits, and those read from its commits alone.
async fn import_files(files: &[(&str, String)]) -> Result<Vec<String>, String> {
    let dir = tempfile::tempdir().unwrap();
    import_into(dir.path(), files, true).await?;

    let fluree = fluree_at(dir.path());
    let ledger = fluree.ledger(IMPORT).await.expect("load");
    assert_eq!(
        ledger.index_t(),
        ledger.t(),
        "the import is read from its index"
    );
    let indexed = statements(&fluree, IMPORT).await;

    fluree
        .reindex(IMPORT, ReindexOptions::default())
        .await
        .expect("full rebuild");
    let rebuilt = statements(&fluree_at(dir.path()), IMPORT).await;
    assert_eq!(
        indexed, rebuilt,
        "a rebuild from the commits reads the same graphs"
    );

    let unindexed = tempfile::tempdir().unwrap();
    import_into(unindexed.path(), files, false).await?;
    let fluree = fluree_at(unindexed.path());
    let ledger = fluree.ledger(IMPORT).await.expect("load");
    assert!(
        ledger.index_t() < ledger.t(),
        "the commits are read without an index"
    );
    let replayed = statements(&fluree, IMPORT).await;
    assert_eq!(indexed, replayed, "the commits alone read the same graphs");
    Ok(indexed)
}

async fn import(name: &str, body: &str) -> Result<Vec<String>, String> {
    import_files(&[(name, body.to_string())]).await
}

/// Import `doc`, expecting a refusal: the refusal, and every statement the
/// ledger holds afterwards.
async fn import_refused(doc: &Value) -> (Option<String>, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let refusal = import_into(dir.path(), &[("data.jsonld", doc.to_string())], true)
        .await
        .err();
    let fluree = fluree_at(dir.path());
    let written = match fluree.ledger(IMPORT).await {
        Ok(_) => statements(&fluree, IMPORT).await,
        Err(_) => Vec::new(),
    };
    (refusal, written)
}

async fn import_jsonld(doc: &Value) -> Result<Vec<String>, String> {
    import("data.jsonld", &doc.to_string()).await
}

/// Every graph-scoping form a transaction honours, and the statements it
/// writes. Bulk import used to refuse each of them.
fn graph_scoped_forms() -> Vec<(&'static str, Value, Vec<&'static str>)> {
    let c = ctx();
    vec![
        (
            "an envelope",
            json!({"@context": c, "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "@included without a selector",
            json!({"@context": c, "@id": "ex:a", "ex:q": "v",
                "@included": [{"@id": "ex:b", "ex:q": "w"}]}),
            vec!["default: ex:a ex:q v", "default: ex:b ex:q w"],
        ),
        (
            // A transaction's routing key, not a selector: dropped on both.
            "a top-level bare `graph` key on a node",
            json!({"@context": c, "@id": "ex:a", "graph": "ex:g", "ex:q": "v"}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "a named-graph document",
            json!({"@context": c, "@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a named-graph document with a property of its own",
            json!({"@context": c, "@id": "ex:g", "ex:label": "G",
                "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
            vec!["default: ex:g ex:label G", "g: ex:a ex:q v"],
        ),
        (
            "an envelope holding a named graph and a default-graph node",
            json!({"@context": c, "@graph": [
                {"@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]},
                {"@id": "ex:b", "ex:q": "w"}
            ]}),
            vec!["default: ex:b ex:q w", "g: ex:a ex:q v"],
        ),
        (
            "a named graph with no content",
            json!({"@context": c, "@id": "ex:g", "ex:label": "G", "@graph": []}),
            vec!["default: ex:g ex:label G"],
        ),
        (
            "a named graph as a property value",
            json!({"@context": c, "@id": "ex:a",
                "ex:p": {"@id": "ex:g", "@graph": [{"@id": "ex:b", "ex:q": "w"}]}}),
            vec!["default: ex:a ex:p ex:g", "g: ex:b ex:q w"],
        ),
        (
            "a named graph holding a node with its own selector",
            json!({"@context": c, "@id": "ex:g", "@graph": [
                {"@id": "ex:a", "@graph": "ex:h", "ex:q": "v"},
                {"@id": "ex:b", "ex:q": "w"}
            ]}),
            vec!["g: ex:b ex:q w", "h: ex:a ex:q v"],
        ),
        (
            "a selector",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g", "ex:q": "v"}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a selector written as a node reference",
            json!({"@context": c, "@id": "ex:a", "@graph": {"@id": "ex:g"}, "ex:q": "v"}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a selector relative to @base",
            json!({"@context": {"ex": EX, "@base": EX}, "@id": "ex:a", "@graph": "g", "ex:q": "v"}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a selector naming this ledger's own address",
            json!({"@context": c, "@id": "ex:a", "@graph": format!("urn:fluree:{TXN}"), "ex:q": "v"}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "a selector naming this ledger's bare address",
            json!({"@context": c, "@id": "ex:a", "@graph": TXN, "ex:q": "v"}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "a selector naming one of this ledger's graphs by its address",
            json!({"@context": c, "@id": "ex:a", "@graph": format!("urn:fluree:{TXN}#{EX}g"),
                "ex:q": "v"}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a named graph named by this ledger's own address",
            json!({"@context": c, "@id": format!("urn:fluree:{TXN}"),
                "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "a selector naming the default graph",
            json!({"@context": c, "@id": "ex:a", "@graph": "default", "ex:q": "v"}),
            vec!["default: ex:a ex:q v"],
        ),
        (
            "a bare `graph` selector on a node in an envelope",
            json!({"@context": c, "@graph": [{"@id": "ex:a", "graph": "ex:g", "ex:q": "v"}]}),
            vec!["g: ex:a ex:q v"],
        ),
        (
            "a selector over a nested node",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g",
                "ex:knows": {"@id": "ex:b", "ex:q": "w"}}),
            vec!["g: ex:a ex:knows ex:b", "g: ex:b ex:q w"],
        ),
        (
            "a selector over an anonymous nested node",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g", "ex:knows": {"ex:q": "w"}}),
            vec!["g: _: ex:q w", "g: ex:a ex:knows _:"],
        ),
        (
            "a selector over a list of nodes",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g",
                "ex:items": {"@list": [{"@id": "ex:b", "ex:q": "w"}]}}),
            vec!["g: ex:a ex:items ex:b", "g: ex:b ex:q w"],
        ),
        (
            "a selector over @included",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g", "ex:q": "v",
                "@included": [{"@id": "ex:b", "ex:q": "w"}]}),
            vec!["g: ex:a ex:q v", "g: ex:b ex:q w"],
        ),
        (
            "a selector over @reverse",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g",
                "@reverse": {"ex:knows": {"@id": "ex:b"}}}),
            vec!["g: ex:b ex:knows ex:a"],
        ),
        (
            "a selector over an annotated edge",
            json!({"@context": c, "@id": "ex:a", "@graph": "ex:g",
                "ex:knows": {"@id": "ex:b", "@annotation": {"ex:since": 2020}}}),
            vec!["g: _: ex:since 2020", "g: ex:a ex:knows ex:b"],
        ),
        (
            "a list of nodes in the default graph",
            json!({"@context": c, "@id": "ex:a",
                "ex:items": {"@list": [{"@id": "ex:b", "ex:q": "w"}]}}),
            vec!["default: ex:a ex:items ex:b", "default: ex:b ex:q w"],
        ),
    ]
}

#[tokio::test]
async fn bulk_import_writes_each_graph_scoped_form_where_insert_does() {
    let mut mismatches = Vec::new();
    for (form, doc, expected) in graph_scoped_forms() {
        let expected: Vec<String> = expected.into_iter().map(String::from).collect();
        let transacted = transact(&doc).await;
        let imported = import_jsonld(&doc).await;
        if transacted.as_ref() != Ok(&expected) || imported.as_ref() != Ok(&expected) {
            mismatches.push(format!(
                "{form}\n  expected: {expected:?}\n  insert:   {transacted:?}\n  import:   {imported:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// Newline-delimited JSON-LD: the lines after a leading `@context` line are
/// the nodes of one envelope, so a named-graph line writes its graph and a
/// plain line the default graph. A selector resolves against that shared
/// context, as an envelope's does.
#[tokio::test]
async fn bulk_import_of_jsonl_writes_each_line_to_its_graph() {
    let c = ctx();
    let nodes = [
        json!({"@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
        json!({"@id": "ex:b", "ex:q": "w"}),
        json!({"@id": "ex:c", "@graph": "ex:h", "ex:q": "x"}),
    ];
    let expected = vec!["default: ex:b ex:q w", "g: ex:a ex:q v", "h: ex:c ex:q x"];
    let envelope = json!({"@context": c, "@graph": nodes});
    assert_eq!(transact(&envelope).await.unwrap(), expected, "insert");
    let body: String = std::iter::once(json!({"@context": c}))
        .chain(nodes)
        .map(|line| format!("{line}\n"))
        .collect();
    assert_eq!(
        import("data.jsonl", &body).await.unwrap(),
        expected,
        "import"
    );
}

/// TriG writes named graphs on a path of its own; a JSON-LD import of the
/// same statements reads back the same.
#[tokio::test]
async fn bulk_import_of_jsonld_named_graphs_reads_back_like_trig() {
    let trig = "@prefix ex: <http://example.org/> .\n\
                ex:b ex:q \"w\" .\n\
                GRAPH <http://example.org/g> { ex:a ex:q \"v\" . }\n\
                GRAPH <http://example.org/h> { ex:c ex:q \"x\" . }\n";
    let from_trig = import("data.trig", trig).await.unwrap();
    assert_eq!(
        from_trig,
        vec!["default: ex:b ex:q w", "g: ex:a ex:q v", "h: ex:c ex:q x"]
    );
    let doc = json!({"@context": ctx(), "@graph": [
        {"@id": "ex:b", "ex:q": "w"},
        {"@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]},
        {"@id": "ex:c", "@graph": "ex:h", "ex:q": "x"}
    ]});
    assert_eq!(import_jsonld(&doc).await.unwrap(), from_trig);
}

/// Files of one import are commits of their own. A named graph is
/// introduced by the first commit that writes it, and a later commit that
/// writes it again adds to the same graph.
#[tokio::test]
async fn bulk_import_across_files_shares_each_named_graph() {
    let c = ctx();
    let files = [
        (
            "1.jsonld",
            json!({"@context": c, "@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
        ),
        (
            "2.jsonld",
            json!({"@context": c, "@graph": [
                {"@id": "ex:b", "@graph": "ex:g", "ex:q": "w"},
                {"@id": "ex:c", "@graph": "ex:h", "ex:q": "x"}
            ]}),
        ),
        (
            "3.jsonld",
            json!({"@context": c, "@id": "ex:d", "ex:q": "y"}),
        ),
    ];
    let files: Vec<(&str, String)> = files
        .iter()
        .map(|(name, doc)| (*name, doc.to_string()))
        .collect();
    assert_eq!(
        import_files(&files).await.unwrap(),
        vec![
            "default: ex:d ex:q y",
            "g: ex:a ex:q v",
            "g: ex:b ex:q w",
            "h: ex:c ex:q x"
        ]
    );
}

/// What `insert` does with a document a bulk import refuses.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Insert {
    Refuses,
    Writes,
}

/// What a bulk import cannot place, it refuses rather than skips, and
/// nothing of the document is written. A transaction refuses the same
/// documents, but for the ledger's config graph, which a transaction may
/// write and a bulk import may not (as a TriG import may not).
#[tokio::test]
async fn bulk_import_refuses_what_it_cannot_place() {
    let c = ctx();
    let node = |graph: Value| json!({"@context": c, "@id": "ex:a", "@graph": graph, "ex:q": "v"});
    let cases: Vec<(&str, Value, &str, Insert)> = vec![
        (
            "the config graph",
            node(json!("config")),
            "config graph",
            Insert::Writes,
        ),
        (
            "the txn-meta graph",
            node(json!("txn-meta")),
            "reserved",
            Insert::Refuses,
        ),
        (
            "the txn-meta graph by IRI",
            node(json!(format!("urn:fluree:{IMPORT}#txn-meta"))),
            "reserved",
            Insert::Refuses,
        ),
        ("a variable", node(json!("?g")), "variable", Insert::Refuses),
        (
            "the default graph's name",
            node(json!("urn:default")),
            "default graph",
            Insert::Refuses,
        ),
        (
            "a relative name",
            node(json!("g")),
            "absolute IRI",
            Insert::Refuses,
        ),
        // A selector resolves against the document's context, not a node's.
        (
            "a prefix only a node's own context defines",
            json!({"@context": c, "@graph": [{"@context": {"y": "http://y.example/"},
                "@id": "ex:a", "@graph": "y:g", "ex:q": "v"}]}),
            "'y:g'",
            Insert::Refuses,
        ),
        (
            "a named graph named by a blank node",
            json!({"@context": c, "@id": "_:g", "@graph": [{"@id": "ex:a", "ex:q": "v"}]}),
            "named by an IRI",
            Insert::Refuses,
        ),
        (
            "a nested named graph without an @id",
            json!({"@context": c, "@graph": [
                {"ex:q": "v", "@graph": [{"@id": "ex:a", "ex:q": "v"}]}
            ]}),
            "needs an `@id`",
            Insert::Refuses,
        ),
        (
            "an unknown keyword",
            json!({"@context": c, "@id": "ex:a", "ex:q": "v", "@foo": "x"}),
            "\"@foo\"",
            Insert::Refuses,
        ),
    ];
    let mut failures = Vec::new();
    for (case, doc, needle, insert) in cases {
        let (refusal, written) = import_refused(&doc).await;
        match refusal {
            Some(e) if e.contains(needle) => {}
            other => failures.push(format!("{case}: import gave {other:?}, wanted {needle:?}")),
        }
        if !written.is_empty() {
            failures.push(format!("{case}: the refused import wrote {written:?}"));
        }
        match (insert, transact(&doc).await) {
            (Insert::Refuses, Ok(rows)) => {
                failures.push(format!("{case}: insert wrote {rows:?}"));
            }
            (Insert::Writes, Err(e)) => failures.push(format!("{case}: insert refused: {e}")),
            _ => {}
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The `f:reifies*` flakes in `graph`, across every reifier.
async fn reifies_flakes_in(fluree: &Fluree, id: &str, graph: &str) -> Vec<fluree_db_core::Flake> {
    let ledger = fluree.ledger(id).await.expect("load");
    let g_id = ledger
        .snapshot
        .graph_registry
        .graph_id_for_iri(graph)
        .expect("named graph registered");
    fluree_db_core::range_with_overlay(
        &ledger.snapshot,
        g_id,
        ledger.novelty.as_ref(),
        fluree_db_core::comparator::IndexType::Spot,
        fluree_db_core::range::RangeTest::Eq,
        fluree_db_core::range::RangeMatch::new(),
        fluree_db_core::range::RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("scan named graph")
    .into_iter()
    .filter(|f| fluree_db_core::is_reserved_reifies_predicate(&f.p))
    .collect()
}

/// An annotated edge in a named graph: its reifier bundle is written to that
/// graph and names it (`f:reifiesGraph`), as insert writes it, so deleting
/// the edge takes the bundle with it.
#[tokio::test]
async fn bulk_import_anchors_an_annotation_to_its_graph() {
    let doc = json!({"@context": ctx(), "@id": "ex:a", "@graph": "ex:g",
        "ex:knows": {"@id": "ex:b", "@annotation": {"ex:since": 2020}}});
    let graph = format!("{EX}g");
    for lane in ["insert", "import"] {
        let dir = tempfile::tempdir().unwrap();
        let id = if lane == "insert" {
            let fluree = fluree_at(dir.path());
            let ledger = fluree.create_ledger(TXN).await.unwrap();
            fluree.insert(ledger, &doc).await.expect("insert");
            TXN
        } else {
            import_into(dir.path(), &[("data.jsonld", doc.to_string())], true)
                .await
                .expect("import");
            IMPORT
        };
        let fluree = fluree_at(dir.path());
        let ledger = fluree.ledger(id).await.expect("load");
        let graph_sid = ledger.snapshot.encode_iri(&graph).expect("graph sid");
        let mut bundle = reifies_flakes_in(&fluree, id, &graph).await;
        assert!(!bundle.is_empty(), "{lane}: the bundle is in the graph");
        // Index-decoded flakes carry `g: None`; stamp the scanned graph the
        // way the cascade in `stage()` does before decoding.
        for flake in &mut bundle {
            flake.g = Some(graph_sid.clone());
        }
        let key = fluree_db_core::edge::EdgeKey::from_reifies_facts(&bundle)
            .unwrap_or_else(|e| panic!("{lane}: the bundle must decode: {e:?}; {bundle:#?}"));
        assert_eq!(key.g, Some(graph_sid), "{lane}: the bundle names its graph");

        fluree
            .graph(id)
            .transact()
            .sparql_update(&format!(
                "PREFIX ex: <{EX}>\nDELETE DATA {{ GRAPH <{graph}> {{ ex:a ex:knows ex:b }} }}"
            ))
            .commit()
            .await
            .expect("delete the edge");
        let remaining = reifies_flakes_in(&fluree, id, &graph).await;
        assert!(
            remaining.is_empty(),
            "{lane}: the bundle must not outlive its edge: {remaining:#?}"
        );
    }
}
