//! Bulk import of Turtle-star: end-to-end from a directory of `.ttl` files
//! through `ImportSink` to a queryable ledger.
//!
//! `ImportSink` opted in to reified triples when the Turtle parser gained the
//! RDF 1.2 asserting forms, but until now no test drove the whole import
//! path (splitter → parser → sink → index) with star input and read the
//! claims back through the query surfaces. This does, with the RDF 1.2
//! `VERSION` directive in the header the way a conformant producer writes it.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::Value as JsonValue;

const CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .
ex:alice ex:knows ex:carol {| ex:source ex:linkedin |} .
<< ex:bob ex:knows ex:dave >> ex:source ex:crm .
ex:carol ex:age 42 ~ ex:claim2 .
"#;

const PLAIN: &str = r#"@prefix ex: <http://example.org/> .
ex:dave ex:name "Dave" .
"#;

async fn import_dir(files: &[(&str, &str)], alias: &str) -> (fluree_db_api::Fluree, LedgerState) {
    import_dir_chunked(files, alias, 0).await
}

/// `chunk_size_mb = 0` derives the chunk size from the budget; any other value
/// forces it, which is how a fixture can be made to cross a chunk boundary.
async fn import_dir_chunked(
    files: &[(&str, &str)],
    alias: &str,
    chunk_size_mb: usize,
) -> (fluree_db_api::Fluree, LedgerState) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    for (name, content) in files {
        std::fs::write(data_dir.path().join(name), content).expect("write fixture");
    }
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(alias)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .chunk_size_mb(chunk_size_mb)
        .cleanup(false)
        .execute()
        .await
        .expect("import of Turtle-star must succeed");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    // Keep the temp dirs alive for the ledger's lifetime.
    std::mem::forget(db_dir);
    std::mem::forget(data_dir);
    (fluree, ledger)
}

fn rows(result: &JsonValue) -> Vec<Vec<String>> {
    result
        .as_array()
        .expect("row array")
        .iter()
        .map(|row| {
            row.as_array()
                .expect("row")
                .iter()
                .map(|cell| match cell {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    other => other
                        .get("@id")
                        .or_else(|| other.get("@value"))
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_else(|| other.to_string()),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn imported_turtle_star_claims_are_queryable() {
    let (fluree, ledger) = import_dir(
        &[("claims.ttl", CLAIMS), ("plain.ttl", PLAIN)],
        "it/import-turtle-star:claims",
    )
    .await;

    // Claim-first: every reifier and the edge it reifies.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?s ?p ?o ?src WHERE {\n\
                    ?r rdf:reifies <<( ?s ?p ?o )>> .\n\
                    OPTIONAL { ?r ex:source ?src }\n\
                  } ORDER BY ?s ?p ?o";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("claim-first query over an imported ledger");
    let got = rows(&result);
    assert_eq!(got.len(), 4, "{got:#?}");
    let edges: Vec<(String, String)> = got.iter().map(|r| (r[0].clone(), r[2].clone())).collect();
    assert!(
        edges
            .iter()
            .any(|(s, o)| s.ends_with("alice") && o.ends_with("bob")),
        "{edges:?}"
    );
    assert!(
        edges
            .iter()
            .any(|(s, o)| s.ends_with("bob") && o.ends_with("dave")),
        "`<< s p o >>` in subject position reifies the (asserted) base edge: {edges:?}"
    );
    assert!(
        edges.iter().any(|(s, o)| s.ends_with("carol") && o == "42"),
        "literal-object edge: {edges:?}"
    );

    // Inline: the named claim's body.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?conf WHERE { ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence ?conf |} }";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("inline annotation query");
    assert_eq!(rows(&result), vec![vec!["0.9".to_string()]]);

    // The base edges are ordinary data: one row per edge, never duplicated
    // by their claims.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?o WHERE { ex:alice ex:knows ?o } ORDER BY ?o";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("plain edge query");
    assert_eq!(rows(&result).len(), 2, "{result:#}");
}

/// A multi-chunk import: a fixture large enough to be cut up, with star
/// statements throughout and an escape-bearing prefix IRI in the header.
///
/// The other fixtures here are a few hundred bytes, so the import runs as a
/// single work item and nothing about chunking is exercised at all. This one
/// forces a 1 MB chunk and writes ~3 MB.
///
/// It does NOT reach `fluree-graph-turtle`'s splitter. Nothing outside that
/// crate calls `extract_prefix_block`, `compute_chunk_boundaries` or
/// `StreamingTurtleReader` — the importer chunks by its own route — so the
/// prefix-block extractor's escaped-IRI and `VERSION` handling is pinned
/// where it actually runs, by
/// `splitter::tests::escaped_iri_in_a_prefix_directive_does_not_swallow_the_next_statement`,
/// which drives `extract_prefix_block` and `StreamingTurtleReader` directly.
/// What this test covers is the importer's own chunking: every edge and every
/// claim crossing those boundaries exactly once, neither dropped nor
/// duplicated.
#[tokio::test]
async fn a_split_file_keeps_every_claim_exactly_once() {
    const N: usize = 40_000;
    // The prefix IRI carries a `\u` escape on purpose: that lexes as its own
    // token kind, which the extractor's SPARQL-directive terminator arm has to
    // recognise as the directive's operand. While it did not, the prefix block
    // ran on to the next `.` and swallowed the first data statement.
    let mut doc = String::from("VERSION \"1.2\"\n@prefix ex: <http://example.org/caf\\u00E9/> .\n");
    for i in 0..N {
        doc.push_str(&format!(
            "ex:a{i} ex:knows ex:b{i} ~ ex:claim{i} {{| ex:confidence 0.9 |}} .\n"
        ));
    }
    assert!(
        doc.len() > 2 * 1024 * 1024,
        "the fixture must exceed one chunk: {} bytes",
        doc.len()
    );

    let (fluree, ledger) =
        import_dir_chunked(&[("big.ttl", &doc)], "it/import-star-split", 1).await;

    let sparql = "PREFIX ex: <http://example.org/caf\u{e9}/>\n\
                  SELECT (COUNT(*) AS ?n) WHERE { ?s ex:knows ?o }";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("edge count");
    assert_eq!(
        rows(&result)[0][0],
        N.to_string(),
        "every edge must land exactly once across the split: {result:#}"
    );

    // And the claims came with them, on both sides of a boundary.
    let sparql = "PREFIX ex: <http://example.org/caf\u{e9}/>\n\
                  SELECT (COUNT(*) AS ?n) WHERE { ?s ex:knows ?o {| ex:confidence ?c |} }";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("claim count");
    assert_eq!(
        rows(&result)[0][0],
        N.to_string(),
        "every claim must survive the split: {result:#}"
    );
}
