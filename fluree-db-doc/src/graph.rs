//! Assembling what gets transacted, and the update that clears a previous
//! extraction of the same document.

use crate::chunk::Chunk;
use crate::vocab;
use crate::{DocError, Result};
use serde_json::{json, Value};

/// Facts about the source file, recorded on the document node so a ledger
/// can answer "what was ingested, when, from what, with which parser".
#[derive(Debug, Clone)]
pub struct DocumentMeta {
    pub doc_iri: String,
    pub file_name: String,
    pub relative_path: String,
    pub sha256: String,
    pub media_type: String,
    pub byte_size: u64,
    pub pages: usize,
    pub escalated_crops: usize,
    pub parser_revision: String,
    /// RFC 3339 timestamp of this ingest.
    pub ingested_at: String,
    /// `min/max` characters the chunker was run with.
    pub chunking: String,
}

/// What extraction did to a document, for its node.
#[derive(Debug, Clone, Default)]
pub struct ExtractionStamp {
    pub fingerprint: String,
    pub model: Option<String>,
    pub mentions: usize,
    pub entities: usize,
    pub relations: usize,
}

pub fn chunk_iri(doc_iri: &str, index: usize) -> String {
    format!("{doc_iri}/chunk/{index}")
}

/// The `doc:SourceDocument` node: file facts plus what the run produced.
pub fn document_node(
    meta: &DocumentMeta,
    chunk_count: usize,
    embedding: Option<(&str, usize)>,
    extraction: Option<&ExtractionStamp>,
) -> Value {
    let mut node = json!({
        "@id": meta.doc_iri,
        "@type": vocab::SOURCE_DOCUMENT_CLASS,
        vocab::FILE_NAME: meta.file_name,
        vocab::RELATIVE_PATH: meta.relative_path,
        vocab::SHA256: meta.sha256,
        vocab::MEDIA_TYPE: meta.media_type,
        vocab::BYTE_SIZE: meta.byte_size,
        vocab::PAGE_COUNT: meta.pages,
        vocab::ESCALATED_CROPS: meta.escalated_crops,
        vocab::PARSER_REVISION: meta.parser_revision,
        vocab::CHUNK_COUNT: chunk_count,
        vocab::CHUNKING: meta.chunking,
        vocab::INGESTED_AT: { "@value": meta.ingested_at, "@type": "xsd:dateTime" },
    });
    if let Some((model, dims)) = embedding {
        node[vocab::EMBEDDING_MODEL] = json!(model);
        node[vocab::EMBEDDING_DIMENSIONS] = json!(dims);
    }
    if let Some(x) = extraction {
        node[vocab::EXTRACTION_FINGERPRINT] = json!(x.fingerprint);
        if let Some(model) = &x.model {
            node[vocab::EXTRACTION_MODEL] = json!(model);
        }
        node[vocab::MENTION_COUNT] = json!(x.mentions);
        node[vocab::ENTITY_COUNT] = json!(x.entities);
        node[vocab::RELATION_COUNT] = json!(x.relations);
    }
    node
}

/// One `doc:Chunk` node per chunk, embedding attached when present.
pub fn chunk_nodes(doc_iri: &str, chunks: &[Chunk], embeddings: Option<&[Vec<f32>]>) -> Vec<Value> {
    chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let mut node = json!({
                "@id": chunk_iri(doc_iri, i),
                "@type": vocab::CHUNK,
                vocab::CHUNK_INDEX: i,
                vocab::TEXT: chunk.text,
                vocab::SOURCE_DOCUMENT: doc_iri,
            });
            if !chunk.header_path.is_empty() {
                node[vocab::HEADER_PATH] = json!(chunk.header_path_string());
            }
            if !chunk.source_ids.is_empty() {
                node[vocab::SOURCE_ELEMENT] = json!(chunk.source_ids);
            }
            if let Some(vec) = embeddings.and_then(|e| e.get(i)) {
                node[vocab::EMBEDDING] = json!({ "@value": vec, "@type": "@vector" });
            }
            node
        })
        .collect()
}

/// The structure graph, the document node, the chunks and whatever
/// extraction added, as one JSON-LD transaction under the shared context.
pub fn transaction(
    doco_json: &str,
    document: Value,
    chunks: Vec<Value>,
    extra: Vec<Value>,
) -> Result<Value> {
    let doco: Value = serde_json::from_str(doco_json)
        .map_err(|e| DocError::Parse(format!("doco graph is not JSON: {e}")))?;
    let mut graph = match doco.get("@graph") {
        Some(Value::Array(nodes)) => nodes.clone(),
        _ => return Err(DocError::Parse("doco graph: missing @graph array".into())),
    };
    graph.push(document);
    graph.extend(chunks);
    graph.extend(extra);
    Ok(json!({
        "@context": vocab::context(),
        "@graph": graph,
    }))
}

/// The namespaces every emitter in this pipeline writes into. A predicate
/// here, on a node this pipeline stamped, was written by this pipeline:
/// the `doc:` terms in [`vocab`], the NIF offsets on mentions and structure
/// elements, and the `po:contains` spine of the structure graph.
///
/// Scoped by namespace rather than term by term because the structure graph
/// is emitted by `fluree-doc-model`, a pinned git dependency — a term list
/// would go stale on a rev bump, silently, and leak the new term forever.
const OWNED_NAMESPACES: [&str; 3] = [vocab::DOC_NS, vocab::NIF_NS, vocab::PO_NS];

/// The predicates this pipeline writes from namespaces it does not own, so
/// they have to be named one at a time: `rdf:type` on every node it mints,
/// the reification triple on a relation node, and the label both the
/// relation emitter and the structure emitter write.
///
/// Anything outside [`OWNED_NAMESPACES`] and this list is somebody else's
/// and survives re-ingest. Kept honest by
/// `every_emitted_predicate_is_owned`.
fn owned_terms() -> [String; 5] {
    [
        format!("{}type", vocab::RDF_NS),
        format!("{}subject", vocab::RDF_NS),
        format!("{}predicate", vocab::RDF_NS),
        format!("{}object", vocab::RDF_NS),
        format!("{}label", vocab::RDFS_NS),
    ]
}

/// Whether re-ingest may retract `predicate` from a node it stamped, given
/// either as a full IRI or in the compact form the emitted JSON-LD uses.
/// `@type` is accepted as the JSON-LD spelling of `rdf:type`.
pub fn is_owned_predicate(predicate: &str) -> bool {
    let full = expand_owned(predicate);
    OWNED_NAMESPACES.iter().any(|ns| full.starts_with(ns)) || owned_terms().contains(&full)
}

/// The prefixes [`vocab::context`] binds, for reading a predicate written
/// in compact form back as an IRI.
fn expand_owned(predicate: &str) -> String {
    if predicate == "@type" {
        return format!("{}type", vocab::RDF_NS);
    }
    for (prefix, ns) in [
        ("doc:", vocab::DOC_NS),
        ("doco:", vocab::DOCO_NS),
        ("nif:", vocab::NIF_NS),
        ("po:", vocab::PO_NS),
        ("rdf:", vocab::RDF_NS),
        ("rdfs:", vocab::RDFS_NS),
        ("skos:", vocab::SKOS_NS),
        ("schema:", vocab::SCHEMA_NS),
        ("f:", vocab::FLUREE_NS),
        ("xsd:", vocab::XSD_NS),
    ] {
        if let Some(rest) = predicate.strip_prefix(prefix) {
            return format!("{ns}{rest}");
        }
    }
    predicate.to_string()
}

/// SPARQL UPDATE that retracts everything a previous extraction of this
/// document wrote: every element and chunk stamped with it, and the
/// document node itself. Re-extraction is retract-then-insert, never a
/// diff; the earlier extraction stays queryable at its commit.
///
/// The sweep is scoped to the predicates this pipeline emits
/// ([`is_owned_predicate`]). It used to be `DELETE { ?s ?p ?o }` with no
/// bound on `?p`, which took anything else on those nodes with it — a
/// reviewer's note on a relation, a trust weight on the document — with no
/// warning and a zero exit. Source trust belongs on the document node, so
/// that node has to be curatable and stay curated across a re-ingest.
pub fn retract_update(doc_iri: &str) -> String {
    let mut clauses: Vec<String> = OWNED_NAMESPACES
        .iter()
        .map(|ns| format!("STRSTARTS(STR(?p), \"{ns}\")"))
        .collect();
    let terms = owned_terms()
        .iter()
        .map(|t| format!("<{t}>"))
        .collect::<Vec<_>>()
        .join(", ");
    clauses.push(format!("?p IN ({terms})"));
    format!(
        "PREFIX doc: <{ns}>\n\
         DELETE {{ ?s ?p ?o }}\n\
         WHERE {{\n  \
           {{\n    \
             {{ ?s doc:sourceDocument <{iri}> . ?s ?p ?o }}\n    \
             UNION\n    \
             {{ VALUES ?s {{ <{iri}> }} ?s ?p ?o }}\n  \
           }}\n  \
           FILTER ( {filter} )\n\
         }}",
        ns = vocab::DOC_NS,
        iri = doc_iri,
        filter = clauses.join(" || ")
    )
}

/// What an earlier ingest of this document recorded — content hash, parser
/// revision, embedding model, extraction fingerprint and chunking — so an
/// unchanged document can be skipped. Rows are `[sha256, parserRevision,
/// embeddingModel | null, extractionFingerprint | null, chunking | null]`.
pub fn exists_query(doc_iri: &str) -> Value {
    json!({
        "@context": { "doc": vocab::DOC_NS },
        "where": [
            { "@id": doc_iri, vocab::SHA256: "?sha", vocab::PARSER_REVISION: "?rev" },
            ["optional", { "@id": doc_iri, vocab::EMBEDDING_MODEL: "?model" }],
            ["optional", { "@id": doc_iri, vocab::EXTRACTION_FINGERPRINT: "?extraction" }],
            ["optional", { "@id": doc_iri, vocab::CHUNKING: "?chunking" }]
        ],
        "select": ["?sha", "?rev", "?model", "?extraction", "?chunking"],
        "limit": 1
    })
}

/// The edges a previous extraction of this document wrote directly: rows
/// `[subject, predicate, object]` of its asserted relations. After the
/// retraction, each is kept only while some other relation still supports
/// it — see [`relation_support_query`] and [`delete_triple_update`].
pub fn asserted_triples_query(doc_iri: &str) -> Value {
    json!({
        "@context": { "doc": vocab::DOC_NS, "rdf": vocab::RDF_NS, "doc:sourceDocument": { "@type": "@id" } },
        "where": [{
            "@id": "?r",
            "@type": vocab::RELATION,
            vocab::SOURCE_DOCUMENT: doc_iri,
            vocab::ASSERTED: true,
            "rdf:subject": "?s",
            "rdf:predicate": "?p",
            "rdf:object": "?o"
        }],
        "select": ["?s", "?p", "?o"]
    })
}

/// Any remaining asserted relation stating this exact edge.
pub fn relation_support_query(subject: &str, predicate: &str, object: &str) -> Value {
    json!({
        "@context": {
            "doc": vocab::DOC_NS,
            "rdf": vocab::RDF_NS,
            "rdf:subject": { "@type": "@id" },
            "rdf:predicate": { "@type": "@id" },
            "rdf:object": { "@type": "@id" }
        },
        "where": [{
            "@id": "?r",
            "@type": vocab::RELATION,
            vocab::ASSERTED: true,
            "rdf:subject": subject,
            "rdf:predicate": predicate,
            "rdf:object": object
        }],
        "select": ["?r"],
        "limit": 1
    })
}

/// Whether an IRI can be written inside `<…>` in a SPARQL update without
/// escaping — the RFC 3987 IRIREF exclusions.
///
/// Minted IRIs are hex and document IRIs are percent-encoded, but gazetteer
/// and model IRIs come from operator-supplied files (`--entities`,
/// `--model`). One carrying `>` would close the brackets and splice extra
/// triples into a `DELETE DATA` on the target ledger.
pub fn sparql_iri_safe(iri: &str) -> bool {
    !iri.is_empty()
        && !iri.chars().any(|c| {
            c <= '\u{20}' || matches!(c, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\')
        })
}

/// One update retracting every edge given, all full IRIs.
///
/// A triple naming an unsafe IRI is skipped rather than escaped: the IRI is
/// already stored that way, so rewriting it would delete nothing and hide
/// that it happened. `None` when nothing is left to retract. Callers should
/// filter with [`sparql_iri_safe`] first so they can report what they
/// skipped; this is the backstop.
pub fn delete_triples_update(triples: &[(String, String, String)]) -> Option<String> {
    let body: Vec<String> = triples
        .iter()
        .filter(|(s, p, o)| sparql_iri_safe(s) && sparql_iri_safe(p) && sparql_iri_safe(o))
        .map(|(s, p, o)| format!("<{s}> <{p}> <{o}> ."))
        .collect();
    (!body.is_empty()).then(|| format!("DELETE DATA {{ {} }}", body.join(" ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retract_update_is_bounded_to_the_predicates_this_pipeline_writes() {
        let update = retract_update("urn:fluree:doc:memo.md");
        // Both branches of the sweep survive: stamped nodes and the
        // document node itself.
        assert!(update.contains("?s doc:sourceDocument <urn:fluree:doc:memo.md>"));
        assert!(update.contains("VALUES ?s { <urn:fluree:doc:memo.md> }"));
        // ?p is no longer free.
        for ns in OWNED_NAMESPACES {
            assert!(
                update.contains(&format!("STRSTARTS(STR(?p), \"{ns}\")")),
                "namespace {ns} is not in the filter: {update}"
            );
        }
        for term in owned_terms() {
            assert!(
                update.contains(&format!("<{term}>")),
                "term {term} is not in the filter: {update}"
            );
        }
        assert!(
            update.contains("FILTER ("),
            "an unfiltered ?p takes foreign triples with it: {update}"
        );
    }

    #[test]
    fn ownership_covers_the_emitters_and_stops_at_a_foreign_namespace() {
        for owned in [
            "doc:text",
            "doc:sourceDocument",
            "doc:verdict",
            // Emitted by the pinned `fluree-doc-model` structure graph, and
            // never named in this crate.
            "doc:bbox",
            "doc:xhtmlTag",
            "https://ns.flur.ee/doc#somethingAddedLater",
            "nif:beginIndex",
            "po:contains",
            "rdf:subject",
            "rdfs:label",
            "@type",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#object",
        ] {
            assert!(is_owned_predicate(owned), "{owned} should be swept");
        }
        for foreign in [
            // The curation points the design puts weight on.
            "https://example.org/trust",
            "ex:reviewedConfidence",
            "schema:name",
            "skos:altLabel",
            "rdfs:comment",
            "rdf:value",
        ] {
            assert!(!is_owned_predicate(foreign), "{foreign} should survive");
        }
    }

    /// The sweep list goes stale the moment an emitter writes a predicate
    /// outside it, and a stale list leaks silently — the retracted document
    /// keeps a triple forever and nothing says so. So: run the real
    /// emitters over a document exercising every structural shape, and
    /// assert the contract holds for every predicate they produced.
    #[test]
    fn every_emitted_predicate_is_owned() {
        use crate::chunk::{chunk_doco, ChunkConfig};
        use crate::extract::{
            resolve, ChunkExtraction, ChunkInput, LlmEntity, LlmRelation, ResolvePolicy,
        };
        use crate::parse::{parse_bytes, ParseOptions, SourceKind};

        let doc_iri = "urn:fluree:doc:guard.md";
        let source = "\
# Heading

Jane Doe joined Acme, with a [link](https://example.org/x).

- first item
- second item

| Name | Role |
| --- | --- |
| Jane Doe | CTO |

## Subheading

Closing paragraph.
";
        let parsed = parse_bytes(
            source.as_bytes().to_vec(),
            SourceKind::Markdown,
            "guard",
            &ParseOptions {
                base_iri: doc_iri.into(),
                doc_iri: doc_iri.into(),
                vlm: None,
            },
        )
        .expect("markdown parses");
        let chunks = chunk_doco(&parsed.doco, &ChunkConfig::default()).expect("chunks");
        assert!(!chunks.is_empty(), "the fixture must produce a chunk");

        let extraction = ChunkExtraction {
            entities: vec![LlmEntity {
                name: Some("Jane Doe".into()),
                class: Some("schema:Person".into()),
                ner_label: Some("PERSON".into()),
                context: Some("Jane Doe joined Acme".into()),
                ..Default::default()
            }],
            relations: vec![LlmRelation {
                subject_name: Some("Jane Doe".into()),
                predicate: Some("schema:worksFor".into()),
                predicate_label: Some("works for".into()),
                object_name: Some("Acme".into()),
                object_is_literal: true,
                context: Some("Jane Doe joined Acme".into()),
                assertion_mode: None,
            }],
            from_cache: false,
            ..Default::default()
        };
        let inputs: Vec<ChunkInput<'_>> = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| ChunkInput {
                chunk,
                chunk_iri: chunk_iri(doc_iri, i),
                mentions: &[],
                extraction: Some(&extraction),
            })
            .collect();
        let extracted = resolve(
            doc_iri,
            "urn:fluree:doc:",
            &inputs,
            None,
            None,
            ResolvePolicy::default(),
        );

        let meta = DocumentMeta {
            doc_iri: doc_iri.into(),
            file_name: "guard.md".into(),
            relative_path: "guard.md".into(),
            sha256: "0".repeat(64),
            media_type: "text/markdown".into(),
            byte_size: source.len() as u64,
            pages: 1,
            escalated_crops: 0,
            parser_revision: "rev".into(),
            ingested_at: "2026-09-16T00:00:00Z".into(),
            chunking: "200/800".into(),
        };
        // Every optional stamp on, so no predicate is missed by omission.
        let document = document_node(
            &meta,
            chunks.len(),
            Some(("embed-model", 3)),
            Some(&ExtractionStamp {
                fingerprint: "fp".into(),
                model: Some("m".into()),
                mentions: 1,
                entities: 1,
                relations: 1,
            }),
        );
        let embeddings: Vec<Vec<f32>> = chunks.iter().map(|_| vec![0.0, 1.0, 0.0]).collect();
        let tx = transaction(
            &parsed.doco,
            document,
            chunk_nodes(doc_iri, &chunks, Some(&embeddings)),
            extracted.nodes.clone(),
        )
        .expect("transaction");

        // The sweep reaches the document node and every node stamped with
        // it; nothing else. Check exactly those.
        let mut seen: std::collections::BTreeSet<String> = Default::default();
        let mut leaked: Vec<String> = Vec::new();
        for node in tx["@graph"].as_array().expect("@graph") {
            let map = node.as_object().expect("node");
            let is_document = map.get("@id").and_then(Value::as_str) == Some(doc_iri);
            let is_stamped = map.contains_key(vocab::SOURCE_DOCUMENT);
            if !is_document && !is_stamped {
                continue;
            }
            for key in map.keys() {
                if key == "@id" {
                    continue;
                }
                seen.insert(key.clone());
                if !is_owned_predicate(key) {
                    leaked.push(key.clone());
                }
            }
        }
        assert!(
            leaked.is_empty(),
            "re-ingest would leave these behind forever — add the namespace \
             or the term to the sweep in graph.rs: {leaked:?}"
        );
        // Non-vacuity: a fixture that emitted almost nothing would pass the
        // assertion above while proving nothing. Pin the shapes it must
        // have reached, one per emitter.
        for required in [
            "@type",
            "doc:text",          // chunk nodes
            "doc:sourceElement", // chunk → structure graph
            "doc:sha256",        // the document node
            "doc:embeddingModel",
            "doc:extractionFingerprint",
            "doc:verdict",  // relation nodes
            "rdf:subject",  // the reification triple
            "nif:isString", // structure elements
            "po:contains",  // the structure spine
            "rdfs:label",
            "doc:xhtmlTag",
        ] {
            assert!(
                seen.contains(required),
                "the guard fixture stopped emitting {required}; it no longer \
                 proves the sweep covers that emitter. saw: {seen:?}"
            );
        }
    }

    #[test]
    fn chunk_nodes_carry_vectors_and_sources() {
        let chunks = vec![Chunk {
            header_path: vec!["A".into(), "B".into()],
            text: "body".into(),
            source_ids: vec!["urn:x/element/1".into()],
            spans: Vec::new(),
        }];
        let emb = vec![vec![0.5f32, 0.25]];
        let nodes = chunk_nodes("urn:x", &chunks, Some(&emb));
        assert_eq!(nodes[0]["@id"], "urn:x/chunk/0");
        assert_eq!(nodes[0]["doc:headerPath"], "A / B");
        assert_eq!(nodes[0]["doc:embedding"]["@type"], "@vector");
        assert_eq!(nodes[0]["doc:sourceElement"][0], "urn:x/element/1");
    }

    #[test]
    fn transaction_merges_graphs() {
        let doco = r#"{"@context":{},"@graph":[{"@id":"e0","@type":"doco:Document"}]}"#;
        let tx = transaction(
            doco,
            json!({"@id":"d"}),
            vec![json!({"@id":"c0"})],
            vec![json!({"@id":"m0"})],
        )
        .unwrap();
        let ids: Vec<&str> = tx["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["@id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["e0", "d", "c0", "m0"]);
        assert_eq!(tx["@context"]["doc"], vocab::DOC_NS);
    }

    #[test]
    fn delete_triples_update_refuses_an_iri_that_escapes_its_brackets() {
        let safe = (
            "urn:fluree:doc:a".to_string(),
            "https://ex.org/knows".to_string(),
            "urn:fluree:doc:b".to_string(),
        );
        let update = delete_triples_update(std::slice::from_ref(&safe)).expect("one safe triple");
        assert!(update.contains("<urn:fluree:doc:a> <https://ex.org/knows> <urn:fluree:doc:b> ."));

        // An operator-supplied gazetteer IRI closing the bracket would
        // otherwise splice a second triple into the DELETE DATA.
        let injected = (
            "https://ex.org/x> <https://ex.org/p> <https://ex.org/o> . <urn:victim".to_string(),
            "https://ex.org/knows".to_string(),
            "urn:fluree:doc:b".to_string(),
        );
        assert!(!sparql_iri_safe(&injected.0));
        assert_eq!(delete_triples_update(std::slice::from_ref(&injected)), None);

        // A safe triple alongside an unsafe one still retracts.
        let update = delete_triples_update(&[injected, safe]).expect("the safe triple survives");
        assert!(!update.contains("urn:victim"));
        assert_eq!(
            update.matches(" .").count(),
            1,
            "exactly one triple: {update}"
        );
    }

    #[test]
    fn sparql_iri_safe_rejects_the_iriref_exclusions() {
        assert!(sparql_iri_safe("https://ex.org/a_b-c~1"));
        assert!(sparql_iri_safe("urn:fluree:doc:folder/file.pdf/chunk/0"));
        for bad in [
            "", "a b", "a>b", "a<b", "a\"b", "a{b", "a}b", "a|b", "a^b", "a`b", "a\\b", "a\nb",
        ] {
            assert!(!sparql_iri_safe(bad), "should reject {bad:?}");
        }
    }
}
