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

/// `{document}-chunk-{n}`. Like the engine's `{document}-element-{n}`, it
/// adds no `/`, `#` or `:` to the document IRI: Fluree encodes an IRI's
/// namespace (everything up to the last of those) once per distinct value,
/// so a delimiter here would cost a namespace per document.
pub fn chunk_iri(doc_iri: &str, index: usize) -> String {
    format!("{doc_iri}-chunk-{index}")
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
/// extraction added, as one JSON-LD transaction.
///
/// The context is the engine's own with this pipeline's merged over it.
/// The engine's carries the prefixes and `@id` coercions its graph is
/// written in; replacing it would store a coerced reference as a string,
/// and a prefix this crate does not know as an IRI of that scheme.
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
    let mut context = match doco.get("@context") {
        Some(Value::Object(engine)) => engine.clone(),
        _ => serde_json::Map::new(),
    };
    if let Value::Object(ours) = vocab::context() {
        context.extend(ours);
    }
    Ok(json!({
        "@context": context,
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
/// the reification triple on a relation node, the label both the relation
/// emitter and the structure emitter write, and the Dublin Core terms the
/// structure emitter puts on its root from what the file declares.
///
/// Anything outside [`OWNED_NAMESPACES`] and this list is somebody else's
/// and survives re-ingest. Kept honest by
/// `every_emitted_predicate_is_owned`.
fn owned_terms() -> [String; 9] {
    [
        format!("{}type", vocab::RDF_NS),
        format!("{}subject", vocab::RDF_NS),
        format!("{}predicate", vocab::RDF_NS),
        format!("{}object", vocab::RDF_NS),
        format!("{}label", vocab::RDFS_NS),
        format!("{}title", vocab::DCTERMS_NS),
        format!("{}creator", vocab::DCTERMS_NS),
        format!("{}created", vocab::DCTERMS_NS),
        format!("{}modified", vocab::DCTERMS_NS),
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
        ("dcterms:", vocab::DCTERMS_NS),
        ("foaf:", vocab::FOAF_NS),
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
/// document wrote: every node stamped with it, the table cells those hold,
/// and the document node itself. Re-extraction is retract-then-insert,
/// never a diff; the earlier extraction stays queryable at its commit.
///
/// Cells are the one node the engine does not stamp: a cell is always one
/// `po:contains` below its stamped table, and cells are most of a
/// table-heavy graph. The hop is bounded to `doc:TableCell` so a node
/// someone else linked into the structure is not taken with it.
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
         PREFIX po: <{po}>\n\
         DELETE {{ ?s ?p ?o }}\n\
         WHERE {{\n  \
           {{\n    \
             {{ ?s doc:sourceDocument <{iri}> . ?s ?p ?o }}\n    \
             UNION\n    \
             {{ ?x doc:sourceDocument <{iri}> . ?x po:contains ?s . ?s a doc:TableCell . ?s ?p ?o }}\n    \
             UNION\n    \
             {{ VALUES ?s {{ <{iri}> }} ?s ?p ?o }}\n  \
           }}\n  \
           FILTER ( {filter} )\n\
         }}",
        ns = vocab::DOC_NS,
        po = vocab::PO_NS,
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
/// `--ontology`). One carrying `>` would close the brackets and splice extra
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
        // All three branches of the sweep survive: stamped nodes, the
        // unstamped cells they hold, and the document node itself.
        assert!(update.contains("?s doc:sourceDocument <urn:fluree:doc:memo.md>"));
        assert!(update.contains(
            "?x doc:sourceDocument <urn:fluree:doc:memo.md> . ?x po:contains ?s . ?s a doc:TableCell"
        ));
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
            // What a file declares about itself, on the structure root.
            "dcterms:title",
            "http://purl.org/dc/terms/created",
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
            // Dublin Core is owned term by term, not as a namespace.
            "dcterms:subject",
            "foaf:name",
        ] {
            assert!(!is_owned_predicate(foreign), "{foreign} should survive");
        }
    }

    /// The sweep list goes stale the moment an emitter writes a predicate
    /// outside it, and a stale list leaks silently — the retracted document
    /// keeps a triple forever and nothing says so. So: run the real
    /// emitters over documents exercising every structural shape, one per
    /// reader that writes terms of its own, and assert the contract holds
    /// for every predicate they produced.
    #[test]
    fn every_emitted_predicate_is_owned() {
        use crate::parse::SourceKind;

        // HTML rather than Markdown: it also declares a title and an author,
        // which the engine writes on its root in Dublin Core.
        let html = "\
<html><head><title>Guard</title><meta name=\"author\" content=\"Jane Doe\"></head><body>
<h1>Heading</h1>
<p>Jane Doe joined Acme, with a <a href=\"https://example.org/x\">link</a>.</p>
<ul><li>first item</li><li>second item</li></ul>
<table><tr><th>Name</th><th>Role</th></tr><tr><td>Jane Doe</td><td>CTO</td></tr></table>
<h2>Subheading</h2>
<p>Closing paragraph.</p>
</body></html>
";
        let eml = "From: Jane Doe <jane@example.org>\r\n\
                   To: Bo <bo@example.org>\r\n\
                   Cc: Ana <ana@example.org>\r\n\
                   Subject: Q3\r\n\
                   Date: Tue, 7 Oct 2026 10:00:00 +0000\r\n\
                   Message-ID: <q3@example.org>\r\n\r\n\
                   Jane Doe joined Acme.\r\n\r\n--\r\nJane\r\n";
        let vtt = "WEBVTT\n\n\
                   00:00:01.000 --> 00:00:04.000\n<v Jane Doe>Jane Doe joined Acme.\n\n\
                   00:00:04.000 --> 00:00:06.000\n<v Bo>Welcome.\n";
        // A PDF root also carries the creation and modification dates.
        let pdf: &[u8] = include_bytes!("../testdata/probe-booktabs.pdf");

        let mut seen = std::collections::BTreeSet::new();
        for (file_name, kind, source) in [
            ("guard.html", SourceKind::Html, html.as_bytes()),
            ("guard.eml", SourceKind::Eml, eml.as_bytes()),
            ("guard.vtt", SourceKind::WebVtt, vtt.as_bytes()),
            ("guard.pdf", SourceKind::Pdf, pdf),
        ] {
            seen.extend(swept_predicates(file_name, kind, source));
        }

        // Non-vacuity: a fixture that emitted almost nothing would pass the
        // per-document assertions while proving nothing. Pin the shapes they
        // must have reached, one per emitter.
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
            "po:containsAsHeader",
            "rdfs:label",
            "doc:xhtmlTag",
            "doc:cellValue", // table cells, reached through their table
            "dcterms:title", // what the file declares, on the root
            "dcterms:creator",
            "dcterms:created",
            "dcterms:modified",
            "doc:sourceName",
            "doc:from", // email: messages and mailboxes
            "doc:to",
            "doc:address",
            "doc:sentAt",
            "doc:speaker", // transcript turns
            "doc:startMs",
            "doc:pageIndex", // PDF geometry
            "doc:bbox",
        ] {
            assert!(
                seen.contains(required),
                "the guard fixtures stopped emitting {required}; they no longer \
                 prove the sweep covers that emitter. saw: {seen:?}"
            );
        }
    }

    /// Every predicate on a node re-ingest sweeps, for one document run
    /// through the parser, chunker and extraction into its transaction —
    /// after asserting the sweep reaches every structure node and owns every
    /// predicate on them.
    fn swept_predicates(
        file_name: &str,
        kind: crate::parse::SourceKind,
        source: &[u8],
    ) -> std::collections::BTreeSet<String> {
        use crate::chunk::{chunk_doco, ChunkConfig};
        use crate::extract::{
            resolve, ChunkExtraction, ChunkInput, LlmEntity, LlmRelation, ResolvePolicy,
        };
        use crate::parse::{parse_bytes, ParseOptions};

        let doc_iri = format!("urn:fluree:doc:{file_name}");
        let doc_iri = doc_iri.as_str();
        let parsed = parse_bytes(
            source.to_vec(),
            kind,
            "guard",
            &ParseOptions {
                base_iri: doc_iri.into(),
                doc_iri: doc_iri.into(),
                source_name: Some(file_name.into()),
                vlm: None,
            },
        )
        .unwrap_or_else(|e| panic!("{file_name}: {e}"));
        let chunks = chunk_doco(&parsed.doco, &ChunkConfig::default()).expect("chunks");
        assert!(
            !chunks.is_empty(),
            "{file_name}: the fixture must produce a chunk"
        );

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
            file_name: file_name.into(),
            relative_path: file_name.into(),
            sha256: "0".repeat(64),
            media_type: kind.media_type().into(),
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

        // The sweep reaches the document node, every node stamped with it,
        // and the table cells those contain; nothing else. Check exactly
        // those.
        let nodes = tx["@graph"].as_array().expect("@graph");
        let id_of = |n: &Value| n["@id"].as_str().map(str::to_string);
        let stamped: std::collections::BTreeSet<String> = nodes
            .iter()
            .filter(|n| n.get(vocab::SOURCE_DOCUMENT).is_some() || n["@id"] == doc_iri)
            .filter_map(id_of)
            .collect();
        let cells: std::collections::BTreeSet<String> = nodes
            .iter()
            .filter(|n| stamped.contains(&id_of(n).unwrap_or_default()))
            .filter_map(|n| n.get("po:contains").and_then(Value::as_array))
            .flatten()
            .filter_map(Value::as_str)
            .filter(|child| {
                nodes
                    .iter()
                    .any(|n| n["@id"] == *child && n["@type"] == "doc:TableCell")
            })
            .map(str::to_string)
            .collect();

        // Every node of the structure graph is one of those: a node the
        // sweep cannot reach outlives every re-ingest of its document.
        let structure: Value = serde_json::from_str(&parsed.doco).expect("doco is JSON");
        let unreached: Vec<String> = structure["@graph"]
            .as_array()
            .expect("@graph")
            .iter()
            .filter_map(id_of)
            .filter(|id| !stamped.contains(id) && !cells.contains(id))
            .collect();
        assert!(
            unreached.is_empty(),
            "{file_name}: re-ingest would leave these structure nodes behind — \
             stamp them or reach them in retract_update: {unreached:?}"
        );

        let mut seen: std::collections::BTreeSet<String> = Default::default();
        let mut leaked: Vec<String> = Vec::new();
        for node in nodes {
            let map = node.as_object().expect("node");
            let id = map.get("@id").and_then(Value::as_str).unwrap_or_default();
            if !stamped.contains(id) && !cells.contains(id) {
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
            "{file_name}: re-ingest would leave these behind forever — add the \
             namespace or the term to the sweep in graph.rs: {leaked:?}"
        );
        seen
    }

    #[test]
    fn chunk_nodes_carry_vectors_and_sources() {
        let chunks = vec![Chunk {
            header_path: vec!["A".into(), "B".into()],
            text: "body".into(),
            source_ids: vec!["urn:x-element-1".into()],
            spans: Vec::new(),
        }];
        let emb = vec![vec![0.5f32, 0.25]];
        let nodes = chunk_nodes("urn:x", &chunks, Some(&emb));
        assert_eq!(nodes[0]["@id"], "urn:x-chunk-0");
        assert_eq!(nodes[0]["doc:headerPath"], "A / B");
        assert_eq!(nodes[0]["doc:embedding"]["@type"], "@vector");
        assert_eq!(nodes[0]["doc:sourceElement"][0], "urn:x-element-1");
    }

    #[test]
    fn transaction_merges_graphs() {
        let doco = r#"{"@context":{},"@graph":[{"@id":"e0","@type":"foaf:Document"}]}"#;
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

    /// The engine's graph is written in its own context. Every term it
    /// defines must reach the transaction as it defined it: a lost `@id`
    /// coercion stores a reference as a string, and a lost prefix stores
    /// `foaf:Document` as an IRI of the scheme `foaf`.
    #[test]
    fn transaction_keeps_the_engines_context() {
        let doco = fluree_doc_model::to_doco(
            &[fluree_doc_model::Element::new(
                "doco:Paragraph",
                "Text.",
                "rust",
                "layout",
            )],
            &fluree_doc_model::DocoOptions {
                base_iri: "urn:x".into(),
                ..Default::default()
            },
        );
        let engine: Value = serde_json::from_str(&doco).unwrap();
        let engine = engine["@context"].as_object().expect("engine context");
        assert!(
            engine.contains_key("foaf") && engine.contains_key("po:containsAsHeader"),
            "the fixture no longer exercises the terms this guards: {engine:?}"
        );
        let tx = transaction(&doco, json!({"@id":"d"}), vec![], vec![]).unwrap();
        for (term, definition) in engine {
            assert_eq!(
                &tx["@context"][term], definition,
                "the transaction redefines or drops the engine's `{term}`"
            );
        }
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
