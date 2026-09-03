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
}

pub fn chunk_iri(doc_iri: &str, index: usize) -> String {
    format!("{doc_iri}/chunk/{index}")
}

/// The `doc:SourceDocument` node: file facts plus what the run produced.
pub fn document_node(
    meta: &DocumentMeta,
    chunk_count: usize,
    embedding: Option<(&str, usize)>,
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
        vocab::INGESTED_AT: { "@value": meta.ingested_at, "@type": "xsd:dateTime" },
    });
    if let Some((model, dims)) = embedding {
        node[vocab::EMBEDDING_MODEL] = json!(model);
        node[vocab::EMBEDDING_DIMENSIONS] = json!(dims);
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

/// The structure graph, the document node and the chunks as one JSON-LD
/// transaction under the shared context.
pub fn transaction(doco_json: &str, document: Value, chunks: Vec<Value>) -> Result<Value> {
    let doco: Value = serde_json::from_str(doco_json)
        .map_err(|e| DocError::Parse(format!("doco graph is not JSON: {e}")))?;
    let mut graph = match doco.get("@graph") {
        Some(Value::Array(nodes)) => nodes.clone(),
        _ => return Err(DocError::Parse("doco graph: missing @graph array".into())),
    };
    graph.push(document);
    graph.extend(chunks);
    Ok(json!({
        "@context": vocab::context(),
        "@graph": graph,
    }))
}

/// SPARQL UPDATE that retracts everything a previous extraction of this
/// document wrote: every element and chunk stamped with it, and the
/// document node itself. Re-extraction is retract-then-insert, never a
/// diff; the earlier extraction stays queryable at its commit.
pub fn retract_update(doc_iri: &str) -> String {
    format!(
        "PREFIX doc: <{ns}>\n\
         DELETE {{ ?s ?p ?o }}\n\
         WHERE {{\n  \
           {{ ?s doc:sourceDocument <{iri}> . ?s ?p ?o }}\n  \
           UNION\n  \
           {{ VALUES ?s {{ <{iri}> }} ?s ?p ?o }}\n\
         }}",
        ns = vocab::DOC_NS,
        iri = doc_iri
    )
}

/// What an earlier ingest of this document recorded — content hash, parser
/// revision and embedding model — so an unchanged document can be skipped.
/// Rows are `[sha256, parserRevision, embeddingModel | null]`.
pub fn exists_query(doc_iri: &str) -> Value {
    json!({
        "@context": { "doc": vocab::DOC_NS },
        "where": [
            { "@id": doc_iri, vocab::SHA256: "?sha", vocab::PARSER_REVISION: "?rev" },
            ["optional", { "@id": doc_iri, vocab::EMBEDDING_MODEL: "?model" }]
        ],
        "select": ["?sha", "?rev", "?model"],
        "limit": 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_nodes_carry_vectors_and_sources() {
        let chunks = vec![Chunk {
            header_path: vec!["A".into(), "B".into()],
            text: "body".into(),
            source_ids: vec!["urn:x/element/1".into()],
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
        let tx = transaction(doco, json!({"@id":"d"}), vec![json!({"@id":"c0"})]).unwrap();
        let ids: Vec<&str> = tx["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["@id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["e0", "d", "c0"]);
        assert_eq!(tx["@context"]["doc"], vocab::DOC_NS);
    }
}
