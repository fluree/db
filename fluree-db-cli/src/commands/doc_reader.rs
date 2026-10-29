//! Extraction by an HTTP reader (`[doc.reader]`), for `fluree doc`: the
//! setup shared across a run's documents, and one document read.
//!
//! A reader takes the document whole, so where the language-model path
//! asks about each chunk and scans each for catalog labels, this sends the
//! parser's text and structure with the ontology and catalog, in as few
//! calls as the reader's limits allow, and keeps what it answers.

use crate::commands::doc_sources::{self, Source};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_doc::reader::{
    request, Credential, Document, Frame, Limits, PromptSize, ReaderClient, Structure, Usage,
    Vocabulary,
};
use fluree_db_doc::{DocCache, PreparedDocument, ReaderEndpoint};
use serde_json::{json, Value};

/// Cache modes a reader understands, as `--cache` takes them.
const CACHE_MODES: [&str; 3] = ["ontology", "catalog", "off"];

/// What every document of a run is read with.
pub struct ReaderSetup {
    pub client: ReaderClient,
    pub model: String,
    pub cache: Option<String>,
    ontology: fluree_graph_ir::Graph,
    catalog: fluree_graph_ir::Graph,
    size: PromptSize,
    vocabulary: Option<&'static Vocabulary>,
    /// Answers kept by request body, so a rerun that sends the same
    /// request is not read (or billed) twice.
    doc_cache: Option<DocCache>,
    /// Hash of the model, ontology and catalog: what makes an earlier
    /// reading of an unchanged document still current.
    pub fingerprint: String,
    pub catalog_counts: Vec<(String, usize)>,
}

/// Where the reader's credential comes from: today the slot's own key
/// (`[doc.reader] api_key`, in `auth_header`). An account sign-in that
/// covers the reader would be resolved here too.
pub fn credential(endpoint: &ReaderEndpoint) -> Option<Credential> {
    Credential::from_endpoint(endpoint)
}

pub async fn setup(
    model: &str,
    endpoint: &ReaderEndpoint,
    ontology: Option<&str>,
    catalog: &[String],
    cache_flag: Option<&str>,
    doc_cache: Option<DocCache>,
    dirs: Option<&FlureeDir>,
) -> CliResult<ReaderSetup> {
    let Some(spec) = ontology else {
        return Err(CliError::Usage(format!(
            "--model {model} reads against an ontology: pass --ontology <LEDGER|FILE>"
        )));
    };
    let cache = cache_flag
        .map(str::to_string)
        .or_else(|| endpoint.cache.clone())
        .map(|c| c.trim().to_ascii_lowercase());
    if let Some(c) = &cache {
        if !CACHE_MODES.contains(&c.as_str()) {
            return Err(CliError::Usage(format!(
                "--cache {c}: expected one of {}",
                CACHE_MODES.join(", ")
            )));
        }
    }
    let ontology = doc_sources::load_rdf(&Source::parse(spec), dirs).await?;
    let sources: Vec<Source> = catalog.iter().map(|s| Source::parse(s)).collect();
    let loaded = doc_sources::load_catalog(&sources, dirs).await?;
    let size = PromptSize {
        ontology_terms: request::ontology_terms(&ontology),
        catalog_records: request::catalog_records(&loaded.graph),
    };
    let ntriples = request::canonical_ntriples;
    let fingerprint = fluree_db_doc::cache::sha256_hex(
        format!(
            "reader|{model}|{}|{}",
            fluree_db_doc::cache::sha256_hex(ntriples(&ontology).as_bytes()),
            fluree_db_doc::cache::sha256_hex(ntriples(&loaded.graph).as_bytes()),
        )
        .as_bytes(),
    );
    Ok(ReaderSetup {
        client: ReaderClient::with_credential(&endpoint.url, credential(endpoint)),
        model: model.to_string(),
        cache: cache.filter(|c| c != "off"),
        ontology,
        catalog: loaded.graph,
        size,
        vocabulary: fluree_db_doc::reader::profile(model).map(|p| &p.vocabulary),
        doc_cache,
        fingerprint,
        catalog_counts: loaded.counts,
    })
}

impl ReaderSetup {
    pub fn ontology_terms(&self) -> usize {
        self.size.ontology_terms
    }

    pub fn catalog_records(&self) -> usize {
        self.size.catalog_records
    }
}

/// One document's reading.
#[derive(Debug, Default)]
pub struct Read {
    pub nodes: Vec<Value>,
    pub mentions: usize,
    pub entities: usize,
    pub relations: usize,
    /// Catalog records the reading links to.
    pub linked: std::collections::BTreeSet<String>,
    pub usage: Usage,
    /// Calls the document took, and how many of them a cached answer served.
    pub calls: usize,
    pub cached: usize,
    /// The release that served, as the reader names it.
    pub release: Option<String>,
    /// What the reader set aside or adapted in the ontology.
    pub ontology_notes: Vec<String>,
}

/// Read one parsed document: cut it into calls, read each, and gather the
/// answers in the document's own offsets. Blocking (the client is), so run
/// it on a blocking thread.
/// With `requests_to`, each call's body is also written there as
/// `<relative-path>.request-<n>.trig`, beside `--out-dir`'s transactions.
pub fn read_document(
    setup: &ReaderSetup,
    doc: &PreparedDocument,
    requests_to: Option<&std::path::Path>,
) -> CliResult<Read> {
    let document = Document {
        iri: &doc.doc_iri,
        name: &doc.meta.file_name,
        text: &doc.parsed.text,
        doco: &doc.parsed.doco,
    };
    let structure = Structure::new(&document)?;
    let parts = structure.plan(&Limits::default(), &setup.size)?;
    let mut read = Read {
        calls: parts.len(),
        ..Default::default()
    };
    for (i, part) in parts.iter().enumerate() {
        let graph = structure.document_graph(&document, part);
        let body = request::body(&graph, &setup.ontology, &setup.catalog, part)?;
        if let Some(dir) = requests_to {
            let name = format!(
                "{}.request-{}.trig",
                doc.meta.relative_path.replace('/', "__"),
                i + 1
            );
            std::fs::write(dir.join(name), &body)?;
        }
        let key = DocCache::reading_key(
            &format!(
                "reader|{}|{}",
                setup.model,
                setup.cache.as_deref().unwrap_or("")
            ),
            &body,
            &[],
        );
        let cached = setup
            .doc_cache
            .as_ref()
            .and_then(|c| c.load_reading(&key))
            .flatten()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok());
        let (answer_body, release) = match cached {
            Some(hit) => {
                read.cached += 1;
                (
                    hit["body"].as_str().unwrap_or_default().to_string(),
                    hit["release"].as_str().map(str::to_string),
                )
            }
            None => {
                let label = if parts.len() > 1 {
                    format!(
                        "{} (call {} of {})",
                        doc.meta.relative_path,
                        i + 1,
                        parts.len()
                    )
                } else {
                    doc.meta.relative_path.clone()
                };
                let answer =
                    setup
                        .client
                        .read(&body, &setup.model, setup.cache.as_deref(), &|wait| {
                            eprintln!("    {} {label}: {wait}", "…".dimmed());
                        })?;
                read.usage.add(&answer.usage);
                for note in &answer.ontology_notes {
                    if !read.ontology_notes.contains(note) {
                        read.ontology_notes.push(note.clone());
                    }
                }
                if let Some(c) = &setup.doc_cache {
                    let entry = json!({ "body": answer.body, "release": answer.release });
                    // A cache that cannot be written costs a rerun, not this run.
                    let _ = c.store_reading(&key, Some(&entry.to_string()));
                }
                (answer.body, answer.release)
            }
        };
        if release.is_some() {
            read.release = release;
        }
        let frame = Frame {
            doc_iri: &doc.doc_iri,
            offset: part.begin,
            part: i,
            parts: parts.len(),
        };
        let reading = fluree_db_doc::reader::read_answer(&answer_body, &frame, setup.vocabulary)?;
        read.mentions += reading.mentions;
        read.entities += reading.entities;
        read.relations += reading.relations;
        read.linked.extend(reading.linked);
        let mut nodes = reading.nodes;
        source_elements(&structure, &mut nodes);
        read.nodes.extend(nodes);
    }
    Ok(read)
}

/// A mention gets `doc:sourceElement`, the element its span sits in, as
/// the language-model path gives one.
fn source_elements(structure: &Structure, nodes: &mut [Value]) {
    use fluree_db_doc::vocab::{DOC_NS, NIF_NS};
    let mention = format!("{DOC_NS}Mention");
    let offset = |node: &Value, p: &str| {
        let v = node
            .get(format!("{NIF_NS}{p}"))?
            .as_array()?
            .first()?
            .get("@value")?;
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .map(|n| n as usize)
    };
    for node in nodes {
        let is_mention = node
            .get("@type")
            .and_then(Value::as_array)
            .is_some_and(|t| t.iter().any(|t| t == &json!(mention)));
        let key = format!("{DOC_NS}sourceElement");
        if !is_mention || node.get(&key).is_some() {
            continue;
        }
        if let (Some(b), Some(e)) = (offset(node, "beginIndex"), offset(node, "endIndex")) {
            if let Some(element) = structure.element_at(b, e) {
                node[key] = json!([{ "@id": element }]);
            }
        }
    }
}

/// `1,032 tokens, cache 900 read / 0 written, 3.4 fuel` for a document line
/// or a run total; empty when nothing was billed.
pub fn usage_note(usage: &Usage) -> String {
    let mut parts = Vec::new();
    if usage.input_tokens > 0 {
        parts.push(format!("{} tokens", thousands(usage.input_tokens)));
    }
    if usage.cache_read_tokens > 0 || usage.cache_write_tokens > 0 {
        parts.push(format!(
            "cache {} read / {} written",
            thousands(usage.cache_read_tokens),
            thousands(usage.cache_write_tokens)
        ));
    }
    if let Some(fuel) = usage.fuel {
        parts.push(format!("{fuel:.2} fuel"));
    }
    parts.join(", ")
}

fn thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_reads_like_a_receipt() {
        let usage = Usage {
            input_tokens: 1_032,
            windows: 1,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            fuel: None,
        };
        assert_eq!(usage_note(&usage), "1,032 tokens");
        let usage = Usage {
            input_tokens: 1_234_567,
            windows: 3,
            cache_read_tokens: 900,
            cache_write_tokens: 12,
            fuel: Some(3.4),
        };
        assert_eq!(
            usage_note(&usage),
            "1,234,567 tokens, cache 900 read / 12 written, 3.40 fuel"
        );
        assert_eq!(usage_note(&Usage::default()), "");
    }
}
