//! A run of reads: what every document is read with (the client, model,
//! ontology and catalog), and one document read, cut into calls and
//! gathered back in the document's own offsets.
//!
//! Shared by the `fluree` CLI and the Python package, so a document read
//! from either is cut, sent, resent, cached and kept the same way.

use super::client::{ReaderClient, Usage};
use super::reading::{profile, read_answer, Frame, Vocabulary};
use super::request::{self, Document, Limits, PromptSize, Structure};
use crate::vocab::{DOC_NS, NIF_NS};
use crate::{DocCache, DocError, PreparedDocument, Result};
use fluree_graph_ir::Graph;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;

/// Cache modes a reader understands, as `--cache` and `cache=` take them.
pub const CACHE_MODES: [&str; 3] = ["ontology", "catalog", "off"];

/// What every document of a run is read with.
pub struct Session {
    pub client: ReaderClient,
    pub model: String,
    /// The cache mode sent with each call; `None` sends none.
    pub cache: Option<String>,
    ontology: Graph,
    catalog: Graph,
    size: PromptSize,
    vocabulary: Option<&'static Vocabulary>,
    /// Answers kept by request body, so a rerun that sends the same
    /// request is not read (or billed) twice.
    doc_cache: Option<DocCache>,
}

impl Session {
    /// `cache` is checked against [`CACHE_MODES`]; `off` sends none.
    pub fn new(
        client: ReaderClient,
        model: &str,
        cache: Option<&str>,
        ontology: Graph,
        catalog: Graph,
        doc_cache: Option<DocCache>,
    ) -> Result<Self> {
        let cache = cache.map(|c| c.trim().to_ascii_lowercase());
        if let Some(c) = &cache {
            if !CACHE_MODES.contains(&c.as_str()) {
                return Err(DocError::Config(format!(
                    "cache {c}: expected one of {}",
                    CACHE_MODES.join(", ")
                )));
            }
        }
        let size = PromptSize {
            ontology_terms: request::ontology_terms(&ontology),
            catalog_records: request::catalog_records(&catalog),
        };
        Ok(Self {
            client,
            model: model.to_string(),
            cache: cache.filter(|c| c != "off"),
            ontology,
            catalog,
            size,
            vocabulary: profile(model).map(|p| &p.vocabulary),
            doc_cache,
        })
    }

    pub fn ontology_terms(&self) -> usize {
        self.size.ontology_terms
    }

    pub fn catalog_records(&self) -> usize {
        self.size.catalog_records
    }

    /// Hash of the model, ontology and catalog: what makes an earlier
    /// reading of an unchanged document still current.
    pub fn fingerprint(&self) -> String {
        let hash = |g: &Graph| crate::cache::sha256_hex(request::canonical_ntriples(g).as_bytes());
        crate::cache::sha256_hex(
            format!(
                "reader|{}|{}|{}",
                self.model,
                hash(&self.ontology),
                hash(&self.catalog)
            )
            .as_bytes(),
        )
    }

    /// Read one parsed document: cut it into calls, read each, and gather
    /// the answers in the document's own offsets. Blocking (the client is).
    /// `on_wait` hears each wait for capacity, prefixed with the document's
    /// path (and the call, when there are several). With `requests_to`,
    /// each call's body is also written there as
    /// `<relative-path>.request-<n>.trig`.
    pub fn read(
        &self,
        doc: &PreparedDocument,
        requests_to: Option<&Path>,
        on_wait: &dyn Fn(&str),
    ) -> Result<Read> {
        let document = Document {
            iri: &doc.doc_iri,
            name: &doc.meta.file_name,
            text: &doc.parsed.text,
            doco: &doc.parsed.doco,
        };
        let structure = Structure::new(&document)?;
        let parts = structure.plan(&Limits::default(), &self.size)?;
        let mut read = Read {
            calls: parts.len(),
            ..Default::default()
        };
        for (i, part) in parts.iter().enumerate() {
            let graph = structure.document_graph(&document, part);
            let body = request::body(&graph, &self.ontology, &self.catalog, part)?;
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
                    self.model,
                    self.cache.as_deref().unwrap_or("")
                ),
                &body,
                &[],
            );
            let cached = self
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
                        self.client
                            .read(&body, &self.model, self.cache.as_deref(), &|wait| {
                                on_wait(&format!("{label}: {wait}"));
                            })?;
                    read.usage.add(&answer.usage);
                    for note in &answer.ontology_notes {
                        if !read.ontology_notes.contains(note) {
                            read.ontology_notes.push(note.clone());
                        }
                    }
                    if let Some(c) = &self.doc_cache {
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
            let reading = read_answer(&answer_body, &frame, self.vocabulary)?;
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
}

/// One document's reading.
#[derive(Debug, Default)]
pub struct Read {
    /// Expanded JSON-LD nodes, ready for the document's commit.
    pub nodes: Vec<Value>,
    pub mentions: usize,
    pub entities: usize,
    pub relations: usize,
    /// Catalog records the reading links to.
    pub linked: BTreeSet<String>,
    pub usage: Usage,
    /// Calls the document took, and how many of them a cached answer served.
    pub calls: usize,
    pub cached: usize,
    /// The release that served, as the reader names it.
    pub release: Option<String>,
    /// What the reader set aside or adapted in the ontology.
    pub ontology_notes: Vec<String>,
}

/// A mention gets `doc:sourceElement`, the element its span sits in, as
/// the language-model path gives one.
fn source_elements(structure: &Structure, nodes: &mut [Value]) {
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
