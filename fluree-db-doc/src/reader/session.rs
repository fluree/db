//! A run of reads: what every document is read with (the client, model,
//! ontology and catalog, what the answer holds), and one document read, cut
//! into calls and gathered back in the document's own offsets.
//!
//! Shared by the `fluree` CLI and the Python package, so a document read
//! from either is cut, sent, resent, cached and kept the same way.

use super::client::{Controls, Include, ReaderClient, Usage};
use super::reading::{read_answer, Claim, Frame, Mention, Reading};
use super::request::{self, Document, Limits, Part, PromptSize, Structure};
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
    /// What each answer holds besides the facts.
    pub include: Include,
    ontology: Graph,
    catalog: Graph,
    size: PromptSize,
    /// Answers kept by request, so a rerun that sends the same request is
    /// not read (or billed) twice.
    doc_cache: Option<DocCache>,
}

impl Session {
    /// `cache` is checked against [`CACHE_MODES`]; `off` sends none. Each
    /// answer holds the claims and the mentions until [`Self::with_include`]
    /// says otherwise.
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
            include: Include::default(),
            ontology,
            catalog,
            size,
            doc_cache,
        })
    }

    /// What each answer holds besides the facts.
    pub fn with_include(mut self, include: Include) -> Self {
        self.include = include;
        self
    }

    pub fn ontology_terms(&self) -> usize {
        self.size.ontology_terms
    }

    /// Hash of the model, ontology and catalog, and what the answer holds:
    /// what makes an earlier reading of an unchanged document still
    /// current. An error rather than a hash of nothing when a graph can't
    /// be written.
    pub fn fingerprint(&self) -> Result<String> {
        let hash = |g: &Graph| -> Result<String> {
            Ok(crate::cache::sha256_hex(
                request::canonical_ntriples(g)?.as_bytes(),
            ))
        };
        Ok(crate::cache::sha256_hex(
            format!(
                "reader|{}|{}|{}|{}",
                self.model,
                self.include.header(),
                hash(&self.ontology)?,
                hash(&self.catalog)?
            )
            .as_bytes(),
        ))
    }

    /// A document's calls with their bodies, every one built and sized
    /// before anything is sent; none for a document with no text. With
    /// `requests_to`, each body is also written there as
    /// `<relative-path>.request-<n>.trig`.
    fn requests(
        &self,
        doc: &PreparedDocument,
        requests_to: Option<&Path>,
    ) -> Result<(Structure, Vec<(Part, String)>)> {
        let document = Document {
            iri: &doc.doc_iri,
            name: &doc.meta.file_name,
            text: &doc.parsed.text,
            doco: &doc.parsed.doco,
        };
        let structure = Structure::new(&document)?;
        // No text, nothing to read: a call would bill the ontology and
        // catalog for an empty reading.
        if doc.parsed.text.trim().is_empty() {
            return Ok((structure, Vec::new()));
        }
        let requests = structure.requests(
            &document,
            &self.ontology,
            &self.catalog,
            &Limits::default(),
            &self.size,
        )?;
        if let Some(dir) = requests_to {
            for (i, (_, body)) in requests.iter().enumerate() {
                let name = format!(
                    "{}.request-{}.trig",
                    doc.meta.relative_path.replace('/', "__"),
                    i + 1
                );
                std::fs::write(dir.join(name), body)?;
            }
        }
        Ok((structure, requests))
    }

    fn controls(&self, part: &Part) -> Controls {
        Controls {
            include: self.include,
            windows: part.windows.clone(),
        }
    }

    /// Where an answer to `body` asked with `controls` is cached. The
    /// endpoint is part of the key: two readers serving one model name are
    /// two readers. So is every header that shapes the answer.
    fn answer_key(&self, body: &str, controls: &Controls) -> String {
        let headers: Vec<String> = controls
            .headers()
            .into_iter()
            .map(|(h, v)| format!("{h}={v}"))
            .collect();
        DocCache::reading_key(
            &format!(
                "reader|layout-2|{}|{}|{}|{}",
                self.client.url(),
                self.model,
                self.cache.as_deref().unwrap_or(""),
                headers.join(";")
            ),
            body,
            &[],
        )
    }

    /// What reading a document would take, with nothing sent (a dry run):
    /// its calls, and how many a cached answer would serve.
    pub fn plan(
        &self,
        doc: &PreparedDocument,
        requests_to: Option<&Path>,
    ) -> Result<(usize, usize)> {
        let (_, requests) = self.requests(doc, requests_to)?;
        let cached = requests
            .iter()
            .filter(|(part, body)| {
                self.doc_cache
                    .as_ref()
                    .and_then(|c| c.load_reading(&self.answer_key(body, &self.controls(part))))
                    .flatten()
                    .is_some()
            })
            .count();
        Ok((requests.len(), cached))
    }

    /// Read one parsed document: cut it into calls, read each, and move each
    /// answer's offsets into the document's text. Blocking (the client is).
    /// `on_wait` hears each wait for capacity, prefixed with the document's
    /// path (and the call, when there are several). `requests_to` as in
    /// [`Session::plan`].
    pub fn read(
        &self,
        doc: &PreparedDocument,
        requests_to: Option<&Path>,
        on_wait: &dyn Fn(&str),
    ) -> Result<Read> {
        let (structure, requests) = self.requests(doc, requests_to)?;
        let parts = requests.len();
        let mut read = Read::default();
        for (i, (part, body)) in requests.iter().enumerate() {
            let controls = self.controls(part);
            let key = self.answer_key(body, &controls);
            let cached = self
                .doc_cache
                .as_ref()
                .and_then(|c| c.load_reading(&key))
                .flatten()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok());
            let frame = Frame {
                doc_iri: &doc.doc_iri,
                offset: part.begin,
            };
            let field = |v: &Value, k: &str| v[k].as_str().map(str::to_string);
            let (mut reading, release, request_id, run, from_cache) = match cached {
                Some(hit) => (
                    read_answer(hit["body"].as_str().unwrap_or_default(), &frame)?,
                    field(&hit, "release"),
                    field(&hit, "request_id"),
                    field(&hit, "run"),
                    true,
                ),
                None => {
                    let label = if parts > 1 {
                        format!("{} (call {} of {parts})", doc.meta.relative_path, i + 1)
                    } else {
                        doc.meta.relative_path.clone()
                    };
                    let answer = self.client.read(
                        body,
                        &self.model,
                        self.cache.as_deref(),
                        &controls,
                        &|wait| on_wait(&format!("{label}: {wait}")),
                    )?;
                    read.usage.add(&answer.usage);
                    for note in &answer.ontology_notes {
                        if !read.ontology_notes.contains(note) {
                            read.ontology_notes.push(note.clone());
                        }
                    }
                    // Read before it is cached: an answer this can't read is
                    // not kept to be refused again.
                    let reading =
                        read_answer(&answer.body, &frame).map_err(|e| with_request(e, &answer))?;
                    if let Some(c) = &self.doc_cache {
                        let entry = json!({
                            "body": answer.body,
                            "release": answer.release,
                            "request_id": answer.request_id,
                            "run": answer.run,
                        });
                        // A cache that cannot be written costs a rerun, not this run.
                        let _ = c.store_reading(&key, Some(&entry.to_string()));
                    }
                    (
                        reading,
                        answer.release,
                        answer.request_id,
                        answer.run,
                        false,
                    )
                }
            };
            if reading.run.is_none() {
                reading.run = run;
            }
            reading.release = release;
            reading.request_id = request_id;
            reading.cached = from_cache;
            for m in &mut reading.mentions {
                if let (Some(b), Some(e)) = (m.begin, m.end) {
                    m.element = structure.element_at(b, e).map(str::to_string);
                }
            }
            read.readings.push(reading);
        }
        Ok(read)
    }
}

fn with_request(e: DocError, answer: &super::client::Answer) -> DocError {
    match (&e, &answer.request_id) {
        (DocError::Model(m), Some(id)) => DocError::Model(format!("{m} (request {id})")),
        _ => e,
    }
}

/// One document's reading: one transaction per call, in text order.
#[derive(Debug, Default)]
pub struct Read {
    pub readings: Vec<Reading>,
    pub usage: Usage,
    /// What the reader set aside or adapted in the ontology.
    pub ontology_notes: Vec<String>,
}

impl Read {
    /// Calls the document took.
    pub fn calls(&self) -> usize {
        self.readings.len()
    }

    /// How many of them the reading cache answered.
    pub fn cached(&self) -> usize {
        self.readings.iter().filter(|r| r.cached).count()
    }

    /// The release that served the last call that names one.
    pub fn release(&self) -> Option<&str> {
        self.readings
            .iter()
            .rev()
            .find_map(|r| r.release.as_deref())
    }

    pub fn mentions(&self) -> impl Iterator<Item = &Mention> {
        self.readings.iter().flat_map(|r| r.mentions.iter())
    }

    pub fn claims(&self) -> impl Iterator<Item = &Claim> {
        self.readings.iter().flat_map(|r| r.claims.iter())
    }

    /// Statements in the facts, claims aside, over every call.
    pub fn facts(&self) -> usize {
        self.readings.iter().map(|r| r.facts).sum()
    }

    /// Entities the runs minted.
    pub fn new_entities(&self) -> BTreeSet<&str> {
        self.readings
            .iter()
            .flat_map(|r| r.new_entities.iter().map(String::as_str))
            .collect()
    }

    /// Catalog records the mentions link to.
    pub fn linked(&self) -> BTreeSet<&str> {
        self.readings
            .iter()
            .flat_map(|r| r.linked.iter().map(String::as_str))
            .collect()
    }

    /// One line per call: the run, the release and the endpoint's ID for
    /// the call, as far as each is known.
    pub fn run_lines(&self) -> Vec<String> {
        self.readings
            .iter()
            .map(|r| {
                let mut parts = vec![format!("run {}", r.run.as_deref().unwrap_or("(not named)"))];
                if let Some(release) = &r.release {
                    parts.push(format!("release {release}"));
                }
                if let Some(id) = &r.request_id {
                    parts.push(format!("request {id}"));
                }
                if r.cached {
                    parts.push("from cache".into());
                }
                parts.join(", ")
            })
            .collect()
    }
}
