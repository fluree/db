//! Extraction by an HTTP reader (`[doc.reader]`), for `fluree doc`: where
//! the reader, ontology and catalog come from. The read itself is
//! [`fluree_db_doc::reader::Session`], shared with the Python package.
//!
//! A reader takes the document whole, so where the language-model path
//! asks about each chunk and scans each for catalog labels, this sends the
//! parser's text and structure with the ontology and catalog, in as few
//! calls as the reader's limits allow, and keeps what it answers: a Fluree
//! DB transaction per call, committed as it is in the document's reading
//! graph ([`reading_graph`]).

use crate::commands::doc_sources::{self, Source};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_doc::reader::{Credential, Include, Read, ReaderClient, Session, CACHE_MODES};
use fluree_db_doc::{DocCache, PreparedDocument, ReaderEndpoint};

/// What every document of a run is read with, and what the run reports
/// about its catalog.
pub struct ReaderSetup {
    pub session: Session,
    pub catalog_counts: Vec<(String, usize)>,
}

/// Where the reader's credential comes from: today the slot's own key
/// (`[doc.reader] api_key`, in `auth_header`). An account sign-in that
/// covers the reader would be resolved here too.
pub fn credential(endpoint: &ReaderEndpoint) -> Option<Credential> {
    Credential::from_endpoint(endpoint)
}

/// The named graph a document's reading is committed in: `<doc>/reading`,
/// so a re-read replaces it whole by clearing that graph. Every call's
/// transaction (the facts, the claims annotating them, the mentions) goes
/// there; the document's structure stays in the default graph.
pub fn reading_graph(doc_iri: &str) -> String {
    format!("{doc_iri}/reading")
}

/// What `--include` asks for: claims and mentions unless it says otherwise.
pub fn include(flag: Option<&str>) -> CliResult<Include> {
    match flag {
        None => Ok(Include::default()),
        Some(v) => Include::parse(v).map_err(|e| match e {
            fluree_db_doc::DocError::Config(m) => CliError::Usage(format!("--{m}")),
            e => e.into(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn setup(
    model: &str,
    endpoint: &ReaderEndpoint,
    ontology: Option<&str>,
    catalog: &[String],
    cache_flag: Option<&str>,
    include: Include,
    doc_cache: Option<DocCache>,
    dirs: Option<&FlureeDir>,
) -> CliResult<ReaderSetup> {
    let Some(spec) = ontology else {
        return Err(CliError::Usage(format!(
            "--model {model} reads against an ontology: pass --ontology <LEDGER|FILE>"
        )));
    };
    let cache = cache_flag.or(endpoint.cache.as_deref());
    if let Some(c) = cache.map(|c| c.trim().to_ascii_lowercase()) {
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
    let client = ReaderClient::with_credential(&endpoint.url, credential(endpoint));
    let session = Session::new(client, model, cache, ontology, loaded.graph, doc_cache)?
        .with_include(include);
    Ok(ReaderSetup {
        session,
        catalog_counts: loaded.counts,
    })
}

/// Read one parsed document, saying each wait for capacity on stderr.
/// Blocking, so run it on a blocking thread.
pub fn read_document(
    setup: &ReaderSetup,
    doc: &PreparedDocument,
    requests_to: Option<&std::path::Path>,
) -> CliResult<Read> {
    Ok(setup.session.read(doc, requests_to, &|wait| {
        eprintln!("    {} {wait}", "…".dimmed());
    })?)
}

/// What reading a document would take, with nothing sent (`--dry-run`):
/// its calls, and how many a cached answer would serve.
pub fn plan_document(
    setup: &ReaderSetup,
    doc: &PreparedDocument,
    requests_to: Option<&std::path::Path>,
) -> CliResult<(usize, usize)> {
    Ok(setup.session.plan(doc, requests_to)?)
}
