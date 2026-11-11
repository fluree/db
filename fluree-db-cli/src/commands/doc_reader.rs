//! Extraction by an HTTP reader (`[doc.reader]`), for `fluree doc`: where
//! the reader, ontology and catalog come from. The read itself is
//! [`fluree_db_doc::reader::Session`], shared with the Python package.
//!
//! A reader takes the document whole, so where the language-model path
//! asks about each chunk and scans each for catalog labels, this sends the
//! parser's text and structure with the ontology and catalog, in as few
//! calls as the reader's limits allow, and keeps what it answers.

use crate::commands::doc_sources::{self, Source};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_doc::reader::{Credential, Read, ReaderClient, Session, CACHE_MODES};
use fluree_db_doc::{DocCache, DocError, PreparedDocument, ReaderEndpoint};

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
    let session = Session::new(client, model, cache, ontology, loaded.graph, doc_cache)?;
    Ok(ReaderSetup {
        session,
        catalog_counts: loaded.counts,
    })
}

/// Why a document's read failed, and so whether a run of several goes on.
/// `doc ingest` keeps one rule for every failure: one of this document
/// marks it ✗ and the next document is read; one that every remaining
/// document is sure to meet too ends the run with a single message, rather
/// than a ✗ (and a refused call) for each.
pub enum ReadFailure {
    /// This document's own: a refused or bad answer, a reading that won't
    /// parse.
    Document(CliError),
    /// One the next document would meet too (see [`read_document`]).
    Run(CliError),
}

impl From<ReadFailure> for CliError {
    fn from(failure: ReadFailure) -> Self {
        match failure {
            ReadFailure::Document(e) | ReadFailure::Run(e) => e,
        }
    }
}

/// Read one parsed document, saying each wait for capacity on stderr.
/// Blocking, so run it on a blocking thread.
///
/// A reader that asks for a wait past what a call waits
/// ([`DocError::Unavailable`], such as an account over a quota) ends the run
/// ([`ReadFailure::Run`]): it refuses every call until the wait is over.
pub fn read_document(
    setup: &ReaderSetup,
    doc: &PreparedDocument,
    requests_to: Option<&std::path::Path>,
) -> Result<Read, ReadFailure> {
    setup
        .session
        .read(doc, requests_to, &|wait| {
            eprintln!("    {} {wait}", "…".dimmed());
        })
        .map_err(|e| match e {
            DocError::Unavailable { wait, .. } => ReadFailure::Run(CliError::Remote(format!(
                "{e}\n  hint: the reader refuses every call for about {}; try again after that",
                about(wait)
            ))),
            e => ReadFailure::Document(e.into()),
        })
}

/// A wait as a person would say it: seconds, minutes or hours.
fn about(wait: std::time::Duration) -> String {
    match wait.as_secs() {
        s @ 0..120 => format!("{s}s"),
        s @ 120..7_200 => format!("{} min", s.div_ceil(60)),
        s => format!("{} h", s.div_ceil(3_600)),
    }
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
