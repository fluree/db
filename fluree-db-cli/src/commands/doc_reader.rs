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
use fluree_db_doc::{DocCache, PreparedDocument, ReaderEndpoint};

/// What every document of a run is read with, and what the run reports
/// about its catalog.
pub struct ReaderSetup {
    pub session: Session,
    pub catalog_counts: Vec<(String, usize)>,
    /// The credential is the account sign-in, renewed per document.
    account: bool,
}

/// The reader's URL and credential: the slot's own (`url`, `api_key` in
/// `auth_header`), or with `account = true` this machine's `fluree login`,
/// whose API is the default URL.
pub async fn resolve(endpoint: &ReaderEndpoint) -> CliResult<(String, Option<Credential>)> {
    if !endpoint.account {
        return Ok((endpoint.url.clone(), Credential::from_endpoint(endpoint)));
    }
    let session = account_session(false).await?;
    let url = if endpoint.url.trim().is_empty() {
        format!("{}/grounding", session.api_base_url.trim_end_matches('/'))
    } else {
        endpoint.url.clone()
    };
    Ok((url, Some(Credential::Bearer(session.access_token))))
}

/// The sign-in's token; with `force`, renewed even if it has time left.
async fn account_session(force: bool) -> CliResult<crate::account::AccountSession> {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent(crate::USER_AGENT)
        .build()
        .map_err(|e| CliError::Remote(format!("failed to build HTTP client: {e}")))?;
    crate::account::session(&http, force).await?.ok_or_else(|| {
        CliError::Config(
            "the reader uses this machine's sign-in, and it isn't signed in: run `fluree login`"
                .into(),
        )
    })
}

/// Renew an account credential before a document is read, so a run longer
/// than the hourly token keeps going. A no-op for a slot's own key, and
/// cheap otherwise: the token is refreshed only when it is about to expire.
pub async fn renew(setup: &ReaderSetup) -> CliResult<()> {
    if setup.account {
        let session = account_session(false).await?;
        setup
            .session
            .client
            .set_credential(Some(Credential::Bearer(session.access_token)));
    }
    Ok(())
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
    let (url, credential) = resolve(endpoint).await?;
    let client = ReaderClient::with_credential(&url, credential);
    let session = Session::new(client, model, cache, ontology, loaded.graph, doc_cache)?;
    Ok(ReaderSetup {
        session,
        catalog_counts: loaded.counts,
        account: endpoint.account,
    })
}

/// Read one parsed document, saying each wait for capacity on stderr.
/// Blocking, so run it on a blocking thread.
pub fn read_document(
    setup: &ReaderSetup,
    doc: &PreparedDocument,
    requests_to: Option<&std::path::Path>,
) -> CliResult<Read> {
    let on_wait = |wait: &str| eprintln!("    {} {wait}", "…".dimmed());
    let first = setup.session.read(doc, requests_to, &on_wait);
    // The account's token was refused (most often `config_stale`: its
    // configuration changed after the token was issued): renew it once and
    // read again. Calls already answered come from the cache, if there is one.
    let result = match first {
        Err(fluree_db_doc::DocError::Endpoint { status: 401, .. }) if setup.account => {
            let session = tokio::runtime::Handle::current().block_on(account_session(true))?;
            setup
                .session
                .client
                .set_credential(Some(Credential::Bearer(session.access_token)));
            setup.session.read(doc, requests_to, &on_wait)
        }
        other => other,
    };
    result.map_err(|e| match e.to_string() {
        m if setup.account && m.contains(": 401 ") => CliError::Config(format!(
            "{m}\n  hint: the sign-in was refused; run `fluree login` again"
        )),
        _ => CliError::from(e),
    })
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
