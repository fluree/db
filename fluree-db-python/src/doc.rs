//! `fluree.doc.read`: one document read by an HTTP reader, as `fluree doc
//! read` does it. The parse, the cut into calls, the resends, the cache and
//! the reading's shape are `fluree_db_doc`'s, shared with the CLI; this
//! module only finds the credential and hands the result to Python as data.
//!
//! The credential:
//! 1. `api_key` with `auth_header`: that key, in that header, as is, to `url`;
//! 2. `api_key` alone: a Fluree.ai API key (`flk_…`) is traded for an hour's
//!    access token at `<service>/v1/auth/token` (and reused while it lasts);
//!    any other key goes as `Authorization: Bearer`, as the CLI's
//!    `[doc.reader] api_key` does;
//! 3. with no `api_key`: this machine's `fluree login` (`account.toml` in the
//!    global config directory, refreshed when it is about to expire, as the
//!    CLI does), else `FLUREE_AI_API_KEY` as in 2. Either is sent only to its
//!    own service: a `url` on another origin is called with no credential.
//!
//! With no `url`, a Fluree.ai credential reads at its account's
//! `<api base>/grounding`.

use crate::error::{fluree_error, invalid_request, raise_status};
use fluree_db_doc::reader::{
    read_answer, reading, request, Claim, Credential, Frame, Include, Mention, ReaderClient,
    Session, CACHE_MODES,
};
use fluree_db_doc::{DocCache, DocError, PreparedDocument};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// Where an API key is traded for a token when nothing names the service.
const DEFAULT_SERVICE: &str = "https://fluree.ai";
/// `fluree login`'s file in the global config directory.
const ACCOUNT_FILE: &str = "account.toml";
/// Refresh a sign-in, or trade a key again, with less than this left.
const REFRESH_MARGIN_SECS: i64 = 60;

/// Why a read failed, kept typed across the GIL release so Python gets the
/// right exception class.
enum ReadError {
    /// The arguments can't make a read: a bad cache mode, a header with no
    /// key, a missing sign-in.
    Invalid(String),
    /// A credential was refused (401/403), by the reader or the service.
    Refused {
        status: u16,
        message: String,
    },
    Other(String),
}

impl ReadError {
    fn into_py(self) -> PyErr {
        match self {
            ReadError::Invalid(m) => invalid_request(m),
            ReadError::Refused { status, message } => {
                raise_status("PermissionDeniedError", message, status)
            }
            ReadError::Other(m) => fluree_error(m),
        }
    }

    /// A `fluree_db_doc` error for a read. A reader's refusal comes back
    /// typed (`DocError::Endpoint`); a 401 or 403 there is a refused
    /// credential.
    fn from_doc(e: DocError) -> Self {
        match e {
            DocError::Config(m) | DocError::Unsupported(m) => ReadError::Invalid(m),
            e @ DocError::Endpoint { status, .. } => {
                let message = e.to_string();
                match status {
                    401 | 403 => ReadError::Refused { status, message },
                    _ => ReadError::Other(message),
                }
            }
            other => ReadError::Other(other.to_string()),
        }
    }
}

/// Read `path` with `model` and return the reading as plain data.
#[pyfunction]
#[pyo3(signature = (
    path, *, ontology, catalog, model, url=None, api_key=None, auth_header=None,
    cache=None, include="claims,mentions".to_string(), doc_iri=None,
    base_iri="urn:fluree:doc:".to_string(), cache_dir=None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_document(
    py: Python<'_>,
    path: PathBuf,
    ontology: PathBuf,
    catalog: Vec<(PathBuf, Option<String>)>,
    model: String,
    url: Option<String>,
    api_key: Option<String>,
    auth_header: Option<String>,
    cache: Option<String>,
    include: String,
    doc_iri: Option<String>,
    base_iri: String,
    cache_dir: Option<PathBuf>,
) -> PyResult<Bound<'_, PyDict>> {
    let include = Include::parse(&include).map_err(|e| match e {
        DocError::Config(m) => invalid_request(m),
        e => fluree_error(e.to_string()),
    })?;
    if let Some(c) = &cache {
        if !CACHE_MODES.contains(&c.trim().to_ascii_lowercase().as_str()) {
            return Err(invalid_request(format!(
                "cache={c:?}: expected one of {}",
                CACHE_MODES.join(", ")
            )));
        }
    }
    let read = py.detach(|| -> Result<_, ReadError> {
        let (url, credential) = resolve(url, api_key, auth_header)?;
        let ontology = load_rdf(&ontology).map_err(ReadError::Invalid)?;
        let mut records = fluree_graph_ir::Graph::new();
        for (file, class) in &catalog {
            let graph = load_rdf(file).map_err(ReadError::Invalid)?;
            for t in request::catalog_records_of(&graph, class.as_deref()).iter() {
                records.add(t.clone());
            }
        }
        records.sort();
        records.dedupe();
        let doc_cache = cache_dir.map(DocCache::new);
        let session = Session::new(
            ReaderClient::with_credential(&url, credential),
            &model,
            cache.as_deref(),
            ontology,
            records,
            doc_cache.clone(),
        )
        .map_err(ReadError::from_doc)?
        .with_include(include);
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let iri = doc_iri.unwrap_or_else(|| fluree_db_doc::ingest::document_iri(&base_iri, &name));
        let (meta, parsed) =
            fluree_db_doc::parse_file(&path, &name, &iri, doc_cache.as_ref(), None)
                .map_err(ReadError::from_doc)?;
        let notes = parsed.notes.clone();
        let doc = PreparedDocument {
            meta,
            doc_iri: iri,
            parsed,
            chunks: Vec::new(),
        };
        // A wait for capacity can last minutes: warn, so the default
        // `fluree.engine` logger shows it.
        let read = session
            .read(&doc, None, &|wait| tracing::warn!("{wait}"))
            .map_err(ReadError::from_doc)?;
        Ok((read, url, notes))
    });
    let (read, url, notes) = read.map_err(ReadError::into_py)?;
    let out = PyDict::new(py);
    let json =
        |v: &serde_json::Value| serde_json::to_string(v).map_err(|e| fluree_error(e.to_string()));
    let transactions: Vec<serde_json::Value> = read
        .readings
        .iter()
        .map(|r| r.transaction.clone())
        .collect();
    out.set_item(
        "transactions",
        json(&serde_json::Value::Array(transactions))?,
    )?;
    let mentions: Vec<serde_json::Value> = read.mentions().map(mention_row).collect();
    out.set_item("mentions", json(&serde_json::Value::Array(mentions))?)?;
    let claims: Vec<serde_json::Value> = read.claims().map(claim_row).collect();
    out.set_item("claims", json(&serde_json::Value::Array(claims))?)?;
    let runs: Vec<serde_json::Value> = read
        .readings
        .iter()
        .map(|r| {
            serde_json::json!({
                "run": r.run,
                "release": r.release,
                "request_id": r.request_id,
                "cached": r.cached,
            })
        })
        .collect();
    out.set_item("runs", json(&serde_json::Value::Array(runs))?)?;
    out.set_item("facts", read.facts())?;
    out.set_item(
        "new_entities",
        read.new_entities().into_iter().collect::<Vec<_>>(),
    )?;
    out.set_item("linked", read.linked().into_iter().collect::<Vec<_>>())?;
    let usage = PyDict::new(py);
    usage.set_item("input_tokens", read.usage.input_tokens)?;
    usage.set_item("windows", read.usage.windows)?;
    usage.set_item("cache_read_tokens", read.usage.cache_read_tokens)?;
    usage.set_item("cache_write_tokens", read.usage.cache_write_tokens)?;
    usage.set_item("fuel", read.usage.fuel)?;
    usage.set_item("note", read.usage.note())?;
    out.set_item("usage", usage)?;
    out.set_item("calls", read.calls())?;
    out.set_item("cached", read.cached())?;
    out.set_item("release", read.release())?;
    out.set_item("ontology_notes", read.ontology_notes.clone())?;
    out.set_item("notes", notes)?;
    out.set_item("url", url)?;
    Ok(out)
}

/// A mention as a row: its id, words, offsets, class, the record or new
/// entity it names, its confidence, and the structure element it sits in.
fn mention_row(m: &Mention) -> serde_json::Value {
    serde_json::json!({
        "id": m.id,
        "text": m.text,
        "begin": m.begin,
        "end": m.end,
        "class": m.class,
        "entity": m.entity,
        "new_entity": m.new_entity,
        "confidence": m.confidence,
        "element": m.element,
    })
}

/// A claim as a row; its object an IRI, or a literal's lexical form.
fn claim_row(c: &Claim) -> serde_json::Value {
    let object = c.object.as_ref().map(|o| match o.as_iri() {
        Some(iri) => iri.to_string(),
        None => o
            .as_literal()
            .map(|(v, _, _)| v.lexical())
            .unwrap_or_default(),
    });
    serde_json::json!({
        "id": c.id,
        "subject": c.subject,
        "predicate": c.predicate,
        "object": object,
        "status": c.status,
        "accepted": c.accepted,
        "score": c.score,
        "asserted": c.asserted,
    })
}

/// Readings (their transactions, as JSON) as TriG or Turtle.
#[pyfunction]
pub(crate) fn reading_text(transactions: &str, format: &str) -> PyResult<String> {
    let transactions: Vec<serde_json::Value> =
        serde_json::from_str(transactions).map_err(|e| invalid_request(e.to_string()))?;
    let readings = transactions
        .iter()
        .map(|t| {
            // The offsets are the document's already.
            read_answer(
                &t.to_string(),
                &Frame {
                    doc_iri: "",
                    offset: 0,
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fluree_error(e.to_string()))?;
    match format {
        "trig" => reading::trig(&readings),
        "turtle" => reading::turtle(&readings),
        other => return Err(invalid_request(format!("format {other:?}: trig or turtle"))),
    }
    .map_err(|e| fluree_error(e.to_string()))
}

fn load_rdf(path: &Path) -> Result<fluree_graph_ir::Graph, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let graph = match ext.as_str() {
        "ttl" | "nt" => request::parse_turtle(&text),
        "jsonld" | "json" => request::parse_jsonld(&text),
        _ => {
            return Err(format!(
                "{}: an ontology or catalog is a .ttl, .nt, .jsonld or .json file",
                path.display()
            ))
        }
    };
    graph.map_err(|e| format!("{}: {e}", path.display()))
}

fn resolve(
    url: Option<String>,
    api_key: Option<String>,
    auth_header: Option<String>,
) -> Result<(String, Option<Credential>), ReadError> {
    match (api_key, auth_header) {
        (None, Some(header)) => Err(ReadError::Invalid(format!(
            "auth_header={header:?} names a header for a key, but no api_key= was given"
        ))),
        (Some(key), Some(header)) => {
            let url = url.ok_or_else(|| {
                ReadError::Invalid("a key in `auth_header` needs the reader's `url`".into())
            })?;
            let credential = if header.eq_ignore_ascii_case("authorization") {
                Credential::Bearer(key)
            } else {
                Credential::Header {
                    name: header,
                    value: key,
                }
            };
            Ok((url, Some(credential)))
        }
        (Some(key), None) if is_service_key(&key) => {
            let service = service_for(url.as_deref());
            exchange_key(service, url, &key)
        }
        (Some(key), None) => {
            let url = url.ok_or_else(|| {
                ReadError::Invalid(
                    "api_key= that isn't a Fluree.ai key (flk_…) is sent as a bearer token, and needs the reader's url=".into(),
                )
            })?;
            Ok((url, Some(Credential::Bearer(key))))
        }
        (None, None) => resolve_ambient(url),
    }
}

/// No key given: this machine's sign-in, else `FLUREE_AI_API_KEY`, each sent
/// only to its own service.
fn resolve_ambient(url: Option<String>) -> Result<(String, Option<Credential>), ReadError> {
    if let Some(account) = Account::load()? {
        if url
            .as_deref()
            .is_none_or(|u| origin(u) == origin(&account.api_base()))
        {
            let (api_base, token) = account.session()?;
            let url =
                url.unwrap_or_else(|| format!("{}/grounding", api_base.trim_end_matches('/')));
            return Ok((url, Some(Credential::Bearer(token))));
        }
    }
    if let Some(key) = std::env::var("FLUREE_AI_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
    {
        let service = env_service();
        if url.as_deref().is_none_or(|u| origin(u) == origin(&service)) {
            return exchange_key(service, url, &key);
        }
    }
    match url {
        Some(url) => Ok((url, None)),
        None => Err(ReadError::Invalid(
            "sign in with `fluree login`, or set FLUREE_AI_API_KEY (or pass api_key=)".into(),
        )),
    }
}

/// A Fluree.ai API key, the kind traded for an access token.
fn is_service_key(key: &str) -> bool {
    key.starts_with("flk_")
}

/// The service a key is traded at: `url`'s origin when given, else
/// `FLUREE_AI_URL`, else fluree.ai.
fn service_for(url: Option<&str>) -> String {
    match url {
        Some(u) => origin(u),
        None => env_service(),
    }
}

fn env_service() -> String {
    std::env::var("FLUREE_AI_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SERVICE.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(30)))
        .http_status_as_error(false)
        .max_redirects(0)
        // fluree.ai's edge refuses requests with no User-Agent.
        .user_agent(concat!("fluree-python/", env!("CARGO_PKG_VERSION")))
        .build()
        .into()
}

/// Tokens traded for API keys in this process, by `(service, key)`, with
/// the Unix second each expires: one trade an hour, not one a read.
type TokenCache = Mutex<HashMap<(String, String), (String, i64)>>;

fn tokens() -> &'static TokenCache {
    static TOKENS: OnceLock<TokenCache> = OnceLock::new();
    TOKENS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// A Fluree.ai API key traded for an access token at `service`, reused from
/// an earlier trade while more than a minute of it is left.
fn exchange_key(
    service: String,
    url: Option<String>,
    key: &str,
) -> Result<(String, Option<Credential>), ReadError> {
    let url = url.unwrap_or_else(|| format!("{service}/v1/grounding"));
    let slot = (service.clone(), key.to_string());
    if let Some((token, expires)) = tokens().lock().expect("token cache").get(&slot) {
        if expires - now_secs() > REFRESH_MARGIN_SECS {
            return Ok((url, Some(Credential::Bearer(token.clone()))));
        }
    }
    let mut response = agent()
        .post(&format!("{service}/v1/auth/token"))
        .header("X-API-Key", key)
        .send_empty()
        .map_err(|e| ReadError::Other(format!("{service}/v1/auth/token: {e}")))?;
    let status = response.status().as_u16();
    let body: serde_json::Value = response.body_mut().read_json().unwrap_or_default();
    let Some(token) = body
        .get("accessToken")
        .and_then(|v| v.as_str())
        .filter(|_| status == 200)
    else {
        let message = format!(
            "{service}/v1/auth/token: {status} {}",
            body.get("message")
                .or_else(|| body.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("the API key was refused")
        );
        return Err(match status {
            401 | 403 => ReadError::Refused { status, message },
            _ => ReadError::Other(message),
        });
    };
    // An answer with no expiry is used for this read and not kept.
    if let Some(expires) = body.get("expiresAt").and_then(serde_json::Value::as_i64) {
        tokens()
            .lock()
            .expect("token cache")
            .insert(slot, (token.to_string(), expires));
    }
    Ok((url, Some(Credential::Bearer(token.to_string()))))
}

fn origin(url: &str) -> String {
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    let origin = match url[after_scheme..].find('/') {
        Some(i) => &url[..after_scheme + i],
        None => url,
    };
    origin.trim_end_matches('/').to_ascii_lowercase()
}

/// `fluree login`'s sign-in (`account.toml`), read and renewed the way the
/// CLI's `account` module does: the same keys, the same refresh, the same
/// owner-only atomic save.
struct Account {
    path: PathBuf,
    table: toml::Table,
}

impl Account {
    /// The stored sign-in, or `None` when this machine isn't signed in.
    fn load() -> Result<Option<Self>, ReadError> {
        let Some(dirs) = fluree_db_api::server_defaults::FlureeDir::global() else {
            return Ok(None);
        };
        let path = dirs.config_dir().join(ACCOUNT_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(ReadError::Other(format!("{}: {e}", path.display()))),
        };
        let table: toml::Table = toml::from_str(&text).map_err(|e| {
            ReadError::Invalid(format!(
                "{} is unreadable ({e}); run `fluree logout` and sign in again",
                path.display()
            ))
        })?;
        Ok(Some(Self { path, table }))
    }

    fn field(&self, key: &str) -> Option<String> {
        self.table
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    fn api_base(&self) -> String {
        self.field("api_base_url").unwrap_or_default()
    }

    /// `(api base, access token)`, refreshing and saving the sign-in first
    /// when its token is about to expire.
    fn session(mut self) -> Result<(String, String), ReadError> {
        let (Some(api_base), Some(token)) =
            (self.field("api_base_url"), self.field("access_token"))
        else {
            return Err(ReadError::Invalid(format!(
                "{} is unreadable; run `fluree logout` and sign in again",
                self.path.display()
            )));
        };
        if seconds_left(&token).is_some_and(|s| s > REFRESH_MARGIN_SECS) {
            return Ok((api_base, token));
        }
        let (Some(exchange), Some(refresh)) =
            (self.field("exchange_url"), self.field("refresh_token"))
        else {
            return Err(ReadError::Invalid(format!(
                "{} has no refresh credential; run `fluree login`",
                self.path.display()
            )));
        };
        let mut response = agent()
            .post(&exchange)
            .send_json(
                serde_json::json!({ "grant_type": "refresh_token", "refresh_token": refresh }),
            )
            .map_err(|e| ReadError::Other(format!("refreshing the sign-in: {e}")))?;
        let status = response.status().as_u16();
        let body: serde_json::Value = response.body_mut().read_json().unwrap_or_default();
        let Some(token) = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .filter(|_| status == 200)
            .map(str::to_string)
        else {
            let error = body.get("error").and_then(|v| v.as_str());
            let why = body
                .get("error_description")
                .and_then(|v| v.as_str())
                .or(error)
                .unwrap_or("refresh rejected");
            // The server revoked this machine's sign-in (logout, or
            // disconnected from the dashboard), as against a failed refresh.
            if error == Some("invalid_grant") {
                return Err(ReadError::Refused {
                    status: 401,
                    message: format!(
                        "your sign-in is no longer valid ({why}); run `fluree login` again"
                    ),
                });
            }
            return Err(ReadError::Other(format!(
                "refreshing the sign-in failed ({status}: {why}); run `fluree login` if it persists"
            )));
        };
        self.table
            .insert("access_token".into(), toml::Value::String(token.clone()));
        if let Some(rotated) = body.get("refresh_token").and_then(|v| v.as_str()) {
            self.table.insert(
                "refresh_token".into(),
                toml::Value::String(rotated.to_string()),
            );
        }
        if let Some(summary) = body.get("account").and_then(serde_json::Value::as_object) {
            let mut account = toml::Table::new();
            for key in ["account_id", "org_name", "email", "tier"] {
                if let Some(v) = summary.get(key).and_then(|v| v.as_str()) {
                    account.insert(key.into(), toml::Value::String(v.to_string()));
                }
            }
            self.table
                .insert("account".into(), toml::Value::Table(account));
        }
        self.save().map_err(ReadError::Other)?;
        Ok((api_base, token))
    }

    /// Write the sign-in back as `fluree login` does: owner-only, replaced
    /// atomically.
    fn save(&self) -> Result<(), String> {
        let body = toml::to_string(&self.table).map_err(|e| e.to_string())?;
        let text = format!(
            "# Written by `fluree login`. It holds credentials for your fluree.ai account:\n\
             # keep it private, and sign out with `fluree logout`.\n\n{body}"
        );
        let dir = self.path.parent().ok_or("bad account path")?;
        let tmp = dir.join(format!(".{ACCOUNT_FILE}.{}", std::process::id()));
        write_private(&tmp, text.as_bytes()).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            format!("{}: {e}", self.path.display())
        })
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

/// Seconds until a JWT's `exp`, read without verifying it.
fn seconds_left(token: &str) -> Option<i64> {
    use base64::Engine;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_i64()?;
    Some(exp - now_secs())
}
