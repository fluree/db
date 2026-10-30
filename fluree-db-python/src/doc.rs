//! `fluree.doc.read`: one document read by an HTTP reader, as `fluree doc
//! read` does it. The parse, the cut into calls, the resends, the cache and
//! the reading's shape are `fluree_db_doc`'s, shared with the CLI; this
//! module only finds the credential and hands the result to Python as data.
//!
//! The credential, first found wins:
//! 1. `api_key` with `auth_header`: that key, in that header, as is;
//! 2. `api_key` alone: a fluree.ai API key (`flk_…`), traded for an hour's
//!    access token at `<service>/v1/auth/token`;
//! 3. this machine's `fluree login` (`account.toml` in the global config
//!    directory), refreshed when it is about to expire, as the CLI does;
//! 4. `FLUREE_AI_API_KEY`, as in 2.
//!
//! With no `url`, a fluree.ai credential reads at its account's
//! `<api base>/grounding`.

use crate::error::{fluree_error, invalid_request};
use fluree_db_doc::reader::{request, Credential, ReaderClient, Session};
use fluree_db_doc::{DocCache, PreparedDocument};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::{Path, PathBuf};

/// Where an API key is traded for a token when nothing names the service.
const DEFAULT_SERVICE: &str = "https://fluree.ai";
/// `fluree login`'s file in the global config directory.
const ACCOUNT_FILE: &str = "account.toml";
/// Refresh a sign-in with less than this left, as the CLI does.
const REFRESH_MARGIN_SECS: i64 = 60;

/// Read `path` with `model` and return the reading as plain data.
#[pyfunction]
#[pyo3(signature = (
    path, *, ontology, catalog, model, url=None, api_key=None, auth_header=None,
    cache=None, doc_iri=None, base_iri="urn:fluree:doc:".to_string(), cache_dir=None,
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
    doc_iri: Option<String>,
    base_iri: String,
    cache_dir: Option<PathBuf>,
) -> PyResult<Bound<'_, PyDict>> {
    let read = py.detach(|| -> Result<_, String> {
        let (url, credential) = resolve(url, api_key, auth_header)?;
        let ontology = load_rdf(&ontology)?;
        let mut records = fluree_graph_ir::Graph::new();
        for (file, class) in &catalog {
            for t in request::catalog_records_of(&load_rdf(file)?, class.as_deref()).iter() {
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
        .map_err(|e| e.to_string())?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let iri = doc_iri.unwrap_or_else(|| fluree_db_doc::ingest::document_iri(&base_iri, &name));
        let (meta, parsed) =
            fluree_db_doc::parse_file(&path, &name, &iri, doc_cache.as_ref(), None)
                .map_err(|e| e.to_string())?;
        let doc = PreparedDocument {
            meta,
            doc_iri: iri,
            parsed,
            chunks: Vec::new(),
        };
        let read = session
            .read(&doc, None, &|wait| tracing::info!("{wait}"))
            .map_err(|e| e.to_string())?;
        Ok((read, url))
    });
    let (read, url) = read.map_err(|e| {
        if e.contains(" 4") && (e.contains(": 401 ") || e.contains(": 403 ")) {
            crate::error::raise_status("PermissionDeniedError", e, 401)
        } else if e.starts_with("sign in") || e.contains("no ontology") {
            invalid_request(e)
        } else {
            fluree_error(e)
        }
    })?;
    let out = PyDict::new(py);
    out.set_item(
        "nodes",
        serde_json::to_string(&read.nodes).map_err(|e| fluree_error(e.to_string()))?,
    )?;
    out.set_item("mentions", read.mentions)?;
    out.set_item("entities", read.entities)?;
    out.set_item("relations", read.relations)?;
    out.set_item("linked", read.linked.into_iter().collect::<Vec<_>>())?;
    let usage = PyDict::new(py);
    usage.set_item("input_tokens", read.usage.input_tokens)?;
    usage.set_item("windows", read.usage.windows)?;
    usage.set_item("cache_read_tokens", read.usage.cache_read_tokens)?;
    usage.set_item("cache_write_tokens", read.usage.cache_write_tokens)?;
    usage.set_item("fuel", read.usage.fuel)?;
    usage.set_item("note", read.usage.note())?;
    out.set_item("usage", usage)?;
    out.set_item("calls", read.calls)?;
    out.set_item("cached", read.cached)?;
    out.set_item("release", read.release)?;
    out.set_item("ontology_notes", read.ontology_notes)?;
    out.set_item("url", url)?;
    Ok(out)
}

/// A reading's nodes (JSON) as Turtle, with the pipeline's prefixes.
#[pyfunction]
pub(crate) fn reading_turtle(nodes: &str) -> PyResult<String> {
    let nodes: Vec<serde_json::Value> =
        serde_json::from_str(nodes).map_err(|e| invalid_request(e.to_string()))?;
    fluree_db_doc::reader::reading::turtle(&nodes).map_err(|e| fluree_error(e.to_string()))
}

/// The pipeline's `@context`, for a reading written as JSON-LD.
#[pyfunction]
pub(crate) fn reading_context() -> String {
    fluree_db_doc::vocab::context().to_string()
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
) -> Result<(String, Option<Credential>), String> {
    if let Some(key) = api_key.clone().filter(|_| auth_header.is_some()) {
        let url = url.ok_or("a key in `auth_header` needs the reader's `url`")?;
        let header = auth_header.unwrap_or_default();
        let credential = if header.eq_ignore_ascii_case("authorization") {
            Credential::Bearer(key)
        } else {
            Credential::Header {
                name: header,
                value: key,
            }
        };
        return Ok((url, Some(credential)));
    }
    if let Some(key) = api_key {
        return exchange_key(url, &key);
    }
    if let Some((api_base, token)) = account_session()? {
        let url = url.unwrap_or_else(|| format!("{}/grounding", api_base.trim_end_matches('/')));
        return Ok((url, Some(Credential::Bearer(token))));
    }
    if let Some(key) = std::env::var("FLUREE_AI_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
    {
        return exchange_key(url, &key);
    }
    Err("sign in with `fluree login`, or set FLUREE_AI_API_KEY (or pass api_key=)".into())
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

/// A fluree.ai API key traded for an access token. The service is `url`'s
/// origin when given, else `FLUREE_AI_URL`, else fluree.ai.
fn exchange_key(url: Option<String>, key: &str) -> Result<(String, Option<Credential>), String> {
    let service = match &url {
        Some(u) => origin(u),
        None => std::env::var("FLUREE_AI_URL").unwrap_or_else(|_| DEFAULT_SERVICE.to_string()),
    };
    let service = service.trim_end_matches('/').to_string();
    let mut response = agent()
        .post(&format!("{service}/v1/auth/token"))
        .header("X-API-Key", key)
        .send_empty()
        .map_err(|e| format!("{service}/v1/auth/token: {e}"))?;
    let status = response.status().as_u16();
    let body: serde_json::Value = response.body_mut().read_json().unwrap_or_default();
    let token = body
        .get("accessToken")
        .and_then(|v| v.as_str())
        .filter(|_| status == 200)
        .ok_or_else(|| {
            format!(
                "{service}/v1/auth/token: {status} {}",
                body.get("message")
                    .or_else(|| body.get("error"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("the API key was refused")
            )
        })?;
    let url = url.unwrap_or_else(|| format!("{service}/v1/grounding"));
    Ok((url, Some(Credential::Bearer(token.to_string()))))
}

fn origin(url: &str) -> String {
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    match url[after_scheme..].find('/') {
        Some(i) => url[..after_scheme + i].to_string(),
        None => url.to_string(),
    }
}

/// `fluree login`'s sign-in, refreshed and saved when it is about to expire:
/// `(api base, access token)`, or `None` when this machine isn't signed in.
/// The file and its refresh are the CLI's (`fluree login`); this reads and
/// renews it the same way.
fn account_session() -> Result<Option<(String, String)>, String> {
    let Some(dirs) = fluree_db_api::server_defaults::FlureeDir::global() else {
        return Ok(None);
    };
    let path = dirs.config_dir().join(ACCOUNT_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let mut account: toml::Table =
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let field = |t: &toml::Table, k: &str| t.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let (Some(api_base), Some(token)) = (
        field(&account, "api_base_url"),
        field(&account, "access_token"),
    ) else {
        return Err(format!(
            "{} is unreadable; run `fluree logout` and sign in again",
            path.display()
        ));
    };
    if seconds_left(&token).is_some_and(|s| s > REFRESH_MARGIN_SECS) {
        return Ok(Some((api_base, token)));
    }
    let (Some(exchange), Some(refresh)) = (
        field(&account, "exchange_url"),
        field(&account, "refresh_token"),
    ) else {
        return Err(format!(
            "{} has no refresh credential; run `fluree login`",
            path.display()
        ));
    };
    let mut response = agent()
        .post(&exchange)
        .send_json(serde_json::json!({ "grant_type": "refresh_token", "refresh_token": refresh }))
        .map_err(|e| format!("refreshing the sign-in: {e}"))?;
    let status = response.status().as_u16();
    let body: serde_json::Value = response.body_mut().read_json().unwrap_or_default();
    let Some(token) = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|_| status == 200)
    else {
        let why = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .and_then(|v| v.as_str())
            .unwrap_or("refresh rejected");
        return Err(format!(
            "sign in again with `fluree login`: refreshing the sign-in failed ({status}: {why})"
        ));
    };
    account.insert(
        "access_token".into(),
        toml::Value::String(token.to_string()),
    );
    if let Some(rotated) = body.get("refresh_token").and_then(|v| v.as_str()) {
        account.insert(
            "refresh_token".into(),
            toml::Value::String(rotated.to_string()),
        );
    }
    save_account(&path, &account)?;
    Ok(Some((api_base, token.to_string())))
}

/// Write the sign-in back as `fluree login` does: owner-only, replaced
/// atomically.
fn save_account(path: &Path, account: &toml::Table) -> Result<(), String> {
    let body = toml::to_string(account).map_err(|e| e.to_string())?;
    let text = format!(
        "# Written by `fluree login`. It holds credentials for your fluree.ai account:\n\
         # keep it private, and sign out with `fluree logout`.\n\n{body}"
    );
    let dir = path.parent().ok_or("bad account path")?;
    let tmp = dir.join(format!(".{ACCOUNT_FILE}.{}", std::process::id()));
    write_private(&tmp, text.as_bytes()).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("{}: {e}", path.display())
    })
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some(exp - now)
}
