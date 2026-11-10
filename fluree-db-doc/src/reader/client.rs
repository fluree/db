//! The call to an HTTP reader: one POST per request dataset.
//!
//! Blocking, like [`crate::LlmClient`] and for the same reason: readings run
//! inside a `spawn_blocking` task, and ureq carries no runtime to drop there.
//!
//! **Controls.** What a call asks for travels in headers, never in the body
//! as well (a reader refuses a request that sends both): the answer's shape
//! (`X-Fluree-Output: fluree-transaction`, a Fluree DB transaction), what
//! comes with the facts (`X-Fluree-Include`), and, for a long text, the
//! windows it is read in (`X-Fluree-Window`). See [`Controls`].
//!
//! **Resends.** A read is resent only when the answer is a 503 that carries
//! `Retry-After`: the endpoint saying it had no capacity (a model still
//! starting, a full queue) and when to come back. Nothing else is resent —
//! not a timeout, not a dropped connection, not a 503 without the header,
//! not a 504 (the gateway stopped waiting, and the read may still have
//! run) — because any of those may follow a read that ran, and a read is
//! billed. Attempts and total waiting are capped, and every wait is reported
//! through the caller's `on_wait`, so a model starting from cold shows as
//! waiting rather than as a hang.
//!
//! Every answer, an error's included, carries the endpoint's ID for the call
//! (`X-Fluree-Request-ID`), which an error's message quotes.

use crate::config::ReaderEndpoint;
use crate::{DocError, Result};
use std::time::Duration;

/// Longest a single call may take, queueing included: a model started from
/// cold can hold a request for minutes before it reads it.
const TIMEOUT: Duration = Duration::from_secs(330);
/// Calls per request, the first included.
pub const ATTEMPTS: u32 = 4;
/// Waiting across all resends of one request.
pub const MAX_WAIT: Duration = Duration::from_secs(300);
/// The longest single `Retry-After` honoured.
const MAX_RETRY_AFTER: u64 = 60;

/// Where the credential goes.
#[derive(Clone)]
pub enum Credential {
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// A header of the endpoint's own carrying the key as is.
    Header { name: String, value: String },
}

impl Credential {
    /// The slot's key in the header it names.
    pub fn from_endpoint(endpoint: &ReaderEndpoint) -> Option<Self> {
        let key = endpoint.resolved_api_key()?;
        Some(match endpoint.auth_header.as_deref().map(str::trim) {
            None | Some("") => Credential::Bearer(key),
            Some(h) if h.eq_ignore_ascii_case("authorization") => Credential::Bearer(key),
            Some(h) => Credential::Header {
                name: h.to_string(),
                value: key,
            },
        })
    }
}

/// What a read was billed, as the endpoint reports it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    /// Input tokens over every window (`X-Fluree-Serialized-Tokens`).
    pub input_tokens: u64,
    pub windows: usize,
    /// Prompt tokens served from the endpoint's cache.
    pub cache_read_tokens: u64,
    /// Prompt tokens the endpoint wrote to its cache.
    pub cache_write_tokens: u64,
    /// Fuel charged, when the endpoint meters in fuel.
    pub fuel: Option<f64>,
}

impl Usage {
    pub fn add(&mut self, other: &Usage) {
        self.input_tokens += other.input_tokens;
        self.windows += other.windows;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        self.fuel = match (self.fuel, other.fuel) {
            (None, None) => None,
            (a, b) => Some(a.unwrap_or(0.0) + b.unwrap_or(0.0)),
        };
    }

    /// `1,032 tokens, cache 900 read / 0 written, 3.40 fuel`, for a
    /// document's line or a run's total; empty when nothing was billed.
    pub fn note(&self) -> String {
        let mut parts = Vec::new();
        if self.input_tokens > 0 {
            parts.push(format!("{} tokens", thousands(self.input_tokens)));
        }
        if self.cache_read_tokens > 0 || self.cache_write_tokens > 0 {
            parts.push(format!(
                "cache {} read / {} written",
                thousands(self.cache_read_tokens),
                thousands(self.cache_write_tokens)
            ));
        }
        if let Some(fuel) = self.fuel {
            parts.push(format!("{fuel:.2} fuel"));
        }
        parts.join(", ")
    }

    fn from_headers(headers: &ureq::http::HeaderMap) -> Self {
        let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
        let counts = |name: &str| -> Vec<u64> {
            get(name)
                .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
                .unwrap_or_default()
        };
        let tokens = counts("x-fluree-serialized-tokens");
        Self {
            input_tokens: tokens.iter().sum(),
            windows: tokens.len(),
            cache_read_tokens: counts("x-fluree-cache-read-tokens").iter().sum(),
            cache_write_tokens: counts("x-fluree-cache-write-tokens").iter().sum(),
            fuel: get("x-fluree-fuel-consumed").and_then(|v| v.trim().parse().ok()),
        }
    }
}

/// What a call asks for besides the reading of its body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Controls {
    pub include: Include,
    /// Character ranges of the request's text, each read as one window
    /// (`X-Fluree-Window`); empty reads the text as one.
    pub windows: Vec<(usize, usize)>,
}

impl Controls {
    /// The headers a call sends for these controls.
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        let mut out = vec![
            ("X-Fluree-Output", "fluree-transaction".to_string()),
            ("X-Fluree-Include", self.include.header().to_string()),
        ];
        if !self.windows.is_empty() {
            let windows: Vec<String> = self
                .windows
                .iter()
                .map(|(b, e)| format!("{b}-{e}"))
                .collect();
            out.push(("X-Fluree-Window", windows.join(",")));
        }
        out
    }
}

/// What comes back with the facts, which always do: the claims behind them,
/// the mentions (each occurrence in the text, and the record it links to),
/// both, or neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Include {
    pub claims: bool,
    pub mentions: bool,
}

impl Default for Include {
    /// Claims and mentions: a reading anchored to the text.
    fn default() -> Self {
        Self {
            claims: true,
            mentions: true,
        }
    }
}

impl Include {
    /// `claims`, `mentions`, `claims,mentions` (either order) or `none`.
    pub fn parse(value: &str) -> Result<Self> {
        let invalid = || {
            DocError::Config(format!(
                "include {value:?}: expected claims, mentions, claims,mentions or none"
            ))
        };
        let parts: Vec<String> = value
            .split(',')
            .map(|p| p.trim().to_ascii_lowercase())
            .filter(|p| !p.is_empty())
            .collect();
        let mut include = Self {
            claims: false,
            mentions: false,
        };
        for part in &parts {
            match part.as_str() {
                "claims" => include.claims = true,
                "mentions" => include.mentions = true,
                "none" if parts.len() == 1 => {}
                _ => return Err(invalid()),
            }
        }
        if parts.is_empty() {
            return Err(invalid());
        }
        Ok(include)
    }

    /// The `X-Fluree-Include` value.
    pub fn header(&self) -> &'static str {
        match (self.claims, self.mentions) {
            (true, true) => "claims,mentions",
            (true, false) => "claims",
            (false, true) => "mentions",
            (false, false) => "none",
        }
    }
}

/// One answered read.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The reading: a Fluree DB transaction, as JSON-LD.
    pub body: String,
    pub usage: Usage,
    /// The release that served (`X-Fluree-Release`), the run's IRI
    /// (`X-Fluree-Run`) and the endpoint's ID for the call
    /// (`X-Fluree-Request-ID`), as the answer names them.
    pub release: Option<String>,
    pub run: Option<String>,
    pub request_id: Option<String>,
    /// What the endpoint set aside or adapted in the ontology, as it reports
    /// it (`X-Fluree-Ontology-Not-Shown`, `-Adapted`).
    pub ontology_notes: Vec<String>,
    /// Waits for capacity before this answer.
    pub resends: u32,
}

pub struct ReaderClient {
    agent: ureq::Agent,
    url: String,
    credential: Option<Credential>,
}

impl std::fmt::Debug for ReaderClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderClient")
            .field("url", &self.url)
            .finish()
    }
}

impl ReaderClient {
    /// A client for `endpoint`, with its configured key.
    pub fn new(endpoint: &ReaderEndpoint) -> Self {
        Self::with_credential(&endpoint.url, Credential::from_endpoint(endpoint))
    }

    /// A client for `url` with a credential from elsewhere (an account
    /// sign-in, say).
    pub fn with_credential(url: &str, credential: Option<Credential>) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(TIMEOUT))
            .http_status_as_error(false)
            // The key travels in a header a redirect would carry along.
            .max_redirects(0)
            .user_agent(concat!("fluree-doc/", env!("CARGO_PKG_VERSION")))
            .build()
            .into();
        Self {
            agent,
            url: url.to_string(),
            credential,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Read one request dataset (TriG) with `model`, asking for `controls`.
    /// `on_wait` hears about each wait for capacity before it starts.
    pub fn read(
        &self,
        body: &str,
        model: &str,
        cache: Option<&str>,
        controls: &Controls,
        on_wait: &dyn Fn(&str),
    ) -> Result<Answer> {
        let mut waited = Duration::ZERO;
        let mut attempt = 1;
        loop {
            let mut req = self
                .agent
                .post(&self.url)
                .header("Content-Type", "application/trig")
                .header("Accept", "application/ld+json")
                .header("X-Fluree-Model", model);
            if let Some(c) = cache {
                req = req.header("X-Fluree-Cache", c);
            }
            for (name, value) in controls.headers() {
                req = req.header(name, value.as_str());
            }
            req = match &self.credential {
                Some(Credential::Bearer(t)) => req.header("Authorization", &format!("Bearer {t}")),
                Some(Credential::Header { name, value }) => {
                    req.header(name.as_str(), value.as_str())
                }
                None => req,
            };
            let mut response = req
                .send(body)
                .map_err(|e| DocError::Model(format!("{}: {e}", self.url)))?;
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let text = response
                .body_mut()
                // A long document's mentions alone can pass ureq's 10 MiB
                // default.
                .with_config()
                .limit(64 * 1024 * 1024)
                .read_to_string()
                .map_err(|e| DocError::Model(format!("{}: reading the answer: {e}", self.url)))?;
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            };
            let request_id = header("x-fluree-request-id");
            if (200..300).contains(&status) {
                let ontology_notes = [
                    ("x-fluree-ontology-not-shown", "not shown to the model"),
                    ("x-fluree-ontology-adapted", "adapted"),
                ]
                .into_iter()
                .filter_map(|(h, what)| header(h).map(|v| format!("{what}: {v}")))
                .collect();
                return Ok(Answer {
                    body: text,
                    usage: Usage::from_headers(&headers),
                    release: header("x-fluree-release"),
                    run: header("x-fluree-run"),
                    request_id,
                    ontology_notes,
                    resends: attempt - 1,
                });
            }
            let mut message = error_message(&text);
            if status == 504 {
                message.push_str(
                    "; the reader may still have read it, and billed it, so it is not resent",
                );
            }
            if let Some(id) = &request_id {
                message.push_str(&format!(" (request {id})"));
            }
            let retry_after = (status == 503)
                .then(|| headers.get("retry-after"))
                .flatten()
                .and_then(|v| v.to_str().ok())
                .and_then(retry_after_secs);
            match retry_after {
                Some(secs) if attempt < ATTEMPTS => {
                    let wait = Duration::from_secs(secs.clamp(1, MAX_RETRY_AFTER));
                    if waited + wait > MAX_WAIT {
                        return Err(DocError::Endpoint {
                            model: model.to_string(),
                            status,
                            message: format!(
                                "no capacity after {}s of waiting: {message}",
                                waited.as_secs()
                            ),
                        });
                    }
                    on_wait(&format!(
                        "{model}: {message}; trying again in {}s (attempt {} of {ATTEMPTS})",
                        wait.as_secs(),
                        attempt + 1
                    ));
                    std::thread::sleep(wait);
                    waited += wait;
                    attempt += 1;
                }
                _ => {
                    return Err(DocError::Endpoint {
                        model: model.to_string(),
                        status,
                        message,
                    });
                }
            }
        }
    }
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

/// `Retry-After` as seconds to wait: delay-seconds, or an HTTP-date
/// (RFC 9110) counted from now (at least a second).
fn retry_after_secs(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs);
    }
    let at = httpdate::parse_http_date(value).ok()?;
    let left = at
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    Some(left.as_secs().max(1))
}

/// What an error body says: its `error` / `message` / `details`, else the
/// body itself, shortened.
fn error_message(body: &str) -> String {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        // Some endpoints nest the envelope: `{"error": {"error", "message"}}`.
        let v = match v.get("error") {
            Some(inner) if inner.is_object() => inner.clone(),
            _ => v,
        };
        let field = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
        let parts: Vec<String> = ["error", "message", "details"]
            .iter()
            .filter_map(|k| field(k))
            .collect();
        if !parts.is_empty() {
            return parts.join(": ");
        }
    }
    let trimmed = body.trim();
    if trimmed.chars().count() > 300 {
        format!("{}…", trimmed.chars().take(300).collect::<String>())
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// A one-thread HTTP stub answering each connection with the next
    /// canned response, recording the request heads it saw.
    fn stub(responses: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/extract", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for response in responses {
                let (mut conn, _) = listener.accept().unwrap();
                let mut buf = vec![0u8; 65536];
                let mut req = Vec::new();
                loop {
                    let n = conn.read(&mut buf).unwrap();
                    req.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&req).to_string();
                    if let Some(head_end) = text.find("\r\n\r\n") {
                        let len = text[..head_end]
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if req.len() >= head_end + 4 + len {
                            log.lock().unwrap().push(text[..head_end].to_string());
                            break;
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
                conn.write_all(response.as_bytes()).unwrap();
            }
        });
        (url, seen)
    }

    fn http(status: &str, headers: &[&str], body: &str) -> String {
        let mut out = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for h in headers {
            out.push_str(h);
            out.push_str("\r\n");
        }
        out.push_str("\r\n");
        out.push_str(body);
        out
    }

    fn endpoint(url: &str) -> ReaderEndpoint {
        ReaderEndpoint {
            url: url.to_string(),
            api_key: Some("k-123".into()),
            auth_header: Some("X-Api-Key".into()),
            models: vec!["m".into()],
            cache: None,
        }
    }

    #[test]
    fn a_read_sends_the_headers_and_reports_usage() {
        let (url, seen) = stub(vec![http(
            "200 OK",
            &[
                "X-Fluree-Serialized-Tokens: 1000, 32",
                "X-Fluree-Cache-Read-Tokens: 0,900",
                "X-Fluree-Fuel-Consumed: 3.4",
                "X-Fluree-Release: example-reader-1.0",
                "X-Fluree-Run: urn:fluree:run:01EXAMPLE",
                "X-Fluree-Request-ID: req-1",
                "X-Fluree-Ontology-Not-Shown: rdfs:domain=2",
            ],
            "[]",
        )]);
        let client = ReaderClient::new(&endpoint(&url));
        let controls = Controls {
            include: Include::parse("mentions, claims").unwrap(),
            windows: vec![(0, 1200), (1100, 2400)],
        };
        let answer = client
            .read("<urn:g> {}", "m", Some("catalog"), &controls, &|_| {})
            .unwrap();
        assert_eq!(answer.release.as_deref(), Some("example-reader-1.0"));
        assert_eq!(answer.run.as_deref(), Some("urn:fluree:run:01EXAMPLE"));
        assert_eq!(answer.request_id.as_deref(), Some("req-1"));
        assert_eq!(answer.usage.input_tokens, 1032);
        assert_eq!(answer.usage.windows, 2);
        assert_eq!(answer.usage.cache_read_tokens, 900);
        assert_eq!(answer.usage.fuel, Some(3.4));
        assert_eq!(
            answer.ontology_notes,
            vec!["not shown to the model: rdfs:domain=2".to_string()]
        );
        let head = seen.lock().unwrap()[0].to_ascii_lowercase();
        assert!(head.contains("x-api-key: k-123"));
        assert!(head.contains("x-fluree-model: m"));
        assert!(head.contains("x-fluree-cache: catalog"));
        assert!(head.contains("content-type: application/trig"));
        assert!(head.contains("accept: application/ld+json"));
        assert!(head.contains("x-fluree-output: fluree-transaction"));
        assert!(head.contains("x-fluree-include: claims,mentions"));
        assert!(head.contains("x-fluree-window: 0-1200,1100-2400"));
        assert!(!head.contains("authorization"));
    }

    #[test]
    fn include_reads_each_part_or_none() {
        let header = |v: &str| Include::parse(v).map(|i| i.header());
        assert_eq!(header("claims").unwrap(), "claims");
        assert_eq!(header("MENTIONS").unwrap(), "mentions");
        assert_eq!(header("mentions,claims").unwrap(), "claims,mentions");
        assert_eq!(header("none").unwrap(), "none");
        for bad in ["", "none,claims", "facts"] {
            assert!(header(bad).is_err(), "{bad:?}");
        }
        // No windows, no window header.
        let headers = Controls::default().headers();
        assert!(headers.iter().all(|(h, _)| *h != "X-Fluree-Window"));
    }

    /// A 504 may follow a read that ran: not resent, and the message says
    /// so with the endpoint's ID for the call.
    #[test]
    fn a_504_is_not_resent_and_names_the_request() {
        let (url, seen) = stub(vec![http(
            "504 Gateway Timeout",
            &["X-Fluree-Request-ID: req-504"],
            r#"{"error":"no answer within the job timeout"}"#,
        )]);
        let err = ReaderClient::new(&endpoint(&url))
            .read("x", "m", None, &Controls::default(), &|_| {
                panic!("no wait expected")
            })
            .unwrap_err();
        assert_eq!(seen.lock().unwrap().len(), 1);
        let text = err.to_string();
        assert!(text.contains("504"), "{text}");
        assert!(text.contains("not resent"), "{text}");
        assert!(text.contains("(request req-504)"), "{text}");
    }

    #[test]
    fn a_503_with_retry_after_is_resent_and_reported() {
        let (url, seen) = stub(vec![
            http(
                "503 Service Unavailable",
                &["Retry-After: 1"],
                r#"{"error":"starting"}"#,
            ),
            http("200 OK", &["X-Fluree-Serialized-Tokens: 5"], "[]"),
        ]);
        let waits = Mutex::new(Vec::new());
        let answer = ReaderClient::new(&endpoint(&url))
            .read("x", "m", None, &Controls::default(), &|w| {
                waits.lock().unwrap().push(w.to_string());
            })
            .unwrap();
        assert_eq!(answer.resends, 1);
        assert_eq!(seen.lock().unwrap().len(), 2);
        let waits = waits.into_inner().unwrap();
        assert_eq!(waits.len(), 1);
        assert!(
            waits[0].contains("starting") && waits[0].contains("attempt 2 of 4"),
            "{waits:?}"
        );
    }

    /// Only a 503 with `Retry-After` is resent: without it the read may have
    /// run, and the resend rule covers no other status.
    #[test]
    fn a_503_without_retry_after_and_any_other_error_are_not_resent() {
        for response in [
            http(
                "503 Service Unavailable",
                &[],
                r#"{"error":"Extraction service unavailable"}"#,
            ),
            http("504 Gateway Timeout", &["Retry-After: 1"], "timed out"),
            http("429 Too Many Requests", &["Retry-After: 1"], "slow down"),
        ] {
            let (url, seen) = stub(vec![response]);
            let err = ReaderClient::new(&endpoint(&url))
                .read("x", "m", None, &Controls::default(), &|_| {
                    panic!("no wait expected")
                })
                .unwrap_err();
            assert_eq!(seen.lock().unwrap().len(), 1, "{err}");
        }
    }

    #[test]
    fn resends_stop_at_the_attempt_cap() {
        let busy = || http("503 Service Unavailable", &["Retry-After: 1"], "busy");
        let (url, seen) = stub(vec![busy(), busy(), busy(), busy()]);
        let err = ReaderClient::new(&endpoint(&url))
            .read("x", "m", None, &Controls::default(), &|_| {})
            .unwrap_err();
        assert_eq!(seen.lock().unwrap().len(), ATTEMPTS as usize);
        assert!(err.to_string().contains("503"), "{err}");
    }

    #[test]
    fn usage_reads_like_a_receipt() {
        let usage = Usage {
            input_tokens: 1_032,
            windows: 1,
            ..Default::default()
        };
        assert_eq!(usage.note(), "1,032 tokens");
        let usage = Usage {
            input_tokens: 1_234_567,
            windows: 3,
            cache_read_tokens: 900,
            cache_write_tokens: 12,
            fuel: Some(3.4),
        };
        assert_eq!(
            usage.note(),
            "1,234,567 tokens, cache 900 read / 12 written, 3.40 fuel"
        );
        assert_eq!(Usage::default().note(), "");
    }

    #[test]
    fn an_error_body_says_what_went_wrong_flat_or_nested() {
        assert_eq!(
            error_message(r#"{"error":"busy","message":"no capacity"}"#),
            "busy: no capacity"
        );
        assert_eq!(
            error_message(
                r#"{"error":{"message":"model is temporarily unavailable","status":503,"error":"unavailable"}}"#
            ),
            "unavailable: model is temporarily unavailable"
        );
        assert_eq!(error_message("plain text"), "plain text");
    }

    #[test]
    fn retry_after_takes_seconds_or_an_http_date() {
        assert_eq!(retry_after_secs(" 15 "), Some(15));
        let soon = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(30));
        let secs = retry_after_secs(&soon).unwrap();
        assert!((28..=30).contains(&secs), "{secs}");
        assert_eq!(retry_after_secs("Thu, 01 Jan 1970 00:00:00 GMT"), Some(1));
        assert_eq!(retry_after_secs("soon"), None);
    }

    #[test]
    fn an_error_status_is_kept_typed() {
        let (url, _) = stub(vec![http("401 Unauthorized", &[], r#"{"error":"no"}"#)]);
        match ReaderClient::new(&endpoint(&url)).read("x", "m", None, &Controls::default(), &|_| {})
        {
            Err(DocError::Endpoint { status: 401, .. }) => {}
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn bearer_is_the_default_credential() {
        let (url, seen) = stub(vec![http("200 OK", &[], "[]")]);
        let mut ep = endpoint(&url);
        ep.auth_header = None;
        ReaderClient::new(&ep)
            .read("x", "m", None, &Controls::default(), &|_| {})
            .unwrap();
        assert!(seen.lock().unwrap()[0]
            .to_ascii_lowercase()
            .contains("authorization: bearer k-123"));
    }
}
