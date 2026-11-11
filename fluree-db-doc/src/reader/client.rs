//! The call to an HTTP reader: one POST per request dataset.
//!
//! Blocking, like [`crate::LlmClient`] and for the same reason: readings run
//! inside a `spawn_blocking` task, and ureq carries no runtime to drop there.
//!
//! **Resends.** A read is resent only when the answer is a 503 or a 429 that
//! carries `Retry-After`: the endpoint saying it had no capacity (a model
//! still starting, a full queue) or that the caller is over a rate limit,
//! and when to come back. Nothing else is resent — not a timeout, not a
//! dropped connection, not a 503 or 429 without the header — because any of
//! those may follow a read that ran, and a read is billed.
//!
//! That a refusal with `Retry-After` was not billed is the reader's promise,
//! not HTTP's: RFC 9110 and RFC 6585 define 503 and 429 by their cause and
//! say nothing about work done. fluree.ai's reader keeps it: it bills only a
//! call it answers, so neither its own rate limit's 429 nor a 503 or 429 it
//! passes on from a model is billed. A reader of another kind should send
//! `Retry-After` with them only when the same holds.
//!
//! A resend waits the time the endpoint names, never less. Waiting is capped
//! at [`MAX_WAIT`] across a call's resends, and attempts at [`ATTEMPTS`]: a
//! wait that would pass the cap fails at once, naming the wait asked for.
//! One that passes it on its own is [`DocError::Unavailable`], since every
//! call is refused until it is over, and a caller with more to read stops.
//! Every wait is reported through the caller's `on_wait`, so a model
//! starting from cold shows as waiting rather than as a hang.

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

/// One answered read.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The reading, as JSON-LD.
    pub body: String,
    pub usage: Usage,
    /// The model and release that served, as the endpoint names them.
    pub model: Option<String>,
    pub release: Option<String>,
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
    /// How a wait is spent: the thread sleeps, except in tests.
    sleep: fn(Duration),
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
            sleep: std::thread::sleep,
        }
    }

    /// Spend waits with `sleep` instead of sleeping.
    #[cfg(test)]
    fn with_sleep(mut self, sleep: fn(Duration)) -> Self {
        self.sleep = sleep;
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Read one request dataset (TriG) with `model`. `on_wait` hears about
    /// each wait for capacity before it starts.
    pub fn read(
        &self,
        body: &str,
        model: &str,
        cache: Option<&str>,
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
                // An answer echoes the structure graph beside the reading;
                // ureq's 10 MiB default is too tight for a large document.
                .with_config()
                .limit(64 * 1024 * 1024)
                .read_to_string()
                .map_err(|e| DocError::Model(format!("{}: reading the answer: {e}", self.url)))?;
            if (200..300).contains(&status) {
                let header = |name: &str| {
                    headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string)
                };
                let ontology_notes = [
                    ("x-fluree-ontology-not-shown", "not shown to the model"),
                    ("x-fluree-ontology-adapted", "adapted"),
                ]
                .into_iter()
                .filter_map(|(h, what)| {
                    header(h)
                        .filter(|v| !v.is_empty())
                        .map(|v| format!("{what}: {v}"))
                })
                .collect();
                return Ok(Answer {
                    body: text,
                    usage: Usage::from_headers(&headers),
                    model: header("x-fluree-model"),
                    release: header("x-fluree-release"),
                    ontology_notes,
                    resends: attempt - 1,
                });
            }
            let message = error_message(&text);
            let retry_after = (status == 503 || status == 429)
                .then(|| headers.get("retry-after"))
                .flatten()
                .and_then(|v| v.to_str().ok())
                .and_then(retry_after_secs);
            let refused = |message: String| DocError::Endpoint {
                model: model.to_string(),
                status,
                message,
            };
            let Some(secs) = retry_after else {
                return Err(refused(message));
            };
            // The wait the endpoint names, never cut short: a resend before
            // it is sure to be refused again.
            let wait = Duration::from_secs(secs.max(1));
            let past_the_cap = || {
                format!(
                    "{message}; the reader asks to wait {}s, and a call waits at most {}s in all \
                     ({}s so far), so it is not resent",
                    wait.as_secs(),
                    MAX_WAIT.as_secs(),
                    waited.as_secs()
                )
            };
            // A wait past the cap on its own holds for every call until it
            // is over (an account over a quota), not for this one alone.
            if wait > MAX_WAIT {
                return Err(DocError::Unavailable {
                    model: model.to_string(),
                    status,
                    wait,
                    message: past_the_cap(),
                });
            }
            if attempt >= ATTEMPTS {
                return Err(refused(format!(
                    "still refused after {ATTEMPTS} attempts and {}s of waiting: {message}",
                    waited.as_secs()
                )));
            }
            if waited + wait > MAX_WAIT {
                return Err(refused(past_the_cap()));
            }
            on_wait(&format!(
                "{model}: {message}; trying again in {}s (attempt {} of {ATTEMPTS})",
                wait.as_secs(),
                attempt + 1
            ));
            (self.sleep)(wait);
            waited += wait;
            attempt += 1;
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
                "X-Fluree-Model: m",
                "X-Fluree-Ontology-Not-Shown: rdfs:domain=2",
            ],
            "[]",
        )]);
        let client = ReaderClient::new(&endpoint(&url));
        let answer = client
            .read("<urn:g> {}", "m", Some("catalog"), &|_| {})
            .unwrap();
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
        assert!(!head.contains("authorization"));
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
            .read("x", "m", None, &|w| {
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

    #[test]
    fn a_429_with_retry_after_is_resent() {
        let (url, seen) = stub(vec![
            http(
                "429 Too Many Requests",
                &["Retry-After: 1"],
                r#"{"error":{"message":"Too many","status":429,"error":"rate_limited"}}"#,
            ),
            http("200 OK", &["X-Fluree-Serialized-Tokens: 5"], "[]"),
        ]);
        let waits = Mutex::new(Vec::new());
        let answer = ReaderClient::new(&endpoint(&url))
            .read("x", "m", None, &|w| {
                waits.lock().unwrap().push(w.to_string());
            })
            .unwrap();
        assert_eq!(answer.resends, 1);
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert!(waits.into_inner().unwrap()[0].contains("attempt 2 of 4"));
    }

    /// Only a 503 or 429 with `Retry-After` is resent: without it the read
    /// may have run, and the resend rule covers no other status.
    #[test]
    fn a_503_or_429_without_retry_after_and_any_other_error_are_not_resent() {
        for response in [
            http(
                "503 Service Unavailable",
                &[],
                r#"{"error":"Extraction service unavailable"}"#,
            ),
            http("429 Too Many Requests", &[], "slow down"),
            http("504 Gateway Timeout", &["Retry-After: 1"], "timed out"),
            http("502 Bad Gateway", &["Retry-After: 1"], "upstream"),
        ] {
            let (url, seen) = stub(vec![response]);
            let err = ReaderClient::new(&endpoint(&url))
                .read("x", "m", None, &|_| panic!("no wait expected"))
                .unwrap_err();
            assert_eq!(seen.lock().unwrap().len(), 1, "{err}");
        }
    }

    /// A wait longer than a call may wait in all fails at once, saying the
    /// wait asked for; it is never resent early. It is told apart
    /// ([`DocError::Unavailable`], with the wait), since every call meets
    /// it until the wait is over.
    #[test]
    fn a_wait_past_the_cap_is_not_resent() {
        for (status, code) in [
            ("429 Too Many Requests", 429),
            ("503 Service Unavailable", 503),
        ] {
            let (url, seen) = stub(vec![http(status, &["Retry-After: 301"], "slow down")]);
            let err = ReaderClient::new(&endpoint(&url))
                .with_sleep(|_| panic!("no wait expected"))
                .read("x", "m", None, &|_| panic!("no wait expected"))
                .unwrap_err();
            assert_eq!(seen.lock().unwrap().len(), 1, "{err}");
            let text = err.to_string();
            assert!(text.contains("asks to wait 301s"), "{text}");
            assert!(text.contains("at most 300s in all (0s so far)"), "{text}");
            match err {
                DocError::Unavailable { status, wait, .. } => {
                    assert_eq!((status, wait), (code, Duration::from_secs(301)));
                }
                other => panic!("{other:?}"),
            }
        }
    }

    /// A 429 asking for two minutes is resent after two minutes, not after
    /// one: the wait is the endpoint's, uncut.
    #[test]
    fn a_long_retry_after_is_waited_in_full() {
        let (url, seen) = stub(vec![
            http("429 Too Many Requests", &["Retry-After: 120"], "slow down"),
            http("200 OK", &[], "[]"),
        ]);
        let waits = Mutex::new(Vec::new());
        let answer = ReaderClient::new(&endpoint(&url))
            .with_sleep(|_| {})
            .read("x", "m", None, &|w| {
                waits.lock().unwrap().push(w.to_string());
            })
            .unwrap();
        assert_eq!(answer.resends, 1);
        assert_eq!(seen.lock().unwrap().len(), 2);
        let waits = waits.into_inner().unwrap();
        assert!(waits[0].contains("trying again in 120s"), "{waits:?}");
    }

    /// Waits add up against [`MAX_WAIT`]: two of two minutes are spent, and
    /// the third, which would pass five minutes, fails at once.
    #[test]
    fn waiting_stops_at_the_total_cap() {
        let busy = || http("503 Service Unavailable", &["Retry-After: 120"], "busy");
        let (url, seen) = stub(vec![busy(), busy(), busy()]);
        let waits = Mutex::new(0);
        let err = ReaderClient::new(&endpoint(&url))
            .with_sleep(|_| {})
            .read("x", "m", None, &|_| *waits.lock().unwrap() += 1)
            .unwrap_err();
        assert_eq!(seen.lock().unwrap().len(), 3);
        assert_eq!(*waits.lock().unwrap(), 2);
        let text = err.to_string();
        assert!(text.contains("asks to wait 120s"), "{text}");
        assert!(text.contains("(240s so far)"), "{text}");
        // This call's waits ran out, but the next call's start afresh: the
        // endpoint isn't refusing every call.
        assert!(
            matches!(err, DocError::Endpoint { status: 503, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn resends_stop_at_the_attempt_cap() {
        let busy = || http("503 Service Unavailable", &["Retry-After: 1"], "busy");
        let (url, seen) = stub(vec![busy(), busy(), busy(), busy()]);
        let err = ReaderClient::new(&endpoint(&url))
            .with_sleep(|_| {})
            .read("x", "m", None, &|_| {})
            .unwrap_err();
        assert_eq!(seen.lock().unwrap().len(), ATTEMPTS as usize);
        assert!(err.to_string().contains("503"), "{err}");
        assert!(
            err.to_string()
                .contains("still refused after 4 attempts and 3s of waiting"),
            "{err}"
        );
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
        match ReaderClient::new(&endpoint(&url)).read("x", "m", None, &|_| {}) {
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
            .read("x", "m", None, &|_| {})
            .unwrap();
        assert!(seen.lock().unwrap()[0]
            .to_ascii_lowercase()
            .contains("authorization: bearer k-123"));
    }
}
