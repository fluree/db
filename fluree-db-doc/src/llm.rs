//! One client for every generation call: crop reading and extraction.
//!
//! Two wire shapes, chosen per endpoint. Chat completions is what OpenAI,
//! Ollama, vLLM and LM Studio serve; the Responses API is what the Fluree AI
//! gateway serves, where the `fluree.intent` field lets the account route
//! each kind of call to a different provider and a model named `auto`
//! leaves the choice to it.
//!
//! Blocking on purpose: crop reading runs inside the synchronous parse and
//! extraction runs in the same `spawn_blocking` task, and ureq carries no
//! runtime of its own to drop inside tokio's.

use crate::config::{ModelEndpoint, WireApi};
use crate::{DocError, Result};
use base64::Engine;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(180);
const ATTEMPTS: u32 = 3;

pub enum Part<'a> {
    Text(&'a str),
    Image { mime: &'a str, bytes: &'a [u8] },
}

pub struct Request<'a> {
    /// System prompt, when the call has one.
    pub system: Option<&'a str>,
    /// The user turn, in order.
    pub parts: Vec<Part<'a>>,
    /// What the gateway should route this as (`doc-parse`, `extraction`).
    pub intent: &'a str,
    /// Upper bound on the answer, sent as `max_completion_tokens`.
    ///
    /// On a reasoning model this budgets reasoning *and* output, so it can
    /// be spent before any visible text is produced.
    pub max_tokens: u32,
}

pub struct LlmClient {
    agent: ureq::Agent,
    endpoint: ModelEndpoint,
    api_key: Option<String>,
    /// Calls this client retried with an adjusted body after a 400. A run
    /// that silently downgraded every request has to be distinguishable
    /// from one that downgraded none, and `tracing::debug!` alone is not:
    /// the operator would have to have had debug logging on to find out.
    ///
    /// Atomic because one client is shared across the chunk workers.
    recoveries: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for LlmClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmClient")
            .field("url", &self.endpoint.url)
            .field("model", &self.endpoint.model)
            .finish()
    }
}

impl LlmClient {
    pub fn new(endpoint: ModelEndpoint) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(TIMEOUT))
            // Read error bodies ourselves: they say what went wrong.
            .http_status_as_error(false)
            .build()
            .into();
        let api_key = endpoint.resolved_api_key();
        Self {
            agent,
            endpoint,
            api_key,
            recoveries: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many calls this client had to adjust and resend, over its life.
    pub fn recoveries(&self) -> usize {
        self.recoveries.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn model(&self) -> &str {
        &self.endpoint.model
    }

    pub fn endpoint(&self) -> &ModelEndpoint {
        &self.endpoint
    }

    /// The assistant's text. `None` when the model answered with nothing,
    /// which for a crop is a real answer: nothing printed there.
    pub fn complete(&self, req: &Request<'_>) -> Result<Option<String>> {
        let (url, mut body) = match self.endpoint.wire_api() {
            WireApi::Chat => (self.endpoint.route("chat/completions"), self.chat_body(req)),
            WireApi::Responses => (self.endpoint.route("responses"), self.responses_body(req)),
        };

        let mut last = String::new();
        let mut recovered = false;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(1 << attempt));
            }
            let mut http = self.agent.post(&url);
            if let Some(key) = &self.api_key {
                http = http.header("Authorization", &format!("Bearer {key}"));
            }
            match http.send_json(&body) {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    let text = resp.body_mut().read_to_string().unwrap_or_default();
                    if (200..300).contains(&status) {
                        let v: serde_json::Value = serde_json::from_str(&text)
                            .map_err(|e| DocError::Model(format!("malformed response: {e}")))?;
                        return match self.endpoint.wire_api() {
                            WireApi::Chat => Ok(completion_text(&v)),
                            WireApi::Responses => responses_text(&v),
                        };
                    }
                    last = format!("{status}: {}", text.chars().take(300).collect::<String>());
                    // A 400 that names a field we sent is the endpoint
                    // telling us its dialect. Take the correction and go
                    // again, once: it is the only mechanism that survives a
                    // provider's published capability table being wrong,
                    // which is the situation we are actually in.
                    if status == 400 && !recovered {
                        if let Some(fixed) = recover(&mut body, &text) {
                            tracing::debug!("{url}: retrying with {fixed}");
                            self.recoveries
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            recovered = true;
                            continue;
                        }
                    }
                    if !(status == 429 || status >= 500) {
                        break;
                    }
                }
                Err(e) => last = e.to_string(),
            }
        }
        Err(DocError::Model(format!("{url}: {last}")))
    }

    fn chat_body(&self, req: &Request<'_>) -> serde_json::Value {
        let mut messages = Vec::new();
        if let Some(system) = req.system {
            messages.push(serde_json::json!({ "role": "system", "content": system }));
        }
        let content: Vec<serde_json::Value> = req
            .parts
            .iter()
            .map(|p| match p {
                Part::Text(t) => serde_json::json!({ "type": "text", "text": t }),
                Part::Image { mime, bytes } => serde_json::json!({
                    "type": "image_url",
                    "image_url": { "url": data_url(mime, bytes) }
                }),
            })
            .collect();
        messages.push(serde_json::json!({ "role": "user", "content": content }));
        // The minimum body that does the job, which is what `responses_body`
        // below has always sent. Every field beyond it is a field some
        // current model refuses:
        //
        // - `temperature: 0` was here for determinism, and 0 is already the
        //   default on every endpoint that accepts it — while gpt-5 and the
        //   o-series reject any value but their own and 400 on the field.
        // - `response_format: {"type":"json_object"}` only guarantees
        //   syntactic JSON, which the prompt already asks for and
        //   `parse_extraction` already tolerates fences and prose around.
        //   Anthropic's OpenAI-compatible route 400s on it today, against
        //   its own published table saying the field is ignored.
        //
        // The output budget stays, under the spelling Chat Completions
        // takes across its whole range: `max_tokens` is the deprecated one
        // and is what gpt-5 and the o-series reject. An endpoint too old to
        // know `max_completion_tokens` says so in a 400, and `recover` puts
        // the old spelling back for it.
        serde_json::json!({
            "model": self.endpoint.model,
            "messages": messages,
            "max_completion_tokens": req.max_tokens,
        })
    }

    fn responses_body(&self, req: &Request<'_>) -> serde_json::Value {
        let content: Vec<serde_json::Value> = req
            .parts
            .iter()
            .map(|p| match p {
                Part::Text(t) => serde_json::json!({ "type": "input_text", "text": t }),
                Part::Image { mime, bytes } => serde_json::json!({
                    "type": "input_image",
                    "image_url": data_url(mime, bytes)
                }),
            })
            .collect();
        // The system prompt travels as a system-role message with string
        // content, not as `instructions`: the Fluree AI gateway forwards the
        // former and drops the latter, and OpenAI accepts both.
        //
        // Keep it this way even after the gateway learns to honor
        // `instructions`. Both shapes are then served, while switching back
        // silently breaks every client pointed at a proxy that has not been
        // updated — and the failure is a prompt-less call that still returns
        // 200, with null subjects and objects in every relation.
        let mut input = Vec::new();
        if let Some(system) = req.system {
            input.push(serde_json::json!({ "role": "system", "content": system }));
        }
        input.push(serde_json::json!({ "role": "user", "content": content }));
        let mut body = serde_json::json!({
            "input": input,
            "stream": false,
            "fluree": { "intent": req.intent }
        });
        if !self.endpoint.model.eq_ignore_ascii_case("auto") {
            body["model"] = serde_json::json!(self.endpoint.model);
        }
        body
    }
}

/// Adjust a refused request body from what the refusal said, returning a
/// description of the change. `None` when nothing here matches, which
/// leaves the 400 terminal and reported verbatim.
///
/// One rule, because we send one field an endpoint can reasonably refuse.
/// Deliberately a short list of known refusals rather than a general
/// solver: a novel 400 from a novel model stays an error the operator
/// sees, not a silent mutation of their request.
fn recover(body: &mut serde_json::Value, error: &str) -> Option<&'static str> {
    // A server that predates `max_completion_tokens` names it. The
    // deprecated spelling is what gpt-5 and the o-series refuse, so we
    // cannot lead with it — but we can fall back to it for exactly the
    // endpoints that ask.
    let map = body.as_object_mut()?;
    if map.contains_key("max_completion_tokens") && error.contains("max_completion_tokens") {
        let budget = map.remove("max_completion_tokens")?;
        map.insert("max_tokens".into(), budget);
        return Some("max_tokens instead of max_completion_tokens");
    }
    None
}

fn data_url(mime: &str, bytes: &[u8]) -> String {
    format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

/// The assistant text of a chat completion, tolerating the content-parts
/// shape some servers return. Empty means the model said nothing.
fn completion_text(v: &serde_json::Value) -> Option<String> {
    let content = v.pointer("/choices/0/message/content")?;
    let text = match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => return None,
    };
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// The output text of a Responses-API envelope. An answer that did not
/// `complete` is refused: a truncated reading is otherwise
/// indistinguishable from a complete reading of a shorter page, and a
/// truncated JSON object fails to parse in a way that looks like a model
/// error.
fn responses_text(v: &serde_json::Value) -> Result<Option<String>> {
    let status = v.get("status").and_then(serde_json::Value::as_str);
    if status != Some("completed") {
        return Err(DocError::Model(format!(
            "response did not complete (status={})",
            status.unwrap_or("<missing>")
        )));
    }
    let message = v
        .get("output")
        .and_then(serde_json::Value::as_array)
        .and_then(|out| {
            out.iter()
                .find(|o| o.get("type").and_then(serde_json::Value::as_str) == Some("message"))
        })
        .ok_or_else(|| DocError::Model("response carries no output message".into()))?;
    let text: String = message
        .get("content")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|p| p.get("type").and_then(serde_json::Value::as_str) == Some("output_text"))
        .filter_map(|p| p.get("text").and_then(serde_json::Value::as_str))
        .collect();
    let text = text.trim();
    Ok((!text.is_empty()).then(|| text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(api: WireApi) -> LlmClient {
        LlmClient::new(ModelEndpoint {
            url: "http://h/v1".into(),
            model: "auto".into(),
            api_key: None,
            dimensions: None,
            api: Some(api),
        })
    }

    #[test]
    fn responses_text_requires_completion_and_joins_parts() {
        let ok = serde_json::json!({"status":"completed","output":[
            {"type":"reasoning","content":[]},
            {"type":"message","content":[
                {"type":"output_text","text":"# T\n"},{"type":"output_text","text":"body"}]}]});
        assert_eq!(responses_text(&ok).unwrap().as_deref(), Some("# T\nbody"));
        let empty = serde_json::json!({"status":"completed","output":[
            {"type":"message","content":[{"type":"output_text","text":"  "}]}]});
        assert_eq!(responses_text(&empty).unwrap(), None);
        let cut = serde_json::json!({"status":"incomplete","output":[]});
        assert!(responses_text(&cut).is_err());
    }

    #[test]
    fn completion_text_handles_string_and_parts() {
        let s = serde_json::json!({"choices":[{"message":{"content":"  hi  "}}]});
        assert_eq!(completion_text(&s).as_deref(), Some("hi"));
        let parts = serde_json::json!({"choices":[{"message":{"content":[
            {"type":"text","text":"a"},{"type":"text","text":"b"}]}}]});
        assert_eq!(completion_text(&parts).as_deref(), Some("ab"));
        let empty = serde_json::json!({"choices":[{"message":{"content":""}}]});
        assert_eq!(completion_text(&empty), None);
    }

    fn extraction_request() -> Request<'static> {
        Request {
            system: Some("sys"),
            parts: vec![Part::Text("user")],
            intent: "extraction",
            max_tokens: 8000,
        }
    }

    #[test]
    fn chat_body_carries_system_and_user_turns() {
        let body = client(WireApi::Chat).chat_body(&extraction_request());
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][1]["content"][0]["text"], "user");
        assert_eq!(body["model"], "auto");
    }

    /// The whole of the chat body, matched exactly.
    ///
    /// Exact rather than field-by-field on purpose. The bug class is a
    /// field that should not be on the wire, and every presence assertion
    /// — `assert_eq!(body["max_completion_tokens"], 8000)` — is equally
    /// true of a body that also carries `max_tokens` and `temperature`.
    /// An equality on the whole object cannot be satisfied by a body with
    /// anything extra in it, and it fails loudly when someone adds a field
    /// without thinking about which models refuse it.
    #[test]
    fn chat_body_sends_nothing_a_current_model_would_refuse() {
        let body = client(WireApi::Chat).chat_body(&extraction_request());
        assert_eq!(
            body,
            serde_json::json!({
                "model": "auto",
                "messages": [
                    { "role": "system", "content": "sys" },
                    { "role": "user", "content": [{ "type": "text", "text": "user" }] }
                ],
                "max_completion_tokens": 8000
            })
        );
    }

    /// The two bodies differ only in what the two APIs actually name.
    /// `responses_body` was already right; this is the property that says
    /// `chat_body` has stopped being the odd one out.
    #[test]
    fn neither_wire_shape_sends_temperature_or_response_format() {
        let req = extraction_request();
        for body in [
            client(WireApi::Chat).chat_body(&req),
            client(WireApi::Responses).responses_body(&req),
        ] {
            for field in ["temperature", "response_format", "max_tokens"] {
                assert!(body.get(field).is_none(), "{field} is on the wire: {body}");
            }
        }
    }

    /// A crop read goes through the same builder, so the same three fields
    /// are gone from every VLM call. Intended — a gpt-5-class vision model
    /// refuses them for the same reasons — and therefore pinned.
    #[test]
    fn crop_reading_shares_the_aligned_body() {
        let crop = Request {
            system: None,
            parts: vec![
                Part::Text("read this"),
                Part::Image {
                    mime: "image/png",
                    bytes: b"x",
                },
            ],
            intent: "doc-parse",
            max_tokens: 4096,
        };
        let body = client(WireApi::Chat).chat_body(&crop);
        assert_eq!(body["max_completion_tokens"], 4096);
        for field in ["temperature", "response_format", "max_tokens"] {
            assert!(body.get(field).is_none(), "{field} is on the wire: {body}");
        }
        assert!(body["messages"][0]["content"][1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }

    /// An endpoint too old for `max_completion_tokens` says so in the 400,
    /// and that is a better source of truth than any table we could ship:
    /// it is the endpoint itself, at the moment it refuses.
    #[test]
    fn a_400_naming_the_budget_field_switches_to_the_old_spelling() {
        let mut body = client(WireApi::Chat).chat_body(&extraction_request());
        let refusal = r#"{"error":{"message":"Unrecognized request argument supplied: max_completion_tokens","type":"invalid_request_error"}}"#;
        assert_eq!(
            recover(&mut body, refusal),
            Some("max_tokens instead of max_completion_tokens")
        );
        assert_eq!(body["max_tokens"], 8000);
        assert!(body.get("max_completion_tokens").is_none(), "{body}");

        // Once only: a second pass finds nothing left to rename, so the
        // caller cannot loop.
        assert_eq!(recover(&mut body, refusal), None);

        // A refusal about something else is left alone and stays terminal.
        // A silent mutation of a request the operator did not ask for is
        // worse than an error they can read.
        let mut other = client(WireApi::Chat).chat_body(&extraction_request());
        let unrelated = r#"{"error":{"message":"model `auto` does not exist"}}"#;
        assert_eq!(recover(&mut other, unrelated), None);
        assert_eq!(other["max_completion_tokens"], 8000);
    }

    #[test]
    fn responses_body_uses_system_message_intent_and_auto_model() {
        let req = Request {
            system: Some("sys"),
            parts: vec![
                Part::Text("u"),
                Part::Image {
                    mime: "image/png",
                    bytes: b"x",
                },
            ],
            intent: "doc-parse",
            max_tokens: 10,
        };
        let body = client(WireApi::Responses).responses_body(&req);
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["input"][0]["content"], "sys");
        assert_eq!(body["fluree"]["intent"], "doc-parse");
        assert!(
            body.get("model").is_none(),
            "auto leaves the model to the gateway"
        );
        assert_eq!(body["input"][1]["content"][1]["type"], "input_image");
        assert!(body["input"][1]["content"][1]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }
}
