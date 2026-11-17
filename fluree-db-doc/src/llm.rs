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
    /// Ask a chat endpoint for a JSON object. The prompt must also say so:
    /// OpenAI refuses the mode otherwise. Dropped automatically for an
    /// endpoint that refuses the field — see [`LlmClient::recover`].
    pub json: bool,
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
    /// Fields this endpoint has refused, learned from its own 400s and
    /// remembered for the rest of the run.
    ///
    /// Without this the correction would be per call — and `complete` is
    /// called once per chunk, so a corpus against an endpoint that refuses
    /// one field would pay a wasted round trip, a backoff and a retry on
    /// every chunk of every document. Remembering makes it once per field
    /// per run.
    refused: std::sync::Mutex<std::collections::BTreeSet<&'static str>>,
}

/// The optional fields a chat body carries, each of which some endpoint
/// refuses. Sent by default and withdrawn on refusal, rather than withheld
/// by default: withholding is a capability judgement made once, for every
/// provider, by whoever wrote this list — which is the thing a 400 lets us
/// avoid.
const ADJUSTABLE: [&str; 3] = ["temperature", "response_format", "max_completion_tokens"];

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
            refused: std::sync::Mutex::new(Default::default()),
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

        let mut last;
        // Retries and dialect corrections are counted separately on
        // purpose. A correction is not a transient failure: it should not
        // back off, and it should not spend one of the three attempts that
        // exist to ride out a 429 or a 503. It terminates on its own —
        // every correction removes or renames a field, and `recover` only
        // matches a field still in the body — so the cap is a guard against
        // a future rule that could undo another, not the thing that ends
        // the loop.
        let mut spent = 0u32;
        let mut corrections = 0usize;
        loop {
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
                    if status == 400 && corrections < ADJUSTABLE.len() {
                        if let Some(fixed) = self.recover(&mut body, &text) {
                            tracing::debug!("{url}: retrying with {fixed}");
                            self.recoveries
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            corrections += 1;
                            continue;
                        }
                    }
                    if !(status == 429 || status >= 500) {
                        break;
                    }
                }
                Err(e) => last = e.to_string(),
            }
            spent += 1;
            if spent >= ATTEMPTS {
                break;
            }
            std::thread::sleep(Duration::from_secs(1 << spent));
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
        // Sent optimistically, withdrawn on refusal. `responses_body` below
        // omits all three because the Responses API names them differently
        // or not at all — not because a chat call is better off without
        // them, which was the reading an earlier draft of this took.
        //
        // - `temperature: 0` is what makes extraction close to greedy, and
        //   this pipeline is built on repeatability: the extraction cache
        //   is content-keyed and every document node carries a
        //   `doc:extractionFingerprint`. It is not a guarantee — there is
        //   no `seed` here and no provider promises determinism — but the
        //   OpenAI chat default is 1, so dropping it for everyone would
        //   make a cold run over one corpus produce a different graph each
        //   time while the fingerprint claimed otherwise.
        // - `response_format` genuinely improves reliability where it is
        //   supported. `clean_json` plus the retry is a weaker fallback,
        //   not an equivalent.
        // - `max_completion_tokens` is the spelling Chat Completions takes
        //   across its current range; `max_tokens` is the deprecated one.
        let refused = self.refused.lock().expect("refused fields");
        let mut body = serde_json::json!({
            "model": self.endpoint.model,
            "messages": messages,
        });
        if !refused.contains("temperature") {
            body["temperature"] = serde_json::json!(0);
        }
        if req.json && !refused.contains("response_format") {
            body["response_format"] = serde_json::json!({ "type": "json_object" });
        }
        let budget = if refused.contains("max_completion_tokens") {
            "max_tokens"
        } else {
            "max_completion_tokens"
        };
        body[budget] = serde_json::json!(req.max_tokens);
        body
    }

    /// Adjust a refused request body from what the refusal said, and
    /// remember the refusal, returning a description of the change. `None`
    /// when nothing here matches, which leaves the 400 terminal and
    /// reported verbatim.
    ///
    /// A 400 naming a field is the endpoint describing its own dialect, and
    /// that is better evidence than any table we could ship: it comes from
    /// the deployment, at the moment it refuses, and it stays right when a
    /// vendor's published capability table does not. Anthropic's
    /// OpenAI-compatibility page documents `response_format` as ignored
    /// while the deployed route 400s on it, which is exactly that case.
    ///
    /// Deliberately a short list of known refusals rather than a general
    /// solver: an unrecognised 400 stays an error the operator reads, not a
    /// silent mutation of their request.
    fn recover(&self, body: &mut serde_json::Value, error: &str) -> Option<&'static str> {
        let map = body.as_object_mut()?;
        // Which field the endpoint named. `error.param` is where OpenAI
        // puts it; the substring is the fallback for everyone else.
        let param = serde_json::from_str::<serde_json::Value>(error)
            .ok()
            .and_then(|v| {
                v.pointer("/error/param")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        let named = |field: &str| param.as_deref() == Some(field) || error.contains(field);

        // On record, verbatim, from the reports these rules exist for:
        //   "Unsupported parameter: 'max_tokens' is not supported with this
        //    model. Use 'max_completion_tokens' instead."
        //   "response_format.type: Input should be 'json_schema'"
        // The gpt-5 temperature refusal is *not* on record here — that rule
        // is inferred from the documented behaviour (only the default is
        // accepted) and from the shape of the other two. It matches
        // `error.param` first for that reason.
        let field = *ADJUSTABLE
            .iter()
            .find(|f| map.contains_key(**f) && named(f))?;
        let value = map.remove(field)?;
        let description = match field {
            // A rename, not a withdrawal: the budget still applies, under
            // the spelling this endpoint knows.
            "max_completion_tokens" => {
                map.insert("max_tokens".into(), value);
                "max_tokens instead of max_completion_tokens"
            }
            "temperature" => "no temperature",
            _ => "no response_format",
        };
        self.refused.lock().expect("refused fields").insert(field);
        Some(description)
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
            json: true,
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
    /// Exact rather than field-by-field on purpose, and it cuts both ways
    /// now: a presence assertion cannot see a field that should not be
    /// there, and an absence assertion cannot see one that stopped being
    /// sent. An equality on the whole object fails loudly either way.
    #[test]
    fn chat_body_sends_every_field_until_the_endpoint_refuses_one() {
        let body = client(WireApi::Chat).chat_body(&extraction_request());
        assert_eq!(
            body,
            serde_json::json!({
                "model": "auto",
                "messages": [
                    { "role": "system", "content": "sys" },
                    { "role": "user", "content": [{ "type": "text", "text": "user" }] }
                ],
                "temperature": 0,
                "response_format": { "type": "json_object" },
                "max_completion_tokens": 8000
            })
        );
    }

    /// Crop reading shares the builder, so it shares the corrections — a
    /// gpt-5-class vision model refuses the same fields for the same
    /// reasons. It asks for no JSON, and that is the one difference.
    #[test]
    fn crop_reading_shares_the_body_but_asks_for_no_json() {
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
            json: false,
            max_tokens: 4096,
        };
        let body = client(WireApi::Chat).chat_body(&crop);
        assert_eq!(body["max_completion_tokens"], 4096);
        assert_eq!(body["temperature"], 0);
        assert!(
            body.get("response_format").is_none(),
            "a crop never asked for JSON: {body}"
        );
        assert!(body["messages"][0]["content"][1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }

    /// The Responses route is unaffected either way: it never carried any
    /// of the three, and nothing here can put them on it.
    #[test]
    fn the_responses_body_carries_none_of_the_adjustable_fields() {
        let body = client(WireApi::Responses).responses_body(&extraction_request());
        for field in ADJUSTABLE.iter().chain(["max_tokens"].iter()) {
            assert!(
                body.get(field).is_none(),
                "{field} reached /responses: {body}"
            );
        }
    }

    /// A refusal is learned once and remembered for the rest of the run.
    ///
    /// This is the property that decides the design rather than decorates
    /// it. `complete` runs once per chunk, so without it an endpoint that
    /// refuses one field costs a wasted round trip, a backoff and one of
    /// three retries on **every chunk of every document** — about 10k
    /// wasted calls and over an hour of pure sleep on a 10k-chunk corpus.
    #[test]
    fn a_refusal_is_remembered_so_the_next_call_does_not_repeat_it() {
        let client = client(WireApi::Chat);
        let refusal = r#"{"error":{"param":"temperature","message":"Unsupported value: 'temperature' does not support 0 with this model. Only the default (1) is supported."}}"#;

        let mut first = client.chat_body(&extraction_request());
        assert_eq!(first["temperature"], 0, "sent optimistically to begin with");
        assert_eq!(client.recover(&mut first, refusal), Some("no temperature"));
        assert!(first.get("temperature").is_none(), "{first}");

        // The next chunk's body, built fresh, has already learned it.
        let second = client.chat_body(&extraction_request());
        assert!(
            second.get("temperature").is_none(),
            "the refusal was not remembered, so every chunk would pay for it: {second}"
        );
        // And only that field was withdrawn.
        assert_eq!(second["max_completion_tokens"], 8000);
        assert_eq!(second["response_format"]["type"], "json_object");
    }

    /// Each of the three fields, refused in the endpoint's own words.
    #[test]
    fn each_adjustable_field_is_withdrawn_when_the_endpoint_names_it() {
        // Verbatim from the reports these rules exist for.
        let budget = r#"{"error":{"message":"Unsupported parameter: 'max_completion_tokens' is not supported with this model. Use 'max_tokens' instead."}}"#;
        let format =
            r#"{"error":{"message":"response_format.type: Input should be 'json_schema'"}}"#;
        // Inferred, not on record — hence the `error.param` match first.
        let temperature = r#"{"error":{"param":"temperature","message":"Unsupported value"}}"#;

        for (refusal, gone, description) in [
            (
                budget,
                "max_completion_tokens",
                "max_tokens instead of max_completion_tokens",
            ),
            (format, "response_format", "no response_format"),
            (temperature, "temperature", "no temperature"),
        ] {
            let client = client(WireApi::Chat);
            let mut body = client.chat_body(&extraction_request());
            assert_eq!(client.recover(&mut body, refusal), Some(description));
            assert!(body.get(gone).is_none(), "{gone} survived: {body}");
            if gone == "max_completion_tokens" {
                // Renamed, not withdrawn: the budget still applies.
                assert_eq!(body["max_tokens"], 8000);
            }
        }
    }

    /// A refusal naming something we do not adjust stays terminal. A silent
    /// mutation of a request the operator did not ask for is worse than an
    /// error they can read.
    #[test]
    fn an_unrecognised_refusal_changes_nothing() {
        let client = client(WireApi::Chat);
        let mut body = client.chat_body(&extraction_request());
        let before = body.clone();
        let unrelated = r#"{"error":{"param":"model","message":"model `auto` does not exist"}}"#;
        assert_eq!(client.recover(&mut body, unrelated), None);
        assert_eq!(body, before);
        assert_eq!(client.recoveries(), 0);
    }

    /// Correction terminates without needing a counter to stop it: every
    /// rule removes or renames the field it matched, and `recover` only
    /// matches a field still in the body. The cap in `complete` guards a
    /// future rule that could undo another, not this.
    #[test]
    fn a_field_is_only_ever_withdrawn_once() {
        let client = client(WireApi::Chat);
        let mut body = client.chat_body(&extraction_request());
        let refusal = r#"{"error":{"message":"Unrecognized request argument supplied: max_completion_tokens"}}"#;
        assert!(client.recover(&mut body, refusal).is_some());
        assert_eq!(
            client.recover(&mut body, refusal),
            None,
            "a second pass found something to change, so a caller could loop: {body}"
        );
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
            json: false,
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
