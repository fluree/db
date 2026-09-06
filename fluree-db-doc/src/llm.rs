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
    /// OpenAI refuses the mode otherwise.
    pub json: bool,
    pub max_tokens: u32,
}

pub struct LlmClient {
    agent: ureq::Agent,
    endpoint: ModelEndpoint,
    api_key: Option<String>,
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
        }
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
        let (url, body) = match self.endpoint.wire_api() {
            WireApi::Chat => (self.endpoint.route("chat/completions"), self.chat_body(req)),
            WireApi::Responses => (self.endpoint.route("responses"), self.responses_body(req)),
        };

        let mut last = String::new();
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
        let mut body = serde_json::json!({
            "model": self.endpoint.model,
            "messages": messages,
            // Transcription and extraction, not composition: the same input
            // should give the same answer every time.
            "temperature": 0,
            "max_tokens": req.max_tokens
        });
        if req.json {
            body["response_format"] = serde_json::json!({ "type": "json_object" });
        }
        body
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

    #[test]
    fn chat_body_carries_system_and_json_mode() {
        let req = Request {
            system: Some("sys"),
            parts: vec![Part::Text("user")],
            intent: "extraction",
            json: true,
            max_tokens: 10,
        };
        let body = client(WireApi::Chat).chat_body(&req);
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][1]["content"][0]["text"], "user");
        assert_eq!(body["response_format"]["type"], "json_object");
        assert_eq!(body["model"], "auto");
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
