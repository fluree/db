//! The deep reader: crops the engine could not read, sent to a vision model.
//!
//! The engine decides *what* to read (`fluree_doc_pdf::escalate::crops_for`)
//! and how to arbitrate what comes back; this module only carries pixels to
//! an OpenAI-compatible endpoint and readings back. Two wire shapes: chat
//! completions with `image_url` parts (OpenAI, Ollama, vLLM, LM Studio) and
//! the Responses API with `input_image` parts (the Fluree AI gateway, which
//! routes the `doc-parse` intent to whichever vision provider the account
//! holds keys for).
//!
//! Escalation is all-or-nothing per document. A crop that fails is not
//! skipped: the crops that did answer would be spliced around a hole, which
//! reads as a plausible document with a paragraph missing. Better to fail
//! the parse and say which crop.
//!
//! Every reading is cached on the crop's pixels, so re-running after an
//! engine upgrade or an IRI change costs nothing for pages that did not
//! change.

use crate::cache::DocCache;
use crate::config::{ModelEndpoint, WireApi};
use crate::{DocError, Result};
use base64::Engine;
use fluree_doc_pdf::document::Analysis;
use fluree_doc_pdf::escalate::{self, Crop, Readings};
use fluree_doc_pdf::Document;
use hayro_syntax::Pdf;
use std::collections::HashMap;
use std::time::Duration;

/// Crops per document before the reader refuses. A VLM reads ~6–11 s per
/// crop; past this a document is a batch job, not an ingest.
pub const DEFAULT_MAX_CROPS: usize = 70;
const READ_TIMEOUT: Duration = Duration::from_secs(180);
const ATTEMPTS: u32 = 3;

pub struct VlmReader {
    agent: ureq::Agent,
    endpoint: ModelEndpoint,
    api_key: Option<String>,
    cache: Option<DocCache>,
    max_crops: usize,
}

impl std::fmt::Debug for VlmReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlmReader")
            .field("url", &self.endpoint.url)
            .field("model", &self.endpoint.model)
            .field("max_crops", &self.max_crops)
            .finish()
    }
}

impl VlmReader {
    pub fn new(endpoint: ModelEndpoint, cache: Option<DocCache>, max_crops: usize) -> Result<Self> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(READ_TIMEOUT))
            // Read error bodies ourselves: they say which crop and why.
            .http_status_as_error(false)
            .build()
            .into();
        let api_key = endpoint.resolved_api_key();
        Ok(Self {
            agent,
            endpoint,
            api_key,
            cache,
            max_crops,
        })
    }

    pub fn model(&self) -> &str {
        &self.endpoint.model
    }

    pub fn max_crops(&self) -> usize {
        self.max_crops
    }

    /// Read every crop `analysis` asks for. Empty when the document asks for
    /// nothing, which is the common case and must stay silent.
    pub fn read_pdf(&self, pdf: &Pdf, doc: &Document, analysis: &Analysis) -> Result<Readings> {
        let jobs = escalate::crops_for(doc, analysis, false);
        if jobs.is_empty() {
            return Ok(Readings::from_map(HashMap::new()));
        }
        let crops = escalate::render_crops(pdf, &jobs);
        if crops.is_empty() {
            return Ok(Readings::from_map(HashMap::new()));
        }
        if crops.len() > self.max_crops {
            return Err(DocError::CropCap {
                crops: crops.len(),
                cap: self.max_crops,
            });
        }
        tracing::info!(crops = crops.len(), model = %self.endpoint.model, "escalating to the vision model");

        let links = fluree_doc_pdf::link::extract(pdf);
        let mut readings = Readings::from_map(HashMap::new());
        let mut failures = Vec::new();
        for crop in &crops {
            let hints = escalate::links_in(&links, crop, doc);
            let prompt = escalate::prompt_for_crop(crop, &hints);
            match self.read(&crop.png, "image/png", &prompt) {
                Ok(Some(text)) => readings.insert(crop.name.clone(), text),
                // A crop with nothing printed on it is a real answer.
                Ok(None) => {}
                Err(e) => failures.push(format!("{}: {e}", crop.name)),
            }
        }
        if !failures.is_empty() {
            return Err(DocError::Model(format!(
                "{} of {} crop(s) could not be read — {}",
                failures.len(),
                crops.len(),
                failures.join("; ")
            )));
        }
        Ok(readings)
    }

    /// A whole raster image, transcribed with the engine's full-page prompt.
    pub fn read_image(&self, bytes: &[u8], mime: &str) -> Result<Option<String>> {
        let crop = Crop {
            name: "p0_full".into(),
            page: 0,
            bbox: None,
            png: Vec::new(),
        };
        let prompt = escalate::prompt_for_crop(&crop, &[]);
        self.read(bytes, mime, &prompt)
    }

    fn read(&self, image: &[u8], mime: &str, prompt: &str) -> Result<Option<String>> {
        let key = DocCache::reading_key(&self.endpoint.model, prompt, image);
        if let Some(hit) = self.cache.as_ref().and_then(|c| c.load_reading(&key)) {
            return Ok(hit);
        }
        let reading = self.request(image, mime, prompt)?;
        if let Some(cache) = &self.cache {
            cache.store_reading(&key, reading.as_deref())?;
        }
        Ok(reading)
    }

    fn request(&self, image: &[u8], mime: &str, prompt: &str) -> Result<Option<String>> {
        let data_url = format!(
            "data:{mime};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(image)
        );
        let (url, body) = match self.endpoint.wire_api() {
            WireApi::Chat => (
                self.endpoint.route("chat/completions"),
                serde_json::json!({
                    "model": self.endpoint.model,
                    "messages": [{
                        "role": "user",
                        "content": [
                            { "type": "text", "text": prompt },
                            { "type": "image_url", "image_url": { "url": data_url } }
                        ]
                    }],
                    // Transcription, not composition: the same pixels should
                    // give the same reading every time.
                    "temperature": 0,
                    "max_tokens": 8000
                }),
            ),
            WireApi::Responses => {
                let mut body = serde_json::json!({
                    "input": [{
                        "role": "user",
                        "content": [
                            { "type": "input_text", "text": prompt },
                            { "type": "input_image", "image_url": data_url }
                        ]
                    }],
                    "stream": false,
                    // The gateway routes this intent to the account's vision
                    // provider; a model named `auto` leaves the choice to it.
                    "fluree": { "intent": "doc-parse" }
                });
                if !self.endpoint.model.eq_ignore_ascii_case("auto") {
                    body["model"] = serde_json::json!(self.endpoint.model);
                }
                (self.endpoint.route("responses"), body)
            }
        };

        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(1 << attempt));
            }
            let mut req = self.agent.post(&url);
            if let Some(key) = &self.api_key {
                req = req.header("Authorization", &format!("Bearer {key}"));
            }
            match req.send_json(&body) {
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
}

/// The assistant text of a chat completion, tolerating the content-parts
/// shape some servers return. Empty means the model read nothing.
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

/// The output text of a Responses-API envelope. A reading that did not
/// `complete` is refused: a truncated transcription is otherwise
/// indistinguishable from a complete reading of a shorter page.
fn responses_text(v: &serde_json::Value) -> Result<Option<String>> {
    let status = v.get("status").and_then(serde_json::Value::as_str);
    if status != Some("completed") {
        return Err(DocError::Model(format!(
            "reading did not complete (status={})",
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
}
