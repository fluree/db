//! The deep reader: crops the engine could not read, sent to a vision model.
//!
//! The engine decides *what* to read (`fluree_doc_pdf::escalate::crops_for`)
//! and how to arbitrate what comes back; this module only carries pixels to
//! an OpenAI-compatible chat endpoint and readings back. Any provider that
//! accepts `image_url` content parts works: OpenAI, Ollama, vLLM, LM Studio,
//! the Fluree AI gateway.
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
use crate::config::ModelEndpoint;
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
    client: reqwest::blocking::Client,
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
        let client = reqwest::blocking::Client::builder()
            .timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| DocError::Model(format!("http client: {e}")))?;
        let api_key = endpoint.resolved_api_key();
        Ok(Self {
            client,
            endpoint,
            api_key,
            cache,
            max_crops,
        })
    }

    pub fn model(&self) -> &str {
        &self.endpoint.model
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
            return Err(DocError::Model(format!(
                "document routes {} crop(s) to the vision model, past the cap of {} \
                 (`--max-crops`); raise it deliberately, or pass `--no-escalate` \
                 to keep the deterministic tier only",
                crops.len(),
                self.max_crops
            )));
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
        let body = serde_json::json!({
            "model": self.endpoint.model,
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": prompt },
                    { "type": "image_url", "image_url": { "url": data_url } }
                ]
            }],
            // Transcription, not composition: the same pixels should give the
            // same reading every time.
            "temperature": 0,
            "max_tokens": 8000
        });
        let url = self.endpoint.route("chat/completions");

        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(1 << attempt));
            }
            let mut req = self.client.post(&url).json(&body);
            if let Some(key) = &self.api_key {
                req = req.bearer_auth(key);
            }
            match req.send() {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().unwrap_or_default();
                    if status.is_success() {
                        let v: serde_json::Value = serde_json::from_str(&text)
                            .map_err(|e| DocError::Model(format!("malformed response: {e}")))?;
                        return Ok(completion_text(&v));
                    }
                    last = format!("{status}: {}", text.chars().take(300).collect::<String>());
                    if !(status.as_u16() == 429 || status.is_server_error()) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
