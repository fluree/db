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
use crate::config::ModelEndpoint;
use crate::llm::{LlmClient, Part, Request};
use crate::payload_fit::{fit_under_cap, MAX_CROP_BYTES};
use crate::{DocError, Result};
use fluree_doc_pdf::document::Analysis;
use fluree_doc_pdf::escalate::{self, Crop, Readings};
use fluree_doc_pdf::Document;
use hayro_syntax::Pdf;
use std::collections::HashMap;

/// Crops per document before the reader refuses. A VLM reads ~6–11 s per
/// crop; past this a document is a batch job, not an ingest.
pub const DEFAULT_MAX_CROPS: usize = 70;
const MAX_TOKENS: u32 = 8000;

pub struct VlmReader {
    client: LlmClient,
    cache: Option<DocCache>,
    max_crops: usize,
}

impl std::fmt::Debug for VlmReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlmReader")
            .field("url", &self.client.endpoint().url)
            .field("model", &self.client.model())
            .field("max_crops", &self.max_crops)
            .finish()
    }
}

impl VlmReader {
    pub fn new(endpoint: ModelEndpoint, cache: Option<DocCache>, max_crops: usize) -> Result<Self> {
        Ok(Self {
            client: LlmClient::new(endpoint),
            cache,
            max_crops,
        })
    }

    pub fn model(&self) -> &str {
        self.client.model()
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
        tracing::info!(crops = crops.len(), model = %self.client.model(), "escalating to the vision model");

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
        let key = DocCache::reading_key(self.client.model(), prompt, image);
        if let Some(hit) = self.cache.as_ref().and_then(|c| c.load_reading(&key)) {
            return Ok(hit);
        }
        // The cache is keyed on the pixels as rendered; what is sent may
        // be a smaller re-encoding of them.
        let fitted = fit_under_cap(image, mime, MAX_CROP_BYTES)?;
        let reading = self.client.complete(&Request {
            system: None,
            parts: vec![
                Part::Text(prompt),
                Part::Image {
                    mime: fitted.mime,
                    bytes: &fitted.bytes,
                },
            ],
            intent: "doc-parse",
            json: false,
            max_tokens: MAX_TOKENS,
        })?;
        if let Some(cache) = &self.cache {
            cache.store_reading(&key, reading.as_deref())?;
        }
        Ok(reading)
    }
}
