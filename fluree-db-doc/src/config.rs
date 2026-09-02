//! Model endpoints: where embeddings, entity extraction and crop reading go.
//!
//! Three slots, all speaking the OpenAI wire shape so one client covers
//! OpenAI, Ollama, vLLM, LM Studio, Voyage's compatible route, and the
//! Fluree AI gateway. Each is optional and independent; `vlm` falls back to
//! `llm` because one multimodal model is often all a machine has.
//!
//! Read from the CLI config file's `[doc]` table:
//!
//! ```toml
//! [doc.embedding]
//! url = "http://localhost:11434/v1"
//! model = "nomic-embed-text"
//!
//! [doc.llm]
//! url = "https://api.openai.com/v1"
//! model = "gpt-5-mini"
//! api_key = "$OPENAI_API_KEY"
//! ```
//!
//! Environment variables override the file, slot by slot:
//! `FLUREE_DOC_{EMBEDDING,LLM,VLM}_{URL,MODEL,API_KEY,DIMENSIONS}`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ModelEndpoint {
    /// Base URL of an OpenAI-compatible API, up to and including `/v1`.
    pub url: String,
    /// Model name, passed through unchanged.
    pub model: String,
    /// Bearer token. A value starting with `$` names an environment variable
    /// holding the real key, so the config file need not contain it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Requested output dimensions, for embedding models that accept a
    /// `dimensions` parameter. Absent means the model's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<usize>,
}

impl ModelEndpoint {
    /// `{url}{path}` with exactly one slash between them.
    pub fn route(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// The bearer token to send, with `$VAR` indirection resolved.
    pub fn resolved_api_key(&self) -> Option<String> {
        let key = self.api_key.as_deref()?.trim();
        if key.is_empty() {
            return None;
        }
        if let Some(var) = key.strip_prefix('$') {
            return std::env::var(var).ok().filter(|v| !v.is_empty());
        }
        Some(key.to_string())
    }

    fn apply_env(&mut self, slot: &str) {
        let var = |field: &str| std::env::var(format!("FLUREE_DOC_{slot}_{field}")).ok();
        if let Some(v) = var("URL") {
            self.url = v;
        }
        if let Some(v) = var("MODEL") {
            self.model = v;
        }
        if let Some(v) = var("API_KEY") {
            self.api_key = Some(v);
        }
        if let Some(v) = var("DIMENSIONS").and_then(|d| d.parse().ok()) {
            self.dimensions = Some(v);
        }
    }

    fn is_usable(&self) -> bool {
        !self.url.trim().is_empty() && !self.model.trim().is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DocConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<ModelEndpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm: Option<ModelEndpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vlm: Option<ModelEndpoint>,
}

impl DocConfig {
    /// Layer `FLUREE_DOC_*` environment variables over the file values. A
    /// slot absent from the file is created when the environment names its
    /// URL and model.
    pub fn with_env(mut self) -> Self {
        for (slot, field) in [
            ("EMBEDDING", &mut self.embedding),
            ("LLM", &mut self.llm),
            ("VLM", &mut self.vlm),
        ] {
            let mut ep = field.take().unwrap_or_default();
            ep.apply_env(slot);
            *field = ep.is_usable().then_some(ep);
        }
        self
    }

    /// The reader for document crops: `vlm`, or `llm` when no separate
    /// vision endpoint is configured.
    pub fn crop_reader(&self) -> Option<&ModelEndpoint> {
        self.vlm.as_ref().or(self.llm.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_joins_with_single_slash() {
        let ep = ModelEndpoint {
            url: "http://localhost:11434/v1/".into(),
            model: "m".into(),
            api_key: None,
            dimensions: None,
        };
        assert_eq!(
            ep.route("/embeddings"),
            "http://localhost:11434/v1/embeddings"
        );
        assert_eq!(
            ep.route("embeddings"),
            "http://localhost:11434/v1/embeddings"
        );
    }

    #[test]
    fn dollar_key_reads_environment() {
        std::env::set_var("FLUREE_DOC_TEST_KEY", "sk-test");
        let ep = ModelEndpoint {
            url: "u".into(),
            model: "m".into(),
            api_key: Some("$FLUREE_DOC_TEST_KEY".into()),
            dimensions: None,
        };
        assert_eq!(ep.resolved_api_key().as_deref(), Some("sk-test"));
    }

    #[test]
    fn crop_reader_falls_back_to_llm() {
        let llm = ModelEndpoint {
            url: "u".into(),
            model: "m".into(),
            api_key: None,
            dimensions: None,
        };
        let cfg = DocConfig {
            embedding: None,
            llm: Some(llm.clone()),
            vlm: None,
        };
        assert_eq!(cfg.crop_reader(), Some(&llm));
    }
}
