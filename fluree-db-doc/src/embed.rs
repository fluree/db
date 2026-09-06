//! Embeddings from an OpenAI-compatible `/embeddings` route.

use crate::config::ModelEndpoint;
use crate::{DocError, Result};
use std::time::Duration;

/// Inputs per request. Ollama and OpenAI both take far more, but a modest
/// batch keeps a single failure cheap to retry.
const BATCH: usize = 64;
const ATTEMPTS: u32 = 4;

#[derive(Debug, Clone)]
pub struct EmbeddingClient {
    client: reqwest::Client,
    endpoint: ModelEndpoint,
    api_key: Option<String>,
}

impl EmbeddingClient {
    pub fn new(endpoint: ModelEndpoint) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| DocError::Model(format!("http client: {e}")))?;
        let api_key = endpoint.resolved_api_key();
        Ok(Self {
            client,
            endpoint,
            api_key,
        })
    }

    pub fn model(&self) -> &str {
        &self.endpoint.model
    }

    /// One vector per input, in input order.
    pub async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(inputs.len());
        for batch in inputs.chunks(BATCH) {
            out.extend(self.embed_batch(batch).await?);
        }
        Ok(out)
    }

    async fn embed_batch(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut body = serde_json::json!({
            "model": self.endpoint.model,
            "input": inputs,
            "encoding_format": "float",
        });
        // Only sent when asked for: servers that do not support it reject
        // the key rather than ignore it.
        if let Some(d) = self.endpoint.dimensions {
            body["dimensions"] = serde_json::json!(d);
        }
        let url = self.endpoint.route("embeddings");

        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
            let mut req = self.client.post(&url).json(&body);
            if let Some(key) = &self.api_key {
                req = req.bearer_auth(key);
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    if status.is_success() {
                        return parse_embeddings(&text, inputs.len());
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

fn parse_embeddings(text: &str, expected: usize) -> Result<Vec<Vec<f32>>> {
    let v: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| DocError::Model(format!("malformed embeddings response: {e}")))?;
    let data = v
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| DocError::Model("embeddings response has no `data` array".into()))?;
    let mut rows: Vec<(usize, Vec<f32>)> = Vec::with_capacity(data.len());
    for (pos, item) in data.iter().enumerate() {
        let index = item
            .get("index")
            .and_then(serde_json::Value::as_u64)
            .map_or(pos, |i| i as usize);
        let vector: Vec<f32> = item
            .get("embedding")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| DocError::Model("embedding item has no `embedding` array".into()))?
            .iter()
            .map(|x| x.as_f64().unwrap_or(f64::NAN) as f32)
            .collect();
        if vector.iter().any(|x| !x.is_finite()) {
            return Err(DocError::Model(
                "embedding contains a non-finite value".into(),
            ));
        }
        rows.push((index, vector));
    }
    if rows.len() != expected {
        return Err(DocError::Model(format!(
            "asked for {expected} embeddings, got {}",
            rows.len()
        )));
    }
    rows.sort_by_key(|(i, _)| *i);
    let dims = rows.first().map_or(0, |(_, v)| v.len());
    if rows.iter().any(|(_, v)| v.len() != dims) {
        return Err(DocError::Model(
            "embeddings in one batch differ in length".into(),
        ));
    }
    Ok(rows.into_iter().map(|(_, v)| v).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_orders_by_index() {
        let text =
            r#"{"data":[{"index":1,"embedding":[3.0,4.0]},{"index":0,"embedding":[1.0,2.0]}]}"#;
        let v = parse_embeddings(text, 2).unwrap();
        assert_eq!(v, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    }

    #[test]
    fn rejects_count_mismatch() {
        let text = r#"{"data":[{"index":0,"embedding":[1.0]}]}"#;
        assert!(parse_embeddings(text, 2).is_err());
    }
}
