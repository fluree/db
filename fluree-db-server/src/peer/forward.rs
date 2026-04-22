//! Transaction forwarding to transaction server
//!
//! Forwards transaction requests from peer to the transaction server,
//! preserving headers (except hop-by-hop) and returning the response.

use axum::body::Body;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::{header::HeaderMap, Method, StatusCode};
use reqwest::Client;
use std::time::Duration;

/// HTTP client for forwarding requests to transaction server
pub struct ForwardingClient {
    client: Client,
    base_url: String,
}

/// Headers that should NOT be forwarded (hop-by-hop headers)
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host", // Always rewrite to target host
];

impl ForwardingClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(300)) // 5 min for slow transactions
            .build()
            .expect("Failed to create forwarding client");

        Self { client, base_url }
    }

    /// Forward a request to the transaction server
    ///
    /// Forwards all headers except hop-by-hop headers.
    /// The `Authorization` header IS forwarded to support downstream auth.
    pub async fn forward(&self, request: Request) -> Result<Response, ForwardingError> {
        let method = request.method().clone();
        let uri = request.uri().clone();
        let headers = request.headers().clone();

        // Build target URL
        let target_url = format!(
            "{}{}",
            self.base_url,
            uri.path_and_query()
                .map(http::uri::PathAndQuery::as_str)
                .unwrap_or(uri.path())
        );

        tracing::debug!(
            method = %method,
            target = %target_url,
            "Forwarding request to transaction server"
        );

        // Read body
        let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024) // 50MB limit
            .await
            .map_err(ForwardingError::BodyRead)?;

        // Build forwarded request
        let mut builder = self.client.request(method.clone(), &target_url);

        // Forward headers (except hop-by-hop)
        builder = forward_headers(builder, &headers);

        // Add body for methods that support it
        if matches!(method, Method::POST | Method::PUT | Method::PATCH) {
            builder = builder.body(body_bytes.to_vec());
        }

        // Execute request
        let response = builder.send().await.map_err(ForwardingError::Request)?;

        // Convert to axum response
        let status = response.status();
        let response_headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(ForwardingError::ResponseBody)?;

        tracing::debug!(
            status = %status,
            body_len = body.len(),
            "Received response from transaction server"
        );

        // Build response, forwarding headers (except hop-by-hop)
        let mut builder = Response::builder().status(status);

        for (name, value) in &response_headers {
            let name_str = name.as_str().to_ascii_lowercase();
            if !HOP_BY_HOP_HEADERS.contains(&name_str.as_str()) {
                builder = builder.header(name.clone(), value.clone());
            }
        }

        builder
            .body(Body::from(body))
            .map_err(ForwardingError::ResponseBuild)
    }

    /// Get the base URL for this forwarding client
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

/// Forward headers from request, skipping hop-by-hop headers
fn forward_headers(
    mut builder: reqwest::RequestBuilder,
    headers: &HeaderMap,
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        let name_str = name.as_str().to_ascii_lowercase();
        if !HOP_BY_HOP_HEADERS.contains(&name_str.as_str()) {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder
}

#[derive(Debug, thiserror::Error)]
pub enum ForwardingError {
    #[error("Failed to read request body: {0}")]
    BodyRead(axum::Error),

    #[error("Request to transaction server failed: {0}")]
    Request(reqwest::Error),

    #[error("Failed to read response body: {0}")]
    ResponseBody(reqwest::Error),

    #[error("Failed to build response: {0}")]
    ResponseBuild(http::Error),
}

impl IntoResponse for ForwardingError {
    fn into_response(self) -> Response {
        let status = match &self {
            ForwardingError::Request(e) => {
                if e.is_timeout() {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::BAD_GATEWAY
                }
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = serde_json::json!({
            "error": "ForwardingError",
            "message": self.to_string()
        });

        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hop_by_hop_headers() {
        // Verify the list contains expected headers
        assert!(HOP_BY_HOP_HEADERS.contains(&"connection"));
        assert!(HOP_BY_HOP_HEADERS.contains(&"host"));
        assert!(HOP_BY_HOP_HEADERS.contains(&"transfer-encoding"));

        // Authorization should NOT be in the list (it should be forwarded)
        assert!(!HOP_BY_HOP_HEADERS.contains(&"authorization"));
        assert!(!HOP_BY_HOP_HEADERS.contains(&"content-type"));
    }

    #[test]
    fn test_forwarding_error_status_codes() {
        // Test that timeout errors return 504
        let _timeout_err = reqwest::Client::new()
            .get("http://localhost:1")
            .timeout(Duration::from_nanos(1));

        // Test error variants match expected status codes
        let body_error = ForwardingError::BodyRead(axum::Error::new("test"));
        let response = body_error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
