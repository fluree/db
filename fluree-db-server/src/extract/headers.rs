//! Fluree-specific HTTP headers extractor

use axum::extract::FromRequestParts;
use axum::http::header::HeaderMap;
use axum::http::request::Parts;
use serde_json::Value as JsonValue;

use crate::error::{Result, ServerError};

/// Fluree-specific HTTP headers
///
/// These headers allow clients to specify query options, ledger selection,
/// identity, and policy without modifying the request body.
#[derive(Debug, Clone)]
pub struct FlureeHeaders {
    /// Raw HTTP headers (for telemetry/tracing)
    pub raw: HeaderMap,

    /// Ledger alias from header (lower precedence than path)
    pub ledger: Option<String>,

    /// Query identity (DID)
    pub identity: Option<String>,

    /// Policy document as JSON
    pub policy: Option<JsonValue>,

    /// Policy class IRIs. Multiple `fluree-policy-class` headers (or a single
    /// header with comma-separated values) accumulate into this Vec.
    pub policy_class: Vec<String>,

    /// Policy values as JSON
    pub policy_values: Option<JsonValue>,

    /// Default-allow flag — when true, permit access in the absence of matching
    /// policy rules. Delivered via the `fluree-default-allow` header.
    pub default_allow: bool,

    /// Enable all metadata tracking
    pub track_meta: bool,

    /// Track query fuel consumption
    pub track_fuel: bool,

    /// Track execution time
    pub track_time: bool,

    /// Maximum fuel limit (decimal). Internally converted to micro-fuel.
    pub max_fuel: Option<f64>,

    /// Content-Type header value
    pub content_type: Option<String>,

    /// Accept header value (for content negotiation)
    pub accept: Option<String>,
}

impl Default for FlureeHeaders {
    fn default() -> Self {
        Self {
            raw: HeaderMap::new(),
            ledger: None,
            identity: None,
            policy: None,
            policy_class: Vec::new(),
            policy_values: None,
            default_allow: false,
            track_meta: false,
            track_fuel: false,
            track_time: false,
            max_fuel: None,
            content_type: None,
            accept: None,
        }
    }
}

impl FlureeHeaders {
    /// Header names
    pub const LEDGER: &'static str = "fluree-ledger";
    pub const IDENTITY: &'static str = "fluree-identity";
    pub const POLICY: &'static str = "fluree-policy";
    pub const POLICY_CLASS: &'static str = "fluree-policy-class";
    pub const POLICY_VALUES: &'static str = "fluree-policy-values";
    pub const DEFAULT_ALLOW: &'static str = "fluree-default-allow";
    pub const TRACK_META: &'static str = "fluree-track-meta";
    pub const TRACK_FUEL: &'static str = "fluree-track-fuel";
    pub const TRACK_TIME: &'static str = "fluree-track-time";
    pub const MAX_FUEL: &'static str = "fluree-max-fuel";

    /// Parse headers from a HeaderMap
    pub fn from_headers(headers: &HeaderMap) -> Result<Self> {
        let mut fluree_headers = Self {
            raw: headers.clone(),
            ..Default::default()
        };

        // String headers
        if let Some(val) = get_header_str(headers, Self::LEDGER) {
            fluree_headers.ledger = Some(val.to_string());
        }

        if let Some(val) = get_header_str(headers, Self::IDENTITY) {
            fluree_headers.identity = Some(val.to_string());
        }

        // Accumulate every `fluree-policy-class` header value, splitting any
        // single value on commas to support both `H: a, b` and repeated `H`.
        for hv in headers.get_all(Self::POLICY_CLASS) {
            if let Ok(s) = hv.to_str() {
                for part in s.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        fluree_headers.policy_class.push(trimmed.to_string());
                    }
                }
            }
        }

        // JSON headers
        if let Some(val) = get_header_str(headers, Self::POLICY) {
            fluree_headers.policy = Some(serde_json::from_str(val).map_err(|e| {
                ServerError::invalid_header(format!("{} is not valid JSON: {}", Self::POLICY, e))
            })?);
        }

        if let Some(val) = get_header_str(headers, Self::POLICY_VALUES) {
            fluree_headers.policy_values = Some(serde_json::from_str(val).map_err(|e| {
                ServerError::invalid_header(format!(
                    "{} is not valid JSON: {}",
                    Self::POLICY_VALUES,
                    e
                ))
            })?);
        }

        // Boolean headers (presence or "true" value)
        fluree_headers.default_allow = is_header_truthy(headers, Self::DEFAULT_ALLOW);
        fluree_headers.track_meta = is_header_truthy(headers, Self::TRACK_META);
        fluree_headers.track_fuel =
            fluree_headers.track_meta || is_header_truthy(headers, Self::TRACK_FUEL);
        fluree_headers.track_time =
            fluree_headers.track_meta || is_header_truthy(headers, Self::TRACK_TIME);

        // Numeric headers (decimal allowed)
        if let Some(val) = get_header_str(headers, Self::MAX_FUEL) {
            fluree_headers.max_fuel = Some(val.parse().map_err(|_| {
                ServerError::invalid_header(format!("{} must be a number", Self::MAX_FUEL))
            })?);
        }

        // Content-Type
        if let Some(ct) = headers.get(axum::http::header::CONTENT_TYPE) {
            if let Ok(ct_str) = ct.to_str() {
                fluree_headers.content_type = Some(ct_str.to_string());
            }
        }

        // Accept
        if let Some(accept) = headers.get(axum::http::header::ACCEPT) {
            if let Ok(accept_str) = accept.to_str() {
                fluree_headers.accept = Some(accept_str.to_string());
            }
        }

        Ok(fluree_headers)
    }

    /// Check if tracking is enabled (any tracking header or max-fuel limit)
    pub fn has_tracking(&self) -> bool {
        self.track_meta || self.track_fuel || self.track_time || self.max_fuel.is_some()
    }

    /// Build `TrackingOptions` from header values.
    ///
    /// Used for SPARQL queries where tracking options can't come from a JSON
    /// body — they must come from HTTP headers instead.
    pub fn to_tracking_options(&self) -> fluree_db_core::tracking::TrackingOptions {
        fluree_db_core::tracking::TrackingOptions {
            track_time: self.track_meta || self.track_time,
            track_fuel: self.track_meta || self.track_fuel || self.max_fuel.is_some(),
            track_policy: self.track_meta,
            max_fuel: self.max_fuel.map(fluree_db_core::tracking::fuel_to_micro),
        }
    }

    /// Check if this is a SPARQL query based on Content-Type
    pub fn is_sparql_query(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/sparql-query"))
            .unwrap_or(false)
    }

    /// Check if this is a SPARQL update based on Content-Type
    pub fn is_sparql_update(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/sparql-update"))
            .unwrap_or(false)
    }

    /// Check if the client explicitly requests TSV output via Accept header.
    ///
    /// Matches `text/tab-separated-values` or `text/tsv` (case-insensitive).
    /// Does NOT match `*/*` — TSV must be explicitly requested.
    pub fn wants_tsv(&self) -> bool {
        self.accept
            .as_ref()
            .map(|a| {
                let lower = a.to_ascii_lowercase();
                lower.contains("text/tab-separated-values") || lower.contains("text/tsv")
            })
            .unwrap_or(false)
    }

    /// Check if the client explicitly requests CSV output via Accept header.
    ///
    /// Matches `text/csv` (case-insensitive).
    /// Does NOT match `*/*` — CSV must be explicitly requested.
    pub fn wants_csv(&self) -> bool {
        self.accept
            .as_ref()
            .map(|a| a.to_ascii_lowercase().contains("text/csv"))
            .unwrap_or(false)
    }

    /// Check if the client explicitly requests SPARQL Results XML output via Accept header.
    ///
    /// Matches `application/sparql-results+xml` (case-insensitive).
    /// Does NOT match `*/*` — XML must be explicitly requested.
    pub fn wants_sparql_results_xml(&self) -> bool {
        self.accept
            .as_ref()
            .map(|a| {
                a.to_ascii_lowercase()
                    .contains("application/sparql-results+xml")
            })
            .unwrap_or(false)
    }

    /// Check if the client explicitly requests AgentJson output via Accept header.
    ///
    /// Matches `application/vnd.fluree.agent+json` (case-insensitive).
    /// Does NOT match `*/*` — AgentJson must be explicitly requested.
    pub fn wants_agent_json(&self) -> bool {
        self.accept
            .as_ref()
            .map(|a| {
                a.to_ascii_lowercase()
                    .contains("application/vnd.fluree.agent+json")
            })
            .unwrap_or(false)
    }

    /// Get the max-bytes value from the `Fluree-Max-Bytes` header.
    ///
    /// Used by AgentJson format to set the byte budget for response truncation.
    pub fn max_bytes(&self) -> Option<usize> {
        get_header_str(&self.raw, "fluree-max-bytes").and_then(|v| v.parse().ok())
    }

    /// Check if the client explicitly requests RDF/XML output via Accept header.
    ///
    /// Matches `application/rdf+xml` (case-insensitive).
    /// Does NOT match `*/*` — RDF/XML must be explicitly requested.
    pub fn wants_rdf_xml(&self) -> bool {
        self.accept
            .as_ref()
            .map(|a| a.to_ascii_lowercase().contains("application/rdf+xml"))
            .unwrap_or(false)
    }

    /// Check if this is a JWT/JWS based on Content-Type
    pub fn is_jwt(&self) -> bool {
        self.content_type
            .as_ref()
            .map(|ct| ct.contains("application/jwt"))
            .unwrap_or(false)
    }

    /// Convert policy_values to a HashMap for credential API
    ///
    /// Returns None if no policy values are set, or an error if they're not a valid JSON object.
    pub fn policy_values_map(
        &self,
    ) -> Result<Option<std::collections::HashMap<String, JsonValue>>> {
        match &self.policy_values {
            None => Ok(None),
            Some(JsonValue::Object(obj)) => {
                let map: std::collections::HashMap<String, JsonValue> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Ok(Some(map))
            }
            Some(_) => Err(ServerError::invalid_header(
                "policy-values must be a JSON object",
            )),
        }
    }

    /// Inject header values into query opts JSON
    ///
    /// Header values serve as defaults - they don't override explicit body values.
    pub fn inject_into_opts(&self, opts: &mut serde_json::Map<String, JsonValue>) {
        // Only inject if not already present in opts
        if self.identity.is_some() && !opts.contains_key("identity") {
            opts.insert(
                "identity".to_string(),
                JsonValue::String(self.identity.clone().unwrap()),
            );
        }

        if self.policy.is_some() && !opts.contains_key("policy") {
            opts.insert("policy".to_string(), self.policy.clone().unwrap());
        }

        if !self.policy_class.is_empty() && !opts.contains_key("policy-class") {
            opts.insert(
                "policy-class".to_string(),
                JsonValue::Array(
                    self.policy_class
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                ),
            );
        }

        if self.policy_values.is_some() && !opts.contains_key("policy-values") {
            opts.insert(
                "policy-values".to_string(),
                self.policy_values.clone().unwrap(),
            );
        }

        if self.default_allow
            && !opts.contains_key("default-allow")
            && !opts.contains_key("default_allow")
            && !opts.contains_key("defaultAllow")
        {
            opts.insert("default-allow".to_string(), JsonValue::Bool(true));
        }

        if let Some(max_fuel) = self.max_fuel {
            if !opts.contains_key("max-fuel") {
                if let Some(n) = serde_json::Number::from_f64(max_fuel) {
                    opts.insert("max-fuel".to_string(), JsonValue::Number(n));
                }
            }
        }

        // Inject tracking options into meta if any tracking is enabled via headers
        // (only if meta is not already present in opts)
        if self.has_tracking() && !opts.contains_key("meta") {
            if self.track_meta {
                // track-meta enables all tracking
                opts.insert("meta".to_string(), JsonValue::Bool(true));
            } else {
                // Selective tracking via individual flags
                let mut meta = serde_json::Map::new();
                if self.track_time {
                    meta.insert("time".to_string(), JsonValue::Bool(true));
                }
                if self.track_fuel {
                    meta.insert("fuel".to_string(), JsonValue::Bool(true));
                }
                if !meta.is_empty() {
                    opts.insert("meta".to_string(), JsonValue::Object(meta));
                }
            }
        }
    }
}

/// Get a header value as a string slice
fn get_header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Check if a header is present and truthy
fn is_header_truthy(headers: &HeaderMap, name: &str) -> bool {
    match get_header_str(headers, name) {
        Some(v) => v.eq_ignore_ascii_case("true") || v == "1" || v.is_empty(),
        None => false,
    }
}

/// Axum extractor implementation
#[axum::async_trait]
impl<S> FromRequestParts<S> for FlureeHeaders
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        FlureeHeaders::from_headers(&parts.headers)
    }
}
