use crate::error::{CliError, CliResult};
use std::path::Path;

/// Format for data mutations (insert).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFormat {
    Turtle,
    JsonLd,
}

/// Format for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryFormat {
    Sparql,
    JsonLd,
}

/// Detect data format from file extension and content.
///
/// Priority: explicit `--format` flag > file extension > content sniffing.
pub fn detect_data_format(
    path: Option<&Path>,
    content: &str,
    explicit: Option<&str>,
) -> CliResult<DataFormat> {
    // Explicit flag
    if let Some(fmt) = explicit {
        return match fmt.to_lowercase().as_str() {
            "turtle" | "ttl" => Ok(DataFormat::Turtle),
            "jsonld" | "json-ld" | "json" => Ok(DataFormat::JsonLd),
            other => Err(CliError::Usage(format!(
                "unknown data format '{other}'\n  {} valid formats: turtle, jsonld",
                colored::Colorize::bold(colored::Colorize::cyan("help:"))
            ))),
        };
    }

    // File extension
    if let Some(p) = path {
        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
            return match ext.to_lowercase().as_str() {
                "ttl" => Ok(DataFormat::Turtle),
                "json" | "jsonld" => Ok(DataFormat::JsonLd),
                _ => sniff_data_format(content),
            };
        }
    }

    // Content sniffing
    sniff_data_format(content)
}

fn sniff_data_format(content: &str) -> CliResult<DataFormat> {
    // Attempt JSON parse for robust detection (not just first-char)
    if serde_json::from_str::<serde_json::Value>(content).is_ok() {
        Ok(DataFormat::JsonLd)
    } else {
        Ok(DataFormat::Turtle)
    }
}

/// Detect query format from file extension and content.
///
/// Priority: explicit flags > file extension > content sniffing.
pub fn detect_query_format(
    path: Option<&Path>,
    content: &str,
    sparql_flag: bool,
    jsonld_flag: bool,
) -> CliResult<QueryFormat> {
    if sparql_flag {
        return Ok(QueryFormat::Sparql);
    }
    if jsonld_flag {
        return Ok(QueryFormat::JsonLd);
    }

    // File extension
    if let Some(p) = path {
        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
            return match ext.to_lowercase().as_str() {
                "rq" | "sparql" => Ok(QueryFormat::Sparql),
                "json" | "jsonld" => Ok(QueryFormat::JsonLd),
                _ => sniff_query_format(content),
            };
        }
    }

    sniff_query_format(content)
}

fn sniff_query_format(content: &str) -> CliResult<QueryFormat> {
    let trimmed = content.trim();

    // SPARQL keywords (case-insensitive)
    let upper = trimmed.to_uppercase();
    let sparql_keywords = ["SELECT", "ASK", "CONSTRUCT", "DESCRIBE", "PREFIX", "BASE"];
    for kw in &sparql_keywords {
        if upper.starts_with(kw) {
            return Ok(QueryFormat::Sparql);
        }
    }

    // Valid JSON → JSON-LD query (parse to confirm, not just first-char check)
    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        return Ok(QueryFormat::JsonLd);
    }

    Err(CliError::Usage(format!(
        "could not detect query format\n  {} use --sparql or --jsonld to specify",
        colored::Colorize::bold(colored::Colorize::cyan("help:"))
    )))
}
