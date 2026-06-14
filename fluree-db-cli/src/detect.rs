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

    // File extension. Strip an outer `.gz`/`.zst` so `.ttl.gz` / `.jsonld.zst`
    // map to the same DataFormat as their plain counterparts (the bulk-import
    // path decompresses transparently; the insert/upsert HTTP path does not
    // yet, and will surface a UTF-8 error if a raw compressed file is sent).
    if let Some(p) = path {
        let outer = p
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_lowercase);
        let inner = match outer.as_deref() {
            Some("gz" | "zst" | "zstd") => p
                .file_stem()
                .map(std::path::Path::new)
                .and_then(|s| s.extension())
                .and_then(|e| e.to_str())
                .map(str::to_lowercase),
            _ => outer,
        };
        if let Some(ext) = inner {
            return match ext.as_str() {
                // `.nt` (N-Triples) is a Turtle subset — same parser.
                "ttl" | "nt" => Ok(DataFormat::Turtle),
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

/// Whether a query should be treated as Cypher.
///
/// Cypher is dispatched out-of-band from the SPARQL/JSON-LD `QueryFormat`
/// path because it uses a separate API method and result shape. Priority:
/// explicit `--cypher` flag > `.cypher`/`.cyp`/`.cql` extension > content
/// sniff. The sniffed lead keywords (`MATCH`/`MERGE`/`UNWIND`/`OPTIONAL`/
/// `DETACH`/`CREATE`) do not collide with any valid SPARQL query (which
/// leads with SELECT/ASK/CONSTRUCT/DESCRIBE/PREFIX/BASE) or JSON-LD (which
/// is JSON), so auto-detection never reinterprets an existing query.
pub fn detect_is_cypher(path: Option<&Path>, content: &str, cypher_flag: bool) -> bool {
    if cypher_flag {
        return true;
    }
    if let Some(p) = path {
        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
            if matches!(ext.to_lowercase().as_str(), "cypher" | "cyp" | "cql") {
                return true;
            }
        }
    }
    sniff_is_cypher(content)
}

fn sniff_is_cypher(content: &str) -> bool {
    let upper = content.trim_start().to_uppercase();
    const CYPHER_LEAD: [&str; 6] = [
        "MATCH ",
        "MERGE ",
        "UNWIND ",
        "OPTIONAL ",
        "DETACH ",
        "CREATE ",
    ];
    CYPHER_LEAD.iter().any(|kw| upper.starts_with(kw))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn cypher_flag_forces_cypher() {
        assert!(detect_is_cypher(None, "SELECT * WHERE { ?s ?p ?o }", true));
    }

    #[test]
    fn cypher_by_extension() {
        assert!(detect_is_cypher(
            Some(Path::new("q.cypher")),
            "anything",
            false
        ));
        assert!(detect_is_cypher(
            Some(Path::new("q.cyp")),
            "anything",
            false
        ));
    }

    #[test]
    fn cypher_sniffed_by_lead_keyword() {
        assert!(detect_is_cypher(None, "MATCH (n:Person) RETURN n", false));
        assert!(detect_is_cypher(None, "  merge (n:Person {x:1})", false));
        assert!(detect_is_cypher(None, "CREATE (n:Person)", false));
    }

    #[test]
    fn non_cypher_not_misdetected() {
        // SPARQL and JSON-LD must never sniff as Cypher.
        assert!(!detect_is_cypher(
            None,
            "SELECT * WHERE { ?s ?p ?o }",
            false
        ));
        assert!(!detect_is_cypher(None, "ASK { ?s ?p ?o }", false));
        assert!(!detect_is_cypher(
            None,
            "PREFIX ex: <http://e/>\nSELECT *",
            false
        ));
        assert!(!detect_is_cypher(
            None,
            r#"{"select":["?s"],"where":{}}"#,
            false
        ));
    }
}
