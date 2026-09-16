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
            other if is_dataset_format(other) => Err(CliError::Usage(dataset_format_help(other))),
            other => Err(CliError::Usage(format!(
                "unknown data format '{other}'\n  {} valid formats: turtle (ttl), jsonld (json-ld, json)",
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
                // A dataset file would otherwise sniff as Turtle and die
                // inside the parser on its first `GRAPH`, which tells the
                // reader nothing. Now that `fluree export --format trig`
                // produces these routinely, feeding one straight back is the
                // obvious next move and it deserves the actual answer.
                other if is_dataset_format(other) => {
                    Err(CliError::Usage(dataset_format_help(other)))
                }
                _ => sniff_data_format(content),
            };
        }
    }

    // Content sniffing
    sniff_data_format(content)
}

/// Dataset serializations: they carry named graphs, which `insert`/`upsert`
/// have no way to place.
fn is_dataset_format(s: &str) -> bool {
    matches!(s, "trig" | "nq" | "nquads" | "n-quads")
}

/// One message for both routes into the same dead end.
///
/// Still names every format the flag accepts, because a user who reached this
/// error guessed wrong once already — see
/// `the_usage_error_names_every_format_the_flag_accepts`, whose reasoning
/// applies to this branch exactly as much as to the generic one.
fn dataset_format_help(fmt: &str) -> String {
    let help = colored::Colorize::bold(colored::Colorize::cyan("help:"));
    format!(
        "'{fmt}' is a dataset format and carries named graphs, which insert cannot place\n  \
         {help} import it with `fluree create <ledger> --from <file>.{fmt}`, which reads \
         named graphs\n  \
         {help} insert accepts: turtle (ttl), jsonld (json-ld, json)"
    )
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
    // A JSON `{"cypher": "...", "params": {...}}` envelope is Cypher even though
    // it leads with `{` (which would otherwise sniff as JSON-LD). The server
    // accepts the same envelope under `Content-Type: application/cypher`.
    if looks_like_cypher_envelope(content) {
        return true;
    }
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

/// Whether `content` is a JSON `{"cypher": "...", ...}` envelope — the bundled
/// statement-plus-params form the server accepts as `application/cypher`. Used
/// so envelope bodies are not mis-sniffed as JSON-LD before the Cypher path.
pub fn looks_like_cypher_envelope(content: &str) -> bool {
    let trimmed = content.trim_start();
    if !trimmed.starts_with('{') {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(trimmed)
        .ok()
        .and_then(|v| v.get("cypher").and_then(|c| c.as_str()).map(|_| ()))
        .is_some()
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
    /// Every spelling the match arms accept is named by the error that lists
    /// them.
    ///
    /// The message advertised two of the five, so `--format ttl`, `json-ld`
    /// and `json` all worked while the only text telling a user what to type
    /// said they did not exist. A user who reached this error had already
    /// guessed wrong once; sending them to a shorter list than the code
    /// accepts is the one moment where being incomplete costs the most.
    #[test]
    fn the_usage_error_names_every_format_the_flag_accepts() {
        let accepted = ["turtle", "ttl", "jsonld", "json-ld", "json"];
        for fmt in accepted {
            assert!(
                super::detect_data_format(None, "", Some(fmt)).is_ok(),
                "--format {fmt} must be accepted"
            );
        }
        // `trig` takes the dataset-format branch, and `rdfxml` the generic
        // one. Both are errors a user reaches by guessing, so both owe the
        // full list.
        for guess in ["trig", "rdfxml"] {
            let err = match super::detect_data_format(None, "", Some(guess)) {
                Ok(_) => panic!("'{guess}' is not a data format the flag accepts"),
                Err(e) => e.to_string(),
            };
            for fmt in accepted {
                assert!(
                    err.contains(fmt),
                    "the usage error for '{guess}' must name '{fmt}'; got: {err}"
                );
            }
        }
    }

    /// A dataset file is not an unknown format — it is a known one that
    /// `insert` structurally cannot take, so it gets the command that can
    /// rather than a list to guess from again.
    #[test]
    fn a_dataset_format_names_create_from() {
        for fmt in ["trig", "nq", "nquads", "n-quads"] {
            let err = super::detect_data_format(None, "", Some(fmt))
                .expect_err("a dataset format is not insertable")
                .to_string();
            assert!(
                err.contains("fluree create <ledger> --from"),
                "'{fmt}' must name the command that works; got: {err}"
            );
        }
        // And by extension, which is the route an exported file arrives by.
        let err = super::detect_data_format(
            Some(std::path::Path::new("dump.trig")),
            "GRAPH <http://example.org/g> { }",
            None,
        )
        .expect_err("a .trig file is not insertable")
        .to_string();
        assert!(
            err.contains("fluree create <ledger> --from"),
            "a .trig path must name the command that works; got: {err}"
        );
    }

    use super::*;

    #[test]
    fn json_cypher_envelope_detected_as_cypher() {
        // A `{"cypher": ..., "params": ...}` envelope is Cypher, even though it
        // is valid JSON that would otherwise sniff as JSON-LD.
        let body = r#"{"cypher": "MATCH (n) RETURN n", "params": {}}"#;
        assert!(looks_like_cypher_envelope(body));
        assert!(detect_is_cypher(None, body, false));

        // A plain JSON-LD query object is not a Cypher envelope.
        let jsonld = r#"{"select": ["?s"], "where": {"@id": "?s"}}"#;
        assert!(!looks_like_cypher_envelope(jsonld));
        assert!(!detect_is_cypher(None, jsonld, false));

        // Leading-keyword Cypher still detected; SPARQL/JSON-LD still not.
        assert!(detect_is_cypher(None, "MATCH (n) RETURN n", false));
        assert!(!detect_is_cypher(
            None,
            "SELECT * WHERE { ?s ?p ?o }",
            false
        ));
    }
}
