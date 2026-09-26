//! W3C SPARQL 1.1 Protocol request parameters.
//!
//! The dataset parameters (`default-graph-uri`, `named-graph-uri`,
//! `using-graph-uri`, `using-named-graph-uri`) are REPEATED keys, one per
//! graph. `serde_urlencoded` — what `axum::extract::Query` deserializes with —
//! rejects a repeated key in a struct as "duplicate field", so these requests
//! are parsed from the decoded pair list instead.

use crate::error::ServerError;

pub(crate) const DEFAULT_GRAPH_URI: &str = "default-graph-uri";
pub(crate) const NAMED_GRAPH_URI: &str = "named-graph-uri";
pub(crate) const USING_GRAPH_URI: &str = "using-graph-uri";
pub(crate) const USING_NAMED_GRAPH_URI: &str = "using-named-graph-uri";

/// Percent/`+`-decode one `application/x-www-form-urlencoded` component.
///
/// `+` is a space; percent escapes cover everything else. A malformed escape
/// (`%ZZ`, a trailing `%`) or an escape that decodes to invalid UTF-8 is an
/// error, never a verbatim passthrough: a graph IRI that silently kept its
/// raw escapes would name a graph that does not exist.
pub(crate) fn percent_decode_strict(raw: &str) -> Result<String, String> {
    // `urlencoding::decode` only fails on invalid UTF-8; it leaves a malformed
    // escape in the string and returns `Ok`, so escapes are validated first.
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            match bytes.get(i + 1..i + 3) {
                Some(hex) if hex.iter().all(u8::is_ascii_hexdigit) => i += 3,
                _ => return Err("malformed percent-escape".to_string()),
            }
        } else {
            i += 1;
        }
    }
    urlencoding::decode(&raw.replace('+', " "))
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| e.to_string())
}

/// Decode a query string or form body into `(key, value)` pairs, keeping
/// repeats in order. A bare key (`?flag`) decodes to an empty value.
pub(crate) fn decode_pairs(raw: &str) -> Result<Vec<(String, String)>, ServerError> {
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |part: &str| {
                percent_decode_strict(part).map_err(|why| {
                    ServerError::bad_request(format!(
                        "request parameter {part:?} is not valid percent-encoded UTF-8 ({why})"
                    ))
                })
            };
            Ok((decode(key)?, decode(value)?))
        })
        .collect()
}

/// The update dataset parameters of one request, gathered from the URL and,
/// for a form-encoded update, the body.
#[derive(Debug, Default, Clone)]
pub struct UsingParams {
    /// `using-graph-uri` values, in request order.
    pub using_graph_uri: Vec<String>,
    /// `using-named-graph-uri` values, in request order.
    pub using_named_graph_uri: Vec<String>,
}

impl UsingParams {
    /// Collect the `using-*` keys from decoded pairs.
    pub(crate) fn extend_from_pairs(&mut self, pairs: &[(String, String)]) {
        for (key, value) in pairs {
            match key.as_str() {
                USING_GRAPH_URI => self.using_graph_uri.push(value.clone()),
                USING_NAMED_GRAPH_URI => self.using_named_graph_uri.push(value.clone()),
                _ => {}
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.using_graph_uri.is_empty() && self.using_named_graph_uri.is_empty()
    }

    /// Rewrite `USING` / `USING NAMED` into a SPARQL UPDATE (see
    /// [`fluree_db_sparql::protocol::apply_update_using`]).
    pub(crate) fn apply(&self, update: String) -> Result<String, ServerError> {
        match fluree_db_sparql::protocol::apply_update_using(
            &update,
            &self.using_graph_uri,
            &self.using_named_graph_uri,
        ) {
            Ok(std::borrow::Cow::Borrowed(_)) => Ok(update),
            Ok(std::borrow::Cow::Owned(rewritten)) => Ok(rewritten),
            Err(e) => Err(ServerError::bad_request(e.to_string())),
        }
    }

    /// A non-SPARQL update has no USING clause to set; refuse rather than
    /// silently run it unscoped.
    pub(crate) fn reject_outside_sparql(&self) -> Result<(), ServerError> {
        if self.is_empty() {
            return Ok(());
        }
        Err(ServerError::bad_request(
            "using-graph-uri / using-named-graph-uri apply only to SPARQL UPDATE requests; \
             a JSON-LD update scopes its WHERE with `from` / `fromNamed`",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_keys_are_kept_in_order() {
        let pairs =
            decode_pairs("using-graph-uri=urn%3Aa&ledger=x&using-graph-uri=urn:b&flag").unwrap();
        let mut using = UsingParams::default();
        using.extend_from_pairs(&pairs);
        assert_eq!(using.using_graph_uri, vec!["urn:a", "urn:b"]);
        assert!(pairs.contains(&("flag".to_string(), String::new())));
    }

    #[test]
    fn malformed_escapes_are_rejected() {
        assert!(decode_pairs("using-graph-uri=urn%ZZa").is_err());
        assert!(decode_pairs("using-graph-uri=urn%FFa").is_err());
    }

    #[test]
    fn plus_decodes_to_space() {
        assert_eq!(
            decode_pairs("query=SELECT+*").unwrap(),
            vec![("query".to_string(), "SELECT *".to_string())]
        );
    }
}
