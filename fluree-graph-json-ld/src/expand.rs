use crate::context::{Container, ContextEntry, ParsedContext, TypeValue};
use crate::error::{JsonLdError, Result};
use crate::iri::{self, UnresolvedIriDisposition};
use serde_json::{json, Map, Value as JsonValue};

/// Check if a value is a variable (starts with ?)
fn is_variable(s: &str) -> bool {
    s.starts_with('?')
}

/// Attempts exact match with a compact-iri in context.
/// Returns (full_iri, context_entry) if successful.
fn match_exact(compact_iri: &str, context: &ParsedContext) -> Option<(String, ContextEntry)> {
    context.get(compact_iri).map(|entry| {
        let iri = entry
            .id
            .clone()
            .or_else(|| entry.reverse.clone())
            .unwrap_or_else(|| compact_iri.to_string());
        (iri, entry.clone())
    })
}

/// Attempts prefix match with a compact-iri.
/// Returns (full_iri, context_entry) if successful.
fn match_prefix(compact_iri: &str, context: &ParsedContext) -> Option<(String, ContextEntry)> {
    iri::parse_prefix(compact_iri).and_then(|(prefix, suffix)| {
        context.get(&prefix).and_then(|entry| {
            entry.id.as_ref().map(|prefix_iri| {
                let full_iri = format!("{prefix_iri}{suffix}");
                (full_iri, entry.clone())
            })
        })
    })
}

/// Uses default vocab/base if compact-iri doesn't look like an IRI.
/// Returns (full_iri, context_entry) if successful.
fn match_default(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
) -> Option<(String, ContextEntry)> {
    if is_variable(compact_iri) {
        return Some((compact_iri.to_string(), ContextEntry::default()));
    }

    let default_match = if vocab {
        context.vocab.as_ref()
    } else {
        context.base.as_ref()
    };

    default_match.and_then(|default| {
        if !iri::any_iri(compact_iri) && !compact_iri.starts_with('@') {
            let full_iri = format!("{default}{compact_iri}");
            Some((
                full_iri.clone(),
                ContextEntry {
                    id: Some(full_iri),
                    ..Default::default()
                },
            ))
        } else {
            None
        }
    })
}

/// Expand details: returns (expanded_iri, context_entry)
/// If vocab is true, uses @vocab for properties/classes.
/// If vocab is false, uses @base for @id values.
pub fn details(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
) -> (String, Option<ContextEntry>) {
    // Try exact match first
    if let Some((iri, entry)) = match_exact(compact_iri, context) {
        return (iri, Some(entry));
    }

    // Try prefix match
    if let Some((iri, entry)) = match_prefix(compact_iri, context) {
        return (iri, Some(entry));
    }

    // Try default vocab/base
    if let Some((iri, entry)) = match_default(compact_iri, context, vocab) {
        return (iri, Some(entry));
    }

    // No match - return as-is
    if compact_iri.starts_with('@') {
        (
            compact_iri.to_string(),
            Some(ContextEntry {
                id: Some(compact_iri.to_string()),
                ..Default::default()
            }),
        )
    } else {
        (compact_iri.to_string(), None)
    }
}

/// Expand a single IRI
pub fn iri(compact_iri: &str, context: &ParsedContext, vocab: bool) -> String {
    details(compact_iri, context, vocab).0
}

/// Reject an unresolved IRI if it looks like a compact IRI with a missing prefix.
fn guard_unresolved(value: &str) -> Result<()> {
    if let UnresolvedIriDisposition::RejectLikelyCompact { prefix } =
        iri::check_unresolved_iri(value)
    {
        Err(JsonLdError::UnresolvedCompactIri {
            value: value.to_string(),
            prefix,
        })
    } else {
        Ok(())
    }
}

/// Like [`details`] but rejects unresolved compact-looking IRIs.
///
/// Use this at JSON-LD boundaries where every IRI position should either
/// resolve through `@context` or be a recognisable absolute IRI.
pub fn details_checked(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
) -> Result<(String, Option<ContextEntry>)> {
    let result = details(compact_iri, context, vocab);
    if result.1.is_none() {
        guard_unresolved(&result.0)?;
    }
    Ok(result)
}

/// Like [`iri`] but rejects unresolved compact-looking IRIs.
pub fn iri_checked(compact_iri: &str, context: &ParsedContext, vocab: bool) -> Result<String> {
    let (expanded, entry) = details(compact_iri, context, vocab);
    if entry.is_none() {
        guard_unresolved(&expanded)?;
    }
    Ok(expanded)
}

// ── Internal dispatch: strict=true → checked, strict=false → unchecked ──

fn iri_dispatch(s: &str, ctx: &ParsedContext, vocab: bool, strict: bool) -> Result<String> {
    if strict {
        iri_checked(s, ctx, vocab)
    } else {
        Ok(iri(s, ctx, vocab))
    }
}

fn details_dispatch(
    s: &str,
    ctx: &ParsedContext,
    vocab: bool,
    strict: bool,
) -> Result<(String, Option<ContextEntry>)> {
    if strict {
        details_checked(s, ctx, vocab)
    } else {
        Ok(details(s, ctx, vocab))
    }
}

/// Check if map is a @list container
fn is_list_item(map: &Map<String, JsonValue>) -> bool {
    (map.contains_key("@list") || map.contains_key("list"))
        && (map.len() == 1
            || (map.len() == 2 && (map.contains_key("@index") || map.contains_key("index"))))
}

/// Check if map is a @set container
fn is_set_item(map: &Map<String, JsonValue>) -> bool {
    (map.contains_key("@set") || map.contains_key("set"))
        && (map.len() == 1
            || (map.len() == 2 && (map.contains_key("@index") || map.contains_key("index"))))
}

/// Parse a value based on its type and context info
fn parse_node_value(
    value: &JsonValue,
    entry: Option<&ContextEntry>,
    context: &ParsedContext,
    idx: &[JsonValue],
    strict: bool,
) -> Result<Vec<JsonValue>> {
    let type_val = entry.and_then(|e| e.type_.as_ref());
    let id_val = entry.and_then(|e| e.id.as_deref());

    match value {
        JsonValue::Null => Ok(vec![]),

        JsonValue::Bool(b) => {
            let mut obj = Map::new();
            obj.insert("@value".to_string(), json!(*b));
            if let Some(TypeValue::Iri(t)) = type_val {
                obj.insert("@type".to_string(), json!(t));
            }
            Ok(vec![JsonValue::Object(obj)])
        }

        JsonValue::Number(n) => {
            let mut obj = Map::new();
            obj.insert("@value".to_string(), JsonValue::Number(n.clone()));
            if let Some(TypeValue::Iri(t)) = type_val {
                obj.insert("@type".to_string(), json!(t));
            }
            Ok(vec![JsonValue::Object(obj)])
        }

        JsonValue::String(s) => {
            // Check for @id or @type expansion
            if id_val == Some("@id") || id_val == Some("@type") {
                return Ok(vec![json!(iri_dispatch(s, context, false, strict)?)]);
            }

            // Check if type is @id (value should be expanded as IRI)
            if type_val == Some(&TypeValue::Id) {
                let mut obj = Map::new();
                obj.insert(
                    "@id".to_string(),
                    json!(iri_dispatch(s, context, false, strict)?),
                );
                return Ok(vec![JsonValue::Object(obj)]);
            }

            // Check for language tag
            let lang = entry
                .and_then(|e| e.language.as_ref())
                .and_then(|l| l.as_ref())
                .or(context.language.as_ref());

            let mut obj = Map::new();
            obj.insert("@value".to_string(), json!(s));

            if let Some(TypeValue::Iri(t)) = type_val {
                if lang.is_some() {
                    return Err(JsonLdError::LanguageWithType);
                }
                obj.insert("@type".to_string(), json!(t));
            } else if let Some(lang) = lang {
                obj.insert("@language".to_string(), json!(lang));
            }

            Ok(vec![JsonValue::Object(obj)])
        }

        JsonValue::Array(arr) => {
            let mut results = Vec::new();
            for (i, item) in arr.iter().enumerate() {
                let mut new_idx = idx.to_vec();
                new_idx.push(json!(i));

                if let JsonValue::Array(_) = item {
                    return Err(JsonLdError::NestedSequence {
                        idx: new_idx.clone(),
                    });
                }

                let expanded = if item.is_object() {
                    let map = item.as_object().unwrap();
                    if map.contains_key("@value") || map.contains_key("value") {
                        parse_node_value(item, entry, context, &new_idx, strict)?
                    } else {
                        vec![expand_node_internal(item, context, &new_idx, strict)?]
                    }
                } else {
                    parse_node_value(item, entry, context, &new_idx, strict)?
                };
                results.extend(expanded);
            }

            // Check if container is @list
            if let Some(e) = entry {
                if let Some(ref containers) = e.container {
                    if containers.contains(&Container::List) {
                        let mut obj = Map::new();
                        obj.insert("@list".to_string(), JsonValue::Array(results));
                        return Ok(vec![JsonValue::Object(obj)]);
                    }
                }
            }

            Ok(results)
        }

        JsonValue::Object(map) => {
            // Check for @json type
            if type_val == Some(&TypeValue::Json) {
                let mut obj = Map::new();
                obj.insert("@value".to_string(), value.clone());
                obj.insert("@type".to_string(), json!("@json"));
                return Ok(vec![JsonValue::Object(obj)]);
            }

            // Check for @list
            if is_list_item(map) {
                let list_val = map.get("@list").or_else(|| map.get("list")).unwrap();
                let mut new_idx = idx.to_vec();
                new_idx.push(json!("@list"));
                let expanded = parse_node_value(list_val, entry, context, &new_idx, strict)?;
                let mut obj = Map::new();
                obj.insert("@list".to_string(), JsonValue::Array(expanded));
                return Ok(vec![JsonValue::Object(obj)]);
            }

            // Check for @set (flatten it)
            if is_set_item(map) {
                let set_val = map.get("@set").or_else(|| map.get("set")).unwrap();
                let mut new_idx = idx.to_vec();
                new_idx.push(json!("@set"));
                return parse_node_value(set_val, entry, context, &new_idx, strict);
            }

            // Check for @value
            if map.contains_key("@value") || map.contains_key("value") {
                return parse_value_object(map, entry, context, strict);
            }

            // Check for @container: @language
            if let Some(e) = entry {
                if let Some(ref containers) = e.container {
                    if containers.contains(&Container::Language) {
                        let mut results = Vec::new();
                        for (lang, v) in map {
                            let values = match v {
                                JsonValue::Array(arr) => arr.clone(),
                                _ => vec![v.clone()],
                            };
                            for val in values {
                                let val_str = if let JsonValue::Object(m) = &val {
                                    m.get("@value")
                                        .or_else(|| m.get("value"))
                                        .and_then(|v| v.as_str())
                                        .map(std::string::ToString::to_string)
                                } else {
                                    val.as_str().map(std::string::ToString::to_string)
                                };

                                if let Some(s) = val_str {
                                    let mut obj = Map::new();
                                    obj.insert("@value".to_string(), json!(s));
                                    obj.insert("@language".to_string(), json!(lang));
                                    results.push(JsonValue::Object(obj));
                                }
                            }
                        }
                        return Ok(results);
                    }
                }
            }

            // Otherwise, expand as nested node
            let ctx = if let Some(e) = entry {
                if let Some(ref nested) = e.context {
                    let mut merged = context.clone();
                    merged.terms.extend(nested.terms.clone());
                    if nested.vocab.is_some() {
                        merged.vocab = nested.vocab.clone();
                    }
                    merged
                } else {
                    context.clone()
                }
            } else {
                context.clone()
            };

            Ok(vec![expand_node_internal(value, &ctx, idx, strict)?])
        }
    }
}

/// Parse a @value object
fn parse_value_object(
    map: &Map<String, JsonValue>,
    entry: Option<&ContextEntry>,
    context: &ParsedContext,
    strict: bool,
) -> Result<Vec<JsonValue>> {
    let val = map.get("@value").or_else(|| map.get("value")).unwrap();

    // Check for explicit @type
    let explicit_type = match map
        .get("@type")
        .or_else(|| map.get("type"))
        .and_then(|t| t.as_str())
    {
        Some(t) => Some(iri_dispatch(t, context, true, strict)?),
        None => None,
    };

    // Get type from entry if not explicit
    let type_iri = explicit_type.or_else(|| {
        entry.as_ref().and_then(|e| match &e.type_ {
            Some(TypeValue::Iri(t)) => Some(t.clone()),
            Some(TypeValue::Id) => Some("@id".to_string()),
            _ => None,
        })
    });

    // Check for language
    let lang = map
        .get("@language")
        .or_else(|| map.get("language"))
        .and_then(|l| l.as_str())
        .map(std::string::ToString::to_string)
        .or_else(|| context.language.clone());

    // Build result
    let mut obj = Map::new();

    if type_iri.as_deref() == Some("@id") {
        let iri_val = match val.as_str() {
            Some(s) => iri_dispatch(s, context, false, strict)?,
            None => String::new(),
        };
        obj.insert("@id".to_string(), json!(iri_val));
    } else {
        obj.insert("@value".to_string(), val.clone());

        if let Some(t) = type_iri {
            if lang.is_some() {
                return Err(JsonLdError::LanguageWithType);
            }
            obj.insert("@type".to_string(), json!(t));
        } else if let Some(l) = lang {
            obj.insert("@language".to_string(), json!(l));
        }
    }

    Ok(vec![JsonValue::Object(obj)])
}

/// Parse @type values and return expanded types with potentially updated context
fn parse_type(
    node_map: &Map<String, JsonValue>,
    context: &ParsedContext,
    strict: bool,
) -> Result<(Vec<String>, ParsedContext)> {
    let type_key = &context.type_key;
    let type_id = context.get(type_key).and_then(|e| e.id.clone());

    let type_val = node_map
        .get(type_key)
        .or_else(|| type_id.as_ref().and_then(|id| node_map.get(id)));

    if let Some(type_val) = type_val {
        let types: Vec<String> = match type_val {
            JsonValue::String(s) => vec![iri_dispatch(s, context, true, strict)?],
            JsonValue::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for v in arr {
                    if let Some(s) = v.as_str() {
                        result.push(iri_dispatch(s, context, true, strict)?);
                    }
                }
                result
            }
            _ => vec![],
        };

        // Check for type-dependent sub-contexts
        let mut updated_context = context.clone();
        // Check original (unexpanded) types for sub-context
        let original_types: Vec<&str> = match type_val {
            JsonValue::String(s) => vec![s.as_str()],
            JsonValue::Array(arr) => arr.iter().filter_map(|v| v.as_str()).collect(),
            _ => vec![],
        };

        for orig_t in original_types {
            if let Some(entry) = context.get(orig_t) {
                if let Some(ref sub_ctx) = entry.context {
                    updated_context.terms.extend(sub_ctx.terms.clone());
                    if sub_ctx.vocab.is_some() {
                        updated_context.vocab = sub_ctx.vocab.clone();
                    }
                }
            }
        }

        Ok((types, updated_context))
    } else {
        Ok((vec![], context.clone()))
    }
}

/// Get context from node map
fn get_context(node_map: &Map<String, JsonValue>) -> Option<&JsonValue> {
    node_map.get("@context").or_else(|| node_map.get("context"))
}

/// Internal node expansion
fn expand_node_internal(
    node_map: &JsonValue,
    context: &ParsedContext,
    idx: &[JsonValue],
    strict: bool,
) -> Result<JsonValue> {
    match node_map {
        JsonValue::Array(arr) => {
            let expanded: Result<Vec<JsonValue>> = arr
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let mut new_idx = idx.to_vec();
                    new_idx.push(json!(i));
                    expand_node_internal(item, context, &new_idx, strict)
                })
                .collect();
            Ok(JsonValue::Array(expanded?))
        }

        JsonValue::Object(map) => {
            // Parse local context if present
            let local_context = get_context(map);
            let merged_context = if let Some(lc) = local_context {
                ParsedContext::parse(Some(context), lc)?
            } else {
                context.clone()
            };

            // Parse @type and get updated context
            let (types, context_with_types) = parse_type(map, &merged_context, strict)?;

            // Build result
            let mut result = Map::new();

            // Add @type if present
            if !types.is_empty() {
                result.insert("@type".to_string(), json!(types));
            }

            // Process other keys
            let type_key = &merged_context.type_key;
            let type_id = merged_context.get(type_key).and_then(|e| e.id.clone());

            for (k, v) in map {
                // Skip context and type keys
                if k == "@context" || k == "context" {
                    continue;
                }
                if k == type_key || Some(k.clone()) == type_id {
                    continue;
                }

                let mut key_idx = idx.to_vec();
                key_idx.push(json!(k));

                let (expanded_key, entry) = details_dispatch(k, &context_with_types, true, strict)?;

                // Handle @graph
                if expanded_key == "@graph" || k == "@graph" || k == "graph" {
                    let graph_expanded =
                        expand_node_internal(v, &context_with_types, &key_idx, strict)?;
                    result.insert("@graph".to_string(), graph_expanded);
                    continue;
                }

                // Handle @id
                if expanded_key == "@id" || k == "@id" {
                    if let JsonValue::String(s) = v {
                        result.insert(
                            "@id".to_string(),
                            json!(iri_dispatch(s, &context_with_types, false, strict)?),
                        );
                    }
                    continue;
                }

                // Expand value
                let expanded_values =
                    parse_node_value(v, entry.as_ref(), &context_with_types, &key_idx, strict)?;

                if !expanded_values.is_empty() {
                    // Check for @reverse
                    if let Some(ref e) = entry {
                        if e.reverse.is_some() {
                            result.insert(expanded_key, JsonValue::Array(expanded_values));
                            continue;
                        }
                    }

                    // Append to existing or create new
                    if let Some(existing) = result.get_mut(&expanded_key) {
                        if let JsonValue::Array(arr) = existing {
                            arr.extend(expanded_values);
                        }
                    } else {
                        result.insert(expanded_key, JsonValue::Array(expanded_values));
                    }
                }
            }

            Ok(JsonValue::Object(result))
        }

        _ => Ok(node_map.clone()),
    }
}

/// Shared implementation for node expansion with configurable strictness.
pub(crate) fn node_impl(
    node_map: &JsonValue,
    context: &ParsedContext,
    strict: bool,
) -> Result<JsonValue> {
    let idx = vec![];

    match node_map {
        JsonValue::Array(arr) => {
            let expanded: Result<Vec<JsonValue>> = arr
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let new_idx = vec![json!(i)];
                    expand_node_internal(item, context, &new_idx, strict)
                })
                .collect();
            Ok(JsonValue::Array(expanded?))
        }

        JsonValue::Object(map) => {
            // Check for @graph at top level
            let graph_key = if map.contains_key("@graph") {
                Some("@graph")
            } else if map.contains_key("graph") {
                Some("graph")
            } else {
                None
            };

            // Handle envelope form: when @graph is present at top level WITHOUT
            // an @id, this is an envelope/default-graph wrapper. Expand only the
            // @graph contents. Extra top-level keys (txn-meta properties) are
            // handled separately by the transact parser's extract_txn_meta(),
            // so we must not feed the whole envelope into expand_node_internal —
            // that would treat the envelope as a single node and silently drop @graph.
            //
            // When @id IS present alongside @graph, this is a JSON-LD named graph
            // (the @id names the graph) — fall through to normal expansion which
            // preserves the @id, properties, and nested @graph.
            if let Some(gk) = graph_key {
                let has_id = map.contains_key("@id") || map.contains_key("id");
                if !has_id {
                    let local_context = get_context(map);
                    let merged_context = if let Some(lc) = local_context {
                        ParsedContext::parse(Some(context), lc)?
                    } else {
                        context.clone()
                    };

                    if let Some(graph) = map.get(gk) {
                        let new_idx = vec![json!(gk)];
                        return expand_node_internal(graph, &merged_context, &new_idx, strict);
                    }
                }
            }

            expand_node_internal(node_map, context, &idx, strict)
        }

        _ => Ok(node_map.clone()),
    }
}

/// Expand a JSON-LD node (document).
///
/// Permissive: unresolved compact-looking IRIs pass through silently.
pub fn node(node_map: &JsonValue, context: &ParsedContext) -> Result<JsonValue> {
    node_impl(node_map, context, false)
}

/// Expand a JSON-LD node (document) with strict compact-IRI checking.
///
/// Rejects unresolved compact-looking IRIs (e.g. `ex:Person` when `ex` is
/// not defined in `@context`) at every IRI position. Use this at Fluree's
/// JSON-LD parsing boundaries (queries and transactions).
pub fn node_checked(node_map: &JsonValue, context: &ParsedContext) -> Result<JsonValue> {
    node_impl(node_map, context, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_expand_iri_exact_match() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "REPLACE": "http://schema.org/Person"
            }),
        )
        .unwrap();

        assert_eq!(iri("schema:name", &ctx, true), "http://schema.org/name");
        assert_eq!(iri("REPLACE", &ctx, true), "http://schema.org/Person");
    }

    #[test]
    fn test_expand_iri_vocab() {
        let ctx = ParsedContext::parse(None, &json!("https://schema.org")).unwrap();

        assert_eq!(iri("name", &ctx, true), "https://schema.org/name");
        // Full IRIs should pass through
        assert_eq!(
            iri("http://example.org/ns#Book", &ctx, true),
            "http://example.org/ns#Book"
        );
    }

    #[test]
    fn test_expand_iri_no_match() {
        let ctx = ParsedContext::parse(None, &json!({"schema": "http://schema.org/"})).unwrap();

        assert_eq!(iri("not:matching", &ctx, true), "not:matching");
    }

    #[test]
    fn test_expand_reverse_iri() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "parent": {"@reverse": "schema:child"}
            }),
        )
        .unwrap();

        let (expanded, entry) = details("parent", &ctx, true);
        assert_eq!(expanded, "http://schema.org/child");
        assert_eq!(
            entry.unwrap().reverse,
            Some("http://schema.org/child".to_string())
        );
    }

    #[test]
    fn test_expand_node_basic() {
        let doc = json!({
            "@context": {
                "ical": "http://www.w3.org/2002/12/cal/ical#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "ical:dtstart": {"@type": "xsd:dateTime"}
            },
            "ical:summary": "Lady Gaga Concert",
            "ical:location": "New Orleans Arena, New Orleans, Louisiana, USA",
            "ical:dtstart": "2011-04-09T20:00:00Z"
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        assert!(obj.contains_key("http://www.w3.org/2002/12/cal/ical#summary"));
        assert!(obj.contains_key("http://www.w3.org/2002/12/cal/ical#dtstart"));

        let dtstart = &obj["http://www.w3.org/2002/12/cal/ical#dtstart"][0];
        assert_eq!(dtstart["@value"], "2011-04-09T20:00:00Z");
        assert_eq!(
            dtstart["@type"],
            "http://www.w3.org/2001/XMLSchema#dateTime"
        );
    }

    #[test]
    fn test_expand_node_with_id_and_type() {
        let doc = json!({
            "@context": "https://schema.org",
            "@id": "https://www.wikidata.org/wiki/Q836821",
            "@type": "Movie",
            "name": "Hitchhiker's Guide to the Galaxy"
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        assert_eq!(obj["@id"], "https://www.wikidata.org/wiki/Q836821");
        // Without external context mapping, https://schema.org stays as https
        assert_eq!(obj["@type"], json!(["https://schema.org/Movie"]));
        assert!(obj.contains_key("https://schema.org/name"));
    }

    #[test]
    fn test_expand_list() {
        let doc = json!({
            "@context": {
                "nick": {"@id": "http://xmlns.com/foaf/0.1/nick", "@container": "@list"}
            },
            "@id": "http://example.org/people#joebob",
            "nick": ["joe", "bob", "jaybee"]
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        let nicks = &obj["http://xmlns.com/foaf/0.1/nick"][0];
        assert!(nicks.get("@list").is_some());
        let list = nicks["@list"].as_array().unwrap();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_expand_set_flattens() {
        let doc = json!({
            "@context": {"foaf": "http://xmlns.com/foaf/0.1/"},
            "@id": "http://example.org/people#joebob",
            "foaf:nick": {"@set": ["joe", "bob", "jaybee"]}
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        let nicks = obj["http://xmlns.com/foaf/0.1/nick"].as_array().unwrap();
        assert_eq!(nicks.len(), 3);
        // @set should be flattened (no @set wrapper)
        assert!(nicks[0].get("@value").is_some());
    }

    #[test]
    fn test_expand_base_and_vocab() {
        let doc = json!({
            "@context": {
                "@base": "https://base.com/base/iri",
                "@vocab": "https://vocab.com/vocab/iri/",
                "iriProperty": {"@type": "@id"}
            },
            "@id": "#joebob",
            "@type": "Joey",
            "name": "Joe Bob",
            "iriProperty": "#a-relative-id"
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        assert_eq!(obj["@id"], "https://base.com/base/iri#joebob");
        assert_eq!(obj["@type"], json!(["https://vocab.com/vocab/iri/Joey"]));
    }

    #[test]
    fn test_false_value_survives() {
        let doc = json!({
            "@id": "foo",
            "bar": {"@value": false}
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        assert_eq!(obj["bar"][0]["@value"], false);
    }

    #[test]
    fn test_variables_not_expanded() {
        let doc = json!({
            "@context": {"@base": "https:example.com/", "@vocab": "ns/"},
            "foo": {"@id": "?s", "?p": "?o"},
            "bar": {"@id": "me", "name": "Dan"}
        });

        let result = node(&doc, &ParsedContext::new()).unwrap();
        let obj = result.as_object().unwrap();

        let foo = &obj["https:example.com/ns/foo"][0];
        assert_eq!(foo["@id"], "?s");
    }
}
