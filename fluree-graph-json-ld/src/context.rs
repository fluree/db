use crate::error::{JsonLdError, Result};
use crate::iri;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;

/// Container types for JSON-LD @container values
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Container {
    List,
    Set,
    Language,
    Index,
}

/// Type values can be keywords or IRIs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeValue {
    Id,          // @id - value is an IRI reference
    Vocab,       // @vocab
    Json,        // @json - JSON literal
    Iri(String), // Specific datatype IRI
}

/// A single context entry (term definition)
#[derive(Debug, Clone, Default)]
pub struct ContextEntry {
    /// The expanded IRI (@id)
    pub id: Option<String>,
    /// The datatype (@type)
    pub type_: Option<TypeValue>,
    /// Reverse property (@reverse)
    pub reverse: Option<String>,
    /// Container type (@container)
    pub container: Option<Vec<Container>>,
    /// Nested context for this term (@context)
    pub context: Option<Box<ParsedContext>>,
    /// Language tag (@language)
    pub language: Option<Option<String>>, // Some(None) means explicitly cleared
    /// Whether this entry defines @type (maps to @type keyword)
    pub is_type_definition: bool,
    /// Whether this entry was derived (copied from compact IRI to full IRI)
    pub derived: bool,
}

impl ContextEntry {
    /// Serialize this entry to a JSON-LD value.
    ///
    /// Returns a simple string if only `@id` is set, otherwise an object
    /// with `@id`, `@type`, `@reverse`, `@container`, `@language`, and/or `@context`.
    pub fn to_json(&self) -> JsonValue {
        let has_extras = self.type_.is_some()
            || self.reverse.is_some()
            || self.container.is_some()
            || self.context.is_some()
            || self.language.is_some();

        if !has_extras {
            if let Some(ref id) = self.id {
                return JsonValue::String(id.clone());
            }
            return JsonValue::Null;
        }

        let mut obj = Map::new();

        if let Some(ref id) = self.id {
            obj.insert("@id".to_string(), JsonValue::String(id.clone()));
        }
        if let Some(ref rev) = self.reverse {
            obj.insert("@reverse".to_string(), JsonValue::String(rev.clone()));
        }
        if let Some(ref t) = self.type_ {
            let type_str = match t {
                TypeValue::Id => "@id".to_string(),
                TypeValue::Vocab => "@vocab".to_string(),
                TypeValue::Json => "@json".to_string(),
                TypeValue::Iri(iri) => iri.clone(),
            };
            obj.insert("@type".to_string(), JsonValue::String(type_str));
        }
        if let Some(ref containers) = self.container {
            let vals: Vec<&str> = containers
                .iter()
                .map(|c| match c {
                    Container::List => "@list",
                    Container::Set => "@set",
                    Container::Language => "@language",
                    Container::Index => "@index",
                })
                .collect();
            if vals.len() == 1 {
                obj.insert(
                    "@container".to_string(),
                    JsonValue::String(vals[0].to_string()),
                );
            } else {
                obj.insert(
                    "@container".to_string(),
                    JsonValue::Array(
                        vals.iter()
                            .map(|v| JsonValue::String(v.to_string()))
                            .collect(),
                    ),
                );
            }
        }
        if let Some(ref lang) = self.language {
            match lang {
                Some(l) => {
                    obj.insert("@language".to_string(), JsonValue::String(l.clone()));
                }
                None => {
                    obj.insert("@language".to_string(), JsonValue::Null);
                }
            }
        }
        if let Some(ref nested) = self.context {
            obj.insert("@context".to_string(), nested.to_json());
        }

        JsonValue::Object(obj)
    }
}

/// The fully parsed context
#[derive(Debug, Clone, Default)]
pub struct ParsedContext {
    /// Key used for @id (defaults to "@id", can be aliased)
    pub id_key: String,
    /// Key used for @type (defaults to "@type", can be aliased)
    pub type_key: String,
    /// Default vocabulary (@vocab)
    pub vocab: Option<String>,
    /// Base IRI (@base)
    pub base: Option<String>,
    /// Default language (@language)
    pub language: Option<String>,
    /// JSON-LD version (@version)
    pub version: Option<f64>,
    /// Whether context is protected (@protected)
    pub protected: Option<bool>,
    /// Term definitions (string keys map to entries)
    pub terms: HashMap<String, ContextEntry>,
}

impl ParsedContext {
    pub fn new() -> Self {
        Self {
            id_key: "@id".to_string(),
            type_key: "@type".to_string(),
            ..Default::default()
        }
    }

    /// Get a term entry by key
    pub fn get(&self, key: &str) -> Option<&ContextEntry> {
        self.terms.get(key)
    }

    /// Check if context contains a term
    pub fn contains(&self, key: &str) -> bool {
        self.terms.contains_key(key)
    }

    /// Serialize this ParsedContext back to a JSON-LD @context value.
    ///
    /// Reconstructs the context object from parsed term definitions,
    /// preserving @vocab, @base, @language, @version, @protected, and all
    /// term metadata (@type, @container, @reverse, @language, nested @context).
    ///
    /// Returns `JsonValue::Null` for empty contexts.
    pub fn to_json(&self) -> JsonValue {
        if self.terms.is_empty()
            && self.vocab.is_none()
            && self.base.is_none()
            && self.language.is_none()
        {
            return JsonValue::Null;
        }

        let mut ctx = Map::new();

        if let Some(ref vocab) = self.vocab {
            ctx.insert("@vocab".to_string(), JsonValue::String(vocab.clone()));
        }
        if let Some(ref base) = self.base {
            ctx.insert("@base".to_string(), JsonValue::String(base.clone()));
        }
        if let Some(ref lang) = self.language {
            ctx.insert("@language".to_string(), JsonValue::String(lang.clone()));
        }
        if let Some(version) = self.version {
            ctx.insert("@version".to_string(), serde_json::json!(version));
        }
        if let Some(protected) = self.protected {
            ctx.insert("@protected".to_string(), JsonValue::Bool(protected));
        }

        for (key, entry) in &self.terms {
            if entry.derived {
                continue;
            }
            ctx.insert(key.clone(), entry.to_json());
        }

        JsonValue::Object(ctx)
    }

    /// Parse a JSON-LD context value (string, map, array, or null)
    pub fn parse(
        base_context: Option<&ParsedContext>,
        context: &JsonValue,
    ) -> Result<ParsedContext> {
        let mut active = base_context.cloned().unwrap_or_else(|| ParsedContext {
            id_key: "@id".to_string(),
            type_key: "@type".to_string(),
            ..Default::default()
        });

        // Ensure id_key is set
        if active.id_key.is_empty() {
            active.id_key = "@id".to_string();
        }

        // Ensure type_key is set
        if active.type_key.is_empty() {
            active.type_key = "@type".to_string();
        }

        match context {
            JsonValue::Null => Ok(ParsedContext::default()), // null resets context

            JsonValue::String(s) => {
                // String context: default vocab
                active.vocab = Some(iri::add_trailing_slash(s));
                Ok(active)
            }

            JsonValue::Object(map) => {
                // Check for wrapped @context
                if let Some(inner) = map.get("@context") {
                    return Self::parse(Some(&active), inner);
                }
                parse_context_map(&active, map)
            }

            JsonValue::Array(arr) => {
                // Sequential contexts: process in order
                for ctx in arr {
                    active = Self::parse(Some(&active), ctx)?;
                }
                Ok(active)
            }

            _ => Err(JsonLdError::InvalidContext {
                message: format!("Invalid context type: {context:?}"),
            }),
        }
    }
}

/// Compute @vocab value, handling empty string and relative IRIs
fn compute_vocab(
    base_context: &ParsedContext,
    context: &Map<String, JsonValue>,
    value: &JsonValue,
) -> Result<Option<String>> {
    match value {
        JsonValue::String(s) => {
            if s.is_empty() {
                // Empty string means use @base as @vocab
                let base = context
                    .get("@base")
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string)
                    .or_else(|| base_context.base.clone());
                Ok(base.map(|b| iri::add_trailing_slash(&b)))
            } else if !iri::is_absolute(s) {
                // Relative vocab: join with base
                let base = context
                    .get("@base")
                    .and_then(|v| v.as_str())
                    .or(base_context.base.as_deref());
                if let Some(base) = base {
                    Ok(Some(iri::join(base, s)))
                } else {
                    Ok(Some(iri::add_trailing_slash(s)))
                }
            } else {
                Ok(Some(iri::add_trailing_slash(s)))
            }
        }
        JsonValue::Null => Ok(None),
        _ => Err(JsonLdError::InvalidContext {
            message: format!("@vocab must be a string, got: {value:?}"),
        }),
    }
}

/// Parse a context object (map)
fn parse_context_map(base: &ParsedContext, map: &Map<String, JsonValue>) -> Result<ParsedContext> {
    let mut result = base.clone();

    // First pass: extract @-prefixed keys
    for (key, value) in map {
        if let Some(keyword) = key.strip_prefix('@') {
            match keyword {
                "vocab" => {
                    result.vocab = compute_vocab(base, map, value)?;
                }
                "base" => {
                    if let JsonValue::String(s) = value {
                        result.base = Some(s.clone());
                    } else if value.is_null() {
                        result.base = None;
                    }
                }
                "language" => {
                    result.language = value.as_str().map(std::string::ToString::to_string);
                }
                "version" => {
                    result.version = value.as_f64();
                }
                "protected" => {
                    result.protected = value.as_bool();
                }
                _ => {} // Ignore unknown @-keywords
            }
        }
    }

    // Compute default vocab for resolving compact IRIs
    let default_vocab = result.vocab.clone();

    // Second pass: parse term definitions
    for (key, value) in map {
        if !key.starts_with('@') {
            let entry = parse_context_entry(key, value, map, base, default_vocab.as_deref())?;

            // Track if this entry defines @id
            if entry.id.as_deref() == Some("@id") {
                result.id_key = key.clone();
            }

            // Track if this entry defines @type
            if entry.id.as_deref() == Some("@type") {
                result.type_key = key.clone();
            }

            // Copy entry to full IRI key if it defines a datatype
            if let Some(ref id) = entry.id {
                if entry.type_.is_some() && id != key && !entry.derived {
                    let mut derived_entry = entry.clone();
                    derived_entry.derived = true;
                    result.terms.insert(id.clone(), derived_entry);
                }
            }

            result.terms.insert(key.clone(), entry);
        }
    }

    Ok(result)
}

/// Recursively resolve term references within the same context
fn recursively_get_id(
    term: &str,
    context: &Map<String, JsonValue>,
    visited: &mut Vec<String>,
) -> Result<String> {
    // Check for cycles
    if visited.contains(&term.to_string()) {
        return Err(JsonLdError::InvalidIriMapping {
            term: term.to_string(),
            context: JsonValue::Object(context.clone()),
        });
    }

    if let Some(value) = context.get(term) {
        match value {
            JsonValue::String(s) => {
                if s == term {
                    // Self-reference is a cycle
                    return Err(JsonLdError::InvalidIriMapping {
                        term: term.to_string(),
                        context: JsonValue::Object(context.clone()),
                    });
                }
                // Check if this references another term
                if !s.contains(':') && !s.starts_with('@') {
                    visited.push(term.to_string());
                    return recursively_get_id(s, context, visited);
                }
                Ok(s.clone())
            }
            JsonValue::Object(map) => {
                if let Some(JsonValue::String(id)) = map.get("@id") {
                    Ok(id.clone())
                } else {
                    Ok(term.to_string())
                }
            }
            _ => Ok(term.to_string()),
        }
    } else {
        Ok(term.to_string())
    }
}

/// Resolve a potentially compact IRI using the context
fn resolve_compact_iri(
    value: &str,
    context: &Map<String, JsonValue>,
    base_context: &ParsedContext,
    default_vocab: Option<&str>,
) -> String {
    // Check if it's a compact IRI (has colon but not a full IRI)
    if let Some((prefix, suffix)) = iri::parse_prefix(value) {
        // Try to resolve the prefix from the context
        if let Some(prefix_val) = context.get(&prefix) {
            if let Some(prefix_iri) = prefix_val.as_str() {
                return format!("{prefix_iri}{suffix}");
            } else if let Some(map) = prefix_val.as_object() {
                if let Some(JsonValue::String(prefix_iri)) = map.get("@id") {
                    return format!("{prefix_iri}{suffix}");
                }
            }
        }
        // Try base context
        if let Some(entry) = base_context.terms.get(&prefix) {
            if let Some(ref prefix_iri) = entry.id {
                return format!("{prefix_iri}{suffix}");
            }
        }
    }

    // Not a compact IRI, check if we should prepend vocab
    if !value.starts_with('@') && !iri::any_iri(value) {
        if let Some(vocab) = default_vocab {
            return format!("{vocab}{value}");
        }
    }

    value.to_string()
}

/// Parse @type value into TypeValue
fn parse_type_value(
    value: &JsonValue,
    context: &Map<String, JsonValue>,
    base_context: &ParsedContext,
    default_vocab: Option<&str>,
) -> Result<Option<TypeValue>> {
    match value {
        JsonValue::String(s) => {
            let resolved = resolve_compact_iri(s, context, base_context, default_vocab);
            match resolved.as_str() {
                "@id" => Ok(Some(TypeValue::Id)),
                "@vocab" => Ok(Some(TypeValue::Vocab)),
                "@json" => Ok(Some(TypeValue::Json)),
                _ => Ok(Some(TypeValue::Iri(resolved))),
            }
        }
        JsonValue::Null => Ok(None),
        _ => Err(JsonLdError::InvalidContext {
            message: format!("@type must be a string, got: {value:?}"),
        }),
    }
}

/// Parse @container value
fn parse_container(value: &JsonValue) -> Result<Vec<Container>> {
    match value {
        JsonValue::String(s) => parse_container_string(s).map(|c| vec![c]),
        JsonValue::Array(arr) => {
            let mut containers = Vec::new();
            for item in arr {
                if let JsonValue::String(s) = item {
                    containers.push(parse_container_string(s)?);
                } else {
                    return Err(JsonLdError::InvalidContext {
                        message: format!("@container array items must be strings, got: {item:?}"),
                    });
                }
            }
            Ok(containers)
        }
        _ => Err(JsonLdError::InvalidContext {
            message: format!("@container must be a string or array, got: {value:?}"),
        }),
    }
}

fn parse_container_string(s: &str) -> Result<Container> {
    match s {
        "@list" => Ok(Container::List),
        "@set" => Ok(Container::Set),
        "@language" => Ok(Container::Language),
        "@index" => Ok(Container::Index),
        _ => Err(JsonLdError::InvalidContext {
            message: format!("Unknown @container value: {s}"),
        }),
    }
}

/// Parse a single context entry value
fn parse_context_entry(
    key: &str,
    value: &JsonValue,
    original_context: &Map<String, JsonValue>,
    base_context: &ParsedContext,
    default_vocab: Option<&str>,
) -> Result<ContextEntry> {
    match value {
        JsonValue::String(s) => {
            // Resolve potentially nested references
            let mut visited = Vec::new();
            let resolved = recursively_get_id(s, original_context, &mut visited)?;
            let iri = resolve_compact_iri(&resolved, original_context, base_context, default_vocab);

            Ok(ContextEntry {
                id: Some(iri.clone()),
                is_type_definition: iri == "@type",
                ..Default::default()
            })
        }

        JsonValue::Object(map) => {
            let mut entry = ContextEntry::default();

            for (k, v) in map {
                match k.as_str() {
                    "@id" => {
                        if let JsonValue::String(s) = v {
                            entry.id = Some(resolve_compact_iri(
                                s,
                                original_context,
                                base_context,
                                default_vocab,
                            ));
                        }
                    }
                    "@type" => {
                        entry.type_ =
                            parse_type_value(v, original_context, base_context, default_vocab)?;
                    }
                    "@reverse" => {
                        if let JsonValue::String(s) = v {
                            entry.reverse = Some(resolve_compact_iri(
                                s,
                                original_context,
                                base_context,
                                default_vocab,
                            ));
                        }
                    }
                    "@container" => {
                        entry.container = Some(parse_container(v)?);
                    }
                    "@context" => {
                        let nested = ParsedContext::parse(Some(&ParsedContext::new()), v)?;
                        entry.context = Some(Box::new(nested));
                    }
                    "@language" => {
                        if v.is_null() {
                            entry.language = Some(None); // Explicitly cleared
                        } else {
                            entry.language = Some(v.as_str().map(std::string::ToString::to_string));
                        }
                    }
                    _ => {} // Ignore unknown keys
                }
            }

            // If no @id but @type exists, infer @id from key
            if entry.id.is_none() && entry.reverse.is_none() {
                entry.id = Some(resolve_compact_iri(
                    key,
                    original_context,
                    base_context,
                    default_vocab,
                ));
            }

            // Check if this defines @type
            if entry.id.as_deref() == Some("@type") {
                entry.is_type_definition = true;
            }

            Ok(entry)
        }

        _ => Err(JsonLdError::InvalidContext {
            message: format!("Invalid context entry for key '{key}': {value:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::owl;
    use serde_json::json;

    #[test]
    fn test_default_vocabularies() {
        let ctx = ParsedContext::parse(None, &json!({"@vocab": "https://schema.org/"})).unwrap();
        assert_eq!(ctx.type_key, "@type");
        assert_eq!(ctx.vocab, Some("https://schema.org/".to_string()));
    }

    #[test]
    fn test_map_context_parsing() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "owl": owl::NS,
                "ex": "http://example.org/ns#"
            }),
        )
        .unwrap();

        assert_eq!(ctx.get("owl").unwrap().id, Some(owl::NS.to_string()));
        assert_eq!(
            ctx.get("ex").unwrap().id,
            Some("http://example.org/ns#".to_string())
        );
    }

    #[test]
    fn test_dependent_context_one_level() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "nc": "http://release.niem.gov/niem/niem-core/4.0/#",
                "name": "nc:PersonName"
            }),
        )
        .unwrap();

        assert_eq!(
            ctx.get("name").unwrap().id,
            Some("http://release.niem.gov/niem/niem-core/4.0/#PersonName".to_string())
        );
    }

    #[test]
    fn test_dependent_context_two_levels() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "clri": "https://purl.imsglobal.org/spec/clr/vocab#",
                "Address": "dtAddress",
                "dtAddress": "clri:dtAddress"
            }),
        )
        .unwrap();

        assert_eq!(
            ctx.get("Address").unwrap().id,
            Some("https://purl.imsglobal.org/spec/clr/vocab#dtAddress".to_string())
        );
    }

    #[test]
    fn test_cyclic_context() {
        let result = ParsedContext::parse(None, &json!({"foo": "foo"}));
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_contexts() {
        let ctx = ParsedContext::parse(
            None,
            &json!([
                {"schema": "http://schema.org/"},
                {"owl": owl::NS, "ex": "http://example.org/ns#"}
            ]),
        )
        .unwrap();

        assert_eq!(
            ctx.get("schema").unwrap().id,
            Some("http://schema.org/".to_string())
        );
        assert_eq!(ctx.get("owl").unwrap().id, Some(owl::NS.to_string()));
    }

    #[test]
    fn test_reverse_refs() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "derivedWorks": {"@reverse": "schema:isBasedOn"}
            }),
        )
        .unwrap();

        assert_eq!(
            ctx.get("derivedWorks").unwrap().reverse,
            Some("http://schema.org/isBasedOn".to_string())
        );
    }

    #[test]
    fn test_type_only() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "ical": "http://www.w3.org/2002/12/cal/ical#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "ical:dtstart": {"@type": "xsd:dateTime"}
            }),
        )
        .unwrap();

        let entry = ctx.get("ical:dtstart").unwrap();
        assert_eq!(
            entry.id,
            Some("http://www.w3.org/2002/12/cal/ical#dtstart".to_string())
        );
        assert_eq!(
            entry.type_,
            Some(TypeValue::Iri(
                "http://www.w3.org/2001/XMLSchema#dateTime".to_string()
            ))
        );
    }

    #[test]
    fn test_blank_vocab() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "@base": "https://hub.flur.ee/some/ledger/",
                "@vocab": ""
            }),
        )
        .unwrap();

        assert_eq!(
            ctx.vocab,
            Some("https://hub.flur.ee/some/ledger/".to_string())
        );
    }

    #[test]
    fn test_container_values() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "post": {"@id": "schema:blogPost", "@container": "@set"}
            }),
        )
        .unwrap();

        let entry = ctx.get("post").unwrap();
        assert_eq!(entry.container, Some(vec![Container::Set]));
    }

    #[test]
    fn test_nil_context_clears() {
        let base = ParsedContext::parse(None, &json!({"schema": "http://schema.org/"})).unwrap();
        let cleared = ParsedContext::parse(Some(&base), &JsonValue::Null).unwrap();
        assert!(cleared.terms.is_empty());
    }

    #[test]
    fn test_to_json_round_trip() {
        let original = json!({
            "@vocab": "https://schema.org/",
            "@base": "https://example.com/",
            "schema": "http://schema.org/",
            "name": {"@id": "http://schema.org/name", "@type": "@id"},
            "nick": {"@id": "http://xmlns.com/foaf/0.1/nick", "@container": "@list"},
            "derivedWorks": {"@reverse": "http://schema.org/isBasedOn"}
        });

        let parsed = ParsedContext::parse(None, &original).unwrap();
        let reconstructed = parsed.to_json();

        // Re-parse the reconstructed JSON
        let reparsed = ParsedContext::parse(None, &reconstructed).unwrap();

        // Verify structural equivalence
        assert_eq!(parsed.vocab, reparsed.vocab);
        assert_eq!(parsed.base, reparsed.base);

        // Check prefix term
        assert_eq!(
            reparsed.get("schema").unwrap().id,
            Some("http://schema.org/".to_string())
        );

        // Check @type: @id
        let name = reparsed.get("name").unwrap();
        assert_eq!(name.id, Some("http://schema.org/name".to_string()));
        assert_eq!(name.type_, Some(TypeValue::Id));

        // Check @container: @list
        let nick = reparsed.get("nick").unwrap();
        assert_eq!(nick.id, Some("http://xmlns.com/foaf/0.1/nick".to_string()));
        assert_eq!(nick.container, Some(vec![Container::List]));

        // Check @reverse
        let derived = reparsed.get("derivedWorks").unwrap();
        assert_eq!(
            derived.reverse,
            Some("http://schema.org/isBasedOn".to_string())
        );
    }

    #[test]
    fn test_to_json_empty_context() {
        let ctx = ParsedContext::new();
        assert_eq!(ctx.to_json(), JsonValue::Null);
    }

    #[test]
    fn test_to_json_simple_prefix() {
        let ctx = ParsedContext::parse(None, &json!({"ex": "http://example.org/"})).unwrap();

        let json = ctx.to_json();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("ex").unwrap(), "http://example.org/");
    }
}
