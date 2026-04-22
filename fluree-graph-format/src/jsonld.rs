//! JSON-LD graph formatter
//!
//! Formats a `Graph` as JSON-LD: `{"@context": ..., "@graph": [...]}`

use crate::policy::{BlankNodePolicy, ContextPolicy, TypeHandling};
use fluree_graph_ir::datatype::iri as dt_iri;
use fluree_graph_ir::{BlankId, Datatype, Graph, LiteralValue, Term};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Type alias for IRI compaction functions used in JSON-LD formatting.
type IriCompactor = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Configuration for JSON-LD formatting
#[derive(Clone, Default)]
pub struct JsonLdFormatConfig {
    /// How to handle @context
    pub context_policy: ContextPolicy,

    /// How to format blank node IDs
    pub blank_node_policy: BlankNodePolicy,

    /// How to handle rdf:type predicate
    pub type_handling: TypeHandling,

    /// Always wrap property values in arrays (CONSTRUCT parity)
    ///
    /// When true, all property values are arrays, even single values.
    /// When false (default), single values are scalars, multiple values are arrays.
    pub multicardinal_arrays: bool,

    /// Deduplicate identical values within a property (CONSTRUCT parity)
    ///
    /// When true, duplicate values for the same predicate are removed.
    /// When false (default), duplicates are preserved.
    pub dedupe_values: bool,

    /// Optional IRI compaction function for vocab positions (predicates and @type values)
    ///
    /// If None, IRIs are output in expanded form.
    compactor_vocab: Option<IriCompactor>,

    /// Optional IRI compaction function for `@id` positions (node identifiers)
    ///
    /// If None, falls back to `compactor_vocab` when present; otherwise expanded IRIs.
    compactor_id: Option<IriCompactor>,
}

impl std::fmt::Debug for JsonLdFormatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonLdFormatConfig")
            .field("context_policy", &self.context_policy)
            .field("blank_node_policy", &self.blank_node_policy)
            .field("type_handling", &self.type_handling)
            .field("multicardinal_arrays", &self.multicardinal_arrays)
            .field("dedupe_values", &self.dedupe_values)
            .field(
                "compactor_vocab",
                &self.compactor_vocab.as_ref().map(|_| "<fn>"),
            )
            .field("compactor_id", &self.compactor_id.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl JsonLdFormatConfig {
    /// Create a new config with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the context policy
    pub fn with_context_policy(mut self, policy: ContextPolicy) -> Self {
        self.context_policy = policy;
        self
    }

    /// Set the blank node policy
    pub fn with_blank_node_policy(mut self, policy: BlankNodePolicy) -> Self {
        self.blank_node_policy = policy;
        self
    }

    /// Set the type handling
    pub fn with_type_handling(mut self, handling: TypeHandling) -> Self {
        self.type_handling = handling;
        self
    }

    /// Enable multicardinal arrays (all values as arrays)
    pub fn with_multicardinal_arrays(mut self, enabled: bool) -> Self {
        self.multicardinal_arrays = enabled;
        self
    }

    /// Enable value deduplication
    pub fn with_dedupe_values(mut self, enabled: bool) -> Self {
        self.dedupe_values = enabled;
        self
    }

    /// Set the IRI compactor function
    pub fn with_compactor<F>(mut self, compactor: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        self.compactor_vocab = Some(Arc::new(compactor));
        self
    }

    /// Set the `@id` IRI compactor function (vocab=false semantics)
    pub fn with_id_compactor<F>(mut self, compactor: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        self.compactor_id = Some(Arc::new(compactor));
        self
    }

    /// Create a config for CONSTRUCT query output
    ///
    /// This convenience builder encapsulates all CONSTRUCT-specific settings:
    /// - `rdf:type` → `@type` conversion
    /// - Singleton values unwrapped (arrays only for multi-values)
    /// - Value deduplication (duplicate values removed)
    /// - Deterministic blank node IDs
    ///
    /// # Arguments
    ///
    /// * `orig_context` - Original @context from the query (for output), or None
    /// * `compactor` - Function to compact expanded IRIs to prefixed form
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_graph_format::JsonLdFormatConfig;
    ///
    /// let config = JsonLdFormatConfig::construct_parity(None, |iri| iri.to_string());
    /// assert!(config.multicardinal_arrays);
    /// assert!(config.dedupe_values);
    /// ```
    pub fn construct_parity<F>(orig_context: Option<JsonValue>, compactor: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let context_policy = match orig_context {
            Some(ctx) => ContextPolicy::UseOriginal(ctx),
            None => ContextPolicy::None,
        };

        Self::new()
            .with_context_policy(context_policy)
            .with_type_handling(TypeHandling::AsAtType)
            .with_blank_node_policy(BlankNodePolicy::Deterministic)
            // For CONSTRUCT (JSON-LD): always use arrays for values.
            // (The API layer may override this for SPARQL CONSTRUCT output.)
            .with_multicardinal_arrays(true)
            .with_dedupe_values(true)
            .with_compactor(compactor)
    }

    /// Create a config for expanded graph output (debugging/export)
    ///
    /// Outputs fully expanded IRIs with no compaction and no context.
    /// Useful for debugging or when you need raw RDF graph output.
    ///
    /// - No @context in output
    /// - All IRIs in expanded form
    /// - `rdf:type` → `@type` conversion
    /// - Deterministic blank node IDs
    pub fn expanded_graph() -> Self {
        Self::new()
            .with_context_policy(ContextPolicy::None)
            .with_type_handling(TypeHandling::AsAtType)
            .with_blank_node_policy(BlankNodePolicy::Deterministic)
    }

    /// Compact an IRI for vocab positions (predicates and @type values)
    fn compact_vocab_iri(&self, iri: &str) -> String {
        match &self.compactor_vocab {
            Some(f) => f(iri),
            None => iri.to_string(),
        }
    }

    /// Compact an IRI for `@id` positions (node identifiers)
    fn compact_id_iri(&self, iri: &str) -> String {
        if let Some(f) = &self.compactor_id {
            return f(iri);
        }
        // Fallback (best effort) if caller only provided one compactor.
        self.compact_vocab_iri(iri)
    }
}

/// Blank node renamer for deterministic output
struct BlankNodeRenamer {
    policy: BlankNodePolicy,
    counter: u32,
    mapping: HashMap<String, String>,
}

impl BlankNodeRenamer {
    fn new(policy: BlankNodePolicy) -> Self {
        Self {
            policy,
            counter: 0,
            mapping: HashMap::new(),
        }
    }

    /// Rename a blank node ID according to the policy
    fn rename(&mut self, id: &BlankId) -> String {
        let original = id.as_str();

        match &self.policy {
            BlankNodePolicy::PreserveLabeled => {
                // Keep original label
                format!("_:{original}")
            }
            BlankNodePolicy::Deterministic => {
                // Rewrite to _:b0, _:b1, etc.
                if let Some(renamed) = self.mapping.get(original) {
                    renamed.clone()
                } else {
                    let renamed = format!("_:b{}", self.counter);
                    self.counter += 1;
                    self.mapping.insert(original.to_string(), renamed.clone());
                    renamed
                }
            }
            BlankNodePolicy::FlureeStyle => {
                // Keep original (Fluree-style IDs should already be in the right format)
                format!("_:{original}")
            }
        }
    }
}

/// Format a Graph as JSON-LD
///
/// Produces: `{"@context": ..., "@graph": [...]}`
///
/// The graph should be sorted before calling this function for deterministic output.
///
/// # List support
///
/// Triples with `list_index` are grouped by (subject, predicate) and formatted as:
/// ```json
/// "pred": {"@list": [item0, item1, item2]}
/// ```
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Graph, Term};
/// use fluree_graph_format::{format_jsonld, JsonLdFormatConfig};
///
/// let mut graph = Graph::new();
/// graph.add_triple(
///     Term::iri("http://example.org/alice"),
///     Term::iri("http://xmlns.com/foaf/0.1/name"),
///     Term::string("Alice"),
/// );
/// graph.sort();
///
/// let config = JsonLdFormatConfig::default();
/// let json = format_jsonld(&graph, &config);
/// ```
pub fn format_jsonld(graph: &Graph, config: &JsonLdFormatConfig) -> JsonValue {
    let mut output = Map::new();

    // Add context based on policy
    if let Some(ctx) = config.context_policy.context() {
        output.insert("@context".to_string(), ctx.clone());
    }

    // Initialize blank node renamer
    let mut bnode_renamer = BlankNodeRenamer::new(config.blank_node_policy.clone());

    // Group triples by subject, then by predicate
    // This allows us to detect and handle list predicates
    let mut subjects: BTreeMap<String, SubjectData> = BTreeMap::new();

    for triple in graph.iter() {
        let subj_key = term_to_subject_key(&triple.s, config, &mut bnode_renamer);

        let subj_data = subjects
            .entry(subj_key.clone())
            .or_insert_with(|| SubjectData::new(subj_key.clone()));

        subj_data.add_triple(triple);
    }

    // Convert grouped data to JSON-LD nodes
    let mut nodes: BTreeMap<String, Map<String, JsonValue>> = BTreeMap::new();

    for (subj_key, subj_data) in subjects {
        let node = subj_data.into_jsonld_node(config, &mut bnode_renamer);
        nodes.insert(subj_key, node);
    }

    // Post-process: wrap single values in arrays if multicardinal_arrays is enabled
    // Note: @list values should NOT be wrapped
    if config.multicardinal_arrays {
        for node in nodes.values_mut() {
            wrap_values_in_arrays(node);
        }
    }

    // Build @graph array
    let graph_array: Vec<JsonValue> = nodes.into_values().map(JsonValue::Object).collect();

    output.insert("@graph".to_string(), JsonValue::Array(graph_array));

    JsonValue::Object(output)
}

/// Intermediate structure for grouping triples by predicate
struct SubjectData {
    id: String,
    /// Predicates mapped to their values
    /// Each predicate has a list of (list_index, triple) pairs
    predicates: BTreeMap<String, Vec<(Option<i32>, fluree_graph_ir::Triple)>>,
}

impl SubjectData {
    fn new(id: String) -> Self {
        Self {
            id,
            predicates: BTreeMap::new(),
        }
    }

    fn add_triple(&mut self, triple: &fluree_graph_ir::Triple) {
        let pred_iri = match &triple.p {
            Term::Iri(iri) => iri.to_string(),
            _ => return, // Predicate must be IRI
        };

        self.predicates
            .entry(pred_iri)
            .or_default()
            .push((triple.list_index, triple.clone()));
    }

    fn into_jsonld_node(
        self,
        config: &JsonLdFormatConfig,
        bnode_renamer: &mut BlankNodeRenamer,
    ) -> Map<String, JsonValue> {
        let mut node = Map::new();
        node.insert("@id".to_string(), JsonValue::String(self.id.clone()));

        // Track seen values for deduplication
        let mut seen_values: HashSet<String> = HashSet::new();

        for (pred_iri, triples) in self.predicates {
            // Check if this predicate is a list (any triple has list_index)
            let is_list = triples.iter().any(|(idx, _)| idx.is_some());

            // Handle rdf:type specially
            if pred_iri == dt_iri::RDF_TYPE && config.type_handling.use_at_type() {
                for (_, triple) in &triples {
                    add_type_value(&mut node, &triple.o, config, bnode_renamer);
                }
                continue;
            }

            let pred_key = config.compact_vocab_iri(&pred_iri);

            if is_list {
                // Format as @list
                let list_value = format_list_value(&triples, config, bnode_renamer);
                node.insert(pred_key, list_value);
            } else {
                // Normal multi-valued predicate
                for (_, triple) in triples {
                    let obj_val = term_to_object(&triple.o, config, bnode_renamer);

                    // Dedupe check
                    if config.dedupe_values {
                        let value_str = obj_val.to_string();
                        if seen_values.contains(&value_str) {
                            continue; // Skip duplicate
                        }
                        seen_values.insert(value_str);
                    }

                    add_property(&mut node, &pred_key, obj_val);
                }
            }
        }

        node
    }
}

/// Format a list of triples as a JSON-LD @list value
fn format_list_value(
    triples: &[(Option<i32>, fluree_graph_ir::Triple)],
    config: &JsonLdFormatConfig,
    bnode_renamer: &mut BlankNodeRenamer,
) -> JsonValue {
    // Sort by list index
    let mut sorted: Vec<_> = triples
        .iter()
        .filter_map(|(idx, t)| idx.map(|i| (i, t)))
        .collect();
    sorted.sort_by_key(|(i, _)| *i);

    // Build list array
    let list_items: Vec<JsonValue> = sorted
        .into_iter()
        .map(|(_, triple)| term_to_object(&triple.o, config, bnode_renamer))
        .collect();

    json!({"@list": list_items})
}

/// Wrap all non-@id, non-@type values in arrays
///
/// Skips values that are already @list objects (they should not be wrapped).
fn wrap_values_in_arrays(node: &mut Map<String, JsonValue>) {
    let keys: Vec<String> = node
        .keys()
        .filter(|k| *k != "@id" && *k != "@type")
        .cloned()
        .collect();

    for key in keys {
        if let Some(value) = node.get_mut(&key) {
            // Skip if already an array
            if value.is_array() {
                continue;
            }

            // Skip @list objects - they shouldn't be wrapped in arrays
            if let Some(obj) = value.as_object() {
                if obj.contains_key("@list") {
                    continue;
                }
            }

            let v = std::mem::replace(value, JsonValue::Null);
            *value = JsonValue::Array(vec![v]);
        }
    }
}

/// Convert a term to a subject key string
fn term_to_subject_key(
    term: &Term,
    config: &JsonLdFormatConfig,
    bnode_renamer: &mut BlankNodeRenamer,
) -> String {
    match term {
        Term::Iri(iri) => config.compact_id_iri(iri),
        Term::BlankNode(id) => bnode_renamer.rename(id),
        Term::Literal { .. } => panic!("Literal cannot be subject"),
    }
}

/// Convert a term to a JSON-LD object value
fn term_to_object(
    term: &Term,
    config: &JsonLdFormatConfig,
    bnode_renamer: &mut BlankNodeRenamer,
) -> JsonValue {
    match term {
        Term::Iri(iri) => {
            json!({"@id": config.compact_id_iri(iri)})
        }
        Term::BlankNode(id) => {
            json!({"@id": bnode_renamer.rename(id)})
        }
        Term::Literal {
            value,
            datatype,
            language,
        } => format_literal(value, datatype, language.as_deref()),
    }
}

/// Format a literal value as JSON-LD
fn format_literal(value: &LiteralValue, datatype: &Datatype, language: Option<&str>) -> JsonValue {
    // Handle language-tagged strings
    if let Some(lang) = language {
        return json!({
            "@value": literal_value_to_json(value),
            "@language": lang
        });
    }

    // Handle values with inferable types (output as plain JSON values)
    match value {
        LiteralValue::Boolean(b) => json!(b),
        LiteralValue::Integer(i) => json!(i),
        LiteralValue::Double(d) => {
            if d.is_nan() {
                json!({"@value": "NaN", "@type": dt_iri::XSD_DOUBLE})
            } else if d.is_infinite() {
                let s = if d.is_sign_positive() { "INF" } else { "-INF" };
                json!({"@value": s, "@type": dt_iri::XSD_DOUBLE})
            } else {
                json!(d)
            }
        }
        LiteralValue::String(s) => {
            // Plain strings: output without datatype annotation
            if datatype.is_xsd_string() {
                json!(s.as_ref())
            } else {
                // Typed string: include datatype
                json!({
                    "@value": s.as_ref(),
                    "@type": datatype.as_iri()
                })
            }
        }
        LiteralValue::Json(canonical_json) => {
            // Parse the canonical JSON string back to a Value
            let parsed: JsonValue = serde_json::from_str(canonical_json).unwrap_or_else(|_| {
                // Fallback: treat as string if parsing fails
                JsonValue::String(canonical_json.to_string())
            });
            json!({
                "@value": parsed,
                "@type": "@json"
            })
        }
    }
}

/// Convert a LiteralValue to a plain JSON value (for @value field)
fn literal_value_to_json(value: &LiteralValue) -> JsonValue {
    match value {
        LiteralValue::String(s) => json!(s.as_ref()),
        LiteralValue::Boolean(b) => json!(b),
        LiteralValue::Integer(i) => json!(i),
        LiteralValue::Double(d) => json!(d),
        LiteralValue::Json(s) => {
            serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string()))
        }
    }
}

/// Add a @type value to a node
fn add_type_value(
    node: &mut Map<String, JsonValue>,
    object: &Term,
    config: &JsonLdFormatConfig,
    bnode_renamer: &mut BlankNodeRenamer,
) {
    // Extract the type IRI
    let type_iri = match object {
        Term::Iri(iri) => config.compact_vocab_iri(iri),
        Term::BlankNode(id) => bnode_renamer.rename(id),
        Term::Literal { .. } => return, // Types should be IRIs
    };

    match node.get_mut("@type") {
        None => {
            // First type: insert as string
            node.insert("@type".to_string(), JsonValue::String(type_iri));
        }
        Some(JsonValue::Array(arr)) => {
            // Already an array: push if not duplicate
            let val = JsonValue::String(type_iri);
            if !arr.contains(&val) {
                arr.push(val);
            }
        }
        Some(existing) => {
            // Single value: convert to array if different
            let prev = existing.as_str().unwrap_or_default().to_string();
            if prev != type_iri {
                *existing =
                    JsonValue::Array(vec![JsonValue::String(prev), JsonValue::String(type_iri)]);
            }
        }
    }
}

/// Add a property to a node, preserving duplicates as arrays
fn add_property(node: &mut Map<String, JsonValue>, predicate: &str, value: JsonValue) {
    match node.get_mut(predicate) {
        None => {
            // First value for this predicate
            node.insert(predicate.to_string(), value);
        }
        Some(JsonValue::Array(arr)) => {
            // Already an array: push
            arr.push(value);
        }
        Some(existing) => {
            // Single value: convert to array
            let prev = std::mem::replace(existing, JsonValue::Null);
            *existing = JsonValue::Array(vec![prev, value]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::Term;
    use fluree_vocab::rdf;
    use pretty_assertions::assert_eq;

    fn make_simple_graph() -> Graph {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri(rdf::TYPE),
            Term::iri("http://xmlns.com/foaf/0.1/Person"),
        );

        graph.sort();
        graph
    }

    #[test]
    fn test_format_simple_graph() {
        let graph = make_simple_graph();
        let config = JsonLdFormatConfig::default();

        let result = format_jsonld(&graph, &config);

        assert!(result.get("@graph").is_some());
        let graph_arr = result["@graph"].as_array().unwrap();
        assert_eq!(graph_arr.len(), 1);

        let node = &graph_arr[0];
        assert_eq!(node["@id"], "http://example.org/alice");
    }

    #[test]
    fn test_format_with_type_as_at_type() {
        let graph = make_simple_graph();
        let config = JsonLdFormatConfig::default().with_type_handling(TypeHandling::AsAtType);

        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];
        assert_eq!(node["@type"], "http://xmlns.com/foaf/0.1/Person");
        // Should not have rdf:type
        assert!(node.get(rdf::TYPE).is_none());
    }

    #[test]
    fn test_format_with_type_as_rdf_type() {
        let graph = make_simple_graph();
        let config = JsonLdFormatConfig::default().with_type_handling(TypeHandling::AsRdfType);

        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];
        // Should NOT have @type
        assert!(node.get("@type").is_none());
        // Should have rdf:type
        assert!(node.get(rdf::TYPE).is_some());
    }

    #[test]
    fn test_format_with_context() {
        let graph = make_simple_graph();
        let context = json!({"foaf": "http://xmlns.com/foaf/0.1/"});
        let config =
            JsonLdFormatConfig::default().with_context_policy(ContextPolicy::UseOriginal(context));

        let result = format_jsonld(&graph, &config);

        assert!(result.get("@context").is_some());
        assert_eq!(result["@context"]["foaf"], "http://xmlns.com/foaf/0.1/");
    }

    #[test]
    fn test_format_with_compactor() {
        let graph = make_simple_graph();
        let config = JsonLdFormatConfig::default().with_compactor(|iri| {
            if let Some(suffix) = iri.strip_prefix("http://xmlns.com/foaf/0.1/") {
                format!("foaf:{suffix}")
            } else if let Some(suffix) = iri.strip_prefix("http://example.org/") {
                format!("ex:{suffix}")
            } else {
                iri.to_string()
            }
        });

        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];
        assert_eq!(node["@id"], "ex:alice");
        assert_eq!(node["@type"], "foaf:Person");
        assert!(node.get("foaf:name").is_some());
    }

    #[test]
    fn test_format_multiple_types() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri(rdf::TYPE),
            Term::iri("http://xmlns.com/foaf/0.1/Person"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri(rdf::TYPE),
            Term::iri("http://xmlns.com/foaf/0.1/Agent"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let types = &result["@graph"][0]["@type"];
        assert!(types.is_array());
        let arr = types.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_format_multiple_values_same_predicate() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Al"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Ali"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let nicks = &result["@graph"][0]["http://xmlns.com/foaf/0.1/nick"];
        assert!(nicks.is_array());
        let arr = nicks.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_format_language_tagged_string() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::lang_string("Alicia", "es"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let name = &result["@graph"][0]["http://xmlns.com/foaf/0.1/name"];
        assert_eq!(name["@value"], "Alicia");
        assert_eq!(name["@language"], "es");
    }

    #[test]
    fn test_format_typed_literal() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/age"),
            Term::integer(30),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let age = &result["@graph"][0]["http://xmlns.com/foaf/0.1/age"];
        // Integer should be output as plain JSON number
        assert_eq!(age, &json!(30));
    }

    #[test]
    fn test_format_blank_node_subject() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::blank("person1"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Unknown"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];
        assert_eq!(node["@id"], "_:person1");
    }

    #[test]
    fn test_format_blank_node_object() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/knows"),
            Term::blank("person1"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let knows = &result["@graph"][0]["http://xmlns.com/foaf/0.1/knows"];
        assert_eq!(knows["@id"], "_:person1");
    }

    #[test]
    fn test_format_nan_and_infinity() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/test"),
            Term::iri("http://example.org/nan"),
            Term::double(f64::NAN),
        );

        graph.add_triple(
            Term::iri("http://example.org/test"),
            Term::iri("http://example.org/inf"),
            Term::double(f64::INFINITY),
        );

        graph.add_triple(
            Term::iri("http://example.org/test"),
            Term::iri("http://example.org/neginf"),
            Term::double(f64::NEG_INFINITY),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];

        let nan = &node["http://example.org/nan"];
        assert_eq!(nan["@value"], "NaN");

        let inf = &node["http://example.org/inf"];
        assert_eq!(inf["@value"], "INF");

        let neginf = &node["http://example.org/neginf"];
        assert_eq!(neginf["@value"], "-INF");
    }

    #[test]
    fn test_format_json_literal() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/test"),
            Term::iri("http://example.org/data"),
            Term::json(r#"{"key":"value"}"#),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);

        let data = &result["@graph"][0]["http://example.org/data"];
        assert_eq!(data["@type"], "@json");
        assert!(data["@value"].is_object());
        assert_eq!(data["@value"]["key"], "value");
    }

    // New tests for BlankNodePolicy

    #[test]
    fn test_blank_node_policy_deterministic() {
        let mut graph = Graph::new();

        // Add triples with different blank node IDs
        graph.add_triple(
            Term::blank("xyz123"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Person 1"),
        );

        graph.add_triple(
            Term::blank("abc456"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Person 2"),
        );

        // xyz comes after abc in sort order
        graph.sort();

        let config =
            JsonLdFormatConfig::default().with_blank_node_policy(BlankNodePolicy::Deterministic);
        let result = format_jsonld(&graph, &config);

        let nodes = result["@graph"].as_array().unwrap();
        // After sorting, abc comes before xyz, so abc becomes _:b0
        assert_eq!(nodes[0]["@id"], "_:b0");
        assert_eq!(nodes[1]["@id"], "_:b1");
    }

    #[test]
    fn test_blank_node_policy_preserve_labeled() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::blank("myCustomLabel"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Person"),
        );

        graph.sort();

        let config =
            JsonLdFormatConfig::default().with_blank_node_policy(BlankNodePolicy::PreserveLabeled);
        let result = format_jsonld(&graph, &config);

        let node = &result["@graph"][0];
        assert_eq!(node["@id"], "_:myCustomLabel");
    }

    // New tests for multicardinal_arrays and dedupe_values

    #[test]
    fn test_multicardinal_arrays() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default().with_multicardinal_arrays(true);
        let result = format_jsonld(&graph, &config);

        let name = &result["@graph"][0]["http://xmlns.com/foaf/0.1/name"];
        // Should be an array even with single value
        assert!(name.is_array());
        assert_eq!(name.as_array().unwrap().len(), 1);
        assert_eq!(name[0], "Alice");
    }

    #[test]
    fn test_dedupe_values() {
        let mut graph = Graph::new();

        // Add duplicate values
        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Al"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Al"), // duplicate
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Ali"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::default().with_dedupe_values(true);
        let result = format_jsonld(&graph, &config);

        let nicks = &result["@graph"][0]["http://xmlns.com/foaf/0.1/nick"];
        assert!(nicks.is_array());
        let arr = nicks.as_array().unwrap();
        // Should have 2 values, not 3
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_construct_parity_mode() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Al"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/nick"),
            Term::string("Al"), // duplicate
        );

        graph.sort();

        // Construct mode: singletons unwrapped + dedupe
        let config = JsonLdFormatConfig::default()
            .with_multicardinal_arrays(false)
            .with_dedupe_values(true);

        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        // name should be scalar (singleton unwrapped)
        assert!(node["http://xmlns.com/foaf/0.1/name"].is_string());
        assert_eq!(node["http://xmlns.com/foaf/0.1/name"], "Alice");

        // nick should be scalar with deduplicated value
        let nicks = &node["http://xmlns.com/foaf/0.1/nick"];
        assert!(nicks.is_string());
        assert_eq!(nicks, "Al"); // Only "Al" (deduplicated)
    }

    #[test]
    fn test_expanded_graph_config() {
        let mut graph = Graph::new();

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri(rdf::TYPE),
            Term::iri("http://xmlns.com/foaf/0.1/Person"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        graph.sort();

        let config = JsonLdFormatConfig::expanded_graph();
        let result = format_jsonld(&graph, &config);

        // No @context
        assert!(result.get("@context").is_none());

        let node = &result["@graph"][0];

        // Fully expanded IRI
        assert_eq!(node["@id"], "http://example.org/alice");
        assert_eq!(node["@type"], "http://xmlns.com/foaf/0.1/Person");

        // Property with expanded IRI key
        assert!(node.get("http://xmlns.com/foaf/0.1/name").is_some());
    }

    // =========================================================================
    // @list formatting tests
    // =========================================================================

    #[test]
    fn test_list_with_scalar_literals() {
        let mut graph = Graph::new();

        // Add list items with indices (out of order to test sorting)
        graph.add_list_item(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/favorites"),
            Term::string("Charlie"),
            2,
        );
        graph.add_list_item(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/favorites"),
            Term::string("Alice"),
            0,
        );
        graph.add_list_item(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/favorites"),
            Term::string("Bob"),
            1,
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        // Should have @list with items in order
        let favorites = &node["http://example.org/favorites"];
        assert!(
            favorites.get("@list").is_some(),
            "Should have @list container"
        );

        let list = favorites["@list"].as_array().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0], "Alice");
        assert_eq!(list[1], "Bob");
        assert_eq!(list[2], "Charlie");
    }

    #[test]
    fn test_list_with_iri_objects() {
        let mut graph = Graph::new();

        graph.add_list_item(
            Term::iri("http://example.org/collection"),
            Term::iri("http://example.org/members"),
            Term::iri("http://example.org/bob"),
            1,
        );
        graph.add_list_item(
            Term::iri("http://example.org/collection"),
            Term::iri("http://example.org/members"),
            Term::iri("http://example.org/alice"),
            0,
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        let members = &node["http://example.org/members"];
        let list = members["@list"].as_array().unwrap();

        assert_eq!(list.len(), 2);
        assert_eq!(list[0]["@id"], "http://example.org/alice");
        assert_eq!(list[1]["@id"], "http://example.org/bob");
    }

    #[test]
    fn test_list_preserves_duplicates() {
        let mut graph = Graph::new();

        // Same value at different indices - must be preserved
        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("repeat"),
            0,
        );
        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("middle"),
            1,
        );
        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("repeat"),
            2,
        );

        graph.sort();

        // Even with dedupe_values enabled, list indices should preserve duplicates
        let config = JsonLdFormatConfig::default().with_dedupe_values(true);
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        let list = node["http://example.org/p"]["@list"].as_array().unwrap();
        assert_eq!(
            list.len(),
            3,
            "List should preserve all items including duplicates"
        );
        assert_eq!(list[0], "repeat");
        assert_eq!(list[1], "middle");
        assert_eq!(list[2], "repeat");
    }

    #[test]
    fn test_list_with_typed_literals() {
        use fluree_graph_ir::{Datatype, LiteralValue};

        let mut graph = Graph::new();

        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/numbers"),
            Term::Literal {
                value: LiteralValue::Integer(42),
                datatype: Datatype::xsd_integer(),
                language: None,
            },
            0,
        );
        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/numbers"),
            Term::Literal {
                value: LiteralValue::Integer(100),
                datatype: Datatype::xsd_integer(),
                language: None,
            },
            1,
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        let list = node["http://example.org/numbers"]["@list"]
            .as_array()
            .unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], 42);
        assert_eq!(list[1], 100);
    }

    #[test]
    fn test_list_single_item() {
        let mut graph = Graph::new();

        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("only"),
            0,
        );

        graph.sort();

        let config = JsonLdFormatConfig::default();
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        // Even single-item should be @list
        let list = node["http://example.org/p"]["@list"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], "only");
    }

    #[test]
    fn test_list_not_wrapped_in_array() {
        let mut graph = Graph::new();

        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("a"),
            0,
        );
        graph.add_list_item(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("b"),
            1,
        );

        graph.sort();

        // Even with multicardinal_arrays, @list should not be wrapped
        let config = JsonLdFormatConfig::default().with_multicardinal_arrays(true);
        let result = format_jsonld(&graph, &config);
        let node = &result["@graph"][0];

        // Should be {"@list": [...]} not [{"@list": [...]}]
        let prop = &node["http://example.org/p"];
        assert!(prop.is_object(), "Should be object, not array");
        assert!(prop.get("@list").is_some(), "Should have @list key");
    }
}
