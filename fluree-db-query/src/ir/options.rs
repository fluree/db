//! Query execution options and reasoning modes.
//!
//! `QueryOptions` carries the reasoning configuration the rewriter consumes
//! (`ReasoningModes` plus an optional pre-resolved `SchemaBundleFlakes`).
//! Solution modifiers (LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING) ride on
//! [`Query`](super::Query) directly.
//!
//! `ReasoningModes` is pure config — no behavior, just bit flags and a JSON
//! rule list. The rewriter consumes it as input. Both types live here in `ir`
//! because they're embedded in [`Query`](super::Query).

use std::sync::Arc;

use crate::schema_bundle::SchemaBundleFlakes;

/// Reasoning modes for RDFS / OWL / datalog query rewriting.
///
/// A composite descriptor — multiple modes can be enabled simultaneously.
/// The rewriter inspects this to decide which transformations to apply.
///
/// # OWL-Datalog Mode
///
/// `owl_datalog` extends `owl2rl` with reasoning patterns expressible in
/// Datalog rules but not in standard OWL-RL semantics:
/// - Complex intersections in equivalentClass / subClassOf
/// - Property chains with inverse elements and arbitrary length (≥2)
/// - Nested restrictions and complex class expressions
/// - Enhanced someValuesFrom/allValuesFrom reasoning in equivalences
///
/// This mode is opt-in and separate from standard `owl2rl`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasoningModes {
    /// RDFS reasoning - subclass/subproperty expansion
    pub rdfs: bool,
    /// OWL2-QL reasoning - owl:inverseOf, domain/range type queries
    pub owl2ql: bool,
    /// Datalog rules (tx-time rules) - Phase 3
    pub datalog: bool,
    /// OWL2-RL materialization - Phase 5
    pub owl2rl: bool,
    /// OWL-Datalog: Extended OWL with complex class expressions
    ///
    /// This is a superset of owl2rl that supports additional constructs
    /// expressible in Datalog: complex intersections, property chains with
    /// inverses, nested restrictions, etc.
    ///
    /// Opt-in mode, not enabled by default.
    pub owl_datalog: bool,
    /// Explicitly disable all reasoning (overrides auto-RDFS)
    ///
    /// When true, no reasoning is applied even if hierarchy exists.
    /// This is set by `"reasoning": "none"` in JSON.
    pub explicit_none: bool,
    /// Query-time datalog rules (JSON-LD format)
    ///
    /// These rules are merged with database rules when datalog reasoning is enabled.
    /// Each rule should have `where` and `insert` clauses.
    pub rules: Vec<serde_json::Value>,
}

impl ReasoningModes {
    /// Create with no reasoning modes enabled
    pub fn none() -> Self {
        Self {
            explicit_none: true,
            ..Default::default()
        }
    }

    /// Create with RDFS enabled
    pub fn rdfs() -> Self {
        Self {
            rdfs: true,
            ..Default::default()
        }
    }

    /// Create with OWL2-QL enabled (includes RDFS for subclass expansion)
    pub fn owl2ql() -> Self {
        Self {
            rdfs: true,
            owl2ql: true,
            ..Default::default()
        }
    }

    /// Create with datalog enabled and query-time rules
    pub fn datalog_with_rules(rules: Vec<serde_json::Value>) -> Self {
        Self {
            datalog: true,
            rules,
            ..Default::default()
        }
    }

    /// Builder: enable RDFS
    pub fn with_rdfs(mut self) -> Self {
        self.rdfs = true;
        self
    }

    /// Builder: enable OWL2-QL
    pub fn with_owl2ql(mut self) -> Self {
        self.owl2ql = true;
        self
    }

    /// Builder: enable Datalog rules
    pub fn with_datalog(mut self) -> Self {
        self.datalog = true;
        self
    }

    /// Builder: enable OWL2-RL materialization
    pub fn with_owl2rl(mut self) -> Self {
        self.owl2rl = true;
        self
    }

    /// Builder: enable OWL-Datalog extended reasoning
    ///
    /// This enables complex OWL class expressions that can be expressed in Datalog:
    /// - Complex intersectionOf/unionOf with restrictions
    /// - Property chains with inverse elements and arbitrary length
    /// - Enhanced someValuesFrom/allValuesFrom in equivalences
    pub fn with_owl_datalog(mut self) -> Self {
        self.owl_datalog = true;
        self
    }

    /// Builder: add query-time rules
    ///
    /// Note: This also enables datalog reasoning mode if not already enabled.
    pub fn with_rules(mut self, rules: Vec<serde_json::Value>) -> Self {
        if !rules.is_empty() {
            self.datalog = true;
        }
        self.rules = rules;
        self
    }

    /// Check if any mode is explicitly enabled
    pub fn has_any_enabled(&self) -> bool {
        self.rdfs || self.owl2ql || self.datalog || self.owl2rl || self.owl_datalog
    }

    /// Check if reasoning is explicitly disabled
    pub fn is_disabled(&self) -> bool {
        self.explicit_none
    }

    /// Parse from a JSON value (string or array of strings)
    ///
    /// Valid values:
    /// - `"none"` - disable all reasoning
    /// - `"rdfs"` - RDFS only
    /// - `"owl2ql"` - OWL2-QL (implies RDFS)
    /// - `"datalog"` - Datalog rules
    /// - `"owl2rl"` - OWL2-RL materialization
    /// - `["rdfs", "owl2ql", ...]` - multiple modes
    pub fn from_json(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::default()),
            serde_json::Value::String(s) => Self::parse_single(s),
            serde_json::Value::Array(arr) => {
                let mut modes = Self::default();
                for item in arr {
                    let s = item
                        .as_str()
                        .ok_or_else(|| "reasoning array must contain strings".to_string())?;
                    modes = modes.merge(&Self::parse_single(s)?);
                }
                Ok(modes)
            }
            _ => Err("reasoning must be a string or array of strings".to_string()),
        }
    }

    /// Parse reasoning modes from a query JSON object
    ///
    /// This extracts both the `reasoning` key (mode selection) and the `rules` key
    /// (query-time datalog rules) from a query object.
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_db_query::ir::ReasoningModes;
    /// use serde_json::json;
    ///
    /// let query = json!({
    ///     "reasoning": "datalog",
    ///     "rules": [{
    ///         "@context": {"ex": "http://example.org/"},
    ///         "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
    ///         "insert": {"@id": "?person", "ex:grandparent": "?grandparent"}
    ///     }]
    /// });
    /// let modes = ReasoningModes::from_query_json(&query).unwrap();
    /// assert!(modes.datalog);
    /// assert!(modes.has_rules());
    /// ```
    pub fn from_query_json(query: &serde_json::Value) -> Result<Self, String> {
        // Parse reasoning modes
        let mut modes = if let Some(reasoning) = query.get("reasoning") {
            Self::from_json(reasoning)?
        } else {
            Self::default()
        };

        // Parse query-time rules
        if let Some(rules) = query.get("rules") {
            match rules {
                serde_json::Value::Array(arr) => {
                    modes.rules = arr.clone();
                    // Enable datalog if rules are provided
                    if !modes.rules.is_empty() {
                        modes.datalog = true;
                    }
                }
                serde_json::Value::Null => {}
                _ => {
                    return Err("rules must be an array".to_string());
                }
            }
        }

        Ok(modes)
    }

    /// Parse a single mode string
    fn parse_single(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::none()),
            "rdfs" => Ok(Self {
                rdfs: true,
                ..Default::default()
            }),
            "owl2ql" | "owl-ql" | "owlql" => Ok(Self {
                rdfs: true, // OWL2-QL implies RDFS for subclass expansion
                owl2ql: true,
                ..Default::default()
            }),
            "datalog" => Ok(Self {
                datalog: true,
                ..Default::default()
            }),
            "owl2rl" | "owl-rl" | "owlrl" => Ok(Self {
                owl2rl: true,
                ..Default::default()
            }),
            "owl-datalog" | "owldatalog" | "owl_datalog" => Ok(Self {
                owl_datalog: true,
                // owl-datalog is a superset of owl2rl - enable both
                owl2rl: true,
                ..Default::default()
            }),
            other => Err(format!(
                "unknown reasoning mode '{other}', expected: none, rdfs, owl2ql, datalog, owl2rl, owl-datalog"
            )),
        }
    }

    /// Merge another ReasoningModes into this one (OR of flags)
    fn merge(mut self, other: &Self) -> Self {
        self.rdfs |= other.rdfs;
        self.owl2ql |= other.owl2ql;
        self.datalog |= other.datalog;
        self.owl2rl |= other.owl2rl;
        self.owl_datalog |= other.owl_datalog;
        // explicit_none is only set by "none" alone, not merged
        // rules are combined
        self.rules.extend(other.rules.iter().cloned());
        self
    }

    /// Build from a list of mode name strings (e.g., from config graph).
    ///
    /// Accepts the same names as `parse_single`: "rdfs", "owl2ql",
    /// "owl2rl", "datalog", "owl-datalog", "none".
    /// Unknown names are logged as warnings and skipped.
    pub fn from_mode_strings(names: &[String]) -> Self {
        let mut modes = Self::default();
        for name in names {
            // Config reader returns full IRIs (e.g., "https://ns.flur.ee/db#rdfs").
            // Strip known namespace prefix before parsing.
            let short = name.strip_prefix(fluree_vocab::fluree::DB).unwrap_or(name);
            match Self::parse_single(short) {
                Ok(single) => {
                    if single.explicit_none {
                        // "none" force-disables reasoning; return immediately.
                        // merge() deliberately does not propagate explicit_none,
                        // so we must handle it here.
                        if names.len() > 1 {
                            tracing::warn!(
                                "Config reasoning modes contain 'none' alongside other modes; \
                                 'none' takes precedence"
                            );
                        }
                        return Self::none();
                    }
                    modes = modes.merge(&single);
                }
                Err(e) => {
                    tracing::warn!(mode = %name, "Unknown reasoning mode in config: {e}");
                }
            }
        }
        modes
    }

    /// Check if query-time rules are provided
    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Compute effective reasoning given available hierarchy
    ///
    /// Auto-enables RDFS if:
    /// - RDFS is not already enabled
    /// - User didn't explicitly disable reasoning with "none"
    /// - A schema hierarchy is available
    ///
    /// This means other modes (datalog, owl2rl) can be enabled independently
    /// and RDFS will still be auto-enabled unless explicitly disabled.
    pub fn effective_with_hierarchy(self, hierarchy_available: bool) -> Self {
        if self.explicit_none {
            // User explicitly disabled - no auto-enable
            return self;
        }
        // Auto-enable RDFS if not already enabled and hierarchy exists
        if !self.rdfs && hierarchy_available {
            Self { rdfs: true, ..self }
        } else {
            self
        }
    }
}

/// Reasoning configuration consumed by the rewriter. Solution modifiers
/// (LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING) ride on [`Query`](super::Query)
/// directly.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Reasoning modes for RDFS/OWL reasoning
    ///
    /// Controls pattern expansion based on class/property hierarchies.
    /// Default is to auto-enable RDFS when hierarchy exists.
    ///
    /// Use `reasoning.effective_with_hierarchy(has_hierarchy)` at execution
    /// time to compute the actual modes to apply.
    pub reasoning: ReasoningModes,
    /// Pre-resolved schema bundle flakes projected to `g_id=0`.
    ///
    /// Populated upstream (in `fluree-db-api`) from the ledger's
    /// `f:schemaSource` and transitive `owl:imports` closure. When set, the
    /// runner layers a [`SchemaBundleOverlay`](crate::schema_bundle::SchemaBundleOverlay)
    /// over the query's base overlay for reasoning prep so that hierarchy
    /// extraction and OWL axiom discovery see the full import closure.
    ///
    /// When `None`, reasoning reads schema from `db.g_id` directly.
    pub schema_bundle: Option<Arc<SchemaBundleFlakes>>,
}

impl QueryOptions {
    /// Create new execution options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set reasoning modes
    ///
    /// # Example
    ///
    /// ```
    /// use fluree_db_query::ir::{QueryOptions, ReasoningModes};
    ///
    /// let opts = QueryOptions::new()
    ///     .with_reasoning(ReasoningModes::rdfs().with_owl2ql());
    /// assert!(opts.reasoning.rdfs);
    /// assert!(opts.reasoning.owl2ql);
    /// ```
    pub fn with_reasoning(mut self, modes: ReasoningModes) -> Self {
        self.reasoning = modes;
        self
    }

    /// Attach a pre-resolved schema bundle for reasoning.
    ///
    /// Populated upstream once per (ledger, `to_t`, schema-source) — see
    /// `fluree_db_api::ontology_imports::resolve_schema_bundle`.
    pub fn with_schema_bundle(mut self, bundle: Arc<SchemaBundleFlakes>) -> Self {
        self.schema_bundle = Some(bundle);
        self
    }

    /// Check if any reasoning mode is explicitly enabled
    pub fn has_reasoning(&self) -> bool {
        self.reasoning.has_any_enabled()
    }

    /// Check if reasoning is explicitly disabled
    pub fn is_reasoning_disabled(&self) -> bool {
        self.reasoning.is_disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================
    // QueryOptions tests
    // ========================

    #[test]
    fn test_default_options() {
        let opts = QueryOptions::default();
        // Default reasoning: nothing explicitly enabled
        assert!(!opts.has_reasoning());
        assert!(!opts.is_reasoning_disabled());
    }

    #[test]
    fn test_reasoning_modes() {
        // Default: no explicit reasoning
        let opts = QueryOptions::new();
        assert!(!opts.has_reasoning());
        assert!(!opts.is_reasoning_disabled());

        // With RDFS
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::rdfs());
        assert!(opts.has_reasoning());
        assert!(opts.reasoning.rdfs);
        assert!(!opts.reasoning.owl2ql);

        // With OWL2-QL (includes RDFS)
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::owl2ql());
        assert!(opts.has_reasoning());
        assert!(opts.reasoning.rdfs);
        assert!(opts.reasoning.owl2ql);

        // Explicit none
        let opts = QueryOptions::new().with_reasoning(ReasoningModes::none());
        assert!(!opts.has_reasoning());
        assert!(opts.is_reasoning_disabled());
    }

    // ========================
    // ReasoningModes tests
    // ========================

    #[test]
    fn test_reasoning_modes_default() {
        let modes = ReasoningModes::default();
        assert!(!modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.datalog);
        assert!(!modes.owl2rl);
        assert!(!modes.explicit_none);
        assert!(!modes.has_any_enabled());
        assert!(!modes.is_disabled());
    }

    #[test]
    fn test_reasoning_modes_none() {
        let modes = ReasoningModes::none();
        assert!(!modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(modes.explicit_none);
        assert!(modes.is_disabled());
    }

    #[test]
    fn test_reasoning_modes_rdfs() {
        let modes = ReasoningModes::rdfs();
        assert!(modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.explicit_none);
        assert!(modes.has_any_enabled());
    }

    #[test]
    fn test_reasoning_modes_owl2ql() {
        let modes = ReasoningModes::owl2ql();
        assert!(modes.rdfs); // owl2ql implies rdfs
        assert!(modes.owl2ql);
        assert!(modes.has_any_enabled());
    }

    #[test]
    fn test_reasoning_modes_builders() {
        let modes = ReasoningModes::default()
            .with_rdfs()
            .with_owl2ql()
            .with_datalog();
        assert!(modes.rdfs);
        assert!(modes.owl2ql);
        assert!(modes.datalog);
        assert!(!modes.owl2rl);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_none() {
        let value = serde_json::json!("none");
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.explicit_none);
        assert!(!modes.rdfs);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_rdfs() {
        let value = serde_json::json!("rdfs");
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(!modes.owl2ql);
        assert!(!modes.explicit_none);
    }

    #[test]
    fn test_reasoning_modes_from_json_string_owl2ql() {
        // Test various spellings
        for spelling in &["owl2ql", "owl-ql", "owlql", "OWL2QL", "Owl-Ql"] {
            let value = serde_json::json!(spelling);
            let modes = ReasoningModes::from_json(&value).unwrap();
            assert!(modes.owl2ql, "Failed for spelling: {spelling}");
            assert!(modes.rdfs, "OWL2QL should imply RDFS for: {spelling}");
        }
    }

    #[test]
    fn test_reasoning_modes_from_json_string_owl2rl() {
        for spelling in &["owl2rl", "owl-rl", "owlrl"] {
            let value = serde_json::json!(spelling);
            let modes = ReasoningModes::from_json(&value).unwrap();
            assert!(modes.owl2rl, "Failed for spelling: {spelling}");
        }
    }

    #[test]
    fn test_reasoning_modes_from_json_array() {
        let value = serde_json::json!(["rdfs", "owl2ql"]);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(modes.owl2ql);
        assert!(!modes.datalog);
    }

    #[test]
    fn test_reasoning_modes_from_json_array_multiple() {
        let value = serde_json::json!(["rdfs", "datalog", "owl2rl"]);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(modes.rdfs);
        assert!(modes.datalog);
        assert!(modes.owl2rl);
        assert!(!modes.owl2ql);
    }

    #[test]
    fn test_reasoning_modes_from_json_null() {
        let value = serde_json::json!(null);
        let modes = ReasoningModes::from_json(&value).unwrap();
        assert!(!modes.rdfs);
        assert!(!modes.explicit_none);
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_string() {
        let value = serde_json::json!("invalid_mode");
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown reasoning mode"));
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_type() {
        let value = serde_json::json!(123);
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be a string or array"));
    }

    #[test]
    fn test_reasoning_modes_from_json_invalid_array_element() {
        let value = serde_json::json!(["rdfs", 123]);
        let result = ReasoningModes::from_json(&value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must contain strings"));
    }

    #[test]
    fn test_reasoning_modes_effective_with_hierarchy_auto_rdfs() {
        // Default modes (no explicit setting) + hierarchy available = RDFS enabled
        let modes = ReasoningModes::default();
        let effective = modes.effective_with_hierarchy(true);
        assert!(effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_with_hierarchy_no_auto_without_hierarchy() {
        // Default modes + no hierarchy = RDFS not enabled
        let modes = ReasoningModes::default();
        let effective = modes.effective_with_hierarchy(false);
        assert!(!effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_explicit_none_overrides_auto() {
        // Explicit "none" + hierarchy available = RDFS NOT enabled
        let modes = ReasoningModes::none();
        let effective = modes.effective_with_hierarchy(true);
        assert!(!effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_explicit_rdfs_without_hierarchy() {
        // Explicit RDFS + no hierarchy = RDFS still requested (may warn at runtime)
        let modes = ReasoningModes::rdfs();
        let effective = modes.effective_with_hierarchy(false);
        assert!(effective.rdfs);
    }

    #[test]
    fn test_reasoning_modes_effective_preserves_other_modes() {
        // Auto-RDFS shouldn't affect other modes
        let modes = ReasoningModes::default().with_datalog();
        let effective = modes.effective_with_hierarchy(true);
        assert!(effective.rdfs);
        assert!(effective.datalog);
    }
}
