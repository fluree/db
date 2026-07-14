//! Policy query executor implementation
//!
//! Implements `PolicyQueryExecutor` using the query engine asynchronously.

use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::var_registry::VarRegistry;
use fluree_db_core::{
    DatatypeConstraint, FlakeValue, GraphId, LedgerSnapshot, OverlayProvider, Sid,
};
use fluree_db_policy::{
    ConditionState, PolicyQuery, PolicyQueryExecutor, PolicyQueryFut, PolicyQueryLanguage,
    Result as PolicyResult, UNBOUND_IDENTITY_PREFIX,
};
use fluree_vocab::namespaces::{EMPTY, RDF, XSD};
use fluree_vocab::{rdf_names, xsd_names};
use std::collections::HashMap;

/// Policy query executor that runs queries against a database
///
/// This executor converts `PolicyQuery` to the query engine's IR and
/// executes with a root context (no policy filtering).
pub struct QueryPolicyExecutor<'a> {
    /// The database snapshot to query
    pub snapshot: &'a LedgerSnapshot,
    /// Optional overlay provider (for staged flakes)
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Target transaction time
    pub to_t: i64,
    /// Graph ID for range queries (default: 0 = default graph)
    pub g_id: GraphId,
    /// Post-state overlay for `f:queryState f:postState` conditions:
    /// committed state plus the transaction's staged flakes. Absent on read
    /// paths (no transaction in flight — post-state conditions then evaluate
    /// against current state, which pre and post coincide with).
    pub post_overlay: Option<&'a dyn OverlayProvider>,
    /// Target transaction time for the post-state overlay (the staged t)
    pub post_to_t: i64,
}

impl<'a> QueryPolicyExecutor<'a> {
    /// Create a new query executor for the default graph
    pub fn new(snapshot: &'a LedgerSnapshot) -> Self {
        Self {
            snapshot,
            overlay: None,
            to_t: snapshot.t,
            g_id: 0,
            post_overlay: None,
            post_to_t: snapshot.t,
        }
    }

    /// Create a query executor with overlay support for the default graph
    pub fn with_overlay(
        snapshot: &'a LedgerSnapshot,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
    ) -> Self {
        Self {
            snapshot,
            overlay: Some(overlay),
            to_t,
            g_id: 0,
            post_overlay: None,
            post_to_t: to_t,
        }
    }

    /// Set the graph ID for range queries.
    ///
    /// Policy queries will execute against this graph instead of the default graph.
    pub fn with_graph_id(mut self, g_id: GraphId) -> Self {
        self.g_id = g_id;
        self
    }

    /// Attach a post-state overlay (committed + staged flakes) for
    /// `f:queryState f:postState` conditions, with the staged t.
    pub fn with_post_state(mut self, overlay: &'a dyn OverlayProvider, to_t: i64) -> Self {
        self.post_overlay = Some(overlay);
        self.post_to_t = to_t;
        self
    }
}

impl PolicyQueryExecutor for QueryPolicyExecutor<'_> {
    fn evaluate_policy_query<'b>(
        &'b self,
        query: &'b PolicyQuery,
        bindings: &'b HashMap<String, FlakeValue>,
    ) -> PolicyQueryFut<'b> {
        Box::pin(self.evaluate_async(query, bindings))
    }
}

/// Map a JSON-LD special-variable name to its SPARQL registry name.
///
/// JSON-LD policy bindings use `?$this` / `?$identity`; SPARQL has no `$`
/// in variable names — the SHACL-SPARQL-style `$this` lexes as sigil `$` +
/// name `this` and registers as `?this`. So `?$this` maps to `?this`;
/// names without the `$` marker pass through unchanged.
fn sparql_var_name(json_ld_name: &str) -> String {
    match json_ld_name.strip_prefix("?$") {
        Some(rest) => format!("?{rest}"),
        None => json_ld_name.to_string(),
    }
}

/// True when a binding value is the never-match unbound-identity marker.
fn is_unbound_marker(value: &FlakeValue) -> bool {
    matches!(value, FlakeValue::Ref(sid) if sid.name.starts_with(UNBOUND_IDENTITY_PREFIX))
}

/// Object IRI seeded for `?$value` when the flake's object has no faithful
/// binding representation (`Vector`, `GeoPoint`, `Null`). Absent from real
/// data, so a positional `?$value` condition finds no match. See
/// [`binding_for_value`] for why this must not be UNDEF.
const NON_REPRESENTABLE_VALUE_IRI: &str = "urn:fluree:policy:non-representable-value";

/// Convert a binding value to a seeded `Binding` for a policy VALUES row.
///
/// Every special variable seeds a CONCRETE binding — never `Binding::Unbound`.
/// A positional VALUES pattern treats an unbound variable as "matches
/// anything" (VALUES-UNDEF is compatible with every row), so seeding UNDEF for
/// a never-match marker would make a positional condition such as
/// `$identity <ex:user> $this` *vanish* and hold for every row — fail-OPEN.
/// Seeding a concrete never-match value instead fails closed positionally and
/// keeps `FILTER` equality false.
///
/// - Refs — including the never-match unbound-identity marker, whose sentinel
///   IRI is absent from data — seed as `Binding::Sid`.
/// - Literals with a faithful default datatype seed as `Binding::Lit`.
/// - Literals whose kind has no faithful datatype (`Vector`, `GeoPoint`,
///   `Null`) seed the [`NON_REPRESENTABLE_VALUE_IRI`] ref sentinel (fail-closed).
fn binding_for_value(value: &FlakeValue) -> Binding {
    match value {
        FlakeValue::Ref(sid) => Binding::Sid {
            sid: sid.clone(),
            t: None,
            op: None,
        },
        literal => match default_literal_datatype(literal) {
            Some(dt_sid) => Binding::Lit {
                val: literal.clone(),
                dtc: DatatypeConstraint::Explicit(dt_sid),
                t: None,
                op: None,
                p_id: None,
            },
            None => Binding::Sid {
                sid: Sid::new(EMPTY, NON_REPRESENTABLE_VALUE_IRI),
                t: None,
                op: None,
            },
        },
    }
}

/// Default XSD datatype Sid for a literal binding value, for seeding
/// VALUES rows (`Binding::Lit` equality includes the datatype). Mirrors the
/// datatypes the SPARQL literal lowering assigns, so seeded values compare
/// like written literals. Returns `None` only for `Vector` / `GeoPoint` /
/// `Null`, whose object value has no faithful literal datatype for seeding;
/// [`binding_for_value`] then seeds a never-match ref sentinel (fail-closed).
fn default_literal_datatype(value: &FlakeValue) -> Option<Sid> {
    // rdf:JSON lives in the RDF namespace, not XSD — handle before the
    // XSD-namespaced fallthrough below.
    if matches!(value, FlakeValue::Json(_)) {
        return Some(Sid::new(RDF, rdf_names::JSON));
    }
    let name = match value {
        FlakeValue::String(_) => xsd_names::STRING,
        FlakeValue::Boolean(_) => xsd_names::BOOLEAN,
        FlakeValue::Long(_) | FlakeValue::BigInt(_) => xsd_names::INTEGER,
        FlakeValue::Double(_) => xsd_names::DOUBLE,
        FlakeValue::Decimal(_) => xsd_names::DECIMAL,
        FlakeValue::DateTime(_) => xsd_names::DATE_TIME,
        FlakeValue::Date(_) => xsd_names::DATE,
        FlakeValue::Time(_) => xsd_names::TIME,
        FlakeValue::GYear(_) => xsd_names::G_YEAR,
        FlakeValue::GYearMonth(_) => xsd_names::G_YEAR_MONTH,
        FlakeValue::GMonth(_) => xsd_names::G_MONTH,
        FlakeValue::GDay(_) => xsd_names::G_DAY,
        FlakeValue::GMonthDay(_) => xsd_names::G_MONTH_DAY,
        FlakeValue::Duration(_) => xsd_names::DURATION,
        FlakeValue::DayTimeDuration(_) => xsd_names::DAY_TIME_DURATION,
        FlakeValue::YearMonthDuration(_) => xsd_names::YEAR_MONTH_DURATION,
        _ => return None,
    };
    Some(Sid::new(XSD, name))
}

impl QueryPolicyExecutor<'_> {
    /// Async implementation of policy query evaluation
    async fn evaluate_async(
        &self,
        query: &PolicyQuery,
        bindings: &HashMap<String, FlakeValue>,
    ) -> PolicyResult<bool> {
        let state = query.state;
        match query.language {
            PolicyQueryLanguage::JsonLd => {
                self.evaluate_jsonld(&query.source, bindings, state).await
            }
            PolicyQueryLanguage::Sparql => {
                self.evaluate_sparql(&query.source, bindings, state).await
            }
            PolicyQueryLanguage::Cypher => {
                self.evaluate_cypher(&query.source, bindings, state).await
            }
            // `PolicyQueryLanguage` is non_exhaustive; an unknown language
            // fails closed (error → deny), never open.
            other => Err(fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Unsupported policy query language: {}", other.as_str()),
            }),
        }
    }

    /// Evaluate a Cypher policy query via the registered lowering hook.
    ///
    /// Bindings become Cypher **parameters** (`?$this` → `$this`): refs
    /// carry IRI strings, literals (`$value`) carry their scalar values —
    /// substituted into the AST before lowering, no variable seeding. An
    /// unbound identity substitutes as `null`, which never compares equal,
    /// so identity-referencing conditions cannot hold. Fails closed when no
    /// Cypher support is registered.
    async fn evaluate_cypher(
        &self,
        source: &str,
        bindings: &HashMap<String, FlakeValue>,
        state: ConditionState,
    ) -> PolicyResult<bool> {
        let Some(support) = crate::lang_support::cypher_support() else {
            return Err(fluree_db_policy::PolicyError::QueryExecution {
                message: "Cypher policy support is not registered in this process".to_string(),
            });
        };

        let mut params = serde_json::Map::new();
        for (name, value) in bindings {
            // "?$this" → parameter name "this"; custom "?myVar" → "myVar".
            let key = name
                .strip_prefix("?$")
                .or_else(|| name.strip_prefix('?'))
                .unwrap_or(name)
                .to_string();
            let json = if is_unbound_marker(value) {
                serde_json::Value::Null
            } else {
                match value {
                    FlakeValue::Ref(sid) => {
                        let iri = self
                            .snapshot
                            .decode_sid(sid)
                            .unwrap_or_else(|| sid.name.to_string());
                        serde_json::Value::String(iri)
                    }
                    FlakeValue::String(s) => serde_json::Value::String(s.clone()),
                    FlakeValue::Long(l) => serde_json::Value::from(*l),
                    FlakeValue::Double(d) => serde_json::Value::from(*d),
                    FlakeValue::Boolean(b) => serde_json::Value::from(*b),
                    // No faithful Cypher parameter representation: null never
                    // compares equal, so conditions on it fail closed.
                    _ => serde_json::Value::Null,
                }
            };
            params.insert(key, json);
        }

        let mut vars = VarRegistry::new();
        let patterns = (support.lower_policy_query)(source, self.snapshot, &mut vars, &params)
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Failed to lower Cypher policy query: {e}"),
            })?;

        self.run_existence_check(&vars, &patterns, state).await
    }

    /// Evaluate a JSON-LD policy query (the historical default).
    async fn evaluate_jsonld(
        &self,
        source: &str,
        bindings: &HashMap<String, FlakeValue>,
        state: ConditionState,
    ) -> PolicyResult<bool> {
        // Parse and lower the policy's f:query using the main query parser/IR.
        //
        // We intentionally do NOT implement a bespoke parser here; this ensures full
        // feature parity (e.g., FILTER patterns) and avoids divergence.
        //
        // Policy queries behave like existence checks, with:
        // - select forced to ["?$this"]
        // - limit forced to 1
        // - VALUES injected into WHERE for special variables (?$this, ?$identity, etc.)
        let mut query_json: serde_json::Value = serde_json::from_str(source).map_err(|e| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Invalid policy query JSON: {e}"),
            }
        })?;

        let obj = query_json.as_object_mut().ok_or_else(|| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: "Policy query must be a JSON object".to_string(),
            }
        })?;

        // Accept the query language's `ask` form as the preferred spelling
        // of a condition — its value IS a where-pattern, so it normalizes
        // onto the same existence-check path as the legacy `{"where": ...}`
        // form. Both keys together is ambiguous and fails closed (deny).
        if let Some(ask) = obj.remove("ask") {
            if obj.contains_key("where") {
                return Err(fluree_db_policy::PolicyError::QueryExecution {
                    message: "Policy query cannot carry both 'ask' and 'where'".to_string(),
                });
            }
            obj.insert("where".to_string(), ask);
        }

        // Force select + limit for policy queries
        obj.insert(
            "select".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::String("?$this".to_string())]),
        );
        obj.insert("limit".to_string(), serde_json::Value::from(1));

        // Build VALUES clause JSON for the ref-valued special variables
        // (?$this, ?$identity, custom policy values). Inject VALUES into
        // WHERE BEFORE parsing — this ensures even empty queries (no WHERE)
        // work, the VALUES provides the pattern.
        //
        // ?$value / ?$op are NOT JSON-injected: the flake's object can be a
        // ref whose decoded IRI doesn't round-trip through the strict
        // compact-IRI parser (e.g. ledger-scoped `ledger:...` IRIs), and a
        // literal can carry a datatype JSON can't express. They seed as
        // direct Bindings after parsing (same mechanism as the SPARQL path).
        //
        // Format: ["values", [["?$this", "?$identity", ...], [[iri1, iri2, ...]]]]
        let mut var_names: Vec<String> = bindings
            .keys()
            .filter(|name| *name != "?$value" && *name != "?$op")
            .cloned()
            .collect();
        var_names.sort();

        // Build VALUES row with IRIs for each variable.
        //
        // The unbound-identity marker is a ref carrying its never-match
        // sentinel IRI: emit it as an `{"@id": ...}` just like any other ref
        // (NOT as null/UNDEF — a positional VALUES treats UNDEF as
        // "matches anything", which would make an `$identity`-positioned
        // condition hold for every row; see `binding_for_value`). Its sentinel
        // IRI is absent from data, so the condition finds no match.
        let values_row: Vec<serde_json::Value> = var_names
            .iter()
            .map(|name| {
                let value = bindings.get(name).expect("binding value exists");
                match value {
                    FlakeValue::Ref(sid) => {
                        // Decode SID to IRI for JSON representation
                        let iri = self
                            .snapshot
                            .decode_sid(sid)
                            .unwrap_or_else(|| sid.name.to_string());
                        serde_json::json!({"@id": iri})
                    }
                    FlakeValue::String(s) => serde_json::Value::String(s.clone()),
                    FlakeValue::Long(l) => serde_json::Value::from(*l),
                    FlakeValue::Double(d) => serde_json::Value::from(*d),
                    FlakeValue::Boolean(b) => serde_json::Value::from(*b),
                    // No faithful JSON representation → seed the never-match
                    // ref sentinel (fail-closed), never null/UNDEF.
                    _ => serde_json::json!({"@id": NON_REPRESENTABLE_VALUE_IRI}),
                }
            })
            .collect();

        let values_clause = serde_json::json!(["values", [var_names.clone(), [values_row]]]);

        // Inject VALUES into WHERE clause (or create WHERE if missing)
        let where_clause = obj.get_mut("where");
        match where_clause {
            Some(serde_json::Value::Array(arr)) => {
                // WHERE is an array - prepend VALUES
                arr.insert(0, values_clause);
            }
            Some(serde_json::Value::Object(_)) => {
                // WHERE is an object (single pattern) - wrap in array with VALUES
                let existing = obj.remove("where").unwrap();
                obj.insert(
                    "where".to_string(),
                    serde_json::json!([values_clause, existing]),
                );
            }
            Some(_) | None => {
                // No WHERE or invalid - create with just VALUES
                // This handles empty queries like {}
                obj.insert("where".to_string(), serde_json::json!([values_clause]));
            }
        }

        // Create a variable registry for this query execution
        let mut vars = VarRegistry::new();

        // Pre-register special variables so they are present even if not referenced.
        // This matches the "always ground" behavior.
        for var_name in &var_names {
            vars.get_or_insert(var_name);
        }

        let parsed = crate::parse::parse_query(&query_json, self.snapshot, &mut vars, None)
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Failed to parse policy query: {e}"),
            })?;

        // Seed ?$value / ?$op as direct Bindings (no JSON round-trip).
        let mut patterns = parsed.patterns;
        let mut extra_vars = Vec::new();
        let mut extra_row = Vec::new();
        for name in ["?$value", "?$op"] {
            let Some(value) = bindings.get(name) else {
                continue;
            };
            extra_vars.push(vars.get_or_insert(name));
            extra_row.push(binding_for_value(value));
        }
        if !extra_vars.is_empty() {
            patterns.insert(
                0,
                Pattern::Values {
                    vars: extra_vars,
                    rows: vec![extra_row],
                },
            );
        }

        self.run_existence_check(&vars, &patterns, state).await
    }

    /// Evaluate a SPARQL policy query (`f:query` stored with the `f:sparql`
    /// datatype).
    ///
    /// SPARQL support is provided by a higher layer via
    /// [`crate::lang_support::register_sparql_support`]; if it is absent this
    /// fails closed (error → deny), never open.
    async fn evaluate_sparql(
        &self,
        source: &str,
        bindings: &HashMap<String, FlakeValue>,
        state: ConditionState,
    ) -> PolicyResult<bool> {
        let support = crate::lang_support::sparql_support().ok_or_else(|| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: "SPARQL policy support is not registered in this process; \
                          cannot evaluate f:sparql policy query"
                    .to_string(),
            }
        })?;

        let mut vars = VarRegistry::new();
        let mut patterns =
            (support.lower_policy_query)(source, self.snapshot, &mut vars).map_err(|e| {
                fluree_db_policy::PolicyError::QueryExecution {
                    message: format!("Failed to parse SPARQL policy query: {e}"),
                }
            })?;

        // Seed special variables with a VALUES pattern, mirroring the JSON-LD
        // path's injected VALUES clause. Binding keys arrive in JSON-LD form
        // (`?$this`); the SPARQL query references them as `$this`/`?this`,
        // registered as `?this`.
        let mut var_names: Vec<String> = bindings.keys().cloned().collect();
        var_names.sort();

        let mut var_ids = Vec::with_capacity(var_names.len());
        let mut row = Vec::with_capacity(var_names.len());
        for name in &var_names {
            let var_id = vars.get_or_insert(&sparql_var_name(name));
            if var_ids.contains(&var_id) {
                continue;
            }
            let value = bindings.get(name).expect("binding value exists");
            var_ids.push(var_id);
            // Unbound identity seeds as UNDEF, same as the JSON-LD path's
            // null VALUES cell.
            row.push(binding_for_value(value));
        }
        patterns.insert(
            0,
            Pattern::Values {
                vars: var_ids,
                rows: vec![row],
            },
        );

        self.run_existence_check(&vars, &patterns, state).await
    }

    /// Execute WHERE patterns with a root (policy-free) context and report
    /// whether any solution exists.
    async fn run_existence_check(
        &self,
        vars: &VarRegistry,
        patterns: &[Pattern],
        state: ConditionState,
    ) -> PolicyResult<bool> {
        // Per-condition state selection: `f:postState` reads through the
        // staged overlay when one is attached; otherwise (read paths, no
        // transaction in flight) pre and post coincide with current state.
        let (overlay, to_t) = match state {
            ConditionState::Post => match self.post_overlay {
                Some(post) => (Some(post), self.post_to_t),
                None => (self.overlay, self.to_t),
            },
            ConditionState::Pre => (self.overlay, self.to_t),
        };

        // Create the execution context WITHOUT policy (root context)
        // This is critical - policy queries must not be filtered by policy
        let ctx = if let Some(overlay) = overlay {
            ExecutionContext::with_time_and_overlay(self.snapshot, vars, to_t, None, overlay)
                .with_graph_id(self.g_id)
        } else {
            ExecutionContext::with_time(self.snapshot, vars, to_t, None).with_graph_id(self.g_id)
        };

        // Build the where clause operators (VALUES is now part of the patterns).
        //
        // Root: policy queries always evaluate at the selected state's t for
        // current state — they're access-control predicates, not
        // history-range queries. Always plan as `Current`.
        let mut operator = build_where_operators_seeded(
            None,
            patterns,
            None,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        )
        .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
            message: e.to_string(),
        })?;

        // Execute and check if there's at least one result (existence check)
        operator
            .open(&ctx)
            .await
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: e.to_string(),
            })?;

        let has_results = match operator.next_batch(&ctx).await {
            Ok(Some(batch)) => !batch.is_empty(),
            Ok(None) => false,
            Err(e) => {
                operator.close();
                return Err(fluree_db_policy::PolicyError::QueryExecution {
                    message: e.to_string(),
                });
            }
        };

        operator.close();

        Ok(has_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uuid::Uuid;

    /// The security invariant behind both PR review findings: a policy VALUES
    /// row must never seed `Binding::Unbound` for a special variable. A
    /// positional VALUES treats UNDEF as "matches anything", so an unbound
    /// `$identity` / `$value` would make a positional condition hold for every
    /// row (fail-OPEN). Every kind must seed a concrete never-match-or-exact
    /// binding.
    #[test]
    fn binding_for_value_never_unbound() {
        // Finding 1: the unbound-identity marker seeds a concrete ref, not UNDEF.
        let marker = FlakeValue::Ref(Sid::new(
            EMPTY,
            format!("{UNBOUND_IDENTITY_PREFIX}{}", Uuid::nil()),
        ));
        assert!(
            matches!(binding_for_value(&marker), Binding::Sid { .. }),
            "unbound-identity marker must seed a never-match Sid, not UNDEF"
        );

        // Finding 2: kinds with no faithful datatype seed the ref sentinel.
        for value in [
            FlakeValue::Vector(Arc::from([1.0_f64, 2.0].as_slice())),
            FlakeValue::Null,
        ] {
            match binding_for_value(&value) {
                Binding::Sid { sid, .. } => {
                    assert_eq!(sid.name.as_ref(), NON_REPRESENTABLE_VALUE_IRI);
                }
                other => panic!("non-faithful {value:?} must seed the sentinel, got {other:?}"),
            }
        }

        // Faithful kinds seed concrete literals with the right datatype.
        for (value, ns, name) in [
            (FlakeValue::String("x".into()), XSD, xsd_names::STRING),
            (FlakeValue::Long(1), XSD, xsd_names::INTEGER),
            (FlakeValue::Json("{}".into()), RDF, rdf_names::JSON),
        ] {
            match binding_for_value(&value) {
                Binding::Lit { dtc, .. } => {
                    assert_eq!(
                        dtc.datatype(),
                        &Sid::new(ns, name),
                        "datatype for {value:?}"
                    );
                }
                other => panic!("faithful {value:?} must seed a Lit, got {other:?}"),
            }
        }

        // A regular ref seeds itself.
        let real = FlakeValue::Ref(Sid::new(XSD, "someSubject"));
        assert!(matches!(binding_for_value(&real), Binding::Sid { .. }));
    }
}
