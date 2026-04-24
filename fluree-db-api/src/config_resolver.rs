//! Config graph reader and resolver.
//!
//! Reads `f:LedgerConfig` from the config graph (g_id=2) via privileged
//! (policy-bypassing) queries, resolves effective settings by merging
//! ledger-wide defaults with per-graph overrides, and provides per-subsystem
//! merge functions for request-time identity gating.
//!
//! # Architecture
//!
//! ```text
//! resolve_ledger_config()     — privileged read from g_id=2 → LedgerConfig
//! resolve_effective_config()  — three-tier merge → ResolvedConfig
//! merge_policy_opts()         — request-scoped policy merge
//! merge_reasoning()           — request-scoped reasoning merge
//! merge_shacl_opts()          — transaction-scoped SHACL merge
//! merge_transact_opts()       — transaction-scoped uniqueness merge
//! merge_datalog_opts()        — query-scoped datalog merge
//! ```
//!
//! Identity gating happens at the request boundary (not here) because
//! the server-verified identity is only available per-request.

use std::collections::HashSet;
use std::sync::Arc;

use fluree_db_core::ledger_config::{
    DatalogDefaults, FullTextDefaults, FullTextProperty, GraphConfig, GraphSourceRef, LedgerConfig,
    OntologyImportBinding, OverrideControl, PolicyDefaults, ReasoningDefaults, ResolvedConfig,
    RollbackGuard, ShaclDefaults, TransactDefaults, TrustMode, TrustPolicy, ValidationMode,
};
use fluree_db_core::{GraphDbRef, LedgerSnapshot, OverlayProvider, Sid, CONFIG_GRAPH_ID};
use fluree_db_query::{
    execute_pattern_with_overlay_at, Binding, Ref, Term, TriplePattern, VarRegistry,
};
use fluree_vocab::config_iris;
use fluree_vocab::rdf::TYPE as RDF_TYPE_IRI;

use crate::error::Result;
use crate::view::ReasoningModePrecedence;
use crate::QueryConnectionOptions;

// ============================================================================
// Public: Privileged config read
// ============================================================================

/// Read the `f:LedgerConfig` from the config graph (g_id=2).
///
/// This is a **privileged** read — it bypasses policy by querying the config
/// graph directly via `GraphDbRef::new(snapshot, CONFIG_GRAPH_ID, overlay, to_t)`.
///
/// Returns `Ok(None)` if the config graph is empty (no `f:LedgerConfig`
/// resource found). System defaults apply in that case.
pub async fn resolve_ledger_config(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
) -> Result<Option<LedgerConfig>> {
    // Encode rdf:type (namespace code 3) and f:LedgerConfig (namespace code 7).
    // Both namespaces are always in default_namespace_codes().
    let rdf_type_sid = match snapshot.encode_iri(RDF_TYPE_IRI) {
        Some(sid) => sid,
        None => {
            tracing::warn!("Failed to encode rdf:type IRI — config graph unavailable");
            return Ok(None);
        }
    };
    let config_type_sid = match snapshot.encode_iri(config_iris::LEDGER_CONFIG) {
        Some(sid) => sid,
        None => {
            tracing::warn!("Failed to encode f:LedgerConfig IRI — config graph unavailable");
            return Ok(None);
        }
    };

    // Query: ?s rdf:type f:LedgerConfig at CONFIG_GRAPH_ID
    let config_sids =
        find_instances_of_type(snapshot, overlay, to_t, &rdf_type_sid, &config_type_sid).await?;

    if config_sids.is_empty() {
        return Ok(None);
    }

    // If multiple config resources exist, sort by decoded IRI and use the first.
    let config_sid = if config_sids.len() == 1 {
        config_sids[0].clone()
    } else {
        tracing::warn!(
            count = config_sids.len(),
            "Multiple f:LedgerConfig resources found in config graph — using first by IRI order"
        );
        let mut with_iris: Vec<(String, Sid)> = config_sids
            .into_iter()
            .map(|sid| {
                let iri = snapshot
                    .decode_sid(&sid)
                    .unwrap_or_else(|| format!("<unknown:{sid:?}>"));
                (iri, sid)
            })
            .collect();
        with_iris.sort_by(|(a, _), (b, _)| a.cmp(b));
        with_iris.into_iter().next().unwrap().1
    };

    // Read the config_id (@id)
    let config_id = snapshot.decode_sid(&config_sid);

    // Read each setting group
    let policy = read_policy_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let shacl = read_shacl_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let reasoning = read_reasoning_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let datalog = read_datalog_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let transact = read_transact_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let full_text = read_fulltext_defaults(snapshot, overlay, to_t, &config_sid).await?;
    let graph_overrides = read_graph_overrides(snapshot, overlay, to_t, &config_sid).await?;

    Ok(Some(LedgerConfig {
        config_id,
        policy,
        shacl,
        reasoning,
        datalog,
        transact,
        full_text,
        graph_overrides,
    }))
}

// ============================================================================
// Public: Three-tier merge
// ============================================================================

/// Resolve effective config for a specific graph within a ledger.
///
/// Merges ledger-wide defaults with matching per-graph overrides.
/// `graph_iri` is `None` for the default graph (g_id=0).
///
/// For each setting group:
/// 1. Start with ledger-wide group
/// 2. Find matching `GraphConfig` by `target_graph` IRI
/// 3. If per-graph group exists: merge field-by-field, tighten override control
pub fn resolve_effective_config(config: &LedgerConfig, graph_iri: Option<&str>) -> ResolvedConfig {
    // Find matching per-graph override
    let graph_override = config
        .graph_overrides
        .iter()
        .find(|gc| matches_graph_target(&gc.target_graph, graph_iri));

    ResolvedConfig {
        policy: merge_setting_group(
            &config.policy,
            graph_override.and_then(|gc| gc.policy.as_ref()),
        ),
        shacl: merge_setting_group(
            &config.shacl,
            graph_override.and_then(|gc| gc.shacl.as_ref()),
        ),
        reasoning: merge_setting_group(
            &config.reasoning,
            graph_override.and_then(|gc| gc.reasoning.as_ref()),
        ),
        datalog: merge_setting_group(
            &config.datalog,
            graph_override.and_then(|gc| gc.datalog.as_ref()),
        ),
        transact: merge_setting_group(
            &config.transact,
            graph_override.and_then(|gc| gc.transact.as_ref()),
        ),
        full_text: merge_setting_group(
            &config.full_text,
            graph_override.and_then(|gc| gc.full_text.as_ref()),
        ),
    }
}

/// Check if a `target_graph` IRI matches the given graph context.
///
/// Sentinel IRIs:
/// - `f:defaultGraph` matches `None` (g_id=0)
/// - `f:txnMetaGraph` matches the txn-meta IRI
/// - Other IRIs match by exact string comparison
fn matches_graph_target(target: &str, graph_iri: Option<&str>) -> bool {
    match graph_iri {
        None => target == config_iris::DEFAULT_GRAPH,
        Some(iri) => {
            target == iri || (target == config_iris::TXN_META_GRAPH && iri.ends_with("#txn-meta"))
        }
    }
}

/// Collect the effective configured full-text properties from a resolved
/// [`LedgerConfig`], in the shape the indexer expects.
///
/// Semantics:
/// - Ledger-wide properties (declared on `LedgerConfig.full_text.properties`)
///   are emitted with scope `AnyGraph` — they apply to every graph in the
///   ledger.
/// - Per-graph overrides emit only the **delta** — properties added on top
///   of the ledger-wide list — with a scope that mirrors the override's
///   `f:targetGraph`: `DefaultGraph` for `f:defaultGraph`, `TxnMetaGraph`
///   for `f:txnMetaGraph`, or `NamedGraph(iri)` otherwise. Inherited
///   ledger-wide entries are not re-emitted per graph because `AnyGraph`
///   already covers them.
/// - Override blocking (`f:OverrideNone` on the ledger-wide group) is
///   enforced by reusing the shared `merge_setting_group` helper: when
///   blocked, the "effective" per-graph group equals the ledger-wide group,
///   so the delta is empty and no extra entries are emitted.
///
/// Empty result when no full-text defaults are configured — the indexer treats
/// this as "only the `@fulltext` datatype path contributes entries."
pub fn configured_fulltext_properties_for_indexer(
    config: &LedgerConfig,
) -> Vec<fluree_db_indexer::ConfiguredFulltextProperty> {
    use fluree_db_indexer::{ConfiguredFulltextProperty, ConfiguredFulltextScope};

    let mut out: Vec<ConfiguredFulltextProperty> = Vec::new();

    // Ledger-wide: applies to every graph (scope::AnyGraph).
    let ledger_wide_iris: std::collections::HashSet<&str> = config
        .full_text
        .as_ref()
        .map(|ft| ft.properties.iter().map(|p| p.target.as_str()).collect())
        .unwrap_or_default();
    if let Some(ref ft) = config.full_text {
        for prop in &ft.properties {
            out.push(ConfiguredFulltextProperty {
                scope: ConfiguredFulltextScope::AnyGraph,
                property_iri: prop.target.clone(),
            });
        }
    }

    // Per-graph overrides: emit only the DELTA (properties the override adds
    // beyond the ledger-wide list), under a scope that matches the override's
    // `f:targetGraph`. Sentinel IRIs `f:defaultGraph` / `f:txnMetaGraph` map
    // to the `DefaultGraph` / `TxnMetaGraph` scope variants so the indexer
    // can resolve them to the correct `GraphId` without treating them as
    // literal IRIs.
    for gc in &config.graph_overrides {
        let effective_full_text = merge_setting_group(&config.full_text, gc.full_text.as_ref());
        let props = match &effective_full_text {
            Some(ft) => &ft.properties[..],
            None => continue,
        };

        let scope = if gc.target_graph == config_iris::DEFAULT_GRAPH {
            ConfiguredFulltextScope::DefaultGraph
        } else if gc.target_graph == config_iris::TXN_META_GRAPH {
            ConfiguredFulltextScope::TxnMetaGraph
        } else {
            ConfiguredFulltextScope::NamedGraph(gc.target_graph.clone())
        };

        for prop in props {
            if ledger_wide_iris.contains(prop.target.as_str()) {
                // Already covered by the ledger-wide `AnyGraph` entry.
                continue;
            }
            out.push(ConfiguredFulltextProperty {
                scope: scope.clone(),
                property_iri: prop.target.clone(),
            });
        }
    }

    out
}

// ============================================================================
// Public: Per-subsystem merge functions (request-scoped)
// ============================================================================

/// Merge config policy defaults with query-time opts.
///
/// `server_identity` is the auth-layer-verified identity (NOT `opts.identity`
/// which is the user-settable policy evaluation context).
///
/// Algorithm:
/// 1. No config policy → return opts unchanged
/// 2. Query specifies policy → check override control:
///    - Permitted → keep query opts
///    - Denied → log warning, apply config defaults
/// 3. No query policy → apply config defaults
pub fn merge_policy_opts(
    resolved: &ResolvedConfig,
    opts: &QueryConnectionOptions,
    server_identity: Option<&str>,
) -> QueryConnectionOptions {
    let policy = match &resolved.policy {
        Some(p) => p,
        None => return opts.clone(),
    };

    // Does the query specify any policy inputs?
    let query_has_policy = opts.has_any_policy_inputs();

    if query_has_policy {
        // Check if override is permitted
        if policy.override_control.permits_override(server_identity) {
            return opts.clone();
        }
        tracing::warn!(
            server_identity,
            "Query-time policy override denied by config override control — applying config defaults"
        );
    }

    // Apply config defaults
    let mut merged = opts.clone();

    // Apply default_allow from config (config says deny-by-default)
    if let Some(default_allow) = policy.default_allow {
        merged.default_allow = default_allow;
    }

    // Apply policy_class from config
    if let Some(ref classes) = policy.policy_class {
        merged.policy_class = Some(classes.clone());
    }

    // policy_source (GraphSourceRef) is resolved to graph IDs by the caller
    // via resolve_policy_source_g_ids() and passed to build_policy_context_from_opts().
    // See wrap_policy() in fluree_ext.rs.

    merged
}

/// Compute effective reasoning for a view, respecting override control.
///
/// `server_identity` is the auth-layer-verified identity (NOT `opts.identity`).
///
/// Returns `(modes, precedence)` to apply via `view.with_reasoning_precedence()`,
/// or `None` if no config reasoning is configured.
///
/// Override control determines precedence:
/// - `OverrideControl::None` → `Force` (query cannot override)
/// - `AllowAll` → `DefaultUnlessQueryOverrides`
/// - `IdentityRestricted` → check identity:
///   - Permitted → `DefaultUnlessQueryOverrides`
///   - Denied → `Force`
pub fn merge_reasoning(
    resolved: &ResolvedConfig,
    server_identity: Option<&str>,
) -> Option<(Vec<String>, ReasoningModePrecedence)> {
    let reasoning = resolved.reasoning.as_ref()?;
    let modes = reasoning.modes.as_ref()?;

    if modes.is_empty() {
        return None;
    }

    let precedence = if reasoning.override_control.permits_override(server_identity) {
        ReasoningModePrecedence::DefaultUnlessQueryOverrides
    } else {
        ReasoningModePrecedence::Force
    };

    Some((modes.clone(), precedence))
}

/// Transaction-time SHACL configuration from config graph.
#[derive(Debug, Clone)]
pub struct EffectiveShaclConfig {
    /// Whether SHACL validation should run.
    pub enabled: bool,
    /// How to handle validation failures.
    pub validation_mode: ValidationMode,
}

/// Compute effective SHACL settings from resolved config.
///
/// Returns `None` if no SHACL config section is present. When `None`,
/// callers fall back to the shapes-exist heuristic (see `stage_with_config_shacl`).
///
/// Override control is recognized but not actively gated — there's no
/// transaction-time SHACL override mechanism yet. When one is added
/// (e.g., `TxnOpts.skip_shacl`), the gate goes here.
pub fn merge_shacl_opts(
    resolved: &ResolvedConfig,
    _server_identity: Option<&str>,
) -> Option<EffectiveShaclConfig> {
    let shacl = resolved.shacl.as_ref()?;
    Some(EffectiveShaclConfig {
        // Default `false` per docs/ledger-config/setting-groups.md — opt-in is
        // the safer posture. Prior code defaulted to `true`, silently enabling
        // SHACL for any config that declared an `f:shaclDefaults` section
        // without setting `f:shaclEnabled`, diverging from documented behavior.
        enabled: shacl.enabled.unwrap_or(false),
        validation_mode: shacl.validation_mode.unwrap_or(ValidationMode::Reject),
    })
}

/// Query-time datalog configuration from config graph.
#[derive(Debug, Clone)]
pub struct EffectiveDatalogConfig {
    /// Whether datalog rule execution is enabled.
    pub enabled: bool,
    /// Whether query-time rule injection (the `rules` JSON field) is permitted.
    pub allow_query_time_rules: bool,
    /// Whether the query can override these settings.
    pub override_allowed: bool,
}

/// Compute effective datalog settings from resolved config, respecting override control.
///
/// The `override_allowed` flag follows the same pattern as reasoning precedence:
/// when `true`, query-time options can override config defaults;
/// when `false`, config settings are forced.
pub fn merge_datalog_opts(
    resolved: &ResolvedConfig,
    server_identity: Option<&str>,
) -> Option<EffectiveDatalogConfig> {
    let datalog = resolved.datalog.as_ref()?;
    let override_allowed = datalog.override_control.permits_override(server_identity);

    Some(EffectiveDatalogConfig {
        enabled: datalog.enabled.unwrap_or(true),
        allow_query_time_rules: datalog.allow_query_time_rules.unwrap_or(true),
        override_allowed,
    })
}

/// Transaction-time uniqueness configuration from config graph.
#[derive(Debug, Clone)]
pub struct EffectiveTransactConfig {
    /// Whether unique constraint enforcement should run.
    pub unique_enabled: bool,
    /// Graphs containing `f:enforceUnique` annotations.
    pub constraints_sources: Vec<GraphSourceRef>,
}

/// Compute effective transact settings from resolved config.
///
/// Returns `None` if no transact config section is present. When `None`,
/// callers skip uniqueness enforcement entirely (zero-cost path).
pub fn merge_transact_opts(resolved: &ResolvedConfig) -> Option<EffectiveTransactConfig> {
    let transact = resolved.transact.as_ref()?;
    let enabled = transact.unique_enabled.unwrap_or(false);
    if !enabled {
        return None;
    }
    Some(EffectiveTransactConfig {
        unique_enabled: true,
        constraints_sources: transact.constraints_sources.clone(),
    })
}

// ============================================================================
// Internal: Setting group merge helpers
// ============================================================================

/// Generic merge for setting groups: per-graph `Some` fields override ledger-wide.
///
/// If the ledger-wide group's `override_control` is `OverrideNone`, per-graph
/// overrides are blocked entirely — the ledger-wide value is final.
/// Otherwise, per-graph `Some` fields override ledger-wide, and override control
/// is tightened via `effective_min`.
fn merge_setting_group<T: MergeableGroup>(
    ledger_wide: &Option<T>,
    per_graph: Option<&T>,
) -> Option<T> {
    match (ledger_wide, per_graph) {
        (None, None) => None,
        (Some(lw), None) => Some(lw.clone()),
        (None, Some(pg)) => Some(pg.clone()),
        (Some(lw), Some(pg)) => {
            // Step 4 of the resolution algorithm: if ledger-wide overrideControl
            // is OverrideNone, the group is final — per-graph cannot override.
            if matches!(lw.override_control(), OverrideControl::None) {
                tracing::debug!(
                    "Ledger-wide OverrideNone blocks per-graph override for setting group"
                );
                Some(lw.clone())
            } else {
                Some(pg.merge_over(lw))
            }
        }
    }
}

/// Trait for setting groups that support field-level merge with override control tightening.
trait MergeableGroup: Clone {
    /// Access the override control for this group.
    fn override_control(&self) -> &OverrideControl;

    /// Merge `self` (per-graph) over `base` (ledger-wide).
    /// Per-graph `Some` fields override base; override control is tightened.
    fn merge_over(&self, base: &Self) -> Self;
}

impl MergeableGroup for PolicyDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    fn merge_over(&self, base: &Self) -> Self {
        PolicyDefaults {
            default_allow: self.default_allow.or(base.default_allow),
            policy_source: self.policy_source.clone().or(base.policy_source.clone()),
            policy_class: self.policy_class.clone().or(base.policy_class.clone()),
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

impl MergeableGroup for ShaclDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    fn merge_over(&self, base: &Self) -> Self {
        ShaclDefaults {
            enabled: self.enabled.or(base.enabled),
            shapes_source: self.shapes_source.clone().or(base.shapes_source.clone()),
            validation_mode: self.validation_mode.or(base.validation_mode),
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

impl MergeableGroup for ReasoningDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    fn merge_over(&self, base: &Self) -> Self {
        // `ontology_import_map`: per-graph additions extend ledger-wide bindings.
        // Per-graph entries come first so they win on duplicate ontology IRIs.
        let mut import_map = self.ontology_import_map.clone();
        let existing: std::collections::HashSet<String> =
            import_map.iter().map(|b| b.ontology_iri.clone()).collect();
        for b in &base.ontology_import_map {
            if !existing.contains(&b.ontology_iri) {
                import_map.push(b.clone());
            }
        }
        ReasoningDefaults {
            modes: self.modes.clone().or(base.modes.clone()),
            schema_source: self.schema_source.clone().or(base.schema_source.clone()),
            follow_owl_imports: self.follow_owl_imports.or(base.follow_owl_imports),
            ontology_import_map: import_map,
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

impl MergeableGroup for DatalogDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    fn merge_over(&self, base: &Self) -> Self {
        DatalogDefaults {
            enabled: self.enabled.or(base.enabled),
            rules_source: self.rules_source.clone().or(base.rules_source.clone()),
            allow_query_time_rules: self.allow_query_time_rules.or(base.allow_query_time_rules),
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

impl MergeableGroup for TransactDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    /// Additive merge: once enabled at ledger level, stays enabled;
    /// per-graph sources ADD TO ledger-wide sources.
    fn merge_over(&self, base: &Self) -> Self {
        TransactDefaults {
            unique_enabled: Some(
                self.unique_enabled.unwrap_or(false) || base.unique_enabled.unwrap_or(false),
            ),
            constraints_sources: {
                let mut sources = base.constraints_sources.clone();
                sources.extend(self.constraints_sources.iter().cloned());
                sources
            },
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

impl MergeableGroup for FullTextDefaults {
    fn override_control(&self) -> &OverrideControl {
        &self.override_control
    }

    /// Additive merge for `properties`: per-graph entries append to ledger-wide.
    /// Duplicate `target` IRIs resolve to a single entry with per-graph winning,
    /// which leaves room for future per-property knobs (language, boost, …).
    fn merge_over(&self, base: &Self) -> Self {
        let mut properties = base.properties.clone();
        for entry in &self.properties {
            if let Some(slot) = properties.iter_mut().find(|p| p.target == entry.target) {
                *slot = entry.clone();
            } else {
                properties.push(entry.clone());
            }
        }
        FullTextDefaults {
            default_language: self
                .default_language
                .clone()
                .or(base.default_language.clone()),
            properties,
            override_control: base.override_control.effective_min(&self.override_control),
        }
    }
}

// ============================================================================
// Internal: Privileged query helpers
// ============================================================================

/// Query for subjects of a given rdf:type at the config graph.
async fn find_instances_of_type(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    rdf_type_sid: &Sid,
    type_sid: &Sid,
) -> Result<Vec<Sid>> {
    let mut vars = VarRegistry::new();
    let subj_var = vars.get_or_insert("?s");

    let pattern = TriplePattern::new(
        Ref::Var(subj_var),
        Ref::Sid(rdf_type_sid.clone()),
        Term::Sid(type_sid.clone()),
    );

    // Eager materialization: config resolver needs concrete Sid/Lit bindings,
    // not late-materialized EncodedSid/EncodedLit from binary scans.
    let db = GraphDbRef::new(snapshot, CONFIG_GRAPH_ID, overlay, to_t).eager();
    let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None).await?;

    let mut results = Vec::new();
    for batch in &batches {
        for row in 0..batch.len() {
            if let Some(binding) = batch.get(row, subj_var) {
                if let Some(sid) = binding.as_sid() {
                    results.push(sid.clone());
                }
            }
        }
    }

    Ok(results)
}

/// Query for all objects of a predicate on a subject at the config graph.
///
/// Follows the exact pattern from `policy_builder::query_predicate`, but
/// targets `CONFIG_GRAPH_ID` instead of graph 0.
async fn query_config_predicate(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    predicate_sid: &Sid,
) -> Result<Vec<Binding>> {
    let mut vars = VarRegistry::new();
    let obj_var = vars.get_or_insert("?obj");

    let pattern = TriplePattern::new(
        Ref::Sid(subject_sid.clone()),
        Ref::Sid(predicate_sid.clone()),
        Term::Var(obj_var),
    );

    let db = GraphDbRef::new(snapshot, CONFIG_GRAPH_ID, overlay, to_t).eager();
    let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None).await?;

    let mut results = Vec::new();
    for batch in &batches {
        for row in 0..batch.len() {
            if let Some(binding) = batch.get(row, obj_var) {
                results.push(binding.clone());
            }
        }
    }

    Ok(results)
}

/// Encode an IRI to a SID, returning None if the IRI cannot be encoded.
///
/// Unlike `policy_builder::resolve_iri_to_sid`, this returns `None` instead
/// of an error — missing config predicates are normal (unconfigured fields).
fn try_encode(snapshot: &LedgerSnapshot, iri: &str) -> Option<Sid> {
    snapshot.encode_iri(iri)
}

// ============================================================================
// Internal: Field readers
// ============================================================================

/// Read a boolean field from a subject at the config graph.
async fn read_bool_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<bool>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, subject_sid, &pred_sid).await?;
    for binding in bindings {
        if let Some((fluree_db_core::FlakeValue::Boolean(b), _)) = binding.as_lit() {
            return Ok(Some(*b));
        }
    }
    Ok(None)
}

/// Read a single IRI-valued field (object reference) from a subject.
///
/// Returns the SID of the referenced object, suitable for further querying.
async fn read_ref_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<Sid>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, subject_sid, &pred_sid).await?;
    for binding in bindings {
        if let Some(sid) = binding.as_sid() {
            return Ok(Some(sid.clone()));
        }
    }
    Ok(None)
}

/// Read an IRI-valued field and decode to its IRI string.
async fn read_iri_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<String>> {
    let ref_sid = read_ref_field(snapshot, overlay, to_t, subject_sid, pred_iri).await?;
    Ok(ref_sid.and_then(|sid| snapshot.decode_sid(&sid)))
}

/// Read a multi-value IRI list field from a subject at the config graph.
///
/// Each value is an IRI reference (not a string literal). Returns decoded IRI strings.
async fn read_iri_list_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<Vec<String>>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, subject_sid, &pred_sid).await?;
    let mut values = Vec::new();
    for binding in bindings {
        if let Some(sid) = binding.as_sid() {
            if let Some(iri) = snapshot.decode_sid(sid) {
                values.push(iri);
            }
        }
    }

    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values))
    }
}

/// Read an integer field from a subject at the config graph.
async fn read_i64_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<i64>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, subject_sid, &pred_sid).await?;
    for binding in bindings {
        if let Some((fluree_db_core::FlakeValue::Long(n), _)) = binding.as_lit() {
            return Ok(Some(*n));
        }
    }
    Ok(None)
}

// ============================================================================
// Internal: Setting group readers
// ============================================================================

/// Read policy defaults from a parent subject (LedgerConfig or GraphConfig).
async fn read_policy_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<PolicyDefaults>> {
    // Follow f:policyDefaults ref to the group subject
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::POLICY_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let default_allow = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::DEFAULT_ALLOW,
    )
    .await?;
    let policy_source = read_graph_source_ref(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::POLICY_SOURCE,
    )
    .await?;
    let policy_class = read_iri_list_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        fluree_vocab::policy_iris::POLICY_CLASS,
    )
    .await?;
    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(PolicyDefaults {
        default_allow,
        policy_source,
        policy_class,
        override_control,
    }))
}

/// Read SHACL defaults from a parent subject.
async fn read_shacl_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<ShaclDefaults>> {
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::SHACL_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let enabled = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::SHACL_ENABLED,
    )
    .await?;
    let shapes_source = read_graph_source_ref(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::SHAPES_SOURCE,
    )
    .await?;
    let validation_mode = read_validation_mode(snapshot, overlay, to_t, &group_sid).await?;
    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(ShaclDefaults {
        enabled,
        shapes_source,
        validation_mode,
        override_control,
    }))
}

/// Read reasoning defaults from a parent subject.
async fn read_reasoning_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<ReasoningDefaults>> {
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::REASONING_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let modes = read_iri_list_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::REASONING_MODES,
    )
    .await?;
    let schema_source = read_graph_source_ref(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::SCHEMA_SOURCE,
    )
    .await?;
    let follow_owl_imports = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::FOLLOW_OWL_IMPORTS,
    )
    .await?;
    let ontology_import_map = read_ontology_import_map(snapshot, overlay, to_t, &group_sid).await?;
    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(ReasoningDefaults {
        modes,
        schema_source,
        follow_owl_imports,
        ontology_import_map,
        override_control,
    }))
}

/// Read an `f:ontologyImportMap` as a list of [`OntologyImportBinding`].
///
/// Each binding subject has:
/// - `f:ontologyIri` — the external import IRI (IRI ref)
/// - `f:graphRef` — nested `f:GraphRef` resolved via [`read_single_graph_ref_from_sid`]
///
/// Bindings missing either field are skipped with a debug log rather than
/// failing the whole config read — strict error semantics live at the
/// resolution layer in `fluree-db-api::ontology_imports`.
async fn read_ontology_import_map(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Vec<OntologyImportBinding>> {
    let pred_sid = match try_encode(snapshot, config_iris::ONTOLOGY_IMPORT_MAP) {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, parent_sid, &pred_sid).await?;

    let mut result = Vec::new();
    for binding in bindings {
        let Some(entry_sid) = binding.as_sid() else {
            continue;
        };
        let Some(ontology_iri) = read_iri_field(
            snapshot,
            overlay,
            to_t,
            entry_sid,
            config_iris::ONTOLOGY_IRI,
        )
        .await?
        else {
            tracing::debug!("f:ontologyImportMap entry missing f:ontologyIri — skipping");
            continue;
        };
        let Some(graph_ref_sid) = read_ref_field(
            snapshot,
            overlay,
            to_t,
            entry_sid,
            config_iris::GRAPH_REF_PROP,
        )
        .await?
        else {
            tracing::debug!(
                ontology_iri = %ontology_iri,
                "f:ontologyImportMap entry missing f:graphRef — skipping"
            );
            continue;
        };
        let Some(graph_ref) =
            read_single_graph_ref_from_sid(snapshot, overlay, to_t, &graph_ref_sid).await?
        else {
            tracing::debug!(
                ontology_iri = %ontology_iri,
                "f:ontologyImportMap entry's f:graphRef could not be resolved — skipping"
            );
            continue;
        };
        result.push(OntologyImportBinding {
            ontology_iri,
            graph_ref,
        });
    }
    Ok(result)
}

/// Read datalog defaults from a parent subject.
async fn read_datalog_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<DatalogDefaults>> {
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::DATALOG_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let enabled = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::DATALOG_ENABLED,
    )
    .await?;
    let rules_source = read_graph_source_ref(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::RULES_SOURCE,
    )
    .await?;
    let allow_query_time_rules = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::ALLOW_QUERY_TIME_RULES,
    )
    .await?;
    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(DatalogDefaults {
        enabled,
        rules_source,
        allow_query_time_rules,
        override_control,
    }))
}

/// Read transact defaults from a parent subject (LedgerConfig or GraphConfig).
///
/// `f:transactDefaults` points to a group with `f:uniqueEnabled` (bool) and
/// `f:constraintsSource` (one or more `f:GraphRef`). Multiple sources are
/// supported for additive per-graph merging.
async fn read_transact_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<TransactDefaults>> {
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::TRANSACT_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let unique_enabled = read_bool_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::UNIQUE_ENABLED,
    )
    .await?;
    let constraints_sources = read_graph_source_refs(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::CONSTRAINTS_SOURCE,
    )
    .await?;
    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(TransactDefaults {
        unique_enabled,
        constraints_sources,
        override_control,
    }))
}

/// Read a plain string field (e.g., BCP-47 language tag) from a subject.
async fn read_string_field(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<String>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, subject_sid, &pred_sid).await?;
    for binding in bindings {
        if let Some((fluree_db_core::FlakeValue::String(s), _)) = binding.as_lit() {
            return Ok(Some(s.clone()));
        }
    }
    Ok(None)
}

/// Read full-text defaults from a parent subject (LedgerConfig or GraphConfig).
///
/// `f:fullTextDefaults` points to a group with:
///  - `f:defaultLanguage` — optional BCP-47 string
///  - `f:property` — 0..n refs to `f:FullTextProperty` nodes (each with `f:target`)
///  - `f:overrideControl` — optional override control
///
/// Absent fields yield `None` / empty. Returns `None` if the parent has no
/// `f:fullTextDefaults` at all (not even an empty one).
async fn read_fulltext_defaults(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<FullTextDefaults>> {
    let group_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::FULL_TEXT_DEFAULTS,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let default_language = read_string_field(
        snapshot,
        overlay,
        to_t,
        &group_sid,
        config_iris::DEFAULT_LANGUAGE,
    )
    .await?;

    // Read each `f:property` ref and resolve its `f:target` IRI.
    let pred_sid = try_encode(snapshot, config_iris::FULL_TEXT_PROPERTY);
    let mut properties = Vec::new();
    if let Some(pred_sid) = pred_sid {
        let bindings =
            query_config_predicate(snapshot, overlay, to_t, &group_sid, &pred_sid).await?;
        for binding in bindings {
            if let Some(prop_sid) = binding.as_sid() {
                let target = read_iri_field(
                    snapshot,
                    overlay,
                    to_t,
                    prop_sid,
                    config_iris::FULL_TEXT_TARGET,
                )
                .await?;
                match target {
                    Some(iri) => properties.push(FullTextProperty { target: iri }),
                    None => {
                        tracing::warn!("FullTextProperty node without f:target — skipping");
                    }
                }
            }
        }
    }

    let override_control = read_override_control(snapshot, overlay, to_t, &group_sid).await?;

    Ok(Some(FullTextDefaults {
        default_language,
        properties,
        override_control,
    }))
}

/// Read per-graph config overrides (`f:graphOverrides`).
async fn read_graph_overrides(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    config_sid: &Sid,
) -> Result<Vec<GraphConfig>> {
    let pred_sid = match try_encode(snapshot, config_iris::GRAPH_OVERRIDES) {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, config_sid, &pred_sid).await?;

    let mut overrides = Vec::new();
    for binding in bindings {
        if let Some(gc_sid) = binding.as_sid() {
            if let Some(gc) = read_single_graph_config(snapshot, overlay, to_t, gc_sid).await? {
                overrides.push(gc);
            }
        }
    }

    Ok(overrides)
}

/// Read a single `f:GraphConfig` resource.
async fn read_single_graph_config(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    gc_sid: &Sid,
) -> Result<Option<GraphConfig>> {
    // f:targetGraph is required
    let target_graph =
        match read_iri_field(snapshot, overlay, to_t, gc_sid, config_iris::TARGET_GRAPH).await? {
            Some(iri) => iri,
            None => {
                tracing::warn!("GraphConfig without f:targetGraph — skipping");
                return Ok(None);
            }
        };

    let policy = read_policy_defaults(snapshot, overlay, to_t, gc_sid).await?;
    let shacl = read_shacl_defaults(snapshot, overlay, to_t, gc_sid).await?;
    let reasoning = read_reasoning_defaults(snapshot, overlay, to_t, gc_sid).await?;
    let datalog = read_datalog_defaults(snapshot, overlay, to_t, gc_sid).await?;
    let transact = read_transact_defaults(snapshot, overlay, to_t, gc_sid).await?;
    let full_text = read_fulltext_defaults(snapshot, overlay, to_t, gc_sid).await?;

    Ok(Some(GraphConfig {
        target_graph,
        policy,
        shacl,
        reasoning,
        datalog,
        transact,
        full_text,
    }))
}

/// Read override control from a setting group subject.
///
/// Reads `f:overrideControl` which can be:
/// - IRI: `f:OverrideNone` → `OverrideControl::None`
/// - IRI: `f:OverrideAll` → `OverrideControl::AllowAll`
/// - Ref to an object with `f:controlMode` = `f:IdentityRestricted` and `f:allowedIdentities` (IRI list)
///
/// Defaults to `AllowAll` if not specified.
async fn read_override_control(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    group_sid: &Sid,
) -> Result<OverrideControl> {
    let ref_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        group_sid,
        config_iris::OVERRIDE_CONTROL,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(OverrideControl::default()),
    };

    // Decode to IRI and check for simple mode values
    if let Some(iri) = snapshot.decode_sid(&ref_sid) {
        if iri == config_iris::OVERRIDE_NONE {
            return Ok(OverrideControl::None);
        }
        if iri == config_iris::OVERRIDE_ALL {
            return Ok(OverrideControl::AllowAll);
        }
    }

    // Otherwise it's an object — read controlMode (IRI) and allowedIdentities (IRI list)
    let control_mode_iri =
        read_iri_field(snapshot, overlay, to_t, &ref_sid, config_iris::CONTROL_MODE).await?;

    if control_mode_iri.as_deref() == Some(config_iris::IDENTITY_RESTRICTED) {
        let identities = read_iri_list_field(
            snapshot,
            overlay,
            to_t,
            &ref_sid,
            config_iris::ALLOWED_IDENTITIES,
        )
        .await?;
        let set: HashSet<Arc<str>> = identities
            .unwrap_or_default()
            .into_iter()
            .map(|s| Arc::from(s.as_str()))
            .collect();
        return Ok(OverrideControl::IdentityRestricted {
            allowed_identities: set,
        });
    }

    // Unrecognized — default to AllowAll
    tracing::warn!(
        control_mode = ?control_mode_iri,
        "Unrecognized override control mode — defaulting to AllowAll"
    );
    Ok(OverrideControl::AllowAll)
}

/// Read a `GraphSourceRef` from a predicate on a subject.
///
/// The proposal specifies a two-level structure:
/// ```json
/// "f:policySource": {
///   "@type": "f:GraphRef",
///   "f:graphSource": { "f:ledger": {"@id": "..."}, "f:graphSelector": {"@id": "..."}, "f:atT": 123 },
///   "f:trustPolicy": { "f:trustMode": {"@id": "f:Trusted"} }
/// }
/// ```
///
/// `f:graphSource` is a nested object containing source coordinates;
/// `f:trustPolicy` sits on the `f:GraphRef` level.
async fn read_graph_source_ref(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
    pred_iri: &str,
) -> Result<Option<GraphSourceRef>> {
    // Follow pred_iri to get the f:GraphRef subject
    let ref_sid = match read_ref_field(snapshot, overlay, to_t, parent_sid, pred_iri).await? {
        Some(sid) => sid,
        None => return Ok(None),
    };

    // Follow f:graphSource to the nested source object
    let source_sid =
        match read_ref_field(snapshot, overlay, to_t, &ref_sid, config_iris::GRAPH_SOURCE).await? {
            Some(sid) => sid,
            None => {
                tracing::debug!("GraphRef without f:graphSource — no source coordinates");
                return Ok(Some(GraphSourceRef {
                    ledger: None,
                    graph_selector: None,
                    at_t: None,
                    trust_policy: read_trust_policy(snapshot, overlay, to_t, &ref_sid).await?,
                    rollback_guard: read_rollback_guard(snapshot, overlay, to_t, &ref_sid).await?,
                }));
            }
        };

    // Read source fields from the nested f:graphSource object
    let ledger = read_iri_field(
        snapshot,
        overlay,
        to_t,
        &source_sid,
        config_iris::LEDGER_PRED,
    )
    .await?;
    let graph_selector = read_iri_field(
        snapshot,
        overlay,
        to_t,
        &source_sid,
        config_iris::GRAPH_SELECTOR,
    )
    .await?;
    let at_t = read_i64_field(snapshot, overlay, to_t, &source_sid, config_iris::AT_T).await?;

    // Read trust policy and rollback guard from the GraphRef level (not the nested source)
    let trust_policy = read_trust_policy(snapshot, overlay, to_t, &ref_sid).await?;
    let rollback_guard = read_rollback_guard(snapshot, overlay, to_t, &ref_sid).await?;

    Ok(Some(GraphSourceRef {
        ledger,
        graph_selector,
        at_t,
        trust_policy,
        rollback_guard,
    }))
}

/// Read zero or more `GraphSourceRef` values from a multi-valued predicate.
///
/// Unlike `read_graph_source_ref()` which returns `Option<GraphSourceRef>`,
/// this returns a `Vec` — each object of the predicate is independently
/// resolved as a `GraphSourceRef`. Used for `f:constraintsSource` which
/// supports multiple constraint annotation sources.
async fn read_graph_source_refs(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
    pred_iri: &str,
) -> Result<Vec<GraphSourceRef>> {
    let pred_sid = match try_encode(snapshot, pred_iri) {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    let bindings = query_config_predicate(snapshot, overlay, to_t, parent_sid, &pred_sid).await?;

    let mut refs = Vec::new();
    for binding in bindings {
        if let Some(ref_sid) = binding.as_sid() {
            if let Some(gsr) =
                read_single_graph_ref_from_sid(snapshot, overlay, to_t, ref_sid).await?
            {
                refs.push(gsr);
            }
        }
    }

    Ok(refs)
}

/// Read a `GraphSourceRef` from a known ref SID (the `f:GraphRef` subject).
///
/// Shared logic between `read_graph_source_ref()` (single-valued) and
/// `read_graph_source_refs()` (multi-valued).
async fn read_single_graph_ref_from_sid(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    ref_sid: &Sid,
) -> Result<Option<GraphSourceRef>> {
    // Follow f:graphSource to the nested source object
    let source_sid =
        match read_ref_field(snapshot, overlay, to_t, ref_sid, config_iris::GRAPH_SOURCE).await? {
            Some(sid) => sid,
            None => {
                tracing::debug!("GraphRef without f:graphSource — no source coordinates");
                return Ok(Some(GraphSourceRef {
                    ledger: None,
                    graph_selector: None,
                    at_t: None,
                    trust_policy: read_trust_policy(snapshot, overlay, to_t, ref_sid).await?,
                    rollback_guard: read_rollback_guard(snapshot, overlay, to_t, ref_sid).await?,
                }));
            }
        };

    let ledger = read_iri_field(
        snapshot,
        overlay,
        to_t,
        &source_sid,
        config_iris::LEDGER_PRED,
    )
    .await?;
    let graph_selector = read_iri_field(
        snapshot,
        overlay,
        to_t,
        &source_sid,
        config_iris::GRAPH_SELECTOR,
    )
    .await?;
    let at_t = read_i64_field(snapshot, overlay, to_t, &source_sid, config_iris::AT_T).await?;

    let trust_policy = read_trust_policy(snapshot, overlay, to_t, ref_sid).await?;
    let rollback_guard = read_rollback_guard(snapshot, overlay, to_t, ref_sid).await?;

    Ok(Some(GraphSourceRef {
        ledger,
        graph_selector,
        at_t,
        trust_policy,
        rollback_guard,
    }))
}

/// Read a `TrustPolicy` from a subject.
async fn read_trust_policy(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<TrustPolicy>> {
    let tp_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::TRUST_POLICY_PRED,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let mode_iri =
        read_iri_field(snapshot, overlay, to_t, &tp_sid, config_iris::TRUST_MODE).await?;
    let trust_mode = match mode_iri.as_deref() {
        Some(iri) if iri == config_iris::TRUSTED => TrustMode::Trusted,
        Some(iri) if iri == config_iris::SIGNED_INDEX => TrustMode::SignedIndex,
        Some(iri) if iri == config_iris::COMMIT_VERIFIED => TrustMode::CommitVerified,
        _ => TrustMode::Trusted, // default
    };

    Ok(Some(TrustPolicy { trust_mode }))
}

/// Read a `RollbackGuard` from a subject.
///
/// `f:rollbackGuard` is a sibling of `f:trustPolicy` on the `f:GraphRef` level.
async fn read_rollback_guard(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    parent_sid: &Sid,
) -> Result<Option<RollbackGuard>> {
    let rg_sid = match read_ref_field(
        snapshot,
        overlay,
        to_t,
        parent_sid,
        config_iris::ROLLBACK_GUARD,
    )
    .await?
    {
        Some(sid) => sid,
        None => return Ok(None),
    };

    let min_t = read_i64_field(snapshot, overlay, to_t, &rg_sid, config_iris::MIN_T).await?;

    Ok(Some(RollbackGuard { min_t }))
}

/// Read the validation mode IRI field.
async fn read_validation_mode(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    to_t: i64,
    group_sid: &Sid,
) -> Result<Option<ValidationMode>> {
    let mode_iri = read_iri_field(
        snapshot,
        overlay,
        to_t,
        group_sid,
        config_iris::VALIDATION_MODE,
    )
    .await?;
    Ok(match mode_iri.as_deref() {
        Some(iri) if iri == config_iris::VALIDATION_REJECT => Some(ValidationMode::Reject),
        Some(iri) if iri == config_iris::VALIDATION_WARN => Some(ValidationMode::Warn),
        _ => None,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ledger_config::OverrideControl;
    use std::collections::HashSet;

    fn identity_set(ids: &[&str]) -> HashSet<Arc<str>> {
        ids.iter().map(|s| Arc::from(*s)).collect()
    }

    // --- resolve_effective_config ---

    #[test]
    fn empty_config_gives_all_none() {
        let config = LedgerConfig::default();
        let resolved = resolve_effective_config(&config, None);
        assert!(resolved.policy.is_none());
        assert!(resolved.shacl.is_none());
        assert!(resolved.reasoning.is_none());
        assert!(resolved.datalog.is_none());
        assert!(resolved.transact.is_none());
    }

    #[test]
    fn ledger_wide_policy_propagates() {
        let config = LedgerConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                policy_class: Some(vec!["ex:AdminPolicy".into()]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = resolve_effective_config(&config, None);
        let p = resolved.policy.unwrap();
        assert_eq!(p.default_allow, Some(false));
        assert_eq!(
            p.policy_class.as_deref(),
            Some(&["ex:AdminPolicy".into()][..])
        );
    }

    #[test]
    fn per_graph_override_merges_correctly() {
        let config = LedgerConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(true),
                policy_class: Some(vec!["ex:Base".into()]),
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                policy: Some(PolicyDefaults {
                    default_allow: Some(false), // override
                    policy_class: None,         // inherit from ledger-wide
                    ..Default::default()
                }),
                shacl: None,
                reasoning: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        // Default graph (matches the override)
        let resolved = resolve_effective_config(&config, None);
        let p = resolved.policy.unwrap();
        assert_eq!(p.default_allow, Some(false)); // per-graph wins
        assert_eq!(p.policy_class.as_deref(), Some(&["ex:Base".into()][..])); // inherited
    }

    #[test]
    fn non_matching_per_graph_falls_back_to_ledger_wide() {
        let config = LedgerConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: "urn:other:graph".into(),
                reasoning: Some(ReasoningDefaults {
                    modes: Some(vec!["owl2-rl".into()]),
                    ..Default::default()
                }),
                policy: None,
                shacl: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        // Default graph — doesn't match "urn:other:graph"
        let resolved = resolve_effective_config(&config, None);
        let r = resolved.reasoning.unwrap();
        assert_eq!(r.modes.as_deref(), Some(&["rdfs".into()][..]));
    }

    #[test]
    fn override_control_tightens_on_merge() {
        let config = LedgerConfig {
            policy: Some(PolicyDefaults {
                override_control: OverrideControl::AllowAll,
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                policy: Some(PolicyDefaults {
                    override_control: OverrideControl::None, // tighter
                    ..Default::default()
                }),
                shacl: None,
                reasoning: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        let p = resolved.policy.unwrap();
        assert!(matches!(p.override_control, OverrideControl::None));
    }

    #[test]
    fn ledger_wide_override_none_blocks_per_graph_policy() {
        // Truth table: `defaultAllow: false`, OverrideNone | `defaultAllow: true` | → **deny**
        let config = LedgerConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                policy: Some(PolicyDefaults {
                    default_allow: Some(true), // attempts to loosen — should be blocked
                    ..Default::default()
                }),
                shacl: None,
                reasoning: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        let p = resolved.policy.unwrap();
        assert_eq!(p.default_allow, Some(false)); // ledger-wide wins
        assert!(matches!(p.override_control, OverrideControl::None));
    }

    #[test]
    fn ledger_wide_override_none_blocks_per_graph_reasoning() {
        // Truth table: `reasoningModes: [rdfs]`, OverrideNone | `[owl2-rl]` | → **rdfs**
        let config = LedgerConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                reasoning: Some(ReasoningDefaults {
                    modes: Some(vec!["owl2-rl".into()]),
                    ..Default::default()
                }),
                policy: None,
                shacl: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        let r = resolved.reasoning.unwrap();
        assert_eq!(r.modes.as_deref(), Some(&["rdfs".into()][..])); // ledger-wide wins
    }

    #[test]
    fn ledger_wide_override_none_blocks_per_graph_shacl() {
        // Truth table: `shaclEnabled: false`, OverrideNone | `shaclEnabled: true` | → **disabled**
        let config = LedgerConfig {
            shacl: Some(ShaclDefaults {
                enabled: Some(false),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                shacl: Some(ShaclDefaults {
                    enabled: Some(true),
                    ..Default::default()
                }),
                policy: None,
                reasoning: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        let s = resolved.shacl.unwrap();
        assert_eq!(s.enabled, Some(false)); // ledger-wide wins
    }

    #[test]
    fn ledger_wide_override_none_independent_per_group() {
        // OverrideNone on policy should NOT affect reasoning (per-group independence)
        let config = LedgerConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                override_control: OverrideControl::AllowAll,
                ..Default::default()
            }),
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                policy: Some(PolicyDefaults {
                    default_allow: Some(true),
                    ..Default::default()
                }),
                reasoning: Some(ReasoningDefaults {
                    modes: Some(vec!["owl2-rl".into()]),
                    ..Default::default()
                }),
                shacl: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        // Policy: blocked by OverrideNone
        let p = resolved.policy.unwrap();
        assert_eq!(p.default_allow, Some(false));
        // Reasoning: per-graph wins (AllowAll)
        let r = resolved.reasoning.unwrap();
        assert_eq!(r.modes.as_deref(), Some(&["owl2-rl".into()][..]));
    }

    #[test]
    fn sentinel_default_graph_matches_none_iri() {
        let config = LedgerConfig {
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::DEFAULT_GRAPH.into(),
                reasoning: Some(ReasoningDefaults {
                    modes: Some(vec!["owl2-ql".into()]),
                    ..Default::default()
                }),
                policy: None,
                shacl: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, None);
        assert!(resolved.reasoning.is_some());
        assert_eq!(
            resolved.reasoning.unwrap().modes.as_deref(),
            Some(&["owl2-ql".into()][..])
        );
    }

    #[test]
    fn sentinel_txn_meta_matches_txn_meta_iri() {
        let config = LedgerConfig {
            graph_overrides: vec![GraphConfig {
                target_graph: config_iris::TXN_META_GRAPH.into(),
                reasoning: Some(ReasoningDefaults {
                    modes: Some(vec!["owl2-rl".into()]),
                    ..Default::default()
                }),
                policy: None,
                shacl: None,
                datalog: None,
                transact: None,
                full_text: None,
            }],
            ..Default::default()
        };

        let resolved = resolve_effective_config(&config, Some("urn:fluree:mydb:main#txn-meta"));
        assert!(resolved.reasoning.is_some());
    }

    // --- merge_policy_opts ---

    #[test]
    fn no_config_policy_returns_opts_unchanged() {
        let resolved = ResolvedConfig::default();
        let opts = QueryConnectionOptions {
            identity: Some("did:key:alice".into()),
            ..Default::default()
        };
        let merged = merge_policy_opts(&resolved, &opts, None);
        assert_eq!(merged.identity.as_deref(), Some("did:key:alice"));
    }

    #[test]
    fn applies_config_defaults_when_no_query_policy() {
        let resolved = ResolvedConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                policy_class: Some(vec!["ex:DefaultPolicy".into()]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = QueryConnectionOptions::default();
        let merged = merge_policy_opts(&resolved, &opts, None);
        assert!(!merged.default_allow);
        assert_eq!(
            merged.policy_class.as_deref(),
            Some(&["ex:DefaultPolicy".into()][..])
        );
    }

    #[test]
    fn permits_override_with_allow_all() {
        let resolved = ResolvedConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                override_control: OverrideControl::AllowAll,
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = QueryConnectionOptions {
            identity: Some("did:key:alice".into()),
            ..Default::default()
        };
        let merged = merge_policy_opts(&resolved, &opts, None);
        // Query opts kept because AllowAll permits override
        assert_eq!(merged.identity.as_deref(), Some("did:key:alice"));
    }

    #[test]
    fn denies_override_with_control_none() {
        let resolved = ResolvedConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                policy_class: Some(vec!["ex:Locked".into()]),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = QueryConnectionOptions {
            identity: Some("did:key:alice".into()),
            ..Default::default()
        };
        let merged = merge_policy_opts(&resolved, &opts, Some("did:key:alice"));
        // Config defaults applied despite query specifying identity
        assert!(!merged.default_allow);
        assert_eq!(
            merged.policy_class.as_deref(),
            Some(&["ex:Locked".into()][..])
        );
    }

    #[test]
    fn identity_restricted_matching_permits_override() {
        let resolved = ResolvedConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                override_control: OverrideControl::IdentityRestricted {
                    allowed_identities: identity_set(&["did:key:admin"]),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = QueryConnectionOptions {
            identity: Some("did:key:user".into()),
            ..Default::default()
        };
        let merged = merge_policy_opts(&resolved, &opts, Some("did:key:admin"));
        // Server identity is admin → override permitted
        assert_eq!(merged.identity.as_deref(), Some("did:key:user"));
    }

    #[test]
    fn identity_restricted_non_matching_denies_override() {
        let resolved = ResolvedConfig {
            policy: Some(PolicyDefaults {
                default_allow: Some(false),
                policy_class: Some(vec!["ex:Restricted".into()]),
                override_control: OverrideControl::IdentityRestricted {
                    allowed_identities: identity_set(&["did:key:admin"]),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = QueryConnectionOptions {
            identity: Some("did:key:user".into()),
            ..Default::default()
        };
        let merged = merge_policy_opts(&resolved, &opts, Some("did:key:non-admin"));
        // Server identity is not admin → override denied
        assert!(!merged.default_allow);
        assert_eq!(
            merged.policy_class.as_deref(),
            Some(&["ex:Restricted".into()][..])
        );
    }

    // --- merge_reasoning ---

    #[test]
    fn no_config_reasoning_returns_none() {
        let resolved = ResolvedConfig::default();
        assert!(merge_reasoning(&resolved, None).is_none());
    }

    #[test]
    fn config_reasoning_with_control_none_gives_force() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                override_control: OverrideControl::None,
                ..Default::default()
            }),
            ..Default::default()
        };
        let (modes, prec) = merge_reasoning(&resolved, None).unwrap();
        assert_eq!(modes, vec!["rdfs".to_string()]);
        assert_eq!(prec, ReasoningModePrecedence::Force);
    }

    #[test]
    fn config_reasoning_with_allow_all_gives_default_unless() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["owl2-rl".into()]),
                override_control: OverrideControl::AllowAll,
                ..Default::default()
            }),
            ..Default::default()
        };
        let (modes, prec) = merge_reasoning(&resolved, None).unwrap();
        assert_eq!(modes, vec!["owl2-rl".to_string()]);
        assert_eq!(prec, ReasoningModePrecedence::DefaultUnlessQueryOverrides);
    }

    #[test]
    fn config_reasoning_identity_restricted_matching_gives_default_unless() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                override_control: OverrideControl::IdentityRestricted {
                    allowed_identities: identity_set(&["did:key:admin"]),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let (_modes, prec) = merge_reasoning(&resolved, Some("did:key:admin")).unwrap();
        assert_eq!(prec, ReasoningModePrecedence::DefaultUnlessQueryOverrides);
    }

    #[test]
    fn config_reasoning_identity_restricted_non_matching_gives_force() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec!["rdfs".into()]),
                override_control: OverrideControl::IdentityRestricted {
                    allowed_identities: identity_set(&["did:key:admin"]),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let (_modes, prec) = merge_reasoning(&resolved, Some("did:key:user")).unwrap();
        assert_eq!(prec, ReasoningModePrecedence::Force);
    }

    #[test]
    fn empty_reasoning_modes_returns_none() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: Some(vec![]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(merge_reasoning(&resolved, None).is_none());
    }

    #[test]
    fn no_modes_field_returns_none() {
        let resolved = ResolvedConfig {
            reasoning: Some(ReasoningDefaults {
                modes: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(merge_reasoning(&resolved, None).is_none());
    }

    // --- matches_graph_target ---

    #[test]
    fn matches_default_graph_sentinel() {
        assert!(matches_graph_target(config_iris::DEFAULT_GRAPH, None));
        assert!(!matches_graph_target(
            config_iris::DEFAULT_GRAPH,
            Some("urn:other")
        ));
    }

    #[test]
    fn matches_exact_iri() {
        assert!(matches_graph_target("urn:my:graph", Some("urn:my:graph")));
        assert!(!matches_graph_target("urn:my:graph", Some("urn:other")));
    }

    #[test]
    fn matches_txn_meta_sentinel() {
        assert!(matches_graph_target(
            config_iris::TXN_META_GRAPH,
            Some("urn:fluree:mydb:main#txn-meta")
        ));
        assert!(!matches_graph_target(config_iris::TXN_META_GRAPH, None));
    }
}
