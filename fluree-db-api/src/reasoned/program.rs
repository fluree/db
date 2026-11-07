//! A reasoned head's program: what is registered, and the settled form a
//! query is matched against.
//!
//! A program is registered as the reasoning keys a query would carry
//! (`reasoning`, `rules`, `ontology`, `reasoningTime`, `@context`) plus an
//! optional `graph`, the IRI of the graph it reasons against. Both the worker
//! and a query settle reasoning the same way — ledger-config defaults,
//! precedence, rule sources, the schema bundle — and the **settled key** is a
//! digest of what that produced. A query is served from a generation only when
//! its settled key equals the generation's, so a change to the ledger's
//! reasoning configuration can never be answered from a head computed under
//! the old one.

use fluree_db_core::reasoned_head::{program_id, ClockPolicy, SemanticKey};
use fluree_db_core::{ContentId, ContentKind, GraphId};
use fluree_db_query::ir::ReasoningConfig;
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::{ApiError, Result};

/// The keys a registered program may carry.
const PROGRAM_KEYS: &[&str] = &[
    "@context",
    "reasoning",
    "rules",
    "ontology",
    "reasoningTime",
    "reasoningBudget",
    "graph",
];

/// The query keys a program contributes when it is settled like a query.
pub(crate) const QUERY_KEYS: &[&str] = &[
    "@context",
    "reasoning",
    "rules",
    "ontology",
    "reasoningTime",
    "reasoningBudget",
];

/// JSON with object keys sorted at every depth, so equal programs encode to
/// equal bytes.
pub(crate) fn canonical(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                out.insert(key.clone(), canonical(&map[key]));
            }
            JsonValue::Object(out)
        }
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

/// Canonical bytes: a sorted-key JSON encoding.
pub(crate) fn canonical_bytes(value: &JsonValue) -> Vec<u8> {
    // `canonical` builds maps in sorted order; serde_json writes them in
    // iteration order whether or not `preserve_order` is enabled.
    serde_json::to_vec(&canonical(value)).expect("JSON value serializes")
}

/// Validate a registration request and return its canonical form.
pub(crate) fn canonical_program(program: &JsonValue) -> Result<JsonValue> {
    let obj = program
        .as_object()
        .ok_or_else(|| ApiError::invalid_query("a reasoning program is a JSON object"))?;
    if let Some(key) = obj.keys().find(|k| !PROGRAM_KEYS.contains(&k.as_str())) {
        return Err(ApiError::invalid_query(format!(
            "a reasoning program carries only {PROGRAM_KEYS:?}; found `{key}`"
        )));
    }
    if !obj.contains_key("reasoning") && !obj.contains_key("rules") {
        return Err(ApiError::invalid_query(
            "a reasoning program names its reasoning (`reasoning`) or its rules (`rules`)",
        ));
    }
    if let Some(graph) = obj.get("graph") {
        if !graph.is_string() {
            return Err(ApiError::invalid_query(
                "a reasoning program's `graph` is the IRI of the graph it reasons against",
            ));
        }
    }
    Ok(canonical(program))
}

/// The registration's semantic key: the program by content, its graph and
/// its clock.
pub(crate) fn registration_key(canonical_program: &JsonValue) -> SemanticKey {
    let clock = match canonical_program
        .get("reasoningTime")
        .and_then(|v| v.as_str())
    {
        Some(time) => ClockPolicy::Fixed {
            time: time.to_string(),
        },
        None => ClockPolicy::SnapshotCommitTime,
    };
    SemanticKey {
        program: program_id(&canonical_bytes(canonical_program)),
        reasoned_graph: canonical_program
            .get("graph")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        clock,
    }
}

/// The query a program is settled as: its reasoning keys over a pattern that
/// is never executed.
pub(crate) fn settling_query(canonical_program: &JsonValue) -> JsonValue {
    let mut query = Map::new();
    for &key in QUERY_KEYS {
        if let Some(value) = canonical_program.get(key) {
            query.insert(key.to_string(), value.clone());
        }
    }
    query.insert("select".to_string(), serde_json::json!(["?s"]));
    query.insert(
        "where".to_string(),
        serde_json::json!({"@id": "?s", "?p": "?o"}),
    );
    JsonValue::Object(query)
}

/// The digest of settled reasoning, as a query and the worker both reach it,
/// for the graph `g_id` it reasons against. Budgets are left out: a head
/// holds only complete results, so a budget never changes what it holds.
pub(crate) fn settled_key(reasoning: &ReasoningConfig, g_id: GraphId) -> ContentId {
    let modes = &reasoning.modes;
    let schema_digest = reasoning
        .schema_bundle
        .as_ref()
        .filter(|bundle| !bundle.is_empty())
        .map(|bundle| {
            let mut hasher = Sha256::new();
            for flake in bundle.flakes_for_merge() {
                hasher.update(serde_json::to_vec(&flake).expect("flake serializes"));
            }
            hex::encode(hasher.finalize())
        });
    let settled = serde_json::json!({
        "rdfs": modes.rdfs,
        "owl2ql": modes.owl2ql,
        "datalog": modes.datalog,
        "owl2rl": modes.owl2rl,
        "owlDatalog": modes.owl_datalog,
        "rules": modes.rules.iter().map(canonical).collect::<Vec<_>>(),
        "ontology": modes.ontology.as_ref().map(canonical),
        "evaluationTime": modes.evaluation_time.as_ref().map(ToString::to_string),
        "rulesSource": reasoning.rules_source_g_id,
        "graph": g_id,
        "schema": schema_digest,
    });
    ContentId::new(ContentKind::LedgerConfig, &canonical_bytes(&settled))
}

/// Whether settled reasoning runs anything a head could hold.
pub(crate) fn materializes(reasoning: &ReasoningConfig) -> bool {
    let modes = &reasoning.modes;
    modes.owl2rl || modes.owl_datalog || modes.datalog || !modes.rules.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn equal_programs_have_equal_keys_whatever_their_key_order() {
        let a =
            canonical_program(&json!({"reasoning": "owl2rl", "@context": {"ex": "x:"}})).unwrap();
        let b =
            canonical_program(&json!({"@context": {"ex": "x:"}, "reasoning": "owl2rl"})).unwrap();
        assert_eq!(registration_key(&a), registration_key(&b));
    }

    #[test]
    fn a_fixed_time_is_part_of_the_key() {
        let fixed = canonical_program(
            &json!({"reasoning": "datalog", "reasoningTime": "2026-01-01T00:00:00Z"}),
        )
        .unwrap();
        assert!(matches!(
            registration_key(&fixed).clock,
            ClockPolicy::Fixed { .. }
        ));
        let snapshot = canonical_program(&json!({"reasoning": "datalog"})).unwrap();
        assert_eq!(
            registration_key(&snapshot).clock,
            ClockPolicy::SnapshotCommitTime
        );
    }

    #[test]
    fn unknown_keys_and_empty_programs_are_refused() {
        assert!(canonical_program(&json!({"reasoning": "owl2rl", "where": {}})).is_err());
        assert!(canonical_program(&json!({"@context": {}})).is_err());
        assert!(canonical_program(&json!({"reasoning": "owl2rl", "graph": 3})).is_err());
    }
}
