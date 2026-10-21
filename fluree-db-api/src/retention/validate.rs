//! Stage-time checks on writes that carry retention control input.
//!
//! The staged-config guard's pass over the staged flakes calls [`note`] on
//! each flake; a write that asserts or retracts no TTL instruction and names
//! no retention term in the config graph does no more work here. Otherwise
//! [`check_staged`] validates what the write states and marks its commit
//! with `f:retentionControl`, the activation boundary for per-value
//! instructions.

use std::collections::HashMap;

use fluree_db_core::graph_registry::CONFIG_GRAPH_ID;
use fluree_db_core::{
    range_with_overlay, Flake, FlakeValue, GraphId, IndexType, RangeMatch, RangeOptions, RangeTest,
    Sid, TripleTermValue,
};
use fluree_db_ledger::StagedLedger;
use fluree_db_transact::TransactError;
use fluree_vocab::config_iris;
use fluree_vocab::namespaces::{FLUREE_DB, RDF};

use super::is_ttl_predicate;
use crate::config_guard::GuardError;

/// What a staged write carries for retention.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetentionSignals {
    /// A TTL instruction asserted or retracted in a user graph.
    pub(crate) ttl: bool,
    /// A retention term written in the config graph.
    pub(crate) config: bool,
}

impl RetentionSignals {
    pub(crate) fn any(self) -> bool {
        self.ttl || self.config
    }
}

/// Note what `flake`, staged into graph `g_id`, carries for retention.
#[inline]
pub(crate) fn note(signals: &mut RetentionSignals, g_id: GraphId, flake: &Flake) {
    if flake.p.namespace_code != FLUREE_DB
        && !(g_id == CONFIG_GRAPH_ID && flake.p.namespace_code == RDF)
    {
        return;
    }
    if g_id == CONFIG_GRAPH_ID {
        signals.config |= is_retention_config_term(flake);
    } else if !crate::export::is_system_graph(g_id) && is_ttl_predicate(&flake.p) {
        signals.ttl = true;
    }
}

fn local(iri: &'static str) -> &'static str {
    iri.strip_prefix(fluree_vocab::fluree::DB).unwrap_or(iri)
}

fn is_retention_config_term(flake: &Flake) -> bool {
    if flake.p.namespace_code == FLUREE_DB {
        let name = &*flake.p.name;
        return name == local(config_iris::RETENTION_DEFAULTS)
            || name == local(config_iris::NO_HISTORY)
            || name == local(config_iris::TTL_SECONDS);
    }
    fluree_db_core::is_rdf_type(&flake.p)
        && matches!(&flake.o, FlakeValue::Ref(c) if c.namespace_code == FLUREE_DB
            && (&*c.name == local(config_iris::RETENTION_DEFAULTS_CLASS)
                || &*c.name == local(config_iris::RETENTION_PROPERTY_CLASS)))
}

/// Validate what a staged write carries for retention and mark its commit.
///
/// `authoring` is false for branch operations, which carry instructions an
/// earlier write already validated: they are marked, not re-checked.
pub(crate) async fn check_staged(
    view: &mut StagedLedger,
    signals: RetentionSignals,
    policy: Option<&crate::PolicyContext>,
    tracker: Option<&fluree_db_core::tracking::Tracker>,
    authoring: bool,
) -> Result<(), GuardError> {
    if !signals.any() {
        return Ok(());
    }
    if authoring {
        if signals.config {
            validate_retention_config(view).await?;
        }
        if signals.ttl {
            validate_ttl_instructions(view, policy, tracker).await?;
        }
    }
    view.push_system_txn_meta(super::retention_control_marker());
    Ok(())
}

/// Refuse retention config the reader would not apply as written. The guard
/// and the background worker read it with the same reader, so a config this
/// accepts is one the worker runs, and one it refuses could not become a
/// silently disabled policy.
async fn validate_retention_config(view: &StagedLedger) -> Result<(), GuardError> {
    let snapshot = &view.base().snapshot;
    let config = crate::config_resolver::resolve_ledger_config(snapshot, view, view.staged_t())
        .await
        .map_err(|e| GuardError::Failed(TransactError::ConfigUnavailable(e.to_string())))?;
    let Some(config) = config else {
        return Ok(());
    };
    let mut problems: Vec<String> = Vec::new();
    if let Some(group) = &config.retention {
        problems.extend(group.problems.iter().cloned());
    }
    let mut graphs_with_retention: HashMap<&str, usize> = HashMap::new();
    for graph in &config.graph_overrides {
        let Some(group) = &graph.retention else {
            continue;
        };
        let target = graph.target_graph.as_str();
        if target == config_iris::TXN_META_GRAPH
            || target.ends_with("#txn-meta")
            || target.ends_with("#config")
        {
            problems.push(format!(
                "f:retentionDefaults may not govern the reserved graph <{target}>"
            ));
        }
        *graphs_with_retention.entry(target).or_default() += 1;
        problems.extend(group.problems.iter().cloned());
    }
    for (target, count) in graphs_with_retention {
        if count > 1 {
            problems.push(format!(
                "{count} f:GraphConfig overrides set f:retentionDefaults for <{target}>; \
                 a graph takes one"
            ));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    problems.sort();
    problems.dedup();
    Err(GuardError::Refused(format!(
        "invalid retention config: {}",
        problems.join("; ")
    )))
}

/// A TTL instruction asserted by the write: the reifier carrying it and the
/// graph it is in.
struct Instruction<'a> {
    g_id: GraphId,
    flake: &'a Flake,
}

async fn validate_ttl_instructions(
    view: &StagedLedger,
    policy: Option<&crate::PolicyContext>,
    tracker: Option<&fluree_db_core::tracking::Tracker>,
) -> Result<(), GuardError> {
    let instructions: Vec<Instruction<'_>> = view
        .staged_flakes_by_graph()
        .filter(|(g_id, flake)| {
            flake.op && !crate::export::is_system_graph(*g_id) && is_ttl_predicate(&flake.p)
        })
        .map(|(g_id, flake)| Instruction { g_id, flake })
        .collect();
    let mut targets: HashMap<(GraphId, TripleTermValue), Sid> = HashMap::new();
    let mut checked: Vec<(GraphId, Sid)> = Vec::new();
    for Instruction { g_id, flake } in instructions {
        check_instruction_value(flake).map_err(GuardError::Refused)?;
        if checked.contains(&(g_id, flake.s.clone())) {
            continue;
        }
        checked.push((g_id, flake.s.clone()));
        let reifier = &flake.s;
        let mut ttl_facts = staged(
            view,
            g_id,
            RangeMatch::subject_predicate(reifier.clone(), super::ttl_seconds_sid()),
        )
        .await?;
        ttl_facts.extend(
            staged(
                view,
                g_id,
                RangeMatch::subject_predicate(reifier.clone(), super::expires_at_sid()),
            )
            .await?,
        );
        if ttl_facts.len() > 1 {
            return Err(GuardError::Refused(
                "an annotation carries one TTL instruction: f:ttlSeconds or f:expiresAt, \
                 with one value"
                    .to_string(),
            ));
        }
        let links = staged(
            view,
            g_id,
            RangeMatch::subject_predicate(
                reifier.clone(),
                fluree_db_core::rdf_reifies_sid().clone(),
            ),
        )
        .await?;
        let target = match links.as_slice() {
            [link] => match &link.o {
                FlakeValue::TripleTerm(tt) => (**tt).clone(),
                _ => {
                    return Err(GuardError::Refused(
                        "a TTL instruction's annotation must reify a triple".to_string(),
                    ))
                }
            },
            [] => {
                return Err(GuardError::Refused(
                    "f:ttlSeconds / f:expiresAt is a TTL instruction only on an annotation \
                     (@annotation, @ttl, {| ... |}); this node reifies no triple"
                        .to_string(),
                ))
            }
            _ => {
                return Err(GuardError::Refused(
                    "a TTL instruction's annotation must identify one triple; this one \
                     reifies several"
                        .to_string(),
                ))
            }
        };
        if let Some(other) = targets.insert((g_id, target.clone()), reifier.clone()) {
            if other != *reifier {
                return Err(GuardError::Refused(
                    "two TTL instructions for the same value in one transaction".to_string(),
                ));
            }
        }
        if shared_relationship(view, g_id, &target).await? {
            return Err(GuardError::Refused(
                "TTL on a value that several annotations or relationships share is not \
                 supported: expiring it would remove every one of them"
                    .to_string(),
            ));
        }
        if let Some(policy) = policy {
            authorize_target(view, g_id, &target, policy, tracker).await?;
        }
    }
    Ok(())
}

/// The value rules for a TTL instruction: `f:ttlSeconds` a non-negative
/// whole number, `f:expiresAt` an `xsd:dateTime`.
pub(crate) fn check_instruction_value(flake: &Flake) -> Result<(), String> {
    if &*flake.p.name == fluree_vocab::db::TTL_SECONDS {
        return match &flake.o {
            FlakeValue::Long(n) if *n >= 0 => Ok(()),
            FlakeValue::Long(_) => Err("f:ttlSeconds may not be negative".to_string()),
            _ => Err("f:ttlSeconds must be a whole number of seconds".to_string()),
        };
    }
    match &flake.o {
        FlakeValue::DateTime(_) => Ok(()),
        _ => Err("f:expiresAt must be an xsd:dateTime with a time zone".to_string()),
    }
}

/// Whether the triple has more than one annotation or relationship besides
/// TTL-only annotations: expiring the triple would end all of them.
pub(crate) async fn shared_relationship(
    view: &StagedLedger,
    g_id: GraphId,
    target: &TripleTermValue,
) -> Result<bool, GuardError> {
    let reifiers = staged_reifiers(view, g_id, target).await?;
    let mut substantive = 0;
    for reifier in reifiers {
        let body = staged(view, g_id, RangeMatch::subject(reifier)).await?;
        if body
            .iter()
            .any(|f| !fluree_db_core::is_rdf_reifies(&f.p) && !is_ttl_predicate(&f.p))
        {
            substantive += 1;
        }
    }
    Ok(substantive > 1)
}

/// The subjects whose `rdf:reifies` link names `target` in `g_id`.
async fn staged_reifiers(
    view: &StagedLedger,
    g_id: GraphId,
    target: &TripleTermValue,
) -> Result<Vec<Sid>, GuardError> {
    let links = range_with_overlay(
        &view.base().snapshot,
        g_id,
        view,
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch::predicate_object(
            fluree_db_core::rdf_reifies_sid().clone(),
            FlakeValue::TripleTerm(Box::new(target.clone())),
        ),
        RangeOptions {
            to_t: Some(view.staged_t()),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| GuardError::Failed(TransactError::ConfigUnavailable(e.to_string())))?;
    let mut reifiers: Vec<Sid> = links.into_iter().map(|f| f.s).collect();
    reifiers.sort();
    reifiers.dedup();
    Ok(reifiers)
}

/// Setting a value's lifetime schedules its retraction, so it needs the
/// permission to retract the value, not only to write the annotation.
async fn authorize_target(
    view: &StagedLedger,
    g_id: GraphId,
    target: &TripleTermValue,
    policy: &crate::PolicyContext,
    tracker: Option<&fluree_db_core::tracking::Tracker>,
) -> Result<(), GuardError> {
    if policy.wrapper().is_root() {
        return Ok(());
    }
    let base = view.base();
    let graph = match g_id {
        0 => None,
        g => base
            .snapshot
            .graph_registry
            .iri_for_graph_id(g)
            .and_then(|iri| base.snapshot.encode_iri(iri)),
    };
    let meta = target
        .lang
        .clone()
        .map(fluree_db_core::FlakeMeta::with_lang);
    let retraction = Flake {
        g: graph,
        s: target.s.clone(),
        p: target.p.clone(),
        o: target.o.clone(),
        dt: target.dt.clone(),
        t: view.staged_t(),
        op: false,
        m: meta,
    };
    fluree_db_transact::enforce_modify_policy(&[retraction], policy, base, tracker)
        .await
        .map_err(GuardError::Failed)
}

/// Graph `g_id`'s flakes matching `pattern` in the staged state.
async fn staged(
    view: &StagedLedger,
    g_id: GraphId,
    pattern: RangeMatch,
) -> Result<Vec<Flake>, GuardError> {
    range_with_overlay(
        &view.base().snapshot,
        g_id,
        view,
        IndexType::Spot,
        RangeTest::Eq,
        pattern,
        RangeOptions {
            to_t: Some(view.staged_t()),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| GuardError::Failed(TransactError::ConfigUnavailable(e.to_string())))
}
