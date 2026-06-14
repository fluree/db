//! Conditional Cypher writes.
//!
//! Most Cypher writes lower to a single declarative `Txn` (one WHERE plus
//! delete/insert templates). A few need to **branch or error based on the
//! latest pre-write state** — they can't be represented faithfully by one
//! `Txn`:
//!
//! - `MERGE … ON MATCH SET` — create-and-`ON CREATE` if absent, else
//!   `ON MATCH SET` (mutually exclusive guards).
//! - bare `DELETE n` — must error if `n` still has relationships.
//! - `DELETE r` — must know whether retracting the base edge would affect
//!   parallel relationship identities.
//!
//! These resolve as a [`WritePlan::Conditional`]: the executor probes the
//! current writer snapshot, then resolves to a concrete `Txn` (or a typed
//! Cypher error) which commits through the **same** staging path as any other
//! write — keeping identity, policy, tracking, provenance, index config, cache
//! freshness, and retry behavior consistent. This mirrors how UPSERT reads
//! current state, derives the actual write, and commits it as one transaction.

use fluree_db_cypher::ast::{
    CypherAst, DeleteClause, Direction, Expr, Literal, MatchClause, MergeClause, NodePattern,
    Pattern, PatternPart, ProjectionItem, Query, ReadClause, RelPattern, ReturnClause, SetClause,
    Statement, Update, Variable, WriteClause,
};
use fluree_db_transact::ir::Txn;

/// A lowered Cypher write: either a ready-to-stage `Txn`, or a conditional
/// write that must probe the writer snapshot before it can be resolved.
pub enum WritePlan {
    /// A ready-to-stage transaction.
    Single(Box<Txn>),
    /// A write needing a pre-write probe. Boxed (both variants embed a large
    /// AST clause).
    Conditional(Box<ConditionalCypherWrite>),
}

/// A write that needs a pre-write probe to choose between branches.
pub enum ConditionalCypherWrite {
    /// Single-node `MERGE` with a non-empty `ON MATCH SET`: probe existence,
    /// then stage the create branch (when absent) or the `ON MATCH SET`
    /// (when present).
    MergeOnMatch(MergeClause),
    /// Bare `MATCH … DELETE n` (non-`DETACH`): probe whether any matched node
    /// still has a relationship, error if so, otherwise stage the node
    /// retraction (via the `DETACH DELETE` lowering — equivalent when there
    /// are no relationships).
    DeleteNode(Update),
}

/// Detect a write shape that requires a pre-write probe. Returns `None` for
/// the common single-`Txn` shapes (which lower directly).
pub fn detect_conditional(ast: &CypherAst) -> Option<ConditionalCypherWrite> {
    let Statement::Update(u) = &ast.statement else {
        return None;
    };
    if u.write_clauses.len() != 1 {
        return None;
    }
    match &u.write_clauses[0] {
        // MERGE … ON MATCH SET: standalone single-node MERGE with on-match.
        WriteClause::Merge(m) => {
            let single_node = m.pattern.parts.len() == 1 && m.pattern.parts[0].tail.is_empty();
            if u.read_clauses.is_empty() && !m.on_match.is_empty() && single_node {
                Some(ConditionalCypherWrite::MergeOnMatch(m.clone()))
            } else {
                None
            }
        }
        // Bare DELETE n (non-DETACH). DETACH DELETE lowers directly. DELETE on
        // a relationship variable is a different operation (deferred), so only
        // node-variable targets qualify here.
        WriteClause::Delete(d) => {
            if !d.detach && !u.read_clauses.is_empty() && !any_target_is_rel_var(u, d) {
                Some(ConditionalCypherWrite::DeleteNode(u.clone()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// True if any DELETE target is bound as a *relationship* variable in the
/// MATCH (i.e. it's a `DELETE r`, not a `DELETE n`).
fn any_target_is_rel_var(u: &Update, d: &DeleteClause) -> bool {
    let mut rel_vars = std::collections::HashSet::new();
    for clause in &u.read_clauses {
        if let ReadClause::Match(m) | ReadClause::OptionalMatch(m) = clause {
            for part in &m.pattern.parts {
                for (rel, _) in &part.tail {
                    if let Some(v) = &rel.var {
                        rel_vars.insert(v.name.as_str());
                    }
                }
            }
        }
    }
    d.targets.iter().any(|t| rel_vars.contains(t.name.as_str()))
}

/// Build a probe that returns at most one row when a node matching the MERGE
/// identity pattern exists: `MATCH (<node>) RETURN <var> LIMIT 1`. Built from
/// the AST (the node is cloned verbatim) so labels/properties — including
/// backtick-quoted identifiers — round-trip exactly, with no text
/// serialization. The node carries a variable (required for ON MATCH SET).
pub(crate) fn build_merge_probe_ast(node: &NodePattern) -> CypherAst {
    let span = node.span;
    let var = node
        .var
        .clone()
        .expect("MERGE node has a variable (checked before resolution)");
    CypherAst {
        statement: Statement::Query(Query {
            clauses: vec![ReadClause::Match(MatchClause {
                pattern: Pattern {
                    parts: vec![PatternPart {
                        path_var: None,
                        head: node.clone(),
                        tail: Vec::new(),
                        span,
                    }],
                    span,
                },
                where_clause: None,
                span,
            })],
            return_clause: ReturnClause {
                items: vec![ProjectionItem {
                    expr: Expr::Var(var),
                    alias: None,
                    span,
                }],
                distinct: false,
                order_by: Vec::new(),
                skip: None,
                limit: Some(Expr::Lit(Literal::Integer(1, span))),
                span,
            },
            union_tail: None,
            span,
        }),
        span,
    }
}

/// Build the on-match branch: `MATCH (pattern) SET <on_match>`. Reuses the
/// existing MATCH … SET lowering.
pub(crate) fn build_on_match_ast(merge: &MergeClause) -> CypherAst {
    let span = merge.span;
    CypherAst {
        statement: Statement::Update(Update {
            read_clauses: vec![ReadClause::Match(MatchClause {
                pattern: merge.pattern.clone(),
                where_clause: None,
                span,
            })],
            write_clauses: vec![WriteClause::Set(SetClause {
                items: merge.on_match.clone(),
                span,
            })],
            return_clause: None,
            span,
        }),
        span,
    }
}

/// The DELETE clause inside a `DeleteNode` plan (its single write clause).
pub(crate) fn delete_clause(update: &Update) -> Option<&DeleteClause> {
    match update.write_clauses.first() {
        Some(WriteClause::Delete(d)) => Some(d),
        _ => None,
    }
}

/// Build a probe that returns at most one row when a matched `target` node
/// still participates in a (reified) relationship — `<original MATCH>` plus a
/// `(target)-[__r]->(__o)` (outbound) or `(__s)-[__r]->(target)` (inbound)
/// hop, returning `target LIMIT 1`. Named relationship ⇒ only reified edges
/// match, which is exactly Cypher's notion of a relationship.
pub(crate) fn build_relationship_probe_ast(
    read_clauses: &[ReadClause],
    target: &Variable,
    inbound: bool,
) -> CypherAst {
    let span = target.span;
    let mk_var = |name: &str| Variable {
        name: name.to_string(),
        span,
    };
    let anon = |var: Variable| NodePattern {
        var: Some(var),
        labels: Vec::new(),
        props: None,
        span,
    };
    let rel = RelPattern {
        var: Some(mk_var("__cydel_r")),
        direction: Direction::Outgoing,
        types: Vec::new(),
        length: None,
        props: None,
        span,
    };
    let (head, tail_node) = if inbound {
        (anon(mk_var("__cydel_s")), anon(target.clone()))
    } else {
        (anon(target.clone()), anon(mk_var("__cydel_o")))
    };
    let rel_part = PatternPart {
        path_var: None,
        head,
        tail: vec![(rel, tail_node)],
        span,
    };
    let mut clauses: Vec<ReadClause> = read_clauses.to_vec();
    clauses.push(ReadClause::Match(MatchClause {
        pattern: Pattern {
            parts: vec![rel_part],
            span,
        },
        where_clause: None,
        span,
    }));

    CypherAst {
        statement: Statement::Query(Query {
            clauses,
            return_clause: ReturnClause {
                items: vec![ProjectionItem {
                    expr: Expr::Var(target.clone()),
                    alias: None,
                    span,
                }],
                distinct: false,
                order_by: Vec::new(),
                skip: None,
                limit: Some(Expr::Lit(Literal::Integer(1, span))),
                span,
            },
            union_tail: None,
            span,
        }),
        span,
    }
}

/// Build the deletion branch for a verified-relationship-free `DELETE n`: the
/// same statement as `DETACH DELETE n` (equivalent when there are no
/// relationships), which lowers to the in/out-bound retraction templates.
pub(crate) fn build_detach_delete_ast(update: &Update) -> CypherAst {
    let mut u = update.clone();
    for w in &mut u.write_clauses {
        if let WriteClause::Delete(d) = w {
            d.detach = true;
        }
    }
    CypherAst {
        statement: Statement::Update(u),
        span: update.span,
    }
}

/// Build the create branch: the MERGE with `ON MATCH SET` cleared (so it
/// lowers to the single-Txn create-if-absent path with `ON CREATE SET`).
pub(crate) fn build_create_ast(merge: &MergeClause) -> CypherAst {
    let span = merge.span;
    CypherAst {
        statement: Statement::Update(Update {
            read_clauses: Vec::new(),
            write_clauses: vec![WriteClause::Merge(MergeClause {
                pattern: merge.pattern.clone(),
                on_create: merge.on_create.clone(),
                on_match: Vec::new(),
                span,
            })],
            return_clause: None,
            span,
        }),
        span,
    }
}
