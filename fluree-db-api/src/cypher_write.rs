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
    BinOp, CypherAst, DeleteClause, Direction, Expr, FuncCall, Literal, MatchClause, MergeClause,
    NodePattern, Pattern, PatternPart, ProjectionItem, Query, ReadClause, RelPattern, ReturnClause,
    SetClause, Statement, Update, Variable, WithClause, WriteClause,
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
    /// `MATCH (a)-[r:T]->(b) DELETE r`: probe whether the matched edge has
    /// parallel siblings (a shared `(s,p,o)` carrying multiple annotation
    /// SIDs), reject if so, otherwise stage the base-edge retraction (the
    /// `f:reifies*` cascade removes the bundle).
    DeleteRel(Update),
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
        // DELETE needs a MATCH. All-relationship-variable targets → DeleteRel
        // (parallel-edge probe). All-node-variable bare targets → DeleteNode
        // (relationship-existence probe). DETACH DELETE (node) and mixed/other
        // shapes lower directly (the lowering handles or rejects them).
        WriteClause::Delete(d) => {
            if u.read_clauses.is_empty() {
                return None;
            }
            let rel_targets = d.targets.iter().filter(|t| is_rel_var(u, &t.name)).count();
            if rel_targets == d.targets.len() && rel_targets > 0 {
                Some(ConditionalCypherWrite::DeleteRel(u.clone()))
            } else if rel_targets == 0 && !d.detach {
                Some(ConditionalCypherWrite::DeleteNode(u.clone()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// True if `name` is bound as a *relationship* variable in any MATCH.
fn is_rel_var(u: &Update, name: &str) -> bool {
    u.read_clauses.iter().any(|clause| {
        let (ReadClause::Match(m) | ReadClause::OptionalMatch(m)) = clause else {
            return false;
        };
        m.pattern
            .parts
            .iter()
            .flat_map(|p| &p.tail)
            .any(|(rel, _)| rel.var.as_ref().is_some_and(|v| v.name == name))
    })
}

/// True if `name` is bound as a node variable by a **mandatory** (non-OPTIONAL)
/// MATCH. Used to reject bare DELETE targets that are only optionally bound
/// (the relationship probe could otherwise bind an unrelated relationship).
pub(crate) fn bound_by_mandatory_match(u: &Update, name: &str) -> bool {
    u.read_clauses.iter().any(|clause| {
        let ReadClause::Match(m) = clause else {
            return false;
        };
        m.pattern.parts.iter().any(|part| {
            let mut nodes = std::iter::once(&part.head).chain(part.tail.iter().map(|(_, n)| n));
            nodes.any(|n| n.var.as_ref().is_some_and(|v| v.name == name))
        })
    })
}

/// Find the (subject-side, object-side) endpoint variables of relationship
/// variable `rel_var` in the MATCH, honoring direction. Returns `None` if the
/// endpoints aren't both named.
pub(crate) fn rel_endpoint_vars(u: &Update, rel_var: &str) -> Option<(Variable, Variable)> {
    for clause in &u.read_clauses {
        let (ReadClause::Match(m) | ReadClause::OptionalMatch(m)) = clause else {
            continue;
        };
        for part in &m.pattern.parts {
            let mut prev = &part.head;
            for (rel, next) in &part.tail {
                if rel.var.as_ref().is_some_and(|v| v.name == rel_var) {
                    let (s, o) = match rel.direction {
                        Direction::Incoming => (next, prev),
                        _ => (prev, next),
                    };
                    return Some((s.var.clone()?, o.var.clone()?));
                }
                prev = next;
            }
        }
    }
    None
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

/// Build a probe that returns at most one row when the matched relationship
/// `rel_var` has a **parallel sibling** — another reified edge sharing the same
/// `(a)-[:T]->(b)` base triple. Appends
/// `WITH <a>, <b>, count(<rel_var>) AS __cyrel_c WHERE __cyrel_c > 1
///  RETURN <a> LIMIT 1` to the original read clauses. Named relationships bind
/// one row per annotation SID, so a `count > 1` per `(a, b)` group means the
/// base edge backs multiple relationship identities — retracting it would
/// disturb the siblings, so `DELETE r` must reject.
pub(crate) fn build_parallel_probe_ast(
    read_clauses: &[ReadClause],
    a: &Variable,
    b: &Variable,
    rel_var: &str,
) -> CypherAst {
    let span = a.span;
    let proj = |v: &Variable| ProjectionItem {
        expr: Expr::Var(v.clone()),
        alias: None,
        span,
    };
    let count_alias = Variable {
        name: "__cyrel_c".to_string(),
        span,
    };
    let count_item = ProjectionItem {
        // DISTINCT: count relationship *identities*, not solution rows. Extra
        // multiplicity in the original MATCH (another matched variable) can
        // repeat one identity across rows, which would falsely trip the guard.
        expr: Expr::Call(FuncCall {
            name: "count".to_string(),
            args: vec![Expr::Var(Variable {
                name: rel_var.to_string(),
                span,
            })],
            distinct: true,
            span,
        }),
        alias: Some(count_alias.clone()),
        span,
    };
    let having = Expr::BinOp(
        BinOp::Gt,
        Box::new(Expr::Var(count_alias)),
        Box::new(Expr::Lit(Literal::Integer(1, span))),
        span,
    );
    let mut clauses: Vec<ReadClause> = read_clauses.to_vec();
    clauses.push(ReadClause::With(WithClause {
        items: vec![proj(a), proj(b), count_item],
        distinct: false,
        where_clause: Some(having),
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span,
    }));

    CypherAst {
        statement: Statement::Query(Query {
            clauses,
            return_clause: ReturnClause {
                items: vec![proj(a)],
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
