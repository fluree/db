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
    CypherAst, Expr, Label, Literal, MatchClause, MergeClause, NodePattern, ReadClause, SetClause,
    Statement, Update, WriteClause,
};
use fluree_db_transact::ir::Txn;

/// A lowered Cypher write: either a ready-to-stage `Txn`, or a conditional
/// write that must probe the writer snapshot before it can be resolved.
pub enum WritePlan {
    /// A ready-to-stage transaction. Boxed because `Txn` is large relative to
    /// the conditional variant.
    Single(Box<Txn>),
    Conditional(ConditionalCypherWrite),
}

/// A write that needs a pre-write probe to choose between branches.
pub enum ConditionalCypherWrite {
    /// Single-node `MERGE` with a non-empty `ON MATCH SET`: probe existence,
    /// then stage the create branch (when absent) or the `ON MATCH SET`
    /// (when present).
    MergeOnMatch(MergeClause),
}

/// Detect a write shape that requires a pre-write probe. Returns `None` for
/// the common single-`Txn` shapes (which lower directly).
pub fn detect_conditional(ast: &CypherAst) -> Option<ConditionalCypherWrite> {
    let Statement::Update(u) = &ast.statement else {
        return None;
    };
    // Conditional shapes are standalone single write clauses.
    if !u.read_clauses.is_empty() || u.write_clauses.len() != 1 {
        return None;
    }
    if let WriteClause::Merge(m) = &u.write_clauses[0] {
        let single_node = m.pattern.parts.len() == 1 && m.pattern.parts[0].tail.is_empty();
        if !m.on_match.is_empty() && single_node {
            return Some(ConditionalCypherWrite::MergeOnMatch(m.clone()));
        }
    }
    None
}

/// Fixed probe variable — uncollidable with user identifiers (leading
/// underscore is fine in Cypher, and this name is generated, not parsed from
/// user input).
const PROBE_VAR: &str = "__cyprobe";

/// Serialize a MERGE node pattern into a probe query that returns at most one
/// row when a matching node exists: `MATCH (<probe>:Labels {props}) RETURN
/// <probe> LIMIT 1`.
pub(crate) fn merge_probe_cypher(node: &NodePattern) -> Result<String, String> {
    let mut s = String::from("MATCH (");
    s.push_str(PROBE_VAR);
    for Label { name, .. } in &node.labels {
        s.push(':');
        s.push_str(name);
    }
    if let Some(props) = &node.props {
        s.push_str(" {");
        for (i, (key, val)) in props.entries.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(key);
            s.push_str(": ");
            s.push_str(&serialize_literal(val)?);
        }
        s.push('}');
    }
    s.push_str(") RETURN ");
    s.push_str(PROBE_VAR);
    s.push_str(" LIMIT 1");
    Ok(s)
}

fn serialize_literal(e: &Expr) -> Result<String, String> {
    match e {
        Expr::Lit(Literal::String(v, _)) => Ok(format!("\"{}\"", escape_string(v))),
        Expr::Lit(Literal::Integer(n, _)) => Ok(n.to_string()),
        Expr::Lit(Literal::Float(f, _)) => Ok(format!("{f}")),
        Expr::Lit(Literal::Bool(b, _)) => Ok(b.to_string()),
        Expr::Lit(Literal::Null(_)) => {
            Err("null in a MERGE identity map is not supported".to_string())
        }
        _ => Err("MERGE identity properties must be literals".to_string()),
    }
}

fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
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
