//! Sequential Cypher writes — multi-clause write statements staged into one
//! commit.
//!
//! A Cypher write statement is a *pipeline*: each clause observes the writes
//! of the clauses before it, and clauses pipe **rows** (variable bindings)
//! downstream. A single `Txn` evaluates its whole WHERE — including every
//! MERGE `NOT EXISTS` guard — against one snapshot horizon, so it cannot
//! express `MERGE (a …) MERGE (b …) MERGE (a)-[:R]->(b)`: the later guards
//! must see the earlier clauses' creations.
//!
//! This driver executes such statements as an ordered sequence of sub-`Txn`s
//! over a [`crate::tx::SequentialStager`] (the same virtual-state machinery
//! behind SPARQL `;`-separated updates): every clause stages against the
//! accumulated virtual state, and everything publishes as ONE commit at
//! `t+1` — all-or-nothing. Row threading between clauses:
//!
//! 1. The read prefix evaluates as an ordinary read query → a row table
//!    (entities as `Binding`s, extracted losslessly from the result).
//! 2. Each write clause becomes one or two sub-`Txn`s *seeded* with the row
//!    table — an `UnresolvedPattern::Values` of
//!    [`UnresolvedValue::PreBound`] cells prepended to the lowered WHERE.
//! 3. A MERGE clause partitions rows by probing the virtual state (match
//!    probe → absent rows → identity-key dedup → create branch → re-bind →
//!    `ON MATCH SET` branch), converging on Cypher's sequential per-row
//!    semantics: the first row of an absent key creates (+`ON CREATE SET`),
//!    every other row matches (+`ON MATCH SET`).
//! 4. CREATE-bound variables thread as reconstructed skolem Sids (the same
//!    `{txn_id}-{solution}-{label}` scheme behind write-statement RETURN);
//!    each sub-`Txn` gets a distinct skolem id.
//! 5. A trailing `RETURN` is answered by one final read query over the row
//!    table against the final virtual state — the full read-side expression
//!    surface, not just created-entity variables.
//!
//! See `docs/design/cypher-sequential-writes.md` for the full design.

use std::sync::Arc;

use fluree_db_cypher::ast::{
    CypherAst, DeleteClause, Direction, Expr, Literal, MatchClause, MergeClause, NodePattern,
    Pattern, PatternPart, ProjectionItem, Query, ReadClause, RelPattern, RemoveClause, RemoveItem,
    ReturnClause, SetClause, SetItem, Statement, Update, Variable, WriteClause,
};
use fluree_db_cypher::span::SourceSpan;
use fluree_db_ledger::IndexConfig;
use fluree_db_ledger::LedgerState;
use fluree_db_query::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use fluree_db_query::parse::{UnresolvedPattern, UnresolvedValue};
use fluree_db_query::{Binding, Materializer};
use fluree_db_transact::lower_cypher_update::{cypher_blank_label, CypherBlank};

use crate::error::ApiError;
use crate::query::QueryResult;
use crate::tx::SequentialStager;
use crate::view::GraphDb;
use crate::{Fluree, GovernanceOptions, Result, StageResult, Tracker};

/// Row-table ceiling: read-prefix rows and every post-MERGE re-bind are
/// checked against it, with a clear error rather than silent truncation.
const MAX_SEQUENTIAL_ROWS: usize = 100_000;

/// Synthetic row-index column threaded through probe / re-bind queries so
/// driver-side bookkeeping (match partition, dedup, ordering) survives
/// engine-side reordering. `#`-prefixed so it can never collide with a user
/// variable (Cypher identifiers cannot start with `#`).
const ROW_IDX_VAR: &str = "#__cy_seq_row";

/// Prefix of the variables the driver names a relationship's endpoints with
/// (see [`rel_delete_edges`]); `#` keeps them clear of user variables.
const REL_END_VAR: &str = "#__cy_seq_rel_";

/// Synthetic column used when a read prefix produces rows but binds no user
/// variables (for example `MATCH (:Person)`). Without a column, that N-row
/// relation is indistinguishable from the one-row unit table to the seeded
/// write path. Values are unique row ordinals, preserving multiplicity.
const ROW_SEED_VAR: &str = "#__cy_seq_seed";

/// A multi-clause Cypher write statement routed to the sequential driver.
pub struct SequentialCypherWrite {
    pub update: Update,
    pub span: SourceSpan,
}

/// Detect a write statement the sequential driver executes. Runs AFTER
/// [`crate::cypher_write::detect_conditional`] — the proven single-clause
/// probe/branch shapes keep their existing paths — and takes only shapes the
/// single-`Txn` lowering rejects today:
///
/// - two or more write clauses containing a MERGE, beyond the
///   `[MERGE, SET…]` family the conditional path owns (MERGE chains,
///   CREATE+MERGE mixes, interleaved SET/REMOVE);
/// - a node MERGE under a plain leading read (`MATCH … MERGE (n {…})`),
///   with or without `ON MATCH SET` / trailing SETs — the driver's per-row
///   partition creates once per distinct absent key and binds every row,
///   which is exactly Cypher's semantics for that shape;
/// - write clauses without a MERGE that can see or undo one another's
///   writes ([`interacting_writes`]).
pub fn detect_sequential(ast: &CypherAst) -> Option<SequentialCypherWrite> {
    let Statement::Update(u) = &ast.statement else {
        return None;
    };
    let has_merge = u
        .write_clauses
        .iter()
        .any(|w| matches!(w, WriteClause::Merge(_)));
    if !has_merge {
        return interacting_writes(u).map(|update| SequentialCypherWrite {
            update,
            span: ast.span,
        });
    }

    let merge_set_shape = matches!(
        u.write_clauses.as_slice(),
        [WriteClause::Merge(_), rest @ ..] if rest.iter().all(|w| matches!(w, WriteClause::Set(_)))
    );
    if !merge_set_shape {
        return Some(SequentialCypherWrite {
            update: u.clone(),
            span: ast.span,
        });
    }

    // `[MERGE, SET…]` the conditional path declined: take the node MERGE
    // under a plain (non-`InlineRows`) read prefix; leave pattern-shape
    // errors to the single-`Txn` lowering's specific messages.
    let WriteClause::Merge(m) = &u.write_clauses[0] else {
        unreachable!("shape checked above");
    };
    let node_merge = m.pattern.parts.len() == 1
        && m.pattern.parts[0].tail.is_empty()
        && m.pattern.parts[0].path_search.is_none()
        && m.pattern.parts[0].path_var.is_none();
    let plain_reads = !u.read_clauses.is_empty()
        && !u
            .read_clauses
            .iter()
            .all(|c| matches!(c, ReadClause::InlineRows { .. }));
    if node_merge && plain_reads {
        return Some(SequentialCypherWrite {
            update: u.clone(),
            span: ast.span,
        });
    }
    None
}

/// `u` prepared for the driver when its write clauses can see or undo one
/// another's writes.
///
/// The single-`Txn` lowering evaluates every clause against the state before
/// the statement and stages them as one operation, so a later clause neither
/// sees an earlier one's write nor replaces it: `SET n.p = 1 SET n.p = 2`
/// asserts both values, `REMOVE n.p SET n.p = <stored value>` removes it. A
/// clause whose lowering reads stored values (an `OPTIONAL` lookup) also adds
/// rows that a `CREATE` in the same statement fires on, once per row. The
/// driver stages each clause against the state the clauses before it leave,
/// and the stager folds each fact to its last write (as for SPARQL's
/// `;`-separated operations), so these statements route there. A SET clause
/// writing one key twice is split at the repeat, since its items apply in
/// order.
///
/// Statements whose clauses can't meet keep the single `Txn`, as do shapes
/// the driver doesn't stage ([`validate_members`]: a `CALL` before the
/// writes, an `OPTIONAL MATCH` before a `CREATE`, a node's bare `DELETE`,
/// `FOREACH`), which the single `Txn`'s lowering refuses in turn.
fn interacting_writes(u: &Update) -> Option<Update> {
    // One clause meets itself only as a SET writing one key twice, and two
    // CREATEs never meet.
    if let [only] = u.write_clauses.as_slice() {
        if !matches!(only, WriteClause::Set(s) if set_repeats_a_key(s)) {
            return None;
        }
    }
    if u.write_clauses
        .iter()
        .all(|c| matches!(c, WriteClause::Create(_)))
    {
        return None;
    }
    let writes: Vec<ClauseWrites<'_>> = u.write_clauses.iter().map(ClauseWrites::of).collect();
    let meet = writes
        .iter()
        .enumerate()
        .any(|(i, a)| writes[i + 1..].iter().any(|b| a.meets(b)))
        || u.write_clauses
            .iter()
            .any(|c| matches!(c, WriteClause::Set(s) if set_repeats_a_key(s)));
    if !meet {
        return None;
    }
    if validate_members(u).is_err() {
        return None;
    }
    Some(Update {
        write_clauses: split_repeated_set_keys(&u.write_clauses),
        ..u.clone()
    })
}

/// The edges a `DELETE` clause's relationship targets retract, as `MATCH`
/// clauses its seeded sub-statement re-matches: the lowering retracts the edge
/// a relationship variable was matched on, in the state the clauses before it
/// leave, and staging refuses an edge that carries another relationship (one
/// an earlier clause created included), as for a `DELETE r` of its own.
///
/// Each re-match is `(s)-[r:T]->(o)`. Its endpoints are the variables that
/// hold `r`'s endpoints at the end of the read clauses ([`RelBinding::ends`],
/// carried through any `WITH` under names of the driver's own), so with `r`
/// and both endpoints seeded it is a point lookup of the edge, and the names
/// the statement's `MATCH` gave the endpoints play no part. When the pattern
/// read the edge in either direction, the re-match takes endpoint variables
/// of its own, which the seeded `r` and its `f:reifies*` bundle pin to the
/// edge `r` is on: slower over many rows, but it never reaches another edge.
fn rel_delete_edges(
    rels: &[RelBinding],
    delete: &DeleteClause,
    span: SourceSpan,
) -> Vec<ReadClause> {
    delete
        .targets
        .iter()
        .filter_map(|target| {
            let rel = rels.iter().find(|r| r.name == target.name)?;
            Some((target, rel))
        })
        .map(|(target, rel)| {
            let (s, o) = rel.ends.clone().unwrap_or_else(|| {
                (
                    format!("{REL_END_VAR}s_{}", target.name),
                    format!("{REL_END_VAR}o_{}", target.name),
                )
            });
            let node = |name: String| NodePattern {
                var: Some(Variable { name, span }),
                labels: Vec::new(),
                props: None,
                span,
            };
            ReadClause::Match(MatchClause {
                pattern: Pattern {
                    parts: vec![PatternPart {
                        path_var: None,
                        path_search: None,
                        head: node(s),
                        tail: vec![(
                            RelPattern {
                                var: Some(target.clone()),
                                direction: Direction::Outgoing,
                                types: rel.pattern.types.clone(),
                                length: rel.pattern.length.clone(),
                                props: None,
                                span,
                            },
                            node(o),
                        )],
                        span,
                    }],
                    span,
                },
                where_clause: None,
                span,
            })
        })
        .collect()
}

/// The endpoint variables of the relationships a `DELETE` in `u` targets that
/// only the read prefix names ([`name_anonymous_ends`]): the read prefix
/// returns them as row-table columns, so the DELETE's re-match is seeded with
/// them.
fn hidden_delete_ends(u: &Update, rels: &[RelBinding]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for w in &u.write_clauses {
        let WriteClause::Delete(d) = w else {
            continue;
        };
        for t in &d.targets {
            let Some((s, o)) = rels
                .iter()
                .find(|r| r.name == t.name)
                .and_then(|r| r.ends.as_ref())
            else {
                continue;
            };
            for end in [s, o] {
                if end.starts_with(REL_END_VAR) && !out.contains(end) {
                    out.push(end.clone());
                }
            }
        }
    }
    out
}

/// `clauses` with every unnamed node beside a named relationship given a
/// name of the driver's own, so a relationship's DELETE can be seeded with its
/// endpoints (see [`rel_delete_edges`]). Naming a node changes nothing the
/// pattern matches.
fn name_anonymous_ends(clauses: &[ReadClause]) -> Vec<ReadClause> {
    let mut clauses = clauses.to_vec();
    for (ci, c) in clauses.iter_mut().enumerate() {
        let (ReadClause::Match(m) | ReadClause::OptionalMatch(m)) = c else {
            continue;
        };
        for (pi, part) in m.pattern.parts.iter_mut().enumerate() {
            let named_rel: Vec<bool> = part.tail.iter().map(|(rel, _)| rel.var.is_some()).collect();
            let span = part.head.span;
            let name = |node: &mut NodePattern, ni: usize| {
                if node.var.is_none() {
                    node.var = Some(Variable {
                        name: format!("{REL_END_VAR}n{ci}_{pi}_{ni}"),
                        span,
                    });
                }
            };
            if named_rel.first() == Some(&true) {
                name(&mut part.head, 0);
            }
            for (i, (_, node)) in part.tail.iter_mut().enumerate() {
                if named_rel[i] || named_rel.get(i + 1) == Some(&true) {
                    name(node, i + 1);
                }
            }
        }
    }
    clauses
}

/// Whether a SET's items write one property key twice.
fn set_repeats_a_key(s: &SetClause) -> bool {
    let mut seen = ClauseWrites::default();
    for item in &s.items {
        let mut writes = ClauseWrites::default();
        writes.add_set_items(std::slice::from_ref(item));
        if seen.shares_keys(&writes) {
            return true;
        }
        seen.add_set_items(std::slice::from_ref(item));
    }
    false
}

/// A relationship variable in scope after the read clauses.
struct RelBinding {
    name: String,
    /// The pattern that bound it.
    pattern: RelPattern,
    /// The variables holding its endpoints, subject first: `None` when one is
    /// unnamed, when the pattern reads the edge in either direction, or (unless
    /// the scope carries them, see [`relationship_scope`]) once a `WITH` drops
    /// or rebinds one.
    ends: Option<(String, String)>,
}

/// The relationship variables in scope after the read clauses. A `WITH`
/// carries a relationship (and its endpoints) under the names it projects them
/// as and drops what it doesn't project, so a name the `MATCH` gave a
/// relationship can name a node afterwards, and the reverse.
///
/// With `carry_ends`, the clauses come back prepared for the read prefix: an
/// unnamed endpoint beside a named relationship gets a name of its own
/// ([`name_anonymous_ends`]), and a `WITH` that keeps a relationship but drops
/// or rebinds an endpoint also projects that endpoint under a name of the
/// driver's own. Neither changes what the clauses match or bind for the
/// statement (an endpoint is a function of its relationship, so carrying it
/// changes no grouping or `DISTINCT`), and every relationship keeps the
/// variables holding its endpoints, which seed its DELETE's re-match.
fn relationship_scope(
    clauses: &[ReadClause],
    carry_ends: bool,
) -> (Vec<ReadClause>, Vec<RelBinding>) {
    // A name that is bound anew holds no relationship or endpoint it held.
    fn forget(vars: &mut Vec<RelBinding>, name: &str) {
        vars.retain(|r| r.name != name);
        for r in vars.iter_mut() {
            if r.ends.as_ref().is_some_and(|(s, o)| s == name || o == name) {
                r.ends = None;
            }
        }
    }
    let mut clauses = if carry_ends {
        name_anonymous_ends(clauses)
    } else {
        clauses.to_vec()
    };
    let mut vars: Vec<RelBinding> = Vec::new();
    for (ci, c) in clauses.iter_mut().enumerate() {
        match c {
            ReadClause::Match(m) | ReadClause::OptionalMatch(m) => {
                for part in &m.pattern.parts {
                    let mut prev = &part.head;
                    for (rel, next) in &part.tail {
                        if let Some(v) = &rel.var {
                            let ends = match rel.direction {
                                Direction::Outgoing => Some((prev, next)),
                                Direction::Incoming => Some((next, prev)),
                                Direction::Either => None,
                            }
                            .and_then(|(s, o)| Some((s.var.clone()?.name, o.var.clone()?.name)));
                            vars.retain(|r| r.name != v.name);
                            vars.push(RelBinding {
                                name: v.name.clone(),
                                pattern: rel.clone(),
                                ends,
                            });
                        }
                        prev = next;
                    }
                }
            }
            ReadClause::With(w) => {
                let (next, carried) = with_scope(w, std::mem::take(&mut vars), carry_ends, ci);
                vars = next;
                w.items.extend(carried);
            }
            ReadClause::Unwind(u) => forget(&mut vars, &u.alias.name),
            ReadClause::InlineRows { vars: cols, .. } => {
                for v in cols {
                    forget(&mut vars, &v.name);
                }
            }
            ReadClause::CallSubquery(_) => {}
        }
    }
    (clauses, vars)
}

/// The relationship variables in scope after `w` (clause `ci`), and, with
/// `carry_ends`, the items `w` must also project to keep each surviving
/// relationship's endpoints (see [`relationship_scope`]).
fn with_scope(
    w: &fluree_db_cypher::ast::WithClause,
    vars: Vec<RelBinding>,
    carry_ends: bool,
    ci: usize,
) -> (Vec<RelBinding>, Vec<ProjectionItem>) {
    // Where each name this WITH projects a variable under comes from. `WITH *`
    // carries every user variable as it is.
    let star = w
        .items
        .iter()
        .any(|i| i.alias.is_none() && matches!(&i.expr, Expr::Var(v) if v.name == "*"));
    let renames: Vec<(&str, &str)> = w
        .items
        .iter()
        .filter_map(|i| match (&i.alias, &i.expr) {
            (None, Expr::Var(v)) if v.name == "*" => None,
            (alias, Expr::Var(v)) => {
                Some((v.name.as_str(), alias.as_ref().unwrap_or(v).name.as_str()))
            }
            _ => None,
        })
        .collect();
    let assigned = |name: &str| {
        w.items
            .iter()
            .any(|i| i.alias.as_ref().is_some_and(|a| a.name == name))
    };
    let new_name = |old: &str| -> Option<String> {
        if let Some((_, new)) = renames.iter().find(|(from, _)| *from == old) {
            return Some((*new).to_string());
        }
        (star && !old.starts_with('#') && !assigned(old)).then(|| old.to_string())
    };
    let mut carried: Vec<ProjectionItem> = Vec::new();
    let mut carry = |old: &str| -> String {
        if let Some(i) = carried
            .iter()
            .find(|i| matches!(&i.expr, Expr::Var(v) if v.name == old))
        {
            return i
                .alias
                .as_ref()
                .expect("carried items are aliased")
                .name
                .clone();
        }
        let alias = Variable {
            name: format!("{REL_END_VAR}w{ci}_{}", carried.len()),
            span: w.span,
        };
        carried.push(ProjectionItem {
            expr: Expr::Var(Variable {
                name: old.to_string(),
                span: w.span,
            }),
            alias: Some(alias.clone()),
            span: w.span,
        });
        alias.name
    };
    let next = vars
        .into_iter()
        .filter_map(|r| {
            let name = new_name(&r.name)?;
            let ends = r.ends.and_then(|(s, o)| {
                let mut keep =
                    |end: String| new_name(&end).or_else(|| carry_ends.then(|| carry(&end)));
                Some((keep(s)?, keep(o)?))
            });
            Some(RelBinding {
                name,
                pattern: r.pattern,
                ends,
            })
        })
        .collect();
    (next, carried)
}

/// `clauses` with each SET that writes one property key twice split into
/// consecutive SETs at every repeat.
fn split_repeated_set_keys(clauses: &[WriteClause]) -> Vec<WriteClause> {
    let mut out = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let WriteClause::Set(s) = clause else {
            out.push(clause.clone());
            continue;
        };
        let mut run: Vec<SetItem> = Vec::new();
        let mut run_writes = ClauseWrites::default();
        for item in &s.items {
            let mut item_writes = ClauseWrites::default();
            item_writes.add_set_items(std::slice::from_ref(item));
            if !run.is_empty() && run_writes.shares_keys(&item_writes) {
                out.push(WriteClause::Set(SetClause {
                    items: std::mem::take(&mut run),
                    span: s.span,
                }));
                run_writes = ClauseWrites::default();
            }
            run.push(item.clone());
            run_writes.add_set_items(std::slice::from_ref(item));
        }
        out.push(WriteClause::Set(SetClause {
            items: run,
            span: s.span,
        }));
    }
    out
}

/// What one write clause writes, and whether its lowering reads stored values
/// (an `OPTIONAL` lookup, which adds rows to the statement's solutions).
#[derive(Default)]
struct ClauseWrites<'a> {
    keys: Vec<&'a str>,
    /// `SET n = {…}`: every property key of `n`.
    all_keys: bool,
    labels: Vec<&'a str>,
    reads: bool,
    creates: bool,
    /// A `DELETE`: a node's `DETACH DELETE` takes every fact of the node, and
    /// a relationship's takes its edge and every property of the
    /// relationship, so it meets every other clause.
    deletes: bool,
}

impl<'a> ClauseWrites<'a> {
    fn of(clause: &'a WriteClause) -> Self {
        let mut w = Self::default();
        match clause {
            WriteClause::Create(c) => {
                w.creates = true;
                for part in &c.pattern.parts {
                    for node in std::iter::once(&part.head).chain(part.tail.iter().map(|(_, n)| n))
                    {
                        w.labels.extend(node.labels.iter().map(|l| l.name.as_str()));
                        if let Some(props) = &node.props {
                            w.keys.extend(props.entries.iter().map(|(k, _)| k.as_str()));
                        }
                    }
                }
            }
            WriteClause::Set(s) => w.add_set_items(&s.items),
            WriteClause::Remove(r) => {
                for item in &r.items {
                    match item {
                        RemoveItem::Property { property, .. } => {
                            w.keys.push(property);
                            w.reads = true;
                        }
                        RemoveItem::Labels { labels, .. } => {
                            w.labels.extend(labels.iter().map(String::as_str));
                        }
                    }
                }
            }
            WriteClause::Delete(_) => {
                w.deletes = true;
                w.reads = true;
            }
            WriteClause::Merge(_) | WriteClause::Foreach(_) => {}
        }
        w
    }

    fn add_set_items(&mut self, items: &'a [SetItem]) {
        for item in items {
            match item {
                SetItem::Property { property, .. } => {
                    self.keys.push(property);
                    self.reads = true;
                }
                SetItem::MapMerge { map, .. } => {
                    self.keys
                        .extend(map.entries.iter().map(|(k, _)| k.as_str()));
                    self.reads = true;
                }
                SetItem::MapReplace { map, .. } => {
                    self.keys
                        .extend(map.entries.iter().map(|(k, _)| k.as_str()));
                    self.all_keys = true;
                    self.reads = true;
                }
                SetItem::Labels { labels, .. } => {
                    self.labels.extend(labels.iter().map(String::as_str));
                }
            }
        }
    }

    /// Whether this and `other` write one property key, on any variables.
    fn shares_keys(&self, other: &Self) -> bool {
        (self.all_keys && (other.all_keys || !other.keys.is_empty()))
            || (other.all_keys && !self.keys.is_empty())
            || self.keys.iter().any(|k| other.keys.contains(k))
    }

    /// Whether this clause and `other` can see or undo each other's writes,
    /// on any variables (two can bind one node). A CREATE writes only the
    /// entities it creates, so two never meet.
    fn meets(&self, other: &Self) -> bool {
        if self.creates && other.creates {
            return false;
        }
        self.deletes
            || other.deletes
            || (self.creates && other.reads)
            || (other.creates && self.reads)
            || self.shares_keys(other)
            || self.labels.iter().any(|l| other.labels.contains(l))
    }
}

/// Classify Cypher source for transports: whether the (param-substituted)
/// statement routes to the sequential driver, and whether it carries a
/// trailing `RETURN`. Parse/substitution errors report `(false, false)` —
/// the submission path surfaces them with full diagnostics.
pub fn classify_sequential_source(
    cypher: &str,
    params: Option<&fluree_db_cypher::ParamMap>,
) -> (bool, bool) {
    match crate::query::helpers::substituted_cypher_ast(cypher, params) {
        Ok(ast) => match detect_sequential(&ast) {
            Some(sq) => (true, sq.update.return_clause.is_some()),
            None => (false, false),
        },
        Err(_) => (false, false),
    }
}

/// Outcome of staging a sequential write: the merged one-commit stage result,
/// and the trailing `RETURN` answered, when the statement has one.
pub(crate) struct SequentialOutcome {
    pub stage_result: StageResult,
    pub returned: Option<AnsweredReturn>,
}

/// A trailing `RETURN`'s result, with the state it was read from: the final
/// virtual state (base + every clause). Call sites format the result against
/// it, or against the equivalent committed state.
///
/// The final state is only reachable here. The driver builds the state after
/// the last write clause only for a statement with a `RETURN`
/// ([`StateAfter`]), so no caller can read a state it didn't build.
pub(crate) struct AnsweredReturn {
    pub result: QueryResult,
    pub final_state: LedgerState,
}

/// Whether anything reads the virtual state after a clause is staged.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StateAfter {
    /// A later clause, a MERGE probe or the trailing `RETURN` reads it.
    Read,
    /// Nothing does: the last write clause of a statement with no `RETURN`.
    /// Its stage folds the clause's facts into the merged result, which is
    /// all the commit reads, and builds no state after it.
    Unread,
}

/// Routing stamp site for the state after a statement's last write clause:
/// `proceed` when it was left unbuilt, a fallback when it was built (a
/// `RETURN` or a MERGE reads it, or the last clause staged nothing).
pub const UNREAD_FINAL_STATE_SITE: &str = "cypher_unread_final_state";

/// Input for staging a sequential Cypher write through the transaction
/// builder ([`crate::RefTransactBuilder::cypher_sequential`]): the detected
/// plan plus the request's governance (probe policy wrapping) and optional
/// caller-pinned skolem id.
pub struct CypherSeqInput {
    pub plan: SequentialCypherWrite,
    pub governance: crate::GovernanceOptions,
    pub skolem_txn_id: Option<String>,
}

/// The evolving row table piped between clauses.
#[derive(Clone)]
struct RowTable {
    /// User-visible column names (Cypher variable names, no prefix).
    cols: Vec<String>,
    rows: Vec<Vec<Binding>>,
}

impl RowTable {
    /// The unit table: no columns, ONE empty row — the semantics of a write
    /// with no read prefix (each clause fires exactly once).
    fn unit() -> Self {
        Self {
            cols: Vec::new(),
            rows: vec![Vec::new()],
        }
    }

    fn col_idx(&self, name: &str) -> Option<usize> {
        self.cols.iter().position(|c| c == name)
    }

    fn is_seeded(&self) -> bool {
        !self.cols.is_empty()
    }

    /// The rows that bind every column in `vars`, when some row doesn't;
    /// `None` when every row does.
    fn bound_on(&self, vars: &[&str]) -> Option<Self> {
        let cols: Vec<usize> = vars.iter().filter_map(|v| self.col_idx(v)).collect();
        let binds = |row: &Vec<Binding>| cols.iter().all(|&c| !row[c].is_unbound_or_poisoned());
        if self.rows.iter().all(binds) {
            return None;
        }
        Some(Self {
            cols: self.cols.clone(),
            rows: self.rows.iter().filter(|r| binds(r)).cloned().collect(),
        })
    }
}

/// The variables a SET, REMOVE or DELETE clause writes to.
fn clause_targets(clause: &WriteClause) -> Vec<&str> {
    match clause {
        WriteClause::Set(s) => s.items.iter().map(set_item_target).collect(),
        WriteClause::Remove(r) => r.items.iter().map(remove_item_target).collect(),
        WriteClause::Delete(d) => d.targets.iter().map(|t| t.name.as_str()).collect(),
        WriteClause::Create(_) | WriteClause::Merge(_) | WriteClause::Foreach(_) => Vec::new(),
    }
}

/// A SET, REMOVE or DELETE clause as the parts it stages in, each with the
/// rows that seed it (`None`: every row in `table`), leaving out a part no
/// row binds.
///
/// When every row binds every target, the clause stages whole. Otherwise each
/// target's writes stage on their own, seeded with the rows that bind that
/// target, in the clause's order. Items on different targets are independent
/// (a SET that writes one key twice was split at the repeat, and the rest
/// write distinct keys or labels), so staging them apart writes what staging
/// them together would. A DELETE's relationship targets go last: a node's
/// DETACH DELETE beside them takes every relationship on the node, which a
/// relationship's DELETE staged first would refuse if the edge carries
/// another.
fn parts_by_bound_target(
    clause: &WriteClause,
    table: &RowTable,
    rel_vars: &[RelBinding],
) -> Vec<(WriteClause, Option<RowTable>)> {
    if table.rows.is_empty() {
        return Vec::new();
    }
    let targets = clause_targets(clause);
    let Some(bound) = table.bound_on(&targets) else {
        return vec![(clause.clone(), None)];
    };
    let mut distinct: Vec<&str> = Vec::new();
    for t in targets {
        if !distinct.contains(&t) {
            distinct.push(t);
        }
    }
    if distinct.len() == 1 {
        return if bound.rows.is_empty() {
            Vec::new()
        } else {
            vec![(clause.clone(), Some(bound))]
        };
    }
    if matches!(clause, WriteClause::Delete(_)) {
        let is_rel = |t: &&str| rel_vars.iter().any(|r| r.name == *t);
        distinct.sort_by_key(|t| is_rel(t));
    }
    distinct
        .into_iter()
        .filter_map(|target| {
            let rows = table.bound_on(&[target]);
            if rows.as_ref().is_some_and(|r| r.rows.is_empty()) {
                return None;
            }
            Some((clause_on(clause, target), rows))
        })
        .collect()
}

/// The part of a SET, REMOVE or DELETE clause that writes to `target`.
fn clause_on(clause: &WriteClause, target: &str) -> WriteClause {
    match clause {
        WriteClause::Set(s) => WriteClause::Set(SetClause {
            items: s
                .items
                .iter()
                .filter(|item| set_item_target(item) == target)
                .cloned()
                .collect(),
            span: s.span,
        }),
        WriteClause::Remove(r) => WriteClause::Remove(RemoveClause {
            items: r
                .items
                .iter()
                .filter(|item| remove_item_target(item) == target)
                .cloned()
                .collect(),
            span: r.span,
        }),
        WriteClause::Delete(d) => WriteClause::Delete(DeleteClause {
            targets: d
                .targets
                .iter()
                .filter(|t| t.name == target)
                .cloned()
                .collect(),
            ..d.clone()
        }),
        WriteClause::Create(_) | WriteClause::Merge(_) | WriteClause::Foreach(_) => clause.clone(),
    }
}

fn set_item_target(item: &SetItem) -> &str {
    match item {
        SetItem::Property { target, .. }
        | SetItem::MapMerge { target, .. }
        | SetItem::MapReplace { target, .. }
        | SetItem::Labels { target, .. } => &target.name,
    }
}

fn remove_item_target(item: &RemoveItem) -> &str {
    match item {
        RemoveItem::Property { target, .. } | RemoveItem::Labels { target, .. } => &target.name,
    }
}

fn cypher_err(msg: impl Into<String>) -> ApiError {
    ApiError::cypher(msg.into(), Vec::new())
}

impl Fluree {
    /// Stage a multi-clause Cypher write against `ledger` (the writer's
    /// pre-commit state) into one merged [`StageResult`], answering a
    /// trailing `RETURN` from the final row table.
    ///
    /// POLICY CONTRACT: `governance` gates the driver's internal probe /
    /// re-bind queries — when it carries policy inputs every probe view is
    /// policy-wrapped (a restricted writer partitions match-vs-create from
    /// policy-visible data only), mirroring the conditional-MERGE contract.
    /// `policy` is the staging-side context enforced per sub-`Txn`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_cypher_sequential(
        &self,
        ledger: LedgerState,
        plan: &SequentialCypherWrite,
        ledger_id: &str,
        governance: Option<&GovernanceOptions>,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
        skolem_txn_id: Option<String>,
        request: Option<&crate::tx::RequestGovernance>,
    ) -> Result<SequentialOutcome> {
        let update = &plan.update;
        let span = plan.span;
        validate_members(update)?;

        // Resolved once; every probe view gets the ledger's default context so
        // bare identifiers resolve exactly as the writes do.
        let default_context = match self.get_default_context(ledger_id).await {
            Ok(ctx) => ctx,
            Err(ApiError::NotFound(_)) => None,
            Err(e) => return Err(e),
        };

        // Base skolem id: each sub-`Txn` derives a distinct id from it so
        // created-entity Sids never collide across clauses and remain
        // reconstructible for row threading.
        let skolem_base = skolem_txn_id.unwrap_or_else(crate::cypher_write::fresh_skolem_txn_id);

        // Inside an enclosing request (a transaction), its governance and its
        // config guard apply; otherwise this statement is the request.
        let mut stager = match request {
            Some(request) => SequentialStager::within(ledger, request),
            None => SequentialStager::new(ledger),
        };

        // ---- 1. Read prefix → initial row table --------------------------
        let (read_clauses, rel_vars) = relationship_scope(&update.read_clauses, true);
        let mut table = if update.read_clauses.is_empty() {
            RowTable::unit()
        } else {
            let mut cols = visible_vars(&update.read_clauses)?;
            cols.extend(hidden_delete_ends(update, &rel_vars));
            let ast = read_prefix_query(&read_clauses, &cols, span);
            let view = self
                .seq_probe_view(stager.state(), governance, default_context.as_ref())
                .await?;
            let result = self.query_cypher_ast_tracked(&view, &ast, tracker).await?;
            let mut rows = if cols.is_empty() {
                extract_rows(result, &[ROW_SEED_VAR.to_string()])?
            } else {
                extract_rows(result, &cols)?
            };
            if cols.is_empty() {
                cols.push(ROW_SEED_VAR.to_string());
                for (i, row) in rows.iter_mut().enumerate() {
                    row[0] = row_index_binding(i as u64);
                }
            }
            check_row_cap(rows.len())?;
            RowTable { cols, rows }
        };

        // ---- 2. Per-clause staging loop ----------------------------------
        let last_clause = update.write_clauses.len().saturating_sub(1);
        for (ci, clause) in update.write_clauses.iter().enumerate() {
            let clause_skolem = format!("{skolem_base}-s{ci}");
            // The state after the last write clause is read only by a
            // trailing RETURN, or by the clause itself when it's a MERGE.
            let after = if ci == last_clause
                && update.return_clause.is_none()
                && !matches!(clause, WriteClause::Merge(_))
            {
                StateAfter::Unread
            } else {
                StateAfter::Read
            };
            // Hoist computed value expressions (`{name: p.team}`,
            // `SET n.x = p.age + 1`) into engine-evaluated row columns —
            // the write lowering takes literals and bound variables only.
            let clause = self
                .hoist_clause_value_exprs(
                    &mut table,
                    clause,
                    governance,
                    default_context.as_ref(),
                    stager.state(),
                    span,
                    tracker,
                )
                .await?;
            let clause = &clause;
            match clause {
                WriteClause::Create(c) => {
                    self.seq_stage_create(
                        &mut stager,
                        &mut table,
                        c,
                        clause_skolem,
                        ledger_id,
                        index_config,
                        policy,
                        tracker,
                        span,
                        after,
                    )
                    .await?;
                }
                WriteClause::Merge(m) => {
                    self.seq_stage_merge(
                        &mut stager,
                        &mut table,
                        m,
                        clause_skolem,
                        ledger_id,
                        governance,
                        default_context.as_ref(),
                        index_config,
                        policy,
                        tracker,
                        span,
                    )
                    .await?;
                }
                WriteClause::Set(_) | WriteClause::Remove(_) | WriteClause::Delete(_) => {
                    // A row that leaves one of the clause's targets unbound
                    // (an OPTIONAL MATCH that found nothing) skips the
                    // clause's writes to that target and keeps the rest. The
                    // seeded `VALUES` would leave the target free for the
                    // clause's own lookup of stored values to bind, so each
                    // part is seeded only with the rows that bind its target.
                    let parts = parts_by_bound_target(clause, &table, &rel_vars);
                    let last_part = parts.len().saturating_sub(1);
                    for (pi, (part, rows)) in parts.into_iter().enumerate() {
                        let edges = match &part {
                            WriteClause::Delete(d) => rel_delete_edges(&rel_vars, d, span),
                            _ => Vec::new(),
                        };
                        let skolem = if last_part == 0 {
                            clause_skolem.clone()
                        } else {
                            format!("{clause_skolem}-t{pi}")
                        };
                        self.seq_stage_seeded(
                            &mut stager,
                            rows.as_ref().unwrap_or(&table),
                            edges,
                            vec![part],
                            Some(skolem),
                            ledger_id,
                            index_config,
                            policy,
                            tracker,
                            span,
                            if pi == last_part {
                                after
                            } else {
                                StateAfter::Read
                            },
                        )
                        .await?;
                    }
                }
                WriteClause::Foreach(_) => {
                    unreachable!("rejected by validate_members");
                }
            }
        }

        // Stamped from what the stager holds rather than from the decision,
        // so `proceed` means the state was in fact left unbuilt.
        stamp_fast_path(
            UNREAD_FINAL_STATE_SITE,
            if stager.has_state() {
                FastPathOutcome::Fallback(FastPathFallback::GateDeclined)
            } else {
                FastPathOutcome::Proceed
            },
        );

        // ---- 3. Trailing RETURN off the final row table ------------------
        let return_result = match &update.return_clause {
            Some(rc) if !table.rows.is_empty() || table.cols.is_empty() => {
                let mut clauses: Vec<ReadClause> = Vec::new();
                let seeded = table.is_seeded();
                if seeded {
                    clauses.push(inline_rows_placeholder(&table.cols, span));
                }
                let ast = CypherAst {
                    statement: Statement::Query(Query {
                        clauses,
                        return_clause: rc.clone(),
                        union_tail: None,
                        span,
                    }),
                    span,
                };
                let view = self
                    .seq_probe_view(stager.state(), governance, default_context.as_ref())
                    .await?;
                let result = if seeded {
                    self.query_cypher_ast_seeded(
                        &view,
                        &ast,
                        &table.cols,
                        table.rows.clone(),
                        tracker,
                    )
                    .await?
                } else {
                    self.query_cypher_ast_tracked(&view, &ast, tracker).await?
                };
                Some(result)
            }
            // Zero rows: the RETURN yields zero rows. Represent as a seeded
            // query over the empty table so the projection schema (columns)
            // still comes back correctly formatted.
            Some(rc) => {
                let ast = CypherAst {
                    statement: Statement::Query(Query {
                        clauses: vec![inline_rows_placeholder(&table.cols, span)],
                        return_clause: rc.clone(),
                        union_tail: None,
                        span,
                    }),
                    span,
                };
                let view = self
                    .seq_probe_view(stager.state(), governance, default_context.as_ref())
                    .await?;
                let result = self
                    .query_cypher_ast_seeded(&view, &ast, &table.cols, Vec::new(), tracker)
                    .await?;
                Some(result)
            }
            None => None,
        };

        let returned = return_result.map(|result| AnsweredReturn {
            result,
            final_state: stager.state().clone(),
        });
        Ok(SequentialOutcome {
            stage_result: stager.finish().await?,
            returned,
        })
    }

    /// Commit a sequential outcome (autocommit path): zero staged flakes is
    /// Cypher's zero-updates success (no commit); otherwise the merged view
    /// commits as one transaction. The trailing RETURN formats as the
    /// Cypher-JSON envelope against the committed state.
    pub(crate) async fn commit_sequential_outcome(
        &self,
        outcome: SequentialOutcome,
        index_config: &IndexConfig,
    ) -> Result<(crate::TransactResult, Option<serde_json::Value>)> {
        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
            sync_graph: _,
            scope: _,
        } = outcome.stage_result;
        // The rows format against the committed state, not the virtual one.
        let return_result = outcome.returned.map(|answered| answered.result);

        let commit_opts = fluree_db_transact::CommitOpts::default()
            .with_txn_meta(txn_meta)
            .with_graph_iris(graph_delta.into_values());

        let (receipt, ledger) = if !view.has_staged() {
            let (base, flakes) = view.into_parts();
            debug_assert!(
                flakes.is_empty(),
                "no-op sequential write with staged flakes"
            );
            (fluree_db_transact::CommitReceipt::no_op(base.t()), base)
        } else {
            self.commit_staged(view, ns_registry, index_config, commit_opts)
                .await?
        };
        let result = self
            .finalize_owned_commit(receipt, ledger, index_config)
            .await;

        let rows = match return_result {
            Some(qr) => {
                let view = GraphDb::from_ledger_state(&result.ledger);
                Some(
                    qr.to_cypher_json_async(view.as_graph_db_ref())
                        .await
                        .map_err(|e| {
                            ApiError::internal(format!("sequential write RETURN formatting: {e}"))
                        })?,
                )
            }
            None => None,
        };
        Ok((result, rows))
    }

    /// A queryable, default-context-attached (and policy-wrapped when the
    /// governance carries policy inputs) view of the current virtual state's
    /// default graph.
    async fn seq_probe_view(
        &self,
        state: &LedgerState,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
    ) -> Result<GraphDb> {
        let view = GraphDb::from_ledger_state(state).with_default_context(default_context.cloned());
        let view = match governance {
            Some(g) if g.has_any_policy_inputs() => self.wrap_policy(view, g).await?,
            _ => view,
        };
        // A write statement's reads see the default graph alone, whatever the
        // ledger's union default graph: its writes stage against that graph,
        // as a transaction's WHERE reads it.
        Ok(view.with_union_default_graph(false))
    }

    /// A [`seq_probe_view`](Self::seq_probe_view) that reads asserted data
    /// only, for the MERGE guard and its re-bind.
    ///
    /// Those two decide what gets written: the guard picks match-vs-create per
    /// row, and the re-bind then binds the pattern's variables from the rows
    /// it settled. Reasoning is a read-time view over the ledger rather than
    /// something the ledger holds, so an entailed triple must not answer
    /// "does this row already match?" -- staging does not reason, so a row
    /// matched by entailment alone is neither created nor updatable.
    ///
    /// `seq_probe_view` itself stays reasoning-enabled: it also backs the
    /// statement's own read clauses and its trailing RETURN, which are
    /// ordinary reads and should see entailments like any other query.
    async fn seq_decision_view(
        &self,
        state: &LedgerState,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
    ) -> Result<GraphDb> {
        Ok(self
            .seq_probe_view(state, governance, default_context)
            .await?
            .with_reasoning(fluree_db_query::ir::reasoning::ReasoningModes::none()))
    }

    /// Stage a CREATE clause seeded with the row table, then extend the table
    /// with the created entities' skolem Sids (row `i` ↔ WHERE solution `i`;
    /// the seeded WHERE is a single `VALUES` block, which preserves row
    /// order — pinned by test).
    #[allow(clippy::too_many_arguments)]
    async fn seq_stage_create(
        &self,
        stager: &mut SequentialStager,
        table: &mut RowTable,
        create: &fluree_db_cypher::ast::CreateClause,
        clause_skolem: String,
        ledger_id: &str,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
        span: SourceSpan,
        after: StateAfter,
    ) -> Result<()> {
        if table.rows.is_empty() {
            return Ok(());
        }

        // Variables this CREATE introduces, with the labels the lowering
        // mints for them (`cypher_blank_label`).
        let mut new_cols: Vec<(String, String)> = Vec::new(); // (var, skolem label)
        for part in &create.pattern.parts {
            for node in std::iter::once(&part.head).chain(part.tail.iter().map(|(_, n)| n)) {
                if let Some(v) = &node.var {
                    if table.col_idx(&v.name).is_none()
                        && !new_cols.iter().any(|(n, _)| n == &v.name)
                    {
                        new_cols.push((
                            v.name.clone(),
                            cypher_blank_label(CypherBlank::NodeVar(&v.name)),
                        ));
                    }
                }
            }
            for (rel, _) in &part.tail {
                if let Some(v) = &rel.var {
                    if table.col_idx(&v.name).is_none()
                        && !new_cols.iter().any(|(n, _)| n == &v.name)
                    {
                        new_cols.push((
                            v.name.clone(),
                            cypher_blank_label(CypherBlank::RelVar(&v.name)),
                        ));
                    }
                }
            }
        }

        self.seq_stage_seeded(
            stager,
            table,
            Vec::new(),
            vec![WriteClause::Create(create.clone())],
            Some(clause_skolem.clone()),
            ledger_id,
            index_config,
            policy,
            tracker,
            span,
            after,
        )
        .await?;

        for (var, label) in new_cols {
            table.cols.push(var);
            for (i, row) in table.rows.iter_mut().enumerate() {
                row.push(Binding::sid(crate::cypher_write::skolem_sid(
                    &clause_skolem,
                    i as u64,
                    &label,
                )));
            }
        }
        Ok(())
    }

    /// Stage a MERGE clause: probe → partition → create branch → re-bind →
    /// `ON MATCH SET` branch. See the module doc for the semantics argument.
    #[allow(clippy::too_many_arguments)]
    async fn seq_stage_merge(
        &self,
        stager: &mut SequentialStager,
        table: &mut RowTable,
        merge: &MergeClause,
        clause_skolem: String,
        ledger_id: &str,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
        span: SourceSpan,
    ) -> Result<()> {
        validate_merge_pattern(merge)?;
        if table.rows.is_empty() {
            return Ok(());
        }
        let part = &merge.pattern.parts[0];

        // ---- Probe: which rows already match the pattern? ----------------
        let matched_rows: std::collections::HashSet<usize> = {
            let view = self
                .seq_decision_view(stager.state(), governance, default_context)
                .await?;
            if table.is_seeded() {
                let ast = seeded_pattern_query(
                    &table.cols,
                    &merge.pattern,
                    vec![row_idx_projection(span)],
                    span,
                );
                let result = self
                    .query_cypher_ast_seeded(
                        &view,
                        &ast,
                        &cols_with_index(&table.cols),
                        rows_with_index(&table.rows, span),
                        tracker,
                    )
                    .await?;
                extract_row_indices(result)?
            } else {
                // Unit table: one empty row; matched iff the pattern exists.
                let probe = crate::cypher_write::build_merge_path_probe_ast(merge);
                if self
                    .query_cypher_ast_tracked(&view, &probe, tracker)
                    .await?
                    .row_count()
                    > 0
                {
                    std::iter::once(0).collect()
                } else {
                    std::collections::HashSet::new()
                }
            }
        };

        // ---- Partition absent rows; dedup by identity key ----------------
        let absent: Vec<usize> = (0..table.rows.len())
            .filter(|i| !matched_rows.contains(i))
            .collect();
        let creators: Vec<usize> = if absent.is_empty() {
            Vec::new()
        } else {
            let keys = self
                .merge_identity_keys(
                    table,
                    &absent,
                    merge,
                    stager.state(),
                    governance,
                    default_context,
                    span,
                    tracker,
                )
                .await?;
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            absent
                .iter()
                .zip(keys)
                .filter_map(|(&row, key)| seen.insert(key).then_some(row))
                .collect()
        };

        // ---- Create branch (first row per distinct absent key) -----------
        // Values that need engine evaluation against variables introduced by
        // this MERGE cannot be hoisted until after re-bind. Keep simple
        // literals / already-bound row columns in the create Txn; defer only
        // computed ON CREATE expressions to a post-create seeded SET.
        let deferred_on_create = set_items_need_hoist(&merge.on_create, &table.cols);
        if !creators.is_empty() {
            let mut create_merge = merge.clone();
            create_merge.on_match.clear();
            if deferred_on_create {
                create_merge.on_create.clear();
            }
            let create_ast_clause = WriteClause::Merge(create_merge);
            if table.is_seeded() {
                let sub = RowTable {
                    cols: table.cols.clone(),
                    rows: creators.iter().map(|&i| table.rows[i].clone()).collect(),
                };
                self.seq_stage_seeded(
                    stager,
                    &sub,
                    Vec::new(),
                    vec![create_ast_clause],
                    Some(clause_skolem.clone()),
                    ledger_id,
                    index_config,
                    policy,
                    tracker,
                    span,
                    StateAfter::Read,
                )
                .await?;
            } else {
                // Unit mode: the plain standalone-MERGE create lowering.
                let ast = update_ast(vec![create_ast_clause], span);
                let mut txn = self
                    .lower_cypher_ast_to_txn(&ast, ledger_id, &stager.state().snapshot)
                    .await?;
                txn.opts.skolem_txn_id = Some(clause_skolem.clone());
                stager
                    .stage(self, txn, index_config, policy, tracker, true)
                    .await?;
            }
        }

        // ---- Re-bind: every row now matches; bind the pattern's vars -----
        let new_vars = merge_new_vars(table, part);
        if !new_vars.is_empty() || table.is_seeded() {
            let view = self
                .seq_decision_view(stager.state(), governance, default_context)
                .await?;
            let mut projections = vec![row_idx_projection(span)];
            for c in &table.cols {
                projections.push(var_projection(c, span));
            }
            for v in &new_vars {
                projections.push(var_projection(v, span));
            }
            let (result, seeded_cols) = if table.is_seeded() {
                let ast = seeded_pattern_query(&table.cols, &merge.pattern, projections, span);
                (
                    self.query_cypher_ast_seeded(
                        &view,
                        &ast,
                        &cols_with_index(&table.cols),
                        rows_with_index(&table.rows, span),
                        tracker,
                    )
                    .await?,
                    table.cols.clone(),
                )
            } else {
                // Unit table: plain pattern match, constant row index.
                let ast = CypherAst {
                    statement: Statement::Query(Query {
                        clauses: vec![ReadClause::Match(MatchClause {
                            pattern: merge.pattern.clone(),
                            where_clause: None,
                            span,
                        })],
                        return_clause: ReturnClause {
                            items: {
                                let mut items = vec![ProjectionItem {
                                    expr: Expr::Lit(Literal::Integer(0, span)),
                                    alias: Some(Variable {
                                        name: ROW_IDX_VAR.to_string(),
                                        span,
                                    }),
                                    span,
                                }];
                                items.extend(new_vars.iter().map(|v| var_projection(v, span)));
                                items
                            },
                            distinct: false,
                            order_by: Vec::new(),
                            skip: None,
                            limit: None,
                            span,
                        },
                        union_tail: None,
                        span,
                    }),
                    span,
                };
                (
                    self.query_cypher_ast_tracked(&view, &ast, tracker).await?,
                    Vec::new(),
                )
            };

            let mut wanted: Vec<String> = vec![ROW_IDX_VAR.to_string()];
            wanted.extend(seeded_cols.iter().cloned());
            wanted.extend(new_vars.iter().cloned());
            let mut rebound = extract_rows(result, &wanted)?;
            check_row_cap(rebound.len())?;
            // Deterministic order: original row index, stable across engine
            // reordering (later duplicates-key dedup depends on it).
            rebound.sort_by_key(|r| binding_row_index(&r[0]).unwrap_or(u64::MAX));

            if rebound.is_empty() && !table.rows.is_empty() {
                return Err(cypher_err(
                    "MERGE re-bind matched no rows — the created pattern is not visible to \
                     the writer (a policy may hide it)",
                ));
            }

            // `ON MATCH SET` applies to every row that did NOT create.
            let creator_set: std::collections::HashSet<u64> =
                creators.iter().map(|&i| i as u64).collect();
            let has_on_match = !merge.on_match.is_empty();
            let mut on_create_rows: Vec<Vec<Binding>> = Vec::new();
            let mut on_match_rows: Vec<Vec<Binding>> = Vec::new();
            let mut new_rows: Vec<Vec<Binding>> = Vec::with_capacity(rebound.len());
            for row in rebound {
                let idx = binding_row_index(&row[0]).ok_or_else(|| {
                    ApiError::internal("sequential MERGE re-bind lost its row index")
                })?;
                let cells: Vec<Binding> = row[1..].to_vec();
                if deferred_on_create && creator_set.contains(&idx) {
                    on_create_rows.push(cells.clone());
                } else if has_on_match && !creator_set.contains(&idx) {
                    on_match_rows.push(cells.clone());
                }
                new_rows.push(cells);
            }

            let mut new_cols = table.cols.clone();
            new_cols.extend(new_vars.iter().cloned());

            if deferred_on_create && !on_create_rows.is_empty() {
                self.seq_stage_set_items(
                    stager,
                    &new_cols,
                    on_create_rows,
                    &merge.on_create,
                    ledger_id,
                    governance,
                    default_context,
                    index_config,
                    policy,
                    tracker,
                    span,
                )
                .await?;
            }

            if has_on_match && !on_match_rows.is_empty() {
                self.seq_stage_set_items(
                    stager,
                    &new_cols,
                    on_match_rows,
                    &merge.on_match,
                    ledger_id,
                    governance,
                    default_context,
                    index_config,
                    policy,
                    tracker,
                    span,
                )
                .await?;
            }

            table.cols = new_cols;
            table.rows = new_rows;
        } else if !merge.on_match.is_empty() {
            // Unit table, var-less pattern, ON MATCH SET: nothing to target.
            return Err(cypher_err(
                "MERGE … ON MATCH SET requires a variable in the MERGE pattern",
            ));
        }
        Ok(())
    }

    /// Stage SET items over selected MERGE-branch rows. Simple literal /
    /// row-column values stay batched. Computed values are evaluated and
    /// staged one row at a time against the evolving virtual state so a
    /// duplicate row observes updates made by the preceding row.
    #[allow(clippy::too_many_arguments)]
    async fn seq_stage_set_items(
        &self,
        stager: &mut SequentialStager,
        cols: &[String],
        rows: Vec<Vec<Binding>>,
        items: &[fluree_db_cypher::ast::SetItem],
        ledger_id: &str,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
        span: SourceSpan,
    ) -> Result<()> {
        if rows.is_empty() || items.is_empty() {
            return Ok(());
        }
        if !set_items_need_hoist(items, cols) {
            let table = RowTable {
                cols: cols.to_vec(),
                rows,
            };
            return self
                .seq_stage_seeded(
                    stager,
                    &table,
                    Vec::new(),
                    vec![WriteClause::Set(SetClause {
                        items: items.to_vec(),
                        span,
                    })],
                    None,
                    ledger_id,
                    index_config,
                    policy,
                    tracker,
                    span,
                    StateAfter::Read,
                )
                .await
                .map(|_| ());
        }

        for row in rows {
            let mut table = RowTable {
                cols: cols.to_vec(),
                rows: vec![row],
            };
            let clause = WriteClause::Set(SetClause {
                items: items.to_vec(),
                span,
            });
            let clause = self
                .hoist_clause_value_exprs(
                    &mut table,
                    &clause,
                    governance,
                    default_context,
                    stager.state(),
                    span,
                    tracker,
                )
                .await?;
            self.seq_stage_seeded(
                stager,
                &table,
                Vec::new(),
                vec![clause],
                None,
                ledger_id,
                index_config,
                policy,
                tracker,
                span,
                StateAfter::Read,
            )
            .await?;
        }
        Ok(())
    }

    /// Evaluate each absent row's identity-value tuple (the values of the
    /// pattern's identifying property expressions, plus bound endpoints for a
    /// relationship MERGE) into a hashable key. Direct cell/constant reads
    /// cover the common shapes; computed expressions fall back to one
    /// engine-evaluated `VALUES` projection.
    #[allow(clippy::too_many_arguments)]
    async fn merge_identity_keys(
        &self,
        table: &RowTable,
        absent: &[usize],
        merge: &MergeClause,
        state: &LedgerState,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
        span: SourceSpan,
        tracker: Option<&Tracker>,
    ) -> Result<Vec<Vec<u8>>> {
        let part = &merge.pattern.parts[0];
        // The identity expressions, in a fixed order.
        let mut exprs: Vec<&Expr> = Vec::new();
        let mut endpoint_cols: Vec<usize> = Vec::new();
        for node in std::iter::once(&part.head).chain(part.tail.iter().map(|(_, n)| n)) {
            if let Some(v) = &node.var {
                if let Some(ci) = table.col_idx(&v.name) {
                    endpoint_cols.push(ci);
                    continue; // a bound endpoint IS its identity
                }
            }
            if let Some(props) = &node.props {
                for (_, e) in &props.entries {
                    exprs.push(e);
                }
            }
        }
        for (rel, _) in &part.tail {
            if let Some(props) = &rel.props {
                for (_, e) in &props.entries {
                    exprs.push(e);
                }
            }
        }

        // Resolve each expression per absent row to a Binding.
        let direct = |e: &Expr, row: &[Binding]| -> Option<Binding> {
            match e {
                Expr::Var(v) => table.col_idx(&v.name).map(|ci| row[ci].clone()),
                Expr::Lit(Literal::Integer(n, _)) => Some(Binding::lit(
                    fluree_db_core::FlakeValue::Long(*n),
                    fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "integer"),
                )),
                Expr::Lit(Literal::Float(f, _)) => Some(Binding::lit(
                    fluree_db_core::FlakeValue::Double(*f),
                    fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "double"),
                )),
                Expr::Lit(Literal::String(s, _)) => Some(Binding::lit(
                    fluree_db_core::FlakeValue::String(s.clone()),
                    fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "string"),
                )),
                Expr::Lit(Literal::Bool(b, _)) => Some(Binding::lit(
                    fluree_db_core::FlakeValue::Boolean(*b),
                    fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "boolean"),
                )),
                _ => None,
            }
        };

        let all_direct = exprs
            .iter()
            .all(|e| direct(e, &table.rows[absent[0]]).is_some());

        let mut keys: Vec<Vec<Binding>> = Vec::with_capacity(absent.len());
        if all_direct {
            for &row in absent {
                let mut key: Vec<Binding> = endpoint_cols
                    .iter()
                    .map(|&ci| table.rows[row][ci].clone())
                    .collect();
                for e in &exprs {
                    key.push(direct(e, &table.rows[row]).expect("checked all_direct"));
                }
                keys.push(key);
            }
        } else {
            // Engine-evaluated: one VALUES projection over the absent rows.
            let view = self
                .seq_probe_view(state, governance, default_context)
                .await?;
            let mut items = vec![row_idx_projection(span)];
            for (i, e) in exprs.iter().enumerate() {
                items.push(ProjectionItem {
                    expr: (*e).clone(),
                    alias: Some(Variable {
                        name: format!("#__cy_seq_k{i}"),
                        span,
                    }),
                    span,
                });
            }
            let ast = CypherAst {
                statement: Statement::Query(Query {
                    clauses: vec![inline_rows_placeholder_with_index(&table.cols, span)],
                    return_clause: ReturnClause {
                        items,
                        distinct: false,
                        order_by: Vec::new(),
                        skip: None,
                        limit: None,
                        span,
                    },
                    union_tail: None,
                    span,
                }),
                span,
            };
            let sub_rows: Vec<Vec<Binding>> = absent
                .iter()
                .map(|&i| {
                    let mut r = table.rows[i].clone();
                    r.push(row_index_binding(i as u64));
                    r
                })
                .collect();
            let mut seed_cols = table.cols.clone();
            seed_cols.push(ROW_IDX_VAR.to_string());
            let result = self
                .query_cypher_ast_seeded(&view, &ast, &seed_cols, sub_rows, tracker)
                .await?;
            let mut wanted: Vec<String> = vec![ROW_IDX_VAR.to_string()];
            for i in 0..exprs.len() {
                wanted.push(format!("#__cy_seq_k{i}"));
            }
            let mut evaluated = extract_rows(result, &wanted)?;
            evaluated.sort_by_key(|r| binding_row_index(&r[0]).unwrap_or(u64::MAX));
            if evaluated.len() != absent.len() {
                return Err(ApiError::internal(
                    "sequential MERGE identity evaluation lost rows",
                ));
            }
            for (pos, row) in evaluated.into_iter().enumerate() {
                let mut key: Vec<Binding> = endpoint_cols
                    .iter()
                    .map(|&ci| table.rows[absent[pos]][ci].clone())
                    .collect();
                key.extend(row.into_iter().skip(1));
                keys.push(key);
            }
        }

        Ok(keys.into_iter().map(|k| binding_key_bytes(&k)).collect())
    }

    /// Hoist computed value expressions out of a write clause into
    /// engine-evaluated row columns: every pattern-property / SET value that
    /// is neither a literal nor a bound-column variable (`{name: p.team}`,
    /// `SET n.x = p.age + 1`) is evaluated per row against the current
    /// virtual state and replaced by a fresh synthetic column reference.
    /// The write lowering then sees only literals and bound variables.
    #[allow(clippy::too_many_arguments)]
    async fn hoist_clause_value_exprs(
        &self,
        table: &mut RowTable,
        clause: &WriteClause,
        governance: Option<&GovernanceOptions>,
        default_context: Option<&serde_json::Value>,
        state: &LedgerState,
        span: SourceSpan,
        tracker: Option<&Tracker>,
    ) -> Result<WriteClause> {
        let include_merge_sets = !matches!(clause, WriteClause::Merge(_));
        let mut clause = clause.clone();
        let mut hoisted: Vec<(String, Expr)> = Vec::new();
        {
            let base = table.cols.len();
            let cols = &table.cols;
            visit_value_exprs(&mut clause, include_merge_sets, &mut |e: &mut Expr| {
                let bound_var = matches!(e, Expr::Var(v) if cols.iter().any(|c| c == &v.name));
                if matches!(e, Expr::Lit(_)) || bound_var {
                    return;
                }
                let idx = match hoisted.iter().position(|(_, h)| h == e) {
                    Some(i) => i,
                    None => {
                        hoisted.push((format!("#__cy_seq_h{}", base + hoisted.len()), e.clone()));
                        hoisted.len() - 1
                    }
                };
                let name = hoisted[idx].0.clone();
                *e = Expr::Var(Variable {
                    name,
                    span: e.span(),
                });
            });
        }
        if hoisted.is_empty() {
            return Ok(clause);
        }
        if table.rows.is_empty() {
            table.cols.extend(hoisted.into_iter().map(|(n, _)| n));
            return Ok(clause);
        }

        let view = self
            .seq_probe_view(state, governance, default_context)
            .await?;
        let (result, wanted) = if table.is_seeded() {
            let mut items = vec![row_idx_projection(span)];
            for (name, e) in &hoisted {
                items.push(ProjectionItem {
                    expr: e.clone(),
                    alias: Some(Variable {
                        name: name.clone(),
                        span,
                    }),
                    span,
                });
            }
            let ast = CypherAst {
                statement: Statement::Query(Query {
                    clauses: vec![inline_rows_placeholder_with_index(&table.cols, span)],
                    return_clause: ReturnClause {
                        items,
                        distinct: false,
                        order_by: Vec::new(),
                        skip: None,
                        limit: None,
                        span,
                    },
                    union_tail: None,
                    span,
                }),
                span,
            };
            let result = self
                .query_cypher_ast_seeded(
                    &view,
                    &ast,
                    &cols_with_index(&table.cols),
                    rows_with_index(&table.rows, span),
                    tracker,
                )
                .await?;
            let mut wanted: Vec<String> = vec![ROW_IDX_VAR.to_string()];
            wanted.extend(hoisted.iter().map(|(n, _)| n.clone()));
            (result, wanted)
        } else {
            // Unit table: constant expressions, one row.
            let ast = CypherAst {
                statement: Statement::Query(Query {
                    clauses: Vec::new(),
                    return_clause: ReturnClause {
                        items: hoisted
                            .iter()
                            .map(|(name, e)| ProjectionItem {
                                expr: e.clone(),
                                alias: Some(Variable {
                                    name: name.clone(),
                                    span,
                                }),
                                span,
                            })
                            .collect(),
                        distinct: false,
                        order_by: Vec::new(),
                        skip: None,
                        limit: None,
                        span,
                    },
                    union_tail: None,
                    span,
                }),
                span,
            };
            let result = self.query_cypher_ast_tracked(&view, &ast, tracker).await?;
            (result, hoisted.iter().map(|(n, _)| n.clone()).collect())
        };

        let mut evaluated = extract_rows(result, &wanted)?;
        if table.is_seeded() {
            evaluated.sort_by_key(|r| binding_row_index(&r[0]).unwrap_or(u64::MAX));
        }
        if evaluated.len() != table.rows.len() {
            return Err(cypher_err(
                "a computed value in a multi-clause write must be row-scalar — \
                 aggregates cannot be hoisted into a write clause",
            ));
        }
        let skip = usize::from(table.is_seeded());
        for (row, cells) in table.rows.iter_mut().zip(evaluated) {
            row.extend(cells.into_iter().skip(skip));
        }
        table.cols.extend(hoisted.into_iter().map(|(n, _)| n));
        Ok(clause)
    }

    /// Lower `write_clauses` (after `read_clauses`, which re-match what the
    /// writes need matched) as one sub-statement with the table's columns
    /// declared bound, seed the row block, and stage it on the stager.
    #[allow(clippy::too_many_arguments)]
    async fn seq_stage_seeded(
        &self,
        stager: &mut SequentialStager,
        table: &RowTable,
        read_clauses: Vec<ReadClause>,
        write_clauses: Vec<WriteClause>,
        skolem: Option<String>,
        ledger_id: &str,
        index_config: Option<&IndexConfig>,
        policy: Option<&crate::PolicyContext>,
        tracker: Option<&Tracker>,
        span: SourceSpan,
        after: StateAfter,
    ) -> Result<usize> {
        let ast = CypherAst {
            statement: Statement::Update(Update {
                read_clauses,
                write_clauses,
                return_clause: None,
                span,
            }),
            span,
        };
        let mut txn = self
            .lower_cypher_ast_to_txn_seeded(
                &ast,
                ledger_id,
                &stager.state().snapshot,
                table.cols.clone(),
            )
            .await?;
        if table.is_seeded() {
            let vars: Vec<Arc<str>> = table
                .cols
                .iter()
                .map(|c| Arc::from(format!("?{c}").as_str()))
                .collect();
            let rows: Vec<Vec<UnresolvedValue>> = table
                .rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|b| UnresolvedValue::PreBound(b.clone()))
                        .collect()
                })
                .collect();
            txn.where_patterns
                .insert(0, UnresolvedPattern::Values { vars, rows });
        }
        txn.opts.skolem_txn_id = skolem;
        stager
            .stage(
                self,
                txn,
                index_config,
                policy,
                tracker,
                after == StateAfter::Read,
            )
            .await
    }
}

/// Visit every *value* expression position of a write clause. MERGE branch
/// sets may be excluded because their newly-bound variables do not exist
/// until the driver's post-create re-bind.
fn visit_value_exprs(
    clause: &mut WriteClause,
    include_merge_sets: bool,
    f: &mut impl FnMut(&mut Expr),
) {
    use fluree_db_cypher::ast::SetItem;
    fn visit_pattern(p: &mut Pattern, f: &mut impl FnMut(&mut Expr)) {
        for part in &mut p.parts {
            if let Some(props) = &mut part.head.props {
                for (_, e) in &mut props.entries {
                    f(e);
                }
            }
            for (rel, node) in &mut part.tail {
                if let Some(props) = &mut rel.props {
                    for (_, e) in &mut props.entries {
                        f(e);
                    }
                }
                if let Some(props) = &mut node.props {
                    for (_, e) in &mut props.entries {
                        f(e);
                    }
                }
            }
        }
    }
    fn visit_items(items: &mut [SetItem], f: &mut impl FnMut(&mut Expr)) {
        for item in items {
            match item {
                SetItem::Property { value, .. } => f(value),
                SetItem::MapMerge { map, .. } | SetItem::MapReplace { map, .. } => {
                    for (_, e) in &mut map.entries {
                        f(e);
                    }
                }
                SetItem::Labels { .. } => {}
            }
        }
    }
    match clause {
        WriteClause::Create(c) => visit_pattern(&mut c.pattern, f),
        WriteClause::Merge(m) => {
            visit_pattern(&mut m.pattern, f);
            if include_merge_sets {
                visit_items(&mut m.on_create, f);
                visit_items(&mut m.on_match, f);
            }
        }
        WriteClause::Set(s) => visit_items(&mut s.items, f),
        WriteClause::Remove(_) | WriteClause::Delete(_) | WriteClause::Foreach(_) => {}
    }
}

fn set_items_need_hoist(items: &[fluree_db_cypher::ast::SetItem], cols: &[String]) -> bool {
    use fluree_db_cypher::ast::SetItem;
    let needs_hoist = |e: &Expr| {
        !matches!(e, Expr::Lit(_))
            && !matches!(e, Expr::Var(v) if cols.iter().any(|c| c == &v.name))
    };
    items.iter().any(|item| match item {
        SetItem::Property { value, .. } => needs_hoist(value),
        SetItem::MapMerge { map, .. } | SetItem::MapReplace { map, .. } => {
            map.entries.iter().any(|(_, value)| needs_hoist(value))
        }
        SetItem::Labels { .. } => false,
    })
}

/// Reject members the v1 driver defers, with clear errors.
fn validate_members(u: &Update) -> Result<()> {
    // A row an OPTIONAL MATCH leaves unbound is not seeded into a SET,
    // REMOVE or DETACH DELETE (see the staging loop), but could assert part
    // of a CREATE or MERGE pattern.
    let creates = u
        .write_clauses
        .iter()
        .any(|w| matches!(w, WriteClause::Create(_) | WriteClause::Merge(_)));
    for c in &u.read_clauses {
        match c {
            ReadClause::OptionalMatch(_) if creates => {
                return Err(cypher_err(
                    "OPTIONAL MATCH before a multi-clause write is not supported — a \
                     partially-bound row could assert a partial pattern; bind the variables \
                     with a mandatory MATCH",
                ));
            }
            ReadClause::CallSubquery(_) => {
                return Err(cypher_err(
                    "CALL { … } before a multi-clause write is deferred",
                ));
            }
            _ => {}
        }
    }
    let (_, rel_vars) = relationship_scope(&u.read_clauses, false);
    for w in &u.write_clauses {
        match w {
            // A DELETE stages as a seeded `Txn`. A node's DETACH DELETE finds
            // the node's triples in the state it stages on, and a
            // relationship's DELETE (DETACH or not) re-matches the edge it
            // reifies there (see `rel_delete_edges`); one clause may hold both.
            WriteClause::Delete(d)
                if d.detach
                    || d.targets
                        .iter()
                        .all(|t| rel_vars.iter().any(|r| r.name == t.name)) => {}
            WriteClause::Delete(_) => {
                return Err(cypher_err(
                    "a bare node DELETE in a multi-clause write statement is deferred — run \
                     the DELETE as its own statement (a node's DETACH DELETE and a \
                     relationship's DELETE are supported)",
                ));
            }
            WriteClause::Foreach(_) => {
                return Err(cypher_err(
                    "FOREACH in a multi-clause write statement is deferred",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Mirror `lower_merge`'s pattern-shape constraints up front so probes never
/// run for a shape the create branch would reject mid-statement.
fn validate_merge_pattern(m: &MergeClause) -> Result<()> {
    if m.pattern.parts.len() != 1 {
        return Err(cypher_err(
            "multi-part MERGE (comma-separated patterns) is deferred — v1 supports one pattern",
        ));
    }
    let part = &m.pattern.parts[0];
    if part.path_search.is_some() || part.path_var.is_some() {
        return Err(cypher_err(
            "shortestPath / path-variable MERGE is not supported",
        ));
    }
    match part.tail.as_slice() {
        [] => Ok(()),
        [(rel, _)] => {
            if matches!(rel.direction, fluree_db_cypher::ast::Direction::Either) {
                return Err(cypher_err(
                    "undirected relationship `-[r]-` in MERGE — use `-[r]->` or `<-[r]-`",
                ));
            }
            if rel.length.is_some() {
                return Err(cypher_err(
                    "variable-length relationship in MERGE is not supported",
                ));
            }
            if rel.types.len() != 1 {
                return Err(cypher_err(
                    "MERGE relationship needs exactly one type — `-[:T]->`",
                ));
            }
            Ok(())
        }
        _ => Err(cypher_err(
            "multi-hop MERGE pattern is deferred — v1 supports a single relationship \
             `(a)-[r:T]->(b)`",
        )),
    }
}

/// Variables a MERGE pattern binds that the table doesn't already carry.
fn merge_new_vars(table: &RowTable, part: &PatternPart) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |v: &Option<Variable>| {
        if let Some(v) = v {
            if table.col_idx(&v.name).is_none() && !out.contains(&v.name) {
                out.push(v.name.clone());
            }
        }
    };
    push(&part.head.var);
    for (rel, node) in &part.tail {
        push(&rel.var);
        push(&node.var);
    }
    out
}

/// Ordered, deduped variables visible at the end of the read prefix
/// (Cypher scoping: a `WITH` narrows the set to its projection).
fn visible_vars(clauses: &[ReadClause]) -> Result<Vec<String>> {
    let mut vars: Vec<String> = Vec::new();
    let add = |name: &str, vars: &mut Vec<String>| {
        if !vars.iter().any(|v| v == name) {
            vars.push(name.to_string());
        }
    };
    for c in clauses {
        match c {
            ReadClause::Match(m) | ReadClause::OptionalMatch(m) => {
                for part in &m.pattern.parts {
                    if let Some(pv) = &part.path_var {
                        add(&pv.name, &mut vars);
                    }
                    for node in std::iter::once(&part.head).chain(part.tail.iter().map(|(_, n)| n))
                    {
                        if let Some(v) = &node.var {
                            add(&v.name, &mut vars);
                        }
                    }
                    for (rel, _) in &part.tail {
                        if let Some(v) = &rel.var {
                            add(&v.name, &mut vars);
                        }
                    }
                }
            }
            ReadClause::Unwind(u) => add(&u.alias.name, &mut vars),
            ReadClause::InlineRows { vars: cols, .. } => {
                for v in cols {
                    add(&v.name, &mut vars);
                }
            }
            ReadClause::With(w) => {
                let mut narrowed: Vec<String> = Vec::new();
                for item in &w.items {
                    if matches!((&item.alias, &item.expr), (None, Expr::Var(v)) if v.name == "*") {
                        for name in &vars {
                            if !narrowed.contains(name) {
                                narrowed.push(name.clone());
                            }
                        }
                        continue;
                    }
                    let name = match (&item.alias, &item.expr) {
                        (Some(a), _) => a.name.clone(),
                        (None, Expr::Var(v)) => v.name.clone(),
                        (None, _) => {
                            return Err(cypher_err(
                                "WITH items before a multi-clause write must be variables or \
                                 aliased expressions",
                            ));
                        }
                    };
                    if !narrowed.contains(&name) {
                        narrowed.push(name);
                    }
                }
                vars = narrowed;
            }
            ReadClause::CallSubquery(_) => unreachable!("rejected by validate_members"),
        }
    }
    Ok(vars)
}

/// The read prefix as a read query projecting `cols`.
fn read_prefix_query(read_clauses: &[ReadClause], cols: &[String], span: SourceSpan) -> CypherAst {
    let items = if cols.is_empty() {
        vec![ProjectionItem {
            expr: Expr::Lit(Literal::Integer(0, span)),
            alias: Some(Variable {
                name: ROW_SEED_VAR.to_string(),
                span,
            }),
            span,
        }]
    } else {
        cols.iter().map(|c| var_projection(c, span)).collect()
    };
    CypherAst {
        statement: Statement::Query(Query {
            clauses: read_clauses.to_vec(),
            return_clause: ReturnClause {
                items,
                distinct: false,
                order_by: Vec::new(),
                skip: None,
                limit: None,
                span,
            },
            union_tail: None,
            span,
        }),
        span,
    }
}

/// `[InlineRows(cols + row-index), Match(pattern)] RETURN <projections>`.
fn seeded_pattern_query(
    cols: &[String],
    pattern: &Pattern,
    projections: Vec<ProjectionItem>,
    span: SourceSpan,
) -> CypherAst {
    CypherAst {
        statement: Statement::Query(Query {
            clauses: vec![
                inline_rows_placeholder_with_index(cols, span),
                ReadClause::Match(MatchClause {
                    pattern: pattern.clone(),
                    where_clause: None,
                    span,
                }),
            ],
            return_clause: ReturnClause {
                items: projections,
                distinct: false,
                order_by: Vec::new(),
                skip: None,
                limit: None,
                span,
            },
            union_tail: None,
            span,
        }),
        span,
    }
}

/// A write sub-statement: no reads (rows are seeded post-lowering), the given
/// write clauses, no RETURN.
fn update_ast(write_clauses: Vec<WriteClause>, span: SourceSpan) -> CypherAst {
    CypherAst {
        statement: Statement::Update(Update {
            read_clauses: Vec::new(),
            write_clauses,
            return_clause: None,
            span,
        }),
        span,
    }
}

/// An `InlineRows` clause declaring `cols`, with one placeholder row (the
/// seeded execution replaces the rows wholesale — see
/// `query_cypher_ast_seeded`).
fn inline_rows_placeholder(cols: &[String], span: SourceSpan) -> ReadClause {
    ReadClause::InlineRows {
        vars: cols
            .iter()
            .map(|c| Variable {
                name: c.clone(),
                span,
            })
            .collect(),
        rows: vec![cols
            .iter()
            .map(|_| Expr::Lit(Literal::Integer(0, span)))
            .collect()],
    }
}

/// [`inline_rows_placeholder`] with the synthetic row-index column appended.
fn inline_rows_placeholder_with_index(cols: &[String], span: SourceSpan) -> ReadClause {
    let mut all: Vec<String> = cols.to_vec();
    all.push(ROW_IDX_VAR.to_string());
    inline_rows_placeholder(&all, span)
}

fn var_projection(name: &str, span: SourceSpan) -> ProjectionItem {
    ProjectionItem {
        expr: Expr::Var(Variable {
            name: name.to_string(),
            span,
        }),
        alias: None,
        span,
    }
}

fn row_idx_projection(span: SourceSpan) -> ProjectionItem {
    var_projection(ROW_IDX_VAR, span)
}

fn row_index_binding(i: u64) -> Binding {
    Binding::lit(
        fluree_db_core::FlakeValue::Long(i as i64),
        fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "integer"),
    )
}

fn binding_row_index(b: &Binding) -> Option<u64> {
    match b {
        Binding::Lit {
            val: fluree_db_core::FlakeValue::Long(n),
            ..
        } => u64::try_from(*n).ok(),
        _ => None,
    }
}

/// The table's columns with the synthetic row-index column appended (the
/// seed-column list matching [`inline_rows_placeholder_with_index`]).
fn cols_with_index(cols: &[String]) -> Vec<String> {
    let mut all: Vec<String> = cols.to_vec();
    all.push(ROW_IDX_VAR.to_string());
    all
}

/// The table's rows with the synthetic row-index column appended.
fn rows_with_index(rows: &[Vec<Binding>], _span: SourceSpan) -> Vec<Vec<Binding>> {
    rows.iter()
        .enumerate()
        .map(|(i, r)| {
            let mut row = r.clone();
            row.push(row_index_binding(i as u64));
            row
        })
        .collect()
}

fn check_row_cap(n: usize) -> Result<()> {
    if n > MAX_SEQUENTIAL_ROWS {
        return Err(cypher_err(format!(
            "a multi-clause write is capped at {MAX_SEQUENTIAL_ROWS} rows per clause \
             (got {n}); split the batch"
        )));
    }
    Ok(())
}

/// Extract the distinct row indices present in a probe result.
fn extract_row_indices(result: QueryResult) -> Result<std::collections::HashSet<usize>> {
    let rows = extract_rows(result, std::slice::from_ref(&ROW_IDX_VAR.to_string()))?;
    let mut out = std::collections::HashSet::with_capacity(rows.len());
    for row in rows {
        let idx = binding_row_index(&row[0])
            .ok_or_else(|| ApiError::internal("sequential MERGE probe lost its row index"))?;
        out.insert(idx as usize);
    }
    Ok(out)
}

/// Extract `wanted` columns from a query result as fully-materialized
/// bindings (encoded values decoded through the result's binary graph view,
/// so seeded reuse never depends on late materialization). Consumes the
/// result — the view moves into the materializer.
fn extract_rows(mut result: QueryResult, wanted: &[String]) -> Result<Vec<Vec<Binding>>> {
    let var_ids: Vec<fluree_db_query::VarId> = wanted
        .iter()
        .map(|name| {
            result.vars.get(name).ok_or_else(|| {
                ApiError::internal(format!(
                    "sequential write driver: projected variable `{name}` missing from result"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut materializer = result
        .binary_graph
        .take()
        .map(|bg| Materializer::new(bg, fluree_db_query::JoinKeyMode::SingleLedger));

    let mut out: Vec<Vec<Binding>> = Vec::with_capacity(result.row_count());
    for batch in &result.batches {
        for row in 0..batch.len() {
            let mut cells = Vec::with_capacity(var_ids.len());
            for &var in &var_ids {
                let b = batch.get(row, var).cloned().unwrap_or(Binding::Unbound);
                let b = match &mut materializer {
                    Some(m) => m.to_term(&b)?,
                    None => b,
                };
                cells.push(normalize_binding(b)?);
            }
            out.push(cells);
        }
    }
    Ok(out)
}

/// Normalize a materialized binding for row threading: strip history
/// metadata (so identity keys and VALUES set-semantics behave), map
/// `IriMatch` to its primary Sid (the driver is single-ledger), and reject
/// shapes that cannot round-trip through a seeded `VALUES`.
fn normalize_binding(b: Binding) -> Result<Binding> {
    Ok(match b {
        Binding::Sid { sid, .. } => Binding::sid(sid),
        Binding::IriMatch { primary_sid, .. } => Binding::sid(primary_sid),
        Binding::Lit { val, dtc, .. } => Binding::Lit {
            val,
            dtc,
            t: None,
            op: None,
            p_id: None,
        },
        Binding::Unbound | Binding::Poisoned => Binding::Unbound,
        other @ (Binding::Iri(_)
        | Binding::Path { .. }
        | Binding::Rel(_)
        | Binding::List(_)
        | Binding::Map(_)) => other,
        other => {
            return Err(ApiError::internal(format!(
                "sequential write driver: unexpected binding shape in row table: {other:?}"
            )))
        }
    })
}

/// A stable hashable byte key for an identity-value tuple. Uses the debug
/// representation of normalized bindings — values are already materialized
/// and metadata-stripped, so equal values render equally.
fn binding_key_bytes(key: &[Binding]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut s = String::new();
    for b in key {
        let _ = write!(s, "{b:?}\u{1f}");
    }
    s.into_bytes()
}
