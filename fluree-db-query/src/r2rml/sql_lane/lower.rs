//! Lowering one `GRAPH <sql-source> { … }` block over its R2RML mapping into a
//! [`RelPlan`] — the statement a SQL expert would write for it — or declining.
//!
//! The block is read as an *entity graph*: triple patterns grouped by subject
//! are one entity each, resolved to exactly one triples map and therefore one
//! table access; object variables that are subjects of other entities are
//! edges, joined on the mapping's `rr:joinCondition` columns or on the columns
//! of an identical IRI template — never on a rendered IRI string. Everything
//! the plan cannot express exactly is either kept in the engine (a residual
//! `FILTER` over the returned rows) or declines the whole block, in which case
//! the per-scan lane runs. There is no approximate translation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::{DatatypeConstraint, FlakeValue, LedgerSnapshot};
use fluree_db_r2rml::mapping::{
    CompiledR2rmlMapping, ConstantValue, ObjectMap, PredicateMap, TermType, TriplesMap,
};
use fluree_db_r2rml::materialize::reverse_subject_template;
use fluree_db_r2rml::RdfTerm;
use fluree_db_tabular::plan::{
    like_escape, ArithOp, CmpOp, ColRef, Expr, KeySet, Literal, OrderKey, OutputCol, OutputExpr,
    Pred, PushdownCapabilities, RelNode, RelPlan, RelSource,
};
use fluree_db_tabular::{BatchSchema, FieldType};
use fluree_vocab::{rdf, xsd, UnresolvedDatatypeConstraint};

use crate::binding::Binding;
use crate::error::Result;
use crate::ir::expression::Function;
use crate::ir::grouping::AggregateFn;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Pattern, SubqueryPattern};
use crate::r2rml::policy::{derived_type_map, static_classes, Verdict};
use crate::var_registry::VarId;

use super::aggregate::{group_plan, Decode, NumKind};

/// Why the lane declined; logged at debug so a `MustNotFire` test can name it.
#[derive(Debug)]
pub(crate) struct Decline(pub &'static str);

pub(crate) type Lowering<T> = std::result::Result<T, Decline>;

fn decline<T>(why: &'static str) -> Lowering<T> {
    Err(Decline(why))
}

/// How a block variable's binding is rebuilt from returned columns.
#[derive(Debug, Clone)]
pub(crate) enum TermSource {
    /// The subject of the triples map behind `alias`.
    Subject {
        alias: String,
    },
    /// Object map `pom` of triples map `tm_iri`, read from the row of
    /// `alias` (a map sharing the access's table and subject, when not the
    /// access's own).
    Object {
        alias: String,
        tm_iri: String,
        pom: usize,
    },
    Constant(RdfTerm),
    /// An aggregate a subquery computed, read from the derived table's
    /// outputs under `alias`.
    Aggregate {
        alias: String,
        kind: AggTerm,
    },
    /// A variable of a union entity: the branch named by the `tag` column
    /// of `alias` (the union's derived table) decodes it.
    Union {
        alias: String,
        tag: String,
        branches: Vec<TermSource>,
    },
}

/// How a derived table's aggregate output becomes a binding.
#[derive(Debug, Clone)]
pub(crate) enum AggTerm {
    Count {
        column: String,
    },
    Numeric {
        sum: String,
        count: String,
        kind: NumKind,
        avg: bool,
    },
}

/// A term alias that reads a derived table: the mapping columns it needs,
/// each with the derived output holding it.
#[derive(Debug, Clone)]
struct DerivedCols {
    derived: String,
    tm_iri: String,
    columns: Vec<(String, String)>,
}

/// What a pushed expression yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprKind {
    Num,
    Str,
}

/// The RDF value class of a column or literal, for exact-comparison checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RdfClass {
    Iri,
    Str,
    LangStr(String),
    Numeric,
    Bool,
    Date,
    DateTime,
    Other,
}

/// The columns a variable's value is a function of — what a join, a seed or
/// a filter can be expressed on.
#[derive(Debug, Clone)]
pub(crate) enum KeyShape {
    /// An IRI rendered from `template` over `cols` (in placeholder order);
    /// `types` are the probed column types, when known, so a reversed key
    /// that cannot be a value of its column is recognized as matching nothing.
    Template {
        template: String,
        cols: Vec<ColRef>,
        types: Vec<Option<FieldType>>,
    },
    /// A raw column value, as a literal of `class` (or an IRI when `Iri`).
    Column { col: ColRef, class: RdfClass },
}

#[derive(Debug, Clone)]
pub(crate) struct VarSource {
    pub term: TermSource,
    pub key: Option<KeyShape>,
    pub nullable: bool,
}

/// One table access in the plan.
#[derive(Debug, Clone)]
pub(crate) struct AccessInfo {
    pub alias: String,
    pub tm_iri: String,
    /// Columns returned for this alias, original names.
    pub columns: Vec<String>,
    /// Statement outputs feeding `columns`, when they are not the columns
    /// themselves: an aggregate's extreme stands in for the column it reads.
    pub output_names: Option<Vec<String>>,
}

/// A block variable the engine can seed from bindings it already holds.
#[derive(Debug, Clone)]
pub(crate) struct SeedSpec {
    pub var: VarId,
    pub shape: KeyShape,
}

#[derive(Debug, Clone)]
pub(crate) struct Lowered {
    pub root: RelNode,
    pub outputs: Vec<OutputCol>,
    pub accesses: Vec<AccessInfo>,
    pub terms: Vec<(VarId, TermSource)>,
    pub residual_filters: Vec<Expression>,
    /// `BIND`s evaluated in the engine over each returned row, in order,
    /// before the residual filters.
    pub binds: Vec<(VarId, Expression)>,
    pub seeds: Vec<SeedSpec>,
    /// Variables the block binds, in schema order.
    pub block_vars: Vec<VarId>,
    /// Where each block variable's value comes from, and the plan columns
    /// it is a function of (what a GROUP BY or an aggregate reads).
    pub vars: HashMap<VarId, VarSource>,
    pub var_columns: HashMap<VarId, Vec<ColRef>>,
    /// Required (non-nullable) literal columns a `ORDER BY` may be pushed on.
    pub order_columns: HashMap<VarId, (ColRef, RdfClass)>,
    /// `BIND` variables whose numeric expression the statement can order by.
    pub order_exprs: HashMap<VarId, Expr>,
    /// A `LIMIT` on the statement is a superset of the block's result only
    /// when no residual filter drops rows afterwards and every variable
    /// shared with the outer query is seeded into the statement.
    pub limit_is_exact: bool,
    /// The statement is `SELECT DISTINCT` over the columns of the variables
    /// a `DISTINCT` above reads (plus what the join and residuals need);
    /// the other block variables come back unbound.
    pub distinct: bool,
}

#[derive(Debug, Clone)]
enum SubjRef {
    Var(VarId),
    Iri(String),
}

#[derive(Debug, Clone)]
enum Obj {
    Var(VarId),
    Iri(String),
    Lit(FlakeValue, Option<DatatypeConstraint>),
}

#[derive(Debug, Clone)]
struct Tp {
    s: SubjRef,
    p: String,
    o: Obj,
}

struct Block {
    triples: Vec<Tp>,
    filters: Vec<Expression>,
    optionals: Vec<(Vec<Tp>, Vec<Expression>)>,
    values: Vec<(Vec<VarId>, Vec<Vec<Binding>>)>,
    binds: Vec<(VarId, Expression)>,
    subqueries: Vec<SubqueryPattern>,
}

/// Per-`(triples map, predicate)` view-policy verdict; `None` when not
/// decidable before the rows are read (the lane declines).
pub(crate) type PolicyVerdict<'a> = &'a mut dyn FnMut(&TriplesMap, &str) -> Result<Option<Verdict>>;

pub(crate) struct LowerInput<'a> {
    pub patterns: &'a [Pattern],
    pub mapping: &'a CompiledR2rmlMapping,
    pub snapshot: &'a LedgerSnapshot,
    pub caps: &'a PushdownCapabilities,
    /// Variables bound by the outer query that this block shares.
    pub child_vars: &'a [VarId],
    pub policy: Option<PolicyVerdict<'a>>,
    /// Probed schemas for the relations [`wanted_schemas`] named.
    pub schemas: &'a HashMap<RelSource, Arc<BatchSchema>>,
    /// The variables a `DISTINCT` directly above the block reads, when there
    /// is one: the statement may then be `SELECT DISTINCT` over their columns.
    pub projection: Option<&'a [VarId]>,
}

/// The most `UNION` branch combinations one block expands into.
const MAX_UNION_BLOCKS: usize = 8;

/// The most resolutions (choices of a providing triples map per member) one
/// entity unions.
const MAX_MAP_ALTERNATIVES: usize = 8;

/// Lower a block: one lowering per `UNION` branch combination, each its own
/// statement. `Ok(Err(Decline))` is a structural decline (the per-scan lane
/// runs); an empty `Ok(Ok(_))` means the block provably yields no rows.
pub(crate) fn lower_block(input: LowerInput<'_>) -> Result<Lowering<Vec<Lowered>>> {
    let blocks = match expand_unions(input.patterns) {
        Ok(b) => b,
        Err(d) => return Ok(Err(d)),
    };
    let mut policy = input.policy;
    let mut out = Vec::with_capacity(blocks.len());
    for patterns in &blocks {
        let block = match parse_block(patterns, input.snapshot) {
            Ok(b) => b,
            Err(d) => return Ok(Err(d)),
        };
        let mut lw = Lowerer {
            mapping: input.mapping,
            caps: input.caps,
            policy: policy.take(),
            schemas: input.schemas,
            next_alias: 0,
            accesses: Vec::new(),
            access_preds: HashMap::new(),
            edges: Vec::new(),
            left_joins: Vec::new(),
            vars: HashMap::new(),
            var_order: Vec::new(),
            residuals: Vec::new(),
            bind_exprs: HashMap::new(),
            snapshot: input.snapshot,
            derived: HashMap::new(),
            derived_terms: HashMap::new(),
            required_columns: HashSet::new(),
            static_keysets: Vec::new(),
            unions: HashMap::new(),
            forced: None,
            derived_types: HashMap::new(),
        };
        let lowered = lw.lower(
            block,
            input.child_vars,
            input.projection,
            input.projection.is_some(),
        )?;
        policy = lw.policy.take();
        match lowered {
            Ok(Some(l)) => out.push(l),
            Ok(None) => {}
            Err(d) => return Ok(Err(d)),
        }
    }
    Ok(Ok(out))
}

/// Every `UNION`-free block the patterns stand for: each `UNION` multiplies
/// the blocks by its branches, a branch's patterns taking the union's place
/// (SPARQL joins the union's result with the rest of the group, which is the
/// same as joining each branch with it).
fn expand_unions(patterns: &[Pattern]) -> Lowering<Vec<Vec<Pattern>>> {
    let mut blocks: Vec<Vec<Pattern>> = vec![Vec::new()];
    for p in patterns {
        match p {
            Pattern::Union(branches) => {
                if branches.is_empty() {
                    return decline("UNION without branches");
                }
                let mut next = Vec::new();
                for branch in branches {
                    for expanded in expand_unions(branch)? {
                        for prefix in &blocks {
                            let mut block = prefix.clone();
                            block.extend(expanded.iter().cloned());
                            next.push(block);
                        }
                    }
                }
                if next.len() > MAX_UNION_BLOCKS {
                    return decline("too many UNION branch combinations");
                }
                blocks = next;
            }
            other => blocks.iter_mut().for_each(|b| b.push(other.clone())),
        }
    }
    Ok(blocks)
}

struct Lowerer<'a> {
    mapping: &'a CompiledR2rmlMapping,
    caps: &'a PushdownCapabilities,
    policy: Option<PolicyVerdict<'a>>,
    schemas: &'a HashMap<RelSource, Arc<BatchSchema>>,
    next_alias: usize,
    accesses: Vec<AccessInfo>,
    /// Predicates local to one access (its `WHERE`, or `ON` when left-joined).
    access_preds: HashMap<String, Vec<Pred>>,
    /// Inner-join predicates between two accesses.
    edges: Vec<(String, String, Pred)>,
    /// Left-joined accesses in order: `(alias, on-predicates)`.
    left_joins: Vec<(String, Vec<Pred>)>,
    vars: HashMap<VarId, VarSource>,
    var_order: Vec<VarId>,
    residuals: Vec<Expression>,
    /// `BIND` expressions the statement can compute, by variable, with the
    /// kind of value they yield.
    bind_exprs: HashMap<VarId, (Expr, ExprKind)>,
    snapshot: &'a LedgerSnapshot,
    /// Derived tables (subqueries) by alias, joined like accesses.
    derived: HashMap<String, RelPlan>,
    /// Term aliases reading a derived table.
    derived_terms: HashMap<String, DerivedCols>,
    /// `(alias, column)` pairs that are `IS NOT NULL`.
    required_columns: HashSet<(String, String)>,
    static_keysets: Vec<(KeySet, Vec<Pred>)>,
    /// Union entities by alias, joined like accesses.
    unions: HashMap<String, UnionInfo>,
    /// The one resolution a union branch's lowering takes for its entity.
    forced: Option<Vec<Part<'a>>>,
    /// Column types of derived and union outputs, by alias then column.
    derived_types: HashMap<String, HashMap<String, FieldType>>,
}

/// A union entity's derived table: one plan per resolution, each output
/// row tagged with its branch under `tag`.
struct UnionInfo {
    branches: Vec<RelPlan>,
    tag: String,
    /// Per parent triples map a foreign key may target, the parent's join
    /// columns as `(column, slot)` — present only when every branch reads
    /// the parent's row.
    parent_cols: HashMap<String, Vec<(String, String)>>,
}

/// A foreign key another entity may point at an entity: the parent map and
/// its join columns; `certain` when every map that could provide the
/// pointing member uses it (so the join is placed whichever map is chosen).
struct Incoming {
    parent: String,
    cols: Vec<String>,
    certain: bool,
}

/// A required entity that resolves to no triples map: the block is empty.
struct Empty;

/// A deferred foreign-key edge: `(child alias, join conditions, parent
/// triples map, the entity variable the FK points at)`.
type RefEdge = (String, Vec<(String, String)>, String, VarId);

impl<'a> Lowerer<'a> {
    fn lower(
        &mut self,
        mut block: Block,
        child_vars: &[VarId],
        projection: Option<&[VarId]>,
        distinct: bool,
    ) -> Result<Lowering<Option<Lowered>>> {
        // A BIND is computed in the engine after the statement, so it cannot
        // be a join key with the outer query.
        let binds = std::mem::take(&mut block.binds);
        if binds.iter().any(|(v, _)| child_vars.contains(v)) {
            return Ok(Err(Decline("BIND variable bound by the outer query")));
        }
        // Required entities.
        let entities = group_entities(&block.triples);
        let required_subjects: HashSet<VarId> = entities
            .iter()
            .filter_map(|(s, _)| match s {
                SubjRef::Var(v) => Some(*v),
                SubjRef::Iri(_) => None,
            })
            .collect();
        // An entity whose members split across triples maps sharing its
        // subject (vertical partitioning) is one access per distinct row
        // source, joined on the subject's key columns; maps over the same
        // table and subject share one access.
        let mut entity_accesses: HashMap<usize, Vec<(String, &'a TriplesMap)>> = HashMap::new();
        let mut union_entities: HashMap<usize, String> = HashMap::new();
        let mut pending_refs: Vec<RefEdge> = Vec::new();
        // Foreign keys the other entities may point at each entity, known
        // before it is lowered so a union can expose the parent's columns.
        let mut incoming: HashMap<VarId, Vec<Incoming>> = HashMap::new();
        for (subject, members) in &entities {
            for (pred, obj) in members {
                let Obj::Var(v) = obj else { continue };
                if !required_subjects.contains(v) || matches!(subject, SubjRef::Var(s) if s == v) {
                    continue;
                }
                let mut refs: Vec<(String, Vec<String>)> = Vec::new();
                let mut providers = 0usize;
                for tm in self.mapping.triples_maps.values() {
                    let Some(i) = pom_for(tm, pred) else { continue };
                    providers += 1;
                    if let ObjectMap::RefObjectMap(rom) = &tm.predicate_object_maps[i].object_map {
                        let cols = rom
                            .join_conditions
                            .iter()
                            .map(|jc| jc.parent_column.clone())
                            .collect();
                        refs.push((rom.parent_triples_map.clone(), cols));
                    }
                }
                let certain =
                    providers > 0 && refs.len() == providers && refs.iter().all(|r| r == &refs[0]);
                for (parent, cols) in refs {
                    let entry = incoming.entry(*v).or_default();
                    if !entry.iter().any(|i| i.parent == parent && i.cols == cols) {
                        entry.push(Incoming {
                            parent,
                            cols,
                            certain,
                        });
                    }
                }
            }
        }
        for (idx, (subject, members)) in entities.iter().enumerate() {
            let mut alternatives = match self.resolve_alternatives(members, None) {
                Ok(a) => a,
                Err(d) => return Ok(Err(d)),
            };
            let parts = match alternatives.len() {
                0 => return Ok(Ok(None)),
                1 => alternatives.pop().expect("one resolution"),
                // Several resolutions: their union, as one derived table.
                _ => {
                    let refs: &[Incoming] = match subject {
                        SubjRef::Var(v) => incoming.get(v).map_or(&[], Vec::as_slice),
                        SubjRef::Iri(_) => &[],
                    };
                    match self.lower_union_entity(subject, members, alternatives, refs)? {
                        Ok(Ok(alias)) => {
                            union_entities.insert(idx, alias);
                        }
                        Ok(Err(Empty)) => return Ok(Ok(None)),
                        Err(d) => return Ok(Err(d)),
                    }
                    entity_accesses.insert(idx, Vec::new());
                    continue;
                }
            };
            let mut accesses: Vec<(String, &'a TriplesMap)> = Vec::new();
            for (tm, member_idxs) in parts {
                let alias = match accesses.iter().find(|(_, a)| same_row(a, tm)) {
                    Some((alias, _)) => alias.clone(),
                    None => {
                        let alias = self.new_access(tm);
                        match self.bind_subject(&alias, tm, subject, false) {
                            Ok(Ok(())) => {}
                            Ok(Err(Empty)) => return Ok(Ok(None)),
                            Err(d) => return Ok(Err(d)),
                        }
                        accesses.push((alias.clone(), tm));
                        alias
                    }
                };
                for i in &member_idxs {
                    match self.allowed(tm, &members[*i].0)? {
                        Some(Verdict::Allow) => {}
                        Some(Verdict::Deny) => return Ok(Ok(None)),
                        Some(verdict) => match self.verdict_pred(&alias, tm, &verdict) {
                            Ok(Ok(Some(pred))) => self.place_pred(pred),
                            Ok(Ok(None)) => {}
                            Ok(Err(Empty)) => return Ok(Ok(None)),
                            Err(d) => return Ok(Err(d)),
                        },
                        None => return Ok(Err(Decline("policy not static"))),
                    }
                }
                for i in member_idxs {
                    let (pred, obj) = &members[i];
                    if pred == rdf::TYPE {
                        // A class the map derives from a column is a value
                        // of that column.
                        let Obj::Iri(class) = obj else { continue };
                        if static_classes(tm).iter().any(|c| c == class) {
                            continue;
                        }
                        match self.class_value(&alias, tm, class) {
                            Ok(Some((col, value))) => self.place_pred(Pred::Cmp {
                                col,
                                op: CmpOp::Eq,
                                value,
                            }),
                            Ok(None) => return Ok(Ok(None)),
                            Err(d) => return Ok(Err(d)),
                        }
                        continue;
                    }
                    match self.bind_member(&alias, tm, pred, obj, &required_subjects, false) {
                        Ok(Ok(Some(edge))) => pending_refs.push(edge),
                        Ok(Ok(None)) => {}
                        Ok(Err(Empty)) => return Ok(Ok(None)),
                        Err(d) => return Ok(Err(d)),
                    }
                }
            }
            entity_accesses.insert(idx, accesses);
        }
        // Deferred foreign-key edges to entities that got their alias later.
        for (child_alias, conds, parent_tm_iri, var) in pending_refs {
            let target = entities
                .iter()
                .position(|(s, _)| matches!(s, SubjRef::Var(v) if *v == var));
            // A union exposes the parent's columns when every branch reads
            // the parent's row.
            if let Some(u) = target.and_then(|i| union_entities.get(&i)) {
                let Some(cols) = self
                    .unions
                    .get(u)
                    .and_then(|info| info.parent_cols.get(&parent_tm_iri))
                else {
                    return Ok(Err(Decline(
                        "ref object map into a union entity without the parent's row",
                    )));
                };
                for (child_col, parent_col) in conds {
                    let Some((_, slot)) = cols.iter().find(|(c, _)| c == &parent_col) else {
                        return Ok(Err(Decline(
                            "ref object map parent column not in the union",
                        )));
                    };
                    self.edges.push((
                        child_alias.clone(),
                        u.clone(),
                        Pred::ColEq {
                            left: ColRef::new(&child_alias, child_col),
                            right: ColRef::new(u, slot),
                        },
                    ));
                }
                continue;
            }
            let Some(accesses) = target.and_then(|i| entity_accesses.get(&i)) else {
                return Ok(Err(Decline("ref target entity missing")));
            };
            let Some(parent) = self.mapping.get(&parent_tm_iri) else {
                return Ok(Err(Decline("ref object map parent missing")));
            };
            // The parent columns are on the parent map's table: join the
            // entity's access over that table and subject, which need not
            // be the parent map itself.
            let parent_alias = match accesses.iter().find(|(_, tm)| same_row(tm, parent)) {
                Some((alias, _)) => alias.clone(),
                None if accesses.iter().any(|(_, tm)| same_subject(tm, parent)) => {
                    return Ok(Err(Decline(
                        "ref object map parent on a table the entity does not access",
                    )));
                }
                // A subject the entity's accesses provably never mint: no
                // row of the child can join a row of that entity.
                None if accesses.iter().all(|(_, tm)| subjects_disjoint(tm, parent)) => {
                    return Ok(Ok(None))
                }
                None => {
                    return Ok(Err(Decline(
                        "ref object map parent whose subject the entity may or may not mint",
                    )))
                }
            };
            for (child_col, parent_col) in conds {
                self.edges.push((
                    child_alias.clone(),
                    parent_alias.clone(),
                    Pred::ColEq {
                        left: ColRef::new(&child_alias, child_col),
                        right: ColRef::new(&parent_alias, parent_col),
                    },
                ));
            }
        }

        // Optional blocks.
        for (triples, filters) in &block.optionals {
            match self.lower_optional(triples, filters, &required_subjects) {
                Ok(()) => {}
                Err(d) => return Ok(Err(d)),
            }
        }

        // VALUES inside the block: a static key set.
        for (vars, rows) in &block.values {
            if let Err(d) = self.static_values(vars, rows) {
                return Ok(Err(d));
            }
        }

        // Sub-selects: each a derived table joined on its projected
        // variables' key columns.
        for sq in &block.subqueries {
            match self.lower_subquery(sq)? {
                Ok(Ok(())) => {}
                Ok(Err(Empty)) => return Ok(Ok(None)),
                Err(d) => return Ok(Err(d)),
            }
        }

        // Filters: exact ones into the plan; the rest stay in the engine,
        // with a widening predicate in the plan where one exists.
        for (v, e) in &binds {
            if let Some(x) = self.lower_expr(e) {
                self.bind_exprs.insert(*v, x);
            }
        }
        for f in &block.filters {
            match self.lower_filter(f) {
                Some(pred) => self.place_pred(pred),
                None => {
                    if let Some(pred) = self.lower_superset(f) {
                        self.place_pred(pred);
                    }
                    self.residuals.push(f.clone());
                }
            }
        }

        let root = match self.assemble() {
            Ok(r) => r,
            Err(d) => return Ok(Err(d)),
        };

        // A projection above reads only `projection`: the statement then
        // returns those variables' columns, keeping the columns the
        // in-memory join and the residual filters read. Under a DISTINCT
        // each distinct combination comes back once, which is distinct
        // terms only when no string column is among them or the database
        // keeps byte-distinct strings apart (a case-folding collation would
        // merge two IRIs or literals).
        let distinct_vars: Option<HashSet<VarId>> = projection.map(|proj| {
            let mut keep: HashSet<VarId> = proj.iter().copied().collect();
            keep.extend(child_vars.iter().copied());
            for f in &self.residuals {
                keep.extend(f.referenced_vars());
            }
            for (_, e) in &binds {
                keep.extend(e.referenced_vars());
            }
            keep
        });
        let projected: Vec<VarId> = self
            .var_order
            .iter()
            .copied()
            .filter(|v| distinct_vars.as_ref().is_none_or(|keep| keep.contains(v)))
            .collect();

        // Projection: every column a term source reads.
        let mut outputs: Vec<OutputCol> = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut per_alias: HashMap<String, Vec<String>> = HashMap::new();
        let mut var_columns: HashMap<VarId, Vec<ColRef>> = HashMap::new();
        for var in &projected {
            let src = &self.vars[var];
            let cols = self.term_columns(&src.term);
            for col in &cols {
                if seen.insert((col.alias.clone(), col.column.clone())) {
                    per_alias
                        .entry(col.alias.clone())
                        .or_default()
                        .push(col.column.clone());
                    let name = format!("c{}", outputs.len());
                    outputs.push(OutputCol::column(col.clone(), name));
                }
            }
            var_columns.insert(*var, cols);
        }
        if outputs.is_empty() {
            return Ok(Err(Decline("no columns to project")));
        }
        // A union's rows carry their branch tag, which is not part of the
        // solution: the engine keeps its own DISTINCT over them.
        let distinct_exact = (self.caps.string_distinct_is_binary
            || outputs.iter().all(|o| {
                o.expr
                    .col()
                    .is_some_and(|c| !matches!(self.field_type(c), None | Some(FieldType::String)))
            }))
            && !projected
                .iter()
                .any(|v| matches!(self.vars[v].term, TermSource::Union { .. }));
        let mut accesses: Vec<AccessInfo> = self
            .accesses
            .iter()
            .map(|a| AccessInfo {
                alias: a.alias.clone(),
                tm_iri: a.tm_iri.clone(),
                // A derived table's columns are read through its term
                // aliases below, never through the table itself; a union's
                // own column is its tag.
                columns: if self.derived.contains_key(&a.alias) {
                    per_alias.remove(&a.alias);
                    Vec::new()
                } else if let Some(u) = self.unions.get(&a.alias) {
                    per_alias
                        .remove(&a.alias)
                        .map(|cols| cols.into_iter().filter(|c| c == &u.tag).collect())
                        .unwrap_or_default()
                } else {
                    per_alias.remove(&a.alias).unwrap_or_default()
                },
                output_names: None,
            })
            .collect();
        // Term aliases over derived tables: mapping column names, fed by
        // the outputs that project the derived columns.
        let mut derived_aliases: Vec<&String> = self.derived_terms.keys().collect();
        derived_aliases.sort();
        for alias in derived_aliases {
            let dc = &self.derived_terms[alias];
            let mut columns = Vec::new();
            let mut names = Vec::new();
            for (mapping_col, out) in &dc.columns {
                let projected = outputs.iter().find(|o| {
                    o.expr
                        .col()
                        .is_some_and(|c| c.alias == dc.derived && &c.column == out)
                });
                if let Some(o) = projected {
                    columns.push(mapping_col.clone());
                    names.push(o.name.clone());
                }
            }
            if !columns.is_empty() {
                accesses.push(AccessInfo {
                    alias: alias.clone(),
                    tm_iri: dc.tm_iri.clone(),
                    columns,
                    output_names: Some(names),
                });
            }
        }

        let terms: Vec<(VarId, TermSource)> = projected
            .iter()
            .map(|v| (*v, self.vars[v].term.clone()))
            .collect();

        let mut seeds = Vec::new();
        let mut all_shared_seeded = true;
        for v in child_vars {
            if let Some(src) = self.vars.get(v) {
                match &src.key {
                    Some(shape) if !src.nullable && self.seedable(shape) => seeds.push(SeedSpec {
                        var: *v,
                        shape: shape.clone(),
                    }),
                    _ => all_shared_seeded = false,
                }
            }
        }

        let mut order_columns = HashMap::new();
        for (v, src) in &self.vars {
            if let (Some(KeyShape::Column { col, class }), false) = (&src.key, src.nullable) {
                let orderable = match class {
                    RdfClass::Numeric | RdfClass::Date | RdfClass::DateTime | RdfClass::Bool => {
                        true
                    }
                    RdfClass::Str => self.caps.string_order_is_codepoint,
                    _ => false,
                };
                if orderable
                    && self
                        .required_columns
                        .contains(&(col.alias.clone(), col.column.clone()))
                {
                    order_columns.insert(*v, (col.clone(), class.clone()));
                }
            }
        }

        // An expression orders exactly when every column it reads is
        // required (a NULL would order as the dialect's NULL, not as
        // unbound) and, for a string, where the dialect orders code points.
        let order_exprs: HashMap<VarId, Expr> = self
            .bind_exprs
            .iter()
            .filter(|(_, (e, kind))| {
                let mut cols = Vec::new();
                e.columns(&mut cols);
                (*kind == ExprKind::Num || self.caps.string_order_is_codepoint)
                    && cols.iter().all(|c| {
                        self.required_columns
                            .contains(&(c.alias.clone(), c.column.clone()))
                    })
            })
            .map(|(v, (e, _))| (*v, e.clone()))
            .collect();
        // Every column equality the renderer will emit must be one it accepts.
        // `same_class` is its own predicate, exported rather than restated so
        // the two cannot drift: on a mismatch the renderer raises `Unsupported`,
        // which reaches the caller as an `InvalidQuery` — but `open` has already
        // committed to the lane by then, so the query hard-fails where the
        // per-scan lane would have answered it. A `varchar` FK against a
        // `bigint` key is the common legacy shape that hits this.
        let mut eqs: Vec<(ColRef, ColRef)> = Vec::new();
        fluree_db_tabular::plan::collect_col_eqs(&root, &mut eqs);
        for (l, r) in &eqs {
            if let (Some(a), Some(b)) = (self.field_type(l), self.field_type(r)) {
                if !fluree_db_tabular::plan::same_class(a, b) {
                    return Ok(Err(Decline("join between two column classes")));
                }
            }
        }

        let limit_is_exact = self.residuals.is_empty() && all_shared_seeded;
        Ok(Ok(Some(Lowered {
            root,
            outputs,
            accesses,
            terms,
            residual_filters: std::mem::take(&mut self.residuals),
            binds,
            seeds,
            block_vars: self.var_order.clone(),
            vars: self.vars.clone(),
            var_columns,
            order_columns,
            order_exprs,
            limit_is_exact,
            distinct: distinct && distinct_vars.is_some() && distinct_exact,
        })))
    }

    /// An entity with several resolutions, as one derived table: each
    /// resolution is lowered on its own (its accesses, joins, policy and
    /// class predicates inside), the branches are `UNION ALL`ed under
    /// shared output columns plus a tag naming the branch, and every
    /// variable is bound once on those columns, decoded per branch. The
    /// branches must bind the same variables on columns of the same probed
    /// types; a key keeps its shape only where every branch agrees on it.
    fn lower_union_entity(
        &mut self,
        subject: &SubjRef,
        members: &[(String, Obj)],
        alternatives: Vec<Vec<Part<'a>>>,
        incoming: &[Incoming],
    ) -> Result<Lowering<std::result::Result<String, Empty>>> {
        let triples: Vec<Tp> = members
            .iter()
            .map(|(p, o)| Tp {
                s: subject.clone(),
                p: p.clone(),
                o: o.clone(),
            })
            .collect();
        // A foreign key certain to point at this entity joins its parent's
        // row: a resolution minting subjects provably apart from the
        // parent's can never meet it and is dropped; one on the parent's
        // subject over other tables takes the parent's row as a part of its
        // own, joined on the subject key; one the lane can neither join nor
        // rule out (one prefix, another skeleton) declines.
        let mapping = self.mapping;
        let mut alternatives = alternatives;
        for inc in incoming.iter().filter(|i| i.certain) {
            let Some(parent) = mapping.get(&inc.parent) else {
                return Ok(Err(Decline("ref object map parent missing")));
            };
            let mut kept = Vec::with_capacity(alternatives.len());
            for mut parts in alternatives {
                if parts.iter().any(|(tm, _)| same_row(tm, parent)) {
                    kept.push(parts);
                } else if parts.iter().any(|(tm, _)| same_subject(tm, parent)) {
                    parts.push((parent, Vec::new()));
                    kept.push(parts);
                } else if !parts.iter().all(|(tm, _)| subjects_disjoint(tm, parent)) {
                    return Ok(Err(Decline(
                        "ref object map into a union entity over templates the lane cannot relate",
                    )));
                }
            }
            alternatives = kept;
        }
        let mut branches: Vec<(Lowered, HashSet<(String, String)>)> = Vec::new();
        for alt in alternatives {
            let mut inner = self.nested();
            inner.forced = Some(alt);
            let block = Block {
                triples: triples.clone(),
                filters: Vec::new(),
                optionals: Vec::new(),
                values: Vec::new(),
                binds: Vec::new(),
                subqueries: Vec::new(),
            };
            let lowered = inner.lower(block, &[], None, false)?;
            let required = std::mem::take(&mut inner.required_columns);
            self.rejoin(inner);
            match lowered {
                Ok(Some(l)) => branches.push((l, required)),
                // A branch no row can satisfy contributes nothing.
                Ok(None) => {}
                Err(d) => return Ok(Err(d)),
            }
        }
        let row_of = |l: &Lowered, parent: &TriplesMap| -> Option<String> {
            l.accesses
                .iter()
                .find(|a| {
                    self.mapping
                        .get(&a.tm_iri)
                        .is_some_and(|tm| same_row(tm, parent))
                })
                .map(|a| a.alias.clone())
        };
        let Some((first, _)) = branches.first() else {
            return Ok(Ok(Err(Empty)));
        };
        let vars: Vec<VarId> = first.block_vars.clone();
        if branches.iter().any(|(l, _)| {
            l.block_vars.len() != vars.len() || l.block_vars.iter().any(|v| !vars.contains(v))
        }) {
            return Ok(Err(Decline("union branches bind different variables")));
        }
        // Output slots: each variable's columns in order, one slot per
        // column of the first branch (a column two variables share takes
        // one), the other branches' columns alongside.
        let mut slots: Vec<Vec<ColRef>> = Vec::new();
        for v in &vars {
            let n = first.var_columns.get(v).map_or(0, Vec::len);
            for j in 0..n {
                let per_branch: Option<Vec<ColRef>> = branches
                    .iter()
                    .map(|(l, _)| l.var_columns.get(v).and_then(|c| c.get(j)).cloned())
                    .collect();
                let Some(per_branch) = per_branch else {
                    return Ok(Err(Decline(
                        "union branches bind a variable on different column counts",
                    )));
                };
                match slots.iter().position(|s| s[0] == per_branch[0]) {
                    Some(k) if slots[k] != per_branch => {
                        return Ok(Err(Decline("union branches disagree on a shared column")));
                    }
                    Some(_) => {}
                    None => slots.push(per_branch),
                }
            }
        }
        // The parents' join columns, as slots, where every branch reads
        // the parent's row.
        let mut parent_slots: HashMap<String, Vec<(String, usize)>> = HashMap::new();
        for inc in incoming {
            let Some(parent) = self.mapping.get(&inc.parent) else {
                continue;
            };
            let rows: Option<Vec<String>> =
                branches.iter().map(|(l, _)| row_of(l, parent)).collect();
            let Some(rows) = rows else { continue };
            let mut cols_of = Vec::with_capacity(inc.cols.len());
            for col in &inc.cols {
                let per_branch: Vec<ColRef> = rows.iter().map(|a| ColRef::new(a, col)).collect();
                // A branch whose subject key is the parent's column shares
                // the key's slot; one that reads the parent's row through a
                // join gets a slot of its own.
                let k = match slots.iter().position(|s| *s == per_branch) {
                    Some(k) => k,
                    None => {
                        slots.push(per_branch);
                        slots.len() - 1
                    }
                };
                cols_of.push((col.clone(), k));
            }
            parent_slots.insert(inc.parent.clone(), cols_of);
        }
        let mut types: Vec<FieldType> = Vec::with_capacity(slots.len());
        for slot in &slots {
            let mut ty = None;
            for (i, col) in slot.iter().enumerate() {
                let Some(t) = self.branch_field_type(&branches[i].0, col) else {
                    return Ok(Err(Decline("union branch column of unknown type")));
                };
                if *ty.get_or_insert(t) != t {
                    return Ok(Err(Decline("union branch column types differ")));
                }
            }
            types.push(ty.expect("a slot has a column per branch"));
        }
        let alias = format!("u{}", self.unions.len());
        let tag = format!("c{}", slots.len());
        let slot_name = |k: usize| format!("c{k}");
        let slot_of = |col: &ColRef, branch: usize| slots.iter().position(|s| &s[branch] == col);

        let plans: Vec<RelPlan> = branches
            .iter()
            .enumerate()
            .map(|(i, (l, _))| RelPlan {
                root: l.root.clone(),
                output: slots
                    .iter()
                    .enumerate()
                    .map(|(k, s)| OutputCol::column(s[i].clone(), slot_name(k)))
                    .chain(std::iter::once(OutputCol {
                        expr: OutputExpr::Tag(i as i64),
                        name: tag.clone(),
                    }))
                    .collect(),
                group_by: Vec::new(),
                distinct: false,
                order_by: Vec::new(),
                limit: None,
                having: None,
            })
            .collect();
        let Some(first_tm) = first.accesses.first().map(|a| a.tm_iri.clone()) else {
            return Ok(Err(Decline("union branch without an access")));
        };
        self.accesses.push(AccessInfo {
            alias: alias.clone(),
            tm_iri: first_tm,
            columns: Vec::new(),
            output_names: None,
        });
        let mut col_types: HashMap<String, FieldType> = types
            .iter()
            .enumerate()
            .map(|(k, t)| (slot_name(k), *t))
            .collect();
        col_types.insert(tag.clone(), FieldType::Int64);
        self.derived_types.insert(alias.clone(), col_types);
        // A slot every branch requires is required of the union.
        for (k, slot) in slots.iter().enumerate() {
            if slot
                .iter()
                .enumerate()
                .all(|(i, c)| branches[i].1.contains(&(c.alias.clone(), c.column.clone())))
            {
                self.required_columns.insert((alias.clone(), slot_name(k)));
            }
        }
        // Each branch's inner aliases read the union's slots.
        for (i, (l, _)) in branches.iter().enumerate() {
            for (k, slot) in slots.iter().enumerate() {
                let col = &slot[i];
                let Some(tm_iri) = access_tm(l, &col.alias) else {
                    return Ok(Err(Decline("union branch column without an access")));
                };
                let entry = self
                    .derived_terms
                    .entry(format!("{alias}/{}", col.alias))
                    .or_insert_with(|| DerivedCols {
                        derived: alias.clone(),
                        tm_iri,
                        columns: Vec::new(),
                    });
                let pair = (col.column.clone(), slot_name(k));
                if !entry.columns.contains(&pair) {
                    entry.columns.push(pair);
                }
            }
        }
        let retarget = |term: &TermSource| -> TermSource {
            match term {
                TermSource::Subject { alias: a } => TermSource::Subject {
                    alias: format!("{alias}/{a}"),
                },
                TermSource::Object {
                    alias: a,
                    tm_iri,
                    pom,
                } => TermSource::Object {
                    alias: format!("{alias}/{a}"),
                    tm_iri: tm_iri.clone(),
                    pom: *pom,
                },
                other => other.clone(),
            }
        };
        for v in &vars {
            let sources: Vec<&VarSource> = branches.iter().map(|(l, _)| &l.vars[v]).collect();
            if sources.iter().any(|s| {
                matches!(
                    s.term,
                    TermSource::Aggregate { .. } | TermSource::Union { .. }
                )
            }) {
                return Ok(Err(Decline("union branch variable without a plain term")));
            }
            let term = TermSource::Union {
                alias: alias.clone(),
                tag: tag.clone(),
                branches: sources.iter().map(|s| retarget(&s.term)).collect(),
            };
            // The key's shape survives when every branch has the same one,
            // column for column over the slots.
            let remapped = |branch: usize, cols: &[ColRef]| -> Option<Vec<usize>> {
                cols.iter().map(|c| slot_of(c, branch)).collect()
            };
            let key = match &sources[0].key {
                Some(KeyShape::Template { template, cols, .. }) => {
                    let ks = remapped(0, cols);
                    let uniform = sources.iter().enumerate().skip(1).all(|(i, s)| {
                        matches!(&s.key, Some(KeyShape::Template { template: t, cols: c, .. })
                            if t == template && remapped(i, c) == ks)
                    });
                    match (uniform, ks) {
                        (true, Some(ks)) => Some(KeyShape::Template {
                            template: template.clone(),
                            cols: ks
                                .iter()
                                .map(|k| ColRef::new(&alias, slot_name(*k)))
                                .collect(),
                            types: ks.iter().map(|k| Some(types[*k])).collect(),
                        }),
                        _ => None,
                    }
                }
                Some(KeyShape::Column { col, class }) => {
                    let k = slot_of(col, 0);
                    let uniform = sources.iter().enumerate().skip(1).all(|(i, s)| {
                        matches!(&s.key, Some(KeyShape::Column { col: c, class: cl })
                            if cl == class && slot_of(c, i) == k)
                    });
                    match (uniform, k) {
                        (true, Some(k)) => Some(KeyShape::Column {
                            col: ColRef::new(&alias, slot_name(k)),
                            class: class.clone(),
                        }),
                        _ => None,
                    }
                }
                None => None,
            };
            let nullable = sources.iter().any(|s| s.nullable);
            if let Err(d) = self.bind_var(
                *v,
                VarSource {
                    term,
                    key,
                    nullable,
                },
            ) {
                return Ok(Err(d));
            }
        }
        let parent_cols = parent_slots
            .into_iter()
            .map(|(p, cols)| {
                (
                    p,
                    cols.into_iter().map(|(c, k)| (c, slot_name(k))).collect(),
                )
            })
            .collect();
        self.unions.insert(
            alias.clone(),
            UnionInfo {
                branches: plans,
                tag,
                parent_cols,
            },
        );
        Ok(Ok(Ok(alias)))
    }

    /// A sub-select as a derived table: its block is lowered on its own
    /// (sharing the alias counter and the policy), grouped when it groups,
    /// and each projected variable is bound on the derived table's columns:
    /// a key variable keeps its key shape over the derived outputs, so a
    /// shared variable joins on them; an aggregate decodes from its output.
    fn lower_subquery(
        &mut self,
        sq: &SubqueryPattern,
    ) -> Result<Lowering<std::result::Result<(), Empty>>> {
        let block = match parse_block(&sq.patterns, self.snapshot) {
            Ok(b) => b,
            Err(d) => return Ok(Err(d)),
        };
        let mut inner = self.nested();
        // The inner statement keeps what the projection and the grouping
        // read: the keys, and every aggregate's input.
        let mut keep: Vec<VarId> = sq.select.clone();
        if let Some(g) = &sq.grouping {
            keep.extend(g.group_by_vars());
            for a in g.aggregates() {
                match &a.function {
                    AggregateFn::Count(v)
                    | AggregateFn::CountDistinct(v)
                    | AggregateFn::Sum(v, _)
                    | AggregateFn::Avg(v, _)
                    | AggregateFn::Min(v)
                    | AggregateFn::Max(v) => keep.push(*v),
                    _ => {}
                }
            }
        }
        let lowered = inner.lower(block, &[], Some(&keep), sq.distinct)?;
        let lowered = match lowered {
            Ok(Some(l)) => l,
            Ok(None) => {
                self.rejoin(inner);
                return Ok(Ok(Err(Empty)));
            }
            Err(d) => {
                self.rejoin(inner);
                return Ok(Err(d));
            }
        };
        self.rejoin(inner);
        if !lowered.residual_filters.is_empty() {
            return Ok(Err(Decline("subquery with a residual filter")));
        }
        if !lowered.binds.is_empty() {
            return Ok(Err(Decline("BIND inside a subquery")));
        }
        if sq.distinct && !lowered.distinct {
            return Ok(Err(Decline("subquery DISTINCT not pushable")));
        }
        let alias = format!("d{}", self.derived.len());
        let Some(first_tm) = lowered.accesses.first().map(|a| a.tm_iri.clone()) else {
            return Ok(Err(Decline("subquery without an access")));
        };
        let topk = sq.limit.map(|k| {
            (
                sq.ordering.clone(),
                k.saturating_add(sq.offset.unwrap_or(0)),
            )
        });

        // The derived plan, and each projected variable's term and columns
        // over the inner aliases (an aggregate's under the empty alias).
        let (plan, sources): (RelPlan, Vec<(VarId, TermSource, Vec<ColRef>)>) = match &sq.grouping {
            Some(g) => {
                let group_by: Vec<VarId> = g.group_by_vars().collect();
                let aggregates: Vec<(VarId, AggregateFn)> = g
                    .aggregates()
                    .map(|a| (a.output_var, a.function.clone()))
                    .collect();
                let grouped = match group_plan(
                    &group_by,
                    &aggregates,
                    g.having(),
                    topk.as_ref(),
                    &lowered,
                    self.mapping,
                    self.caps,
                    self.schemas,
                ) {
                    Ok(g) => g,
                    Err(why) => return Ok(Err(Decline(why))),
                };
                if g.having().is_some() && grouped.plan.having.is_none() {
                    return Ok(Err(Decline("subquery HAVING not pushable")));
                }
                if topk.is_some() && grouped.plan.limit.is_none() {
                    return Ok(Err(Decline("subquery ORDER BY not pushable")));
                }
                let mut sources = Vec::new();
                let outs = group_by.iter().chain(aggregates.iter().map(|(v, _)| v));
                for (i, v) in outs.enumerate() {
                    if !sq.select.contains(v) {
                        continue;
                    }
                    let (term, cols) = match &grouped.decodes[i] {
                        Decode::Term { idx } => {
                            let (_, term) = &grouped.terms[*idx];
                            let cols = match term {
                                TermSource::Object { alias: a, .. }
                                | TermSource::Subject { alias: a } => {
                                    match grouped.extremes.iter().find(|e| &e.alias == a) {
                                        // An extreme: its one column, under
                                        // the output carrying it.
                                        Some(e) => vec![ColRef::new(a, &e.columns[0])],
                                        None => {
                                            lowered.var_columns.get(v).cloned().unwrap_or_default()
                                        }
                                    }
                                }
                                _ => Vec::new(),
                            };
                            (term.clone(), cols)
                        }
                        Decode::Count { name } => (
                            TermSource::Aggregate {
                                alias: format!("{alias}#{name}"),
                                kind: AggTerm::Count {
                                    column: name.clone(),
                                },
                            },
                            vec![ColRef::new("", name)],
                        ),
                        Decode::Numeric {
                            sum,
                            count,
                            kind,
                            avg,
                        } => (
                            TermSource::Aggregate {
                                alias: format!("{alias}#{sum}"),
                                kind: AggTerm::Numeric {
                                    sum: sum.clone(),
                                    count: count.clone(),
                                    kind: *kind,
                                    avg: *avg,
                                },
                            },
                            vec![ColRef::new("", sum), ColRef::new("", count)],
                        ),
                    };
                    sources.push((*v, term, cols));
                }
                (grouped.plan, sources)
            }
            None => {
                let mut order_by = Vec::new();
                let mut limit = None;
                if let Some((ordering, k)) = &topk {
                    let keys: Option<Vec<(OrderKey, bool)>> = ordering
                        .iter()
                        .map(|s| {
                            let key = match lowered.order_columns.get(&s.var) {
                                Some((col, _)) => OrderKey::Col(col.clone()),
                                None => OrderKey::Expr(lowered.order_exprs.get(&s.var)?.clone()),
                            };
                            Some((key, s.ascending()))
                        })
                        .collect();
                    match keys {
                        Some(keys) => {
                            order_by = keys;
                            limit = Some(*k as u64);
                        }
                        None => return Ok(Err(Decline("subquery ORDER BY not pushable"))),
                    }
                }
                let plan = RelPlan {
                    root: lowered.root.clone(),
                    output: lowered.outputs.clone(),
                    group_by: Vec::new(),
                    distinct: lowered.distinct,
                    order_by,
                    limit,
                    having: None,
                };
                let mut sources = Vec::new();
                for v in &sq.select {
                    let Some(src) = lowered.vars.get(v) else {
                        return Ok(Err(Decline("subquery projects an unbound variable")));
                    };
                    let cols = lowered.var_columns.get(v).cloned().unwrap_or_default();
                    sources.push((*v, src.term.clone(), cols));
                }
                (plan, sources)
            }
        };

        // The derived output holding an inner column (an aggregate output
        // is its own).
        let out_of = |col: &ColRef| -> Option<String> {
            if col.alias.is_empty() {
                return Some(col.column.clone());
            }
            plan.output
                .iter()
                .find(|o| o.expr.col().is_some_and(|c| c == col))
                .map(|o| o.name.clone())
        };
        let mut binds: Vec<(VarId, VarSource)> = Vec::new();
        for (v, term, cols) in sources {
            let (term_alias, tm_iri) = match &term {
                TermSource::Subject { alias: a } | TermSource::Object { alias: a, .. } => {
                    let Some(tm) = access_tm(&lowered, a) else {
                        return Ok(Err(Decline("subquery term without an access")));
                    };
                    (format!("{alias}/{a}"), tm)
                }
                TermSource::Aggregate { alias: a, .. } => (a.clone(), first_tm.clone()),
                TermSource::Constant(_) => {
                    binds.push((
                        v,
                        VarSource {
                            term,
                            key: None,
                            nullable: lowered.vars.get(&v).is_some_and(|s| s.nullable),
                        },
                    ));
                    continue;
                }
                TermSource::Union { .. } => {
                    return Ok(Err(Decline("subquery over a union entity")));
                }
            };
            let mut derived_cols = Vec::with_capacity(cols.len());
            for col in &cols {
                let Some(out) = out_of(col) else {
                    return Ok(Err(Decline("subquery column not projected")));
                };
                derived_cols.push((col.column.clone(), out));
            }
            let entry = self
                .derived_terms
                .entry(term_alias.clone())
                .or_insert_with(|| DerivedCols {
                    derived: alias.clone(),
                    tm_iri,
                    columns: Vec::new(),
                });
            for c in derived_cols {
                if !entry.columns.contains(&c) {
                    entry.columns.push(c);
                }
            }
            let term = match term {
                TermSource::Subject { .. } => TermSource::Subject { alias: term_alias },
                TermSource::Object { tm_iri, pom, .. } => TermSource::Object {
                    alias: term_alias,
                    tm_iri,
                    pom,
                },
                other => other,
            };
            // The key keeps its shape, over the derived outputs.
            let mut key = None;
            if let Some(k) = lowered.vars.get(&v).and_then(|s| s.key.clone()) {
                let remap = |c: &ColRef| out_of(c).map(|o| ColRef::new(&alias, &o));
                key = match k {
                    KeyShape::Template {
                        template,
                        cols: kc,
                        types,
                    } => {
                        let cols: Option<Vec<ColRef>> = kc.iter().map(remap).collect();
                        cols.map(|cols| KeyShape::Template {
                            template,
                            cols,
                            types,
                        })
                    }
                    KeyShape::Column { col, class } => {
                        remap(&col).map(|col| KeyShape::Column { col, class })
                    }
                };
            }
            // An OPTIONAL variable stays nullable across the derived table,
            // so the outer block declines to unify it as it would inside.
            binds.push((
                v,
                VarSource {
                    term,
                    key,
                    nullable: lowered.vars.get(&v).is_some_and(|s| s.nullable),
                },
            ));
        }
        self.accesses.push(AccessInfo {
            alias: alias.clone(),
            tm_iri: first_tm,
            columns: Vec::new(),
            output_names: None,
        });
        // The outputs' types, so a filter or seed over them is checked like
        // one over the columns they carry.
        let types: HashMap<String, FieldType> = plan
            .output
            .iter()
            .filter_map(|o| match &o.expr {
                OutputExpr::Col(c) | OutputExpr::Min(c) | OutputExpr::Max(c) => self
                    .branch_field_type(&lowered, c)
                    .map(|t| (o.name.clone(), t)),
                _ => None,
            })
            .collect();
        self.derived_types.insert(alias.clone(), types);
        self.derived.insert(alias, plan);
        for (v, src) in binds {
            if let Err(d) = self.bind_var(v, src) {
                return Ok(Err(d));
            }
        }
        Ok(Ok(Ok(())))
    }

    fn allowed(&mut self, tm: &TriplesMap, pred: &str) -> Result<Option<Verdict>> {
        match self.policy.as_mut() {
            Some(verdict) => verdict(tm, pred),
            None => Ok(Some(Verdict::Allow)),
        }
    }

    /// The column of `tm`'s derived `rdf:type` and the value that yields
    /// `class` from it: `None` when no value can (the IRI is outside the
    /// template), a decline when the template cannot be reversed or the
    /// column's type cannot be keyed.
    fn class_value(
        &self,
        alias: &str,
        tm: &TriplesMap,
        class: &str,
    ) -> Lowering<Option<(ColRef, Literal)>> {
        let Some(om) = derived_type_map(tm) else {
            return decline("column-derived rdf:type with a class constraint");
        };
        let (column, raw) = match om {
            ObjectMap::Column {
                column, term_type, ..
            } => {
                if *term_type != TermType::Iri {
                    return Ok(None);
                }
                (column.clone(), class.to_string())
            }
            ObjectMap::Template {
                template,
                term_type,
                ..
            } => {
                if *term_type != TermType::Iri {
                    return Ok(None);
                }
                let prefix = template.split('{').next().unwrap_or_default();
                if !class.starts_with(prefix) {
                    return Ok(None);
                }
                match reverse_subject_template(template, class).as_deref() {
                    Some([(column, value)]) => (column.clone(), value.clone()),
                    _ => return decline("rdf:type template cannot be reversed"),
                }
            }
            _ => return decline("column-derived rdf:type with a class constraint"),
        };
        let col = ColRef::new(alias, &column);
        match self.field_type(&col) {
            None | Some(FieldType::String) => {}
            Some(FieldType::Int32 | FieldType::Int64) => {
                if raw.parse::<i64>().is_err() {
                    return Ok(None);
                }
            }
            _ => return decline("rdf:type column type cannot be keyed"),
        }
        Ok(Some((col, Literal::TemplateKey(raw))))
    }

    /// A [`Verdict::ByClass`] as a predicate on the class column of `tm`'s
    /// access `alias`: rows of a denied class drop out (a row without a
    /// class keeps `otherwise`), or only rows of an allowed class stay.
    /// `Empty` when no row can pass.
    fn policy_pred(
        &self,
        alias: &str,
        tm: &TriplesMap,
        classes: &[(String, bool)],
        otherwise: bool,
    ) -> Lowering<std::result::Result<Option<Pred>, Empty>> {
        let mut col = None;
        let (mut allowed, mut denied) = (Vec::new(), Vec::new());
        for (class, ok) in classes {
            let Some((c, value)) = self.class_value(alias, tm, class)? else {
                continue; // no row of this map carries the class
            };
            col = Some(c);
            if *ok { &mut allowed } else { &mut denied }.push(value);
        }
        let Some(col) = col else {
            return Ok(if otherwise { Ok(None) } else { Err(Empty) });
        };
        Ok(Ok(Some(if otherwise {
            if denied.is_empty() {
                return Ok(Ok(None));
            }
            Pred::Or(vec![
                Pred::IsNull(col.clone()),
                Pred::Not(Box::new(Pred::Cmp {
                    col,
                    op: CmpOp::In,
                    value: Literal::Set(denied),
                })),
            ])
        } else {
            if allowed.is_empty() {
                return Ok(Err(Empty));
            }
            Pred::Cmp {
                col,
                op: CmpOp::In,
                value: Literal::Set(allowed),
            }
        })))
    }

    /// The alias the next [`Self::new_access`] mints.
    fn next_alias_name(&self) -> String {
        format!("t{}", self.next_alias)
    }

    /// A row-dependent verdict as a predicate on `tm`'s access `alias`;
    /// `Empty` when no row can pass.
    fn verdict_pred(
        &self,
        alias: &str,
        tm: &TriplesMap,
        verdict: &Verdict,
    ) -> Lowering<std::result::Result<Option<Pred>, Empty>> {
        match verdict {
            Verdict::Allow => Ok(Ok(None)),
            Verdict::Deny => Ok(Err(Empty)),
            Verdict::ByClass { classes, otherwise } => {
                self.policy_pred(alias, tm, classes, *otherwise)
            }
            Verdict::BySubject {
                subjects,
                otherwise,
            } => self.subject_pred(alias, tm, subjects, *otherwise),
        }
    }

    /// A [`Verdict::BySubject`] as a predicate on the subject key columns:
    /// each targeted subject reverses through the subject template (or is
    /// the column's value) into a key; a subject the map cannot mint has no
    /// row and is dropped. A constant subject decides the whole map.
    fn subject_pred(
        &self,
        alias: &str,
        tm: &TriplesMap,
        subjects: &[(String, bool)],
        otherwise: bool,
    ) -> Lowering<std::result::Result<Option<Pred>, Empty>> {
        let sm = &tm.subject_map;
        if let Some(constant) = &sm.constant {
            let verdict = subjects
                .iter()
                .find(|(s, _)| s == constant)
                .map_or(otherwise, |(_, ok)| *ok);
            return Ok(if verdict { Ok(None) } else { Err(Empty) });
        }
        let (mut allowed, mut denied) = (Vec::new(), Vec::new());
        for (subject, ok) in subjects {
            let key = if let Some(template) = &sm.template {
                let prefix = template.split('{').next().unwrap_or_default();
                if !subject.starts_with(prefix) {
                    continue;
                }
                let Some(keys) = reverse_subject_template(template, subject) else {
                    return decline("subject template cannot be reversed");
                };
                let mut parts = Vec::with_capacity(keys.len());
                let mut fits = true;
                for (column, raw) in keys {
                    let col = ColRef::new(alias, &column);
                    if !key_fits(self.field_type(&col), &raw) {
                        fits = false;
                        break;
                    }
                    parts.push(Pred::Cmp {
                        col,
                        op: CmpOp::Eq,
                        value: Literal::TemplateKey(raw),
                    });
                }
                if !fits {
                    continue;
                }
                match parts.len() {
                    1 => parts.pop().unwrap(),
                    _ => Pred::And(parts),
                }
            } else if let Some(column) = &sm.column {
                let col = ColRef::new(alias, column);
                if !matches!(self.field_type(&col), None | Some(FieldType::String)) {
                    return decline("subject column type cannot be keyed");
                }
                Pred::Cmp {
                    col,
                    op: CmpOp::Eq,
                    value: Literal::TemplateKey(subject.clone()),
                }
            } else {
                return decline("subject policy on a blank-node subject map");
            };
            if *ok { &mut allowed } else { &mut denied }.push(key);
        }
        Ok(Ok(Some(if otherwise {
            if denied.is_empty() {
                return Ok(Ok(None));
            }
            Pred::Not(Box::new(any_of(denied)))
        } else {
            if allowed.is_empty() {
                return Ok(Err(Empty));
            }
            any_of(allowed)
        })))
    }

    /// A lowerer for a nested statement, sharing the alias counter and the
    /// policy; `rejoin` hands both back.
    fn nested(&mut self) -> Lowerer<'a> {
        Lowerer {
            mapping: self.mapping,
            caps: self.caps,
            policy: self.policy.take(),
            schemas: self.schemas,
            next_alias: self.next_alias,
            accesses: Vec::new(),
            access_preds: HashMap::new(),
            edges: Vec::new(),
            left_joins: Vec::new(),
            vars: HashMap::new(),
            var_order: Vec::new(),
            residuals: Vec::new(),
            bind_exprs: HashMap::new(),
            snapshot: self.snapshot,
            derived: HashMap::new(),
            derived_terms: HashMap::new(),
            required_columns: HashSet::new(),
            static_keysets: Vec::new(),
            unions: HashMap::new(),
            forced: None,
            derived_types: HashMap::new(),
        }
    }

    fn rejoin(&mut self, mut inner: Lowerer<'a>) {
        self.policy = inner.policy.take();
        self.next_alias = inner.next_alias;
    }

    fn new_access(&mut self, tm: &TriplesMap) -> String {
        let alias = format!("t{}", self.next_alias);
        self.next_alias += 1;
        self.accesses.push(AccessInfo {
            alias: alias.clone(),
            tm_iri: tm.iri.clone(),
            columns: Vec::new(),
            output_names: None,
        });
        alias
    }

    fn source_of(&self, tm: &TriplesMap) -> RelSource {
        source_of_tm(tm)
    }

    /// The probed type of a column, when the provider supplied its
    /// relation's schema.
    fn field_type(&self, col: &ColRef) -> Option<FieldType> {
        if let Some(ty) = self
            .derived_types
            .get(&col.alias)
            .and_then(|cols| cols.get(&col.column))
        {
            return Some(*ty);
        }
        self.schemas
            .get(&self.rel_source(&col.alias))?
            .field_by_name(&col.column)
            .map(|f| f.field_type)
    }

    /// The probed type of a column of a nested lowering's table access.
    fn branch_field_type(&self, lowered: &Lowered, col: &ColRef) -> Option<FieldType> {
        let tm = self.mapping.get(&access_tm(lowered, &col.alias)?)?;
        self.schemas
            .get(&source_of_tm(tm))?
            .field_by_name(&col.column)
            .map(|f| f.field_type)
    }

    /// Whether an engine literal of `class` compares exactly against `col`:
    /// the probed column type must carry the class natively (a numeric
    /// literal against a text column the mapping *reads* as a number is the
    /// engine's comparison, not the database's). A dateTime literal is a UTC
    /// instant; a zoneless column is read as UTC, so it is exact there too,
    /// except where the database keeps it as text in an unknown format. An
    /// unprobed column is trusted; the renderer then types the literal.
    fn literal_exact(&self, col: &ColRef, class: &RdfClass) -> bool {
        use FieldType as F;
        let Some(ty) = self.field_type(col) else {
            return *class != RdfClass::DateTime;
        };
        match class {
            RdfClass::Numeric => matches!(
                ty,
                F::Int32 | F::Int64 | F::Float32 | F::Float64 | F::Decimal { .. }
            ),
            RdfClass::Str | RdfClass::LangStr(_) => ty == F::String,
            RdfClass::Bool => ty == F::Boolean,
            RdfClass::Date => ty == F::Date,
            RdfClass::DateTime => {
                ty == F::TimestampTz || (ty == F::Timestamp && !self.caps.timestamp_is_text)
            }
            RdfClass::Iri | RdfClass::Other => true,
        }
    }

    /// Whether outer values can be sent as a key set on this shape.
    fn seedable(&self, shape: &KeyShape) -> bool {
        match shape {
            KeyShape::Template { .. } => true,
            KeyShape::Column { col, class } => self.literal_exact(col, class),
        }
    }

    /// The one triples map providing every member, for an entity that
    /// must be a single access.
    fn resolve_tm(
        &mut self,
        members: &[(String, Obj)],
        only: Option<&'a TriplesMap>,
    ) -> Lowering<std::result::Result<&'a TriplesMap, Empty>> {
        match self.resolve_parts(members, only)? {
            Ok(parts) => match parts.as_slice() {
                [(tm, _)] => Ok(Ok(tm)),
                _ => decline("optional entity spans several triples maps"),
            },
            Err(Empty) => Ok(Err(Empty)),
        }
    }

    /// The one resolution of an entity that must have exactly one.
    fn resolve_parts(
        &mut self,
        members: &[(String, Obj)],
        only: Option<&'a TriplesMap>,
    ) -> Lowering<std::result::Result<Vec<Part<'a>>, Empty>> {
        let mut alternatives = self.resolve_alternatives(members, only)?;
        match alternatives.len() {
            0 => Ok(Err(Empty)),
            1 => Ok(Ok(alternatives.pop().expect("one resolution"))),
            _ => decline("optional entity spans several triples maps"),
        }
    }

    /// Every resolution of an entity's members into triples maps, each a
    /// list of parts (a map with the members it provides): one choice of a
    /// providing map per member, the chosen maps minting the same subject.
    /// A class member follows a chosen part declaring it, and is a part of
    /// its own (a type-only access on the shared subject) only when none
    /// does. Empty when no member has a provider.
    fn resolve_alternatives(
        &mut self,
        members: &[(String, Obj)],
        only: Option<&'a TriplesMap>,
    ) -> Lowering<Vec<Vec<Part<'a>>>> {
        if let Some(forced) = self.forced.take() {
            return Ok(vec![forced]);
        }
        let mut candidates: Vec<&'a TriplesMap> = match only {
            Some(tm) => vec![tm],
            None => self.mapping.triples_maps.values().collect(),
        };
        candidates.sort_by(|a, b| a.iri.cmp(&b.iri));
        let mut providers: Vec<Vec<&'a TriplesMap>> = Vec::with_capacity(members.len());
        for (pred, obj) in members {
            if pred == rdf::TYPE {
                let class = match obj {
                    Obj::Iri(c) => c,
                    _ => return decline("rdf:type object is not an IRI"),
                };
                // A map deriving `rdf:type` from a column provides the class
                // when the column can hold its value; a derivation the lane
                // cannot key (several maps, a multi-column template) could
                // provide any class, so it declines.
                let mut found: Vec<&'a TriplesMap> = Vec::new();
                for tm in &candidates {
                    let derived = !super::super::policy::derived_type_columns(tm).is_empty();
                    if static_classes(tm).iter().any(|c| c == class) {
                        found.push(tm);
                    } else if derived {
                        if derived_type_map(tm).is_none() {
                            return decline("column-derived rdf:type with a class constraint");
                        }
                        if self.class_value("t", tm, class)?.is_some() {
                            found.push(tm);
                        }
                    }
                }
                // Two maps declaring the class on the same rows mint the
                // same type triples.
                let mut kept: Vec<&'a TriplesMap> = Vec::with_capacity(found.len());
                for tm in found {
                    let alike = kept.iter().any(|k| {
                        k.same_source_row(tm)
                            && static_classes(k).iter().any(|c| c == class)
                            && static_classes(tm).iter().any(|c| c == class)
                    });
                    if !alike {
                        kept.push(tm);
                    }
                }
                providers.push(kept);
            } else {
                // Maps minting the predicate alike mint the same triples,
                // which the graph holds once.
                let mut kept: Vec<&'a TriplesMap> = Vec::new();
                for tm in candidates
                    .iter()
                    .copied()
                    .filter(|tm| pom_for(tm, pred).is_some())
                {
                    if !kept.iter().any(|k| k.mints_alike(tm, pred)) {
                        kept.push(tm);
                    }
                }
                providers.push(kept);
            }
        }
        if providers.iter().any(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let mut combos: usize = 1;
        for p in &providers {
            combos = combos.saturating_mul(p.len());
            if combos > MAX_MAP_ALTERNATIVES {
                return decline("too many triples-map combinations");
            }
        }
        let mut out: Vec<Vec<Part<'a>>> = Vec::new();
        let mut choice = vec![0usize; members.len()];
        loop {
            if let Some(parts) = parts_of(members, &providers, &choice)? {
                out.push(parts);
            }
            let mut i = 0;
            loop {
                if i == members.len() {
                    return Ok(out);
                }
                choice[i] += 1;
                if choice[i] < providers[i].len() {
                    break;
                }
                choice[i] = 0;
                i += 1;
            }
        }
    }

    /// The key shape of a template over `cols`, declined when a column's
    /// probed type has no template-key rendering.
    fn template_shape(&self, template: &str, cols: Vec<ColRef>) -> Lowering<KeyShape> {
        let types: Vec<Option<FieldType>> = cols.iter().map(|c| self.field_type(c)).collect();
        if types.iter().any(|t| {
            !matches!(
                t,
                None | Some(FieldType::String | FieldType::Int32 | FieldType::Int64)
            )
        }) {
            return decline("template over a column type that cannot be keyed");
        }
        Ok(KeyShape::Template {
            template: template.to_string(),
            cols,
            types,
        })
    }

    /// `Ok(Err(Empty))`: a constant subject that no row of the table can mint.
    fn bind_subject(
        &mut self,
        alias: &str,
        tm: &TriplesMap,
        subject: &SubjRef,
        nullable: bool,
    ) -> Lowering<std::result::Result<(), Empty>> {
        let sm = &tm.subject_map;
        if sm.term_type == TermType::BlankNode {
            return decline("blank-node subject");
        }
        let shape = if let Some(template) = &sm.template {
            let cols = sm
                .template_columns
                .iter()
                .map(|c| ColRef::new(alias, c))
                .collect::<Vec<_>>();
            if cols.is_empty() {
                return decline("subject template without placeholders");
            }
            Some(self.template_shape(template, cols)?)
        } else if let Some(col) = &sm.column {
            Some(KeyShape::Column {
                col: ColRef::new(alias, col),
                class: RdfClass::Iri,
            })
        } else if sm.constant.is_some() {
            None
        } else {
            return decline("generated blank-node subject");
        };
        if !nullable {
            for c in tm.subject_columns() {
                self.require(alias, c);
            }
        }
        match subject {
            SubjRef::Var(v) => self
                .bind_var(
                    *v,
                    VarSource {
                        term: TermSource::Subject {
                            alias: alias.to_string(),
                        },
                        key: shape,
                        nullable,
                    },
                )
                .map(Ok),
            SubjRef::Iri(iri) => {
                let Some(KeyShape::Template {
                    template,
                    cols,
                    types,
                }) = shape
                else {
                    return decline("constant subject on a non-template subject map");
                };
                let Some(keys) = reverse_subject_template(&template, iri) else {
                    return decline("constant subject does not reverse through the template");
                };
                for (col, raw) in keys {
                    let Some(idx) = cols.iter().position(|c| c.column == col) else {
                        return decline("reversed column not in template");
                    };
                    if !key_fits(types[idx], &raw) {
                        return Ok(Err(Empty));
                    }
                    self.access_preds
                        .entry(alias.to_string())
                        .or_default()
                        .push(Pred::Cmp {
                            col: cols[idx].clone(),
                            op: CmpOp::Eq,
                            value: Literal::TemplateKey(raw),
                        });
                }
                Ok(Ok(()))
            }
        }
    }

    fn require(&mut self, alias: &str, col: &str) {
        if self
            .required_columns
            .insert((alias.to_string(), col.to_string()))
        {
            self.access_preds
                .entry(alias.to_string())
                .or_default()
                .push(Pred::IsNotNull(ColRef::new(alias, col)));
        }
    }

    /// Give `var` a source, or unify it with the source it already has by
    /// emitting a column-equality edge.
    fn bind_var(&mut self, var: VarId, src: VarSource) -> Lowering<()> {
        let Some(existing) = self.vars.get(&var).cloned() else {
            self.vars.insert(var, src);
            self.var_order.push(var);
            return Ok(());
        };
        if src.nullable || existing.nullable {
            return decline("optional variable already bound");
        }
        let (Some(a), Some(b)) = (&existing.key, &src.key) else {
            return decline("repeated variable without joinable columns");
        };
        match (a, b) {
            (
                KeyShape::Template {
                    template: ta,
                    cols: ca,
                    types: tya,
                },
                KeyShape::Template {
                    template: tb,
                    cols: cb,
                    types: tyb,
                },
            ) => {
                // Two templates mint one IRI when their literal parts agree
                // and their placeholders carry equal values, whatever the
                // columns are called: `order/{id}` and `order/{order_ref}`
                // join on `id = order_ref`.
                if template_skeleton(ta) != template_skeleton(tb) || ca.len() != cb.len() {
                    return decline("repeated variable joins two different templates");
                }
                for (i, (l, r)) in ca.iter().zip(cb).enumerate() {
                    if let (Some(a), Some(b)) = (tya[i], tyb[i]) {
                        if !fluree_db_tabular::plan::same_class(a, b) {
                            return decline("join between two column classes");
                        }
                    }
                    self.push_edge(l.clone(), r.clone());
                }
                Ok(())
            }
            (KeyShape::Column { col: l, class: cl }, KeyShape::Column { col: r, class: cr }) => {
                if cl != cr {
                    return decline("repeated variable joins two value classes");
                }
                // Matching RDF classes are not a matching comparison. The class
                // is what the mapping *reads* the column as, so two text columns
                // both mapped `xsd:decimal` agree here and render a string `=`,
                // where '99.5' and '99.50' are different strings and the same
                // number — a silently wrong answer rather than an error. Require
                // both sides to carry the class natively, the rule
                // `literal_exact` already applies to a literal comparison.
                // `Str` is exempt: a string join on text columns is exact.
                if *cl != RdfClass::Str && !(self.literal_exact(l, cl) && self.literal_exact(r, cr))
                {
                    return decline("repeated variable joins columns that do not carry the class");
                }
                self.push_edge(l.clone(), r.clone());
                Ok(())
            }
            _ => decline("repeated variable joins a template with a column"),
        }
    }

    fn push_edge(&mut self, l: ColRef, r: ColRef) {
        let (la, ra) = (l.alias.clone(), r.alias.clone());
        self.edges.push((la, ra, Pred::ColEq { left: l, right: r }));
    }

    /// Bind one `(predicate, object)` member on `alias`. Returns a deferred
    /// FK edge `(child alias, join conditions, parent tm, target var)` when
    /// the object is the subject of another required entity.
    fn bind_member(
        &mut self,
        alias: &str,
        tm: &'a TriplesMap,
        pred: &str,
        obj: &Obj,
        required_subjects: &HashSet<VarId>,
        nullable: bool,
    ) -> Lowering<std::result::Result<Option<RefEdge>, Empty>> {
        let Some(pom_idx) = pom_for(tm, pred) else {
            return decline("member predicate not on the entity's triples map");
        };
        if tm
            .predicate_object_maps
            .iter()
            .filter(|p| matches!(&p.predicate_map, PredicateMap::Constant(c) if c == pred))
            .count()
            > 1
        {
            return decline("duplicate predicate on one triples map");
        }
        let om = &tm.predicate_object_maps[pom_idx].object_map;
        let term = TermSource::Object {
            alias: alias.to_string(),
            tm_iri: tm.iri.clone(),
            pom: pom_idx,
        };
        match om {
            ObjectMap::Column {
                column,
                datatype,
                language,
                term_type,
            } => {
                let class = class_of(datatype.as_deref(), language.as_deref(), *term_type);
                let col = ColRef::new(alias, column);
                if !nullable {
                    self.require(alias, column);
                }
                match obj {
                    Obj::Var(v) => self
                        .bind_var(
                            *v,
                            VarSource {
                                term,
                                key: Some(KeyShape::Column { col, class }),
                                nullable,
                            },
                        )
                        .map(|()| Ok(None)),
                    Obj::Iri(iri) => {
                        if class != RdfClass::Iri {
                            return Ok(Err(Empty));
                        }
                        self.access_preds
                            .entry(alias.to_string())
                            .or_default()
                            .push(Pred::Cmp {
                                col,
                                op: CmpOp::Eq,
                                value: Literal::Str(iri.clone()),
                            });
                        Ok(Ok(None))
                    }
                    Obj::Lit(val, dtc) => {
                        let Some((lit, lclass)) = literal_of(val, dtc.as_ref()) else {
                            return decline("constant object literal not pushable");
                        };
                        if !self.exact_eq(&col, &class, &lclass) {
                            return decline("constant object type differs from the column");
                        }
                        self.access_preds
                            .entry(alias.to_string())
                            .or_default()
                            .push(Pred::Cmp {
                                col,
                                op: CmpOp::Eq,
                                value: lit,
                            });
                        Ok(Ok(None))
                    }
                }
            }
            ObjectMap::Template {
                template,
                columns,
                term_type,
                ..
            } => {
                if !nullable {
                    for c in columns {
                        self.require(alias, c);
                    }
                }
                let cols: Vec<ColRef> = columns.iter().map(|c| ColRef::new(alias, c)).collect();
                match obj {
                    Obj::Var(v) => {
                        let key = if *term_type == TermType::Iri && !cols.is_empty() {
                            Some(self.template_shape(template, cols)?)
                        } else {
                            None
                        };
                        self.bind_var(
                            *v,
                            VarSource {
                                term,
                                key,
                                nullable,
                            },
                        )
                        .map(|()| Ok(None))
                    }
                    Obj::Iri(iri) if *term_type == TermType::Iri => {
                        let KeyShape::Template { types, .. } =
                            self.template_shape(template, cols)?
                        else {
                            unreachable!("template_shape builds a template");
                        };
                        let Some(keys) = reverse_subject_template(template, iri) else {
                            return decline("constant object does not reverse");
                        };
                        for (col, raw) in keys {
                            let ty = columns
                                .iter()
                                .position(|c| c.as_str() == col)
                                .and_then(|i| types[i]);
                            if !key_fits(ty, &raw) {
                                return Ok(Err(Empty));
                            }
                            self.access_preds
                                .entry(alias.to_string())
                                .or_default()
                                .push(Pred::Cmp {
                                    col: ColRef::new(alias, col),
                                    op: CmpOp::Eq,
                                    value: Literal::TemplateKey(raw),
                                });
                        }
                        Ok(Ok(None))
                    }
                    _ => decline("constant object on a template object map"),
                }
            }
            ObjectMap::Constant { value } => {
                let rdf_term = match value {
                    ConstantValue::Iri(iri) => RdfTerm::iri(iri.clone()),
                    ConstantValue::Literal {
                        value,
                        datatype,
                        language,
                    } => RdfTerm::Literal {
                        value: value.clone(),
                        dtc: match (language, datatype) {
                            (Some(l), _) => {
                                Some(UnresolvedDatatypeConstraint::LangTag(l.as_str().into()))
                            }
                            (None, Some(d)) => {
                                Some(UnresolvedDatatypeConstraint::Explicit(d.as_str().into()))
                            }
                            (None, None) => None,
                        },
                    },
                };
                match obj {
                    Obj::Var(v) => self
                        .bind_var(
                            *v,
                            VarSource {
                                term: TermSource::Constant(rdf_term),
                                key: None,
                                nullable,
                            },
                        )
                        .map(|()| Ok(None)),
                    Obj::Iri(iri) => Ok(match value {
                        ConstantValue::Iri(c) if c == iri => Ok(None),
                        _ => Err(Empty),
                    }),
                    Obj::Lit(..) => decline("constant literal object on a constant map"),
                }
            }
            ObjectMap::RefObjectMap(rom) => {
                let Some(parent) = self.mapping.get(&rom.parent_triples_map) else {
                    return decline("ref object map parent missing");
                };
                if rom.join_conditions.is_empty() {
                    return decline("ref object map without join conditions");
                }
                let conds: Vec<(String, String)> = rom
                    .join_conditions
                    .iter()
                    .map(|jc| (jc.child_column.clone(), jc.parent_column.clone()))
                    .collect();
                if !nullable {
                    for (c, _) in &conds {
                        self.require(alias, c);
                    }
                }
                match obj {
                    Obj::Var(v) if required_subjects.contains(v) => {
                        Ok(Ok(Some((alias.to_string(), conds, parent.iri.clone(), *v))))
                    }
                    Obj::Var(v) => {
                        // The parent is not an entity of the block: access it
                        // for its subject, joined on the FK.
                        let palias = self.new_access(parent);
                        let on: Vec<Pred> = conds
                            .iter()
                            .map(|(c, p)| Pred::ColEq {
                                left: ColRef::new(alias, c),
                                right: ColRef::new(&palias, p),
                            })
                            .collect();
                        if nullable {
                            self.left_joins.push((palias.clone(), on));
                        } else {
                            for p in on {
                                self.edges.push((alias.to_string(), palias.clone(), p));
                            }
                        }
                        self.bind_subject(&palias, parent, &SubjRef::Var(*v), nullable)
                            .map(|r| r.map(|()| None))
                    }
                    Obj::Iri(iri) => {
                        let palias = self.new_access(parent);
                        for (c, p) in &conds {
                            self.edges.push((
                                alias.to_string(),
                                palias.clone(),
                                Pred::ColEq {
                                    left: ColRef::new(alias, c),
                                    right: ColRef::new(&palias, p),
                                },
                            ));
                        }
                        self.bind_subject(&palias, parent, &SubjRef::Iri(iri.clone()), false)
                            .map(|r| r.map(|()| None))
                    }
                    Obj::Lit(..) => Ok(Err(Empty)),
                }
            }
        }
    }

    /// `OPTIONAL { … }`: members on an already-accessed entity become
    /// nullable columns of that access (no join at all); a new entity hanging
    /// off a required one by a foreign key becomes one `LEFT JOIN`.
    fn lower_optional(
        &mut self,
        triples: &[Tp],
        filters: &[Expression],
        required_subjects: &HashSet<VarId>,
    ) -> Lowering<()> {
        if !self.caps.left_join {
            return decline("provider cannot left join");
        }
        let entities = group_entities(triples);
        if entities.len() != 1 {
            return decline("optional block with several entities");
        }
        let (subject, members) = &entities[0];
        let SubjRef::Var(sv) = subject else {
            return decline("optional block with a constant subject");
        };
        if let Some(existing) = self.vars.get(sv).cloned() {
            // Same entity: fold as nullable columns of its access.
            let TermSource::Subject { alias } = existing.term else {
                return decline("optional on a variable that is not an entity subject");
            };
            if existing.nullable {
                return decline("optional chained on an optional entity");
            }
            let tm_iri = self
                .accesses
                .iter()
                .find(|a| a.alias == alias)
                .map(|a| a.tm_iri.clone())
                .unwrap_or_default();
            let Some(tm) = self.mapping.get(&tm_iri) else {
                return decline("optional entity triples map missing");
            };
            if !filters.is_empty() {
                return decline("filter inside a folded optional");
            }
            // SPARQL's LeftJoin binds an OPTIONAL group as a unit: if any triple
            // of the group is absent for a row, every variable the group binds
            // is unbound. Folding members into nullable columns of the required
            // access makes each column independently NULL, so an order with
            // `placed` set and `shipped` absent bound ?p and left ?s unbound
            // where SPARQL unbinds both. One member is the case where
            // per-column and per-group agree — including a policy-hidden one
            // below, which then leaves the group's only variable unbound, as it
            // should. Several members need a self LEFT JOIN; until the lowering
            // can build one, they belong to the per-scan lane.
            if members.len() != 1 {
                return decline("several members in a folded optional");
            }
            for (pred, obj) in members {
                if pred == rdf::TYPE {
                    return decline("rdf:type inside an optional");
                }
                // A constant object's equality lands in `access_preds` of the
                // *required* access — only the new-entity path below relocates
                // its predicates into the ON clause — so the OPTIONAL would
                // filter the rows it is supposed to leave alone:
                // `OPTIONAL { ?o ex:placed "2024-01-05"^^xsd:date }` returned
                // one row where SPARQL returns every order.
                if !matches!(obj, Obj::Var(_)) {
                    return decline("constant object in a folded optional");
                }
                if pom_for(tm, pred).is_none() {
                    return decline("optional member not on the entity's triples map");
                }
                match self
                    .allowed(tm, pred)
                    .map_err(|_| Decline("policy error"))?
                {
                    Some(Verdict::Allow) => {}
                    Some(Verdict::Deny) => continue, // hidden: the variable stays unbound
                    // Hidden per row: the column would have to be nulled
                    // by a predicate, which no statement here expresses.
                    Some(Verdict::ByClass { .. } | Verdict::BySubject { .. }) | None => {
                        return decline("policy not static")
                    }
                }
                match self.bind_member(&alias, tm, pred, obj, required_subjects, true) {
                    Ok(Ok(None)) => {}
                    Ok(Ok(Some(_))) => return decline("optional ref to a required entity"),
                    Ok(Err(Empty)) => return decline("optional member cannot match"),
                    Err(d) => return Err(d),
                }
            }
            return Ok(());
        }
        // A new entity: it must hang off a required entity through one of
        // its own foreign keys, or by a shared literal column.
        let tm = match self.resolve_tm(members, None)? {
            Ok(tm) => tm,
            Err(Empty) => return Ok(()), // nothing to join: its vars stay unbound
        };
        // A row-dependent verdict joins as a condition: a row it hides
        // simply does not join, and the variables stay unbound. Its
        // predicates name the access this entity is about to get.
        let alias = self.next_alias_name();
        let mut policy_on: Vec<Pred> = Vec::new();
        for (pred, _) in members {
            match self
                .allowed(tm, pred)
                .map_err(|_| Decline("policy error"))?
            {
                Some(Verdict::Allow) => {}
                Some(Verdict::Deny) => return Ok(()),
                Some(verdict) => match self.verdict_pred(&alias, tm, &verdict)? {
                    Ok(Some(p)) => policy_on.push(p),
                    Ok(None) => {}
                    Err(Empty) => return Ok(()),
                },
                None => return decline("policy not static"),
            }
        }
        debug_assert_eq!(alias, self.next_alias_name());
        let alias = self.new_access(tm);
        let mut on: Vec<Pred> = policy_on;
        for c in tm.subject_columns() {
            on.push(Pred::IsNotNull(ColRef::new(&alias, c)));
        }
        let edges_before = self.edges.len();
        let mut connected = false;
        let mut fk_edges: Vec<Pred> = Vec::new();
        if self.bind_subject(&alias, tm, subject, true)?.is_err() {
            return decline("optional member cannot match");
        }
        for (pred, obj) in members {
            if pred == rdf::TYPE {
                continue;
            }
            match self.bind_member(&alias, tm, pred, obj, required_subjects, true) {
                Ok(Ok(None)) => {}
                Ok(Ok(Some((child_alias, conds, parent_tm, var)))) => {
                    // The FK points at a required entity: join on its access.
                    let Some(VarSource {
                        term:
                            TermSource::Subject {
                                alias: parent_alias,
                            },
                        ..
                    }) = self.vars.get(&var).cloned()
                    else {
                        return decline("optional FK target is not an entity subject");
                    };
                    let target_tm = self
                        .accesses
                        .iter()
                        .find(|a| a.alias == parent_alias)
                        .map(|a| a.tm_iri.clone())
                        .unwrap_or_default();
                    if target_tm != parent_tm {
                        return decline("optional FK target has another triples map");
                    }
                    for (c, p) in conds {
                        fk_edges.push(Pred::ColEq {
                            left: ColRef::new(&child_alias, c),
                            right: ColRef::new(&parent_alias, p),
                        });
                    }
                    connected = true;
                }
                Ok(Err(Empty)) => return decline("optional member cannot match"),
                Err(d) => return Err(d),
            }
            // Required columns of the optional side belong in ON.
            if let Some(pom) = pom_for(tm, pred) {
                for c in object_columns(&tm.predicate_object_maps[pom].object_map) {
                    on.push(Pred::IsNotNull(ColRef::new(&alias, c)));
                }
            }
        }
        // Edges created while binding (variable unification with required
        // columns) are the join condition, not inner-join edges.
        let new_edges: Vec<(String, String, Pred)> = self.edges.drain(edges_before..).collect();
        if new_edges.is_empty() && !connected {
            return decline("optional entity not connected to the required part");
        }
        on.extend(fk_edges);
        for (a, b, pred) in new_edges {
            if a != alias && b != alias {
                return decline("optional edge between two other entities");
            }
            on.push(pred);
        }
        for f in filters {
            match self.lower_filter(f) {
                Some(p) => on.push(p),
                None => return decline("filter inside an optional is not exact"),
            }
        }
        if let Some(local) = self.access_preds.remove(&alias) {
            on.extend(local);
        }
        self.left_joins.push((alias, on));
        Ok(())
    }

    fn static_values(&mut self, vars: &[VarId], rows: &[Vec<Binding>]) -> Lowering<()> {
        let mut columns: Vec<(String, Option<FieldType>)> = Vec::new();
        let mut targets: Vec<(KeyShape, usize)> = Vec::new();
        let kalias = format!("v{}", self.static_keysets.len());
        for (i, v) in vars.iter().enumerate() {
            let Some(src) = self.vars.get(v) else {
                return decline("VALUES variable not bound by the block");
            };
            let Some(shape) = src.key.clone() else {
                return decline("VALUES variable has no key columns");
            };
            if src.nullable {
                return decline("VALUES over an optional variable");
            }
            if !self.seedable(&shape) {
                return decline("VALUES on a column its literals cannot seed exactly");
            }
            let n = match &shape {
                KeyShape::Template { cols, .. } => cols.len(),
                KeyShape::Column { .. } => 1,
            };
            for _ in 0..n {
                columns.push((format!("k{}", columns.len()), None));
            }
            targets.push((shape, i));
        }
        let mut out_rows: Vec<Vec<Literal>> = Vec::with_capacity(rows.len());
        let mut on: Vec<Pred> = Vec::new();
        let mut col_idx = 0;
        for (shape, _) in &targets {
            match shape {
                KeyShape::Template { cols, .. } => {
                    for c in cols {
                        on.push(Pred::ColEq {
                            left: ColRef::new(&kalias, &columns[col_idx].0),
                            right: c.clone(),
                        });
                        col_idx += 1;
                    }
                }
                KeyShape::Column { col, .. } => {
                    on.push(Pred::ColEq {
                        left: ColRef::new(&kalias, &columns[col_idx].0),
                        right: col.clone(),
                    });
                    col_idx += 1;
                }
            }
        }
        for row in rows {
            let mut lits = Vec::with_capacity(columns.len());
            for (shape, i) in &targets {
                let b = &row[*i];
                let Some(vals) = super::terms::seed_values(b, shape, None) else {
                    return decline("VALUES row not seedable");
                };
                lits.extend(vals);
            }
            out_rows.push(lits);
        }
        if out_rows.is_empty() {
            return decline("empty VALUES");
        }
        // A key set the provider would not take in one statement stays with
        // the engine: the block's VALUES is not chunked the way outer
        // bindings are.
        let bytes: usize = out_rows.iter().flatten().map(|l| literal_len(l) + 4).sum();
        if out_rows.len() > self.caps.keyset_max_rows || bytes > self.caps.statement_max_bytes / 2 {
            return decline("VALUES too large to push");
        }
        self.static_keysets.push((
            KeySet {
                alias: kalias,
                columns,
                rows: out_rows,
            },
            on,
        ));
        Ok(())
    }

    /// An exact plan predicate for `expr`, or `None` to keep it in the engine.
    fn lower_filter(&self, expr: &Expression) -> Option<Pred> {
        let Expression::Call { func, args } = expr else {
            return None;
        };
        match func {
            Function::And | Function::Or => {
                let parts: Option<Vec<Pred>> = args.iter().map(|a| self.lower_filter(a)).collect();
                let parts = parts?;
                if parts.is_empty() {
                    return None;
                }
                Some(if matches!(func, Function::And) {
                    Pred::And(parts)
                } else {
                    Pred::Or(parts)
                })
            }
            Function::Not => {
                if args.len() != 1 {
                    return None;
                }
                Some(Pred::Not(Box::new(self.lower_filter(&args[0])?)))
            }
            Function::In | Function::NotIn => {
                let Some(Expression::Var(v)) = args.first() else {
                    return None;
                };
                let (col, class) = self.literal_column(*v)?;
                let mut members = Vec::with_capacity(args.len() - 1);
                for a in &args[1..] {
                    let Expression::Const(c) = a else {
                        return None;
                    };
                    let (lit, lclass) = literal_of(c, None)?;
                    if !self.exact_eq(&col, &class, &lclass) {
                        return None;
                    }
                    members.push(lit);
                }
                if members.is_empty() || members.len() > self.caps.keyset_max_rows {
                    return None;
                }
                let pred = Pred::Cmp {
                    col,
                    op: CmpOp::In,
                    value: Literal::Set(members),
                };
                Some(if matches!(func, Function::NotIn) {
                    Pred::Not(Box::new(pred))
                } else {
                    pred
                })
            }
            Function::Eq
            | Function::Ne
            | Function::Lt
            | Function::Le
            | Function::Gt
            | Function::Ge => {
                if args.len() != 2 {
                    return None;
                }
                let ordering = !matches!(func, Function::Eq | Function::Ne);
                match (&args[0], &args[1]) {
                    (Expression::Var(a), Expression::Var(b)) => {
                        if !matches!(func, Function::Eq) {
                            return None;
                        }
                        let (l, cl) = self.literal_column(*a)?;
                        let (r, cr) = self.literal_column(*b)?;
                        (cl == cr && self.exact_eq(&l, &cl, &cr) && self.literal_exact(&r, &cr))
                            .then_some(Pred::ColEq { left: l, right: r })
                    }
                    (Expression::Var(v), Expression::Const(c))
                    | (Expression::Const(c), Expression::Var(v)) => {
                        let reversed = matches!(&args[0], Expression::Const(_));
                        let op = match (func, reversed) {
                            (Function::Eq, _) => CmpOp::Eq,
                            (Function::Ne, _) => CmpOp::NotEq,
                            (Function::Lt, false) | (Function::Gt, true) => CmpOp::Lt,
                            (Function::Le, false) | (Function::Ge, true) => CmpOp::LtEq,
                            (Function::Gt, false) | (Function::Lt, true) => CmpOp::Gt,
                            (Function::Ge, false) | (Function::Le, true) => CmpOp::GtEq,
                            _ => return None,
                        };
                        let (lit, lclass) = literal_of(c, None)?;
                        if let Some((expr, kind)) = self.bind_exprs.get(v) {
                            return self.expr_cmp(expr.clone(), *kind, op, lit, lclass, ordering);
                        }
                        let (col, class) = self.literal_column(*v)?;
                        if !self.exact_eq(&col, &class, &lclass) {
                            return None;
                        }
                        if ordering
                            && class == RdfClass::Str
                            && !self.caps.string_order_is_codepoint
                        {
                            return None;
                        }
                        Some(Pred::Cmp {
                            col,
                            op,
                            value: lit,
                        })
                    }
                    // A computed value against a literal, the expression
                    // written out in the filter itself.
                    (e, Expression::Const(c)) | (Expression::Const(c), e) => {
                        let reversed = matches!(&args[0], Expression::Const(_));
                        let op = match (func, reversed) {
                            (Function::Eq, _) => CmpOp::Eq,
                            (Function::Ne, _) => CmpOp::NotEq,
                            (Function::Lt, false) | (Function::Gt, true) => CmpOp::Lt,
                            (Function::Le, false) | (Function::Ge, true) => CmpOp::LtEq,
                            (Function::Gt, false) | (Function::Lt, true) => CmpOp::Gt,
                            (Function::Ge, false) | (Function::Le, true) => CmpOp::GtEq,
                            _ => return None,
                        };
                        let (lit, lclass) = literal_of(c, None)?;
                        let (expr, kind) = self.lower_expr(e)?;
                        self.expr_cmp(expr, kind, op, lit, lclass, ordering)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// `expr op lit` when the database evaluates it as the engine does: a
    /// numeric value promotes the same way (no division), a string compares
    /// (and orders) by code point.
    fn expr_cmp(
        &self,
        expr: Expr,
        kind: ExprKind,
        op: CmpOp,
        lit: Literal,
        lclass: RdfClass,
        ordering: bool,
    ) -> Option<Pred> {
        let exact = match (kind, &lclass) {
            (ExprKind::Num, RdfClass::Numeric) => true,
            (ExprKind::Str, RdfClass::Str) => {
                self.caps.string_eq_is_binary && (!ordering || self.caps.string_order_is_codepoint)
            }
            _ => false,
        };
        exact.then_some(Pred::ExprCmp {
            expr,
            op,
            value: lit,
        })
    }

    /// A predicate every row `expr` keeps satisfies, for an `expr` the plan
    /// cannot evaluate exactly: the statement filters with it and the engine
    /// still runs `expr` over what comes back. `None` when nothing can be
    /// said. A `LIKE` widens because a collation can only match more (case
    /// or accent folding), never fewer, of the strings a byte prefix does.
    fn lower_superset(&self, expr: &Expression) -> Option<Pred> {
        if let Some(pred) = self.lower_filter(expr) {
            return Some(pred);
        }
        let Expression::Call { func, args } = expr else {
            return None;
        };
        match func {
            // Dropping a conjunct widens; every disjunct must widen.
            Function::And => {
                Pred::and(args.iter().filter_map(|a| self.lower_superset(a)).collect())
            }
            Function::Or => {
                let parts: Option<Vec<Pred>> =
                    args.iter().map(|a| self.lower_superset(a)).collect();
                let parts = parts?;
                (!parts.is_empty()).then_some(Pred::Or(parts))
            }
            Function::StrStarts | Function::StrEnds | Function::Contains => {
                let (col, needle) = self.string_column_and_literal(args)?;
                let needle = like_escape(&needle);
                let pattern = match func {
                    Function::StrStarts => format!("{needle}%"),
                    Function::StrEnds => format!("%{needle}"),
                    _ => format!("%{needle}%"),
                };
                Some(Pred::Like { col, pattern })
            }
            Function::Eq => self
                .case_fold_superset(args)
                .or_else(|| self.text_timestamp_day_bounds(func, args)),
            Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                self.text_timestamp_day_bounds(func, args)
            }
            // `^literal` with no flags is a case-sensitive prefix.
            Function::Regex => {
                if args.len() != 2 {
                    return None;
                }
                let (col, pattern) = self.string_column_and_literal(args)?;
                let prefix = pattern.strip_prefix('^')?;
                if prefix.is_empty() || prefix.chars().any(|c| r".^$*+?()[]{}|\".contains(c)) {
                    return None;
                }
                Some(Pred::Like {
                    col,
                    pattern: format!("{}%", like_escape(prefix)),
                })
            }
            _ => None,
        }
    }

    /// An `xsd:dateTime` literal compared with a timestamp the database
    /// keeps as text (SQLite), in whatever format its writer used: the day
    /// of the literal bounds the comparison, `'2024-01-10'` sorting below
    /// every timestamp of that day with either time separator, and the
    /// engine applies the exact one. The column is read as UTC, like every
    /// zoneless timestamp, so the literal's own day is the bound.
    fn text_timestamp_day_bounds(&self, func: &Function, args: &[Expression]) -> Option<Pred> {
        if !self.caps.timestamp_is_text {
            return None;
        }
        let (var, lit, reversed) = match args {
            [Expression::Var(v), Expression::Const(c)] => (*v, c, false),
            [Expression::Const(c), Expression::Var(v)] => (*v, c, true),
            _ => return None,
        };
        let (col, RdfClass::DateTime) = self.literal_column(var)? else {
            return None;
        };
        if self.field_type(&col) != Some(FieldType::Timestamp) {
            return None;
        }
        let (Literal::Timestamp { micros, tz: true }, _) = literal_of(lit, None)? else {
            return None;
        };
        let day = micros.div_euclid(MICROS_PER_DAY);
        let bound = |day: i64| Some(Literal::Date(i32::try_from(day).ok()?));
        let (lo, hi) = (bound(day)?, bound(day + 1)?);
        let cmp = |op, value| Pred::Cmp {
            col: col.clone(),
            op,
            value,
        };
        let below = matches!(func, Function::Lt | Function::Le) != reversed;
        Some(match func {
            Function::Eq => Pred::And(vec![cmp(CmpOp::GtEq, lo), cmp(CmpOp::Lt, hi)]),
            _ if below => cmp(CmpOp::Lt, hi),
            _ => cmp(CmpOp::GtEq, lo),
        })
    }

    /// `LCASE(?v) = "lit"` / `UCASE(?v) = "lit"` over a plain string
    /// column: every dialect's case mapping agrees with SPARQL's on
    /// printable ASCII, so the folded comparison is exact for such values
    /// and any other value is sent back for the engine to decide.
    fn case_fold_superset(&self, args: &[Expression]) -> Option<Pred> {
        let (call, c) = match args {
            [call @ Expression::Call { .. }, Expression::Const(c)]
            | [Expression::Const(c), call @ Expression::Call { .. }] => (call, c),
            _ => return None,
        };
        let Expression::Call { func, args } = call else {
            return None;
        };
        let [Expression::Var(v)] = args.as_slice() else {
            return None;
        };
        let fold = match func {
            Function::Lcase => Expr::Lower,
            Function::Ucase => Expr::Upper,
            _ => return None,
        };
        let (col, RdfClass::Str) = self.literal_column(*v)? else {
            return None;
        };
        if !self.literal_exact(&col, &RdfClass::Str) || !self.caps.string_eq_is_binary {
            return None;
        }
        let (lit @ Literal::Str(_), RdfClass::Str) = literal_of(c, None)? else {
            return None;
        };
        Some(Pred::Or(vec![
            Pred::ExprCmp {
                expr: fold(Box::new(Expr::Col(col.clone()))),
                op: CmpOp::Eq,
                value: lit,
            },
            Pred::NonAscii(col),
        ]))
    }

    /// `(?v, "literal")` where `?v` is a physical string column.
    fn string_column_and_literal(&self, args: &[Expression]) -> Option<(ColRef, String)> {
        let [Expression::Var(v), Expression::Const(c)] = args else {
            return None;
        };
        let (col, class) = self.literal_column(*v)?;
        if !matches!(class, RdfClass::Str | RdfClass::LangStr(_))
            || !self.literal_exact(&col, &class)
        {
            return None;
        }
        match literal_of(c, None)? {
            (Literal::Str(s), RdfClass::Str) => Some((col, s)),
            _ => None,
        }
    }

    /// `expr` as a statement expression, when it is `+`, `-` and `*` over
    /// numeric columns the database holds natively, numeric constants and
    /// other such `BIND`s. Division is left out: SPARQL divides integers
    /// into a decimal, SQL into an integer. Overflow is the dialect's.
    /// An expression the statement computes as the engine would, with the
    /// kind of value it yields: arithmetic (no division) over exact numeric
    /// columns and numeric constants; `CONCAT`, `STRLEN`, `SUBSTR` (from a
    /// positive constant position) and `STR` over plain string columns and
    /// constants. `None` where a step's SQL definition could differ.
    fn lower_expr(&self, expr: &Expression) -> Option<(Expr, ExprKind)> {
        match expr {
            Expression::Var(v) => {
                if let Some(e) = self.bind_exprs.get(v) {
                    return Some(e.clone());
                }
                let (col, class) = self.literal_column(*v)?;
                if !self.literal_exact(&col, &class) {
                    return None;
                }
                match class {
                    RdfClass::Numeric if self.field_type(&col) != Some(FieldType::Float32) => {
                        Some((Expr::Col(col), ExprKind::Num))
                    }
                    RdfClass::Str => Some((Expr::Col(col), ExprKind::Str)),
                    _ => None,
                }
            }
            // An integer constant is exact in every dialect; a decimal or
            // double one is typed per dialect (SQLite computes in floating
            // point, Postgres reads `1E-1` as exact), so an expression over
            // one stays in the engine.
            Expression::Const(c) => match literal_of(c, None)? {
                (lit @ Literal::Int(_), _) => Some((Expr::Lit(lit), ExprKind::Num)),
                (lit @ Literal::Str(_), RdfClass::Str) => Some((Expr::Lit(lit), ExprKind::Str)),
                _ => None,
            },
            Expression::Call { func, args } => {
                let num = |e: &Expression| -> Option<Expr> {
                    match self.lower_expr(e)? {
                        (x, ExprKind::Num) => Some(x),
                        _ => None,
                    }
                };
                let str = |e: &Expression| -> Option<Expr> {
                    match self.lower_expr(e)? {
                        (x, ExprKind::Str) => Some(x),
                        _ => None,
                    }
                };
                let count = |e: &Expression| -> Option<u64> {
                    match e {
                        Expression::Const(c) => match literal_of(c, None)? {
                            (Literal::Int(n), _) => u64::try_from(n).ok(),
                            _ => None,
                        },
                        _ => None,
                    }
                };
                match func {
                    Function::Add | Function::Sub | Function::Mul => {
                        let op = match func {
                            Function::Add => ArithOp::Add,
                            Function::Sub => ArithOp::Sub,
                            _ => ArithOp::Mul,
                        };
                        let [l, r] = args.as_slice() else {
                            return None;
                        };
                        Some((
                            Expr::Arith {
                                op,
                                left: Box::new(num(l)?),
                                right: Box::new(num(r)?),
                            },
                            ExprKind::Num,
                        ))
                    }
                    Function::Concat if !args.is_empty() => {
                        let parts: Option<Vec<Expr>> = args.iter().map(str).collect();
                        Some((Expr::Concat(parts?), ExprKind::Str))
                    }
                    Function::Strlen => {
                        let [e] = args.as_slice() else {
                            return None;
                        };
                        Some((Expr::Strlen(Box::new(str(e)?)), ExprKind::Num))
                    }
                    Function::Str => {
                        let [e] = args.as_slice() else {
                            return None;
                        };
                        Some((str(e)?, ExprKind::Str))
                    }
                    // Positions before the start or a negative length have
                    // dialect-specific readings; SPARQL's are exact only
                    // from position 1.
                    Function::Substr => {
                        let (e, start, len) = match args.as_slice() {
                            [e, s] => (e, count(s)?, None),
                            [e, s, n] => (e, count(s)?, Some(count(n)?)),
                            _ => return None,
                        };
                        if start < 1 {
                            return None;
                        }
                        Some((
                            Expr::Substr {
                                expr: Box::new(str(e)?),
                                start,
                                len,
                            },
                            ExprKind::Str,
                        ))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// The plain literal column behind `var`, if it has one.
    fn literal_column(&self, var: VarId) -> Option<(ColRef, RdfClass)> {
        match self.vars.get(&var)?.key.as_ref()? {
            KeyShape::Column { col, class } if *class != RdfClass::Iri => {
                Some((col.clone(), class.clone()))
            }
            _ => None,
        }
    }

    /// Whether `col`, of `class`, compared with a literal of `lit` class
    /// evaluates identically in SPARQL and in the database.
    fn exact_eq(&self, col: &ColRef, class: &RdfClass, lit: &RdfClass) -> bool {
        let same = match (class, lit) {
            (RdfClass::Str, RdfClass::Str) => self.caps.string_eq_is_binary,
            (RdfClass::LangStr(a), RdfClass::LangStr(b)) => {
                a.eq_ignore_ascii_case(b) && self.caps.string_eq_is_binary
            }
            (RdfClass::Numeric, RdfClass::Numeric)
            | (RdfClass::Bool, RdfClass::Bool)
            | (RdfClass::Date, RdfClass::Date)
            | (RdfClass::DateTime, RdfClass::DateTime) => true,
            _ => false,
        };
        same && self.literal_exact(col, class)
    }

    fn place_pred(&mut self, pred: Pred) {
        let mut aliases: HashSet<String> = HashSet::new();
        collect_aliases(&pred, &mut aliases);
        let left_joined: HashSet<&str> = self.left_joins.iter().map(|(a, _)| a.as_str()).collect();
        if aliases.len() == 1 && !aliases.iter().any(|a| left_joined.contains(a.as_str())) {
            let alias = aliases.into_iter().next().unwrap();
            self.access_preds.entry(alias).or_default().push(pred);
        } else {
            // Multi-relation, or touching a left-joined side: a top-level
            // WHERE, which is what the comparison semantics call for (a NULL
            // from the optional side fails the comparison in both worlds).
            self.access_preds
                .entry(String::new())
                .or_default()
                .push(pred);
        }
    }

    /// Join the accesses into a left-deep tree: required accesses connected
    /// by their edges, then static key sets, then the left joins.
    fn assemble(&mut self) -> Lowering<RelNode> {
        let left_joined: HashSet<String> = self.left_joins.iter().map(|(a, _)| a.clone()).collect();
        let required: Vec<String> = self
            .accesses
            .iter()
            .map(|a| a.alias.clone())
            .filter(|a| !left_joined.contains(a))
            .collect();
        let global_preds = self.access_preds.remove("").unwrap_or_default();
        let Some(first) = required.first().cloned() else {
            return decline("no required access");
        };
        let mut placed: HashSet<String> = HashSet::new();
        let mut tree = self.leaf(&first);
        placed.insert(first);
        let mut edges: Vec<(String, String, Pred)> = std::mem::take(&mut self.edges);
        loop {
            if placed.len() == required.len() {
                break;
            }
            let next = required.iter().find(|a| {
                !placed.contains(*a)
                    && edges.iter().any(|(x, y, _)| {
                        (x == *a && placed.contains(y)) || (y == *a && placed.contains(x))
                    })
            });
            let Some(next) = next.cloned() else {
                return decline("disconnected entities (cartesian product)");
            };
            let mut on = Vec::new();
            edges.retain(|(x, y, p)| {
                let joins_next =
                    (x == &next && placed.contains(y)) || (y == &next && placed.contains(x));
                if joins_next {
                    on.push(p.clone());
                }
                !joins_next
            });
            tree = RelNode::Join {
                left: Box::new(tree),
                right: Box::new(self.leaf(&next)),
                on: Pred::and(on).expect("at least one edge"),
            };
            placed.insert(next);
        }
        // Any edge left is between two placed accesses: a plain predicate.
        let mut extra: Vec<Pred> = edges.into_iter().map(|(_, _, p)| p).collect();
        extra.extend(global_preds);
        for (ks, on) in std::mem::take(&mut self.static_keysets) {
            tree = RelNode::Join {
                left: Box::new(tree),
                right: Box::new(RelNode::KeySet(ks)),
                on: Pred::and(on).expect("key set joins on its columns"),
            };
        }
        for (alias, on) in std::mem::take(&mut self.left_joins) {
            let source = self.rel_source(&alias);
            tree = RelNode::LeftJoin {
                left: Box::new(tree),
                right: Box::new(RelNode::Access {
                    alias: alias.clone(),
                    source,
                }),
                on: Pred::and(on).expect("left join has its null guards"),
            };
        }
        if let Some(p) = Pred::and(extra) {
            tree = RelNode::Filter {
                input: Box::new(tree),
                pred: p,
            };
        }
        Ok(tree)
    }

    fn rel_source(&self, alias: &str) -> RelSource {
        let tm_iri = self
            .accesses
            .iter()
            .find(|a| a.alias == alias)
            .map(|a| a.tm_iri.as_str())
            .unwrap_or_default();
        match self.mapping.get(tm_iri) {
            Some(tm) => self.source_of(tm),
            None => RelSource::Table(String::new()),
        }
    }

    fn leaf(&mut self, alias: &str) -> RelNode {
        let node = if let Some(plan) = self.derived.get(alias) {
            RelNode::Derived {
                alias: alias.to_string(),
                plan: Box::new(plan.clone()),
            }
        } else if let Some(u) = self.unions.get(alias) {
            RelNode::UnionAll {
                alias: alias.to_string(),
                branches: u.branches.clone(),
            }
        } else {
            RelNode::Access {
                alias: alias.to_string(),
                source: self.rel_source(alias),
            }
        };
        match self.access_preds.remove(alias).and_then(Pred::and) {
            Some(pred) => RelNode::Filter {
                input: Box::new(node),
                pred,
            },
            None => node,
        }
    }

    fn term_columns(&self, term: &TermSource) -> Vec<ColRef> {
        let alias = match term {
            TermSource::Subject { alias }
            | TermSource::Object { alias, .. }
            | TermSource::Aggregate { alias, .. } => Some(alias),
            TermSource::Constant(_) => None,
            // The tag, then whatever any branch's term reads.
            TermSource::Union {
                alias,
                tag,
                branches,
            } => {
                let mut cols = vec![ColRef::new(alias, tag)];
                for c in branches.iter().flat_map(|b| self.term_columns(b)) {
                    if !cols.contains(&c) {
                        cols.push(c);
                    }
                }
                return cols;
            }
        };
        if let Some(dc) = alias.and_then(|a| self.derived_terms.get(a)) {
            return dc
                .columns
                .iter()
                .map(|(_, out)| ColRef::new(&dc.derived, out))
                .collect();
        }
        match term {
            TermSource::Constant(_) | TermSource::Aggregate { .. } | TermSource::Union { .. } => {
                Vec::new()
            }
            TermSource::Subject { alias } => {
                let tm = self.tm_of(alias);
                tm.map(|tm| {
                    tm.subject_columns()
                        .into_iter()
                        .map(|c| ColRef::new(alias, c))
                        .collect()
                })
                .unwrap_or_default()
            }
            TermSource::Object { alias, tm_iri, pom } => self
                .mapping
                .get(tm_iri)
                .map(|tm| {
                    object_columns(&tm.predicate_object_maps[*pom].object_map)
                        .into_iter()
                        .map(|c| ColRef::new(alias, c))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    fn tm_of(&self, alias: &str) -> Option<&'a TriplesMap> {
        let iri = self
            .accesses
            .iter()
            .find(|a| a.alias == alias)
            .map(|a| a.tm_iri.as_str())?;
        self.mapping.get(iri)
    }
}

/// Rendered size of a key-set literal, roughly: what the chunking budgets.
pub(crate) fn literal_len(l: &Literal) -> usize {
    match l {
        Literal::Str(s) | Literal::TemplateKey(s) => s.len() + 2,
        Literal::Set(items) => items.iter().map(literal_len).sum(),
        _ => 24,
    }
}

/// A triples map and the entity members (by index) it provides.
type Part<'a> = (&'a TriplesMap, Vec<usize>);

/// The parts of one choice of providers (`choice[i]` indexes
/// `providers[i]`): `None` when the choice is not a resolution of its own
/// (a class choice a chosen part already covers, or parts minting
/// different subjects, which no row can carry together).
fn parts_of<'a>(
    members: &[(String, Obj)],
    providers: &[Vec<&'a TriplesMap>],
    choice: &[usize],
) -> Lowering<Option<Vec<Part<'a>>>> {
    let mut parts: Vec<Part<'a>> = Vec::new();
    for (i, (pred, _)) in members.iter().enumerate() {
        if pred == rdf::TYPE {
            continue;
        }
        let tm = providers[i][choice[i]];
        match parts.iter_mut().find(|(t, _)| t.iri == tm.iri) {
            Some((_, idxs)) => idxs.push(i),
            None => parts.push((tm, vec![i])),
        }
    }
    for (i, (pred, _)) in members.iter().enumerate() {
        if pred != rdf::TYPE {
            continue;
        }
        let chosen = providers[i][choice[i]];
        let declaring = parts
            .iter_mut()
            .find(|(t, _)| providers[i].iter().any(|c| c.iri == t.iri));
        match declaring {
            Some((t, idxs)) => {
                if t.iri != chosen.iri {
                    return Ok(None);
                }
                idxs.push(i);
            }
            None => parts.push((chosen, vec![i])),
        }
    }
    if parts.iter().any(|(tm, _)| {
        let sm = &tm.subject_map;
        sm.template.is_none() && sm.column.is_none()
    }) {
        return decline("entity spans triples maps with a constant subject");
    }
    // The lane joins parts by key columns, never by rendered IRI, so every
    // part must mint the same subject space. Parts whose subjects provably
    // never meet leave no row carrying every member; parts the lane cannot
    // relate either way are left to the engine.
    if parts
        .iter()
        .any(|(tm, _)| subjects_disjoint(parts[0].0, tm))
    {
        return Ok(None);
    }
    if parts.iter().any(|(tm, _)| !same_subject(parts[0].0, tm)) {
        return decline("entity spans subject templates the lane cannot relate");
    }
    for (_, idxs) in &mut parts {
        idxs.sort_unstable();
    }
    Ok(Some(parts))
}

/// A template with its placeholders anonymized: two templates of one
/// skeleton mint one IRI exactly when their placeholder values agree.
fn template_skeleton(template: &str) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                out.push_str("{}");
                for c in chars.by_ref() {
                    if c == '}' {
                        break;
                    }
                }
            }
            '\\' => {
                out.push(c);
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// Whether two triples maps mint subjects in one space, so a subject
/// shared between them is a join on their key columns: templates of one
/// skeleton, or two column-valued subjects.
fn same_subject(a: &TriplesMap, b: &TriplesMap) -> bool {
    let (sa, sb) = (&a.subject_map, &b.subject_map);
    match (&sa.template, &sb.template) {
        (Some(ta), Some(tb)) => {
            template_skeleton(ta) == template_skeleton(tb)
                && sa.template_columns.len() == sb.template_columns.len()
        }
        (None, None) => sa.column.is_some() && sb.column.is_some(),
        _ => false,
    }
}

/// Whether no row of `a` can mint a subject of `b`: both are templates and
/// neither's literal prefix (the text before the first placeholder) begins
/// the other's. A column-valued subject can be anything, so it is never
/// provably apart from another.
fn subjects_disjoint(a: &TriplesMap, b: &TriplesMap) -> bool {
    let (Some(ta), Some(tb)) = (&a.subject_map.template, &b.subject_map.template) else {
        return false;
    };
    let prefix = |t: &str| t.split('{').next().unwrap_or("").to_string();
    let (pa, pb) = (prefix(ta), prefix(tb));
    !pa.starts_with(&pb) && !pb.starts_with(&pa)
}

/// Whether two triples maps read the same row: one relation, one subject
/// map, column for column.
fn same_row(a: &TriplesMap, b: &TriplesMap) -> bool {
    let (sa, sb) = (&a.subject_map, &b.subject_map);
    (sa.template.is_some() || sa.column.is_some())
        && sa.template == sb.template
        && sa.template_columns == sb.template_columns
        && sa.column == sb.column
        && source_of_tm(a) == source_of_tm(b)
}

fn collect_aliases(pred: &Pred, out: &mut HashSet<String>) {
    match pred {
        Pred::Cmp { col, .. }
        | Pred::IsNull(col)
        | Pred::IsNotNull(col)
        | Pred::Like { col, .. }
        | Pred::NonAscii(col) => {
            out.insert(col.alias.clone());
        }
        Pred::ColEq { left, right } => {
            out.insert(left.alias.clone());
            out.insert(right.alias.clone());
        }
        Pred::ExprCmp { expr, .. } => {
            let mut cols = Vec::new();
            expr.columns(&mut cols);
            out.extend(cols.into_iter().map(|c| c.alias.clone()));
        }
        Pred::And(ps) | Pred::Or(ps) => ps.iter().for_each(|p| collect_aliases(p, out)),
        Pred::Not(p) => collect_aliases(p, out),
        Pred::OutputCmp { .. } => {}
    }
}

fn pom_for(tm: &TriplesMap, pred: &str) -> Option<usize> {
    tm.predicate_object_maps
        .iter()
        .position(|p| matches!(&p.predicate_map, PredicateMap::Constant(c) if c == pred))
}

/// The columns an object map reads from its row.
pub(crate) fn object_columns(om: &ObjectMap) -> Vec<&str> {
    match om {
        ObjectMap::Column { column, .. } => vec![column.as_str()],
        ObjectMap::Template { columns, .. } => columns.iter().map(String::as_str).collect(),
        ObjectMap::Constant { .. } => Vec::new(),
        ObjectMap::RefObjectMap(rom) => rom.child_columns(),
    }
}

fn group_entities(triples: &[Tp]) -> Vec<(SubjRef, Vec<(String, Obj)>)> {
    let mut out: Vec<(SubjRef, Vec<(String, Obj)>)> = Vec::new();
    for tp in triples {
        let same = |s: &SubjRef| match (s, &tp.s) {
            (SubjRef::Var(a), SubjRef::Var(b)) => a == b,
            (SubjRef::Iri(a), SubjRef::Iri(b)) => a == b,
            _ => false,
        };
        match out.iter_mut().find(|(s, _)| same(s)) {
            Some((_, members)) => members.push((tp.p.clone(), tp.o.clone())),
            None => out.push((tp.s.clone(), vec![(tp.p.clone(), tp.o.clone())])),
        }
    }
    out
}

fn parse_block(patterns: &[Pattern], snapshot: &LedgerSnapshot) -> Lowering<Block> {
    let mut block = Block {
        triples: Vec::new(),
        filters: Vec::new(),
        optionals: Vec::new(),
        values: Vec::new(),
        binds: Vec::new(),
        subqueries: Vec::new(),
    };
    for p in patterns {
        match p {
            Pattern::Triple(tp) => block.triples.push(parse_triple(tp, snapshot)?),
            Pattern::Subquery(sq) => block.subqueries.push(sq.clone()),
            Pattern::Filter(e) => block.filters.push(e.clone()),
            Pattern::Optional(inner) => {
                let mut triples = Vec::new();
                let mut filters = Vec::new();
                for q in inner {
                    match q {
                        Pattern::Triple(tp) => triples.push(parse_triple(tp, snapshot)?),
                        Pattern::Filter(e) => filters.push(e.clone()),
                        _ => return decline("unsupported pattern inside OPTIONAL"),
                    }
                }
                if triples.is_empty() {
                    return decline("OPTIONAL without triples");
                }
                block.optionals.push((triples, filters));
            }
            Pattern::Values { vars, rows } => block.values.push((vars.clone(), rows.clone())),
            Pattern::Bind { var, expr } => block.binds.push((*var, expr.clone())),
            _ => return decline("unsupported pattern in block"),
        }
    }
    if block.triples.is_empty() {
        return decline("block without required triples");
    }
    Ok(block)
}

fn parse_triple(tp: &TriplePattern, snapshot: &LedgerSnapshot) -> Lowering<Tp> {
    let s = match &tp.s {
        Ref::Var(v) => SubjRef::Var(*v),
        Ref::Iri(iri) => SubjRef::Iri(iri.to_string()),
        Ref::Sid(sid) => match snapshot.decode_sid(sid) {
            Some(iri) => SubjRef::Iri(iri),
            None => return decline("undecodable subject sid"),
        },
    };
    let p = match &tp.p {
        Ref::Var(_) => return decline("variable predicate"),
        Ref::Iri(iri) => iri.to_string(),
        Ref::Sid(sid) => match snapshot.decode_sid(sid) {
            Some(iri) => iri,
            None => return decline("undecodable predicate sid"),
        },
    };
    let o = match &tp.o {
        Term::Var(v) => Obj::Var(*v),
        Term::Iri(iri) => Obj::Iri(iri.to_string()),
        Term::Sid(sid) => match snapshot.decode_sid(sid) {
            Some(iri) => Obj::Iri(iri),
            None => return decline("undecodable object sid"),
        },
        Term::Value(FlakeValue::Ref(sid)) => match snapshot.decode_sid(sid) {
            Some(iri) => Obj::Iri(iri),
            None => return decline("undecodable object ref"),
        },
        Term::Value(v) => Obj::Lit(v.clone(), tp.dtc.clone()),
    };
    if p == rdf::TYPE && !matches!(o, Obj::Iri(_)) {
        return decline("rdf:type with a non-IRI object");
    }
    Ok(Tp { s, p, o })
}

/// The RDF class of an object map's values.
pub(crate) fn class_of(
    datatype: Option<&str>,
    language: Option<&str>,
    term_type: TermType,
) -> RdfClass {
    if term_type == TermType::Iri {
        return RdfClass::Iri;
    }
    if let Some(l) = language {
        return RdfClass::LangStr(l.to_string());
    }
    match datatype {
        None => RdfClass::Str,
        Some(dt) => class_of_datatype(dt),
    }
}

fn class_of_datatype(dt: &str) -> RdfClass {
    match dt {
        xsd::STRING => RdfClass::Str,
        xsd::INTEGER
        | xsd::LONG
        | xsd::INT
        | xsd::SHORT
        | xsd::BYTE
        | xsd::UNSIGNED_LONG
        | xsd::UNSIGNED_INT
        | xsd::UNSIGNED_SHORT
        | xsd::UNSIGNED_BYTE
        | xsd::NON_NEGATIVE_INTEGER
        | xsd::POSITIVE_INTEGER
        | xsd::NON_POSITIVE_INTEGER
        | xsd::NEGATIVE_INTEGER
        | xsd::DECIMAL
        | xsd::DOUBLE
        | xsd::FLOAT => RdfClass::Numeric,
        xsd::BOOLEAN => RdfClass::Bool,
        xsd::DATE => RdfClass::Date,
        xsd::DATE_TIME => RdfClass::DateTime,
        _ => RdfClass::Other,
    }
}

/// A query literal as a plan literal and its RDF class, or `None` when it
/// has no exact SQL form.
pub(crate) fn literal_of(
    value: &FlakeValue,
    dtc: Option<&DatatypeConstraint>,
) -> Option<(Literal, RdfClass)> {
    if let Some(DatatypeConstraint::LangTag(lang)) = dtc {
        return match value {
            FlakeValue::String(s) => {
                Some((Literal::Str(s.clone()), RdfClass::LangStr(lang.to_string())))
            }
            _ => None,
        };
    }
    match value {
        FlakeValue::String(s) => Some((Literal::Str(s.clone()), RdfClass::Str)),
        FlakeValue::Long(n) => Some((Literal::Int(*n), RdfClass::Numeric)),
        FlakeValue::Boolean(b) => Some((Literal::Bool(*b), RdfClass::Bool)),
        FlakeValue::Date(d) => Some((Literal::Date(d.days_since_epoch()), RdfClass::Date)),
        FlakeValue::Double(f) if f.is_finite() => Some((Literal::Double(*f), RdfClass::Numeric)),
        FlakeValue::Decimal(d) => decimal_literal(d).map(|l| (l, RdfClass::Numeric)),
        FlakeValue::BigInt(n) => {
            let s = n.to_string();
            let bd = bigdecimal::BigDecimal::parse_bytes(s.as_bytes(), 10)?;
            decimal_literal(&bd).map(|l| (l, RdfClass::Numeric))
        }
        // A UTC instant; exact only against a zoned column, which the
        // lowering checks against the probed schema (`literal_exact`).
        FlakeValue::DateTime(dt) => Some((
            Literal::Timestamp {
                micros: dt.epoch_micros(),
                tz: true,
            },
            RdfClass::DateTime,
        )),
        _ => None,
    }
}

fn decimal_literal(bd: &bigdecimal::BigDecimal) -> Option<Literal> {
    use num_traits::ToPrimitive;
    let (unscaled_bi, scale) = bd.normalized().as_bigint_and_exponent();
    Some(Literal::Decimal {
        unscaled: unscaled_bi.to_i128()?,
        scale: i8::try_from(scale).ok()?,
    })
}

/// The triples map behind an inner term alias, an extreme's (`t1.c3`)
/// included.
fn access_tm(lowered: &Lowered, alias: &str) -> Option<String> {
    let base = alias.split('.').next().unwrap_or(alias);
    lowered
        .accesses
        .iter()
        .find(|a| a.alias == alias || a.alias == base)
        .map(|a| a.tm_iri.clone())
}

/// A disjunction of key predicates; equalities on one column fold into an
/// `IN` list.
fn any_of(mut keys: Vec<Pred>) -> Pred {
    if keys.len() == 1 {
        return keys.pop().unwrap();
    }
    let single = keys
        .iter()
        .all(|k| matches!(k, Pred::Cmp { op: CmpOp::Eq, .. }));
    let same_col = keys
        .windows(2)
        .all(|w| matches!((&w[0], &w[1]), (Pred::Cmp { col: a, .. }, Pred::Cmp { col: b, .. }) if a == b));
    if single && same_col {
        let col = match &keys[0] {
            Pred::Cmp { col, .. } => col.clone(),
            _ => unreachable!(),
        };
        let values = keys
            .into_iter()
            .map(|k| match k {
                Pred::Cmp { value, .. } => value,
                _ => unreachable!(),
            })
            .collect();
        return Pred::Cmp {
            col,
            op: CmpOp::In,
            value: Literal::Set(values),
        };
    }
    Pred::Or(keys)
}

/// The widest offset a zone can put between a naive timestamp and the
/// instant it denotes (UTC-12 to UTC+14), as micros.
const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;

pub(crate) fn source_of_tm(tm: &TriplesMap) -> RelSource {
    match tm.sql_query() {
        Some(q) => RelSource::Query(q.to_string()),
        None => RelSource::Table(tm.table_name().unwrap_or_default().to_string()),
    }
}

/// The relations the block's predicates can reach, whose probed schemas the
/// lowering wants up front so literals and template keys lower against known
/// column types. The statement's own tables are probed again at execution
/// from the same cache, so this adds no round trip for them.
pub(crate) fn candidate_sources(
    patterns: &[Pattern],
    snapshot: &LedgerSnapshot,
    mapping: &CompiledR2rmlMapping,
) -> Vec<RelSource> {
    let Ok(blocks) = expand_unions(patterns) else {
        return Vec::new();
    };
    let blocks: Vec<Block> = blocks
        .iter()
        .filter_map(|b| parse_block(b, snapshot).ok())
        .collect();
    let preds: HashSet<&str> = blocks
        .iter()
        .flat_map(|block| {
            block
                .triples
                .iter()
                .chain(block.optionals.iter().flat_map(|(t, _)| t))
        })
        .map(|t| t.p.as_str())
        .collect();
    let mut reached: Vec<&str> = Vec::new();
    for tm in mapping.triples_maps.values() {
        if preds
            .iter()
            .any(|p| *p == rdf::TYPE || pom_for(tm, p).is_some())
        {
            reached.push(tm.iri.as_str());
        }
    }
    // A `rr:parentTriplesMap` is reached through its FK, not through a
    // predicate of the block, so its table has to be walked to as well:
    // without it the parent's columns are unprobed, `field_type` reads them as
    // unknown, and every check that vets a column type — the join-class check
    // above all — silently passes on the half it cannot see.
    let mut i = 0;
    while i < reached.len() {
        let Some(tm) = mapping.get(reached[i]) else {
            i += 1;
            continue;
        };
        for pom in &tm.predicate_object_maps {
            if let ObjectMap::RefObjectMap(rom) = &pom.object_map {
                let parent = rom.parent_triples_map.as_str();
                if mapping.get(parent).is_some() && !reached.contains(&parent) {
                    reached.push(parent);
                }
            }
        }
        i += 1;
    }
    let mut out: Vec<RelSource> = Vec::new();
    for iri in reached {
        let Some(tm) = mapping.get(iri) else { continue };
        let src = source_of_tm(tm);
        if !out.contains(&src) {
            out.push(src);
        }
    }
    // A sub-select's tables are typed the same way.
    for sq in blocks.iter().flat_map(|b| &b.subqueries) {
        for src in candidate_sources(&sq.patterns, snapshot, mapping) {
            if !out.contains(&src) {
                out.push(src);
            }
        }
    }
    out
}

/// Whether a reversed template key can be a value of a column of type `ty`
/// (unknown types are trusted; the renderer types the literal).
pub(crate) fn key_fits(ty: Option<FieldType>, raw: &str) -> bool {
    match ty {
        Some(FieldType::Int64) => raw.parse::<i64>().is_ok(),
        Some(FieldType::Int32) => raw.parse::<i32>().is_ok(),
        _ => true,
    }
}

/// Structural admission at plan time: only shapes the lowering could accept.
/// The mapping-dependent decisions happen at open.
pub(crate) fn block_is_admissible(patterns: &[Pattern]) -> bool {
    let mut has_triple = false;
    let mut in_scope: Vec<VarId> = Vec::new();
    for (i, p) in patterns.iter().enumerate() {
        match p {
            Pattern::Triple(tp) => {
                has_triple = true;
                if !triple_is_admissible(tp) {
                    return false;
                }
            }
            Pattern::Filter(_) | Pattern::Values { .. } => {}
            // Evaluated in the engine over the statement's rows, so it must
            // read only what the block bound before it, and nothing the
            // statement joins or filters on may read it (a filter above is
            // group-scoped and runs after the BIND either way).
            Pattern::Bind { var, expr } => {
                if crate::filter::contains_exists(expr)
                    || crate::eval::metadata_resolve::contains_metadata_read(expr)
                    || in_scope.contains(var)
                    || expr.referenced_vars().iter().any(|v| !in_scope.contains(v))
                    || patterns.iter().enumerate().any(|(j, q)| {
                        j != i
                            && !matches!(q, Pattern::Filter(_))
                            && q.referenced_vars().contains(var)
                    })
                {
                    return false;
                }
            }
            Pattern::Optional(inner) => {
                if !inner.iter().all(|q| match q {
                    Pattern::Triple(tp) => triple_is_admissible(tp),
                    Pattern::Filter(_) => true,
                    _ => false,
                }) {
                    return false;
                }
            }
            Pattern::Subquery(sq) => {
                if !subquery_is_admissible(sq, patterns) {
                    return false;
                }
            }
            // Every branch is a block of its own; a branch without triples
            // would leave a combination with nothing to access.
            Pattern::Union(branches) => {
                if branches.is_empty() || !branches.iter().all(|b| block_is_admissible(b)) {
                    return false;
                }
                has_triple = true;
            }
            _ => return false,
        }
        in_scope.extend(p.produced_vars());
    }
    has_triple
}

/// A sub-select the lane embeds as a derived table: evaluated once (SPARQL's
/// sub-`SELECT`, or a subquery without a `LIMIT` per-row seeding could
/// change), its own block admissible with no nested subquery, grouped
/// without aggregate `BIND`s (a `HAVING` goes with it when the grouped
/// lane can push it; the lowering declines one it cannot), and hiding no
/// variable the enclosing block uses (a non-projected inner variable is
/// invisible outside in SPARQL, which the join must not reconnect).
fn subquery_is_admissible(sq: &SubqueryPattern, enclosing: &[Pattern]) -> bool {
    if !sq.pinned_vars.is_empty()
        || !sq.order_binds.is_empty()
        || sq.offset.is_some()
        || (!sq.uncorrelated && sq.limit.is_some())
        || sq
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Subquery(_)))
        || !block_is_admissible(&sq.patterns)
    {
        return false;
    }
    if let Some(g) = &sq.grouping {
        if g.aggregation().is_some_and(|a| !a.binds.is_empty()) {
            return false;
        }
        let outs: Vec<VarId> = g
            .group_by_vars()
            .chain(g.aggregates().map(|a| a.output_var))
            .collect();
        if sq.select.iter().any(|v| !outs.contains(v)) {
            return false;
        }
    }
    let inner: HashSet<VarId> = sq
        .patterns
        .iter()
        .flat_map(Pattern::referenced_vars)
        .chain(sq.patterns.iter().flat_map(Pattern::produced_vars))
        .collect();
    let outside: HashSet<VarId> = enclosing
        .iter()
        .filter(|p| !matches!(p, Pattern::Subquery(s) if std::ptr::eq(s, sq)))
        .flat_map(|p| p.referenced_vars().into_iter().chain(p.produced_vars()))
        .collect();
    !inner
        .iter()
        .any(|v| !sq.select.contains(v) && outside.contains(v))
}

fn triple_is_admissible(tp: &TriplePattern) -> bool {
    if matches!(tp.p, Ref::Var(_)) {
        return false;
    }
    if tp.p.is_rdf_type() {
        return matches!(
            tp.o,
            Term::Iri(_) | Term::Sid(_) | Term::Value(FlakeValue::Ref(_))
        );
    }
    true
}
