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
    like_escape, CmpOp, ColRef, KeySet, Literal, OutputCol, Pred, PushdownCapabilities, RelNode,
    RelSource,
};
use fluree_db_tabular::{BatchSchema, FieldType};
use fluree_vocab::{rdf, xsd, UnresolvedDatatypeConstraint};

use crate::binding::Binding;
use crate::error::Result;
use crate::ir::expression::Function;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Pattern};
use crate::r2rml::policy::{derived_type_map, static_classes, Verdict};
use crate::var_registry::VarId;

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
            required_columns: HashSet::new(),
            static_keysets: Vec::new(),
        };
        let lowered = lw.lower(block, input.child_vars, input.projection)?;
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
    /// `(alias, column)` pairs that are `IS NOT NULL`.
    required_columns: HashSet<(String, String)>,
    static_keysets: Vec<(KeySet, Vec<Pred>)>,
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
        let mut pending_refs: Vec<RefEdge> = Vec::new();
        for (idx, (subject, members)) in entities.iter().enumerate() {
            let parts = match self.resolve_parts(members, None) {
                Ok(Ok(parts)) => parts,
                Ok(Err(Empty)) => return Ok(Ok(None)),
                Err(d) => return Ok(Err(d)),
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
                .position(|(s, _)| matches!(s, SubjRef::Var(v) if *v == var))
                .and_then(|i| entity_accesses.get(&i));
            let Some(accesses) = target else {
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
                // Another subject: no row of the child can join a row of
                // that entity.
                None => return Ok(Ok(None)),
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

        // Filters: exact ones into the plan; the rest stay in the engine,
        // with a widening predicate in the plan where one exists.
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

        // A DISTINCT above reads only `projection`; the statement then
        // returns each distinct combination of those variables' columns
        // once, keeping the columns the in-memory join and the residual
        // filters read. Distinct column values are distinct terms only when
        // the database keeps byte-distinct strings apart (a case-folding
        // collation would merge two IRIs or literals).
        let distinct_vars: Option<HashSet<VarId>> = projection
            .filter(|_| self.caps.string_distinct_is_binary)
            .map(|proj| {
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
        let accesses: Vec<AccessInfo> = self
            .accesses
            .iter()
            .map(|a| AccessInfo {
                alias: a.alias.clone(),
                tm_iri: a.tm_iri.clone(),
                columns: per_alias.remove(&a.alias).unwrap_or_default(),
                output_names: None,
            })
            .collect();

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
            limit_is_exact,
            distinct: distinct_vars.is_some(),
        })))
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
        self.schemas
            .get(&self.rel_source(&col.alias))?
            .field_by_name(&col.column)
            .map(|f| f.field_type)
    }

    /// Whether an engine literal of `class` compares exactly against `col`:
    /// the probed column type must carry the class natively (a numeric
    /// literal against a text column the mapping *reads* as a number is the
    /// engine's comparison, not the database's). A dateTime literal is a UTC
    /// instant, exact only against a zoned column. An unprobed column is
    /// trusted; the renderer then types the literal.
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
            RdfClass::DateTime => ty == F::TimestampTz,
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

    /// The single triples map every member of an entity resolves to.
    /// The one triples map providing every member, for an entity that
    /// must be a single access.
    fn resolve_tm(
        &self,
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

    /// The triples maps an entity's members come from, each with the
    /// members (by index) it provides: one map providing everything, or a
    /// vertical partition where every member has exactly one provider and
    /// the providers mint the same subject.
    fn resolve_parts(
        &self,
        members: &[(String, Obj)],
        only: Option<&'a TriplesMap>,
    ) -> Lowering<std::result::Result<Vec<Part<'a>>, Empty>> {
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
                providers.push(found);
            } else {
                providers.push(
                    candidates
                        .iter()
                        .copied()
                        .filter(|tm| pom_for(tm, pred).is_some())
                        .collect(),
                );
            }
        }
        let whole: Vec<&'a TriplesMap> = candidates
            .iter()
            .copied()
            .filter(|tm| providers.iter().all(|p| p.iter().any(|c| c.iri == tm.iri)))
            .collect();
        match whole.as_slice() {
            [tm] => return Ok(Ok(vec![(tm, (0..members.len()).collect())])),
            [] => {}
            _ => return decline("entity spans several triples maps"),
        }
        let mut parts: Vec<Part<'a>> = Vec::new();
        let mut classes: Vec<usize> = Vec::new();
        for (i, p) in providers.iter().enumerate() {
            if members[i].0 == rdf::TYPE {
                classes.push(i);
                continue;
            }
            match p.as_slice() {
                [] => return Ok(Err(Empty)),
                [tm] => match parts.iter_mut().find(|(t, _)| t.iri == tm.iri) {
                    Some((_, idxs)) => idxs.push(i),
                    None => parts.push((tm, vec![i])),
                },
                _ => return decline("predicate provided by several triples maps"),
            }
        }
        // A class comes with any part's map that declares it; otherwise it
        // is a part of its own (a type-only access on the shared subject).
        for i in classes {
            let p = &providers[i];
            if let Some((_, idxs)) = parts
                .iter_mut()
                .find(|(t, _)| p.iter().any(|c| c.iri == t.iri))
            {
                idxs.push(i);
                continue;
            }
            match p.as_slice() {
                [] => return Ok(Err(Empty)),
                [tm] => parts.push((tm, vec![i])),
                _ => return decline("class declared by several triples maps"),
            }
        }
        if parts.iter().any(|(tm, _)| {
            let sm = &tm.subject_map;
            sm.template.is_none() && sm.column.is_none()
        }) {
            return decline("entity spans triples maps with a constant subject");
        }
        // Different subjects never name one entity (the lane joins by key
        // columns, never by rendered IRI): no row can carry every member.
        if parts.iter().any(|(tm, _)| !same_subject(parts[0].0, tm)) {
            return Ok(Err(Empty));
        }
        Ok(Ok(parts))
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
                    ..
                },
                KeyShape::Template {
                    template: tb,
                    cols: cb,
                    ..
                },
            ) => {
                if ta != tb || ca.len() != cb.len() {
                    return decline("repeated variable joins two different templates");
                }
                for (l, r) in ca.iter().zip(cb) {
                    self.push_edge(l.clone(), r.clone());
                }
                Ok(())
            }
            (KeyShape::Column { col: l, class: cl }, KeyShape::Column { col: r, class: cr }) => {
                if cl != cr {
                    return decline("repeated variable joins two value classes");
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
            for (pred, obj) in members {
                if pred == rdf::TYPE {
                    return decline("rdf:type inside an optional");
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
                        let (col, class) = self.literal_column(*v)?;
                        let (lit, lclass) = literal_of(c, None)?;
                        if !self.exact_eq(&col, &class, &lclass) {
                            return None;
                        }
                        if ordering
                            && class == RdfClass::Str
                            && !self.caps.string_order_is_codepoint
                        {
                            return None;
                        }
                        let op = match (func, reversed) {
                            (Function::Eq, _) => CmpOp::Eq,
                            (Function::Ne, _) => CmpOp::NotEq,
                            (Function::Lt, false) | (Function::Gt, true) => CmpOp::Lt,
                            (Function::Le, false) | (Function::Ge, true) => CmpOp::LtEq,
                            (Function::Gt, false) | (Function::Lt, true) => CmpOp::Gt,
                            (Function::Ge, false) | (Function::Le, true) => CmpOp::GtEq,
                            _ => return None,
                        };
                        Some(Pred::Cmp {
                            col,
                            op,
                            value: lit,
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
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
            Function::Eq | Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                self.naive_timestamp_window(func, args)
            }
            // `^literal` with no flags is a case-sensitive prefix.
            Function::Regex => {
                if args.len() != 2 {
                    return None;
                }
                let (col, pattern) = self.string_column_and_literal(args)?;
                let prefix = pattern.strip_prefix('^')?;
                if prefix.is_empty() || prefix.chars().any(|c| r".^$*+?()[]{}|".contains(c)) {
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

    /// An `xsd:dateTime` literal (a UTC instant) compared with a `timestamp`
    /// column that carries no zone: the column's values sit within
    /// [`NAIVE_ZONE_SPAN`] of the instant they denote whatever zone they were
    /// written in, so a window that wide around the literal keeps every row
    /// the exact comparison can. Rendered naive, so the database converts
    /// nothing. Where timestamps are text the bounds are whole days.
    fn naive_timestamp_window(&self, func: &Function, args: &[Expression]) -> Option<Pred> {
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
        let lo = micros.checked_sub(NAIVE_ZONE_SPAN)?;
        let hi = micros.checked_add(NAIVE_ZONE_SPAN)?;
        // Text bounds are the day of the low end and the day after the high
        // end, so the upper comparison is strict.
        let bound = |micros: i64, upper: bool| -> Option<Literal> {
            if self.caps.timestamp_is_text {
                let day = micros.div_euclid(MICROS_PER_DAY) + i64::from(upper);
                Some(Literal::Date(i32::try_from(day).ok()?))
            } else {
                Some(Literal::Timestamp { micros, tz: false })
            }
        };
        let (lo, hi) = (bound(lo, false)?, bound(hi, true)?);
        let hi_op = if self.caps.timestamp_is_text {
            CmpOp::Lt
        } else {
            CmpOp::LtEq
        };
        let cmp = |op, value| Pred::Cmp {
            col: col.clone(),
            op,
            value,
        };
        let below = matches!(func, Function::Lt | Function::Le) != reversed;
        Some(match func {
            Function::Eq => Pred::And(vec![cmp(CmpOp::GtEq, lo), cmp(hi_op, hi)]),
            _ if below => cmp(hi_op, hi),
            _ => cmp(CmpOp::GtEq, lo),
        })
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
        let node = RelNode::Access {
            alias: alias.to_string(),
            source: self.rel_source(alias),
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
        match term {
            TermSource::Constant(_) => Vec::new(),
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

/// Whether two triples maps mint their subject the same way from the same
/// columns, so a row of each with equal key columns is one entity.
fn same_subject(a: &TriplesMap, b: &TriplesMap) -> bool {
    let (sa, sb) = (&a.subject_map, &b.subject_map);
    (sa.template.is_some() || sa.column.is_some())
        && sa.template == sb.template
        && sa.template_columns == sb.template_columns
        && sa.column == sb.column
}

/// Whether two triples maps read the same row: one relation, one subject.
fn same_row(a: &TriplesMap, b: &TriplesMap) -> bool {
    same_subject(a, b) && source_of_tm(a) == source_of_tm(b)
}

fn collect_aliases(pred: &Pred, out: &mut HashSet<String>) {
    match pred {
        Pred::Cmp { col, .. }
        | Pred::IsNull(col)
        | Pred::IsNotNull(col)
        | Pred::Like { col, .. } => {
            out.insert(col.alias.clone());
        }
        Pred::ColEq { left, right } => {
            out.insert(left.alias.clone());
            out.insert(right.alias.clone());
        }
        Pred::And(ps) | Pred::Or(ps) => ps.iter().for_each(|p| collect_aliases(p, out)),
        Pred::Not(p) => collect_aliases(p, out),
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
    };
    for p in patterns {
        match p {
            Pattern::Triple(tp) => block.triples.push(parse_triple(tp, snapshot)?),
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
const NAIVE_ZONE_SPAN: i64 = 14 * 3_600 * 1_000_000;
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
    let mut out: Vec<RelSource> = Vec::new();
    for tm in mapping.triples_maps.values() {
        if preds
            .iter()
            .any(|p| *p == rdf::TYPE || pom_for(tm, p).is_some())
        {
            let src = source_of_tm(tm);
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
