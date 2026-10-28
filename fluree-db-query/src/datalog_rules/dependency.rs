//! Which rules can observe which other rules' conclusions.
//!
//! Each rule gets a footprint: the graph-qualified predicates and classes its
//! body may read and its heads may write. A rule depends on another when one
//! of its reads may observe one of the other's writes. Strongly connected
//! components of that graph are the rule program's recursive groups.
//!
//! The analysis over-approximates. A variable predicate, a wildcard path or a
//! construct the walk does not model reads (or writes) everything, and two
//! graph scopes are kept apart only when both name different graphs. Extra
//! edges merge components and cost speed; a missing edge would let a rule run
//! before the facts it needs exist.

use super::{DatalogRule, DatalogRuleSet, HeadTerm, RuleHead};
use crate::eval::function_supported_for_bool_cache as function_is_repeatable;
use crate::ir::{
    Expression, Function, GraphName, Grouping, Pattern, Query, Ref, Term, TriplePattern,
};
use crate::parse::IriEncoder;
use fluree_db_core::{is_rdf_type, is_reserved_reifies_predicate, Sid};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

/// The graph a read or write touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphScope {
    /// The graph the rule program runs against.
    Default,
    /// `GRAPH <iri> { … }`.
    Named(Arc<str>),
    /// `GRAPH ?g { … }`: every named graph.
    AnyNamed,
}

impl GraphScope {
    fn may_overlap(&self, other: &GraphScope) -> bool {
        match (self, other) {
            (GraphScope::Named(a), GraphScope::Named(b)) => a == b,
            // A named-graph IRI can denote the default graph, so only two
            // distinct names are provably disjoint.
            _ => true,
        }
    }
}

/// The facts a read or write touches within a graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateFootprint {
    /// Facts with this predicate. For `rdf:type`, every class.
    Exact(Sid),
    /// `rdf:type` facts naming this class.
    Class(Sid),
    /// The reserved `f:reifies*` predicates an edge annotation is stored as.
    Reifier,
    /// Any fact: a variable predicate, a wildcard path, or a construct the
    /// analysis does not model.
    Any,
}

impl PredicateFootprint {
    fn may_overlap(&self, other: &PredicateFootprint) -> bool {
        use PredicateFootprint::{Any, Class, Exact, Reifier};
        match (self, other) {
            (Any, _) | (_, Any) => true,
            (Exact(a), Exact(b)) | (Class(a), Class(b)) => a == b,
            (Exact(p), Class(_)) | (Class(_), Exact(p)) => is_rdf_type(p),
            (Exact(p), Reifier) | (Reifier, Exact(p)) => is_reserved_reifies_predicate(p),
            (Reifier, Reifier) => true,
            (Class(_), Reifier) | (Reifier, Class(_)) => false,
        }
    }
}

impl fmt::Display for PredicateFootprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PredicateFootprint::Exact(p) => write!(f, "{p}"),
            PredicateFootprint::Class(c) => write!(f, "rdf:type {c}"),
            PredicateFootprint::Reifier => f.write_str("f:reifies*"),
            PredicateFootprint::Any => f.write_str("*"),
        }
    }
}

/// One graph-qualified read or write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    pub graph: GraphScope,
    pub predicate: PredicateFootprint,
}

impl Access {
    fn new(graph: &GraphScope, predicate: PredicateFootprint) -> Self {
        Self {
            graph: graph.clone(),
            predicate,
        }
    }

    /// The facts this access touches, for diagnostics, with IRIs spelled out.
    fn describe(&self, iri: &impl Fn(&Sid) -> Option<String>) -> String {
        let name = |sid: &Sid| iri(sid).unwrap_or_else(|| sid.to_string());
        let facts = match &self.predicate {
            PredicateFootprint::Exact(p) => format!("{} facts", name(p)),
            PredicateFootprint::Class(c) => format!("rdf:type {} facts", name(c)),
            PredicateFootprint::Reifier => "edge annotations".to_string(),
            PredicateFootprint::Any => "facts of any predicate".to_string(),
        };
        match &self.graph {
            GraphScope::Default => facts,
            GraphScope::Named(g) => format!("{facts} in graph {g}"),
            GraphScope::AnyNamed => format!("{facts} in any named graph"),
        }
    }

    /// Whether a read with this footprint may match a fact written by `write`.
    pub fn may_observe(&self, write: &Access) -> bool {
        self.graph.may_overlap(&write.graph) && self.predicate.may_overlap(&write.predicate)
    }
}

/// Everything one rule may read and write.
#[derive(Debug, Clone, Default)]
pub struct RuleFootprint {
    pub reads: Vec<Access>,
    pub writes: Vec<Access>,
    /// Re-running the body over the same facts yields the same rows, and more
    /// facts never remove a row. False when the body calls a function whose
    /// result can change between calls (`NOW`, `RAND`, `UUID`, `BNODE`, …),
    /// reads a match's transaction metadata (`t`, `op`), or uses a shortest
    /// path, whose chosen path can change as edges are added.
    /// Only a repeatable rule may run once instead of every round, or re-read
    /// only new facts in a recursive group. `NOW()` and `TODAY()` are
    /// repeatable here: every rule body in a run sees one evaluation time.
    pub repeatable: bool,
    /// The body calls `NOW()` or `TODAY()`, so the run needs an evaluation
    /// time.
    pub reads_clock: bool,
    /// The reads made under a negation (`NOT EXISTS`, `!EXISTS`). Each is
    /// also in `reads`. A rule may negate only facts that are complete
    /// before it runs, so none of these may observe its own group's writes.
    pub negated_reads: Vec<Access>,
    /// The reads made inside a grouped or aggregating subquery. Each is also
    /// in `reads`, and none may observe its own group's writes: a count over
    /// facts still being derived would change as they arrive.
    pub aggregated_reads: Vec<Access>,
}

impl RuleFootprint {
    /// Footprint of a parsed body and its heads. Body IRIs are encoded with
    /// the same encoder the heads were, so a class or predicate written by
    /// one rule compares equal to the same constant read by another, whether
    /// the body lowered it to a SID or kept the IRI for scan-time encoding.
    pub(super) fn of<E: IriEncoder + ?Sized>(
        query: &Query,
        heads: &[RuleHead],
        encoder: &E,
    ) -> Self {
        let mut walker = FootprintWalker::new(encoder);
        walker.read_patterns(&query.patterns, &GraphScope::Default);
        walker.read_query_tail(query);
        for head in heads {
            walker.write_head(head);
        }
        walker.footprint
    }

    /// The rule negates or aggregates facts, so its conclusions are only
    /// meaningful once everything it reads that way is complete.
    pub fn is_non_monotone(&self) -> bool {
        self.non_monotone_construct().is_some()
    }

    /// What makes the rule non-monotone, for messages.
    pub fn non_monotone_construct(&self) -> Option<&'static str> {
        if !self.negated_reads.is_empty() {
            Some("negation")
        } else if !self.aggregated_reads.is_empty() {
            Some("an aggregate")
        } else {
            None
        }
    }

    /// Whether some read of `self` may observe some write of `producer`.
    pub fn depends_on(&self, producer: &RuleFootprint) -> bool {
        self.reads
            .iter()
            .any(|read| producer.writes.iter().any(|write| read.may_observe(write)))
    }
}

/// The expressions a grouping phase evaluates: its HAVING and per-group
/// binds.
pub(super) fn grouping_expressions(grouping: &Grouping) -> impl Iterator<Item = &Expression> {
    let (having, binds) = match grouping {
        Grouping::Implicit { having, binds, .. } | Grouping::Explicit { having, binds, .. } => {
            (having, binds)
        }
    };
    having.iter().chain(binds.iter().map(|(_, expr)| expr))
}

/// What `patterns` may read in the default graph.
pub(super) fn pattern_reads<E: IriEncoder + ?Sized>(
    patterns: &[Pattern],
    encoder: &E,
) -> Vec<Access> {
    let mut walker = FootprintWalker::new(encoder);
    walker.read_patterns(patterns, &GraphScope::Default);
    walker.footprint.reads
}

/// What the parts of a body outside its `WHERE` patterns may read: the
/// trailing `VALUES` block and its binds, and `ORDER BY` expressions.
pub(super) fn query_tail_reads<E: IriEncoder + ?Sized>(query: &Query, encoder: &E) -> Vec<Access> {
    let mut walker = FootprintWalker::new(encoder);
    walker.read_query_tail(query);
    walker.footprint.reads
}

struct FootprintWalker<'e, E: ?Sized> {
    encoder: &'e E,
    footprint: RuleFootprint,
    /// Inside a negation, at any depth. Double negation counts as negated
    /// too: the stratification check stays conservative.
    negated: bool,
    /// Inside a grouped or aggregating subquery, at any depth.
    aggregated: bool,
}

impl<'e, E: IriEncoder + ?Sized> FootprintWalker<'e, E> {
    fn new(encoder: &'e E) -> Self {
        Self {
            encoder,
            footprint: RuleFootprint {
                repeatable: true,
                ..Default::default()
            },
            negated: false,
            aggregated: false,
        }
    }

    /// Walk `f` with every read it makes counted as aggregated.
    fn under_aggregation(&mut self, f: impl FnOnce(&mut Self)) {
        let outer = std::mem::replace(&mut self.aggregated, true);
        f(self);
        self.aggregated = outer;
    }

    /// Walk `f` with every read it makes counted as negated.
    fn under_negation(&mut self, f: impl FnOnce(&mut Self)) {
        let outer = std::mem::replace(&mut self.negated, true);
        f(self);
        self.negated = outer;
    }

    fn read_query_tail(&mut self, query: &Query) {
        if let Some(values) = &query.post_values {
            self.read_patterns(&values.to_patterns(), &GraphScope::Default);
        }
        for (_, expr) in &query.order_binds {
            self.read_expression(expr, &GraphScope::Default);
        }
    }

    fn encode(&self, iri: &str) -> Option<Sid> {
        self.encoder.encode_iri(iri)
    }

    fn read(&mut self, graph: &GraphScope, predicate: PredicateFootprint) {
        let access = Access::new(graph, predicate);
        if self.negated && !self.footprint.negated_reads.contains(&access) {
            self.footprint.negated_reads.push(access.clone());
        }
        if self.aggregated && !self.footprint.aggregated_reads.contains(&access) {
            self.footprint.aggregated_reads.push(access.clone());
        }
        if !self.footprint.reads.contains(&access) {
            self.footprint.reads.push(access);
        }
    }

    fn read_triple(&mut self, triple: &TriplePattern, graph: &GraphScope) {
        let predicate = match &triple.p {
            Ref::Sid(p) => Some(p.clone()),
            Ref::Iri(iri) => self.encode(iri),
            Ref::Var(_) => None,
        };
        let object = match &triple.o {
            Term::Sid(o) => Some(o.clone()),
            Term::Iri(iri) => self.encode(iri),
            Term::Var(_) | Term::Value(_) => None,
        };
        let footprint = match (predicate, object) {
            (Some(p), Some(class)) if is_rdf_type(&p) => PredicateFootprint::Class(class),
            (Some(p), _) => PredicateFootprint::Exact(p),
            (None, _) => PredicateFootprint::Any,
        };
        self.read(graph, footprint);
    }

    fn read_patterns(&mut self, patterns: &[Pattern], graph: &GraphScope) {
        for pattern in patterns {
            match pattern {
                Pattern::Triple(triple) => self.read_triple(triple, graph),
                Pattern::ListView(lv) => self.read_triple(&lv.triple, graph),
                Pattern::Filter(expr)
                | Pattern::Bind { expr, .. }
                | Pattern::Unwind { list: expr, .. } => self.read_expression(expr, graph),
                Pattern::Minus(inner) | Pattern::NotExists(inner) => {
                    self.under_negation(|w| w.read_patterns(inner, graph));
                }
                Pattern::Optional(inner)
                | Pattern::Exists(inner)
                | Pattern::DefaultGraphSource { patterns: inner } => {
                    self.read_patterns(inner, graph);
                }
                Pattern::Union(branches) => {
                    for branch in branches {
                        self.read_patterns(branch, graph);
                    }
                }
                Pattern::Graph { name, patterns } => {
                    let scope = match name {
                        GraphName::Iri(iri) => GraphScope::Named(iri.clone()),
                        GraphName::Var(_) => GraphScope::AnyNamed,
                    };
                    self.read_patterns(patterns, &scope);
                }
                Pattern::EdgeAnnotation { edge, body, .. }
                | Pattern::AnnotationTarget { edge, body, .. } => {
                    self.read_triple(edge, graph);
                    self.read(graph, PredicateFootprint::Reifier);
                    self.read_patterns(body, graph);
                }
                Pattern::PropertyPath(path) => {
                    // A zero-hop match pairs every term in the graph with
                    // itself, so a new term under any predicate adds a match.
                    if path.wildcard || path.effective_min_hops() == 0 {
                        self.read(graph, PredicateFootprint::Any);
                    }
                    let steps = path.sequence_steps.iter().flat_map(|s| &s.predicates);
                    for p in path.predicates.iter().chain(steps) {
                        self.read(graph, PredicateFootprint::Exact(p.clone()));
                    }
                }
                Pattern::ShortestPath(path) => {
                    self.footprint.repeatable = false;
                    let predicate = match &path.predicate {
                        Some(p) => PredicateFootprint::Exact(p.clone()),
                        None => PredicateFootprint::Any,
                    };
                    self.read(graph, predicate);
                }
                Pattern::Subquery(sub) => {
                    let read_subquery = |w: &mut Self| {
                        w.read_patterns(&sub.patterns, graph);
                        for (_, expr) in &sub.order_binds {
                            w.read_expression(expr, graph);
                        }
                        if let Some(grouping) = &sub.grouping {
                            for expr in grouping_expressions(grouping) {
                                w.read_expression(expr, graph);
                            }
                        }
                    };
                    if sub.grouping.is_some() {
                        self.under_aggregation(read_subquery);
                    } else {
                        read_subquery(self);
                    }
                }
                Pattern::Values { .. } => {}
                // Rejected in rule bodies; read as everything if one ever
                // gets through.
                Pattern::IndexSearch(_)
                | Pattern::VectorSearch(_)
                | Pattern::R2rml(_)
                | Pattern::GeoSearch(_)
                | Pattern::S2Search(_)
                | Pattern::Service(_) => self.read(graph, PredicateFootprint::Any),
            }
        }
    }

    fn read_expression(&mut self, expr: &Expression, graph: &GraphScope) {
        match expr {
            Expression::Exists { patterns, negated } => {
                if *negated {
                    self.under_negation(|w| w.read_patterns(patterns, graph));
                } else {
                    self.read_patterns(patterns, graph);
                }
            }
            Expression::Call { func, args } => {
                if matches!(func, Function::Now | Function::Today) {
                    self.footprint.reads_clock = true;
                } else if !function_is_repeatable(func) {
                    self.footprint.repeatable = false;
                }
                // Anything under `NOT` may be negated; the validator admits an
                // `EXISTS` only where its polarity is readable.
                if matches!(func, Function::Not) {
                    self.under_negation(|w| {
                        for arg in args {
                            w.read_expression(arg, graph);
                        }
                    });
                } else {
                    for arg in args {
                        self.read_expression(arg, graph);
                    }
                }
            }
            Expression::PatternComprehension { .. } => {
                self.read(graph, PredicateFootprint::Any);
            }
            Expression::Map(entries) => {
                for (_, value) in entries {
                    self.read_expression(value, graph);
                }
            }
            Expression::ListComprehension {
                list, filter, map, ..
            } => {
                self.read_expression(list, graph);
                for part in [filter, map].into_iter().flatten() {
                    self.read_expression(part, graph);
                }
            }
            Expression::Reduce {
                init, list, body, ..
            } => {
                for part in [init, list, body] {
                    self.read_expression(part, graph);
                }
            }
            Expression::ListPredicate {
                list, predicate, ..
            } => {
                self.read_expression(list, graph);
                self.read_expression(predicate, graph);
            }
            Expression::Member { target, .. } => self.read_expression(target, graph),
            Expression::Var(_) | Expression::Const(_) | Expression::Resolved(_) => {}
        }
    }

    /// A head writes into its named graph, else into the graph the rules run
    /// against.
    fn write_head(&mut self, head: &RuleHead) {
        let predicate = match (&head.predicate, &head.object) {
            (HeadTerm::Sid(p), HeadTerm::Sid(class)) if is_rdf_type(p) => {
                PredicateFootprint::Class(class.clone())
            }
            (HeadTerm::Sid(p), _) => PredicateFootprint::Exact(p.clone()),
            (HeadTerm::Var(_) | HeadTerm::Literal { .. }, _) => PredicateFootprint::Any,
        };
        let graph = match &head.graph {
            Some(iri) => GraphScope::Named(iri.clone()),
            None => GraphScope::Default,
        };
        let access = Access::new(&graph, predicate);
        if !self.footprint.writes.contains(&access) {
            self.footprint.writes.push(access);
        }
    }
}

/// A strongly connected group of rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleComponent {
    /// Indexes into the rule set's execution order, ascending.
    pub rules: Vec<usize>,
    /// A rule in the group may observe a fact the group itself derives, so the
    /// group must iterate to a fixpoint. A non-recursive group's rules need
    /// only the facts of the groups before it.
    pub recursive: bool,
}

/// The rule program's footprints and recursive groups.
#[derive(Debug, Clone)]
pub struct RuleDependencies {
    footprints: Vec<RuleFootprint>,
    /// `depends_on[consumer]` lists every producer it may observe.
    depends_on: Vec<Vec<usize>>,
    /// Producers before consumers.
    components: Vec<RuleComponent>,
}

impl RuleDependencies {
    pub fn analyze(rules: &[DatalogRule]) -> Self {
        Self::from_footprints(rules.iter().map(|r| r.footprint.clone()).collect())
    }

    pub fn from_footprints(footprints: Vec<RuleFootprint>) -> Self {
        let depends_on: Vec<Vec<usize>> = footprints
            .iter()
            .map(|consumer| {
                footprints
                    .iter()
                    .enumerate()
                    .filter(|(_, producer)| consumer.depends_on(producer))
                    .map(|(i, _)| i)
                    .collect()
            })
            .collect();
        let components = strongly_connected_components(&depends_on);
        Self {
            footprints,
            depends_on,
            components,
        }
    }

    pub fn footprint(&self, rule: usize) -> &RuleFootprint {
        &self.footprints[rule]
    }

    /// Producers `rule` may observe, ascending.
    pub fn producers_of(&self, rule: usize) -> &[usize] {
        &self.depends_on[rule]
    }

    /// Recursive groups in dependency order: every group comes after each
    /// group it depends on.
    pub fn components(&self) -> &[RuleComponent] {
        &self.components
    }

    /// Groups batched into stages. A group's stage is one past the latest
    /// stage of any group it reads from, so no group reads what another
    /// group in its own stage writes, and every stage can run against one
    /// overlay built from the stages before it.
    pub fn stages(&self) -> Vec<Vec<&RuleComponent>> {
        let mut component_of = vec![0; self.footprints.len()];
        for (c, component) in self.components.iter().enumerate() {
            for &rule in &component.rules {
                component_of[rule] = c;
            }
        }
        let mut stage_of = vec![0usize; self.components.len()];
        for (c, component) in self.components.iter().enumerate() {
            stage_of[c] = component
                .rules
                .iter()
                .flat_map(|&rule| &self.depends_on[rule])
                .map(|&producer| component_of[producer])
                .filter(|&producer_component| producer_component != c)
                .map(|producer_component| {
                    debug_assert!(producer_component < c, "components are producers first");
                    stage_of[producer_component] + 1
                })
                .max()
                .unwrap_or(0);
        }
        let stage_count = stage_of.iter().max().map_or(0, |last| last + 1);
        let mut stages: Vec<Vec<&RuleComponent>> = vec![Vec::new(); stage_count];
        for (c, component) in self.components.iter().enumerate() {
            stages[stage_of[c]].push(component);
        }
        stages
    }

    /// Whether some rule negates or aggregates facts its own recursive group
    /// derives. Such a program has no stratification: those facts are not
    /// complete before the rule reads them, so no evaluation order is sound.
    /// The error names the facts read and the cycle, the offending rule
    /// first: `ex:a -not-> ex:b -> ex:a` (or `-agg->`), where each arrow reads
    /// facts from the rule it points to. `iri` spells out predicates and
    /// classes.
    pub fn unstratified(
        &self,
        rule_set: &DatalogRuleSet,
        iri: impl Fn(&Sid) -> Option<String>,
    ) -> Option<String> {
        let name = |i: usize| rule_set.rules[i].name.as_str();
        for component in self.components.iter().filter(|c| c.recursive) {
            for &consumer in &component.rules {
                let footprint = &self.footprints[consumer];
                let non_monotone_reads = footprint
                    .negated_reads
                    .iter()
                    .map(|read| (read, "not", "negates"))
                    .chain(
                        footprint
                            .aggregated_reads
                            .iter()
                            .map(|read| (read, "agg", "aggregates")),
                    );
                for (negated, arrow, verb) in non_monotone_reads {
                    let Some((producer, write)) = component.rules.iter().find_map(|&p| {
                        self.footprints[p]
                            .writes
                            .iter()
                            .find(|write| negated.may_observe(write))
                            .map(|write| (p, write))
                    }) else {
                        continue;
                    };
                    // A read of every predicate is the analysis's limit, not
                    // something the rule wrote, so say what read it and
                    // which of its facts close the cycle.
                    let facts = match negated.predicate {
                        PredicateFootprint::Any => format!(
                            "{} (a variable predicate, a `*` or `?` path, a negated property \
                             set, or another read the analysis cannot narrow), among them {}",
                            negated.describe(&iri),
                            write.describe(&iri)
                        ),
                        _ => negated.describe(&iri),
                    };
                    let mut cycle = vec![name(consumer)];
                    cycle.extend(
                        self.path_within(&component.rules, producer, consumer)
                            .into_iter()
                            .map(name),
                    );
                    let arrows = cycle
                        .iter()
                        .enumerate()
                        .map(|(i, n)| match i {
                            0 => (*n).to_string(),
                            1 => format!(" -{arrow}-> {n}"),
                            _ => format!(" -> {n}"),
                        })
                        .collect::<String>();
                    return Some(format!(
                        "datalog rule {} {verb} {facts}, which depend on its own conclusions \
                         ({arrows}; each arrow reads facts from the rule it points to). \
                         Negation and aggregation need what they read to be complete before \
                         the rule runs, which a cycle through them prevents",
                        name(consumer)
                    ));
                }
            }
        }
        None
    }

    /// Rules on a dependency path `from` → … → `to` inside one component,
    /// following reads (each rule reads from the next), `from` and `to`
    /// included. A component is strongly connected, so one exists.
    fn path_within(&self, component: &[usize], from: usize, to: usize) -> Vec<usize> {
        if from == to {
            return vec![from];
        }
        let mut previous: HashMap<usize, usize> = HashMap::new();
        let mut queue = VecDeque::from([from]);
        'search: while let Some(rule) = queue.pop_front() {
            for &next in &self.depends_on[rule] {
                if next != from && component.contains(&next) && !previous.contains_key(&next) {
                    previous.insert(next, rule);
                    if next == to {
                        break 'search;
                    }
                    queue.push_back(next);
                }
            }
        }
        let mut path = vec![to];
        while let Some(&before) = path.last().and_then(|at| previous.get(at)) {
            path.push(before);
        }
        path.reverse();
        path
    }

    /// One line naming each group in order, recursive groups starred, for
    /// diagnostics: `[ex:base ex:step]* -> [ex:flag]`.
    pub fn describe(&self, rule_set: &DatalogRuleSet) -> String {
        self.components
            .iter()
            .map(|component| {
                let names: Vec<&str> = component
                    .rules
                    .iter()
                    .map(|&i| rule_set.rules[i].name.as_str())
                    .collect();
                let star = if component.recursive { "*" } else { "" };
                format!("[{}]{star}", names.join(" "))
            })
            .collect::<Vec<_>>()
            .join(" -> ")
    }
}

/// Tarjan's algorithm over `depends_on` (consumer → producers). Tarjan emits
/// a component only after every component reachable from it, and the edges
/// point at producers, so emission order is already producers first.
fn strongly_connected_components(depends_on: &[Vec<usize>]) -> Vec<RuleComponent> {
    struct Tarjan<'a> {
        edges: &'a [Vec<usize>],
        index: Vec<Option<usize>>,
        lowlink: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        next_index: usize,
        components: Vec<RuleComponent>,
    }

    impl Tarjan<'_> {
        fn visit(&mut self, v: usize) {
            self.index[v] = Some(self.next_index);
            self.lowlink[v] = self.next_index;
            self.next_index += 1;
            self.stack.push(v);
            self.on_stack[v] = true;

            for &w in &self.edges[v] {
                match self.index[w] {
                    None => {
                        self.visit(w);
                        self.lowlink[v] = self.lowlink[v].min(self.lowlink[w]);
                    }
                    Some(w_index) if self.on_stack[w] => {
                        self.lowlink[v] = self.lowlink[v].min(w_index);
                    }
                    Some(_) => {}
                }
            }

            if Some(self.lowlink[v]) == self.index[v] {
                let mut rules = Vec::new();
                loop {
                    let w = self.stack.pop().expect("v is on the stack");
                    self.on_stack[w] = false;
                    rules.push(w);
                    if w == v {
                        break;
                    }
                }
                rules.sort_unstable();
                let recursive = rules.len() > 1 || self.edges[v].contains(&v);
                self.components.push(RuleComponent { rules, recursive });
            }
        }
    }

    let n = depends_on.len();
    let mut tarjan = Tarjan {
        edges: depends_on,
        index: vec![None; n],
        lowlink: vec![0; n],
        on_stack: vec![false; n],
        stack: Vec::new(),
        next_index: 0,
        components: Vec::new(),
    };
    for v in 0..n {
        if tarjan.index[v].is_none() {
            tarjan.visit(v);
        }
    }
    tarjan.components
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::namespaces::{FLUREE_DB, RDF};

    fn ex(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn rdf_type() -> Sid {
        Sid::new(RDF, "type")
    }

    fn exact(name: &str) -> PredicateFootprint {
        PredicateFootprint::Exact(ex(name))
    }

    fn class(name: &str) -> PredicateFootprint {
        PredicateFootprint::Class(ex(name))
    }

    fn default(predicate: PredicateFootprint) -> Access {
        Access {
            graph: GraphScope::Default,
            predicate,
        }
    }

    fn rule(reads: Vec<PredicateFootprint>, writes: Vec<PredicateFootprint>) -> RuleFootprint {
        RuleFootprint {
            reads: reads.into_iter().map(default).collect(),
            writes: writes.into_iter().map(default).collect(),
            repeatable: true,
            ..Default::default()
        }
    }

    #[test]
    fn class_reads_observe_only_matching_or_generic_type_writes() {
        let reads_a = default(class("A"));
        assert!(reads_a.may_observe(&default(class("A"))));
        assert!(!reads_a.may_observe(&default(class("B"))));
        assert!(reads_a.may_observe(&default(PredicateFootprint::Exact(rdf_type()))));
        assert!(!reads_a.may_observe(&default(exact("A"))));

        let reads_any_type = default(PredicateFootprint::Exact(rdf_type()));
        assert!(reads_any_type.may_observe(&default(class("B"))));
    }

    #[test]
    fn wildcards_observe_everything() {
        let any = default(PredicateFootprint::Any);
        for write in [exact("p"), class("A"), PredicateFootprint::Reifier] {
            assert!(any.may_observe(&default(write.clone())));
            assert!(default(write).may_observe(&any));
        }
    }

    #[test]
    fn reifier_reads_observe_only_reserved_predicates() {
        let reifier = default(PredicateFootprint::Reifier);
        let reifies_subject = PredicateFootprint::Exact(Sid::new(FLUREE_DB, "reifiesSubject"));
        assert!(reifier.may_observe(&default(reifies_subject)));
        assert!(!reifier.may_observe(&default(exact("p"))));
        assert!(!reifier.may_observe(&default(class("A"))));
    }

    #[test]
    fn distinct_named_graphs_are_disjoint_and_everything_else_overlaps() {
        let named = |iri: &str| Access {
            graph: GraphScope::Named(Arc::from(iri)),
            predicate: exact("p"),
        };
        let any_named = Access {
            graph: GraphScope::AnyNamed,
            predicate: exact("p"),
        };
        assert!(!named("urn:g1").may_observe(&named("urn:g2")));
        assert!(named("urn:g1").may_observe(&named("urn:g1")));
        assert!(named("urn:g1").may_observe(&default(exact("p"))));
        assert!(any_named.may_observe(&named("urn:g2")));
        assert!(any_named.may_observe(&default(exact("p"))));
    }

    #[test]
    fn components_are_producers_first_with_recursion_flagged() {
        // 0: parent -> ancestor        (base)
        // 1: ancestor -> ancestor      (recursive)
        // 2: ancestor -> flagged       (downstream)
        // 3: flagged -> reported       (downstream of 2)
        // 4: unrelated -> other        (independent)
        let deps = RuleDependencies::from_footprints(vec![
            rule(vec![exact("parent")], vec![exact("ancestor")]),
            rule(vec![exact("ancestor")], vec![exact("ancestor")]),
            rule(vec![exact("ancestor")], vec![exact("flagged")]),
            rule(vec![exact("flagged")], vec![exact("reported")]),
            rule(vec![exact("unrelated")], vec![exact("other")]),
        ]);

        let position = |rule: usize| {
            deps.components()
                .iter()
                .position(|c| c.rules.contains(&rule))
                .unwrap()
        };
        assert_eq!(deps.components().len(), 5);
        assert!(!deps.components()[position(0)].recursive);
        assert!(deps.components()[position(1)].recursive);
        assert!(!deps.components()[position(2)].recursive);
        assert!(position(0) < position(1));
        assert!(position(1) < position(2));
        assert!(position(2) < position(3));
        assert_eq!(deps.producers_of(2), &[0, 1]);
    }

    #[test]
    fn stages_batch_groups_that_do_not_read_each_other() {
        // 0: base -> a; 1: a -> a (recursive); 2,3: a -> x / a -> y;
        // 4: x -> z; 5: unrelated -> w
        let deps = RuleDependencies::from_footprints(vec![
            rule(vec![exact("base")], vec![exact("a")]),
            rule(vec![exact("a")], vec![exact("a")]),
            rule(vec![exact("a")], vec![exact("x")]),
            rule(vec![exact("a")], vec![exact("y")]),
            rule(vec![exact("x")], vec![exact("z")]),
            rule(vec![exact("unrelated")], vec![exact("w")]),
        ]);
        let stages: Vec<Vec<Vec<usize>>> = deps
            .stages()
            .iter()
            .map(|stage| {
                let mut groups: Vec<Vec<usize>> =
                    stage.iter().map(|group| group.rules.clone()).collect();
                groups.sort();
                groups
            })
            .collect();
        assert_eq!(
            stages,
            vec![
                vec![vec![0], vec![5]],
                vec![vec![1]],
                vec![vec![2], vec![3]],
                vec![vec![4]],
            ]
        );
    }

    #[test]
    fn mutual_recursion_forms_one_component() {
        let deps = RuleDependencies::from_footprints(vec![
            rule(vec![exact("p")], vec![exact("q")]),
            rule(vec![exact("q")], vec![exact("p")]),
            rule(vec![exact("q")], vec![exact("r")]),
        ]);
        assert_eq!(
            deps.components(),
            &[
                RuleComponent {
                    rules: vec![0, 1],
                    recursive: true
                },
                RuleComponent {
                    rules: vec![2],
                    recursive: false
                },
            ]
        );
    }

    #[test]
    fn a_wildcard_head_feeds_every_rule() {
        let deps = RuleDependencies::from_footprints(vec![
            rule(vec![exact("p")], vec![PredicateFootprint::Any]),
            rule(vec![exact("q")], vec![exact("r")]),
        ]);
        assert!(deps.components()[0].recursive, "it can feed itself");
        assert_eq!(deps.producers_of(1), &[0]);
    }
}
