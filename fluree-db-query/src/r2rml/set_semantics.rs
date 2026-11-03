//! Set semantics for R2RML scans.
//!
//! A mapping's output is an RDF dataset (R2RML §11): a SET of triples. A
//! logical table is a bag of rows, and two rows can mint the same triple (a
//! history table holding several versions of one subject, an exact duplicate
//! row), so a scan that turns each row into solutions answers over a bag unless
//! it removes the repeats. This module holds what every scan lane asks before
//! it trusts a row-per-solution answer:
//!
//! - [`TableKeyEvidence`]: what is known about each table's keys, the union of
//!   the mapping's `f:key` declarations and what the source reports, fetched
//!   once per table through [`super::effective_table_keys`];
//! - [`emits_distinct_for`]: whether one row of a map never yields the same
//!   solution of a pattern twice;
//! - [`SeenSolutions`]: the streaming, first-seen set a scan pass checks its
//!   solutions against when the answer to either is no.
//!
//! Deduplication belongs to the query lanes only. The materialize seams that
//! stream a table's rows to a bulk builder report raw rows and never consult
//! this module.

use std::collections::{BTreeSet, HashMap};
use std::hash::BuildHasher;
use std::sync::Mutex;

use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TableKeys, TriplesMap};
use rustc_hash::FxBuildHasher;

use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::R2rmlPattern;
use crate::var_registry::VarId;

/// Routing stamp for a scan whose every map is proven to mint each of its
/// pattern's solutions once, so it skips deduplication (`proceed`); a scan
/// that deduplicates records `fallback:gate_declined`, and one run with fast
/// paths disabled `fallback:kill_switch` (it deduplicates everything).
pub const PROVEN_DISTINCT_SITE: &str = "r2rml_scan_proven_distinct";

/// What a query knows about the keys of the tables a mapping reads.
///
/// A table's keys come from [`super::effective_table_keys`] (the mapping's
/// `f:key` declarations united with what the source reports), the one way a
/// lane obtains them: the source checks the declarations against the table's
/// schema there and refuses the query on one it cannot hold. They are fetched
/// lazily, once per table: [`Self::subject_unique`] answers from what has been
/// fetched and records each table it could not answer for, and
/// [`Self::fetch_wanted`] fetches those. A caller that cannot wait (the
/// synchronous rewrite) answers, fetches, and asks again.
#[derive(Debug, Default)]
pub struct TableKeyEvidence {
    /// The effective keys per table name, once fetched.
    known: HashMap<String, TableKeys>,
    /// Tables a question could not answer for before their keys are fetched.
    wanted: Mutex<BTreeSet<String>>,
}

impl TableKeyEvidence {
    /// Whether no two rows of `tm`'s logical table can mint the same subject,
    /// from the keys fetched for that table
    /// ([`TriplesMap::subject_unique_under`]). `false` when they have not been
    /// fetched yet; the table is then recorded for [`Self::fetch_wanted`].
    pub fn subject_unique(&self, tm: &TriplesMap) -> bool {
        let Some(table) = tm.table_name() else {
            return false;
        };
        match self.known.get(table) {
            Some(keys) => tm.subject_unique_under(keys),
            None => {
                self.wanted
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(table.to_string());
                false
            }
        }
    }

    /// Fetch the keys of every table a question wanted. Returns whether any of
    /// them has a key, i.e. whether asking the questions again can change an
    /// answer. A refusal from the source (a declared key the table cannot
    /// hold) is the caller's refusal.
    pub async fn fetch_wanted(
        &mut self,
        provider: &dyn super::R2rmlTableProvider,
        mapping: &CompiledR2rmlMapping,
        graph_source_id: &str,
        as_of_t: Option<i64>,
    ) -> Result<bool> {
        let wanted = std::mem::take(
            self.wanted
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut learned = false;
        for table in wanted {
            if self.known.contains_key(&table) {
                continue;
            }
            let keys =
                super::effective_table_keys(provider, mapping, graph_source_id, &table, as_of_t)
                    .await?;
            learned |= !keys.is_empty();
            self.known.insert(table, keys);
        }
        Ok(learned)
    }

    /// [`Self::subject_unique`], fetching the table's keys first if needed.
    pub async fn subject_unique_fetching(
        &mut self,
        provider: &dyn super::R2rmlTableProvider,
        mapping: &CompiledR2rmlMapping,
        graph_source_id: &str,
        as_of_t: Option<i64>,
        tm: &TriplesMap,
    ) -> Result<bool> {
        if self.subject_unique(tm) {
            return Ok(true);
        }
        self.fetch_wanted(provider, mapping, graph_source_id, as_of_t)
            .await?;
        Ok(self.subject_unique(tm))
    }
}

/// Whether one row of `tm` never yields the same solution of `pattern` twice.
///
/// Read off the solution shapes the scan materializes for each pattern kind:
///
/// - a variable predicate (`?s ?p ?o`, with or without a fused `?type`): each
///   `(predicate, object)` pair and class at most once
///   ([`TriplesMap::emits_distinct_triples_per_row`]);
/// - a projected type (`?s a ?t`): each class once;
/// - a subject-only scan (a class, or a constant object, or a star of constant
///   objects alone): at most one solution per row, always;
/// - an object variable, and each variable member of a same-subject star:
///   each of its predicate's objects once
///   ([`TriplesMap::emits_distinct_per_row`]), so their per-row cross product
///   repeats nothing either.
///
/// `false` for anything it cannot read, which only costs a deduplication.
pub(crate) fn emits_distinct_for(pattern: &R2rmlPattern, tm: &TriplesMap) -> bool {
    if pattern.predicate_var.is_some() {
        return tm.emits_distinct_triples_per_row();
    }
    if pattern.object_var.is_none() && pattern.star_bindings.is_empty() {
        return pattern.type_var.is_none() || tm.emits_distinct_per_row(fluree_vocab::rdf::TYPE);
    }
    let base = match (pattern.object_var, pattern.predicate_filter.as_deref()) {
        (Some(_), Some(p)) => Some(p),
        (Some(_), None) => return false,
        (None, _) => None,
    };
    base.into_iter()
        .chain(pattern.star_bindings.iter().map(|(p, _)| p.as_str()))
        .all(|p| tm.emits_distinct_per_row(p))
}

/// Estimated table overhead of one [`SeenSolutions`] entry: its slot (a boxed
/// slice's pointer and length), its control byte, and the load factor's slack.
const SEEN_ENTRY_OVERHEAD_BYTES: usize = 32;

/// The solutions one scan pass has emitted, for a pass whose maps can yield a
/// solution more than once.
///
/// Streaming and first-seen: a solution passes the first time it is seen, in
/// the scan's order, so a `LIMIT` above the scan still stops it early. Exact:
/// entries are the solutions themselves (bindings compared as the engine
/// compares terms), never a hash alone. Each entry is charged to the query's
/// memory budget as it is added; a set that outgrows the budget fails the
/// query with the typed `MemoryBudgetExceeded`, and [`Self::release`] returns
/// the charge when the pass ends.
pub(crate) struct SeenSolutions {
    seen: hashbrown::HashTable<Box<[(VarId, Binding)]>>,
    hasher: FxBuildHasher,
    charged: usize,
    since_checkpoint: usize,
}

/// How many entries a [`SeenSolutions`] adds between budget checkpoints.
const SEEN_CHECKPOINT_EVERY: usize = 1024;

impl SeenSolutions {
    pub(crate) fn new() -> Self {
        Self {
            seen: hashbrown::HashTable::new(),
            hasher: FxBuildHasher,
            charged: 0,
            since_checkpoint: 0,
        }
    }

    /// Whether `solution` is seen here for the first time; it is remembered
    /// either way.
    pub(crate) fn first_sight(
        &mut self,
        solution: &[(VarId, Binding)],
        ctx: &ExecutionContext<'_>,
    ) -> Result<bool> {
        let hasher = &self.hasher;
        let hash = hasher.hash_one(solution);
        if self
            .seen
            .find(hash, |entry| entry.as_ref() == solution)
            .is_some()
        {
            return Ok(false);
        }
        let bytes = entry_bytes(solution);
        self.seen.insert_unique(hash, solution.into(), |entry| {
            hasher.hash_one(entry.as_ref())
        });
        ctx.record_alloc(bytes);
        self.charged += bytes;
        self.since_checkpoint += 1;
        if self.since_checkpoint >= SEEN_CHECKPOINT_EVERY {
            self.since_checkpoint = 0;
            ctx.checkpoint()?;
        }
        Ok(true)
    }

    /// Return this set's charge to the query budget (it is about to drop).
    pub(crate) fn release(self, ctx: &ExecutionContext<'_>) {
        tracing::debug!(
            solutions = self.seen.len(),
            charged_bytes = self.charged,
            "R2RML scan: deduplication set released"
        );
        ctx.release(self.charged);
    }

    /// Bytes charged so far.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> usize {
        self.charged
    }

    /// Distinct solutions held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.seen.len()
    }
}

/// Estimated resident bytes of one remembered solution: its slots, plus the
/// text it keeps alive (an IRI's shared string, a string literal's copy).
fn entry_bytes(solution: &[(VarId, Binding)]) -> usize {
    SEEN_ENTRY_OVERHEAD_BYTES
        + std::mem::size_of_val(solution)
        + solution
            .iter()
            .map(|(_, b)| binding_heap_bytes(b))
            .sum::<usize>()
}

fn binding_heap_bytes(b: &Binding) -> usize {
    match b {
        // An `Arc<str>`: the text and its two counts.
        Binding::Iri(iri) => iri.len() + 2 * std::mem::size_of::<usize>(),
        Binding::Lit {
            val: fluree_db_core::FlakeValue::String(s),
            ..
        } => s.capacity(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};

    const NAME: &str = "http://example.org/name";
    const CITY: &str = "http://example.org/city";

    fn col(predicate: &str, column: &str) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::constant(predicate),
            object_map: ObjectMap::column(column),
        }
    }

    fn tm() -> TriplesMap {
        TriplesMap::new("http://example.org/m#M", "sets.customers_history")
            .with_subject_template("http://example.org/customer/{customer_id}")
            .with_class("http://example.org/Customer")
            .with_predicate_object(col(NAME, "name"))
            .with_predicate_object(col(CITY, "city"))
    }

    fn pattern() -> R2rmlPattern {
        R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)))
    }

    #[test]
    fn a_second_object_source_for_a_pattern_predicate_is_not_distinct() {
        let one = tm();
        let two = tm().with_predicate_object(col(NAME, "nickname"));
        let name = pattern().with_predicate(NAME);
        assert!(emits_distinct_for(&name, &one));
        assert!(!emits_distinct_for(&name, &two));
        // The star reads the name too, so the second source matters there;
        // a pattern on the city alone never reads it.
        let star = pattern()
            .with_predicate(CITY)
            .with_star_bindings(vec![(NAME.to_string(), VarId(2))]);
        assert!(emits_distinct_for(&star, &one));
        assert!(!emits_distinct_for(&star, &two));
        assert!(emits_distinct_for(&pattern().with_predicate(CITY), &two));
        // A wildcard reads every predicate.
        let mut wildcard = pattern();
        wildcard.predicate_var = Some(VarId(3));
        assert!(emits_distinct_for(&wildcard, &one));
        assert!(!emits_distinct_for(&wildcard, &two));
    }

    #[test]
    fn a_subject_only_pattern_yields_one_solution_per_row() {
        let mut class = R2rmlPattern::new("gs:main", VarId(0), None);
        class.class_filter = Some("http://example.org/Customer".to_string());
        let two_names = tm().with_predicate_object(col(NAME, "nickname"));
        assert!(emits_distinct_for(&class, &two_names));
        // A projected type reads every class: two equal ones repeat.
        let mut typed = R2rmlPattern::new("gs:main", VarId(0), None);
        typed.type_var = Some(VarId(1));
        assert!(emits_distinct_for(&typed, &tm()));
        let twice = tm().with_class("http://example.org/Customer");
        assert!(!emits_distinct_for(&typed, &twice));
    }

    #[test]
    fn an_object_variable_without_a_predicate_is_never_distinct() {
        // No such scan is built today; refusing it is the safe answer.
        assert!(!emits_distinct_for(&pattern(), &tm()));
    }

    #[test]
    fn evidence_answers_from_fetched_keys_and_wants_the_rest() {
        let evidence = TableKeyEvidence::default();
        assert!(!evidence.subject_unique(&tm()));
        assert_eq!(
            evidence.wanted.lock().unwrap().iter().collect::<Vec<_>>(),
            vec!["sets.customers_history"]
        );

        // A declaration alone never settles it: only fetched keys do, since
        // fetching is where the source checks the declaration.
        let declared = tm().with_declared_key(["customer_id"]);
        let evidence = TableKeyEvidence::default();
        assert!(!evidence.subject_unique(&declared));

        let mut evidence = TableKeyEvidence::default();
        evidence.known.insert(
            "sets.customers_history".to_string(),
            TableKeys::new([["customer_id"]]),
        );
        assert!(evidence.subject_unique(&tm()));
        evidence
            .known
            .insert("sets.customers_history".to_string(), TableKeys::none());
        assert!(!evidence.subject_unique(&tm()));
        assert!(evidence.wanted.lock().unwrap().is_empty());
    }

    #[test]
    fn seen_solutions_pass_each_solution_once_and_charge_the_budget() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars)
            .with_cancellation(fluree_db_core::QueryCancellation::new());
        let row = |s: &str, n: &str| {
            vec![
                (VarId(0), Binding::iri(s)),
                (
                    VarId(1),
                    Binding::lit(
                        fluree_db_core::FlakeValue::String(n.to_string()),
                        fluree_db_core::Sid::new(2, "string"),
                    ),
                ),
            ]
        };
        let mut seen = SeenSolutions::new();
        let before = ctx.mem_used();
        assert!(seen.first_sight(&row("ex:1", "alice"), &ctx).unwrap());
        assert!(!seen.first_sight(&row("ex:1", "alice"), &ctx).unwrap());
        assert!(seen.first_sight(&row("ex:1", "alicia"), &ctx).unwrap());
        assert!(seen.first_sight(&row("ex:2", "alice"), &ctx).unwrap());
        // The same values under another variable are another solution.
        let mut swapped = row("ex:1", "alice");
        swapped[1].0 = VarId(7);
        assert!(seen.first_sight(&swapped, &ctx).unwrap());
        assert_eq!(seen.len(), 4);
        assert_eq!(ctx.mem_used() - before, seen.charged());
        assert!(seen.charged() >= 4 * 2 * std::mem::size_of::<(VarId, Binding)>());
        seen.release(&ctx);
        assert_eq!(ctx.mem_used(), before);
    }

    #[test]
    fn seen_solutions_past_the_budget_fail_typed() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let cancel = fluree_db_core::QueryCancellation::new();
        cancel.set_memory_limit(64 * 1024);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);
        let mut seen = SeenSolutions::new();
        let mut failed = None;
        for i in 0..10 * SEEN_CHECKPOINT_EVERY {
            let row = [(VarId(0), Binding::iri(format!("http://example.org/s/{i}")))];
            match seen.first_sight(&row, &ctx) {
                Ok(first) => assert!(first),
                Err(e) => {
                    failed = Some((i, e));
                    break;
                }
            }
        }
        let (at, err) = failed.expect("a set past the budget must fail the query");
        assert!(
            matches!(err, crate::error::QueryError::MemoryBudgetExceeded { .. }),
            "{err:?}"
        );
        // It fails at the first checkpoint past the budget, not at the end.
        assert!(at < 2 * SEEN_CHECKPOINT_EVERY, "failed only at entry {at}");
    }
}
