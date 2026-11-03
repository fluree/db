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
use fluree_db_core::{DatatypeConstraint, FlakeValue};

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

#[cfg(test)]
impl TableKeyEvidence {
    /// Evidence that every table is keyed by the subject columns of each map
    /// over it, for tests of fusion mechanics over keyed tables.
    pub(crate) fn assume_keyed_by_subjects(mapping: &CompiledR2rmlMapping) -> Self {
        let mut known: HashMap<String, TableKeys> = HashMap::new();
        for tm in mapping.triples_maps.values() {
            if let Some(table) = tm.table_name() {
                let columns = subject_columns_of(tm);
                let keys = known.remove(table).unwrap_or_default().union([columns]);
                known.insert(table.to_string(), keys);
            }
        }
        Self {
            known,
            wanted: Mutex::default(),
        }
    }
}

#[cfg(test)]
impl TableKeyEvidence {
    /// Evidence that `table`'s keys were fetched and are `keys`.
    pub(crate) fn with_known(mut self, table: &str, keys: TableKeys) -> Self {
        self.known.insert(table.to_string(), keys);
        self
    }

    /// The tables a question wanted keys for, sorted.
    pub(crate) fn wanted_tables(&self) -> Vec<String> {
        self.wanted.lock().unwrap().iter().cloned().collect()
    }
}

/// `maps` with each one's subject columns declared a key of its table, for
/// tests that run lanes over tables whose rows hold one subject each.
#[cfg(test)]
pub(crate) fn keyed_by_subjects(maps: Vec<TriplesMap>) -> CompiledR2rmlMapping {
    CompiledR2rmlMapping::new(
        maps.into_iter()
            .map(|tm| {
                let columns = subject_columns_of(&tm);
                tm.with_declared_key(columns)
            })
            .collect(),
    )
}

/// The columns a map's subject reads, from its template text (maps built in
/// tests carry no loader-computed column list).
#[cfg(test)]
fn subject_columns_of(tm: &TriplesMap) -> Vec<String> {
    let sm = &tm.subject_map;
    let mut columns: Vec<String> = sm
        .template
        .as_deref()
        .map(fluree_db_r2rml::mapping::extract_template_columns)
        .unwrap_or_default();
    columns.extend(sm.column.clone());
    columns
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

/// The maps a scan of `pattern` can read: the pinned map, or every map
/// declaring its class and naming its predicate. A superset of what the scan
/// resolves (it prunes further), so a property every candidate has holds for
/// every map the scan reads.
pub(crate) fn candidate_maps<'m>(
    pattern: &R2rmlPattern,
    mapping: &'m CompiledR2rmlMapping,
) -> Vec<&'m TriplesMap> {
    if let Some(iri) = &pattern.triples_map_iri {
        return mapping.get(iri).into_iter().collect();
    }
    mapping
        .triples_maps
        .values()
        .filter(|tm| {
            pattern
                .class_filter
                .as_ref()
                .is_none_or(|c| tm.classes().contains(c))
                && pattern.predicate_filter.as_deref().is_none_or(|p| {
                    tm.predicate_object_maps
                        .iter()
                        .any(|pom| pom.predicate_map.as_constant() == Some(p))
                })
        })
        .collect()
}

/// Whether a lane that turns each row of each map `patterns` read into one
/// solution answers over the graph: for every pattern, every candidate map
/// ([`candidate_maps`]) mints each subject from one row and each solution
/// once per row, and no two candidates of one pattern can mint one subject.
/// Lanes that fold rows without materializing solutions (the fused aggregate,
/// its manifest COUNT) ask this before they run. A source that refuses a
/// table's declared keys refuses the query.
pub(crate) async fn rows_are_solutions(
    ctx: &ExecutionContext<'_>,
    patterns: &[crate::ir::Pattern],
    graph_source_id: &str,
    mapping: &CompiledR2rmlMapping,
) -> Result<bool> {
    let Some(provider) = ctx.r2rml_table_provider else {
        return Ok(false);
    };
    let as_of_t = if ctx.explicit_dataset().is_some() {
        None
    } else {
        Some(ctx.to_t)
    };
    let mut keys = TableKeyEvidence::default();
    for pattern in patterns {
        let crate::ir::Pattern::R2rml(pattern) = pattern else {
            return Ok(false);
        };
        let maps = candidate_maps(pattern, mapping);
        for (i, tm) in maps.iter().enumerate() {
            let unique = keys
                .subject_unique_fetching(provider, mapping, graph_source_id, as_of_t, tm)
                .await?;
            if !unique
                || !emits_distinct_for(pattern, tm)
                || maps[i + 1..]
                    .iter()
                    .any(|other| !tm.subjects_provably_disjoint(other))
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Estimated table overhead of one [`SeenSolutions`] entry: its slot (a boxed
/// slice's pointer and length, and the key's tag), its control byte, and the
/// load factor's slack.
const SEEN_ENTRY_OVERHEAD_BYTES: usize = 32;

/// One remembered solution.
///
/// `Compact` is a byte encoding of the solution ([`encode_solution`]) that two
/// solutions share exactly when the engine's term equality makes them equal;
/// a solution holding a term it cannot encode that way is kept as its terms.
/// Whether a solution encodes depends only on which terms it holds, and equal
/// terms are of one encodable kind or neither, so two equal solutions always
/// take the same form: the two forms never need comparing.
#[derive(PartialEq, Eq)]
enum SeenKey {
    Compact(Box<[u8]>),
    Terms(Box<[(VarId, Binding)]>),
}

/// The solutions one scan pass has emitted, for a pass whose maps can yield a
/// solution more than once.
///
/// Streaming and first-seen: a solution passes the first time it is seen, in
/// the scan's order, so a `LIMIT` above the scan still stops it early. Exact:
/// entries identify the solutions themselves (never a hash alone), compactly
/// where the encoding is exact. Each entry is charged to the query's memory
/// budget as it is added; a set that outgrows the budget fails the query with
/// the typed `MemoryBudgetExceeded`, and [`Self::release`] returns the charge
/// when the pass ends.
pub(crate) struct SeenSolutions {
    seen: hashbrown::HashTable<SeenKey>,
    hasher: FxBuildHasher,
    /// A prefix every subject the pass's maps mint starts with; IRIs that
    /// start with it are remembered without it.
    strip: String,
    /// Datatypes and language tags met so far, remembered by position.
    dtcs: Vec<DatatypeConstraint>,
    scratch: Vec<u8>,
    charged: usize,
    since_checkpoint: usize,
}

/// How many entries a [`SeenSolutions`] adds between budget checkpoints.
const SEEN_CHECKPOINT_EVERY: usize = 1024;

impl SeenSolutions {
    /// A set whose subjects all start with `strip` (any prefix is exact; a
    /// longer one only saves more).
    pub(crate) fn new(strip: impl Into<String>) -> Self {
        Self {
            seen: hashbrown::HashTable::new(),
            hasher: FxBuildHasher,
            strip: strip.into(),
            dtcs: Vec::new(),
            scratch: Vec::new(),
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
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        let compact = encode_solution(solution, &self.strip, &mut self.dtcs, &mut scratch);
        let hasher = &self.hasher;
        let (hash, found) = if compact {
            let hash = hasher.hash_one((0u8, scratch.as_slice()));
            let found = self.seen.find(
                hash,
                |entry| matches!(entry, SeenKey::Compact(bytes) if **bytes == *scratch),
            );
            (hash, found.is_some())
        } else {
            let hash = hasher.hash_one((1u8, solution));
            let found = self.seen.find(
                hash,
                |entry| matches!(entry, SeenKey::Terms(terms) if **terms == *solution),
            );
            (hash, found.is_some())
        };
        if found {
            self.scratch = scratch;
            return Ok(false);
        }
        let (key, bytes) = if compact {
            let bytes = SEEN_ENTRY_OVERHEAD_BYTES + scratch.len();
            (SeenKey::Compact(scratch.as_slice().into()), bytes)
        } else {
            (SeenKey::Terms(solution.into()), term_entry_bytes(solution))
        };
        self.scratch = scratch;
        self.seen.insert_unique(hash, key, |entry| match entry {
            SeenKey::Compact(bytes) => hasher.hash_one((0u8, &**bytes)),
            SeenKey::Terms(terms) => hasher.hash_one((1u8, &**terms)),
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

/// Append an exact byte encoding of `solution` to `out`, or return `false`
/// when one of its terms has none.
///
/// Encoded terms, each after its variable: an IRI's text (without `strip`
/// when it starts with it, tagged so the two forms never collide); a string
/// or boolean literal's value with its datatype or language tag (by its
/// position in `dtcs`); an integer under an integer datatype; an unbound
/// variable. Each encodes one class of the engine's term equality: a string
/// or boolean literal equals only a literal of the same kind, datatype and
/// value, and an in-range integer under an integer datatype is only ever an
/// integer. Other numerics compare by value across representations (`1`
/// equals `1.0`), temporals by instant, so a solution holding one is kept as
/// its terms.
fn encode_solution(
    solution: &[(VarId, Binding)],
    strip: &str,
    dtcs: &mut Vec<DatatypeConstraint>,
    out: &mut Vec<u8>,
) -> bool {
    for (var, binding) in solution {
        out.extend_from_slice(&var.0.to_le_bytes());
        match binding {
            Binding::Unbound => out.push(0),
            Binding::Iri(iri) => match iri.strip_prefix(strip) {
                Some(rest) if !strip.is_empty() => {
                    out.push(1);
                    put_bytes(out, rest.as_bytes());
                }
                _ => {
                    out.push(2);
                    put_bytes(out, iri.as_bytes());
                }
            },
            Binding::Lit { val, dtc, .. } => {
                let tag = match val {
                    FlakeValue::String(_) => 3,
                    FlakeValue::Boolean(_) => 4,
                    FlakeValue::Long(_) if is_integer_datatype(dtc) => 5,
                    _ => return false,
                };
                out.push(tag);
                let id = match dtcs.iter().position(|d| d == dtc) {
                    Some(id) => id,
                    None => {
                        dtcs.push(dtc.clone());
                        dtcs.len() - 1
                    }
                };
                put_len(out, id);
                match val {
                    FlakeValue::String(s) => put_bytes(out, s.as_bytes()),
                    FlakeValue::Boolean(b) => out.push(u8::from(*b)),
                    FlakeValue::Long(n) => out.extend_from_slice(&n.to_le_bytes()),
                    _ => unreachable!("tagged above"),
                }
            }
            _ => return false,
        }
    }
    true
}

/// Whether a literal under `dtc` holding an in-range integer is always a
/// `Long`: the integer datatypes the natural mapping of integer columns uses.
fn is_integer_datatype(dtc: &DatatypeConstraint) -> bool {
    match dtc {
        DatatypeConstraint::Explicit(sid) => {
            sid.namespace_code == fluree_vocab::namespaces::XSD
                && matches!(&*sid.name, "integer" | "long" | "int")
        }
        DatatypeConstraint::LangTag(_) => false,
    }
}

/// A length, as a LEB128 varint.
fn put_len(out: &mut Vec<u8>, mut n: usize) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

/// Estimated resident bytes of one solution remembered as its terms: its
/// slots, plus the text it keeps alive (an IRI's shared string, a string
/// literal's copy).
fn term_entry_bytes(solution: &[(VarId, Binding)]) -> usize {
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
            val: FlakeValue::String(s),
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
        let mut seen = SeenSolutions::new("ex:");
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
        // Remembered compactly: well under the solutions' term form.
        let as_terms: usize = (0..4)
            .map(|_| term_entry_bytes(&row("ex:1", "alice")))
            .sum();
        assert!(
            seen.charged() * 2 < as_terms,
            "{} vs {as_terms}",
            seen.charged()
        );
        seen.release(&ctx);
        assert_eq!(ctx.mem_used(), before);
    }

    #[test]
    fn seen_solutions_past_the_budget_fail_typed() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let cancel = fluree_db_core::QueryCancellation::new();
        cancel.set_memory_limit(32 * 1024);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);
        let mut seen = SeenSolutions::new("http://example.org/s/");
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

    fn lit(val: FlakeValue, dt: &str) -> Binding {
        Binding::lit(
            val,
            fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, dt),
        )
    }

    /// Encode as one set does: datatypes are numbered in a table the set's
    /// entries share.
    fn encoded_in(
        dtcs: &std::cell::RefCell<Vec<DatatypeConstraint>>,
        solution: &[(VarId, Binding)],
        strip: &str,
    ) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        encode_solution(solution, strip, &mut dtcs.borrow_mut(), &mut out).then_some(out)
    }

    fn encoded(solution: &[(VarId, Binding)], strip: &str) -> Option<Vec<u8>> {
        encoded_in(&std::cell::RefCell::default(), solution, strip)
    }

    #[test]
    fn the_compact_encoding_separates_exactly_what_term_equality_separates() {
        let s = |iri: &str| (VarId(0), Binding::iri(iri));
        let o = |b: Binding| (VarId(1), b);
        let strip = "http://example.org/customer/";
        let dtcs = std::cell::RefCell::default();
        let enc = |sol: &[(VarId, Binding)]| encoded_in(&dtcs, sol, strip).expect("encodes");
        let alice = o(lit(FlakeValue::String("alice".into()), "string"));
        let base = [s("http://example.org/customer/1"), alice.clone()];
        assert_eq!(enc(&base), enc(&base.clone()));
        // A subject outside the prefix whose text equals a stripped suffix.
        assert_ne!(enc(&base), enc(&[s("1"), alice.clone()]));
        // Boundaries are length-prefixed.
        let a = [
            s("http://example.org/customer/ab"),
            o(lit(FlakeValue::String("c".into()), "string")),
        ];
        let b = [
            s("http://example.org/customer/a"),
            o(lit(FlakeValue::String("bc".into()), "string")),
        ];
        assert_ne!(enc(&a), enc(&b));
        // Datatype and language are part of the term.
        let en = o(Binding::lit_lang(FlakeValue::String("alice".into()), "en"));
        assert_ne!(enc(&base), enc(&[base[0].clone(), en]));
        assert_ne!(
            enc(&base),
            enc(&[
                base[0].clone(),
                o(lit(FlakeValue::String("alice".into()), "token"))
            ])
        );
        // The same values under other variables are another solution.
        assert_ne!(
            enc(&base),
            enc(&[(VarId(5), base[0].1.clone()), alice.clone()])
        );
        // Integers under an integer datatype encode; a string "1" is not 1.
        let one = enc(&[s("x"), o(lit(FlakeValue::Long(1), "integer"))]);
        assert_ne!(
            one,
            enc(&[s("x"), o(lit(FlakeValue::String("1".into()), "integer"))])
        );
        assert_ne!(one, enc(&[s("x"), o(lit(FlakeValue::Long(1), "long"))]));
    }

    #[test]
    fn terms_compared_by_value_across_representations_are_kept_as_terms() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let s = (VarId(0), Binding::iri("http://example.org/customer/1"));
        let long = lit(FlakeValue::Long(1), "decimal");
        let decimal = lit(
            FlakeValue::Decimal(Box::new("1.0".parse().unwrap())),
            "decimal",
        );
        // 1 and 1.0 under xsd:decimal are one term to the engine...
        assert_eq!(long, decimal);
        // ...so neither encodes compactly, and the set sees one solution.
        assert!(encoded(&[s.clone(), (VarId(1), long.clone())], "").is_none());
        assert!(encoded(&[s.clone(), (VarId(1), decimal.clone())], "").is_none());
        let double = lit(FlakeValue::Double(2.5), "double");
        assert!(encoded(&[s.clone(), (VarId(1), double)], "").is_none());
        let mut seen = SeenSolutions::new("http://example.org/customer/");
        assert!(seen
            .first_sight(&[s.clone(), (VarId(1), long)], &ctx)
            .unwrap());
        assert!(!seen.first_sight(&[s, (VarId(1), decimal)], &ctx).unwrap());
    }
}
