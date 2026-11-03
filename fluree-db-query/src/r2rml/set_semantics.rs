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

/// The size of the allocation that holds `n` bytes: the allocator hands out
/// small blocks in 16-byte classes.
pub(crate) fn alloc_size_class(n: usize) -> usize {
    n.div_ceil(16) * 16
}

/// A hash table of remembered keys, charged to the query's memory budget for
/// what it actually holds: the table's own allocation (every bucket's slot and
/// control byte, occupied or not) plus each key's heap allocation.
///
/// Growing the table allocates the new buckets while the old ones are still
/// live, so before growing it charges the new allocation on top of the old one
/// and checks the budget (a growth the budget cannot hold fails the query with
/// the typed `MemoryBudgetExceeded` before it allocates), then settles the
/// charge to the new allocation once the old one is freed. [`Self::release`]
/// returns everything when the table drops.
pub(crate) struct ChargedTable<K> {
    table: hashbrown::HashTable<K>,
    /// Bytes recorded against the budget and not yet released.
    charged: usize,
    /// The part of `charged` that is the table's own allocation.
    table_bytes: usize,
    /// The most this table has held at once, growth transients included.
    peak: usize,
    since_checkpoint: usize,
}

/// How many inserts a [`ChargedTable`] makes between budget checkpoints when
/// it does not grow (every growth checkpoints).
const CHARGED_TABLE_CHECKPOINT_EVERY: usize = 1024;

impl<K> ChargedTable<K> {
    pub(crate) fn new() -> Self {
        Self {
            table: hashbrown::HashTable::new(),
            charged: 0,
            table_bytes: 0,
            peak: 0,
            since_checkpoint: 0,
        }
    }

    /// Whether a key with `hash` satisfying `eq` is held.
    pub(crate) fn contains(&self, hash: u64, eq: impl FnMut(&K) -> bool) -> bool {
        self.table.find(hash, eq).is_some()
    }

    /// Hold `key` (not already held), whose own heap allocation is
    /// `key_heap` bytes, growing the table first if it is full.
    pub(crate) fn insert(
        &mut self,
        hash: u64,
        key: K,
        key_heap: usize,
        hasher: impl Fn(&K) -> u64,
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        if self.table.len() == self.table.capacity() {
            let old = self.table_bytes;
            let keys = self.charged - old;
            // The new allocation is about twice the old one; charge it before
            // allocating, with the old one still charged.
            let estimate = (2 * old).max(4 * std::mem::size_of::<K>() + 32);
            ctx.record_alloc(estimate);
            self.charged += estimate;
            self.peak = self.peak.max(self.charged);
            ctx.checkpoint()?;
            self.table.reserve(1, &hasher);
            let new = self.table.allocation_size();
            self.peak = self.peak.max(keys + old + new);
            // The old allocation is freed: the table's charge is the new one.
            let target = keys + new;
            if self.charged > target {
                ctx.release(self.charged - target);
            } else {
                ctx.record_alloc(target - self.charged);
            }
            self.charged = target;
            self.table_bytes = new;
        }
        self.table.insert_unique(hash, key, hasher);
        ctx.record_alloc(key_heap);
        self.charged += key_heap;
        self.peak = self.peak.max(self.charged);
        self.since_checkpoint += 1;
        if self.since_checkpoint >= CHARGED_TABLE_CHECKPOINT_EVERY {
            self.since_checkpoint = 0;
            ctx.checkpoint()?;
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    /// Bytes charged now.
    pub(crate) fn charged(&self) -> usize {
        self.charged
    }

    /// The most this table has held at once.
    pub(crate) fn peak(&self) -> usize {
        self.peak
    }

    /// The table's own allocation, as the table reports it.
    #[cfg(test)]
    pub(crate) fn allocation_size(&self) -> usize {
        self.table.allocation_size()
    }

    /// Return the table's charge to the query budget (it is about to drop).
    pub(crate) fn release(self, ctx: &ExecutionContext<'_>) {
        ctx.release(self.charged);
    }
}

/// Longest key an [`ArenaSet`] holds (bytes): a longer one is the caller's to
/// keep another way.
const ARENA_MAX_KEY: usize = (1 << 20) - 1;
/// The first arena chunk's size; each next chunk doubles, up to
/// [`ARENA_MAX_CHUNK`].
const ARENA_FIRST_CHUNK: usize = 4 << 10;
const ARENA_MAX_CHUNK: usize = 16 << 20;

/// A set of byte keys kept in one arena, charged to the query's memory budget
/// for exactly what it holds.
///
/// Keys are appended to chunks (4 KiB doubling to 16 MiB; a key never spans
/// two), so a key costs its bytes and nothing per key beyond them, and adding
/// a chunk copies nothing. The table holds one `u64` per key locating it
/// (chunk, offset, length). The charge is the table's allocation, as hashbrown
/// reports it, plus every chunk's capacity: what the set holds, not an
/// estimate. A new chunk, and a table growth (the old buckets and the new ones
/// are live at once), are charged and the budget checked before they allocate,
/// so a set the budget cannot hold fails the query with the typed
/// `MemoryBudgetExceeded` before it grows.
pub(crate) struct ArenaSet {
    chunks: Vec<Vec<u8>>,
    table: hashbrown::HashTable<u64>,
    /// Bytes recorded against the budget and not yet released.
    charged: usize,
    /// The part of `charged` that is the table's own allocation.
    table_bytes: usize,
    /// The most this set has held at once, growth transients included.
    peak: usize,
    since_checkpoint: usize,
}

impl ArenaSet {
    pub(crate) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            table: hashbrown::HashTable::new(),
            charged: 0,
            table_bytes: 0,
            peak: 0,
            since_checkpoint: 0,
        }
    }

    fn slot(chunk: usize, offset: usize, len: usize) -> u64 {
        ((chunk as u64) << 44) | ((offset as u64) << 20) | len as u64
    }

    fn key_at(chunks: &[Vec<u8>], slot: u64) -> &[u8] {
        let chunk = (slot >> 44) as usize;
        let offset = ((slot >> 20) & ((1 << 24) - 1)) as usize;
        let len = (slot & ((1 << 20) - 1)) as usize;
        &chunks[chunk][offset..offset + len]
    }

    fn charge(&mut self, bytes: usize, ctx: &ExecutionContext<'_>) {
        ctx.record_alloc(bytes);
        self.charged += bytes;
        self.peak = self.peak.max(self.charged);
    }

    /// Hold `key` unless it is held already: `Some(true)` when it is new,
    /// `Some(false)` when it was held, `None` when it is longer than this set
    /// holds (it is not held then; a caller keeps such a key another way, or
    /// not at all). `hash` is `hasher` over the key's bytes.
    pub(crate) fn insert_new(
        &mut self,
        key: &[u8],
        hash: u64,
        hasher: &impl BuildHasher,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        let chunks = &self.chunks;
        if self
            .table
            .find(hash, |&slot| Self::key_at(chunks, slot) == key)
            .is_some()
        {
            return Ok(Some(false));
        }
        if key.len() > ARENA_MAX_KEY {
            return Ok(None);
        }
        if self.table.len() == self.table.capacity() {
            let old = self.table_bytes;
            // The new allocation is about twice the old one, live beside it.
            let estimate = (2 * old).max(64);
            self.charge(estimate, ctx);
            ctx.checkpoint()?;
            let chunks = &self.chunks;
            self.table
                .reserve(1, |&slot| hasher.hash_one(Self::key_at(chunks, slot)));
            let new = self.table.allocation_size();
            self.peak = self.peak.max(self.charged - estimate + new);
            // The old allocation is freed: the table's charge is the new one.
            let target = self.charged - estimate - old + new;
            if self.charged > target {
                ctx.release(self.charged - target);
            } else {
                ctx.record_alloc(target - self.charged);
            }
            self.charged = target;
            self.table_bytes = new;
        }
        let fits = self
            .chunks
            .last()
            .is_some_and(|c| c.capacity() - c.len() >= key.len());
        if !fits {
            let size = self
                .chunks
                .last()
                .map_or(ARENA_FIRST_CHUNK, |c| {
                    (2 * c.capacity()).min(ARENA_MAX_CHUNK)
                })
                .max(key.len());
            // A slot's offset field holds up to 16 MiB, and its chunk field
            // 2^20 chunks: 16 TiB, far past any budget.
            self.charge(size + std::mem::size_of::<Vec<u8>>(), ctx);
            ctx.checkpoint()?;
            self.chunks.push(Vec::with_capacity(size));
        }
        let chunk = self.chunks.len() - 1;
        let last = &mut self.chunks[chunk];
        let offset = last.len();
        last.extend_from_slice(key);
        let chunks = &self.chunks;
        self.table
            .insert_unique(hash, Self::slot(chunk, offset, key.len()), |&slot| {
                hasher.hash_one(Self::key_at(chunks, slot))
            });
        self.since_checkpoint += 1;
        if self.since_checkpoint >= CHARGED_TABLE_CHECKPOINT_EVERY {
            self.since_checkpoint = 0;
            ctx.checkpoint()?;
        }
        Ok(Some(true))
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    /// Bytes charged now.
    pub(crate) fn charged(&self) -> usize {
        self.charged
    }

    /// The most this set has held at once.
    pub(crate) fn peak(&self) -> usize {
        self.peak
    }

    /// What the set holds now, by its own accounts: the table's allocation
    /// and every chunk's capacity.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.table.allocation_size()
            + self
                .chunks
                .iter()
                .map(|c| c.capacity() + std::mem::size_of::<Vec<u8>>())
                .sum::<usize>()
    }

    /// Return the set's charge to the query budget (it is about to drop).
    pub(crate) fn release(self, ctx: &ExecutionContext<'_>) {
        ctx.release(self.charged);
    }
}

/// The solutions one scan pass has emitted, for a pass whose maps can yield a
/// solution more than once.
///
/// Streaming and first-seen: a solution passes the first time it is seen, in
/// the scan's order, so a `LIMIT` above the scan still stops it early. Exact:
/// entries identify the solutions themselves (never a hash alone). A solution
/// is kept as its byte encoding ([`encode_solution`]) in an [`ArenaSet`], or,
/// when one of its terms has no exact encoding (or the encoding is longer than
/// the arena holds), as its terms. Whether a solution takes the first form
/// depends only on which terms it holds, and equal terms are of one kind, so
/// two equal solutions always take the same form and the two sets never need
/// comparing. Both are charged to the query's memory budget for what they
/// hold; a set that outgrows the budget fails the query with the typed
/// `MemoryBudgetExceeded`, and [`Self::release`] returns the charge when the
/// pass ends.
pub(crate) struct SeenSolutions {
    compact: ArenaSet,
    terms: ChargedTable<Box<[(VarId, Binding)]>>,
    hasher: FxBuildHasher,
    /// A prefix every subject the pass's maps mint starts with; IRIs that
    /// start with it are remembered without it.
    strip: String,
    /// Datatypes and language tags met so far, remembered by position.
    dtcs: Vec<DatatypeConstraint>,
    scratch: Vec<u8>,
}

impl SeenSolutions {
    /// A set whose subjects all start with `strip` (any prefix is exact; a
    /// longer one only saves more).
    pub(crate) fn new(strip: impl Into<String>) -> Self {
        Self {
            compact: ArenaSet::new(),
            terms: ChargedTable::new(),
            hasher: FxBuildHasher,
            strip: strip.into(),
            dtcs: Vec::new(),
            scratch: Vec::new(),
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
        let held = if compact {
            let hash = self.hasher.hash_one(scratch.as_slice());
            self.compact.insert_new(&scratch, hash, &self.hasher, ctx)?
        } else {
            None
        };
        self.scratch = scratch;
        if let Some(new) = held {
            return Ok(new);
        }
        let hasher = &self.hasher;
        let hash = hasher.hash_one(solution);
        if self.terms.contains(hash, |terms| **terms == *solution) {
            return Ok(false);
        }
        self.terms.insert(
            hash,
            solution.into(),
            terms_heap_bytes(solution),
            |terms| hasher.hash_one(&**terms),
            ctx,
        )?;
        Ok(true)
    }

    /// Return this set's charge to the query budget (it is about to drop).
    pub(crate) fn release(self, ctx: &ExecutionContext<'_>) {
        tracing::debug!(
            solutions = self.compact.len() + self.terms.len(),
            charged_bytes = self.compact.charged() + self.terms.charged(),
            peak_bytes = self.compact.peak() + self.terms.peak(),
            "R2RML scan: deduplication set released"
        );
        self.compact.release(ctx);
        self.terms.release(ctx);
    }

    /// Bytes charged now.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> usize {
        self.compact.charged() + self.terms.charged()
    }

    /// Distinct solutions held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.compact.len() + self.terms.len()
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

/// The heap a solution remembered as its terms holds: its slots' allocation
/// and the text its terms keep alive (an IRI's shared string with its counts,
/// a string literal's copy).
fn terms_heap_bytes(solution: &[(VarId, Binding)]) -> usize {
    alloc_size_class(std::mem::size_of_val(solution))
        + solution
            .iter()
            .map(|(_, b)| match b {
                Binding::Iri(iri) => alloc_size_class(iri.len() + 2 * std::mem::size_of::<usize>()),
                Binding::Lit {
                    val: FlakeValue::String(s),
                    ..
                } => alloc_size_class(s.capacity()),
                _ => 0,
            })
            .sum::<usize>()
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
        // All four encode: they are held in the arena, charged for exactly
        // what it holds.
        assert_eq!(seen.compact.len(), 4);
        assert_eq!(seen.charged(), seen.compact.held());
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
        for i in 0..10 * CHARGED_TABLE_CHECKPOINT_EVERY {
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
        assert!(
            at < 2 * CHARGED_TABLE_CHECKPOINT_EVERY,
            "failed only at entry {at}"
        );
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

    fn budgeted(limit: Option<usize>) -> fluree_db_core::QueryCancellation {
        let cancel = fluree_db_core::QueryCancellation::new();
        if let Some(limit) = limit {
            cancel.set_memory_limit(limit);
        }
        cancel
    }

    /// Insert `n` distinct `u64` keys with no heap of their own; the result
    /// of each insert, and the table.
    fn fill(ctx: &ExecutionContext<'_>, n: u64) -> (Vec<Result<()>>, ChargedTable<u64>) {
        let mut table = ChargedTable::new();
        let hasher = FxBuildHasher;
        let mut results = Vec::new();
        for k in 0..n {
            let r = table.insert(hasher.hash_one(k), k, 0, |k| hasher.hash_one(*k), ctx);
            let failed = r.is_err();
            results.push(r);
            if failed {
                break;
            }
        }
        (results, table)
    }

    #[test]
    fn a_charged_table_charges_its_allocation_and_both_tables_while_growing() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(None));
        let (results, table) = fill(&ctx, 5000);
        assert!(results.iter().all(Result::is_ok));
        // Every bucket, occupied or not: what the table reports allocating.
        assert_eq!(table.charged(), table.allocation_size());
        assert_eq!(ctx.mem_used(), table.charged());
        // The last growth held the old buckets and the new ones at once.
        assert!(
            table.peak() >= table.allocation_size() + table.allocation_size() / 2 - 64,
            "peak {} for a final {}",
            table.peak(),
            table.allocation_size()
        );
        table.release(&ctx);
        assert_eq!(ctx.mem_used(), 0);
    }

    #[test]
    fn a_growth_the_budget_cannot_hold_fails_typed_before_it_allocates() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        // Unlimited, the 5000 keys end in one table; its last growth briefly
        // held that table and the one before it.
        let free = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(None));
        let (_, table) = fill(&free, 5000);
        let (fin, peak) = (table.allocation_size(), table.peak());
        assert!(peak > fin);
        // A budget the final table fits but the growth into it does not.
        let limit = fin + (peak - fin) / 2;
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(Some(limit)));
        let (results, _) = fill(&ctx, 5000);
        let err = results
            .last()
            .unwrap()
            .as_ref()
            .expect_err("the growth must fail");
        assert!(
            matches!(err, crate::error::QueryError::MemoryBudgetExceeded { .. }),
            "{err:?}"
        );
        assert!(results.len() < 5000);
    }

    fn arena_fill(
        ctx: &ExecutionContext<'_>,
        keys: impl Iterator<Item = Vec<u8>>,
    ) -> (Vec<Result<Option<bool>>>, ArenaSet) {
        let mut set = ArenaSet::new();
        let hasher = FxBuildHasher;
        let mut results = Vec::new();
        for key in keys {
            let r = set.insert_new(&key, hasher.hash_one(key.as_slice()), &hasher, ctx);
            let failed = r.is_err();
            results.push(r);
            if failed {
                break;
            }
        }
        (results, set)
    }

    fn key(i: u64) -> Vec<u8> {
        // Varying lengths: a key and a longer one sharing its prefix.
        let mut k = i.to_le_bytes().to_vec();
        k.truncate(1 + (i % 8) as usize);
        k.extend(std::iter::repeat_n(b'x', (i % 37) as usize));
        k
    }

    #[test]
    fn an_arena_set_holds_each_key_once_and_charges_exactly_what_it_holds() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(None));
        let (results, mut set) = arena_fill(&ctx, (0..20_000).map(key));
        let distinct: std::collections::HashSet<Vec<u8>> = (0..20_000).map(key).collect();
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(Some(true))))
                .count(),
            distinct.len()
        );
        assert_eq!(set.len(), distinct.len());
        assert_eq!(set.charged(), set.held());
        assert_eq!(ctx.mem_used(), set.charged());
        assert!(set.peak() > set.charged(), "a growth held both tables");
        // Held keys are found, by their bytes (lengths included).
        let hasher = FxBuildHasher;
        for k in [key(3), key(3 + 37 * 8), vec![1u8], vec![1u8, b'x']] {
            let r = set.insert_new(&k, hasher.hash_one(k.as_slice()), &hasher, &ctx);
            assert_eq!(r.unwrap(), Some(!distinct.contains(&k)), "{k:?}");
        }
        // A key longer than the arena holds is not held.
        let long = vec![7u8; ARENA_MAX_KEY + 1];
        let r = set.insert_new(&long, hasher.hash_one(long.as_slice()), &hasher, &ctx);
        assert_eq!(r.unwrap(), None);
        set.release(&ctx);
        assert_eq!(ctx.mem_used(), 0);
    }

    #[test]
    fn an_arena_growth_the_budget_cannot_hold_fails_typed_before_it_allocates() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("set-semantics:main");
        let vars = crate::var_registry::VarRegistry::new();
        let free = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(None));
        let (_, set) = arena_fill(&free, (0..20_000).map(key));
        let (fin, peak) = (set.held(), set.peak());
        assert!(peak > fin);
        let limit = fin + (peak - fin) / 2;
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(budgeted(Some(limit)));
        let (results, _) = arena_fill(&ctx, (0..20_000).map(key));
        let err = results
            .last()
            .unwrap()
            .as_ref()
            .expect_err("the growth must fail");
        assert!(
            matches!(err, crate::error::QueryError::MemoryBudgetExceeded { .. }),
            "{err:?}"
        );
    }
}
