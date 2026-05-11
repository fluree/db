//! Fulltext scoring function implementation
//!
//! Implements `fulltext(?content, "query")` — a scoring function for
//! `@fulltext`-typed literals. Returns a numeric score (f64) for `@fulltext`
//! values, `0.0` when no terms match, or `None` (unbound) for non-`@fulltext` values.
//!
//! Predicate scoping is implicit: when `?content` is bound from a where-clause
//! pattern, the binding is an `EncodedLit` which carries `p_id` and `dt_id`.
//! The function reads the raw `Binding` to access this metadata rather than
//! going through `eval_to_comparable()` which strips it.
//!
//! **Scoring strategy**:
//! - When a `FulltextArena` is available for `(g_id, p_id)`, uses unified BM25
//!   scoring with **effective corpus stats** = arena stats + novelty delta.
//!   Both indexed docs (from arena BoW) and novelty docs (analyzed on the fly)
//!   are scored with the same formula, ensuring consistent ranking.
//! - Falls back to per-document TF-saturation when no arena is available
//!   (e.g., novelty-only data not yet indexed, or Binding::Lit without p_id).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::binding::RowAccess;
use crate::bm25::analyzer::Analyzer;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;

use super::helpers::check_arity;
use super::value::ComparableValue;

use fluree_db_binary_index::analyzer::Language;
use fluree_db_binary_index::arena::fulltext::DocBoW;
use fluree_db_binary_index::FulltextArena;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{FlakeValue, OverlayProvider, Sid};
use fluree_vocab::namespaces::FLUREE_DB;

/// Lazily-initialized English analyzer (reused for the `@fulltext` datatype
/// path which is always English — kept for backward compatibility of
/// non-config callers; new code should prefer `Analyzer::for_language(...)`).
static ENGLISH_ANALYZER: Lazy<Analyzer> = Lazy::new(Analyzer::english_default);

/// BM25 parameters
const K1: f64 = 1.2;
const B: f64 = 0.75;

/// Analyzer version constant — bump when the analyzer changes to invalidate cached deltas.
const ANALYZER_VERSION: u8 = 1;

// =============================================================================
// Novelty delta types
// =============================================================================

/// Novelty delta for a single (g_id, p_id) fulltext predicate.
///
/// Built by scanning overlay flakes and analyzing @fulltext strings.
/// Cached per (epoch, to_t, g_id, p_id, analyzer_version).
struct NoveltyFulltextDelta {
    /// Precomputed delta_df per term: net change in document frequency.
    /// Built once during delta construction; O(1) lookup at scoring time.
    delta_df: HashMap<String, i64>,
    /// Aggregate: net change in triple count.
    delta_n: i64,
    /// Aggregate: net change in sum of (triple_count × doc_len).
    delta_sum_dl: i64,
}

struct NoveltyDocEntry {
    /// Analyzed term → frequency for this string.
    term_freqs: HashMap<String, u32>,
    /// Total term count (sum of all TF values).
    doc_len: u32,
    /// Net assertions - retractions for this string_id (after de-staling).
    triple_count_delta: i64,
}

// =============================================================================
// Novelty delta cache
// =============================================================================

#[derive(Clone)]
struct NoveltyCacheKey {
    /// Hash of the ledger ID — discriminates across ledgers sharing the process.
    ledger_id_hash: u64,
    epoch: u64,
    to_t: i64,
    g_id: fluree_db_core::GraphId,
    p_id: u32,
    /// Arena bucket lang_id — deltas are built per (g_id, p_id, lang_id)
    /// because the analyzer and overlay filter both depend on language.
    lang_id: u16,
    analyzer_version: u8,
}

impl PartialEq for NoveltyCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.ledger_id_hash == other.ledger_id_hash
            && self.epoch == other.epoch
            && self.to_t == other.to_t
            && self.g_id == other.g_id
            && self.p_id == other.p_id
            && self.lang_id == other.lang_id
            && self.analyzer_version == other.analyzer_version
    }
}
impl Eq for NoveltyCacheKey {}

impl Hash for NoveltyCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ledger_id_hash.hash(state);
        self.epoch.hash(state);
        self.to_t.hash(state);
        self.g_id.hash(state);
        self.p_id.hash(state);
        self.lang_id.hash(state);
        self.analyzer_version.hash(state);
    }
}

static NOVELTY_CACHE: Lazy<moka::sync::Cache<NoveltyCacheKey, Arc<NoveltyFulltextDelta>>> =
    Lazy::new(|| moka::sync::Cache::builder().max_capacity(64).build());

// =============================================================================
// Delta build
// =============================================================================

/// Build a novelty delta by scanning overlay flakes for a specific
/// `(g_id, p_id, lang_id)` bucket.
///
/// Filter semantics:
/// - Every flake with `flake.p == target_p_sid` is a candidate.
/// - `@fulltext`-datatype flakes are routed to the English bucket only
///   (their arena is always keyed under the dict-assigned `"en"` lang_id,
///   regardless of any per-row lang metadata).
/// - Non-`@fulltext` flakes are routed to the bucket whose language matches
///   the flake's `rdf:langString` tag. Untagged (`xsd:string`) values fall
///   into the English bucket.
///
/// Analyzer selection: one analyzer per bucket, chosen from
/// `Language::from_bcp47(bucket_lang_tag)`. The per-row analyzer choice is
/// implicit — since all rows that survive the filter share the bucket's
/// language, the analyzer is fixed for the whole scan.
///
/// De-stales overlay ops: keeps only the latest op (by `t`) per triple
/// `(subject, string_id, list_index)`, so re-assertions and retractions
/// are counted correctly.
fn build_novelty_delta(
    overlay: &dyn OverlayProvider,
    binary_store: &fluree_db_binary_index::BinaryIndexStore,
    dict_novelty: Option<&fluree_db_core::DictNovelty>,
    g_id: fluree_db_core::GraphId,
    target_p_sid: &Sid,
    to_t: i64,
    bucket_lang_tag: &str,
) -> NoveltyFulltextDelta {
    let fulltext_dt_sid = Sid::new(FLUREE_DB, "fullText");
    let bucket_language = Language::from_bcp47(bucket_lang_tag);
    let bucket_is_english = bucket_language == Language::English;

    // Phase 1: Scan overlay and de-stale — keep latest op per triple.
    // Triple key: (subject Sid, string_id, list_index) — collision-free.
    // List index distinguishes multiple values of the same string on one subject
    // (e.g., JSON-LD arrays where the same text appears at different positions).
    // Value: (t, op, string_text)
    type TripleKey = (Sid, u32, Option<i32>);
    let mut latest_ops: HashMap<TripleKey, (i64, bool, String)> = HashMap::new();

    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Psot,
        None,
        None,
        true,
        to_t,
        &mut |flake| {
            // Filter by predicate (Sid comparison)
            if flake.p != *target_p_sid {
                return;
            }

            let is_datatype_fulltext = flake.dt == fulltext_dt_sid;
            // Compute which bucket this flake belongs to:
            //   - @fulltext datatype → English bucket only.
            //   - Else lang-tagged string → bucket matching the tag.
            //   - Else untagged string → English bucket.
            // A flake that doesn't match the caller's bucket is skipped.
            let belongs_to_this_bucket = if is_datatype_fulltext {
                bucket_is_english
            } else {
                match flake.m.as_ref().and_then(|m| m.lang.as_deref()) {
                    Some(tag) => Language::from_bcp47(tag) == bucket_language,
                    None => bucket_is_english,
                }
            };
            if !belongs_to_this_bucket {
                return;
            }

            // Extract string content
            let text = match &flake.o {
                FlakeValue::String(s) => s.clone(),
                _ => return,
            };

            // Resolve string_id
            let string_id = resolve_string_id(binary_store, dict_novelty, &text);
            let Some(string_id) = string_id else {
                tracing::debug!(text = %text, "fulltext delta: could not resolve string_id");
                return;
            };

            // Include list index for multi-valued properties
            let list_idx = flake.m.as_ref().and_then(|m| m.i);
            let triple_key = (flake.s.clone(), string_id, list_idx);

            // Keep only the latest op (highest t)
            match latest_ops.entry(triple_key) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((flake.t, flake.op, text));
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if flake.t > e.get().0 {
                        e.insert((flake.t, flake.op, text));
                    }
                }
            }
        },
    );

    // Phase 2: Build per-doc entries from de-staled ops.
    // Group by string_id, accumulate triple_count_delta per string_id.
    let analyzer = Analyzer::for_language(bucket_language);
    let mut doc_entries: HashMap<u32, NoveltyDocEntry> = HashMap::new();

    for ((_, string_id, _), (_t, op, text)) in &latest_ops {
        let entry = doc_entries.entry(*string_id).or_insert_with(|| {
            let term_freqs = analyzer.analyze_to_term_freqs(text);
            let doc_len = term_freqs.values().sum::<u32>();
            NoveltyDocEntry {
                term_freqs,
                doc_len,
                triple_count_delta: 0,
            }
        });
        if *op {
            entry.triple_count_delta += 1;
        } else {
            entry.triple_count_delta -= 1;
        }
    }

    // Phase 3: Compute aggregates and precompute delta_df.
    let mut delta_n: i64 = 0;
    let mut delta_sum_dl: i64 = 0;

    // Remove entries with zero net delta (no-ops)
    doc_entries.retain(|_string_id, entry| {
        if entry.triple_count_delta == 0 {
            return false;
        }
        delta_n += entry.triple_count_delta;
        delta_sum_dl += entry.triple_count_delta * entry.doc_len as i64;
        true
    });

    // Precompute delta_df: for each term, sum triple_count_delta across docs containing it.
    // This makes per-row scoring O(query_terms) instead of O(query_terms × novelty_docs).
    let mut delta_df: HashMap<String, i64> = HashMap::new();
    for entry in doc_entries.values() {
        for term in entry.term_freqs.keys() {
            *delta_df.entry(term.clone()).or_insert(0) += entry.triple_count_delta;
        }
    }

    NoveltyFulltextDelta {
        delta_df,
        delta_n,
        delta_sum_dl,
    }
}

/// Resolve string text → string_id using persisted dict then novelty dict.
fn resolve_string_id(
    binary_store: &fluree_db_binary_index::BinaryIndexStore,
    dict_novelty: Option<&fluree_db_core::DictNovelty>,
    text: &str,
) -> Option<u32> {
    // Try persisted reverse dict first
    if let Ok(Some(id)) = binary_store.find_string_id(text) {
        return Some(id);
    }
    // Try novelty dict
    if let Some(dn) = dict_novelty {
        if let Some(id) = dn.strings.find_string(text) {
            return Some(id);
        }
    }
    None
}

/// Get or build the novelty delta for `(g_id, p_id, lang_id)`, using the
/// global cache.
///
/// `lang_id` is the arena-bucket lang_id the caller is about to score
/// against. The delta's overlay filter and analyzer both key off the
/// BCP-47 tag associated with that lang_id — resolved via the binary
/// store's language dict. Missing tag falls back to `"en"`.
fn get_or_build_delta(
    ctx: &ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    p_id: u32,
    lang_id: u16,
) -> Option<Arc<NoveltyFulltextDelta>> {
    let overlay = ctx.overlay?;
    let epoch = overlay.epoch();
    if epoch == 0 {
        return None; // No novelty
    }
    let binary_store = ctx.binary_store.as_ref()?;

    // Resolve target predicate Sid for overlay filtering
    let pred_iri = binary_store.resolve_predicate_iri(p_id)?;
    let target_p_sid = binary_store.encode_iri(pred_iri);

    // Resolve the bucket's BCP-47 tag once. `lang_id == 0` shouldn't appear
    // as a bucket key (buckets are always keyed by real dict-assigned IDs),
    // but if it does we default to English to mirror the arena-build path.
    let bucket_lang_tag: String = if lang_id == 0 {
        "en".to_string()
    } else {
        binary_store
            .resolve_language_tag(lang_id)
            .unwrap_or_else(|| "en".to_string())
    };

    // Hash the ledger_id to discriminate across ledgers sharing the process cache.
    let ledger_id_hash = {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        ctx.active_snapshot.ledger_id.hash(&mut h);
        h.finish()
    };

    let key = NoveltyCacheKey {
        ledger_id_hash,
        epoch,
        to_t: ctx.to_t,
        g_id,
        p_id,
        lang_id,
        analyzer_version: ANALYZER_VERSION,
    };

    let dict_novelty = ctx.dict_novelty.clone();
    let store = Arc::clone(binary_store);
    let to_t = ctx.to_t;

    Some(NOVELTY_CACHE.get_with(key, || {
        Arc::new(build_novelty_delta(
            overlay,
            &store,
            dict_novelty.as_deref(),
            g_id,
            &target_p_sid,
            to_t,
            &bucket_lang_tag,
        ))
    }))
}

// =============================================================================
// Unified BM25 scoring
// =============================================================================

/// Compute effective corpus stats from arena + optional novelty delta.
struct EffectiveStats {
    n: f64,
    avgdl: f64,
}

fn compute_effective_stats(
    arena: &FulltextArena,
    delta: Option<&NoveltyFulltextDelta>,
) -> EffectiveStats {
    let delta_n = delta.map(|d| d.delta_n).unwrap_or(0);
    let delta_sum_dl = delta.map(|d| d.delta_sum_dl).unwrap_or(0);
    let n = (arena.stats().n as i64 + delta_n).max(1) as f64;
    let sum_dl = (arena.stats().sum_dl as i64 + delta_sum_dl).max(1) as f64;
    EffectiveStats {
        n,
        avgdl: sum_dl / n,
    }
}

/// Compute effective df for a query term: arena_df + delta_df.
///
/// Uses the precomputed `delta_df` map for O(1) lookup per term.
fn effective_df(
    term: &str,
    arena: &FulltextArena,
    delta: Option<&NoveltyFulltextDelta>,
    n_prime: f64,
) -> f64 {
    let arena_df = arena
        .term_id(term)
        .map(|tid| arena.stats().df[tid as usize] as i64)
        .unwrap_or(0);

    let delta_df_val = delta
        .map(|d| d.delta_df.get(term).copied().unwrap_or(0))
        .unwrap_or(0);

    // Clamp to [0, N']
    let df = (arena_df + delta_df_val).max(0) as f64;
    df.min(n_prime)
}

/// Score an indexed doc (from arena DocBoW) with unified BM25.
///
/// Iterates query terms directly against DocBoW via binary search —
/// no HashMap<String,u32> allocation per row.
fn score_bm25_indexed(
    arena: &FulltextArena,
    doc_bow: &DocBoW,
    query_terms: &[String],
    delta: Option<&NoveltyFulltextDelta>,
) -> f64 {
    let stats = compute_effective_stats(arena, delta);
    let dl = doc_bow.doc_len as f64;
    let mut score = 0.0;

    for term in query_terms {
        let df = effective_df(term, arena, delta, stats.n);
        let idf = ((stats.n - df + 0.5) / (df + 0.5) + 1.0).ln();

        // Look up TF via arena term_id → binary search in DocBoW.terms
        let tf = arena
            .term_id(term)
            .and_then(|tid| doc_bow.terms.binary_search_by_key(&tid, |(id, _)| *id).ok())
            .map(|idx| doc_bow.terms[idx].1 as f64)
            .unwrap_or(0.0);

        if tf > 0.0 {
            let tf_norm = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / stats.avgdl));
            score += idf * tf_norm;
        }
    }

    score
}

/// Score a novelty/decoded doc with unified BM25 using explicit term_freqs.
fn score_bm25_novelty(
    doc_term_freqs: &HashMap<String, u32>,
    doc_len: u32,
    query_terms: &[String],
    arena: &FulltextArena,
    delta: Option<&NoveltyFulltextDelta>,
) -> f64 {
    let stats = compute_effective_stats(arena, delta);
    let dl = doc_len as f64;
    let mut score = 0.0;

    for term in query_terms {
        let df = effective_df(term, arena, delta, stats.n);
        let idf = ((stats.n - df + 0.5) / (df + 0.5) + 1.0).ln();

        let tf = doc_term_freqs.get(term.as_str()).copied().unwrap_or(0) as f64;

        if tf > 0.0 {
            let tf_norm = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / stats.avgdl));
            score += idf * tf_norm;
        }
    }

    score
}

/// Analyze query text with the default English analyzer and deduplicate stems.
///
/// Used on the `@fulltext`-datatype path where the bucket is always English.
/// For language-aware lookup the caller should use
/// [`analyze_and_dedup_query_with`] with the bucket's analyzer so the query
/// stems match the arena's indexed stems.
fn analyze_and_dedup_query(query_text: &str) -> Vec<String> {
    analyze_and_dedup_query_with(&ENGLISH_ANALYZER, query_text)
}

/// Analyze query text with the caller-provided analyzer and deduplicate stems.
fn analyze_and_dedup_query_with(analyzer: &Analyzer, query_text: &str) -> Vec<String> {
    let mut terms = analyzer.analyze_to_strings(query_text);
    terms.sort();
    terms.dedup();
    terms
}

// =============================================================================
// eval_fulltext
// =============================================================================

/// Evaluate `fulltext(?var, "query")`.
///
/// Return contract:
/// - `Some(Double(score))` for `@fulltext` values (score >= 0.0, including 0.0 for no match)
/// - `None` (unbound) for non-`@fulltext` values or type mismatches
///
/// This lets users sort all `@fulltext` rows by score without needing
/// `(bound ?score)` — non-`@fulltext` rows naturally drop via unbound.
pub fn eval_fulltext<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "fulltext")?;

    // Extract the query string from args[1]
    // Type mismatch → None (graceful, consistent with vector functions)
    let query_str = match args[1].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::String(s)) => s,
        _ => return Ok(None),
    };

    // Read the raw binding for args[0] — we need p_id and dt_id from EncodedLit
    let var_id = match &args[0] {
        Expression::Var(v) => *v,
        _ => {
            // Non-variable first arg: can't determine predicate context
            return Ok(None);
        }
    };

    let binding = match row.get(var_id) {
        Some(b) => b,
        None => return Ok(None),
    };

    match binding {
        crate::binding::Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            // Accept either path:
            //   1. `@fulltext`-datatype values (always English bucket), OR
            //   2. Any other string value on a property that has a configured
            //      BM25 arena at the resolved language bucket.
            // Values with no matching arena fall through to the TF-saturation
            // fallback below so we still produce some score for callers.
            let is_fulltext_dt = *dt_id == DatatypeDictId::FULL_TEXT.as_u16();

            // Arena-based unified BM25 scoring.
            // Guard on LEX_ID: the arena maps string_id → BoW.
            if ObjKind::from_u8(*o_kind) == ObjKind::LEX_ID {
                if let Some(ctx) = ctx {
                    let g_id = ctx.binary_g_id;
                    // Resolve arena lang_id:
                    //   1. @fulltext datatype → English bucket only.
                    //   2. row lang_id if non-zero (rdf:langString)
                    //   3. context english_lang_id fallback (untagged strings).
                    // Full resolution order with explicit-arg / config-derived
                    // language lands with the 3rd-arg fulltext() form (follow-up).
                    let lookup_lang_id = if is_fulltext_dt {
                        ctx.english_lang_id.unwrap_or(0)
                    } else if *lang_id != 0 {
                        *lang_id
                    } else {
                        ctx.english_lang_id.unwrap_or(0)
                    };
                    if let Some(arena) = ctx
                        .fulltext_providers
                        .and_then(|providers| providers.get(&(g_id, *p_id, lookup_lang_id)))
                    {
                        // Bucket's BCP-47 tag → analyzer; same choice that built the arena.
                        let bucket_tag: String = ctx
                            .binary_store
                            .as_ref()
                            .and_then(|s| s.resolve_language_tag(lookup_lang_id))
                            .unwrap_or_else(|| "en".to_string());
                        let bucket_language = Language::from_bcp47(&bucket_tag);
                        let bucket_analyzer = Analyzer::for_language(bucket_language);
                        let query_terms =
                            analyze_and_dedup_query_with(&bucket_analyzer, &query_str);
                        if query_terms.is_empty() {
                            return Ok(Some(ComparableValue::Double(0.0)));
                        }

                        let delta = get_or_build_delta(ctx, g_id, *p_id, lookup_lang_id);

                        if let Some(bow) = arena.doc_bow(*o_key as u32) {
                            // Indexed doc: score directly from arena BoW
                            let score =
                                score_bm25_indexed(arena, bow, &query_terms, delta.as_deref());
                            return Ok(Some(ComparableValue::Double(score)));
                        }

                        // Doc not in arena (novelty doc appearing as EncodedLit — rare).
                        // Decode string, analyze, score with unified BM25.
                        if let Some(gv) = ctx.graph_view() {
                            if let Ok(FlakeValue::String(text)) =
                                gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                            {
                                let doc_term_freqs = bucket_analyzer.analyze_to_term_freqs(&text);
                                let doc_len = doc_term_freqs.values().sum::<u32>();
                                let score = score_bm25_novelty(
                                    &doc_term_freqs,
                                    doc_len,
                                    &query_terms,
                                    arena,
                                    delta.as_deref(),
                                );
                                return Ok(Some(ComparableValue::Double(score)));
                            }
                        }
                    }
                }
            }

            // Fallback: decode string and score with TF-saturation.
            // Restricted to `@fulltext`-datatype values to preserve the
            // pre-config behavior that `fulltext(?v, ...)` returns unbound
            // for non-fulltext string values — otherwise every string
            // variable would score against any query, which is surprising.
            // Configured plain-string properties only score when their
            // arena is available (handled by the arena branch above).
            if !is_fulltext_dt {
                return Ok(None);
            }
            let gv = match ctx.and_then(super::super::context::ExecutionContext::graph_view) {
                Some(gv) => gv,
                None => return Ok(None),
            };

            let val = gv
                .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                .map_err(|e| {
                    crate::error::QueryError::Internal(format!("fulltext decode_value: {e}"))
                })?;

            let text = match &val {
                FlakeValue::String(s) => s.as_str(),
                _ => return Ok(None),
            };

            let score = score_tf_saturation(text, &query_str);
            Ok(Some(ComparableValue::Double(score)))
        }
        crate::binding::Binding::Lit {
            val,
            dtc,
            p_id: lit_p_id,
            ..
        } => {
            // Check if the datatype matches @fulltext (Sid fields: namespace_code, name)
            let dt = dtc.datatype();
            if !(dt.namespace_code == FLUREE_DB && dt.name.as_ref() == "fullText") {
                return Ok(None);
            }

            let text = match val {
                FlakeValue::String(s) => s.as_str(),
                _ => return Ok(None),
            };

            // If p_id is available (from early materialization), use unified BM25
            if let (Some(p_id), Some(ctx)) = (lit_p_id, ctx) {
                let g_id = ctx.binary_g_id;
                // `@fulltext`-datatype values carry no lang tag — always route
                // through the English bucket, keyed by the dict-assigned `"en"` lang_id.
                let lookup_lang_id = ctx.english_lang_id.unwrap_or(0);
                if let Some(arena) = ctx
                    .fulltext_providers
                    .and_then(|providers| providers.get(&(g_id, *p_id, lookup_lang_id)))
                {
                    let query_terms = analyze_and_dedup_query(&query_str);
                    if query_terms.is_empty() {
                        return Ok(Some(ComparableValue::Double(0.0)));
                    }

                    let delta = get_or_build_delta(ctx, g_id, *p_id, lookup_lang_id);
                    let doc_term_freqs = ENGLISH_ANALYZER.analyze_to_term_freqs(text);
                    let doc_len = doc_term_freqs.values().sum::<u32>();
                    let score = score_bm25_novelty(
                        &doc_term_freqs,
                        doc_len,
                        &query_terms,
                        arena,
                        delta.as_deref(),
                    );
                    return Ok(Some(ComparableValue::Double(score)));
                }
            }

            // Fallback: TF-saturation (no arena or no p_id)
            let score = score_tf_saturation(text, &query_str);
            Ok(Some(ComparableValue::Double(score)))
        }
        _ => Ok(None),
    }
}

/// Score a document against a query using TF-saturation (BM25 TF component).
///
/// Fallback path used when no FulltextArena is available. Uses only per-document
/// term-frequency saturation without corpus-wide IDF or avgdl normalization.
fn score_tf_saturation(doc_text: &str, query_text: &str) -> f64 {
    let analyzer = &*ENGLISH_ANALYZER;

    let query_terms = analyzer.analyze_to_strings(query_text);
    if query_terms.is_empty() {
        return 0.0;
    }

    let doc_term_freqs = analyzer.analyze_to_term_freqs(doc_text);
    if doc_term_freqs.is_empty() {
        return 0.0;
    }

    // Document length = total term count
    let dl: f64 = doc_term_freqs.values().sum::<u32>() as f64;
    let avgdl = dl; // single-doc; no corpus stats yet

    let mut score = 0.0;
    for qt in &query_terms {
        if let Some(&tf) = doc_term_freqs.get(qt) {
            let tf = tf as f64;
            // TF saturation: tf*(k1+1) / (tf + k1*(1-b+b*dl/avgdl))
            // With dl == avgdl this simplifies to tf*(k1+1) / (tf + k1)
            let tf_component = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
            score += tf_component;
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_basic_match() {
        let score = score_tf_saturation("cargo testing with nextest", "cargo testing");
        assert!(score > 0.0, "Should have positive score for matching terms");
    }

    #[test]
    fn test_score_no_match() {
        let score = score_tf_saturation("hello world", "cargo testing");
        assert_eq!(score, 0.0, "Should have zero score for no matching terms");
    }

    #[test]
    fn test_score_empty_query() {
        let score = score_tf_saturation("cargo testing", "");
        assert_eq!(score, 0.0, "Empty query should produce zero score");
    }

    #[test]
    fn test_score_empty_doc() {
        let score = score_tf_saturation("", "cargo testing");
        assert_eq!(score, 0.0, "Empty document should produce zero score");
    }

    #[test]
    fn test_score_stopwords_only_query() {
        // "the" and "is" are English stopwords; "test" is also a stopword
        // in the Fluree English stopword list
        let score = score_tf_saturation("cargo testing with nextest", "the is");
        assert_eq!(score, 0.0, "Stopwords-only query should produce zero score");
    }

    #[test]
    fn test_score_stemming() {
        // "indexing" and "indexed" should both stem to "index"
        let score1 = score_tf_saturation("indexing documents", "indexed");
        let score2 = score_tf_saturation("indexing documents", "indexing");
        assert!(score1 > 0.0, "Stemmed match should score > 0");
        assert!(score2 > 0.0, "Same-form match should score > 0");
        assert_eq!(score1, score2, "Same stem should produce same score");
    }

    #[test]
    fn test_score_more_matches_higher() {
        let score_one = score_tf_saturation("cargo nextest runner", "cargo");
        let score_two = score_tf_saturation("cargo nextest runner", "cargo nextest");
        assert!(
            score_two > score_one,
            "More matching terms should produce higher score: {score_two} vs {score_one}"
        );
    }

    #[test]
    fn test_score_returns_zero_for_no_overlap() {
        // Verify 0.0 (not None) is returned for analyzed-but-non-matching content
        let score = score_tf_saturation("database query engine", "weather forecast");
        assert_eq!(score, 0.0);
    }

    /// Helper to build a FulltextArena from text strings (analyze + insert).
    ///
    /// Uses two-pass approach to avoid term_id shifting:
    /// 1. Collect all unique terms from all documents
    /// 2. Build BoWs with stable term_ids
    fn build_test_arena(docs: &[(u32, &str)]) -> FulltextArena {
        let analyzer = &*ENGLISH_ANALYZER;
        let mut arena = FulltextArena::new();

        // Pass 1: collect all unique terms across all documents
        let mut all_terms = std::collections::BTreeSet::new();
        let per_doc: Vec<_> = docs
            .iter()
            .map(|&(string_id, text)| {
                let term_freqs = analyzer.analyze_to_term_freqs(text);
                for term in term_freqs.keys() {
                    all_terms.insert(term.clone());
                }
                (string_id, term_freqs)
            })
            .collect();

        // Insert all terms in sorted order (stable IDs)
        for term in &all_terms {
            arena.get_or_insert_term(term);
        }

        // Pass 2: build BoWs using stable term_ids
        for (string_id, term_freqs) in per_doc {
            let mut bow: Vec<(u32, u16)> = term_freqs
                .iter()
                .map(|(term, &tf)| {
                    let tid = arena.term_id(term).unwrap();
                    (tid, tf as u16)
                })
                .collect();
            bow.sort_by_key(|(tid, _)| *tid);
            arena.inc_string(string_id, &bow);
        }
        arena.finalize_stats();
        arena
    }

    #[test]
    fn test_arena_bm25_scoring() {
        let arena = build_test_arena(&[
            (10, "cargo nextest runner for fast testing"),
            (20, "cargo build optimizations and cargo features"),
            (30, "database query engine performance"),
        ]);

        let query_terms = analyze_and_dedup_query("cargo");

        // score_bm25_indexed with no delta
        let bow_10 = arena.doc_bow(10).unwrap();
        let bow_20 = arena.doc_bow(20).unwrap();
        let bow_30 = arena.doc_bow(30).unwrap();

        let score_10 = score_bm25_indexed(&arena, bow_10, &query_terms, None);
        let score_20 = score_bm25_indexed(&arena, bow_20, &query_terms, None);
        let score_30 = score_bm25_indexed(&arena, bow_30, &query_terms, None);

        assert!(score_10 > 0.0, "Doc with 'cargo' should score > 0");
        assert!(score_20 > 0.0, "Doc with 'cargo' should score > 0");
        assert_eq!(score_30, 0.0, "Doc without 'cargo' should score 0");

        // Doc 20 mentions "cargo" twice → higher TF
        assert!(
            score_20 > score_10,
            "Doc with higher TF should score higher: {score_20} vs {score_10}"
        );
    }

    #[test]
    fn test_unified_scoring_no_delta_matches_pure_arena() {
        let arena = build_test_arena(&[
            (10, "cargo nextest runner"),
            (20, "database performance tuning"),
        ]);

        let query_terms = analyze_and_dedup_query("cargo runner");

        // Score with unified (no delta) should produce same result as arena.score_bm25
        let bow = arena.doc_bow(10).unwrap();
        let unified_score = score_bm25_indexed(&arena, bow, &query_terms, None);

        // Compare with arena's own scoring
        let arena_term_ids: Vec<u32> = query_terms
            .iter()
            .filter_map(|t| arena.term_id(t))
            .collect();
        let arena_score = arena.score_bm25(10, &arena_term_ids);

        assert!(
            (unified_score - arena_score).abs() < 1e-10,
            "Unified scoring (no delta) should match arena.score_bm25: {unified_score} vs {arena_score}"
        );
    }

    #[test]
    fn test_unified_scoring_with_delta() {
        let arena = build_test_arena(&[
            (10, "cargo nextest runner"),
            (20, "database performance tuning"),
        ]);

        let query_terms = analyze_and_dedup_query("cargo");

        // Create a delta simulating one novelty assertion with "cargo" in it
        let delta = NoveltyFulltextDelta {
            delta_df: HashMap::from([("cargo".to_string(), 1)]),
            delta_n: 1,
            delta_sum_dl: 3,
        };

        let bow = arena.doc_bow(10).unwrap();
        let score_no_delta = score_bm25_indexed(&arena, bow, &query_terms, None);
        let score_with_delta = score_bm25_indexed(&arena, bow, &query_terms, Some(&delta));

        // With an extra doc containing "cargo", the IDF of "cargo" should decrease
        // (it's now present in more documents), so the score should be lower
        assert!(
            score_with_delta < score_no_delta,
            "Extra doc with 'cargo' should decrease IDF: with_delta={score_with_delta} no_delta={score_no_delta}"
        );
    }

    #[test]
    fn test_novelty_doc_scored_with_unified_bm25() {
        let arena = build_test_arena(&[
            (10, "cargo nextest runner"),
            (20, "database performance tuning"),
        ]);

        let query_terms = analyze_and_dedup_query("cargo");

        // A novelty doc with the same content as doc 10 should score identically
        let doc_term_freqs = ENGLISH_ANALYZER.analyze_to_term_freqs("cargo nextest runner");
        let doc_len = doc_term_freqs.values().sum::<u32>();

        let indexed_score =
            score_bm25_indexed(&arena, arena.doc_bow(10).unwrap(), &query_terms, None);
        let novelty_score =
            score_bm25_novelty(&doc_term_freqs, doc_len, &query_terms, &arena, None);

        assert!(
            (indexed_score - novelty_score).abs() < 1e-10,
            "Same content should produce same score: indexed={indexed_score} novelty={novelty_score}"
        );
    }

    #[test]
    fn test_effective_df_clamped() {
        let arena = build_test_arena(&[(10, "cargo nextest runner")]);

        // Create delta with large negative df (shouldn't happen, but test clamping)
        let delta = NoveltyFulltextDelta {
            delta_df: HashMap::from([("cargo".to_string(), -100)]),
            delta_n: -100,
            delta_sum_dl: -100,
        };

        let stats = compute_effective_stats(&arena, Some(&delta));
        let df = effective_df("cargo", &arena, Some(&delta), stats.n);

        // df should be clamped to [0, N']
        assert!(df >= 0.0, "df should be >= 0, got {df}");
        assert!(
            df <= stats.n,
            "df should be <= N', got {} (N'={})",
            df,
            stats.n
        );
    }

    #[test]
    fn test_analyze_and_dedup_query() {
        // Duplicate terms should be deduplicated
        let terms = analyze_and_dedup_query("cargo cargo cargo");
        // After stemming, "cargo" stays "cargo", should appear once
        let cargo_count = terms.iter().filter(|t| t.as_str() == "cargo").count();
        assert_eq!(
            cargo_count, 1,
            "Duplicate query terms should be deduplicated"
        );
    }

    #[test]
    fn test_delta_df_precomputed() {
        // Verify that precomputed delta_df values are used correctly by effective_df.
        let arena = build_test_arena(&[
            (10, "cargo nextest runner"),
            (20, "database performance tuning"),
        ]);

        // "cargo" in both docs: delta_df cancels out (1 assert + 1 retract = 0)
        // "build" only asserted: delta_df = +1
        // "runner" only retracted: delta_df = -1
        let delta = NoveltyFulltextDelta {
            delta_df: HashMap::from([
                ("cargo".to_string(), 0),
                ("build".to_string(), 1),
                ("runner".to_string(), -1),
            ]),
            delta_n: 0,
            delta_sum_dl: 0,
        };

        let stats = compute_effective_stats(&arena, Some(&delta));

        // "cargo" has arena_df=1 (in doc 10) + delta_df=0 = 1
        let df_cargo = effective_df("cargo", &arena, Some(&delta), stats.n);
        assert_eq!(df_cargo, 1.0);

        // "build" has arena_df=0 (not in arena) + delta_df=1 = 1
        let df_build = effective_df("build", &arena, Some(&delta), stats.n);
        assert_eq!(df_build, 1.0);

        // "missing" has arena_df=0 + delta_df=0 = 0
        let df_missing = effective_df("missing", &arena, Some(&delta), stats.n);
        assert_eq!(df_missing, 0.0);
    }

    #[test]
    fn test_de_staling_key_includes_list_index() {
        // Two assertions of the same string on the same subject but at different
        // list indices should be counted as separate triples (triple_count_delta = +2).
        //
        // This tests the de-staling key shape: (Sid, string_id, Option<i32>).
        // Without list_index in the key, these would collapse to a single entry.
        use fluree_db_core::flake::{Flake, FlakeMeta};

        let subject = Sid::new(0, "ex:item1");
        let pred = Sid::new(0, "ex:content");
        let fulltext_dt = Sid::new(FLUREE_DB, "fullText");
        let text = "cargo nextest runner".to_string();

        // Simulate two overlay flakes: same subject, same string, different list indices
        let flake_i0 = Flake {
            g: None,
            s: subject.clone(),
            p: pred.clone(),
            o: FlakeValue::String(text.clone()),
            dt: fulltext_dt.clone(),
            t: 1,
            op: true,
            m: Some(FlakeMeta {
                lang: None,
                i: Some(0),
            }),
        };
        let flake_i1 = Flake {
            g: None,
            s: subject.clone(),
            p: pred.clone(),
            o: FlakeValue::String(text.clone()),
            dt: fulltext_dt.clone(),
            t: 1,
            op: true,
            m: Some(FlakeMeta {
                lang: None,
                i: Some(1),
            }),
        };

        // Build the de-staling map the same way build_novelty_delta does
        type TripleKey = (Sid, u32, Option<i32>);
        let mut latest_ops: HashMap<TripleKey, (i64, bool, String)> = HashMap::new();

        for flake in [&flake_i0, &flake_i1] {
            let list_idx = flake.m.as_ref().and_then(|m| m.i);
            let triple_key = (flake.s.clone(), 42_u32, list_idx); // string_id = 42 (mock)
            match latest_ops.entry(triple_key) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((flake.t, flake.op, text.clone()));
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if flake.t > e.get().0 {
                        e.insert((flake.t, flake.op, text.clone()));
                    }
                }
            }
        }

        // With list_index in the key, we should have 2 separate entries
        assert_eq!(
            latest_ops.len(),
            2,
            "Same string at different list indices should produce 2 de-staled entries, got {}",
            latest_ops.len()
        );
    }
}
