//! Fulltext collection hook for commit resolution.
//!
//! Collects string values that should be full-text indexed during commit
//! resolution. Two sources route entries here:
//!
//! 1. **`@fulltext` datatype** — zero-config shortcut. Always English,
//!    regardless of any per-property or ledger-wide language configuration.
//! 2. **Configured properties** — properties listed in the ledger's
//!    `f:fullTextDefaults` (per-graph or ledger-wide). Language follows the
//!    value's `rdf:langString` tag or the effective `f:defaultLanguage`.
//!
//! Which path was taken is preserved on each entry via [`FulltextSource`]
//! so the arena builder can pick the correct analyzer language. The raw
//! `lang_id` is also carried through — meaningful only for the `Configured`
//! source; inert for `DatatypeFulltext` (which is always English).

use std::collections::HashSet;

use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::GraphId;

/// Why a value was routed to the fulltext hook.
///
/// This discriminator lets the arena builder decide *how* to resolve the
/// language for a given entry — without reusing the `lang_id=0` invariant
/// for anything other than "no literal lang tag on the value."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FulltextSource {
    /// The value's datatype is `@fulltext`. Always English; never consults
    /// the per-property configuration or the ledger-wide default language.
    DatatypeFulltext,
    /// The value's property is listed in `f:fullTextDefaults`. Language
    /// comes from the row's `lang_id` (if `rdf:langString`) or the
    /// effective `f:defaultLanguage` (if untagged plain string).
    Configured,
}

/// Entry collected for fulltext indexing.
#[derive(Debug, Clone)]
pub struct FulltextEntry {
    /// Graph ID (0 = default graph).
    pub g_id: GraphId,
    /// Predicate ID in binary index space.
    pub p_id: u32,
    /// String dictionary ID (the o_key for ObjKind::LEX_ID).
    pub string_id: u32,
    /// Raw `lang_id` from the value (0 = no literal lang tag).
    ///
    /// Meaningful only when [`source`] is `Configured`. For `DatatypeFulltext`
    /// entries this field is always `0` and must be ignored by consumers.
    pub lang_id: u16,
    /// Which routing path put this entry here.
    pub source: FulltextSource,
    /// Transaction time.
    pub t: i64,
    /// true = assertion, false = retraction.
    pub is_assert: bool,
}

/// Per-indexing-run fulltext configuration.
///
/// Built once per indexing run / snapshot from the resolver's effective
/// `full_text.properties`. Exposed to the hot path as an immutable view so
/// `FulltextHook::on_op` stays scalar-only for values that are neither
/// `@fulltext`-typed nor on a configured property.
///
/// Two tiers:
/// - `any_graph`: ledger-wide properties that apply to every graph (they
///   were declared in `f:fullTextDefaults` at the ledger level, so they
///   index the property on any graph that has triples for it).
/// - `per_graph`: properties that apply only to a specific graph (added
///   via a `f:GraphConfig` override targeting that graph).
///
/// Both sets are consulted on each on_op; the hot path is still one or
/// two hash probes.
#[derive(Debug, Clone, Default)]
pub struct FulltextHookConfig {
    any_graph: HashSet<u32>,
    per_graph: HashSet<(GraphId, u32)>,
}

impl FulltextHookConfig {
    /// Construct from explicit any-graph and per-graph iterators.
    pub fn new<A, P>(any_graph: A, per_graph: P) -> Self
    where
        A: IntoIterator<Item = u32>,
        P: IntoIterator<Item = (GraphId, u32)>,
    {
        Self {
            any_graph: any_graph.into_iter().collect(),
            per_graph: per_graph.into_iter().collect(),
        }
    }

    /// Add a ledger-wide property that applies to any graph.
    pub fn add_any_graph(&mut self, p_id: u32) {
        self.any_graph.insert(p_id);
    }

    /// Add a per-graph property (only applies when `g_id` matches).
    pub fn add_per_graph(&mut self, g_id: GraphId, p_id: u32) {
        self.per_graph.insert((g_id, p_id));
    }

    /// True if the given `(g_id, p_id)` is listed in the configured
    /// full-text properties set — either ledger-wide (any graph) or
    /// explicitly scoped to this graph.
    #[inline]
    pub fn contains(&self, g_id: GraphId, p_id: u32) -> bool {
        self.any_graph.contains(&p_id) || self.per_graph.contains(&(g_id, p_id))
    }

    /// True if no properties are configured — i.e., the config path is a
    /// no-op. Callers can short-circuit in this case if they want.
    pub fn is_empty(&self) -> bool {
        self.any_graph.is_empty() && self.per_graph.is_empty()
    }
}

/// Bundle of per-record inputs to [`FulltextHook::on_op`].
///
/// Passed as a single reference to keep the hot-path signature small and
/// readable as the hook grows new inputs. Fields mirror the legacy
/// positional arguments.
#[derive(Debug, Clone, Copy)]
pub struct FulltextOpInput<'a> {
    pub g_id: GraphId,
    pub p_id: u32,
    pub dt_id: u16,
    pub o_kind: u8,
    pub o_key: u64,
    /// Raw lang_id from the flake value (0 = no literal lang tag).
    pub lang_id: u16,
    pub t: i64,
    pub is_assert: bool,
    pub config: &'a FulltextHookConfig,
}

/// Hook for collecting fulltext-indexable literals during commit resolution.
///
/// Two routing paths:
///   1. `dt_id == DatatypeDictId::FULL_TEXT` → always English.
///   2. Property listed in the configured-property set → language derived
///      later from `lang_id` or the effective `f:defaultLanguage`.
///
/// The string text itself is NOT copied — only the string dictionary ID is
/// stored. The arena builder retrieves text from the string dict when it
/// needs to analyze (tokenize/stem).
#[derive(Debug)]
pub struct FulltextHook {
    entries: Vec<FulltextEntry>,
}

impl FulltextHook {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Process a resolved record during commit resolution.
    ///
    /// Gating logic (in order):
    /// 1. `o_kind != LEX_ID` → reject (string values only).
    /// 2. `dt_id == FULL_TEXT` → accept as `DatatypeFulltext`, `lang_id = 0`
    ///    (inert for this source).
    /// 3. `(g_id, p_id)` in `config.configured` → accept as `Configured`
    ///    with the row's actual `lang_id`.
    /// 4. Otherwise → reject.
    ///
    /// The reject path (step 4) is the common case for non-fulltext workloads
    /// and does only a couple of branches plus one hash-set probe — no string
    /// decoding, no analyzer selection.
    #[inline]
    pub fn on_op(&mut self, input: FulltextOpInput<'_>) {
        if ObjKind::from_u8(input.o_kind) != ObjKind::LEX_ID {
            return;
        }
        if input.dt_id == DatatypeDictId::FULL_TEXT.as_u16() {
            self.entries.push(FulltextEntry {
                g_id: input.g_id,
                p_id: input.p_id,
                string_id: input.o_key as u32,
                lang_id: 0,
                source: FulltextSource::DatatypeFulltext,
                t: input.t,
                is_assert: input.is_assert,
            });
            return;
        }
        let matched = input.config.contains(input.g_id, input.p_id);
        // [DIAG] Every LEX_ID non-fulltext assertion flowing through the
        // hook logs whether the configured-property check matched. Lets us
        // tell apart (a) the resolver never calling us with this op vs.
        // (b) calling us with a p_id that doesn't match the pre-registered
        // one. Fires once per op in this narrow LEX_ID + non-`@fulltext`
        // slice — bounded on the user's repro ledger. Remove after the
        // Solo c3000-04 configured-properties-match bug is diagnosed.
        tracing::info!(
            g_id = input.g_id,
            p_id = input.p_id,
            dt_id = input.dt_id,
            lang_id = input.lang_id,
            t = input.t,
            is_assert = input.is_assert,
            matched_config = matched,
            config_empty = input.config.is_empty(),
            "[DIAG] fulltext hook on_op: LEX_ID non-fulltext candidate"
        );
        if matched {
            self.entries.push(FulltextEntry {
                g_id: input.g_id,
                p_id: input.p_id,
                string_id: input.o_key as u32,
                lang_id: input.lang_id,
                source: FulltextSource::Configured,
                t: input.t,
                is_assert: input.is_assert,
            });
        }
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn into_entries(self) -> Vec<FulltextEntry> {
        self.entries
    }

    pub fn entries(&self) -> &[FulltextEntry] {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut [FulltextEntry] {
        &mut self.entries
    }
}

impl Default for FulltextHook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEX: u8 = ObjKind::LEX_ID.as_u8();

    fn empty_config() -> FulltextHookConfig {
        FulltextHookConfig::default()
    }

    #[allow(clippy::too_many_arguments)]
    fn make_input(
        g_id: GraphId,
        p_id: u32,
        dt_id: u16,
        o_kind: u8,
        o_key: u64,
        lang_id: u16,
        t: i64,
        is_assert: bool,
        config: &FulltextHookConfig,
    ) -> FulltextOpInput<'_> {
        FulltextOpInput {
            g_id,
            p_id,
            dt_id,
            o_kind,
            o_key,
            lang_id,
            t,
            is_assert,
            config,
        }
    }

    #[test]
    fn test_hook_collects_fulltext_datatype() {
        let mut hook = FulltextHook::new();
        let config = empty_config();
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            42,
            0,
            1,
            true,
            &config,
        ));
        assert_eq!(hook.entry_count(), 1);
        let entry = &hook.entries()[0];
        assert_eq!(entry.string_id, 42);
        assert_eq!(entry.p_id, 5);
        assert_eq!(entry.source, FulltextSource::DatatypeFulltext);
        assert_eq!(entry.lang_id, 0, "datatype path sets lang_id to 0 (inert)");
        assert!(entry.is_assert);
    }

    #[test]
    fn test_hook_datatype_overrides_configured_set() {
        // A property that is ALSO in the configured set, but the value's
        // datatype is @fulltext — datatype wins, always English.
        let config = FulltextHookConfig::new(std::iter::empty::<u32>(), [(0u16, 5u32)]);
        let mut hook = FulltextHook::new();
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            42,
            // pretend the value had a French lang tag — it must be ignored.
            7,
            1,
            true,
            &config,
        ));
        assert_eq!(hook.entry_count(), 1);
        let entry = &hook.entries()[0];
        assert_eq!(entry.source, FulltextSource::DatatypeFulltext);
        assert_eq!(entry.lang_id, 0, "datatype path must not retain lang_id");
    }

    #[test]
    fn test_hook_collects_configured_property_with_lang_id() {
        let config = FulltextHookConfig::new(std::iter::empty::<u32>(), [(0u16, 5u32)]);
        let mut hook = FulltextHook::new();
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::STRING.as_u16(),
            LEX,
            42,
            7, // French lang_id, say
            1,
            true,
            &config,
        ));
        assert_eq!(hook.entry_count(), 1);
        let entry = &hook.entries()[0];
        assert_eq!(entry.source, FulltextSource::Configured);
        assert_eq!(entry.lang_id, 7);
    }

    #[test]
    fn test_hook_skips_untagged_string_on_non_configured_property() {
        let config = empty_config();
        let mut hook = FulltextHook::new();
        // Plain string, property not in config → should NOT be collected.
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::STRING.as_u16(),
            LEX,
            42,
            0,
            1,
            true,
            &config,
        ));
        assert!(hook.is_empty());
    }

    #[test]
    fn test_hook_skips_non_fulltext_non_configured() {
        let mut hook = FulltextHook::new();
        let config = empty_config();

        // Vector dt_id, not fulltext, not configured
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::VECTOR.as_u16(),
            LEX,
            42,
            0,
            1,
            true,
            &config,
        ));
        assert!(hook.is_empty());
    }

    #[test]
    fn test_hook_skips_non_lex_kind() {
        let mut hook = FulltextHook::new();
        let config = FulltextHookConfig::new(std::iter::empty::<u32>(), [(0u16, 5u32)]);

        // @fulltext dt but wrong o_kind (REF_ID)
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            ObjKind::REF_ID.as_u8(),
            42,
            0,
            1,
            true,
            &config,
        ));
        assert!(hook.is_empty());

        // configured property but NUM_INT o_kind
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::STRING.as_u16(),
            ObjKind::NUM_INT.as_u8(),
            42,
            0,
            1,
            true,
            &config,
        ));
        assert!(hook.is_empty());
    }

    #[test]
    fn test_hook_tracks_retractions() {
        let mut hook = FulltextHook::new();
        let config = empty_config();
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            42,
            0,
            1,
            true,
            &config,
        ));
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            42,
            0,
            2,
            false,
            &config,
        ));
        assert_eq!(hook.entry_count(), 2);
        assert!(hook.entries()[0].is_assert);
        assert!(!hook.entries()[1].is_assert);
    }

    #[test]
    fn test_into_entries() {
        let mut hook = FulltextHook::new();
        let config = empty_config();
        hook.on_op(make_input(
            0,
            5,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            10,
            0,
            1,
            true,
            &config,
        ));
        hook.on_op(make_input(
            1,
            7,
            DatatypeDictId::FULL_TEXT.as_u16(),
            LEX,
            20,
            0,
            2,
            true,
            &config,
        ));
        let entries = hook.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].g_id, 0);
        assert_eq!(entries[1].g_id, 1);
    }
}
