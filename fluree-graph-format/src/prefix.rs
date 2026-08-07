//! Prefix compaction for Turtle-family output
//!
//! A [`PrefixMap`] turns expanded IRIs — the only kind the IR holds — back
//! into `prefix:local` names at write time. Lifted from `fluree-db-api`'s
//! `export.rs` unchanged so the streaming writers and the store-backed
//! exporter compact IRIs the same way, rather than drifting into two
//! near-identical implementations.

use crate::escape::write_escaped_iri;
use fluree_graph_ir::chars::{is_pn_local, is_pn_prefix};
use std::collections::BTreeMap;
use std::io::{self, Write};

/// A sorted prefix map for compressing IRIs into prefixed names.
///
/// Prefixes are sorted by namespace-IRI length descending so that
/// longest-prefix-first matching produces the most specific result: given both
/// `ex: <http://example.org/>` and `exdeep: <http://example.org/deep/>`,
/// `http://example.org/deep/x` compacts to `exdeep:x`, not `ex:deep/x` (which
/// would not even be a legal prefixed name).
#[derive(Debug, Clone, Default)]
pub struct PrefixMap {
    /// (prefix, namespace_iri) sorted by namespace IRI length descending.
    entries: Vec<(String, String)>,
}

impl PrefixMap {
    /// An empty map — every IRI writes in full.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a prefix map from a JSON-LD `@context` object.
    ///
    /// Expects `{"prefix": "iri", ...}` — ignores entries where the value is
    /// not a string or the key starts with `@`.
    ///
    /// A `@context` is a map of *terms*, only some of which are namespace
    /// prefixes, so keys that could not be written as a `PNAME_NS` are dropped
    /// rather than stored — see [`Self::insert`].
    pub fn from_context(ctx: &serde_json::Value) -> Self {
        let mut map = PrefixMap::new();
        if let Some(obj) = ctx.as_object() {
            for (key, val) in obj {
                if key.starts_with('@') {
                    continue;
                }
                if let Some(iri) = val.as_str() {
                    map.insert(key.clone(), iri.to_string());
                }
            }
        }
        map
    }

    /// Build from an explicit map of prefix → IRI.
    pub fn from_map(map: BTreeMap<String, String>) -> Self {
        let mut out = PrefixMap::new();
        for (prefix, iri) in map {
            out.insert(prefix, iri);
        }
        out
    }

    /// Declare (or redeclare) `prefix` as `namespace_iri`.
    ///
    /// This is what a streaming writer calls on an `on_prefix` event. A
    /// repeated prefix replaces its previous namespace, matching Turtle, where
    /// a later `@prefix` for the same name shadows the earlier one for every
    /// statement that follows it.
    ///
    /// A prefix that cannot be spelled as a Turtle `PNAME_NS` is **dropped**,
    /// not stored. The map exists to make output shorter; a prefix that cannot
    /// be written would instead make it invalid, on every IRI it matched. The
    /// live source of these is a JSON-LD `@context`, whose keys are arbitrary
    /// strings — `1st`, `has space`, `-x` are all legal there and none is a
    /// legal prefix. Dropping costs nothing but verbosity.
    pub fn insert(&mut self, prefix: impl Into<String>, namespace_iri: impl Into<String>) {
        let prefix = prefix.into();
        if !is_pn_prefix(&prefix) {
            return;
        }
        self.entries.retain(|(p, _)| p != &prefix);
        self.entries.push((prefix, namespace_iri.into()));
        self.resort();
    }

    /// Try to compress a full IRI into a prefixed name (e.g., `ex:alice`).
    ///
    /// Returns `None` if no prefix matches or the local name contains
    /// characters that are invalid in a Turtle prefixed name.
    pub fn compact(&self, iri: &str) -> Option<String> {
        for (prefix, ns) in &self.entries {
            if let Some(local) = iri.strip_prefix(ns.as_str()) {
                if is_emittable_local(local) {
                    return Some(format!("{prefix}:{local}"));
                }
            }
        }
        None
    }

    /// Returns `true` if the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate `(prefix, namespace_iri)` pairs, longest namespace first.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(p, n)| (p.as_str(), n.as_str()))
    }

    /// Restore the longest-namespace-first order [`Self::compact`] depends on.
    fn resort(&mut self) {
        self.entries.sort_by_key(|b| std::cmp::Reverse(b.1.len()));
    }
}

/// Whether this writer will emit `local` as a prefixed name's local part.
///
/// Narrower than the grammar by exactly one character. `PN_LOCAL` admits `:`
/// in every position, so `ex:has:colon` is a legal prefixed name and
/// [`is_pn_local`] says so — but accepting it here would change which IRIs
/// this compacts, and therefore what `fluree export` emits, for no correctness
/// gain. The other narrowings in this file exist because the wider behavior
/// produced *invalid* output; this one would only produce *different* output,
/// which is a product decision rather than a bug fix.
///
/// So it is deliberately not widened, and left as a decision to make on its
/// own: dropping the `:` clause here is the whole change.
fn is_emittable_local(local: &str) -> bool {
    is_pn_local(local) && !local.contains(':')
}

/// Write `@prefix` declarations to a Turtle/TriG writer.
///
/// Alphabetical by prefix for deterministic, readable output, followed by a
/// blank line when anything was written.
pub fn write_prefix_declarations<W: Write>(prefixes: &PrefixMap, writer: &mut W) -> io::Result<()> {
    let mut sorted: Vec<(&str, &str)> = prefixes.iter().collect();
    sorted.sort_by_key(|(p, _)| *p);
    for (prefix, ns) in sorted {
        write!(writer, "@prefix {prefix}: <")?;
        write_escaped_iri(writer, ns)?;
        writeln!(writer, "> .")?;
    }
    if !prefixes.is_empty() {
        writeln!(writer)?; // blank line after prefixes
    }
    Ok(())
}

/// Write an IRI as a Turtle prefixed name or `<full-iri>`.
pub fn write_turtle_iri<W: Write>(w: &mut W, iri: &str, prefixes: &PrefixMap) -> io::Result<()> {
    if let Some(pname) = prefixes.compact(iri) {
        w.write_all(pname.as_bytes())
    } else {
        w.write_all(b"<")?;
        write_escaped_iri(w, iri)?;
        w.write_all(b">")
    }
}

/// Write a term already rendered as a string — `_:label` passes through as a
/// blank node, anything else goes through [`write_turtle_iri`].
///
/// Blank-node labels are written verbatim, deliberately: an export has to
/// round-trip, and rewriting a label to fit the Turtle grammar would merge two
/// distinct nodes onto one id (contrast a validation report's output, which
/// sanitizes because it does not round-trip).
///
/// Current blank-node ids are all writable as-is: import mints `[0-9a-z-]`
/// (see `fluree_db_core::skolem`), staged transactions mint hex, `BNODE()`
/// mints a UUID. Ledgers imported by Fluree 4.1.4 or earlier hold ids that
/// embedded the ledger id, so they contain `/` and `:` and an export of such a
/// ledger emits Turtle that strict parsers reject — the identity is preserved,
/// the syntax is not. Re-importing the source fixes it.
pub fn write_turtle_iri_or_bnode<W: Write>(
    w: &mut W,
    iri: &str,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    if iri.starts_with("_:") {
        w.write_all(iri.as_bytes())
    } else {
        write_turtle_iri(w, iri, prefixes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx_map() -> PrefixMap {
        PrefixMap::from_context(&json!({
            "ex": "http://example.org/",
            "schema": "http://schema.org/",
            "@vocab": "http://ignored.example/",
            "notastring": 42
        }))
    }

    #[test]
    fn from_context_keeps_string_entries_and_drops_keyword_keys() {
        let pm = ctx_map();
        let prefixes: Vec<&str> = pm.iter().map(|(p, _)| p).collect();
        assert_eq!(prefixes.len(), 2, "{prefixes:?}");
        assert!(prefixes.contains(&"ex"));
        assert!(prefixes.contains(&"schema"));
    }

    #[test]
    fn compaction_prefers_the_longest_matching_namespace() {
        let pm = PrefixMap::from_context(&json!({
            "ex": "http://example.org/",
            "deep": "http://example.org/deep/"
        }));
        assert_eq!(
            pm.compact("http://example.org/deep/thing").as_deref(),
            Some("deep:thing")
        );
        assert_eq!(
            pm.compact("http://example.org/thing").as_deref(),
            Some("ex:thing")
        );
    }

    /// `-` is in `PN_CHARS` but not `PN_CHARS_U`, so it cannot start a local
    /// name. Compacting `…/-dash` to `ex:-dash` emitted a prefixed name no
    /// parser reads.
    /// The one place this writer is narrower than the grammar, pinned so the
    /// carve-out cannot be lost by accident. `is_pn_local` says yes; this
    /// writer says no, on purpose — see `is_emittable_local`.
    #[test]
    fn compaction_declines_a_colon_the_grammar_would_allow() {
        let pm = ctx_map();
        assert!(
            is_pn_local("has:colon"),
            "the grammar admits ':' — this is a writer policy, not a syntax rule"
        );
        assert_eq!(pm.compact("http://example.org/has:colon"), None);
        assert_eq!(pm.compact("http://example.org/:leading"), None);
    }

    #[test]
    fn compaction_declines_a_local_name_starting_with_a_hyphen() {
        let pm = ctx_map();
        assert_eq!(pm.compact("http://example.org/-dash"), None);
        assert_eq!(pm.compact("http://example.org/-"), None);
        assert_eq!(pm.compact("http://example.org/--x"), None);
        // Non-initially it is fine, which is why the check is positional.
        assert_eq!(
            pm.compact("http://example.org/x-dash").as_deref(),
            Some("ex:x-dash")
        );
        assert_eq!(
            pm.compact("http://example.org/x--").as_deref(),
            Some("ex:x--")
        );
    }

    /// A JSON-LD `@context` is the live source of keys that are not prefixes.
    /// Storing one produces `1st:thing` or `has space:thing` on every IRI it
    /// matches — invalid output, and worse than not compacting at all.
    #[test]
    fn a_prefix_that_cannot_be_written_is_dropped_not_stored() {
        let pm = PrefixMap::from_context(&json!({
            "ok": "http://ok.example/",
            "": "http://empty.example/",
            "1st": "http://digit.example/",
            "-lead": "http://hyphen.example/",
            "_under": "http://underscore.example/",
            "has space": "http://space.example/",
            "trailing.": "http://dot.example/"
        }));
        let prefixes: Vec<&str> = pm.iter().map(|(p, _)| p).collect();
        assert_eq!(prefixes.len(), 2, "{prefixes:?}");
        assert!(prefixes.contains(&"ok"), "{prefixes:?}");
        assert!(prefixes.contains(&""), "the empty prefix is legal Turtle");

        // The dropped ones simply do not compact.
        assert_eq!(pm.compact("http://digit.example/x"), None);
        assert_eq!(pm.compact("http://space.example/x"), None);
        // And `insert` refuses them on the same terms.
        let mut pm = PrefixMap::new();
        pm.insert("1st", "http://digit.example/");
        assert!(pm.is_empty());
    }

    /// Everything a writer can emit must re-read. This walks the whole
    /// surface: any IRI that compacts must produce a prefixed name our own
    /// parser accepts.
    #[test]
    fn every_compaction_this_map_produces_is_parseable() {
        let mut pm = PrefixMap::new();
        pm.insert("ex", "http://example.org/");
        pm.insert("", "http://empty.example/");

        let locals = [
            "plain",
            "with-dash",
            "x--",
            "_under",
            "0digit",
            "a.b",
            "x:y",
            "-lead",
            "-",
            ".lead",
            "trail.",
            "has space",
            "a/b",
            "",
        ];
        for local in locals {
            for ns in ["http://example.org/", "http://empty.example/"] {
                let iri = format!("{ns}{local}");
                let Some(pname) = pm.compact(&iri) else {
                    continue;
                };
                let doc = format!(
                    "@prefix ex: <http://example.org/> .\n\
                                   @prefix : <http://empty.example/> .\n\
                                   {pname} <http://ex/p> <http://ex/o> ."
                );
                let mut sink = fluree_graph_ir::GraphCollectorSink::new();
                let parsed = fluree_graph_turtle::parse_with_options(
                    &doc,
                    &mut sink,
                    fluree_graph_turtle::ParserOptions::conformant(),
                );
                assert!(
                    parsed.is_ok(),
                    "compacted {iri:?} to {pname:?}, which does not parse: {parsed:?}"
                );
                let graph = sink.into_graph();
                assert_eq!(
                    graph.iter().next().expect("one triple").s.as_iri(),
                    Some(iri.as_str()),
                    "{pname:?} does not denote {iri:?}"
                );
            }
        }
    }

    #[test]
    fn compaction_declines_local_names_it_cannot_spell() {
        let pm = ctx_map();
        // A slash, a space, and a leading/trailing dot are all outside the
        // conservative PN_LOCAL this map will emit.
        assert_eq!(pm.compact("http://example.org/a/b"), None);
        assert_eq!(pm.compact("http://example.org/a b"), None);
        assert_eq!(pm.compact("http://example.org/.lead"), None);
        assert_eq!(pm.compact("http://example.org/trail."), None);
        // And an IRI under no declared namespace.
        assert_eq!(pm.compact("http://other.org/x"), None);
        // A bare namespace hit is a legal `ex:`.
        assert_eq!(pm.compact("http://example.org/").as_deref(), Some("ex:"));
    }

    #[test]
    fn declarations_are_alphabetical_and_end_in_a_blank_line() {
        let mut buf = Vec::new();
        write_prefix_declarations(&ctx_map(), &mut buf).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "@prefix ex: <http://example.org/> .\n@prefix schema: <http://schema.org/> .\n\n"
        );
    }

    #[test]
    fn an_empty_map_declares_nothing_at_all() {
        let mut buf = Vec::new();
        write_prefix_declarations(&PrefixMap::new(), &mut buf).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn turtle_iris_compact_when_they_can_and_expand_when_they_cannot() {
        let pm = ctx_map();
        let render = |iri: &str| {
            let mut buf = Vec::new();
            write_turtle_iri(&mut buf, iri, &pm).unwrap();
            String::from_utf8(buf).unwrap()
        };
        assert_eq!(render("http://example.org/alice"), "ex:alice");
        assert_eq!(render("http://other.org/bob"), "<http://other.org/bob>");
        // The expanded fallback still escapes what IRIREF forbids.
        assert_eq!(
            render("http://other.org/a b"),
            "<http://other.org/a\\u0020b>"
        );
    }

    #[test]
    fn blank_labels_pass_through_the_turtle_term_writer() {
        let mut buf = Vec::new();
        write_turtle_iri_or_bnode(&mut buf, "_:b123", &ctx_map()).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "_:b123");
    }

    /// A later declaration of the same prefix shadows the earlier one, and
    /// the map stays in longest-namespace-first order afterwards — the
    /// invariant `compact` depends on, and the one a naive push would break.
    #[test]
    fn a_redeclared_prefix_replaces_its_namespace_and_keeps_the_order() {
        let mut pm = PrefixMap::new();
        pm.insert("ex", "http://example.org/");
        pm.insert("deep", "http://example.org/deep/");
        assert_eq!(
            pm.compact("http://example.org/deep/x").as_deref(),
            Some("deep:x")
        );

        pm.insert("ex", "http://example.org/deep/other/");
        assert_eq!(
            pm.iter().count(),
            2,
            "redeclaration replaces, never appends"
        );
        assert_eq!(
            pm.compact("http://example.org/deep/other/x").as_deref(),
            Some("ex:x"),
            "the longest namespace still wins after a redeclaration"
        );
        assert_eq!(
            pm.compact("http://example.org/deep/x").as_deref(),
            Some("deep:x")
        );
        // The old `ex:` namespace is gone.
        assert_eq!(pm.compact("http://example.org/x"), None);
    }
}
