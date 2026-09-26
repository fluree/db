//! Prefix map: IRI → prefixed name compression for Turtle and TriG.

use fluree_graph_ir::syntax;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::io::{self, Write};

/// Prefixes for writing IRIs as prefixed names (`ex:alice`).
///
/// Only prefixes that are valid Turtle `PN_PREFIX` names are kept, so every
/// declaration and prefixed name the map produces parses.
#[derive(Debug, Clone, Default)]
pub struct PrefixMap {
    /// `(prefix, namespace)`, longest namespace first so the most specific
    /// prefix wins.
    entries: Vec<(String, String)>,
}

impl PrefixMap {
    /// Build from a JSON-LD `@context`: its string-valued, non-`@` entries.
    /// An array context contributes the entries of each object in it.
    pub fn from_context(ctx: &JsonValue) -> Self {
        let mut map = BTreeMap::new();
        collect_context(ctx, &mut map);
        Self::from_map(map)
    }

    /// Build from an explicit prefix → namespace map.
    pub fn from_map(map: BTreeMap<String, String>) -> Self {
        let mut entries: Vec<(String, String)> = map
            .into_iter()
            .filter(|(prefix, _)| syntax::is_pn_prefix(prefix))
            .collect();
        entries.sort_by_key(|(_, ns)| std::cmp::Reverse(ns.len()));
        PrefixMap { entries }
    }

    /// Split `iri` into `(prefix, local)` for the longest namespace it starts
    /// with whose remainder is a valid local name. No allocation.
    pub fn prefixed_name<'a>(&'a self, iri: &'a str) -> Option<(&'a str, &'a str)> {
        self.entries.iter().find_map(|(prefix, ns)| {
            iri.strip_prefix(ns.as_str())
                .filter(|local| syntax::is_pn_local(local))
                .map(|local| (prefix.as_str(), local))
        })
    }

    /// `iri` as a prefixed name (`ex:alice`), if one applies.
    pub fn compact(&self, iri: &str) -> Option<String> {
        self.prefixed_name(iri)
            .map(|(prefix, local)| format!("{prefix}:{local}"))
    }

    /// Append `iri` as a prefixed name, or as `<iri>` when none applies.
    pub fn push_iri(&self, out: &mut String, iri: &str) {
        match self.prefixed_name(iri) {
            Some((prefix, local)) => {
                out.push_str(prefix);
                out.push(':');
                out.push_str(local);
            }
            None => syntax::push_iri_ref(out, iri),
        }
    }

    /// Write `iri` as a prefixed name, or as `<iri>` when none applies.
    pub fn write_iri<W: Write + ?Sized>(&self, w: &mut W, iri: &str) -> io::Result<()> {
        match self.prefixed_name(iri) {
            Some((prefix, local)) => {
                w.write_all(prefix.as_bytes())?;
                w.write_all(b":")?;
                w.write_all(local.as_bytes())
            }
            None => syntax::write_iri_ref(w, iri),
        }
    }

    /// `@prefix` lines in prefix order, then a blank line if there were any.
    pub fn push_declarations(&self, out: &mut String) {
        for (prefix, ns) in self.sorted() {
            out.push_str("@prefix ");
            out.push_str(prefix);
            out.push_str(": ");
            syntax::push_iri_ref(out, ns);
            out.push_str(" .\n");
        }
        if !self.is_empty() {
            out.push('\n');
        }
    }

    /// [`Self::push_declarations`] to a writer.
    pub fn write_declarations<W: Write + ?Sized>(&self, w: &mut W) -> io::Result<()> {
        for (prefix, ns) in self.sorted() {
            write!(w, "@prefix {prefix}: ")?;
            syntax::write_iri_ref(w, ns)?;
            w.write_all(b" .\n")?;
        }
        if !self.is_empty() {
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    fn sorted(&self) -> Vec<(&str, &str)> {
        let mut sorted: Vec<(&str, &str)> = self.iter().collect();
        sorted.sort_unstable();
        sorted
    }

    /// Whether the map has no prefixes.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `(prefix, namespace)` pairs, longest namespace first.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(p, n)| (p.as_str(), n.as_str()))
    }
}

fn collect_context(ctx: &JsonValue, map: &mut BTreeMap<String, String>) {
    match ctx {
        JsonValue::Object(obj) => {
            for (key, val) in obj {
                if let (false, Some(ns)) = (key.starts_with('@'), val.as_str()) {
                    map.insert(key.clone(), ns.to_string());
                }
            }
        }
        JsonValue::Array(items) => items.iter().for_each(|item| collect_context(item, map)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn longest_namespace_with_a_valid_local_wins() {
        let pm = PrefixMap::from_context(&json!({
            "@vocab": "http://example.org/",
            "ex": "http://example.org/",
            "exns": "http://example.org/ns/",
            "_bad": "http://bad.org/",
            "term": {"@id": "http://example.org/term"}
        }));
        assert_eq!(
            pm.compact("http://example.org/ns/thing").as_deref(),
            Some("exns:thing")
        );
        assert_eq!(
            pm.compact("http://example.org/alice").as_deref(),
            Some("ex:alice")
        );
        // Neither `exns:a/b` nor `ex:ns/a/b` is a valid prefixed name.
        assert_eq!(pm.compact("http://example.org/ns/a/b").as_deref(), None);
        assert_eq!(
            pm.compact("http://bad.org/x"),
            None,
            "invalid prefix dropped"
        );
        assert_eq!(pm.iter().count(), 2);

        let mut out = String::new();
        pm.push_iri(&mut out, "http://example.org/a b");
        assert_eq!(out, r"<http://example.org/a\u0020b>");
    }

    #[test]
    fn declarations_are_sorted_and_escaped() {
        let pm = PrefixMap::from_context(&json!([
            {"z": "http://z.org/"},
            {"a": "http://a.org/<x>/"}
        ]));
        let mut out = String::new();
        pm.push_declarations(&mut out);
        assert_eq!(
            out,
            "@prefix a: <http://a.org/\\u003Cx\\u003E/> .\n@prefix z: <http://z.org/> .\n\n"
        );
        let mut buf = Vec::new();
        pm.write_declarations(&mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), out);
    }
}
