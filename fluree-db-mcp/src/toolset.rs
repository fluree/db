//! The set of MCP toolsets the unified Fluree service can expose.
//!
//! A *toolset* is a named group of MCP tools (`memory_*`, `docs_*`, …) that can
//! be turned on independently. One `fluree` MCP server exposes a selected subset
//! over a single transport, instead of one server per feature.

use std::fmt;

/// A selectable group of MCP tools.
///
/// `Database` (the server's `sparql_query` / `get_data_model`) is intentionally
/// not yet reachable over stdio — it depends on a running server's state. It is
/// reserved here so the `--toolsets` surface is designed for it now; see the
/// crate-level docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Toolset {
    /// `memory_*` + `kg_query` — the developer-memory store (lazy-created).
    Memory,
    /// `docs_*` — the embedded, version-pinned documentation (stateless).
    Docs,
}

impl Toolset {
    /// Every toolset, in canonical order. Used to expand `all`.
    pub const ALL: &'static [Toolset] = &[Toolset::Memory, Toolset::Docs];

    /// The `--toolsets` token for this toolset (`memory`, `docs`).
    pub fn as_str(self) -> &'static str {
        match self {
            Toolset::Memory => "memory",
            Toolset::Docs => "docs",
        }
    }

    /// Parse a single token. `None` for anything unrecognized.
    pub fn parse(token: &str) -> Option<Toolset> {
        match token.trim() {
            "memory" => Some(Toolset::Memory),
            "docs" => Some(Toolset::Docs),
            _ => None,
        }
    }

    /// Parse a `--toolsets` value: `all` (every toolset) or a comma-separated
    /// list (`memory,docs`). Order is normalized to [`Toolset::ALL`] order and
    /// duplicates are removed. Errors on an empty value or any unknown token.
    pub fn parse_selection(value: &str) -> Result<Vec<Toolset>, String> {
        let value = value.trim();
        if value.is_empty() {
            return Err("no toolsets given; use `all` or e.g. `memory,docs`".to_string());
        }
        if value.eq_ignore_ascii_case("all") {
            return Ok(Toolset::ALL.to_vec());
        }
        let mut selected = Vec::new();
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let ts = Toolset::parse(token)
                .ok_or_else(|| format!("unknown toolset '{token}'; valid: memory, docs, all"))?;
            if !selected.contains(&ts) {
                selected.push(ts);
            }
        }
        if selected.is_empty() {
            return Err("no valid toolsets given; use `all` or e.g. `memory,docs`".to_string());
        }
        // Normalize to canonical order for stable args/output.
        selected.sort();
        Ok(selected)
    }

    /// Render a selection back to a canonical comma-separated string
    /// (`memory,docs`), for installer args and `status` output.
    pub fn join(toolsets: &[Toolset]) -> String {
        toolsets
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl fmt::Display for Toolset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_expands_to_every_toolset() {
        assert_eq!(
            Toolset::parse_selection("all").unwrap(),
            vec![Toolset::Memory, Toolset::Docs]
        );
        // case-insensitive
        assert_eq!(Toolset::parse_selection("ALL").unwrap(), Toolset::ALL);
    }

    #[test]
    fn csv_parses_and_normalizes() {
        // out-of-order + duplicate + whitespace -> canonical, deduped
        assert_eq!(
            Toolset::parse_selection(" docs , memory , docs ").unwrap(),
            vec![Toolset::Memory, Toolset::Docs]
        );
        assert_eq!(
            Toolset::parse_selection("memory").unwrap(),
            vec![Toolset::Memory]
        );
    }

    #[test]
    fn unknown_and_empty_are_errors() {
        assert!(Toolset::parse_selection("bogus").is_err());
        assert!(Toolset::parse_selection("").is_err());
        assert!(Toolset::parse_selection("  ").is_err());
    }

    #[test]
    fn join_is_canonical() {
        assert_eq!(Toolset::join(Toolset::ALL), "memory,docs");
        assert_eq!(Toolset::join(&[Toolset::Docs]), "docs");
    }
}
