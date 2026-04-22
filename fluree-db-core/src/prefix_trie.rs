//! Byte-level prefix trie for O(len(iri)) longest-prefix matching.
//!
//! Used for fast IRI prefix resolution when namespace tables are large.
//! This is a shared utility across crates (query/indexer/transact) to avoid
//! duplicating trie implementations with subtly different behavior.

use std::collections::HashMap;

/// A node in the byte-level prefix trie.
///
/// Children are stored as a sorted `Vec<(u8, u32)>` instead of a HashMap.
/// Most nodes in URI prefix tries have 1-3 children, where a linear scan
/// beats HashMap's hashing + heap allocation overhead.
#[derive(Debug, Clone)]
struct TrieNode {
    /// Namespace code if a registered prefix ends at this node.
    code: Option<u16>,
    /// Children sorted by byte value.
    children: Vec<(u8, u32)>,
}

/// Byte-level trie for longest-prefix matching of IRI strings.
///
/// Each registered namespace prefix is inserted byte-by-byte. Lookup walks
/// the trie following the IRI's bytes, tracking the deepest node that has a
/// namespace code set. This gives O(len(iri)) lookup time independent of the
/// number of registered prefixes.
#[derive(Debug, Clone)]
pub struct PrefixTrie {
    nodes: Vec<TrieNode>,
}

impl Default for PrefixTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefixTrie {
    /// Create an empty trie.
    pub fn new() -> Self {
        Self {
            nodes: vec![TrieNode {
                code: None,
                children: Vec::new(),
            }],
        }
    }

    /// Build a PrefixTrie from a namespace code → prefix map.
    ///
    /// Skips the empty prefix (code 0) — unmatched IRIs fall through
    /// to the caller's default handling.
    pub fn from_namespace_codes(codes: &HashMap<u16, String>) -> Self {
        let mut trie = Self::new();
        for (&code, prefix) in codes {
            if !prefix.is_empty() {
                trie.insert(prefix, code);
            }
        }
        trie
    }

    /// Insert a prefix string with its namespace code.
    pub fn insert(&mut self, prefix: &str, code: u16) {
        let mut node_idx: u32 = 0;
        for &byte in prefix.as_bytes() {
            let children = &self.nodes[node_idx as usize].children;
            node_idx = match children.iter().find(|(b, _)| *b == byte) {
                Some(&(_, child_idx)) => child_idx,
                None => {
                    let new_idx = self.nodes.len() as u32;
                    self.nodes.push(TrieNode {
                        code: None,
                        children: Vec::new(),
                    });
                    let node = &mut self.nodes[node_idx as usize];
                    let pos = node.children.partition_point(|(b, _)| *b < byte);
                    node.children.insert(pos, (byte, new_idx));
                    new_idx
                }
            };
        }
        self.nodes[node_idx as usize].code = Some(code);
    }

    /// Find the longest registered prefix that matches the start of `iri`.
    ///
    /// Returns `(namespace_code, prefix_byte_length)` or `None` if no
    /// non-empty prefix matches.
    pub fn longest_match(&self, iri: &str) -> Option<(u16, usize)> {
        let mut node_idx: u32 = 0;
        let mut best: Option<(u16, usize)> = None;

        for (i, &byte) in iri.as_bytes().iter().enumerate() {
            let children = &self.nodes[node_idx as usize].children;
            match children.iter().find(|(b, _)| *b == byte) {
                Some(&(_, child_idx)) => {
                    node_idx = child_idx;
                    if let Some(code) = self.nodes[node_idx as usize].code {
                        best = Some((code, i + 1));
                    }
                }
                None => break,
            }
        }

        best
    }

    /// Number of nodes in the trie (for diagnostics).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_match() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://www.w3.org/1999/02/22-rdf-syntax-ns#", 3);
        trie.insert("http://www.w3.org/2001/XMLSchema#", 2);

        let result = trie.longest_match("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        assert_eq!(result, Some((3, 43)));

        let result = trie.longest_match("http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(result, Some((2, 33)));
    }

    #[test]
    fn test_longest_prefix_wins() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://example.org/", 10);
        trie.insert("http://example.org/ontology/", 20);

        // Longer prefix should win
        let result = trie.longest_match("http://example.org/ontology/Person");
        assert_eq!(result, Some((20, 28)));

        // Short prefix matches when longer doesn't
        let result = trie.longest_match("http://example.org/data/123");
        assert_eq!(result, Some((10, 19)));
    }

    #[test]
    fn test_no_match() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://example.org/", 10);

        // Different scheme
        assert_eq!(trie.longest_match("https://example.org/foo"), None);

        // Empty string
        assert_eq!(trie.longest_match(""), None);

        // Partial match (prefix not fully consumed)
        assert_eq!(trie.longest_match("http://example.com/foo"), None);
    }

    #[test]
    fn test_from_namespace_codes() {
        let mut codes = HashMap::new();
        codes.insert(0, String::new()); // empty prefix, should be skipped
        codes.insert(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
        codes.insert(8, "https://ns.flur.ee/db#".to_string());

        let trie = PrefixTrie::from_namespace_codes(&codes);

        // Empty prefix should NOT match
        assert_eq!(trie.longest_match("something-random"), None);

        // Registered prefixes should match
        let result = trie.longest_match("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        assert_eq!(result, Some((3, 43)));

        let result = trie.longest_match("https://ns.flur.ee/db#address");
        assert_eq!(result, Some((8, 22)));
    }

    #[test]
    fn test_exact_prefix_match() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://example.org/", 10);

        // Exact prefix with no local name
        let result = trie.longest_match("http://example.org/");
        assert_eq!(result, Some((10, 19)));
    }
}
