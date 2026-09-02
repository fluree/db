//! Structure-aware chunking.
//!
//! Walks the DoCO graph rather than the text: chunk boundaries fall between
//! paragraphs, list items, captions and table cells, never mid-sentence by
//! character count alone; each chunk cites the element IRIs it was built
//! from; and the `doco:Section → doco:SectionTitle` chain travels with it as
//! a header path, so retrieval can boost or filter on hierarchy and the
//! embedding input carries the context a bare paragraph loses.

use crate::{DocError, Result};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    /// Section titles enclosing the chunk, outermost first.
    pub header_path: Vec<String>,
    /// Body text, whitespace collapsed. The header path is separate.
    pub text: String,
    /// `@id` of every element that contributed text, in walk order.
    pub source_ids: Vec<String>,
}

impl Chunk {
    /// What gets embedded: the hierarchy as a preamble, then the body.
    pub fn embedding_input(&self) -> String {
        if self.header_path.is_empty() {
            self.text.clone()
        } else {
            format!("{}\n\n{}", self.header_path.join(" / "), self.text)
        }
    }

    pub fn header_path_string(&self) -> String {
        self.header_path.join(" / ")
    }
}

#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// A chunk is emitted once the buffer reaches this many bytes.
    pub min_chars: usize,
    /// A single element longer than this is split at sentence boundaries.
    pub max_chars: usize,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            min_chars: 800,
            max_chars: 2000,
        }
    }
}

/// Chunk a DoCO JSON-LD document (the `doco` output of a parse).
pub fn chunk_doco(doco_json: &str, config: &ChunkConfig) -> Result<Vec<Chunk>> {
    let graph: Value = serde_json::from_str(doco_json)
        .map_err(|e| DocError::Parse(format!("doco graph is not JSON: {e}")))?;
    chunk_graph(&graph, config)
}

pub fn chunk_graph(graph_doc: &Value, config: &ChunkConfig) -> Result<Vec<Chunk>> {
    let graph = graph_doc
        .get("@graph")
        .and_then(Value::as_array)
        .ok_or_else(|| DocError::Parse("doco graph: missing @graph array".into()))?;

    let mut by_id: HashMap<&str, &Value> = HashMap::with_capacity(graph.len());
    for el in graph {
        if let Some(id) = el.get("@id").and_then(Value::as_str) {
            by_id.insert(id, el);
        }
    }
    let Some(root) = graph
        .iter()
        .find(|el| type_of(el) == "doco:Document")
        .or_else(|| graph.first())
    else {
        return Ok(Vec::new());
    };
    let root_id = root.get("@id").and_then(Value::as_str).unwrap_or_default();

    let mut state = WalkState::new(config.clone());
    walk(root_id, &by_id, &mut state);
    state.flush();
    Ok(state.chunks)
}

struct WalkState {
    config: ChunkConfig,
    headers: Vec<(u64, String)>,
    buffer: String,
    /// The header path when the buffer's first text arrived: a chunk is
    /// labelled by where it starts, not by where the walk was when it filled.
    buffer_headers: Vec<String>,
    pending_ids: Vec<String>,
    chunks: Vec<Chunk>,
}

impl WalkState {
    fn new(config: ChunkConfig) -> Self {
        Self {
            config,
            headers: Vec::new(),
            buffer: String::new(),
            buffer_headers: Vec::new(),
            pending_ids: Vec::new(),
            chunks: Vec::new(),
        }
    }

    fn push_text(&mut self, text: &str, source_id: &str) {
        let collapsed = collapse_whitespace(text);
        if collapsed.is_empty() {
            return;
        }
        for (i, piece) in split_to_max(&collapsed, self.config.max_chars)
            .into_iter()
            .enumerate()
        {
            if self.buffer.is_empty() {
                self.buffer_headers = self.headers.iter().map(|(_, t)| t.clone()).collect();
            } else {
                self.buffer.push_str("\n\n");
            }
            self.buffer.push_str(piece);
            if i == 0 {
                self.pending_ids.push(source_id.to_string());
            }
            if self.buffer.len() >= self.config.min_chars {
                self.flush();
            }
        }
    }

    /// A heading is a natural seam. Cut there once the buffer is at least
    /// half full, so a chunk rarely straddles sections but a run of short
    /// sections still packs together.
    fn heading_boundary(&mut self) {
        if self.buffer.len() >= self.config.min_chars / 2 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.chunks.push(Chunk {
            header_path: std::mem::take(&mut self.buffer_headers),
            text: std::mem::take(&mut self.buffer),
            source_ids: std::mem::take(&mut self.pending_ids),
        });
    }
}

fn type_of(node: &Value) -> &str {
    match node.get("@type") {
        Some(Value::String(s)) => s,
        Some(Value::Array(a)) => a.first().and_then(Value::as_str).unwrap_or(""),
        _ => "",
    }
}

/// Full text of a text-bearing element. `rdfs:label` is a display preview
/// capped at 100 chars, so it is only the fallback.
fn body_text(node: &Value) -> Option<&str> {
    node.get("nif:isString")
        .and_then(Value::as_str)
        .or_else(|| node.get("rdfs:label").and_then(Value::as_str))
}

fn walk(node_id: &str, by_id: &HashMap<&str, &Value>, state: &mut WalkState) {
    let Some(node) = by_id.get(node_id).copied() else {
        return;
    };
    match type_of(node) {
        "doco:Section" => {
            let level = node
                .get("doc:sectionLevel")
                .and_then(Value::as_u64)
                .unwrap_or(2);
            let title = section_title(node, by_id);
            state.heading_boundary();
            // A new heading closes every section at its depth or deeper.
            while state.headers.last().is_some_and(|(l, _)| *l >= level) {
                state.headers.pop();
            }
            if let Some(t) = title {
                state.headers.push((level, t));
            }
            for child in children(node) {
                if by_id
                    .get(child)
                    .is_some_and(|c| type_of(c) == "doco:SectionTitle")
                {
                    continue;
                }
                walk(child, by_id, state);
            }
        }
        "doco:Title" | "doco:Paragraph" | "doco:ListItem" | "doco:Caption" => {
            if let Some(text) = body_text(node) {
                state.push_text(text, node_id);
            }
        }
        // Row and column headers travel with the value, so a cell embeds as
        // "Supply voltage / LM358B: 3 V" rather than a bare "3 V".
        "doc:TableCell" => {
            let value = node
                .get("doc:cellValue")
                .and_then(Value::as_str)
                .unwrap_or("");
            if value.trim().is_empty() {
                return;
            }
            let row = node
                .get("doc:rowHeader")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            let col = node
                .get("doc:columnHeader")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            let formatted = match (row, col) {
                (Some(r), Some(c)) => format!("{r} / {c}: {value}"),
                (Some(r), None) => format!("{r}: {value}"),
                (None, Some(c)) => format!("{c}: {value}"),
                (None, None) => value.to_string(),
            };
            state.push_text(&formatted, node_id);
        }
        // Consumed as its parent's header; repeating it here would put the
        // heading into the body as well.
        "doco:SectionTitle" => {}
        // Containers, and anything this walker does not know: recurse.
        _ => {
            for child in children(node) {
                walk(child, by_id, state);
            }
        }
    }
}

fn children(node: &Value) -> impl Iterator<Item = &str> {
    node.get("po:contains")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
}

fn section_title(section: &Value, by_id: &HashMap<&str, &Value>) -> Option<String> {
    children(section)
        .filter_map(|id| by_id.get(id).copied())
        .find(|c| type_of(c) == "doco:SectionTitle")
        .and_then(body_text)
        .map(collapse_whitespace)
        .filter(|t| !t.is_empty())
}

fn collapse_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = true;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
        } else {
            out.push(ch);
            last_was_space = false;
        }
    }
    out.trim().to_string()
}

/// Split one oversized string into pieces of at most `max` bytes, cutting at
/// a sentence end where one falls in the back half, else at whitespace,
/// else at a character boundary.
fn split_to_max(s: &str, max: usize) -> Vec<&str> {
    if s.len() <= max {
        return vec![s];
    }
    let mut pieces = Vec::new();
    let mut start = 0;
    while start < s.len() {
        let rest = &s[start..];
        if rest.len() <= max {
            pieces.push(rest.trim());
            break;
        }
        let window = &rest[..floor_char_boundary(rest, max)];
        let floor = window.len() / 2;
        let cut = window
            .rmatch_indices(['.', '!', '?'])
            .find(|(i, _)| *i >= floor && window[*i + 1..].starts_with(' '))
            .map(|(i, _)| i + 1)
            .or_else(|| window.rfind(' ').filter(|i| *i >= floor))
            .unwrap_or(window.len());
        pieces.push(window[..cut].trim());
        start += cut;
        while start < s.len() && s.as_bytes()[start] == b' ' {
            start += 1;
        }
    }
    pieces.retain(|p| !p.is_empty());
    pieces
}

fn floor_char_boundary(s: &str, idx: usize) -> usize {
    let mut i = idx.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn graph(elements: Vec<Value>) -> Value {
        json!({ "@graph": elements })
    }

    #[test]
    fn small_doc_yields_one_chunk_with_sources() {
        let g = graph(vec![
            json!({"@id":"d","@type":"doco:Document","po:contains":["b"]}),
            json!({"@id":"b","@type":"doco:BodyMatter","po:contains":["p1","p2"]}),
            json!({"@id":"p1","@type":"doco:Paragraph","nif:isString":"Hello   world."}),
            json!({"@id":"p2","@type":"doco:Paragraph","nif:isString":"Second."}),
        ]);
        let chunks = chunk_graph(&g, &ChunkConfig::default()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Hello world.\n\nSecond.");
        assert_eq!(chunks[0].source_ids, vec!["p1", "p2"]);
        assert!(chunks[0].header_path.is_empty());
    }

    #[test]
    fn section_titles_become_header_path_and_pop_by_level() {
        let g = graph(vec![
            json!({"@id":"d","@type":"doco:Document","po:contains":["s1","s2"]}),
            json!({"@id":"s1","@type":"doco:Section","doc:sectionLevel":1,"po:contains":["t1","s11"]}),
            json!({"@id":"t1","@type":"doco:SectionTitle","nif:isString":"Intro"}),
            json!({"@id":"s11","@type":"doco:Section","doc:sectionLevel":2,"po:contains":["t11","p1"]}),
            json!({"@id":"t11","@type":"doco:SectionTitle","rdfs:label":"Scope"}),
            json!({"@id":"p1","@type":"doco:Paragraph","nif:isString":"A"}),
            json!({"@id":"s2","@type":"doco:Section","doc:sectionLevel":1,"po:contains":["t2","p2"]}),
            json!({"@id":"t2","@type":"doco:SectionTitle","nif:isString":"Method"}),
            json!({"@id":"p2","@type":"doco:Paragraph","nif:isString":"B"}),
        ]);
        let cfg = ChunkConfig {
            min_chars: 1,
            max_chars: 100,
        };
        let chunks = chunk_graph(&g, &cfg).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].header_path, vec!["Intro", "Scope"]);
        assert_eq!(chunks[0].embedding_input(), "Intro / Scope\n\nA");
        assert_eq!(chunks[1].header_path, vec!["Method"]);
    }

    #[test]
    fn chunk_is_labelled_by_where_it_starts() {
        let g = graph(vec![
            json!({"@id":"d","@type":"doco:Document","po:contains":["s1","s2"]}),
            json!({"@id":"s1","@type":"doco:Section","doc:sectionLevel":2,"po:contains":["t1","p1"]}),
            json!({"@id":"t1","@type":"doco:SectionTitle","nif:isString":"Setup"}),
            json!({"@id":"p1","@type":"doco:Paragraph","nif:isString":"short"}),
            json!({"@id":"s2","@type":"doco:Section","doc:sectionLevel":2,"po:contains":["t2","p2"]}),
            json!({"@id":"t2","@type":"doco:SectionTitle","nif:isString":"Policy"}),
            json!({"@id":"p2","@type":"doco:Paragraph","nif:isString":"also short"}),
        ]);
        // Too little text to reach the heading-boundary threshold: one chunk,
        // named for the section it began in.
        let chunks = chunk_graph(&g, &ChunkConfig::default()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].header_path, vec!["Setup"]);

        // With a threshold the first section reaches, the heading is a seam.
        let cfg = ChunkConfig {
            min_chars: 10,
            max_chars: 100,
        };
        let chunks = chunk_graph(&g, &cfg).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].header_path, vec!["Policy"]);
    }

    #[test]
    fn table_cells_carry_headers() {
        let g = graph(vec![
            json!({"@id":"d","@type":"doco:Document","po:contains":["t"]}),
            json!({"@id":"t","@type":"doco:Table","po:contains":["c"]}),
            json!({"@id":"c","@type":"doc:TableCell","doc:cellValue":"3 V",
                   "doc:rowHeader":"Supply","doc:columnHeader":"LM358B"}),
        ]);
        let chunks = chunk_graph(&g, &ChunkConfig::default()).unwrap();
        assert_eq!(chunks[0].text, "Supply / LM358B: 3 V");
    }

    #[test]
    fn oversized_element_splits_at_sentences() {
        let text = "One sentence here. Another sentence follows. And a third one ends.";
        let pieces = split_to_max(text, 40);
        assert_eq!(
            pieces,
            vec![
                "One sentence here. Another sentence",
                "follows. And a third one ends."
            ]
        );
    }

    #[test]
    fn split_respects_char_boundaries() {
        let text = "ééééééééééééééééééééé ééééééééééé";
        for piece in split_to_max(text, 11) {
            assert!(piece.len() <= 11);
        }
    }
}
