//! Cypher bulk import — a front-end that converts `.cypher` scripts of
//! `CREATE` / `MATCH … CREATE` statements (the Neo4j / Memgraph dump
//! convention) into JSON-LD objects, which then feed the existing JSON-LD
//! bulk-import pipeline. By emitting JSON-LD we reuse the whole downstream
//! machinery: `@annotation` → `f:reifies*` lowering, datatype typing,
//! namespace allocation, chunked commits, and annotation-arena sealing —
//! instead of replaying millions of statements over the per-transaction
//! write path.
//!
//! # Supported statement shapes
//!
//! 1. `CREATE (:Label {props}), (a:Label {props})-[:TYPE {props}]->(b), …;`
//!    — node and path creation. Variables are scoped to the statement.
//! 2. `MATCH (n:Label {key: val}), (m:Label {key: val}) CREATE (n)-[e:TYPE {props}]->(m);`
//!    — edge creation between property-matched nodes (the dump idiom for
//!    relationships). MATCH parts must be single nodes with a label and a
//!    literal property map; `WHERE` is not supported.
//!
//! Anything else (`MERGE`, `SET`, `DELETE`, reads, …) fails the import with
//! the statement's line number: a bulk loader must not silently drop data.
//!
//! # Node identity
//!
//! Cypher `CREATE` mints anonymous nodes; a bulk file's only cross-statement
//! identity is the property map a later `MATCH` finds a node by. The importer
//! therefore learns, in a first pass, which property sets each label is
//! matched by (e.g. `User` → `{id}`), and derives every node's stable id from
//! its label + those key values: `CREATE (:User {id: 42, age: 7})` becomes
//! `@id: "User/42"`, and `MATCH (n:User {id: 42})` resolves to the same id.
//! Nodes whose label is never matched (or that lack the key properties) get a
//! sequential `Label/_anon-N` id. Consequences, by design:
//!
//! - Two `CREATE`s with identical key values merge into one node (Cypher
//!   would create two); dumps use unique keys, so this is the useful reading.
//! - An edge whose endpoint was never created is skipped and counted
//!   (`MATCH` finds nothing → the statement is a no-op), reported in
//!   [`CypherImportStats::edges_skipped`].
//!
//! # Naming
//!
//! By default labels, relationship types, and property keys are emitted as
//! **bare names** — they land in namespace 0 exactly like Cypher's own write
//! path, so the imported data is readable with zero-config Cypher
//! (`MATCH (n:User {id: 42})`). Setting [`CypherImportOptions::vocab`]
//! prefixes every minted name with an IRI base instead (the RDF-compat mode,
//! matching a ledger default context with that `@vocab`).
//!
//! # Edges
//!
//! Property-less edges are plain RDF triples; property-bearing edges follow
//! [`EdgePolicy`] exactly like the CSV importer (`@annotation` by default).
//! Note the Cypher read contract (`docs/query/cypher.md`): a plain triple is
//! visible to `(a)-[:T]->(b)` but a relationship *variable* (`[r:T]`) only
//! binds reified edges.

pub use crate::csv_import::EdgePolicy;
use crate::csv_import::{iri_segment, CsvImportError};

use fluree_db_cypher::ast::{
    Direction, Expr, Literal, MapLit, NodePattern, Pattern, ReadClause, RelPattern, Statement,
    UnaryOp, WriteClause,
};
use fluree_db_cypher::parse_cypher;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::{Map, Value};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, Write};

/// Options for the `.cypher` → JSON-LD conversion.
#[derive(Debug, Clone, Default)]
pub struct CypherImportOptions {
    /// Edge-property encoding ([`EdgePolicy`], the CLI's `--edge-properties`).
    pub edge_policy: EdgePolicy,
    /// Optional IRI prefix for minted names (RDF-compat mode). `None` (the
    /// default) emits bare names in namespace 0, matching zero-config Cypher.
    pub vocab: Option<String>,
}

/// Cypher-import failure. Statement-level errors carry the 1-based line the
/// statement starts on.
#[derive(Debug, thiserror::Error)]
pub enum CypherImportError {
    #[error("line {line}: Cypher parse error: {msg}")]
    Parse { line: usize, msg: String },
    #[error("line {line}: unsupported statement for bulk import: {msg}")]
    Unsupported { line: usize, msg: String },
    #[error(
        "label `{label}` is matched by conflicting property sets ({first:?} vs {second:?}); \
         node identity needs one key set per label"
    )]
    ConflictingMatchKeys {
        label: String,
        first: Vec<String>,
        second: Vec<String>,
    },
    #[error(
        "`--edge-properties nary` is not implemented yet (relationship `{rel_type}` carries \
         properties); use `annotated` or `plain` for now"
    )]
    NaryDeferred { rel_type: String },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<CypherImportError> for CsvImportError {
    fn from(e: CypherImportError) -> Self {
        match e {
            CypherImportError::Io(io) => CsvImportError::Io(io),
            other => CsvImportError::Io(std::io::Error::other(other)),
        }
    }
}

type Result<T> = std::result::Result<T, CypherImportError>;

/// Conversion counters, accumulated across all passes and files.
#[derive(Debug, Clone, Copy, Default)]
pub struct CypherImportStats {
    /// Node objects emitted.
    pub nodes: usize,
    /// Edges emitted.
    pub edges: usize,
    /// `MATCH … CREATE` edges skipped because an endpoint node was never
    /// created (Cypher's `MATCH` would find nothing → no-op).
    pub edges_skipped: usize,
}

/// Streaming `.cypher` → JSON-LD converter. Three passes over the input
/// (cheap sequential scans; each statement is fully parsed exactly once):
///
/// 1. [`learn_keys`](Self::learn_keys) — scan `MATCH … CREATE` statements to
///    learn each label's match-key property set.
/// 2. [`write_nodes_ndjson`](Self::write_nodes_ndjson) — emit node objects
///    (and edges inlined in pure `CREATE` paths), recording minted node ids.
/// 3. [`write_edges_ndjson`](Self::write_edges_ndjson) — emit
///    `MATCH … CREATE` edges, skipping ones with unknown endpoints.
///
/// For multi-file imports run each pass over *all* files before starting the
/// next, so cross-file references resolve regardless of file order.
#[derive(Debug, Default)]
pub struct CypherImporter {
    opts: CypherImportOptions,
    /// label → sorted property names the label is `MATCH`ed by.
    match_keys: FxHashMap<String, Vec<String>>,
    /// Hashes of every minted node id (dangling-edge detection).
    node_ids: FxHashSet<u64>,
    /// Sequence for `Label/_anon-N` ids of key-less nodes.
    anon_seq: u64,
    /// Conversion counters.
    pub stats: CypherImportStats,
}

impl CypherImporter {
    pub fn new(opts: CypherImportOptions) -> Self {
        Self {
            opts,
            ..Default::default()
        }
    }

    /// Pass 1: learn match-key property sets from `MATCH … CREATE` statements.
    pub fn learn_keys<R: BufRead>(&mut self, src: R) -> Result<()> {
        for_each_statement(src, |line, stmt| {
            if !starts_with_keyword(stmt, "MATCH") {
                return Ok(()); // only MATCH statements define keys
            }
            let update = parse_update(stmt, line)?;
            for node in match_nodes(&update.read_clauses, line)? {
                let (label, props) = match_node_parts(node, line)?;
                let mut keys: Vec<String> = props.entries.iter().map(|(k, _)| k.clone()).collect();
                keys.sort_unstable();
                keys.dedup();
                match self.match_keys.get(&label) {
                    None => {
                        self.match_keys.insert(label, keys);
                    }
                    Some(existing) if *existing == keys => {}
                    Some(existing) => {
                        return Err(CypherImportError::ConflictingMatchKeys {
                            label,
                            first: existing.clone(),
                            second: keys,
                        });
                    }
                }
            }
            Ok(())
        })
    }

    /// Pass 2: emit `CREATE` statements (nodes, plus any edges inlined in
    /// their paths) as newline-delimited JSON-LD. Returns objects written.
    pub fn write_nodes_ndjson<R: BufRead, W: Write>(
        &mut self,
        src: R,
        out: &mut W,
    ) -> Result<usize> {
        let mut count = 0usize;
        for_each_statement(src, |line, stmt| {
            if !starts_with_keyword(stmt, "CREATE") {
                // MATCH statements are pass 3; anything else must error here
                // so unsupported statements surface even in node-only files.
                if !starts_with_keyword(stmt, "MATCH") {
                    let update = parse_update(stmt, line)?;
                    let name = update
                        .write_clauses
                        .first()
                        .map(write_clause_name)
                        .unwrap_or("this statement");
                    return Err(unsupported(
                        line,
                        &format!("{name} is not supported in bulk import"),
                    ));
                }
                return Ok(());
            }
            let update = parse_update(stmt, line)?;
            if !update.read_clauses.is_empty() {
                return Err(unsupported(line, "CREATE with read clauses (use MATCH … CREATE for edges between existing nodes)"));
            }
            count += self.emit_update_creates(
                &update.write_clauses,
                line,
                &mut FxHashMap::default(),
                out,
            )?;
            Ok(())
        })?;
        Ok(count)
    }

    /// Pass 3: emit `MATCH … CREATE` edge statements as newline-delimited
    /// JSON-LD. Endpoints resolve through the same key-derived ids pass 2
    /// minted; edges whose endpoints were never created are skipped and
    /// counted. Returns objects written.
    pub fn write_edges_ndjson<R: BufRead, W: Write>(
        &mut self,
        src: R,
        out: &mut W,
    ) -> Result<usize> {
        let mut count = 0usize;
        for_each_statement(src, |line, stmt| {
            if !starts_with_keyword(stmt, "MATCH") {
                return Ok(()); // CREATE statements were pass 2
            }
            let update = parse_update(stmt, line)?;

            // Bind each matched variable to its key-derived node id.
            let mut vars: FxHashMap<String, String> = FxHashMap::default();
            let mut all_known = true;
            for node in match_nodes(&update.read_clauses, line)? {
                let (label, props) = match_node_parts(node, line)?;
                let var = node
                    .var
                    .as_ref()
                    .ok_or_else(|| unsupported(line, "MATCH node needs a variable"))?;
                let id = self.node_id_from_props(&label, props, line)?;
                all_known &= self.node_ids.contains(&id_hash(&id));
                vars.insert(var.name.clone(), id);
            }
            if !all_known {
                self.stats.edges_skipped += 1;
                return Ok(());
            }
            count += self.emit_update_creates(&update.write_clauses, line, &mut vars, out)?;
            Ok(())
        })?;
        Ok(count)
    }

    /// Emit every CREATE clause of a statement: mint ids for new nodes, walk
    /// each path, and write node/edge JSON-LD objects. `vars` carries
    /// MATCH-bound (pass 3) and within-statement bindings.
    fn emit_update_creates<W: Write>(
        &mut self,
        write_clauses: &[WriteClause],
        line: usize,
        vars: &mut FxHashMap<String, String>,
        out: &mut W,
    ) -> Result<usize> {
        let mut count = 0usize;
        for wc in write_clauses {
            let create = match wc {
                WriteClause::Create(c) => c,
                other => {
                    return Err(unsupported(
                        line,
                        &format!(
                            "{} is not supported in bulk import",
                            write_clause_name(other)
                        ),
                    ))
                }
            };
            count += self.emit_pattern(&create.pattern, line, vars, out)?;
        }
        Ok(count)
    }

    fn emit_pattern<W: Write>(
        &mut self,
        pattern: &Pattern,
        line: usize,
        vars: &mut FxHashMap<String, String>,
        out: &mut W,
    ) -> Result<usize> {
        let mut count = 0usize;
        for part in &pattern.parts {
            if part.path_search.is_some() || part.path_var.is_some() {
                return Err(unsupported(line, "path expressions in CREATE"));
            }
            let mut subject = self.create_node(&part.head, line, vars, out, &mut count)?;
            for (rel, node) in &part.tail {
                let object = self.create_node(node, line, vars, out, &mut count)?;
                let (from, to) = match rel.direction {
                    Direction::Outgoing => (&subject, &object),
                    Direction::Incoming => (&object, &subject),
                    Direction::Either => {
                        return Err(unsupported(line, "undirected relationship in CREATE"))
                    }
                };
                self.emit_edge(rel, from, to, line, out)?;
                count += 1;
                subject = object;
            }
        }
        Ok(count)
    }

    /// Resolve a node pattern inside CREATE: an already-bound variable is a
    /// reference; anything else mints a new node (emitting its object when it
    /// has labels or properties).
    fn create_node<W: Write>(
        &mut self,
        node: &NodePattern,
        line: usize,
        vars: &mut FxHashMap<String, String>,
        out: &mut W,
        count: &mut usize,
    ) -> Result<String> {
        if let Some(var) = &node.var {
            if let Some(id) = vars.get(&var.name) {
                if !node.labels.is_empty() || node.props.is_some() {
                    return Err(unsupported(
                        line,
                        &format!("variable `{}` is already bound; a reference takes no labels/properties", var.name),
                    ));
                }
                return Ok(id.clone());
            }
        }

        let label = node.labels.first().map(|l| l.name.as_str());
        let id = match (label, &node.props) {
            (Some(label), Some(props)) if self.covers_match_keys(label, props) => {
                self.node_id_from_props(label, props, line)?
            }
            _ => self.fresh_anon_id(label),
        };
        self.node_ids.insert(id_hash(&id));
        if let Some(var) = &node.var {
            vars.insert(var.name.clone(), id.clone());
        }

        if !node.labels.is_empty() || node.props.is_some() {
            let mut obj = Map::new();
            obj.insert("@id".to_string(), Value::String(id.clone()));
            match node.labels.len() {
                0 => {}
                1 => {
                    obj.insert(
                        "@type".to_string(),
                        Value::String(self.name(&node.labels[0].name)),
                    );
                }
                _ => {
                    obj.insert(
                        "@type".to_string(),
                        Value::Array(
                            node.labels
                                .iter()
                                .map(|l| Value::String(self.name(&l.name)))
                                .collect(),
                        ),
                    );
                }
            }
            if let Some(props) = &node.props {
                for (key, expr) in &props.entries {
                    if let Some(v) = literal_json(expr, line, key)? {
                        obj.insert(self.name(key), v);
                    }
                }
            }
            write_ndjson_line(&Value::Object(obj), out)?;
            *count += 1;
            self.stats.nodes += 1;
        }
        Ok(id)
    }

    fn emit_edge<W: Write>(
        &mut self,
        rel: &RelPattern,
        from: &str,
        to: &str,
        line: usize,
        out: &mut W,
    ) -> Result<()> {
        if rel.length.is_some() {
            return Err(unsupported(line, "variable-length relationship in CREATE"));
        }
        let rel_type = match rel.types.as_slice() {
            [t] => &t.name,
            _ => {
                return Err(unsupported(
                    line,
                    "CREATE relationship needs exactly one type",
                ))
            }
        };

        let mut props = Map::new();
        if let Some(map) = &rel.props {
            for (key, expr) in &map.entries {
                if let Some(v) = literal_json(expr, line, key)? {
                    props.insert(self.name(key), v);
                }
            }
        }
        if !props.is_empty() && self.opts.edge_policy == EdgePolicy::Nary {
            return Err(CypherImportError::NaryDeferred {
                rel_type: rel_type.clone(),
            });
        }

        let mut object = Map::new();
        object.insert("@id".to_string(), Value::String(to.to_string()));
        if !props.is_empty() && self.opts.edge_policy == EdgePolicy::Annotated {
            object.insert("@annotation".to_string(), Value::Object(props));
        }
        let mut subj = Map::new();
        subj.insert("@id".to_string(), Value::String(from.to_string()));
        subj.insert(self.name(rel_type), Value::Object(object));
        write_ndjson_line(&Value::Object(subj), out)?;
        self.stats.edges += 1;
        Ok(())
    }

    /// Whether a node's literal property map covers the label's learned
    /// match-key set (making its identity derivable).
    fn covers_match_keys(&self, label: &str, props: &MapLit) -> bool {
        let Some(keys) = self.match_keys.get(label) else {
            return false;
        };
        keys.iter()
            .all(|k| props.entries.iter().any(|(name, _)| name == k))
    }

    /// Derive a node id from its label + match-key property values:
    /// `Label/v1[/v2…]`, values in sorted-key order, percent-encoded.
    fn node_id_from_props(&self, label: &str, props: &MapLit, line: usize) -> Result<String> {
        let keys = self
            .match_keys
            .get(label)
            .expect("caller checked covers_match_keys / learned from this MATCH");
        let mut id = String::from(iri_segment(label));
        for key in keys {
            let (_, expr) = props
                .entries
                .iter()
                .find(|(name, _)| name == key)
                .ok_or_else(|| {
                    unsupported(
                        line,
                        &format!("MATCH on `{label}` must include key property `{key}`"),
                    )
                })?;
            let seg = literal_key_segment(expr, line, key)?;
            id.push('/');
            id.push_str(&seg);
        }
        Ok(self.prefixed(id))
    }

    fn fresh_anon_id(&mut self, label: Option<&str>) -> String {
        self.anon_seq += 1;
        let id = format!(
            "{}/_anon-{}",
            iri_segment(label.unwrap_or("node")),
            self.anon_seq
        );
        self.prefixed(id)
    }

    /// A label / relationship type / property key as emitted: bare by
    /// default, `@vocab`-prefixed in RDF-compat mode.
    fn name(&self, name: &str) -> String {
        self.prefixed(name.to_string())
    }

    fn prefixed(&self, name: String) -> String {
        match &self.opts.vocab {
            Some(v) => format!("{v}{name}"),
            None => name,
        }
    }
}

/// Convert a `.cypher` script into a list of JSON-LD objects (all three
/// passes, collected in memory). For large datasets use [`CypherImporter`]
/// with the streaming NDJSON passes + the chunked bulk import.
pub fn cypher_to_jsonld(text: &str, opts: &CypherImportOptions) -> Result<Vec<Value>> {
    let mut importer = CypherImporter::new(opts.clone());
    importer.learn_keys(text.as_bytes())?;
    let mut buf = Vec::new();
    importer.write_nodes_ndjson(text.as_bytes(), &mut buf)?;
    importer.write_edges_ndjson(text.as_bytes(), &mut buf)?;
    buf.split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| {
            serde_json::from_slice(l).map_err(|e| CypherImportError::Io(std::io::Error::other(e)))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Statement splitting + parsing
// ---------------------------------------------------------------------------

/// Split an in-memory Cypher script into its statements (same rules as
/// [`for_each_statement`]). A single statement — with or without a trailing
/// `;` — yields a one-element vec. Used by `Fluree::transact_cypher_returning`
/// to accept `;`-separated scripts.
pub(crate) fn split_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    // Reading from an in-memory slice cannot fail, and the callback is
    // infallible.
    let _ = for_each_statement(script.as_bytes(), |_, stmt| {
        out.push(stmt.to_string());
        Ok(())
    });
    out
}

/// Split a `.cypher` script into `;`-terminated statements, respecting
/// string literals (`'…'`, `"…"` with backslash escapes), backticked
/// identifiers, and `//` / `/* */` comments. Invokes `f(start_line, stmt)`
/// per non-empty statement; a trailing unterminated statement is included.
fn for_each_statement<R: BufRead>(
    mut src: R,
    mut f: impl FnMut(usize, &str) -> Result<()>,
) -> Result<()> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Normal,
        Str(u8),
        Backtick,
        LineComment,
        BlockComment,
    }

    let mut stmt = String::new();
    let mut stmt_line = 1usize; // line the current statement starts on
    let mut line_no = 0usize;
    let mut state = State::Normal;
    let mut buf = String::new();

    loop {
        buf.clear();
        if src.read_line(&mut buf)? == 0 {
            break;
        }
        line_no += 1;
        // A line comment always ends with its line.
        if state == State::LineComment {
            state = State::Normal;
        }

        let bytes = buf.as_bytes();
        let mut seg_start = 0usize; // start of the pending copy segment
        let mut i = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            match state {
                State::Normal => match b {
                    b';' => {
                        stmt.push_str(&buf[seg_start..i]);
                        seg_start = i + 1;
                        let trimmed = stmt.trim();
                        if !trimmed.is_empty() {
                            f(stmt_line, trimmed)?;
                        }
                        stmt.clear();
                        stmt_line = line_no; // next statement starts here
                    }
                    b'\'' | b'"' => state = State::Str(b),
                    b'`' => state = State::Backtick,
                    b'/' if bytes.get(i + 1) == Some(&b'/') => {
                        stmt.push_str(&buf[seg_start..i]);
                        stmt.push('\n');
                        seg_start = bytes.len();
                        state = State::LineComment;
                        i = bytes.len();
                        continue;
                    }
                    b'/' if bytes.get(i + 1) == Some(&b'*') => {
                        stmt.push_str(&buf[seg_start..i]);
                        stmt.push(' ');
                        seg_start = bytes.len(); // re-anchored on comment close
                        state = State::BlockComment;
                        i += 1; // skip the '*' so "/*/" doesn't close
                    }
                    _ => {}
                },
                State::Str(q) => match b {
                    b'\\' => i += 1, // skip escaped byte
                    _ if b == q => state = State::Normal,
                    _ => {}
                },
                State::Backtick => {
                    if b == b'`' {
                        state = State::Normal;
                    }
                }
                State::LineComment => unreachable!("line comment consumes the rest of the line"),
                State::BlockComment => {
                    if b == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        state = State::Normal;
                        i += 1;
                        seg_start = i + 1;
                    }
                }
            }
            i += 1;
        }
        if state != State::BlockComment && seg_start < bytes.len() {
            stmt.push_str(&buf[seg_start..]);
        }
        // Track where a still-empty statement starts (skip leading blanks).
        if stmt.trim().is_empty() {
            stmt.clear();
            stmt_line = line_no + 1;
        }
    }
    let trimmed = stmt.trim();
    if !trimmed.is_empty() {
        f(stmt_line, trimmed)?;
    }
    Ok(())
}

/// Cheap pre-parse dispatch: does the statement start with `keyword`
/// (case-insensitive, word-delimited)?
fn starts_with_keyword(stmt: &str, keyword: &str) -> bool {
    let head = stmt.as_bytes();
    head.len() >= keyword.len()
        && head[..keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
        && head
            .get(keyword.len())
            .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_')
}

/// Parse one statement and require it to be a write (`Update`).
fn parse_update(stmt: &str, line: usize) -> Result<fluree_db_cypher::ast::Update> {
    let out = parse_cypher(stmt);
    let Some(ast) = out.ast else {
        let msg = out
            .diagnostics
            .iter()
            .map(|d| d.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(CypherImportError::Parse { line, msg });
    };
    match ast.statement {
        Statement::Update(u) => Ok(u),
        Statement::Query(_) => Err(unsupported(line, "read-only statement in an import script")),
        Statement::Schema(_) => Err(unsupported(
            line,
            "schema command in an import script (Fluree needs no index/constraint DDL)",
        )),
        Statement::CallProcedure(_) => Err(unsupported(
            line,
            "procedure call in an import script (only CREATE / MATCH…CREATE statements load data)",
        )),
    }
}

/// Extract the single-node MATCH patterns of an edge statement.
fn match_nodes(read_clauses: &[ReadClause], line: usize) -> Result<Vec<&NodePattern>> {
    let mut nodes = Vec::new();
    for rc in read_clauses {
        let m = match rc {
            ReadClause::Match(m) => m,
            other => {
                return Err(unsupported(
                    line,
                    &format!(
                        "{} is not supported in bulk import",
                        read_clause_name(other)
                    ),
                ))
            }
        };
        if m.where_clause.is_some() {
            return Err(unsupported(
                line,
                "MATCH … WHERE (bulk import matches nodes by literal property maps only)",
            ));
        }
        for part in &m.pattern.parts {
            if !part.tail.is_empty() || part.path_search.is_some() {
                return Err(unsupported(
                    line,
                    "MATCH with relationships (bulk import matches single nodes only)",
                ));
            }
            nodes.push(&part.head);
        }
    }
    Ok(nodes)
}

/// A MATCH node's `(label, literal property map)`; both are required.
fn match_node_parts(node: &NodePattern, line: usize) -> Result<(String, &MapLit)> {
    let label = match node.labels.as_slice() {
        [l] => l.name.clone(),
        [] => return Err(unsupported(line, "MATCH node needs a label")),
        _ => return Err(unsupported(line, "MATCH node with multiple labels")),
    };
    let props = node
        .props
        .as_ref()
        .filter(|p| !p.entries.is_empty())
        .ok_or_else(|| unsupported(line, "MATCH node needs a literal property map"))?;
    Ok((label, props))
}

fn unsupported(line: usize, msg: &str) -> CypherImportError {
    CypherImportError::Unsupported {
        line,
        msg: msg.to_string(),
    }
}

fn read_clause_name(rc: &ReadClause) -> &'static str {
    match rc {
        ReadClause::Match(_) => "MATCH",
        ReadClause::OptionalMatch(_) => "OPTIONAL MATCH",
        ReadClause::With(_) => "WITH",
        ReadClause::Unwind(_) => "UNWIND",
        _ => "this read clause",
    }
}

fn write_clause_name(wc: &WriteClause) -> &'static str {
    match wc {
        WriteClause::Create(_) => "CREATE",
        WriteClause::Merge(_) => "MERGE",
        WriteClause::Set(_) => "SET",
        WriteClause::Remove(_) => "REMOVE",
        WriteClause::Delete(_) => "DELETE",
        WriteClause::Foreach(_) => "FOREACH",
    }
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

/// A property literal as a JSON-LD value. `null` → `None` (absent property).
/// Handles unary minus and flat lists of scalars; anything else errors.
fn literal_json(expr: &Expr, line: usize, key: &str) -> Result<Option<Value>> {
    match expr {
        Expr::Lit(lit) => Ok(literal_scalar(lit, line, key)?),
        Expr::UnaryOp(UnaryOp::Neg, inner, _) => match inner.as_ref() {
            Expr::Lit(Literal::Integer(i, _)) => Ok(Some(Value::from(-i))),
            Expr::Lit(Literal::Float(f, _)) => float_json(-f, line, key).map(Some),
            _ => Err(non_literal(line, key)),
        },
        Expr::List(items, _) => {
            let vals = items
                .iter()
                .map(|item| match literal_json(item, line, key)? {
                    Some(v) => Ok(v),
                    None => Err(non_literal(line, key)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(Value::Array(vals)))
        }
        _ => Err(non_literal(line, key)),
    }
}

fn literal_scalar(lit: &Literal, line: usize, key: &str) -> Result<Option<Value>> {
    Ok(match lit {
        Literal::Integer(i, _) => Some(Value::from(*i)),
        Literal::Float(f, _) => Some(float_json(*f, line, key)?),
        Literal::String(s, _) => Some(Value::String(s.clone())),
        Literal::Bool(b, _) => Some(Value::Bool(*b)),
        Literal::Null(_) => None,
    })
}

fn float_json(f: f64, line: usize, key: &str) -> Result<Value> {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .ok_or_else(|| unsupported(line, &format!("non-finite number for property `{key}`")))
}

/// Canonical id segment for a match-key value (percent-encoded).
fn literal_key_segment(expr: &Expr, line: usize, key: &str) -> Result<String> {
    let raw = match expr {
        Expr::Lit(Literal::Integer(i, _)) => i.to_string(),
        Expr::Lit(Literal::Float(f, _)) => f.to_string(),
        Expr::Lit(Literal::String(s, _)) => s.clone(),
        Expr::Lit(Literal::Bool(b, _)) => b.to_string(),
        Expr::UnaryOp(UnaryOp::Neg, inner, _) => match inner.as_ref() {
            Expr::Lit(Literal::Integer(i, _)) => (-i).to_string(),
            Expr::Lit(Literal::Float(f, _)) => (-f).to_string(),
            _ => return Err(non_literal(line, key)),
        },
        _ => return Err(non_literal(line, key)),
    };
    Ok(iri_segment(&raw).into_owned())
}

fn non_literal(line: usize, key: &str) -> CypherImportError {
    unsupported(
        line,
        &format!("property `{key}` needs a literal value (no parameters or expressions)"),
    )
}

fn id_hash(id: &str) -> u64 {
    let mut h = rustc_hash::FxHasher::default();
    id.hash(&mut h);
    h.finish()
}

fn write_ndjson_line<W: Write>(obj: &Value, out: &mut W) -> Result<()> {
    serde_json::to_writer(&mut *out, obj)
        .map_err(|e| CypherImportError::Io(std::io::Error::other(e)))?;
    out.write_all(b"\n")?;
    Ok(())
}
