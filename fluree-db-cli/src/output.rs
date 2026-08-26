use crate::detect::QueryFormat;
use crate::error::CliResult;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::format::IriCompactor;
use fluree_db_api::QueryResult;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::{FlakeValue, LedgerSnapshot};
use fluree_db_query::binding::Binding;

/// Output format for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormatKind {
    Json,
    TypedJson,
    /// Neo4j-compatible Cypher JSON (native scalars). The default for Cypher
    /// queries; available for any query via `--format cypher-json`.
    CypherJson,
    Table,
    Csv,
    Tsv,
    /// Newline-delimited JSON, produced incrementally by the streaming query
    /// path. Emits one bare binding object per line (or the full record
    /// protocol when `--envelope` is set). See `commands::query_stream`.
    Ndjson,
}

impl std::fmt::Display for OutputFormatKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => f.write_str("json"),
            Self::TypedJson => f.write_str("typed-json"),
            Self::CypherJson => f.write_str("cypher-json"),
            Self::Table => f.write_str("table"),
            Self::Csv => f.write_str("csv"),
            Self::Tsv => f.write_str("tsv"),
            Self::Ndjson => f.write_str("ndjson"),
        }
    }
}

/// Result of formatting: the rendered string plus the total row count.
pub struct FormatOutput {
    pub text: String,
    pub total_rows: usize,
}

/// Fast-path SPARQL table formatting directly from `QueryResult` (no intermediate JSON).
///
/// Returns:
/// - `Ok(Some(output))` when formatting succeeded
/// - `Ok(None)` when the result contains grouped bindings that require SPARQL disaggregation;
///   callers should fall back to the JSON-based formatter for correctness.
pub fn format_sparql_table_from_result(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: Option<usize>,
) -> CliResult<Option<FormatOutput>> {
    // ASK queries: display boolean result directly instead of an empty table.
    if result.output.is_ask() {
        let has_solution = result.batches.iter().any(|b| !b.is_empty());
        return Ok(Some(FormatOutput {
            text: has_solution.to_string(),
            total_rows: 1,
        }));
    }

    // Grouped bindings require cartesian disaggregation (SPARQL formatter logic).
    // Rather than re-implement that here, fall back to the existing SPARQL JSON formatter.
    let compactor = IriCompactor::new(snapshot.shared_namespaces(), &result.context);
    let gv = result.binary_graph.as_ref();

    let head_var_ids: Vec<fluree_db_query::VarId> = if result.output.is_wildcard() {
        result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
                    // Skip internal variables (?__pp0, ?__s0, etc.) from wildcard output.
                    .filter(|&vid| !result.vars.name(vid).starts_with("?__"))
                    .collect()
            })
            .unwrap_or_else(|| {
                // Empty result set: derive vars from the registry (all user-visible variables).
                result
                    .vars
                    .iter()
                    .filter(|(name, _)| !name.starts_with("?__"))
                    .map(|(_, id)| id)
                    .collect()
            })
    } else {
        result.output.projected_vars_or_empty()
    };

    // Match SPARQL JSON head var behavior: strip '?' and sort lexicographically.
    let mut head_pairs: Vec<(String, fluree_db_query::VarId)> = head_var_ids
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let headers: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    if headers.is_empty() {
        return Ok(Some(FormatOutput {
            text: "(empty result set)".to_string(),
            total_rows: 0,
        }));
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(&headers);

    let mut printed = 0usize;
    let max_rows = limit.unwrap_or(usize::MAX);

    // SelectOne should render only a single row (parity with SPARQL formatter).
    let select_one = result.output.is_select_one();

    for batch in &result.batches {
        for row in 0..batch.len() {
            let mut cells: Vec<String> = Vec::with_capacity(head_pairs.len());
            for (_, var_id) in &head_pairs {
                let b = batch.get(row, *var_id).unwrap_or(&Binding::Unbound);
                match sparql_table_cell(b, &compactor, gv) {
                    Ok(cell) => cells.push(cell),
                    Err(SparqlTableFastPath::NeedsDisaggregation) => return Ok(None),
                }
            }
            table.add_row(cells);
            printed += 1;

            if select_one || printed >= max_rows {
                break;
            }
        }
        if select_one || printed >= max_rows {
            break;
        }
    }

    let total_rows = if select_one {
        usize::from(printed > 0)
    } else {
        result.row_count()
    };

    Ok(Some(FormatOutput {
        text: table.to_string(),
        total_rows,
    }))
}

#[derive(Debug)]
enum SparqlTableFastPath {
    NeedsDisaggregation,
}

/// Placeholder rendered when an encoded binding cannot be resolved to a
/// user-facing value (missing graph view, or an ID the dictionaries don't
/// know). Internal `Debug` representations must never reach user output.
const UNRESOLVED_CELL: &str = "<unresolved>";

fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}

fn sparql_table_cell(
    b: &Binding,
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
) -> Result<String, SparqlTableFastPath> {
    let s = match b {
        Binding::Unbound | Binding::Poisoned => String::new(),

        // Use display compaction (includes auto-derived fallback prefixes)
        Binding::Sid { sid, .. } => {
            compact_bnode_strip(compactor.compact_sid_for_display(sid).ok())
        }
        Binding::IriMatch { iri, .. } => {
            compact_bnode_strip(compactor.compact_iri_for_display(iri).ok())
        }
        Binding::Iri(iri) => compact_bnode_strip(compactor.compact_iri_for_display(iri).ok()),

        Binding::Lit { val, .. } => flake_value_to_table_cell(val, compactor),

        Binding::EncodedSid { s_id, .. } => {
            let Some(gv) = gv else {
                return Ok(UNRESOLVED_CELL.to_string());
            };
            // Novelty-aware resolve (parity with the API formatters'
            // materialization): subjects first seen after the index snapshot
            // live in DictNovelty, which the store-level resolver can't see.
            match gv.resolve_subject_iri(*s_id) {
                Ok(iri) => compact_bnode_strip(compactor.compact_iri_for_display(&iri).ok()),
                Err(_) => UNRESOLVED_CELL.to_string(),
            }
        }
        Binding::EncodedPid { p_id } => {
            let Some(gv) = gv else {
                return Ok(UNRESOLVED_CELL.to_string());
            };
            match gv.store().resolve_predicate_iri(*p_id) {
                Some(iri) => compact_bnode_strip(compactor.compact_iri_for_display(iri).ok()),
                None => UNRESOLVED_CELL.to_string(),
            }
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            let Some(gv) = gv else {
                return Ok(UNRESOLVED_CELL.to_string());
            };
            match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id) {
                Ok(v) => flake_value_to_table_cell(&v, compactor),
                Err(_) => UNRESOLVED_CELL.to_string(),
            }
        }

        // Grouped values must be disaggregated into multiple rows for SPARQL semantics.
        Binding::Grouped(_) => return Err(SparqlTableFastPath::NeedsDisaggregation),

        // A path renders as arrow-joined node IRIs (Cypher-only; never reached
        // via the SPARQL surface).
        Binding::Path { nodes, .. } => nodes
            .iter()
            .map(|sid| compact_bnode_strip(compactor.compact_sid_for_display(sid).ok()))
            .collect::<Vec<_>>()
            .join("->"),

        // A list (Cypher collect/list value) — semicolon-joined cells; never
        // reached via the SPARQL surface.
        Binding::List(values) => {
            let mut parts = Vec::with_capacity(values.len());
            for v in values {
                parts.push(sparql_table_cell(v, compactor, gv)?);
            }
            parts.join(";")
        }

        // A map (Cypher map value) — `key=value` pairs; never reached via SPARQL.
        Binding::Map(entries) => {
            let mut parts = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                parts.push(format!("{k}={}", sparql_table_cell(v, compactor, gv)?));
            }
            parts.join(";")
        }

        // A relationship value — `start-[type]->end`; never reached via SPARQL.
        Binding::Rel(rel) => format!(
            "{}-[{}]->{}",
            compact_bnode_strip(compactor.compact_sid_for_display(&rel.start).ok()),
            compact_bnode_strip(compactor.compact_sid_for_display(&rel.predicate).ok()),
            compact_bnode_strip(compactor.compact_sid_for_display(&rel.end).ok()),
        ),
    };
    Ok(s)
}

fn compact_bnode_strip(compacted: Option<String>) -> String {
    let Some(s) = compacted else {
        return String::new();
    };
    s.strip_prefix("_:").unwrap_or(&s).to_string()
}

fn flake_value_to_table_cell(v: &FlakeValue, compactor: &IriCompactor) -> String {
    match v {
        FlakeValue::String(s) => s.clone(),
        FlakeValue::Long(n) => n.to_string(),
        FlakeValue::Double(d) => d.to_string(),
        FlakeValue::Boolean(b) => b.to_string(),
        FlakeValue::Vector(v) => serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()),
        FlakeValue::Json(s) => s.clone(),
        FlakeValue::Ref(sid) => compact_bnode_strip(compactor.compact_sid_for_display(sid).ok()),
        FlakeValue::Null => String::new(),
        other => other.to_string(),
    }
}

/// Format a query result JSON value for display.
///
/// When `limit` is `Some(n)`, only the first `n` rows are rendered.
/// `total_rows` always reflects the *untruncated* result set size.
pub fn format_result(
    json: &serde_json::Value,
    format: OutputFormatKind,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    match format {
        OutputFormatKind::Json | OutputFormatKind::TypedJson => {
            format_json(json, query_format, limit)
        }
        OutputFormatKind::CypherJson => {
            // Already the final Neo4j-compatible envelope; pretty-print and
            // count `results[0].data` rows.
            let total = json
                .pointer("/results/0/data")
                .and_then(|d| d.as_array())
                .map_or(0, Vec::len);
            let text = serde_json::to_string_pretty(json).unwrap_or_else(|_| json.to_string());
            Ok(FormatOutput {
                text,
                total_rows: total,
            })
        }
        OutputFormatKind::Table => format_as_table(json, query_format, limit),
        OutputFormatKind::Csv | OutputFormatKind::Tsv => {
            // TSV/CSV should be handled before reaching this function (via QueryResult methods).
            // If we get here, the caller didn't have access to the raw QueryResult.
            Err(crate::error::CliError::Usage(format!(
                "{format} format requires direct access to query results (not available for remote queries)",
            )))
        }
        OutputFormatKind::Ndjson => {
            // NDJSON is streamed incrementally via the streaming query path
            // (commands::query_stream) and never goes through this buffered
            // formatter.
            Err(crate::error::CliError::Usage(
                "ndjson format is produced by the streaming query path, not the buffered formatter"
                    .to_string(),
            ))
        }
    }
}

fn format_json(
    json: &serde_json::Value,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    let (total, output_json) = match query_format {
        QueryFormat::Sparql => {
            let total = sparql_row_count(json);
            match limit {
                Some(n) if n < total => {
                    let mut truncated = json.clone();
                    if let Some(bindings) = truncated
                        .pointer_mut("/results/bindings")
                        .and_then(|v| v.as_array_mut())
                    {
                        bindings.truncate(n);
                    }
                    (total, truncated)
                }
                _ => (total, json.clone()),
            }
        }
        QueryFormat::JsonLd => {
            let total = fql_row_count(json);
            match limit {
                Some(n) if n < total => {
                    let mut truncated = json.clone();
                    if let Some(arr) = truncated.as_array_mut() {
                        arr.truncate(n);
                    }
                    (total, truncated)
                }
                _ => (total, json.clone()),
            }
        }
    };
    let text =
        serde_json::to_string_pretty(&output_json).unwrap_or_else(|_| output_json.to_string());
    Ok(FormatOutput {
        text,
        total_rows: total,
    })
}

fn format_as_table(
    json: &serde_json::Value,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    match query_format {
        QueryFormat::Sparql => format_sparql_table(json, limit),
        QueryFormat::JsonLd => format_jsonld_table(json, limit),
    }
}

fn sparql_row_count(json: &serde_json::Value) -> usize {
    json.pointer("/results/bindings")
        .and_then(|v| v.as_array())
        .map(std::vec::Vec::len)
        .unwrap_or(0)
}

fn fql_row_count(json: &serde_json::Value) -> usize {
    json.as_array().map(std::vec::Vec::len).unwrap_or(0)
}

fn format_sparql_table(json: &serde_json::Value, limit: Option<usize>) -> CliResult<FormatOutput> {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let vars = json
        .pointer("/head/vars")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if vars.is_empty() {
        return Ok(FormatOutput {
            text: serde_json::to_string_pretty(json).unwrap_or_default(),
            total_rows: 0,
        });
    }

    table.set_header(&vars);

    let bindings = json.pointer("/results/bindings").and_then(|v| v.as_array());
    let total_rows = bindings.map(std::vec::Vec::len).unwrap_or(0);

    if let Some(rows) = bindings {
        let display_rows: &[serde_json::Value] = match limit {
            Some(n) if n < rows.len() => &rows[..n],
            _ => rows,
        };
        for row in display_rows {
            let cells: Vec<String> = vars
                .iter()
                .map(|var| {
                    row.get(var)
                        .and_then(|b| b.get("value"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            table.add_row(cells);
        }
    }

    Ok(FormatOutput {
        text: table.to_string(),
        total_rows,
    })
}

fn format_jsonld_table(json: &serde_json::Value, limit: Option<usize>) -> CliResult<FormatOutput> {
    let arr = match json.as_array() {
        Some(a) => a,
        None => {
            return Ok(FormatOutput {
                text: serde_json::to_string_pretty(json).unwrap_or_default(),
                total_rows: 0,
            })
        }
    };

    let total_rows = arr.len();
    if arr.is_empty() {
        return Ok(FormatOutput {
            text: "(empty result set)".to_string(),
            total_rows: 0,
        });
    }

    // Collect all keys from all objects for column headers
    let mut columns: Vec<String> = Vec::new();
    for obj in arr {
        if let Some(map) = obj.as_object() {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(&columns);

    let display_rows: &[serde_json::Value] = match limit {
        Some(n) if n < arr.len() => &arr[..n],
        _ => arr,
    };
    for obj in display_rows {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                obj.get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        table.add_row(cells);
    }

    Ok(FormatOutput {
        text: table.to_string(),
        total_rows,
    })
}

#[cfg(test)]
mod tests {
    //! Guards for the encoded-binding arms of the table fast path (#1466).
    //!
    //! Two properties are pinned here: encoded subject IDs minted *after* the
    //! index snapshot (novelty-only subjects) must render as their IRI, and an
    //! encoded binding that cannot be resolved at all must render an explicit
    //! placeholder — never a Rust `Debug` representation of an internal struct.

    use super::*;
    use fluree_db_api::LedgerState;
    use fluree_db_api::{FlureeBuilder, ParsedContext, ReindexOptions};
    use fluree_db_binary_index::BinaryGraphView;
    use serde_json::json;

    const LEDGER: &str = "cli/table-novelty:main";

    /// Base data (indexed) plus a subject that lands only in novelty.
    ///
    /// Mirrors the reported repro's shape: `ex:m9` is committed after the
    /// index snapshot, so its subject ID lives in `DictNovelty` and the
    /// persisted forward packs know nothing about it.
    async fn indexed_ledger_with_novelty_subject() -> (LedgerState, fluree_db_api::GraphDb) {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger(LEDGER).await.expect("create ledger");

        fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@graph": [
                        {"@id": "ex:x1", "ex:indexedProp": "val1"},
                        {"@id": "ex:s1", "@type": "ex:Probe", "ex:ref": "val1"}
                    ]
                }),
            )
            .await
            .expect("base insert");

        // Persist an index: everything above moves into the forward packs.
        fluree
            .reindex(LEDGER, ReindexOptions::default())
            .await
            .expect("reindex");

        // Commit again *without* reindexing — ex:m9 is novelty-only.
        let ledger = fluree.ledger(LEDGER).await.expect("post-index ledger");
        let ledger = fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@graph": [
                        {"@id": "ex:x2", "ex:indexedProp": "val2"},
                        {"@id": "ex:m9", "@type": "ex:Probe", "ex:ref": "val2"}
                    ]
                }),
            )
            .await
            .expect("novelty insert")
            .ledger;

        let db = fluree.db(LEDGER).await.expect("indexed view");
        (ledger, db)
    }

    /// The novelty-minted subject ID for `iri`, as the scan layer would emit it.
    fn novelty_s_id(ledger: &LedgerState, gv: &BinaryGraphView, iri: &str) -> u64 {
        let sid = gv.store().encode_iri(iri);
        ledger
            .dict_novelty
            .subjects
            .find_subject(sid.namespace_code, &sid.name)
            .unwrap_or_else(|| panic!("{iri} was expected in DictNovelty but is not there"))
    }

    fn compactor_for(db: &fluree_db_api::GraphDb) -> IriCompactor {
        IriCompactor::new(db.snapshot.shared_namespaces(), &ParsedContext::default())
    }

    /// #1466: a subject first seen after the index snapshot must resolve
    /// through the novelty-aware `BinaryGraphView` resolver.
    ///
    /// The store-only assertion in the middle keeps this test honest: it fails
    /// loudly if the fixture stops producing a genuinely novelty-only ID, which
    /// is the only condition under which the old code path was wrong.
    #[tokio::test(flavor = "current_thread")]
    async fn encoded_sid_from_novelty_renders_iri_not_debug_repr() {
        let (ledger, db) = indexed_ledger_with_novelty_subject().await;
        let gv = db.binary_graph().expect("indexed view has a binary graph");
        let compactor = compactor_for(&db);

        let s_id = novelty_s_id(&ledger, &gv, "http://example.org/m9");

        // Non-vacuity: the base-store resolver (what this code used to call)
        // genuinely cannot see this ID. Without this, a fixture that quietly
        // stopped being novelty-only would let the test pass against the bug.
        assert!(
            gv.store().resolve_subject_iri(s_id).is_err(),
            "fixture is not exercising the novelty path: the persisted store \
             already resolves s_id={s_id}"
        );

        let cell = sparql_table_cell(&Binding::encoded_sid(s_id), &compactor, Some(&gv))
            .expect("cell renders");

        assert!(
            cell.ends_with("m9"),
            "novelty-only subject should render as its IRI, got {cell:?}"
        );
        assert!(
            !cell.contains("EncodedSid"),
            "internal Debug repr leaked into table output: {cell:?}"
        );
        assert_ne!(cell, UNRESOLVED_CELL, "resolvable subject fell back");
    }

    /// A binding nothing can resolve renders the placeholder — never a
    /// `Debug` repr. Covers every encoded arm, with and without a graph view.
    #[tokio::test(flavor = "current_thread")]
    async fn unresolvable_encoded_bindings_render_placeholder() {
        let (_ledger, db) = indexed_ledger_with_novelty_subject().await;
        let gv = db.binary_graph().expect("indexed view has a binary graph");
        let compactor = compactor_for(&db);

        // IDs above every watermark that were never minted in novelty either.
        let bogus_sid = Binding::encoded_sid(u64::MAX - 1);
        let bogus_pid = Binding::EncodedPid { p_id: u32::MAX };
        let bogus_lit = Binding::EncodedLit {
            o_kind: fluree_db_core::ObjKind::LEX_ID.as_u8(),
            o_key: u64::from(u32::MAX - 1),
            p_id: u32::MAX,
            dt_id: 0,
            lang_id: 0,
            i_val: 0,
            t: 1,
        };

        for b in [&bogus_sid, &bogus_pid, &bogus_lit] {
            // With a graph view: resolution fails.
            let cell = sparql_table_cell(b, &compactor, Some(&gv)).expect("cell renders");
            assert_eq!(cell, UNRESOLVED_CELL, "with gv, binding {b:?}");

            // Without a graph view: nothing to resolve against.
            let cell = sparql_table_cell(b, &compactor, None).expect("cell renders");
            assert_eq!(cell, UNRESOLVED_CELL, "without gv, binding {b:?}");
        }
    }

    /// Nested containers (Cypher list cells) recurse through the same arm, so
    /// the placeholder has to hold there too.
    #[tokio::test(flavor = "current_thread")]
    async fn unresolvable_binding_inside_list_renders_placeholder() {
        let (_ledger, db) = indexed_ledger_with_novelty_subject().await;
        let gv = db.binary_graph().expect("indexed view has a binary graph");
        let compactor = compactor_for(&db);

        let cell = sparql_table_cell(
            &Binding::List(vec![Binding::encoded_sid(u64::MAX - 1)]),
            &compactor,
            Some(&gv),
        )
        .expect("cell renders");

        assert_eq!(cell, UNRESOLVED_CELL);
        assert!(!cell.contains("EncodedSid"), "Debug repr leaked: {cell:?}");
    }
}
