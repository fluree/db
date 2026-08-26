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

/// Prefix of the placeholder rendered when an encoded binding cannot be
/// resolved to a user-facing value. Internal `Debug` representations must never
/// reach user output.
const UNRESOLVED_CELL_PREFIX: &str = "(unresolved ";

/// Render — and log — the fallback cell for an encoded binding that did not
/// resolve, either because there is no graph view to resolve against or because
/// the dictionaries don't know the ID.
///
/// `detail` carries the ID the resolver was handed. Without it every
/// unresolvable binding collapses to the same text, so two distinct broken rows
/// look alike and the cell is undiagnosable — which matters more, not less, for
/// an arm this cold: the day it fires is the day the ID is wanted. This mirrors
/// what the rest of the tree does with the same failure: the API formatters
/// hard-error with the ID in the message (`fluree-db-api/src/format/materialize.rs:63`)
/// and the materializer fabricates `_:unknown_p_{p_id}` alongside a
/// `tracing::warn!` (`fluree-db-query/src/materializer.rs:669-673`).
///
/// The parenthesised shape is deliberate. A bare `<unresolved>` in RDF-facing
/// output reads like a relative IRI — a user can reasonably parse that cell as a
/// subject actually named `unresolved`.
fn unresolved_cell(reason: &str, detail: &str) -> String {
    tracing::warn!(
        reason,
        binding = detail,
        "table cell: encoded binding did not resolve; rendering placeholder"
    );
    format!("{UNRESOLVED_CELL_PREFIX}{detail})")
}

/// `gv` is `None`: the query result carried no binary graph, so there is
/// nothing to resolve encoded IDs against.
const NO_GRAPH_VIEW: &str = "no binary graph view on the query result";
/// The graph view was present and the resolver still came up empty.
const RESOLVER_MISS: &str = "graph view could not resolve the ID";

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
                return Ok(unresolved_cell(NO_GRAPH_VIEW, &format!("s_id={s_id}")));
            };
            // Novelty-aware resolve (parity with the API formatters'
            // materialization): subjects first seen after the index snapshot
            // live in DictNovelty, which the store-level resolver can't see.
            match gv.resolve_subject_iri(*s_id) {
                Ok(iri) => compact_bnode_strip(compactor.compact_iri_for_display(&iri).ok()),
                Err(_) => unresolved_cell(RESOLVER_MISS, &format!("s_id={s_id}")),
            }
        }
        // The store-level predicate resolver is correct here, and deliberate.
        //
        // There *is* an ephemeral predicate layer (`EphemeralPredicateMap`,
        // `fluree-db-query/src/binary_scan.rs:2577`, routed by
        // `DictOverlay::resolve_predicate_iri` at `dict_overlay.rs:291`), so
        // "predicates have no novelty layer" would be the wrong reason. The
        // right one is reachability: the engine has exactly one construction
        // site for `Binding::EncodedPid` (`binary_scan.rs:1486`), and it is
        // gated on `late_materialize`, which requires
        // `overlay.epoch() == 0` (`binary_scan.rs:1366`) — no novelty overlay
        // at all. Ephemeral p_ids are minted only during overlay translation,
        // i.e. only when an overlay exists; under an overlay that same scan
        // takes the eager branch (`binary_scan.rs:1488`) and emits a resolved
        // `Binding::Sid`. So an ephemeral p_id cannot coexist with the path
        // that puts an `EncodedPid` in front of this formatter, and the index
        // root's inline predicate dict is complete for every p_id that can.
        //
        // Every other formatter resolves predicates the same way:
        // `fluree-db-api/src/format/sparql_xml.rs:302`,
        // `fluree-db-api/src/lib.rs:4580`, `fluree-db-query/src/sort.rs:93`.
        Binding::EncodedPid { p_id } => {
            let Some(gv) = gv else {
                return Ok(unresolved_cell(NO_GRAPH_VIEW, &format!("p_id={p_id}")));
            };
            match gv.store().resolve_predicate_iri(*p_id) {
                Some(iri) => compact_bnode_strip(compactor.compact_iri_for_display(iri).ok()),
                None => unresolved_cell(RESOLVER_MISS, &format!("p_id={p_id}")),
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
                return Ok(unresolved_cell(
                    NO_GRAPH_VIEW,
                    &format!("o_kind={o_kind} o_key={o_key} p_id={p_id}"),
                ));
            };
            match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id) {
                Ok(v) => flake_value_to_table_cell(&v, compactor),
                Err(_) => unresolved_cell(
                    RESOLVER_MISS,
                    &format!("o_kind={o_kind} o_key={o_key} p_id={p_id}"),
                ),
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
    use fluree_db_api::{Fluree, LedgerState};
    use fluree_db_api::{FlureeBuilder, ParsedContext, ReindexOptions};
    use fluree_db_binary_index::BinaryGraphView;
    use serde_json::json;

    const LEDGER: &str = "cli/table-novelty:main";

    /// Base data (indexed) plus a subject that lands only in novelty.
    ///
    /// Mirrors the reported repro's shape: `ex:m9` is committed after the
    /// index snapshot, so its subject ID lives in `DictNovelty` and the
    /// persisted forward packs know nothing about it.
    ///
    /// `ex:knows` gives both `ex:s1` and `ex:m9` a reference to an indexed
    /// node. That edge is what lets a query keep the *subject* variable
    /// late-materialized (see
    /// `novelty_only_encoded_sid_from_a_real_query_renders_its_iri`); a
    /// literal-only fixture never produces an `EncodedSid` at all.
    async fn indexed_ledger_with_novelty_subject() -> (Fluree, LedgerState, fluree_db_api::GraphDb)
    {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger(LEDGER).await.expect("create ledger");

        fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@graph": [
                        {"@id": "ex:x1", "@type": "ex:Target", "ex:indexedProp": "val1"},
                        {"@id": "ex:s1", "@type": "ex:Probe", "ex:ref": "val1",
                         "ex:knows": {"@id": "ex:x1"}}
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
                        {"@id": "ex:m9", "@type": "ex:Probe", "ex:ref": "val2",
                         "ex:knows": {"@id": "ex:x1"}}
                    ]
                }),
            )
            .await
            .expect("novelty insert")
            .ledger;

        let db = fluree.db(LEDGER).await.expect("indexed view");
        (fluree, ledger, db)
    }

    /// The SPARQL shape that actually puts a novelty-only `EncodedSid` in front
    /// of the table formatter.
    ///
    /// Joining a reference edge against a typed object keeps `?s`
    /// late-materialized, so subject IDs arrive at the formatter encoded rather
    /// than already resolved to `Binding::Sid`. A literal-only join does not —
    /// its subjects come back materialized, which is why the plain repro shape
    /// never reaches the arm this PR fixes.
    const ENCODED_SID_QUERY: &str = "SELECT ?s ?o WHERE { \
         ?s <http://example.org/knows> ?o . \
         ?o a <http://example.org/Target> . }";

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
        let (_fluree, ledger, db) = indexed_ledger_with_novelty_subject().await;
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
        assert!(
            !cell.starts_with(UNRESOLVED_CELL_PREFIX),
            "resolvable subject fell back: {cell:?}"
        );
    }

    /// The same property, but with the binding produced by the real query
    /// engine rather than constructed by hand — and rendered through the whole
    /// table fast path, not just one cell.
    ///
    /// This is the test that proves the `EncodedSid` arm is on a live path.
    /// It asserts, before rendering anything, that the engine actually put an
    /// `EncodedSid` in the batch *and* that the base store cannot resolve its
    /// ID — the two conditions that together make the old store-only resolver
    /// wrong. A shape that stopped emitting encoded subjects, or a fixture that
    /// stopped being novelty-only, reddens here instead of passing quietly.
    #[tokio::test(flavor = "current_thread")]
    async fn novelty_only_encoded_sid_from_a_real_query_renders_its_iri() {
        let (fluree, _ledger, db) = indexed_ledger_with_novelty_subject().await;
        let result = fluree
            .query(&db, ENCODED_SID_QUERY)
            .await
            .expect("join query runs");
        let gv = result
            .binary_graph
            .as_ref()
            .expect("query result carries a binary graph");

        // Non-vacuity, in two parts.
        let mut encoded = 0usize;
        let mut novelty_only_encoded = 0usize;
        for batch in &result.batches {
            let schema: Vec<_> = batch.schema().to_vec();
            for row in 0..batch.len() {
                for var_id in &schema {
                    if let Some(Binding::EncodedSid { s_id, .. }) = batch.get(row, *var_id) {
                        encoded += 1;
                        if gv.store().resolve_subject_iri(*s_id).is_err() {
                            novelty_only_encoded += 1;
                        }
                    }
                }
            }
        }
        assert!(
            encoded > 0,
            "this query no longer emits EncodedSid to the formatter — the arm \
             under test is unreachable through it, so the assertions below \
             would pass vacuously"
        );
        assert!(
            novelty_only_encoded > 0,
            "EncodedSid reaches the formatter, but every ID is resolvable from \
             the persisted store — the novelty case (the one the old code got \
             wrong) is not being exercised"
        );

        let output = format_sparql_table_from_result(&result, &db.snapshot, None)
            .expect("table renders")
            .expect("no grouped bindings, so the fast path applies");

        assert!(
            output.text.contains("m9"),
            "novelty-only subject missing from the rendered table:\n{}",
            output.text
        );
        assert!(
            !output.text.contains("Encoded"),
            "internal Debug repr leaked into table output:\n{}",
            output.text
        );
        assert!(
            !output.text.contains(UNRESOLVED_CELL_PREFIX),
            "a resolvable binding fell back to the placeholder:\n{}",
            output.text
        );
    }

    /// A binding nothing can resolve renders the placeholder — never a
    /// `Debug` repr. Covers every encoded arm, with and without a graph view.
    ///
    /// The placeholder has to carry the ID: an arm this cold is only ever read
    /// when something has already gone wrong, and a bare marker would make two
    /// distinct broken rows indistinguishable.
    #[tokio::test(flavor = "current_thread")]
    async fn unresolvable_encoded_bindings_render_placeholder() {
        let (_fluree, _ledger, db) = indexed_ledger_with_novelty_subject().await;
        let gv = db.binary_graph().expect("indexed view has a binary graph");
        let compactor = compactor_for(&db);

        // IDs above every watermark that were never minted in novelty either.
        // Each is paired with the substring the placeholder must carry.
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
        let cases = [
            (&bogus_sid, format!("s_id={}", u64::MAX - 1)),
            (&bogus_pid, format!("p_id={}", u32::MAX)),
            (&bogus_lit, format!("o_key={}", u64::from(u32::MAX - 1))),
        ];

        for (b, id_text) in &cases {
            for gv in [Some(&gv), None] {
                let cell = sparql_table_cell(b, &compactor, gv).expect("cell renders");

                assert!(
                    cell.starts_with(UNRESOLVED_CELL_PREFIX) && cell.ends_with(')'),
                    "gv={}, binding {b:?}: {cell:?}",
                    gv.is_some()
                );
                // The datum that makes the cell diagnosable.
                assert!(
                    cell.contains(id_text.as_str()),
                    "placeholder dropped the ID (gv={}): {cell:?}",
                    gv.is_some()
                );
                // Never the internal struct.
                assert!(
                    !cell.contains("Encoded"),
                    "Debug repr leaked (gv={}): {cell:?}",
                    gv.is_some()
                );
                // Never something a reader could take for a relative IRI.
                assert!(
                    !cell.starts_with('<'),
                    "placeholder reads as an IRI (gv={}): {cell:?}",
                    gv.is_some()
                );
            }
        }
    }

    /// Nested containers (Cypher list cells) recurse through the same arm, so
    /// the placeholder has to hold there too.
    #[tokio::test(flavor = "current_thread")]
    async fn unresolvable_binding_inside_list_renders_placeholder() {
        let (_fluree, _ledger, db) = indexed_ledger_with_novelty_subject().await;
        let gv = db.binary_graph().expect("indexed view has a binary graph");
        let compactor = compactor_for(&db);

        let cell = sparql_table_cell(
            &Binding::List(vec![Binding::encoded_sid(u64::MAX - 1)]),
            &compactor,
            Some(&gv),
        )
        .expect("cell renders");

        assert!(cell.starts_with(UNRESOLVED_CELL_PREFIX), "{cell:?}");
        assert!(
            cell.contains(&format!("s_id={}", u64::MAX - 1)),
            "placeholder dropped the ID inside a list cell: {cell:?}"
        );
        assert!(!cell.contains("EncodedSid"), "Debug repr leaked: {cell:?}");
    }
}
