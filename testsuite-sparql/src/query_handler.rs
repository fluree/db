//! `QueryEvaluationTest` / `UpdateEvaluationTest` handlers: create an
//! in-memory Fluree ledger, load test data (default + named graphs), execute
//! a SPARQL query or update, and compare against expected results.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use fluree_db_api::{
    format, Fluree, FlureeBuilder, FormatterConfig, GraphDb, LedgerState, ParsedContext,
    QueryOutput,
};

use crate::files::read_file_to_string;
use crate::manifest::Test;
use crate::rdfxml;
use crate::result_comparison::{are_results_isomorphic, format_results_diff};
use crate::result_format::{
    fluree_construct_to_sparql_results, fluree_json_to_sparql_results, parse_expected_graph,
    parse_expected_results, project_to_csv_space, RdfTerm, SparqlResults, Triple,
};
use crate::subprocess::{run_in_subprocess, TestDescriptor};

/// Max time for a single query evaluation test (data load + query + compare).
const EVAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Ledger alias used for every W3C test (each test runs in its own
/// subprocess with a fresh in-memory Fluree, so the alias never collides).
const TEST_LEDGER: &str = "w3c:test";

/// Handler for `mf:QueryEvaluationTest`.
///
/// Runs the test in an isolated subprocess for reliable timeout enforcement.
/// If the test exceeds `EVAL_TIMEOUT`, the subprocess is killed — no zombie
/// threads, no CPU leak.
pub fn evaluate_query_evaluation_test(test: &Test) -> Result<()> {
    let test_id = test.id.clone();
    let query_url = test
        .query
        .clone()
        .context("QueryEvaluationTest missing qt:query (query file URL)")?;
    let data_url = test.data.clone();
    let result_url = test
        .result
        .clone()
        .context("QueryEvaluationTest missing mf:result (expected result file)")?;
    let graph_data = test.graph_data.clone();

    let descriptor = TestDescriptor::Eval {
        test_id,
        query_url,
        data_url,
        result_url,
        graph_data,
    };

    let result = run_in_subprocess(&descriptor, EVAL_TIMEOUT)?;

    if !result.passed {
        let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
        bail!("{error_msg}");
    }

    Ok(())
}

/// Handler for `mf:UpdateEvaluationTest`.
///
/// Same subprocess isolation as query evaluation. Loads the initial graph
/// store state (`ut:data` / `ut:graphData`), applies the SPARQL UPDATE
/// (`ut:request`), and compares the resulting graph store state against the
/// expected state (`ut:data` / `ut:graphData` on `mf:result`).
pub fn evaluate_update_evaluation_test(test: &Test) -> Result<()> {
    let test_id = test.id.clone();
    let request_url = test
        .update_request
        .clone()
        .context("UpdateEvaluationTest missing ut:request (update file URL)")?;

    // Guard against a mis-parsed mf:result degrading to a trivially
    // satisfiable "expected empty store". The distinguishing signal is whether
    // an `mf:result` node was PRESENT at all: a bare `mf:result []` (no
    // ut:data/ut:graphData/ut:result) is a deliberate empty-store expectation
    // used by the W3C graph-management tests (`DROP ALL`, `update-silent/*`),
    // whereas a completely ABSENT `mf:result` on an UpdateEvaluationTest means
    // the manifest parse dropped it. Fire only on the latter.
    if !test.result_present
        && test.result_data.is_none()
        && test.result_graph_data.is_empty()
        && !test.result_success
        && test.result.is_none()
    {
        bail!(
            "UpdateEvaluationTest has no mf:result node at all — unrecognized \
             manifest shape or dropped result?\nTest: {test_id}"
        );
    }

    let descriptor = TestDescriptor::UpdateEval {
        test_id,
        request_url,
        data_url: test.data.clone(),
        graph_data: test.graph_data.clone(),
        result_data_url: test.result_data.clone(),
        result_graph_data: test.result_graph_data.clone(),
    };

    let result = run_in_subprocess(&descriptor, EVAL_TIMEOUT)?;

    if !result.passed {
        let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
        bail!("{error_msg}");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Graph store setup (shared by query-eval and update-eval)
// ---------------------------------------------------------------------------

/// Create the test ledger and load initial state: default graph data plus
/// named graphs. Returns the resulting ledger state.
///
/// Named graphs are loaded as TriG `GRAPH <name> { ... }` blocks through the
/// alias-based transact builder (`upsert_turtle`), which routes through
/// `parse_trig_phase1`. Each file's `@prefix`/`@base` directives are hoisted
/// above the GRAPH block (TriG directives are document-scoped), and each
/// graph loads in its own transaction so prefix declarations can't collide
/// across files.
async fn setup_graph_store(
    fluree: &Fluree,
    data_url: Option<&str>,
    graph_data: &[(String, String)],
) -> Result<LedgerState> {
    let ledger = fluree
        .create_ledger(TEST_LEDGER)
        .await
        .context("Failed to create test ledger")?;

    // Default graph data (.ttl or .rdf).
    // For .ttl: prepend @base so relative IRIs resolve correctly.
    // For .rdf: convert RDF/XML to N-Triples (absolute IRIs) first.
    // Some W3C tests (e.g., empty.ttl) have valid syntax but no triples —
    // Fluree rejects these as empty transactions, so we skip gracefully.
    let mut current = ledger;
    if let Some(data_url) = data_url {
        let raw = read_file_to_string(data_url)
            .with_context(|| format!("Reading test data: {data_url}"))?;
        if !raw.trim().is_empty() {
            let turtle = prepare_for_insert(&raw, data_url)?;
            match fluree.insert_turtle(current.clone(), &turtle).await {
                Ok(result) => current = result.ledger,
                Err(e) if is_empty_transaction(&e) => { /* no triples — skip */ }
                Err(e) => return Err(e).with_context(|| format!("Loading test data: {data_url}")),
            }
        }
    }

    // Named graph data as TriG GRAPH blocks.
    for (graph_name, graph_url) in graph_data {
        let raw = read_file_to_string(graph_url)
            .with_context(|| format!("Reading named graph data: {graph_url}"))?;
        if raw.trim().is_empty() {
            continue;
        }
        let content = prepare_for_insert(&raw, graph_url)?;
        let (directives, body) = split_turtle_directives(&content);
        if body.trim().is_empty() {
            continue;
        }
        let trig = format!("{directives}GRAPH <{graph_name}> {{\n{body}}}\n");
        // upsert_turtle (not insert_turtle): the builder's insert_turtle
        // fast path bypasses TriG GRAPH-block extraction.
        match fluree
            .graph(TEST_LEDGER)
            .transact()
            .upsert_turtle(&trig)
            .commit()
            .await
        {
            Ok(_) => {
                current = fetch_state(fluree).await?;
            }
            Err(e) if is_empty_transaction(&e) => { /* no triples — skip */ }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Loading named graph data: {graph_url} as <{graph_name}>")
                })
            }
        }
    }

    Ok(current)
}

/// Fetch the latest committed state of the test ledger by alias.
async fn fetch_state(fluree: &Fluree) -> Result<LedgerState> {
    let handle = fluree
        .ledger_cached(TEST_LEDGER)
        .await
        .context("Fetching test ledger state")?;
    Ok(handle.snapshot().await.to_ledger_state())
}

/// Split a Turtle document into (directives, body).
///
/// TriG requires directives at document scope, so when wrapping a Turtle
/// file's content in a `GRAPH { }` block its `@prefix`/`@base` lines must be
/// hoisted out. W3C test data declares directives one per line.
///
/// ASSUMPTION: line-based detection. A multi-line string literal whose
/// continuation line happens to start with `prefix`/`base` would be
/// mis-hoisted; no W3C test data has that shape.
fn split_turtle_directives(content: &str) -> (String, String) {
    let mut directives = String::new();
    let mut body = String::new();
    for line in content.lines() {
        let t = line.trim_start();
        let lower = t.to_ascii_lowercase();
        if lower.starts_with("@prefix")
            || lower.starts_with("@base")
            || lower.starts_with("prefix ")
            || lower.starts_with("prefix\t")
            || lower.starts_with("base ")
            || lower.starts_with("base\t")
        {
            directives.push_str(line);
            directives.push('\n');
        } else {
            body.push_str(line);
            body.push('\n');
        }
    }
    (directives, body)
}

// ---------------------------------------------------------------------------
// Graph readback (used by update-eval comparison)
// ---------------------------------------------------------------------------

/// Read all triples of one graph (default graph when `graph` is `None`)
/// through the public SPARQL query surface.
async fn read_graph_triples(
    fluree: &Fluree,
    ledger: &LedgerState,
    graph: Option<&str>,
) -> Result<Vec<Triple>> {
    let db = GraphDb::from_ledger_state(ledger);
    let sparql = match graph {
        Some(g) => format!("SELECT ?s ?p ?o WHERE {{ GRAPH <{g}> {{ ?s ?p ?o }} }}"),
        None => "SELECT ?s ?p ?o WHERE { ?s ?p ?o }".to_string(),
    };
    let query_result = fluree
        .query(&db, &sparql)
        .await
        .with_context(|| format!("Reading back graph {graph:?}"))?;

    let empty_context = ParsedContext::new();
    let config = FormatterConfig::sparql_json();
    let json = format::format_results(&query_result, &empty_context, &ledger.snapshot, &config)
        .map_err(|e| anyhow::anyhow!("Formatting readback results: {e}"))?;
    let results = fluree_json_to_sparql_results(&json)
        .context("Converting readback results to SparqlResults")?;

    let SparqlResults::Solutions { solutions, .. } = results else {
        bail!("Graph readback returned non-SELECT results");
    };

    let mut triples = Vec::with_capacity(solutions.len());
    for sol in solutions {
        let (Some(s), Some(p), Some(o)) = (sol.get("s"), sol.get("p"), sol.get("o")) else {
            continue; // partial row — cannot happen for a bare SPO scan
        };
        triples.push(Triple {
            subject: s.clone(),
            predicate: p.clone(),
            object: o.clone(),
        });
    }
    // An RDF graph is a set; the SPO scan should already be duplicate-free,
    // but normalize anyway so comparison semantics don't depend on it.
    dedup_triples(&mut triples);
    Ok(triples)
}

fn dedup_triples(triples: &mut Vec<Triple>) {
    let mut seen = std::collections::HashSet::new();
    triples.retain(|t| seen.insert(format!("{t:?}")));
}

/// Set-equality of two triple lists. Both inputs come from `read_graph_triples`
/// (already deduplicated), so comparing them as sets is exact. Used to
/// recognize the engine's default-graph leak into `GRAPH ?g` by content rather
/// than by the alias string (see `run_update_eval_test` step 5).
fn triples_eq(a: &[Triple], b: &[Triple]) -> bool {
    let sa: std::collections::HashSet<String> = a.iter().map(|t| format!("{t:?}")).collect();
    let sb: std::collections::HashSet<String> = b.iter().map(|t| format!("{t:?}")).collect();
    sa == sb
}

/// Enumerate the names of all non-empty named graphs.
async fn list_named_graphs(fluree: &Fluree, ledger: &LedgerState) -> Result<Vec<String>> {
    let db = GraphDb::from_ledger_state(ledger);
    let sparql = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }";
    let query_result = fluree
        .query(&db, sparql)
        .await
        .context("Enumerating named graphs")?;

    let empty_context = ParsedContext::new();
    let config = FormatterConfig::sparql_json();
    let json = format::format_results(&query_result, &empty_context, &ledger.snapshot, &config)
        .map_err(|e| anyhow::anyhow!("Formatting graph enumeration: {e}"))?;
    let results = fluree_json_to_sparql_results(&json)?;

    let SparqlResults::Solutions { solutions, .. } = results else {
        bail!("Graph enumeration returned non-SELECT results");
    };
    // The engine currently binds `?g` as a plain string literal, not an IRI
    // term, and also leaks the default graph into `GRAPH ?g` under the ledger
    // alias (both registered engine gaps — audit burn-down/named-graph-dataset.md
    // BUG-1/BUG-2, issue #1279). Accept both term kinds so this enumeration
    // keeps working when the literal-vs-IRI gap is fixed.
    //
    // We deliberately do NOT filter the aliased default graph here by name:
    // the caller (run_update_eval_test step 5) recognizes it by CONTENT, which
    // is robust to the engine renaming / IRI-expanding the alias. String-
    // comparing the alias here would silently break every default-graph update
    // test the day that representation changes.
    Ok(solutions
        .into_iter()
        .filter_map(|sol| match sol.get("g") {
            Some(RdfTerm::Iri(iri)) => Some(iri.clone()),
            Some(RdfTerm::Literal { value, .. }) => Some(value.clone()),
            _ => None,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Query evaluation (inner, runs inside the subprocess)
// ---------------------------------------------------------------------------

/// Files referenced by a query's `FROM` / `FROM NAMED` dataset clause, each as
/// a `(graph-name, data-url)` pair keyed by the clause IRI.
///
/// The dataset tests define their graphs through the query's dataset clause
/// (not qt:data / qt:graphData), so the harness must pre-load those files.
/// Resolving the clause here (against the query's prologue BASE) yields the
/// exact absolute IRIs the engine's `resolve_dataset_clause` produces, so a
/// graph loaded under this name resolves against the ledger's graph registry
/// during within-ledger dataset construction. Both `graph-name` and `data-url`
/// are the clause IRI: the file is read from it and its relative IRIs resolve
/// against it as base (matching `setup_graph_store`'s named-graph loading).
///
/// Returns empty for queries with no dataset clause, or that fail to parse
/// (the real parse error surfaces later at query execution).
fn dataset_clause_graphs(sparql: &str) -> Vec<(String, String)> {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    if parsed.has_errors() {
        return Vec::new();
    }
    let Some(ast) = parsed.ast.as_ref() else {
        return Vec::new();
    };
    let Ok(Some(clause)) = fluree_db_sparql::resolve_dataset_clause(ast) else {
        return Vec::new();
    };

    let mut seen = std::collections::HashSet::new();
    let mut graphs = Vec::new();
    for iri in clause
        .default_graphs
        .iter()
        .chain(clause.named_graphs.iter())
    {
        let iri = iri.to_string();
        if seen.insert(iri.clone()) {
            graphs.push((iri.clone(), iri));
        }
    }
    graphs
}

/// Inner async function that does the actual test work.
///
/// Public for use by the `run-w3c-test` subprocess binary.
pub async fn run_eval_test(
    test_id: &str,
    query_url: &str,
    data_url: Option<&str>,
    result_url: &str,
    graph_data: &[(String, String)],
) -> Result<()> {
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Read the SPARQL query (before loading the store — a dataset clause
    //    below decides which files to pre-load).
    //
    // W3C engines resolve relative IRIs in a query against the query
    // document's URL (RFC 3986 §5.1.3 "URI used to retrieve the entity").
    // Supply that document base the same way the data loader does
    // (`prepare_for_insert` prepends `@base`): a leading BASE declaration.
    // An explicit BASE in the query text overrides it (last declaration
    // wins), exactly like a document-supplied base would be overridden.
    let sparql = read_file_to_string(query_url)
        .with_context(|| format!("Reading query file: {query_url}"))?;
    let sparql = format!("BASE <{query_url}>\n{sparql}");

    // The data-r2/dataset tests (and constructwhere04) define their dataset
    // entirely through the query's FROM / FROM NAMED clause — they carry no
    // qt:data / qt:graphData. Pre-load each clause-referenced file as a named
    // graph under the same BASE-resolved IRI the engine resolves the clause to,
    // so within-ledger dataset construction (`build_within_ledger_dataset`) can
    // resolve it against the ledger's graph registry. No-op for the vast
    // majority of tests, which carry no dataset clause.
    let mut graph_data = graph_data.to_vec();
    for (name, url) in dataset_clause_graphs(&sparql) {
        if !graph_data.iter().any(|(existing, _)| existing == &name) {
            graph_data.push((name, url));
        }
    }

    // 2. Create the ledger + load default and named graph data.
    let ledger = setup_graph_store(&fluree, data_url, &graph_data).await?;

    // 3. Execute the SPARQL query.
    let db = GraphDb::from_ledger_state(&ledger);
    let query_result = fluree
        .query(&db, &sparql)
        .await
        .with_context(|| format!("Executing SPARQL query for test {test_id}"))?;

    // 4. Parse expected results
    let expected = parse_expected_results(result_url)?;

    // 5. Detect CONSTRUCT vs SELECT/ASK from the parsed query's select mode.
    //    Previous heuristic checked file extension (.ttl/.rdf), but many SPARQL
    //    1.0 SELECT tests use .ttl result files encoded in the DAWG Result Set
    //    vocabulary — not CONSTRUCT graphs. See issue #44.
    let is_construct = matches!(query_result.output, QueryOutput::Construct(_));

    let actual = if is_construct {
        // CONSTRUCT path: format as JSON-LD graph
        let construct_json = query_result
            .to_construct(&ledger.snapshot)
            .map_err(|e| anyhow::anyhow!("Formatting CONSTRUCT result: {e}"))?;
        fluree_construct_to_sparql_results(&construct_json)
            .context("Converting CONSTRUCT output to graph")?
    } else {
        // SELECT/ASK path: format as SPARQL JSON
        let empty_context = ParsedContext::new();
        let config = FormatterConfig::sparql_json();
        let actual_json =
            format::format_results(&query_result, &empty_context, &ledger.snapshot, &config)
                .map_err(|e| anyhow::anyhow!("Formatting SPARQL JSON: {e}"))?;
        fluree_json_to_sparql_results(&actual_json)
            .context("Converting Fluree results to SparqlResults")?
    };

    // CSV expected results are lossy (no term kinds / datatypes); project the
    // actual results into the same value space before comparing.
    let actual = if result_url.ends_with(".csv") {
        project_to_csv_space(actual)
    } else {
        actual
    };

    // 6. Compare
    if !are_results_isomorphic(&expected, &actual) {
        let diff = format_results_diff(&expected, &actual);
        bail!(
            "Results not isomorphic.\n\
             Test: {test_id}\n\
             Query: {query_url}\n\
             Expected result: {result_url}\n\n\
             {diff}"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Update evaluation (inner, runs inside the subprocess)
// ---------------------------------------------------------------------------

/// Inner async function for `mf:UpdateEvaluationTest`.
///
/// Public for use by the `run-w3c-test` subprocess binary.
pub async fn run_update_eval_test(
    test_id: &str,
    request_url: &str,
    data_url: Option<&str>,
    graph_data: &[(String, String)],
    result_data_url: Option<&str>,
    result_graph_data: &[(String, String)],
) -> Result<()> {
    // 1. Initial graph store state
    let fluree = FlureeBuilder::memory().build_memory();
    setup_graph_store(&fluree, data_url, graph_data).await?;

    // 2. Apply the update request
    let sparql = read_file_to_string(request_url)
        .with_context(|| format!("Reading update request: {request_url}"))?;
    match fluree
        .graph(TEST_LEDGER)
        .transact()
        .sparql_update(&sparql)
        .commit()
        .await
    {
        Ok(_) => {}
        // An update whose WHERE clause matches nothing (or whose data is
        // already present/absent) is a valid no-op per SPARQL Update.
        Err(e) if is_empty_transaction(&e) => {}
        Err(e) => {
            return Err(e).with_context(|| format!("Applying update for test {test_id}"));
        }
    }

    let ledger = fetch_state(&fluree).await?;

    // 3. Compare default graph state
    let expected_default = match result_data_url {
        Some(url) => parse_expected_graph(url)?,
        None => Vec::new(),
    };
    let actual_default = read_graph_triples(&fluree, &ledger, None).await?;
    compare_graph(
        test_id,
        "default graph",
        expected_default,
        actual_default.clone(),
        result_data_url,
    )?;

    // 4. Compare each expected named graph
    for (graph_name, expected_url) in result_graph_data {
        let expected = parse_expected_graph(expected_url)?;
        let actual = read_graph_triples(&fluree, &ledger, Some(graph_name)).await?;
        compare_graph(
            test_id,
            &format!("named graph <{graph_name}>"),
            expected,
            actual,
            Some(expected_url),
        )?;
    }

    // 5. No unexpected non-empty named graphs.
    //
    // Two caveats bound what this step can assert:
    //
    // (a) The engine leaks the default graph into `GRAPH ?g` under the ledger
    //     alias (burn-down/named-graph-dataset.md BUG-2 / #1279). We recognize
    //     that leak by CONTENT — a graph whose triples equal the default-graph
    //     readback IS the aliased default graph — instead of string-comparing
    //     the alias, so this survives the engine renaming / IRI-expanding that
    //     alias (and, once BUG-2 is fixed and the leak disappears, the content
    //     branch simply never fires). The residual risk is a genuinely
    //     unexpected named graph whose content happens to equal the default
    //     graph; no update-eval test has that shape, and the previous
    //     name-filter had the analogous blind spot for a graph literally named
    //     `w3c:test`.
    //
    // (b) This compares *non-empty* graphs only. Fluree does not track empty
    //     named graphs, so CLEAR (graph remains, empty) vs DROP (graph removed)
    //     and CREATE (empty graph exists) are not observable here. Those tests
    //     are all currently registered; see tests/registers/mod.rs header for
    //     why they must not be silently un-registered when the grammar lands.
    let expected_names: std::collections::HashSet<&str> = result_graph_data
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    for name in &list_named_graphs(&fluree, &ledger).await? {
        if expected_names.contains(name.as_str()) {
            continue;
        }
        // Not an expected graph — but it may be the aliased default-graph leak.
        let triples = read_graph_triples(&fluree, &ledger, Some(name)).await?;
        if triples_eq(&triples, &actual_default) {
            if name != TEST_LEDGER {
                // The engine changed how it surfaces the default graph under
                // `GRAPH ?g`; recognized by content, but flag it so the
                // coupling stays visible rather than silently drifting.
                eprintln!(
                    "note: [{test_id}] default graph surfaced under GRAPH ?g as \
                     <{name}> (expected ledger alias <{TEST_LEDGER}>) — recognized \
                     by content; update the harness if the engine's default-graph \
                     representation changed."
                );
            }
            continue;
        }
        bail!(
            "Unexpected non-empty named graph after update.\n\
             Test: {test_id}\n\
             Graph: <{name}>\n\
             Expected named graphs: {expected_names:?}"
        );
    }

    Ok(())
}

/// Compare one graph's expected vs actual triples isomorphically.
fn compare_graph(
    test_id: &str,
    which: &str,
    expected: Vec<Triple>,
    actual: Vec<Triple>,
    expected_url: Option<&str>,
) -> Result<()> {
    let expected = SparqlResults::Graph(expected);
    let actual = SparqlResults::Graph(actual);
    if !are_results_isomorphic(&expected, &actual) {
        let diff = format_results_diff(&expected, &actual);
        bail!(
            "Graph state after update not isomorphic ({which}).\n\
             Test: {test_id}\n\
             Expected: {expected_url:?}\n\n\
             {diff}"
        );
    }
    Ok(())
}

/// Check if an error is a Fluree "empty transaction" rejection.
///
/// Turtle files with only `@prefix` declarations and no triples produce zero
/// flakes. Fluree rejects these as empty transactions, but for W3C tests we
/// should treat them as a no-op (the test is querying an empty graph).
///
/// FRAGILE: uses string matching because `ApiError` doesn't expose a typed
/// variant for this case. Update if `ApiError::Transact(TransactError::EmptyTransaction)`
/// becomes publicly matchable.
fn is_empty_transaction(e: &fluree_db_api::ApiError) -> bool {
    e.to_string().contains("Empty transaction")
}

/// Prepare file content for insertion into Fluree.
///
/// - `.rdf` files: convert RDF/XML to N-Triples (absolute IRIs, valid Turtle)
/// - All others: prepend `@base` so relative IRIs resolve correctly
fn prepare_for_insert(content: &str, url: &str) -> Result<String> {
    if url.ends_with(".rdf") {
        rdfxml::rdfxml_to_ntriples(content, url)
            .with_context(|| format!("Converting RDF/XML to N-Triples: {url}"))
    } else {
        Ok(format!("@base <{url}> .\n{content}"))
    }
}
