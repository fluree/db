//! Multi-ledger JSON-LD query formatting (API/engine level).
//!
//! These tests exercise the connection-scoped builder path
//! (`Fluree::query_from().jsonld(..).execute_formatted()`) — the same path the
//! HTTP `/v1/fluree/query` route reaches via `run_jsonld_subquery`.
//!
//! Unlike the `it_query_connection.rs` tests, these do NOT hand-pick a
//! formatting view (`query_connection(..)` + `to_jsonld_async(picked_ledger)`).
//! They go through `execute_formatted`, which auto-selects the formatting view
//! from `spec.default_graphs.first()`. That auto-selection is where the
//! multi-ledger formatter bugs live:
//!   - cross-graph IRIs decoded against the wrong ledger's namespace dict
//!   - `fromNamed`-only queries erroring with "No default graph for formatting"
//!
//! See GitHub issue #1259.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Seed three federated ledgers with *divergent* entity vocabularies.
///
/// Mirrors the structure of the passing
/// `query_connection_from_combined_datasets_selecting_subgraphs_depth_3` test
/// (movie → isBasedOn → book → author → person), but each ledger uses its own
/// entity prefix instead of a shared `wikidata.org`:
///   - movies under `http://movie.example/`
///   - books  under `http://book.example/`
///   - authors under `http://author.example/`
///
/// User namespaces are allocated per-ledger from code 13 in first-appearance
/// order, so the same numeric code maps to different prefixes across ledgers.
/// The movies ledger never references an `author.example` IRI directly, so its
/// dict either lacks that namespace or maps the colliding code to a different
/// prefix — which is exactly what trips the single-snapshot formatter.
async fn seed_divergent_ledgers(fluree: &MemoryFluree) {
    // Authors — entity prefix `author`.
    fluree
        .insert(
            genesis_ledger(fluree, "test/authors:main"),
            &json!({
                "@context": {
                    "author": "http://author.example/",
                    "schema": "http://schema.org/",
                    "id": "@id",
                    "type": "@type",
                },
                "@graph": [
                    {"@id": "author:a1", "@type": "schema:Person", "schema:name": "Margaret Mitchell"}
                ]
            }),
        )
        .await
        .expect("insert authors");

    // Books — entity prefix `book`; author ref points into `author` namespace.
    fluree
        .insert(
            genesis_ledger(fluree, "test/books:main"),
            &json!({
                "@context": {
                    "book": "http://book.example/",
                    "author": "http://author.example/",
                    "schema": "http://schema.org/",
                    "id": "@id",
                    "type": "@type",
                },
                "@graph": [
                    {
                        "@id": "book:b1",
                        "@type": "schema:Book",
                        "schema:name": "Gone with the Wind",
                        "schema:isbn": "0-582-41805-4",
                        "schema:author": {"@id": "author:a1"}
                    }
                ]
            }),
        )
        .await
        .expect("insert books");

    // Movies — entity prefix `movie`; isBasedOn ref points into `book`
    // namespace. NOTE: movies never references an `author` IRI, so the movies
    // dict has no `http://author.example/` entry (or a colliding code).
    fluree
        .insert(
            genesis_ledger(fluree, "test/movies:main"),
            &json!({
                "@context": {
                    "movie": "http://movie.example/",
                    "book": "http://book.example/",
                    "schema": "http://schema.org/",
                    "id": "@id",
                    "type": "@type",
                },
                "@graph": [
                    {
                        "@id": "movie:m1",
                        "@type": "schema:Movie",
                        "schema:name": "Gone with the Wind",
                        "schema:isBasedOn": {"@id": "book:b1"}
                    }
                ]
            }),
        )
        .await
        .expect("insert movies");
}

/// Issue 2 — a result subject sourced from one ledger is decoded against a
/// different ledger's namespace dict, silently returning the wrong `@id`.
#[tokio::test]
async fn cross_graph_projection_iri_decode() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    // movies is FIRST in `from`, so `execute_formatted` auto-picks the movies
    // dict as the single formatting snapshot. But the only matching subject —
    // a schema:Person — lives in the authors ledger and is SID-encoded against
    // the authors dict. Decoding that SID against the movies dict is the bug:
    // the movies dict has no `http://author.example/` (or a colliding code).
    let q = json!({
        "@context": {
            "movie": "http://movie.example/",
            "author": "http://author.example/",
            "schema": "http://schema.org/",
            "id": "@id",
            "type": "@type",
        },
        "from": ["test/movies:main", "test/authors:main"],
        "select": { "?person": ["*"] },
        "where": { "@id": "?person", "type": "schema:Person" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    // The person's @id MUST decode to `http://author.example/a1` (compacted via
    // the query @context to `author:a1`).
    //
    // BUG (issue #1259, Issue 2): the formatter decodes every result SID
    // against `spec.default_graphs.first()` = the movies dict. The authors-dict
    // code for `http://author.example/` collides with the movies-dict code for
    // `http://movie.example/`, so the @id silently comes back as `movie:a1`.
    let person = value
        .as_array()
        .and_then(|a| a.first())
        .expect("one person");
    let person_id = person.get("@id").and_then(|v| v.as_str());
    assert_eq!(
        person_id,
        Some("author:a1"),
        "cross-graph @id decoded against the wrong ledger's namespace dict: {value:#}"
    );
}

/// Issue 3 layer 2 — a `fromNamed`-only query (no default graph) must still
/// format. The sibling `DatasetQueryBuilder::execute_formatted` handles this
/// via `dataset.primary()` (which falls back to the first named graph);
/// `FromQueryBuilder::execute_formatted` lacks that fallback and instead errors
/// "No default graph for formatting".
#[tokio::test]
async fn from_named_only_formats() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "author": "http://author.example/",
            "schema": "http://schema.org/",
            "id": "@id",
            "type": "@type",
        },
        "fromNamed": ["test/authors:main"],
        "select": { "?author": ["*"] },
        "where": [
            ["graph", "test/authors:main",
                { "@id": "?author", "type": "schema:Person" }
            ]
        ]
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("fromNamed-only query should format, not error");

    let person = value
        .as_array()
        .and_then(|a| a.first())
        .expect("one person");
    assert_eq!(
        person.get("@id").and_then(|v| v.as_str()),
        Some("author:a1"),
    );
    assert_eq!(
        person.get("schema:name").and_then(|v| v.as_str()),
        Some("Margaret Mitchell"),
    );
}

/// Flat select (no hydration) goes through the `Binding::IriMatch` path
/// (jsonld.rs:82), which already decodes the cross-ledger @id correctly via the
/// canonical IRI. This guards that path against regressions and documents the
/// contrast with the hydration path fixed for issue #1259.
#[tokio::test]
async fn flat_select_cross_graph_iri_decode() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "movie": "http://movie.example/",
            "author": "http://author.example/",
            "schema": "http://schema.org/",
            "id": "@id",
            "type": "@type",
        },
        "from": ["test/movies:main", "test/authors:main"],
        "select": ["?person"],
        "where": { "@id": "?person", "type": "schema:Person" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("flat select should format");

    // Flat projection rows are arrays of bound values; the single column is the
    // person IRI, which must compact to `author:a1` (decoded against the authors
    // ledger via the IriMatch canonical IRI, not the movies primary dict).
    let row = value.as_array().and_then(|a| a.first()).expect("one row");
    let person_id = row
        .as_array()
        .and_then(|cols| cols.first())
        .and_then(|v| v.as_str());
    assert_eq!(
        person_id,
        Some("author:a1"),
        "flat select result: {value:#}"
    );
}

/// Seed a primary "catalog" ledger and a "people" ledger whose person uses a
/// divergent *predicate* vocabulary (`p:` = http://people.example/). The
/// shared `schema:` type lets the BGP match across both; the divergent
/// `p:fullName` predicate is what exercises home-ledger decoding.
async fn seed_divergent_predicate_ledgers(fluree: &MemoryFluree) {
    fluree
        .insert(
            genesis_ledger(fluree, "test/catalog:main"),
            &json!({
                "@context": {
                    "schema": "http://schema.org/",
                    "cat": "http://catalog.example/",
                    "id": "@id",
                    "type": "@type",
                },
                "@graph": [
                    {"@id": "cat:item1", "@type": "schema:Product", "cat:sku": "X-1"}
                ]
            }),
        )
        .await
        .expect("insert catalog");

    fluree
        .insert(
            genesis_ledger(fluree, "test/people:main"),
            &json!({
                "@context": {
                    "schema": "http://schema.org/",
                    "p": "http://people.example/",
                    "id": "@id",
                    "type": "@type",
                },
                "@graph": [
                    {"@id": "p:ada", "@type": "schema:Person", "p:fullName": "Ada"}
                ]
            }),
        )
        .await
        .expect("insert people");
}

/// Commit 1 — a hydration root that lives in a NON-primary ledger must have its
/// own properties decoded against its HOME ledger's namespace dict.
///
/// `catalog` is the primary (first default graph). The matching `schema:Person`
/// lives in `people` and carries `p:fullName` (the `people` ledger's own
/// vocabulary). Before the dataset-aware fix the property predicate was decoded
/// against the catalog dict — where the colliding code maps to `cat:`, silently
/// renaming `p:fullName` to `cat:fullName`. Routing the root to its home ledger
/// fixes it.
#[tokio::test]
async fn cross_graph_root_properties_decode_in_home_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_predicate_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "schema": "http://schema.org/",
            "cat": "http://catalog.example/",
            "p": "http://people.example/",
            "id": "@id",
            "type": "@type",
        },
        "from": ["test/catalog:main", "test/people:main"],
        "select": { "?person": ["*"] },
        "where": { "@id": "?person", "type": "schema:Person" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let person = value
        .as_array()
        .and_then(|a| a.first())
        .expect("one person");
    assert_eq!(person.get("@id").and_then(|v| v.as_str()), Some("p:ada"));
    // The property MUST be decoded against the people ledger's dict.
    assert_eq!(
        person.get("p:fullName").and_then(|v| v.as_str()),
        Some("Ada"),
        "root property decoded against the wrong ledger's dict: {value:#}"
    );
    // And must NOT have been mis-decoded under the catalog vocabulary.
    assert!(
        person.get("cat:fullName").is_none(),
        "property mis-decoded against the primary (catalog) dict: {value:#}"
    );
}

/// Cross-ledger hydration *expansion* with divergent vocabularies.
///
/// `movie → isBasedOn → book → author` spans three ledgers, each using its own
/// entity prefix (so the shared-vocab coincidence the older
/// `query_connection_from_combined_datasets_selecting_subgraphs_depth_3` test
/// relies on does NOT apply). Each ref is decoded to its canonical IRI in the
/// source ledger, then re-encoded and expanded in the ledger that actually
/// stores the subject — so the nested objects hydrate fully instead of
/// collapsing to bare `{"@id": ...}` (issue #1259, dataset-aware hydration).
#[tokio::test]
async fn cross_graph_hydration_expansion_divergent_vocab() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "movie": "http://movie.example/",
            "book": "http://book.example/",
            "author": "http://author.example/",
            "schema": "http://schema.org/",
            "id": "@id",
            "type": "@type",
        },
        "from": ["test/movies:main", "test/books:main", "test/authors:main"],
        "select": { "?movie": ["*"] },
        "depth": 3,
        "where": { "@id": "?movie", "type": "schema:Movie" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let movie = value.as_array().and_then(|a| a.first()).expect("one movie");
    assert_eq!(movie.get("@id").and_then(|v| v.as_str()), Some("movie:m1"));

    // Hop 1: movie (movies ledger) → isBasedOn → book (books ledger).
    let book = movie.get("schema:isBasedOn").expect("isBasedOn present");
    assert_eq!(
        book.get("@id").and_then(|v| v.as_str()),
        Some("book:b1"),
        "isBasedOn should hydrate, not collapse to a bare @id: {value:#}"
    );
    assert_eq!(
        book.get("schema:name").and_then(|v| v.as_str()),
        Some("Gone with the Wind"),
        "book (foreign ledger) properties must hydrate: {value:#}"
    );
    assert_eq!(
        book.get("schema:isbn").and_then(|v| v.as_str()),
        Some("0-582-41805-4")
    );

    // Hop 2: book (books ledger) → author → person (authors ledger), at depth 3.
    let author = book.get("schema:author").expect("author present");
    assert_eq!(
        author.get("@id").and_then(|v| v.as_str()),
        Some("author:a1")
    );
    assert_eq!(
        author.get("schema:name").and_then(|v| v.as_str()),
        Some("Margaret Mitchell"),
        "second cross-ledger hop (author) must hydrate too: {value:#}"
    );
}

/// Repro for the open issue on PR #1267 (Jack's comment): the canonical
/// documented multi-ledger shape `fromNamed: {alias: {"@id": ...}}` +
/// `["graph", "<alias>", ...]`. The inner GRAPH scope runs against a single
/// active graph, so no provenance is stamped, bindings are plain `Binding::Sid`,
/// and the formatter falls back to the primary (catalog) view — decoding the
/// `lists` SID against catalog's namespace dict.
#[tokio::test]
async fn dataset_nested_projection_cross_graph_iri_resolution_via_connection() {
    let fluree = FlureeBuilder::memory().build_memory();

    fluree
        .insert(
            genesis_ledger(&fluree, "catalog:main"),
            &json!({
                "@context": {"@vocab": "http://example.org/catalog/"},
                "@graph": [
                    {"@id": "http://example.org/items/q1", "@type": "Item",
                     "name": "Item One", "isbn": "0001"}
                ]
            }),
        )
        .await
        .expect("insert catalog");

    fluree
        .insert(
            genesis_ledger(&fluree, "lists:main"),
            &json!({
                "@context": {"@vocab": "http://example.org/lists/"},
                "@graph": [
                    {"@id": "http://example.org/lists/summer", "@type": "List",
                     "name": "Summer",
                     "contains": [{"@id": "http://example.org/items/q1"}]}
                ]
            }),
        )
        .await
        .expect("insert lists");

    // Explicit prefixes (no `@vocab`) so the cross-graph @id compacts to
    // `lists:summer` — unambiguously proving it decoded against the lists
    // namespace, not catalog's (whose colliding code maps to
    // `http://example.org/items/`, the pre-fix symptom).
    let query = json!({
        "from": "catalog:main",
        "fromNamed": { "lists_g": { "@id": "lists:main" } },
        "@context": {
            "lists": "http://example.org/lists/",
            "catalog": "http://example.org/catalog/"
        },
        "select": {"?list": ["@id", "lists:name",
            {"lists:contains": ["@id", "catalog:name", "catalog:isbn"]}
        ]},
        "where": [["graph", "lists_g", {"@id": "?list", "@type": "lists:List"}]]
    });

    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("nested cross-graph projection should succeed");

    let list = result.as_array().and_then(|a| a.first()).expect("one list");

    // The list lives in the named (lists) ledger; its @id must decode against
    // that ledger's namespace dict, not the primary (catalog) view's.
    assert_eq!(
        list.get("@id").and_then(|v| v.as_str()),
        Some("lists:summer"),
        "cross-graph @id decoded against the wrong ledger's namespace dict: {result:#}"
    );

    let s = result.to_string();
    assert!(
        !s.contains("http://example.org/items/summer") && !s.contains("catalog:summer"),
        "the mis-decoded (catalog-namespace) @id must not appear: {s}"
    );
}

/// Seed a primary `catalog` ledger whose product REFERENCES a person in a
/// separate `people` ledger, where the person carries a predicate (`p:fullName`)
/// in a namespace that is allocated a DIFFERENT code in each ledger:
///   - catalog: people.example registered last (via the `schema:author` ref) → high code
///   - people:  people.example registered first (the subject `p:ada`)        → low code
///
/// This is the cross-ledger explicit-projection drop condition.
async fn seed_divergent_predicate_ref_ledgers(fluree: &MemoryFluree) {
    fluree
        .insert(
            genesis_ledger(fluree, "test/catalog:main"),
            &json!({
                "@context": {
                    "schema": "http://schema.org/",
                    "cat": "http://catalog.example/",
                    "p": "http://people.example/",
                    "id": "@id", "type": "@type",
                },
                "@graph": [
                    {"@id": "cat:item1", "@type": "schema:Product",
                     "schema:author": {"@id": "p:ada"}}
                ]
            }),
        )
        .await
        .expect("insert catalog");

    fluree
        .insert(
            genesis_ledger(fluree, "test/people:main"),
            &json!({
                "@context": {
                    "schema": "http://schema.org/",
                    "p": "http://people.example/",
                    "id": "@id", "type": "@type",
                },
                "@graph": [
                    {"@id": "p:ada", "@type": "schema:Person",
                     "schema:name": "Ada Lovelace", "p:fullName": "Augusta Ada King"}
                ]
            }),
        )
        .await
        .expect("insert people");
}

/// Cross-ledger NESTED EXPLICIT projection must return ALL projected predicates
/// of the foreign subject — not just `@id` and reserved-namespace ones.
///
/// Pre-fix: `p:fullName` is lowered against the primary (catalog) dict and its
/// Sid is filtered against the people view, where the same IRI has a different
/// namespace code → silently dropped. `@id` and `schema:name` (schema.org shares
/// a code here) survive, masking the loss. With the cross-ledger predicate-Sid
/// rebind the predicate is re-encoded into the people dict and resolves to
/// "Augusta Ada King".
#[tokio::test]
async fn cross_graph_nested_explicit_projection_divergent_predicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_predicate_ref_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "schema": "http://schema.org/",
            "cat": "http://catalog.example/",
            "p": "http://people.example/",
            "id": "@id", "type": "@type",
        },
        "from": ["test/catalog:main", "test/people:main"],
        // EXPLICIT projection (not ["*"]) crossing catalog -> people:
        "select": { "?item": ["@id",
            { "schema:author": ["@id", "schema:name", "p:fullName"] }
        ]},
        "where": { "@id": "?item", "type": "schema:Product" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let item = value.as_array().and_then(|a| a.first()).expect("one item");
    let author = item.get("schema:author").expect("author present");

    assert_eq!(author.get("@id").and_then(|v| v.as_str()), Some("p:ada"));
    // Reserved/shared-code predicate survives even pre-fix:
    assert_eq!(
        author.get("schema:name").and_then(|v| v.as_str()),
        Some("Ada Lovelace"),
    );
    // THE REGRESSION: divergent-namespace predicate must be present, not dropped.
    assert_eq!(
        author.get("p:fullName").and_then(|v| v.as_str()),
        Some("Augusta Ada King"),
        "cross-ledger explicit projection dropped a non-primary-namespace predicate: {value:#}"
    );
}

// =============================================================================
// Issue #1295 — "non-primary hydration root" extension (predicted by the
// issue's Direction section). A *second, distinct mechanism* from the
// per-predicate-Sid drop: a hydration root whose subject lives in a non-primary
// ledger is hydrated against the PRIMARY view, so the whole subject comes back
// `@id`-only — even reserved-namespace predicates are dropped.
//
// These two tests share the `schema.org` namespace, which is allocated the SAME
// code across all three `seed_divergent_ledgers` ledgers. So a per-predicate Sid
// mismatch (the original #1295 mechanism) CANNOT explain `schema:name` dropping
// — that isolates the root-view-routing mechanism cleanly.
// =============================================================================

/// CONTROL — when the root variable is bound by a pattern in the subject's HOME
/// graph, the binding carries home-ledger provenance, so it hydrates correctly.
#[tokio::test]
async fn cross_graph_root_bound_in_home_graph_hydrates() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    // ?b is matched by its own type/predicate in the books ledger.
    let q = json!({
        "@context": {
            "book": "http://book.example/",
            "schema": "http://schema.org/",
            "id": "@id", "type": "@type",
        },
        "from": ["test/movies:main", "test/books:main"],
        "select": { "?b": ["@id", "schema:name", "schema:isbn"] },
        "where": { "@id": "?b", "type": "schema:Book" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let b = value.as_array().and_then(|a| a.first()).expect("one book");
    assert_eq!(b.get("@id").and_then(|v| v.as_str()), Some("book:b1"));
    assert_eq!(
        b.get("schema:name").and_then(|v| v.as_str()),
        Some("Gone with the Wind"),
        "home-graph-bound root should hydrate its properties: {value:#}"
    );
}

/// BUG (#1295 root extension) — when the SAME root subject is bound as the
/// OBJECT of a primary-ledger triple (`movie:m1 schema:isBasedOn ?b`, matched in
/// the movies ledger) but the subject (`book:b1`) lives in the books ledger, the
/// bare object SID carries no home-ledger provenance, so the formatter routes
/// hydration to the primary (movies) view — where `book:b1` has no triples — and
/// returns it `@id`-only. `schema:name` (aligned namespace) is dropped, proving
/// this is the root-view-routing mechanism, not the per-predicate-Sid mismatch.
///
/// Out of scope for the #1295 core fix (the rebind lives in `expand_ref`; this
/// root path goes through `format_hydration_column`/`FormatterSet::pick`).
/// Kept as a documented, ignored repro pending the root-routing follow-up.
#[ignore = "pending #1295 non-primary-root routing follow-up"]
#[tokio::test]
async fn cross_graph_root_bound_as_object_hydrates_in_home_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "movie": "http://movie.example/",
            "book": "http://book.example/",
            "schema": "http://schema.org/",
            "id": "@id", "type": "@type",
        },
        "from": ["test/movies:main", "test/books:main"],
        "select": { "?b": ["@id", "schema:name", "schema:isbn"] },
        "where": { "@id": "?m", "schema:isBasedOn": "?b" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let b = value.as_array().and_then(|a| a.first()).expect("one book");
    assert_eq!(b.get("@id").and_then(|v| v.as_str()), Some("book:b1"));
    assert_eq!(
        b.get("schema:name").and_then(|v| v.as_str()),
        Some("Gone with the Wind"),
        "root bound as object of a primary-ledger triple came back @id-only \
         (hydrated against the wrong view): {value:#}"
    );
}

/// Cross-ledger WILDCARD projection with a divergent-namespace **refinement**.
/// The refinement key (`s:parent`, in `http://sys.example/`) is lowered against
/// the primary (app) dict; hydration of `s:cat` happens in the sys ledger, where
/// `sys.example` has a different namespace code. Without rebinding the refinement
/// key, `select_predicate` misses it and the refined ref renders as a bare `@id`
/// instead of expanding. Locks in the `refinements`-key rebind (issue #1295),
/// which had no prior coverage.
#[tokio::test]
async fn cross_graph_wildcard_refinement_divergent_ns() {
    let fluree = FlureeBuilder::memory().build_memory();
    // Primary (app): item → category in the sys ledger. `app.example` registers
    // first; `sys.example` only via the cross-ledger ref → a higher code here.
    fluree
        .insert(
            genesis_ledger(&fluree, "test/wr-app:main"),
            &json!({
                "@context": {"app": "http://app.example/", "s": "http://sys.example/",
                             "id": "@id", "type": "@type"},
                "@graph": [
                    {"@id": "app:item", "@type": "app:Item", "app:cat": {"@id": "s:cat"}}
                ]
            }),
        )
        .await
        .expect("insert app");
    // sys: category with a divergent-namespace ref predicate `s:parent` → root.
    // `sys.example` registers first here → a lower code (divergent from app's).
    fluree
        .insert(
            genesis_ledger(&fluree, "test/wr-sys:main"),
            &json!({
                "@context": {"s": "http://sys.example/", "id": "@id"},
                "@graph": [
                    {"@id": "s:cat", "s:label": "Category", "s:parent": {"@id": "s:root"}},
                    {"@id": "s:root", "s:label": "Root"}
                ]
            }),
        )
        .await
        .expect("insert sys");

    let q = json!({
        "@context": {"app": "http://app.example/", "s": "http://sys.example/",
                     "id": "@id", "type": "@type"},
        "from": ["test/wr-app:main", "test/wr-sys:main"],
        // ?i's app:cat crosses into sys; s:cat is hydrated with a WILDCARD that
        // refines the divergent-namespace ref `s:parent` to expand it.
        "select": {"?i": ["@id", {"app:cat": ["*", {"s:parent": ["@id", "s:label"]}]}]},
        "where": {"@id": "?i", "type": "app:Item"}
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let item = value.as_array().and_then(|a| a.first()).expect("one item");
    let cat = item.get("app:cat").expect("app:cat present");
    // Wildcard includes the category's own s:label:
    assert_eq!(
        cat.get("s:label").and_then(|v| v.as_str()),
        Some("Category"),
        "wildcard cross-ledger hydration dropped s:label: {value:#}"
    );
    // The REFINEMENT on the divergent-namespace ref must apply cross-ledger →
    // s:parent expands to include s:label, not render as a bare @id.
    let parent = cat.get("s:parent").expect("s:parent present");
    assert_eq!(
        parent.get("s:label").and_then(|v| v.as_str()),
        Some("Root"),
        "wildcard refinement on a divergent-namespace ref did not apply cross-ledger: {value:#}"
    );
}

/// BUG (#1295 root path) — a hydration ROOT whose subject lives in a non-primary
/// ledger, bound by a home-graph WHERE pattern (so it routes to the home ledger
/// correctly — reserved/shared-code predicates hydrate), STILL drops a
/// divergent-namespace predicate from its EXPLICIT projection.
///
/// This is the same per-predicate-Sid drop as the nested #1295 mechanism, one
/// level up: the projection level is lowered once against the primary (catalog)
/// dict, so every predicate Sid carries catalog's namespace codes. When the root
/// (`p:ada`) is hydrated against the people view, `p:fullName`'s Sid carries the
/// wrong code and misses the people index — while `schema:name` (shared code)
/// survives, masking the loss. The nested fix lives in `expand_ref`; this root
/// path routes through `format_hydration_column`, which now rebinds the level
/// into the routed view's dict when the root is non-primary.
///
/// Distinct from `cross_graph_root_bound_as_object_hydrates_in_home_ledger`
/// (Finding B): there the whole subject comes back `@id`-only because routing
/// itself fails; here routing succeeds (`schema:name` is present) and only the
/// divergent-namespace predicate is dropped.
#[tokio::test]
async fn cross_graph_root_projection_divergent_predicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_divergent_predicate_ref_ledgers(&fluree).await;

    let q = json!({
        "@context": {
            "schema": "http://schema.org/",
            "cat": "http://catalog.example/",
            "p": "http://people.example/",
            "id": "@id", "type": "@type",
        },
        "from": ["test/catalog:main", "test/people:main"],
        // Root ?person is bound in its HOME (people) graph, but the projection is
        // lowered against the primary (catalog) dict.
        "select": { "?person": ["@id", "schema:name", "p:fullName"] },
        "where": { "@id": "?person", "type": "schema:Person" }
    });

    let value = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted should not error");

    let person = value
        .as_array()
        .and_then(|a| a.first())
        .expect("one person");
    assert_eq!(person.get("@id").and_then(|v| v.as_str()), Some("p:ada"));
    // Shared-code predicate survives even pre-fix (routing succeeds):
    assert_eq!(
        person.get("schema:name").and_then(|v| v.as_str()),
        Some("Ada Lovelace"),
    );
    // THE REGRESSION: divergent-namespace predicate on the ROOT must be present.
    assert_eq!(
        person.get("p:fullName").and_then(|v| v.as_str()),
        Some("Augusta Ada King"),
        "cross-ledger root projection dropped a non-primary-namespace predicate: {value:#}"
    );
}
