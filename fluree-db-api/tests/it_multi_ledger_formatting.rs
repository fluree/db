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

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, MemoryFluree};

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
