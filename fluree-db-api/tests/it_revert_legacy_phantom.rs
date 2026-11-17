//! Reverting a commit that holds a legacy upsert phantom must not assert an
//! invalid literal.
//!
//! Before the retraction resolver, an upsert over `"a"@en` retracted a
//! tagless `"a"^^rdf:langString`, which matched nothing and is inert for
//! reads and indexing. Commits written then still hold it. A revert
//! inverts every flake of the reverted commit, and inverting that one
//! asserted `"a"` as an `rdf:langString` with no language tag (LANG ""),
//! a literal that cannot exist. The revert now skips such retractions.

use crate::support;
use fluree_db_api::{CommitRef, ConflictStrategy, FlureeBuilder, IndexConfig};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_transact::{CommitOpts, NamespaceRegistry, StageOptions};
use serde_json::json;

const EX: &str = "http://example.org/";

#[tokio::test]
async fn revert_skips_a_tagless_lang_string_retraction() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": EX},
                "@id": "ex:s",
                "ex:label": {"@value": "a", "@language": "en"}
            }),
        )
        .await
        .unwrap()
        .ledger;

    // The commit an old upsert of `"b"@en` wrote: the phantom retraction of
    // the tagless `"a"` plus the assertion of `"b"@en`.
    let s = ledger.snapshot.encode_iri(&format!("{EX}s")).unwrap();
    let p = ledger.snapshot.encode_iri(&format!("{EX}label")).unwrap();
    let lang_string = Sid::new(fluree_vocab::namespaces::RDF, "langString");
    let t = ledger.t() + 1;
    let legacy = vec![
        Flake::new(
            s.clone(),
            p.clone(),
            FlakeValue::String("a".into()),
            lang_string.clone(),
            t,
            false,
            None,
        ),
        Flake::new(
            s,
            p,
            FlakeValue::String("b".into()),
            lang_string,
            t,
            true,
            Some(FlakeMeta::with_lang("en")),
        ),
    ];
    let ns = NamespaceRegistry::from_db(&ledger.snapshot);
    let view = fluree_db_transact::stage_flakes(ledger, legacy, StageOptions::new())
        .await
        .expect("stage the legacy commit");
    let (receipt, _ledger) = fluree
        .commit_staged(
            view,
            ns,
            &IndexConfig {
                reindex_min_bytes: 1 << 40,
                reindex_max_bytes: 1 << 41,
            },
            CommitOpts::default(),
        )
        .await
        .expect("commit the legacy commit");

    fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(receipt.commit_id)],
            ConflictStrategy::Abort,
        )
        .await
        .expect("revert");

    let ledger = fluree.ledger("mydb:main").await.unwrap();
    let r = support::query_sparql(
        &fluree,
        &ledger,
        &format!(
            "SELECT ?v ?l WHERE {{ <{EX}s> <{EX}label> ?o BIND(STR(?o) AS ?v) BIND(LANG(?o) AS ?l) }}"
        ),
    )
    .await
    .expect("query");
    let rows = r.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        rows,
        json!([["a", "en"]]),
        "the revert undoes `\"b\"@en` and asserts no tagless literal"
    );
}
