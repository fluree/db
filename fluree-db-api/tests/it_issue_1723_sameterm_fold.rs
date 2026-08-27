//! #1723: `FILTER(sameTerm(?x, ?y))` must not be folded into an equijoin on a
//! literal-valued predicate.
//!
//! `filter_fold` rewrites `?a :p ?x . ?b :p ?y . FILTER(?x = ?y)` into
//! `?a :p ?x . ?b :p ?x`, and used to do it unconditionally for `sameTerm` on
//! the premise that `sameTerm` is term equality and so needs no guard. The join
//! it rewrites into is not term equality for literals — it unifies on the
//! encoded term, which is deliberately lenient across numeric subtypes and,
//! today, flattens the string-dictionary datatypes onto `xsd:string`.
//!
//! The oracle here is the unfolded answer, obtained by projecting BOTH compared
//! variables: `find_foldable` skips a filter whose two variables are both
//! projected at the top level, because keeping one and renaming the other would
//! collapse two output columns into one. So `SELECT ?a ?b ?x ?y` is the same
//! query without the rewrite, and `SELECT ?a ?b` is the one that used to take
//! it. They must agree.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Thirteen objects under one predicate, spanning the cases where value
/// equality, term equality and the join's encoded-term unification pull apart:
/// numeric subtypes, a normalized `xsd:dateTime` pair, the string family
/// (plain / `xsd:string` / two language tags / a custom datatype), a boolean and
/// a ref.
fn seed() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:i_int",   "ex:p": 1},
            {"@id":"ex:i_dbl",   "ex:p": {"@value":"1.0","@type":"xsd:double"}},
            {"@id":"ex:i_long",  "ex:p": {"@value":"1","@type":"xsd:long"}},
            {"@id":"ex:i_dec",   "ex:p": {"@value":"1.0","@type":"xsd:decimal"}},
            {"@id":"ex:s_plain", "ex:p": "abc"},
            {"@id":"ex:s_str",   "ex:p": {"@value":"abc","@type":"xsd:string"}},
            {"@id":"ex:s_en",    "ex:p": {"@value":"abc","@language":"en"}},
            {"@id":"ex:s_EN",    "ex:p": {"@value":"abc","@language":"EN"}},
            {"@id":"ex:s_cust",  "ex:p": {"@value":"abc","@type":"ex:custom"}},
            {"@id":"ex:b_true",  "ex:p": true},
            {"@id":"ex:dt_z",    "ex:p": {"@value":"2020-01-01T00:00:00Z","@type":"xsd:dateTime"}},
            {"@id":"ex:dt_off",  "ex:p": {"@value":"2019-12-31T19:00:00-05:00","@type":"xsd:dateTime"}},
            {"@id":"ex:ref",     "ex:p": {"@id":"ex:n1"}}
        ]
    })
}

fn pairs(rows: &[serde_json::Value]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            format!(
                "{} ~ {}",
                r[0].as_str().unwrap_or("?"),
                r[1].as_str().unwrap_or("?")
            )
        })
        .collect();
    out.sort();
    out
}

async fn pairs_for(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &str,
) -> Vec<String> {
    let rows = normalize_rows(
        &support::query_sparql(fluree, ledger, query)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld"),
    );
    pairs(&rows)
}

fn non_identity(pairs: &[String]) -> Vec<String> {
    pairs
        .iter()
        .filter(|p| {
            let (a, b) = p.split_once(" ~ ").expect("pair");
            a != b
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn issue_1723_sameterm_answers_the_unfolded_result() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "issue1723-sameterm:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    const FOLDABLE: &str = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b WHERE { ?a ex:p ?x . ?b ex:p ?y . FILTER(sameTerm(?x, ?y)) }
    ";
    // Both compared variables projected => `find_foldable` bails => unfolded.
    const UNFOLDED: &str = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b ?x ?y WHERE { ?a ex:p ?x . ?b ex:p ?y . FILTER(sameTerm(?x, ?y)) }
    ";

    let folded = pairs_for(&fluree, &ledger, FOLDABLE).await;
    let unfolded = pairs_for(&fluree, &ledger, UNFOLDED).await;

    assert_eq!(
        folded, unfolded,
        "sameTerm answered differently depending on whether the fold was reachable"
    );

    // 13 identity pairs plus 8 more: the two `xsd:dateTime` spellings of one
    // instant, `xsd:integer`/`xsd:long` (one stored term), `"abc"`/
    // `"abc"^^xsd:string` (one RDF 1.1 term) and `@en`/`@EN` (BCP 47 tags are
    // case-insensitive). Before the fix this answered 35, equating `1` with
    // `1.0` and `"abc"^^xsd:string` with `"abc"^^ex:custom`.
    assert_eq!(folded.len(), 21, "sameTerm rows: {folded:?}");
    assert_eq!(
        non_identity(&folded),
        vec![
            "ex:dt_off ~ ex:dt_z",
            "ex:dt_z ~ ex:dt_off",
            "ex:i_int ~ ex:i_long",
            "ex:i_long ~ ex:i_int",
            "ex:s_EN ~ ex:s_en",
            "ex:s_en ~ ex:s_EN",
            "ex:s_plain ~ ex:s_str",
            "ex:s_str ~ ex:s_plain",
        ]
    );
}

/// The companion guard: `=` was already gated on the node-valued check, and its
/// answer must not move. Value equality is the looser of the two — it promotes
/// across numeric types — so it legitimately answers more rows than `sameTerm`.
#[tokio::test]
async fn issue_1723_value_equality_answer_is_unchanged() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "issue1723-eq:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    const FOLDABLE: &str = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b WHERE { ?a ex:p ?x . ?b ex:p ?y . FILTER(?x = ?y) }
    ";
    const UNFOLDED: &str = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b ?x ?y WHERE { ?a ex:p ?x . ?b ex:p ?y . FILTER(?x = ?y) }
    ";

    let folded = pairs_for(&fluree, &ledger, FOLDABLE).await;
    let unfolded = pairs_for(&fluree, &ledger, UNFOLDED).await;
    assert_eq!(folded, unfolded, "`=` diverged across the fold");

    // 13 identity pairs, the 4 numeric spellings of one value mutually equal
    // (12), the two dateTime spellings (2), and the two string pairs sameTerm
    // also accepts (4).
    assert_eq!(folded.len(), 31, "`=` rows: {folded:?}");
    // The join is looser than `=` here: it also equates `"abc"^^ex:custom` with
    // the rest of the string family. That is exactly what the node-valued guard
    // keeps out of the answer.
    const HAND_WRITTEN_JOIN: &str = r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b WHERE { ?a ex:p ?x . ?b ex:p ?x . }
    ";
    let join = pairs_for(&fluree, &ledger, HAND_WRITTEN_JOIN).await;
    assert!(
        join.len() > folded.len(),
        "the fold's target join is expected to be looser than `=` on this data; \
         join={} eq={}",
        join.len(),
        folded.len()
    );
}
