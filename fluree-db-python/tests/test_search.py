"""Inline search: full-text scoring and vector similarity, configured and
queried from Python."""

import asyncio
import math

import pytest

import fluree
import fluree.aio
from fluree import IRI, FullText, InvalidRequestError, LangString, PermissionDeniedError, Vector

EX = "http://example.org/"
F = "https://ns.flur.ee/db#"
CTX = {"ex": EX}
P = f"PREFIX ex: <{EX}> PREFIX f: <{F}> "
SCORE_TITLES = P + (
    "SELECT ?d ?score WHERE { ?d ex:title ?t BIND(fulltext(?t, $q) AS ?score) FILTER(?score > 0) } "
    "ORDER BY DESC(?score)"
)


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def docs(conn):
    ledger = conn.create("docs")
    ledger.set_context(CTX)
    ledger.insert({
        "@context": CTX,
        "@graph": [
            {"@id": "ex:d1", "ex:title": "Rust programming guide"},
            {"@id": "ex:d2", "ex:title": "Cooking pasta at home"},
            {"@id": "ex:d3", "ex:title": "Advanced Rust macros"},
        ],
    })
    return ledger


def hits(ledger, query="rust"):
    return ledger.query(SCORE_TITLES, q=query).value("d")


def test_set_full_text_makes_existing_and_new_values_searchable(docs):
    assert docs.full_text() is None
    assert hits(docs) == []
    docs.set_full_text(["ex:title"])
    assert docs.full_text() == FullText((IRI(EX + "title"),), "en")
    assert sorted(hits(docs)) == [IRI(EX + "d1"), IRI(EX + "d3")]
    docs.insert({"@context": CTX, "@id": "ex:d4", "ex:title": "Rust in production"})
    assert IRI(EX + "d4") in hits(docs)


def test_configuring_before_data_takes_an_index_build(conn):
    # A property is scored once an index build has seen values of it; the
    # reindex in set_full_text has none to see on an empty ledger.
    ledger = conn.create("empty-first")
    ledger.set_context(CTX)
    ledger.set_full_text(["ex:title"])
    ledger.insert({"@context": CTX, "@id": "ex:d1", "ex:title": "Rust programming guide"})
    assert hits(ledger) == []
    ledger.reindex()
    assert hits(ledger) == [IRI(EX + "d1")]
    ledger.insert({"@context": CTX, "@id": "ex:d2", "ex:title": "Rust macros"})
    assert sorted(hits(ledger)) == [IRI(EX + "d1"), IRI(EX + "d2")]


def test_scores_agree_across_languages_and_spellings(docs):
    docs.set_full_text([EX + "title"])
    sparql = docs.query(SCORE_TITLES, q="rust programming").values()
    namespaced = docs.query(SCORE_TITLES.replace("fulltext(", "f:fulltext("), q="rust programming").values()
    jsonld = docs.query({
        "@context": CTX,
        "select": ["?d", "?score"],
        "where": [
            {"@id": "?d", "ex:title": "?t"},
            ["bind", "?score", '(fulltext ?t "rust programming")'],
            ["filter", "(> ?score 0)"],
        ],
        "orderBy": [["desc", "?score"]],
    })
    assert sparql == namespaced
    assert [[str(d).replace(EX, "ex:"), score] for d, score in sparql] == jsonld
    assert sparql[0][0] == IRI(EX + "d1")


def test_replace_and_clear(docs):
    docs.set_full_text(["ex:title", IRI(EX + "body")], language="fr", reindex=False)
    assert docs.full_text() == FullText((IRI(EX + "body"), IRI(EX + "title")), "fr")
    docs.set_full_text(["ex:title"])
    assert docs.full_text().properties == (IRI(EX + "title"),)
    docs.set_full_text([])
    assert docs.full_text() is None
    assert hits(docs) == []


def test_language_tagged_values_use_their_own_language(docs):
    docs.insert({
        "@context": CTX,
        "@id": "ex:fr",
        "ex:title": {"@value": "Les maladies cardiaques chroniques", "@language": "fr"},
    })
    docs.set_full_text(["ex:title"])
    # The French stemmer reduces "maladie" and "maladies" to one stem.
    assert hits(docs, "maladie") == [IRI(EX + "fr")]
    assert docs.query(P + "SELECT ?t WHERE { ex:fr ex:title ?t }").single().t == LangString(
        "Les maladies cardiaques chroniques", "fr"
    )


def test_existing_config_is_kept(conn):
    ledger = conn.create("configured")
    ledger.upsert(
        f"""@prefix f: <{F}> .
        GRAPH <urn:fluree:configured:main#config> {{
          <urn:my:config> a f:LedgerConfig ; <{EX}note> "kept" .
        }}""",
        format="trig",
    )
    ledger.set_full_text([EX + "title"], reindex=False)
    rows = conn.query(
        P + "SELECT ?c ?note FROM <urn:fluree:configured:main#config> "
        "WHERE { ?c a f:LedgerConfig OPTIONAL { ?c ex:note ?note } }"
    )
    assert rows.values() == [[IRI("urn:my:config"), "kept"]]
    assert ledger.full_text().properties == (IRI(EX + "title"),)


def test_policy_governed_ledgers_cannot_configure(docs):
    governed = docs.with_policy(identity="did:example:someone")
    with pytest.raises(PermissionDeniedError, match="set_full_text"):
        governed.set_full_text(["ex:title"])
    with pytest.raises(PermissionDeniedError, match="full_text"):
        governed.full_text()


def test_search_sees_staged_writes(docs):
    docs.set_full_text(["ex:title"])
    with docs.transaction() as txn:
        txn.insert({"@context": CTX, "@id": "ex:d9", "ex:title": "Rust for data science"})
        staged = txn.query(SCORE_TITLES, q="rust").value("d")
        assert IRI(EX + "d9") in staged
    assert IRI(EX + "d9") in hits(docs)


# -- vectors -----------------------------------------------------------------


@pytest.fixture
def vectors(conn):
    ledger = conn.create("vectors")
    ledger.set_context(CTX)
    ledger.insert({
        "@context": CTX,
        "@graph": [
            {"@id": "ex:a", "ex:embedding": Vector([0.9, 0.1, 0.0])},
            {"@id": "ex:b", "ex:embedding": Vector((0.0, 0.2, 0.9))},
        ],
    })
    return ledger


def test_vectors_round_trip(vectors):
    v = vectors.query(P + "SELECT ?v WHERE { ex:a ex:embedding ?v }").single().v
    assert isinstance(v, Vector) and len(v) == 3
    assert all(math.isclose(x, y, rel_tol=1e-6) for x, y in zip(v, (0.9, 0.1, 0.0)))
    assert v == Vector(v) and repr(v).startswith("Vector([")
    long = Vector(range(20))
    assert "dims=20" in repr(long)


def test_vector_parameters_and_values(vectors):
    by_dot = P + "SELECT ?s ?score WHERE { ?s ex:embedding ?v BIND(dotProduct(?v, $q) AS ?score) } ORDER BY DESC(?score)"
    assert vectors.query(by_dot, q=Vector([1, 0, 0])).value("s") == [IRI(EX + "a"), IRI(EX + "b")]
    with pytest.raises(InvalidRequestError, match="not an RDF term"):
        vectors.query(by_dot, q=[0, 0, 1])  # a list could mean many values; say Vector
    jsonld = vectors.query({
        "select": ["?s", "?score"],
        "values": [["?q"], [Vector([0, 0, 1])]],
        "where": [{"@id": "?s", "ex:embedding": "?v"}, ["bind", "?score", "(cosineSimilarity ?v ?q)"]],
        "orderBy": [["desc", "?score"]],
    })
    assert [row[0] for row in jsonld] == ["ex:b", "ex:a"]


def test_numpy_arrays_are_vectors(vectors):
    np = pytest.importorskip("numpy")
    vectors.insert({"@context": CTX, "@id": "ex:c", "ex:embedding": np.array([0.5, 0.5, 0.0], dtype=np.float32)})
    v = vectors.query(P + "SELECT ?v WHERE { ex:c ex:embedding ?v }").single().v
    assert np.asarray(v).dtype == np.float32
    assert np.allclose(np.asarray(v), [0.5, 0.5, 0.0])
    nearest = vectors.query(
        P + "SELECT ?s WHERE { ?s ex:embedding ?v BIND(cosineSimilarity(?v, $q) AS ?score) } ORDER BY DESC(?score) LIMIT 1",
        q=np.array([0.6, 0.4, 0.0]),
    )
    assert nearest.value("s") == [IRI(EX + "c")]
    with pytest.raises(InvalidRequestError, match="one-dimensional"):
        vectors.insert({"@context": CTX, "@id": "ex:d", "ex:embedding": np.zeros((2, 2))})
    with pytest.raises(InvalidRequestError, match="one-dimensional"):
        vectors.query(P + "SELECT ?s WHERE { ?s ex:embedding ?v FILTER(?v = $q) }", q=np.zeros((2, 2)))


def test_non_finite_vectors_are_refused(vectors):
    with pytest.raises(InvalidRequestError, match="finite"):
        vectors.insert({"@context": CTX, "@id": "ex:e", "ex:embedding": Vector([1.0, float("nan")])})


def test_hybrid_search_in_one_query(conn):
    shop = conn.create("shop")
    shop.insert({
        "@context": CTX,
        "@graph": [
            {"@id": "ex:speaker", "ex:description": "Wireless audio speaker", "ex:embedding": Vector([0.8, 0.1, 0.1])},
            {"@id": "ex:cable", "ex:description": "Audio cable", "ex:embedding": Vector([0.1, 0.9, 0.1])},
        ],
    })
    shop.set_full_text([EX + "description"])
    ranked = shop.query(
        P + "SELECT ?p WHERE { ?p ex:description ?d ; ex:embedding ?v "
        'BIND(fulltext(?d, "wireless audio") AS ?text) BIND(cosineSimilarity(?v, $q) AS ?sim) } '
        "ORDER BY DESC(0.5 * ?text + 0.5 * ?sim)",
        q=Vector([0.8, 0.1, 0.1]),
    )
    assert ranked.value("p") == [IRI(EX + "speaker"), IRI(EX + "cable")]


def test_asyncio(tmp_path):
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("docs")
            await ledger.insert({"@context": CTX, "@id": "ex:d1", "ex:title": "Rust programming"})
            await ledger.set_full_text([EX + "title"])
            config = await ledger.full_text()
            result = await ledger.query(SCORE_TITLES, q="rust")
            return config, result.value("d")

    config, found = asyncio.run(main())
    assert config.properties == (IRI(EX + "title"),)
    assert found == [IRI(EX + "d1")]


def test_search_follows_time_travel_and_branches(docs):
    docs.set_full_text(["ex:title"])
    before = docs.log()[0].t
    docs.insert({"@context": CTX, "@id": "ex:d4", "ex:title": "Rust in production"})
    assert IRI(EX + "d4") not in hits(docs.at(t=before))
    assert IRI(EX + "d4") in hits(docs)
    dev = docs.branch("dev")
    dev.insert({"@context": CTX, "@id": "ex:d5", "ex:title": "Rust on a branch"})
    assert IRI(EX + "d5") in hits(dev)
    assert IRI(EX + "d5") not in hits(docs)
