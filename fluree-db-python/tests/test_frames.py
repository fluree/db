"""Tables in and out: rows (DataFrames or dicts) as ledger nodes, results as
DataFrames, and results shown in notebooks."""

import datetime as dt
import math
from decimal import Decimal

import pytest

import fluree
from fluree import IRI, BlankNode, InvalidRequestError

EX = "http://example.org/"
P = f"PREFIX ex: <{EX}> "
PEOPLE = P + "SELECT ?p ?name ?age ?joined WHERE { ?p a ex:Person ; ex:name ?name OPTIONAL { ?p ex:age ?age } OPTIONAL { ?p ex:joined ?joined } } ORDER BY ?name"
ROWS = [
    {"id": 1, "name": "Ann Lee", "age": 31, "joined": dt.date(2020, 1, 2), "manager_id": None},
    {"id": 2, "name": "Ben", "age": None, "joined": dt.date(2021, 3, 4), "manager_id": 1},
    {"id": 3, "name": "Cy", "age": 27, "joined": None, "manager_id": 1},
]


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("people")
        ledger.set_context({"ex": EX})
        yield ledger


def load(ledger, rows, **options):
    options = {
        "id": "ex:person/{id}",
        "type": "ex:Person",
        "columns": {"id": None, "manager_id": None},
        "refs": {"ex:manager": "ex:person/{manager_id}"},
        "vocab": None,
        **options,
    }
    return ledger.insert_rows(rows, **options)


def renamed(rows):
    return [{**{f"ex:{k}": v for k, v in r.items() if k not in ("id", "manager_id")}, "id": r["id"], "manager_id": r["manager_id"]} for r in rows]


def test_dicts_become_nodes(ledger):
    commit = load(ledger, renamed(ROWS))
    assert commit.asserts > 0
    rows = ledger.select(PEOPLE).values()
    assert rows == [
        [IRI(EX + "person/1"), "Ann Lee", 31, dt.date(2020, 1, 2)],
        [IRI(EX + "person/2"), "Ben", None, dt.date(2021, 3, 4)],
        [IRI(EX + "person/3"), "Cy", 27, None],
    ]
    managed = ledger.select(P + "SELECT ?p WHERE { ?p ex:manager ex:person\\/1 } ORDER BY ?p")
    assert managed.value("p") == [IRI(EX + "person/2"), IRI(EX + "person/3")]


def test_a_vocab_prefixes_plain_column_names(ledger):
    ledger.insert_rows(
        [{"id": "a b", "name": "Spaced"}],
        id="ex:thing/{id}",
        vocab=EX,
        columns={"id": None},
    )
    row = ledger.select(P + "SELECT ?t ?n WHERE { ?t ex:name ?n }").single()
    assert row.t == IRI(EX + "thing/a%20b") and row.n == "Spaced"


def test_without_an_id_each_row_is_a_new_node(ledger):
    ledger.insert_rows([{"ex:name": "x"}, {"ex:name": "x"}])
    nodes = ledger.select(P + "SELECT ?n WHERE { ?n ex:name ?name }")
    assert len(nodes) == 2 and all(isinstance(n, BlankNode) for n in nodes.value("n"))


def test_pandas_frames_with_missing_values(ledger):
    pandas = pytest.importorskip("pandas")
    frame = pandas.DataFrame(renamed(ROWS))
    frame["ex:joined"] = pandas.to_datetime(frame["ex:joined"])  # NaT for the missing one
    # A missing value turns an int column into floats; the nullable Int64
    # dtype keeps them ints, with pd.NA for the gap.
    frame["ex:age"] = frame["ex:age"].astype("Int64")
    load(ledger, frame)
    rows = ledger.select(PEOPLE).values()
    assert [r[1] for r in rows] == ["Ann Lee", "Ben", "Cy"]
    assert rows[1][2] is None  # NaN age added nothing
    assert rows[2][3] is None  # NaT added nothing
    assert rows[0][2] == 31 and isinstance(rows[0][2], int)
    assert ledger.select(PEOPLE).to_pandas()["name"].tolist() == ["Ann Lee", "Ben", "Cy"]


def test_polars_frames(ledger):
    polars = pytest.importorskip("polars")
    frame = polars.DataFrame(renamed(ROWS))
    load(ledger, frame)
    assert ledger.select(PEOPLE).value("name") == ["Ann Lee", "Ben", "Cy"]
    out = ledger.select(PEOPLE).to_polars()
    assert out.columns == ["p", "name", "age", "joined"]
    assert out["name"].to_list() == ["Ann Lee", "Ben", "Cy"]
    assert out["joined"].to_list()[0] == dt.date(2020, 1, 2)


def test_upsert_rows_replaces_values(ledger):
    load(ledger, renamed(ROWS))
    ledger.upsert_rows(
        [{"id": 1, "ex:age": 32}],
        id="ex:person/{id}",
        columns={"id": None},
    )
    assert ledger.select(P + "SELECT ?age WHERE { ex:person\\/1 ex:age ?age }").value("age") == [32]


def test_rows_in_a_transaction(ledger):
    with ledger.transaction() as txn:
        txn.insert_rows([{"id": 9, "ex:name": "Zed"}], id="ex:person/{id}", type="ex:Person", columns={"id": None})
        assert txn.select(P + "SELECT ?n WHERE { ?p a ex:Person ; ex:name ?n }").value("n") == ["Zed"]
    assert ledger.select(P + "SELECT ?n WHERE { ?p ex:name ?n }").value("n") == ["Zed"]


def test_values_convert_as_in_insert(ledger):
    ledger.insert_rows(
        [{"id": 1, "ex:price": Decimal("9.99"), "ex:tags": ["a", "b"], "ex:lang": fluree.LangString("chat", "fr")}],
        id="ex:item/{id}",
        columns={"id": None},
    )
    row = ledger.select(P + "SELECT ?price ?lang WHERE { ex:item\\/1 ex:price ?price ; ex:lang ?lang }").single()
    assert row.price == Decimal("9.99") and row.lang.language == "fr"
    tags = ledger.select(P + "SELECT ?t WHERE { ex:item\\/1 ex:tags ?t } ORDER BY ?t").value("t")
    assert tags == ["a", "b"]


def test_mistakes_are_refused(ledger):
    with pytest.raises(InvalidRequestError, match="lack"):
        ledger.insert_rows([{"name": "x"}], id="ex:person/{id}")
    with pytest.raises(InvalidRequestError, match="no value"):
        ledger.insert_rows([{"id": None, "ex:name": "x"}], id="ex:person/{id}")
    with pytest.raises(TypeError, match="wrap a single row"):
        ledger.insert_rows({"ex:name": "x"})
    with pytest.raises(TypeError):
        ledger.insert_rows(42)


def test_results_show_as_tables_in_notebooks(ledger):
    load(ledger, renamed(ROWS))
    html = ledger.select(PEOPLE)._repr_html_()
    assert html.startswith("<table>") and "<th>name</th>" in html and "Ann Lee" in html
    many = ledger.select(P + "SELECT ?i WHERE { VALUES ?i { " + " ".join(str(i) for i in range(60)) + " } }")
    assert "60 records, 10 not shown" in many._repr_html_()
    escaped = ledger.select(P + 'SELECT ?x WHERE { BIND("<b>" AS ?x) }')._repr_html_()
    assert "&lt;b&gt;" in escaped and "<b>" not in escaped.replace("<tbody>", "")


def test_missing_values_are_recognized():
    from fluree._frames import _missing

    assert _missing(None) and _missing(math.nan)
    assert not _missing(0) and not _missing("")


# -- logging -------------------------------------------------------------------


def wait_for(caplog, text, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(text in r.getMessage() for r in caplog.records):
            return True
        time.sleep(0.02)
    return False


def test_engine_events_reach_python_logging(ledger, caplog):
    import logging

    caplog.set_level(logging.DEBUG, logger="fluree.engine")
    ledger.insert(f'@prefix ex: <{EX}> . GRAPH <{EX}g> {{ ex:a ex:n 1 }}', format="trig")
    try:
        ledger.drop_graph(EX + "g")  # logs "Named graph dropped" at INFO
        assert not wait_for(caplog, "Named graph dropped", timeout=0.5)  # warnings only, by default
        fluree.set_log_level("INFO")
        ledger.insert(f'@prefix ex: <{EX}> . GRAPH <{EX}g> {{ ex:a ex:n 2 }}', format="trig")
        ledger.drop_graph(EX + "g")
        assert wait_for(caplog, "Named graph dropped")
        record = next(r for r in caplog.records if "Named graph dropped" in r.getMessage())
        assert record.name == "fluree.engine" and record.levelno == logging.INFO
        assert record.target.startswith("fluree_db_api")
        assert "graph_iri=" in record.getMessage()
    finally:
        fluree.set_log_level("WARNING")
    with pytest.raises(ValueError):
        fluree.set_log_level("LOUD")


def test_the_log_shows_once_the_application_configures_logging():
    import subprocess
    import sys

    warn = "import fluree, logging; logging.getLogger('fluree.engine').warning('engine warning')"

    def stderr(setup):
        return subprocess.run([sys.executable, "-c", setup + warn], capture_output=True, text=True, check=True).stderr

    assert "engine warning" not in stderr("")
    assert "engine warning" in stderr("import logging; logging.basicConfig(); ")
