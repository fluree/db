"""The reference worker in examples/reference_worker, run against its
example data: pinned inputs, an explicit clock, owned output graphs with a
generation record, replay without the model, failed and stale runs."""

import dataclasses
import importlib.util
import json
import sys
from pathlib import Path

import pytest

import fluree

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "reference_worker"
_spec = importlib.util.spec_from_file_location("reference_worker", EXAMPLE / "worker.py")
assert _spec is not None and _spec.loader is not None
worker = importlib.util.module_from_spec(_spec)
sys.modules["reference_worker"] = worker
_spec.loader.exec_module(worker)

EX = "http://example.org/"
W = worker.W
P = f"PREFIX ex: <{EX}> PREFIX w: <{W}> "
MANIFEST = worker.Manifest.from_json(json.loads((EXAMPLE / "manifest.json").read_text()))
DECISIONS = MANIFEST.decision_graph
EXTRACTIONS = MANIFEST.extraction_graph


class Model:
    """The toy model, counting calls; texts in ``unavailable`` get no answer."""

    def __init__(self, unavailable=()):
        self.calls = []
        self.unavailable = set(unavailable)
        self.broken = False

    def __call__(self, text):
        if self.broken:
            raise RuntimeError("the model is down")
        self.calls.append(text)
        return None if text in self.unavailable else worker.toy_model(text)


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("claims")
        ledger.insert(EXAMPLE / "claims.ttl")
        yield ledger


def outcomes(view, graph=DECISIONS):
    rows = view.query(
        P + f"SELECT ?case ?outcome ?reason WHERE {{ GRAPH <{graph}> {{ "
        "?d w:case ?case ; w:outcome ?outcome OPTIONAL { ?d w:reason ?reason } } }"
    )
    found = {}
    for case, outcome, reason in rows:
        _, reasons = found.setdefault(case.removeprefix(EX), (outcome, []))
        if reason is not None:
            reasons.append(reason.replace(EX, "ex:"))
    return {case: (outcome, sorted(reasons)) for case, (outcome, reasons) in found.items()}


def generation(view, graph):
    rows = view.query(
        P + f"SELECT ?p ?o WHERE {{ GRAPH <{graph}> {{ <{graph}#generation> ?p ?o }} }}"
    )
    return {p.removeprefix(W): o for p, o in rows}


def with_(**changes):
    return dataclasses.replace(MANIFEST, **changes)


V1 = {
    "case1": ("flagged", ["over-threshold"]),
    "case2": ("cleared", []),
    "case3": ("flagged", ["unapproved-vendor:Initech"]),
    "case4": ("indeterminate", ["incomplete-coverage:1/2"]),
    "case5": ("indeterminate", ["missing-text:ex:doc5a"]),
    "case6": ("cleared", []),
}


def test_each_case_is_decided_from_pinned_evidence(ledger):
    model = Model()
    extracted = worker.run_extract(ledger, MANIFEST, model)
    assert len(model.calls) == 7  # every document with text
    gated = worker.run_gate(ledger, MANIFEST)

    assert outcomes(ledger) == V1
    record = generation(ledger, DECISIONS)
    assert record["generation"] == 1
    assert record["stage"] == "gate"
    assert record["inputT"] == extracted.t == gated.input_t
    assert record["inputLedger"] == "claims:main"
    assert record["evaluationTime"] == MANIFEST.clock
    assert record["gateVersion"] == "v1"
    assert record["model"] == "toy-extractor-1"
    assert record["codeVersion"] == worker.CODE_VERSION
    assert record["engineVersion"] == fluree.__version__
    assert record["outputHash"] == gated.output_hash
    assert record["outputCount"] == 6
    assert record["status"] == "complete"
    assert ledger.log()[0].message == f"gate generation 1 of <{DECISIONS}>"


def test_a_new_gate_version_replays_the_same_inputs_without_the_model(ledger):
    model = Model()
    worker.run_extract(ledger, MANIFEST, model)
    first = worker.run_gate(ledger, MANIFEST)
    calls = len(model.calls)

    replay = worker.run_gate(ledger, with_(gate_version="v2", input_t=first.input_t))
    assert len(model.calls) == calls
    assert replay.input_t == first.input_t
    assert outcomes(ledger) == {**V1, "case2": ("flagged", ["over-threshold"])}
    assert generation(ledger, DECISIONS)["generation"] == 2
    # The earlier generation is still there to read.
    assert outcomes(ledger.at(t=first.t)) == V1
    assert generation(ledger.at(t=first.t), DECISIONS)["gateVersion"] == "v1"


def test_retained_extractions_are_reused_until_the_text_changes(ledger):
    model = Model()
    worker.run_extract(ledger, MANIFEST, model)
    model.calls.clear()

    again = worker.run_extract(ledger, MANIFEST, model)
    assert model.calls == []
    # Only the generation record changed.
    changed = {c.subject for c in ledger.changes(again.t)}
    assert changed == {f"{EXTRACTIONS}#generation"}

    ledger.update(
        P + 'DELETE { ex:doc1b ex:text ?t } INSERT { ex:doc1b ex:text "Invoice from Acme for 900.00" } '
        "WHERE { ex:doc1b ex:text ?t }"
    )
    worker.run_extract(ledger, MANIFEST, model)
    assert model.calls == ["Invoice from Acme for 900.00"]


def test_unavailable_evidence_is_never_a_clearance(ledger):
    model = Model(unavailable={"Thank you for your order."})
    worker.run_extract(ledger, MANIFEST, model)
    worker.run_gate(ledger, MANIFEST)
    assert outcomes(ledger)["case6"] == ("indeterminate", ["missing-extraction:ex:doc6a"])

    # The next run asks the model again for what it could not answer.
    model.unavailable.clear()
    model.calls.clear()
    worker.run_extract(ledger, MANIFEST, model)
    assert model.calls == ["Thank you for your order."]
    worker.run_gate(ledger, MANIFEST)
    assert outcomes(ledger)["case6"] == ("cleared", [])


def test_the_clock_decides_which_documents_had_arrived(ledger):
    worker.run_extract(ledger, MANIFEST, Model())
    early = with_(clock=MANIFEST.clock.replace(month=6, day=10))
    worker.run_gate(ledger, early)
    assert outcomes(ledger)["case1"] == ("indeterminate", ["incomplete-coverage:1/2"])
    assert generation(ledger, DECISIONS)["evaluationTime"] == early.clock


def test_a_failed_run_leaves_the_previous_generation(ledger):
    model = Model()
    worker.run_extract(ledger, MANIFEST, model)
    worker.run_gate(ledger, MANIFEST)
    t = ledger.snapshot().t

    model.broken = True
    ledger.update(P + 'INSERT DATA { ex:doc6a ex:text "Invoice from Acme for 5.00" }')
    with pytest.raises(RuntimeError, match="the model is down"):
        worker.run_extract(ledger, MANIFEST, model)
    assert ledger.snapshot().t == t + 1
    assert generation(ledger, EXTRACTIONS)["generation"] == 1

    ledger.update(P + "DELETE WHERE { ex:policy ex:reviewThreshold ?limit }")
    with pytest.raises(ValueError, match="reviewThreshold"):
        worker.run_gate(ledger, MANIFEST)
    assert ledger.snapshot().t == t + 2
    assert generation(ledger, DECISIONS)["generation"] == 1
    assert outcomes(ledger) == V1


def test_an_empty_result_still_publishes_a_generation(ledger):
    worker.run_extract(ledger, MANIFEST, Model())
    first = worker.run_gate(ledger, MANIFEST)
    ledger.update(P + "DELETE WHERE { ?case a ex:Case }")

    empty = worker.run_gate(ledger, MANIFEST)
    assert outcomes(ledger) == {}
    record = generation(ledger, DECISIONS)
    assert (record["generation"], record["outputCount"]) == (2, 0)
    assert empty.output_hash == worker.output_hash([])
    assert DECISIONS in ledger.graphs()
    assert outcomes(ledger.at(t=first.t)) == V1


def test_a_stale_run_does_not_publish_over_a_newer_generation(ledger):
    worker.run_extract(ledger, MANIFEST, Model())
    worker.run_gate(ledger, MANIFEST)
    worker.run_gate(ledger, with_(gate_version="v2"))

    with pytest.raises(worker.StaleRun, match="generation 2; this run started from 1"):
        worker.publish(ledger, DECISIONS, [], {W + "stage": "gate"}, started_from=1, input_t=1)
    assert generation(ledger, DECISIONS)["generation"] == 2


def test_a_run_that_loses_the_race_to_publish_refuses(ledger, monkeypatch):
    """Another run publishes while this one is staging its publication: the
    commit conflicts, the transaction runs again, and the re-read generation
    no longer matches."""
    worker.run_extract(ledger, MANIFEST, Model())
    transact = fluree.Ledger.transact
    raced = []

    def racing(self, fn, *args, **kwargs):
        def first_attempt_races(txn, *a, **kw):
            if not raced:
                raced.append(True)
                worker.run_gate(ledger, with_(gate_version="v2"))
            return fn(txn, *a, **kw)

        return transact(self, first_attempt_races, *args, **kwargs)

    monkeypatch.setattr(fluree.Ledger, "transact", racing)
    with pytest.raises(worker.StaleRun, match="generation 1; this run started from 0"):
        worker.run_gate(ledger, MANIFEST)
    record = generation(ledger, DECISIONS)
    assert (record["generation"], record["gateVersion"]) == (1, "v2")


def test_native_rules_can_read_a_published_graph(ledger):
    worker.run_extract(ledger, MANIFEST, Model())
    worker.run_gate(ledger, MANIFEST)
    ledger.insert({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": EX + "needsAnalyst",
        "f:rule": {
            "@type": "f:sparql",
            "@value": P + f"CONSTRUCT {{ ?case a ex:NeedsAnalyst }} WHERE {{ GRAPH <{DECISIONS}> "
            '{ ?d w:case ?case ; w:outcome "indeterminate" } }',
        },
    })
    rows = ledger.query("# PRAGMA reasoning: datalog\n" + P + "SELECT ?c WHERE { ?c a ex:NeedsAnalyst } ORDER BY ?c")
    assert [c for (c,) in rows] == [EX + "case4", EX + "case5"]


def test_the_command_line_runs_the_example_manifest(tmp_path, capsys):
    with fluree.connect(tmp_path) as conn:
        conn.create("claims").insert(EXAMPLE / "claims.ttl")
    worker.main([str(EXAMPLE / "manifest.json"), "--storage", str(tmp_path)])
    printed = capsys.readouterr().out
    assert "extract: Published(generation=1" in printed
    assert "gate: Published(generation=1" in printed
    with fluree.connect(tmp_path) as conn:
        assert outcomes(conn.ledger("claims")) == V1


def test_the_manifest_is_checked():
    doc = json.loads((EXAMPLE / "manifest.json").read_text())
    with pytest.raises(ValueError, match="gate version"):
        worker.Manifest.from_json({**doc, "gate": {**doc["gate"], "version": "v9"}})
    with pytest.raises(ValueError, match="timezone"):
        worker.Manifest.from_json({**doc, "clock": "2026-06-30T00:00:00"})
    with pytest.raises(ValueError, match="manifest"):
        worker.Manifest.from_json({**doc, "format": "something-else"})
