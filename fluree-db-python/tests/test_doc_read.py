"""fluree.doc.read against a stub reader: the request it sends, how it signs
in, and the reading it returns."""

import base64
import hashlib
import json
import os
import re
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import fluree
from fluree.errors import InvalidRequestError, PermissionDeniedError

DOC = "https://ns.flur.ee/doc#"
NIF = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XT = "https://ns.flur.ee/extract#"

ONTOLOGY = """@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix schema: <https://schema.org/> .
schema:Organization a rdfs:Class ; rdfs:label "organization" .
schema:Person a rdfs:Class ; rdfs:label "person" .
schema:worksFor a rdf:Property ; rdfs:label "works for" .
"""

CATALOG = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix schema: <https://schema.org/> .
<https://example.org/org/acme> a schema:Organization ;
    skos:prefLabel "Acme Corporation" ; skos:altLabel "Acme" ; schema:foundingDate "1999" .
"""

PEOPLE = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix schema: <https://schema.org/> .
<https://example.org/person/jane> a schema:Person ; skos:prefLabel "Jane Doe" .
"""

MEMO = "# Staffing memo\n\nJane Doe joined Acme as Chief Technology Officer in March.\n"

SOURCE_DOC = re.compile(rf"<([^>]+)> <{re.escape(RDF)}type> <{re.escape(DOC)}SourceDocument>")


CONTEXT = {
    "id": "@id", "type": "@type",
    "nif": NIF, "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "schema": "https://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#", "xt": XT, "prov": "http://www.w3.org/ns/prov#",
    "label": "rdfs:label", "anchorOf": "nif:anchorOf",
    "beginIndex": "nif:beginIndex", "endIndex": "nif:endIndex",
    "occurrenceClass": {"@id": "xt:occurrenceClass", "@type": "@id"},
    "assertionStatus": {"@id": "xt:assertionStatus", "@type": "@id"},
    "accepted": "xt:accepted",
    "entity": {"@id": "xt:entity", "@type": "@id"},
    "localEntity": {"@id": "xt:localEntity", "@type": "@id"},
    "linkConfidence": {"@id": "xt:linkConfidence", "@type": "xsd:double"},
    "score": {"@id": "xt:score", "@type": "xsd:double"},
    "subjectOccurrence": {"@id": "xt:subjectOccurrence", "@type": "@id"},
    "objectOccurrence": {"@id": "xt:objectOccurrence", "@type": "@id"},
    "responseLayout": {"@id": "xt:responseLayout", "@type": "@id"},
    "used": {"@id": "prov:used", "@type": "@id"},
    "wasAssociatedWith": {"@id": "prov:wasAssociatedWith", "@type": "@id"},
    "wasGeneratedBy": {"@id": "prov:wasGeneratedBy", "@type": "@id"},
    "Claim": "xt:Claim", "LocalEntity": "xt:LocalEntity", "Occurrence": "xt:Occurrence",
}


def reading(doc_iri: str, include: str = "claims,mentions") -> dict:
    """A reading of the document in the reader's layout: a Fluree DB
    transaction whose commit metadata names the run, the fact that an entity
    the run minted works for Acme with the claim behind it annotating it,
    and (as `include` asks) the mentions of both."""
    run = "urn:fluree:run:01TEST" + hashlib.sha256(doc_iri.encode()).hexdigest()[:8]
    jane = "urn:fluree:entity:" + hashlib.sha256(doc_iri.encode()).hexdigest()[:6] + "-jane"
    context = dict(CONTEXT, run={"@id": run + "-", "@prefix": True})
    fact = {"@id": "https://example.org/org/acme"}
    if "claims" in include:
        fact["@annotation"] = {
            "@id": "run:r-0", "type": "Claim", "assertionStatus": "xt:affirmed",
            "accepted": True, "score": 0.97, "wasGeneratedBy": run,
        }
    graph = [{"id": jane, "type": "schema:Person", "label": "Jane Doe", "schema:worksFor": fact}]
    if "mentions" in include:
        graph.insert(0, {"id": "xt:mentions", "@graph": [
            {"id": "run:o15-23", "type": ["Occurrence", "nif:RFC5147String"], "anchorOf": "Jane Doe",
             "beginIndex": 15, "endIndex": 23, "occurrenceClass": "schema:Person",
             "localEntity": jane, "score": 0.99},
            {"id": "run:o31-35", "type": ["Occurrence", "nif:RFC5147String"], "anchorOf": "Acme",
             "beginIndex": 31, "endIndex": 35, "occurrenceClass": "schema:Organization",
             "entity": "https://example.org/org/acme", "linkConfidence": 0.98},
            {"id": jane, "type": "LocalEntity"},
        ]})
    return {
        "@context": context,
        "txn-meta": {
            "used": {"@id": doc_iri},
            "wasAssociatedWith": {"@id": "https://ns.flur.ee/model/example-reader-1.0"},
            "responseLayout": {"@id": "https://ns.flur.ee/extract/response/2"},
            "xt:run": {"@id": run},
        },
        "@graph": graph,
    }


EARLIER = [{"@id": "urn:d", "@graph": [{"@id": "urn:d", NIF + "isString": [{"@value": "echo"}]}]}]


def jwt(exp: int) -> str:
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    return f"{enc({'alg': 'none'})}.{enc({'exp': exp})}.sig"


FAR = 4_102_444_800


class Stub:
    """A reader, a token exchange and a sign-in refresh on localhost.
    Records each request as ``(path, headers, body)``; ``reader_status``,
    ``refresh_answer`` and ``earlier`` (answer in the earlier layout) shape
    the answers."""

    def __init__(self):
        self.seen: list[tuple[str, dict, str]] = []
        self.reader_status = 200
        self.earlier = False
        self.token_expires = FAR
        self.exchange_status = 200
        self.refresh_answer: tuple[int, dict] = (200, {
            "access_token": jwt(FAR), "refresh_token": "flk_rotated",
            "account": {"org_name": "Rotated Org", "email": "a@example.org"},
        })
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
                stub.seen.append((self.path, {k.lower(): v for k, v in self.headers.items()}, body))
                if self.path == "/v1/auth/token" and stub.exchange_status != 200:
                    self._send(stub.exchange_status, {"error": {
                        "message": "Invalid API key", "status": stub.exchange_status,
                        "error": "invalid_api_key"}})
                elif self.path == "/v1/auth/token":
                    self._send(200, {"accessToken": "exchanged-token", "expiresAt": stub.token_expires})
                elif self.path == "/v1/oauth/token":
                    self._send(*stub.refresh_answer)
                elif stub.reader_status != 200:
                    self._send(stub.reader_status, {"error": "refused", "message": "no"})
                else:
                    doc_iri = SOURCE_DOC.search(body).group(1)
                    include = self.headers.get("X-Fluree-Include", "claims")
                    answer = EARLIER if stub.earlier else reading(doc_iri, include)
                    self._send(200, answer, {
                        "Content-Type": "application/ld+json",
                        "X-Fluree-Serialized-Tokens": "1032",
                        "X-Fluree-Release": "example-reader-1.0",
                        "X-Fluree-Request-ID": f"req-{len(stub.seen)}",
                    })

            def _send(self, status, payload, headers=None):
                data = json.dumps(payload).encode()
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def paths(self) -> list[str]:
        return [p for p, _, _ in self.seen]


@pytest.fixture
def stub():
    s = Stub()
    yield s
    s.server.shutdown()


@pytest.fixture
def files(tmp_path, monkeypatch):
    monkeypatch.setenv("FLUREE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("FLUREE_AI_API_KEY", raising=False)
    monkeypatch.delenv("FLUREE_AI_URL", raising=False)
    (tmp_path / "onto.ttl").write_text(ONTOLOGY)
    (tmp_path / "catalog.ttl").write_text(CATALOG)
    (tmp_path / "people.ttl").write_text(PEOPLE)
    (tmp_path / "memo.md").write_text(MEMO)
    return tmp_path


def sign_in(files, base: str, token: str, api_base: str | None = None) -> None:
    home = files / "home"
    home.mkdir(exist_ok=True)
    (home / "account.toml").write_text(
        f'url = "{base}"\napi_base_url = "{api_base or base + "/v1"}"\n'
        f'exchange_url = "{base}/v1/oauth/token"\naccess_token = "{token}"\nrefresh_token = "flk_x"\n')


def test_a_reader_with_its_own_key_reads_a_file(stub, files):
    r = fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", catalog=files / "catalog.ttl",
                        model="stub-reader", url=stub.base + "/extract", api_key="k-123", auth_header="X-Api-Key")
    (path, headers, body), = stub.seen
    assert path == "/extract"
    assert headers["x-api-key"] == "k-123"
    assert headers["x-fluree-model"] == "stub-reader"
    assert headers["content-type"] == "application/trig"
    for role in ("document", "ontology", "instances"):
        assert f"<{XT}{role}> {{" in body
    assert '"Acme Corporation"' in body
    # A record carries its names and types, not everything known about it.
    assert "foundingDate" not in body

    # The controls are headers, never also in the body.
    assert headers["x-fluree-output"] == "fluree-transaction"
    assert headers["x-fluree-include"] == "claims,mentions"
    assert "extract#request" not in body and "extract#Request" not in body

    assert r.claim_count == 1 and r.mention_count == 2 and r.fact_count == 3
    assert r.usage.input_tokens == 1032 and r.usage.note == "1,032 tokens"
    assert r.calls == 1 and r.cached == 0
    assert r.release == "example-reader-1.0"
    run, = r.runs
    assert run["run"].startswith("urn:fluree:run:01TEST") and run["request_id"] == "req-1"
    assert isinstance(r.notes, list)
    tx = r.to_jsonld()
    assert tx["txn-meta"]["used"]["@id"] == "urn:fluree:doc:memo.md"
    acme = next(m for m in r.mentions() if m["text"] == "Acme")
    assert set(acme) == {"id", "text", "begin", "end", "class", "entity", "new_entity",
                         "confidence", "element"}
    assert (acme["begin"], acme["end"]) == (31, 35)
    assert acme["entity"] == "https://example.org/org/acme" and not acme["new_entity"]
    assert acme["element"].startswith("urn:fluree:doc:memo.md-")
    jane = next(m for m in r.mentions() if m["text"] == "Jane Doe")
    assert jane["new_entity"] and jane["entity"] in r.new_entities
    assert r.linked == ["https://example.org/org/acme"]
    claim, = r.claims()
    assert set(claim) == {"id", "subject", "predicate", "object", "status", "accepted", "score",
                          "asserted"}
    assert claim["predicate"] == "https://schema.org/worksFor" and claim["asserted"]
    assert claim["object"] == "https://example.org/org/acme" and claim["status"] == XT + "affirmed"
    assert "@prefix xt: <https://ns.flur.ee/extract#>" in r.to_turtle()
    assert "<#txn-meta>" in r.to_trig()

    with fluree.connect(":memory:") as conn:
        ledger = conn.create("readings")
        ledger.insert(r.to_jsonld())
        rows = ledger.query("SELECT ?o WHERE { ?s <https://schema.org/worksFor> ?o }")
        assert [row[0] for row in rows] == ["https://example.org/org/acme"]


def test_include_says_what_comes_with_the_facts(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract",
                api_key="k", auth_header="X-Api-Key")
    r = fluree.doc.read(files / "memo.md", include="none", **args)
    assert stub.seen[-1][1]["x-fluree-include"] == "none"
    assert (r.claim_count, r.mention_count, r.fact_count) == (0, 0, 3)
    r = fluree.doc.read(files / "memo.md", include=["mentions"], **args)
    assert stub.seen[-1][1]["x-fluree-include"] == "mentions"
    assert (r.claim_count, r.mention_count) == (0, 2)
    with pytest.raises(InvalidRequestError, match="include"):
        fluree.doc.read(files / "memo.md", include="facts", **args)


def test_an_answer_in_the_earlier_layout_is_refused_by_name(stub, files):
    stub.earlier = True
    with pytest.raises(fluree.errors.FlureeError, match="response layout 2"):
        fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                        url=stub.base + "/extract", api_key="k", auth_header="X-Api-Key")


def test_text_and_a_scoped_catalog(stub, files):
    r = fluree.doc.read(text=MEMO, ontology=files / "onto.ttl",
                        catalog=[(files / "catalog.ttl", "https://schema.org/Person")],
                        model="stub-reader", url=stub.base + "/extract", api_key="k", auth_header="X-Api-Key")
    body = stub.seen[0][2]
    # No record of that class: the instances graph is left out.
    assert f"<{XT}instances>" not in body
    assert r.claim_count == 1


def test_two_texts_are_two_documents(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract",
                api_key="k", auth_header="X-Api-Key")
    a = fluree.doc.read(text="Jane Doe joined Acme.", **args)
    b = fluree.doc.read(text="John Smith joined Globex.", **args)
    doc_of = lambda r: {t["txn-meta"]["used"]["@id"] for t in r.transactions}  # noqa: E731
    (doc_a,), (doc_b,) = doc_of(a), doc_of(b)
    assert doc_a != doc_b and doc_a.startswith("urn:fluree:doc:text/")
    assert not set(a.new_entities) & set(b.new_entities)
    # The same text is the same document; doc_iri= names one outright.
    assert doc_of(fluree.doc.read(text="Jane Doe joined Acme.", **args)) == {doc_a}
    assert doc_of(fluree.doc.read(text="x", doc_iri="urn:ex:memo", **args)) == {"urn:ex:memo"}


def test_a_cache_dir_answers_a_repeated_request(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract",
                api_key="k", auth_header="X-Api-Key", cache_dir=files / "cache")
    fluree.doc.read(files / "memo.md", **args)
    again = fluree.doc.read(files / "memo.md", **args)
    assert len(stub.seen) == 1
    assert again.cached == 1 and again.usage.input_tokens == 0
    assert again.runs[0]["cached"] and again.runs[0]["request_id"] == "req-1"


def test_the_fluree_login_sign_in_reads_through_the_account(stub, files):
    token = jwt(FAR)
    sign_in(files, stub.base, token)
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    (path, headers, _), = stub.seen
    assert path == "/v1/grounding"
    assert headers["authorization"] == f"Bearer {token}"


def test_an_expired_sign_in_is_refreshed_and_saved(stub, files):
    sign_in(files, stub.base, jwt(int(time.time()) - 10))
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    assert stub.paths() == ["/v1/oauth/token", "/v1/grounding"]
    refresh = json.loads(stub.seen[0][2])
    assert refresh == {"grant_type": "refresh_token", "refresh_token": "flk_x"}
    fresh = stub.refresh_answer[1]["access_token"]
    assert stub.seen[1][1]["authorization"] == f"Bearer {fresh}"
    account = files / "home" / "account.toml"
    saved = account.read_text()
    assert f'access_token = "{fresh}"' in saved
    assert 'refresh_token = "flk_rotated"' in saved
    assert 'org_name = "Rotated Org"' in saved
    if os.name == "posix":
        assert stat.S_IMODE(account.stat().st_mode) == 0o600


def test_a_revoked_sign_in_says_to_sign_in_again(stub, files):
    sign_in(files, stub.base, jwt(int(time.time()) - 10))
    stub.refresh_answer = (400, {"error": "invalid_grant", "error_description": "revoked"})
    with pytest.raises(PermissionDeniedError, match="no longer valid"):
        fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    assert stub.paths() == ["/v1/oauth/token"]


def test_a_sign_in_is_never_sent_to_another_service(stub, files):
    # Signed in to a service elsewhere: the stub's url gets no credential.
    sign_in(files, stub.base, jwt(FAR), api_base="https://service.example/v1")
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                    url=stub.base + "/extract")
    (path, headers, _), = stub.seen
    assert path == "/extract" and "authorization" not in headers


def test_a_url_with_no_credential_is_called_without_one(stub, files):
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                    url=stub.base + "/extract")
    (path, headers, _), = stub.seen
    assert path == "/extract" and "authorization" not in headers


def test_an_api_key_is_traded_for_a_token_once_an_hour(stub, files, monkeypatch):
    monkeypatch.setenv("FLUREE_AI_URL", stub.base)
    monkeypatch.setenv("FLUREE_AI_API_KEY", "flk_test")
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    fluree.doc.read(text="Another memo.", ontology=files / "onto.ttl", model="stub-reader")
    assert stub.paths() == ["/v1/auth/token", "/v1/grounding", "/v1/grounding"]
    (_, h1, _), (_, h2, _), _ = stub.seen
    assert h1["x-api-key"] == "flk_test"
    assert h1["user-agent"].startswith("fluree-python/")
    assert h2["authorization"] == "Bearer exchanged-token"


def test_a_developer_key_the_exchange_refuses_goes_to_the_reader_as_is(stub, files):
    # Only a connection's key trades for a token; a developer key is the
    # bearer itself, checked by the reader's service.
    stub.exchange_status = 401
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                    url=stub.base + "/v1/grounding", api_key="flk_dev_secret")
    assert stub.paths() == ["/v1/auth/token", "/v1/grounding"]
    (_, _, _), (_, headers, _) = stub.seen
    assert headers["authorization"] == "Bearer flk_dev_secret"


def test_an_api_key_that_isnt_a_service_key_is_a_bearer_token(stub, files):
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                    url=stub.base + "/extract", api_key="plain-key")
    (path, headers, _), = stub.seen
    assert (path, headers["authorization"]) == ("/extract", "Bearer plain-key")


def test_a_refused_credential_is_permission_denied(stub, files):
    stub.reader_status = 401
    with pytest.raises(PermissionDeniedError):
        fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                        url=stub.base + "/extract", api_key="k", auth_header="X-Api-Key")


def test_arguments_that_cant_make_a_read_are_invalid(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract")
    with pytest.raises(InvalidRequestError, match="auth_header"):
        fluree.doc.read(files / "memo.md", auth_header="X-Api-Key", **args)
    with pytest.raises(InvalidRequestError, match="cache"):
        fluree.doc.read(files / "memo.md", cache="bogus", **args)
    assert stub.seen == []


def test_catalog_entries_are_files_or_file_class_pairs(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract")
    with pytest.raises(TypeError, match="pair"):
        fluree.doc.read(files / "memo.md", catalog=[(files / "catalog.ttl",)], **args)
    # A bare pair of RDF files is two catalogs, not a file and a class.
    fluree.doc.read(files / "memo.md", catalog=(files / "catalog.ttl", files / "people.ttl"), **args)
    body = stub.seen[-1][2]
    assert '"Acme Corporation"' in body and '"Jane Doe"' in body


def test_no_sign_in_says_how_to_get_one(files):
    with pytest.raises(InvalidRequestError, match="fluree login"):
        fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")


def test_a_path_or_text_not_both(files):
    with pytest.raises(TypeError):
        fluree.doc.read(files / "memo.md", text="x", ontology=files / "onto.ttl", model="m")
