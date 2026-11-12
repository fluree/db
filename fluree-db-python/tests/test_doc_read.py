"""fluree.doc.read against a stub reader: the request it sends, how it signs
in, and the reading it returns."""

import base64
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


def reading(doc_iri: str, nif_only: bool = False) -> list:
    """A reading of the document: the echoed document graph, a mention of
    Acme (typed `doc:Mention`, or only as a NIF occurrence), and the claim
    that a local entity works for it."""
    mention_types = [{"@id": NIF + "RFC5147String"}] if nif_only else [{"@id": DOC + "Mention"}]
    return [
        {"@id": doc_iri, "@graph": [{"@id": doc_iri, NIF + "isString": [{"@value": "echo"}]}]},
        {"@id": doc_iri + "/reading", "@graph": [
            {"@id": doc_iri + "/reading#o31-35",
             RDF + "type": mention_types,
             NIF + "anchorOf": [{"@value": "Acme"}],
             NIF + "beginIndex": [{"@value": 31}], NIF + "endIndex": [{"@value": 35}],
             NIF + "entity": [{"@id": "https://example.org/org/acme"}]},
            {"@id": doc_iri + "/entity/1",
             RDF + "type": [{"@id": DOC + "Entity"}],
             "https://schema.org/worksFor": [{"@id": "https://example.org/org/acme"}]},
            {"@id": doc_iri + "/reading#r-0",
             RDF + "type": [{"@id": RDF + "Statement"}],
             RDF + "subject": [{"@id": doc_iri + "/entity/1"}],
             RDF + "predicate": [{"@id": "https://schema.org/worksFor"}],
             RDF + "object": [{"@id": "https://example.org/org/acme"}]},
        ]},
    ]


def jwt(exp: int) -> str:
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    return f"{enc({'alg': 'none'})}.{enc({'exp': exp})}.sig"


FAR = 4_102_444_800


class Stub:
    """A reader, a token exchange and a sign-in refresh on localhost.
    Records each request as ``(path, headers, body)``; ``reader_status``,
    ``refresh_answer`` and ``nif_only`` shape the answers."""

    def __init__(self):
        self.seen: list[tuple[str, dict, str]] = []
        self.reader_status = 200
        self.nif_only = False
        self.token_expires = FAR
        self.refresh_answer: tuple[int, dict] = (200, {
            "access_token": jwt(FAR), "refresh_token": "flk_rotated",
            "account": {"org_name": "Rotated Org", "email": "a@example.org"},
        })
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
                stub.seen.append((self.path, {k.lower(): v for k, v in self.headers.items()}, body))
                if self.path == "/v1/auth/token":
                    self._send(200, {"accessToken": "exchanged-token", "expiresAt": stub.token_expires})
                elif self.path == "/v1/oauth/token":
                    self._send(*stub.refresh_answer)
                elif stub.reader_status != 200:
                    self._send(stub.reader_status, {"error": "refused", "message": "no"})
                else:
                    doc_iri = SOURCE_DOC.search(body).group(1)
                    self._send(200, reading(doc_iri, stub.nif_only), {
                        "Content-Type": "application/ld+json",
                        "X-Fluree-Serialized-Tokens": "1032",
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

    assert r.relation_count == 1
    assert r.usage.input_tokens == 1032 and r.usage.note == "1,032 tokens"
    assert r.calls == 1 and r.cached == 0
    assert isinstance(r.notes, list)
    assert all(n[DOC + "sourceDocument"][0]["@id"] == "urn:fluree:doc:memo.md" for n in r.nodes)
    mention, = r.mentions()
    assert set(mention) == {"id", "text", "begin", "end", "entity", "element"}
    assert (mention["text"], mention["begin"], mention["end"]) == ("Acme", 31, 35)
    assert mention["entity"] == "https://example.org/org/acme"
    assert mention["element"].startswith("urn:fluree:doc:memo.md-")
    claim, = r.claims()
    assert set(claim) == {"id", "subject", "predicate", "object", "asserted"}
    assert claim["predicate"] == "https://schema.org/worksFor" and claim["asserted"]
    assert "@prefix doc: <https://ns.flur.ee/doc#>" in r.to_turtle()

    with fluree.connect(":memory:") as conn:
        ledger = conn.create("readings")
        ledger.insert(r.to_jsonld())
        rows = ledger.query("SELECT ?e WHERE { ?m a <https://ns.flur.ee/doc#Mention> ; "
                            "<http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#entity> ?e }")
        assert [row[0] for row in rows] == [fluree.IRI("https://example.org/org/acme")]


def test_a_nif_occurrence_is_a_mention_row_whatever_else_it_is(stub, files):
    stub.nif_only = True
    r = fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader",
                        url=stub.base + "/extract", api_key="k", auth_header="X-Api-Key")
    mention, = r.mentions()
    assert (mention["text"], mention["begin"], mention["entity"]) == ("Acme", 31, "https://example.org/org/acme")


def test_text_and_a_scoped_catalog(stub, files):
    r = fluree.doc.read(text=MEMO, ontology=files / "onto.ttl",
                        catalog=[(files / "catalog.ttl", "https://schema.org/Person")],
                        model="stub-reader", url=stub.base + "/extract", api_key="k", auth_header="X-Api-Key")
    body = stub.seen[0][2]
    # No record of that class: the instances graph is left out.
    assert f"<{XT}instances>" not in body
    assert r.relation_count == 1


def test_two_texts_are_two_documents(stub, files):
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=stub.base + "/extract",
                api_key="k", auth_header="X-Api-Key")
    a = fluree.doc.read(text="Jane Doe joined Acme.", **args)
    b = fluree.doc.read(text="John Smith joined Globex.", **args)
    doc_of = lambda r: {n[DOC + "sourceDocument"][0]["@id"] for n in r.nodes}  # noqa: E731
    (doc_a,), (doc_b,) = doc_of(a), doc_of(b)
    assert doc_a != doc_b and doc_a.startswith("urn:fluree:doc:text/")
    assert not {n["@id"] for n in a.nodes} & {n["@id"] for n in b.nodes}
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
