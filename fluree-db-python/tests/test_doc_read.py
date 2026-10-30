"""fluree.doc.read against a stub reader: the request it sends, how it signs
in, and the reading it returns."""

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import fluree
from fluree.errors import InvalidRequestError

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

MEMO = "# Staffing memo\n\nJane Doe joined Acme as Chief Technology Officer in March.\n"


def reading(doc_iri: str) -> list:
    """A reading in the pipeline's own terms: the echoed document graph, a
    mention of Acme, and the claim that a local entity works for it."""
    return [
        {"@id": doc_iri, "@graph": [{"@id": doc_iri, NIF + "isString": [{"@value": "echo"}]}]},
        {"@id": doc_iri + "/reading", "@graph": [
            {"@id": doc_iri + "/reading#o31-35",
             RDF + "type": [{"@id": DOC + "Mention"}],
             NIF + "anchorOf": [{"@value": "Acme"}],
             NIF + "beginIndex": [{"@value": 31}], NIF + "endIndex": [{"@value": 35}],
             NIF + "entity": [{"@id": "https://example.org/org/acme"}],
             DOC + "score": [{"@value": 0.9}]},
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


@pytest.fixture
def stub():
    """A reader (and a token exchange) on localhost. Records each request as
    ``(method, path, headers, body)``."""
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
            seen.append(("POST", self.path, {k.lower(): v for k, v in self.headers.items()}, body))
            if self.path == "/v1/auth/token":
                payload = json.dumps({"accessToken": "exchanged-token", "expiresAt": 0})
                self._send(200, payload, {})
                return
            doc_iri = "urn:fluree:doc:" + ("text.txt" if "text.txt" in body else "memo.md")
            self._send(200, json.dumps(reading(doc_iri)), {
                "Content-Type": "application/ld+json",
                "X-Fluree-Serialized-Tokens": "1032",
                "X-Fluree-Model": "stub-reader",
            })

        def _send(self, status, payload, headers):
            data = payload.encode()
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", seen
    server.shutdown()


@pytest.fixture
def files(tmp_path, monkeypatch):
    monkeypatch.setenv("FLUREE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("FLUREE_AI_API_KEY", raising=False)
    (tmp_path / "onto.ttl").write_text(ONTOLOGY)
    (tmp_path / "catalog.ttl").write_text(CATALOG)
    (tmp_path / "memo.md").write_text(MEMO)
    return tmp_path


def test_a_reader_with_its_own_key_reads_a_file(stub, files):
    base, seen = stub
    r = fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", catalog=files / "catalog.ttl",
                        model="stub-reader", url=base + "/extract", api_key="k-123", auth_header="X-Api-Key")
    (method, path, headers, body), = seen
    assert path == "/extract"
    assert headers["x-api-key"] == "k-123"
    assert headers["x-fluree-model"] == "stub-reader"
    assert headers["content-type"] == "application/trig"
    for role in ("document", "ontology", "instances"):
        assert f"<{XT}{role}> {{" in body
    assert '"Acme Corporation"' in body
    # A record carries its names and types, not everything known about it.
    assert "foundingDate" not in body

    assert (r.mention_count, r.relation_count) == (1, 1)
    assert r.usage.input_tokens == 1032 and r.usage.note == "1,032 tokens"
    assert r.calls == 1 and r.cached == 0
    assert all(n[DOC + "sourceDocument"][0]["@id"] == "urn:fluree:doc:memo.md" for n in r.nodes)
    mention, = r.mentions()
    assert (mention["text"], mention["begin"], mention["end"]) == ("Acme", 31, 35)
    assert mention["entity"] == "https://example.org/org/acme"
    assert mention["element"].startswith("urn:fluree:doc:memo.md-")
    claim, = r.claims()
    assert claim["predicate"] == "https://schema.org/worksFor" and claim["asserted"]
    assert "@prefix doc: <https://ns.flur.ee/doc#>" in r.to_turtle()

    with fluree.connect(":memory:") as conn:
        ledger = conn.create("readings")
        ledger.insert(r.to_jsonld())
        rows = ledger.query("SELECT ?e WHERE { ?m a <https://ns.flur.ee/doc#Mention> ; "
                            "<http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#entity> ?e }")
        assert [row[0] for row in rows] == ["https://example.org/org/acme"]


def test_text_and_a_scoped_catalog(stub, files):
    base, seen = stub
    r = fluree.doc.read(text=MEMO, ontology=files / "onto.ttl",
                        catalog=[(files / "catalog.ttl", "https://schema.org/Person")],
                        model="stub-reader", url=base + "/extract", api_key="k", auth_header="X-Api-Key")
    body = seen[0][3]
    # No record of that class: the instances graph is left out.
    assert f"<{XT}instances>" not in body
    assert r.mention_count == 1


def test_a_cache_dir_answers_a_repeated_request(stub, files):
    base, seen = stub
    args = dict(ontology=files / "onto.ttl", model="stub-reader", url=base + "/extract",
                api_key="k", auth_header="X-Api-Key", cache_dir=files / "cache")
    fluree.doc.read(files / "memo.md", **args)
    again = fluree.doc.read(files / "memo.md", **args)
    assert len(seen) == 1
    assert again.cached == 1 and again.usage.input_tokens == 0


def jwt(exp: int) -> str:
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    return f"{enc({'alg': 'none'})}.{enc({'exp': exp})}.sig"


def test_the_fluree_login_sign_in_reads_through_the_account(stub, files):
    base, seen = stub
    home = files / "home"
    home.mkdir()
    token = jwt(4_102_444_800)
    (home / "account.toml").write_text(
        f'url = "{base}"\napi_base_url = "{base}/v1"\nexchange_url = "{base}/v1/oauth/token"\n'
        f'access_token = "{token}"\nrefresh_token = "flk_x"\n')
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    (_, path, headers, _), = seen
    assert path == "/v1/grounding"
    assert headers["authorization"] == f"Bearer {token}"


def test_an_api_key_is_traded_for_a_token(stub, files, monkeypatch):
    base, seen = stub
    monkeypatch.setenv("FLUREE_AI_URL", base)
    monkeypatch.setenv("FLUREE_AI_API_KEY", "flk_test")
    fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")
    (_, p1, h1, _), (_, p2, h2, _) = seen
    assert (p1, h1["x-api-key"]) == ("/v1/auth/token", "flk_test")
    assert h1["user-agent"].startswith("fluree-python/")
    assert (p2, h2["authorization"]) == ("/v1/grounding", "Bearer exchanged-token")


def test_no_sign_in_says_how_to_get_one(files):
    with pytest.raises(InvalidRequestError, match="fluree login"):
        fluree.doc.read(files / "memo.md", ontology=files / "onto.ttl", model="stub-reader")


def test_a_path_or_text_not_both(files):
    with pytest.raises(TypeError):
        fluree.doc.read(files / "memo.md", text="x", ontology=files / "onto.ttl", model="m")
