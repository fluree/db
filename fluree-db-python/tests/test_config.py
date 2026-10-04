import base64
import os

import pytest

import fluree

EX = "http://example.org/"
KEY = base64.b64encode(bytes(range(32))).decode()


def config(path, **storage):
    return {
        "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
        "@graph": [
            {"@id": "storage", "@type": "Storage", "filePath": str(path), **storage},
            {"@id": "connection", "@type": "Connection", "indexStorage": {"@id": "storage"}},
        ],
    }


def all_bytes(path):
    data = b""
    for root, _, files in os.walk(path):
        for name in files:
            with open(os.path.join(root, name), "rb") as f:
                data += f.read()
    return data


def test_file_storage_from_config(tmp_path):
    with fluree.connect(config=config(tmp_path)) as conn:
        conn.create("people").insert({"@context": {"ex": EX}, "@id": "ex:alice", "ex:name": "Alice"})
    # The same directory opened by path sees the same ledger.
    with fluree.connect(tmp_path) as conn:
        assert conn.ledger("people").query(f"ASK {{ <{EX}alice> ?p ?o }}") is True


def test_plaintext_is_visible_without_a_key(tmp_path):
    # The control for test_encryption_at_rest: the probe can see plaintext.
    with fluree.connect(config=config(tmp_path)) as conn:
        conn.create("secrets").insert({"@context": {"ex": EX}, "@id": "ex:alice", "ex:secret": "hunter2-plaintext"})
    assert b"hunter2-plaintext" in all_bytes(tmp_path)


def test_encryption_at_rest(tmp_path):
    with fluree.connect(config=config(tmp_path, AES256Key=KEY)) as conn:
        conn.create("secrets").insert({"@context": {"ex": EX}, "@id": "ex:alice", "ex:secret": "hunter2-plaintext"})
    assert b"hunter2-plaintext" not in all_bytes(tmp_path)
    with fluree.connect(config=config(tmp_path, AES256Key=KEY)) as conn:
        rows = conn.ledger("secrets").query(f"SELECT ?v WHERE {{ <{EX}alice> <{EX}secret> ?v }}")
        assert [r.v for r in rows] == ["hunter2-plaintext"]


def test_env_var_values(tmp_path, monkeypatch):
    monkeypatch.setenv("FLUREE_TEST_DATA_DIR", str(tmp_path))
    cfg = config(tmp_path)
    cfg["@graph"][0]["filePath"] = {"envVar": "FLUREE_TEST_DATA_DIR"}
    with fluree.connect(config=cfg) as conn:
        conn.create("people")
    with fluree.connect(tmp_path) as conn:
        assert conn.ledgers() == ["people:main"]


def test_connect_needs_exactly_one_target(tmp_path):
    with pytest.raises(ValueError):
        fluree.connect()
    with pytest.raises(ValueError):
        fluree.connect(tmp_path, config=config(tmp_path))


def test_bad_config_is_a_value_error():
    with pytest.raises(ValueError):
        fluree.connect(config={"@graph": [{"@type": "Nonsense"}]})
