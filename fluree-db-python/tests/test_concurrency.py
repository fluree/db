"""One engine per directory: threads in a process share it, other processes
are refused until it closes."""

import subprocess
import sys
import textwrap
import threading

import pytest

import fluree

EX = "http://example.org/"
COUNT = f"PREFIX ex: <{EX}> SELECT (COUNT(?s) AS ?n) WHERE {{ ?s ex:n ?v }}"


def item(n):
    return {"@context": {"ex": EX}, "@id": f"ex:item-{n}", "ex:n": n}


def count(ledger):
    return ledger.query(COUNT).single().n


def hold(path):
    """A process holding `path` open until its stdin closes."""
    script = textwrap.dedent(f"""
        import sys, fluree
        conn = fluree.connect({str(path)!r})
        print("READY", flush=True)
        sys.stdin.read()
    """)
    holder = subprocess.Popen(
        [sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert holder.stdout.readline().strip() == "READY"
    return holder


def opens_elsewhere(path):
    """Whether another process can open `path`, and what it said."""
    script = textwrap.dedent(f"""
        import fluree
        try:
            fluree.connect({str(path)!r}).close()
            print("OPENED")
        except fluree.DatabaseInUseError as e:
            print("IN USE", e.pid)
    """)
    done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


def test_connections_in_one_process_share_commits(tmp_path):
    with fluree.connect(tmp_path) as writer, fluree.connect(tmp_path / ".") as reader:
        writer.create("items")
        items = reader.ledger("items")
        assert count(items) == 0
        writer.ledger("items").insert(item(1))
        assert count(items) == 1


def run_threads(work, n):
    errors = []

    def guarded(worker):
        try:
            work(worker)
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(e)

    threads = [threading.Thread(target=guarded, args=(w,)) for w in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


def test_threads_writing_through_their_own_connections_all_commit(tmp_path):
    # Each connection used to be an engine of its own, and their commits
    # raced until writes ran out of retries with a ConflictError.
    with fluree.connect(tmp_path) as conn:
        conn.create("items")

    def work(worker):
        with fluree.connect(tmp_path) as conn:
            items = conn.ledger("items")
            for round in range(25):
                items.insert(item(worker * 100 + round))

    run_threads(work, 16)
    with fluree.connect(tmp_path) as conn:
        assert count(conn.ledger("items")) == 16 * 25


def test_threads_opening_and_closing_connections_lose_no_writes(tmp_path):
    with fluree.connect(tmp_path) as conn:
        conn.create("items")

    def work(worker):
        for round in range(5):
            with fluree.connect(tmp_path) as conn:
                conn.ledger("items").insert(item(worker * 100 + round))

    run_threads(work, 8)
    with fluree.connect(tmp_path) as conn:
        assert count(conn.ledger("items")) == 8 * 5


def test_closing_one_connection_leaves_the_others_open(tmp_path):
    first = fluree.connect(tmp_path)
    second = fluree.connect(tmp_path)
    ledger = first.create("items")
    first.close()

    with pytest.raises(fluree.InvalidRequestError, match="closed"):
        ledger.insert(item(1))
    second.ledger("items").insert(item(2))
    assert count(second.ledger("items")) == 1
    second.close()


def test_closing_releases_the_directory_while_its_objects_live(tmp_path):
    conn = fluree.connect(tmp_path)
    ledger = conn.create("items")
    snapshot = ledger.snapshot()
    conn.close()

    assert opens_elsewhere(tmp_path) == "OPENED"
    with pytest.raises(fluree.InvalidRequestError, match="closed"):
        snapshot.query(COUNT)
    del ledger


def test_connections_must_agree_on_indexing(tmp_path):
    with fluree.connect(tmp_path):
        with pytest.raises(fluree.InvalidRequestError, match="indexing"):
            fluree.connect(tmp_path, indexing=False)


def test_another_process_is_refused_while_the_directory_is_open(tmp_path):
    with fluree.connect(tmp_path):
        said = opens_elsewhere(tmp_path)
    assert said.startswith("IN USE")
    assert opens_elsewhere(tmp_path) == "OPENED"


def test_the_refusal_names_the_holder(tmp_path):
    holder = hold(tmp_path)
    try:
        with pytest.raises(fluree.DatabaseInUseError, match="fluree server") as refused:
            fluree.connect(tmp_path)
        if sys.platform != "win32":  # Windows cannot read a locked file
            assert refused.value.pid == holder.pid
    finally:
        holder.stdin.close()
        holder.wait(timeout=60)


def test_a_killed_holder_frees_the_directory(tmp_path):
    holder = hold(tmp_path)
    holder.kill()
    holder.wait(timeout=60)
    with fluree.connect(tmp_path) as conn:
        assert conn.ledgers() == []


def test_a_config_connection_to_file_storage_holds_it(tmp_path):
    config = {
        "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
        "@graph": [
            {"@id": "storage", "@type": "Storage", "filePath": str(tmp_path)},
            {"@id": "connection", "@type": "Connection", "indexStorage": {"@id": "storage"}},
        ],
    }
    conn = fluree.connect(config=config)
    ledger = conn.create("items")
    with pytest.raises(fluree.DatabaseInUseError, match="already open in this process"):
        fluree.connect(tmp_path)
    assert opens_elsewhere(tmp_path).startswith("IN USE")

    conn.close()
    with fluree.connect(tmp_path) as reopened:
        assert reopened.ledgers() == ["items:main"]
    del ledger
