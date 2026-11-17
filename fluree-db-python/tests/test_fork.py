"""Forked processes: a child opens its own connection; what it inherited
from its parent is refused, never half-working.

Each case runs in a fresh interpreter so whether the engine started before
the fork is under the test's control."""

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(not hasattr(__import__("os"), "fork"), reason="no fork on this platform")

SETUP = """
import faulthandler, os, sys, tempfile, multiprocessing
import fluree
faulthandler.enable()
EX = "http://example.org/"
Q = f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ?s ex:name ?n }}"

def report(fn):
    try:
        print("OK", fn(), flush=True)
    except fluree.FlureeError as e:
        print("ERR", e, flush=True)

def in_child(fn):
    pid = os.fork()
    if pid == 0:
        report(fn)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    print("STATUS", status, flush=True)

def work(path):
    return fluree.connect(path).ledger("people").query(Q).values()

def work_elsewhere(path):
    try:
        work(path)
    except fluree.DatabaseInUseError:
        pass
    else:
        raise AssertionError("opened a directory the parent holds")
    return fluree.connect(tempfile.mkdtemp()).create("people").query(Q).values()

def started(path):
    conn = fluree.connect(path)
    people = conn.create("people")
    people.insert({"@context": {"ex": EX}, "@id": "ex:a", "ex:name": "A"})
    return conn, people
"""


def run(tmp_path, body: str) -> list[str]:
    main = textwrap.indent("path = tempfile.mkdtemp()\n" + textwrap.dedent(body), "    ")
    script = tmp_path / "case.py"
    script.write_text(SETUP + '\nif __name__ == "__main__":\n' + main)
    done = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr
    # A child that dies leaves its last words here, shown when a case fails.
    sys.stderr.write(done.stderr)
    return done.stdout.splitlines()


def test_a_child_forked_before_the_engine_started_can_use_it(tmp_path):
    lines = run(tmp_path, """
        in_child(lambda: fluree.connect(path).create("people").query(Q).values())
    """)
    assert lines == ["OK []", "STATUS 0"]


def test_inherited_objects_are_refused(tmp_path):
    lines = run(tmp_path, """
        conn, people = started(path)
        snapshot = people.snapshot()
        in_child(lambda: people.query(Q).values())
        in_child(lambda: snapshot.query(Q).values())
        in_child(lambda: conn.ledgers())
        print("PARENT", people.query(Q).values())
    """)
    assert lines[0].startswith("ERR") and "parent of a forked process" in lines[0]
    assert lines[2].startswith("ERR") and "parent of a forked process" in lines[2]
    assert lines[4].startswith("ERR")
    assert lines[1] == lines[3] == lines[5] == "STATUS 0"
    assert lines[6] == "PARENT [['A']]"


def test_a_child_exits_cleanly_holding_inherited_objects(tmp_path):
    lines = run(tmp_path, """
        conn, people = started(path)
        pid = os.fork()
        if pid == 0:
            sys.exit(0)  # finalizes the inherited connection and ledger
        _, status = os.waitpid(pid, 0)
        print("STATUS", status)
    """)
    assert lines == ["STATUS 0"]


def test_a_child_forked_after_the_engine_started_refuses_it(tmp_path):
    # Its lock state names the parent's threads, which the child lacks; a
    # spawned process starts clean, though not on the directory the parent
    # holds.
    lines = run(tmp_path, """
        conn, people = started(path)
        in_child(lambda: work(path))
        with multiprocessing.get_context("spawn").Pool(1) as pool:
            print("SPAWN", pool.map(work_elsewhere, [path]))
        print("PARENT", people.query(Q).values())
    """)
    assert lines[0].startswith("ERR") and "forked after Fluree started" in lines[0] and "'spawn'" in lines[0]
    assert lines[1:] == ["STATUS 0", "SPAWN [[]]", "PARENT [['A']]"]


def test_a_forked_child_does_not_keep_the_directory_locked(tmp_path):
    # A child forked while the directory is open shares the lock's open file,
    # yet the directory is free once its parent exits without closing it, or
    # closes it.
    lines = run(tmp_path, """
        started_r, started_w = os.pipe()
        release_r, release_w = os.pipe()

        def fork_idle_child():
            if os.fork() == 0:
                os.close(release_w)
                os.write(started_w, b"s")
                os.read(release_r, 1)
                os._exit(0)
            os.read(started_r, 1)

        holder = os.fork()
        if holder == 0:
            conn, people = started(path)
            fork_idle_child()
            os._exit(0)  # holding the directory, without closing it
        os.waitpid(holder, 0)
        report(lambda: fluree.connect(path).ledgers())

        conn = fluree.connect(path)
        fork_idle_child()
        conn.close()
        report(lambda: fluree.connect(path).ledgers())
        os.close(release_w)
    """)
    assert lines == ["OK ['people:main']", "OK ['people:main']"]
