#!/usr/bin/env python3
"""Official-driver smoke test for the Fluree Bolt listener.

The Rust integration tests (`bolt_integration.rs`) cover the protocol with
our own codec; this script is the cross-implementation compatibility bar —
the official `neo4j` driver probes things ad-hoc clients don't (`server`
agent parsing, `qid` handling, summary metadata shapes, routing fallbacks).

Run manually:

    # 1. Build and start a server with the bolt feature:
    cargo run -p fluree-db-server --features bolt -- \
        --storage-path /tmp/fluree-bolt-smoke \
        --bolt-listen-addr 127.0.0.1:7687

    # 2. Create a ledger and seed it:
    curl -s -X POST localhost:8090/v1/fluree/create \
        -H content-type:application/json -d '{"ledger": "boltsmoke"}'
    curl -s -X POST localhost:8090/v1/fluree/insert/boltsmoke \
        -H content-type:application/json -d '{
          "@context": {"ex": "http://example.org/"},
          "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 45}
          ]}'

    # 3. pip install neo4j && python3 bolt_driver_smoke.py
"""

import sys

from neo4j import GraphDatabase

URI = "bolt://127.0.0.1:7687"
DB = "boltsmoke"

failures = []


def check(name, condition, detail=""):
    status = "ok" if condition else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


def main():
    with GraphDatabase.driver(URI, auth=None) as driver:
        driver.verify_connectivity()
        print("connected:", URI)

        with driver.session(database=DB) as session:
            # Plain read.
            result = session.run(
                "MATCH (n:Person) RETURN n.name AS name ORDER BY name"
            )
            names = [r["name"] for r in result]
            summary = result.consume()
            check("read rows", names == ["Alice", "Bob"], f"got {names}")
            check("summary database", summary.database == DB, summary.database)
            check(
                "server agent",
                (summary.server.agent or "").startswith("Neo4j/"),
                summary.server.agent,
            )

            # Parameterized read.
            result = session.run(
                "MATCH (n:Person) WHERE n.age > $min RETURN n.name AS name",
                min=40,
            )
            names = [r["name"] for r in result]
            check("param read", names == ["Bob"], f"got {names}")

            # Autocommit write + read-back.
            result = session.run(
                "CREATE (n:Person {name: $name, age: 27})", name="Carol"
            )
            summary = result.consume()
            check(
                "write counters",
                summary.counters.contains_updates,
                str(summary.counters),
            )
            count = session.run(
                "MATCH (n:Person) RETURN count(n) AS c"
            ).single()["c"]
            check("write visible", count == 3, f"count={count}")

            # Error surface: bad syntax raises a ClientError.
            try:
                session.run("MATCH (n RETURN n").consume()
                check("syntax error raised", False, "no exception")
            except Exception as e:  # neo4j.exceptions.ClientError
                check(
                    "syntax error raised",
                    type(e).__name__ in ("ClientError", "CypherSyntaxError"),
                    f"{type(e).__name__}: {e}",
                )

            # Session still usable after the failure (driver auto-RESET).
            count = session.run(
                "MATCH (n:Person) RETURN count(n) AS c"
            ).single()["c"]
            check("session recovers after failure", count == 3, f"count={count}")

            # Explicit transaction: write + read-your-writes, then commit.
            with session.begin_transaction() as tx:
                tx.run("CREATE (n:Person {name: $name, age: 31})", name="Dave").consume()
                in_tx = tx.run(
                    "MATCH (n:Person) RETURN count(n) AS c"
                ).single()["c"]
                check("tx read-your-writes", in_tx == 4, f"count={in_tx}")
                tx.commit()
            count = session.run(
                "MATCH (n:Person) RETURN count(n) AS c"
            ).single()["c"]
            check("tx commit visible", count == 4, f"count={count}")

            # Managed transaction function (the retryable-closure API).
            def add_person(tx):
                tx.run("CREATE (n:Person {name: 'Eve', age: 29})").consume()
                return tx.run("MATCH (n:Person) RETURN count(n) AS c").single()["c"]

            managed = session.execute_write(add_person)
            check("managed tx function", managed == 5, f"count={managed}")

            # Rollback discards.
            with session.begin_transaction() as tx:
                tx.run("CREATE (n:Person {name: 'Ghost'})").consume()
                tx.rollback()
            count = session.run(
                "MATCH (n:Person) RETURN count(n) AS c"
            ).single()["c"]
            check("tx rollback discards", count == 5, f"count={count}")

            # RETURN n gives a typed Node with labels + properties.
            record = session.run(
                'MATCH (n:Person {name: "Alice"}) RETURN n'
            ).single()
            node = record["n"]
            check("node labels", "Person" in node.labels, str(node.labels))
            check("node properties", node.get("age") == 30, str(dict(node)))

    print()
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
        sys.exit(1)
    print("all driver smoke checks passed")


if __name__ == "__main__":
    main()
