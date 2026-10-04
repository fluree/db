import asyncio
import time

import pytest

import fluree
import fluree.aio

EX = "http://example.org/"
CONTEXT = {"ex": EX}
NAMES = f"PREFIX ex: <{EX}> SELECT ?name WHERE {{ ?s ex:name ?name }} ORDER BY ?name"
NUMS = [{"@id": f"ex:s{i}", "ex:n": i} for i in range(300)]
ALL_NUMS = f"PREFIX ex: <{EX}> SELECT ?s ?n WHERE {{ ?s ex:n ?n }}"
# A three-way cross product over 300 subjects: 27 million rows to count.
CROSS = f"PREFIX ex: <{EX}> SELECT (COUNT(*) AS ?count) WHERE {{ ?a ex:n ?x . ?b ex:n ?y . ?c ex:n ?z }}"
CROSS_ROWS = f"PREFIX ex: <{EX}> SELECT ?x ?y ?z WHERE {{ ?a ex:n ?x . ?b ex:n ?y . ?c ex:n ?z }}"


def person(name):
    return {"@context": CONTEXT, "@id": f"ex:{name.lower()}", "ex:name": name}


def run(coroutine, within=30):
    """Run ``coroutine`` and require it, and every engine call it started, to
    finish within ``within`` seconds: ``asyncio.run`` waits for the worker
    threads, so a query left running past a cancellation shows up here."""
    started = time.monotonic()
    result = asyncio.run(coroutine)
    assert time.monotonic() - started < within
    return result


def test_the_api_end_to_end():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            people = await conn.create("people")
            assert await conn.exists("people")
            commit = await people.insert(person("Alice"), message="first")
            assert commit.t == 1
            assert [n for (n,) in await people.query(NAMES)] == ["Alice"]
            assert [c.message for c in await people.log()] == ["first"]

            async with people.transaction(message="batch") as txn:
                await txn.insert(person("Bob"))
                assert len(await txn.query(NAMES)) == 2
            assert txn.committed.t == 2

            past = await people.at(t=1)
            assert past.t == 1 and len(await past.query(NAMES)) == 1

            dev = await people.branch("dev")
            await dev.insert(person("Carol"))
            assert (await people.merge(dev)).fast_forward
            assert [b.name for b in await people.branches()] == ["dev", "main"]

            names = [row.name async for row in people.stream(NAMES)]
            assert names == ["Alice", "Bob", "Carol"]

            await people.sync({"@context": CONTEXT, "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]})
            assert [n for (n,) in await people.query(NAMES)] == ["Dave"]

            assert (await people.verify()).healthy
            assert (await people.validate()).conforms
            assert (await people.index_status()).commit_t == (await people.log())[0].t

            other = await conn.ledger("people:dev")
            assert other.id == "people:dev"
            assert await conn.ledgers() == ["people:dev", "people:main"]
        return True

    assert run(main())


def test_awaiting_connect_and_transaction():
    async def main():
        conn = await fluree.aio.connect(":memory:")
        try:
            ledger = await conn.create("people")
            txn = await ledger.transaction()
            await txn.insert(person("Alice"))
            await txn.rollback()
            assert len(await ledger.query(NAMES)) == 0
        finally:
            await conn.close()

    run(main())


def test_an_exception_rolls_the_transaction_back():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("people")
            with pytest.raises(RuntimeError):
                async with ledger.transaction() as txn:
                    await txn.insert(person("Alice"))
                    raise RuntimeError
            assert txn.committed is None
            assert len(await ledger.query(NAMES)) == 0

    run(main())


def test_queries_run_concurrently():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("nums")
            await ledger.insert({"@context": CONTEXT, "@graph": NUMS})
            results = await asyncio.gather(*(ledger.query(ALL_NUMS) for _ in range(20)))
            assert all(len(rows) == 300 for rows in results)

    run(main())


def test_cancelling_a_task_stops_its_query():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("nums")
            await ledger.insert({"@context": CONTEXT, "@graph": NUMS})
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ledger.query(CROSS), 0.3)
            # The engine is free again at once.
            assert len(await ledger.query(ALL_NUMS)) == 300

    # Uncancelled, the cross product would hold a worker thread for a minute.
    run(main(), within=10)


def test_cancelling_a_task_stops_its_stream():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("nums")
            await ledger.insert({"@context": CONTEXT, "@graph": NUMS})

            async def read_all():
                return [row async for row in ledger.stream(CROSS_ROWS, batch_size=10)]

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(read_all(), 0.3)

            async with ledger.stream(ALL_NUMS) as rows:
                async for _ in rows:
                    break
            assert len(await ledger.query(ALL_NUMS)) == 300

    run(main(), within=10)


def test_errors_are_the_sync_api_errors():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            with pytest.raises(fluree.NotFoundError):
                await conn.ledger("missing")
            ledger = await conn.create("people")
            with pytest.raises(fluree.InvalidRequestError):
                await ledger.query("SELECT nonsense")

    run(main())


def test_transact_reruns_on_conflict():
    age_query = "PREFIX ex: <http://example.org/> SELECT ?age WHERE { ex:alice ex:age ?age }"

    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("people")
            await ledger.insert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": 30})
            attempts = []

            async def birthday(txn):
                (age,) = (await txn.query(age_query))[0]
                attempts.append(age)
                if len(attempts) == 1:
                    await ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": 50})
                await txn.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": age + 1})
                return age + 1

            assert await ledger.transact(birthday) == 51
            assert attempts == [30, 50]

    run(main())
