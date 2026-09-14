# /// script
# requires-python = ">=3.12"
# dependencies = ["deltalake==1.6.3", "pyarrow==21.0.0"]
# ///
"""Generate synthetic Delta history and an independently constructed row oracle.

Run: uv run --python 3.12 scripts/delta-spike/fixture.py /tmp/delta-fixture-<run>
The destination must not exist. Nothing is uploaded or vacuumed by this script.
"""

import argparse
import copy
import hashlib
import json
from pathlib import Path
from urllib.parse import unquote, urlsplit

import deltalake
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def ordered(rows, key):
    return sorted(rows, key=lambda row: row[key])


def verify(root, specification):
    """Check every retained version, including schema and exact filtered results."""
    checks = 0
    for name, table in specification.items():
        for expected in table["versions"]:
            dt = DeltaTable(f"{str(root).rstrip('/')}/{name}", version=expected["version"])
            actual = dt.to_pyarrow_table()
            assert actual.column_names == expected["columns"], (name, "schema")
            assert ordered(actual.to_pylist(), table["key"]) == expected["rows"], (
                name, expected["version"], "rows"
            )
            if name == "fact_order":
                projected = dt.to_pyarrow_table(
                    columns=["order_id", "amount"], filters=[("amount", ">=", 300)]
                ).to_pylist()
                wanted = [
                    {"order_id": r["order_id"], "amount": r["amount"]}
                    for r in expected["rows"]
                    if r["amount"] is not None and r["amount"] >= 300
                ]
                assert ordered(projected, "order_id") == wanted
                assert dt.to_pyarrow_table(filters=[("order_id", "<", 0)]).num_rows == 0
            checks += 1
    return checks


def generate(root):
    root.mkdir(parents=True, exist_ok=False)
    specification = {}

    def save(name, key, rows, columns):
        table = specification.setdefault(name, {"key": key, "versions": []})
        table["versions"].append({
            "version": DeltaTable(str(root / name)).version(),
            "columns": columns,
            "rows": ordered(copy.deepcopy(rows), key),
        })

    stores = [
        {"store_id": 1, "name": "East shop"},
        {"store_id": 2, "name": "West shop"},
        {"store_id": 3, "name": "Unassigned shop"},
    ]
    write_deltalake(str(root / "dim_store"), pa.Table.from_pylist(stores))
    save("dim_store", "store_id", stores, ["store_id", "name"])

    schema = pa.schema([
        ("order_id", pa.int64()), ("store_id", pa.int64()),
        ("amount", pa.int64()), ("region", pa.string()),
    ])
    rows = [
        {"order_id": 1, "store_id": 1, "amount": 100, "region": "east"},
        {"order_id": 2, "store_id": 2, "amount": 200, "region": "west"},
        {"order_id": 3, "store_id": 1, "amount": None, "region": "east"},
        {"order_id": 4, "store_id": 3, "amount": 400, "region": None},
    ]
    path = str(root / "fact_order")
    write_deltalake(path, pa.Table.from_pylist(rows, schema=schema), partition_by=["region"])
    save("fact_order", "order_id", rows, schema.names)

    appended = [
        {"order_id": 5, "store_id": 2, "amount": 500, "region": "west"},
        {"order_id": 6, "store_id": 1, "amount": 600, "region": "east"},
    ]
    write_deltalake(path, pa.Table.from_pylist(appended, schema=schema), mode="append")
    rows.extend(appended)
    save("fact_order", "order_id", rows, schema.names)

    DeltaTable(path).update(updates={"amount": "250"}, predicate="order_id = 2")
    rows[1]["amount"] = 250
    save("fact_order", "order_id", rows, schema.names)

    DeltaTable(path).delete(predicate="order_id = 1")
    rows = [r for r in rows if r["order_id"] != 1]
    save("fact_order", "order_id", rows, schema.names)

    schema = schema.append(pa.field("note", pa.string()))
    addition = {"order_id": 7, "store_id": 1, "amount": 700, "region": "east", "note": "new column"}
    write_deltalake(path, pa.Table.from_pylist([addition], schema=schema), mode="append", schema_mode="merge")
    rows = [dict(r, note=None) for r in rows] + [addition]
    save("fact_order", "order_id", rows, schema.names)

    verify(root, specification)
    DeltaTable(path).create_checkpoint()
    checks = verify(root, specification)

    # Copying a quiescent local fixture to S3 is valid only with contained,
    # relative data-file references. Preserve all removed files for history.
    for log in root.glob("*/_delta_log/*.json"):
        for line in log.read_text().splitlines():
            action = json.loads(line)
            for kind in ("add", "remove"):
                if kind in action:
                    reference = unquote(action[kind]["path"])
                    assert not urlsplit(reference).scheme
                    assert not reference.startswith("/") and ".." not in Path(reference).parts
                    assert (log.parent.parent / reference).is_file(), reference
                    assert not action[kind].get("deletionVector"), "DV requires a separate fixture"

    inventory = {
        str(p.relative_to(root)): {
            "bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()
        }
        for p in sorted(root.rglob("*")) if p.is_file()
    }
    manifest = {
        "fixture_version": 1, "writer": f"deltalake-python {deltalake.__version__}",
        "tables": specification, "files": inventory,
        "limitations": ["copy-on-write deletes; no deletion vectors or column mapping", "synthetic correctness fixture, not a benchmark"],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"fixture": str(root), "verified_snapshots": checks, "files": len(inventory), "bytes": sum(f["bytes"] for f in inventory.values())}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", help="new local fixture directory, or verification table root")
    parser.add_argument("--verify-manifest", type=Path, help="verify existing tables instead of generating them")
    args = parser.parse_args()
    if args.verify_manifest:
        manifest = json.loads(args.verify_manifest.read_text())
        print(json.dumps({"reader": f"deltalake-python {deltalake.__version__}", "verified_snapshots": verify(args.destination, manifest["tables"]), "status": "passed"}))
    else:
        generate(Path(args.destination).resolve())
