# /// script
# requires-python = ">=3.12"
# dependencies = ["delta-spark==4.0.0", "pyspark==4.0.0", "pyarrow==21.0.0"]
# ///
"""Generate advanced, synthetic Delta fixtures with Spark; destination must be new.

Requires Java 17 or 21. Downloads the pinned Delta JVM package on first use.
Only the disposable history_loss table created here has a retired file removed.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pyarrow.parquet as pq
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def actions(path):
    return [json.loads(line) for log in sorted((path / "_delta_log").glob("*.json"))
            for line in log.read_text().splitlines()]


def generate(root):
    root.mkdir(parents=True, exist_ok=False)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (SparkSession.builder.master("local[2]").appName("fluree-delta-fixture")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.hadoop.parquet.block.size", "8192")
               .config("spark.hadoop.parquet.page.size", "1024")
               .config("spark.hadoop.parquet.enable.dictionary", "false")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", str(root / "spark-warehouse"))
               .config("spark.databricks.delta.snapshotPartitions", "2"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    tables = {}

    def sql_table(name):
        return f"delta.`{root / name}`"

    def save(name, expected, filter_column="amount"):
        table = tables.setdefault(name, {"key": "id", "versions": []})
        version = spark.sql(f"DESCRIBE HISTORY {sql_table(name)} LIMIT 1").first().version
        frame = spark.read.format("delta").option("versionAsOf", version).load(str(root / name))
        rows = sorted([r.asDict() for r in frame.collect()], key=lambda r: r["id"])
        assert rows == expected, (name, version, "Spark differs from independent oracle")
        entry = {"version": version, "columns": frame.columns, "rows": copy.deepcopy(expected)}
        if filter_column:
            entry["filter"] = {"column": filter_column, "minimum": 20000,
                               "columns": ["id", filter_column]}
        table["versions"].append(entry)

    try:
        rows = [{"id": i, "amount": i * 10, "region": str(i % 2)} for i in range(4096)]
        dv = root / "deletion_vectors"
        (spark.createDataFrame(rows, "id long, amount long, region string").coalesce(1)
         .write.format("delta").partitionBy("region")
         .option("delta.enableDeletionVectors", "true")
         .option("compression", "uncompressed").save(str(dv)))
        save("deletion_vectors", rows)
        initial = {a["add"]["path"] for a in actions(dv) if "add" in a}
        groups = {p: pq.ParquetFile(dv / unquote(p)).metadata.num_row_groups for p in initial}
        assert all(n > 1 for n in groups.values()), groups
        # Delete across the file, including positions near row-group boundaries.
        removed = {0, 1, 1023, 1024, 2047, 2048, 3071, 4094, 4095}
        spark.sql(f"DELETE FROM {sql_table('deletion_vectors')} WHERE id IN ({','.join(map(str, sorted(removed)))})")
        save("deletion_vectors", [r for r in rows if r["id"] not in removed])
        vector_adds = [a["add"] for a in actions(dv) if a.get("add", {}).get("deletionVector")]
        assert {a["path"] for a in vector_adds} == initial, "delete must reuse both data files with DVs"
        assert sum(a["deletionVector"]["cardinality"] for a in vector_adds) == len(removed)

        mapped = root / "column_mapping"
        small = [{"id": 1, "amount": 100}, {"id": 2, "amount": 30000}]
        (spark.createDataFrame(small, "id long, amount long").coalesce(1).write.format("delta")
         .option("delta.columnMapping.mode", "name").save(str(mapped)))
        save("column_mapping", small)
        spark.sql(f"ALTER TABLE {sql_table('column_mapping')} RENAME COLUMN amount TO renamed_amount")
        save("column_mapping", [{"id": r["id"], "renamed_amount": r["amount"]} for r in small], "renamed_amount")
        spark.sql(f"ALTER TABLE {sql_table('column_mapping')} DROP COLUMN renamed_amount")
        save("column_mapping", [{"id": r["id"]} for r in small], None)
        spark.sql(f"ALTER TABLE {sql_table('column_mapping')} ADD COLUMNS (amount BIGINT)")
        save("column_mapping", [{"id": r["id"], "amount": None} for r in small])
        adds = [a["add"] for a in actions(mapped) if "add" in a]
        assert len(adds) == 1, "rename/drop/re-add must not rewrite Parquet"
        physical = pq.ParquetFile(mapped / unquote(adds[0]["path"])).schema_arrow.names
        assert "id" not in physical and "amount" not in physical, physical

        # In-commit timestamps make time travel independent of file mtimes,
        # which a copied or checked-out table does not preserve.
        timed = root / "in_commit_time"
        first = [{"id": 1, "amount": 100}]
        (spark.createDataFrame(first, "id long, amount long").coalesce(1).write.format("delta")
         .option("delta.enableInCommitTimestamps", "true").save(str(timed)))
        save("in_commit_time", first)
        second = first + [{"id": 2, "amount": 30000}]
        (spark.createDataFrame(second[1:], "id long, amount long").coalesce(1)
         .write.format("delta").mode("append").save(str(timed)))
        save("in_commit_time", second)
        spark.sql(f"DELETE FROM {sql_table('in_commit_time')} WHERE id = 1")
        save("in_commit_time", second[1:])
        stamps = [a["commitInfo"]["inCommitTimestamp"] for a in actions(timed) if "commitInfo" in a]
        assert len(stamps) == 3 and stamps == sorted(set(stamps)), stamps
        tables["in_commit_time"]["commit_timestamps_ms"] = stamps

        loss = root / "history_loss"
        spark.createDataFrame(small, "id long, amount long").coalesce(1).write.format("delta").save(str(loss))
        retired = [a["add"]["path"] for a in actions(loss) if "add" in a]
        current = [{"id": 3, "amount": 50000}]
        (spark.createDataFrame(current, "id long, amount long").coalesce(1)
         .write.format("delta").mode("overwrite").save(str(loss)))
        save("history_loss", current)
        removed_paths = {a["remove"]["path"] for a in actions(loss) if "remove" in a}
        assert len(retired) == 1 and retired[0] in removed_paths
        victim = (loss / unquote(retired[0])).resolve()
        assert victim.is_relative_to(loss) and victim.suffix == ".parquet"
        victim.unlink()  # Only a retired synthetic file created by this invocation.
        tables["history_loss"]["missing_data_versions"] = [{"version": 0, "file": retired[0]}]
        # Spark must also fail rather than substitute the retained current version.
        try:
            spark.read.format("delta").option("versionAsOf", 0).load(str(loss)).collect()
        except Exception as error:
            assert "FAILED_READ_FILE.FILE_NOT_EXIST" in str(error) or "FileNotFoundException" in str(error), str(error)
        else:
            raise AssertionError("Spark silently read missing historical data")

        # Keep fixtures portable to an isolated S3 prefix. Only history_loss's
        # explicitly recorded retired file may be missing.
        for name in tables:
            for action in actions(root / name):
                for kind in ("add", "remove"):
                    if kind not in action:
                        continue
                    entry = action[kind]
                    reference = unquote(entry["path"])
                    assert not urlsplit(reference).scheme
                    assert not reference.startswith("/") and ".." not in Path(reference).parts
                    assert (root / name / reference).is_file() or (name == "history_loss" and reference == retired[0])
                    if entry.get("deletionVector"):
                        assert entry["deletionVector"]["storageType"] in ("u", "i"), "absolute DV paths cannot be copied"

        inventory = {str(p.relative_to(root)): {"bytes": p.stat().st_size,
                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                     for p in sorted(root.rglob("*")) if p.is_file()}
        manifest = {"fixture_version": 2, "writer": "delta-spark 4.0.0 / pyspark 4.0.0",
                    "tables": tables, "files": inventory,
                    "evidence": {"dv_row_groups": groups, "dv_deleted_rows": len(removed),
                                 "mapped_physical_columns": physical},
                    "limitations": ["synthetic correctness fixture, not a benchmark",
                                    "history_loss intentionally lacks one retired data file"]}
        (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"fixture": str(root), "snapshots": sum(len(t["versions"]) for t in tables.values()),
                          "evidence": manifest["evidence"], "status": "passed"}))
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    generate(args.destination.resolve())
