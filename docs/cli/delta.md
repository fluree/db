# fluree delta

Manage Delta Lake graph sources — R2RML mappings over Delta tables read in
place. See [Delta Lake tables](../graph-sources/delta.md).

Reading Delta tables needs a build with the `delta` feature. A CLI without it
can still map and query Delta sources on a server that has it (`--remote`).

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `map` | Map Delta tables as a graph source |
| `list` | List mapped graph sources (Delta, SQL, Iceberg and R2RML) |
| `info` | Show details for a mapped graph source |
| `drop` | Drop a mapped graph source |

`list`, `info` and `drop` are shared with [`fluree iceberg`](iceberg.md): both
commands operate on the same family of mapped sources.

## fluree delta map

### Usage

```bash
fluree delta map <NAME> --r2rml <PATH> (--root <LOCATION> | --table <NAME=LOCATION>...) [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Graph source name (e.g., "sales") |

### Options

**Table locations:**

| Option | Description |
|--------|-------------|
| `--root <LOCATION>` | Directory the mapping's table names resolve beneath. Each `rr:tableName` becomes a path under it, with dots as separators: `dbo.orders` → `<root>/dbo/orders` |
| `--table <NAME=LOCATION>` | Explicit location for one table (repeatable). Wins over `--root`; use it for a table outside the root or whose directory does not follow its name |

A location is `s3://bucket/prefix`, or a local path (`file:///…` or absolute)
under [`FLUREE_ICEBERG_LOCAL_ROOTS`](../graph-sources/iceberg.md#enabling-local-tables).
Give `--root`, one or more `--table`, or both.

**Storage:**

| Option | Description |
|--------|-------------|
| `--s3-region <REGION>` | S3 region override |
| `--s3-endpoint <URL>` | S3 endpoint override (MinIO, LocalStack) |
| `--s3-path-style` | Use path-style S3 URLs |

S3 credentials come from the environment of the process that reads the tables
(`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, instance or task roles); none are stored
on the graph source.

**R2RML mapping:**

| Option | Description |
|--------|-------------|
| `--r2rml <PATH>` | Mapping file (required). Each `rr:tableName` names a Delta table; `rr:sqlQuery` is not supported |
| `--r2rml-type <TYPE>` | Mapping media type (e.g., `text/turtle`); inferred from extension if omitted |

**General:**

| Option | Description |
|--------|-------------|
| `--branch <BRANCH>` | Branch name (defaults to `main`) |
| `--model <LEDGER>` | Model ledger (`name:branch`) whose default graph supplies the source's view policies and class/property hierarchy. Must exist. See [Access policy](../graph-sources/iceberg.md#access-policy) |
| `--default-allow <BOOL>` | Fallback for governed requests that match no policy; `true` keeps the source readable under authentication without a model (unset: deny) |
| `--remote <NAME>` | Execute against a remote server |

### Examples

```bash
# Every mapped table lives under one root
fluree delta map sales --root s3://lake/Tables --r2rml mappings/sales.ttl

# One table lives elsewhere
fluree delta map sales \
  --root s3://lake/Tables \
  --table orders=s3://lake/raw/orders_v2 \
  --r2rml mappings/sales.ttl
```

### Output

```
Mapped Delta tables as graph source 'sales:main'
  R2RML:       bafy…
  TriplesMaps: 3
  Tables:      2
    dbo.customers (version 12)
    dbo.orders (version 847)
```

Each mapped table is opened once at registration, checked for the columns its
maps reference, and its current Delta version reported. `(not readable yet)`
and a `Warning:` line mean the table could not be opened — it may not exist
yet, or this process may lack the credentials the querying process has — or
that it lacks a mapped column. The source is registered either way and the
first query reports the underlying error. A table name that cannot be
placed (no `--table` entry and no `--root`) is an error.

## Querying a past version

```bash
fluree query sales --at snapshot:840 --sparql 'SELECT …'
fluree query sales --at time:2026-03-01T00:00:00Z --sparql 'SELECT …'
```

`snapshot:<n>` is the Delta table version. See
[Time travel](../graph-sources/delta.md#time-travel).

## fluree delta list / info / drop

```bash
fluree delta list
fluree delta info sales
fluree delta drop sales --force
```

Behave exactly as the [`fluree iceberg`](iceberg.md) equivalents; Delta sources
show the type `Delta`.
