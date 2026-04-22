# fluree iceberg

Manage Apache Iceberg table connections.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `map` | Map an Iceberg table as a graph source |
| `list` | List Iceberg-family graph sources (`Iceberg` and `R2RML`) |
| `info` | Show details for an Iceberg-family graph source |
| `drop` | Drop an Iceberg-family graph source |

## fluree iceberg map

Map an Iceberg table as a queryable graph source.

### Usage

```bash
fluree iceberg map <NAME> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Graph source name (e.g., "warehouse-orders") |

### Options

**Catalog mode:**

| Option | Description |
|--------|-------------|
| `--mode <MODE>` | Catalog mode: `rest` (default) or `direct` |

**REST catalog mode options:**

| Option | Description |
|--------|-------------|
| `--catalog-uri <URI>` | REST catalog URI (required for rest mode) |
| `--table <ID>` | Table identifier in `namespace.table` format (required if not specified in R2RML mapping) |
| `--warehouse <NAME>` | Warehouse identifier |
| `--no-vended-credentials` | Disable vended credentials (enabled by default) |

**Direct S3 mode options:**

| Option | Description |
|--------|-------------|
| `--table-location <URI>` | S3 table location (required for direct mode, e.g., `s3://bucket/warehouse/ns/table`) |

**R2RML mapping:**

| Option | Description |
|--------|-------------|
| `--r2rml <PATH>` | R2RML mapping file (Turtle format, required). Defines how table rows become RDF triples. Table references come from the mapping's `rr:tableName` entries. |
| `--r2rml-type <TYPE>` | Mapping media type (e.g., `text/turtle`); inferred from extension if omitted |

**Authentication:**

| Option | Description |
|--------|-------------|
| `--auth-bearer <TOKEN>` | Bearer token for REST catalog authentication |
| `--oauth2-token-url <URL>` | OAuth2 token URL for client credentials auth |
| `--oauth2-client-id <ID>` | OAuth2 client ID |
| `--oauth2-client-secret <SECRET>` | OAuth2 client secret |

**S3 overrides:**

| Option | Description |
|--------|-------------|
| `--s3-region <REGION>` | S3 region override |
| `--s3-endpoint <URL>` | S3 endpoint override (for MinIO, LocalStack) |
| `--s3-path-style` | Use path-style S3 URLs |

**Other:**

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Execute against a remote server (by remote name) |
| `--branch <NAME>` | Branch name (defaults to "main") |

### Description

Maps an Apache Iceberg table as a graph source that can be queried using SPARQL or JSON-LD queries. The table is accessed read-only; Fluree does not modify the Iceberg table.

An R2RML mapping (`--r2rml`) is required to define how Iceberg table rows are transformed into RDF triples.

Two catalog modes are supported:

- **REST mode** (default): Connects to an Iceberg REST catalog (e.g., Apache Polaris) to discover table metadata. Supports vended credentials and warehouse selection.
- **Direct S3 mode**: Reads table metadata directly from S3 by resolving `version-hint.text` in the table's `metadata/` directory. No catalog server required.

### Examples

```bash
# REST catalog with R2RML mapping
fluree iceberg map airlines \
  --catalog-uri https://polaris.example.com/api/catalog \
  --r2rml mappings/airlines.ttl \
  --auth-bearer $POLARIS_TOKEN

# REST catalog with explicit table and warehouse
fluree iceberg map warehouse-orders \
  --catalog-uri https://polaris.example.com/api/catalog \
  --table sales.orders \
  --r2rml mappings/orders.ttl \
  --auth-bearer $POLARIS_TOKEN \
  --warehouse my-warehouse

# Direct S3 (no catalog server)
fluree iceberg map execution-log \
  --mode direct \
  --table-location s3://my-bucket/warehouse/logs/execution_log \
  --r2rml mappings/execution_log.ttl \
  --s3-region us-east-1

# OAuth2 authentication
fluree iceberg map orders \
  --catalog-uri https://polaris.example.com/api/catalog \
  --table sales.orders \
  --r2rml mappings/orders.ttl \
  --oauth2-token-url https://auth.example.com/token \
  --oauth2-client-id my-client \
  --oauth2-client-secret $CLIENT_SECRET

# Create the graph source on a remote Fluree server
fluree iceberg map warehouse-orders \
  --remote origin \
  --catalog-uri https://polaris.example.com/api/catalog \
  --table sales.orders \
  --r2rml mappings/orders.ttl
```

### Output

```
Mapped Iceberg table as R2RML graph source 'airlines:main'
  Table:       openflights.airlines
  Catalog:     https://polaris.example.com/api/catalog
  R2RML:       mappings/airlines.ttl
  TriplesMaps: 3
  Connection:  verified
  Mapping:     validated
```

### After Mapping

Once mapped, the graph source appears in standard commands:

```bash
# Listed alongside ledgers
fluree list

# Inspect configuration
fluree info warehouse-orders

# Query via SPARQL GRAPH pattern
fluree query mydb 'SELECT ?id ?total FROM <mydb:main> WHERE { GRAPH <warehouse-orders:main> { ?o ex:id ?id ; ex:total ?total } }'

# Remove the mapping
fluree drop warehouse-orders --force
```

### Feature Flag

Requires the `iceberg` feature flag. Without it, the command returns:
```
error: Iceberg support not compiled. Rebuild with `--features iceberg`.
```

## See Also

- [Iceberg / Parquet](../graph-sources/iceberg.md) - Iceberg integration details
- [R2RML](../graph-sources/r2rml.md) - R2RML mapping reference
- [list](list.md) - List ledgers and graph sources
- [info](info.md) - Show graph source details
- [drop](drop.md) - Remove a graph source

## fluree iceberg list

List Iceberg-family graph sources (`Iceberg` and `R2RML` types).

### Usage

```bash
fluree iceberg list [--remote <NAME>]
```

### Examples

```bash
# Local
fluree iceberg list

# Remote
fluree iceberg list --remote origin
```

## fluree iceberg info

Show details for an Iceberg-family graph source.

### Usage

```bash
fluree iceberg info <NAME> [--remote <NAME>]
```

### Examples

```bash
# Local
fluree iceberg info warehouse-orders

# Remote
fluree iceberg info warehouse-orders --remote origin
```

## fluree iceberg drop

Drop an Iceberg-family graph source. This command only targets Iceberg/R2RML graph sources; it does not fall back to dropping ledgers of the same name.

### Usage

```bash
fluree iceberg drop <NAME> --force [--remote <NAME>]
```

### Examples

```bash
# Local
fluree iceberg drop warehouse-orders --force

# Remote
fluree iceberg drop warehouse-orders --force --remote origin
```
