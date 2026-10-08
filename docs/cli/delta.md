# fluree delta

Manage Delta Lake graph sources — R2RML mappings over Delta tables read in
place. See [Delta Lake tables](../graph-sources/delta.md).

Delta support is part of the default build (the `delta` feature). A CLI built
without it can still map and query Delta sources on a server that has it
(`--remote`).

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `map` | Map Delta tables as a graph source |
| `browse` | List a Unity Catalog's catalogs, schemas or tables |
| `preview` | Show a Unity Catalog table's columns and declared keys |
| `verify` | Read a Unity Catalog table with the credentials Unity issues |
| `generate` | Generate an R2RML mapping from Unity Catalog tables |
| `validate` | Check a mapping against the tables `map` would read |
| `list` | List mapped graph sources (Delta, SQL, Iceberg and R2RML) |
| `info` | Show details for a mapped graph source |
| `drop` | Drop a mapped graph source |

`list`, `info` and `drop` are shared with [`fluree iceberg`](iceberg.md): both
commands operate on the same family of mapped sources.

## fluree delta map

### Usage

```bash
fluree delta map <NAME> --r2rml <PATH> (--root <LOCATION> | --unity-uri <URL> | --table <NAME=LOCATION>...) [OPTIONS]
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

A location is `s3://bucket/prefix`,
`abfss://<container>@<account>.dfs.core.windows.net/<path>` (ADLS Gen2),
`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/<path>` (OneLake),
or a local path (`file:///…` or absolute) under
[`FLUREE_ICEBERG_LOCAL_ROOTS`](../graph-sources/iceberg.md#enabling-local-tables).
Give `--root`, one or more `--table`, or both.

**S3:**

| Option | Description |
|--------|-------------|
| `--s3-region <REGION>` | S3 region override |
| `--s3-endpoint <URL>` | S3 endpoint override (MinIO, LocalStack) |
| `--s3-path-style` | Use path-style S3 URLs |

**Azure:**

| Option | Description |
|--------|-------------|
| `--azure-tenant-id <ID>` | Microsoft Entra tenant of a service principal |
| `--azure-client-id <ID>` | Service principal (application) client id |
| `--azure-client-secret-env <VAR>` | Environment variable holding the client secret, read by the process that reads the tables. The secret is not stored |
| `--azure-client-secret <SECRET>` | Literal client secret. Stored with the graph source; prefer the option above |

Give the tenant, the client id and one of the two secret options together, or
none of them to use ambient credentials. See
[Credentials](../graph-sources/delta.md#credentials).

**Unity Catalog (Databricks):**

| Option | Description |
|--------|-------------|
| `--unity-uri <URL>` | Databricks workspace URL. Tables without a `--table` entry are then named in Unity Catalog (`catalog.schema.table`), which says where each lives and issues, and renews, the credentials that read it. Excludes `--root` |
| `--unity-catalog <NAME>` | Completes a mapped table name of fewer than three parts |
| `--unity-schema <NAME>` | Completes a one-part mapped table name |
| `--oauth2-client-id <ID>` | Application id of a Databricks service principal |
| `--oauth2-client-secret-env <VAR>` | Environment variable holding the service principal's OAuth secret, read by the process that reads the tables. The secret is not stored. With `--remote`, the server must list the variable in [`FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS`](../operations/configuration.md#iceberg--r2rml-graph-source-tuning) |
| `--oauth2-client-secret <SECRET>` | Literal OAuth secret. Stored with the graph source; prefer the option above |
| `--auth-bearer-env <VAR>` / `--auth-bearer <TOKEN>` | A Databricks personal access token in place of a service principal, by variable or literal. It does not renew |
| `--oauth2-token-url <URL>` | OAuth2 token URL (default: the workspace's own, `<unity-uri>/oidc/v1/token`) |
| `--oauth2-scope <SCOPE>` | OAuth2 scope (default: `all-apis`) |

Give a service principal's id and secret, or a token. On AWS also give
`--s3-region`: Unity Catalog does not name the bucket's region. See
[Unity Catalog](../graph-sources/delta.md#unity-catalog).

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

# OneLake, reading as a service principal whose secret stays in the environment
fluree delta map fabric-sales \
  --root abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables \
  --azure-tenant-id "$TENANT" --azure-client-id "$APP_ID" \
  --azure-client-secret-env FABRIC_CLIENT_SECRET \
  --r2rml mappings/sales.ttl

# Databricks tables by name, read as a service principal
fluree delta map dbx-sales \
  --unity-uri https://<workspace>.cloud.databricks.com --unity-catalog main \
  --oauth2-client-id "$APP_ID" --oauth2-client-secret-env DATABRICKS_CLIENT_SECRET \
  --s3-region us-east-1 \
  --r2rml mappings/sales.ttl

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

## fluree delta browse / preview / verify / generate / validate

The read-only commands that lead up to `map` on Unity Catalog. None registers
anything. See
[From a catalog to a mapping](../graph-sources/delta.md#from-a-catalog-to-a-mapping).

```bash
fluree delta browse   [--depth schemas|tables]          <UNITY OPTIONS>
fluree delta preview  <TABLE>                           <UNITY OPTIONS>
fluree delta verify   <TABLE> [--s3-region <REGION>]    <UNITY OPTIONS>
fluree delta generate <TABLE>... --base-namespace <IRI> <UNITY OPTIONS> [-o <FILE>]
fluree delta validate --r2rml <PATH>                    <the location options of `map`>
```

`<UNITY OPTIONS>` are `--unity-uri` (required here) and the `--unity-*`,
`--auth-bearer*` and `--oauth2-*` options of [`map`](#options). A `<TABLE>` of
fewer than three parts is completed from `--unity-catalog` and
`--unity-schema`, which also set how far `browse` reaches.

All five take `--remote <NAME>` and `--json`, which prints the endpoint's
answer as is.

| Command | Options of its own |
|---------|--------------------|
| `browse` | `--depth schemas\|tables` (default `tables`): how far a listing of one catalog reaches |
| `verify` | `--s3-region`, `--s3-endpoint`, `--s3-path-style` |
| `generate` | `--base-namespace <IRI>` (required): what every generated IRI derives from |
| | `-o, --output <FILE>`: write the mapping here instead of standard output |
| | `--subject-key <TABLE=COLUMN[,COLUMN]>` (repeatable): a table's subject columns |
| | `--class-name <TABLE=NAME>` (repeatable): a table's class |
| | `--strict-subjects`: give a table no subject unless its key is declared or cannot be null |
| | `--no-joins`: keep foreign keys as plain values |

`generate` writes the mapping to standard output or `--output`, and what it
decided — a key it chose, a column it passed over — to standard error.
`verify` exits non-zero when the table cannot be read, and `validate` when the
mapping has an error, so a script can gate on either.

```bash
fluree delta generate main.sales.orders main.sales.customers \
  --unity-uri https://<workspace>.cloud.databricks.com --auth-bearer-env DATABRICKS_TOKEN \
  --base-namespace https://example.org/sales# -o sales.ttl
```

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
