# fluree sql

Manage SQL graph sources — R2RML mappings over tables reached through a
Trino-protocol endpoint (Trino, Starburst, PrestoDB, or a `fluree-sql-bridge`
sidecar). See [SQL graph sources](../graph-sources/sql.md).

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `map` | Map tables behind a SQL endpoint as a graph source |
| `list` | List mapped graph sources (SQL, Iceberg and R2RML) |
| `info` | Show details for a mapped graph source |
| `drop` | Drop a mapped graph source |

`list`, `info` and `drop` are shared with [`fluree iceberg`](iceberg.md): both
commands operate on the same family of mapped sources.

## fluree sql map

### Usage

```bash
fluree sql map <NAME> --endpoint <URL> --r2rml <PATH> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Graph source name (e.g., "orders-db") |

### Options

**Endpoint:**

| Option | Description |
|--------|-------------|
| `--endpoint <URL>` | Statement endpoint base URL (required), e.g. `https://trino.example.com:8443` or `http://localhost:8080` for a sidecar |
| `--dialect <NAME>` | SQL rendering dialect: `trino` (default), `postgres`, `mysql`, `sqlite`. Use the engine behind a bridge. |
| `--protocol <NAME>` | Header family: `trino` (default) or `presto` |
| `--catalog <NAME>` | Default catalog for unqualified table names |
| `--schema <NAME>` | Default schema for unqualified table names |
| `--user <NAME>` | Protocol user (`X-Trino-User`); defaults to `fluree` |
| `--session KEY=VALUE` | Session property (repeatable), e.g. `--session query_max_run_time=5m` |

**R2RML mapping:**

| Option | Description |
|--------|-------------|
| `--r2rml <PATH>` | Mapping file (required). Each `rr:tableName` names a table reachable through the endpoint; `rr:sqlQuery` is also accepted. |
| `--r2rml-type <TYPE>` | Mapping media type (e.g., `text/turtle`); inferred from extension if omitted |

**Authentication:**

| Option | Description |
|--------|-------------|
| `--auth-bearer <TOKEN>` | Static bearer token |
| `--oauth2-token-url <URL>` | OAuth2 client-credentials token endpoint |
| `--oauth2-client-id <ID>` | OAuth2 client ID |
| `--oauth2-client-secret <SECRET>` | OAuth2 client secret |
| `--oauth2-scope <SCOPE>` | OAuth2 scope |
| `--oauth2-audience <AUD>` | OAuth2 audience |

**General:**

| Option | Description |
|--------|-------------|
| `--branch <BRANCH>` | Branch name (defaults to `main`) |
| `--model <LEDGER>` | Model ledger (`name:branch`) whose default graph supplies the source's view policies and class/property hierarchy. Must exist. See [Access policy](../graph-sources/iceberg.md#access-policy) |
| `--default-allow <BOOL>` | Fallback for governed requests that match no policy; `true` keeps the source readable under authentication without a model (unset: deny) |
| `--allow-duplicate-subjects` | Accept subject keys the registration probe finds non-unique. The probe still warns; without this flag the pushdown lane refuses statements over the flagged tables. See [Subject keys must be unique](../graph-sources/sql.md#subject-keys-must-be-unique) |
| `--remote <NAME>` | Execute against a remote server |

### Examples

```bash
# Trino with a bearer token; tables are qualified inside hive.sales
fluree sql map orders-db \
  --endpoint https://trino.example.com:8443 \
  --catalog hive --schema sales \
  --auth-bearer "$TRINO_TOKEN" \
  --r2rml mappings/orders.ttl

# A bridge sidecar in front of Postgres
fluree sql map crm \
  --endpoint http://localhost:8080 \
  --dialect postgres --schema public \
  --r2rml mappings/crm.ttl
```

### Output

```
Mapped SQL endpoint as graph source 'orders-db:main'
  Endpoint:    https://trino.example.com:8443
  R2RML:       bafy…
  TriplesMaps: 3
  Tables:      2 (sales.orders, sales.customers)
  Connection:  verified
  Mapping:     validated
```

`Connection: not tested` means the `SELECT 1` probe failed; the source is
still registered and the first query reports the underlying error.

A `Warning:` line per finding of the subject-key uniqueness probe follows
(a table whose subject key repeats, or a table that could not be probed).

## fluree sql check

```bash
fluree sql check orders-db
```

Re-runs the subject-key uniqueness probe against the live tables and stores
the result on the source. Local only (no `--remote`).

## fluree sql list / info / drop

```bash
fluree sql list
fluree sql info orders-db
fluree sql drop orders-db --force
```

Behave exactly as the [`fluree iceberg`](iceberg.md) equivalents; SQL sources
show the type `SQL`.
