# Connecting to Lakehouse Platforms

How to get from a table in Azure Data Lake Storage, Microsoft Fabric OneLake or
Databricks to a working Fluree graph source: which route to use, what to
create on the platform side, which credentials Fluree needs and where they go.

The option reference is in [Delta Lake tables](delta.md) and
[Iceberg / Parquet](iceberg.md); this page is the walk-through.

## Which route

| Your tables are | Use | Credentials Fluree needs |
|---|---|---|
| Delta tables in your own ADLS Gen2 account | [Delta source](#azure-data-lake-storage-gen2), by path | A service principal, managed identity, account key or SAS with read access to the container |
| Microsoft Fabric lakehouse tables (OneLake) | [Delta source](#microsoft-fabric-onelake), by path | A service principal with a workspace role that includes OneLake data access |
| Databricks Delta tables, **managed or external**, by name (Databricks on AWS or Azure) | [Delta source through Unity Catalog](#databricks-tables-through-unity-catalog) | A Databricks service principal (or token); Unity Catalog issues and renews the storage credentials |
| Databricks **external** tables, by path, without Unity Catalog | [Delta source](#databricks-external-tables), by path | Your own credentials for that S3 bucket or ADLS container |
| Databricks tables with Iceberg reads (UniForm) or managed Iceberg tables | [Iceberg REST source](#databricks-through-the-iceberg-rest-endpoint) | A Databricks service principal (or a personal access token); storage credentials are vended per request |
| Delta tables on S3 | Delta source, by path — see [Delta Lake tables](delta.md#credentials) | AWS credentials in the environment, or the instance / container role |

Everything Fabric stores in OneLake is a Delta table, whichever Fabric engine
wrote it, so OneLake is always the Delta route. Iceberg tables on Azure storage
are not readable yet: the Iceberg reader addresses S3 and GCS only.

Credentials always belong to the process that **reads** the tables — the Fluree
server, or a CLI running locally. A CLI that maps a source on a remote server
(`--remote`) needs none.

## Azure Data Lake Storage Gen2

**1. Find the table's location.** A Delta table is a directory holding a
`_delta_log` folder. Its location is

```
abfss://<container>@<account>.dfs.core.windows.net/<path to the table directory>
```

The storage account must have hierarchical namespace enabled (that is what
makes it "Gen2"). The host must be written in full, as above.

**2. Create an identity and let it read.** For a server outside Azure, a
service principal:

```bash
# Prints appId (client id), password (client secret) and tenant.
az ad sp create-for-rbac --name fluree-reader

az role assignment create \
  --assignee <appId> \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>/blobServices/default/containers/<container>"
```

Drop everything from `/blobServices` on to grant the whole account. A new role
assignment takes a minute or two to apply; until then reads fail with 403. The
*Reader* and *Contributor* roles on the account do **not** grant data access —
it has to be a *Storage Blob Data* role.

For a server running in Azure (VM, AKS, Container Apps), assign the same role
to its managed identity and give Fluree no Azure options at all.

**3. Map the source.**

```bash
export LAKE_CLIENT_SECRET='<password from step 2>'

fluree delta map sales \
  --root abfss://lake@contosolake.dfs.core.windows.net/Tables \
  --azure-tenant-id <tenant> --azure-client-id <appId> \
  --azure-client-secret-env LAKE_CLIENT_SECRET \
  --r2rml mappings/sales.ttl
```

The secret stays in the environment of the reading process; only the variable's
name is stored. With no `--azure-*` options the ambient chain is used instead:
`AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`,
`AZURE_STORAGE_ACCOUNT_KEY`, a SAS token, a workload-identity token file, then
the host's managed identity.

## Microsoft Fabric OneLake

**1. Find the workspace and lakehouse ids.** Open the lakehouse in Fabric; the
address bar reads

```
https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>
```

The tables are under

```
abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables
```

In a lakehouse created with schemas, a table is `Tables/<schema>/<table>` and is
mapped as `rr:tableName "dbo.orders"`. Without schemas it is `Tables/<table>`
and mapped by its bare name.

**2. Create a service principal** as in the ADLS section
(`az ad sp create-for-rbac`), or register an application in Microsoft Entra and
add a client secret. No Azure role assignment is involved: OneLake access is
granted inside Fabric.

**3. Give it access in Fabric.** In the workspace, **Manage access → Add people
or groups**, search for the application by name, and choose **Contributor**.
*Viewer* is not enough: a Viewer principal authenticates and is then refused
with `403 … not authorized … for workspace`. A OneLake data access role that
grants read on the lakehouse also works. No tenant-wide setting is needed for a
service principal to read OneLake files.

**4. Map the source.**

```bash
export FABRIC_CLIENT_SECRET='…'

fluree delta map fabric-sales \
  --root abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables \
  --azure-tenant-id <tenant> --azure-client-id <appId> \
  --azure-client-secret-env FABRIC_CLIENT_SECRET \
  --r2rml mappings/sales.ttl
```

Row- and column-level rules defined in Fabric apply to Fabric's own engines,
not to a reader of the files. Govern what Fluree users see with a model
ledger's [access policy](iceberg.md#access-policy).

## Databricks

Databricks writes Delta tables with its own defaults — deletion vectors, column
mapping, liquid clustering, v2 checkpoints, row tracking, type widening — and
the Delta reader reads all of them. A `VARIANT` column cannot be mapped; the
rest of such a table can.

What differs is how Fluree gets to the files.

### Databricks tables through Unity Catalog

Name the tables as Databricks does — `catalog.schema.table` — and let Unity
Catalog say where each one lives and issue the credentials that read it. This
reaches **managed** tables, whose storage path Unity owns, as well as external
ones, and needs no storage credentials on the Fluree side at all.

Unity issues credentials per table, scoped to that table's path and valid for
an hour. Fluree holds a set per table and asks for the next one a few minutes
before the current one expires, so a source may span any number of tables and
run indefinitely.

**1. Turn on external data access** for the metastore, once: in the workspace,
**Catalog → ⚙ → Metastore → External data access**.

**2. Create the identity Fluree reads as.** A service principal, whose OAuth
token Fluree requests and renews by itself:

- **Settings → Identity and access → Service principals → Add service
  principal.** Note its **Application ID** — that is the client id.
- On its **Secrets** tab, **Generate secret**. The secret is shown once.

A personal access token also works (user icon → **Settings → Developer →
Access tokens → Generate new token**; if the dialog asks for scopes,
`unity-catalog` covers this route). It does not renew: give it a lifetime to
match, and re-map when it is rotated.

**3. Grant it the privileges.** `EXTERNAL USE SCHEMA` is implied by nothing —
not ownership, not metastore admin — and the ordinary read privileges are
needed as well, even for a principal that can already see the table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<application-id>`;
GRANT USE SCHEMA, SELECT, EXTERNAL USE SCHEMA ON SCHEMA main.sales TO `<application-id>`;
```

A user is named by email in place of the application id.

**4. Map the source.**

```bash
export DATABRICKS_CLIENT_SECRET='<secret from step 2>'

fluree delta map sales \
  --unity-uri https://<workspace>.cloud.databricks.com \
  --unity-catalog main \
  --oauth2-client-id <application-id> \
  --oauth2-client-secret-env DATABRICKS_CLIENT_SECRET \
  --s3-region us-east-1 \
  --r2rml mappings/sales.ttl
```

- The mapping names tables as `rr:tableName "main.sales.orders"`.
  `--unity-catalog` (and `--unity-schema`) complete shorter names, so with the
  command above `"sales.orders"` is enough.
- The secret stays in the environment of the process that reads the tables;
  only the variable's name is stored. When mapping on a server (`--remote`, or
  the HTTP API), its operator lists the variable in
  `FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS` first. With a token, give
  `--auth-bearer-env DATABRICKS_TOKEN` in place of the two `--oauth2-` options.
- On AWS, give the bucket's region with `--s3-region` (or `AWS_REGION`): Unity
  does not say which it is. On Azure Databricks the workspace URL is
  `https://adb-….azuredatabricks.net` and no region is needed.
- A table that lives outside Unity can sit in the same source:
  `--table raw.events=s3://lake/raw/events` is read by path, with the
  process's own credentials.

Only Delta tables with files of their own are read. A view, a table in another
format, a table with a row filter or column mask, and a table the principal
cannot see are each reported by name — as a warning when the source is
mapped, and as the error of any query that touches them.

Databricks on Google Cloud is not covered yet: Unity Catalog places its tables
on Google Cloud Storage (`gs://…`), which the Delta reader does not read, and
mapping such a table reports exactly that.

### Databricks external tables

An external table lives at a path you chose, in storage you control, so it can
also be read without Unity Catalog. Find the path:

```sql
DESCRIBE DETAIL main.sales.orders;   -- the `location` column
```

Map that location as a Delta source, with your own credentials for the bucket
or container — the [ADLS steps above](#azure-data-lake-storage-gen2), or AWS
credentials for S3. Unity Catalog is not involved in the read.

### Databricks through the Iceberg REST endpoint

Unity Catalog serves an Iceberg REST catalog. Fluree's Iceberg source reads
through it with nothing but a Databricks token: each table load returns
temporary storage credentials, requested again as they expire.

Only two kinds of table are visible there:

- managed Iceberg tables, and
- Delta tables with Iceberg reads (UniForm) enabled:
  ```sql
  ALTER TABLE main.sales.orders SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg');
  ```
  Databricks documents the conditions; notably, Iceberg v2 compatibility cannot
  be combined with deletion vectors.

Any other table answers `… is not an Iceberg compatible table`.

The prerequisites are those of the previous section — external data access
and the four privileges — and a token with the **`all-apis`** scope: the Iceberg
endpoint refuses narrower ones (`Provided access token does not have required
scopes: all-apis`).

```bash
fluree iceberg map dbx-sales \
  --catalog-uri https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest \
  --warehouse main \
  --auth-bearer-env DATABRICKS_TOKEN \
  --r2rml mappings/sales.ttl
```

`--warehouse` is the Unity **catalog** name, and the mapping names tables as
`<schema>.<table>` (`rr:tableName "sales.orders"`). `--auth-bearer-env` names
the environment variable holding the token, read by the process that reads the
tables, so the token itself is not stored. A personal access token does not
renew; give it a lifetime to match.

**For a standing deployment, use the service principal** from
[the section above](#databricks-tables-through-unity-catalog), with the same
four privileges; its OAuth token is requested and renewed by Fluree:

```bash
fluree iceberg map dbx-sales \
  --catalog-uri https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest \
  --warehouse main \
  --oauth2-token-url https://<workspace>.cloud.databricks.com/oidc/v1/token \
  --oauth2-client-id <application-id> \
  --oauth2-client-secret-env DATABRICKS_CLIENT_SECRET \
  --oauth2-scope all-apis \
  --r2rml mappings/sales.ttl
```

The secret stays in the environment of the process that reads the tables; only
the variable's name is stored. When mapping on a server (`--remote`, or the
HTTP API), the server's operator lists the variable in
`FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS` first; the same holds for
`--auth-bearer-env`.

## When it does not work

| Message | Cause |
|---|---|
| `403` from ADLS right after setup | The role assignment has not applied yet (1–2 minutes), or the role is *Reader* / *Contributor* rather than a *Storage Blob Data* role |
| `403 … not authorized … for workspace` from OneLake | The principal is a workspace *Viewer*; it needs *Contributor* or a OneLake data access role |
| `(not readable yet)` when mapping | The mapping process could not open the table — often only because it lacks the credentials the server has. The source is registered; the first query reports the real error |
| S3 reads fail although `aws` works in the same shell | `AWS_PROFILE` and SSO sessions are not read. Export the profile's keys (`aws configure export-credentials --format env`) |
| `User does not have EXTERNAL USE SCHEMA on Schema …` (or `USE CATALOG`, `USE SCHEMA`, `SELECT`) | A grant from [step 3](#databricks-tables-through-unity-catalog) is missing; none is implied by ownership or admin rights |
| `Unity Catalog issued no credentials. The table has a row filter …` (or `column mask`) | Whether such a table can be read is Unity Catalog's decision, and today it refuses: it enforces those rules only in its own compute and issues no credentials to read such a table's files, even to its owner. Expose the permitted rows and columns as a separate table, and govern access in Fluree with a model ledger's [access policy](iceberg.md#access-policy) |
| `… is a VIEW, not a Delta table` / `… in PARQUET format` | Unity Catalog places only Delta tables with files of their own; map the underlying table |
| `Unity Catalog placed this table on Google Cloud Storage …` | The workspace is Databricks on Google Cloud, which the Delta reader does not support yet |
| `Received redirect` from S3 when a Unity table is first read | The bucket is in another region than `--s3-region` / `AWS_REGION` names |
| `Catalog … authorized the table but vended no storage credentials` | The principal can see the table but lacks `USE CATALOG`, `USE SCHEMA`, `SELECT` or `EXTERNAL USE SCHEMA`. The Iceberg endpoint answers without credentials rather than with an error; `POST …/temporary-table-credentials` as the same principal names the missing privilege |
| `Provided access token does not have required scopes: all-apis` | The token was created with narrower scopes than the Iceberg endpoint accepts |
| `… is not an Iceberg compatible table` | The table is plain Delta; enable UniForm or read it as a Delta source |
| `unsupported Delta column type: Struct([… metadata … value …])` | The mapping names a `VARIANT` column; leave it out of the mapping |
| `relative IRI '#Map' has no base` | The R2RML file uses a relative subject; add `@base` or write the IRI in full |

## Related Documentation

- [Delta Lake tables](delta.md) — locations, credentials, types, time travel
- [Iceberg / Parquet](iceberg.md) — REST catalogs, vended credentials, access policy
- [`fluree delta`](../cli/delta.md), [`fluree iceberg`](../cli/iceberg.md)
- [R2RML](r2rml.md)
