# Connecting with Neo4j drivers (Bolt)

Fluree serves its [openCypher surface](../query/cypher.md) over the Bolt
protocol, so official Neo4j drivers and Bolt-compatible tools connect
directly. This guide covers enabling the listener and connecting from
driver code; protocol details live in the [Bolt reference](../api/bolt.md).

## Enable the listener

Bolt is compiled in by default but binds only when an address is
configured (conventional port 7687):

```bash
fluree-server \
  --bolt-listen-addr 0.0.0.0:7687 \
  --bolt-default-db mydb:main
```

The CLI's server management exposes the same flags on `run`, `start`,
and `restart` (background servers keep them across restarts, and
`fluree server status` shows the Bolt address):

```bash
fluree server start --bolt-listen-addr 0.0.0.0:7687 --bolt-default-db mydb:main
```

or in the config file:

```toml
[server.bolt]
listen_addr = "0.0.0.0:7687"
default_db = "mydb:main"
```

| Setting | Flag / Env | Meaning |
|---|---|---|
| `listen_addr` | `--bolt-listen-addr` / `FLUREE_BOLT_LISTEN_ADDR` | Address to serve Bolt on. Unset = disabled. |
| `default_db` | `--bolt-default-db` / `FLUREE_BOLT_DEFAULT_DB` | Ledger served to sessions that select no database. |

Sessions pick their ledger with the driver's standard database
selection; `default_db` is the fallback. A session with neither fails
its queries with a clear error.

Authentication follows the server's `data_auth_mode`, exactly like the
HTTP data plane: `none` (default) runs open, `optional` verifies tokens
when presented, `required` refuses anonymous sessions. Front with a TCP
proxy for TLS if the wire crosses trust boundaries — tokens on a bare
TCP wire are visible to the network.

## Authenticate

Pass the **same data-plane token the HTTP API accepts** (DID-JWS or
OIDC JWT) as a bearer credential — there is no separate Bolt user store:

```python
from neo4j import GraphDatabase, AuthTokens

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=AuthTokens.bearer(token))

# Driver code shaped as user/password works too: the password field
# carries the token and the username is ignored.
driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=("token", token))
```

The token's ledger scopes (`fluree.ledger.read.*` / `.write.*`) gate
every statement, its identity (`fluree.identity` claim) drives
[data policies](cookbook-policies.md) — two sessions with different
tokens can see different graphs from the same Cypher — and expiry is
re-checked per statement (`Neo.ClientError.Security.TokenExpired`
tells 5.x drivers to re-authenticate). See the
[Bolt reference](../api/bolt.md#authentication) for details.

## Names

Labels, relationship types, and property keys are plain names, exactly
as a Neo4j user expects — `CREATE (n:Person {name: "Ada"})` stores and
matches the names `Person` and `name` with no namespace involved. If
the ledger also holds RDF-style data (full IRIs), configure the
ledger's default context with `@vocab` so bare Cypher names resolve
into that vocabulary — see
[names and IRIs](../query/cypher.md#names-and-opting-into-iris).

## Connect (Python)

```python
from neo4j import GraphDatabase

# bolt:// (direct) scheme — neo4j:// routing is not served.
# auth=None works when the server doesn't require data auth.
driver = GraphDatabase.driver("bolt://localhost:7687", auth=None)

with driver.session(database="mydb:main") as session:
    # Autocommit query with parameters.
    result = session.run(
        "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name",
        min=30,
    )
    for record in result:
        print(record["name"])

    # Nodes come back typed: labels + properties + elementId (the durable identity).
    node = session.run("MATCH (p:Person) RETURN p LIMIT 1").single()["p"]
    print(node.labels, dict(node), node.element_id)

    # Managed transaction function — retried automatically on
    # concurrent-write conflicts.
    def add_person(tx):
        tx.run("CREATE (n:Person {name: $name})", name="Ada").consume()
        return tx.run("MATCH (n:Person) RETURN count(n) AS c").single()["c"]

    count = session.execute_write(add_person)
```

## Connect (JavaScript)

```javascript
const neo4j = require("neo4j-driver");
const driver = neo4j.driver("bolt://localhost:7687");

const session = driver.session({ database: "mydb:main" });
const result = await session.executeWrite(async (tx) => {
  await tx.run("CREATE (n:Person {name: $name})", { name: "Ada" });
  const res = await tx.run("MATCH (n:Person) RETURN count(n) AS c");
  return res.records[0].get("c");
});
await session.close();
```

## Transactions and retries

Explicit transactions are optimistic: statements execute against the
state the transaction began on (with read-your-writes), and `COMMIT`
succeeds only if no concurrent write landed in between. On conflict the
driver receives a `Neo.TransientError.*` failure — **use transaction
functions** (`execute_read` / `execute_write` / `executeQuery`), which
retry automatically; hand-rolled `begin_transaction()` code must be
prepared to retry on transient errors. Explicit transactions require a
single-node server; Raft/peer deployments reject `BEGIN` (autocommit
works everywhere).

## Troubleshooting

- **Connection refused** — the listener only binds when
  `bolt_listen_addr` is set; check the startup log for
  `Bolt listener starting`.
- **"unsupported protocol version" / immediate close** — the driver
  proposed only versions outside 4.4/5.0–5.4 (very old drivers).
  Upgrade the driver.
- **`neo4j://` scheme fails** — use `bolt://`; server-side routing
  tables are not served.
- **AuthError / `Security.Unauthorized`** — the server runs
  `data_auth_mode=required` and the session presented no (or an
  invalid) token; connect with `AuthTokens.bearer(<data-plane token>)`.
- **Queries fail with `DatabaseNotFound` on a ledger that exists** —
  the token's ledger scopes don't cover it (out-of-scope access is
  reported as not-found to avoid leaking ledger existence).
- **`Security.TokenExpired` mid-session** — the bearer token outlived
  its `exp`; re-authenticate (5.x drivers with an auth-token manager do
  this automatically) or reconnect with a fresh token.
- **BEGIN fails with "single-node server"** — the deployment replicates
  writes (Raft) or is a peer; use autocommit queries.
- **Numbers look different from the JSON API** — `xsd:decimal` is
  rendered as Float over Bolt (Neo4j parity); the JSON transport keeps
  exact decimal strings. See
  [Bolt value mapping](../api/bolt.md#value-mapping).
- **Bare `MATCH (n)` rejected** — whole-graph scans are opt-in via
  `FLUREE_CYPHER_ALLOW_FULL_SCAN=1`, same as over HTTP.
