# Connect an agent over MCP

A Fluree server can hand any ledger to an AI agent as a Model Context Protocol (MCP)
tool. Turn on `/mcp`, point your agent at it, and the agent can learn the ledger's
schema and query it without you writing any glue code.

The agent gets two tools, both read-only:

| Tool | What it does |
|------|--------------|
| `get_data_model` | Describes a ledger as markdown: its classes, properties, instance counts, and size. Agents are told to call this first. |
| `sparql_query` | Runs a SPARQL `SELECT` against one ledger and returns [Agent JSON](../query/output-formats.md#agent-json-format): compact rows with a byte budget, plus a `t` and `hasMore` so the agent can page through large results. |

The endpoint uses MCP's streamable HTTP transport. Any MCP client that can connect to a
URL works.

> This page covers the server's `/mcp` endpoint. `fluree mcp serve` is a different thing:
> a local stdio server that gives IDE agents Fluree's documentation and memory tools. See
> [`fluree mcp`](../cli/mcp.md).

## Try it in two minutes

### 1. Start a server with MCP on

```bash
mkdir mcp-demo && cd mcp-demo
fluree init
fluree server run --listen-addr 127.0.0.1:8090 -- --mcp-enabled
```

You don't need keys or tokens for this. Out of the box the server runs without
authentication, so `/mcp` is exactly as open as the query API next to it. Binding to
`127.0.0.1` keeps it on your machine. The default `0.0.0.0` would expose it to your
network.

### 2. Add some data

In a second terminal:

```bash
curl -X POST http://127.0.0.1:8090/v1/fluree/create \
  -H "Content-Type: application/json" \
  -d '{"ledger": "movies"}'

curl -X POST http://127.0.0.1:8090/v1/fluree/insert/movies \
  -H "Content-Type: application/json" \
  -d '{
    "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
    "@graph": [
      {"@id": "ex:inception", "@type": "schema:Movie", "schema:name": "Inception",
       "schema:datePublished": 2010, "schema:director": {"@id": "ex:nolan"}},
      {"@id": "ex:interstellar", "@type": "schema:Movie", "schema:name": "Interstellar",
       "schema:datePublished": 2014, "schema:director": {"@id": "ex:nolan"}},
      {"@id": "ex:nolan", "@type": "schema:Person", "schema:name": "Christopher Nolan"}
    ]
  }'
```

### 3. Connect your agent

**Claude Code**

```bash
claude mcp add --transport http fluree http://127.0.0.1:8090/mcp
```

**Cursor** (`.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "fluree": { "url": "http://127.0.0.1:8090/mcp" }
  }
}
```

**VS Code** (`.vscode/mcp.json`)

```json
{
  "servers": {
    "fluree": { "type": "http", "url": "http://127.0.0.1:8090/mcp" }
  }
}
```

**Any other client:** give it the URL `http://127.0.0.1:8090/mcp` and choose the
streamable HTTP transport. To poke at the tools by hand, run
`npx @modelcontextprotocol/inspector` and connect to the same URL.

### 4. Ask it something

> What's in the movies ledger? Which films did Christopher Nolan direct, and when?

The agent calls `get_data_model` on `movies` to learn the classes and properties, then
writes and runs a `sparql_query`.

<details>
<summary>Checking the endpoint with curl instead</summary>

MCP is JSON-RPC over HTTP. A session is an `initialize` call, which returns an
`mcp-session-id` header, then an `initialized` notification, then tool calls:

```bash
URL=http://127.0.0.1:8090/mcp
HDRS=(-H "Content-Type: application/json" -H "Accept: application/json, text/event-stream")

SID=$(curl -s -D - -o /dev/null "${HDRS[@]}" "$URL" -d '{
  "jsonrpc": "2.0", "id": 1, "method": "initialize",
  "params": {"protocolVersion": "2025-03-26", "capabilities": {},
             "clientInfo": {"name": "curl", "version": "0"}}}' \
  | awk 'tolower($1) == "mcp-session-id:" {print $2}' | tr -d '\r')

curl -s "${HDRS[@]}" -H "mcp-session-id: $SID" "$URL" \
  -d '{"jsonrpc": "2.0", "method": "notifications/initialized"}'

curl -s "${HDRS[@]}" -H "mcp-session-id: $SID" "$URL" -d '{
  "jsonrpc": "2.0", "id": 2, "method": "tools/call",
  "params": {"name": "sparql_query", "arguments": {
    "ledger": "movies",
    "query": "PREFIX schema: <http://schema.org/> SELECT ?title ?year WHERE { ?m a schema:Movie ; schema:name ?title ; schema:datePublished ?year } ORDER BY ?year"}}}'
```

The tool result's text is an Agent JSON envelope:

```json
{"schema": {"?title": "http://www.w3.org/2001/XMLSchema#string",
            "?year": "http://www.w3.org/2001/XMLSchema#integer"},
 "rows": [{"?title": "Inception", "?year": 2010},
          {"?title": "Interstellar", "?year": 2014}],
 "rowCount": 2, "t": 1, "hasMore": false}
```

Once tokens are on (below), add `-H "Authorization: Bearer $TOKEN"` to each call.

</details>

## When `/mcp` needs a token

MCP follows the server's data API authentication, so turning MCP on never makes a server
more open than it already is:

| Server setup | `/mcp` behavior |
|--------------|-----------------|
| Data auth `none` (the default) and no MCP issuer configured | Open. No token needed, every ledger readable. A token that is sent anyway is ignored, as it is on the data API. |
| An MCP issuer is configured (`--mcp-auth-trusted-issuer`, or `--events-auth-trusted-issuer` as a fallback) | A token is required, whatever the data auth mode. |
| Data auth `optional` or `required` | A token is required. The server refuses to start unless an MCP issuer is configured. |

## Protecting it

Choose the level that fits how far the server is exposed.

### Local development: bind to localhost

The two-minute setup above is fine as long as only your machine can reach it. Keep the
`--listen-addr 127.0.0.1:...`.

### Anything shared: require tokens for the whole server

A token on `/mcp` alone doesn't protect your data if `/v1/fluree/query` next to it is
still open. Turn on data auth as well, and trust the same signing key for both.

Fluree tokens are Ed25519-signed JWTs. You mint them yourself with the CLI, so you don't
need an identity provider:

```bash
# 1. Make a signing key. Keep agent.key private: anyone holding it can mint
#    tokens this server accepts.
fluree token keygen --output agent.key
#    → DID:        did:key:z6Mk...

# 2. Start the server trusting that key for the data API and for MCP.
fluree server run -- \
  --mcp-enabled \
  --data-auth-mode required \
  --data-auth-trusted-issuer did:key:z6Mk... \
  --mcp-auth-trusted-issuer did:key:z6Mk...

# 3. Mint a token for the agent.
TOKEN=$(fluree token create --private-key @agent.key \
  --read-ledger movies --subject agent:reporting --expires-in 30d)

# 4. Give the agent the token.
claude mcp add --transport http fluree https://fluree.example.com/mcp \
  --header "Authorization: Bearer $TOKEN"
```

For Cursor and VS Code, add a `headers` entry next to `url`:

```json
"headers": { "Authorization": "Bearer <token>" }
```

The same server settings in `.fluree/config.toml`:

```toml
[server.auth.data]
mode = "required"
trusted_issuers = ["did:key:z6Mk..."]

[server.mcp]
enabled = true
auth_trusted_issuers = ["did:key:z6Mk..."]
```

### Scope what each agent can read

A token reaches only the ledgers its read claims name. The same claims govern the data
API (see [Authentication](../security/authentication.md#query-scopes-flureeledger)):

| `fluree token create` flag | The agent can read |
|----------------------------|--------------------|
| `--read-all` | Every ledger |
| `--read-ledger movies` | `movies:main` only. A bare name means the `main` branch. |
| `--read-ledger movies:dev` | The `dev` branch only |
| None of these | Nothing. Every call answers `Ledger not found`. |

A ledger outside the token's scope answers `Ledger not found`, the same as one that
doesn't exist, so an agent can't use the tools to discover what other ledgers are on the
server. `--all` also grants read access, but it adds write, events, and storage access
that an agent doesn't need, so prefer `--read-ledger` or `--read-all`.

Tokens are checked on every request. `--expires-in` defaults to `1h`, so choose a
lifetime you're prepared to rotate on.

### Restrict what an agent sees within a ledger

The token's identity is `--identity`, or `--subject` if that isn't set. Fluree evaluates
the ledger's [policies](../security/policy-in-queries.md) against that identity, so policy
rules can restrict which subjects and properties a particular agent sees, down to single
values.

A request with no identity, meaning a tokenless request or a token without `sub` or
`fluree.identity`, is treated as anonymous: the ledger's configured
[policy defaults](../ledger-config/setting-groups.md) apply, exactly as they do for an
anonymous query on the data API.

MCP rejects policy-delegation tokens (`fluree.policy` claims, from
`--policy-select`); use identity-based policies instead.

`get_data_model` reports ledger-wide statistics (class and property names, instance
counts, size) for any ledger the token can read, and policy doesn't filter it. The data
API's ledger info endpoint behaves the same way. If the shape of a ledger is itself
sensitive, keep it out of the token's read scope rather than relying on policy.

### Production notes

- **TLS**: the server speaks plain HTTP. Put it behind a reverse proxy or load balancer
  that terminates TLS, since bearer tokens must not cross a network in the clear.
- **Token types**: `/mcp` accepts Ed25519 `did:key` tokens, like the ones `fluree token`
  mints. OIDC/JWKS tokens that the data API accepts are not supported on `/mcp` yet.
- **`--mcp-auth-insecure`** accepts a token signed by any key. It is for tests only and is
  hidden from `--help` for that reason.

## Tuning

| Flag | Env var | Default | Effect |
|------|---------|---------|--------|
| `--mcp-enabled` | `FLUREE_MCP_ENABLED` | `false` | Serve `/mcp` |
| `--mcp-auth-trusted-issuer` | `FLUREE_MCP_AUTH_TRUSTED_ISSUERS` | none | `did:key` issuers whose tokens `/mcp` accepts (repeatable) |
| `--mcp-agent-json-max-bytes` | `FLUREE_MCP_AGENT_JSON_MAX_BYTES` | `32768` | Byte budget for a `sparql_query` result; larger results are truncated with `hasMore: true` |
| `--mcp-query-timeout-ms` | `FLUREE_MCP_QUERY_TIMEOUT_MS` | `300000` | Server-side timeout for `sparql_query` (`0` disables it) |

The byte budget is the main dial: a smaller budget keeps each tool result within the
agent's context, and a larger one saves round trips. See
[Configuration: MCP endpoint](../operations/configuration.md#mcp-endpoint).

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| `404` on `/mcp` | MCP isn't enabled. Add `--mcp-enabled`. |
| `401 Bearer token required for MCP endpoint` | The server requires tokens and the client sent none. Check the client's `Authorization` header. |
| `401 Invalid or unauthorized token` | The token has expired, its signature doesn't verify, or its issuer isn't trusted. The server log shows which, at `warn`. |
| Tool result `Ledger not found` | The token's read claims don't cover that ledger or branch. Remember a bare name means `:main`. |
| Server won't start: `mcp_enabled with --data-auth-mode optional or required needs --mcp-auth-trusted-issuer ...` | Data auth is on, so MCP needs an issuer to trust. Add `--mcp-auth-trusted-issuer`. |
| An agent's results stop partway, with `hasMore: true` | The result hit the byte budget. The agent should re-run with the same `t`, an `ORDER BY`, and `OFFSET` advanced by `rowCount`, or raise `--mcp-agent-json-max-bytes`. |
