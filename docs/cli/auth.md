# fluree auth

Manage authentication tokens for remote servers. Tokens are stored in `.fluree/config.toml` as part of the remote configuration.

Token values are never printed to stdout. The `status` command shows token presence, expiry, and identity only.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `status` | Show authentication status for a remote |
| `login` | Store a bearer token for a remote |
| `logout` | Clear the stored token for a remote |

---

## fluree auth status

Show the current authentication state for a remote, including token presence, expiry time, identity, and issuer.

### Usage

```bash
fluree auth status [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Remote name (defaults to the only configured remote) |

### Examples

```bash
# Show auth status (single remote)
fluree auth status

# Show auth status for a specific remote
fluree auth status --remote origin
```

### Output

When a token is configured:
```
Auth Status:
  Remote: origin
  Token:  configured
  Expiry: 2026-02-15 12:00 UTC
  Identity: did:example:alice
  Issuer: did:key:z6Mk...
  Subject: alice@example.com
```

When no token is configured:
```
Auth Status:
  Remote: origin
  Token:  not configured
  hint: fluree auth login --remote origin
```

---

## fluree auth login

Store a bearer token for a remote. The token is saved in `.fluree/config.toml` and will be sent as a `Authorization: Bearer` header on subsequent remote operations (`fetch`, `pull`, `push`, `query --remote`, etc.).

### Usage

```bash
fluree auth login [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Remote name (defaults to the only configured remote) |
| `--token <VALUE>` | Token value, `@filepath` to read from file, or `@-` for stdin |

If `--token` is omitted, you will be prompted to paste the token interactively.

### Token Input Methods

| Method | Example |
|--------|---------|
| Inline value | `--token eyJhbG...` |
| File | `--token @/path/to/token.jwt` |
| File (tilde) | `--token @~/.fluree/token.jwt` |
| Stdin | `--token @-` (pipe or redirect) |
| Interactive | Omit `--token` to be prompted |

### Examples

```bash
# Store a token (prompted interactively)
fluree auth login

# Store a token from a value
fluree auth login --token eyJhbGciOiJFZERTQSI...

# Store a token from a file
fluree auth login --token @~/.fluree/my-token.jwt

# Pipe a token from another command
fluree token create --private-key @~/.fluree/key --all | fluree auth login --token @-

# Login to a specific remote
fluree auth login --remote staging --token @token.jwt
```

### Output

```
Token stored for remote 'origin'
  Expiry: 2026-02-15 12:00 UTC
  Identity: did:example:alice
```

---

## fluree auth logout

Clear the stored token for a remote.

### Usage

```bash
fluree auth logout [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--remote <NAME>` | Remote name (defaults to the only configured remote) |

### Examples

```bash
# Clear token for the default remote
fluree auth logout

# Clear token for a specific remote
fluree auth logout --remote staging
```

### Output

```
Token cleared for remote 'origin'
```

---

## Token Types

The `auth` command stores bearer tokens that are sent in the `Authorization` header. Fluree supports two types of bearer tokens:

### Ed25519 JWS Tokens (did:key)

Created locally with `fluree token create`. These contain an embedded JWK (JSON Web Key) in the token header and are verified against the embedded public key. The issuer is a `did:key` identifier derived from the signing key.

```bash
# Create and store a token in one step
fluree token create --private-key @~/.fluree/key --all | fluree auth login --token @-
```

### OIDC/JWKS Tokens (RS256)

Issued by external identity providers (OIDC). These contain a `kid` (Key ID) in the token header and are verified by the server against the provider's JWKS (JSON Web Key Set) endpoint. The issuer is the provider's URL.

The server must be configured with `--jwks-issuer` to trust these tokens. See [Configuration](../operations/configuration.md#oidc--jwks-token-verification).

## Remote Resolution

When `--remote` is omitted:
- If exactly one remote is configured, it is used automatically.
- If no remotes are configured, an error is shown with a hint to use `fluree remote add`.
- If multiple remotes are configured, an error asks you to specify `--remote <name>`.

## Security Notes

- Tokens are stored in plaintext in `.fluree/config.toml`. Protect this file with appropriate filesystem permissions.
- The `status` command never displays the raw token value.
- On 401 errors from remote operations, the CLI checks token expiry and suggests `fluree auth login` if the token appears expired.

## OIDC login flow

When a remote is configured with `auth.type = "oidc_device"` (auto-discovered from the server's `/.well-known/fluree.json`), `fluree auth login` runs an OIDC interactive login flow and then exchanges the IdP token for a Fluree-scoped Bearer token:

1. Discovers OIDC endpoints from the configured issuer
2. Chooses the flow based on IdP support:
   - If the IdP discovery document includes `device_authorization_endpoint`: use OAuth device-code (prints a URL + code and polls).
   - Otherwise, if it includes `authorization_endpoint`: use OAuth authorization-code + PKCE (opens a browser and receives a localhost callback).
3. Exchanges the IdP token for a Fluree-scoped Bearer token via the server's `exchange_url`
4. Stores the token (and optional refresh token) in the remote config

### Cognito note (Authorization Code + PKCE)

AWS Cognito does not publish `device_authorization_endpoint`, so the CLI will use authorization-code + PKCE.

Cognito requires the callback URL to be pre-allowlisted (no wildcard ports). Allowlist:

- `http://127.0.0.1:8400/callback`
- `http://127.0.0.1:8401/callback`
- `http://127.0.0.1:8402/callback`
- `http://127.0.0.1:8403/callback`
- `http://127.0.0.1:8404/callback`
- `http://127.0.0.1:8405/callback`

If your app only allowlists one callback URL, configure a fixed port with `redirect_port` in `/.well-known/fluree.json` (or set `FLUREE_AUTH_PORT` locally) and allowlist that single callback URL.

On subsequent 401 errors, the CLI automatically attempts a silent refresh using the stored refresh token before prompting for re-login.

See [Auth contract (CLI ↔ Server)](../design/auth-contract.md) for the full protocol specification.

## See Also

- [token](token.md) - Create and inspect JWS tokens
- [remote](remote.md) - Manage remote servers
- [Authentication](../security/authentication.md) - Auth model, modes, and token claims
- [Auth contract (CLI ↔ Server)](../design/auth-contract.md) - Discovery, exchange, and refresh protocol
- [Configuration](../operations/configuration.md) - Server authentication configuration
