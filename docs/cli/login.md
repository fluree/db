# fluree login

Sign this machine in to a fluree.ai account. The sign-in is kept once per machine, not per project, so it works from any
directory (no `fluree init` needed), and it renews itself until you sign out.

An account is not a [remote](remote.md): remotes are Fluree servers whose ledgers you query and sync, while a fluree.ai
account is where hosted services (and their usage and billing) live. `fluree remote add https://fluree.ai` therefore
refuses and points here.

## Usage

```bash
fluree login [--url <URL>] [--no-browser]
```

### Options

| Option | Description |
|--------|-------------|
| `--url <URL>` | Account service to sign in to (default `https://fluree.ai`) |
| `--no-browser` | Print the approval link instead of opening a browser (for example over SSH) |

## What happens

1. The CLI reads `<URL>/.well-known/fluree.json` and checks that it describes an account service.
2. It asks the service for a one-time code and opens the approval page (`<URL>/activate?user_code=…`) in your browser,
   printing the link and the code as well:

   ```
   >> Open this URL and enter the code below:
     URL:  https://fluree.ai/activate
     (or)  https://fluree.ai/activate?user_code=ACDE-2467
     Code: ACDE-2467
   ```

3. In the browser, sign in to (or create) your account, check that the code matches the one in your terminal, and approve.
   The page names the machine (its host name) that is asking.
4. The CLI picks up the approval and stores the sign-in:

   ```
   Signed in to fluree.ai as you@example.com (Your Org)
     Credentials: ~/Library/Application Support/fluree/account.toml
   ```

Running `fluree login` again replaces the stored sign-in and signs the previous one out on the server.

## Where the sign-in is stored

`account.toml` in the global config directory: `$FLUREE_HOME` when set, otherwise the platform config directory
(`~/Library/Application Support/fluree/` on macOS, `~/.config/fluree/` on Linux, `%LOCALAPPDATA%\fluree\` on Windows).
It is written with owner-only permissions (0600 on Unix) and holds:

- a short-lived access token (an hour on fluree.ai), renewed automatically when it is about to expire;
- a long-lived refresh credential for this machine. Treat the file like a password; `fluree logout` revokes it.

`fluree auth status` shows the sign-in without contacting the server. Each signed-in machine also appears on the account's
dashboard, where it can be disconnected.

## Signing out

See [`fluree logout`](logout.md).

## Protocol

The sign-in uses the discovery document, OAuth 2.0 device authorization grant (RFC 8628) and token exchange described in the
[auth contract](../design/auth-contract.md#account-services), so any service implementing that contract with an `account`
object can be signed in to with `--url`.
