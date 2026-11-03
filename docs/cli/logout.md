# fluree logout

Sign this machine out of its fluree.ai account (see [`fluree login`](login.md)).

## Usage

```bash
fluree logout
```

The CLI revokes the machine's credential on the server (`POST <account API>/auth/disconnect`) and deletes the local
`account.toml`. If the server can't be reached, the local sign-in is still removed and the CLI says so; disconnect the
machine from the account's dashboard to revoke it there.

```
$ fluree logout
Signed out of fluree.ai as you@example.com (Your Org)
```

When the machine isn't signed in, `fluree logout` prints `Not signed in` and does nothing. When `account.toml` can't be read
(damaged, or written by a newer CLI), `fluree logout` removes it and warns that the server wasn't told: disconnect the machine
from the account's dashboard if it was signed in. `fluree login` also replaces such a file.

Remote logins are separate: use [`fluree auth logout --remote <name>`](auth.md#fluree-auth-logout) for those.
