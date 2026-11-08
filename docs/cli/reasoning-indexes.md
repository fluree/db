# fluree reasoning-indexes

Register a reasoning program on a branch, and list, refresh or retire its reasoning indexes.

## Usage

```bash
fluree reasoning-indexes register [LEDGER] (--reasoning <MODES> | --program <JSON>) [--remote <NAME>]
fluree reasoning-indexes list [LEDGER] [--remote <NAME>]
fluree reasoning-indexes refresh [LEDGER] [--index <ID>] [--remote <NAME>]
fluree reasoning-indexes retire <ID> [-l <LEDGER>] [--remote <NAME>]
```

`LEDGER` defaults to the active ledger. Every action takes `--remote <NAME>` to run against a remote server.

## Actions

| Action | Description |
|--------|-------------|
| `reasoning-indexes register` | Register a program. `--reasoning owl2rl` names reasoning modes as a query's `reasoning` key would; `--program` takes the whole program as JSON (`reasoning`, `rules`, `ontology`, `reasoningTime`, `@context`, and an optional `graph` IRI to reason against). Registering a program that is already active returns its existing index. |
| `reasoning-indexes list` | Show the branch's indexes: each one's program, state and published generation. |
| `reasoning-indexes refresh` | Bring the branch's indexes up to its current commit now, rather than waiting for the background worker. `--index <ID>` refreshes one. |
| `reasoning-indexes retire` | Stop serving and refreshing an index. Its files stay until a cleanup pass removes them. |

## Description

A reasoning index holds a branch's asserted facts together with the facts a registered program derives from them. A reasoning query whose program matches an index, at the index's commit, is answered from the index; any other query reasons at query time, as before. A branch holds at most eight active indexes.

See [Materialized reasoning](../query/materialized-reasoning.md) for which queries an index answers, freshness modes, storage and retention.

## Examples

```bash
# Register OWL 2 RL on the active ledger, build it now, and inspect it
fluree reasoning-indexes register --reasoning owl2rl
fluree reasoning-indexes refresh
fluree reasoning-indexes list

# A Datalog program on another ledger
fluree reasoning-indexes register mydb --program '{"reasoning": "datalog", "rules": [...]}'

# Retire an index
fluree reasoning-indexes retire 01JC... -l mydb
```

## Output

Each action prints the server's JSON response, for example:

```json
{
  "ledger": "mydb:main",
  "indexId": "01JC..."
}
```
