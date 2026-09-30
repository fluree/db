# Writing Config Data

The config graph is mutated using normal ledger transactions — config writes are signed, versioned, and replicable like any other write. The only difference is that the triples target the config graph IRI.

## Config graph IRI

Each ledger's config graph has a deterministic IRI:

```
urn:fluree:{ledger_id}#config
```

For a ledger named `mydb:main`, the config graph is `urn:fluree:mydb:main#config`.

## Writing via TriG

TriG is the most natural format for writing to named graphs. Wrap your config triples in a `GRAPH` block targeting the config graph IRI:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false
    ] ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:validationMode f:ValidationReject
    ] .
}
```

## Writing from the CLI

`fluree insert` and `fluree upsert` read TriG, but the recipe above writes its settings as anonymous blank nodes (`[ … ]`) inside the `GRAPH` block, which those commands do not read yet ([#1930](https://github.com/fluree/db/issues/1930)). Write the config graph with SPARQL UPDATE through `fluree update` instead:

```bash
fluree update -l mydb:main --format sparql -e '
PREFIX f: <https://ns.flur.ee/db#>
INSERT DATA {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
      f:datalogDefaults [
        f:datalogEnabled true ;
        f:rulesSource [ a f:GraphRef ; f:graphSource [ f:graphSelector f:defaultGraph ] ] ;
        f:allowQueryTimeRules true
      ] ;
      f:reasoningDefaults [ f:reasoningModes f:Datalog ; f:overrideControl f:OverrideAll ] .
  }
}'
```

`fluree update -f config.ru --format sparql` reads the same statement from a file. `fluree insert` takes the JSON-LD form below.

## Writing via SPARQL UPDATE

Use `INSERT DATA` with a `GRAPH` clause:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
      f:reasoningDefaults [
        f:reasoningModes f:RDFS ;
        f:schemaSource [
          a f:GraphRef ;
          f:graphSource [ f:graphSelector f:defaultGraph ]
        ]
      ] .
  }
}
```

## Writing via JSON-LD

Give the config node an `@graph` naming the config graph:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "@graph": [
    {
      "@id": "urn:fluree:mydb:main:config:ledger",
      "@type": "f:LedgerConfig",
      "@graph": "urn:fluree:mydb:main#config",
      "f:shaclDefaults": {
        "f:shaclEnabled": true,
        "f:validationMode": { "@id": "f:ValidationReject" }
      }
    }
  ]
}
```

A node's `@graph` scopes the node and everything nested in it, so the `f:shaclDefaults` group above lands in the config graph with its parent. (Before this release the nested group's fields landed in the default graph, where the config reader never looks; see [Repairing a config split across graphs](#repairing-a-config-split-across-graphs).)

`"@graph": "config"` names the ledger's own config graph without spelling out its IRI:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "@id": "urn:fluree:mydb:main:config:ledger",
  "@type": "f:LedgerConfig",
  "@graph": "config",
  "f:policyDefaults": { "f:defaultAllow": false }
}
```

The JSON-LD 1.1 named-graph form works too: an object whose `@id` is the graph and whose `@graph` holds the nodes written to it.

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "@id": "urn:fluree:mydb:main#config",
  "@graph": [
    {
      "@id": "urn:fluree:mydb:main:config:ledger",
      "@type": "f:LedgerConfig",
      "f:reasoningDefaults": { "f:reasoningModes": { "@id": "f:rdfs" } }
    }
  ]
}
```

In an update, the `graph` key scopes every template to one graph, and accepts the same `"config"` keyword:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "graph": "config",
  "where": { "@id": "urn:fluree:mydb:main:config:ledger", "f:shaclDefaults": "?group" },
  "delete": { "@id": "?group", "f:shaclEnabled": false },
  "insert": { "@id": "?group", "f:shaclEnabled": true }
}
```

## Enabling SHACL

SHACL is enforced only where the ledger config sets `f:shaclEnabled true`, ledger-wide or for a graph. Shapes on their own never enable it. A ledger that holds shapes without that setting reports `shapes present; SHACL enforcement not configured` in ledger info (`configDiagnostics`, also printed by `fluree info`).

Attach the setting to the ledger's existing config subject: a ledger has one `f:LedgerConfig` subject, and a second one is refused. When the ledger has a config, this attaches a SHACL group to it (and does nothing when there is none):

```sparql
PREFIX f: <https://ns.flur.ee/db#>

INSERT {
  GRAPH <urn:fluree:mydb:main#config> {
    ?c f:shaclDefaults <urn:fluree:mydb:main:config:shacl> .
    <urn:fluree:mydb:main:config:shacl> f:shaclEnabled true .
  }
}
WHERE {
  GRAPH <urn:fluree:mydb:main#config> { ?c a f:LedgerConfig }
}
```

When the ledger has no config yet, write one:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
      f:shaclDefaults <urn:fluree:mydb:main:config:shacl> .
    <urn:fluree:mydb:main:config:shacl> f:shaclEnabled true .
  }
}
```

If the config already has an `f:shaclDefaults` group, flip its `f:shaclEnabled` with `DELETE`/`INSERT` (see [Updating config](#updating-config)) rather than adding a second group, which is refused.

## Updating config

Config changes are normal ledger operations. To change a setting, use a `DELETE/INSERT WHERE` pattern that binds the existing blank node:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

DELETE {
  GRAPH <urn:fluree:mydb:main#config> {
    ?policy f:defaultAllow false .
  }
}
INSERT {
  GRAPH <urn:fluree:mydb:main#config> {
    ?policy f:defaultAllow true .
  }
}
WHERE {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main:config:ledger> f:policyDefaults ?policy .
    ?policy f:defaultAllow false .
  }
}
```

This pattern binds `?policy` to the existing setting-group blank node, retracts the old value, and asserts the new one. It avoids the problem of `DELETE DATA` with blank nodes (which cannot match stored blank node identities).

Alternatively, give setting-group nodes explicit IRIs so they can be addressed directly:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults <urn:fluree:mydb:main:config:policy> .

  <urn:fluree:mydb:main:config:policy>
    f:defaultAllow false ;
    f:overrideControl f:OverrideAll .
}
```

With explicit IRIs, individual fields can be retracted by subject IRI without binding.

Retracting a field returns the ledger to the system default for that setting (as if the field were absent).

## What a config write is checked for

A transaction that writes ledger config is checked before it commits, and refused (a `Parse error`, HTTP 400) when what it writes would not do what it says:

| Refused | Why | Write it instead |
|---|---|---|
| A setting group linked from the config graph whose fields are written to another graph | The reader reads groups only from the config graph, so the group reads as empty | Put the group's fields in the config graph too (in JSON-LD, nest the group under the config node, or give it `"@graph": "config"`) |
| An `f:LedgerConfig` or `f:GraphConfig` written outside the config graph | Config is read only from the config graph, so the write has no effect | Write it to `urn:fluree:{ledger_id}#config` (or `"@graph": "config"`) |
| A second value for a single-valued setting (`f:shaclEnabled`, `f:defaultAllow`, a group pointer such as `f:shaclDefaults`, ...), or a second `f:LedgerConfig` subject | The reader would pick one of them | Upsert, or delete the old value in the same transaction; add settings to the existing config subject |
| An unrecognized `f:reasoningModes` value | Query-time reasoning would skip it silently | Use a supported mode name |

Writing the same value again is not refused. The checks read only the transaction and the current config graph.

A transaction that writes only the config graph is never validated against SHACL shapes or uniqueness constraints, and never needs a shapes, schema or constraints source to be available: you can always turn SHACL or uniqueness off, or point a source somewhere else, even when the source it names is gone. Policy is the exception. A config write is still subject to the ledger's policy, including a policy source in another ledger, because policy decides who may change the config.

## Repairing a config split across graphs

Before this release, a JSON-LD config written with nested setting groups (the form in [Writing via JSON-LD](#writing-via-json-ld)) put the groups' fields in the default graph, where the config reader never looks. The group reads as empty, so the setting it held is off: for a policy group, `f:defaultAllow false` is lost and anonymous reads and writes are open; for a SHACL group, validation is off. Ledger info reports such configs as `empty-group` and `stranded-fields` diagnostics.

This SPARQL moves the stranded fields into the config graph in one transaction, restoring exactly what was written. Run it with `SELECT ?n ?p ?o` in place of the `DELETE`/`INSERT` first to see what it will move. It covers fields in the default graph; for another graph, wrap the last two patterns in `GRAPH <g> { }`.

```sparql
PREFIX f: <https://ns.flur.ee/db#>

DELETE { ?n ?p ?o }
INSERT { GRAPH <urn:fluree:mydb:main#config> { ?n ?p ?o } }
WHERE {
  GRAPH <urn:fluree:mydb:main#config> { ?parent ?edge ?root }
  VALUES ?edge { f:policyDefaults f:shaclDefaults f:reasoningDefaults f:datalogDefaults
                 f:transactDefaults f:fullTextDefaults f:servingDefaults f:graphOverrides }
  ?root (f:overrideControl|f:shapesSource|f:policySource|f:schemaSource|f:rulesSource|
         f:constraintsSource|f:graphSource|f:trustPolicy|f:rollbackGuard|f:ontologyImportMap|
         f:graphRef|f:property|f:shaclDefaults|f:policyDefaults|f:reasoningDefaults|
         f:datalogDefaults|f:transactDefaults|f:fullTextDefaults)* ?n .
  ?n ?p ?o .
}
```

Upserting the corrected config also restores the settings, but leaves the stray fields behind in the default graph.

## Config mutation governance

Config writes go through the normal policy-enforced transaction path. This means:

- **Reading** config is privileged (system read, bypasses policy) — necessary to bootstrap.
- **Writing** config is **not** privileged — policy enforcement applies.

A `defaultAllow: false` config is self-protecting: the policy it defines must explicitly grant write access to the config graph for any changes to be possible.

If a ledger becomes unmodifiable due to a policy misconfiguration (no authorized config writers), recovery requires a ledger fork/restore — there is no superuser bypass.

## Recommended subject IRI

For operational simplicity, use a stable, conventional subject IRI:

```
urn:fluree:{ledger_id}:config:ledger
```

Colons (not a second `#` fragment) keep the IRI well-formed: the graph IRI already uses a fragment (`#config`), and RFC 3986 allows only one fragment per IRI. Using colons produces a valid URN (RFC 8141) that stays scoped to the ledger and avoids accidental multiple-config instances.

## Querying the config graph

The config graph is a named graph like any other — you can query it with SPARQL or JSON-LD to inspect the current configuration.

### SPARQL

```sparql
PREFIX f: <https://ns.flur.ee/db#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?setting ?pred ?val
FROM <mydb:main#config>
WHERE {
  ?config rdf:type f:LedgerConfig ;
          ?setting ?group .
  ?group ?pred ?val .
  FILTER(?setting IN (
    f:policyDefaults, f:shaclDefaults, f:reasoningDefaults,
    f:datalogDefaults, f:transactDefaults
  ))
}
```

### JSON-LD query

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "from": {
    "@id": "mydb:main",
    "graph": "urn:fluree:mydb:main#config"
  },
  "select": ["?config", "?pred", "?val"],
  "where": [
    { "@id": "?config", "@type": "f:LedgerConfig", "?pred": "?val" }
  ]
}
```

### Ledger-scoped endpoint

```bash
curl -X POST "http://localhost:8090/v1/fluree/query/mydb:main" \
  -H "Content-Type: application/sparql-query" \
  -d 'PREFIX f: <https://ns.flur.ee/db#>
      SELECT ?s ?p ?o
      FROM <urn:fluree:mydb:main#config>
      WHERE { ?s ?p ?o }'
```

### Policy applies to reads

User queries against the config graph go through normal **policy enforcement**. If `f:defaultAllow` is `false` and no policy grants read access to the config graph, user queries will return empty results. The *system* still reads config via a privileged path (bypassing policy), so config always takes effect regardless of policy.

### Time-travel

Config is part of the ledger's immutable commit chain. You can query config at any historical point:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?setting ?val
FROM <mydb:main@t:5#config>
WHERE {
  ?config a f:LedgerConfig ;
          f:policyDefaults ?policy .
  ?policy ?setting ?val .
}
```

## Lagging semantics

Config changes take effect on the **next** transaction. The transaction pipeline reads config from the pre-transaction state (`t - 1`). This prevents a transaction from changing the rules it is validated against.

This means:
- Enabling SHACL in the same transaction as invalid data will **not** reject that data
- Enabling `f:uniqueEnabled` in the same transaction as duplicate values will **not** reject those duplicates
- The next transaction after the config change will be validated against the new config
