# Setting Groups

Each setting group configures a different subsystem. Groups are resolved independently — locking down one group does not affect others.

All setting groups can appear on both `f:LedgerConfig` (ledger-wide defaults) and `f:GraphConfig` (per-graph overrides), except where noted.

## System defaults

When no config graph is present (or a setting group is absent), the system defaults apply:

| Setting group | System default |
|---------------|----------------|
| Policy | `f:defaultAllow true` — all queries and transactions are permitted |
| SHACL | Disabled — no shape validation |
| Reasoning | Disabled — no OWL/RDFS inference |
| Datalog | Disabled — no rule evaluation |
| Transact constraints | Disabled — no uniqueness enforcement |
| Override control | `f:OverrideAll` — any request can override any setting |

In other words, an unconfigured ledger is **fully open**: no policy, no validation, no reasoning. This matches the behavior of a fresh ledger and ensures backward compatibility.

---

## Policy defaults

**Group predicate**: `f:policyDefaults`

Controls default policy enforcement behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:defaultAllow` | boolean | `true` | Allow (`true`) or deny (`false`) when no policy rule matches |
| `f:policySource` | `f:GraphRef` | (none) | Graph containing policy rules (`f:Allow`, `f:Modify`, etc.) |
| `f:policyClass` | IRI or list | (none) | Default policy classes to apply |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating (see [Override control](override-control.md)) |

`f:policySource` is non-overridable — it can only be changed by writing to the config graph, not at query time. `f:defaultAllow` and `f:policyClass` are overridable (subject to override control).

When `f:policySource` is set, the policy loader scans the specified graph for policy rules instead of the default graph. This keeps policy rules separate from end-user data. If `f:policySource` is not set, policies are loaded from the default graph (backward compatible).

**Current limitations**: `f:policySource` only supports same-ledger graphs. Cross-ledger references (`f:ledger`), temporal pinning (`f:atT`), trust policy, and rollback guard fields are parsed but will produce an error if configured.

### Example: policies in the default graph

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false ;
      f:policySource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:overrideControl f:OverrideAll
    ] .
}
```

### Example: policies in a named graph

Storing policy rules in a dedicated named graph keeps them out of the default data graph. The identity's `f:policyClass` triples must also be in the policy graph.

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false ;
      f:policySource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector <urn:fluree:mydb:main/policy> ]
      ]
    ] .
}
```

---

## SHACL defaults

**Group predicate**: `f:shaclDefaults`

Controls SHACL shape validation at transaction time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:shaclEnabled` | boolean | `false` | Enable or disable SHACL validation |
| `f:shapesSource` | `f:GraphRef` | (none) | Graph containing SHACL shapes |
| `f:validationMode` | IRI | `f:ValidationReject` | `f:ValidationReject` (reject invalid data) or `f:ValidationWarn` (log warning, allow) |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating |

`f:shapesSource` is non-overridable. `f:shaclEnabled` and `f:validationMode` are overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:shapesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:validationMode f:ValidationReject ;
      f:overrideControl f:OverrideNone
    ] .
}
```

---

## Reasoning defaults

**Group predicate**: `f:reasoningDefaults`

Controls OWL/RDFS reasoning applied at query time.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:reasoningModes` | IRI or list | (none) | Reasoning modes: `f:RDFS`, `f:OWL2QL`, `f:OWL2RL`, `f:Datalog` |
| `f:schemaSource` | `f:GraphRef` | (none) | Graph containing schema triples (`rdfs:subClassOf`, etc.) |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating |

`f:schemaSource` is non-overridable. `f:reasoningModes` is overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:reasoningDefaults [
      f:reasoningModes f:RDFS ;
      f:schemaSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:overrideControl f:OverrideAll
    ] .
}
```

---

## Datalog defaults

**Group predicate**: `f:datalogDefaults`

Controls Fluree's stored datalog rules (`f:rule`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:datalogEnabled` | boolean | `false` | Enable or disable datalog rule evaluation |
| `f:rulesSource` | `f:GraphRef` | (none) | Graph containing `f:rule` definitions |
| `f:allowQueryTimeRules` | boolean | `true` | Allow queries to supply ad-hoc rules |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating |

`f:rulesSource` is non-overridable. `f:datalogEnabled` and `f:allowQueryTimeRules` are overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:datalogDefaults [
      f:datalogEnabled true ;
      f:rulesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:allowQueryTimeRules false ;
      f:overrideControl f:OverrideNone
    ] .
}
```

---

## Transact defaults

**Group predicate**: `f:transactDefaults`

Controls transaction-time constraint enforcement, such as property value uniqueness.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:uniqueEnabled` | boolean | `false` | Enable unique constraint enforcement |
| `f:constraintsSource` | `f:GraphRef` or list | default graph | Graph(s) containing constraint annotations (e.g., `f:enforceUnique`) |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating |

When `f:uniqueEnabled` is `true` and `f:constraintsSource` is omitted, the default graph is used as the constraint source.

### Additive merge semantics

Unlike other setting groups where per-graph values **replace** ledger-wide values field-by-field, transact defaults use **additive** merge semantics:

- **`f:uniqueEnabled`**: Once enabled at the ledger level, it stays enabled for all graphs. Per-graph configs cannot disable it.
- **`f:constraintsSource`**: Per-graph sources are **added to** ledger-wide sources, not substituted. A graph checks annotations from all sources (ledger-wide + graph-specific).

This prevents a per-graph override from accidentally disabling enforcement or dropping constraint sources.

Note: additive merge is still subject to override control. If the ledger-wide `f:overrideControl` for `f:transactDefaults` is `f:OverrideNone`, per-graph additions are blocked entirely — the ledger-wide settings are final.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

# Define constraint annotations in the default graph
ex:email f:enforceUnique true .
ex:ssn   f:enforceUnique true .

# Enable enforcement via config
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true ;
      f:constraintsSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ]
    ] .
}
```

See [Unique constraints](unique-constraints.md) for full details on `f:enforceUnique`.

---

## Full-text defaults

**Group predicate**: `f:fullTextDefaults`

Declares properties whose string values should be indexed for BM25 full-text
scoring without requiring the `@fulltext` datatype per value, and sets the
default analyzer language for untagged plain strings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `f:defaultLanguage` | BCP-47 string | `"en"` | Analyzer language for plain (`xsd:string`) values on configured properties |
| `f:property` | `f:FullTextProperty` list | empty | One node per property to full-text index |
| `f:overrideControl` | IRI or object | `f:OverrideAll` | Override gating |

Each `f:property` entry is an `f:FullTextProperty` node carrying `f:target` —
the IRI of the property being indexed. Additional optional knobs (per-property
language, tokenizer, etc.) can be added to `f:FullTextProperty` in the future
without breaking the schema.

The `@fulltext` datatype retains its zero-config shortcut semantics: any value
tagged `@fulltext` always indexes as English, regardless of what
`f:fullTextDefaults` declares. Configured plain-string paths and
`@fulltext`-datatype English content share the same per-property English
arena — no duplication.

`rdf:langString` values auto-route to per-language arenas by their tag. An
unrecognized BCP-47 tag tokenizes + lowercases only (no stopwords, no
stemming) — consistent on both indexing and query sides.

### Additive merge semantics

Like `f:transactDefaults`, `f:fullTextDefaults` uses additive merge. Per-graph
`f:property` entries are appended to the ledger-wide list (deduping by
target IRI — per-graph wins on a collision). Per-graph `f:defaultLanguage`
shadows the ledger-wide value. Ledger-wide `f:OverrideNone` blocks per-graph
overrides entirely.

### Config changes require a manual reindex

Editing `f:fullTextDefaults` never triggers any indexing automatically. Arenas
reflect the config that was in effect at their build time; to pick up a
changed property list or default language, run a full reindex (`fluree
reindex …` or equivalent). Until then, existing arenas stay authoritative and
novelty written after the config change is scored with whatever language the
current effective config resolves to — which may produce temporarily
mismatched scoring until the reindex completes.

An in-flight reindex operates on a point-in-time snapshot and will not see a
config change committed during its run. Wait for the reindex to finish, then
trigger a new one against the post-change state.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:fullTextDefaults [
      a f:FullTextDefaults ;
      f:defaultLanguage "en" ;
      f:property [ a f:FullTextProperty ; f:target ex:title ] ,
                 [ a f:FullTextProperty ; f:target ex:body ]
    ] .
}
```

### Per-graph override example

```trig
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:fullTextDefaults [
      a f:FullTextDefaults ;
      f:defaultLanguage "en" ;
      f:property [ a f:FullTextProperty ; f:target ex:title ]
    ] ;
    f:graphOverrides [
      a f:GraphConfig ;
      f:targetGraph <urn:example:productCatalog> ;
      f:fullTextDefaults [
        a f:FullTextDefaults ;
        f:defaultLanguage "es" ;
        f:property [ a f:FullTextProperty ; f:target ex:productName ]
      ]
    ] .
}
```

Under this config, queries touching the `productCatalog` graph analyze
untagged plain strings as Spanish (`"es"`); other graphs keep English.
`ex:title` is full-text indexed everywhere (ledger-wide); `ex:productName`
is indexed only in the `productCatalog` graph.

See [Inline fulltext search](../indexing-and-search/fulltext.md) for the
end-user guide — when to pick this path over the `@fulltext` datatype,
supported languages, per-graph multilingual setups, the reindex workflow,
and how configured properties interact with `@fulltext`-datatype values.

---

## Ledger-scoped settings

Some settings are structurally tied to the ledger as a whole and are **not meaningful per-graph**. They live exclusively on `f:LedgerConfig` and are ignored if present on `f:GraphConfig`.

Override control does not apply to ledger-scoped settings — they are changed only by writing to the config graph.

> **Note:** `f:authzSource` (an identity/relationship graph used by policy evaluation) is planned as a ledger-scoped setting but is not yet implemented. When available, it will let the config graph specify which graph contains identity data (e.g., DID→role mappings) for policy resolution.

---

## `f:GraphRef`: referencing source graphs

Several fields (`f:policySource`, `f:shapesSource`, `f:schemaSource`, `f:rulesSource`, `f:constraintsSource`) use `f:GraphRef` to point at graphs containing rules, shapes, schema, or constraints.

A `f:GraphRef` has two levels: the outer node carries the type and optional trust/rollback settings, and a nested `f:graphSource` object carries the source coordinates:

| Field | Level | Type | Description |
|-------|-------|------|-------------|
| `f:graphSource` | `f:GraphRef` | object | Nested source coordinates (required) |
| `f:trustPolicy` | `f:GraphRef` | object | How to verify the referenced graph (future) |
| `f:rollbackGuard` | `f:GraphRef` | object | Freshness constraints (future) |
| `f:graphSelector` | `f:graphSource` | IRI | Target graph: `f:defaultGraph`, `f:txnMetaGraph`, or a named graph IRI |
| `f:ledger` | `f:graphSource` | IRI | Ledger identifier (for cross-ledger references; not yet supported for constraint sources) |
| `f:atT` | `f:graphSource` | integer | Pin to a specific transaction time (optional) |

For the common case of referencing a graph within the same ledger, only `f:graphSelector` is needed inside `f:graphSource`:

```trig
f:shapesSource [
  a f:GraphRef ;
  f:graphSource [ f:graphSelector f:defaultGraph ]
] .
```

For referencing the config graph itself (co-resident rules/shapes):

```trig
f:policySource [
  a f:GraphRef ;
  f:graphSource [ f:graphSelector <urn:fluree:mydb:main#config> ]
] .
```

Cross-ledger `f:GraphRef` (using `f:ledger` to reference another ledger) is defined in the schema but not yet supported for constraint source resolution. Currently, only local graph references are resolved.
