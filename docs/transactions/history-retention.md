# History retention: `f:noHistory` and targeted removal

Fluree keeps every assertion and retraction, so a query can read any past state. Two retention features narrow what that history serves:

- **`f:noHistory`** on a predicate: its superseded values stop being served. A frequently updated counter keeps only its current value in ordinary views.
- **Targeted removal**: selected occurrences of a fact stop being served, now and in history. One that is still current is retracted.

Both are about *serving*. The commit log still holds the original operations, for audit access. Neither is erasure: backups and copies of the log still hold the values.

Ledgers that use neither feature behave exactly as before. They take the same query paths, and their commit log reads need no extra permission.

## `f:noHistory`

Set `f:noHistory true` on a predicate's entry in the ledger's [retention defaults](../ledger-config/setting-groups.md#retention-defaults):

```json
{
  "@context": { "f": "https://ns.flur.ee/db#", "ex": "https://example.org/" },
  "@id": "urn:fluree:mydb:main:config:ledger",
  "@type": "f:LedgerConfig",
  "@graph": "config",
  "f:retentionDefaults": {
    "@id": "urn:fluree:mydb:main:config:retention",
    "@type": "f:RetentionDefaults",
    "f:property": [
      {
        "@id": "urn:fluree:mydb:main:config:retention:count",
        "@type": "f:RetentionProperty",
        "f:target": { "@id": "ex:count" },
        "f:noHistory": true
      }
    ]
  }
}
```

The current value of `ex:count` is always served. A read that might need one of its earlier values fails with `410` and `@type` `err:db/HistoryUnavailable` instead of answering from incomplete history. Three kinds of read can need them:

- a query as of an earlier point (`ledger@t:5`, `@iso:`, `@commit:`);
- a history range (`from` … `to`);
- a query of a view the application opened before a later commit: once the ledger has moved past it, the view is of an earlier point.

A view of a ledger that has since been dropped, or replaced by a new ledger under the same name, is refused (`404`) for every read, current or historical: its journal went with it.

A historical query that never touches the predicate is still answered. The check is per query, on the predicates its patterns name. A pattern with a variable predicate, a wildcard path, a graph crawl or hydration (`select {"?s": ["*"]}`), or reasoning counts as touching every predicate. So do shortest paths, geo searches and `SERVICE`.

The same `f:noHistory` entry can carry an `f:ttl` too: values expire, and their history is not served.

### Coverage

- **Enabling covers existing history.** The values the predicate had before the config commit are no longer served either.
- **Disabling covers only the past.** `f:noHistory false`, or removing the entry, ends the rule at that config commit. History written from then on is kept and served. History from the covered period stays unavailable, because the index no longer holds it.
- **Graphs.** A ledger-wide entry governs the predicate in every data graph. A graph override with `f:noHistory false` keeps that graph's history.

A rule takes effect when the retention worker next runs, within `--retention-interval-secs` (15 seconds by default) of the config commit. Until then, history is served as before. Status shows the rule once it is recorded.

### The index

The background indexer leaves the covered history out of the index, in incremental builds as well as full rebuilds. Enabling a rule also starts a background rebuild, so history indexed before the rule is dropped too. While that rebuild runs, reads that need the history are already refused. `historyBackfilled` in [status](#status) reaches the journal's `generation` when it finishes.

## Targeted removal

`POST /v1/fluree/remove/{ledger}` removes selected occurrences. The endpoint is admin-protected. The request selects facts, or names occurrences directly:

```json
{
  "matching": [
    {
      "subject": "https://example.org/alice",
      "predicate": "https://example.org/email",
      "object": "alice@old.example"
    }
  ]
}
```

| Field | Description |
|---|---|
| `matching[].subject`, `matching[].predicate` | Full IRIs of the fact |
| `matching[].object` | The value: a JSON string, number or boolean, or `{"@id": "..."}` for a reference. Absent selects every value |
| `matching[].graph` | Named graph IRI; absent for the default graph |
| `occurrences[]` | `{"commit": "<commit id>", "slot": n}`: one operation of one commit |

A selector expands into every assertion of the fact up to the current head, plus the retractions that ended them. An occurrence that is still current is retracted with an ordinary commit made by the system (`f:identity` `urn:fluree:system:retention`). Each removed occurrence is then recorded in the branch's removal journal.

```json
{
  "requestId": "removal-5d1e...",
  "scopeT": 41,
  "manifest": [{ "commit": "bafy...", "slot": 3 }],
  "retractedT": 42,
  "generation": 4
}
```

`manifest` lists every occurrence removed. `retractedT` is the commit that retracted the current ones, if there were any. A repeated request does not add a second journal entry for an occurrence already removed.

The removal is complete when the request returns: the journal records it and the lineage checkpoint (see [Limits](#limits)) holds it. If the checkpoint cannot be written, the request fails after the journal has recorded the removal; repeating it writes the checkpoint.

After the removal:

- Current queries no longer see the value, because it was retracted.
- A historical read that could see a removed occurrence fails with `410` `err:db/HistoryUnavailable`. A read whose pattern has a different constant subject is still answered, as is one that never touches the predicate. This stays true after the occurrence's commit is [permanently erased](permanent-erasure.md), since the journal records the occurrence's subject itself.
- Other occurrences of the fact are unaffected. So is the same value asserted again later: that is a new occurrence.

## Search indexes and exports

Full-text (BM25) and vector graph sources index documents built from their source ledger, so a removal or a superseded `f:noHistory` value can survive in them until they sync. A search of such a graph source fails with `410` `err:db/HistoryUnavailable` while:

- it has not yet indexed a removal of its source ledger;
- it lags the head of a source ledger that has an `f:noHistory` rule;
- it is read as of an earlier snapshot of a source ledger with either.

Only removals and rules of properties the graph source reads count: its indexing query's properties and, for a vector index, its embedding property. An index of `ex:bio` is served behind a removal of `ex:email`. When the indexing query's properties cannot all be listed (a `*` select, a variable predicate, a `@path` alias, patterns on another subject, or reasoning, which infers from properties the query does not name), every removal and rule counts.

It is served again once it syncs. An export as of an earlier point is refused like a query that reads every predicate.

## Audit access

On a ledger whose journal records a rule or a removal, raw commit content is audit data. These reads need the `fluree.ledger.audit.all` or `fluree.ledger.audit.ledgers` [token claim](../security/authentication.md#scope-claims), and answer `403` without it:

- commit show (`GET /show/{ledger}`), commit export (`GET /commits/{ledger}`) and pack (`POST /pack/{ledger}`);
- storage-proxy reads of commit, transaction and history-index blobs, and vended storage credentials;
- the change and conflict details of a merge preview.

Ordinary read, history, export and storage scopes do not imply audit access. On ledgers without a journal, these reads keep their usual permissions.

## Clusters

The journal is replicated with the rest of the cluster's nameservice state. A replica serves a ledger's history only after it confirms that its copy is current: it asks the leader for the cluster's committed position and applies through it. This covers every ledger, since a replica that has not yet applied a ledger's first retention registration cannot tell the ledger uses retention. A replica that cannot confirm answers `503` `err:db/ReplicaNotCurrent`, which is retryable; it has no leader, or cannot reach a quorum. Current-state reads on that replica continue, and are not checked.

This applies to every time-travel query and history range in a cluster, of any ledger, whether or not it uses retention. Before the query runs, the leader confirms its leadership with a round of heartbeats to a quorum, and a follower adds a round trip to the leader, so each such read needs a leader and a reachable quorum. Benchmarks of current-state queries do not measure this cost. A view held past a later commit is checked against the journal the node has applied, without the round trip.

A removal returns after every replica has applied it, or after 10 seconds. The receipt's `unacknowledgedReplicas` lists any replica that had not:

```json
{ "unacknowledgedReplicas": ["node 3 (http://10.0.0.7:9090/raft)"] }
```

Such a replica refuses the ledger's history until it catches up.

## Status

`GET /v1/fluree/retention/{ledger}` includes the journal:

```json
{
  "ledgerId": "mydb:main",
  "active": true,
  "registration": {
    "activeSinceT": 12,
    "watermarkT": 40,
    "journal": {
      "generation": 4,
      "historyRules": [
        { "predicate": "https://example.org/count", "fromT": 0, "enabledT": 18 }
      ],
      "removals": [
        {
          "requestId": "removal-5d1e...",
          "commit": "bafy...",
          "slot": 3,
          "predicate": "https://example.org/email",
          "fromT": 7,
          "untilT": 42,
          "scopeT": 41,
          "subject": "v1:4f0c..."
        }
      ]
    },
    "historyBackfilled": 4
  },
  "maintenanceRunning": true
}
```

A history rule that has been turned off carries `untilT`, the config commit that ended it. The journal records predicates, transaction ranges and occurrence positions, and for each removed occurrence a `subject`: a SHA-256 fingerprint of its subject's IRI, which reads are matched against. It never records the removed values. A fingerprint identifies the subject without spelling out its IRI, but anyone can hash a guessed IRI and compare.

## Limits

- **Not erasure.** The commit log, backups and copies of the ledger keep the original values. [Permanent erasure](permanent-erasure.md) replaces the commits themselves.
- **Branches.** A branch created from one with a journal starts with that journal, since it holds the same history.
- **Activation delay.** `f:noHistory` takes effect at the retention worker's next pass. A ledger with no maintenance owner running records no rules.
- **Removal is not undone** by retracting or re-asserting data, or by resetting a branch. The journal lives in the branch's nameservice record, outside ledger content.
- **Drops and restores.** A soft drop keeps each branch's retention state with the dropped ledger: its removal journal, expiry schedule and erasure requests. A restore reinstates it before the ledger is visible, so the restored ledger refuses the same history and its pending work resumes. Like an archive restore, it also takes what the lineage's checkpoint holds, so a removal made on another copy while the ledger was dropped is refused too, and retracted if the restored ledger still holds it as current. A soft drop whose retention state no longer fits in the nameservice's dropped-ledger entry (the limit some backends, such as DynamoDB, have) is refused and undone, leaving the ledger live. A ledger dropped by an earlier version kept none of this; its restore takes the journal from the lineage's checkpoint alone, and its erasure requests must be made again.
- **Archives and restores.** Every journal change is also written to a checkpoint for the ledger's lineage (its genesis commit, which every copy shares), kept in storage under `@retention/journal/` outside any ledger's folder; drops, purges and the orphan sweep leave it. A checkpoint is a set of immutable objects whose union is the lineage's journal, so copies changing their journals at once each add to it. A removal or `f:noHistory` change that cannot write the checkpoint reports failure; a retry, or the worker's next pass, writes it. A ledger restored from a `.flpack` archive takes the archive's journal and that checkpoint, so a copy restored from an archive made before a removal, even after its source is purged, refuses the same history. A removed occurrence the archive still held as current is retracted before the restored ledger becomes visible, and the restore's `commit_t` is that retraction; the same value asserted again after the removal is a different occurrence and stays. A restore fails, and leaves nothing behind, when the checkpoint cannot be read. The restored ledger rebuilds its expiry schedule from its commits, and keeps the history rules it inherits, even ones enabled after the archive was made.
- **Clones and pulls** copy the raw commit log; on a ledger with a journal, they need audit access. The copy they make does not carry the journal.
