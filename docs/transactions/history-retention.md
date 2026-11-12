# History retention: `f:noHistory` and targeted removal

Fluree keeps every assertion and retraction, so a query can read any past state. Two retention features change that:

- **`f:noHistory`** on a predicate: the index keeps only the predicate's current values. A counter that changes often stops adding to the index's history on every change. No read is refused; a read of an earlier point sees a current value from the transaction that asserted it.
- **Targeted removal**: selected occurrences of a fact stop being served, now and in history. One that is still current is retracted. The commit log still holds the original operations, for audit access.

Neither is erasure: the commit log, backups and copies of the log still hold the values.

Ledgers that use neither feature behave exactly as before. `f:noHistory` changes only what the index holds: queries take the same paths, and commit log reads need no extra permission unless a removal was made.

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

Reads of `ex:count` are never refused. Once its changes are indexed, its earlier values are gone from the index, and every read answers from what is left:

- a query as of an earlier point (`ledger@t:5`, `@iso:`, `@commit:`) sees a current value from the transaction that asserted it, and no value of `ex:count` before then;
- a history range (`from` … `to`) lists only the current values' assertions;
- other predicates keep their full history, in the same query. A graph crawl (`select {"?s": ["*"]}`) as of an earlier point returns the subject's other properties as they were then.

Changes not yet indexed still carry their history. Until the background indexer next runs, a read as of an earlier point can see a superseded value, and the same read stops seeing it once the change is indexed.

The same `f:noHistory` entry can carry an `f:ttl` too: values expire, and once indexed, their history goes too.

### Coverage

- **Enabling covers existing history.** The values the predicate had before the config commit leave the index too.
- **Disabling covers only the past.** `f:noHistory false`, or removing the entry, ends the rule at that config commit. History written from then on is kept. History from the covered period stays out of the index, except a value that was still current when the rule ended: it keeps its whole span, so a read between its assertion and its later retraction still sees it.
- **Graphs.** A ledger-wide entry governs the predicate in every data graph. A graph override with `f:noHistory false` keeps that graph's history.

A rule takes effect when the retention worker next runs, within `--retention-interval-secs` (15 seconds by default) of the config commit. Until then, index builds keep the history. Status shows the rule once it is recorded.

An index build applies each rule only through the last commit the worker has saved as processed (status `watermarkT`). The worker saves it about every 30 seconds while commits arrive, so it can trail the head. History written after that point is kept, because a config commit there may have ended the rule; a later build that rewrites its leaf leaves it out if the rule still covers it.

### The index

The background indexer leaves the covered history out of the index, in incremental builds as well as full rebuilds, and for graphs with index trees of their own as well as small graphs that share them (see [graph placement](../ledger-config/setting-groups.md#indexing-defaults)). History indexed before the rule was recorded stays in the index until a build rewrites the leaf that holds it, and reads as of an earlier point can see it until then. In shared trees, some reads can see it until the trees fold in their pending changes. A [reindex](../indexing-and-search/reindex.md) drops it at once, through the last commit the worker has saved as processed.

The [geospatial](../indexing-and-search/geospatial.md#s2-spatial-index-complex-geometries) index of polygons and lines keeps the predicate's history: a spatial search as of an earlier point can match a geometry the predicate has since replaced.

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
| `matching[].object` | The value: a JSON string (an `xsd:string`, not the same text with a language or another datatype), number (the number under any numeric datatype) or boolean; `{"@id": "..."}` for a reference; or a value object, `{"@value": "...", "@type": "<datatype>"}` or `{"@value": "...", "@language": "<tag>"}`. A selector has no `@context`: a datatype is an absolute IRI, `@json`, or an `xsd:` or `rdf:` compact IRI. Absent selects every value; a `null`, or a misspelled field anywhere in the request, is refused |
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
- A historical read that could see a removed occurrence fails with `410` `err:db/HistoryUnavailable`. A Cypher query, and a query under the RDF list view (`rdfLists`), reads properties its patterns do not name, so at an earlier point either is refused when any removal covers that point. A read whose pattern has a different constant subject is still answered, as is one that never touches the predicate. This stays true after the occurrence's commit is [permanently erased](permanent-erasure.md), since the journal records the occurrence's subject itself.
- Other occurrences of the fact are unaffected. So is the same value asserted again later: that is a new occurrence.

## Search indexes and exports

Full-text (BM25) and vector graph sources index documents built from their source ledger, so a removal can survive in them until they sync. A search of such a graph source fails with `410` `err:db/HistoryUnavailable` while:

- it has not yet indexed a removal of its source ledger;
- it is read as of an earlier snapshot of a source ledger with a removal.

Only removals of properties the graph source reads count: its indexing query's properties and, for a vector index, its embedding property. An index of `ex:bio` is served behind a removal of `ex:email`. When the indexing query's properties cannot all be listed (a `*` select, a variable predicate, a `@path` alias, patterns on another subject, or reasoning, which infers from properties the query does not name), every removal counts.

It is served again once it syncs. An export as of an earlier point is refused like a query that reads every predicate.

`f:noHistory` refuses no search and no export. A graph source that lags its ledger can match a superseded value until it syncs, and an earlier snapshot holds the values it was built from.

## Audit access

On a ledger whose journal records a removal, raw content is audit data: its commits, and an index built before the removal and its dictionaries, which still hold the removed values. These reads need the `fluree.ledger.audit.all` or `fluree.ledger.audit.ledgers` [token claim](../security/authentication.md#scope-claims), and answer `403` without it:

- commit show (`GET /show/{ledger}`), commit export (`GET /commits/{ledger}`) and pack (`POST /pack/{ledger}`);
- storage-proxy reads of every blob but the ledger's config (commits, transactions, index roots, branches, leaves and sidecars, dictionaries, spatial and statistics artifacts), for a query peer too, and vended storage credentials;
- the change and conflict details of a merge preview.

Ordinary read, history, export and storage scopes do not imply audit access. On ledgers without a removal, these reads keep their usual permissions; an `f:noHistory` rule alone does not change them.

## Clusters

The journal travels through the normal nameservice replication channel. Configuration revisions accompany record updates and reconnect snapshots, including changes that do not move a commit head. A node serves the journal it has learned: current queries, historical queries, packs, exports and storage-proxy reads make no leader round trip. A partitioned or lagging node may therefore serve history removed elsewhere until replication delivers the change.

Every read still checks local erasure installation. A node that has applied an erasure but has not installed it refuses affected reads with retryable `503` `err:db/ReplicaNotCurrent`. Installing an erasure changes what the node serves; physical deletion is the separate purge phase. Write-side erasure proving and startup/rejoin coordination still catch up with the cluster.

Records carry the version of their runtime configuration. An explicit version zero means no configuration or journal; an absent version in older records is unknown and requires a read. A query of a ledger without a removal reads nothing for retention, and a ledger's journal is read once per configuration revision, not once per commit. A process that shares its storage with other writers (S3, or a file root opened without its lock) sees none of their changes as events: it serves the journal it has learned and looks the ledger up again in the background at most once a second while it is queried. A read that a removal could refuse (time travel, history, a search index, or raw storage) and that finds what the process learned more than two seconds old, as the first one after a quiet spell does, looks the ledger up before it is decided. A removal made through another process therefore reaches such reads within about two seconds; a query of current state never waits on the lookup.

A removal returns after every replica has applied it, or after 10 seconds. The receipt's `unacknowledgedReplicas` lists any replica that had not:

```json
{ "unacknowledgedReplicas": ["node 3 (http://10.0.0.7:9090/raft)"] }
```

An unacknowledged replica may continue serving its earlier journal until it catches up. The acknowledgement list covers tracked cluster members, not independent query peers or disconnected copies.

## Status

`GET /v1/fluree/retention/{ledger}` includes the journal, for a token with the
`fluree.ledger.audit.*` permission; other readers see only how many removals it holds
(`removals`):

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
    }
  },
  "maintenanceRunning": true
}
```

A history rule that has been turned off carries `untilT`, the config commit that ended it. The journal records predicates, transaction ranges and occurrence positions, and for each removed occurrence a `subject`: a SHA-256 fingerprint of its subject's IRI, which reads are matched against. It never records the removed values. A fingerprint identifies the subject without spelling out its IRI, but anyone can hash a guessed IRI and compare.

## Limits

- **Not erasure.** The commit log, backups and copies of the ledger keep the original values. [Permanent erasure](permanent-erasure.md) replaces the commits themselves.
- **Branches.** A removal is made on one branch. A branch created from one with a journal starts with that journal, since it holds the same history. A branch that already existed when its source removed an occurrence is not covered: it keeps serving the occurrence at earlier points until the removal is requested on it too. (Raw reads through it still need audit access, since they reach the source's commits, and a [permanent erasure](permanent-erasure.md) removes from every branch that holds the occurrence.) A merge or a rebase brings the source's commits onto a branch, and with them the source's removals: the branch then refuses the same history, and its raw content becomes audit data. After a merge that does not fast-forward, the branch refuses the removed fact over the same span of `t` on its own line too. A rebase that would replay a commit one of the branch's own removals names is refused with `409`, since the replay would write the occurrence again in a new commit no removal names; merge the source into the branch instead. A branch created at an earlier point (`--at`) can hold a removed occurrence as current; it is retracted before the branch is visible, so the new branch's head is that retraction, one commit past the point asked for. Creating such a branch replays the history to the point; meanwhile erasure requests, publications and purges, and restores from archives, wait for it. Other branch creations and restores of dropped ledgers do not, unless one of those is already waiting: then they wait behind it.
- **Activation delay.** `f:noHistory` takes effect at the retention worker's next pass. A ledger with no maintenance owner running records no rules, and its index builds keep the property's history; an embedding application without one calls `Fluree::run_retention`, and a host with no process between requests installs the build hook and schedules passes (see [TTL](ttl.md#hosts-without-a-background-process)).
- **Removal is not undone** by retracting or re-asserting data, or by resetting a branch. The journal lives in the branch's nameservice record, outside ledger content.
- **Drops and restores.** A soft drop keeps each branch's retention state with the dropped ledger: its removal journal, expiry schedule and erasure requests. A restore reinstates it before the ledger is visible, so the restored ledger refuses the same history and its pending work resumes. Like an archive restore, it also takes what the lineage's checkpoint holds, so a removal made on another copy while the ledger was dropped is refused too, and retracted if the restored ledger still holds it as current. A soft drop whose retention state no longer fits in the nameservice's dropped-ledger entry (the limit some backends, such as DynamoDB, have) is refused and undone, leaving the ledger live. A ledger dropped by an earlier version kept none of this; its restore takes the journal from the lineage's checkpoint alone, and its erasure requests must be made again.
- **Archives and restores.** Every journal change is also written to a checkpoint for the ledger's lineage (its genesis commit, which every copy shares), kept in storage under `@retention/journal/` outside any ledger's folder; drops, purges and the orphan sweep leave it. A checkpoint is a set of immutable objects whose union is the lineage's journal, so copies changing their journals at once each add to it. A removal or `f:noHistory` change that cannot write the checkpoint reports failure; a retry, or the worker's next pass, writes it. A ledger restored from a `.flpack` archive takes the archive's journal and that checkpoint, so a copy restored from an archive made before a removal, even after its source is purged, refuses the same history. A removed occurrence the archive still held as current is retracted before the restored ledger becomes visible, and the restore's `commit_t` is that retraction; the same value asserted again after the removal is a different occurrence and stays. A restore fails, and leaves nothing behind, when the checkpoint cannot be read. The restored ledger rebuilds its expiry schedule from its commits, and keeps the history rules it inherits, even ones enabled after the archive was made.
- **Clones and pulls** copy the raw commit log; on a ledger with a removal, they need audit access. Through the pack protocol, the journal travels with the commits and the copy refuses the same history, from a named remote or an origin; a pull with no new commits still takes the journal. An origin pull's object-by-object fallback first discovers history changes through a pack request. The paginated export carries no journal.
- **Pushes** carry the journal too, and the server merges its removals into its own. Removals its journal lacks need the authority a removal request needs there. History rules are not taken from a push: the server derives them from the pushed config commits. A push with no new commits still carries the journal. A cluster takes the commits and keeps its own journal.

### Publication and synchronization

A targeted removal becomes visible to historical reads when its journal update publishes. Its retraction commit, if needed, publishes first; seeing that commit alone does not mean the removal is complete. A failed request can be retried. The receipt follows journal publication and its lineage checkpoint, then reports the acknowledgement wait. Reversing those writes would create a different partial-failure window.

Incremental pulls must synchronize changes to commits the receiver already holds, even when both heads are equal. Origin-based CLI pulls use an erasure-aware pack discovery round before falling back to individual object fetches. An origin that cannot provide that discovery produces an error instead of an “already up to date” claim; configure a pack-capable origin to continue.

New erasure maps record the namespaces holding each original at publication, and later copies register their addresses. An unrelated ledger's transfer therefore performs no probes for those erased commits. Older maps without ownership metadata retain conservative discovery probes.
