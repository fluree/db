# Upgrading from 4.2 and earlier

This release changes how Fluree keeps track of ledgers. A ledger's name and
the ledger itself are now separate things: the name points at one ledger at a
time, dropping a ledger frees its name at once, and a dropped ledger can be
restored or purged later. To make this safe, every writer is checked against
the ledger it loaded, so a process still holding a dropped ledger cannot write
into whatever holds the name next.

The upgrade happens by itself the first time this release starts against an
existing store. It rewrites nameservice metadata only; no commit, index or
other ledger file is moved or rewritten.

**The one rule:** every process that reads or writes a store must run the same
release. Stop them all, upgrade them all, then start them. An older release
left running against an upgraded store is stopped from writing where this
release can arrange it: on file and object stores the upgrade itself makes the
old release fail, and on DynamoDB you can deny it write access with IAM (see
[Why every process at once](#why-every-process-at-once) and
[Deployments that cannot stop at once](#deployments-that-cannot-stop-at-once)).

## What changes

### For operators

- **One ledger per name.** A name holds one ledger, with the branches created
  from it. In 4.2, `mydb:a` and `mydb:b` could be created as two unrelated
  ledgers; now a create of `mydb:b` while `mydb` exists fails with `409`, and
  a new branch is made with [`fluree branch create`](../cli/branch.md). A 4.2
  store that already has two such ledgers keeps them, as two branches of one
  ledger: dropping the name drops both.
- **Where data lives.** Every ledger created from now on gets a folder of its
  own, `mydb/@{instance}/`, where the instance is an id fixed when the ledger
  is created. A ledger dropped and created again under the same name never
  shares files with the one before it, and backing up `mydb/` captures every
  incarnation. Existing ledgers stay where they are (`mydb/main/…`,
  `mydb/@shared/…`) and go on working there.
- **Dropping frees the name.** In 4.2 a soft drop marked the ledger retracted
  and kept its name reserved. Now the name is free as soon as the drop
  returns, and the dropped ledger goes to a list of dropped ledgers
  ([`fluree dropped`](../cli/dropped.md), [`GET /dropped`](../api/endpoints.md#get-dropped)),
  from which it can be restored under its name or purged.
- **Ledgers 4.2 soft-dropped are moved to that list** by the upgrade, and
  their names become free.
- **`fluree drop` keeps the data by default.** The 4.2 CLI deleted it. Scripts
  that relied on `fluree drop` freeing disk space need `--hard --force`.
  `POST /drop` was already soft by default.
- **Interrupted operations finish themselves.** A drop, restore or purge that
  a crash interrupted is resumed, and a create or import that stopped is rolled
  back, by the background indexer's periodic tick; see
  [Periodic maintenance](configuration.md#periodic-maintenance). A new option,
  `--orphan-sweep-interval-secs`, schedules the sweep that deletes storage no
  ledger owns.
- **Graph sources are tied to one ledger.** A BM25 or vector index built from a
  ledger, and an Iceberg, SQL or Delta source governed by a model ledger, is
  suspended if that ledger is dropped and another created under its name,
  rather than silently switching to the new one
  ([BM25](../indexing-and-search/bm25.md#when-the-source-ledger-is-dropped),
  [Iceberg](../graph-sources/iceberg.md#where-policies-live-the-model-ledger)).
- **Key rotation covers dropped ledgers**, so retiring a key never leaves a
  restorable ledger unreadable.
- **One process per storage directory.** Every process that opens a file
  storage directory now locks it: the server, CLI commands, the Python package
  and Rust programs. Another process that opens it meanwhile is refused with
  the holder's pid. A CLI command waits for another command to finish with it
  (30 seconds, `FLUREE_STORAGE_LOCK_WAIT_SECS`). With a server running, most
  commands go through it, `create` and `use` now included; the few that cannot
  yet (`index`, `verify`, `config set-origins`, `graphql`, `track add`,
  `materialize`, `doc`, and `clone`/`pull`/`push`/`fetch`/`publish`) are
  refused until it stops. Shared-storage query peers and Raft nodes take no lock; a
  Rust program that reads a directory another process owns opts out with
  `FlureeBuilder::without_storage_lock()`. A program that embeds a Raft node
  builds its engine with `FlureeBuilder::with_storage_wal_owner(<node id>)`, as
  the server does; without it the node locks the shared store and a second node
  on it is refused. See
  [One process per directory](storage.md#one-process-per-directory).
- **`fluree memory` no longer opens the project's storage.** In a project, its
  ledger is a cache in memory, rebuilt from `.fluree-memory/*.ttl` on each run.
  The `__memory` ledger earlier releases kept in `.fluree/storage` is no longer
  read.

### For API clients

| Change | Where |
|---|---|
| New `409` errors: `err:db/Fenced` (a write to a ledger dropped or restored since it was loaded), `err:db/LifecycleConflict` (another create, drop or restore holds the name), `err:db/GraphSourceSuspended` | [Errors](../api/errors.md) |
| Creating `name:branch` for a name that already holds a ledger fails with `409` | [`POST /create`](../api/endpoints.md) |
| The drop response adds `instance`, `name_released` and `data` | [`POST /drop`](../api/endpoints.md#post-drop) |
| New endpoints to list, restore, purge and sweep dropped ledgers | [`GET /dropped`](../api/endpoints.md#get-dropped) |
| Server-sent `ns-record` events carry the ledger's `instance` and `storage_root`; `ns-retracted` may carry `instance` | [Query peers](query-peers.md) |

Every existing response field keeps its name and meaning.

## What the first start does

On every backend the upgrade:

- binds each ledger to its name, with its data where it already is;
- gives every branch record a fence, the token a writer must present;
- moves each ledger whose branches 4.2 had all retracted into the list of
  dropped ledgers, freeing its name;
- on file and object stores, retires the old nameservice: each file under
  `ns@v2/` is kept under `ns@v2.bak/` and replaced with a note that 4.2 cannot
  read;
- records that the store is upgraded, so later starts skip all of this.

It is safe to run from several processes at once and to interrupt: the ids it
issues are derived from the ledger names, and it resumes where it stopped.
On file and object stores it resumes only while `ns@v2/` is as it found it:
if a 4.2 process has written there since an attempt stopped, the copy that
attempt left under `ns@v3/` is out of date, and the upgrade refuses to start
until you delete `ns@v3/`.
Its time grows with the number of ledgers and branches; expect seconds for a
small store.

| Backend | Where the upgraded metadata lives | The marker | Log line |
|---|---|---|---|
| File | copied from `ns@v2/` to `ns@v3/`; `ns@v2/` retired, its files kept under `ns@v2.bak/` | `ns@v3/@format.json` | `nameservice migrated from ns@v2 to ns@v3` |
| S3 and other object stores | the same, under the configured prefix | `ns@v3/@format.json` | the same |
| DynamoDB | in place: a `fence` on each item, and new binding and dropped-ledger items in the same table ([layout](dynamodb-guide.md)) | the `@format` item | `nameservice migrated to format 3` |
| Raft | in the replicated state, by the first leader elected on this release | the state itself | `bound the ledgers created before name bindings` |

The line reports how many ledgers were bound and how many were moved to the
dropped list, and is logged only when there was something to do.

## Before you upgrade

1. **List every process that touches the store.** Servers and query peers,
   search services (`fluree-search-httpd`, which upgrades the store it opens
   as a server does), CLIs working on a local store, applications that embed
   the Rust crate, and serverless functions. All of them must move to this release together. A CLI
   that only talks to a server through `--remote` does not touch the store and
   can be upgraded on its own schedule; a 4.2 CLI's `fluree drop --remote`
   still asks for a hard drop, as it always did.
2. **Take a backup you can roll back to.**
   - File and object stores: nothing to do. The upgrade keeps the files it
     retires under `ns@v2.bak/`; see [Rolling back](#rolling-back).
   - DynamoDB: take an on-demand backup or enable point-in-time recovery. The
     upgrade writes in place, and a backup is the only way back.
   - Raft: copy each node's `--raft-storage-path`.

   On every backend the backup names each ledger's index as it was at the
   upgrade, and the new release's index garbage collection deletes that index
   once newer ones replace it (by default, five newer builds and fifteen
   minutes later). To keep it for as long as you might roll back, raise
   `--gc-max-old-indexes` for that time, at the cost of the disk the old
   indexes hold. Otherwise a rollback reindexes the ledgers whose index was
   collected.
3. **Check your scripts** for `fluree drop` (now soft; add `--hard --force` to
   delete) and for creating separate ledgers as branches of one name.
4. **Check ledger names for `@` and `#`.** Older releases let a ledger
   or branch name contain them; this release reserves both. A file, object
   or DynamoDB store holding such a ledger is refused at the first start,
   with an error naming each one, before anything is written: the store
   still works with the release that wrote it. Move each to a valid name
   with that release, drop the old one, then upgrade. On Raft, the first
   upgraded leader binds every other ledger and logs an error naming the
   ones it left unbound, which no request can reach.
5. **Query peers.** A peer in shared storage mode opens its server's store, and
   upgrades it if it starts first, so it is one of the processes above. A peer
   in proxy storage mode keeps no store of its own: it can be upgraded before
   its server, but not after. A 4.2 peer works out a ledger's files from its
   name, so it cannot read a ledger created after the upgrade, and it ignores a
   ledger recreated under a name it already holds, because the new ledger's `t`
   starts again from 0.

## Why every process at once

The risk differs by backend.

**File and object stores.** A 4.2 process reads and writes only `ns@v2/`, and
this release reads only `ns@v3/`. Were `ns@v2/` left in place, the two would
keep separate books: a commit made by the 4.2 process would be invisible to
this release, and each side's history of the ledger would continue from its
own last commit. The usual way this happens is a 4.2 CLI on the same store as
an upgraded server, for example a CLI installed from Homebrew beside a server
in a container.

So the upgrade retires `ns@v2/`. A 4.2 process then fails on every read and
write of an existing ledger, instead of writing where this release never
looks: its commands report a serialization error, or that the ledger is not
found. It can still create a
ledger under a name `ns@v2/` never held, which this release does not see. This
release warns at start when that has happened: on a local filesystem it
notices any file under `ns@v2/` changed since the upgrade, and on an object
store any file added or removed there.

**DynamoDB.** 4.2 and this release read and write the same items, so a commit
made by a 4.2 process is visible to this release and history does not split.
What a 4.2 process gets wrong is everything that should go through a name's
binding:

- a ledger it creates is not bound, and this release does not see it;
- a ledger or branch it drops is not moved to the dropped list;
- a branch it creates is not listed;
- it can commit to a branch whose drop is in progress, since it does not check
  fences;
- once a ledger it holds has been dropped and created again under its name,
  it can commit into the new ledger: a commit stored the 4.2 way, whose parent
  is the dropped ledger's last commit. The new ledger's head starts again at
  `t` 0, so 4.2's only check, that `t` moves forward, passes.

Running the upgrade again binds the ledgers and lists the branches it created;
see [A ledger a 4.2 process created on DynamoDB](#a-ledger-a-42-process-created-on-dynamodb).

**Raft.** Raft clusters are upgraded one node at a time (see
[Rolling upgrades](raft-clusters.md#rolling-upgrades)). The upgrade runs on the
first leader elected on this release, through commands a 4.2 node cannot read,
so finish upgrading the followers before an upgraded node leads: upgrade the
leader last, as that procedure says.

## Deployments that cannot stop at once

Some deployments cannot stop every process at the same moment. On AWS Lambda,
invocations already running when a function is updated finish on the old code,
for up to the function's timeout, and a stack with many functions updates them
one after another. Traffic shifting between versions or provisioned
concurrency on an alias can stretch the overlap further.

On DynamoDB you can make the old release unable to write instead of stopping
it, with IAM. Deny the old code write access to the nameservice table before
the new code starts. IAM evaluates a role's policies on every request, so the
deny applies to invocations already running under that role too.

1. Create a new execution role for this release, with the same permissions as
   the current one.
2. Attach to the **current** role an explicit `Deny` of `dynamodb:PutItem`,
   `dynamodb:UpdateItem`, `dynamodb:DeleteItem`, `dynamodb:BatchWriteItem` and
   `dynamodb:TransactWriteItems` on the nameservice table. From here the old
   code can read but not write.
3. Wait for the deny to take effect. IAM changes propagate in seconds; allow a
   minute.
4. Deploy every function on this release under the new role. The first to
   start upgrades the table.
5. Remove the old role in a later deployment.

Writes that reach a function not yet updated fail between steps 2 and 4, so
plan for a short write outage; reads keep working throughout. Work an old
invocation loses to the deny, such as a queue message, is retried by the new
code when the message becomes visible again. Old code may still write ledger
files to S3; those are content-addressed and harmless.

In CloudFormation all five steps fit one stack update when the functions share
an execution role:

```yaml
Resources:
  LambdaExecutionRoleV2:
    Type: AWS::IAM::Role
    Properties:
      # The same trust policy and policies as LambdaExecutionRole.

  DenyLegacyNameserviceWrites:
    Type: AWS::IAM::Policy
    Properties:
      PolicyName: deny-nameservice-writes-before-upgrade
      Roles: [!Ref LambdaExecutionRole]
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Deny
            Action:
              - dynamodb:PutItem
              - dynamodb:UpdateItem
              - dynamodb:DeleteItem
              - dynamodb:BatchWriteItem
              - dynamodb:TransactWriteItems
            Resource: !GetAtt NameserviceTable.Arn

  # A custom resource that sleeps for a minute once the deny exists: IAM
  # reports the policy created before it is in effect everywhere.
  IamPropagationWait:
    Type: Custom::Sleep
    DependsOn: DenyLegacyNameserviceWrites
    Properties:
      ServiceToken: !GetAtt SleepFunction.Arn
      Seconds: 60

  TransactFunction:
    Type: AWS::Serverless::Function
    DependsOn: IamPropagationWait
    Properties:
      Role: !GetAtt LambdaExecutionRoleV2.Arn
      # ...
  # Every other function that used LambdaExecutionRole, likewise.
```

Keep `LambdaExecutionRole` and the deny in the template for this release, and
remove them in the next. Deploy this update with rollback disabled
(`--disable-rollback`): a rollback after the new code has upgraded the table
would put the old code back on an upgraded table. Fix a failed update forward
instead.

The same approach works for any DynamoDB deployment whose old and new
processes can run under different IAM identities. On file and object stores
the upgrade itself stops a 4.2 process from writing to existing ledgers;
stopping every process first still spares their users failed requests.

## After you upgrade

- Look for the log line above on the first process to start.
- `fluree dropped list` shows the ledgers 4.2 had soft-dropped, now
  restorable or purgeable, and `fluree list` shows the rest as before.
- Watch for a warning that files under `ns@v2/` changed after the migration:
  a process on the old release is still running, and has created a ledger
  this release does not see.

### A ledger a 4.2 process created on DynamoDB

On DynamoDB a 4.2 process still running after the upgrade can create a
ledger, which this release does not see: it is not listed, cannot be queried,
and creating the name again fails because it already exists. To find such
ledgers, scan the table for ledger `meta` items without a `fence`:

```bash
aws dynamodb scan --table-name <table> \
  --filter-expression "#sk = :meta AND #kind = :ledger AND attribute_not_exists(#fence)" \
  --expression-attribute-names '{"#sk":"sk","#kind":"kind","#fence":"fence","#pk":"pk"}' \
  --expression-attribute-values '{":meta":{"S":"meta"},":ledger":{"S":"ledger"}}' \
  --projection-expression "#pk"
```

To bring them in, stop every 4.2 process, delete the `@format` item, and start
one process on this release:

```bash
aws dynamodb delete-item --table-name <table> \
  --key '{"pk":{"S":"@format"},"sk":{"S":"meta"}}'
```

The upgrade runs again. It binds each ledger it finds unbound, and lists a
branch a 4.2 process added to an upgraded ledger; ledgers and branches this
release created, and those already upgraded, are left as they are. Its log
line names what it bound.

## Rolling back

- **File and object stores:** stop every process, then move `ns@v2.bak/`
  back in place of `ns@v2/`, leaving no `ns@v2.bak/` behind: a later upgrade
  keeps a file it retires only where `ns@v2.bak/` holds none, so a copy left
  there would keep this upgrade's state in place of the next one's. A 4.2
  release then sees the ledgers as they were at the upgrade. Commits made
  since are not in them, and ledgers created since live in folders 4.2
  cannot read. To upgrade again later, delete `ns@v3/` first, so the upgrade
  starts from what 4.2 wrote; what this release wrote before the rollback is
  then lost, and the orphan sweep deletes the folders of ledgers it created.
- **DynamoDB:** restore the backup taken before the upgrade. Running 4.2
  against the upgraded table is not supported.
- **Raft:** see [Rolling upgrades](raft-clusters.md#rolling-upgrades); a
  downgraded node restores its `--raft-storage-path` from before the upgrade
  or rejoins and takes a snapshot.

On every backend, after the rollback:

- A ledger whose index the new release collected fails to load. Its commits
  are all kept: `fluree reindex <ledger>` on 4.2 rebuilds the index from them.
- A ledger hard-dropped or purged since the upgrade is listed again, but its
  files are gone. Drop it again on 4.2.

## A newer release on the same store

A release refuses to start against a store that a newer release has moved to
a format it does not understand, on every backend. A newer release can also
stop running processes of this one without a restart: a name whose binding it
has rewritten in a form this release does not recognize is an error here, not
a free name, so nothing reads, writes or creates under it.
