# Two-tab live board

The demo for `@fluree/react`. Open it in two browser tabs, write in one, and
watch the other update. **There is no polling code in this app** — search
`src/` for an interval, a refetch, or an invalidation key and you will not
find one. A commit reaches every mounted query because the database says so.

The `rendered N×` counters are the point. They show which components React
actually re-rendered, so you can see that:

- voting on one note re-renders **only that row** — not its siblings, not the
  header, not the list container;
- the **Count** panel, which subscribes to a different query on the same
  ledger, re-runs on every commit but does not re-render when voting, because
  its result did not move;
- unchanged results cost zero renders even though the query really did
  re-execute.

## Run it (remote mode)

Two terminals.

```sh
# 1. a Fluree server (from the repo root)
cargo build --release -p fluree-db-server
./target/release/fluree-server \
  --listen-addr 127.0.0.1:8090 \
  --storage-path /tmp/fluree-demo

# 2. the demo (from fluree-react/demo)
npm install
npm run dev
```

Open <http://localhost:5173> — twice. The ledger (`demo/board`) is created on
first load. Vite proxies `/v1` to the server, so the demo is same-origin and
CORS never enters the story; point it elsewhere with `FLUREE_URL`.

## Peer mode

```
http://localhost:5173/?mode=peer&token=<bearer>
```

Peer mode runs the engine in a web worker instead of querying over HTTP. It
needs two things this repo builds separately:

1. `@fluree/db-wasm`'s generated glue — `wasm-pack` output in
   `fluree-db-wasm/pkg/` (see that package's README);
2. a server serving the blocks tier, and a bearer token with
   `fluree.storage.*` scope, passed as `&token=`.

Everything else is identical: same components, same hooks, same result
shapes. The only line that differs is the `createClient` call in
`src/main.tsx`.

Two behaviours differ by mode, and both fail loudly rather than silently:
peer mode cannot serve a time-anchored (`opts.at`) query — the in-browser
engine has no historical view — and cannot serve a format other than the
query language's own.

## The trap this demo fell into first

The first version of this app mapped the query result into fresh row objects
on every render:

```ts
const notes = data.results.bindings.map((b) => ({ id: …, text: …, votes: … }));
```

Everything still worked — and **every row re-rendered on every commit**. The
package had carefully preserved the identity of rows that did not change, and
that one `.map()` threw all of it away before React ever saw it. Nothing
warns you; the counters were the only reason it was caught.

So components here take the **raw binding** and convert per row
(`noteOf`). If you derive objects from query results, memoize the derivation
per row, or pass the row through and convert at the leaf. This is the single
easiest way to lose the benefit of the whole package.

(For the same reason there is no `<StrictMode>` here: its dev-only
double-render doubles every counter, and legible counters are the point.
That subscriptions survive StrictMode's double-mount is pinned by a unit
test instead.)

## What to show

1. Two tabs side by side. Add a note in the left tab; it appears in the right
   one immediately.
2. Vote on one note repeatedly. Only that row's counter climbs — in **both**
   tabs. The Count panel never moves.
3. Add a note. Now the Count panel re-renders, because its answer changed.
4. Stop the server. The connection dot goes amber and the data **stays on
   screen** (keep-last-good-data). Start it again; it reconnects and catches
   up.
