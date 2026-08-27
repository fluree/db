# Demoing the live board

A script for showing `@fluree/react` to the team. Read once, run cold.

The whole argument is **"the sibling rows did not re-render"** — and that is
invisible unless you tell people where to look. Every step below names what
to point at.

Budget ~10 minutes: 4 for setup, 5 to present, and one for the caveats
someone will ask about.

---

## 1. Setup (remote mode — do this one live)

Three terminals. Remote mode is the one to demo: it is reliable, it starts in
milliseconds, and the story does not depend on anything being warm.

> **You need a server built from PR #1730** (`fix/events-per-ledger-subscription`).
> No released build works — the SSE subscription 400s and nothing ever
> updates. This is the single most likely way the demo fails on you.

```sh
# 1. build the server (once; release, not debug — debug is far slower here)
cargo build --release -p fluree-db-server

# 2. run it on file storage
./target/release/fluree-server \
  --listen-addr 127.0.0.1:8090 \
  --storage-path /tmp/fluree-demo

# 3. the app
cd fluree-react/demo
npm install
npm run dev
```

Open <http://localhost:5173> **twice**, side by side. The ledger
(`demo/board`) is created on first load. Vite proxies `/v1` to the server, so
everything is same-origin and CORS never enters the story.

Add three or four notes before anyone is watching, so the board is not empty
when you start.

## 2. The script

**Say first:** "This app contains no polling code. No interval, no refetch,
no invalidation keys. Search the source afterwards — `useQuery` is the whole
read path."

### Step 1 — the setup shot

Both tabs, same data. Point at the header: **`live`**, and **`ledger head t = N`**.

> "Two browser tabs, one ledger. Each component subscribes to one query."

### Step 2 — the update (the easy half)

Vote on a note in the **left** tab. It changes in the **right** tab.

> "Nothing polled. The database announced the commit and the query re-ran."

This gets nods but it is not the interesting part. Move on quickly.

### Step 3 — the counters (the actual argument)

Point at the `rendered N×` badge on **each row** of the right-hand tab.

> "Here is the part that matters. Only the row I voted on re-rendered — its
> counter went to `2×`. Every other row is still `1×`. They are the same
> React elements they were before the commit."

Vote a few more times on the same note. Its counter climbs; **nothing else
moves**. Let the silence do the work — the still counters are the proof.

> "A commit arrived, the query re-ran, the results changed — and React
> re-rendered exactly one row."

### Step 4 — the second query (the part people don't expect)

Point at the **Count** panel and its counter.

> "This is a *different* query on the *same* ledger. Every commit re-runs it
> too. But voting doesn't change how many notes exist, so the result came
> back identical — and the component did not re-render at all. Its counter
> hasn't moved since the page loaded."

Now **add a note**. The Count panel's number goes up *and* its counter
finally ticks — while the existing rows stay put.

> "Now its answer changed, so now it re-renders. That is the whole product:
> you subscribe to a question, and you pay only when the answer changes."

### Step 5 — keep-last-good-data (optional, 20 seconds)

Stop the server. The connection dot goes amber; **the data stays on screen**.

> "The server is gone and the UI didn't blank out. Errors don't discard your
> last good data."

Start it again — it reconnects and catches up.

## 3. Showing peer mode (optional, and read the caveat first)

Peer mode runs the engine — Fluree itself, compiled to wasm — in a web
worker. Queries re-run **in the browser**, against a local snapshot, with no
round trip.

It works, and it is a genuinely different thing to show. But:

- it is **slower to first paint** than remote mode — a ~9 MB wasm download
  and compile, against remote's one HTTP query. The Board's
  `first data N ms` badge shows this in both modes, so don't claim
  otherwise; the win is update latency, not startup. A cold open is ~3.4 s
  today, and that cost is a known open engine issue — say so if asked rather
  than explaining it away;
- setup is heavier (below), so do it before the room is watching.

```sh
# generated wasm glue
cd fluree-db-wasm/js && npm install && node scripts/build.mjs

# a server with the storage tier on, plus a token — this script does all of it
node fluree-db-wasm/js/scripts/fluree-server.mjs   # see createIdentity / mintStorageProxyToken
```

The server needs `--storage-proxy-enabled` and
`--storage-proxy-trusted-issuer <did:key>`, and the token must carry
`fluree.storage.*` — the shipped `fluree-events-token` CLI **cannot** mint
that. Then open:

```
http://localhost:5173/?mode=peer&token=<token>
```

The token is moved into `sessionStorage` and stripped from the URL on load.

**What to say:** "Same components. Same hooks. The only line that changed is
the `createClient` call. This tab is running the database."

Then run steps 3 and 4 again — the counters behave identically, which is the
point: switching modes cannot change what a component sees.

## 4. Questions you will get

**"Is this just a websocket subscription?"** No — the server announces
*commits*, not query results. Each client decides what that means for its own
queries. In peer mode that decision happens locally, with no server work.

**"What happens with 50 subscriptions?"** In remote mode, one HTTP request
per subscription per commit, capped at 6 concurrent. That is the honest
current cost and the reason a multi-query endpoint is on the list. Peer mode
re-runs locally and doesn't have this shape.

**"Does it work today?"** Remote mode, against a server built from #1730 —
yes. Against a released server — no, and that PR is why. Peer mode — yes,
including with the block cache unavailable entirely; its open question is
cold-open *cost* (~3.4 s), not correctness.

**"How does it know nothing changed?"** In peer mode the worker hashes the
formatted result and ships **zero bytes** when it matches. In remote mode the
client diffs. Either way, unchanged rows keep their object identity on the
main thread, which is what lets `React.memo` skip them — that is what the
counters are showing you.

**"What's the catch for app authors?"** One, and it is easy to hit: if you
`.map()` query results into new objects during render, you throw the identity
away and every row re-renders again. Pass result rows down and convert at the
leaf. This demo got it wrong the first time and only the counters revealed
it.
