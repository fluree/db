#!/usr/bin/env node
// Phase-A exit gate: a BROWSER PEER against a REAL running `fluree-db-server`.
//
//   node scripts/build.mjs && node scripts/smoke-peer-server.mjs
//   FLUREE_SERVER_BIN=/path/to/fluree-server node scripts/smoke-peer-server.mjs
//   CHROME=/path/to/chrome node scripts/smoke-peer-server.mjs
//
// Every other test in this stack stops at a mock: native tests drive the
// browser driver with a mock job consumer, and `smoke-browser.mjs` drives the
// shipped package in a real Chrome but with no server behind it. This one
// closes the chain — real server, real storage proxy, real CID-verified block
// fetches, real SSE — and it is the only proof that the peer works end to end.
//
// What it does:
//   1. mints an Ed25519 did:key identity and a `fluree.storage.ledgers` JWS,
//      then starts `fluree-server` trusting exactly that issuer, storage
//      proxy ON (the production verification path, not `--insecure`);
//   2. creates a ledger over HTTP, pins `f:serveBlocks true` on it, and
//      inserts Alice + Bob;
//   3. loads the SHIPPED package in headless Chrome from a DIFFERENT origin
//      (so the server's CORS posture is exercised, not bypassed) and calls
//      `connect()` at the server through a recording reverse proxy;
//   4. asserts the query returns the named rows, and that the recorded
//      traffic shows real `GET /storage/objects/{cid}` fetches — the peer
//      computed locally over CAS bytes, it did not query-ship;
//   5. commits a THIRD row over the server's HTTP API from here (an external
//      writer), then asserts the page saw the SSE head change, re-queried at
//      the new head, and got the new row — the live loop, closed.
//
// Exit 0 only when every marker is present. A missing marker is a failure,
// never a pass.

import { existsSync } from "node:fs";
import { join } from "node:path";
import { awaitPageMarker, cdpConnect, evaluate, findChrome, launchChrome, openPage } from "./chrome.mjs";
import {
  createIdentity,
  mintStorageProxyToken,
  postJson,
  postText,
  startFlureeServer,
} from "./fluree-server.mjs";
import { startTap } from "./http-tap.mjs";
import { packageRoot, startServer } from "./serve.mjs";

const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS ?? 300_000);
const LEDGER = "peersmoke:main";
const CTX = { ex: "http://example.org/ns/" };

for (const f of ["dist/index.js", "dist/worker.js", "pkg/fluree_db_wasm_bg.wasm"]) {
  if (!existsSync(join(packageRoot, f))) {
    console.error(`missing ${f} — run \`node scripts/build.mjs\` first`);
    process.exit(2);
  }
}
const chromeBin = findChrome();
if (!chromeBin) {
  console.error("no Chrome/Chromium found; set CHROME=/path/to/binary");
  process.exit(2);
}

const cleanups = [];
const cleanup = () => {
  while (cleanups.length) {
    try { cleanups.pop()(); } catch { /* cleanup never decides the exit code */ }
  }
};
let exit = 1;
const deadline = setTimeout(() => {
  console.error(`FAIL: no result within ${TIMEOUT_MS} ms`);
  cleanup();
  process.exit(1);
}, TIMEOUT_MS);

/** Assert with a named marker; the message names what was expected AND seen. */
function check(label, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${label}: expected ${e}, got ${a}`);
  console.log(`  ok  ${label} = ${a}`);
}

let server;
try {
  // ── 1. Identity + server ────────────────────────────────────────────────
  const identity = createIdentity();
  const token = mintStorageProxyToken(identity, { ledgers: [LEDGER] });
  console.log(`storage-proxy issuer: ${identity.did}`);

  server = await startFlureeServer({ trustedIssuer: identity.did });
  cleanups.push(server.stop);
  console.log(`fluree-server up at ${server.url}`);

  // ── 2. Ledger + data, over the server's HTTP API ────────────────────────
  await postJson(`${server.apiBase}/create`, { ledger: LEDGER }, { expect: 201 });
  // Pin the serving posture explicitly. `f:serveBlocks` absent already means
  // "allowed", so this asserts the gate is engaged rather than relying on a
  // default the peer would silently depend on.
  await postText(
    `${server.apiBase}/upsert/${LEDGER}`,
    `@prefix f: <https://ns.flur.ee/db#> .
     GRAPH <urn:fluree:${LEDGER}#config> {
       <urn:cfg:main>    a                 f:LedgerConfig ;
                         f:servingDefaults <urn:cfg:serving> .
       <urn:cfg:serving> f:serveBlocks     true .
     }`,
    { contentType: "application/trig", expect: 200 },
  );
  await postJson(
    `${server.apiBase}/insert/${LEDGER}`,
    {
      "@context": CTX,
      insert: [
        { "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:knows": { "@id": "ex:bob" } },
        { "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob" },
      ],
    },
    { expect: 200 },
  );

  // The serving tier the peer will negotiate on, straight from the server.
  const nsRes = await fetch(`${server.apiBase}/storage/ns/${encodeURIComponent(LEDGER)}`, {
    headers: { authorization: `Bearer ${token}` },
  });
  const nsRecord = await nsRes.json();
  check("nameservice record status", nsRes.status, 200);
  check("advertised serving tiers", nsRecord.serving, ["query", "blocks"]);

  // ── 3. The tap + the page's origin ──────────────────────────────────────
  const tap = await startTap(server.url);
  cleanups.push(tap.close);
  const { server: pageServer, url: pageOrigin } = await startServer(packageRoot, 0);
  cleanups.push(() => pageServer.close());
  console.log(`page origin ${pageOrigin} → tap ${tap.url} → server ${server.url}`);

  // ── 4. Drive the browser ────────────────────────────────────────────────
  const chrome = launchChrome(chromeBin);
  cleanups.push(chrome.kill);
  const cdp = await cdpConnect(await chrome.wsUrl);
  cleanups.push(cdp.close);

  const pageUrl =
    `${pageOrigin}/demo/peer-smoke.html` +
    `?api=${encodeURIComponent(tap.apiBase)}` +
    `&token=${encodeURIComponent(token)}` +
    `&ledger=${encodeURIComponent(LEDGER)}`;
  const session = await openPage(cdp, pageUrl);

  // Handshake step 1: the page has connected, queried, and primed.
  const ready = await evaluate(
    cdp,
    session,
    `(async () => {
       for (let i = 0; i < 900 && !window.__peerReady; i++) await new Promise(r => setTimeout(r, 100));
       if (!window.__peerReady) return { ready: false, error: "page never installed window.__peerReady" };
       try { return await window.__peerReady; }
       catch (e) { return { ready: false, error: String(e?.stack ?? e) }; }
     })()`,
  );
  if (!ready?.ready) {
    const partial = await evaluate(cdp, session, `window.__peerSmoke ? window.__peerSmoke : null`);
    throw new Error(
      `peer never became ready: ${ready?.error ?? "unknown"}\npage report: ${JSON.stringify(partial, null, 2)}`,
    );
  }
  console.log(`peer ready at t=${ready.t}`);

  // Handshake step 2: an EXTERNAL writer commits a third row.
  await postJson(
    `${server.apiBase}/insert/${LEDGER}`,
    { "@context": CTX, insert: [{ "@id": "ex:carol", "@type": "ex:Person", "ex:name": "Carol" }] },
    { expect: 200 },
  );
  console.log("committed ex:carol over HTTP; waiting for the peer to see it…");

  // Handshake step 3: the page's full verdict.
  const r = await awaitPageMarker(cdp, session, "__peerSmoke", { tries: 1200 });
  console.log("page report:", JSON.stringify(r, null, 2));

  // ── 5. Assertions ───────────────────────────────────────────────────────
  if (r?.status !== "pass") throw new Error(`page reported failure: ${r?.error ?? "no report"}`);
  check("phases reached", r.phases, [
    "connected", "coldQuery", "warmQuery", "livePrimed",
    "headChanged", "reopened", "freshQuery", "liveUpdated",
  ]);
  check("token requested once, at connect", r.tokenReasons, ["connect"]);
  check("cold query rows", r.coldNames, ["Alice", "Bob"]);
  check("warm query rows (same head)", r.warmSameNames, ["Alice", "Bob"]);
  check("live prime rows", r.primedNames, ["Alice", "Bob"]);
  check("re-query at the new head", r.freshNames, ["Alice", "Bob", "Carol"]);
  check("live subscription updates", r.liveNames, [["Alice", "Bob"], ["Alice", "Bob", "Carol"]]);
  if (!(r.newT > r.openT)) {
    throw new Error(`SSE head change did not advance past the opened head: openT=${r.openT}, newT=${r.newT}`);
  }
  console.log(`  ok  SSE head advanced ${r.openT} → ${r.newT}`);
  if (r.reopenedT !== r.newT) {
    throw new Error(`engine did not absorb the head change: reopened at t=${r.reopenedT}, SSE said ${r.newT}`);
  }
  console.log(`  ok  engine absorbed the advance (ledger re-opens at t=${r.reopenedT})`);

  // Traffic evidence: the peer really pulled CAS objects and held SSE open.
  const objects = tap.matching("/v1/fluree/storage/objects/");
  const okObjects = objects.filter((x) => x.status === 200 || x.status === 206);
  const nsLookups = tap.matching("/v1/fluree/storage/ns/");
  const sse = tap.matching("/v1/fluree/events");
  const queries = tap.matching("/v1/fluree/query");
  if (okObjects.length === 0) {
    throw new Error("no successful GET /storage/objects/{cid} was recorded — the peer never read CAS bytes");
  }
  if (nsLookups.length === 0) throw new Error("no /storage/ns/ head resolution was recorded");
  if (sse.length === 0) throw new Error("no /events SSE stream was recorded");
  if (queries.length > 0) {
    throw new Error(`the peer query-shipped: ${queries.length} request(s) to /query — local compute not proven`);
  }
  console.log(
    `  ok  proxy traffic: ${okObjects.length}/${objects.length} CAS object fetches OK, ` +
      `${nsLookups.length} head resolution(s), ${sse.length} SSE stream(s), 0 /query calls`,
  );

  console.log(
    `\nPASS: browser peer ↔ real fluree-db-server. ` +
      `Engine ${r.version} opened ${LEDGER} at t=${r.openT} and answered ${r.coldNames.join(",")} ` +
      `from ${okObjects.length} CID-verified CAS objects; an external HTTP commit advanced the head to ` +
      `t=${r.newT}, SSE delivered it, and the re-query returned ${r.freshNames.join(",")}.`,
  );
  exit = 0;
} catch (err) {
  console.error("FAIL:", err.message ?? err);
  if (server) {
    const log = server.log();
    if (log.trim()) console.error(`\n--- fluree-server log ---\n${log}`);
  }
} finally {
  clearTimeout(deadline);
  cleanup();
}
process.exit(exit);
