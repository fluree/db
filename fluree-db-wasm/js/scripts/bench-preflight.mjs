#!/usr/bin/env node
// What do CORS preflights cost a browser peer's cold open?
//
//   node scripts/bench-preflight.mjs                 # default matrix
//   node scripts/bench-preflight.mjs --reps 5 --latencies 0,25,50,100
//
// Every CAS object fetch carries an `Authorization` header, which makes it a
// non-simple cross-origin request: the browser must send an `OPTIONS`
// preflight first, and because each object lives at its own `/{cid}` URL,
// `Access-Control-Max-Age` can never amortize it. So a cold open pays roughly
// two round trips per block instead of one.
//
// The experiment isolates exactly that. Two arms, identical code and server:
//
//   cross-origin  page on origin A, API on origin B  → preflight per object
//   same-origin   page served BY the tap, API on the same origin → none
//
// and an injected per-request delay standing in for network latency, since
// loopback RTT (~0.1 ms) hides the very cost being measured. Each measurement
// gets a fresh Chrome profile, so the IndexedDB block cache and the browser's
// HTTP cache are both cold — the only state in which preflights are paid.

import { existsSync } from "node:fs";
import { join } from "node:path";
import { awaitPageMarker, cdpConnect, findChrome, launchChrome, openPage } from "./chrome.mjs";
import {
  createIdentity,
  mintStorageProxyToken,
  postJson,
  postText,
  startFlureeServer,
} from "./fluree-server.mjs";
import { startTap } from "./http-tap.mjs";
import { packageRoot, startServer } from "./serve.mjs";

const argv = process.argv.slice(2);
const argOf = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const REPS = Number(argOf("--reps", 3));
const LATENCIES = String(argOf("--latencies", "0,25,50")).split(",").map(Number);
const LEDGER = "benchmark:main";
const CTX = { ex: "http://example.org/ns/" };
/** Rows to insert — enough distinct subjects to make the index fan out. */
const ROWS = Number(argOf("--rows", 200));

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

/** One measurement: fresh Chrome, fresh tap, one cold open. */
async function measure({ sameOrigin, delayMs, token, serverUrl }) {
  const tap = await startTap(serverUrl, {
    delayMs,
    staticRoot: sameOrigin ? packageRoot : undefined,
  });
  let pageOrigin;
  let stopPageServer = () => {};
  if (sameOrigin) {
    pageOrigin = tap.url;
  } else {
    const s = await startServer(packageRoot, 0);
    pageOrigin = s.url;
    stopPageServer = () => s.server.close();
  }

  const chrome = launchChrome(chromeBin);
  let cdp;
  try {
    cdp = await cdpConnect(await chrome.wsUrl);
    const pageUrl =
      `${pageOrigin}/demo/peer-bench.html` +
      `?api=${encodeURIComponent(tap.apiBase)}` +
      `&token=${encodeURIComponent(token)}` +
      `&ledger=${encodeURIComponent(LEDGER)}`;
    const session = await openPage(cdp, pageUrl);
    const r = await awaitPageMarker(cdp, session, "__bench", { tries: 1800, settleMs: 120_000 });
    if (r?.status !== "pass") {
      throw new Error(`bench page failed: ${r?.error ?? JSON.stringify(r)}`);
    }
    return {
      ...r,
      preflights: tap.requests.filter((x) => x.method === "OPTIONS").length,
      objectGets: tap.matching("GET", "/v1/fluree/storage/objects/").length,
      apiRequests: tap.requests.length,
    };
  } finally {
    cdp?.close();
    chrome.kill();
    stopPageServer();
    tap.close();
  }
}

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length % 2 ? s[(s.length - 1) / 2] : Math.round((s[s.length / 2 - 1] + s[s.length / 2]) / 2);
};

let exit = 1;
try {
  const identity = createIdentity();
  const token = mintStorageProxyToken(identity, { ledgers: [LEDGER] });
  const server = await startFlureeServer({ trustedIssuer: identity.did });
  cleanups.push(server.stop);

  await postJson(`${server.apiBase}/create`, { ledger: LEDGER }, { expect: 201 });
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
      insert: Array.from({ length: ROWS }, (_, i) => ({
        "@id": `ex:p${i}`,
        "@type": "ex:Person",
        "ex:name": `Person ${String(i).padStart(4, "0")}`,
      })),
    },
    { expect: 200 },
  );
  console.log(`server up, ${ROWS} rows in ${LEDGER}\n`);

  const results = [];
  for (const delayMs of LATENCIES) {
    for (const sameOrigin of [false, true]) {
      const runs = [];
      for (let i = 0; i < REPS; i++) {
        runs.push(await measure({ sameOrigin, delayMs, token, serverUrl: server.url }));
      }
      const row = {
        arm: sameOrigin ? "same-origin" : "cross-origin",
        delayMs,
        coldOpenMs: median(runs.map((r) => r.coldOpenMs)),
        openMs: median(runs.map((r) => r.openMs)),
        queryMs: median(runs.map((r) => r.queryMs)),
        connectMs: median(runs.map((r) => r.connectMs)),
        objectGets: runs[0].objectGets,
        preflights: runs[0].preflights,
        apiRequests: runs[0].apiRequests,
      };
      results.push(row);
      console.log(
        `  ${row.arm.padEnd(12)} delay=${String(delayMs).padStart(3)}ms  ` +
          `coldOpen=${String(row.coldOpenMs).padStart(6)}ms  ` +
          `(open ${row.openMs}ms + query ${row.queryMs}ms)  ` +
          `${row.objectGets} object GETs, ${row.preflights} preflights, ` +
          `${row.apiRequests} API requests total`,
      );
    }
  }

  console.log(`\n=== cold-open cost of CORS preflights (median of ${REPS}) ===`);
  console.log("delay   cross-origin   same-origin   delta      overhead");
  for (const delayMs of LATENCIES) {
    const x = results.find((r) => r.delayMs === delayMs && r.arm === "cross-origin");
    const s = results.find((r) => r.delayMs === delayMs && r.arm === "same-origin");
    const delta = x.coldOpenMs - s.coldOpenMs;
    const pct = ((delta / s.coldOpenMs) * 100).toFixed(0);
    console.log(
      `${String(delayMs).padStart(4)}ms  ${String(x.coldOpenMs).padStart(9)}ms  ` +
        `${String(s.coldOpenMs).padStart(10)}ms  ${String(delta).padStart(6)}ms  ${pct.padStart(7)}%`,
    );
  }
  console.log(`\nJSON: ${JSON.stringify(results)}`);
  exit = 0;
} catch (err) {
  console.error("FAIL:", err.message ?? err);
} finally {
  cleanup();
}
process.exit(exit);
