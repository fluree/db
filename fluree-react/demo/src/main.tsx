/**
 * Entry point. The ONLY thing that differs between the two modes is which
 * config object `createClient` gets — the component tree is identical, which
 * is the whole claim of the two-mode design.
 *
 *   ?mode=remote  (default)  queries over HTTP, SSE head events re-run them
 *   ?mode=peer               the wasm engine in a worker re-runs them locally
 */

import { createRoot } from "react-dom/client";
import { createClient, FlureeProvider, type LiveClient } from "@fluree/react";
import App from "./App.js";
import { ensureLedger } from "./api.js";
import "./styles.css";

const mode = new URLSearchParams(location.search).get("mode") ?? "remote";

/**
 * Peer mode's bearer token.
 *
 * `?token=…` is accepted for convenience, then immediately moved into
 * sessionStorage and scrubbed from the address bar — a credential left in a
 * URL ends up in history, in server logs, and in `Referer` headers, and a
 * demo is exactly where people copy patterns from. Reload-safe within the
 * tab; gone when the tab closes.
 */
const TOKEN_KEY = "fluree-demo-token";

/** Run at startup, not lazily: the worker only asks for a token once it has
 * booted, so capturing it inside `getToken` loses it whenever anything fails
 * earlier. */
function captureToken(): void {
  const params = new URLSearchParams(location.search);
  const fromUrl = params.get("token");
  if (!fromUrl) return;
  sessionStorage.setItem(TOKEN_KEY, fromUrl);
  params.delete("token");
  const query = params.toString();
  history.replaceState(null, "", `${location.pathname}${query ? `?${query}` : ""}`);
}

function peerToken(): string {
  const stored = sessionStorage.getItem(TOKEN_KEY);
  if (stored) return stored;
  throw new Error(
    "peer mode needs a bearer token with fluree.storage.* scope: load this " +
      "page once with &token=… and it will be remembered for the tab.",
  );
}

async function buildClient(): Promise<LiveClient> {
  if (mode !== "peer") {
    // Same-origin: vite proxies /v1 to the Fluree server (vite.config.ts).
    return createClient({ url: location.origin });
  }
  // Loaded lazily so remote mode never pays for — or requires — the wasm
  // package, which needs its generated pkg/ glue from wasm-pack.
  const { connect } = await import("@fluree/db-wasm");
  return createClient({
    peer: {
      connect,
      url: `${location.origin}/v1/fluree`,
      getToken: peerToken,
    },
  });
}

function fail(err: unknown): void {
  const root = document.getElementById("root");
  if (root) {
    root.innerHTML = `<main><h1>Could not start</h1><pre>${String(err)}</pre>
      <p>Is the Fluree server running? See demo/README.md.</p></main>`;
  }
  console.error(err);
}

async function main(): Promise<void> {
  captureToken();
  // Idempotent; the first tab to load creates the ledger.
  await ensureLedger();
  const client = await buildClient();
  const root = document.getElementById("root");
  if (!root) throw new Error("no #root");
  // No StrictMode here, deliberately. Its dev-only double-render doubles
  // every counter on screen, and legible counters ARE this demo. That the
  // subscription survives StrictMode's double-mount is pinned by a unit test
  // instead (`useQuery.test.tsx`).
  createRoot(root).render(
    <FlureeProvider client={client}>
      <App mode={mode} />
    </FlureeProvider>,
  );
}

void main().catch(fail);
