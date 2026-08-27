// Launch a REAL `fluree-server` with the storage proxy enabled, and mint the
// storage-proxy bearer token a browser peer needs.
//
// The token is a JWS with an embedded Ed25519 JWK, exactly the shape
// `fluree-db-server/tests/proxy_integration.rs::create_storage_proxy_token`
// builds: `iss` is the did:key derived from the signing key, and the server
// is started with that did as a TRUSTED ISSUER — the production verification
// path (`extract/storage_proxy.rs::verify_token`), not the hidden
// `--storage-proxy-insecure` dev escape.

import { spawn, spawnSync } from "node:child_process";
import { generateKeyPairSync, sign } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const repoRoot = resolve(here, "..", "..", "..");

const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/** base58btc (Bitcoin alphabet), the encoding did:key uses. */
function base58(bytes) {
  let n = 0n;
  for (const b of bytes) n = n * 256n + BigInt(b);
  let out = "";
  while (n > 0n) {
    out = B58[Number(n % 58n)] + out;
    n /= 58n;
  }
  // Leading zero bytes are not representable in the numeric form; each one
  // encodes as the alphabet's zero digit.
  for (const b of bytes) {
    if (b !== 0) break;
    out = B58[0] + out;
  }
  return out;
}

const b64url = (buf) => Buffer.from(buf).toString("base64url");

/**
 * A fresh Ed25519 identity: the raw public key, its JWK, and the did:key
 * (multicodec 0xed01 + key, base58btc, `z`-prefixed).
 */
export function createIdentity() {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const jwk = publicKey.export({ format: "jwk" });
  const raw = Buffer.from(jwk.x, "base64url");
  if (raw.length !== 32) throw new Error(`unexpected Ed25519 public key length ${raw.length}`);
  const did = `did:key:z${base58(Buffer.concat([Buffer.from([0xed, 0x01]), raw]))}`;
  return { privateKey, jwk, did };
}

/**
 * Mint a storage-proxy bearer token. `ledgers` scopes it via
 * `fluree.storage.ledgers`; omit for `fluree.storage.all`.
 */
export function mintStorageProxyToken(identity, { ledgers, ttlSecs = 3600, identityIri } = {}) {
  const header = {
    alg: "EdDSA",
    jwk: { kty: "OKP", crv: "Ed25519", x: identity.jwk.x },
  };
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: identity.did,
    sub: "browser-peer-smoke@example.com",
    iat: now,
    exp: now + ttlSecs,
    ...(ledgers ? { "fluree.storage.ledgers": ledgers } : { "fluree.storage.all": true }),
    ...(identityIri ? { "fluree.identity": identityIri } : {}),
  };
  const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(payload))}`;
  const sig = sign(null, Buffer.from(signingInput), identity.privateKey);
  return `${signingInput}.${b64url(sig)}`;
}

/** Path to the `fluree-server` binary, building it if it is not there yet. */
export function serverBinary({ build = true } = {}) {
  if (process.env.FLUREE_SERVER_BIN) return process.env.FLUREE_SERVER_BIN;
  for (const profile of ["debug", "release"]) {
    const p = join(repoRoot, "target", profile, "fluree-server");
    if (existsSync(p)) return p;
  }
  if (!build) return null;
  console.log("building fluree-server (cargo build -p fluree-db-server --bin fluree-server)…");
  const r = spawnSync("cargo", ["build", "-p", "fluree-db-server", "--bin", "fluree-server"], {
    cwd: repoRoot,
    stdio: "inherit",
  });
  if (r.status !== 0) throw new Error("cargo build of fluree-server failed");
  const p = join(repoRoot, "target", "debug", "fluree-server");
  if (!existsSync(p)) throw new Error(`cargo reported success but ${p} is missing`);
  return p;
}

/**
 * Start a transaction server with the storage proxy on, `trustedIssuer` as
 * the only trusted did:key, and file storage under a temp dir. Resolves once
 * `/health` answers.
 *
 * Returns `{ url, apiBase, log, stop }`. `log()` is the captured
 * stdout+stderr — printed on failure, never parsed for assertions.
 */
export async function startFlureeServer({ trustedIssuer, port, env = {}, timeoutMs = 60_000 }) {
  const bin = serverBinary();
  const dataDir = mkdtempSync(join(tmpdir(), "fluree-peer-smoke-"));
  const listenPort = port ?? (await freePort());
  const child = spawn(bin, [], {
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      FLUREE_LISTEN_ADDR: `127.0.0.1:${listenPort}`,
      FLUREE_STORAGE_PATH: dataDir,
      FLUREE_CORS_ENABLED: "true",
      FLUREE_STORAGE_PROXY_ENABLED: "true",
      FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS: trustedIssuer,
      RUST_LOG: process.env.RUST_LOG ?? "warn,fluree_db_server=info",
      ...env,
    },
  });

  let captured = "";
  const capture = (d) => { captured += d.toString(); };
  child.stdout.on("data", capture);
  child.stderr.on("data", capture);

  let exited = null;
  child.on("exit", (code, signal) => { exited = `exit code ${code}${signal ? ` (${signal})` : ""}`; });

  const url = `http://127.0.0.1:${listenPort}`;
  const stop = () => {
    try { child.kill("SIGKILL"); } catch { /* already gone */ }
    try {
      rmSync(dataDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch { /* leftover temp dir is harmless */ }
  };

  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (exited) {
      stop();
      throw new Error(`fluree-server exited before becoming ready (${exited}):\n${captured}`);
    }
    try {
      const res = await fetch(`${url}/health`);
      if (res.ok) break;
    } catch { /* not listening yet */ }
    if (Date.now() > deadline) {
      stop();
      throw new Error(`fluree-server did not answer /health within ${timeoutMs} ms:\n${captured}`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  return { url, apiBase: `${url}/v1/fluree`, dataDir, log: () => captured, stop };
}

/** An ephemeral loopback port, released before the server claims it. */
function freePort() {
  return new Promise((resolvePort, reject) => {
    import("node:net").then(({ createServer }) => {
      const s = createServer();
      s.once("error", reject);
      s.listen(0, "127.0.0.1", () => {
        const { port } = s.address();
        s.close(() => resolvePort(port));
      });
    }, reject);
  });
}

/** POST JSON to the server, asserting the expected status. */
export async function postJson(url, body, { expect: expectStatus, headers = {} } = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (expectStatus !== undefined && res.status !== expectStatus) {
    throw new Error(`POST ${url} → ${res.status} (expected ${expectStatus}): ${text}`);
  }
  return { status: res.status, text, json: text ? safeJson(text) : null };
}

/** POST a text body (Turtle/TriG) to the server, asserting the status. */
export async function postText(url, body, { contentType, expect: expectStatus } = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": contentType },
    body,
  });
  const text = await res.text();
  if (expectStatus !== undefined && res.status !== expectStatus) {
    throw new Error(`POST ${url} → ${res.status} (expected ${expectStatus}): ${text}`);
  }
  return { status: res.status, text, json: text ? safeJson(text) : null };
}

function safeJson(text) {
  try { return JSON.parse(text); } catch { return null; }
}
