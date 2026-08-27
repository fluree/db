import { existsSync } from "node:fs";
import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));

// The engine package spawns its worker with a literal `new Worker(new
// URL("./worker.js", …))`, which bundlers statically follow into the
// wasm-pack glue. Without that glue the import fails the BUILD — and it
// would take remote mode, which needs none of it, down with it. So point at
// the real package only when it is actually built.
const wasmGlue = here("../../fluree-db-wasm/js/pkg/fluree_db_wasm.js");
const peerModule = existsSync(wasmGlue)
  ? here("../../fluree-db-wasm/js/src/index.ts")
  : here("./src/peerUnavailable.ts");

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // Run against the working tree, not a published build — a demo that
      // exercises stale `dist/` output would not tell us anything about the
      // code under review.
      "@fluree/react": here("../src/index.ts"),
      "@fluree/db-wasm": peerModule,
    },
  },
  server: {
    port: 5173,
    // Both aliases resolve outside this project, and Vite's dev server
    // refuses to serve files outside the project root — without this the
    // engine's worker module and its .wasm come back 403 and peer mode dies
    // as `engine_crashed` with nothing in the page console, because the
    // failure happens inside the worker.
    fs: { allow: [here("../..")] },
    // The Fluree server runs on 8090 by default; proxying keeps the demo
    // same-origin so CORS configuration is not part of the demo's story.
    proxy: {
      "/v1": {
        target: process.env.FLUREE_URL ?? "http://127.0.0.1:8090",
        changeOrigin: true,
      },
    },
  },
});
