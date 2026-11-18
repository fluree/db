/**
 * Stand-in for `@fluree/db-wasm` when its generated glue has not been built.
 *
 * The engine package spawns its worker with a literal
 * `new Worker(new URL("./worker.js", import.meta.url))`, which bundlers
 * statically follow — so without `fluree-db-wasm/pkg/`, importing it at all
 * fails the BUILD, taking remote mode down with it. `vite.config.ts` swaps
 * this module in when the glue is missing, so the demo always builds and
 * peer mode explains itself at the moment you ask for it.
 */

export function connect(): never {
  throw new Error(
    "peer mode needs @fluree/db-wasm's generated glue, which is not built.\n" +
      "Build it first (see fluree-db-wasm/README.md — wasm-pack writes\n" +
      "fluree-db-wasm/pkg/), then reload. Remote mode needs nothing extra:\n" +
      "drop the ?mode=peer parameter.",
  );
}
