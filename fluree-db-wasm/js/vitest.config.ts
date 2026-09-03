import { defineConfig } from "vitest/config";

// No DOM needed: the suite drives index.ts's main-thread proxy directly and
// substitutes `globalThis.Worker` with a scriptable stub (test/helpers.ts) —
// see fluree-react/vitest.config.ts for the sibling package's (jsdom, for
// React) config this mirrors.
export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    include: ["test/**/*.test.ts"],
  },
});
