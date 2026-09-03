/**
 * Structural sharing is the load-bearing primitive of this package: it is
 * what turns "the transport handed us a freshly-parsed JSON tree" into "the
 * rows that did not change are the SAME objects the component rendered last
 * time", which is what makes `React.memo` and `useMemo` deps work down the
 * tree.
 *
 * Every SHARING claim here is `toBe` (identity), never `toEqual` (deep
 * equality): a deep-equality assertion passes against an implementation that
 * shares nothing at all, so it proves nothing about the property we care
 * about. `toEqual` appears only alongside those, to pin that the merged tree
 * is also CORRECT — an implementation that returned `prev` unconditionally
 * would ace every identity assertion in this file.
 */

import { describe, expect, it } from "vitest";
import { replaceEqualDeep } from "../src/core/structuralShare.js";

/** A SPARQL-JSON-shaped result, freshly allocated on every call — exactly
 * what `JSON.parse` of a transport payload gives you. */
function bindings(names: string[]) {
  return {
    head: { vars: ["s", "name"] },
    results: {
      bindings: names.map((name, i) => ({
        s: { type: "uri", value: `http://ex/${i}` },
        name: { type: "literal", value: name },
      })),
    },
  };
}

describe("replaceEqualDeep", () => {
  it("returns the previous value itself when nothing changed", () => {
    const prev = bindings(["ada", "grace", "alan"]);
    const next = bindings(["ada", "grace", "alan"]);
    expect(next).not.toBe(prev); // the fixture really is a fresh tree

    const out = replaceEqualDeep(prev, next);

    // Identity, not equality: one `===` answers "did this query change?".
    expect(out).toBe(prev);
  });

  it("keeps object identity for unchanged rows across a change", () => {
    const prev = bindings(["ada", "grace", "alan"]);
    const next = bindings(["ada", "hopper", "alan"]);

    const out = replaceEqualDeep(prev, next);

    // The result is a new tree...
    expect(out).not.toBe(prev);
    expect(out).toEqual(next);
    // ...but every unchanged row is the SAME object the component already
    // rendered, so a memoized row component skips re-rendering.
    expect(out.results.bindings[0]).toBe(prev.results.bindings[0]);
    expect(out.results.bindings[2]).toBe(prev.results.bindings[2]);
    // Only the row that actually changed is a new object.
    expect(out.results.bindings[1]).not.toBe(prev.results.bindings[1]);
    // Untouched sibling branches are reused wholesale.
    expect(out.head).toBe(prev.head);
    // And within the changed row, the untouched cell keeps identity too.
    expect(out.results.bindings[1]!.s).toBe(prev.results.bindings[1]!.s);
  });

  it("keeps identity of surviving rows when a row is appended", () => {
    const prev = bindings(["ada", "grace"]);
    const next = bindings(["ada", "grace", "alan"]);

    const out = replaceEqualDeep(prev, next);

    expect(out).not.toBe(prev);
    expect(out).toEqual(next);
    expect(out.results.bindings[0]).toBe(prev.results.bindings[0]);
    expect(out.results.bindings[1]).toBe(prev.results.bindings[1]);
    expect(out.results.bindings).toHaveLength(3);
  });

  it("keeps identity of surviving rows when a row is removed", () => {
    const prev = bindings(["ada", "grace", "alan"]);
    const next = bindings(["ada", "grace"]);

    const out = replaceEqualDeep(prev, next);

    expect(out).not.toBe(prev);
    expect(out).toEqual(next);
    expect(out.results.bindings[0]).toBe(prev.results.bindings[0]);
    expect(out.results.bindings[1]).toBe(prev.results.bindings[1]);
    expect(out.results.bindings).toHaveLength(2);
  });

  it("does not reuse a same-length object whose key set differs", () => {
    const out = replaceEqualDeep({ a: 1 }, { b: 1 });
    expect(out).toEqual({ b: 1 });
    expect(Object.keys(out)).toEqual(["b"]);
  });

  it("does not reuse a same-length object whose key set differs by an undefined value", () => {
    const prev = { a: undefined };
    const out = replaceEqualDeep(prev, { b: undefined });
    expect(out).not.toBe(prev);
    expect(Object.keys(out)).toEqual(["b"]);
  });

  it("handles JSON-LD shaped documents", () => {
    const prev = {
      "@context": { ex: "http://ex/" },
      "@graph": [
        { "@id": "ex:1", "ex:name": "ada" },
        { "@id": "ex:2", "ex:name": "grace" },
      ],
    };
    const next = {
      "@context": { ex: "http://ex/" },
      "@graph": [
        { "@id": "ex:1", "ex:name": "ada" },
        { "@id": "ex:2", "ex:name": "hopper" },
      ],
    };

    const out = replaceEqualDeep(prev, next);

    expect(out).toEqual(next);
    expect(out["@context"]).toBe(prev["@context"]);
    expect(out["@graph"][0]).toBe(prev["@graph"][0]);
    expect(out["@graph"][1]).not.toBe(prev["@graph"][1]);
  });

  it("passes primitives and mismatched shapes through", () => {
    expect(replaceEqualDeep(1, 1)).toBe(1);
    expect(replaceEqualDeep(1, 2)).toBe(2);
    expect(replaceEqualDeep("a", "b")).toBe("b");
    expect(replaceEqualDeep(null, null)).toBe(null);
    expect(replaceEqualDeep(undefined, null)).toBe(null);
    expect(replaceEqualDeep(null, { a: 1 })).toEqual({ a: 1 });
    expect(replaceEqualDeep({ a: 1 }, [1])).toEqual([1]);
    expect(replaceEqualDeep([1], { a: 1 })).toEqual({ a: 1 });
  });

  it("replaces non-plain objects wholesale rather than merging them", () => {
    const prev = { at: new Date(0) };
    const nextDate = new Date(0);
    const out = replaceEqualDeep(prev, { at: nextDate });
    // Deep-equal Dates are NOT merged — the walk only understands JSON
    // shapes, so a class instance is always the new one.
    expect(out.at).toBe(nextDate);
    expect(out).not.toBe(prev);
  });

  it("treats an empty result as unchanged when it stays empty", () => {
    const prev = { head: { vars: [] }, results: { bindings: [] } };
    const next = { head: { vars: [] }, results: { bindings: [] } };
    expect(replaceEqualDeep(prev, next)).toBe(prev);
  });

  it("is idempotent: re-sharing an already-shared tree is a no-op", () => {
    const prev = bindings(["ada", "grace"]);
    const once = replaceEqualDeep(prev, bindings(["ada", "hopper"]));
    const twice = replaceEqualDeep(once, bindings(["ada", "hopper"]));
    // The transport shares against its own baseline and the cache shares
    // again against the handle's data; the second pass must not churn.
    expect(twice).toBe(once);
  });
});

describe("a __proto__ key in the data", () => {
  // `?__proto__` is a legal SPARQL variable name, a JSON-LD context can
  // compact a term to it, and `JSON.parse` delivers it as an own enumerable
  // property that `Object.keys` reaches — so this arrives through the front
  // door, not through an attack. The failure it used to cause is the worst
  // kind for this module: nothing throws, the row just quietly loses its
  // identity forever after.
  //
  // Read the OWN property throughout. `out["__proto__"]` goes through the
  // inherited accessor and returns the rewritten prototype, so the obvious
  // spelling reports success against the code that has the bug.
  const parse = (text: string) => JSON.parse(text) as Record<string, unknown>;
  const own = (o: unknown) =>
    Object.getOwnPropertyDescriptor(o as object, "__proto__")?.value;

  it("keeps the binding instead of dropping it into the prototype", () => {
    const prev = parse('{"a":1,"__proto__":{"x":1}}');
    const next = parse('{"a":2,"__proto__":{"x":1}}');
    const out = replaceEqualDeep(prev, next) as Record<string, unknown>;

    expect(out).not.toBe(prev); // `a` really did change
    expect(out["a"]).toBe(2);
    expect(Object.hasOwn(out, "__proto__")).toBe(true);
  });

  it("shares the unchanged __proto__ subtree by identity", () => {
    const prev = parse('{"a":1,"__proto__":{"x":1}}');
    const next = parse('{"a":2,"__proto__":{"x":1}}');
    const out = replaceEqualDeep(prev, next);
    expect(own(out)).toBe(prev["__proto__"]);
  });

  it("leaves the result plain, so the NEXT pass still shares", () => {
    // This is the second-order effect and the expensive one. A rewritten
    // prototype makes `isPlainObject(prev)` false on the following cycle,
    // which replaces the whole subtree wholesale — structural sharing dead
    // for that branch, silently, from then on.
    const out = replaceEqualDeep(
      parse('{"a":1,"__proto__":{"x":1}}'),
      parse('{"a":2,"__proto__":{"x":1}}'),
    );
    expect(Object.getPrototypeOf(out)).toBe(Object.prototype);
    expect(replaceEqualDeep(out, parse('{"a":2,"__proto__":{"x":1}}'))).toBe(out);
  });

  it("does not read the inherited accessor when the key is new", () => {
    // `key in prev` walks the prototype chain, so a `__proto__` key
    // appearing for the first time read `Object.prototype` as its previous
    // value — and `Object.prototype` has a null prototype and no enumerable
    // keys, so an empty object from the server compared equal to it and was
    // "shared" with it. The merged data then carried the global prototype
    // object where the result said `{}`. With nothing to share against, the
    // merged value must be the one the transport delivered.
    const next = parse('{"a":1,"__proto__":{}}');
    const out = replaceEqualDeep(parse('{"a":1}'), next);
    expect(Object.hasOwn(out as object, "__proto__")).toBe(true);
    expect(own(out)).not.toBe(Object.prototype);
    expect(own(out)).toBe(next["__proto__"]);
  });
});
