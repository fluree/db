/**
 * Structural sharing: given the previous value and a freshly received one,
 * return a value deep-equal to `next` that reuses `prev`'s references for
 * every branch that did not change. If NOTHING changed, the returned value
 * IS `prev` — one identity check answers "did this query's results change",
 * and unchanged rows keep their object identity so `React.memo` and
 * `useMemo` dependencies keep working down the component tree.
 *
 * Works on JSON-shaped data (plain objects, arrays, primitives) — which is
 * exactly what both result formats are. Non-plain objects are replaced
 * wholesale.
 */

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Write a merged value onto the output object.
 *
 * `out[key] = value` is the obvious form and it is wrong for exactly one
 * key. `__proto__` on an object literal is an accessor inherited from
 * `Object.prototype`, so the assignment sets the prototype instead of
 * defining a property: the key disappears from the data, and when the value
 * is an object, `out` stops being a plain object. The second effect is the
 * expensive one — the next pass sees a non-plain `prev`, replaces the whole
 * subtree wholesale, and structural sharing silently dies for that branch
 * from then on, with nothing to see in the result but the re-renders.
 *
 * Not exotic: `?__proto__` is a legal SPARQL variable name, a JSON-LD
 * context can compact a term to it, and `JSON.parse` delivers it as an own
 * enumerable property that `Object.keys` reaches.
 */
function define(out: Record<string, unknown>, key: string, value: unknown): void {
  if (key === "__proto__") {
    Object.defineProperty(out, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
    return;
  }
  out[key] = value;
}

export function replaceEqualDeep<T>(prev: unknown, next: T): T {
  if (Object.is(prev, next)) {
    return prev as T;
  }

  if (Array.isArray(prev) && Array.isArray(next)) {
    const prevArr = prev as unknown[];
    const nextArr = next as unknown[];
    const out: unknown[] = new Array(nextArr.length);
    let reused = 0;
    for (let i = 0; i < nextArr.length; i++) {
      const merged = replaceEqualDeep(prevArr[i], nextArr[i]);
      out[i] = merged;
      if (i < prevArr.length && Object.is(merged, prevArr[i])) reused++;
    }
    return prevArr.length === nextArr.length && reused === nextArr.length
      ? (prev as T)
      : (out as T);
  }

  if (isPlainObject(prev) && isPlainObject(next)) {
    const nextKeys = Object.keys(next);
    const prevKeys = Object.keys(prev);
    const out: Record<string, unknown> = {};
    let reused = 0;
    for (const key of nextKeys) {
      // Own properties only. `key in prev` walks the prototype chain, and
      // for a `__proto__` key that has just appeared in the data it reads
      // the inherited accessor — handing `Object.prototype` itself in as the
      // previous value. `Object.prototype` passes `isPlainObject` (null
      // prototype) and has no enumerable keys, so a `{}` from the server
      // would be "shared" with it and the merged result would carry the
      // global prototype object where the data said `{}`.
      const had = Object.hasOwn(prev, key);
      const prevValue = had ? prev[key] : undefined;
      const merged = replaceEqualDeep(prevValue, next[key]);
      define(out, key, merged);
      if (had && Object.is(merged, prevValue)) reused++;
    }
    return prevKeys.length === nextKeys.length && reused === nextKeys.length
      ? (prev as T)
      : (out as T);
  }

  return next;
}
