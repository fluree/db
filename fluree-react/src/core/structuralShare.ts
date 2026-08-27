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
      const merged = replaceEqualDeep(prev[key], next[key]);
      out[key] = merged;
      if (key in prev && Object.is(merged, prev[key])) reused++;
    }
    return prevKeys.length === nextKeys.length && reused === nextKeys.length
      ? (prev as T)
      : (out as T);
  }

  return next;
}
