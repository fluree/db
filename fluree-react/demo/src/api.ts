/**
 * Writes. Deliberately NOT part of `@fluree/react`: the package is a read
 * path, and in peer mode the engine is read-only, so a write is always a
 * plain HTTP call to the server. Nothing here tells the UI to refresh —
 * that is the entire point of the demo. The commit reaches the other tab
 * (and this one) the same way: the server announces it, and every mounted
 * query re-evaluates itself.
 */

export const LEDGER = "demo/board";

const EX = "http://example.org/board/";

export interface Note {
  id: string;
  text: string;
  votes: number;
}

async function post(path: string, body: unknown): Promise<unknown> {
  const res = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${text}`);
  return text ? (JSON.parse(text) as unknown) : null;
}

/** Create the ledger if this is the first run. Safe to call repeatedly. */
export async function ensureLedger(): Promise<void> {
  try {
    await post("/v1/fluree/create", { ledger: LEDGER });
  } catch (err) {
    // Already there is the normal case after the first run.
    if (!/conflict|exists/i.test(String(err))) throw err;
  }
}

export async function addNote(text: string): Promise<void> {
  const id = `note-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  await post(`/v1/fluree/insert/${LEDGER}`, {
    "@context": { ex: EX },
    "@graph": [
      { "@id": `ex:${id}`, "@type": "ex:Note", "ex:text": text, "ex:votes": 0 },
    ],
  });
}

/**
 * Upsert the whole node with a new vote count. A read-modify-write, which is
 * exactly the shape that makes stale reads visible — and the reason the
 * count the UI holds must be live rather than polled.
 */
export async function vote(note: Note, delta: number): Promise<void> {
  await post(`/v1/fluree/upsert/${LEDGER}`, {
    "@context": { ex: EX },
    "@graph": [
      {
        "@id": note.id,
        "@type": "ex:Note",
        "ex:text": note.text,
        "ex:votes": Math.max(0, note.votes + delta),
      },
    ],
  });
}

/** The one query every board component subscribes to. */
export const BOARD_QUERY = `
PREFIX ex: <${EX}>
SELECT ?id ?text ?votes
WHERE { ?id a ex:Note ; ex:text ?text ; ex:votes ?votes }
ORDER BY DESC(?votes) ?id
`.trim();

/** One row of SPARQL JSON results. */
export type Binding = Record<string, { value: string }>;

export interface SparqlResults {
  head: { vars: string[] };
  results: { bindings: Binding[] };
}

/**
 * Read one binding as a note.
 *
 * Note what this deliberately is NOT: a `data -> Note[]` mapper. The package
 * hands back a result tree in which a row that did not change is the SAME
 * object as last render — that is the point of structural sharing, and it is
 * what lets `React.memo` skip a row. Mapping the whole tree into fresh
 * `{id, text, votes}` objects on every render throws all of that away
 * silently: everything still WORKS, every row just re-renders on every
 * commit, and nothing tells you.
 *
 * So components pass the binding itself down and convert per row, here.
 */
export function noteOf(binding: Binding): Note {
  return {
    id: binding.id?.value ?? "",
    text: binding.text?.value ?? "",
    votes: Number(binding.votes?.value ?? 0),
  };
}

export function bindingsOf(data: SparqlResults | undefined): Binding[] {
  return data?.results.bindings ?? [];
}
