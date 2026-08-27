/**
 * The two-tab live board.
 *
 * Search this file for a timer, an interval, a refetch, or an invalidation
 * key. There are none. `useQuery` is the whole read path; a commit made
 * anywhere reaches every mounted query because the database says so.
 *
 * The render counters are the demo's real payload. A commit that changes one
 * note re-renders that note's row and nothing else — not the sibling rows,
 * not the header, not the other query on the page. That is the property the
 * package exists for, and it is invisible unless you count.
 */

import { memo, useCallback, useEffect, useRef, useState } from "react";
import { useConnectionState, useFlureeClient, useQuery } from "@fluree/react";
import {
  addNote,
  bindingsOf,
  BOARD_QUERY,
  LEDGER,
  noteOf,
  vote,
  type Binding,
  type Note,
  type SparqlResults,
} from "./api.js";

/** Counts how many times a component instance actually rendered. */
function useRenderCount(): number {
  const n = useRef(0);
  n.current++;
  return n.current;
}

/**
 * Takes the RAW binding, not a derived object. The binding is what keeps its
 * identity across a commit that did not touch this row, so this is what lets
 * `memo` bail out. Handing it a `{id, text, votes}` built during the parent's
 * render would defeat the memo on every commit — see `noteOf`.
 */
const NoteRow = memo(function NoteRow({
  binding,
  onVote,
}: {
  binding: Binding;
  onVote: (note: Note, delta: number) => void;
}) {
  const renders = useRenderCount();
  const note = noteOf(binding);
  return (
    <li className="note">
      <div className="votes">{note.votes}</div>
      <div className="text">{note.text}</div>
      <div className="actions">
        <button onClick={() => onVote(note, 1)} aria-label={`upvote ${note.text}`}>
          ▲
        </button>
        <button onClick={() => onVote(note, -1)} aria-label={`downvote ${note.text}`}>
          ▼
        </button>
      </div>
      <div className="renders" title="renders of this row since the page loaded">
        {renders}×
      </div>
    </li>
  );
});

/** The board: one live subscription, shared by every row. */
function Board() {
  const renders = useRenderCount();
  const { data, status, error, t } = useQuery<SparqlResults>(LEDGER, BOARD_QUERY);
  const bindings = bindingsOf(data);
  const onVote = useCallback((note: Note, delta: number) => {
    void vote(note, delta).catch((e: unknown) => console.error(e));
  }, []);

  return (
    <section>
      <h2>
        Board <span className="badge">rendered {renders}×</span>
        <span className="badge">t = {t ?? "—"}</span>
        {status === "error" && <span className="badge err">{error?.code}</span>}
      </h2>
      {status === "loading" && <p className="muted">loading…</p>}
      {status !== "loading" && bindings.length === 0 && (
        <p className="muted">No notes yet — add one below.</p>
      )}
      <ul className="notes">
        {bindings.map((binding) => (
          <NoteRow
            key={binding.id?.value ?? ""}
            binding={binding}
            onVote={onVote}
          />
        ))}
      </ul>
    </section>
  );
}

/**
 * A second, deliberately unrelated query on the SAME ledger. Every commit
 * re-evaluates it too — but its result only moves when a note is added or
 * removed, so voting leaves this component completely untouched. Its render
 * counter is the proof.
 */
function NoteCount() {
  const renders = useRenderCount();
  const { data } = useQuery<SparqlResults>(
    LEDGER,
    `PREFIX ex: <http://example.org/board/>
     SELECT (COUNT(?id) AS ?n) WHERE { ?id a ex:Note }`,
  );
  const n = data?.results.bindings[0]?.n?.value ?? "—";
  return (
    <section>
      <h2>
        Count <span className="badge">rendered {renders}×</span>
      </h2>
      <p className="big">{n}</p>
      <p className="muted">
        Voting does not change this number, so this component does not
        re-render — even though its query re-ran.
      </p>
    </section>
  );
}

function Composer() {
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const value = text.trim();
    if (!value) return;
    setBusy(true);
    try {
      await addNote(value);
      setText("");
    } catch (err) {
      console.error(err);
    } finally {
      setBusy(false);
    }
  };
  return (
    <form onSubmit={submit} className="composer">
      <input
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder="Add a note, then watch the other tab"
        aria-label="note text"
      />
      <button type="submit" disabled={busy || text.trim() === ""}>
        Add
      </button>
    </form>
  );
}

function Header({ mode }: { mode: string }) {
  const connection = useConnectionState();
  const client = useFlureeClient();
  const [head, setHead] = useState<number | undefined>(undefined);
  // The ledger's newest watermark, which is NOT the same as any one query's
  // `t` — a query whose results did not move keeps its own.
  useEffect(() => {
    const id = setInterval(() => setHead(client.ledgerHead(LEDGER)), 500);
    return () => clearInterval(id);
  }, [client]);

  return (
    <header>
      <h1>Fluree live board</h1>
      <div className="meta">
        <span className={`dot ${connection}`} /> {connection}
        <span className="badge">{mode} mode</span>
        <span className="badge">ledger head t = {head ?? "—"}</span>
      </div>
      <p className="muted">
        Open this page in a second tab. Write in either one — the other
        updates with no polling code in this app. The <code>rendered N×</code>
        counters show which components React actually re-rendered.
      </p>
    </header>
  );
}

export default function App({ mode }: { mode: string }) {
  return (
    <main>
      <Header mode={mode} />
      <div className="columns">
        <Board />
        <NoteCount />
      </div>
      <Composer />
    </main>
  );
}
