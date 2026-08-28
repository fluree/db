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

import {
  memo,
  useCallback,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
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
 * Milliseconds from navigation start to the first render that had data.
 *
 * Worth showing, because the honest story is not "peer mode is faster".
 * Peer mode pays a real cold-start cost — fetching and compiling a ~9 MB
 * wasm engine, then discovering and fetching index blocks — that remote mode
 * simply does not have. What it buys is what happens *after*: subsequent
 * queries and every update run locally, with no round trip.
 */
function useFirstDataMs(hasData: boolean): number | undefined {
  const at = useRef<number | undefined>(undefined);
  if (hasData && at.current === undefined) at.current = Math.round(performance.now());
  return at.current;
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
  const firstDataMs = useFirstDataMs(data !== undefined);
  const onVote = useCallback((note: Note, delta: number) => {
    void vote(note, delta).catch((e: unknown) => console.error(e));
  }, []);

  return (
    <section>
      <h2>
        Board <span className="badge">rendered {renders}×</span>
        <span className="badge">t = {t ?? "—"}</span>
        <span className="badge" title="navigation start to first render with data">
          first data {firstDataMs === undefined ? "—" : `${firstDataMs} ms`}
        </span>
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

/**
 * The ledger's newest watermark, which is NOT the same as any one query's
 * `t` — a query whose results did not move keeps its own. Pushed, like
 * everything else on this page: the client says when the head moves, so
 * there is no timer behind this number.
 *
 * Its own component on purpose. Every commit moves the head, so whatever
 * renders it re-renders on every commit; keeping that to this one badge is
 * what lets the header around it stay still, and it is the same discipline
 * the README asks of any app — subscribe at the leaf that needs it.
 */
function HeadBadge() {
  const client = useFlureeClient();
  const head = useSyncExternalStore(
    useCallback(
      (onChange: () => void) => client.onLedgerHead(LEDGER, onChange),
      [client],
    ),
    () => client.ledgerHead(LEDGER),
    () => undefined,
  );
  return <span className="badge">ledger head t = {head ?? "—"}</span>;
}

function Header({ mode }: { mode: string }) {
  const connection = useConnectionState();

  return (
    <header>
      <h1>Fluree live board</h1>
      <div className="meta">
        <span className={`dot ${connection}`} /> {connection}
        <span className="badge">{mode} mode</span>
        <HeadBadge />
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
