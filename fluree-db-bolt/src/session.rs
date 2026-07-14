//! The per-connection message state machine.
//!
//! Pure and transport-free: the caller decodes a [`Request`], hands it to
//! [`Session::on_request`], and acts on the returned [`Turn`] — writing
//! replies, closing the connection, executing statements, or driving the
//! explicit-transaction lifecycle. Completion callbacks
//! ([`Session::run_succeeded`], [`Session::begin_succeeded`], ...) return
//! the response to write and advance the state.
//!
//! Autocommit `RUN` and explicit transactions (`BEGIN`/`COMMIT`/
//! `ROLLBACK`) are both supported. Results are fully materialized by the
//! caller; `PULL`/`DISCARD` serve batches from buffered [`ResultStream`]s
//! with reactive `has_more` metadata. Inside a transaction multiple
//! results may be open concurrently, addressed by `qid` (Bolt 4.0+
//! `PULL {qid}`); autocommit has one live stream.

use std::collections::VecDeque;

use crate::handshake::BoltVersion;
use crate::message::{Request, Response};
use crate::value::{MapValue, Value};

/// Connection-level configuration the server glue supplies.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub version: BoltVersion,
    /// Advertised in the HELLO SUCCESS `server` field. Official drivers
    /// parse a `Neo4j/<semver>` prefix for feature gating.
    pub server_agent: String,
    /// Advertised in the HELLO SUCCESS `connection_id` field.
    pub connection_id: String,
    /// Ledger used when neither HELLO defaults nor RUN/BEGIN extra name a
    /// `db`.
    pub default_db: Option<String>,
    /// `host:port` for the single-entry ROUTE table. `None` answers ROUTE
    /// with a failure directing clients at the `bolt://` scheme.
    pub advertised_address: Option<String>,
}

/// One executed statement's buffered result, served out via PULL.
#[derive(Debug, Clone, Default)]
pub struct ResultStream {
    pub fields: Vec<String>,
    pub rows: VecDeque<Vec<Value>>,
    /// Metadata for the final SUCCESS once the stream drains (`type`,
    /// `t_last`, `db`, write `stats`, ...).
    pub summary: MapValue,
}

/// What the caller must do after feeding a request in.
#[derive(Debug, Clone, PartialEq)]
pub enum Turn {
    /// Write these replies; connection stays open.
    Reply(Vec<Response>),
    /// Write these replies (possibly none), then close the connection.
    Close(Vec<Response>),
    /// Verify the presented credentials (async, in the glue), then call
    /// `auth_succeeded` / `auth_failed` and act on what returns. Carries
    /// the auth map from HELLO (≤5.0) or LOGON (5.1+).
    Authenticate(AuthRequest),
    /// LOGOFF: drop the caller-side identity, then write these replies.
    /// The session is back in the authentication state (5.1+ only).
    Logoff(Vec<Response>),
    /// Execute the statement (autocommit, or inside the open transaction
    /// when the caller holds one), then call `run_succeeded` /
    /// `run_failed` and write the response that returns.
    Execute(RunRequest),
    /// Open an explicit transaction: set up caller-side state, then call
    /// `begin_succeeded` / `begin_failed` and write the response.
    Begin(BeginRequest),
    /// Commit the open transaction: publish, then call
    /// `commit_succeeded` / `commit_failed` and write the response.
    Commit,
    /// Roll back: drop caller-side transaction state, then call
    /// `rollback_done` and write the response.
    Rollback,
    /// RESET: drop any caller-side transaction state, then write these
    /// replies.
    Reset(Vec<Response>),
}

/// An autocommit or in-transaction statement the server glue must execute.
#[derive(Debug, Clone, PartialEq)]
pub struct RunRequest {
    pub query: String,
    pub parameters: MapValue,
    /// From RUN extra `db`, HELLO defaults, or the configured default — in
    /// that precedence. Inside a transaction the ledger was pinned at
    /// BEGIN; the caller uses its transaction state instead.
    pub db: Option<String>,
}

/// An explicit-transaction open request.
#[derive(Debug, Clone, PartialEq)]
pub struct BeginRequest {
    /// From BEGIN extra `db`, HELLO defaults, or the configured default.
    pub db: Option<String>,
}

/// Credentials presented in HELLO (≤5.0) or LOGON (5.1+). The codec does
/// not interpret them; the glue decides which schemes it honors.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthRequest {
    /// `none`, `basic`, `bearer`, ... Drivers always send one; `None`
    /// means the client omitted the field entirely.
    pub scheme: Option<String>,
    pub principal: Option<String>,
    pub credentials: Option<String>,
}

impl AuthRequest {
    fn from_map(map: &MapValue) -> Self {
        Self {
            scheme: map.get_str("scheme").map(str::to_string),
            principal: map.get_str("principal").map(str::to_string),
            credentials: map.get_str("credentials").map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    AwaitingHello,
    /// 5.1+: HELLO done, awaiting LOGON.
    Authentication,
    /// Credentials handed out via [`Turn::Authenticate`]; awaiting the
    /// `auth_succeeded` / `auth_failed` callback.
    Authenticating {
        /// Whether the credentials came in HELLO (≤5.0) — success must
        /// answer with the HELLO metadata, failure closes the connection
        /// (there is no re-auth path before 5.1).
        via_hello: bool,
    },
    Ready,
    /// Autocommit result being served.
    Streaming,
    /// Explicit transaction open, no results pending.
    TxReady,
    /// Explicit transaction open with buffered results.
    TxStreaming,
    Failed,
}

pub struct Session {
    config: SessionConfig,
    state: State,
    /// Set by `auth_succeeded`, cleared by LOGOFF. Tracked separately from
    /// `state` so RESET after a failed LOGON returns to the authentication
    /// state instead of skipping it.
    authenticated: bool,
    /// Buffered results by `qid`, insertion-ordered (tiny: autocommit has
    /// one; a transaction rarely holds more than a couple).
    streams: Vec<(i64, ResultStream)>,
    next_qid: i64,
    /// `db` from HELLO extra defaults (Bolt 5.x drivers can set a session
    /// database there); RUN/BEGIN extra still overrides.
    hello_db: Option<String>,
}

const CODE_INVALID: &str = "Neo.ClientError.Request.Invalid";

impl Session {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            state: State::AwaitingHello,
            authenticated: false,
            streams: Vec::new(),
            next_qid: 0,
            hello_db: None,
        }
    }

    pub fn version(&self) -> BoltVersion {
        self.config.version
    }

    /// Whether the connection has completed HELLO (+LOGON where required).
    pub fn is_ready(&self) -> bool {
        !matches!(
            self.state,
            State::AwaitingHello | State::Authentication | State::Authenticating { .. }
        )
    }

    /// Whether an explicit transaction is open.
    pub fn in_transaction(&self) -> bool {
        matches!(self.state, State::TxReady | State::TxStreaming)
    }

    pub fn on_request(&mut self, request: Request) -> Turn {
        match request {
            Request::Goodbye => Turn::Close(vec![]),
            Request::Reset => self.on_reset(),
            other => match self.state {
                State::AwaitingHello => self.on_awaiting_hello(other),
                State::Authentication => self.on_authentication(other),
                // Unreachable when the caller resolves Turn::Authenticate
                // before feeding the next request, as the glue loop does.
                State::Authenticating { .. } => Turn::Close(vec![Response::failure(
                    CODE_INVALID,
                    "request while authentication is in flight",
                )]),
                State::Ready => self.on_ready(other),
                State::Streaming => self.on_streaming(other),
                State::TxReady => self.on_tx_ready(other),
                State::TxStreaming => self.on_tx_streaming(other),
                State::Failed => Turn::Reply(vec![Response::Ignored]),
            },
        }
    }

    fn on_reset(&mut self) -> Turn {
        if self.state == State::AwaitingHello {
            // RESET before HELLO is a protocol violation.
            return Turn::Close(vec![Response::failure(CODE_INVALID, "RESET before HELLO")]);
        }
        self.streams.clear();
        // RESET recovers to READY only once authenticated; a failed LOGON
        // (Failed state, not authenticated) recovers to re-authentication.
        self.state = if self.authenticated {
            State::Ready
        } else {
            State::Authentication
        };
        Turn::Reset(vec![Response::success_empty()])
    }

    fn on_awaiting_hello(&mut self, request: Request) -> Turn {
        match request {
            Request::Hello { extra } => {
                self.hello_db = extra.get_str("db").map(str::to_string);
                if self.config.version.uses_logon() {
                    // 5.1+: auth arrives in LOGON; HELLO answers immediately.
                    self.state = State::Authentication;
                    Turn::Reply(vec![Response::success(self.hello_meta())])
                } else {
                    // ≤5.0: auth rides in the HELLO extra map; the HELLO
                    // SUCCESS waits for the auth_succeeded callback.
                    self.state = State::Authenticating { via_hello: true };
                    Turn::Authenticate(AuthRequest::from_map(&extra))
                }
            }
            other => Turn::Close(vec![Response::failure(
                CODE_INVALID,
                format!("expected HELLO, got {}", request_name(&other)),
            )]),
        }
    }

    fn on_authentication(&mut self, request: Request) -> Turn {
        match request {
            Request::Logon { auth } => {
                self.state = State::Authenticating { via_hello: false };
                Turn::Authenticate(AuthRequest::from_map(&auth))
            }
            other => Turn::Close(vec![Response::failure(
                CODE_INVALID,
                format!("expected LOGON, got {}", request_name(&other)),
            )]),
        }
    }

    fn hello_meta(&self) -> MapValue {
        let mut meta = MapValue::new();
        meta.insert("server", self.config.server_agent.as_str());
        meta.insert("connection_id", self.config.connection_id.as_str());
        meta
    }

    /// The credentials handed out via [`Turn::Authenticate`] verified.
    pub fn auth_succeeded(&mut self) -> Response {
        let via_hello = matches!(self.state, State::Authenticating { via_hello: true });
        self.authenticated = true;
        self.state = State::Ready;
        if via_hello {
            Response::success(self.hello_meta())
        } else {
            Response::success_empty()
        }
    }

    /// The credentials handed out via [`Turn::Authenticate`] were rejected
    /// (use `Neo.ClientError.Security.Unauthorized`). During HELLO there is
    /// no re-auth path, so the connection closes; after LOGON the client
    /// recovers with RESET and retries LOGON.
    pub fn auth_failed(&mut self, code: impl Into<String>, message: impl Into<String>) -> Turn {
        let failure = Response::failure(code, message);
        if matches!(self.state, State::Authenticating { via_hello: true }) {
            Turn::Close(vec![failure])
        } else {
            self.state = State::Failed;
            Turn::Reply(vec![failure])
        }
    }

    /// `db` resolution shared by RUN and BEGIN: explicit extra, HELLO
    /// session default, then the server's configured default.
    fn resolve_db(&self, extra: &MapValue) -> Option<String> {
        extra
            .get_str("db")
            .map(str::to_string)
            .or_else(|| self.hello_db.clone())
            .or_else(|| self.config.default_db.clone())
    }

    fn on_ready(&mut self, request: Request) -> Turn {
        match request {
            Request::Run {
                query,
                parameters,
                extra,
            } => {
                let db = self.resolve_db(&extra);
                Turn::Execute(RunRequest {
                    query,
                    parameters,
                    db,
                })
            }
            Request::Begin { extra } => Turn::Begin(BeginRequest {
                db: self.resolve_db(&extra),
            }),
            Request::Commit | Request::Rollback => self.fail(Response::failure(
                CODE_INVALID,
                "COMMIT/ROLLBACK outside a transaction",
            )),
            Request::Logoff if self.config.version.uses_logon() => {
                self.authenticated = false;
                self.state = State::Authentication;
                Turn::Logoff(vec![Response::success_empty()])
            }
            Request::Route { extra, .. } => Turn::Reply(vec![self.route_table(&extra)]),
            Request::Telemetry { .. } => Turn::Reply(vec![Response::success_empty()]),
            Request::Pull { .. } | Request::Discard { .. } => self.fail(Response::failure(
                CODE_INVALID,
                "no result stream to consume",
            )),
            other => self.fail(Response::failure(
                CODE_INVALID,
                format!("unexpected {} in READY state", request_name(&other)),
            )),
        }
    }

    fn on_streaming(&mut self, request: Request) -> Turn {
        match request {
            Request::Pull { extra } => Turn::Reply(self.serve_pull(&extra)),
            Request::Discard { extra } => Turn::Reply(vec![self.serve_discard(&extra)]),
            other => self.fail(Response::failure(
                CODE_INVALID,
                format!("unexpected {} while streaming", request_name(&other)),
            )),
        }
    }

    fn on_tx_ready(&mut self, request: Request) -> Turn {
        match request {
            Request::Run {
                query,
                parameters,
                extra,
            } => {
                let db = self.resolve_db(&extra);
                Turn::Execute(RunRequest {
                    query,
                    parameters,
                    db,
                })
            }
            Request::Commit => {
                self.streams.clear();
                Turn::Commit
            }
            Request::Rollback => {
                self.streams.clear();
                Turn::Rollback
            }
            Request::Begin { .. } => self.fail(Response::failure(
                CODE_INVALID,
                "a transaction is already open on this session",
            )),
            Request::Telemetry { .. } => Turn::Reply(vec![Response::success_empty()]),
            Request::Pull { .. } | Request::Discard { .. } => self.fail(Response::failure(
                CODE_INVALID,
                "no result stream to consume",
            )),
            other => self.fail(Response::failure(
                CODE_INVALID,
                format!("unexpected {} inside a transaction", request_name(&other)),
            )),
        }
    }

    fn on_tx_streaming(&mut self, request: Request) -> Turn {
        match request {
            Request::Pull { extra } => Turn::Reply(self.serve_pull(&extra)),
            Request::Discard { extra } => Turn::Reply(vec![self.serve_discard(&extra)]),
            // A new statement while results are buffered — Bolt 4.0+
            // addresses the streams by qid.
            Request::Run {
                query,
                parameters,
                extra,
            } => {
                let db = self.resolve_db(&extra);
                Turn::Execute(RunRequest {
                    query,
                    parameters,
                    db,
                })
            }
            // COMMIT/ROLLBACK with unconsumed results: the driver chose to
            // abandon them; discard implicitly.
            Request::Commit => {
                self.streams.clear();
                Turn::Commit
            }
            Request::Rollback => {
                self.streams.clear();
                Turn::Rollback
            }
            other => self.fail(Response::failure(
                CODE_INVALID,
                format!("unexpected {} while streaming", request_name(&other)),
            )),
        }
    }

    /// The RUN handed out via [`Turn::Execute`] succeeded. `t_first_ms` is
    /// the time from RUN to result availability, surfaced in the RUN
    /// SUCCESS metadata like Neo4j's.
    pub fn run_succeeded(&mut self, stream: ResultStream, t_first_ms: i64) -> Response {
        let qid = self.next_qid;
        self.next_qid += 1;
        let mut meta = MapValue::new();
        meta.insert(
            "fields",
            Value::List(
                stream
                    .fields
                    .iter()
                    .map(|f| Value::from(f.as_str()))
                    .collect(),
            ),
        );
        meta.insert("t_first", t_first_ms);
        meta.insert("qid", qid);
        self.streams.push((qid, stream));
        self.state = if self.in_transaction() {
            State::TxStreaming
        } else {
            State::Streaming
        };
        Response::success(meta)
    }

    /// The RUN handed out via [`Turn::Execute`] failed. Inside a
    /// transaction this poisons the whole transaction (the caller drops
    /// its state; the client recovers with RESET), matching Bolt.
    pub fn run_failed(&mut self, code: impl Into<String>, message: impl Into<String>) -> Response {
        self.state = State::Failed;
        self.streams.clear();
        Response::Failure {
            code: code.into(),
            message: message.into(),
        }
    }

    /// The BEGIN handed out via [`Turn::Begin`] succeeded.
    pub fn begin_succeeded(&mut self) -> Response {
        self.state = State::TxReady;
        Response::success_empty()
    }

    /// The BEGIN handed out via [`Turn::Begin`] failed.
    pub fn begin_failed(
        &mut self,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Response {
        self.state = State::Failed;
        Response::Failure {
            code: code.into(),
            message: message.into(),
        }
    }

    /// The COMMIT handed out via [`Turn::Commit`] succeeded. `bookmark`
    /// (when given) is surfaced in the SUCCESS metadata; drivers thread it
    /// through causal chaining.
    pub fn commit_succeeded(&mut self, bookmark: Option<String>) -> Response {
        self.state = State::Ready;
        let mut meta = MapValue::new();
        if let Some(bookmark) = bookmark {
            meta.insert("bookmark", bookmark);
        }
        Response::success(meta)
    }

    /// The COMMIT handed out via [`Turn::Commit`] failed (e.g. an
    /// optimistic-concurrency conflict — use a `Neo.TransientError.*`
    /// code so managed transaction functions retry).
    pub fn commit_failed(
        &mut self,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Response {
        self.state = State::Failed;
        Response::Failure {
            code: code.into(),
            message: message.into(),
        }
    }

    /// The ROLLBACK handed out via [`Turn::Rollback`] completed.
    pub fn rollback_done(&mut self) -> Response {
        self.state = State::Ready;
        Response::success_empty()
    }

    /// Resolve which buffered stream a PULL/DISCARD targets: explicit
    /// non-negative `qid`, else the most recent.
    fn stream_index(&self, extra: &MapValue) -> Option<usize> {
        match extra.get_int("qid") {
            Some(qid) if qid >= 0 => self.streams.iter().position(|(q, _)| *q == qid),
            _ => self.streams.len().checked_sub(1),
        }
    }

    fn after_stream_close(&mut self) {
        if self.streams.is_empty() {
            self.state = if self.in_transaction() {
                State::TxReady
            } else {
                State::Ready
            };
        }
    }

    fn serve_pull(&mut self, extra: &MapValue) -> Vec<Response> {
        let n = extra.get_int("n").unwrap_or(-1);
        let Some(idx) = self.stream_index(extra) else {
            return vec![Response::failure(CODE_INVALID, "no such result stream")];
        };
        let stream = &mut self.streams[idx].1;
        let take = if n < 0 {
            stream.rows.len()
        } else {
            (n as usize).min(stream.rows.len())
        };
        let mut replies: Vec<Response> = Vec::with_capacity(take + 1);
        for _ in 0..take {
            let row = stream.rows.pop_front().expect("row count checked");
            replies.push(Response::Record(row));
        }
        if stream.rows.is_empty() {
            let mut summary = std::mem::take(&mut stream.summary);
            summary.insert("has_more", false);
            replies.push(Response::success(summary));
            self.streams.remove(idx);
            self.after_stream_close();
        } else {
            let mut meta = MapValue::new();
            meta.insert("has_more", true);
            replies.push(Response::success(meta));
        }
        replies
    }

    fn serve_discard(&mut self, extra: &MapValue) -> Response {
        let n = extra.get_int("n").unwrap_or(-1);
        let Some(idx) = self.stream_index(extra) else {
            return Response::failure(CODE_INVALID, "no such result stream");
        };
        let stream = &mut self.streams[idx].1;
        let drop_n = if n < 0 {
            stream.rows.len()
        } else {
            (n as usize).min(stream.rows.len())
        };
        stream.rows.drain(..drop_n);
        if stream.rows.is_empty() {
            let mut summary = std::mem::take(&mut stream.summary);
            summary.insert("has_more", false);
            self.streams.remove(idx);
            self.after_stream_close();
            Response::success(summary)
        } else {
            let mut meta = MapValue::new();
            meta.insert("has_more", true);
            Response::success(meta)
        }
    }

    fn fail(&mut self, failure: Response) -> Turn {
        self.state = State::Failed;
        self.streams.clear();
        Turn::Reply(vec![failure])
    }

    /// Single-server routing table: this server plays every role.
    fn route_table(&self, extra: &Value) -> Response {
        let Some(address) = self.config.advertised_address.clone() else {
            return Response::failure(
                CODE_INVALID,
                "server-side routing is not configured; connect with the bolt:// scheme",
            );
        };
        let db = extra
            .as_map()
            .and_then(|m| m.get_str("db"))
            .map(str::to_string)
            .or_else(|| self.config.default_db.clone())
            .unwrap_or_default();
        let server_entry = |role: &str| {
            let mut entry = MapValue::new();
            entry.insert(
                "addresses",
                Value::List(vec![Value::from(address.as_str())]),
            );
            entry.insert("role", role);
            Value::Map(entry)
        };
        let mut rt = MapValue::new();
        rt.insert("ttl", 300i64);
        rt.insert("db", db);
        rt.insert(
            "servers",
            Value::List(vec![
                server_entry("WRITE"),
                server_entry("READ"),
                server_entry("ROUTE"),
            ]),
        );
        let mut meta = MapValue::new();
        meta.insert("rt", rt);
        Response::success(meta)
    }
}

fn request_name(request: &Request) -> &'static str {
    match request {
        Request::Hello { .. } => "HELLO",
        Request::Logon { .. } => "LOGON",
        Request::Logoff => "LOGOFF",
        Request::Goodbye => "GOODBYE",
        Request::Reset => "RESET",
        Request::Run { .. } => "RUN",
        Request::Begin { .. } => "BEGIN",
        Request::Commit => "COMMIT",
        Request::Rollback => "ROLLBACK",
        Request::Discard { .. } => "DISCARD",
        Request::Pull { .. } => "PULL",
        Request::Route { .. } => "ROUTE",
        Request::Telemetry { .. } => "TELEMETRY",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(version: BoltVersion) -> SessionConfig {
        SessionConfig {
            version,
            server_agent: "Neo4j/5.4.0 (compatible; Fluree)".into(),
            connection_id: "bolt-1".into(),
            default_db: Some("test:main".into()),
            advertised_address: None,
        }
    }

    fn hello(extra: MapValue) -> Request {
        Request::Hello { extra }
    }

    fn ready_session_54() -> Session {
        let mut s = Session::new(config(BoltVersion::V5_4));
        s.on_request(hello(MapValue::new()));
        let turn = s.on_request(Request::Logon {
            auth: MapValue::new(),
        });
        assert!(matches!(turn, Turn::Authenticate(_)));
        s.auth_succeeded();
        assert!(s.is_ready());
        s
    }

    fn run_req(query: &str) -> Request {
        Request::Run {
            query: query.into(),
            parameters: MapValue::new(),
            extra: MapValue::new(),
        }
    }

    fn pull(n: i64) -> Request {
        let mut extra = MapValue::new();
        extra.insert("n", n);
        Request::Pull { extra }
    }

    fn pull_qid(n: i64, qid: i64) -> Request {
        let mut extra = MapValue::new();
        extra.insert("n", n);
        extra.insert("qid", qid);
        Request::Pull { extra }
    }

    fn begin() -> Request {
        Request::Begin {
            extra: MapValue::new(),
        }
    }

    fn stream_of(rows: std::ops::RangeInclusive<i64>) -> ResultStream {
        let mut summary = MapValue::new();
        summary.insert("type", "r");
        ResultStream {
            fields: vec!["x".into()],
            rows: rows.map(|i| vec![Value::Integer(i)]).collect(),
            summary,
        }
    }

    fn three_row_stream() -> ResultStream {
        stream_of(1..=3)
    }

    #[test]
    fn bolt5_hello_requires_logon() {
        let mut s = Session::new(config(BoltVersion::V5_4));
        let turn = s.on_request(hello(MapValue::new()));
        let Turn::Reply(replies) = turn else {
            panic!("expected reply")
        };
        let Response::Success(meta) = &replies[0] else {
            panic!("expected success")
        };
        assert!(meta.get_str("server").unwrap().starts_with("Neo4j/"));
        assert!(!s.is_ready(), "5.4 needs LOGON before READY");

        let mut auth = MapValue::new();
        auth.insert("scheme", "bearer");
        auth.insert("credentials", "tok-123");
        let turn = s.on_request(Request::Logon { auth });
        let Turn::Authenticate(req) = turn else {
            panic!("expected authenticate")
        };
        assert_eq!(req.scheme.as_deref(), Some("bearer"));
        assert_eq!(req.credentials.as_deref(), Some("tok-123"));
        assert!(!s.is_ready(), "not ready until auth resolves");

        assert_eq!(s.auth_succeeded(), Response::success_empty());
        assert!(s.is_ready());
    }

    #[test]
    fn bolt44_hello_carries_auth_and_defers_success() {
        let mut s = Session::new(config(BoltVersion::V4_4));
        let mut extra = MapValue::new();
        extra.insert("scheme", "basic");
        extra.insert("principal", "ana");
        extra.insert("credentials", "pw");
        let Turn::Authenticate(req) = s.on_request(hello(extra)) else {
            panic!("expected authenticate")
        };
        assert_eq!(req.scheme.as_deref(), Some("basic"));
        assert_eq!(req.principal.as_deref(), Some("ana"));
        assert!(!s.is_ready(), "HELLO success waits for auth");

        // The deferred HELLO SUCCESS carries the server metadata.
        let Response::Success(meta) = s.auth_succeeded() else {
            panic!("expected success")
        };
        assert!(meta.get_str("server").unwrap().starts_with("Neo4j/"));
        assert!(meta.get_str("connection_id").is_some());
        assert!(s.is_ready());
    }

    #[test]
    fn bolt44_hello_auth_failure_closes() {
        let mut s = Session::new(config(BoltVersion::V4_4));
        assert!(matches!(
            s.on_request(hello(MapValue::new())),
            Turn::Authenticate(_)
        ));
        let turn = s.auth_failed("Neo.ClientError.Security.Unauthorized", "bad token");
        let Turn::Close(replies) = turn else {
            panic!("HELLO auth failure must close the connection")
        };
        assert!(matches!(replies[0], Response::Failure { .. }));
    }

    #[test]
    fn logon_failure_recovers_via_reset_to_reauth() {
        let mut s = Session::new(config(BoltVersion::V5_4));
        s.on_request(hello(MapValue::new()));
        s.on_request(Request::Logon {
            auth: MapValue::new(),
        });
        let turn = s.auth_failed("Neo.ClientError.Security.Unauthorized", "bad token");
        assert!(
            matches!(turn, Turn::Reply(ref r) if matches!(r[0], Response::Failure { .. })),
            "LOGON failure replies, connection stays open"
        );
        // Post-failure requests are IGNORED until RESET.
        assert_eq!(
            s.on_request(run_req("q")),
            Turn::Reply(vec![Response::Ignored])
        );
        // RESET returns to authentication, NOT to READY.
        s.on_request(Request::Reset);
        assert!(!s.is_ready(), "reset before successful auth must re-auth");
        let turn = s.on_request(Request::Logon {
            auth: MapValue::new(),
        });
        assert!(matches!(turn, Turn::Authenticate(_)));
        s.auth_succeeded();
        assert!(s.is_ready());
    }

    #[test]
    fn logoff_returns_to_authentication_and_signals_caller() {
        let mut s = ready_session_54();
        let turn = s.on_request(Request::Logoff);
        assert_eq!(turn, Turn::Logoff(vec![Response::success_empty()]));
        assert!(!s.is_ready());
        // Re-auth works (5.1+ re-authentication).
        let turn = s.on_request(Request::Logon {
            auth: MapValue::new(),
        });
        assert!(matches!(turn, Turn::Authenticate(_)));
        s.auth_succeeded();
        assert!(s.is_ready());
    }

    #[test]
    fn run_pull_all_round_trip() {
        let mut s = ready_session_54();
        let Turn::Execute(run) = s.on_request(run_req("MATCH (n) RETURN n.x AS x")) else {
            panic!("expected execute")
        };
        assert_eq!(run.db.as_deref(), Some("test:main"));

        let success = s.run_succeeded(three_row_stream(), 1);
        let Response::Success(meta) = &success else {
            panic!()
        };
        assert_eq!(
            meta.get("fields"),
            Some(&Value::List(vec![Value::from("x")]))
        );

        let Turn::Reply(replies) = s.on_request(pull(-1)) else {
            panic!()
        };
        assert_eq!(replies.len(), 4); // 3 records + summary
        let Response::Success(summary) = replies.last().unwrap() else {
            panic!()
        };
        assert_eq!(summary.get_str("type"), Some("r"));
        assert_eq!(summary.get("has_more"), Some(&Value::Boolean(false)));
    }

    #[test]
    fn pull_batches_with_has_more() {
        let mut s = ready_session_54();
        s.on_request(run_req("q"));
        s.run_succeeded(three_row_stream(), 0);

        let Turn::Reply(first) = s.on_request(pull(2)) else {
            panic!()
        };
        assert_eq!(first.len(), 3); // 2 records + has_more success
        let Response::Success(meta) = first.last().unwrap() else {
            panic!()
        };
        assert_eq!(meta.get("has_more"), Some(&Value::Boolean(true)));

        let Turn::Reply(rest) = s.on_request(pull(2)) else {
            panic!()
        };
        assert_eq!(rest.len(), 2); // 1 record + final summary
        let Response::Success(summary) = rest.last().unwrap() else {
            panic!()
        };
        assert_eq!(summary.get("has_more"), Some(&Value::Boolean(false)));
    }

    #[test]
    fn discard_finishes_stream() {
        let mut s = ready_session_54();
        s.on_request(run_req("q"));
        s.run_succeeded(three_row_stream(), 0);
        let mut extra = MapValue::new();
        extra.insert("n", -1i64);
        let Turn::Reply(replies) = s.on_request(Request::Discard { extra }) else {
            panic!()
        };
        let Response::Success(summary) = &replies[0] else {
            panic!()
        };
        assert_eq!(summary.get_str("type"), Some("r"));
        // Next RUN is accepted again.
        assert!(matches!(s.on_request(run_req("q2")), Turn::Execute(_)));
    }

    #[test]
    fn run_failure_ignores_pipelined_pull() {
        let mut s = ready_session_54();
        s.on_request(run_req("bad query"));
        let failure = s.run_failed("Neo.ClientError.Statement.SyntaxError", "parse error");
        assert!(matches!(failure, Response::Failure { .. }));
        // The pipelined PULL that followed the RUN must be IGNORED.
        assert_eq!(s.on_request(pull(-1)), Turn::Reply(vec![Response::Ignored]));

        // RESET recovers.
        assert_eq!(
            s.on_request(Request::Reset),
            Turn::Reset(vec![Response::success_empty()])
        );
        assert!(matches!(s.on_request(run_req("q")), Turn::Execute(_)));
    }

    #[test]
    fn reset_mid_stream_drops_result() {
        let mut s = ready_session_54();
        s.on_request(run_req("q"));
        s.run_succeeded(three_row_stream(), 0);
        s.on_request(Request::Reset);
        // Stream gone: PULL now fails (READY state).
        let Turn::Reply(replies) = s.on_request(pull(-1)) else {
            panic!()
        };
        assert!(matches!(replies[0], Response::Failure { .. }));
    }

    #[test]
    fn db_precedence_run_extra_over_hello_over_default() {
        let mut s = Session::new(config(BoltVersion::V4_4));
        let mut hello_extra = MapValue::new();
        hello_extra.insert("db", "hello:db");
        s.on_request(hello(hello_extra));
        s.auth_succeeded();

        let Turn::Execute(run) = s.on_request(run_req("q")) else {
            panic!()
        };
        assert_eq!(run.db.as_deref(), Some("hello:db"));

        let mut run_extra = MapValue::new();
        run_extra.insert("db", "run:db");
        let Turn::Execute(run) = s.on_request(Request::Run {
            query: "q".into(),
            parameters: MapValue::new(),
            extra: run_extra,
        }) else {
            panic!()
        };
        assert_eq!(run.db.as_deref(), Some("run:db"));
    }

    #[test]
    fn goodbye_closes_without_reply() {
        let mut s = ready_session_54();
        assert_eq!(s.on_request(Request::Goodbye), Turn::Close(vec![]));
    }

    #[test]
    fn hello_out_of_order_closes() {
        let mut s = ready_session_54();
        let turn = s.on_request(hello(MapValue::new()));
        assert!(matches!(turn, Turn::Reply(ref r) if matches!(r[0], Response::Failure { .. })));
    }

    #[test]
    fn route_without_advertised_address_fails() {
        let mut s = ready_session_54();
        let Turn::Reply(replies) = s.on_request(Request::Route {
            routing: MapValue::new(),
            bookmarks: vec![],
            extra: Value::Null,
        }) else {
            panic!()
        };
        assert!(matches!(replies[0], Response::Failure { .. }));
    }

    #[test]
    fn route_with_advertised_address_returns_single_entry_table() {
        let mut cfg = config(BoltVersion::V5_4);
        cfg.advertised_address = Some("db.example.com:7687".into());
        let mut s = Session::new(cfg);
        s.on_request(hello(MapValue::new()));
        s.on_request(Request::Logon {
            auth: MapValue::new(),
        });
        s.auth_succeeded();
        let Turn::Reply(replies) = s.on_request(Request::Route {
            routing: MapValue::new(),
            bookmarks: vec![],
            extra: Value::Null,
        }) else {
            panic!()
        };
        let Response::Success(meta) = &replies[0] else {
            panic!()
        };
        let Some(Value::Map(rt)) = meta.get("rt") else {
            panic!("no rt")
        };
        assert_eq!(rt.get_int("ttl"), Some(300));
        let Some(Value::List(servers)) = rt.get("servers") else {
            panic!()
        };
        assert_eq!(servers.len(), 3);
    }

    // ------------------------------------------------------------------
    // Explicit transactions
    // ------------------------------------------------------------------

    #[test]
    fn begin_run_commit_lifecycle() {
        let mut s = ready_session_54();

        let Turn::Begin(begin_req) = s.on_request(begin()) else {
            panic!("expected Turn::Begin")
        };
        assert_eq!(begin_req.db.as_deref(), Some("test:main"));
        assert!(matches!(s.begin_succeeded(), Response::Success(_)));
        assert!(s.in_transaction());

        // RUN inside the transaction.
        assert!(matches!(
            s.on_request(run_req("CREATE (:X)")),
            Turn::Execute(_)
        ));
        s.run_succeeded(ResultStream::default(), 0);
        let Turn::Reply(replies) = s.on_request(pull(-1)) else {
            panic!()
        };
        assert!(matches!(replies.last(), Some(Response::Success(_))));
        assert!(s.in_transaction(), "still in tx after draining a result");

        // COMMIT.
        assert_eq!(s.on_request(Request::Commit), Turn::Commit);
        let reply = s.commit_succeeded(Some("fluree:t:7".into()));
        let Response::Success(meta) = &reply else {
            panic!()
        };
        assert_eq!(meta.get_str("bookmark"), Some("fluree:t:7"));
        assert!(!s.in_transaction());
        // Session is reusable.
        assert!(matches!(s.on_request(run_req("q")), Turn::Execute(_)));
    }

    #[test]
    fn rollback_returns_to_ready() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();
        assert_eq!(s.on_request(Request::Rollback), Turn::Rollback);
        assert!(matches!(s.rollback_done(), Response::Success(_)));
        assert!(!s.in_transaction());
    }

    #[test]
    fn nested_begin_fails() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();
        let Turn::Reply(replies) = s.on_request(begin()) else {
            panic!()
        };
        assert!(matches!(replies[0], Response::Failure { .. }));
    }

    #[test]
    fn commit_outside_transaction_fails() {
        let mut s = ready_session_54();
        let Turn::Reply(replies) = s.on_request(Request::Commit) else {
            panic!()
        };
        assert!(matches!(replies[0], Response::Failure { .. }));
    }

    #[test]
    fn multiple_tx_results_addressed_by_qid() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();

        s.on_request(run_req("q1"));
        let Response::Success(meta1) = s.run_succeeded(stream_of(1..=2), 0) else {
            panic!()
        };
        let qid1 = meta1.get_int("qid").unwrap();

        s.on_request(run_req("q2"));
        let Response::Success(meta2) = s.run_succeeded(stream_of(10..=11), 0) else {
            panic!()
        };
        let qid2 = meta2.get_int("qid").unwrap();
        assert_ne!(qid1, qid2);

        // Pull the FIRST stream explicitly by qid.
        let Turn::Reply(replies) = s.on_request(pull_qid(-1, qid1)) else {
            panic!()
        };
        assert_eq!(replies.len(), 3); // 2 records + summary
        assert_eq!(
            replies[0],
            Response::Record(vec![Value::Integer(1)]),
            "qid must address the first stream"
        );
        assert!(s.in_transaction());

        // Default PULL now drains the remaining (latest) stream.
        let Turn::Reply(replies) = s.on_request(pull(-1)) else {
            panic!()
        };
        assert_eq!(replies[0], Response::Record(vec![Value::Integer(10)]));
        // All streams drained: back to TxReady, still in the transaction.
        assert!(s.in_transaction());
        assert!(matches!(s.on_request(Request::Commit), Turn::Commit));
    }

    #[test]
    fn commit_with_unconsumed_results_discards_them() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();
        s.on_request(run_req("q"));
        s.run_succeeded(three_row_stream(), 0);
        assert_eq!(s.on_request(Request::Commit), Turn::Commit);
        s.commit_succeeded(None);
        assert!(!s.in_transaction());
    }

    #[test]
    fn tx_statement_failure_poisons_until_reset() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();
        s.on_request(run_req("bad"));
        s.run_failed("Neo.ClientError.Statement.SyntaxError", "boom");

        // COMMIT after an in-tx failure is IGNORED (driver must RESET).
        assert_eq!(
            s.on_request(Request::Commit),
            Turn::Reply(vec![Response::Ignored])
        );
        let turn = s.on_request(Request::Reset);
        assert!(matches!(turn, Turn::Reset(_)), "reset drops the tx");
        assert!(!s.in_transaction());
        assert!(matches!(s.on_request(run_req("q")), Turn::Execute(_)));
    }

    #[test]
    fn reset_mid_transaction_signals_caller() {
        let mut s = ready_session_54();
        s.on_request(begin());
        s.begin_succeeded();
        let turn = s.on_request(Request::Reset);
        assert_eq!(turn, Turn::Reset(vec![Response::success_empty()]));
        assert!(!s.in_transaction());
    }

    #[test]
    fn begin_failure_poisons_session() {
        let mut s = ready_session_54();
        s.on_request(begin());
        let reply = s.begin_failed("Neo.ClientError.Request.Invalid", "no tx here");
        assert!(matches!(reply, Response::Failure { .. }));
        assert_eq!(
            s.on_request(run_req("q")),
            Turn::Reply(vec![Response::Ignored])
        );
    }
}
