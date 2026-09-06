//! One trait over the three drivers: describe a statement's columns, then
//! stream its rows as protocol-ready JSON.

use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    /// Trino type name (`bigint`, `decimal(38,6)`, …).
    pub trino_type: String,
}

/// Chunks of rows, or the error that ended the statement.
pub type RowChunk = Result<Vec<Vec<Value>>, String>;

pub const CHUNK_ROWS: usize = 500;

#[derive(Debug, Clone)]
pub struct Session {
    /// `X-Trino-Schema`: Postgres `search_path` / MySQL default database.
    pub schema: Option<String>,
}

#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    fn dialect(&self) -> &'static str;

    /// Prepare the statement, report its columns, and start streaming rows
    /// into `tx` from a spawned task.
    async fn start(
        &self,
        sql: String,
        session: Session,
        tx: mpsc::Sender<RowChunk>,
    ) -> Result<Vec<ColumnMeta>, String>;
}

/// Drain a chunk buffer into the channel; `false` when the consumer is gone.
pub async fn flush(tx: &mpsc::Sender<RowChunk>, buf: &mut Vec<Vec<Value>>) -> bool {
    if buf.is_empty() {
        return true;
    }
    tx.send(Ok(std::mem::take(buf))).await.is_ok()
}
