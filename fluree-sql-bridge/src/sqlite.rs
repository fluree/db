use futures::TryStreamExt;
use serde_json::Value;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Executor, Row, TypeInfo, ValueRef};
use tokio::sync::mpsc;

use crate::backend::{flush, Backend, ColumnMeta, RowChunk, Session, CHUNK_ROWS};
use crate::render::{self, trino};

pub struct Sqlite {
    pool: SqlitePool,
}

impl Sqlite {
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, String> {
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await
            .map_err(|e| format!("connect sqlite: {e}"))?;
        Ok(Self { pool })
    }
}

/// SQLite is dynamically typed; the declared type only hints. Everything
/// unknown is read as text, which is what SQLite itself would hand back.
fn trino_type(t: &str) -> &'static str {
    match t.to_ascii_uppercase().as_str() {
        "BOOLEAN" => trino::BOOLEAN,
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => trino::BIGINT,
        "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => trino::DOUBLE,
        "BLOB" => trino::VARBINARY,
        "DATE" => trino::DATE,
        "DATETIME" | "TIMESTAMP" => trino::TIMESTAMP,
        _ => trino::VARCHAR,
    }
}

fn cell(row: &SqliteRow, i: usize, trino: &str) -> Result<Value, String> {
    let raw = row.try_get_raw(i).map_err(|e| e.to_string())?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    macro_rules! get {
        ($ty:ty) => {
            row.try_get::<$ty, _>(i)
                .map_err(|e| format!("column {i}: {e}"))?
        };
    }
    Ok(match trino {
        t if t == trino::BOOLEAN => render::bool(get!(bool)),
        t if t == trino::BIGINT => render::int(get!(i64)),
        t if t == trino::DOUBLE => render::double(get!(f64)),
        t if t == trino::VARBINARY => render::bytes(&get!(Vec<u8>)),
        t if t == trino::DATE => render::date(get!(chrono::NaiveDate)),
        t if t == trino::TIMESTAMP => render::timestamp(get!(chrono::NaiveDateTime)),
        _ => render::string(get!(String)),
    })
}

#[async_trait::async_trait]
impl Backend for Sqlite {
    fn dialect(&self) -> &'static str {
        "sqlite"
    }

    async fn start(
        &self,
        sql: String,
        _session: Session,
        tx: mpsc::Sender<RowChunk>,
    ) -> Result<Vec<ColumnMeta>, String> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| format!("acquire: {e}"))?;
        let described = conn.describe(&sql).await.map_err(|e| e.to_string())?;
        let columns: Vec<ColumnMeta> = described
            .columns()
            .iter()
            .map(|c| ColumnMeta {
                name: c.name().to_string(),
                trino_type: trino_type(c.type_info().name()).to_string(),
            })
            .collect();
        let types: Vec<String> = columns.iter().map(|c| c.trino_type.clone()).collect();

        tokio::spawn(async move {
            let mut stream = sqlx::query(&sql).fetch(&mut *conn);
            let mut buf: Vec<Vec<Value>> = Vec::with_capacity(CHUNK_ROWS);
            loop {
                match stream.try_next().await {
                    Ok(Some(row)) => {
                        let mut out = Vec::with_capacity(types.len());
                        for (i, t) in types.iter().enumerate() {
                            match cell(&row, i, t) {
                                Ok(v) => out.push(v),
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                        buf.push(out);
                        if buf.len() >= CHUNK_ROWS && !flush(&tx, &mut buf).await {
                            return;
                        }
                    }
                    Ok(None) => {
                        flush(&tx, &mut buf).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string())).await;
                        return;
                    }
                }
            }
        });
        Ok(columns)
    }
}
