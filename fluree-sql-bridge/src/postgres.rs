use futures::TryStreamExt;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Executor, Row, TypeInfo, ValueRef};
use tokio::sync::mpsc;

use crate::backend::{flush, Backend, ColumnMeta, RowChunk, Session, CHUNK_ROWS};
use crate::render::{self, trino};

/// Keep Postgres on the standard-SQL string rule this bridge's clients assume.
///
/// `standard_conforming_strings` has defaulted to `on` since 9.1, which is why
/// Postgres is not exposed the way MySQL is (see `mysql.rs`). But it is still a
/// settable GUC: a server, database or role with it `off` would process
/// backslash escapes inside ordinary literals, and a value ending in one could
/// escape its own closing quote. Setting it per session costs nothing and
/// removes the dependency on how the server happens to be configured.
const ENFORCE_STANDARD_STRING_LITERALS: &str = "SET standard_conforming_strings = on";

pub struct Postgres {
    pool: PgPool,
    decimal_scale: i64,
}

impl Postgres {
    pub async fn connect(
        url: &str,
        max_connections: u32,
        decimal_scale: i64,
    ) -> Result<Self, String> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute(ENFORCE_STANDARD_STRING_LITERALS).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(|e| format!("connect postgres: {e}"))?;
        Ok(Self {
            pool,
            decimal_scale,
        })
    }

    fn trino_type(&self, pg: &str) -> Result<String, String> {
        Ok(match pg {
            "BOOL" => trino::BOOLEAN.into(),
            "INT2" | "INT4" => trino::INTEGER.into(),
            "INT8" | "OID" => trino::BIGINT.into(),
            "FLOAT4" => trino::REAL.into(),
            "FLOAT8" => trino::DOUBLE.into(),
            "NUMERIC" | "MONEY" => trino::decimal(self.decimal_scale),
            "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "NAME" | "UUID" | "JSON" | "JSONB"
            | "INTERVAL" | "CITEXT" => trino::VARCHAR.into(),
            "BYTEA" => trino::VARBINARY.into(),
            "DATE" => trino::DATE.into(),
            "TIMESTAMP" => trino::TIMESTAMP.into(),
            "TIMESTAMPTZ" => trino::TIMESTAMP_TZ.into(),
            "TIME" => trino::TIME.into(),
            other if other.ends_with("[]") => trino::VARCHAR.into(),
            other => {
                return Err(format!(
                    "unsupported Postgres column type {other}; CAST it in the rr:sqlQuery"
                ))
            }
        })
    }
}

fn cell(row: &PgRow, i: usize, pg: &str, decimal_scale: i64) -> Result<Value, String> {
    let raw = row.try_get_raw(i).map_err(|e| e.to_string())?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    macro_rules! get {
        ($t:ty) => {
            row.try_get::<$t, _>(i)
                .map_err(|e| format!("column {i} ({pg}): {e}"))?
        };
    }
    Ok(match pg {
        "BOOL" => render::bool(get!(bool)),
        "INT2" => render::int(i64::from(get!(i16))),
        "INT4" => render::int(i64::from(get!(i32))),
        "INT8" => render::int(get!(i64)),
        "OID" => render::uint(u64::from(get!(sqlx::postgres::types::Oid).0)),
        "FLOAT4" => render::double(f64::from(get!(f32))),
        "FLOAT8" => render::double(get!(f64)),
        "NUMERIC" => render::decimal(&get!(sqlx::types::BigDecimal), decimal_scale),
        "MONEY" => {
            let m = get!(sqlx::postgres::types::PgMoney);
            render::decimal(&m.to_bigdecimal(2), decimal_scale)
        }
        "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "NAME" | "CITEXT" => render::string(get!(String)),
        "UUID" => render::string(get!(sqlx::types::Uuid).to_string()),
        "JSON" | "JSONB" => render::jsonish(&get!(serde_json::Value)),
        "INTERVAL" => {
            let iv = get!(sqlx::postgres::types::PgInterval);
            render::string(format!(
                "{} months {} days {} microseconds",
                iv.months, iv.days, iv.microseconds
            ))
        }
        "BYTEA" => render::bytes(&get!(Vec<u8>)),
        "DATE" => render::date(get!(chrono::NaiveDate)),
        "TIMESTAMP" => render::timestamp(get!(chrono::NaiveDateTime)),
        "TIMESTAMPTZ" => render::timestamp_tz(get!(chrono::DateTime<chrono::Utc>)),
        "TIME" => render::time(get!(chrono::NaiveTime)),
        "TEXT[]" | "VARCHAR[]" => Value::String(serde_json::to_string(&get!(Vec<String>)).unwrap()),
        "INT4[]" => Value::String(serde_json::to_string(&get!(Vec<i32>)).unwrap()),
        "INT8[]" => Value::String(serde_json::to_string(&get!(Vec<i64>)).unwrap()),
        "FLOAT8[]" => Value::String(serde_json::to_string(&get!(Vec<f64>)).unwrap()),
        "BOOL[]" => Value::String(serde_json::to_string(&get!(Vec<bool>)).unwrap()),
        other => return Err(format!("unsupported Postgres column type {other}")),
    })
}

#[async_trait::async_trait]
impl Backend for Postgres {
    fn dialect(&self) -> &'static str {
        "postgres"
    }

    async fn start(
        &self,
        sql: String,
        session: Session,
        tx: mpsc::Sender<RowChunk>,
    ) -> Result<Vec<ColumnMeta>, String> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| format!("acquire: {e}"))?;
        if let Some(schema) = &session.schema {
            let stmt = format!("SET search_path TO {}", quote_ident(schema));
            sqlx::query(&stmt)
                .execute(&mut *conn)
                .await
                .map_err(|e| format!("set search_path: {e}"))?;
        }
        let described = conn.describe(&sql).await.map_err(|e| e.to_string())?;
        let pg_types: Vec<String> = described
            .columns()
            .iter()
            .map(|c| c.type_info().name().to_string())
            .collect();
        let mut columns = Vec::with_capacity(pg_types.len());
        for (c, t) in described.columns().iter().zip(&pg_types) {
            columns.push(ColumnMeta {
                name: c.name().to_string(),
                trino_type: self.trino_type(t)?,
            });
        }

        let scale = self.decimal_scale;
        tokio::spawn(async move {
            let mut stream = sqlx::query(&sql).fetch(&mut *conn);
            let mut buf: Vec<Vec<Value>> = Vec::with_capacity(CHUNK_ROWS);
            loop {
                match stream.try_next().await {
                    Ok(Some(row)) => {
                        let mut out = Vec::with_capacity(pg_types.len());
                        for (i, t) in pg_types.iter().enumerate() {
                            match cell(&row, i, t, scale) {
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

pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
