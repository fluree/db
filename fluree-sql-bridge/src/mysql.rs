use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Executor, Row, TypeInfo, ValueRef};
use tokio::sync::mpsc;

use crate::backend::{flush, Backend, ColumnMeta, RowChunk, Session, CHUNK_ROWS};
use crate::render::{self, trino};

pub struct MySql {
    pool: MySqlPool,
    decimal_scale: i64,
}

impl MySql {
    pub async fn connect(
        url: &str,
        max_connections: u32,
        decimal_scale: i64,
    ) -> Result<Self, String> {
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await
            .map_err(|e| format!("connect mysql: {e}"))?;
        Ok(Self {
            pool,
            decimal_scale,
        })
    }
}

fn base_type(t: &str) -> (&str, bool) {
    match t.strip_suffix(" UNSIGNED") {
        Some(b) => (b, true),
        None => (t, false),
    }
}

fn trino_type(t: &str, scale: i64) -> Result<String, String> {
    let (base, unsigned) = base_type(t);
    Ok(match base {
        "BOOLEAN" => trino::BOOLEAN.into(),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "YEAR" => trino::INTEGER.into(),
        "INT" => {
            if unsigned {
                trino::BIGINT.into()
            } else {
                trino::INTEGER.into()
            }
        }
        "BIGINT" | "BIT" => trino::BIGINT.into(),
        "FLOAT" => trino::REAL.into(),
        "DOUBLE" => trino::DOUBLE.into(),
        "DECIMAL" => trino::decimal(scale),
        "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET"
        | "JSON" => trino::VARCHAR.into(),
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            trino::VARBINARY.into()
        }
        "DATE" => trino::DATE.into(),
        "DATETIME" => trino::TIMESTAMP.into(),
        "TIMESTAMP" => trino::TIMESTAMP_TZ.into(),
        "TIME" => trino::TIME.into(),
        other => {
            return Err(format!(
                "unsupported MySQL column type {other}; CAST it in the rr:sqlQuery"
            ))
        }
    })
}

fn cell(row: &MySqlRow, i: usize, t: &str, scale: i64) -> Result<Value, String> {
    let raw = row.try_get_raw(i).map_err(|e| e.to_string())?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    macro_rules! get {
        ($ty:ty) => {
            row.try_get::<$ty, _>(i)
                .map_err(|e| format!("column {i} ({t}): {e}"))?
        };
    }
    let (base, unsigned) = base_type(t);
    Ok(match (base, unsigned) {
        ("BOOLEAN", _) => render::bool(get!(bool)),
        ("TINYINT", false) => render::int(i64::from(get!(i8))),
        ("TINYINT", true) => render::int(i64::from(get!(u8))),
        ("SMALLINT" | "YEAR", false) => render::int(i64::from(get!(i16))),
        ("SMALLINT" | "YEAR", true) => render::int(i64::from(get!(u16))),
        ("MEDIUMINT" | "INT", false) => render::int(i64::from(get!(i32))),
        ("MEDIUMINT" | "INT", true) => render::int(i64::from(get!(u32))),
        ("BIGINT", false) => render::int(get!(i64)),
        ("BIGINT", true) => render::uint(get!(u64)),
        ("BIT", _) => render::uint(get!(u64)),
        ("FLOAT", _) => render::double(f64::from(get!(f32))),
        ("DOUBLE", _) => render::double(get!(f64)),
        ("DECIMAL", _) => render::decimal(&get!(sqlx::types::BigDecimal), scale),
        (
            "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET",
            _,
        ) => render::string(get!(String)),
        ("JSON", _) => render::jsonish(&get!(serde_json::Value)),
        ("BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB", _) => {
            render::bytes(&get!(Vec<u8>))
        }
        ("DATE", _) => render::date(get!(chrono::NaiveDate)),
        ("DATETIME", _) => render::timestamp(get!(chrono::NaiveDateTime)),
        ("TIMESTAMP", _) => render::timestamp_tz(get!(chrono::DateTime<chrono::Utc>)),
        ("TIME", _) => render::time(get!(chrono::NaiveTime)),
        (other, _) => return Err(format!("unsupported MySQL column type {other}")),
    })
}

#[async_trait::async_trait]
impl Backend for MySql {
    fn dialect(&self) -> &'static str {
        "mysql"
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
            let stmt = format!("USE `{}`", schema.replace('`', "``"));
            sqlx::query(&stmt)
                .execute(&mut *conn)
                .await
                .map_err(|e| format!("use database: {e}"))?;
        }
        let described = conn.describe(&sql).await.map_err(|e| e.to_string())?;
        let types: Vec<String> = described
            .columns()
            .iter()
            .map(|c| c.type_info().name().to_string())
            .collect();
        let scale = self.decimal_scale;
        let mut columns = Vec::with_capacity(types.len());
        for (c, t) in described.columns().iter().zip(&types) {
            columns.push(ColumnMeta {
                name: c.name().to_string(),
                trino_type: trino_type(t, scale)?,
            });
        }

        tokio::spawn(async move {
            let mut stream = sqlx::query(&sql).fetch(&mut *conn);
            let mut buf: Vec<Vec<Value>> = Vec::with_capacity(CHUNK_ROWS);
            loop {
                match stream.try_next().await {
                    Ok(Some(row)) => {
                        let mut out = Vec::with_capacity(types.len());
                        for (i, t) in types.iter().enumerate() {
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
