use std::ffi::{CStr, CString};

use futures::TryStreamExt;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnection, SqlitePool, SqlitePoolOptions, SqliteRow};
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

/// The Trino type for a column, from its *declared* type when the statement
/// reads a table column and from the driver's inference otherwise.
///
/// SQLite is dynamically typed: a `NUMERIC` column holds `5.00` as an
/// INTEGER and `99.50` as a REAL. The driver cannot parse `NUMERIC` or
/// `DECIMAL(10,2)` as a declared type and falls back to the storage class of
/// the *first row*, which would report the column as `bigint` and truncate
/// `99.50` to `99` on the way out. So the declared type is read directly and
/// mapped by SQLite's own affinity rules; only an expression column (no
/// declared type) takes the driver's inference.
fn trino_type(declared: Option<&str>, inferred: &str) -> &'static str {
    match declared {
        Some(decl) => declared_type(decl),
        None => inferred_type(inferred),
    }
}

/// SQLite's affinity rules over a declared type name (`DECIMAL(10,2)`,
/// `VARCHAR(20)`, …), with the date and boolean names the driver also
/// recognizes taken exactly. A column of NUMERIC affinity is a `double`,
/// the one Trino type every storage class it may hold converts to.
fn declared_type(decl: &str) -> &'static str {
    let base = decl
        .split('(')
        .next()
        .unwrap_or(decl)
        .trim()
        .to_ascii_uppercase();
    match base.as_str() {
        "BOOLEAN" | "BOOL" => return trino::BOOLEAN,
        "DATE" => return trino::DATE,
        "DATETIME" | "TIMESTAMP" => return trino::TIMESTAMP,
        _ => {}
    }
    if base.contains("INT") {
        trino::BIGINT
    } else if base.contains("CHAR") || base.contains("CLOB") || base.contains("TEXT") {
        trino::VARCHAR
    } else if base.contains("BLOB") || base.is_empty() {
        trino::VARBINARY
    } else if base.contains("REAL") || base.contains("FLOA") || base.contains("DOUB") {
        trino::DOUBLE
    } else {
        // NUMERIC affinity: NUMERIC, DECIMAL, and any other name.
        trino::DOUBLE
    }
}

/// The driver's inferred type name for an expression column (what it can
/// prove for the expression, else the first row's storage class).
/// Everything unknown is read as text, which is what SQLite itself would
/// hand back.
fn inferred_type(t: &str) -> &'static str {
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

/// The declared type of each result column of `sql`'s first statement
/// (`sqlite3_column_decltype`, `NULL` for an expression). Empty when the
/// statement does not prepare; the driver reports that error itself.
async fn declared_types(conn: &mut SqliteConnection, sql: &str) -> Vec<Option<String>> {
    use libsqlite3_sys as ffi;
    let Ok(c_sql) = CString::new(sql) else {
        return Vec::new();
    };
    let Ok(mut handle) = conn.lock_handle().await else {
        return Vec::new();
    };
    let db = handle.as_raw_handle().as_ptr();
    let mut stmt = std::ptr::null_mut();
    // SAFETY: `db` is the live connection handle the lock guards, `c_sql` is
    // NUL-terminated and outlives the call, and the statement is finalized
    // before the handle is released.
    unsafe {
        let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
        if rc != ffi::SQLITE_OK || stmt.is_null() {
            return Vec::new();
        }
        let out = (0..ffi::sqlite3_column_count(stmt))
            .map(|i| {
                let p = ffi::sqlite3_column_decltype(stmt, i);
                (!p.is_null()).then(|| CStr::from_ptr(p).to_string_lossy().into_owned())
            })
            .collect();
        ffi::sqlite3_finalize(stmt);
        out
    }
}

fn cell(row: &SqliteRow, i: usize, trino: &str) -> Result<Value, String> {
    let raw = row.try_get_raw(i).map_err(|e| e.to_string())?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    // Unchecked: SQLite stores each cell in its own storage class (a
    // `NUMERIC` column holds `5.00` as an INTEGER and `99.50` as a REAL), so
    // the declared type is a conversion target, not a guarantee.
    //
    // `bigint` is the one column where conversion is not wanted. It comes only
    // from an INTEGER-affinity declaration, and SQLite already stores every
    // losslessly-integral value there as an INTEGER (`3.0` lands as one), so a
    // REAL, TEXT or BLOB cell is bad data rather than a representation choice.
    // Converting it would emit `2` for `2.5` and `0` for `'abc'` — a fabricated
    // number no consumer can tell from a real one. Reject it, as the checked
    // decode did before mixed storage classes needed reading.
    let storage_info = raw.type_info();
    let storage = storage_info.name();
    if trino == trino::BIGINT && storage != "INTEGER" {
        return Err(format!(
            "column {i}: {storage} value in an INTEGER-affinity column; \
             declare it NUMERIC to read mixed storage as a double"
        ));
    }
    macro_rules! get {
        ($ty:ty) => {
            row.try_get_unchecked::<$ty, _>(i)
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
        let declared = declared_types(&mut conn, &sql).await;
        let columns: Vec<ColumnMeta> = described
            .columns()
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnMeta {
                name: c.name().to_string(),
                trino_type: trino_type(
                    declared.get(i).and_then(|d| d.as_deref()),
                    c.type_info().name(),
                )
                .to_string(),
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
