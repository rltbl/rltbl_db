//! SQLite support for sql_json.

use crate::core::{DbConnection, DbError, JsonRow, JsonValue};

use deadpool_sqlite::{
    rusqlite::{
        types::ValueRef as RusqliteValueRef, Row as RusqliteRow, Statement as RusqliteStatement,
    },
    Config, Pool, Runtime,
};

/// Represents a SQLite database connection pool
pub struct SqliteConnection {
    pool: Pool,
}

impl SqliteConnection {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<impl DbConnection, DbError> {
        let cfg = Config::new(url);
        let pool = cfg.create_pool(Runtime::Tokio1).unwrap();
        Ok(Self { pool })
    }
}

/// Extract the first value of the first row in `rows`.
fn extract_value(rows: &Vec<JsonRow>) -> JsonValue {
    match rows.iter().next() {
        Some(row) => match row.values().next() {
            Some(value) => value.clone(),
            None => panic!("No values found"),
        },
        None => panic!("No rows found"),
    }
}

fn to_json_row(column_names: &Vec<&str>, row: &RusqliteRow) -> JsonRow {
    let mut json_row = JsonRow::new();
    for column_name in column_names {
        let value = match row.get_ref(*column_name) {
            Ok(value) => match value {
                RusqliteValueRef::Null => JsonValue::Null,
                RusqliteValueRef::Integer(value) => JsonValue::from(value),
                RusqliteValueRef::Real(value) => JsonValue::from(value),
                RusqliteValueRef::Text(value) | RusqliteValueRef::Blob(value) => {
                    let value = std::str::from_utf8(value).unwrap_or_default();
                    JsonValue::from(value)
                }
            },
            Err(_) => JsonValue::Null,
        };
        json_row.insert(column_name.to_string(), value);
    }
    json_row
}

fn query_statement(
    stmt: &mut RusqliteStatement<'_>,
    params: Option<&JsonValue>,
) -> Result<Vec<JsonRow>, DbError> {
    let column_names = stmt
        .column_names()
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>();
    let column_names = column_names.iter().map(|c| c.as_str()).collect::<Vec<_>>();

    if let Some(params) = params {
        for (i, param) in params.as_array().unwrap().iter().enumerate() {
            let param = match param {
                JsonValue::String(s) => s,
                _ => &param.to_string(),
            };
            // Binding must begin with 1 rather than 0:
            stmt.raw_bind_parameter(i + 1, param).unwrap();
        }
    }
    let mut rows = stmt.raw_query();

    let mut result = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        result.push(to_json_row(&column_names, row));
    }
    Ok(result)
}

impl DbConnection for SqliteConnection {
    /// Implements [DbConnection::execute()] for SQLite.
    async fn execute(&self, sql: &str, _params: &[JsonValue]) -> Result<(), DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let sql = sql.to_string();
        // TODO: Remove panics, expects, and unwraps here and elsewhere in this file.
        conn.interact(move |conn| {
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.execute([]).unwrap();
        })
        .await
        .unwrap();
        Ok(())
    }

    /// Implements [DbConnection::query()] for SQLite.
    async fn query(&self, sql: &str, _params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let sql = sql.to_string();
        let rows = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql).unwrap();
                query_statement(&mut stmt, None).unwrap()
            })
            .await
            .unwrap();
        Ok(rows)
    }

    /// Implements [DbConnection::query_row()] for SQLite.
    async fn query_row(&self, sql: &str, _params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let rows = self.query(&sql, &[]).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_row()");
        }
        match rows.iter().next() {
            Some(row) => Ok(row.clone()),
            None => Err("No row found".to_string()),
        }
    }

    /// Implements [DbConnection::query_value()] for SQLite.
    async fn query_value(&self, sql: &str, _params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let rows = self.query(sql, &[]).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_value()");
        }
        Ok(extract_value(&rows))
    }

    /// Implements [DbConnection::query_string()] for SQLite.
    async fn query_string(&self, sql: &str, _params: &[JsonValue]) -> Result<String, DbError> {
        let value = self.query_value(sql, &[]).await?;
        match value.as_str() {
            Some(str_val) => Ok(str_val.to_string()),
            None => {
                tracing::warn!("Not a string: {value}");
                Ok(value.to_string())
            }
        }
    }

    /// Implements [DbConnection::query_u64()] for SQLite.
    async fn query_u64(&self, sql: &str, _params: &[JsonValue]) -> Result<u64, DbError> {
        let value = self.query_value(sql, &[]).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => panic!("Not an unsigned integer: {value}"),
        }
    }

    /// Implements [DbConnection::query_i64()] for SQLite.
    async fn query_i64(&self, sql: &str, _params: &[JsonValue]) -> Result<i64, DbError> {
        let value = self.query_value(sql, &[]).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => panic!("Not an integer: {value}"),
        }
    }

    /// Implements [DbConnection::query_f64()] for SQLite.
    async fn query_f64(&self, sql: &str, _params: &[JsonValue]) -> Result<f64, DbError> {
        let value = self.query_value(sql, &[]).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => panic!("Not an float: {value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_text_column_query() {
        let conn = SqliteConnection::connect("test_text_column.db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_text", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_text ( value TEXT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_text VALUES ('foo')", &[])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text LIMIT 1";
        let value = conn
            .query_value(select_sql, &[])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let value = conn.query_string(select_sql, &[]).await.unwrap();
        assert_eq!("foo", value);

        let row = conn.query_row(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{row:?}"), r#"{"value": String("foo")}"#);

        let rows = conn.query(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{rows:?}"), r#"[{"value": String("foo")}]"#);
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        let conn = SqliteConnection::connect("test_integer_column.db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_int", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_int ( value INT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_int VALUES (1)", &[])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_int LIMIT 1";
        let value = conn
            .query_value(select_sql, &[])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(1, value);

        let value = conn.query_u64(select_sql, &[]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_i64(select_sql, &[]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_string(select_sql, &[]).await.unwrap();
        assert_eq!("1", value);

        let row = conn.query_row(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{row:?}"), r#"{"value": Number(1)}"#);

        let rows = conn.query(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{rows:?}"), r#"[{"value": Number(1)}]"#);
    }

    #[tokio::test]
    async fn test_float_column_query() {
        let conn = SqliteConnection::connect("test_float_column.db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_float", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_float ( value REAL )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_float VALUES (1.05)", &[])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float LIMIT 1";
        let value = conn
            .query_value(select_sql, &[])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = conn.query_f64(select_sql, &[]).await.unwrap();
        assert_eq!(1.05, value);

        let value = conn.query_string(select_sql, &[]).await.unwrap();
        assert_eq!("1.05", value);

        let row = conn.query_row(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{row:?}"), r#"{"value": Number(1.05)}"#);

        let rows = conn.query(select_sql, &[]).await.unwrap();
        assert_eq!(format!("{rows:?}"), r#"[{"value": Number(1.05)}]"#);
    }
}
