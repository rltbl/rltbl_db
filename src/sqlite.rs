//! SQLite support for sql_json.

use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

use deadpool_sqlite::{
    Config, Object, Pool, Runtime,
    rusqlite::{
        Row as RusqliteRow, Statement as RusqliteStatement, Transaction as RusqliteTransaction,
        types::ValueRef as RusqliteValueRef,
    },
};
use serde_json::json;

/// Represents a SQLite database connection pool
pub struct SqliteConnection {
    pool: Pool,
}

impl SqliteConnection {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let cfg = Config::new(url);
        let pool = cfg
            .create_pool(Runtime::Tokio1)
            .map_err(|err| format!("Error creating pool: {err}"))?;
        Ok(Self { pool })
    }

    pub async fn get(&self) -> Result<Object, DbError> {
        self.pool
            .get()
            .await
            .map_err(|err| format!("Error creating pool: {err}"))
    }

    // pub async fn begin<'a>(
    //     conn: &'a mut RusqliteConnection,
    // ) -> Result<SqliteTransaction<'a>, DbError> {
    //     let tx = conn
    //         .transaction()
    //         .map_err(|err| format!("Error creating transaction: {err}"))?;
    //     Ok(SqliteTransaction { tx })
    // }
}

// TODO: Change the type of the error from String to something custom (DbError is not appropriate,
// however, even though that is currently implemented as a String).
/// Extract the first value of the first row in `rows`.
fn extract_value(rows: &Vec<JsonRow>) -> Result<JsonValue, String> {
    match rows.iter().next() {
        Some(row) => match row.values().next() {
            Some(value) => Ok(value.clone()),
            None => Err("No values found".into()),
        },
        None => Err("No rows found".into()),
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
        let params = params.as_array().ok_or(format!(
            "Parameters: {params:?} are not in the form of an array"
        ))?;
        for (i, param) in params.iter().enumerate() {
            let param = match param {
                JsonValue::String(s) => s,
                _ => &param.to_string(),
            };
            // Binding must begin with 1 rather than 0:
            stmt.raw_bind_parameter(i + 1, param)
                .map_err(|err| format!("Error binding parameter '{param}': {err}"))?;
        }
    }
    let mut rows = stmt.raw_query();

    let mut result = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| format!("Error getting next row: {err}"))?
    {
        result.push(to_json_row(&column_names, row));
    }
    Ok(result)
}

pub struct SqliteTransaction<'a> {
    pub tx: RusqliteTransaction<'a>,
}

impl<'a> SqliteTransaction<'a> {
    pub async fn execute(&self, sql: &str, _params: &[JsonValue]) -> Result<(), DbError> {
        self.tx
            .execute(sql, ())
            .map_err(|err| format!("Error executing SQL: {err}"))?;
        Ok(())
    }

    pub async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let sql = sql.to_string();
        let params = json!(params);
        let mut stmt = self.tx.prepare(&sql).map_err(|err| {
            <String as Into<DbError>>::into(format!("Error preparing statement: {err}"))
        })?;
        let rows = query_statement(&mut stmt, Some(&params)).map_err(|err| {
            <String as Into<DbError>>::into(format!("Error while querying: {err}"))
        })?;
        Ok(rows)
    }

    pub async fn commit(self) -> Result<(), DbError> {
        self.tx
            .commit()
            .map_err(|err| format!("Error executing SQL: {err}"))
    }

    pub async fn rollback(self) -> Result<(), DbError> {
        self.tx
            .rollback()
            .map_err(|err| format!("Error executing SQL: {err}"))
    }
}

impl DbQuery for SqliteConnection {
    /// Implements [DbQuery::execute()] for SQLite.
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Implements [DbQuery::query()] for SQLite.
    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let sql = sql.to_string();
        let params = json!(params);
        let rows = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(|err| {
                    <String as Into<DbError>>::into(format!("Error preparing statement: {err}"))
                })?;
                let rows = query_statement(&mut stmt, Some(&params)).map_err(|err| {
                    <String as Into<DbError>>::into(format!("Error while querying: {err}"))
                })?;
                Ok::<Vec<JsonRow>, DbError>(rows)
            })
            .await
            .map_err(|err| <String as Into<DbError>>::into(err.to_string()))?
            .map_err(|err| <String as Into<DbError>>::into(err.to_string()))?;
        Ok(rows)
    }

    /// Implements [DbQuery::query_row()] for SQLite.
    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let rows = self.query(&sql, params).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_row()");
        }
        match rows.iter().next() {
            Some(row) => Ok(row.clone()),
            None => Err("No row found".to_string()),
        }
    }

    /// Implements [DbQuery::query_value()] for SQLite.
    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let rows = self.query(sql, params).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_value()");
        }
        Ok(extract_value(&rows)?)
    }

    /// Implements [DbQuery::query_string()] for SQLite.
    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_str() {
            Some(str_val) => Ok(str_val.to_string()),
            None => {
                tracing::warn!("Not a string: {value}");
                Ok(value.to_string())
            }
        }
    }

    /// Implements [DbQuery::query_u64()] for SQLite.
    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => Err(format!("Not an unsigned integer: {value}")),
        }
    }

    /// Implements [DbQuery::query_i64()] for SQLite.
    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(format!("Not an integer: {value}")),
        }
    }

    /// Implements [DbQuery::query_f64()] for SQLite.
    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(format!("Not an float: {value}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

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
        conn.execute("INSERT INTO test_table_text VALUES (?)", &[json!("foo")])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text WHERE value = ?";
        let value = conn
            .query_value(select_sql, &[json!("foo")])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let value = conn
            .query_string(select_sql, &[json!("foo")])
            .await
            .unwrap();
        assert_eq!("foo", value);

        let row = conn.query_row(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(row), json!({"value":"foo"}));

        let rows = conn.query(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":"foo"}]));
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
        conn.execute("INSERT INTO test_table_int VALUES (?)", &[json!(1)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_int WHERE value = ?";
        let value = conn
            .query_value(select_sql, &[json!(1)])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(1, value);

        let value = conn.query_u64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_i64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_string(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!("1", value);

        let row = conn.query_row(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1}));

        let rows = conn.query(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1}]));
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
        conn.execute("INSERT INTO test_table_float VALUES (?)", &[json!(1.05)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > ?";
        let value = conn
            .query_value(select_sql, &[json!(1)])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = conn.query_f64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1.05, value);

        let value = conn.query_string(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!("1.05", value);

        let row = conn.query_row(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = conn.query(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        let conn = SqliteConnection::connect("test_mixed_columns.db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_mixed", &[])
            .await
            .unwrap();
        conn.execute(
            r#"CREATE TABLE test_table_mixed (
                 text_value TEXT,
                 float_value FLOAT8,
                 int_value INT8
               )"#,
            &[],
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_mixed
               (text_value, float_value, int_value)
               VALUES ($1, $2, $3)"#,
            &[json!("foo"), json!(1.05), json!(1)],
        )
        .await
        .unwrap();

        let select_sql = r#"SELECT text_value, float_value, int_value
                            FROM test_table_mixed
                            WHERE text_value = $1 AND float_value > $2 AND int_value > $3"#;
        let params = [json!("foo"), json!(1.0), json!(0)];
        let value = conn
            .query_value(select_sql, &params)
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let row = conn.query_row(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(row),
            json!({
                "text_value": "foo",
                "float_value": 1.05,
                "int_value": 1,
            })
        );

        let rows = conn.query(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "foo",
                "float_value": 1.05,
                "int_value": 1,
            }])
        );
    }

    #[tokio::test]
    async fn test_tx() {
        let conn = SqliteConnection::connect("test_mixed_columns.db")
            .await
            .unwrap();
        let obj = conn.get().await.unwrap();
        let mut lock = obj.lock().unwrap();
        let tx = lock.transaction().unwrap();
        let rows = select_1(&tx);
        tx.commit().unwrap();
        assert_eq!(json!(rows), json!([{"1": 1}]));
    }

    fn select_1(tx: &RusqliteTransaction) -> Vec<JsonRow> {
        let sql = "SELECT 1";
        let mut stmt = tx
            .prepare(&sql)
            .map_err(|err| {
                <String as Into<DbError>>::into(format!("Error preparing statement: {err}"))
            })
            .unwrap();
        query_statement(&mut stmt, None)
            .map_err(|err| <String as Into<DbError>>::into(format!("Error while querying: {err}")))
            .unwrap()
    }
}
