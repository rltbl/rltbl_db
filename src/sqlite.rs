//! SQLite support for sql_json.

use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

use deadpool_libsql::{
    Manager, Pool,
    libsql::{Builder, Value},
};

/// Represents a SQLite database connection pool
#[derive(Debug)]
pub struct SqliteConnection {
    pool: Pool,
}

impl SqliteConnection {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let db = Builder::new_local(url)
            .build()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error creating pool: {err}")))?;

        let manager = Manager::from_libsql_database(db);
        let pool = Pool::builder(manager)
            .build()
            .map_err(|err| DbError::ConnectError(format!("Error creating pool: {err}")))?;

        Ok(Self { pool })
    }
}

/// Extract the first value of the first row in `rows`.
fn extract_value(rows: &Vec<JsonRow>) -> Result<JsonValue, DbError> {
    match rows.iter().next() {
        Some(row) => match row.values().next() {
            Some(value) => Ok(value.clone()),
            None => Err(DbError::DataError("No values found".to_string())),
        },
        None => Err(DbError::DataError("No rows found".to_string())),
    }
}

impl DbQuery for SqliteConnection {
    /// Implements [DbQuery::execute()] for SQLite.
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err}")))?;
        let sql = sql.to_string();
        conn.execute_batch(&sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error during query: {err}")))?;
        Ok(())
    }

    /// Implements [DbQuery::query()] for SQLite.
    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting pool: {err}")))?;
        let sql = sql.to_string();
        let stmt = conn
            .prepare(&sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err}")))?;
        let libsql_params: Vec<Value> = params
            .iter()
            .map(|param| match param {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(bool) => match bool {
                    true => Value::Integer(1),
                    false => Value::Integer(0),
                },
                serde_json::Value::Number(number) => {
                    if number.is_f64() {
                        Value::Real(number.as_f64().unwrap())
                    } else {
                        Value::Integer(number.as_i64().unwrap())
                    }
                }
                serde_json::Value::String(string) => Value::Text(string.clone()),
                serde_json::Value::Array(_) => Value::Null,
                serde_json::Value::Object(_) => Value::Null,
            })
            .collect();
        let mut rows = stmt.query(libsql_params).await.map_err(|err| {
            DbError::DatabaseError(format!("Error querying prepared statement: {err}"))
        })?;
        let columns = stmt.columns();
        let mut json_rows = Vec::new();
        loop {
            match rows
                .next()
                .await
                .map_err(|err| DbError::DatabaseError(format!("Error retrieving row: {err}")))?
            {
                Some(row) => {
                    let mut json_row = serde_json::Map::new();
                    for i in 0..row.column_count() {
                        let name = row.column_name(i).unwrap();
                        let value = match row.get_value(i).unwrap() {
                            Value::Null => serde_json::Value::Null,
                            Value::Integer(number) => {
                                match columns.get(i as usize).unwrap().decl_type() {
                                    Some(t) => match t.to_lowercase().as_str() {
                                        "bool" => match number {
                                            0 => serde_json::Value::Bool(false),
                                            _ => serde_json::Value::Bool(true),
                                        },
                                        _ => serde_json::Value::Number(number.into()),
                                    },
                                    None => serde_json::Value::Number(number.into()),
                                }
                            }

                            Value::Real(number) => serde_json::Value::from(number),
                            Value::Text(text) => serde_json::Value::String(text),
                            Value::Blob(_) => todo!(),
                        };
                        json_row.insert(name.to_string(), value);
                    }
                    json_rows.push(json_row);
                }
                None => break,
            }
        }
        Ok(json_rows)
    }

    /// Implements [DbQuery::query_row()] for SQLite.
    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let rows = self.query(&sql, params).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_row()");
        }
        match rows.iter().next() {
            Some(row) => Ok(row.clone()),
            None => Err(DbError::DataError("No row found".to_string())),
        }
    }

    /// Implements [DbQuery::query_value()] for SQLite.
    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let rows = self.query(sql, params).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_value()");
        }
        extract_value(&rows)
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
            None => Err(DbError::DataError(format!(
                "Not an unsigned integer: {value}"
            ))),
        }
    }

    /// Implements [DbQuery::query_i64()] for SQLite.
    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not an integer: {value}"))),
        }
    }

    /// Implements [DbQuery::query_f64()] for SQLite.
    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not an float: {value}"))),
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
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_text;\
             CREATE TABLE test_table_text ( value TEXT )",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO test_table_text VALUES ($1)", &[json!("foo")])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text WHERE value = $1";
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
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_int;\
             CREATE TABLE test_table_int ( value INT )",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO test_table_int VALUES ($1)", &[json!(1)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_int WHERE value = $1";
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
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_float;\
             CREATE TABLE test_table_float ( value REAL )",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO test_table_float VALUES ($1)", &[json!(1.05)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
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
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_mixed;\
             CREATE TABLE test_table_mixed (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_mixed
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                json!(1),
                json!(true),
            ],
        )
        .await
        .unwrap();

        let select_sql = r#"SELECT text_value, alt_text_value, float_value, int_value, bool_value
                            FROM test_table_mixed
                            WHERE text_value = $1
                              AND alt_text_value IS $2
                              AND float_value > $3
                              AND int_value > $4
                              AND bool_value = $5"#;
        let params = [
            json!("foo"),
            JsonValue::Null,
            json!(1.0),
            json!(0),
            json!(true),
        ];
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
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "int_value": 1,
                "bool_value": true,
            })
        );

        let rows = conn.query(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "foo",
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "int_value": 1,
                "bool_value": true,
            }])
        );
    }

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let conn = SqliteConnection::connect("test_indirect_columns.db")
            .await
            .unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                json!(1),
                json!(true),
            ],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = conn
            .query("SELECT MAX(int_value) FROM test_table_indirect", &[])
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"MAX(int_value)": 1}]));

        // Test alias:
        let rows = conn
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"bool_value_alias": true}]));

        // Test aggregate with alias:
        let rows = conn
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(json!(rows), json!([{"max_int_value": 1}]));

        // Test non-aggregate function:
        let rows = conn
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"CAST(int_value AS TEXT)": "1"}]));

        // Test non-aggregate function with alias:
        let rows = conn
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value_cast": "1"}]));

        // Test functions over booleans:
        let rows = conn
            .query("SELECT MAX(bool_value) FROM test_table_indirect", &[])
            .await
            .unwrap();
        // It is not possible to represent the boolean result of an aggregate function as a
        // boolean, since internally to sqlite it is stored as an integer, and we can't query
        // the metadata to get the datatype of an expression. If we want to represent it as a
        // boolean, we will need to parse the expression. Note that PostgreSQL does not support
        // MAX(bool_value) - it gives the error:
        //   ERROR: function max(boolean) does not exist\nHINT: No function matches the given
        //          name and argument types. You might need to add explicit type casts.
        // So, perhaps, this is tu quoque an argument that the behaviour below is acceptable for
        // sqlite.
        assert_eq!(json!(rows), json!([{"MAX(bool_value)": 1}]));
    }

    #[tokio::test]
    async fn test_transactions() {
        let pool = SqliteConnection::connect("test_transactions.db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                json!(1),
                json!(true),
            ],
        )
        .await
        .unwrap();

        let conn = pool.pool.get().await.unwrap();
        let tx = conn.transaction().await.unwrap();
        use_tx(&tx).await;

        let mut rows = conn
            .query(r"SELECT regex_match('\d', 'foo')", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let value = row.get_str(0).unwrap();
        assert_eq!(value, "FOO");
    }

    async fn use_tx(tx: &deadpool_libsql::libsql::Transaction) -> u64 {
        tx.query("SELECT 1", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap()
    }
}
