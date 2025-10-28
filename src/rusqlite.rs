//! SQLite support for sql_json.

use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

use deadpool_sqlite::{
    Config, Pool, Runtime,
    rusqlite::{
        Statement,
        fallible_iterator::FallibleIterator,
        types::{Null, ValueRef},
    },
};
use serde_json::json;

/// Represents a SQLite database connection pool
#[derive(Debug)]
pub struct RusqlitePool {
    pool: Pool,
}

impl RusqlitePool {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let cfg = Config::new(url);
        let pool = cfg
            .create_pool(Runtime::Tokio1)
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

/// Query the database using the given prepared statement and json parameters.
fn query_prepared(
    stmt: &mut Statement<'_>,
    params: &Vec<JsonValue>,
) -> Result<Vec<JsonRow>, DbError> {
    // Bind the parameters to the prepared statement:
    for (i, param) in params.iter().enumerate() {
        match param {
            JsonValue::String(s) => {
                stmt.raw_bind_parameter(i + 1, s).map_err(|err| {
                    DbError::InputError(format!("Error binding parameter '{param}': {err}"))
                })?;
            }
            JsonValue::Number(_) => {
                stmt.raw_bind_parameter(i + 1, &param.to_string())
                    .map_err(|err| {
                        DbError::InputError(format!("Error binding parameter '{param}': {err}"))
                    })?;
            }
            JsonValue::Bool(flag) => match flag {
                // Note that SQLite's type affinity means that booleans are actually implemented
                // as numbers (see https://sqlite.org/datatype3.html). They will be converted
                // back to boolean when the results are read below.
                false => {
                    stmt.raw_bind_parameter(i + 1, &0.to_string())
                        .map_err(|err| {
                            DbError::InputError(format!("Error binding parameter '{param}': {err}"))
                        })?;
                }
                true => {
                    stmt.raw_bind_parameter(i + 1, &1.to_string())
                        .map_err(|err| {
                            DbError::InputError(format!("Error binding parameter '{param}': {err}"))
                        })?;
                }
            },
            JsonValue::Null => {
                stmt.raw_bind_parameter(i + 1, &Null).map_err(|err| {
                    DbError::InputError(format!("Error binding parameter '{param}': {err}"))
                })?;
            }
            _ => {
                return Err(DbError::InputError(format!(
                    "Unsupported JSON type: {param}"
                )));
            }
        };
    }

    // Define the struct that we will use to represent information about a given column:
    struct ColumnConfig {
        name: String,
        datatype: Option<String>,
    }

    // Collect the column information from the prepared statement:
    let columns = stmt
        .columns()
        .iter()
        .map(|col| {
            let name = col.name().to_string();
            let datatype = col.decl_type().and_then(|s| Some(s.to_string()));
            ColumnConfig { name, datatype }
        })
        .collect::<Vec<_>>();

    // Execute the statement and send back the results
    let results = stmt
        .raw_query()
        .map(|row| {
            let mut json_row = JsonRow::new();
            for column in &columns {
                let column_name = &column.name;
                let column_type = &column.datatype;
                let value = match row.get_ref(column_name.as_str()) {
                    Ok(value) => match value {
                        ValueRef::Null => JsonValue::Null,
                        ValueRef::Integer(value) => match column_type {
                            Some(ctype) if ctype.to_lowercase() == "bool" => {
                                JsonValue::Bool(value != 0)
                            }
                            // The remaining cases are (a) the column's datatype is integer, and
                            // (b) the column is an expression. In the latter case it doesn't seem
                            // possible to get the datatype of the expression from the metadata.
                            // So the only thing to do here is just to convert the value
                            // to JSON using the default method, and since we already know that it
                            // is an integer, the result of the conversion will be a JSON number.
                            _ => JsonValue::from(value),
                        },
                        ValueRef::Real(value) => JsonValue::from(value),
                        ValueRef::Text(value) | ValueRef::Blob(value) => match column_type {
                            Some(ctype) if ctype.to_lowercase() == "numeric" => {
                                json!(value)
                            }
                            _ => {
                                let value = std::str::from_utf8(value).unwrap_or_default();
                                JsonValue::String(value.to_string())
                            }
                        },
                    },
                    Err(err) => {
                        tracing::warn!(
                            "Error getting value for column '{column_name}' from row: {err}"
                        );
                        JsonValue::Null
                    }
                };
                json_row.insert(column_name.to_string(), value);
            }
            Ok(json_row)
        })
        .collect::<Vec<_>>();
    results.map_err(|err| DbError::DatabaseError(err.to_string()))
}

impl DbQuery for RusqlitePool {
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
        match conn
            .interact(move |conn| match conn.execute_batch(&sql) {
                Err(err) => {
                    return Err(DbError::DatabaseError(format!("Error during query: {err}")));
                }
                Ok(_) => Ok(()),
            })
            .await
        {
            Err(err) => Err(DbError::DatabaseError(format!("Error during query: {err}"))),
            Ok(_) => Ok(()),
        }
    }

    /// Implements [DbQuery::query()] for SQLite.
    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting pool: {err}")))?;
        let sql = sql.to_string();
        let params = params.to_vec();
        let rows = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(|err| {
                    DbError::DatabaseError(format!("Error preparing statement: {err}"))
                })?;
                let rows = query_prepared(&mut stmt, &params).map_err(|err| {
                    DbError::DatabaseError(format!("Error querying prepared statement: {err}"))
                })?;
                Ok::<Vec<JsonRow>, DbError>(rows)
            })
            .await
            .map_err(|err| DbError::DatabaseError(err.to_string()))?
            .map_err(|err| DbError::DatabaseError(err.to_string()))?;
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
        let conn = RusqlitePool::connect(":memory:").await.unwrap();
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
        let conn = RusqlitePool::connect(":memory:").await.unwrap();
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
        let conn = RusqlitePool::connect(":memory:").await.unwrap();
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
        let conn = RusqlitePool::connect(":memory:").await.unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_mixed;\
             CREATE TABLE test_table_mixed (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL,
                 numeric_value NUMERIC\
             )",
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_mixed
               (text_value, alt_text_value, float_value, int_value, bool_value, numeric_value)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                json!(1),
                json!(true),
                json!(1_000_000),
            ],
        )
        .await
        .unwrap();

        let select_sql = r#"SELECT text_value, alt_text_value, float_value, int_value, bool_value, numeric_value
                            FROM test_table_mixed
                            WHERE text_value = $1
                              AND alt_text_value IS $2
                              AND float_value > $3
                              AND int_value > $4
                              AND bool_value = $5
                              AND numeric_value > $6"#;
        let params = [
            json!("foo"),
            JsonValue::Null,
            json!(1.0),
            json!(0),
            json!(true),
            json!(999_999),
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
                "numeric_value": 1_000_000,
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
                "numeric_value": 1_000_000,
            }])
        );
    }

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let conn = RusqlitePool::connect(":memory:").await.unwrap();
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
}
