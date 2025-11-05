//! rusqlite implementation for rltbl_db.

use crate::{
    core::{DbError, DbKind, DbQuery, IntoParams, JsonRow, JsonValue, ParamValue, Params},
    params,
};

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

/// Query a database using the given prepared statement and parameters.
fn query_prepared(
    stmt: &mut Statement<'_>,
    params: impl IntoParams + Send,
) -> Result<Vec<JsonRow>, DbError> {
    match params.into_params() {
        Params::None => (),
        Params::Positional(params) => {
            for (i, param) in params.iter().enumerate() {
                match param {
                    ParamValue::Text(text) => {
                        stmt.raw_bind_parameter(i + 1, text).map_err(|err| {
                            DbError::InputError(format!(
                                "Error binding parameter '{param:?}': {err}"
                            ))
                        })?;
                    }
                    ParamValue::SmallInteger(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Integer(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::BigInteger(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Real(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::BigReal(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Numeric(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Boolean(flag) => {
                        // Note that SQLite's type affinity means that booleans are actually
                        // implemented as numbers (see https://sqlite.org/datatype3.html).
                        let num = match flag {
                            true => 1,
                            false => 0,
                        };
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Null => {
                        stmt.raw_bind_parameter(i + 1, &Null).map_err(|err| {
                            DbError::InputError(format!(
                                "Error binding parameter '{param:?}': {err}"
                            ))
                        })?;
                    }
                };
            }
        }
    };

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
                let value = row.get_ref(column_name.as_str())?;
                let value = match value {
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
                };
                json_row.insert(column_name.to_string(), value);
            }
            Ok(json_row)
        })
        .collect::<Vec<_>>();
    results.map_err(|err| DbError::DatabaseError(err.to_string()))
}

impl DbQuery for RusqlitePool {
    /// Implements [DbQuery::kind()] for SQLite.
    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    /// Implements [DbQuery::execute()] for SQLite.
    async fn execute(&self, sql: &str, params: impl IntoParams + Send) -> Result<(), DbError> {
        let params = params.into_params();
        match params {
            Params::None => self.query(sql, ()).await?,
            _ => self.query(sql, params).await?,
        };
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
    async fn query(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting pool: {err}")))?;
        let sql = sql.to_string();
        let params: Params = params.into_params();
        let rows = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(|err| {
                    DbError::DatabaseError(format!("Error preparing statement: {err}"))
                })?;
                let rows = query_prepared(&mut stmt, params).map_err(|err| {
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
    async fn query_row(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonRow, DbError> {
        let rows = self.query(&sql, params).await?;
        if rows.len() > 1 {
            return Err(DbError::DataError(
                "More than one row returned for query_row()".to_string(),
            ));
        }
        match rows.iter().next() {
            Some(row) => Ok(row.clone()),
            None => Err(DbError::DataError("No row found".to_string())),
        }
    }

    /// Implements [DbQuery::query_value()] for SQLite.
    async fn query_value(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonValue, DbError> {
        let row = self.query_row(sql, params).await?;
        if row.len() > 1 {
            return Err(DbError::DataError(
                "More than one value returned for query_value()".to_string(),
            ));
        }
        match row.values().next() {
            Some(value) => Ok(value.clone()),
            None => Err(DbError::DataError("No values found".to_string())),
        }
    }

    /// Implements [DbQuery::query_string()] for SQLite.
    async fn query_string(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<String, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_str() {
            Some(str_val) => Ok(str_val.to_string()),
            None => Ok(value.to_string()),
        }
    }

    /// Implements [DbQuery::query_u64()] for SQLite.
    async fn query_u64(&self, sql: &str, params: impl IntoParams + Send) -> Result<u64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!(
                "Not an unsigned integer: {value}"
            ))),
        }
    }

    /// Implements [DbQuery::query_i64()] for SQLite.
    async fn query_i64(&self, sql: &str, params: impl IntoParams + Send) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not an integer: {value}"))),
        }
    }

    /// Implements [DbQuery::query_f64()] for SQLite.
    async fn query_f64(&self, sql: &str, params: impl IntoParams + Send) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not an float: {value}"))),
        }
    }

    async fn insert(&self, table: &str, rows: &[&JsonRow]) -> Result<Vec<JsonRow>, DbError> {
        let columns = self
            .query("SELECT name FROM PRAGMA_TABLE_INFO($1)", params![table])
            .await?;
        let columns: Vec<String> = columns
            .iter()
            .map(|row| row.get("name").unwrap().as_str().unwrap().to_string())
            .collect();
        let mut lines: Vec<String> = Vec::new();
        let mut params: Vec<ParamValue> = Vec::new();
        let mut i = 0;
        for row in rows {
            let mut cells: Vec<String> = Vec::new();
            for column in &columns {
                if row.contains_key(column) {
                    i += 1;
                    cells.push(format!("${i}"));
                    let param = match row.get(column).unwrap() {
                        serde_json::Value::Null => ParamValue::Null,
                        serde_json::Value::Bool(bool) => ParamValue::Boolean(*bool),
                        serde_json::Value::Number(number) => {
                            if number.is_i64() {
                                ParamValue::BigInteger(number.as_i64().unwrap())
                            } else if number.is_f64() {
                                ParamValue::BigReal(number.as_f64().unwrap())
                            } else {
                                unimplemented!()
                            }
                        }
                        serde_json::Value::String(text) => ParamValue::Text(text.to_string()),
                        serde_json::Value::Array(values) => {
                            return Err(DbError::InputError(format!(
                                "JSON Arrays not supported: {values:?}"
                            )));
                        }
                        serde_json::Value::Object(map) => {
                            return Err(DbError::InputError(format!(
                                "JSON Objects not supported: {map:?}"
                            )));
                        }
                    };
                    params.push(param);
                } else {
                    cells.push(String::from("NULL"))
                }
            }
            let line = format!("({})", cells.join(", "));
            lines.push(line);
        }
        // WARN: This allows SQL injection.
        let sql = format!(
            r#"INSERT INTO "{table}" VALUES
            {}
            RETURNING *;"#,
            lines.join(",\n")
        );
        self.query(&sql, params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::params;
    use rust_decimal::dec;
    use serde_json::json;

    #[tokio::test]
    async fn test_text_column_query() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_text;\
             CREATE TABLE test_table_text ( value TEXT )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_text VALUES ($1)", &["foo"])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text WHERE value = $1";
        let value = pool
            .query_value(select_sql, &["foo"])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let value = pool.query_string(select_sql, &["foo"]).await.unwrap();
        assert_eq!("foo", value);

        let row = pool.query_row(select_sql, &["foo"]).await.unwrap();
        assert_eq!(json!(row), json!({"value":"foo"}));

        let rows = pool.query(select_sql, &["foo"]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":"foo"}]));
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_int;\
             CREATE TABLE test_table_int ( value INT )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_int VALUES ($1)", &[1])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_int WHERE value = $1";
        let value = pool
            .query_value(select_sql, &[1])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(1, value);

        let value = pool.query_u64(select_sql, &[1]).await.unwrap();
        assert_eq!(1, value);

        let value = pool.query_i64(select_sql, &[1]).await.unwrap();
        assert_eq!(1, value);

        let value = pool.query_string(select_sql, &[1]).await.unwrap();
        assert_eq!("1", value);

        let row = pool.query_row(select_sql, &[1]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1}));

        let rows = pool.query(select_sql, &[1]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1}]));
    }

    #[tokio::test]
    async fn test_float_column_query() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_float;\
             CREATE TABLE test_table_float ( value REAL )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_float VALUES ($1)", &[1.05])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = pool
            .query_value(select_sql, &[1])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = pool.query_f64(select_sql, &[1]).await.unwrap();
        assert_eq!(1.05, value);

        let value = pool.query_string(select_sql, &[1]).await.unwrap();
        assert_eq!("1.05", value);

        let row = pool.query_row(select_sql, &[1]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = pool.query(select_sql, &[1]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
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
        pool.execute(
            r#"INSERT INTO test_table_mixed
               (text_value, alt_text_value, float_value, int_value, bool_value, numeric_value)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
            params!["foo", (), 1.05_f64, 1_i64, true, 1_000_000,],
        )
        .await
        .unwrap();

        let select_sql = "SELECT text_value FROM test_table_mixed WHERE text_value = $1";
        let value = pool
            .query_value(select_sql, ["foo"])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let select_sql = r#"SELECT
                              text_value,
                              alt_text_value,
                              float_value,
                              int_value,
                              bool_value,
                              numeric_value
                            FROM test_table_mixed
                            WHERE text_value = $1
                              AND alt_text_value IS $2
                              AND float_value > $3
                              AND int_value > $4
                              AND bool_value = $5
                              AND numeric_value > $6"#;

        let row = pool
            .query_row(
                select_sql,
                params!["foo", (), 1.0_f64, 0_i64, true, 999_999],
            )
            .await
            .unwrap();
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

        let rows = pool
            .query(
                select_sql,
                params!["foo", (), 1.0_f64, 0_i64, true, 999_999],
            )
            .await
            .unwrap();
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
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
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
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"MAX(int_value)": 1}]));

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"bool_value_alias": true}]));

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(json!(rows), json!([{"max_int_value": 1}]));

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"CAST(int_value AS TEXT)": "1"}]));

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value_cast": "1"}]));

        // Test functions over booleans:
        let rows = pool
            .query("SELECT MAX(bool_value) FROM test_table_indirect", ())
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
    async fn test_insert() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_insert;\
             CREATE TABLE test_insert (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        let rows = pool
            .insert(
                "test_insert",
                &[
                    &json!({"text_value": "TEXT"}).as_object().unwrap(),
                    &json!({"int_value": 1, "bool_value": true})
                        .as_object()
                        .unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "TEXT",
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": JsonValue::Null,
                "bool_value": JsonValue::Null,
            },{
                "text_value": JsonValue::Null,
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": 1,
                "bool_value": true,
            }])
        );
    }

    #[tokio::test]
    async fn test_input_params() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute("DROP TABLE IF EXISTS foo", ()).await.unwrap();
        pool.execute(
            "CREATE TABLE foo (\
               bar TEXT,\
               car INT2,\
               dar INT4,\
               far INT8,\
               gar FLOAT4,\
               har FLOAT8,\
               jar NUMERIC,\
               kar BOOL
             )",
            (),
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO foo (bar) VALUES ($1)", &["one"])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (far) VALUES ($1)", &[1 as i64])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (bar) VALUES ($1)", ["two"])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (far) VALUES ($1)", [2 as i64])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (bar) VALUES ($1)", vec!["three"])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (far) VALUES ($1)", vec![3 as i64])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (gar) VALUES ($1)", vec![3 as f32])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (har) VALUES ($1)", vec![3 as f64])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (jar) VALUES ($1)", vec![dec!(3)])
            .await
            .unwrap();
        pool.execute("INSERT INTO foo (kar) VALUES ($1)", vec![true])
            .await
            .unwrap();
        pool.execute(
            "INSERT INTO foo \
             (bar, car, dar, far, gar, har, jar, kar) \
             VALUES ($1, $2, $3, $4, $5 ,$6, $7, $8)",
            params![
                "four",
                123_i16,
                123_i32,
                123_i64,
                123_f32,
                123_f64,
                dec!(123),
                true,
            ],
        )
        .await
        .unwrap();
        // Two alternative ways of specifying a NULL parameter:
        let row = pool
            .query_row(
                "SELECT COUNT(1) AS count FROM foo \
                 WHERE bar IS $1 AND far IS $2",
                params![ParamValue::Null, ()],
            )
            .await
            .unwrap();
        assert_eq!(json!({"count": 4}), json!(row));
    }
}
