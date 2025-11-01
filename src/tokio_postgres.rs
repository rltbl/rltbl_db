//! tokio-postgres implementation for rltbl_db.

use crate::core::{DbError, DbQuery, IntoParams, JsonRow, JsonValue, ParamValue, Params};

use deadpool_postgres::{Config, Pool, Runtime};
use rust_decimal::Decimal;
use tokio_postgres::{
    NoTls,
    row::Row,
    types::{ToSql, Type},
};

/// Represents a PostgreSQL database connection pool
#[derive(Debug)]
pub struct TokioPostgresPool {
    pool: Pool,
}

impl TokioPostgresPool {
    /// Connect to a PostgreSQL database using the given url, which should be of the form
    /// postgresql:///DATABASE_NAME
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        match url.starts_with("postgresql:///") {
            true => {
                let mut cfg = Config::new();
                let db_name = url
                    .strip_prefix("postgresql:///")
                    .ok_or(DbError::ConnectError("Invalid PostgreSQL URL".to_string()))?;
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg
                    .create_pool(Some(Runtime::Tokio1), NoTls)
                    .map_err(|err| DbError::ConnectError(format!("Error creating pool: {err}")))?;
                Ok(Self { pool })
            }
            false => Err(DbError::ConnectError(format!(
                "Invalid PostgreSQL URL: '{url}'"
            ))),
        }
    }
}

/// Extracts the value at the given index from the given [Row].
fn extract_value(row: &Row, idx: usize) -> Result<JsonValue, DbError> {
    let column = &row.columns()[idx];
    match *column.type_() {
        Type::TEXT | Type::VARCHAR => match row
            .try_get::<usize, Option<&str>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::INT2 => match row
            .try_get::<usize, Option<i16>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::INT4 => match row
            .try_get::<usize, Option<i32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::INT8 => match row
            .try_get::<usize, Option<i64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::BOOL => match row
            .try_get::<usize, Option<bool>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::FLOAT4 => match row
            .try_get::<usize, Option<f32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        Type::FLOAT8 => match row
            .try_get::<usize, Option<f64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(JsonValue::Null),
        },
        // WARN: This downcasts a Postgres NUMERIC to a 64 bit JSON Number.
        Type::NUMERIC => match row
            .try_get::<usize, Option<Decimal>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => {
                let v = value.to_string();
                if let Ok(number) = v.parse::<u64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<i64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<f64>() {
                    Ok(number.into())
                } else {
                    panic!("Not a u64, i64, or f64: {value}");
                }
            }
            None => Ok(JsonValue::Null),
        },
        _ => unimplemented!(),
    }
}

impl DbQuery for TokioPostgresPool {
    /// Implements [DbQuery::execute()] for PostgreSQL.
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Implements [DbQuery::execute()] for PostgreSQL.
    async fn execute_new(
        &self,
        sql: &str,
        params: impl IntoParams + Send + Clone + 'static,
    ) -> Result<(), DbError> {
        let params2 = params.clone();
        match params.into_params()? {
            Params::None => self.query_new(sql, ()).await?,
            _ => self.query_new(sql, params2).await?,
        };
        Ok(())
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err}")))?;
        client
            .batch_execute(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err}")))?;
        Ok(())
    }

    async fn query_new(
        &self,
        sql: &str,
        into_params: impl IntoParams + Send,
    ) -> Result<Vec<JsonRow>, DbError> {
        let into_params = into_params.into_params()?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err}")))?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err}")))?
            .params()
            .to_vec();

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

        match into_params {
            Params::None => (),
            Params::Positional(plist) => {
                for (i, param) in plist.iter().enumerate() {
                    let pg_type = &param_pg_types[i];
                    match pg_type {
                        &Type::TEXT | &Type::VARCHAR => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<String>)),
                                ParamValue::Text(text) => params.push(Box::new(text.to_string())),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::INT2 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i16>)),
                                ParamValue::SmallInteger(num) => params.push(Box::new(*num)),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::INT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i32>)),
                                ParamValue::Integer(num) => params.push(Box::new(*num)),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::INT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i64>)),
                                ParamValue::BigInteger(num) => params.push(Box::new(*num)),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::FLOAT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f32>)),
                                ParamValue::Real(num) => params.push(Box::new(*num)),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::FLOAT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f64>)),
                                ParamValue::Real(num) => params.push(Box::new(*num)),
                                _ => panic!("TODO: Return a proper error here"),
                            };
                        }
                        &Type::BOOL => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<bool>)),
                                _ => todo!(),
                            };
                        }
                        &Type::NUMERIC => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<Decimal>)),
                                _ => todo!(),
                            };
                        }
                        _ => unimplemented!(),
                    };
                }
            }
            Params::Named(_) => todo!(),
        };

        // Finally, execute the query and return the results:
        let query_params: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(sql, &query_params)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err}")))?;
        let mut json_rows = vec![];
        for row in &rows {
            let mut json_row = JsonRow::new();
            let columns = row.columns();
            for (i, column) in columns.iter().enumerate() {
                json_row.insert(column.name().to_string(), extract_value(row, i)?);
            }
            json_rows.push(json_row);
        }
        Ok(json_rows)
    }

    /// Implements [DbQuery::query()] for PostgreSQL.
    async fn query(&self, sql: &str, json_params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err}")))?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err}")))?
            .params()
            .to_vec();

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

        for (i, param) in json_params.iter().enumerate() {
            let pg_type = &param_pg_types[i];
            match pg_type {
                &Type::TEXT | &Type::VARCHAR => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<String>)),
                        _ => {
                            params.push(Box::new(
                                param
                                    .as_str()
                                    .ok_or(DbError::InputError("Not a string".to_string()))?,
                            ));
                        }
                    };
                }
                &Type::INT2 => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<i16>)),
                        _ => {
                            let value: i16 = param
                                .as_i64()
                                .ok_or(DbError::InputError("Not an integer".to_string()))?
                                .try_into()
                                .map_err(|e| DbError::InputError(format!("Not an i16: {e}")))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::INT4 => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<i32>)),
                        _ => {
                            let value: i32 = param
                                .as_i64()
                                .ok_or(DbError::InputError("Not an integer".to_string()))?
                                .try_into()
                                .map_err(|e| DbError::InputError(format!("Not an i32: {e}")))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::INT8 => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<i64>)),
                        _ => {
                            let value: i64 = param
                                .as_i64()
                                .ok_or(DbError::InputError("Not an integer".to_string()))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::FLOAT4 => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<f32>)),
                        _ => {
                            let value: f32 = param
                                .as_f64()
                                .ok_or(DbError::InputError("Not a float".to_string()))?
                                as f32;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::FLOAT8 => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<f64>)),
                        _ => {
                            let value: f64 = param
                                .as_f64()
                                .ok_or(DbError::InputError("Not a float".to_string()))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::BOOL => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<bool>)),
                        _ => {
                            let value: bool = param
                                .as_bool()
                                .ok_or(DbError::InputError("Not a boolean".to_string()))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                &Type::NUMERIC => {
                    match param {
                        JsonValue::Null => params.push(Box::new(None::<Decimal>)),
                        _ => {
                            let value: Decimal = serde_json::from_value(param.clone())
                                .map_err(|err| DbError::DataError(err.to_string()))?;
                            params.push(Box::new(value));
                        }
                    };
                }
                _ => unimplemented!(),
            }
        }

        // Finally, execute the query and return the results:
        let query_params: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(sql, &query_params)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err}")))?;
        let mut json_rows = vec![];
        for row in &rows {
            let mut json_row = JsonRow::new();
            let columns = row.columns();
            for (i, column) in columns.iter().enumerate() {
                json_row.insert(column.name().to_string(), extract_value(row, i)?);
            }
            json_rows.push(json_row);
        }
        Ok(json_rows)
    }

    /// Implements [DbQuery::query_row()] for PostgreSQL.
    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let rows = self.query(sql, params).await?;
        if rows.len() > 1 {
            return Err(DbError::DataError(
                "More than one row returned for query_row()".to_string(),
            ));
        }
        match rows.into_iter().nth(0) {
            Some(row) => Ok(row),
            None => Err(DbError::DataError("No rows found".to_string())),
        }
    }

    /// Implements [DbQuery::query_value()] for PostgreSQL.
    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let row = self.query_row(sql, params).await?;
        if row.values().len() > 1 {
            return Err(DbError::DataError(
                "More than one value returned for query_value()".to_string(),
            ));
        }
        match row.into_iter().map(|(_, v)| v).next() {
            Some(value) => Ok(value),
            None => Err(DbError::DataError("No values found".into())),
        }
    }

    /// Implements [DbQuery::query_string()] for PostgreSQL.
    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_str() {
            Some(str_val) => Ok(str_val.to_string()),
            None => Ok(value.to_string()),
        }
    }

    /// Implements [DbQuery::query_u64()] for PostgreSQL.
    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a u64: {value}"))),
        }
    }

    /// Implements [DbQuery::query_i64()] for PostgreSQL.
    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a i64: {value}"))),
        }
    }

    /// Implements [DbQuery::query_f64] for PostgreSQL.
    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a f64: {value}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;

    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[tokio::test]
    async fn test_text_column_query() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_text;\
             CREATE TABLE test_table_text ( value TEXT )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_text VALUES ($1)", &[json!("foo")])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text WHERE value = $1";
        let value = pool
            .query_value(select_sql, &[json!("foo")])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let value = pool
            .query_string(select_sql, &[json!("foo")])
            .await
            .unwrap();
        assert_eq!("foo", value);

        let row = pool.query_row(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(row), json!({"value":"foo"}));

        let rows = pool.query(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":"foo"}]));
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        let sql_types = vec!["INT2", "INT4", "INT8"];

        for sql_type in sql_types {
            pool.execute_batch(&format!(
                "DROP TABLE IF EXISTS test_table_int;\
             CREATE TABLE test_table_int ( value {sql_type} )",
            ))
            .await
            .unwrap();
            pool.execute("INSERT INTO test_table_int VALUES ($1)", &[json!(1)])
                .await
                .unwrap();
            let select_sql = "SELECT value FROM test_table_int WHERE value = $1";
            let value = pool
                .query_value(select_sql, &[json!(1)])
                .await
                .unwrap()
                .as_i64()
                .unwrap();
            assert_eq!(1, value);

            let value = pool.query_u64(select_sql, &[json!(1)]).await.unwrap();
            assert_eq!(1, value);

            let value = pool.query_i64(select_sql, &[json!(1)]).await.unwrap();
            assert_eq!(1, value);

            let value = pool.query_string(select_sql, &[json!(1)]).await.unwrap();
            assert_eq!("1", value);

            let row = pool.query_row(select_sql, &[json!(1)]).await.unwrap();
            assert_eq!(json!(row), json!({"value":1}));

            let rows = pool.query(select_sql, &[json!(1)]).await.unwrap();
            assert_eq!(json!(rows), json!([{"value":1}]));
        }
    }

    #[tokio::test]
    async fn test_float_column_query() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        // FLOAT8
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_float;\
             CREATE TABLE test_table_float ( value FLOAT8 )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_float VALUES ($1)", &[json!(1.05)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = pool
            .query_value(select_sql, &[json!(1.0)])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = pool.query_f64(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(1.05, value);

        let value = pool.query_string(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!("1.05", value);

        let row = pool.query_row(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = pool.query(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));

        // FLOAT4 is harder to test
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_float;\
             CREATE TABLE test_table_float ( value FLOAT4 )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_float VALUES ($1)", &[json!(1.05)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = pool
            .query_value(select_sql, &[json!(1.0)])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_mixed;\
             CREATE TABLE test_table_mixed (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 alt_float_value FLOAT8,\
                 int_value INT8,\
                 alt_int_value INT8,\
                 bool_value BOOL,\
                 alt_bool_value BOOL,\
                 numeric_value NUMERIC,\
                 alt_numeric_value NUMERIC\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_mixed
               (
                 text_value,
                 alt_text_value,
                 float_value,
                 alt_float_value,
                 int_value,
                 alt_int_value,
                 bool_value,
                 alt_bool_value,
                 numeric_value,
                 alt_numeric_value
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                JsonValue::Null,
                json!(1),
                JsonValue::Null,
                json!(true),
                JsonValue::Null,
                json!(1_000_000),
                JsonValue::Null,
            ],
        )
        .await
        .unwrap();

        let select_sql = "SELECT text_value FROM test_table_mixed WHERE text_value = $1";
        let params = [json!("foo")];
        let value = pool
            .query_value(select_sql, &params)
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
                              alt_float_value,
                              int_value,
                              alt_int_value,
                              bool_value,
                              alt_bool_value,
                              numeric_value,
                              alt_numeric_value
                            FROM test_table_mixed
                            WHERE text_value = $1
                              AND alt_text_value IS NOT DISTINCT FROM $2
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

        let row = pool.query_row(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(row),
            json!({
                "text_value": "foo",
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "alt_float_value": JsonValue::Null,
                "int_value": 1,
                "alt_int_value": JsonValue::Null,
                "bool_value": true,
                "alt_bool_value": JsonValue::Null,
                "numeric_value": 1_000_000,
                "alt_numeric_value": JsonValue::Null,
            })
        );

        let rows = pool.query(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "foo",
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "alt_float_value": JsonValue::Null,
                "int_value": 1,
                "alt_int_value": JsonValue::Null,
                "bool_value": true,
                "alt_bool_value": JsonValue::Null,
                "numeric_value": 1_000_000,
                "alt_numeric_value": JsonValue::Null,
            }])
        );
    }

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
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

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", &[])
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"max": 1}]));

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"bool_value_alias": true}]));

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"max_int_value": 1}]));

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value": "1"}]));

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value_cast": "1"}]));
    }

    #[tokio::test]
    async fn test_new_functions() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_new("DROP TABLE IF EXISTS foo_pg", ())
            .await
            .unwrap();
        pool.execute_new(
            "CREATE TABLE foo_pg (\
               bar TEXT,\
               car INT2,\
               dar INT4,\
               far INT8\
             )",
            (),
        )
        .await
        .unwrap();
        pool.execute_new("INSERT INTO foo_pg (bar) VALUES ($1)", &["one"])
            .await
            .unwrap();
        pool.execute_new("INSERT INTO foo_pg (far) VALUES ($1)", &[1 as i64])
            .await
            .unwrap();
        pool.execute_new("INSERT INTO foo_pg (bar) VALUES ($1)", ["two"])
            .await
            .unwrap();
        pool.execute_new("INSERT INTO foo_pg (far) VALUES ($1)", [2 as i64])
            .await
            .unwrap();
        pool.execute_new("INSERT INTO foo_pg (bar) VALUES ($1)", vec!["three"])
            .await
            .unwrap();
        pool.execute_new("INSERT INTO foo_pg (far) VALUES ($1)", vec![3 as i64])
            .await
            .unwrap();
        pool.execute_new(
            "INSERT INTO foo_pg (bar, car, dar, far) VALUES ($1, $2, $3, $4)",
            params!["four", 123_i16, 123_i32, 123_i64],
        )
        .await
        .unwrap();
    }
}
