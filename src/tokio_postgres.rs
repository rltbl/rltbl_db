//! tokio-postgres implementation for rltbl_db.

use crate::{
    core::{
        DbError, DbKind, DbQuery, IntoParams, JsonRow, JsonValue, ParamValue, Params, parameterize,
        validate_table_name,
    },
    params,
};

use deadpool_postgres::{Config, Pool, Runtime};
use rust_decimal::Decimal;
use tokio_postgres::{
    NoTls,
    row::Row,
    types::{ToSql, Type},
};

// TODO: The maximum number of parameters is artificially set to a very low level during initial
// dev. Make sure to uncomment the correct value before merging this code.
/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query
static MAX_PARAMS_POSTGRES: usize = 6;
//pub static MAX_PARAMS_POSTGRES: usize = 65535;

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

    /// Query the database's metadata to retrieve the columns associated with a given table.
    async fn get_columns(&self, table: &str) -> Result<Vec<String>, DbError> {
        let mut columns = vec![];
        let sql = format!(
            r#"SELECT
                 "columns"."column_name"::TEXT AS "name"
               FROM
                 "information_schema"."columns" "columns"
               WHERE
                 "columns"."table_schema" IN (
                   SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                   FROM "pg_settings"
                   WHERE "name" = 'search_path'
                 )
                 AND "columns"."table_name" = $1
               ORDER BY "columns"."ordinal_position""#
        );

        for row in self.query(&sql, params![&table]).await? {
            match row
                .get("name")
                .and_then(|name| name.as_str().and_then(|name| Some(name)))
            {
                Some(column) => columns.push(column.to_string()),
                None => {
                    return Err(DbError::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }

        Ok(columns)
    }
}

// TODO: Move the location of this function within this file to before the definition of
// TokioPostgresPool:
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
                    Err(DbError::DataError(format!(
                        "Not a u64, i64, or f64: {value}"
                    )))
                }
            }
            None => Ok(JsonValue::Null),
        },
        _ => unimplemented!(),
    }
}

impl DbQuery for TokioPostgresPool {
    /// Implements [DbQuery::kind()] for PostgreSQL.
    fn kind(&self) -> DbKind {
        DbKind::PostgreSQL
    }

    /// Implements [DbQuery::execute()] for PostgreSQL.
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

    /// Implements [DbQuery::query()] for PostgreSQL
    async fn query(
        &self,
        sql: &str,
        into_params: impl IntoParams + Send,
    ) -> Result<Vec<JsonRow>, DbError> {
        let into_params = into_params.into_params();
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
        let gen_err = |param: &ParamValue, sql_type: &str| -> String {
            format!("Param {param:?} is wrong type for {sql_type}")
        };
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
                                _ => return Err(DbError::InputError(gen_err(&param, "TEXT"))),
                            };
                        }
                        &Type::INT2 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i16>)),
                                ParamValue::SmallInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT2"))),
                            };
                        }
                        &Type::INT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i32>)),
                                ParamValue::Integer(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT4"))),
                            };
                        }
                        &Type::INT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i64>)),
                                ParamValue::BigInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT8"))),
                            };
                        }
                        &Type::FLOAT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f32>)),
                                ParamValue::Real(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT4"))),
                            };
                        }
                        &Type::FLOAT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f64>)),
                                ParamValue::BigReal(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT8"))),
                            };
                        }
                        &Type::NUMERIC => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<Decimal>)),
                                ParamValue::Numeric(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "NUMERIC"))),
                            };
                        }
                        &Type::BOOL => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<bool>)),
                                ParamValue::Boolean(flag) => params.push(Box::new(*flag)),
                                _ => return Err(DbError::InputError(gen_err(&param, "BOOL"))),
                            };
                        }
                        unimplemented => match param {
                            // This case will be triggered by an unknown type, for example, when
                            // querying the metadata table column, "columns"."columns", which
                            // reports that it is of type "name", even though is implemented as
                            // TEXT. In such cases we try to bind the parameter based on _its_
                            // type rather than on the reported column type. This will not work
                            // for null values, however, since there is no way to find out the type
                            // of the associated column.
                            ParamValue::Text(text) => params.push(Box::new(text.to_string())),
                            ParamValue::SmallInteger(num) => params.push(Box::new(*num)),
                            ParamValue::Integer(num) => params.push(Box::new(*num)),
                            ParamValue::BigInteger(num) => params.push(Box::new(*num)),
                            ParamValue::Real(num) => params.push(Box::new(*num)),
                            ParamValue::BigReal(num) => params.push(Box::new(*num)),
                            ParamValue::Numeric(num) => params.push(Box::new(*num)),
                            ParamValue::Boolean(flag) => params.push(Box::new(*flag)),
                            ParamValue::Null => {
                                return Err(DbError::InputError(format!(
                                    "Don't know how to bind a NULL param for type {unimplemented}"
                                )));
                            }
                        },
                    };
                }
            }
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

    /// Implements [DbQuery::query_row()] for PostgreSQL.
    async fn query_row(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonRow, DbError> {
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
    async fn query_value(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonValue, DbError> {
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

    /// Implements [DbQuery::query_u64()] for PostgreSQL.
    async fn query_u64(&self, sql: &str, params: impl IntoParams + Send) -> Result<u64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a u64: {value}"))),
        }
    }

    /// Implements [DbQuery::query_i64()] for PostgreSQL.
    async fn query_i64(&self, sql: &str, params: impl IntoParams + Send) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a i64: {value}"))),
        }
    }

    /// Implements [DbQuery::query_f64()] for PostgreSQL.
    async fn query_f64(&self, sql: &str, params: impl IntoParams + Send) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(DbError::DataError(format!("Not a f64: {value}"))),
        }
    }

    /// Implements [DbQuery::insert()] for PostgreSQL
    async fn insert(&self, table: &str, rows: &[&JsonRow]) -> Result<(), DbError> {
        // Begin by verifying that the given table name is valid, which has the side-effect of
        // removing any enclosing double-quotes:
        let table = validate_table_name(table)?;

        // Retrieve the names of all of the table's columns from the database's metadata:
        let columns = self.get_columns(&table).await?;
        if columns.len() > MAX_PARAMS_POSTGRES {
            return Err(DbError::InputError(format!(
                "Unable to insert to table '{}', which has more columns ({}) than the \
                 maximum number of variables ({}) allowed in a SQL statement by Postgres.",
                table,
                columns.len(),
                MAX_PARAMS_POSTGRES,
            )));
        }

        let mut lines_to_bind: Vec<String> = Vec::new();
        let mut params_to_be_bound: Vec<ParamValue> = Vec::new();
        let mut param_idx = 0;
        for row in rows {
            // If we have reached Postgres's limit on the number of bound parameters, insert what
            // we have so far and then reset all of the counters and collections:
            if param_idx + columns.len() > MAX_PARAMS_POSTGRES {
                let sql = format!(
                    r#"INSERT INTO "{table}" VALUES
                       {}"#,
                    lines_to_bind.join(",\n")
                );
                self.query(&sql, params_to_be_bound.clone()).await?;
                lines_to_bind.clear();
                params_to_be_bound.clear();
                param_idx = 0;
            }

            // Optimization to avoid repeated heap allocations while processing a single given row:
            params_to_be_bound.reserve(columns.len());
            let mut cells: Vec<String> = Vec::with_capacity(columns.len());
            for column in &columns {
                param_idx += 1;
                cells.push(format!("${param_idx}"));
                let param = parameterize(row, column)?;
                params_to_be_bound.push(param);
            }
            let line_to_bind = format!("({})", cells.join(", "));
            lines_to_bind.push(line_to_bind);
        }

        // If there is anything left to insert, insert it now:
        if lines_to_bind.len() > 0 {
            let sql = format!(
                r#"INSERT INTO "{table}" VALUES
                   {}"#,
                lines_to_bind.join(",\n")
            );
            self.query(&sql, params_to_be_bound.clone()).await?;
        }
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for PostgreSQL
    async fn insert_returning(
        &self,
        table: &str,
        rows: &[&JsonRow],
        filtered_by: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        // Begin by verifying that the given table name is valid, which has the side-effect of
        // removing any enclosing double-quotes:
        let table = validate_table_name(table)?;

        // Retrieve the names of all of the table's columns from the database's metadata:
        let columns = self.get_columns(&table).await?;
        if columns.len() > MAX_PARAMS_POSTGRES {
            return Err(DbError::InputError(format!(
                "Unable to insert to table '{}', which has more columns ({}) than the \
                 maximum number of variables ({}) allowed in a SQL statement by Postgres.",
                table,
                columns.len(),
                MAX_PARAMS_POSTGRES,
            )));
        }

        // Use the `filtered_by` argument to restrict the RETURNING clause, defaulting
        // to '*' if `filtered_by` is empty:
        let returning = match filtered_by.is_empty() {
            true => "*".to_string(),
            false => filtered_by
                .iter()
                // TODO: Should we log a warning when filtered_by contains non-existent
                // columns?
                // TODO: Try to remove the call to .to_string() here:
                .filter(|col| columns.contains(&col.to_string()))
                .map(|col| *col)
                .collect::<Vec<_>>()
                .join(", "),
        };

        let mut rows_to_return = vec![];
        let mut lines_to_bind: Vec<String> = Vec::new();
        let mut params_to_be_bound: Vec<ParamValue> = Vec::new();
        let mut param_idx = 0;
        for row in rows {
            // If we have reached Postgres's limit on the number of bound parameters, insert what
            // we have so far and then reset all of the counters and collections:
            if param_idx + columns.len() > MAX_PARAMS_POSTGRES {
                let sql = format!(
                    r#"INSERT INTO "{table}" VALUES
                       {}
                       RETURNING {returning}"#,
                    lines_to_bind.join(",\n")
                );
                rows_to_return.append(&mut self.query(&sql, params_to_be_bound.clone()).await?);
                lines_to_bind.clear();
                params_to_be_bound.clear();
                param_idx = 0;
            }

            // Optimization to avoid repeated heap allocations while processing a single given row:
            params_to_be_bound.reserve(columns.len());
            let mut cells: Vec<String> = Vec::with_capacity(columns.len());
            for column in &columns {
                param_idx += 1;
                cells.push(format!("${param_idx}"));
                let param = parameterize(row, column)?;
                params_to_be_bound.push(param);
            }
            let line_to_bind = format!("({})", cells.join(", "));
            lines_to_bind.push(line_to_bind);
        }

        // If there is anything left to insert, insert it now:
        if lines_to_bind.len() > 0 {
            let sql = format!(
                r#"INSERT INTO "{table}" VALUES
                   {}
                   RETURNING {returning}"#,
                lines_to_bind.join(",\n")
            );
            rows_to_return.append(&mut self.query(&sql, params_to_be_bound).await?);
        }
        Ok(rows_to_return)
    }

    /// Implements [DbQuery::drop_table()] for PostgreSQL. Note (see
    /// <https://www.postgresql.org/docs/current/sql-droptable.html>), that in the case of a
    /// dependent foreign key constraint, only the constraint will be removed, not the dependent
    /// table itself.
    async fn drop_table(&self, table: &str) -> Result<(), DbError> {
        let table = validate_table_name(table)?;
        self.execute(&format!(r#"DROP TABLE IF EXISTS "{table}" CASCADE"#), ())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;
    use pretty_assertions::assert_eq;
    use rust_decimal::dec;
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

        // Clean up:
        pool.drop_table("test_table_text").await.unwrap();
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_int;\
             CREATE TABLE test_table_int ( value_2 INT2, value_4 INT4, value_8 INT8 )",
        ))
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_table_int VALUES ($1, $2, $3)",
            params![1_i16, 1_i32, 1_i64],
        )
        .await
        .unwrap();

        for column in ["value_2", "value_4", "value_8"] {
            let params = match column {
                "value_2" => params![1_i16],
                "value_4" => params![1_i32],
                "value_8" => params![1_i64],
                _ => unreachable!(),
            };
            let select_sql = format!("SELECT {column} FROM test_table_int WHERE {column} = $1");
            let value = pool
                .query_value(&select_sql, params.clone())
                .await
                .unwrap()
                .as_i64()
                .unwrap();
            assert_eq!(1, value);

            let value = pool.query_u64(&select_sql, params.clone()).await.unwrap();
            assert_eq!(1, value);

            let value = pool.query_i64(&select_sql, params.clone()).await.unwrap();
            assert_eq!(1, value);

            let value = pool
                .query_string(&select_sql, params.clone())
                .await
                .unwrap();
            assert_eq!("1", value);

            let row = pool.query_row(&select_sql, params.clone()).await.unwrap();
            assert_eq!(json!(row), json!({format!("{column}"):1}));

            let rows = pool.query(&select_sql, params.clone()).await.unwrap();
            assert_eq!(json!(rows), json!([{format!("{column}"):1}]));
        }

        // Clean up:
        pool.drop_table("test_table_int").await.unwrap();
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
        pool.execute("INSERT INTO test_table_float VALUES ($1)", &[1.05_f64])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = pool
            .query_value(select_sql, &[1.0_f64])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = pool.query_f64(select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(1.05, value);

        let value = pool.query_string(select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!("1.05", value);

        let row = pool.query_row(select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = pool.query(select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));

        // FLOAT4
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_float;\
             CREATE TABLE test_table_float ( value FLOAT4 )",
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_table_float VALUES ($1)", &[1.05_f32])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = pool
            .query_value(select_sql, &[1.0_f32])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        // Clean up:
        pool.drop_table("test_table_float").await.unwrap();
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
            params!["foo", (), 1.05_f64, (), 1_i64, (), true, (), dec!(1.0), ()],
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
        let params = params!["foo", (), 1.0_f64, 0_i64, true, dec!(0.999)];

        let row = pool.query_row(select_sql, params.clone()).await.unwrap();
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
                "numeric_value": 1.0,
                "alt_numeric_value": JsonValue::Null,
            })
        );

        let rows = pool.query(select_sql, params.clone()).await.unwrap();
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
                "numeric_value": 1.0,
                "alt_numeric_value": JsonValue::Null,
            }])
        );

        // Clean up:
        pool.drop_table("test_table_mixed").await.unwrap();
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
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"max": 1}]));

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
        assert_eq!(json!(rows), json!([{"max_int_value": 1}]));

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value": "1"}]));

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value_cast": "1"}]));

        // Clean up.
        pool.drop_table("test_table_indirect").await.unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
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

        // Insert rows:
        pool.insert(
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

        // Validate the inserted data:
        let rows = pool
            .query(r#"SELECT * FROM test_insert"#, ())
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

        // Clean up.
        pool.drop_table("test_insert").await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_returning() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_insert_returning;\
             CREATE TABLE test_insert_returning (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();

        // No filtering:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &[
                    &json!({"text_value": "TEXT"}).as_object().unwrap(),
                    &json!({"int_value": 1, "bool_value": true})
                        .as_object()
                        .unwrap(),
                ],
                &[],
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

        // With filtering:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &[
                    &json!({"text_value": "TEXT"}).as_object().unwrap(),
                    &json!({"int_value": 1, "bool_value": true})
                        .as_object()
                        .unwrap(),
                ],
                &["int_value", "float_value"],
            )
            .await
            .unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "float_value": JsonValue::Null,
                "int_value": JsonValue::Null,
            },{
                "float_value": JsonValue::Null,
                "int_value": 1,
            }])
        );

        // Clean up.
        pool.drop_table("test_insert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn drop_table() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        let table1 = "test_drop1";
        let table2 = "test_drop2";
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table1};\
             DROP TABLE IF EXISTS {table2};\
             CREATE TABLE {table1} (\
                 foo TEXT PRIMARY KEY\
             );\
             CREATE TABLE {table2} (\
                 foo TEXT REFERENCES {table1}(foo)\
             );",
        ))
        .await
        .unwrap();

        let columns = pool.get_columns(table1).await.unwrap();
        assert_eq!(columns, ["foo"]);
        pool.drop_table(table1).await.unwrap();

        let columns: Vec<String> = pool.get_columns(table1).await.unwrap();
        assert_eq!(columns, Vec::<String>::new());

        // Clean up.
        pool.drop_table(table2).await.unwrap();
    }

    #[tokio::test]
    async fn test_input_params() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute("DROP TABLE IF EXISTS test_input_params", ())
            .await
            .unwrap();
        pool.execute(
            "CREATE TABLE test_input_params (\
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
        pool.execute("INSERT INTO test_input_params (bar) VALUES ($1)", &["one"])
            .await
            .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (far) VALUES ($1)",
            &[1 as i64],
        )
        .await
        .unwrap();
        pool.execute("INSERT INTO test_input_params (bar) VALUES ($1)", ["two"])
            .await
            .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (far) VALUES ($1)",
            [2 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (bar) VALUES ($1)",
            vec!["three"],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (far) VALUES ($1)",
            vec![3 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (gar) VALUES ($1)",
            vec![3 as f32],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (har) VALUES ($1)",
            vec![3 as f64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (jar) VALUES ($1)",
            vec![dec!(3)],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params (kar) VALUES ($1)",
            vec![true],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_input_params \
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
                "SELECT COUNT(1) AS count FROM test_input_params \
                 WHERE bar IS NOT DISTINCT FROM $1 AND far IS NOT DISTINCT FROM $2",
                params![ParamValue::Null, ()],
            )
            .await
            .unwrap();
        assert_eq!(json!({"count": 4}), json!(row));

        // Clean up:
        pool.drop_table("test_input_params").await.unwrap();
    }
}
