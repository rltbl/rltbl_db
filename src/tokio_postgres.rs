//! tokio-postgres implementation for rltbl_db.

use crate::{
    core::{
        ColumnMap, DbError, DbKind, DbQuery, IntoParams, JsonRow, JsonValue, ParamValue, Params,
        validate_table_name,
    },
    params,
    shared::{EditType, edit},
};

use deadpool_postgres::{Config, Pool, Runtime};
use rust_decimal::Decimal;
use tokio_postgres::{
    NoTls,
    row::Row,
    types::{ToSql, Type},
};

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query
pub static MAX_PARAMS_POSTGRES: usize = 65535;

/// Extracts the value at the given index from the given [Row].
fn extract_value(row: &Row, idx: usize) -> Result<JsonValue, DbError> {
    let column = &row.columns()[idx];
    match *column.type_() {
        Type::TEXT | Type::VARCHAR | Type::NAME => match row
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
        _ => {
            eprint!("Unimplemented column type: {column:?}");
            unimplemented!();
        }
    }
}

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
                    .map_err(|err| {
                        DbError::ConnectError(format!("Error creating pool: {err:?}"))
                    })?;
                Ok(Self { pool })
            }
            false => Err(DbError::ConnectError(format!(
                "Invalid PostgreSQL URL: '{url}'"
            ))),
        }
    }
}

impl DbQuery for TokioPostgresPool {
    /// Implements [DbQuery::kind()] for PostgreSQL.
    fn kind(&self) -> DbKind {
        DbKind::PostgreSQL
    }

    /// Implements [DbQuery::parse()] for PostgreSQL.
    fn parse(&self, sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
        let err = || {
            Err(DbError::DataError(format!(
                "Could not parse '{sql_type}' from '{value}'"
            )))
        };
        match sql_type.to_lowercase().as_str() {
            "text" => Ok(ParamValue::Text(value.to_string())),
            "bool" | "boolean" => match value.to_lowercase().as_str() {
                // TODO: improve this
                "true" | "1" => Ok(ParamValue::Boolean(true)),
                _ => Ok(ParamValue::Boolean(false)),
            },
            "smallint" | "smallinteger" => match value.parse::<i16>() {
                Ok(int) => Ok(ParamValue::SmallInteger(int)),
                Err(_) => err(),
            },
            "int" | "integer" => match value.parse::<i32>() {
                Ok(int) => Ok(ParamValue::Integer(int)),
                Err(_) => err(),
            },
            "bigint" | "biginteger" => match value.parse::<i64>() {
                Ok(int) => Ok(ParamValue::BigInteger(int)),
                Err(_) => err(),
            },
            "real" => match value.parse::<f32>() {
                Ok(float) => Ok(ParamValue::Real(float)),
                Err(_) => err(),
            },
            "bigreal" => match value.parse::<f64>() {
                Ok(float) => Ok(ParamValue::BigReal(float)),
                Err(_) => err(),
            },
            // WARN: Treat NUMERIC as an f64.
            "numeric" => match value.parse::<f64>() {
                Ok(float) => Ok(ParamValue::Numeric(
                    Decimal::from_f64_retain(float).unwrap_or_default(),
                )),
                Err(_) => err(),
            },
            _ => Err(DbError::DatatypeError(format!(
                "Unhandled SQL type: {sql_type}"
            ))),
        }
    }

    /// Implements [DbQuery::columns()] for PostgreSQL.
    async fn columns(&self, table: &str) -> Result<ColumnMap, DbError> {
        let mut columns = ColumnMap::new();
        let sql = format!(
            r#"SELECT
                 "columns"."column_name"::TEXT,
                 "columns"."data_type"::TEXT
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
            match (
                row.get("column_name")
                    .and_then(|name| name.as_str().and_then(|name| Some(name))),
                row.get("data_type")
                    .and_then(|name| name.as_str().and_then(|name| Some(name))),
            ) {
                (Some(column), Some(sql_type)) => {
                    columns.insert(column.to_string(), sql_type.to_lowercase().to_string())
                }
                _ => {
                    return Err(DbError::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }

        Ok(columns)
    }

    /// Implements [DbQuery::primary_keys()] for PostgreSQL.
    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, DbError> {
        self.query(
            r#"SELECT "kcu"."column_name"
               FROM "information_schema"."table_constraints" "tco"
               JOIN "information_schema"."key_column_usage" "kcu"
                 ON "kcu"."constraint_name" = "tco"."constraint_name"
                AND "kcu"."constraint_schema" = "tco"."constraint_schema"
                AND "kcu"."table_name" = $1
                AND "tco"."constraint_type" ILIKE 'primary key'
              WHERE "kcu"."table_schema" IN (
                SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                FROM "pg_settings"
                WHERE "name" = 'search_path'
              )
              ORDER by "kcu"."ordinal_position""#,
            params![&table],
        )
        .await?
        .iter()
        .map(|row| {
            match row
                .get("column_name")
                .and_then(|name| name.as_str().and_then(|name| Some(name)))
            {
                Some(pk_col) => Ok(pk_col.to_string()),
                None => Err(DbError::DataError("Empty row".to_owned())),
            }
        })
        .collect()
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err:?}")))?;
        client
            .batch_execute(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err:?}")))?;
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
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err:?}")))?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err:?}")))?
            .params()
            .to_vec();

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let gen_err = |param: &ParamValue, sql_type: &str| -> String {
            format!("Param {param:?} is wrong type for {sql_type} in query: {sql}")
        };
        match into_params {
            Params::None => (),
            Params::Positional(plist) => {
                for (i, param) in plist.iter().enumerate() {
                    let pg_type = &param_pg_types[i];
                    match pg_type {
                        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
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
                        _ => unimplemented!(),
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
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err:?}")))?;
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

    /// Implements [DbQuery::insert()] for PostgreSQL
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for PostgreSQL
    async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for PostgreSQL.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for PostgreSQL.
    async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for PostgreSQL.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for PostgreSQL.
    async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
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

// TODO: Consider refactoring some of these tests, possibly into a separate shared_tests.rs
// file, with functions defined to accept a DbQuery parameter. A lot of the tests in this module
// have nearly identical code with the unit test module in rusqlite.rs.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect CASCADE;\
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
}
